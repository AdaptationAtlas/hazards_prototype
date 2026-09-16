#!/usr/bin/env python3
"""
ingest_population_knbs_census.py — KNBS 2019 Census of Population and Housing counts -> parquet
(KE-39 exposure, issue #28 item 1).

WHY: the KE-ENSO exposure stack carries two gridded population surfaces (WorldPop constrained
~55.2 M, GRID3/WOPR bottom-up ~55.9 M) and no official Kenyan denominator. The enumerated 2019
census total is 47.56 M — both grids sit ~17 % above it. Absolute headcounts published off a grid
are therefore ~17 % above what a Kenyan counterpart will compare them against. This ingest supplies
the official counts so `R/observational/7_zonal_exposure.R` can take the LEVEL from KNBS and the
within-county SHARE from the grid (see `--pop-source knbs-census-2019` there).

SOURCE (KNBS-hosted, no auth, verified 2026-09-15):
  https://www.knbs.or.ke/wp-content/uploads/2023/09/2019-Kenya-population-and-Housing-Census-Population-households-density-by-sub-county.xlsx
  47 KB xlsx, sheet "Pop by Sex and County": KENYA + 47 counties + 345 sub-county rows, columns
  Total / Male / Female / Households (conventional + group quarters) / Land area (sq km).
  Optional age-and-sex detail: HDX cod-ps-ken (UNFPA, CC-BY-IGO 3.0, sourced from KNBS 2019 Census
  Volume III), adm1 sheet, 5-year age groups x sex, keyed on ADM1_PCODE.

ADMIN LEVEL — READ THIS BEFORE USING THE adm2 TABLE:
  County (adm1) joins cleanly: 47 KNBS counties == 47 IEBC COD-AB adm1 units, resolved by the
  crosswalk in _knbs_admin.py and gated on census order KE001..KE047.
  Sub-county does NOT join. COD-AB adm2 (what the zonal tables key on) is 290 IEBC *constituencies*;
  the census reports 345 KNBS *sub-county* rows, a different universe (constituency splits such as
  Embakasi East/West/Central vs sub-county Embakasi, plus 12 forest and national-park units such as
  Aberdare Forest and Mt Elgon Forest, which carry small enumerated populations of their own and
  have no COD-AB counterpart). Normalised in-county name matching resolves 183 of the 290 COD-AB
  units (183 of the 345 KNBS rows); the workbook's own metadata sheet states 337 sub-counties.
  So the adm2 table is published in KNBS's OWN sub-county universe, name-keyed, with a best-effort
  COD-AB pcode attached ONLY where a normalised name matches exactly one COD-AB unit inside the same
  county (column `codab_match` records how each row resolved). Do not use it as an adm2 denominator;
  use adm1 + a gridded within-county share.

NOTE ON TOTALS: Male + Female = 47,562,772; the published Total is 47,564,296. The 1,524 difference
is the intersex count, which KNBS does not disseminate below national level (so sub-national
male+female sums fall marginally short of the sub-national totals). Both figures are kept
(`pop_total`, `pop_male`, `pop_female`, `pop_intersex`).

Requires: python3 stdlib + pyarrow (for parquet; falls back to CSV with a warning). No auth.

RUN (cglabs): python3 python/ingest_population_knbs_census.py --smoke   # parse + gates, no write
              python3 python/ingest_population_knbs_census.py
Output: <out>/population_knbs_census_adm0.parquet
        <out>/population_knbs_census_adm1.parquet
        <out>/population_knbs_census_adm1_agesex.parquet   (unless --no-age-sex)
        <out>/population_knbs_census_adm2_knbs.parquet
Publish: R/observational/6_publish_obs_to_s3.R --full --tier 17
         (domain=exposure/type=population/source=knbs-census-2019)
"""
import argparse
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _knbs_admin import (  # noqa: E402
    check_county_order,
    log,
    norm_name,
    resolve_county,
    write_table,
    xlsx_table,
)

KNBS_XLSX = (
    "https://www.knbs.or.ke/wp-content/uploads/2023/09/"
    "2019-Kenya-population-and-Housing-Census-Population-households-density-by-sub-county.xlsx"
)
KNBS_SHEET = "Pop by Sex and County"
CODPS_CKAN = "https://data.humdata.org/api/3/action/package_show?id=cod-ps-ken"
CODAB_CKAN = "https://data.humdata.org/api/3/action/package_show?id=cod-ab-ken"

# published national figures, used as hard gates on the parse
EXPECT_NATIONAL_TOTAL = 47_564_296
EXPECT_COUNTIES = 47


def fetch(url, dest):
    """Download unless `dest` is already populated — lets --cache-dir hold the source files."""
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        log(f"  reusing cached {os.path.basename(dest)} ({os.path.getsize(dest)/1e3:.0f} KB)")
        return dest
    log(f"  downloading {os.path.basename(dest)} ...")
    urllib.request.urlretrieve(url, dest)
    log(f"    got {os.path.getsize(dest)/1e3:.0f} KB")
    return dest


def ckan_resource(ckan_url, want_name):
    """Resolve an HDX resource download URL by (case-insensitive) file name — no hardcoded UUIDs."""
    with urllib.request.urlopen(ckan_url, timeout=60) as r:
        pkg = json.load(r)["result"]
    for res in pkg.get("resources", []):
        if (res.get("name") or "").lower() == want_name.lower():
            return res.get("download_url") or res.get("url")
    raise RuntimeError(f"resource '{want_name}' not found on {ckan_url}")


def num(x):
    """'1,208,333' / '219.89648' / None -> int|float|None."""
    if x is None:
        return None
    s = str(x).replace(",", "").strip()
    if not s or s in {"-", "..", "…"}:
        return None
    f = float(s)
    return int(f) if f.is_integer() else f


# ---------------------------------------------------------------------------
# KNBS workbook -> national / county / sub-county records
# ---------------------------------------------------------------------------
# Column order in "Pop by Sex and County" (two header rows, then data):
#   0 name (indent encodes level)  1 Total  2 Male  3 Female
#   4 Households total  5 Households conventional  6 Households group quarters  7 Land area sq km
COLS = ("pop_total", "pop_male", "pop_female", "households", "households_conventional",
        "households_group_quarters", "land_area_km2")


def parse_knbs(path):
    _hdr, rows = xlsx_table(path, KNBS_SHEET)
    national, counties, subcounties = None, [], []
    current = None
    for r in rows:
        raw = r[0]
        if raw is None or not raw.strip():
            continue
        label = raw.strip()
        if num(r[1]) is None:          # header continuation / source footnote
            continue
        vals = {c: num(r[i + 1]) for i, c in enumerate(COLS)}
        indent = len(raw) - len(raw.lstrip())
        if label.upper() == "KENYA":
            national = vals
        elif indent <= 4:
            current = label
            counties.append((label, vals))
        else:
            subcounties.append((current, label, vals))
    if national is None:
        raise RuntimeError("no KENYA row found in the KNBS workbook")
    return national, counties, subcounties


def derive(vals):
    """Add intersex residual + density; both are derived, not published by KNBS."""
    out = dict(vals)
    t, m, f = vals.get("pop_total"), vals.get("pop_male"), vals.get("pop_female")
    out["pop_intersex"] = (t - m - f) if None not in (t, m, f) else None
    area = vals.get("land_area_km2")
    out["pop_density_km2"] = round(t / area, 2) if t and area else None
    return out


# ---------------------------------------------------------------------------
# optional COD-AB sub-county name match (best effort, honestly labelled)
# ---------------------------------------------------------------------------
def codab_adm2_index(tmp):
    """{adm1_pcode: {normalised adm2 name: [adm2_pcode, ...]}} from HDX cod-ab-ken tabular xlsx."""
    url = ckan_resource(CODAB_CKAN, "ken_admin_boundaries.xlsx")
    path = fetch(url, os.path.join(tmp, "ken_admin_boundaries.xlsx"))
    hdr, rows = xlsx_table(path, "ken_admin2")
    i2, p2, p1 = hdr.index("adm2_name"), hdr.index("adm2_pcode"), hdr.index("adm1_pcode")
    idx = {}
    for r in rows:
        idx.setdefault(r[p1], {}).setdefault(norm_name(r[i2]), []).append(r[p2])
    log(f"  COD-AB adm2 index: {sum(len(v) for v in idx.values())} units in {len(idx)} counties")
    return idx


def match_adm2(idx, adm1_pcode, knbs_name):
    """Return (adm2_pcode|None, how). Only a unique in-county name match is accepted."""
    if idx is None:
        return None, "not-attempted"
    hits = idx.get(adm1_pcode, {}).get(norm_name(knbs_name), [])
    if len(hits) == 1:
        return hits[0], "unique-name"
    return None, ("ambiguous-name" if hits else "no-match")


# ---------------------------------------------------------------------------
# optional age x sex detail (HDX cod-ps-ken, adm1, from census Volume III)
# ---------------------------------------------------------------------------
def parse_agesex(tmp):
    url = ckan_resource(CODPS_CKAN, "ken_admpop_2019.xlsx")
    path = fetch(url, os.path.join(tmp, "ken_admpop_2019.xlsx"))
    hdr, rows = xlsx_table(path, "ken_admpop_ADM1_2019")
    ip, inm = hdr.index("ADM1_PCODE"), hdr.index("ADM1_NAME")
    # age columns look like F_00_04 / M_80_84 / T_100Plus / F_Unstated / F_TL
    age_cols = [(i, h) for i, h in enumerate(hdr)
                if h and h[:2] in ("F_", "M_", "T_") and h not in ("F_TL", "M_TL", "T_TL")]
    out = []
    for r in rows:
        pcode, _name = resolve_county(r[inm]) if r[inm] else (r[ip], None)
        for i, h in age_cols:
            sex = {"F": "female", "M": "male", "T": "total"}[h[0]]
            age = h[2:]
            out.append(dict(
                adm1_pcode=pcode,
                adm1_name=_name,
                sex=sex,
                age_group=("100+" if age == "100Plus" else
                           "unstated" if age.lower() == "unstated" else
                           age.replace("_", "-").lstrip("0") or "0-4"),
                population=num(r[i]),
            ))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="Data/exposure/knbs_census")
    ap.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    ap.add_argument("--no-age-sex", action="store_true", help="skip the HDX cod-ps-ken age x sex table")
    ap.add_argument("--no-codab-match", action="store_true", help="skip the best-effort adm2 name match")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--cache-dir", default=None,
                    help="where source downloads are kept (default <out>/.tmp, deleted on success); "
                         "point it at a persistent dir to re-run without re-downloading")
    ap.add_argument("--smoke", action="store_true", help="download + parse + run the gates, write nothing")
    a = ap.parse_args()

    log(f"KNBS 2019 census ingest | out={a.out} smoke={a.smoke}")
    os.makedirs(a.out, exist_ok=True)
    tmp = a.cache_dir or os.path.join(a.out, ".tmp")
    os.makedirs(tmp, exist_ok=True)

    adm0_path = os.path.join(a.out, "population_knbs_census_adm0.parquet")
    if not a.overwrite and not a.smoke and os.path.exists(adm0_path):
        log("population_knbs_census_adm0: exists, skip (use --overwrite to rebuild)")
        return

    national, counties, subcounties = parse_knbs(fetch(KNBS_XLSX, os.path.join(tmp, "knbs_subcounty.xlsx")))
    log(f"  parsed: national + {len(counties)} counties + {len(subcounties)} sub-counties")

    # --- gates ---------------------------------------------------------------
    if national["pop_total"] != EXPECT_NATIONAL_TOTAL:
        raise RuntimeError(f"national total {national['pop_total']:,} != published "
                           f"{EXPECT_NATIONAL_TOTAL:,} — the workbook layout changed")
    if len(counties) != EXPECT_COUNTIES:
        raise RuntimeError(f"parsed {len(counties)} counties, expected {EXPECT_COUNTIES}")
    resolved = [resolve_county(n)[0] for n, _ in counties]
    check_county_order(resolved, "KNBS census workbook")
    csum = sum(v["pop_total"] for _n, v in counties)
    if csum != national["pop_total"]:
        raise RuntimeError(f"county sum {csum:,} != national {national['pop_total']:,}")
    ssum = sum(v["pop_total"] for _c, _n, v in subcounties)
    if ssum != national["pop_total"]:
        raise RuntimeError(f"sub-county sum {ssum:,} != national {national['pop_total']:,}")
    log(f"  GATES OK: total {national['pop_total']:,}; 47 counties in COD-AB order; "
        f"county and sub-county sums reconcile")

    # --- assemble ------------------------------------------------------------
    adm0_rows = [dict(adm0_pcode="KE", adm0_name="Kenya", **derive(national))]
    adm1_rows = []
    for name, vals in counties:
        pcode, codab_name = resolve_county(name)
        adm1_rows.append(dict(adm1_pcode=pcode, adm1_name=codab_name, adm1_name_knbs=name,
                              **derive(vals)))

    idx = None
    if not a.no_codab_match:
        try:
            idx = codab_adm2_index(tmp)
        except Exception as e:  # network/CKAN hiccup must not sink the census tables
            log(f"  WARNING: COD-AB match skipped ({e})")
    adm2_rows = []
    for county, name, vals in subcounties:
        pcode, codab_name = resolve_county(county)
        m_pcode, how = match_adm2(idx, pcode, name)
        adm2_rows.append(dict(adm1_pcode=pcode, adm1_name=codab_name, adm2_name_knbs=name,
                              adm2_pcode_codab=m_pcode, codab_match=how, **derive(vals)))
    matched = sum(1 for r in adm2_rows if r["adm2_pcode_codab"])
    log(f"  COD-AB adm2 name match: {matched}/{len(adm2_rows)} KNBS sub-counties resolved to a "
        f"unique in-county COD-AB unit (expected ~183/290 — the two universes differ; see header)")

    agesex_rows = []
    if not a.no_age_sex:
        try:
            agesex_rows = parse_agesex(tmp)
            tot = sum(r["population"] or 0 for r in agesex_rows if r["sex"] == "total")
            log(f"  age x sex: {len(agesex_rows)} rows, total-sex sum {tot:,} "
                f"(census M+F, excludes the {national['pop_total'] - tot:,} intersex)")
        except Exception as e:
            log(f"  WARNING: age x sex table skipped ({e})")

    if a.smoke:
        log("SMOKE: gates passed, nothing written")
        print(f"\n  national : {national['pop_total']:,}")
        for r in adm1_rows[:3]:
            print(f"  {r['adm1_pcode']} {r['adm1_name']:<16} {r['pop_total']:>10,}")
        return

    base = dict(fmt=a.format)
    adm0_cols = ["adm0_pcode", "adm0_name", "pop_total", "pop_male", "pop_female", "pop_intersex",
                 "households", "households_conventional", "households_group_quarters",
                 "land_area_km2", "pop_density_km2"]
    adm1_cols = ["adm1_pcode", "adm1_name", "adm1_name_knbs"] + adm0_cols[2:]
    adm2_cols = ["adm1_pcode", "adm1_name", "adm2_name_knbs", "adm2_pcode_codab",
                 "codab_match"] + adm0_cols[2:]
    write_table(adm0_rows, adm0_cols, adm0_path, **base)
    write_table(adm1_rows, adm1_cols, os.path.join(a.out, "population_knbs_census_adm1.parquet"), **base)
    write_table(adm2_rows, adm2_cols,
                os.path.join(a.out, "population_knbs_census_adm2_knbs.parquet"), **base)
    if agesex_rows:
        write_table(agesex_rows, ["adm1_pcode", "adm1_name", "sex", "age_group", "population"],
                    os.path.join(a.out, "population_knbs_census_adm1_agesex.parquet"), **base)

    if a.cache_dir is None:                      # only sweep the dir we made ourselves
        for f in os.listdir(tmp):
            os.remove(os.path.join(tmp, f))
        os.rmdir(tmp)
    log("DONE")
    print("\nNext: publish with  Rscript R/observational/6_publish_obs_to_s3.R --full --tier 17")


if __name__ == "__main__":
    main()
