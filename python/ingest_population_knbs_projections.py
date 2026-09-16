#!/usr/bin/env python3
"""
ingest_population_knbs_projections.py — KNBS official population projections 2020-2045 -> parquet
(KE-39 exposure, issue #28 item 2).

WHY: the KE-ENSO flood and drought work is framed forward, but every exposure count is computed on a
2020 population surface. Kenya's official trajectory is 48.8 M (2020) -> 53.3 M (2025) -> 57.8 M
(2030) -> 70.2 M (2045); an exposure headcount read as "current" off a 2020 grid understates the
country by millions, and the gap widens each year the Explorer stays live. This ingest supplies the
official county trajectory so `R/observational/7_zonal_exposure.R` can use a projection year as the
denominator (`--pop-source knbs-projection --pop-year 2025`).

SOURCE (KNBS-hosted, no auth, verified 2026-09-15):
  https://www.knbs.or.ke/wp-content/uploads/2023/09/2019-Kenya-population-and-Housing-Census-Analytical-Report-on-Population-Projections.pdf
  2019 KPHC Analytical Report on Population Projections, Volume XVI (KNBS, 2022). 244 pp, 16.8 MB.
  The 2024/05 re-upload `...-Analytical-Report-on-Population-Projections-Vol-XVI.pdf` (32.9 MB) is
  the SAME report — extracted text is byte-identical — so either URL is fine; the smaller one is
  used here. Appendix 5 "Projected Population by Age, Sex and County, 2020-2045" holds one block per
  county (48 blocks: Kenya + 47 counties), each with:
    Panel A  milestone years 2020/2025/2030/2035/2040/2045 x 5-year age group x sex (+ sex ratios)
    Panel B  components of change (births/deaths/migration) — NOT ingested
    Panels C-E  annual years 2020-2035 x 5-year age group x sex
  Union of the panels = annual 2020-2035 plus 2040 and 2045. Overlapping years (2020/2025/2030/2035
  appear in more than one panel) are cross-checked for equality rather than trusted blind.

ADMIN LEVEL: county (adm1) ONLY. Volume XVI does not project below county — there is not one
sub-county name anywhere in the report. Anything wanting an adm2 projection has to disaggregate with
a gridded weight and say so; this ingest will not invent the split. County labels map to IEBC COD-AB
adm1_pcode via _knbs_admin.py (the PDF line-wraps "Elgeiyo Marakwet", so its table headers read
"Marakwet" — handled by an explicit alias).

Requires: python3 stdlib + `pdftotext` (poppler-utils) on PATH + pyarrow for parquet (falls back to
CSV with a warning). No auth.

RUN (cglabs): python3 python/ingest_population_knbs_projections.py --smoke   # parse + gates, no write
              python3 python/ingest_population_knbs_projections.py
Output: <out>/population_knbs_projections_adm0.parquet        Kenya x year x age x sex
        <out>/population_knbs_projections_adm1.parquet        county x year x age x sex
        <out>/population_knbs_projections_adm1_totals.parquet county x year, All Ages (zonal input)
Publish: R/observational/6_publish_obs_to_s3.R --full --tier 18
         (domain=exposure/type=population/source=knbs-projections-2020-2045)
"""
import argparse
import os
import re
import shutil
import subprocess
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _knbs_admin import (  # noqa: E402
    check_county_order,
    PCODE_NAME,
    log,
    resolve_county,
    write_table,
)

PDF_URL = (
    "https://www.knbs.or.ke/wp-content/uploads/2023/09/"
    "2019-Kenya-population-and-Housing-Census-Analytical-Report-on-Population-Projections.pdf"
)

# Published national totals (Appendix 5, Kenya block, All Ages) — hard gate on the parse.
EXPECT_NATIONAL = {
    2020: 48_817_537, 2025: 53_330_978, 2030: 57_811_161,
    2035: 62_164_808, 2040: 66_306_796, 2045: 70_179_943,
}
EXPECT_YEARS = set(range(2020, 2036)) | {2040, 2045}
AGE_SUM_TOL = 10        # see the gate below — measured worst case is 5

AGE_RE = re.compile(r"^\s*(All Ages|\d{1,3}\s*-\s*\d{1,3}|\d{1,3}\+)\s+(\S.*)$")
NUM_RE = re.compile(r"\d[\d,]*")
# a block header is "<name> <year> <year> ..." — Panel A repeats two years at the end (sex-ratio cols)
HEAD_RE = re.compile(r"^\s*([A-Za-z][A-Za-z .'/-]*?)\s+((?:(?:19|20)\d{2}\s+)*(?:19|20)\d{2})\s*$")
AGE_HEAD_RE = re.compile(r"^\s*Age\s+(Male\s+Female\s+Total\s*)+$")
SEXES = ("male", "female", "total")


def pdf_to_text(pdf_path, txt_path):
    if not shutil.which("pdftotext"):
        raise RuntimeError("pdftotext (poppler-utils) not on PATH — required to read the KNBS PDF")
    subprocess.run(["pdftotext", "-layout", pdf_path, txt_path], check=True)
    log(f"  pdftotext -layout -> {os.path.basename(txt_path)} "
        f"({sum(1 for _ in open(txt_path, errors='replace')):,} lines)")
    return txt_path


def parse_blocks(lines):
    """Yield (block_name, years, rows) where rows = [(age_group, [(m, f, t) per year])]."""
    i = 0
    while i < len(lines):
        if not AGE_HEAD_RE.match(lines[i]):
            i += 1
            continue
        nyears = lines[i].count("Total")
        # the block header is the closest non-blank line above the Age header
        j = i - 1
        while j >= 0 and not lines[j].strip():
            j -= 1
        head = HEAD_RE.match(lines[j]) if j >= 0 else None
        if not head:
            i += 1
            continue
        name = head.group(1).strip()
        years = [int(y) for y in head.group(2).split()][:nyears]
        rows, k = [], i + 1
        while k < len(lines):
            m = AGE_RE.match(lines[k])
            if not m:
                if lines[k].strip() and rows:      # first non-table line after the rows ends the block
                    break
                k += 1
                continue
            nums = [int(n.replace(",", "")) for n in NUM_RE.findall(m.group(2))]
            if len(nums) < 3 * nyears:
                break                              # truncated/wrapped row — stop rather than misalign
            age = re.sub(r"\s+", "", m.group(1)) if m.group(1) != "All Ages" else "All Ages"
            rows.append((age, [tuple(nums[3 * y: 3 * y + 3]) for y in range(nyears)]))
            if m.group(1) == "All Ages":
                k += 1
                break
            k += 1
        if rows:
            yield name, years, canonical_top_age(rows, name, years)
        i = max(k, i + 1)


TOP_AGE = "80+"
# Age groups run 0-4 .. 75-79 and then one open-ended group. Vol XVI prints that group as "80+"
# almost everywhere, but a few panels label it "80-84" (e.g. Kiambu's 2026-2030 panel), which would
# otherwise read as a 18th age group and double-count against the "80+" of the overlapping panels.
SECOND_TOP_AGE = "75-79"


def canonical_top_age(rows, name, years):
    """Relabel a mislabelled open-ended top age group to "80+"; loud, never silent."""
    body = [r for r in rows if r[0] != "All Ages"]
    if len(body) < 2 or body[-1][0] == TOP_AGE or body[-2][0] != SECOND_TOP_AGE:
        return rows
    bad = body[-1][0]
    log(f"  SOURCE LABEL fixed: {name} {years[0]}-{years[-1]} panel labels its open-ended top age "
        f"group '{bad}' (every other panel says '{TOP_AGE}') — relabelled to '{TOP_AGE}'")
    return [((TOP_AGE if a is bad or a == bad else a), v) for a, v in rows]


def collect(lines):
    """
    Parse every block into {(scope_key, year, age, sex): value}, cross-checking the years that
    appear in more than one panel.

    Where two panels disagree, the tie is broken on the row's OWN internal consistency (male +
    female == total, +/-1 for rounding): a cell that contradicts the total it is printed next to
    loses to one that does not. This is not hypothetical — Vol XVI has at least one typo of exactly
    this shape (Busia 2030, 80+, female: 3,120 in the 2030-2035 panel against 5,799 in the other
    two, where the printed row total 9,714 only reconciles with 5,799). Resolutions are returned so
    they are logged, never silent; a disagreement where BOTH readings are internally consistent is
    left as a hard conflict.
    """
    values, order, conflicts, resolved = {}, [], [], []
    for name, years, rows in parse_blocks(lines):
        is_national = name.strip().lower() == "kenya"
        key = "KE" if is_national else resolve_county(name)[0]
        if key not in order:
            order.append(key)
        for age, per_year in rows:
            for yi, year in enumerate(years):
                m, f, t = per_year[yi]
                ok = abs(m + f - t) <= 1                      # row internally consistent?
                for si, sex in enumerate(SEXES):
                    k, v = (key, year, age, sex), per_year[yi][si]
                    if k in values:
                        prev, prev_ok = values[k]
                        if prev == v:
                            continue
                        if prev_ok and not ok:
                            resolved.append((k, v, prev))     # discard v, keep the consistent prev
                            continue
                        if ok and not prev_ok:
                            resolved.append((k, prev, v))     # discard prev, take the consistent v
                        else:
                            conflicts.append((k, prev, v))
                    values[k] = (v, ok)
    return {k: v for k, (v, _ok) in values.items()}, order, conflicts, resolved


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="Data/exposure/knbs_projections")
    ap.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    ap.add_argument("--cache-dir", default=None,
                    help="where the PDF + extracted text are kept (default <out>/.tmp, deleted on "
                         "success); point it at a persistent dir to avoid re-downloading 17 MB")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--smoke", action="store_true", help="download + parse + run the gates, write nothing")
    a = ap.parse_args()

    log(f"KNBS Vol XVI projections ingest | out={a.out} smoke={a.smoke}")
    os.makedirs(a.out, exist_ok=True)
    tmp = a.cache_dir or os.path.join(a.out, ".tmp")
    os.makedirs(tmp, exist_ok=True)

    totals_path = os.path.join(a.out, "population_knbs_projections_adm1_totals.parquet")
    if not a.overwrite and not a.smoke and os.path.exists(totals_path):
        log("population_knbs_projections: exists, skip (use --overwrite to rebuild)")
        return

    pdf = os.path.join(tmp, "knbs_projections_vol16.pdf")
    if not (os.path.exists(pdf) and os.path.getsize(pdf) > 0):
        log("  downloading Vol XVI PDF (~17 MB) ...")
        urllib.request.urlretrieve(PDF_URL, pdf)
    log(f"  PDF {os.path.getsize(pdf)/1e6:.1f} MB")
    txt = pdf_to_text(pdf, os.path.join(tmp, "knbs_projections_vol16.txt"))
    lines = open(txt, errors="replace").read().splitlines()

    values, order, conflicts, resolved = collect(lines)
    counties = [k for k in order if k != "KE"]
    years = sorted({k[1] for k in values})
    ages = [a_ for a_ in dict.fromkeys(k[2] for k in values) if a_ != "All Ages"]
    log(f"  parsed {len(values):,} values | {len(counties)} counties + "
        f"{'national' if 'KE' in order else 'NO NATIONAL BLOCK'} | years {years[0]}-{years[-1]} "
        f"({len(years)}) | {len(ages)} age groups")

    # --- gates ---------------------------------------------------------------
    for k, dropped, kept in resolved:
        log(f"  SOURCE TYPO resolved: {k[0]} {k[1]} {k[2]} {k[3]} — panel value {dropped:,} "
            f"contradicts its own row total; using {kept:,} from the agreeing panels")
    if conflicts:
        sample = "; ".join(f"{k} {a_} vs {b}" for k, a_, b in conflicts[:5])
        raise RuntimeError(f"{len(conflicts)} overlapping-year values disagree between panels and "
                           f"both readings are internally consistent: {sample}")
    check_county_order(counties, "Vol XVI Appendix 5")
    if set(years) != EXPECT_YEARS:
        raise RuntimeError(f"year coverage {sorted(set(years))} != expected {sorted(EXPECT_YEARS)}")
    for year, expect in EXPECT_NATIONAL.items():
        got = values.get(("KE", year, "All Ages", "total"))
        if got != expect:
            raise RuntimeError(f"national All Ages {year}: parsed {got:,} != published {expect:,}")
    for year in years:                                   # counties must sum to the national row
        csum = sum(values[(c, year, "All Ages", "total")] for c in counties)
        nat = values[("KE", year, "All Ages", "total")]
        if abs(csum - nat) > len(counties):              # tolerate per-county rounding, 1 person each  # noqa: E501
            raise RuntimeError(f"{year}: county sum {csum:,} != national {nat:,}")
    # KNBS rounds each printed cell independently, so sums do not close exactly. Measured on the
    # full parse: age-group sums land within +/-5 of All Ages, county sums within ~20 of national.
    # Tolerances sit just above that — tight enough that a misaligned column or a double-counted age
    # group (both of which run to thousands) still trips the gate.
    for (scope, year, sex) in {(k[0], k[1], k[3]) for k in values}:   # age rows must sum to All Ages
        s = sum(v for k, v in values.items() if k[:2] == (scope, year) and k[3] == sex
                and k[2] != "All Ages")
        allages = values[(scope, year, "All Ages", sex)]
        if abs(s - allages) > AGE_SUM_TOL:
            raise RuntimeError(f"{scope} {year} {sex}: age groups sum {s:,} != All Ages "
                               f"{allages:,} (tolerance {AGE_SUM_TOL})")
    log(f"  GATES OK: no panel conflicts; 47 counties in COD-AB order; national totals match the "
        f"published 48.8/53.3/57.8/62.2/66.3/70.2 M; counties sum to national; ages sum to All Ages")

    # --- assemble ------------------------------------------------------------
    adm0_rows, adm1_rows, tot_rows = [], [], []
    for (scope, year, age, sex), v in sorted(values.items()):
        if scope == "KE":
            adm0_rows.append(dict(adm0_pcode="KE", adm0_name="Kenya", year=year,
                                  age_group=age, sex=sex, population=v))
        else:
            adm1_rows.append(dict(adm1_pcode=scope, adm1_name=PCODE_NAME[scope], year=year,
                                  age_group=age, sex=sex, population=v))
    for scope in order:
        for year in years:
            row = dict(year=year,
                       pop_total=values[(scope, year, "All Ages", "total")],
                       pop_male=values[(scope, year, "All Ages", "male")],
                       pop_female=values[(scope, year, "All Ages", "female")])
            if scope == "KE":
                continue
            tot_rows.append(dict(adm1_pcode=scope, adm1_name=PCODE_NAME[scope], **row))

    if a.smoke:
        log("SMOKE: gates passed, nothing written")
        print(f"\n  Kenya   2020 {EXPECT_NATIONAL[2020]:,}   2025 {EXPECT_NATIONAL[2025]:,}   "
              f"2045 {EXPECT_NATIONAL[2045]:,}")
        for r in tot_rows[:3]:
            print(f"  {r['adm1_pcode']} {r['adm1_name']:<16} {r['year']} {r['pop_total']:>12,}")
        return

    base = dict(fmt=a.format)
    write_table(adm0_rows, ["adm0_pcode", "adm0_name", "year", "age_group", "sex", "population"],
                os.path.join(a.out, "population_knbs_projections_adm0.parquet"), **base)
    write_table(adm1_rows, ["adm1_pcode", "adm1_name", "year", "age_group", "sex", "population"],
                os.path.join(a.out, "population_knbs_projections_adm1.parquet"), **base)
    write_table(tot_rows, ["adm1_pcode", "adm1_name", "year", "pop_total", "pop_male", "pop_female"],
                totals_path, **base)

    if a.cache_dir is None:
        for f in os.listdir(tmp):
            os.remove(os.path.join(tmp, f))
        os.rmdir(tmp)
    log("DONE")
    print("\nNext: publish with  Rscript R/observational/6_publish_obs_to_s3.R --full --tier 18")


if __name__ == "__main__":
    main()
