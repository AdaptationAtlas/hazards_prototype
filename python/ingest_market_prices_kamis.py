#!/usr/bin/env python3
"""
ingest_market_prices_kamis.py — KAMIS (Kenya Agricultural Market Information System) -> parquet.

WHY THIS SOURCE, AND NOT THE OBVIOUS ONES
  The KE-ENSO explorer already serves FEWS NET FDW prices (`market_prices.parquet`:
  27,406 rows, 2000-2026, incl. NDMA-sourced `Goats (Local Quality)` 4,864 rows / 20 counties
  and `Cattle (Male, 2-3 years old, Local Quality)` 919 rows / 3 counties). Re-ingesting FDW
  adds nothing. KAMIS is the only verified open Kenyan source that closes the gaps FDW leaves:

    * species        - CAMEL and SHEEP priced per head (FDW Kenya has neither)
    * quality        - an explicit Grade (1/2/3) and Sex per transaction
    * breed          - a Classification column (e.g. "Somali" camel)
    * volume         - Supply Volume alongside the price
    * frequency      - market-day rows, not a monthly aggregate
    * input/other    - 190 commodities incl. hides, skins, milks at collection point

  KIAMIS (`kiamis.kalro.org`) is a farmer registry and e-voucher stack and carries NO prices;
  `kiamis.go.ke` is NXDOMAIN. KAOP's own market feature is dead (`kamis.kaopdata.co.ke` is
  NXDOMAIN) and its UI just iframes KAMIS. See the scout note in the KE-ENSO playbook,
  `dispatches/2026-10-05_market-data-scout-kaop-kiamis-kaznet.md`.

SOURCE (verified 2026-10-05, from this machine, no auth)
  Portal   https://kamis.kilimo.go.ke/site/market          HTTP 200
  Search   https://kamis.kilimo.go.ke/site/market_search
           GET params: product[]=<id>  county[]=<NAME>  market[]=<name>
                       start=YYYY-MM-DD  end=YYYY-MM-DD  per_page=<n>  [export=excel]
  Table    Market | Commodity | Classification | Grade | Sex | Wholesale | Retail |
           Supply Volume | County | Date
  Prices   formatted with the unit inline: "37000.00/Head", "50.00/Kg". "-" means absent.
  Picker   191 product options (190 real), 50 county options (49 real). Verified ids:
           Cattle=140  Sheep=167  Goat=168  Camel=186  (full map pulled live, see --list)
  Example  /site/market_search?product[]=186&start=2024-01-01&end=2026-10-05&per_page=200
           -> 200 Camel rows, Garissa/Modogashe, 2024-03-29..2025-11-24, KSh/Head.

GOTCHAS - ALL OBSERVED, NOT ASSUMED
  1. `per_page` TRUNCATES, and rows come back DATE-DESCENDING. A single wide request silently
     returns only the newest N rows. This script therefore walks fixed date windows per product
     and FAILS LOUD if a window comes back full (see `--per-page` / window halving).
  2. `export=excel` sets `Content-Type: application/vnd.ms-excel` and `filename="Market Prices.xls"`
     but the body is OOXML (magic bytes `PK`). Read it with openpyxl, never xlrd.
  3. History floor is 2021 - a 2016-2020 query returns 0 rows. Do not present KAMIS as a
     long-run series; FDW is the long-run series (back to 2000).
  4. Dirty rows exist: a county literally named `test`, and order-of-magnitude price outliers
     inside one market-day (a Grade-2 Somali camel at 4,800 KSh next to 61,000 KSh ones).
     `--qc` flags these; nothing is silently dropped.
  5. No JSON API and no coordinates. Output is plain parquet, NOT GeoParquet: KAMIS gives a
     market NAME only. Geocoding would have to come from a separate gazetteer join (the KAOP
     ward endpoint `https://kaop.co.ke/weather_api/wards` is one open option, ward centroids).

OUTPUT (schema is a superset of the explorer's `market_prices.parquet`, so the two can UNION)
  county admin_2 market product price_type year month period_date value_kes unit currency
  iso3 source                                      <- incumbent columns
  classification grade sex supply_volume qc_flag   <- KAMIS additions

Requires: python3 stdlib + pyarrow (falls back to CSV with a warning). No auth, no key.

RUN
  python3 python/ingest_market_prices_kamis.py --list
  python3 python/ingest_market_prices_kamis.py --smoke
  python3 python/ingest_market_prices_kamis.py --products 140,167,168,186 \
      --start 2021-01-01 --end 2026-10-05 --out /abs/path/to/common_data/...
Idempotent: an existing output is skipped unless --overwrite.
"""
from __future__ import annotations

import argparse
import datetime as dt
import html
import io
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "https://kamis.kilimo.go.ke"
SEARCH = f"{BASE}/site/market_search"
FORM = f"{BASE}/site/market_search"
UA = "Mozilla/5.0 (compatible; AAAA-hazards-prototype/1.0; +https://github.com/AdaptationAtlas)"

# Verified live 2026-10-05 from the product picker. `--list` re-pulls and will
# report any drift rather than trusting this map.
LIVESTOCK = {140: "Cattle", 167: "Sheep", 168: "Goat", 186: "Camel"}
DEFAULT_PRODUCTS = sorted(LIVESTOCK)

HEADERS_EXPECTED = ["Market", "Commodity", "Classification", "Grade", "Sex",
                    "Wholesale", "Retail", "Supply Volume", "County", "Date"]

PRICE_RE = re.compile(r"^\s*([0-9][0-9,]*\.?[0-9]*)\s*/\s*(\w+)\s*$")

_t0 = time.time()


def log(msg: str) -> None:
    print(f"[{dt.datetime.now():%H:%M:%S} +{time.time() - _t0:7.1f}s] {msg}", flush=True)


# ---------------------------------------------------------------- http ------
def get(url: str, tries: int = 4, timeout: int = 90) -> str:
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", errors="replace")
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            last = e
            wait = 2 ** i
            log(f"  retry {i + 1}/{tries} in {wait}s after {type(e).__name__}: {e}")
            time.sleep(wait)
    raise RuntimeError(f"GET failed after {tries} tries: {url} ({last})")


def search_url(product_id: int, start: str, end: str, per_page: int,
               county: str | None = None) -> str:
    q = [("product[]", str(product_id)), ("start", start), ("end", end),
         ("per_page", str(per_page))]
    if county:
        q.append(("county[]", county))
    return f"{SEARCH}?{urllib.parse.urlencode(q)}"


# --------------------------------------------------------------- parse ------
def _text(fragment: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", fragment)).strip()


def parse_table(page: str) -> tuple[list[str], list[list[str]]]:
    hdr = [_text(h) for h in re.findall(r"<th[^>]*>(.*?)</th>", page, re.S)]
    rows = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", page, re.S):
        cells = [_text(td) for td in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
        if cells:
            rows.append(cells)
    return hdr, rows


def parse_price(cell: str) -> tuple[float | None, str | None]:
    """'37000.00/Head' -> (37000.0, 'Head'); '-' / '' -> (None, None)."""
    if not cell or cell.strip() in {"-", ""}:
        return None, None
    m = PRICE_RE.match(cell)
    if not m:
        return None, None
    try:
        return float(m.group(1).replace(",", "")), m.group(2)
    except ValueError:
        return None, None


def pull_codes() -> dict[str, list[tuple[str, str]]]:
    page = get(FORM)
    out: dict[str, list[tuple[str, str]]] = {}
    for name in ("product", "county"):
        m = re.search(r'<select[^>]*name="' + name + r'(?:\[\])?"[^>]*>(.*?)</select>',
                      page, re.S | re.I)
        if not m:
            out[name] = []
            continue
        opts = re.findall(r'<option[^>]*value="([^"]*)"[^>]*>(.*?)</option>', m.group(1), re.S)
        out[name] = [(v, _text(t)) for v, t in opts if v]
    return out


# ------------------------------------------------------------ windowing -----
def month_windows(start: str, end: str, step_months: int = 3):
    s = dt.date.fromisoformat(start)
    e = dt.date.fromisoformat(end)
    cur = s
    while cur <= e:
        y, m = divmod((cur.year * 12 + cur.month - 1) + step_months, 12)
        nxt = dt.date(y, m + 1, 1)
        stop = min(nxt - dt.timedelta(days=1), e)
        yield cur.isoformat(), stop.isoformat()
        cur = stop + dt.timedelta(days=1)


def fetch_window(pid: int, name: str, w0: str, w1: str, per_page: int,
                 depth: int = 0) -> list[list[str]]:
    """One (product, window) pull. Halves the window if the page comes back full,
    because KAMIS truncates at per_page in DATE-DESCENDING order (gotcha 1)."""
    page = get(search_url(pid, w0, w1, per_page))
    hdr, rows = parse_table(page)
    if hdr and hdr[:len(HEADERS_EXPECTED)] != HEADERS_EXPECTED:
        raise RuntimeError(
            f"KAMIS table schema drifted.\n  expected {HEADERS_EXPECTED}\n  got      {hdr}")
    if len(rows) >= per_page:
        if depth >= 6 or w0 == w1:
            log(f"  !! {name} {w0}..{w1}: {len(rows)} rows == per_page and cannot split "
                f"further - WINDOW IS TRUNCATED, rows are missing")
            return rows
        mid = dt.date.fromisoformat(w0) + (dt.date.fromisoformat(w1)
                                           - dt.date.fromisoformat(w0)) / 2
        log(f"  .. {name} {w0}..{w1}: hit per_page ({per_page}), splitting")
        return (fetch_window(pid, name, w0, mid.isoformat(), per_page, depth + 1)
                + fetch_window(pid, name, (mid + dt.timedelta(days=1)).isoformat(), w1,
                               per_page, depth + 1))
    return rows


# ----------------------------------------------------------------- qc -------
def qc_flags(rec: dict, by_group: dict) -> str:
    f = []
    if (rec["county"] or "").strip().lower() in {"test", "", "-"}:
        f.append("bad_county")
    if rec["value_kes"] is None:
        f.append("no_price")
    elif rec["value_kes"] <= 0:
        f.append("nonpositive")
    else:
        peers = by_group.get((rec["market"], rec["product"], rec["period_date"]), [])
        if len(peers) >= 3:
            peers = sorted(peers)
            med = peers[len(peers) // 2]
            if med > 0 and (rec["value_kes"] / med > 5 or rec["value_kes"] / med < 0.2):
                f.append("outlier_vs_marketday_median")
    if rec["unit"] and rec["product"] in LIVESTOCK.values() and rec["unit"].lower() != "head":
        f.append("unexpected_unit")
    return ",".join(f)


# ---------------------------------------------------------------- main ------
def build_records(rows: list[list[str]]) -> list[dict]:
    out = []
    for c in rows:
        if len(c) < len(HEADERS_EXPECTED):
            continue
        (market, commodity, classification, grade, sex,
         wholesale, retail, supply, county, date) = c[:10]
        if not re.match(r"\d{4}-\d{2}-\d{2}", date or ""):
            continue
        for ptype, cell in (("Wholesale", wholesale), ("Retail", retail)):
            val, unit = parse_price(cell)
            if val is None:
                continue
            y, m, _ = date.split("-")
            try:
                vol = float((supply or "").replace(",", "")) if supply.strip() not in {"", "-"} else None
            except ValueError:
                vol = None
            out.append(dict(
                county=county.strip(), admin_2=None, market=re.sub(r"\s+", " ", market).strip(),
                product=commodity.strip(), price_type=ptype,
                year=int(y), month=int(m), period_date=date,
                value_kes=val, unit=unit, currency="KES", iso3="KEN",
                source="KAMIS (MoALD) kamis.kilimo.go.ke",
                classification=(classification or "").strip() or None,
                grade=(grade or "").strip() or None,
                sex=(sex or "").strip() or None,
                supply_volume=vol, qc_flag=""))
    groups: dict = {}
    for r in out:
        if r["value_kes"] is not None:
            groups.setdefault((r["market"], r["product"], r["period_date"]), []).append(r["value_kes"])
    for r in out:
        r["qc_flag"] = qc_flags(r, groups)
    return out


def write_table(recs: list[dict], path: str) -> None:
    cols = ["county", "admin_2", "market", "product", "price_type", "year", "month",
            "period_date", "value_kes", "unit", "currency", "iso3", "source",
            "classification", "grade", "sex", "supply_volume", "qc_flag"]
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        tbl = pa.table({c: pa.array([r.get(c) for r in recs]) for c in cols})
        pq.write_table(tbl, path, compression="zstd")
        log(f"wrote {path} ({len(recs):,} rows)")
    except ImportError:
        import csv
        alt = os.path.splitext(path)[0] + ".csv"
        log(f"!! pyarrow missing - writing CSV instead: {alt}")
        with open(alt, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols)
            w.writeheader()
            w.writerows({c: r.get(c) for c in cols} for r in recs)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=".",
                    help="output DIRECTORY. Pass an absolute common_data path on cglabs - "
                         "the repo-relative default is a known trap.")
    ap.add_argument("--products", default=",".join(str(p) for p in DEFAULT_PRODUCTS),
                    help="comma-separated KAMIS product ids (default: the 4 live-animal ids)")
    ap.add_argument("--start", default="2021-01-01")
    ap.add_argument("--end", default=dt.date.today().isoformat())
    ap.add_argument("--per-page", type=int, default=500)
    ap.add_argument("--window-months", type=int, default=3)
    ap.add_argument("--smoke", action="store_true",
                    help="Camel, one quarter, parse + gates, no write")
    ap.add_argument("--list", action="store_true", help="print the live product/county pickers and exit")
    ap.add_argument("--overwrite", action="store_true")
    a = ap.parse_args()

    if a.list:
        codes = pull_codes()
        log(f"products: {len(codes['product'])}")
        for v, t in codes["product"]:
            if re.search(r"camel|cattle|goat|sheep|milk|hide|skin", t, re.I):
                log(f"    {v:>4}  {t}")
        log(f"counties: {len(codes['county'])} (param takes the NAME, not an id)")
        drift = {int(v): t for v, t in codes["product"] if v.isdigit() and int(v) in LIVESTOCK
                 and t.strip() != LIVESTOCK[int(v)]}
        if drift:
            log(f"!! product id drift vs the pinned map: {drift}")
        else:
            log("pinned livestock ids still match the live picker")
        return 0

    if a.smoke:
        a.products, a.start, a.end = "186", "2024-01-01", "2024-03-31"
        log("SMOKE: Camel, 2024Q1, no write")

    pids = [int(p) for p in a.products.split(",") if p.strip()]
    out_path = os.path.join(a.out, "market_prices_kamis.parquet")
    if not a.smoke and os.path.exists(out_path) and not a.overwrite:
        log(f"exists, skipping (use --overwrite): {out_path}")
        return 0

    all_rows: list[list[str]] = []
    for pid in pids:
        name = LIVESTOCK.get(pid, f"product {pid}")
        got = 0
        for w0, w1 in month_windows(a.start, a.end, a.window_months):
            rows = fetch_window(pid, name, w0, w1, a.per_page)
            got += len(rows)
            log(f"  {name:8s} {w0}..{w1}  {len(rows):5d} rows  (running {got:,})")
            all_rows.extend(rows)
        log(f"{name}: {got:,} table rows")

    recs = build_records(all_rows)
    log(f"parsed {len(recs):,} price records from {len(all_rows):,} table rows "
        f"(a row yields up to 2: wholesale + retail)")

    # ---- gates (invariants, not exact counts) ----
    ok = True

    def gate(n: int, cond: bool, msg: str) -> None:
        nonlocal ok
        print(f"[{'OK' if cond else 'FAIL'}] {n}. {msg}")
        ok = ok and cond

    gate(1, len(recs) > 0, f"non-empty: {len(recs):,} records")
    if recs:
        ds = sorted(r["period_date"] for r in recs)
        gate(2, ds[0] >= a.start and ds[-1] <= a.end,
             f"every date inside the requested window ({ds[0]} .. {ds[-1]})")
        gate(3, all(r["value_kes"] is not None for r in recs),
             "every emitted record carries a parsed price")
        live = [r for r in recs if r["product"] in LIVESTOCK.values()]
        gate(4, (not live) or all((r["unit"] or "").lower() == "head" for r in live),
             f"live animals priced per head ({len(live):,} records)")
        bad = [r for r in recs if r["qc_flag"]]
        print(f"[INFO] 5. qc-flagged {len(bad):,}/{len(recs):,} "
              f"({100 * len(bad) / len(recs):.1f}%) - kept, not dropped; "
              f"flags: {sorted({f for r in bad for f in r['qc_flag'].split(',') if f})}")
        gate(6, len(bad) / len(recs) < 0.25,
             f"qc-flagged share under 25% ({100 * len(bad) / len(recs):.1f}%)")

    if not ok:
        log("GATES FAILED - not writing. Stop here and report, do not improvise a fix.")
        return 1
    if a.smoke:
        log("SMOKE OK (no write)")
        return 0

    os.makedirs(a.out, exist_ok=True)
    write_table(recs, out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
