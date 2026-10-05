#!/usr/bin/env python3
"""
ingest_livestock_kaznet_kaop.py — KAZNET pastoral livestock transactions -> parquet,
via the open KAOP proxy.

READ THIS BEFORE WIRING IT INTO ANYTHING
  This endpoint is a STALE PARTIAL SLICE of Kaznet, not the dataset. Verified 2026-10-05:
  3,352 Kenya records, 2021-03-27 .. 2023-05-27, **Marsabit county only** (markets Merille
  2,555 / Korr 763 / Ol turot 34), and 2024, 2025 and 2026 all return zero records. It is
  Phase-I data plus a thin tail; the feed was never re-pointed after the Phase-I -> Phase-II
  transition in 2022.

  Use it as a PROVENANCE / SHAPE DEMO - to show what Kaznet records look like and to
  prototype the schema - not as an operational layer.

  The canonical dataset is ILRI's "KAZNET Sentinel Zones Longitudinal High Frequency
  Crowdsourced Data", hdl:20.500.11766.1/FK2/4ZMH2Y on MELSpace (data.mel.cgiar.org), v3.0
  released 2026-04-16. It is labelled CC-BY-4.0 but **every file is restricted**: a direct
  `GET /api/access/datafile/<id>` returns HTTP 403 "Not authorized to access this object via
  this API endpoint" (verified). Getting it means a Dataverse access request or a direct ask
  to the ILRI authors (Shikuku, Kelvin; Lepariyo, Watson). Files behind that wall:
  4_livestock_prices_and_quality.csv (31.4 MB), 8_Transect_forage_conditions.csv (21.7 MB),
  10_livestock_volumes.csv, 6_prices_of_commodities.csv, plus the codebook.

  For an operational livestock-price layer today, prefer in this order:
    1. FEWS NET FDW - already ingested in the explorer's `market_prices.parquet`
       (NDMA-sourced goat 4,864 rows / 20 counties and cattle 919 rows / 3 counties,
       2000-2026, KES per head, monthly). Nothing to do.
    2. KAMIS - `python/ingest_market_prices_kamis.py`. Adds camel and sheep, Grade, Sex,
       breed and supply volume at market-day frequency, 2021 -> today.
  Kaznet's unique contribution over both is individual-animal, photo-verified body condition
  plus the forage-transect and household modules - and all of that sits behind the 403.

SOURCE (verified 2026-10-05, open, unauthenticated)
  GET https://kaop.co.ke/kaznet/api/livestock_prices_and_quality
        ?country=kenya|ethiopia & start=YYYY-MM-DD [& end=YYYY-MM-DD]
  `start` is mandatory. Returns {"status": 1, "records": [...]}.

SHAPE - and why this script melts it
  One record is WIDE: 83 keys covering all four species at once, almost all "N/A". Keys are
  duplicated as opaque `field_NNN` AND as the full question text, e.g.
      "What is the final SELLING price of the Goat (in local currency)?": "1900"
      "What is the body condition of the goat?": "Moderate (Grade 2)"
      "Which animal type is available in the market for trade today?": "Goat&#44;Sheep"
  This script reads the question-text keys (the `field_NNN` numbering is a form-version
  artefact and will drift) and melts to ONE ROW PER (record, species) where a price exists.

  Other observed facts: HTML entities are not decoded upstream (`&#44;` = comma); `lat`/`lng`
  are null in every record, so the output is plain parquet, NOT GeoParquet - the only
  geography is a market name; photo URLs point at `api.ona.io` over plain HTTP.

OUTPUT (plain parquet)
  country county market cluster_name uai_name sub_location_name species
  price_local currency body_condition sex photo_url record_id data_id datetime
  date year month source qc_flag

Requires: python3 stdlib + pyarrow (falls back to CSV with a warning). No auth.

RUN
  python3 python/ingest_livestock_kaznet_kaop.py --smoke
  python3 python/ingest_livestock_kaznet_kaop.py --start 2021-01-01 --end 2026-10-05 \
      --out /abs/path/to/common_data/...
Idempotent: an existing output is skipped unless --overwrite.
"""
from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request

API = "https://kaop.co.ke/kaznet/api/livestock_prices_and_quality"
UA = "Mozilla/5.0 (compatible; AAAA-hazards-prototype/1.0; +https://github.com/AdaptationAtlas)"

# question-text key fragments -> canonical species. Matched case-insensitively on a
# substring, because KAMIS-style capitalisation is inconsistent upstream
# ("the Goat", "the camel", "the Cattle", "the Sheep").
SPECIES = {
    "camel": dict(price="final SELLING price of the camel",
                  body="body condition of the camel",
                  sex="sex of camel",
                  photo="body condition of a camel"),
    "cattle": dict(price="final SELLING price of the Cattle",
                   body="body condition of the cattle",
                   sex="sex of cattle",
                   photo="body condition of a cow"),
    "goat": dict(price="final SELLING price of the Goat",
                 body="body condition of the goat",
                 sex="sex of goat",
                 photo="body condition of a goat"),
    "sheep": dict(price="final SELLING price of the Sheep",
                  body="body condition of the sheep",
                  sex="sex of sheep",
                  photo="body condition of a sheep"),
}
MARKET_KEY = "livestock market are you collecting data from"
NA = {"n/a", "na", "none", "", "-", "null"}

_t0 = time.time()


def log(msg: str) -> None:
    print(f"[{dt.datetime.now():%H:%M:%S} +{time.time() - _t0:6.1f}s] {msg}", flush=True)


def clean(v) -> str | None:
    if v is None:
        return None
    s = html.unescape(str(v)).strip()
    return None if s.lower() in NA else s


def pick(rec: dict, fragment: str) -> str | None:
    """Find a value by question-text substring, case-insensitively."""
    f = fragment.lower()
    for k, v in rec.items():
        if f in k.lower():
            return clean(v)
    return None


def fetch(country: str, start: str, end: str | None, tries: int = 4) -> list[dict]:
    q = {"country": country, "start": start}
    if end:
        q["end"] = end
    url = f"{API}?{urllib.parse.urlencode(q)}"
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=180) as r:
                payload = json.loads(r.read().decode("utf-8", errors="replace"))
            if str(payload.get("status")) != "1":
                log(f"!! API status {payload.get('status')}: {payload.get('message')}")
            return payload.get("records", []) or []
        except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as e:
            last = e
            wait = 2 ** i
            log(f"  retry {i + 1}/{tries} in {wait}s after {type(e).__name__}: {e}")
            time.sleep(wait)
    raise RuntimeError(f"GET failed after {tries} tries: {url} ({last})")


def melt(recs: list[dict], country: str) -> list[dict]:
    out = []
    for r in recs:
        when = clean(r.get("datetime")) or ""
        date = when[:10] if re.match(r"\d{4}-\d{2}-\d{2}", when) else None
        base = dict(
            country=clean(r.get("country_name")) or country.title(),
            county="Marsabit" if clean(r.get("cluster_name")) in {"Merille", "Korr", "Ol turot"} else None,
            market=pick(r, MARKET_KEY) or clean(r.get("cluster_name")),
            cluster_name=clean(r.get("cluster_name")),
            uai_name=clean(r.get("uai_name")),
            sub_location_name=clean(r.get("sub_location_name")),
            record_id=clean(r.get("id")), data_id=clean(r.get("data_id")),
            datetime=when or None, date=date,
            year=int(date[:4]) if date else None,
            month=int(date[5:7]) if date else None,
            currency="KES" if country.lower() == "kenya" else "ETB",
            source="KAZNET (ILRI) via KAOP proxy kaop.co.ke/kaznet/api",
        )
        for sp, keys in SPECIES.items():
            price = pick(r, keys["price"])
            if price is None:
                continue
            try:
                val = float(str(price).replace(",", ""))
            except ValueError:
                val = None
            flags = []
            if val is None:
                flags.append("unparseable_price")
            elif val <= 0:
                flags.append("nonpositive")
            body = pick(r, keys["body"])
            out.append(dict(base, species=sp, price_local=val,
                            body_condition=body, sex=pick(r, keys["sex"]),
                            photo_url=pick(r, keys["photo"]),
                            qc_flag=",".join(flags)))
    return out


def write_table(recs: list[dict], path: str) -> None:
    cols = ["country", "county", "market", "cluster_name", "uai_name", "sub_location_name",
            "species", "price_local", "currency", "body_condition", "sex", "photo_url",
            "record_id", "data_id", "datetime", "date", "year", "month", "source", "qc_flag"]
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        pq.write_table(pa.table({c: pa.array([r.get(c) for r in recs]) for c in cols}),
                       path, compression="zstd")
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
    ap.add_argument("--out", default=".", help="output DIRECTORY (pass an absolute path on cglabs)")
    ap.add_argument("--country", default="kenya", choices=["kenya", "ethiopia"])
    ap.add_argument("--start", default="2021-01-01")
    ap.add_argument("--end", default=dt.date.today().isoformat())
    ap.add_argument("--smoke", action="store_true", help="fetch + gates, no write")
    ap.add_argument("--overwrite", action="store_true")
    a = ap.parse_args()

    out_path = os.path.join(a.out, f"livestock_kaznet_{a.country}.parquet")
    if not a.smoke and os.path.exists(out_path) and not a.overwrite:
        log(f"exists, skipping (use --overwrite): {out_path}")
        return 0

    log(f"GET {a.country} {a.start} .. {a.end}")
    raw = fetch(a.country, a.start, a.end)
    log(f"{len(raw):,} wide records")
    recs = melt(raw, a.country)
    log(f"{len(recs):,} (record x species) price rows after melt")

    ok = True

    def gate(n, cond, msg):
        nonlocal ok
        print(f"[{'OK' if cond else 'FAIL'}] {n}. {msg}")
        ok = ok and cond

    gate(1, len(raw) > 0, f"endpoint returned records: {len(raw):,}")
    gate(2, len(recs) > 0, f"melt produced priced rows: {len(recs):,}")
    if recs:
        import collections
        ds = sorted(r["date"] for r in recs if r["date"])
        sp = collections.Counter(r["species"] for r in recs)
        mk = collections.Counter(r["market"] for r in recs)
        gate(3, bool(ds) and ds[0] >= a.start and ds[-1] <= a.end,
             f"dates inside the requested window ({ds[0]} .. {ds[-1]})")
        gate(4, len(sp) >= 2, f"more than one species present: {dict(sp)}")
        bad = sum(1 for r in recs if r["qc_flag"])
        gate(5, bad / len(recs) < 0.10,
             f"unparseable/nonpositive prices under 10% ({bad}/{len(recs)})")
        print(f"[INFO] 6. markets: {dict(mk)}")
        print(f"[INFO] 7. body-condition coverage: "
              f"{sum(1 for r in recs if r['body_condition'])}/{len(recs)}")
        # The staleness check is INFORMATIONAL on purpose: the feed being frozen is the
        # documented state of this endpoint, not a failure of this script.
        latest = ds[-1] if ds else "none"
        print(f"[INFO] 8. latest record {latest} - if this is still 2023, the KAOP proxy has "
              f"NOT been re-pointed at Kaznet Phase II; do not treat as operational.")

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
