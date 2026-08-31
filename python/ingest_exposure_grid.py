#!/usr/bin/env python3
"""
ingest_exposure_grid.py — KPLC electricity transmission network -> GeoJSON (KE-39, layer 7 / last).

Kenya's official electricity transmission grid for the flood/drought exposure overlay, from
energydata.info (KPLC / Kenya Power). FIVE per-voltage line layers (11/33/66/132/220 kV; Pete: all
five — 132/220 kV = the national backbone, highest-value at-risk assets) merged into one GeoJSON with
a `voltage_kv` attribute. CC0-1.0 (cleanest of the grid sources).

Resolves the resources via the energydata.info CKAN API (no fragile hardcoded UUIDs — same pattern as
the HOTOSM/COD-AB ingests), matches the "{V}kV Network" transmission resources by name, forces
EPSG:4326, clips the Kenya bbox. The 'Unidentified Grid Network' layer (no voltage) is excluded.
gridfinder (modelled MV, CC-BY) is a deferred complement — not ingested here. Note: energydata.info's
WAF 403s the default Python-urllib UA, so a browser UA header is set on all requests (cglabs fix).

Source (cglabs KE-39 #7 pin, non-GEE, no auth, CKAN 200 with UA header):
  CKAN dataset  kenya-kenya-electricity-network  (energydata.info, KPLC) — CC0-1.0
  resources     "{11,33,66,132,220}kV Network"  (per-voltage GeoJSON)

Requires: geopandas + fiona/pyogrio. No auth.

RUN (cglabs): python3 python/ingest_exposure_grid.py --list     # resolve CKAN, list matched resources (verify)
              python3 python/ingest_exposure_grid.py            # write kenya_power_grid.geojson
Output: <out>/kenya_power_grid.geojson   (LineString, EPSG:4326, CC0 — attribute voltage_kv in {11,33,66,132,220})
Publish: R/observational/6_publish_obs_to_s3.R --full --tier 15  (type=infrastructure/source=energydata-kplc)
"""
import argparse
import datetime as dt
import json
import os
import re
import tempfile
import urllib.request

CKAN = "https://energydata.info/api/3/action/package_show?id={id}"
DATASET = "kenya-kenya-electricity-network"
UA = {"User-Agent": "Mozilla/5.0 (atlas-exposure-ingest)"}  # energydata WAF 403s default Python-urllib UA
BBOX = (33.9, -4.7, 41.9, 5.5)                       # Kenya (W,S,E,N)
# match "…transmission…33kv…" (name "33kV Network" + its transmission-line URL) -> capture voltage {11,33,66,132,220}
RES_RE = re.compile(r"transmission.*?(\d{2,3})\s*kv", re.IGNORECASE)


def log(msg):
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def resolve_resources():
    """Return [(voltage_int, download_url, name)] for the transmission-line resources on the CKAN dataset."""
    req = urllib.request.Request(CKAN.format(id=DATASET), headers=UA)
    with urllib.request.urlopen(req, timeout=60) as r:
        pkg = json.load(r)["result"]
    out = []
    for x in pkg.get("resources", []):
        nm = (x.get("name") or "") + " " + (x.get("url") or "")
        m = RES_RE.search(nm)
        url = x.get("download_url") or x.get("url")
        fmt = (x.get("format") or "").lower()
        if m and url and ("json" in fmt or "geojson" in fmt or url.lower().endswith((".json", ".geojson"))):
            out.append((int(m.group(1)), url, x.get("name")))
    # dedupe by voltage (keep first), sort
    seen, uniq = set(), []
    for v, u, n in sorted(out):
        if v not in seen:
            seen.add(v); uniq.append((v, u, n))
    if not uniq:
        raise RuntimeError(f"no transmission-line resources matched on CKAN {DATASET} "
                           f"(have: {[x.get('name') for x in pkg.get('resources', [])]})")
    return uniq


def build(out_dir, overwrite):
    import geopandas as gpd
    import pandas as pd
    out = os.path.join(out_dir, "kenya_power_grid.geojson")
    if not overwrite and os.path.exists(out) and os.path.getsize(out) > 100:
        log("kenya_power_grid: exists, skip"); return "skip"
    res = resolve_resources()
    log("matched: " + ", ".join(f"{v}kV" for v, _, _ in res))
    parts = []
    with tempfile.TemporaryDirectory() as tmp:
        for volt, url, name in res:
            local = os.path.join(tmp, f"{volt}kv.geojson")
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=120) as resp, open(local, "wb") as fh:
                fh.write(resp.read())
            g = gpd.read_file(local).to_crs(4326)
            g["voltage_kv"] = volt
            parts.append(g[["voltage_kv", "geometry"]])
            log(f"  {volt}kV: {len(g)} features ({name})")
    gdf = gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), crs=4326)
    gdf = gdf.cx[BBOX[0]:BBOX[2], BBOX[1]:BBOX[3]]       # clip Kenya bbox
    os.makedirs(out_dir, exist_ok=True)
    gdf.to_file(out, driver="GeoJSON")
    log(f"kenya_power_grid: -> {out} ({len(gdf)} features, "
        f"voltages {sorted(gdf['voltage_kv'].unique().tolist())}, {os.path.getsize(out)/1e6:.1f} MB)")
    return "written"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="Data/exposure/grid")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--list", action="store_true", help="resolve CKAN + list matched resources, don't write")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    if a.list:
        for v, u, n in resolve_resources():
            log(f"  {v}kV  {n}  -> {u}")
        return
    log(f"KPLC grid ingest | dataset={DATASET} out={a.out}")
    log(f"DONE: {build(a.out, a.overwrite)}")
    print("\nNext: publish with  Rscript R/observational/6_publish_obs_to_s3.R --full --tier 15")


if __name__ == "__main__":
    main()
