#!/usr/bin/env python3
"""
ingest_flood_gfd.py — Global Flood Database v1.4 -> per-year flood-occurrence COGs (non-GEE).

Observed satellite (MODIS) flood inundation per event (Tellman et al 2021), from the public GCS
bucket gfd_v1_4 (no auth). ~913 per-event rasters, 2000-2018. This builds a per-YEAR flood layer
over Kenya so the notebook can composite flooding by ENSO/IOD phase:
  - list events; assign each to its START year,
  - cheaply filter to events intersecting Kenya via /vsizip//vsicurl (byte-range header read,
    NO full download of non-Kenya events),
  - for Kenya events: warp the `flooded` band (band 1, 1/0) to a fixed Kenya grid,
  - per year: UNION flooded across that year's events (max) -> "flooded in year Y" (0/1),
  - COG w/ overviews per year.

Verified structure (macbook probe of DFO_1587, 2026-08-17):
  zip = DFO_{id}_From_{YYYYMMDD}_to_{YYYYMMDD}.zip -> inner .tif (same basename) + properties.json;
  EPSG:4326, ~0.002246 deg (~250 m), bands: 1=flooded 2=duration 3=clear_views 4=clear_perc
  5=jrc_perm_water; per-event REGIONAL extent (not global).
Requires: gdal (osgeo) + rasterio + numpy. No auth.

RUN (cglabs):
  python3 python/ingest_flood_gfd.py --smoke      # 1 year (2015) only -> gate
  python3 python/ingest_flood_gfd.py              # all years 2000-2018
Idempotent: skips a year COG that exists unless --overwrite.
Output: <out>/flooded_{YYYY}.tif  (0/1 union; NaN where never observed over Kenya)
"""
import argparse
import datetime as dt
import json
import os
import re
import urllib.request

import numpy as np
import rasterio
from osgeo import gdal

gdal.UseExceptions()

BUCKET = "gfd_v1_4"
LIST_URL = f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o?fields=items(name),nextPageToken&maxResults=1000"
OBJ_BASE = f"https://storage.googleapis.com/{BUCKET}"
BBOX = (33.9, -4.7, 41.9, 5.5)          # Kenya (W,S,E,N) — matches NDVI/JRC region=east-africa
TR = 0.0022457882103            # native GFD pixel size (deg)
COG_OPTS = dict(driver="COG", compress="DEFLATE", predictor=2, blocksize=512,
                overview_resampling="nearest")


def log(msg):
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def list_events():
    """List all event zip object names (paginated)."""
    names, token = [], None
    while True:
        url = LIST_URL + (f"&pageToken={token}" if token else "")
        with urllib.request.urlopen(url, timeout=60) as r:
            j = json.load(r)
        names += [it["name"] for it in j.get("items", []) if it["name"].endswith(".zip")]
        token = j.get("nextPageToken")
        if not token:
            break
    return sorted(names)


def event_year(zipname):
    m = re.search(r"_From_(\d{4})\d{4}_to_", zipname)
    return int(m.group(1)) if m else None


def vsi_tif(zipname):
    inner = zipname[:-4] + ".tif"     # inner tif shares the basename
    return f"/vsizip//vsicurl/{OBJ_BASE}/{zipname}/{inner}"


def intersects_kenya(vsi):
    """Cheap header-only bounds check (byte-range via vsizip/vsicurl)."""
    try:
        ds = gdal.Open(vsi)
    except Exception:
        return False
    if ds is None:
        return False
    gt = ds.GetGeoTransform()
    w = gt[0]
    n = gt[3]
    e = w + gt[1] * ds.RasterXSize
    s = n + gt[5] * ds.RasterYSize
    ds = None
    return not (e < BBOX[0] or w > BBOX[2] or n < BBOX[1] or s > BBOX[3])


def flooded_on_kenya_grid(vsi):
    """Warp the flooded band (1) to the fixed Kenya grid; return 0/1 array (NaN=nodata)."""
    warped = gdal.Warp("", vsi, format="MEM", srcBands=[1],
                       outputBounds=BBOX, outputBoundsSRS="EPSG:4326",
                       xRes=TR, yRes=TR, dstSRS="EPSG:4326",
                       resampleAlg="near", dstNodata=float("nan"))
    arr = warped.GetRasterBand(1).ReadAsArray().astype("float32")
    warped = None
    return arr


def write_cog(arr, out_path):
    h, w = arr.shape
    transform = rasterio.transform.from_bounds(BBOX[0], BBOX[1], BBOX[2], BBOX[3], w, h)
    prof = dict(height=h, width=w, count=1, dtype="float32", crs="EPSG:4326",
                transform=transform, nodata=float("nan"), **COG_OPTS)
    with rasterio.open(out_path, "w", **prof) as dst:
        dst.write(arr, 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="Data/flood_gfd/GFD")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--smoke", action="store_true", help="year 2015 only")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    events = list_events()
    log(f"GFD: {len(events)} events in bucket")
    by_year = {}
    for z in events:
        y = event_year(z)
        if y:
            by_year.setdefault(y, []).append(z)
    years = [2015] if a.smoke else sorted(by_year)

    tally = {"written": 0, "skip": 0, "empty": 0}
    for y in years:
        out = os.path.join(a.out, f"flooded_{y}.tif")
        if not a.overwrite and os.path.exists(out) and os.path.getsize(out) > 100:
            log(f"  {y}: exists, skip"); tally["skip"] += 1; continue
        acc, n_ken = None, 0
        for z in by_year.get(y, []):
            vsi = vsi_tif(z)
            if not intersects_kenya(vsi):
                continue
            n_ken += 1
            fl = flooded_on_kenya_grid(vsi)
            fl = np.where(np.isfinite(fl) & (fl >= 1), 1.0, np.where(np.isfinite(fl), 0.0, np.nan))
            acc = fl if acc is None else np.fmax(acc, fl)   # union of flooded (NaN-aware)
            log(f"    {y}: +{z} (Kenya event {n_ken})")
        if acc is None:
            log(f"  {y}: 0 Kenya events, skip"); tally["empty"] += 1; continue
        write_cog(acc, out)
        finite = np.isfinite(acc)
        log(f"  {y}: {n_ken} Kenya events -> {out} "
            f"(flooded px {int(np.nansum(acc==1))}, coverage {finite.mean():.2f})")
        tally["written"] += 1
    log(f"DONE: {tally}")
    print("\nNext: publish with  Rscript R/observational/6_publish_obs_to_s3.R --full --tier 7")


if __name__ == "__main__":
    main()
