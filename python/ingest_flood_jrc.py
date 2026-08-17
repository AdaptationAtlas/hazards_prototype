#!/usr/bin/env python3
"""
ingest_flood_jrc.py — JRC GloFAS river flood hazard -> return-period COGs (non-GEE).

Static riverine flood DEPTH (m) by return period, from the public source.coop mirror of the
JRC CEMS GloFAS Flood Hazard maps (v2.1, CC-BY-4.0). Per return period: mosaic the 4 Kenya
10-degree tiles (via /vsicurl, no download of the global set) -> crop Kenya bbox -> clamp edge
artifacts (<0 -> NaN) -> COG w/ overviews.

Verified specs (cglabs flood dispatch #1, 2026-08-17):
  source.coop nlebovits/jrc-glofas; 90 m (0.000833 deg); EPSG:4326 Float32; units = flood depth (m);
  RP = 10/20/50/75/100/200/500; Kenya = 4 tiles ID150_N10_E30, ID151_N0_E30, ID161_N10_E40, ID162_N0_E40;
  crop verified 0-42.2 m (min -1.02 = nodata/resample edge -> clamp).
Requires: gdal (osgeo) + rasterio. No auth.

RUN (cglabs):
  python3 python/ingest_flood_jrc.py --smoke               # RP100 only -> gate
  python3 python/ingest_flood_jrc.py                       # all 7 RP
Idempotent: skips an existing output unless --overwrite.
Output: <out>/flood-depth_rp{RP}.tif
"""
import argparse
import datetime as dt
import os

import numpy as np
import rasterio
from osgeo import gdal

gdal.UseExceptions()

RPS = [10, 20, 50, 75, 100, 200, 500]
TILES = ["ID150_N10_E30", "ID151_N0_E30", "ID161_N10_E40", "ID162_N0_E40"]
BASE = "https://data.source.coop/nlebovits/jrc-glofas"
BBOX = (33.9, -4.7, 41.9, 5.5)   # Kenya (W,S,E,N) — matches NDVI region=east-africa footprint
COG_OPTS = dict(driver="COG", compress="DEFLATE", predictor=2, blocksize=512,
                overview_resampling="average")


def log(msg):
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def tile_url(rp, tile):
    return f"/vsicurl/{BASE}/depth-rp{rp}/{tile}/{tile}_RP{rp}_depth.tif"


def build_rp(rp, out_dir, overwrite):
    os.makedirs(out_dir, exist_ok=True)  # CGLABS 2026-08-17 fix: must exist BEFORE the tmp warp write below (was at L65, after gdal.Warp → "No such file or directory")
    out = os.path.join(out_dir, f"flood-depth_rp{rp}.tif")
    if not overwrite and os.path.exists(out) and os.path.getsize(out) > 100:
        log(f"  rp{rp}: exists, skip")
        return "skip"
    srcs = [tile_url(rp, t) for t in TILES]
    tmp = os.path.join(out_dir, f".tmp_rp{rp}.tif")
    # mosaic 4 tiles + crop Kenya bbox in one gdalwarp (nearest = preserve depth values)
    gdal.Warp(tmp, srcs, dstSRS="EPSG:4326",
              outputBounds=BBOX, outputBoundsSRS="EPSG:4326",
              resampleAlg="near", dstNodata=float("nan"))
    with rasterio.open(tmp) as ds:
        arr = ds.read(1).astype("float32")
        transform = ds.transform
    arr[arr < 0] = np.nan   # clamp nodata / resample-edge artifacts (depth < 0 is invalid)
    prof = dict(height=arr.shape[0], width=arr.shape[1], count=1, dtype="float32",
                crs="EPSG:4326", transform=transform, nodata=float("nan"), **COG_OPTS)
    with rasterio.open(out, "w", **prof) as dst:
        dst.write(arr, 1)
    os.remove(tmp)
    finite = np.isfinite(arr)
    log(f"  rp{rp}: {arr.shape[1]}x{arr.shape[0]} -> {out} "
        f"(depth min {np.nanmin(arr[finite]):.2f} / mean {np.nanmean(arr[finite]):.2f} "
        f"/ max {np.nanmax(arr[finite]):.2f} m)")
    return "written"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="Data/flood_jrc/JRC")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--smoke", action="store_true", help="RP100 only")
    a = ap.parse_args()
    rps = [100] if a.smoke else RPS
    log(f"JRC flood ingest | RP={rps} bbox={BBOX} out={a.out}")
    tally = {"written": 0, "skip": 0}
    for rp in rps:
        tally[build_rp(rp, a.out, a.overwrite)] += 1
    log(f"DONE: {tally}")
    print("\nNext: publish with  Rscript R/observational/6_publish_obs_to_s3.R --full --tier 6")


if __name__ == "__main__":
    main()
