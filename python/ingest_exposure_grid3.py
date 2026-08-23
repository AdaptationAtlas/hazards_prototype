#!/usr/bin/env python3
"""
ingest_exposure_grid3.py — GRID3 / WOPR KEN v2.0 bottom-up population -> COG (KE-39, layer 3).

The GRID3-branded gridded population for Kenya = WorldPop Open Population Repository (WOPR) KEN v2.0
(bottom-up, KNBS microcensus-modelled). A 2nd per-pixel pop surface alongside the tier-9 WorldPop
*constrained* (top-down dasymetric) — same 100 m WGS84 grain, different METHOD (Pete: build both).

Source (cglabs KE-39 #4 pin, non-GEE, no auth; WorldPop server has NO range support -> must download):
  https://data.worldpop.org/repo/wopr/KEN/population/v2.0/KEN_population_v2_0_gridded.zip (30 MB, zipped GeoTIFF)
  GeoTIFF, ~100 m (0.000833 deg), EPSG:4326, Float32, NoData -3.4e38, covers Kenya. Licence CC-BY-4.0
  (WOPR standard — cglabs to confirm the exact README wording before publish, per per-asset rule).

Requires: gdal (osgeo) + rasterio + numpy. No auth.

RUN (cglabs): python3 python/ingest_exposure_grid3.py --smoke   # download + COG-ify + gdalinfo gate
              python3 python/ingest_exposure_grid3.py
Output: <out>/population_2020.tif   (COG, overviews, NoData=NaN, bottom-up population count)
Publish: R/observational/6_publish_obs_to_s3.R --full --tier 11  (type=population/source=grid3)
"""
import argparse
import datetime as dt
import io
import os
import urllib.request
import zipfile

import numpy as np
import rasterio
from osgeo import gdal

gdal.UseExceptions()

URL = "https://data.worldpop.org/repo/wopr/KEN/population/v2.0/KEN_population_v2_0_gridded.zip"
BBOX = (33.9, -4.7, 41.9, 5.5)          # Kenya (W,S,E,N) — matches other KE tiers
COG_OPTS = dict(driver="COG", compress="DEFLATE", predictor=2, blocksize=512,
                overview_resampling="average")


def log(msg):
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def build(out_dir, overwrite, smoke):
    out = os.path.join(out_dir, "population_2020.tif")
    if not overwrite and os.path.exists(out) and os.path.getsize(out) > 100:
        log("population_2020: exists, skip"); return "skip"
    os.makedirs(out_dir, exist_ok=True)
    zpath = os.path.join(out_dir, ".KEN_population_v2_0_gridded.zip")
    log("downloading GRID3/WOPR KEN v2.0 gridded population ...")
    urllib.request.urlretrieve(URL, zpath)
    log(f"  got {os.path.getsize(zpath)/1e6:.1f} MB")
    with zipfile.ZipFile(zpath) as zf:
        tifs = [n for n in zf.namelist() if n.lower().endswith(".tif")]
        if not tifs:
            raise RuntimeError(f"no .tif in {URL} (have: {zf.namelist()})")
        inner = tifs[0]
        zf.extract(inner, out_dir)
    tif = os.path.join(out_dir, inner)
    log(f"  extracted {os.path.basename(tif)}")
    # crop Kenya bbox via gdalwarp
    warped = gdal.Warp("", tif, format="MEM", outputBounds=BBOX, outputBoundsSRS="EPSG:4326",
                       dstSRS="EPSG:4326", resampleAlg="near")
    arr = warped.GetRasterBand(1).ReadAsArray().astype("float32")
    src_nodata = warped.GetRasterBand(1).GetNoDataValue()
    warped = None
    if src_nodata is not None:
        arr[arr == src_nodata] = np.nan
    arr[arr < 0] = np.nan                       # WOPR fill (-3.4e38); 0 = valid (no people)
    h, w = arr.shape
    transform = rasterio.transform.from_bounds(BBOX[0], BBOX[1], BBOX[2], BBOX[3], w, h)
    prof = dict(height=h, width=w, count=1, dtype="float32", crs="EPSG:4326",
                transform=transform, nodata=float("nan"), **COG_OPTS)
    with rasterio.open(out, "w", **prof) as dst:
        dst.write(arr, 1)
    if not smoke:
        os.remove(zpath); os.remove(tif)
    finite = np.isfinite(arr)
    log(f"population_2020 (GRID3/WOPR): {w}x{h} -> {out} (pop/px max {np.nanmax(arr[finite]):.0f} / "
        f"national total {np.nansum(arr):,.0f})")
    return "written"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="Data/exposure/grid3")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--smoke", action="store_true", help="download + COG-ify + keep tmp for gdalinfo")
    a = ap.parse_args()
    log(f"GRID3/WOPR exposure ingest | out={a.out}")
    r = build(a.out, a.overwrite, a.smoke)
    log(f"DONE: {r}")
    print("\nNext: publish with  Rscript R/observational/6_publish_obs_to_s3.R --full --tier 11")


if __name__ == "__main__":
    main()
