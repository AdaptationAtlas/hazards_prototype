#!/usr/bin/env python3
"""
ingest_exposure_worldpop.py — WorldPop constrained 2020 100 m population -> COG (KE-39 exposure).

Downloads the WorldPop constrained 2020 100 m population count for Kenya (CC-BY-4.0), fixes NoData,
crops to the Kenya bbox (already Kenya-national), writes a COG w/ overviews. First KE-39 exposure
layer — the people surface that flood/drought hazards intersect.

Source (cglabs KE-39 probe, non-GEE, no auth, HTTP 200 ~34 MB):
  https://data.worldpop.org/GIS/Population/Global_2000_2020_Constrained/2020/maxar_v1/KEN/ken_ppp_2020_constrained.tif
  plain GeoTIFF, ~100 m (0.000833 deg), EPSG:4326, population COUNT per pixel, NoData ~ -99999.

Requires: gdal (osgeo) + rasterio + numpy. No auth.

RUN (cglabs): python3 python/ingest_exposure_worldpop.py --smoke   # download + COG-ify + gdalinfo gate
              python3 python/ingest_exposure_worldpop.py
Output: <out>/population_2020.tif   (COG, overviews, NoData=NaN, population count)
Publish: R/observational/6_publish_obs_to_s3.R --full --tier 9  (type=population)
"""
import argparse
import datetime as dt
import os
import urllib.request

import numpy as np
import rasterio
from osgeo import gdal

gdal.UseExceptions()

URL = ("https://data.worldpop.org/GIS/Population/Global_2000_2020_Constrained/2020/"
       "maxar_v1/KEN/ken_ppp_2020_constrained.tif")
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
    dl = os.path.join(out_dir, ".ken_ppp_2020_constrained.tif")
    log(f"downloading WorldPop 100m constrained 2020 (Kenya) ...")
    urllib.request.urlretrieve(URL, dl)
    log(f"  got {os.path.getsize(dl)/1e6:.1f} MB")
    # crop to Kenya bbox (already Kenya-national; keeps grid exact) via gdalwarp
    warped = gdal.Warp("", dl, format="MEM", outputBounds=BBOX, outputBoundsSRS="EPSG:4326",
                       dstSRS="EPSG:4326", resampleAlg="near")
    arr = warped.GetRasterBand(1).ReadAsArray().astype("float32")
    src_nodata = warped.GetRasterBand(1).GetNoDataValue()
    gt = warped.GetGeoTransform()
    warped = None
    if src_nodata is not None:
        arr[arr == src_nodata] = np.nan
    arr[arr < 0] = np.nan                       # WorldPop fill (~-99999); 0 = valid (no people)
    h, w = arr.shape
    transform = rasterio.transform.from_bounds(BBOX[0], BBOX[1], BBOX[2], BBOX[3], w, h)
    prof = dict(height=h, width=w, count=1, dtype="float32", crs="EPSG:4326",
                transform=transform, nodata=float("nan"), **COG_OPTS)
    with rasterio.open(out, "w", **prof) as dst:
        dst.write(arr, 1)
    if not smoke:
        os.remove(dl)
    finite = np.isfinite(arr)
    log(f"population_2020: {w}x{h} -> {out} (pop/px min {np.nanmin(arr[finite]):.1f} / "
        f"max {np.nanmax(arr[finite]):.0f} / total {np.nansum(arr):,.0f})")
    return "written"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="Data/exposure/worldpop")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--smoke", action="store_true", help="download + COG-ify + keep tmp for gdalinfo")
    a = ap.parse_args()
    log(f"WorldPop exposure ingest | out={a.out}")
    r = build(a.out, a.overwrite, a.smoke)
    log(f"DONE: {r}")
    print("\nNext: publish with  Rscript R/observational/6_publish_obs_to_s3.R --full --tier 9")


if __name__ == "__main__":
    main()
