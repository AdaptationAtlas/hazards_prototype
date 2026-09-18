#!/usr/bin/env python3
"""
ingest_wrsi_fews.py — FEWS NET/USGS CHIRPS-ETos WRSI -> per-season/year COGs (non-GEE).

Water Requirement Satisfaction Index (crop/pasture water balance), CHIRPS v3.0-driven. Per
(region-code, year): download the end-of-season dekad zip -> extract the EOS WRSI GeoTIFF ->
crop East-Africa/Kenya -> mask status codes (>100 -> NoData) -> COG w/ overviews.

Verified (cglabs WRSI dispatch #1, 2026-08-18):
  archive https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/fews/web/africa/east/dekadal/
          wrsi-chirps-etos/{regiondir}/downloads/dekadal/w{YYYY}{DD}{code}.zip   (CHIRPS-ETos, NOT legacy RFE)
  zip -> GeoTIFFs w{YYYY}{DD}{prod}.tif: do=current WRSI, eo=extended/END-OF-SEASON WRSI, dt=anomaly.
  0.1 deg / EPSG:4326 / Int16 / NoData -9999 / values 0-100 (WRSI %) + status codes 253/254 (>100).
  CHIRPS v3.0 confirmed (product page). East Africa season codes: east1/e1 short rains, east2/e2 long rains.

REGION_MAP fixed 2026-09-18 (atlas_nb-KE-enso dispatch, FEWS W_images.pdf Table 1, 2025-02-03):
  e1/e2 are RANGELAND (Sep-Jan / Feb-Jul), ee/et are MAIZE cropland (Mar-Nov / Oct-Feb).
  Previous map had cropland/rangeland swapped. ek (Ethiopian belg sorghum, ~0% of Kenya) dropped.

Requires: gdal (osgeo) + rasterio + numpy + urllib + zipfile. No auth.

RUN (cglabs): python3 python/ingest_wrsi_fews.py --smoke        # one (region,year) -> verify map/EOS
              python3 python/ingest_wrsi_fews.py                 # all mapped regions x years
Output: <out>/wrsi_{crop}_{season}_{YYYY}.tif
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

BASE = ("https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/fews/web/africa/east/"
        "dekadal/wrsi-chirps-etos")
BBOX = (33.9, -4.7, 41.9, 5.5)          # Kenya (W,S,E,N) — matches other tiers
COG_OPTS = dict(driver="COG", compress="DEFLATE", predictor=2, blocksize=512,
                overview_resampling="average")

# code -> (regiondir, crop, season, eos_dekad). FEWS Table 1 (East Africa rows):
#   e1 Rangeland Sep-Jan | e2 Rangeland Feb-Jul | ee Maize Mar-Nov | et Maize Oct-Feb
REGION_MAP = {
    "e1": ("east1", "rangeland", "OND", 36),  # short rains, EOS dk36
    "e2": ("east2", "rangeland", "MAM", 21),  # long rains, EOS dk21
    "ee": ("easte", "cropland",  "MAM", 33),  # maize long rains (Mar-Nov), EOS dk33
    "et": ("eastt", "cropland",  "OND", 36),  # maize short rains (Oct-Feb), EOS dk36
    # ek dropped: Ethiopian-highland belg sorghum, ~0% of Kenya bbox.
    # el (grains, Mar-Nov, dk33): optional sorghum companion to ee, not requested.
}
YEARS = range(2003, 2027)   # CHIRPS-ETos WRSI archive span (verify earliest on smoke)


def log(msg):
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def zip_url(code, year, dekad):
    regiondir = REGION_MAP[code][0]
    return f"{BASE}/{regiondir}/downloads/dekadal/w{year}{dekad:02d}{code}.zip"


def fetch_eos_tif(code, year, dekad, tmpdir):
    """Download the dekad zip, extract the EOS WRSI tif (w{year}{dd}eo.tif) to tmpdir."""
    url = zip_url(code, year, dekad)
    try:
        with urllib.request.urlopen(url, timeout=120) as r:
            zdata = r.read()
    except Exception as e:
        log(f"    no zip {os.path.basename(url)} ({type(e).__name__})")
        return None
    zf = zipfile.ZipFile(io.BytesIO(zdata))
    # EOS product suffix = 'eo'; filename w{year}{dd}eo.tif (region implicit in the zip)
    want = [n for n in zf.namelist() if n.lower().endswith("eo.tif")]
    if not want:
        log(f"    no *eo.tif in {os.path.basename(url)} (have: {zf.namelist()[:4]}...)")
        return None
    out = os.path.join(tmpdir, os.path.basename(want[0]))
    with open(out, "wb") as fh:
        fh.write(zf.read(want[0]))
    return out


def build(code, year, out_dir, overwrite, tmpdir):
    regiondir, crop, season, eos_dekad = REGION_MAP[code]
    out = os.path.join(out_dir, f"wrsi_{crop}_{season}_{year}.tif")
    if not overwrite and os.path.exists(out) and os.path.getsize(out) > 100:
        log(f"  {crop}/{season}/{year}: exists, skip"); return "skip"
    tif = fetch_eos_tif(code, year, eos_dekad, tmpdir)
    if tif is None:
        return "empty"
    # crop Kenya + reproject-safe (already EPSG:4326) via gdalwarp
    warped = gdal.Warp("", tif, format="MEM", outputBounds=BBOX, outputBoundsSRS="EPSG:4326",
                       dstSRS="EPSG:4326", resampleAlg="near")
    arr = warped.GetRasterBand(1).ReadAsArray().astype("float32")
    warped = None
    arr[(arr > 100) | (arr < 0)] = np.nan          # mask status codes 253/254 + nodata -9999
    if not np.isfinite(arr).any():
        log(f"  {crop}/{season}/{year}: all-NaN over Kenya, skip"); return "empty"
    h, w = arr.shape
    transform = rasterio.transform.from_bounds(BBOX[0], BBOX[1], BBOX[2], BBOX[3], w, h)
    os.makedirs(out_dir, exist_ok=True)
    prof = dict(height=h, width=w, count=1, dtype="float32", crs="EPSG:4326",
                transform=transform, nodata=float("nan"), **COG_OPTS)
    with rasterio.open(out, "w", **prof) as dst:
        dst.write(arr, 1)
    finite = np.isfinite(arr)
    log(f"  {crop}/{season}/{year}: -> {out} (WRSI% min {np.nanmin(arr[finite]):.0f} / "
        f"mean {np.nanmean(arr[finite]):.0f} / max {np.nanmax(arr[finite]):.0f})")
    return "written"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="Data/wrsi_fews/WRSI")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--smoke", action="store_true", help="one region (e2 rangeland MAM), one year (2015)")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    import tempfile
    codes = ["e2"] if a.smoke else list(REGION_MAP)
    years = [2015] if a.smoke else list(YEARS)
    log(f"WRSI ingest | codes={codes} years={years[0]}-{years[-1]} out={a.out}")
    tally = {"written": 0, "skip": 0, "empty": 0}
    with tempfile.TemporaryDirectory() as tmp:
        for code in codes:
            for y in years:
                try:
                    tally[build(code, y, a.out, a.overwrite, tmp)] += 1
                except Exception as e:
                    log(f"  ERROR {code} {y}: {type(e).__name__}: {e}")
                    if a.smoke:
                        raise
    log(f"DONE: {tally}")
    print("\nNext: publish with  Rscript R/observational/6_publish_obs_to_s3.R --full --tier 8")


if __name__ == "__main__":
    main()
