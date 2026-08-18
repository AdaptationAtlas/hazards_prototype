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

⚠️ REGION_MAP + EOS_DEKAD below are BEST-GUESS — cglabs MUST verify/correct them against USGS
  product pages 899 (croplands) / 891 (rangelands) during --smoke BEFORE the full run. Do not
  publish rangeland tiles until the ee/ek/el/et -> (rangeland, season) map is confirmed.

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

# code -> (regiondir, crop, season, eos_dekad).  ⚠️ VERIFY on --smoke (see header).
# Confident: e1=short rains, e2=long rains, both cropland (product page + cglabs).
# UNVERIFIED: ee/ek/el/et = rangeland zones — season + EOS dekad TBD; cglabs to confirm/correct.
REGION_MAP = {
    "e1": ("east1", "cropland", "OND", 36),   # short rains (VERIFIED cglabs #2: e1=OND/dk36 correct)
    "e2": ("east2", "cropland", "MAM", 21),   # long rains  (VERIFIED cglabs #2: e2=MAM/dk21 correct)
    # rangeland: pinned empirically by cglabs #2 (EOS dekads + real-WRSI verified):
    "ek": ("eastk", "rangeland", "MAM", 27),  # long-rains rangeland, EOS dk27
    "et": ("eastt", "rangeland", "OND", 36),  # short-rains rangeland, EOS dk36
    # ee/el = bimodal/annual monitor windows (EOS dk33) — season label unconfirmed, deferred.
    # "ee": ("easte", "rangeland", "??", 33),
    # "el": ("eastl", "rangeland", "??", 33),
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
    ap.add_argument("--smoke", action="store_true", help="one region (e2 cropland MAM), one year (2015)")
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
