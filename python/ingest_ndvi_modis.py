#!/usr/bin/env python3
"""
ingest_ndvi_modis.py — MODIS MOD13Q1 NDVI -> seasonal/annual COGs (non-GEE, NASA LP DAAC).

Pulls MOD13Q1 v061 250 m 16-day NDVI via `earthaccess`, per (year, window):
  - mask NoData (-3000) + pixel-reliability (keep 0=good, 1=marginal),
  - scale DN/10000 -> real NDVI,
  - mosaic the region's MODIS tiles per 16-day composite,
  - mean across the composites in the window (seasonal OND/MAM, or annual),
  - reproject MODIS Sinusoidal -> EPSG:4326,
  - write COG WITH overviews.

Verified specs (cglabs dispatch #2b, 2026-08-16):
  SDS "250m 16 days NDVI"; Sinusoidal R=6371007.181; 231.656 m; scale 10000; NoData -3000;
  reliability SDS "250m 16 days pixel reliability"; Kenya = 4 tiles h21v08/09,h22v08/09.
Requires: earthaccess, rasterio, numpy, gdal+libgdal-hdf4 (HDF4 driver). Earthdata login via
  ~/.netrc or EARTHDATA_USERNAME/EARTHDATA_PASSWORD.

RUN (cglabs):
  python3 python/ingest_ndvi_modis.py --smoke              # 1 year x OND, region default -> gate
  python3 python/ingest_ndvi_modis.py --seasons OND,MAM --annual --years 2000:2025
Idempotent: skips an output COG that exists unless --overwrite.
Output: <out>/NDVI_{SEASON}_{YYYY}_mean.tif  and  <out>/NDVI_{YYYY}_mean.tif
"""
import argparse
import datetime as dt
import glob
import os
import re
import sys
import tempfile

import numpy as np
import rasterio
from rasterio.merge import merge as rio_merge
from rasterio.warp import calculate_default_transform, reproject, Resampling

# ---- constants (verified) ---------------------------------------------------
SDS_GRID = "MODIS_Grid_16DAY_250m_500m_VI"
SDS_NDVI = f'{SDS_GRID}:"250m 16 days NDVI"'
SDS_REL = f'{SDS_GRID}:"250m 16 days pixel reliability"'
SCALE = 10000.0
FILL = -3000
DST_CRS = "EPSG:4326"
# East-Africa / Kenya region (the 4 MOD13Q1 tiles cglabs confirmed cover Kenya).
DEFAULT_BBOX = (33.9, -4.7, 41.9, 5.5)          # W,S,E,N
SEASON_MONTHS = {
    "JFM": (1, 3), "FMA": (2, 4), "MAM": (3, 5), "AMJ": (4, 6), "MJJ": (5, 7),
    "JJA": (6, 8), "JAS": (7, 9), "ASO": (8, 10), "SON": (9, 11), "OND": (10, 12),
}
COG_OPTS = dict(driver="COG", compress="DEFLATE", predictor=2, blocksize=512,
                overview_resampling="average")


def log(msg):
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def sds_uri(hdf, sds):
    return f'HDF4_EOS:EOS_GRID:"{hdf}":{sds}'


def granule_date(path):
    m = re.search(r"\.A(\d{4})(\d{3})\.", os.path.basename(path))
    y, doy = int(m.group(1)), int(m.group(2))
    return dt.date(y, 1, 1) + dt.timedelta(days=doy - 1)


# ---- per-granule read (scaled + masked, native Sinusoidal) ------------------
def read_scaled_tile(hdf, tmpdir, mask_reliability=True):
    """Read NDVI SDS, apply fill + scale + reliability mask, write a temp GTiff
    in native Sinusoidal; return the temp path (for mosaicking)."""
    with rasterio.open(sds_uri(hdf, SDS_NDVI)) as ds:
        ndvi = ds.read(1).astype("float32")
        prof = ds.profile.copy()
        transform, crs = ds.transform, ds.crs
    ndvi[ndvi == FILL] = np.nan
    ndvi = ndvi / SCALE
    if mask_reliability:
        with rasterio.open(sds_uri(hdf, SDS_REL)) as ds:
            rel = ds.read(1)
        ndvi[(rel != 0) & (rel != 1)] = np.nan   # keep good(0)+marginal(1)
    prof.update(driver="GTiff", dtype="float32", count=1, nodata=np.nan,
                transform=transform, crs=crs, compress="deflate")
    out = os.path.join(tmpdir, os.path.basename(hdf) + ".ndvi.tif")
    with rasterio.open(out, "w", **prof) as dst:
        dst.write(ndvi, 1)
    return out


def mosaic_date(tile_tifs, tmpdir, tag):
    """Mosaic all tile tifs for one composite date -> single Sinusoidal GTiff."""
    srcs = [rasterio.open(p) for p in tile_tifs]
    try:
        arr, transform = rio_merge(srcs, nodata=np.nan)
        prof = srcs[0].profile.copy()
    finally:
        for s in srcs:
            s.close()
    prof.update(height=arr.shape[1], width=arr.shape[2], transform=transform,
                count=1, dtype="float32", nodata=np.nan, compress="deflate")
    out = os.path.join(tmpdir, f"mosaic_{tag}.tif")
    with rasterio.open(out, "w", **prof) as dst:
        dst.write(arr[0], 1)
    return out


def reproject_to_4326(arr, src_transform, src_crs):
    """Reproject a Sinusoidal array to EPSG:4326; return (arr, transform, profile)."""
    h, w = arr.shape
    dst_transform, dw, dh = calculate_default_transform(
        src_crs, DST_CRS, w, h,
        *rasterio.transform.array_bounds(h, w, src_transform))
    dst = np.full((dh, dw), np.nan, dtype="float32")
    reproject(arr, dst, src_transform=src_transform, src_crs=src_crs,
              dst_transform=dst_transform, dst_crs=DST_CRS,
              src_nodata=np.nan, dst_nodata=np.nan, resampling=Resampling.average)
    return dst, dst_transform


def write_cog(arr, transform, out_path):
    prof = dict(width=arr.shape[1], height=arr.shape[0], count=1, dtype="float32",
                crs=DST_CRS, transform=transform, nodata=np.nan, **COG_OPTS)
    with rasterio.open(out_path, "w", **prof) as dst:
        dst.write(arr, 1)


# ---- window composite -------------------------------------------------------
def window_dates(year, window):
    """Return (start_date, end_date) for a window in a given year."""
    if window == "annual":
        return dt.date(year, 1, 1), dt.date(year, 12, 31)
    m0, m1 = SEASON_MONTHS[window]
    last = 31 if m1 == 12 else (dt.date(year, m1 + 1, 1) - dt.timedelta(days=1)).day
    return dt.date(year, m0, 1), dt.date(year, m1, last)


def build_window(year, window, bbox, out_dir, mask_reliability, overwrite, keep_hdf):
    out_path = os.path.join(out_dir, f"NDVI_{window}_{year}_mean.tif")
    if not overwrite and os.path.exists(out_path) and os.path.getsize(out_path) > 100:
        log(f"  {window} {year}: exists, skip")
        return "skip"
    import earthaccess
    d0, d1 = window_dates(year, window)
    res = earthaccess.search_data(short_name="MOD13Q1", version="061",
                                  bounding_box=bbox,
                                  temporal=(d0.isoformat(), d1.isoformat()))
    if not res:
        log(f"  {window} {year}: 0 granules, skip")
        return "empty"
    with tempfile.TemporaryDirectory() as tmp:
        files = earthaccess.download(res, local_path=os.path.join(tmp, "hdf"))
        # keep only composites whose START date falls in the window months
        keep = [f for f in files if d0 <= granule_date(f) <= d1]
        by_date = {}
        for f in keep:
            by_date.setdefault(granule_date(f), []).append(f)
        if not by_date:
            log(f"  {window} {year}: no composites in-window, skip")
            return "empty"
        # per composite date: scale+mask each tile -> mosaic
        date_mosaics = []
        for cdate, tiles in sorted(by_date.items()):
            tile_tifs = [read_scaled_tile(f, tmp, mask_reliability) for f in tiles]
            date_mosaics.append(mosaic_date(tile_tifs, tmp, cdate.isoformat()))
        # stack date mosaics -> nanmean (window composite, still Sinusoidal)
        arrs, ref_tr, ref_crs = [], None, None
        for mp in date_mosaics:
            with rasterio.open(mp) as ds:
                arrs.append(ds.read(1))
                ref_tr, ref_crs = ds.transform, ds.crs
        stack = np.stack(arrs, axis=0)
        with np.errstate(invalid="ignore"):
            mean = np.nanmean(stack, axis=0).astype("float32")
        # reproject Sinusoidal -> EPSG:4326 -> COG
        rep, rep_tr = reproject_to_4326(mean, ref_tr, ref_crs)
        os.makedirs(out_dir, exist_ok=True)
        write_cog(rep, rep_tr, out_path)
        if keep_hdf:
            for f in keep:
                os.replace(f, os.path.join(out_dir, os.path.basename(f)))
    finite = np.isfinite(rep)
    log(f"  {window} {year}: {len(by_date)} composites -> {out_path} "
        f"(NDVI min {np.nanmin(rep[finite]):.3f} / mean {np.nanmean(rep[finite]):.3f} "
        f"/ max {np.nanmax(rep[finite]):.3f})")
    return "written"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seasons", default="OND,MAM", help="comma windows e.g. OND,MAM")
    ap.add_argument("--annual", action="store_true", help="also build annual mean")
    ap.add_argument("--years", default="2000:2025", help="lo:hi inclusive")
    ap.add_argument("--bbox", default=None, help="W,S,E,N (default East-Africa/Kenya)")
    ap.add_argument("--out", default=None, help="output dir")
    ap.add_argument("--no-reliability-mask", action="store_true")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--keep-hdf", action="store_true")
    ap.add_argument("--smoke", action="store_true", help="1 year x OND only")
    a = ap.parse_args()

    import earthaccess
    earthaccess.login()  # ~/.netrc or EARTHDATA_* env
    bbox = tuple(float(x) for x in a.bbox.split(",")) if a.bbox else DEFAULT_BBOX
    # default under the working-dir Data store; run from working_dir or pass --out.
    out_dir = a.out or "Data/ndvi_modis/NDVI"
    mask = not a.no_reliability_mask

    if a.smoke:
        windows, years = ["OND"], [2015]
    else:
        windows = [w.strip() for w in a.seasons.split(",") if w.strip()]
        if a.annual:
            windows.append("annual")
        lo, hi = (int(x) for x in a.years.split(":"))
        years = list(range(lo, hi + 1))

    log(f"NDVI ingest | windows={windows} years={years[0]}-{years[-1]} bbox={bbox} out={out_dir}")
    tally = {"written": 0, "skip": 0, "empty": 0}
    for y in years:
        for w in windows:
            try:
                tally[build_window(y, w, bbox, out_dir, mask, a.overwrite, a.keep_hdf)] += 1
            except Exception as e:
                log(f"  ERROR {w} {y}: {type(e).__name__}: {e}")
                if a.smoke:
                    raise
    log(f"DONE: {tally}")


if __name__ == "__main__":
    main()
