#!/usr/bin/env python3
"""
ingest_flood_gfm.py — Copernicus CEMS GFM (Sentinel-1 SAR) observed flood extent -> COG tiers (Kenya).

Replaces the MODIS Global Flood Database (GFD, ends 2018) with GFM: continuously updated, cloud-
penetrating SAR flood extent, 2018 -> present.

ARCHITECTURE (v2, Pete 2026-08-28 "111m direct, drop 20m archive"): the notebook only uses the
~111 m aggregates, and the 20 m per-overpass archive was 16,211 acquisitions ≈ 9 days + ~113 GB.
So the monthly aggregate is built DIRECTLY from the STAC tiles at ~111 m (no 20 m overpass COGs
materialized), parallelized across months. Tiers:

  monthly   processing=monthly/variable={flooded,nobs}/{var}-{YYYY}-{MM}.tif                        (~111 m)
  seasonal  processing=seasonal/variable={flooded,nobs}/season={SEASON}/{var}_{SEASON}_{YYYY}.tif   (rolling 3-mo, 12 windows; PTOT-aligned)
  history   processing=history/variable={frequency,footprint}/*.tif                                 (full-record roll-up)
  overpass  processing=overpass/variable=flooded/{YYYYMMDD}T{HHMMSS}.tif  — OPT-IN ONLY (--stage overpass),
            20 m per-acquisition archive, re-pullable on demand for a specific event/date range.

Accuracy note: monthly warp reads source at FULL resolution (overviewLevel=NONE) with resampleAlg=max
+ srcNodata=255, so a ~111 m pixel is flooded if ANY 20 m sub-pixel flooded (no under-detection from
reading class-raster overviews).

Source (cglabs GFM probe, DISPATCH_cglabs_gfm_flood.md #1 — anonymous, no auth):
  STAC  https://stac.eodc.eu/api/v1/  collection=GFM  (POST /search, bbox+datetime)
  asset ensemble_flood_extent  (Byte: 0=not-flooded, 1=flooded, 255=NoData/not-observed; excludes permanent water)
  grid  Equi7-AF 20 m -> reproject EPSG:4326 with gdalwarp. Licence Copernicus EMS free/full/open.
  Attribution: "Contains modified Copernicus EMS information {YEAR}".

Coding: 255 = NOT OBSERVED this overpass, NOT dry. monthly flooded[px] = 1 if any acq flood==1;
0 if observed(>=1 valid) never flooded; 255 if all-255. nobs[px] = # acquisitions with value in {0,1}.

Requires: gdal (osgeo) + rasterio + numpy. No auth.

RUN (cglabs):
  python3 python/ingest_flood_gfm.py --smoke                         # ONE mini-month (3-day window) at 111m + gdalinfo gate. DO FIRST.
  python3 python/ingest_flood_gfm.py --stage all --workers 8         # monthly(direct 111m, parallel) -> seasonal -> history
  python3 python/ingest_flood_gfm.py --stage monthly --workers 8
  python3 python/ingest_flood_gfm.py --stage seasonal
  python3 python/ingest_flood_gfm.py --stage history
  python3 python/ingest_flood_gfm.py --stage overpass --start 2020-04-01 --end 2020-05-31   # OPT-IN 20m archive for a date range
Publish: R/observational/6_publish_obs_to_s3.R --full --tier 14
"""
import argparse
import calendar
import datetime as dt
import json
import os
import urllib.request
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import rasterio
from rasterio.transform import from_bounds
from osgeo import gdal

gdal.UseExceptions()

# ---- constants ---------------------------------------------------------------
STAC = "https://stac.eodc.eu/api/v1"
COLLECTION = "GFM"
ASSET = "ensemble_flood_extent"
BBOX = (33.9, -4.7, 41.9, 5.5)          # Kenya (W, S, E, N) — matches all KE tiers
TR_FINE = 0.0002                        # ~22 m overpass grid (opt-in archive only)
TR_COARSE = 0.001                       # ~111 m aggregate grid (nests 5x into fine; matches ~pop grid)
FLOOD, DRY, NODATA = 1, 0, 255          # GFM Byte coding
SEASONS = {
    1: "JFM", 2: "FMA", 3: "MAM", 4: "AMJ", 5: "MJJ", 6: "JJA",
    7: "JAS", 8: "ASO", 9: "SON", 10: "OND", 11: "NDJ", 12: "DJF",
}


def log(msg):
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


# ---- coarse grid geometry (fixed, deterministic) -----------------------------
def coarse_dims():
    w, s, e, n = BBOX
    return int(round((e - w) / TR_COARSE)), int(round((n - s) / TR_COARSE))


def coarse_transform():
    w, s, e, n = BBOX
    W, H = coarse_dims()
    return from_bounds(w, s, e, n, W, H)


COARSE_PROFILE = dict(driver="COG", crs="EPSG:4326", compress="DEFLATE", blocksize=512)


def write_cog(path, arr, dtype, nodata, overview_resampling):
    W, H = coarse_dims()
    prof = dict(width=W, height=H, count=1, dtype=dtype, nodata=nodata,
                transform=coarse_transform(), overview_resampling=overview_resampling, **COARSE_PROFILE)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with rasterio.open(path, "w", **prof) as dst:
        dst.write(arr.astype(dtype), 1)


# ---- STAC search (POST, paginated) -------------------------------------------
def stac_search(start, end, limit=500):
    """Yield STAC items for collection GFM intersecting Kenya bbox in [start,end] (dates YYYY-MM-DD)."""
    body = {
        "collections": [COLLECTION],
        "bbox": list(BBOX),
        "datetime": f"{start}T00:00:00Z/{end}T23:59:59Z",
        "limit": limit,
        "sortby": [{"field": "datetime", "direction": "asc"}],
    }
    url = f"{STAC}/search"
    while True:
        req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as r:
            fc = json.load(r)
        feats = fc.get("features", [])
        for f in feats:
            yield f
        nxt = next((l for l in fc.get("links", []) if l.get("rel") == "next"), None)
        if not nxt or not feats:
            break
        # stac-fastapi POST paging: link key is 'href' (not 'url'); merge the paging token onto body
        # so collections/bbox/datetime survive across pages (cglabs fix 6759ae2, ratified).
        url = nxt.get("href") or url
        nb = nxt.get("body")
        if isinstance(nb, dict):
            body = {**body, **nb}


def asset_href(item):
    a = item.get("assets", {}).get(ASSET)
    return a.get("href") if a else None


def group_by_overpass(items):
    """Group items by acquisition datetime (one S1 acquisition = a swath spanning several tiles)."""
    groups = {}
    for it in items:
        t = it["properties"]["datetime"]
        href = asset_href(it)
        if href:
            groups.setdefault(t, []).append("/vsicurl/" + href)
    return dict(sorted(groups.items()))


def warp_srcs_to_coarse(srcs):
    """Mosaic acquisition tiles straight onto the fixed ~111 m grid. FULL-res read (overviewLevel=NONE)
    + max over valid {0,1} so a coarse pixel is flooded if ANY 20 m sub-pixel flooded; 255 where unobserved."""
    W, H = coarse_dims()
    ds = gdal.Warp("", srcs, format="MEM", dstSRS="EPSG:4326", outputBounds=BBOX,
                   width=W, height=H, srcNodata=NODATA, dstNodata=NODATA,
                   resampleAlg="max", overviewLevel="NONE")
    a = ds.GetRasterBand(1).ReadAsArray()
    ds = None
    return a


# ---- month helpers -----------------------------------------------------------
def month_bounds(year, month):
    last = calendar.monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last:02d}"


def iter_months(start, end):
    y0, m0 = int(start[:4]), int(start[5:7])
    y1, m1 = int(end[:4]), int(end[5:7])
    cur = y0 * 12 + (m0 - 1)
    out = []
    while cur <= y1 * 12 + (m1 - 1):
        out.append((cur // 12, cur % 12 + 1))
        cur += 1
    return out


# ---- stage: monthly (DIRECT from STAC tiles at 111 m) ------------------------
def build_month(args):
    """Worker: build one month's flooded+nobs COGs directly from STAC. Returns a status tuple."""
    year, month, fl_dir, nb_dir, overwrite = args
    fl_out = os.path.join(fl_dir, f"{year:04d}-{month:02d}.tif")
    nb_out = os.path.join(nb_dir, f"{year:04d}-{month:02d}.tif")
    if not overwrite and os.path.exists(fl_out) and os.path.exists(nb_out):
        return (year, month, "skip", 0, 0)
    start, end = month_bounds(year, month)
    groups = group_by_overpass(stac_search(start, end))
    if not groups:
        return (year, month, "empty", 0, 0)
    W, H = coarse_dims()
    flood_any = np.zeros((H, W), np.uint8)
    obs = np.zeros((H, W), np.uint16)
    for _ts, srcs in groups.items():
        a = warp_srcs_to_coarse(srcs)
        obs += (a != NODATA).astype(np.uint16)
        flood_any = np.maximum(flood_any, (a == FLOOD).astype(np.uint8))
    flooded = np.where(obs > 0, flood_any, NODATA).astype(np.uint8)
    write_cog(fl_out, flooded, "uint8", NODATA, "nearest")
    write_cog(nb_out, obs, "uint16", 0, "average")
    return (year, month, "written", len(groups), int(obs.max()))


def stage_monthly(root, start, end, overwrite, workers):
    fl_dir = os.path.join(root, "monthly", "flooded")
    nb_dir = os.path.join(root, "monthly", "nobs")
    os.makedirs(fl_dir, exist_ok=True)
    os.makedirs(nb_dir, exist_ok=True)
    months = iter_months(start, end)
    W, H = coarse_dims()
    log(f"STAGE monthly (direct 111m) | {len(months)} months {start}..{end} | grid {W}x{H} | workers={workers}")
    tasks = [(y, m, fl_dir, nb_dir, overwrite) for y, m in months]
    tally = {"written": 0, "skip": 0, "empty": 0}
    done = 0
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(build_month, t): t for t in tasks}
        for fut in as_completed(futs):
            y, m, st, nacq, maxobs = fut.result()
            tally[st] += 1
            done += 1
            if st == "written":
                log(f"  [{done}/{len(months)}] {y}-{m:02d}: {nacq} acq -> flooded+nobs (max obs/px {maxobs})")
            elif st == "empty":
                log(f"  [{done}/{len(months)}] {y}-{m:02d}: no acquisitions")
    log(f"STAGE monthly DONE: {tally}")


# ---- stage: seasonal (rolling 3-month from monthly) --------------------------
def _month_add(year, month, k):
    m0 = (year * 12 + (month - 1)) + k
    return m0 // 12, m0 % 12 + 1


def stage_seasonal(root, overwrite):
    fl_m = os.path.join(root, "monthly", "flooded")
    nb_m = os.path.join(root, "monthly", "nobs")
    fl_s = os.path.join(root, "seasonal", "flooded")
    nb_s = os.path.join(root, "seasonal", "nobs")
    W, H = coarse_dims()
    have = {f[:7] for f in os.listdir(fl_m)} if os.path.isdir(fl_m) else set()   # {YYYY-MM}
    log(f"STAGE seasonal | {len(have)} monthly present -> rolling 3-month windows")
    for ym in sorted(have):
        year, month = int(ym[:4]), int(ym[5:7])
        keys = [f"{y:04d}-{m:02d}" for y, m in (_month_add(year, month, k) for k in range(3))]
        if not all(k in have for k in keys):
            continue
        code = SEASONS[month]
        fl_out = os.path.join(fl_s, f"{code}_{year:04d}.tif")
        nb_out = os.path.join(nb_s, f"{code}_{year:04d}.tif")
        if not overwrite and os.path.exists(fl_out) and os.path.exists(nb_out):
            continue
        flood_any = np.zeros((H, W), np.uint8)
        obs = np.zeros((H, W), np.uint32)
        seen = np.zeros((H, W), bool)
        for k in keys:
            with rasterio.open(os.path.join(fl_m, k + ".tif")) as ds:
                fa = ds.read(1)
            with rasterio.open(os.path.join(nb_m, k + ".tif")) as ds:
                nb = ds.read(1)
            valid = fa != NODATA
            seen |= valid
            flood_any = np.maximum(flood_any, np.where(valid, fa, 0).astype(np.uint8))
            obs += nb.astype(np.uint32)
        flooded = np.where(seen, flood_any, NODATA).astype(np.uint8)
        write_cog(fl_out, flooded, "uint8", NODATA, "nearest")
        write_cog(nb_out, np.minimum(obs, 65535).astype(np.uint16), "uint16", 0, "average")
        log(f"  {code}_{year}: {keys} -> seasonal flooded+nobs")
    log("STAGE seasonal DONE")


# ---- stage: history (full-record roll-up) ------------------------------------
def stage_history(root, overwrite):
    fl_m = os.path.join(root, "monthly", "flooded")
    out_dir = os.path.join(root, "history")
    W, H = coarse_dims()
    keys = sorted(f for f in os.listdir(fl_m) if f.endswith(".tif")) if os.path.isdir(fl_m) else []
    log(f"STAGE history | roll up {len(keys)} months")
    freq_out = os.path.join(out_dir, "frequency.tif")
    foot_out = os.path.join(out_dir, "footprint.tif")
    if not overwrite and os.path.exists(freq_out) and os.path.exists(foot_out):
        log("  exists, skip"); return
    flood_months = np.zeros((H, W), np.uint16)
    obs_months = np.zeros((H, W), np.uint16)
    for k in keys:
        with rasterio.open(os.path.join(fl_m, k)) as ds:
            fa = ds.read(1)
        obs_months += (fa != NODATA).astype(np.uint16)
        flood_months += (fa == FLOOD).astype(np.uint16)
    with np.errstate(invalid="ignore", divide="ignore"):
        freq = np.where(obs_months > 0, flood_months / obs_months, np.nan).astype(np.float32)
    footprint = (flood_months > 0).astype(np.uint8)
    write_cog(freq_out, freq, "float32", float("nan"), "average")
    write_cog(foot_out, footprint, "uint8", 255, "nearest")
    log(f"  history -> frequency (max {np.nanmax(freq):.3f}) + footprint (flooded px {int(footprint.sum())})")
    log("STAGE history DONE")


# ---- stage: overpass (OPT-IN 20 m archive for a date range) ------------------
def overpass_key(ts):
    return ts.replace(":", "").replace("-", "").rstrip("Z").split(".")[0]   # 20180421T151203


def stage_overpass(root, start, end, overwrite):
    out_dir = os.path.join(root, "overpass")
    os.makedirs(out_dir, exist_ok=True)
    groups = group_by_overpass(stac_search(start, end))
    log(f"STAGE overpass (20m archive) | {len(groups)} acquisitions {start}..{end}")
    tally = {"written": 0, "skip": 0}
    for i, (ts, srcs) in enumerate(groups.items(), 1):
        out = os.path.join(out_dir, f"{overpass_key(ts)}.tif")
        if not overwrite and os.path.exists(out) and os.path.getsize(out) > 100:
            tally["skip"] += 1; continue
        tmp = out + ".tmp.tif"
        gdal.Warp(tmp, srcs, format="COG", dstSRS="EPSG:4326", outputBounds=BBOX,
                  xRes=TR_FINE, yRes=TR_FINE, srcNodata=NODATA, dstNodata=NODATA, resampleAlg="near",
                  creationOptions=["COMPRESS=DEFLATE", "BLOCKSIZE=512", "OVERVIEW_RESAMPLING=NEAREST"])
        os.replace(tmp, out)
        tally["written"] += 1
        log(f"  [{i}/{len(groups)}] {ts} ({len(srcs)} tiles) -> {os.path.basename(out)}")
    log(f"STAGE overpass DONE: {tally}")


# ---- main --------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="Data/exposure/gfm_flood")
    ap.add_argument("--stage", default="all",
                    choices=["all", "monthly", "seasonal", "history", "overpass"])
    ap.add_argument("--start", default="2018-01-01")
    ap.add_argument("--end", default="2025-12-31")
    ap.add_argument("--workers", type=int, default=min(8, (os.cpu_count() or 4)))
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--smoke", action="store_true", help="one mini-month (3-day window) at 111m + gdalinfo gate; do FIRST")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    log(f"GFM flood ingest v2 | stage={a.stage} smoke={a.smoke} workers={a.workers} out={a.out}")

    if a.smoke:
        # mini-month: a tight 3-day window (Kenya late-Apr-2020 floods) through the REAL month path.
        # Write to a SIBLING _smoke dir so the tier-14 recursive publish (local_dir=gfm_flood) never sees it.
        smoke_dir = a.out.rstrip("/") + "_smoke"
        os.makedirs(smoke_dir, exist_ok=True)
        groups = group_by_overpass(stac_search("2020-04-24", "2020-04-26"))
        log(f"SMOKE: {len(groups)} acquisitions in the 3-day window")
        W, H = coarse_dims()
        flood_any = np.zeros((H, W), np.uint8); obs = np.zeros((H, W), np.uint16)
        for _ts, srcs in groups.items():
            arr = warp_srcs_to_coarse(srcs)
            obs += (arr != NODATA).astype(np.uint16)
            flood_any = np.maximum(flood_any, (arr == FLOOD).astype(np.uint8))
        flooded = np.where(obs > 0, flood_any, NODATA).astype(np.uint8)
        out = os.path.join(smoke_dir, "smoke.tif")
        write_cog(out, flooded, "uint8", NODATA, "nearest")
        info = gdal.Info(out, format="json", stats=True)
        b = info["bands"][0]
        log(f"SMOKE gdalinfo: size={info['size']} nodata={b.get('noDataValue')} "
            f"min={b.get('minimum')} max={b.get('maximum')} overviews={len(b.get('overviews', []))} "
            f"| valid px {int((obs>0).sum())} flooded px {int((flooded==FLOOD).sum())}")
        assert list(info["size"]) == list(coarse_dims()), f"SMOKE FAIL: grid {info['size']} != coarse {coarse_dims()}"
        assert b.get("overviews") or max(info["size"]) <= 512, "SMOKE FAIL: no overviews"
        assert (b.get("maximum") or 0) <= 255, "SMOKE FAIL: value range"
        log("SMOKE OK — direct 111m month path (tiles->coarse max, 0/1/255, overviews) confirmed. Proceed to --stage all.")
        return

    if a.stage in ("all", "monthly"):
        stage_monthly(a.out, a.start, a.end, a.overwrite, a.workers)
    if a.stage in ("all", "seasonal"):
        stage_seasonal(a.out, a.overwrite)
    if a.stage in ("all", "history"):
        stage_history(a.out, a.overwrite)
    if a.stage == "overpass":
        stage_overpass(a.out, a.start, a.end, a.overwrite)
    log("DONE")
    print("\nNext: publish with  Rscript R/observational/6_publish_obs_to_s3.R --full --tier 14")


if __name__ == "__main__":
    main()
