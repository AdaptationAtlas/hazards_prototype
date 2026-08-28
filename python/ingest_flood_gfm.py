#!/usr/bin/env python3
"""
ingest_flood_gfm.py — Copernicus CEMS GFM (Sentinel-1 SAR) observed flood extent -> COG tiers (Kenya).

Replaces the MODIS Global Flood Database (GFD, ends 2018) with GFM: continuously updated, cloud-
penetrating SAR flood extent, 2018 -> present. Four aggregation tiers (Pete: store each overpass,
then monthly, then 3-month seasonal; + optional full-record history):

  1. overpass  processing=overpass/variable=flooded/{YYYY-MM-DDTHHMMSSZ}.tif   (native ~20 m, per S1 acquisition swath)
  2. monthly   processing=monthly/variable={flooded,nobs}/{YYYY-MM}.tif        (~111 m: occurrence + valid-obs count)
  3. seasonal  processing=seasonal/variable={flooded,nobs}/season={SEASON}/{var}_{SEASON}_{YYYY}.tif  (rolling 3-month, 12 windows; PTOT-aligned)
  4. history   processing=history/variable={frequency,footprint}.tif           (full-record roll-up, optional layer)

Source (cglabs GFM probe, DISPATCH_cglabs_gfm_flood.md #1 — anonymous, no auth):
  STAC  https://stac.eodc.eu/api/v1/  collection=GFM  (POST /search, bbox+datetime)
  asset ensemble_flood_extent  (Byte: 0=not-flooded, 1=flooded, 255=NoData/not-observed; excludes permanent water)
  grid  Equi7-AF 20 m -> reproject EPSG:4326 with gdalwarp (nearest / max, nodata-aware)
  licence Copernicus EMS free/full/open. Attribution: "Contains modified Copernicus EMS information {YEAR}".

Coding logic (the load-bearing bit): 255 = NOT OBSERVED this overpass, NOT dry. So:
  monthly flooded[px] = 1 if any overpass in month had flood==1 ; 0 if observed(>=1 valid) but never flooded ; 255 if all-255
  monthly nobs[px]    = count of overpasses where value in {0,1}
Resampling 20m->coarse uses srcNodata=255 + resampleAlg=max so a coarse pixel is flooded if ANY 20m subpixel flooded.

Requires: gdal (osgeo) + rasterio + numpy. No auth.

RUN (cglabs):
  python3 python/ingest_flood_gfm.py --smoke                         # ONE overpass end-to-end + gdalinfo gate (prove Equi7->4326 + coding + overviews). DO THIS FIRST.
  python3 python/ingest_flood_gfm.py --stage overpass --start 2018-01-01 --end 2025-12-31
  python3 python/ingest_flood_gfm.py --stage monthly
  python3 python/ingest_flood_gfm.py --stage seasonal
  python3 python/ingest_flood_gfm.py --stage history
  python3 python/ingest_flood_gfm.py --stage all                     # overpass -> monthly -> seasonal -> history
Publish: R/observational/6_publish_obs_to_s3.R  (GFM tier — added after smoke confirms the on-disk shape)
"""
import argparse
import calendar
import datetime as dt
import json
import os
import urllib.request

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
TR_FINE = 0.0002                        # ~22 m overpass grid (EPSG:4326)
TR_COARSE = 0.001                       # ~111 m aggregate grid (nests 5x into fine; matches ~pop grid)
FLOOD, DRY, NODATA = 1, 0, 255          # GFM Byte coding
# 12 rolling 3-month windows (start month -> 3-letter code)
SEASONS = {
    1: "JFM", 2: "FMA", 3: "MAM", 4: "AMJ", 5: "MJJ", 6: "JJA",
    7: "JAS", 8: "ASO", 9: "SON", 10: "OND", 11: "NDJ", 12: "DJF",
}


def log(msg):
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


# ---- coarse grid geometry (fixed, deterministic) -----------------------------
def coarse_dims():
    w, s, e, n = BBOX
    W = int(round((e - w) / TR_COARSE))
    H = int(round((n - s) / TR_COARSE))
    return W, H


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
    """Yield STAC items for collection GFM intersecting Kenya bbox in [start,end]."""
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
        # stac-fastapi POST paging: next link carries href + a (partial) body token.
        # CGLABS 2026-08-28 fix: the STAC link key is 'href', not 'url' (KeyError on page 2 —
        # smoke's <500-item window never paginated). Merge the token onto the current body so
        # collections/bbox/datetime survive across pages.
        url = nxt.get("href") or url
        nb = nxt.get("body")
        if isinstance(nb, dict):
            body = {**body, **nb} if nb.get("merge") else nb


def asset_href(item):
    a = item.get("assets", {}).get(ASSET)
    return a.get("href") if a else None


def group_by_overpass(items):
    """Group items by acquisition datetime (one S1 acquisition = a swath spanning several tiles)."""
    groups = {}
    for it in items:
        t = it["properties"]["datetime"]                 # e.g. 2018-04-21T15:12:03Z
        href = asset_href(it)
        if href:
            groups.setdefault(t, []).append("/vsicurl/" + href)
    return dict(sorted(groups.items()))


# ---- stage 1: overpass (mosaic Equi7 tiles -> EPSG:4326 fine COG) -------------
def overpass_key(ts):
    return ts.replace(":", "").replace("-", "").replace("T", "T").rstrip("Z").split(".")[0]  # 20180421T151203


def build_overpass(out_dir, srcs, ts, overwrite):
    key = overpass_key(ts)
    out = os.path.join(out_dir, f"{key}.tif")
    if not overwrite and os.path.exists(out) and os.path.getsize(out) > 100:
        return "skip", out
    tmp = out + ".tmp.tif"
    gdal.Warp(tmp, srcs, format="COG", dstSRS="EPSG:4326",
              outputBounds=BBOX, xRes=TR_FINE, yRes=TR_FINE,
              srcNodata=NODATA, dstNodata=NODATA, resampleAlg="near",
              creationOptions=["COMPRESS=DEFLATE", "BLOCKSIZE=512",
                               "OVERVIEW_RESAMPLING=NEAREST"])
    os.replace(tmp, out)
    return "written", out


def stage_overpass(root, start, end, overwrite, smoke=False):
    out_dir = os.path.join(root, "overpass")
    os.makedirs(out_dir, exist_ok=True)
    log(f"STAGE overpass | {start}..{end} bbox={BBOX}")
    groups = group_by_overpass(stac_search(start, end))
    log(f"  {len(groups)} distinct acquisition times")
    if smoke:
        groups = dict(list(groups.items())[:1])          # one overpass only
    tally = {"written": 0, "skip": 0}
    for i, (ts, srcs) in enumerate(groups.items(), 1):
        st, out = build_overpass(out_dir, srcs, ts, overwrite)
        tally[st] += 1
        if st == "written" or smoke:
            log(f"  [{i}/{len(groups)}] {ts} ({len(srcs)} tiles) -> {os.path.basename(out)} [{st}]")
    log(f"STAGE overpass DONE: {tally}")
    return out_dir


# ---- stage 2: monthly (overpass fine -> coarse occurrence + nobs) ------------
def read_overpass_coarse(path):
    """Warp a fine overpass COG onto the fixed coarse grid: max over valid {0,1}, 255 where unobserved."""
    W, H = coarse_dims()
    ds = gdal.Warp("", path, format="MEM", dstSRS="EPSG:4326", outputBounds=BBOX,
                   width=W, height=H, srcNodata=NODATA, dstNodata=NODATA, resampleAlg="max")
    a = ds.GetRasterBand(1).ReadAsArray()
    ds = None
    return a                                              # uint8 {0,1,255}, shape (H,W)


def month_overpasses(overpass_dir, year, month):
    pre = f"{year:04d}{month:02d}"
    return sorted(os.path.join(overpass_dir, f) for f in os.listdir(overpass_dir)
                  if f.startswith(pre) and f.endswith(".tif"))


def stage_monthly(root, overwrite):
    overpass_dir = os.path.join(root, "overpass")
    fl_dir = os.path.join(root, "monthly", "flooded")
    nb_dir = os.path.join(root, "monthly", "nobs")
    W, H = coarse_dims()
    log(f"STAGE monthly | coarse {W}x{H} @ {TR_COARSE}deg")
    files = os.listdir(overpass_dir) if os.path.isdir(overpass_dir) else []
    months = sorted({f[:6] for f in files if f.endswith(".tif")})
    log(f"  {len(months)} months present")
    for ym in months:
        year, month = int(ym[:4]), int(ym[4:6])
        fl_out = os.path.join(fl_dir, f"{year:04d}-{month:02d}.tif")
        nb_out = os.path.join(nb_dir, f"{year:04d}-{month:02d}.tif")
        if not overwrite and os.path.exists(fl_out) and os.path.exists(nb_out):
            continue
        ops = month_overpasses(overpass_dir, year, month)
        flood_any = np.zeros((H, W), np.uint8)
        obs = np.zeros((H, W), np.uint16)
        for p in ops:
            a = read_overpass_coarse(p)
            valid = a != NODATA
            obs += valid.astype(np.uint16)
            flood_any = np.maximum(flood_any, (a == FLOOD).astype(np.uint8))
        flooded = np.where(obs > 0, flood_any, NODATA).astype(np.uint8)      # 0/1 where observed, 255 else
        write_cog(fl_out, flooded, "uint8", NODATA, "nearest")
        write_cog(nb_out, obs, "uint16", 0, "average")
        log(f"  {ym}: {len(ops)} overpasses -> flooded+nobs (max obs/px {int(obs.max())})")
    log("STAGE monthly DONE")


# ---- stage 3: seasonal (rolling 3-month from monthly) ------------------------
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
        win = [_month_add(year, month, k) for k in range(3)]                     # start month + next 2
        keys = [f"{y:04d}-{m:02d}" for y, m in win]
        if not all(k in have for k in keys):
            continue                                                            # window incomplete at record edge
        code = SEASONS[month]
        # {SEASON}_{YYYY} matches PTOT seasonal order so name_fn can build the season= partition
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


# ---- stage 4: history (full-record roll-up) ----------------------------------
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
    flood_months = np.zeros((H, W), np.uint16)      # # months flooded
    obs_months = np.zeros((H, W), np.uint16)        # # months observed (obs-density de-biased)
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


# ---- main --------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="Data/exposure/gfm_flood")
    ap.add_argument("--stage", default="all",
                    choices=["all", "overpass", "monthly", "seasonal", "history"])
    ap.add_argument("--start", default="2018-01-01")
    ap.add_argument("--end", default="2025-12-31")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--smoke", action="store_true", help="one overpass end-to-end + gdalinfo gate; do this FIRST")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    log(f"GFM flood ingest | stage={a.stage} smoke={a.smoke} out={a.out}")

    if a.smoke:
        # tight known-flood window so the STAC query stays cheap (Kenya late-Apr-2020 floods)
        s_start, s_end = ("2020-04-24", "2020-04-30") if a.start == "2018-01-01" else (a.start, a.end)
        out_dir = stage_overpass(a.out, s_start, s_end, overwrite=True, smoke=True)
        tif = sorted(os.path.join(out_dir, f) for f in os.listdir(out_dir) if f.endswith(".tif"))[-1]
        info = gdal.Info(tif, format="json", stats=True)
        b = info["bands"][0]
        log(f"SMOKE gdalinfo: size={info['size']} nodata={b.get('noDataValue')} "
            f"min={b.get('minimum')} max={b.get('maximum')} overviews={len(b.get('overviews', []))}")
        assert b.get("overviews"), "SMOKE FAIL: overpass COG has no overviews"
        assert (b.get("maximum") or 0) <= 255, "SMOKE FAIL: unexpected value range"
        log("SMOKE OK — Equi7->4326 mosaic + 0/1/255 coding + overviews confirmed. Proceed to --stage all.")
        return

    if a.stage in ("all", "overpass"):
        stage_overpass(a.out, a.start, a.end, a.overwrite)
    if a.stage in ("all", "monthly"):
        stage_monthly(a.out, a.overwrite)
    if a.stage in ("all", "seasonal"):
        stage_seasonal(a.out, a.overwrite)
    if a.stage in ("all", "history"):
        stage_history(a.out, a.overwrite)
    log("DONE")
    print("\nNext: publish with the GFM tier in R/observational/6_publish_obs_to_s3.R (added after smoke).")


if __name__ == "__main__":
    main()
