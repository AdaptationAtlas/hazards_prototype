#!/usr/bin/env python3
"""Rangeland vegetation ingest (PLAN_obs_hazard_pipeline.md §3, decision D4): JRC ASAP FPAR dekads -> 0.25 deg
rangeland-weighted means on the NEX-GDDP lattice, plus the static rangeland fraction and phenology.

Source (anonymous HTTPS, checked 2026-10-27):
  https://agricultural-production-hotspots.ec.europa.eu/data/indicators_fpar/fpar/fpar_YYYYMMDD.tif
    global 80640x29346, 0.004464 deg, Byte 0-100 (FPAR x 100; 255 = nodata), DEFLATE, 256x256 tiles,
    dekads 2000-05-21 -> present. "Terra/Aqua MODIS, FPAR, Smoothed ~500m, Product-version=V062".
    It is a rolling NRT archive: a dekad is re-estimated until consolidated (~90 days), so only dekads
    at least --min-age days old are ingested, and every file's Last-Modified / ETag / size go in the manifest.
  https://agricultural-production-hotspots.ec.europa.eu/files/asap_mask_rangeland_v04.tif   Byte 0-100 = % rangeland
  https://agricultural-production-hotspots.ec.europa.eu/files/pheno{s1,e1,m1,s2,e2,m2}_v05.tif  season start/end/max
    as dekad index 1..72 (37-72 = the following year), 255 = nodata;  phenonseasons_v05.tif 1|2, 0 = nodata.

What is computed per dekad, per 0.25 deg cell (area-weighted over the fine cells, so the half-pixel offset
between the ASAP grid and our lattice is handled by reproject-average, never by block means):
  fpar_rl  = sum(fpar * w) / sum(w)      w = rangeland fraction (0..1) of each fine cell, FPAR valid
  rl_frac  = mean(w)                      written once (static)
A cell with sum(w) == 0 (no rangeland) is NaN in every dekad; the NaN mask must therefore be identical
across dekads (enforced, as in S1).

Outputs  <out_root>/Data/obs_fpar/dekads/fpar_rl_YYYYMMDD.tif            float32 FPAR x 100 (0-100), full lattice
         <out_root>/Data/obs_fpar/static/{rl_frac,pheno_s1,pheno_e1,pheno_s2,pheno_e2,pheno_n}.tif
            (phenology aggregated as the rangeland-weighted MODE of the fine dekad values; see pheno_mode)
         <out_root>/manifests/fpar_manifest_<stamp>.csv/.json

Subcommands:  ingest --out-root DIR [--years 2005:2025] [--smoke] [--workers 4] [--min-age 90] [--force] [--rl-min 0]
              validate --out-root DIR [--years ...] [--smoke]
              selftest
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import s1_ingest as S1  # noqa: E402  (lattice, window arithmetic, place(), snap_bbox, retry, log style)

ASAP = "https://agricultural-production-hotspots.ec.europa.eu"
FPAR_URL = ASAP + "/data/indicators_fpar/fpar/fpar_{d:%Y%m%d}.tif"
STATIC = {"rl_frac": "files/asap_mask_rangeland_v04.tif", "pheno_s1": "files/phenos1_v05.tif", "pheno_e1": "files/phenoe1_v05.tif",
          "pheno_s2": "files/phenos2_v05.tif", "pheno_e2": "files/phenoe2_v05.tif", "pheno_n": "files/phenonseasons_v05.tif"}
FPAR_NODATA = 255
DEKAD_DAYS = (1, 11, 21)
T0 = time.time()


def log(*m):
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S} | +{time.time() - T0:8.1f}s] [FPAR] " + " ".join(str(x) for x in m), flush=True)


class Stop(RuntimeError):
    pass


def dekads(years):
    out = []
    for y in years:
        for mth in range(1, 13):
            for d in DEKAD_DAYS:
                out.append(dt.date(y, mth, d))
    return out


# ---- reading ------------------------------------------------------------------------------------------------
def read_window(url, b, pad=0.01):
    """Fine-grid window (array, transform, nodata) of a global ASAP byte raster over bbox b."""
    import rasterio
    from rasterio.windows import from_bounds
    with rasterio.Env(GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR", GDAL_HTTP_MAX_RETRY="5", GDAL_HTTP_RETRY_DELAY="3",
                      CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif", GDAL_HTTP_MULTIRANGE="YES", GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES"):
        with rasterio.open("/vsicurl/" + url) as src:
            win = from_bounds(b[0] - pad, b[1] - pad, b[2] + pad, b[3] + pad, src.transform).round_offsets().round_lengths()
            a = src.read(1, window=win, boundless=True, fill_value=src.nodata if src.nodata is not None else FPAR_NODATA)
            return a, src.window_transform(win), src.nodata


def area_average(arr, wt, b):
    """Area-weighted mean of a float array (NaN = skip) onto the 0.25 window of bbox b."""
    import warnings
    from rasterio.enums import Resampling
    from rasterio.errors import NotGeoreferencedWarning
    from rasterio.warp import reproject
    w, h, transform = S1.window_grid(b)
    dst = np.full((h, w), np.nan, dtype="float64")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", NotGeoreferencedWarning)
        reproject(arr.astype("float64"), dst, src_transform=wt, src_crs="EPSG:4326", src_nodata=np.nan,
                  dst_transform=transform, dst_crs="EPSG:4326", dst_nodata=np.nan, resampling=Resampling.average)
    return dst


def rangeland_weighted(fpar_b, w_fine, wt, b, rl_min):
    """fpar_rl = avg(fpar*w) / avg(w) over the 0.25 cell, with w = rangeland fraction where FPAR is valid."""
    valid = fpar_b != FPAR_NODATA
    w = np.where(valid, w_fine, 0.0)
    num = area_average(np.where(valid, fpar_b.astype("float64") * w, 0.0), wt, b)
    den = area_average(w, wt, b)
    with np.errstate(invalid="ignore", divide="ignore"):
        out = num / den
    out[~(den > rl_min)] = np.nan            # no (or too little) rangeland in the cell
    return out.astype("float32"), den.astype("float32")


def pheno_mode(ph_fine, w_fine, wt, b, nodata):
    """Rangeland-weighted mode of a dekad-index layer per 0.25 cell: for each candidate value v, the weighted
    cover of cells == v; the argmax wins. Exact, if slow (<= 72 passes over the window)."""
    vals = np.unique(ph_fine[(ph_fine != nodata) & (w_fine > 0)])
    w, h, _ = S1.window_grid(b)
    best = np.full((h, w), np.nan, dtype="float32"); bestc = np.zeros((h, w), dtype="float64")
    for v in vals:
        c = area_average(np.where(ph_fine == v, w_fine, 0.0), wt, b)
        c = np.nan_to_num(c)
        upd = c > bestc
        best[upd] = v; bestc[upd] = c[upd]
    best[bestc <= 0] = np.nan
    return best


# ---- files ----------------------------------------------------------------------------------------------------
def out_dekad(root: Path, d):
    return root / "Data" / "obs_fpar" / "dekads" / f"fpar_rl_{d:%Y%m%d}.tif"


def write_full(path: Path, win_arr, b, tags):
    import rasterio
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tif.part")
    prof = dict(driver="GTiff", width=S1.BASE_SHAPE[1], height=S1.BASE_SHAPE[0], count=1, dtype="float32", crs="EPSG:4326",
                transform=S1.lattice_transform(), nodata=np.nan, compress="LZW", tiled=True, blockxsize=128, blockysize=128)
    with rasterio.open(tmp, "w", **prof) as dst:
        dst.write(S1.place(win_arr, b), 1); dst.update_tags(**tags, pipeline="hazards_prototype python/obs/fpar_ingest.py")
    os.replace(tmp, path)


def head_meta(url):
    import requests
    r = S1.retry(lambda: requests.head(url, timeout=60, allow_redirects=True), f"HEAD {url[-24:]}")
    if r.status_code != 200:
        raise Stop(f"{url}: HTTP {r.status_code}")
    return r.headers.get("Last-Modified", ""), r.headers.get("ETag", ""), int(r.headers.get("Content-Length", "0"))


# ---- commands --------------------------------------------------------------------------------------------------
def cmd_ingest(a):
    root = S1.check_root(a)
    b = S1.snap_bbox(S1.KENYA_SMOKE if a.smoke else S1.AFRICA)
    years = S1.parse_years(a.years)
    cutoff = dt.date.today() - dt.timedelta(days=a.min_age)
    want = [d for d in dekads(years) if d <= cutoff]
    skipped_recent = len(dekads(years)) - len(want)
    log(f"ingest bbox={b} years={years[0]}-{years[-1]} dekads={len(want)} (skipped {skipped_recent} younger than {a.min_age} d) rl_min={a.rl_min} root={root}")
    man_dir = root / "manifests"; man_dir.mkdir(parents=True, exist_ok=True)
    stamp = f"{dt.datetime.now():%Y%m%d_%H%M%S}_{os.getpid()}"
    mcsv = man_dir / f"fpar_manifest_{stamp}.csv"
    with open(mcsv, "w", newline="") as fh:
        csv.writer(fh).writerow(["kind", "date", "file", "md5", "source", "last_modified", "etag", "size"])

    def add(row):
        with S1._lock:
            with open(mcsv, "a", newline="") as fh:
                csv.writer(fh).writerow(row)

    # static layers first: rangeland mask (weights) and phenology
    log("static: rangeland mask")
    m_fine, wt, _ = read_window(ASAP + "/" + STATIC["rl_frac"], b)
    w_fine = np.clip(m_fine.astype("float64"), 0, 100) / 100.0
    sdir = root / "Data" / "obs_fpar" / "static"
    rl = area_average(w_fine, wt, b).astype("float32")
    f = sdir / "rl_frac.tif"
    if a.force or not f.exists():
        write_full(f, rl, b, dict(units="fraction", source=ASAP + "/" + STATIC["rl_frac"]))
        lm, et, sz = head_meta(ASAP + "/" + STATIC["rl_frac"]); add(["static", "", str(f.relative_to(root)), S1.md5(f), STATIC["rl_frac"], lm, et, sz])
    for k in ("pheno_s1", "pheno_e1", "pheno_s2", "pheno_e2", "pheno_n"):
        f = sdir / f"{k}.tif"
        if not (a.force or not f.exists()):
            continue
        log(f"static: {k}")
        ph, wt2, nd = read_window(ASAP + "/" + STATIC[k], b)
        nd = 0 if k == "pheno_n" else (nd if nd is not None else 255)
        mode = pheno_mode(ph, w_fine, wt2, b, nd)
        write_full(f, mode, b, dict(units="dekad index 1-72 (37-72 = following year)" if k != "pheno_n" else "seasons per year",
                                    source=ASAP + "/" + STATIC[k], method="rangeland-weighted mode of fine cells"))
        lm, et, sz = head_meta(ASAP + "/" + STATIC[k]); add(["static", "", str(f.relative_to(root)), S1.md5(f), STATIC[k], lm, et, sz])

    todo = [d for d in want if a.force or not out_dekad(root, d).exists()]
    log(f"dekads to write: {len(todo)} of {len(want)}")
    if not todo:
        log("ingest done"); return 0
    ref_mask = None
    d0 = todo[0]
    fp, wt, _ = read_window(FPAR_URL.format(d=d0), b)
    a0, _ = rangeland_weighted(fp, w_fine, wt, b, a.rl_min)
    ref_mask = np.isnan(a0)
    if (~ref_mask).sum() == 0:
        raise Stop("no rangeland cell in the window (rl_min too high?)")

    def one(d):
        url = FPAR_URL.format(d=d)
        fp, wt, _ = S1.retry(lambda: read_window(url, b), f"fpar {d}")
        arr, _ = rangeland_weighted(fp, w_fine, wt, b, a.rl_min)
        v = arr[~np.isnan(arr)]
        if v.size and (v.min() < 0 or v.max() > 100):
            raise Stop(f"{d}: FPAR x100 outside 0-100 ({v.min()}..{v.max()})")
        if not np.array_equal(np.isnan(arr), ref_mask):
            raise Stop(f"{d}: NaN mask differs from the reference dekad in {int((np.isnan(arr) != ref_mask).sum())} cell(s)")
        p = out_dekad(root, d)
        write_full(p, arr, b, dict(units="FPAR x 100, rangeland-weighted mean", source=url, dekad=str(d)))
        lm, et, sz = head_meta(url)
        return ["dekad", str(d), str(p.relative_to(root)), S1.md5(p), url, lm, et, sz]

    done = 0; stop = None; tb = time.time()
    ex = ThreadPoolExecutor(max_workers=a.workers)
    futs = {ex.submit(one, d): d for d in todo}
    try:
        for fu in as_completed(futs):
            try:
                add(fu.result()); done += 1
            except Exception as e:
                add(["FAIL", str(futs[fu]), "", "", FPAR_URL.format(d=futs[fu]), "", "", f"{type(e).__name__}: {e}"])
                log(f"FAIL {futs[fu]}: {e}"); stop = stop or e
                ex.shutdown(wait=False, cancel_futures=True)
            if done % 36 == 0:
                log(f"{done}/{len(todo)} dekads ({(time.time() - tb) / max(done, 1):.1f}s/dekad)")
    finally:
        ex.shutdown(wait=True)
    (man_dir / f"fpar_manifest_{stamp}.json").write_text(json.dumps(dict(
        stamp=stamp, args=vars(a), bbox=b, min_age_days=a.min_age, cutoff=str(cutoff), n_dekads=done,
        source=dict(fpar=FPAR_URL, static=STATIC, product="JRC ASAP FPAR (MODIS V062, smoothed ~500 m), rolling NRT; pinned per file by Last-Modified/ETag"),
        licence="dataset licence to be confirmed from the JRC catalogue (doi:10.2905/1aac79d8-0d68-4f1c-a40f-b6e362264e50); the ESSD article is CC-BY-4.0",
        libs=dict(python=sys.version.split()[0], numpy=np.__version__)), indent=1, default=str))
    log(f"{done}/{len(todo)} dekads written; manifest {mcsv.name}")
    if stop:
        raise SystemExit(f"[FPAR] STOPPED: {stop}")
    log("ingest done"); return 0


def cmd_validate(a):
    import rasterio
    root = S1.check_root(a)
    b = S1.snap_bbox(S1.KENYA_SMOKE if a.smoke else S1.AFRICA)
    years = S1.parse_years(a.years); cutoff = dt.date.today() - dt.timedelta(days=a.min_age)
    want = [d for d in dekads(years) if d <= cutoff]
    pres = [d for d in want if out_dekad(root, d).exists()]
    log(f"dekads present {len(pres)}/{len(want)}" + (f" (first missing {next(d for d in want if d not in set(pres))})" if len(pres) < len(want) else ""))
    bad = len(pres) < len(want)
    row0, col0 = S1.lattice_offsets(b); w, h, _ = S1.window_grid(b)
    ref = None; maskbad = 0; means = []
    for d in pres:
        with rasterio.open(out_dekad(root, d)) as r:
            if (r.width, r.height) != (S1.BASE_SHAPE[1], S1.BASE_SHAPE[0]) or not r.transform.almost_equals(S1.lattice_transform()):
                log(f"{d}: GRID MISMATCH"); bad = True; continue
            x = r.read(1, window=((row0, row0 + h), (col0, col0 + w)))
        m = np.isnan(x); v = x[~m]
        if v.size == 0 or v.min() < 0 or v.max() > 100:
            log(f"{d}: RANGE"); bad = True
        if ref is None:
            ref = m
        elif not np.array_equal(m, ref):
            maskbad += 1
        means.append(float(v.mean()) if v.size else np.nan)
    if maskbad:
        log(f"NaN mask differs from the first dekad on {maskbad} dekad(s)"); bad = True
    if means:
        log(f"rangeland FPAR x100 domain mean over dekads: min {np.nanmin(means):.1f} median {np.nanmedian(means):.1f} max {np.nanmax(means):.1f}")
    for k in STATIC:
        f = root / "Data" / "obs_fpar" / "static" / f"{k}.tif"
        if not f.exists():
            log(f"static missing: {k}"); bad = True
    log("VALIDATE " + ("PASS" if not bad else "FAIL"))
    return 0 if not bad else 1


def cmd_selftest(_a):
    def ok(c, m):
        if not c:
            raise AssertionError("SELFTEST FAIL: " + m)
        print(f"  ok   {m}")
    from rasterio.transform import from_origin
    # a 0.25 bbox of 2x2 cells; fine grid 0.05 with a half-pixel offset like ASAP (origin -0.025)
    b = (0.0, 0.0, 0.5, 0.5)
    wt = from_origin(-0.025, 0.525, 0.05, 0.05)        # 11x11 fine cells cover the bbox with partial edge cells
    fp = np.full((11, 11), 50, dtype="uint8"); fp[:, :5] = 80; fp[0, 0] = FPAR_NODATA
    wf = np.zeros((11, 11)); wf[:, :5] = 1.0                              # rangeland = fine cols 0-4 (x < 0.225): west cell only
    arr, den = rangeland_weighted(fp, wf, wt, b, 0.0)
    ok(arr.shape == (2, 2), "output is the 0.25 window (2x2)")
    ok(np.allclose(arr[:, 0], 80, atol=1e-6), "rangeland-weighted mean uses only rangeland cells (west = 80, not the 50 outside)")
    ok(np.isnan(arr[:, 1]).all(), "cells with no rangeland are NaN")
    print("  den =", np.round(den, 4).tolist())
    # cover = 0.225/0.25 = 0.9; the nodata fine cell (0,0) overlaps the top-west cell by 1/4 of a fine cell = 0.01
    ok(abs(den[1, 0] - 0.9) < 1e-6 and abs(den[0, 0] - 0.89) < 1e-6,
       "rangeland fraction is the exact area-weighted cover (0.9; 0.89 where the nodata fine cell is excluded) - half-pixel offset handled")
    ph = np.full((11, 11), 20, dtype="uint8"); ph[:, :3] = 7; ph[:, 3:6] = 9
    md = pheno_mode(ph, wf, wt, b, 255)
    ok(md[0, 0] in (7.0, 9.0) and np.isnan(md[0, 1]), "phenology mode is taken over rangeland cells only and NaN where none")
    ok(len(dekads([2015])) == 36 and dekads([2015])[1] == dt.date(2015, 1, 11), "36 dekads per year on the 1/11/21 convention")
    print("ALL FPAR SELFTEST ASSERTIONS PASSED")
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for n in ("ingest", "validate"):
        s = sub.add_parser(n)
        s.add_argument("--out-root", required=True); s.add_argument("--smoke", action="store_true")
        s.add_argument("--years", default="2005:2025"); s.add_argument("--min-age", type=int, default=90)
        if n == "ingest":
            s.add_argument("--workers", type=int, default=4); s.add_argument("--force", action="store_true")
            s.add_argument("--rl-min", type=float, default=0.0, help="minimum rangeland fraction of a 0.25 cell to keep it (0 = any)")
    sub.add_parser("selftest")
    a = p.parse_args()
    return {"ingest": cmd_ingest, "validate": cmd_validate, "selftest": cmd_selftest}[a.cmd](a) or 0


if __name__ == "__main__":
    sys.exit(main())
