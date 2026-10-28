#!/usr/bin/env python3
"""Rangeland vegetation ingest (PLAN_obs_hazard_pipeline.md §3, decision D4): JRC ASAP FPAR dekads -> 0.25 deg
rangeland-weighted means on the NEX-GDDP lattice, plus the static rangeland fraction and phenology.

Source (anonymous HTTPS, checked 2026-10-27):
  https://agricultural-production-hotspots.ec.europa.eu/data/indicators_fpar/fpar/fpar_YYYYMMDD.tif
    global 80640x29346, 0.004464 deg, Byte 0-100 (FPAR x 100; 255 = nodata), DEFLATE, 256x256 tiles,
    dekads 2000-05-21 -> present. "Terra/Aqua MODIS, FPAR, Smoothed ~500m, Product-version=V062".
    It is a rolling NRT archive: a dekad is re-estimated until consolidated (~90 days), so only dekads
    at least --min-age days old are ingested (default 110: final CF rewrites were measured 102-129 d after the dekad
    start; a dekad whose consolidation_period tag is not CF stops the run), and every file's Last-Modified / ETag /
    size and the ASAP Product-version / consolidation_period / file_creation tags go in the manifest.
  https://agricultural-production-hotspots.ec.europa.eu/files/asap_mask_rangeland_v04.tif   Byte 0-100 = % rangeland
  https://agricultural-production-hotspots.ec.europa.eu/files/pheno{s1,e1,m1,s2,e2,m2}_v05.tif  season start/end/max
    as dekad index in a 1..108 three-year frame (37-72 = year 2, 73-108 = year 3), 255 = nodata;  phenonseasons_v05.tif 1|2, 0 = nodata.

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

Subcommands:  ingest --out-root DIR [--years 2005:2025] [--smoke] [--workers 4] [--min-age 110] [--force] [--rl-min 0]
              validate --out-root DIR [--years ...] [--smoke]
              selftest
"""
from __future__ import annotations

import argparse
import csv
import re
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


S1.log = log   # S1's retry() logs through its module-level log; make those lines say [FPAR]


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
            return a, src.window_transform(win), src.nodata, dict(src.tags())


ASAP_TAGS = ("Product-version", "consolidation_period", "file_creation", "description")


def asap_meta(tags):
    """The ASAP TIFF tags the baseline depends on. The operational archive is two product versions spliced
    (measured 2026-10-27: V062 Terra/Aqua MODIS to 2018-03-21, V070 MODIS/VIIRS from 2018-04-01; the phenology
    and all dekads carry Product-version / consolidation_period / file_creation), so they go in every file and row."""
    out = {k: str(tags.get(k, "")) for k in ASAP_TAGS}
    if not out["Product-version"]:                       # measured 2026-10-27: the version is only inside description
        m = re.search(r"Product-version=([A-Za-z0-9_.]+)", out["description"])
        out["Product-version"] = m.group(1) if m else ""
    return out


def area_average(arr, wt, b):
    """Area-weighted mean of a float array (NaN = skip) onto the 0.25 window of bbox b."""
    import warnings
    from rasterio.enums import Resampling
    from rasterio.errors import NotGeoreferencedWarning
    from rasterio.warp import reproject
    w, h, transform = S1.window_grid(b)
    dst = np.full((h, w), np.nan, dtype="float32")        # float32 end to end: values are 0-100 means, and a full-Africa fine window is 257 M cells
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", NotGeoreferencedWarning)
        reproject(np.ascontiguousarray(arr, dtype="float32"), dst, src_transform=wt, src_crs="EPSG:4326", src_nodata=np.nan,
                  dst_transform=transform, dst_crs="EPSG:4326", dst_nodata=np.nan, resampling=Resampling.average)
    return dst


def rangeland_weighted(fpar_b, w_fine, wt, b, rl_min):
    """fpar_rl = avg(fpar*w) / avg(w) over the 0.25 cell, with w = rangeland fraction where FPAR is valid."""
    valid = fpar_b <= 100            # 251 = other land, 254 = water, 255 = nodata (Seguini et al. 2026 Table 3) are all invalid
    w = np.where(valid, w_fine, np.float32(0))
    num = area_average(np.where(valid, fpar_b.astype("float32") * w, np.float32(0)), wt, b)
    den = area_average(w, wt, b)
    with np.errstate(invalid="ignore", divide="ignore"):
        out = num / den
    out[~(den > rl_min)] = np.nan            # no (or too little) rangeland in the cell
    return out.astype("float32"), den.astype("float32")


def joint_mode(key_fine, w_fine, wt, b, strip_rows=16):
    """Rangeland-weighted mode of an integer key per 0.25 cell. Each fine cell is assigned to the coarse cell holding
    its centre (the ASAP grid is half a fine pixel off the lattice: at most one fine row/column per cell edge is
    attributed to a neighbour, immaterial for a mode). key < 0 = invalid. Returns float64, NaN where no valid
    rangeland cell. Works in strips of `strip_rows` coarse rows (every fine row belongs to exactly one strip, so a
    cell is never split across two) to keep memory flat on a full-Africa window."""
    w, h, _ = S1.window_grid(b)
    out = np.full((h, w), np.nan, dtype="float64")
    H = key_fine.shape[0]
    frow = np.floor((b[3] - (wt.f + (np.arange(H) + 0.5) * wt.e)) / S1.RES).astype("int64")   # coarse row of every fine row
    for r0 in range(0, h, strip_rows):
        rows = np.nonzero((frow >= r0) & (frow < r0 + strip_rows))[0]
        if rows.size == 0:
            continue
        i0, i1 = int(rows[0]), int(rows[-1]) + 1
        kf = key_fine[i0:i1]; wf = w_fine[i0:i1]
        sel = (wf > 0) & (kf >= 0)
        ii, jj = np.nonzero(sel)
        if ii.size == 0:
            continue
        xc = wt.c + (jj + 0.5) * wt.a; yc = wt.f + (ii + i0 + 0.5) * wt.e
        col = np.floor((xc - b[0]) / S1.RES).astype("int64"); row = np.floor((b[3] - yc) / S1.RES).astype("int64")
        inside = (col >= 0) & (col < w) & (row >= 0) & (row < h)
        cell = (row * w + col)[inside]; key = kf[ii[inside], jj[inside]].astype("int64"); wgt = wf[ii[inside], jj[inside]].astype("float64")
        ukey, kidx = np.unique(key, return_inverse=True)
        comp = cell * len(ukey) + kidx
        ucomp, cidx = np.unique(comp, return_inverse=True)
        wsum = np.bincount(cidx, weights=wgt, minlength=len(ucomp))
        ccell = ucomp // len(ukey); ckey = ukey[ucomp % len(ukey)]
        order = np.lexsort((-wsum, ccell))                  # per cell, heaviest key first; ties -> smallest key
        _, first = np.unique(ccell[order], return_index=True)
        out.flat[ccell[order][first]] = ckey[order][first]
    return out


def pheno_mode(ph_fine, w_fine, wt, b, nodata):
    """Rangeland-weighted mode of one layer (joint_mode of a single key)."""
    k = ph_fine.astype("int64"); k[ph_fine == nodata] = -1
    return joint_mode(k, w_fine, wt, b).astype("float32")


PHENO_KEYS = ("pheno_s1", "pheno_e1", "pheno_s2", "pheno_e2", "pheno_n")


def pheno_joint_mode(ph, w_fine, wt, b, nod):
    """Joint rangeland-weighted mode of the phenology TUPLE (s1, e1, s2, e2, n): the winning tuple comes from one
    pixel population, so s1/e1/s2/e2 are never paired across populations (independent per-layer modes can pair an
    OND-first s1 with a MAM-first e1 in cells that mix the two encodings - the Kenya pastoral geography).
    s2/e2 are 0 in the key where n == 1. Returns {name: float32 array}, NaN where no valid rangeland cell, and
    s2/e2 NaN where n == 1."""
    s1, e1, s2, e2, n = (ph[k] for k in PHENO_KEYS)
    ok1 = (s1 != nod["pheno_s1"]) & (e1 != nod["pheno_e1"]) & (n != nod["pheno_n"]) & (n >= 1)
    two = ok1 & (n >= 2)
    ok = ok1 & (~two | ((s2 != nod["pheno_s2"]) & (e2 != nod["pheno_e2"])))
    s2k = np.where(two, s2, 0).astype("int64"); e2k = np.where(two, e2, 0).astype("int64")
    key = (((s1.astype("int64") * 128 + e1.astype("int64")) * 128 + s2k) * 128 + e2k) * 4 + n.astype("int64")   # fields < 128 (1-108 frame), n < 4
    key = np.where(ok, key, -1)
    mode = joint_mode(key, w_fine, wt, b)
    out = {}
    k = mode.copy(); nn = np.where(np.isnan(k), np.nan, k % 4); k = np.floor_divide(k, 4)
    e2m = k % 128; k = np.floor_divide(k, 128); s2m = k % 128; k = np.floor_divide(k, 128); e1m = k % 128; s1m = np.floor_divide(k, 128)
    out["pheno_s1"], out["pheno_e1"], out["pheno_n"] = s1m, e1m, nn
    out["pheno_s2"] = np.where(nn >= 2, s2m, np.nan); out["pheno_e2"] = np.where(nn >= 2, e2m, np.nan)
    return {k2: v.astype("float32") for k2, v in out.items()}


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


def src_tag(url, lm, et):
    return f"{url} lm={lm} etag={et}"


def needs_write(path: Path, tag, force):
    """Skip only when the file exists AND its source tag (url + Last-Modified + ETag) matches the server's now."""
    if force or not path.exists():
        return True
    return S1.read_source_tag(path) != tag


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
        csv.writer(fh).writerow(["kind", "date", "file", "md5", "source", "last_modified", "etag", "size", "version", "consolidation", "created"])

    def add(row):
        with S1._lock:
            with open(mcsv, "a", newline="") as fh:
                csv.writer(fh).writerow(row)

    # static layers first: rangeland mask (weights) and phenology
    log("static: rangeland mask")
    m_fine, wt, _, _ = read_window(ASAP + "/" + STATIC["rl_frac"], b)
    w_fine = (np.clip(m_fine.astype("float32"), 0, 100) / np.float32(100)).astype("float32")
    sdir = root / "Data" / "obs_fpar" / "static"
    rl = area_average(w_fine, wt, b).astype("float32")
    f = sdir / "rl_frac.tif"; u = ASAP + "/" + STATIC["rl_frac"]; lm, et, sz = head_meta(u)
    if needs_write(f, src_tag(u, lm, et), a.force):
        write_full(f, rl, b, dict(units="fraction", source=src_tag(u, lm, et)))
        add(["static", "", str(f.relative_to(root)), S1.md5(f), STATIC["rl_frac"], lm, et, sz, "", "", ""])
    pmeta = {k: head_meta(ASAP + "/" + STATIC[k]) for k in PHENO_KEYS}
    ptag = {k: src_tag(ASAP + "/" + STATIC[k], pmeta[k][0], pmeta[k][1]) for k in PHENO_KEYS}
    if any(needs_write(sdir / f"{k}.tif", ptag[k], a.force) for k in PHENO_KEYS):
        log("static: phenology (joint mode of the s1/e1/s2/e2/n tuple)")
        ph, nod, ptags = {}, {}, {}
        for k in PHENO_KEYS:
            ph[k], wt2, nd, ptags[k] = read_window(ASAP + "/" + STATIC[k], b)
            assert ph[k].shape == w_fine.shape and wt2 == wt, f"{k}: window shape/transform differs from the mask window"
            nod[k] = 0 if k == "pheno_n" else (nd if nd is not None else 255)
        modes = pheno_joint_mode(ph, w_fine, wt, b, nod)
        # the v05 phenology is in a 1-108 three-year frame (dekad 37-72 = year 2, 73-108 = year 3); where season 2 starts
        # in a later frame-year than season 1 ("OND-first" cells) S6 pairs it with the following calendar year
        s1m, s2m = modes["pheno_s1"], modes["pheno_s2"]
        cross = int(np.sum(~np.isnan(s2m) & (np.floor((s1m - 1) / 36) != np.floor((s2m - 1) / 36))))
        log(f"phenology: {int(np.sum(~np.isnan(s1m)))} rangeland cells with a season; {int(np.sum(modes['pheno_n'] >= 2))} with two; "
            f"s1 range {np.nanmin(s1m):.0f}-{np.nanmax(s1m):.0f}; OND-first cells (s2 in a later frame-year than s1; S6 pairs them with the next calendar year): {cross}")
        for k in PHENO_KEYS:
            f = sdir / f"{k}.tif"; lm, et, sz = pmeta[k]; am = asap_meta(ptags[k])
            write_full(f, modes[k], b, dict(units="dekad index, 1-108 three-year frame (37-72 = year 2)" if k != "pheno_n" else "seasons per year",
                                            source=ptag[k], method="rangeland-weighted joint mode of the (s1,e1,s2,e2,n) tuple over fine cells",
                                            **{"asap_" + t: v for t, v in am.items()}))
            add(["static", "", str(f.relative_to(root)), S1.md5(f), STATIC[k], lm, et, sz, am["Product-version"], am["consolidation_period"], am["file_creation"]])

    # HEAD every wanted dekad (cheap): the tag decides skip vs rewrite, so a reprocessed ASAP file is picked up on resume
    meta = {}
    with ThreadPoolExecutor(max_workers=8) as hx:
        for d, m in zip(want, hx.map(lambda d: head_meta(FPAR_URL.format(d=d)), want)):
            meta[d] = m
    todo = [d for d in want if needs_write(out_dekad(root, d), src_tag(FPAR_URL.format(d=d), meta[d][0], meta[d][1]), a.force)]
    log(f"dekads to write: {len(todo)} of {len(want)} (the rest exist with a matching source tag)")
    failed = man_dir / f"fpar_failed_{stamp}.csv"

    def fail(d, why):
        with S1._lock:
            new = not failed.exists()
            with open(failed, "a", newline="") as fh:
                wr = csv.writer(fh)
                if new:
                    wr.writerow(["date", "reason"])
                wr.writerow([str(d), why])
        log(f"FAIL {d}: {why}")

    if not todo:
        log("ingest done"); return 0
    d0 = todo[0]
    fp, wt0, _, _ = read_window(FPAR_URL.format(d=d0), b)
    assert fp.shape == w_fine.shape and wt0 == wt, "FPAR window shape/transform differs from the mask window"
    a0, _ = rangeland_weighted(fp, w_fine, wt0, b, a.rl_min)
    ref_mask = np.isnan(a0)
    if (~ref_mask).sum() == 0:
        raise Stop("no rangeland cell in the window (rl_min too high?)")

    def one(d):
        url = FPAR_URL.format(d=d)
        fp, wtd, _, tags = S1.retry(lambda: read_window(url, b), f"fpar {d}")
        if fp.shape != w_fine.shape or wtd != wt:
            raise Stop(f"{d}: window shape/transform differs from the mask window")
        am = asap_meta(tags)
        if am["consolidation_period"] and am["consolidation_period"] != "CF":
            raise Stop(f"{d}: consolidation_period={am['consolidation_period']} (not final; CF lands 102-129 d after the dekad start - raise --min-age or wait)")
        arr, _ = rangeland_weighted(fp, w_fine, wtd, b, a.rl_min)
        v = arr[~np.isnan(arr)]
        if v.size and (v.min() < 0 or v.max() > 100):
            raise Stop(f"{d}: FPAR x100 outside 0-100 ({v.min()}..{v.max()})")
        if not np.array_equal(np.isnan(arr), ref_mask):
            raise Stop(f"{d}: NaN mask differs from the reference dekad in {int((np.isnan(arr) != ref_mask).sum())} cell(s)")
        lm, et, sz = meta[d]
        p = out_dekad(root, d)
        write_full(p, arr, b, dict(units="FPAR x 100, rangeland-weighted mean", source=src_tag(url, lm, et), dekad=str(d),
                                   **{"asap_" + t: v for t, v in am.items()}))
        return ["dekad", str(d), str(p.relative_to(root)), S1.md5(p), url, lm, et, sz, am["Product-version"], am["consolidation_period"], am["file_creation"]]

    done = 0; stop = None; tb = time.time()
    ex = ThreadPoolExecutor(max_workers=a.workers)
    futs = {ex.submit(one, d): d for d in todo}
    try:
        pending = list(futs)
        while pending:
            for fu in as_completed(pending):       # see s1_ingest: never as_completed() over cancelled futures
                try:
                    add(fu.result()); done += 1
                except Exception as e:
                    fail(futs[fu], f"{type(e).__name__}: {e}"); stop = stop or e
                    ex.shutdown(wait=False, cancel_futures=True)
                    pending = [g for g in futs if not g.done() and not g.cancelled()]
                    break
                if done % 36 == 0:
                    log(f"{done}/{len(todo)} dekads ({(time.time() - tb) / max(done, 1):.1f}s/dekad)")
            else:
                pending = []
    finally:
        ex.shutdown(wait=True)
    (man_dir / f"fpar_manifest_{stamp}.json").write_text(json.dumps(dict(
        stamp=stamp, args=vars(a), bbox=b, min_age_days=a.min_age, cutoff=str(cutoff), n_dekads=done,
        source=dict(fpar=FPAR_URL, static=STATIC, product="JRC ASAP FPAR (MODIS V062, smoothed ~500 m), rolling NRT; pinned per file by Last-Modified/ETag"),
        licence="dataset licence to be confirmed from the JRC catalogue (doi:10.2905/1aac79d8-0d68-4f1c-a40f-b6e362264e50); the ESSD article is CC-BY-4.0",
        libs=dict(python=sys.version.split()[0], numpy=np.__version__)), indent=1, default=str))
    log(f"{done}/{len(todo)} dekads written; manifest {mcsv.name}")
    if stop:
        raise SystemExit(f"[FPAR] STOPPED: {stop} (see {failed.name})")
    log("ingest done"); return 0


def cmd_validate(a):
    import rasterio
    root = S1.check_root(a)
    b = S1.snap_bbox(S1.KENYA_SMOKE if a.smoke else S1.AFRICA)
    years = S1.parse_years(a.years)
    # the ingest's cutoff is in its manifest json; validating k days later must not expect dekads it never wrote
    mj = sorted((root / "manifests").glob("fpar_manifest_*.json"))
    cuts = [json.loads(m.read_text()).get("cutoff") for m in mj]
    cutoff = dt.date.fromisoformat(a.cutoff) if a.cutoff else (dt.date.fromisoformat(max(c for c in cuts if c)) if any(cuts) else dt.date.today() - dt.timedelta(days=a.min_age))
    want = [d for d in dekads(years) if d <= cutoff]
    later = [d for d in dekads(years) if cutoff < d <= dt.date.today() - dt.timedelta(days=a.min_age)]
    pres = [d for d in want if out_dekad(root, d).exists()]
    log(f"cutoff {cutoff} (from {'--cutoff' if a.cutoff else 'the manifests' if any(cuts) else 'today'}); dekads present {len(pres)}/{len(want)}"
        + (f" (first missing {next(d for d in want if d not in set(pres))})" if len(pres) < len(want) else "")
        + (f"; {len(later)} newer dekad(s) now eligible, not expected" if later else ""))
    bad = len(pres) < len(want)
    rows = set(); fails = {}
    for m in (root / "manifests").glob("fpar_manifest_*.csv"):
        with open(m) as fh:
            for r in csv.DictReader(fh):
                if r["kind"] == "dekad":
                    rows.add(r["file"])
    for m in (root / "manifests").glob("fpar_failed_*.csv"):
        with open(m) as fh:
            for r in csv.DictReader(fh):
                fails[r["date"]] = r["reason"]
    # product-version split and the CF gate, from the manifests (the baseline is two spliced ASAP versions)
    vers = {}
    for m in (root / "manifests").glob("fpar_manifest_*.csv"):
        with open(m) as fh:
            for r in csv.DictReader(fh):
                if r["kind"] == "dekad" and r.get("version") is not None:
                    vers[r["date"]] = (r.get("version", ""), r.get("consolidation", ""))
    if vers:
        from collections import Counter
        cv = Counter(v for v, _ in vers.values()); dates = sorted(vers)
        brk = [d for i, d in enumerate(dates) if i and vers[d][0] != vers[dates[i - 1]][0]]
        notcf = [d for d in dates if vers[d][1] and vers[d][1] != "CF"]
        log(f"product versions: {dict(cv)}; version break(s) at {brk or 'none'}; non-CF dekads: {len(notcf)}")
        if notcf:
            log(f"non-CF dekads in the manifest: {notcf[:5]}"); bad = True
    if not a.no_head:
        from concurrent.futures import ThreadPoolExecutor
        ref = {}
        for m in (root / "manifests").glob("fpar_manifest_*.csv"):
            with open(m) as fh:
                for r in csv.DictReader(fh):
                    if r["kind"] == "dekad":
                        ref[r["date"]] = (r["last_modified"], r["etag"])
        chk = [d for d in pres if str(d) in ref]
        with ThreadPoolExecutor(max_workers=8) as hx:
            heads = list(hx.map(lambda d: head_meta(FPAR_URL.format(d=d)), chk))
        drift = [d for d, (lm, et, _) in zip(chk, heads) if (lm, et) != ref[str(d)]]
        log(f"server drift: {len(drift)} of {len(chk)} present dekads were modified upstream since ingest" + (f" (first {drift[0]}; re-run ingest to rewrite them)" if drift else ""))
        if drift:
            bad = True
    unman = [d for d in pres if str(out_dekad(root, d).relative_to(root)) not in rows]
    if unman:
        log(f"{len(unman)} present dekad(s) absent from every manifest (first {unman[0]})"); bad = True
    open_fail = [d for d in fails if dt.date.fromisoformat(d) in set(want) and str(out_dekad(root, dt.date.fromisoformat(d)).relative_to(root)) not in rows]
    if open_fail:
        log(f"{len(open_fail)} failed dekad(s) never succeeded later: {open_fail[:5]}"); bad = True
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
    # joint mode: three pixel populations A (s1 30, e1 35) 40 %, B (45, 50) 35 %, C (30, 50) 25 % of the rangeland weight.
    # Independent per-layer modes give s1 = 30 (65 %), e1 = 50 (60 %) = population C's tuple (a minority); the joint
    # mode must return A's.
    nr = int(wf.shape[0]); rows_a = slice(0, int(round(nr * 0.40))); rows_b = slice(rows_a.stop, rows_a.stop + int(round(nr * 0.35)))
    s1 = np.full(wf.shape, 30, "uint8"); e1 = np.full(wf.shape, 50, "uint8"); e1[rows_a] = 35; s1[rows_b] = 45
    n1 = np.ones(wf.shape, "uint8"); z = np.full(wf.shape, 255, "uint8")
    jm = pheno_joint_mode(dict(pheno_s1=s1, pheno_e1=e1, pheno_s2=z, pheno_e2=z, pheno_n=n1), wf, wt, b,
                          dict(pheno_s1=255, pheno_e1=255, pheno_s2=255, pheno_e2=255, pheno_n=0))
    ok(jm["pheno_s1"][0, 0] == 30 and jm["pheno_e1"][0, 0] == 35 and jm["pheno_n"][0, 0] == 1 and np.isnan(jm["pheno_s2"][0, 0]),
       "joint phenology mode returns one population's tuple (30, 35), not the per-layer modes' chimera (30, 50)")
    k1 = ph.astype("int64"); k1[ph == 255] = -1
    ok(np.array_equal(joint_mode(k1, wf, wt, b, strip_rows=1), joint_mode(k1, wf, wt, b, strip_rows=64), equal_nan=True),
       "strip-wise joint mode (1 coarse row per strip) equals the single-pass result")
    fv = np.full((11, 11), 80, "uint8"); fv[:, :2] = 254                                      # water flag inside the rangeland strip
    av, _ = rangeland_weighted(fv, wf, wt, b, 0.0)
    ok(np.allclose(av[~np.isnan(av)], 80), "flag values 251/254 are excluded from the rangeland mean like 255")
    am = asap_meta({"description": "Terra/Aqua MODIS, FPAR, Smoothed ~500m, Product-version=V062", "consolidation_period": "CF", "file_creation": "2023-09-09T03:44:35"})
    ok(am["Product-version"] == "V062" and am["consolidation_period"] == "CF", "ASAP product version is parsed out of the description tag (it is not a tag of its own)")
    ok(len(dekads([2015])) == 36 and dekads([2015])[1] == dt.date(2015, 1, 11), "36 dekads per year on the 1/11/21 convention")
    print("ALL FPAR SELFTEST ASSERTIONS PASSED")
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for n in ("ingest", "validate"):
        s = sub.add_parser(n)
        s.add_argument("--out-root", required=True); s.add_argument("--smoke", action="store_true")
        s.add_argument("--years", default="2005:2025"); s.add_argument("--min-age", type=int, default=110)
        if n == "validate":
            s.add_argument("--cutoff", default=None, help="YYYY-MM-DD; default = the newest ingest manifest's cutoff")
            s.add_argument("--no-head", action="store_true", help="skip the per-dekad HEAD drift check against the server")
        if n == "ingest":
            s.add_argument("--workers", type=int, default=4); s.add_argument("--force", action="store_true")
            s.add_argument("--rl-min", type=float, default=0.0, help="minimum rangeland fraction of a 0.25 cell to keep it (0 = any)")
    sub.add_parser("selftest")
    a = p.parse_args()
    return {"ingest": cmd_ingest, "validate": cmd_validate, "selftest": cmd_selftest}[a.cmd](a) or 0


if __name__ == "__main__":
    sys.exit(main())
