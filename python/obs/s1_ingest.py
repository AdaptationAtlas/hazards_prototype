#!/usr/bin/env python3
"""Observed hazard pipeline, stage S1: ingest daily climate onto the 0.25 deg NEX-GDDP hazard grid.

PLAN_obs_hazard_pipeline.md §1-§2. Reads, crops to Africa and aggregates to 0.25 deg:
  pr      CHIRPS v3 daily rnl   CDH Zarr v3 cube on Hugging Face (pinned revision)   0.05 deg -> exact 5x5 mean
  tasmax  CHIRTS-ERA5 Tmax      CDH Zarr v3 cube on Hugging Face (pinned revision)   0.05 deg -> exact 5x5 mean
  tasmin  CHIRTS-ERA5 Tmin      same cube                                            0.05 deg -> exact 5x5 mean
  rsds    AgERA5 v2.0 SRF       FAO GCS mirror, daily COG, J m-2 d-1 -> MJ m-2 d-1    0.1 deg -> area-average
  hurs    AgERA5 v2.0 RH        --rh mean: mean of RH06/09/12/15/18 (NEX-like daily mean, THI variant i)
                                --rh rh15: RH at 15 LT (~RH at Tmax, THI variant ii), written under <token>-rh15
Outputs are written on the FULL NEX-GDDP lattice (1440x400, -180..180 x -50..50; metadata/base_rast_nexgddp.tif),
NaN outside the Africa box, so R/1-R/3 and base_rast work unchanged. Layout = what the upstream index scripts read
(hazards_upstream/R/04_indices/*.R):

  <out_root>/nex-gddp-cmip6/<var>/historical/<token>/<var>_YYYY-MM-DD.tif

Run the upstream scripts with COMMON_DATA=<out_root> GCMS=<token> DAYS_CHECK=strict AVAIL_SEED_MONTHS=<first year>-01,
after `s1_ingest.py link` has placed chirps_wrld/ and atlas_hazards/soils/ in <out_root>.

Reader rules learned in the S0 probes (DISPATCH_obs_hf_probe.md, DISPATCH_pascal_obs_env.md):
  * read the cubes with xarray (real coordinates). GDAL's ZARR driver opens them with NO georeferencing;
  * xarray applies the int16 scale_factor 0.1: never scale again;
  * index by computed integer positions and assert the coordinates (CHIRPS lon carries float noise);
  * select time by date label (the store's time units are days since 1980-01-01, the series starts 1981).

Invariants enforced while writing (a violation stops the run and is recorded in s1_failed_*.csv):
  * every requested day exists in the source;
  * values inside a plausible range per variable (catches an un-converted J -> MJ, a double scale);
  * the NaN mask of a variable is identical on every day (catches a land chunk the server 404'd, which
    zarr fills with NaN silently; one NaN day would blank a cell's water balance for the rest of the series).

Subcommands:
  link      --out-root DIR --common-data DIR            symlink chirps_wrld/ and atlas_hazards/soils/
  ingest    --out-root DIR [--years 2005:2025] [--vars pr,tasmax,tasmin,rsds] [--rh none|mean|rh15|both] [--smoke] [--force]
  validate  --out-root DIR [--years ...] [--vars ...]   completeness, lattice, ranges, NaN mask, manifest coverage
  selftest                                              synthetic checks, no network

Idempotent and resumable: a day whose file exists AND carries the pinned source tag is skipped; a stale tag is rewritten.
Every log line is timestamped. The manifest is appended as the run goes (one file per process).
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np

# ---- pinned sources -----------------------------------------------------------------------------
HF = "https://huggingface.co/datasets/aaguilar90"
CUBES = {
    "chirts": dict(url=f"{HF}/chirts-era5-daily/resolve/c44a0b58772e32f7cbb2b5a740d5e6586ceeb054/chirts-era5-daily.zarr",
                   revision="c44a0b58772e32f7cbb2b5a740d5e6586ceeb054", lat0=69.975, lon0=-179.975),
    "chirps": dict(url=f"{HF}/chirps-v3-daily-rnl/resolve/e9466d3109c6d087c6cf60c19ca6ae20873d358a/chirps-v3-daily-rnl.zarr",
                   revision="e9466d3109c6d087c6cf60c19ca6ae20873d358a", lat0=59.975, lon0=-179.975),
}
CUBE_VARS = {"pr": ("chirps", "precip"), "tasmax": ("chirts", "Tmax"), "tasmin": ("chirts", "Tmin")}
AGERA5_MAPSETS = {"rsds": ["AGERA5-SRF"], "hurs_mean": ["AGERA5-RH06", "AGERA5-RH09", "AGERA5-RH12", "AGERA5-RH15", "AGERA5-RH18"],
                  "hurs_rh15": ["AGERA5-RH15"]}
AGERA5_VERSION = "v2.0 (FAO GISMGR mirror; archive rewritten 2025-09)"
GCS_BUCKET = "fao-gismgr-c3s-data"
GCS_OBJ = "DATA/C3S/MAPSET/{mapset}/C3S.{mapset}.{date}.tif"
GCS_URL = "https://storage.googleapis.com/" + GCS_BUCKET + "/" + GCS_OBJ
AGERA5_NODATA = -9999.0

# ---- grids ----------------------------------------------------------------------------------------------
RES = 0.25
FINE = 0.05
BASE_ORIGIN = (-180.0, 50.0)             # metadata/base_rast_nexgddp.tif
BASE_SHAPE = (400, 1440)                 # rows, cols
AFRICA = (-18.0, -35.0, 52.0, 38.0)      # xmin, ymin, xmax, ymax (on the lattice)
KENYA_SMOKE = (33.5, -5.0, 42.5, 5.5)

RANGES = {"pr": (0.0, 600.0), "tasmax": (-25.0, 62.0), "tasmin": (-45.0, 45.0), "rsds": (0.0, 40.0), "hurs": (0.0, 100.0)}
UNITS = {"pr": "mm d-1", "tasmax": "degC", "tasmin": "degC", "rsds": "MJ m-2 d-1", "hurs": "%"}
RETRIES = 5

T0 = time.time()
_lock = threading.Lock()


def log(*msg):
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S} | +{time.time() - T0:8.1f}s] [S1] " + " ".join(str(m) for m in msg), flush=True)


class S1Stop(RuntimeError):
    """A per-day invariant failure. Recorded, then the run stops."""


def retry(fn, what, tries=RETRIES):
    for k in range(tries):
        try:
            return fn()
        except S1Stop:
            raise
        except Exception as e:                      # transient HTTP / IO
            if k == tries - 1:
                raise
            wait = min(60, 2 ** k + random.random())
            log(f"retry {k + 1}/{tries - 1} {what}: {type(e).__name__}: {str(e)[:120]} (sleep {wait:.0f}s)")
            time.sleep(wait)


# ---- grid arithmetic (pure; covered by selftest) ---------------------------------------------------
def snap_bbox(b):
    """Snap a bbox outward to the 0.25 deg base lattice."""
    xmin = np.floor(round((b[0] - BASE_ORIGIN[0]) / RES, 6)) * RES + BASE_ORIGIN[0]
    xmax = np.ceil(round((b[2] - BASE_ORIGIN[0]) / RES, 6)) * RES + BASE_ORIGIN[0]
    ymin = BASE_ORIGIN[1] - np.ceil(round((BASE_ORIGIN[1] - b[1]) / RES, 6)) * RES
    ymax = BASE_ORIGIN[1] - np.floor(round((BASE_ORIGIN[1] - b[3]) / RES, 6)) * RES
    return (float(xmin), float(ymin), float(xmax), float(ymax))


def window_grid(b):
    """(width, height, transform) of the 0.25 window covering snapped bbox b."""
    from rasterio.transform import from_origin
    w = int(round((b[2] - b[0]) / RES)); h = int(round((b[3] - b[1]) / RES))
    return w, h, from_origin(b[0], b[3], RES, RES)


def lattice_transform():
    from rasterio.transform import from_origin
    return from_origin(BASE_ORIGIN[0], BASE_ORIGIN[1], RES, RES)


def lattice_offsets(b):
    """(row0, col0) of window b inside the full base lattice."""
    col0 = int(round((b[0] - BASE_ORIGIN[0]) / RES)); row0 = int(round((BASE_ORIGIN[1] - b[3]) / RES))
    assert 0 <= row0 and 0 <= col0, "window outside the base lattice"
    return row0, col0


def place(win_arr, b):
    """Put a window array onto the full lattice (NaN elsewhere)."""
    row0, col0 = lattice_offsets(b)
    full = np.full(BASE_SHAPE, np.nan, dtype="float32")
    h, w = win_arr.shape
    assert row0 + h <= BASE_SHAPE[0] and col0 + w <= BASE_SHAPE[1], "window exceeds the base lattice"
    full[row0:row0 + h, col0:col0 + w] = win_arr
    return full


def cube_index(b, lat0, lon0):
    """Integer [row0,row1) / [col0,col1) of the 0.05 cells exactly covering bbox b in a north->south cube."""
    half = FINE / 2
    col0 = int(round((b[0] + half - lon0) / FINE)); col1 = int(round((b[2] - half - lon0) / FINE)) + 1
    row0 = int(round((lat0 - (b[3] - half)) / FINE)); row1 = int(round((lat0 - (b[1] + half)) / FINE)) + 1
    return row0, row1, col0, col1


def coarsen5(a):
    """Exact 5x5 block mean over the last two axes, ignoring NaN; all-NaN blocks stay NaN."""
    k = int(round(RES / FINE))
    *lead, h, w = a.shape
    assert h % k == 0 and w % k == 0, f"fine block {h}x{w} not a multiple of {k}"
    bb = a.reshape(*lead, h // k, k, w // k, k)
    with np.errstate(invalid="ignore"):
        n = np.sum(~np.isnan(bb), axis=(-3, -1))
        s = np.nansum(bb, axis=(-3, -1))
        out = s / n
    out[n == 0] = np.nan
    return out.astype("float32")


# ---- files ------------------------------------------------------------------------------------------------
def out_path(root: Path, var, token, day):
    return root / "nex-gddp-cmip6" / var / "historical" / token / f"{var}_{day:%Y-%m-%d}.tif"


def read_source_tag(path: Path):
    import rasterio
    try:
        with rasterio.open(path) as r:
            return r.tags().get("source", "")
    except Exception:
        return ""


def needs_write(path: Path, src_note, force):
    """Skip only when the file exists AND was written from the pinned source."""
    if force or not path.exists():
        return True
    return read_source_tag(path) != src_note


def write_day(path: Path, full_arr, var, src_note):
    import rasterio
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tif.part")
    prof = dict(driver="GTiff", width=BASE_SHAPE[1], height=BASE_SHAPE[0], count=1, dtype="float32", crs="EPSG:4326",
                transform=lattice_transform(), nodata=np.nan, compress="LZW", tiled=True, blockxsize=128, blockysize=128)
    with rasterio.open(tmp, "w", **prof) as d:
        d.write(full_arr.astype("float32"), 1)
        d.update_tags(units=UNITS[var], source=src_note, pipeline="hazards_prototype python/obs/s1_ingest.py")
    os.replace(tmp, path)                    # atomic: a killed run never leaves a half-written day that looks complete


def md5(path: Path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def check_day(var, arr, day, ref_mask):
    """Range + NaN-mask invariants on a window array. Raises S1Stop."""
    lo, hi = RANGES[var]
    m = np.isnan(arr)
    v = arr[~m]
    if v.size == 0:
        raise S1Stop(f"{var} {day}: all-NaN day")
    if v.min() < lo or v.max() > hi:
        raise S1Stop(f"{var} {day}: values {v.min():.3f}..{v.max():.3f} outside plausible {lo}..{hi} {UNITS[var]} (unit or scale error?)")
    if ref_mask is not None and not np.array_equal(m, ref_mask):
        n = int((m != ref_mask).sum())
        raise S1Stop(f"{var} {day}: NaN mask differs from the reference day in {n} cell(s) (missing source chunk?)")
    return m


class Manifest:
    """Appended as the run goes, one CSV per process; failures in a sibling file."""
    FIELDS = ["var", "token", "date", "file", "md5", "source", "source_version", "source_md5"]

    def __init__(self, root: Path, args):
        self.dir = root / "manifests"; self.dir.mkdir(parents=True, exist_ok=True)
        self.stamp = f"{dt.datetime.now():%Y%m%d_%H%M%S}_{os.getpid()}"
        self.csv = self.dir / f"s1_manifest_{self.stamp}.csv"
        self.failed = self.dir / f"s1_failed_{self.stamp}.csv"
        self.n = 0; self.nfail = 0
        with open(self.csv, "w", newline="") as fh:
            csv.DictWriter(fh, fieldnames=self.FIELDS).writeheader()
        import rasterio, xarray, zarr
        meta = dict(stamp=self.stamp, started=str(dt.datetime.now()), args=vars(args),
                    cubes={k: dict(url=v["url"], revision=v["revision"]) for k, v in CUBES.items()},
                    agera5=dict(mirror=f"gs://{GCS_BUCKET}/DATA/C3S/MAPSET/", version=AGERA5_VERSION),
                    lattice=dict(origin=BASE_ORIGIN, shape=BASE_SHAPE, res=RES),
                    libs=dict(python=sys.version.split()[0], numpy=np.__version__, xarray=xarray.__version__,
                              zarr=zarr.__version__, rasterio=rasterio.__version__, gdal=rasterio.__gdal_version__),
                    licences={"chirps": "HF card CC-BY-4.0 (redistributor); CHC page states public domain AND CC-BY-4.0",
                              "chirts": "HF card CC-BY-4.0 (redistributor); CHC states none ('experimental')",
                              "agera5": "FAO catalogue CC-BY-SA-4.0; CDS CC-BY (PLAN D1)"})
        (self.dir / f"s1_manifest_{self.stamp}.json").write_text(json.dumps(meta, indent=1, default=str))

    def add(self, row):
        with _lock:
            with open(self.csv, "a", newline="") as fh:
                csv.DictWriter(fh, fieldnames=self.FIELDS).writerow(row)
            self.n += 1

    def fail(self, var, day, why):
        with _lock:
            new = not self.failed.exists()
            with open(self.failed, "a", newline="") as fh:
                w = csv.writer(fh)
                if new:
                    w.writerow(["var", "date", "reason"])
                w.writerow([var, str(day), why])
            self.nfail += 1
        log(f"FAIL {var} {day}: {why}")


# ---- sources ----------------------------------------------------------------------------------------------
def open_cube(key):
    import xarray as xr
    import zarr
    zarr.config.set({"async.concurrency": 48})
    so = {}
    tok = os.environ.get("HF_TOKEN")
    if tok:
        so = dict(headers={"Authorization": f"Bearer {tok}"})
    return retry(lambda: xr.open_zarr(CUBES[key]["url"], chunks=None, storage_options=so or None), f"open cube {key}")


def ingest_cube(var, days, b, root, token, force, man):
    key, name = CUBE_VARS[var]
    c = CUBES[key]
    src_note = f"{key} {name} rev {c['revision']}"
    ds = open_cube(key)
    lat = ds["lat"].values; lon = ds["lon"].values
    assert abs(lat[0] - c["lat0"]) < 1e-6 and lat[1] < lat[0], f"{key}: unexpected lat axis"
    assert abs(lon[0] - c["lon0"]) < 1e-6, f"{key}: unexpected lon origin"
    r0, r1, c0, c1 = cube_index(b, c["lat0"], c["lon0"])
    assert abs(lon[c0] - (b[0] + FINE / 2)) < 1e-6 and abs(lon[c1 - 1] - (b[2] - FINE / 2)) < 1e-6, f"{key}: lon window misaligned"
    assert abs(lat[r0] - (b[3] - FINE / 2)) < 1e-6 and abs(lat[r1 - 1] - (b[1] + FINE / 2)) < 1e-6, f"{key}: lat window misaligned"
    w, h, _ = window_grid(b)
    times = ds["time"].values.astype("datetime64[D]")
    tidx = {d: i for i, d in enumerate(times)}
    missing = [d for d in days if np.datetime64(d, "D") not in tidx]
    if missing:
        raise S1Stop(f"{var}: {len(missing)} requested day(s) absent from the cube (first {missing[0]})")
    todo = [d for d in days if needs_write(out_path(root, var, token, d), src_note, force)]
    log(f"{var}: {len(days)} days requested, {len(todo)} to write (cube {key} rev {c['revision'][:7]}, rows {r0}:{r1} cols {c0}:{c1})")
    if not todo:
        return
    blocks = {}
    for d in todo:
        i = tidx[np.datetime64(d, "D")]
        blocks.setdefault(i // 128, []).append(i)
    order = sorted(blocks)

    def fetch(bi):
        members = blocks[bi]; t_a, t_b = members[0], members[-1] + 1
        arr = retry(lambda: ds[name].isel(time=slice(t_a, t_b), lat=slice(r0, r1), lon=slice(c0, c1)).values, f"{var} block {bi}")
        return t_a, members, coarsen5(arr.astype("float32"))

    ref_mask = None
    with ThreadPoolExecutor(max_workers=1) as pre:            # prefetch the next block while this one is written
        nxt = pre.submit(fetch, order[0])
        for k, bi in enumerate(order):
            tb = time.time()
            t_a, members, coarse = nxt.result()
            if k + 1 < len(order):
                nxt = pre.submit(fetch, order[k + 1])
            assert coarse.shape[1:] == (h, w), f"coarse shape {coarse.shape[1:]} != window {(h, w)}"
            for i in members:
                day = times[i].astype(object)
                a = coarse[i - t_a]
                ref_mask = check_day(var, a, day, ref_mask)
                p = out_path(root, var, token, day)
                write_day(p, place(a, b), var, src_note)
                man.add(dict(var=var, token=token, date=str(day), file=str(p.relative_to(root)), md5=md5(p),
                             source=c["url"], source_version=c["revision"], source_md5=""))
            log(f"{var}: block {bi} days {len(members)} ({times[t_a]}..{times[members[-1]]}) in {time.time() - tb:.1f}s")


def gcs_listing(mapset, years):
    """{date: (generation, md5Hash, size)} for the daily COGs of a mapset (JSON API, paged, retried)."""
    import requests
    out = {}
    for y in years:
        token = None
        while True:
            params = dict(prefix=f"DATA/C3S/MAPSET/{mapset}/C3S.{mapset}.{y}-", maxResults=1000,
                          fields="items(name,generation,md5Hash,size),nextPageToken")
            if token:
                params["pageToken"] = token

            def get():
                r = requests.get(f"https://storage.googleapis.com/storage/v1/b/{GCS_BUCKET}/o", params=params, timeout=60)
                r.raise_for_status(); return r.json()
            j = retry(get, f"gcs list {mapset} {y}")
            for it in j.get("items", []):
                if it["name"].endswith(".tif"):
                    out[it["name"].rsplit(".", 2)[-2]] = (it["generation"], it["md5Hash"], int(it["size"]))
            token = j.get("nextPageToken")
            if not token:
                break
    return out


def read_agera5_window(mapset, day, b, w, h, transform):
    import rasterio
    from rasterio.enums import Resampling
    from rasterio.warp import reproject
    from rasterio.windows import from_bounds
    pad = 0.2
    url = "/vsicurl/" + GCS_URL.format(mapset=mapset, date=f"{day:%Y-%m-%d}")
    with rasterio.Env(GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR", GDAL_HTTP_MAX_RETRY="5", GDAL_HTTP_RETRY_DELAY="2",
                      CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif"):
        with rasterio.open(url) as src:
            win = from_bounds(b[0] - pad, b[1] - pad, b[2] + pad, b[3] + pad, src.transform).round_offsets().round_lengths()
            data = src.read(1, window=win, boundless=True, fill_value=AGERA5_NODATA).astype("float64")
            wt = src.window_transform(win)
            nod = src.nodata if src.nodata is not None else AGERA5_NODATA
    data[data == nod] = np.nan
    data[data <= AGERA5_NODATA + 1] = np.nan
    dst = np.full((h, w), np.nan, dtype="float64")
    import warnings
    from rasterio.errors import NotGeoreferencedWarning
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", NotGeoreferencedWarning)   # bare arrays with explicit transforms: benign
        reproject(data, dst, src_transform=wt, src_crs="EPSG:4326", src_nodata=np.nan,
                  dst_transform=transform, dst_crs="EPSG:4326", dst_nodata=np.nan, resampling=Resampling.average)
    return dst


def ingest_agera5(var, mapsets, days, b, root, token, force, man, workers):
    w, h, transform = window_grid(b)
    src_note = f"AgERA5 {AGERA5_VERSION} {'+'.join(mapsets)}" + (" daily mean of 5 local times" if len(mapsets) > 1 else "")
    todo = [d for d in days if needs_write(out_path(root, var, token, d), src_note, force)]
    log(f"{var}: {len(days)} days requested, {len(todo)} to write ({'+'.join(mapsets)}, FAO GCS mirror, token {token})")
    if not todo:
        return
    years = sorted({d.year for d in todo})
    listing = {m: gcs_listing(m, years) for m in mapsets}
    for m in mapsets:
        absent = [d for d in todo if f"{d:%Y-%m-%d}" not in listing[m]]
        if absent:
            raise S1Stop(f"{var}: {len(absent)} day(s) absent from {m} (first {absent[0]})")

    def one(day):
        acc = None
        for m in mapsets:
            dst = retry(lambda: read_agera5_window(m, day, b, w, h, transform), f"{var} {m} {day}")
            acc = dst if acc is None else acc + dst
        dst = acc / len(mapsets)
        if var == "rsds":
            dst = dst / 1e6                     # J m-2 d-1 -> MJ m-2 d-1 (peest2 expects MJ, as NEX rsds after preprocessing)
        return day, dst.astype("float32")

    # reference mask from the first day, synchronously, before the pool
    d0, a0 = one(todo[0]); ref_mask = check_day(var, a0, d0, None)

    def finish(day, a):
        check_day(var, a, day, ref_mask)
        p = out_path(root, var, token, day)
        write_day(p, place(a, b), var, src_note)
        g, mh, _ = listing[mapsets[0]][f"{day:%Y-%m-%d}"]
        man.add(dict(var=var, token=token, date=str(day), file=str(p.relative_to(root)), md5=md5(p),
                     source=GCS_URL.format(mapset="+".join(mapsets), date=f"{day:%Y-%m-%d}"),
                     source_version=f"gcs-generation:{g}", source_md5=mh))

    finish(d0, a0)
    done = 1; tb = time.time(); stop = None
    ex = ThreadPoolExecutor(max_workers=workers)
    futs = {ex.submit(one, d): d for d in todo[1:]}
    try:
        for f in as_completed(futs):
            day = futs[f]
            try:
                finish(*f.result())
                done += 1
            except Exception as e:
                man.fail(var, day, f"{type(e).__name__}: {e}")
                stop = stop or e
                ex.shutdown(wait=False, cancel_futures=True)   # nothing more starts; in-flight days finish and are recorded
            if done % 200 == 0:
                log(f"{var}: {done}/{len(todo)} days ({(time.time() - tb) / max(done - 1, 1):.2f}s/day)")
    finally:
        ex.shutdown(wait=True)
    log(f"{var}: {done}/{len(todo)} days written")
    if stop:
        raise S1Stop(f"{var}: stopped after a day failed ({stop}); see {man.failed.name}")


# ---- commands ------------------------------------------------------------------------------------------
def parse_years(s):
    if ":" in s:
        a, b2 = s.split(":"); return list(range(int(a), int(b2) + 1))
    return [int(x) for x in s.split(",")]


def days_for(years):
    d = dt.date(min(years), 1, 1); end = dt.date(max(years), 12, 31); ys = set(years); out = []
    while d <= end:
        if d.year in ys:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


def check_root(args):
    root = Path(args.out_root)
    if not root.is_absolute():
        raise SystemExit("[S1] --out-root must be an absolute path (a relative path lands wherever you launched from)")
    if args.smoke and "smoke" not in str(root).lower():
        raise SystemExit("[S1] --smoke output must go to a directory whose path contains 'smoke' (smoke and full runs never share a root)")
    cd = os.environ.get("COMMON_DATA")
    if cd and not args.smoke and not str(root).startswith(str(Path(cd))):
        raise SystemExit(f"[S1] --out-root is not under COMMON_DATA ({cd}); the container / is ephemeral")
    return root


def cmd_link(args):
    root = check_root(args); cd = Path(args.common_data)
    for rel in ("chirps_wrld", "atlas_hazards/soils"):
        src = cd / rel; dst = root / rel
        if not src.exists():
            raise SystemExit(f"[S1] link: {src} does not exist")
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.is_symlink() or dst.exists():
            log(f"link: {dst} already present ({'symlink' if dst.is_symlink() else 'dir'})")
        else:
            os.symlink(src, dst); log(f"link: {dst} -> {src}")
    for f in ("chirps_wrld/chirps-v2.0.1981.01.01.tif", "atlas_hazards/soils/sscp_world.tif", "atlas_hazards/soils/ssat_world.tif"):
        if not (root / f).exists():
            raise SystemExit(f"[S1] link: {root / f} missing after linking")
    log("link done")


def cmd_ingest(args):
    root = check_root(args)
    b = snap_bbox(KENYA_SMOKE if args.smoke else AFRICA)
    days = days_for(parse_years(args.years))
    vars_ = [v for v in args.vars.split(",") if v]
    w, h, _ = window_grid(b)
    log(f"ingest token={args.token} bbox={b} window={w}x{h} on lattice {BASE_SHAPE[1]}x{BASE_SHAPE[0]} "
        f"years={days[0].year}-{days[-1].year} ({len(days)} days) vars={vars_} rh={args.rh} root={root}")
    man = Manifest(root, args)
    rc = 0
    try:
        for v in vars_:
            if v in CUBE_VARS:
                ingest_cube(v, days, b, root, args.token, args.force, man)
            elif v == "rsds":
                ingest_agera5("rsds", AGERA5_MAPSETS["rsds"], days, b, root, args.token, args.force, man, args.workers)
            else:
                raise SystemExit(f"[S1] unknown var {v}")
        if args.rh in ("mean", "both"):
            ingest_agera5("hurs", AGERA5_MAPSETS["hurs_mean"], days, b, root, args.token, args.force, man, args.workers)
        if args.rh in ("rh15", "both"):
            tok2 = f"{args.token}-rh15"
            ingest_agera5("hurs", AGERA5_MAPSETS["hurs_rh15"], days, b, root, tok2, args.force, man, args.workers)
            # THI reads tasmax under the same token: link it
            src = root / "nex-gddp-cmip6" / "tasmax" / "historical" / args.token
            dst = root / "nex-gddp-cmip6" / "tasmax" / "historical" / tok2
            if src.exists() and not (dst.exists() or dst.is_symlink()):
                os.symlink(src, dst); log(f"linked tasmax for {tok2}")
    except S1Stop as e:
        man.fail("run", "-", str(e)); rc = 2
    finally:
        log(f"manifest: {man.csv.name} ({man.n} rows, {man.nfail} failures)")
    log("ingest " + ("done" if rc == 0 else "STOPPED"))
    return rc


def cmd_validate(args):
    import rasterio
    root = check_root(args)
    b = snap_bbox(KENYA_SMOKE if args.smoke else AFRICA)
    days = days_for(parse_years(args.years)); bad = 0
    tmpl = Path(__file__).resolve().parents[2] / "metadata" / "base_rast_nexgddp.tif"
    with rasterio.open(tmpl) as r:
        ok_lat = (r.width, r.height) == (BASE_SHAPE[1], BASE_SHAPE[0]) and r.transform.almost_equals(lattice_transform())
    log(f"lattice equals {tmpl.name}: {ok_lat}"); bad += not ok_lat
    row0, col0 = lattice_offsets(b); w, h, _ = window_grid(b)
    # manifest coverage: every present file must be in some manifest
    rows = set()
    for f in (root / "manifests").glob("s1_manifest_*.csv"):
        with open(f) as fh:
            rows.update(r_["file"] for r_ in csv.DictReader(fh))
    for v in args.vars.split(","):
        pres = [d for d in days if out_path(root, v, args.token, d).exists()]
        miss = len(days) - len(pres)
        log(f"{v}: {len(pres)}/{len(days)} days present" + (f" - MISSING first {next(d for d in days if out_path(root, v, args.token, d) not in [out_path(root, v, args.token, x) for x in pres])}" if miss else ""))
        bad += miss > 0
        unman = [d for d in pres if str(out_path(root, v, args.token, d).relative_to(root)) not in rows]
        if unman:
            log(f"{v}: {len(unman)} file(s) absent from every manifest (first {unman[0]})"); bad += 1
        means = []; ref = None; maskbad = 0; tagbad = 0
        for d in pres:
            p = out_path(root, v, args.token, d)
            with rasterio.open(p) as r:
                if (r.width, r.height) != (BASE_SHAPE[1], BASE_SHAPE[0]) or not r.transform.almost_equals(lattice_transform()) or r.crs.to_epsg() != 4326:
                    log(f"{v} {d}: GRID MISMATCH {r.width}x{r.height} {r.crs}"); bad += 1; continue
                a = r.read(1, window=((row0, row0 + h), (col0, col0 + w)))
                tag = r.tags().get("source", "")
            if not tag:
                tagbad += 1
            lo, hi = RANGES[v]; m = np.isnan(a); vv = a[~m]
            if vv.size == 0 or vv.min() < lo or vv.max() > hi:
                log(f"{v} {d}: RANGE {vv.min() if vv.size else 'nan'}..{vv.max() if vv.size else 'nan'}"); bad += 1
            if ref is None:
                ref = m
            elif not np.array_equal(m, ref):
                maskbad += 1
            means.append(float(vv.mean()) if vv.size else np.nan)
        if maskbad:
            log(f"{v}: NaN mask differs from the first day on {maskbad} day(s)"); bad += 1
        if tagbad:
            log(f"{v}: {tagbad} file(s) without a source tag"); bad += 1
        if means:
            log(f"{v}: domain-mean over days min {np.nanmin(means):.2f} median {np.nanmedian(means):.2f} max {np.nanmax(means):.2f} {UNITS[v]}")
    if {"tasmax", "tasmin"} <= set(args.vars.split(",")):
        viol = 0
        for d in days[::30]:
            px, pn = out_path(root, "tasmax", args.token, d), out_path(root, "tasmin", args.token, d)
            if px.exists() and pn.exists():
                with rasterio.open(px) as rx, rasterio.open(pn) as rn:
                    x, n = rx.read(1), rn.read(1)
                viol += int(np.nansum(x < n - 0.051))
        log(f"tasmax < tasmin (beyond the 0.05 quantum) in sampled days: {viol} cells"); bad += viol > 0
    for f in ("chirps_wrld/chirps-v2.0.1981.01.01.tif", "atlas_hazards/soils/sscp_world.tif", "atlas_hazards/soils/ssat_world.tif"):
        if not (root / f).exists():
            log(f"link missing: {root / f} (run `s1_ingest.py link` before S2)")
    log("VALIDATE " + ("PASS" if bad == 0 else f"FAIL ({bad} problem group(s))"))
    return 0 if bad == 0 else 1


def cmd_selftest(_args):
    def ok(c, m):
        if not c:
            raise AssertionError("SELFTEST FAIL: " + m)
        print(f"  ok   {m}")
    ok(snap_bbox(AFRICA) == AFRICA, "Africa bbox lies on the NEX-GDDP 0.25 lattice")
    ok(snap_bbox((33.6, -4.9, 42.4, 5.4)) == (33.5, -5.0, 42.5, 5.5), "off-grid bbox snaps outward to 0.25")
    w, h, t = window_grid(AFRICA)
    ok((w, h) == (280, 292) and abs(t.c + 18) < 1e-12 and abs(t.f - 38) < 1e-12, "Africa window 280x292 from (-18, 38)")
    ok(lattice_offsets(AFRICA) == (48, 648), "Africa window sits at lattice row 48, col 648")
    full = place(np.ones((h, w), "float32"), AFRICA)
    ok(full.shape == BASE_SHAPE and np.nansum(full) == w * h and np.isnan(full[0, 0]) and full[48, 648] == 1 and full[48 + h - 1, 648 + w - 1] == 1,
       "place() puts the window on the 1440x400 lattice with NaN outside")
    ok(cube_index(AFRICA, 69.975, -179.975) == (640, 2100, 3240, 4640), "CHIRTS Africa window rows 640:2100 cols 3240:4640")
    ok(cube_index(AFRICA, 59.975, -179.975) == (440, 1900, 3240, 4640), "CHIRPS Africa window rows 440:1900 cols 3240:4640")
    r0, r1, c0, c1 = cube_index(AFRICA, 69.975, -179.975)
    ok((r1 - r0) == 5 * 292 and (c1 - c0) == 5 * 280, "fine window is exactly 5x the target window")
    a = np.arange(100, dtype="float32").reshape(10, 10)
    ok(np.allclose(coarsen5(a), [[a[:5, :5].mean(), a[:5, 5:].mean()], [a[5:, :5].mean(), a[5:, 5:].mean()]]), "coarsen5 = block mean")
    a[:5, :5] = np.nan; a[5, 5] = np.nan
    c = coarsen5(a)
    ok(np.isnan(c[0, 0]) and abs(c[1, 1] - np.nanmean(a[5:, 5:])) < 1e-5, "all-NaN block -> NaN; partial block -> mean of valid")
    ok(coarsen5(np.ones((3, 10, 10), "float32")).shape == (3, 2, 2), "coarsen5 keeps the leading time axis")
    ok(RANGES["rsds"][0] <= 2e7 / 1e6 <= RANGES["rsds"][1], "2e7 J m-2 d-1 -> 20 MJ is in range")
    try:
        check_day("rsds", np.array([[2e7]], "float32"), "selftest", None); ok(False, "unconverted J must fail")
    except S1Stop:
        ok(True, "unconverted J (2e7) fails the rsds range check")
    m0 = check_day("tasmax", np.array([[30, np.nan], [31, 32]], "float32"), "d0", None)
    try:
        check_day("tasmax", np.array([[30, 31], [np.nan, 32]], "float32"), "d1", m0); ok(False, "mask drift must fail")
    except S1Stop:
        ok(True, "a NaN-mask change between days fails")
    ok(check_day("tasmax", np.array([[29, np.nan], [30, 33]], "float32"), "d2", m0) is not None, "same mask passes")
    from rasterio.enums import Resampling
    from rasterio.transform import from_origin
    from rasterio.warp import reproject
    src = np.full((5, 5), 20e6); src[0, 0] = AGERA5_NODATA
    s = src.copy(); s[s == AGERA5_NODATA] = np.nan
    dst = np.full((2, 2), np.nan)
    reproject(s, dst, src_transform=from_origin(0, 0.5, 0.1, 0.1), src_crs="EPSG:4326", src_nodata=np.nan,
              dst_transform=from_origin(0, 0.5, 0.25, 0.25), dst_crs="EPSG:4326", dst_nodata=np.nan, resampling=Resampling.average)
    ok(np.nanmin(dst) > 19e6, "a -9999 cell is excluded from the 0.1->0.25 average (no ~-400 contamination)")
    # revision-aware skip on a real temp file
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "x.tif"; write_day(p, place(np.ones((h, w), "float32"), AFRICA), "tasmax", "pinned")
        ok(not needs_write(p, "pinned", False) and needs_write(p, "other-rev", False) and needs_write(p, "pinned", True),
           "skip only when the file's source tag matches the pin; --force always rewrites")
    print("ALL S1 SELFTEST ASSERTIONS PASSED")
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("ingest", "validate", "link"):
        s = sub.add_parser(name)
        s.add_argument("--out-root", required=True, help="absolute; under COMMON_DATA on a node")
        s.add_argument("--token", default="chirps-chirts-agera5")
        s.add_argument("--smoke", action="store_true", help="Kenya box only; out-root must contain 'smoke'")
        if name != "link":
            s.add_argument("--years", default="2005:2025")
            s.add_argument("--vars", default="pr,tasmax,tasmin,rsds")
        if name == "ingest":
            s.add_argument("--rh", choices=["none", "mean", "rh15", "both"], default="none",
                           help="AgERA5 RH as hurs: mean of 5 local times (THI variant i, main token) and/or RH15 (variant ii, <token>-rh15)")
            s.add_argument("--workers", type=int, default=8, help="parallel AgERA5 day reads")
            s.add_argument("--force", action="store_true")
        if name == "link":
            s.add_argument("--common-data", required=True, help="the node's common_data root holding chirps_wrld/ and atlas_hazards/soils/")
    sub.add_parser("selftest")
    a = p.parse_args()
    return {"ingest": cmd_ingest, "validate": cmd_validate, "link": cmd_link, "selftest": cmd_selftest}[a.cmd](a) or 0


if __name__ == "__main__":
    sys.exit(main())
