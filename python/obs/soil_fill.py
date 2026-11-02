#!/usr/bin/env python3
"""Observed pipeline: fill the coverage holes of the shared soil rasters for the obs tree only.

Why. NDWS / NDWL0 (hazards_upstream/R/04_indices/fast_calc_*.R) read
<COMMON_DATA>/atlas_hazards/soils/{sscp,ssat}_world.tif and resample them onto the climate grid. Those
rasters are NA on many coastal, lakeshore and valley land cells, so the water-balance indices, the crop
bundles and every NDWS/NDWL0-based exposure total lose that exposure (full-Africa obs run 2026-11-02:
1,338 crop x country pairs short, median coverage 0.956; the live NEX product has the identical gap,
hidden there by its soil-free PTOT set). Decided by Pete 2026-11-02: fill for the observed product,
document it, and investigate better soil sources separately (#56).

What. For each soil layer present (sscp, ssat, sroot), NA cells are filled by GDAL FillNodata
(inverse-distance interpolation from the nearest valid cells, no smoothing) up to --max-dist-deg
(default 0.5 deg); cells farther from any valid soil stay NA (open ocean, deserts with no soil data).
The source rasters are read-only. Outputs replace the obs tree's atlas_hazards/soils SYMLINK with a real
directory of filled copies, each tagged with the method, distance and source path + md5.

Usage:
  soil_fill.py fill --src-dir <common_data>/atlas_hazards/soils --out-root <obs root> [--max-dist-deg 0.5]
                    [--check-pr <obs root>/nex-gddp-cmip6/historical/chirps-chirts-agera5/pr/pr_2015-06-15.tif]
  soil_fill.py selftest
"""
import argparse, datetime as dt, hashlib, os, shutil, sys, tempfile, time
from pathlib import Path

import numpy as np

T0 = time.time()
LAYERS = ("sscp_world.tif", "ssat_world.tif", "sroot_world.tif")
REQUIRED = ("sscp_world.tif", "ssat_world.tif")


def log(*m):
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S} | +{time.time() - T0:7.1f}s] [SOIL] " + " ".join(str(x) for x in m), flush=True)


def md5(p: Path):
    h = hashlib.md5()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def fill_array(a, valid, max_px):
    """Fill invalid cells of float array `a` from valid ones (GDAL FillNodata, IDW, no smoothing) within max_px."""
    from rasterio.fill import fillnodata
    out = np.where(valid, a, 0).astype("float32")
    filled = fillnodata(out, mask=valid.astype("uint8"), max_search_distance=float(max_px), smoothing_iterations=0)
    # fillnodata leaves unreachable cells at their input value (0 here): restore them to invalid
    reach = fillnodata(valid.astype("float32"), mask=valid.astype("uint8"), max_search_distance=float(max_px), smoothing_iterations=0) > 0.5
    return filled, reach


def fill_layer(src: Path, dst: Path, max_deg: float):
    import rasterio
    with rasterio.open(src) as s:
        a = s.read(1).astype("float64"); prof = s.profile.copy(); nd = s.nodata; res = abs(s.transform.a)
    valid = np.isfinite(a) & ((a != nd) if nd is not None else True)
    max_px = max(1, int(round(max_deg / res)))
    filled, reach = fill_array(a, valid, max_px)
    newly = reach & ~valid
    out_nd = nd if nd is not None else np.nan
    res_arr = np.where(valid, a, np.where(newly, filled, out_nd)).astype(prof["dtype"] if np.dtype(prof["dtype"]).kind == "f" else "float32")
    if np.dtype(prof["dtype"]).kind != "f":
        prof.update(dtype="float32")
    prof.update(nodata=out_nd, compress="lzw")
    tmp = dst.with_suffix(".tif.part")
    with rasterio.open(tmp, "w", **prof) as d:
        d.write(res_arr, 1)
        d.update_tags(source=str(src), source_md5=md5(src), method="GDAL FillNodata (inverse distance, no smoothing)",
                      max_search_deg=str(max_deg), max_search_px=str(max_px), filled_cells=str(int(newly.sum())),
                      pipeline="hazards_prototype python/obs/soil_fill.py (observed tree only; NEX soils untouched)")
    os.replace(tmp, dst)
    v = res_arr[newly]
    log(f"{src.name}: res {res:.5f} deg, max {max_px} px; valid {int(valid.sum())}, filled {int(newly.sum())}, "
        f"still NA {int((~valid & ~reach).sum())}; filled range {np.nanmin(v) if v.size else '-'}..{np.nanmax(v) if v.size else '-'} "
        f"(source range {np.nanmin(a[valid]):.3g}..{np.nanmax(a[valid]):.3g})")
    return int(newly.sum())


def check_pr(soil: Path, pr: Path, label):
    """Count land cells of the obs climate grid (pr finite) where the soil, resampled as the R scripts do, is NA."""
    import rasterio
    from rasterio.warp import reproject, Resampling
    with rasterio.open(pr) as p:
        prv = p.read(1); dst = np.full(prv.shape, np.nan, "float32"); pt, pc = p.transform, p.crs
    with rasterio.open(soil) as s:
        reproject(s.read(1).astype("float32"), dst, src_transform=s.transform, src_crs=s.crs, src_nodata=s.nodata,
                  dst_transform=pt, dst_crs=pc, dst_nodata=np.nan, resampling=Resampling.bilinear)
    land = np.isfinite(prv)
    n = int((land & ~np.isfinite(dst)).sum())
    log(f"{label}: obs land cells (pr finite) {int(land.sum())}; soil NA on {n} of them after bilinear resample")
    return n


def cmd_fill(a):
    src_dir = Path(a.src_dir).resolve(); root = Path(a.out_root)
    if not root.is_absolute():
        raise SystemExit("[SOIL] --out-root must be absolute")
    dst_dir = root / "atlas_hazards" / "soils"
    for f in REQUIRED:
        if not (src_dir / f).exists():
            raise SystemExit(f"[SOIL] {src_dir / f} missing")
    if dst_dir.resolve() == src_dir:
        if dst_dir.is_symlink():
            log(f"{dst_dir} is a symlink to the shared soils; replacing the LINK (the target is not touched)")
            os.unlink(dst_dir)
        else:
            raise SystemExit(f"[SOIL] {dst_dir} IS the source directory; refusing to write into the shared soils")
    elif dst_dir.is_symlink():
        raise SystemExit(f"[SOIL] {dst_dir} is a symlink to {os.readlink(dst_dir)}, not to --src-dir; stop and check")
    dst_dir.mkdir(parents=True, exist_ok=True)
    pre = {}
    if a.check_pr:
        for f in REQUIRED:
            pre[f] = check_pr(src_dir / f, Path(a.check_pr), f"before {f}")
    for f in LAYERS:
        if (src_dir / f).exists():
            fill_layer(src_dir / f, dst_dir / f, a.max_dist_deg)
        else:
            log(f"{f}: not in source, skipped")
    for p in sorted(src_dir.iterdir()):
        if p.name not in LAYERS and p.is_file():
            shutil.copy2(p, dst_dir / p.name); log(f"copied unchanged: {p.name}")
    if a.check_pr:
        for f in REQUIRED:
            post = check_pr(dst_dir / f, Path(a.check_pr), f"after {f}")
            log(f"{f}: obs land cells without soil {pre[f]} -> {post}")
    log("done; NDWS/NDWL0 must be rerun with FORCE_OVERWRITE=1 to use the filled soils")


def cmd_selftest(_a):
    import rasterio
    from rasterio.transform import from_origin

    def ok(c, m):
        if not c:
            raise AssertionError("SELFTEST FAIL: " + m)
        print("  ok  ", m)

    with tempfile.TemporaryDirectory() as td:
        td = Path(td); src = td / "common" / "atlas_hazards" / "soils"; src.mkdir(parents=True)
        a = np.full((20, 20), 100.0, "float32"); a[:, 10:] = 200.0
        a[4:6, 4:6] = -9999                       # small coastal-style hole inside valid soil -> filled
        a[:, 18:] = -9999                         # far strip: 2 px wide = 0.1 deg at 0.05 -> within 0.5 deg -> filled
        a[15:, :] = -9999                         # big region 5 rows deep
        prof = dict(driver="GTiff", width=20, height=20, count=1, dtype="float32", crs="EPSG:4326",
                    transform=from_origin(0, 1, 0.05, 0.05), nodata=-9999)
        for f in REQUIRED:
            with rasterio.open(src / f, "w", **prof) as d:
                d.write(a, 1)
        src_md5 = md5(src / "sscp_world.tif")
        root = td / "obs"; (root / "atlas_hazards").mkdir(parents=True)
        os.symlink(src, root / "atlas_hazards" / "soils")
        ns = argparse.Namespace(src_dir=str(src), out_root=str(root), max_dist_deg=0.1, check_pr=None)
        cmd_fill(ns)
        out = root / "atlas_hazards" / "soils"
        ok(not out.is_symlink() and out.is_dir(), "the obs symlink was replaced by a real directory")
        ok(md5(src / "sscp_world.tif") == src_md5 and (src / "ssat_world.tif").exists(), "the shared source soils are untouched")
        with rasterio.open(out / "sscp_world.tif") as d:
            r = d.read(1); nd = d.nodata; tags = d.tags()
        ok(np.all((r[4:6, 4:6] >= 100) & (r[4:6, 4:6] <= 200)), "the small hole is filled with values in the neighbours' range")
        ok(r[0, 0] == 100 and r[0, 12] == 200, "valid cells are unchanged")
        ok(np.all(r[19, :] == nd), "cells beyond --max-dist-deg (0.1 deg = 2 px) stay nodata")
        ok(np.all(r[15:17, 2] != nd), "cells within --max-dist-deg of valid soil are filled")
        ok(tags.get("method", "").startswith("GDAL FillNodata") and tags.get("source_md5") == src_md5 and int(tags["filled_cells"]) > 0,
           "the output carries method, source md5 and the filled-cell count")
        try:
            cmd_fill(ns); ok(True, "a rerun on the real directory is allowed (idempotent)")
        except SystemExit as e:
            ok(False, f"rerun failed: {e}")
        ns2 = argparse.Namespace(src_dir=str(out), out_root=str(root), max_dist_deg=0.1, check_pr=None)
        try:
            cmd_fill(ns2); ok(False, "writing into the source directory must refuse")
        except SystemExit:
            ok(True, "refuses when the output directory IS the source directory")
    print("\nALL SOIL-FILL SELFTEST ASSERTIONS PASSED")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fill"); f.add_argument("--src-dir", required=True); f.add_argument("--out-root", required=True)
    f.add_argument("--max-dist-deg", type=float, default=0.5); f.add_argument("--check-pr", default=None)
    sub.add_parser("selftest")
    a = ap.parse_args()
    {"fill": cmd_fill, "selftest": cmd_selftest}[a.cmd](a)


if __name__ == "__main__":
    main()
