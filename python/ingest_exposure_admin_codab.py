#!/usr/bin/env python3
"""
ingest_exposure_admin_codab.py — IEBC COD-AB Kenya admin boundaries -> GeoJSON (KE-39, layer 2).

The OFFICIAL Kenya admin backbone for KE-39 exposure: HDX `cod-ab-ken` (source IEBC, org OCHA,
CC-BY-IGO) — 47 counties (adm1) + 290 sub-counties (adm2) with official adm1_pcode/adm2_pcode.
Preferred over GAUL24 (which carries the disputed Ilemi Triangle + has no p-codes). GAUL24 stays
the climate/zonal backbone; this is the exposure/official-product admin.

Resolves the HDX resource via the CKAN API (no hardcoded fragile URL), downloads the geo package
(SHP/GDB zip or GeoJSON), auto-detects the adm1/adm2 layers by their p-code fields, makes geometry
valid, forces EPSG:4326, writes ken_adm{1,2}.geojson. (cglabs already verified 47/290 locally;
this is the reproducible route-B script.)

Requires: geopandas + fiona/pyogrio (or falls back to gdal ogr2ogr). No auth.

RUN (cglabs): python3 python/ingest_exposure_admin_codab.py --list    # dump layers/fields (verify)
              python3 python/ingest_exposure_admin_codab.py            # write adm1 + adm2 geojson
Output: <out>/ken_adm1.geojson  <out>/ken_adm2.geojson
Publish: R/observational/6_publish_obs_to_s3.R --full --tier 10  (domain=boundaries/type=admin/source=iebc-codab)
"""
import argparse
import datetime as dt
import io
import json
import os
import tempfile
import urllib.request
import zipfile

CKAN = "https://data.humdata.org/api/3/action/package_show?id=cod-ab-ken"
# expected counts (cglabs #1 verified) — used as a smoke sanity gate
EXPECT = {"adm1": 47, "adm2": 290}


def log(msg):
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def resolve_resource():
    """Query HDX CKAN; return the best geo resource URL (prefer SHP/GDB zip, then GeoJSON)."""
    with urllib.request.urlopen(CKAN, timeout=60) as r:
        pkg = json.load(r)["result"]
    res = pkg.get("resources", [])
    def score(x):
        fmt = (x.get("format") or "").lower()
        name = (x.get("name") or "").lower()
        s = 0
        if "shp" in fmt or "shape" in fmt or name.endswith(".zip"): s += 3
        if "gdb" in fmt or "geodatabase" in name: s += 3
        if "geojson" in fmt: s += 2
        if "adm" in name or "iebc" in name: s += 1
        return s
    res = sorted(res, key=score, reverse=True)
    for x in res:
        if score(x) > 0 and x.get("download_url"):
            return x["download_url"], (x.get("format") or ""), x.get("name")
    raise RuntimeError("no suitable COD-AB geo resource found on HDX cod-ab-ken")


def read_layers(local_path):
    """Return list of (layername, GeoDataFrame) for all vector layers in a file/zip/gdb."""
    import geopandas as gpd
    import fiona
    # if a zip, expose members via /vsizip through fiona's listlayers on the .zip path
    paths = []
    if local_path.lower().endswith(".zip"):
        # find shapefiles / gdb inside
        with zipfile.ZipFile(local_path) as zf:
            names = zf.namelist()
        shps = [n for n in names if n.lower().endswith(".shp")]
        gdbs = sorted({n.split(".gdb/")[0] + ".gdb" for n in names if ".gdb/" in n.lower()})
        if shps:
            paths = [f"zip://{local_path}!{n}" for n in shps]
        elif gdbs:
            paths = [f"/vsizip/{local_path}/{g}" for g in gdbs]
        else:
            paths = [f"zip://{local_path}!{n}" for n in names if n.lower().endswith(".geojson")]
    else:
        paths = [local_path]
    out = []
    for p in paths:
        try:
            layers = fiona.listlayers(p)
        except Exception:
            layers = [None]
        for lyr in layers:
            try:
                gdf = gpd.read_file(p, layer=lyr) if lyr else gpd.read_file(p)
                out.append((lyr or os.path.basename(p), gdf))
            except Exception as e:
                log(f"    skip layer {p}:{lyr} ({type(e).__name__})")
    return out


def pick_admin(layers, level):
    """Pick the layer that is the adm{level} boundary by its p-code field."""
    pc = f"adm{level}_pcode"
    cands = []
    for name, gdf in layers:
        cols = {c.lower() for c in gdf.columns}
        higher = any(f"adm{l}_pcode" in cols for l in range(level + 1, 4))
        if pc in cols and not higher:
            cands.append((name, gdf))
    if not cands:
        # fallback: match by feature count near expected
        for name, gdf in layers:
            if abs(len(gdf) - EXPECT[f"adm{level}"]) <= 2:
                cands.append((name, gdf))
    return cands[0] if cands else (None, None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="Data/exposure/admin_codab")
    ap.add_argument("--list", action="store_true", help="dump layers + fields, don't write")
    ap.add_argument("--overwrite", action="store_true")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    url, fmt, name = resolve_resource()
    log(f"HDX cod-ab-ken resource: {name} [{fmt}] -> {url}")
    with tempfile.TemporaryDirectory() as tmp:
        ext = ".zip" if url.lower().endswith(".zip") or "shp" in fmt.lower() or "gdb" in fmt.lower() else ".geojson"
        local = os.path.join(tmp, "codab" + ext)
        urllib.request.urlretrieve(url, local)
        log(f"  downloaded {os.path.getsize(local)/1e6:.1f} MB")
        layers = read_layers(local)
        log(f"  {len(layers)} layer(s): " + ", ".join(f"{n}({len(g)})" for n, g in layers))
        if a.list:
            for n, g in layers:
                log(f"    layer {n}: {list(g.columns)}")
            return
        for lvl in (1, 2):
            name_l, gdf = pick_admin(layers, lvl)
            if gdf is None:
                log(f"  ADM{lvl}: NOT FOUND — run --list + fix pick_admin"); continue
            gdf = gdf.to_crs(4326)
            gdf["geometry"] = gdf.geometry.make_valid()
            out = os.path.join(a.out, f"ken_adm{lvl}.geojson")
            gdf.to_file(out, driver="GeoJSON")
            ok = "OK" if len(gdf) == EXPECT[f"adm{lvl}"] else f"!!EXPECTED {EXPECT[f'adm{lvl}']}"
            log(f"  ADM{lvl}: layer '{name_l}' -> {out}  ({len(gdf)} features {ok})")
    log("DONE")
    print("\nNext: publish with  Rscript R/observational/6_publish_obs_to_s3.R --full --tier 10")


if __name__ == "__main__":
    main()
