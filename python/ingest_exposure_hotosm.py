#!/usr/bin/env python3
"""
ingest_exposure_hotosm.py — HDX HOTOSM Kenya health + education facilities -> GeoJSON (KE-39 layers 5+6).

Facility points/polygons for the flood/drought exposure overlay, from HDX HOTOSM (OSM-derived, ODbL).
Official registries (KMHFR health / GIGA schools) are deferred — their APIs are unreachable from the
node (need creds/allowlist); HOTOSM is the open, working substitute (Pete: "both / official later").

Resolves each HDX dataset via CKAN (no fragile hardcoded URL), prefers a GeoJSON resource (else GPKG/
SHP via geopandas), forces EPSG:4326, crops the Kenya bbox, writes {health,schools}.geojson.

Sources (cglabs KE-39 probe, non-GEE, no auth, HDX 200):
  health  = HDX dataset  hotosm_ken_health_facilities   (ODbL)
  schools = HDX dataset  hotosm_ken_education_facilities (ODbL)

Requires: geopandas + fiona (installed on cglabs for COD-AB). No auth.

RUN (cglabs): python3 python/ingest_exposure_hotosm.py --smoke           # health only, report count
              python3 python/ingest_exposure_hotosm.py                   # health + schools
Output: <out>/health.geojson  <out>/schools.geojson
Publish: R/observational/6_publish_obs_to_s3.R --full --tier 13  (type=infrastructure/source=hotosm)
"""
import argparse
import datetime as dt
import json
import os
import tempfile
import urllib.request

LAYERS = {
    "health":  {"ckan": "hotosm_ken_health_facilities",    "out": "health.geojson"},
    "schools": {"ckan": "hotosm_ken_education_facilities",  "out": "schools.geojson"},
}
CKAN = "https://data.humdata.org/api/3/action/package_show?id={id}"
BBOX = (33.9, -4.7, 41.9, 5.5)          # Kenya (W,S,E,N)


def log(msg):
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def resolve_resource(ckan_id):
    with urllib.request.urlopen(CKAN.format(id=ckan_id), timeout=60) as r:
        pkg = json.load(r)["result"]
    def score(x):
        fmt = (x.get("format") or "").lower(); nm = (x.get("name") or "").lower()
        s = 0
        if "geojson" in fmt: s += 3
        if "gpkg" in fmt or "geopackage" in fmt: s += 2
        if "shp" in fmt or nm.endswith(".zip"): s += 1
        if "point" in nm or "polygon" in nm or "facilit" in nm: s += 1
        return s
    for x in sorted(pkg.get("resources", []), key=score, reverse=True):
        if score(x) > 0 and x.get("download_url"):
            return x["download_url"], (x.get("format") or ""), x.get("name")
    raise RuntimeError(f"no geo resource on HDX {ckan_id}")


def build(layer, out_dir, overwrite):
    import geopandas as gpd
    spec = LAYERS[layer]
    out = os.path.join(out_dir, spec["out"])
    if not overwrite and os.path.exists(out) and os.path.getsize(out) > 100:
        log(f"{layer}: exists, skip"); return "skip"
    url, fmt, name = resolve_resource(spec["ckan"])
    log(f"{layer}: HDX {spec['ckan']} -> {name} [{fmt}]")
    with tempfile.TemporaryDirectory() as tmp:
        local = os.path.join(tmp, os.path.basename(url.split("?")[0]) or "dl")
        urllib.request.urlretrieve(url, local)
        gdf = gpd.read_file(f"zip://{local}" if local.lower().endswith(".zip") else local)
        gdf = gdf.to_crs(4326)
        gdf = gdf.cx[BBOX[0]:BBOX[2], BBOX[1]:BBOX[3]]     # clip to Kenya bbox
        os.makedirs(out_dir, exist_ok=True)
        gdf.to_file(out, driver="GeoJSON")
    log(f"{layer}: -> {out} ({len(gdf)} features, {os.path.getsize(out)/1e6:.1f} MB)")
    return "written"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="Data/exposure/hotosm")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--smoke", action="store_true", help="health only")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    layers = ["health"] if a.smoke else list(LAYERS)
    log(f"HOTOSM exposure ingest | layers={layers} out={a.out}")
    tally = {"written": 0, "skip": 0}
    for lyr in layers:
        try:
            tally[build(lyr, a.out, a.overwrite)] += 1
        except Exception as e:
            log(f"  ERROR {lyr}: {type(e).__name__}: {e}")
            if a.smoke:
                raise
    log(f"DONE: {tally}")
    print("\nNext: publish with  Rscript R/observational/6_publish_obs_to_s3.R --full --tier 13")


if __name__ == "__main__":
    main()
