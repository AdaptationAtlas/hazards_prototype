#!/usr/bin/env python3
"""
ingest_exposure_osm_roads.py — OSM Kenya roads -> GeoJSON (KE-39, layer 4).

Major road network for the flood/drought exposure overlay. Source: OSM Geofabrik Kenya extract
(ODbL). Extracts the classified highways (motorway..tertiary — the network that matters for access,
not every footpath) via ogr2ogr's OSM driver, crops to the Kenya bbox, writes GeoJSON.

Source (cglabs KE-39 probe, non-GEE, no auth, 302->file):
  https://download.geofabrik.de/africa/kenya-latest.osm.pbf   (ODbL)

Requires: gdal (osgeo, ogr2ogr with the OSM driver). No auth.

RUN (cglabs): python3 python/ingest_exposure_osm_roads.py --smoke   # extract + report feature count
              python3 python/ingest_exposure_osm_roads.py
Output: <out>/kenya_roads.geojson   (LineString, EPSG:4326, ODbL — attribution required)
Publish: R/observational/6_publish_obs_to_s3.R --full --tier 12  (type=infrastructure/source=osm)
"""
import argparse
import datetime as dt
import os
import subprocess
import urllib.request

URL = "https://download.geofabrik.de/africa/kenya-latest.osm.pbf"
BBOX = (33.9, -4.7, 41.9, 5.5)          # Kenya (W,S,E,N)
# classified network only — keeps the overlay light (all-highways incl. paths = huge)
CLASSES = ("motorway", "trunk", "primary", "secondary", "tertiary")


def log(msg):
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def build(out_dir, overwrite, smoke):
    out = os.path.join(out_dir, "kenya_roads.geojson")
    if not overwrite and os.path.exists(out) and os.path.getsize(out) > 100:
        log("kenya_roads: exists, skip"); return "skip"
    os.makedirs(out_dir, exist_ok=True)
    pbf = os.path.join(out_dir, ".kenya-latest.osm.pbf")
    log("downloading OSM Geofabrik kenya-latest.osm.pbf ...")
    urllib.request.urlretrieve(URL, pbf)
    log(f"  got {os.path.getsize(pbf)/1e6:.1f} MB")
    where = "highway IN (" + ",".join(f"'{c}'" for c in CLASSES) + ")"
    # OSM driver "lines" layer carries highway; clip to Kenya bbox; keep name+highway class.
    cmd = [
        "ogr2ogr", "-f", "GeoJSON", out, pbf, "lines",
        "-where", where,
        "-clipdst", str(BBOX[0]), str(BBOX[1]), str(BBOX[2]), str(BBOX[3]),
        "-select", "osm_id,name,highway",
        "-nlt", "PROMOTE_TO_MULTI", "-t_srs", "EPSG:4326",
    ]
    log("  ogr2ogr extract classified highways -> GeoJSON")
    rc = subprocess.run(cmd, capture_output=True, text=True)
    if rc.returncode != 0 or not os.path.exists(out):
        raise RuntimeError(f"ogr2ogr failed: {rc.stderr[:500]}")
    if not smoke:
        os.remove(pbf)
    # feature count
    n = subprocess.run(["ogrinfo", "-so", out, os.path.splitext(os.path.basename(out))[0]],
                       capture_output=True, text=True).stdout
    nf = next((l for l in n.splitlines() if "Feature Count" in l), "Feature Count: ?")
    log(f"kenya_roads: -> {out} ({nf.strip()}, {os.path.getsize(out)/1e6:.1f} MB)")
    return "written"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="Data/exposure/osm_roads")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--smoke", action="store_true", help="keep tmp pbf for inspection")
    a = ap.parse_args()
    log(f"OSM roads ingest | classes={CLASSES} out={a.out}")
    r = build(a.out, a.overwrite, a.smoke)
    log(f"DONE: {r}")
    print("\nNext: publish with  Rscript R/observational/6_publish_obs_to_s3.R --full --tier 12")


if __name__ == "__main__":
    main()
