# Riverine flood ingest plan — JRC hazard + Global Flood Database (two products)

_Scoping doc, 2026-08-17. For the KE-ENSO Explorer. p.steward: build BOTH (static hazard + observed
events). Net-new sources. Access is a solvable detail (GEE or direct — not a decision driver)._

## 1. Two products (complementary, like the rainfall A/B)

| | **JRC GloFAS Flood Hazard v2.1** | **Global Flood Database (GFD) v1** |
|---|---|---|
| What | riverine **flood depth / hazard-class by return period** (static) | **satellite-observed inundation per flood event** (time-varying) |
| Framing | "where is flood-prone" — exposure overlay | "where flooded, and when" — **ENSO-composable** by year/phase |
| Variable | flood depth (m) + hazard class 0–4 | flooded (binary) / flood frequency per event |
| Return periods / time | RP 10,20,50,75,100,200,500 yr | ~2000–2018, 900+ events |
| Res | **90 m**, EPSG:4326, global | 250 m (MODIS), global |
| Licence | **CC-BY-4.0** (Copernicus/JRC) | open (Cloud to Street / DFO) |
| Access | data.europa.eu GeoTIFF · GEE `JRC/CEMS_GLOFAS/FloodHazard/v2_1` · source.coop COGs | GEE `GLOBAL_FLOOD_DB/MODIS_EVENTS/V1` · Cloud to Street portal |

**JRC = static** (doesn't composite by ENSO). **GFD = observed events** → composites by ENSO/IOD
phase with the notebook's year-sets (the "more flooding in El-Niño years" story), but ends 2018.

## 2. Sequence
1. **JRC first (quick win):** 7 return-period rasters, public download, crop East-Africa → COG →
   publish. No compositing, no auth headaches. Small.
2. **GFD second:** per-event / per-year flooded-fraction, ENSO-composable → COG → publish. Bigger;
   access via GEE (or the Cloud to Street portal / Zenodo mirror — cglabs to confirm the route).

## 3. S3 layout (NEW `type=flood` — Brayden's convention call)
```
JRC:  domain=climate/type=flood/source=jrc-glofas/region=east-africa/
        processing=return-period/variable=flood-hazard/rp={10..500}/flood-hazard_rp{RP}.tif
GFD:  domain=climate/type=flood/source=global-flood-db/region=east-africa/
        processing=annual/variable=flooded-frac/floodedfrac_{YYYY}.tif   (per-year composite)
      (or processing=events/…/event_{id}.tif for per-event)
```
COG conventions = rainfall/NDVI tiers (EPSG:4326, tiled, overviews, CORS `*`, range). New Tier(s)
in `6_publish_obs_to_s3.R`. `type=flood` is a new type under `domain=climate` → flag Brayden.

## 4. Extent
East-Africa / Kenya (mirror NDVI `region=east-africa`), expandable. JRC/GFD are global → crop the
Kenya bbox (window-read via `/vsicurl` from the public COG, or download+crop). Tiny for one region.

## 5. Ingest mechanism
- **JRC:** resolve a working public source on cglabs (data.europa.eu / JRC catalogue / source.coop
  COG / GEE — whichever responds) → for each RP: crop East-Africa bbox → COG w/ overviews. No
  per-pixel math; just crop + reformat. ~7 small COGs.
- **GFD:** `GLOBAL_FLOOD_DB/MODIS_EVENTS/V1` on GEE — per event has `flooded` band + dates; reduce
  to **per-year flooded-fraction** (or per-event) over East-Africa → COG. If GEE auth is the
  blocker, GFD is also downloadable from the Cloud to Street data portal — cglabs to confirm.

## 6. Decisions / gates
1. **JRC access:** cglabs resolves the live public URL (probe: try data.jrc/europa mirror + a
   `/vsicurl` crop). No account needed. → quick.
2. **GFD access:** GEE asset (needs a GEE service account on cglabs) OR the portal/Zenodo download.
   cglabs to report which is reachable; provision a GEE service account if that's the clean route
   (p.steward — same kind of setup as the Earthdata login).
3. **`type=flood`** convention → Brayden.
4. Per-year vs per-event GFD granularity → default **per-year flooded-fraction** (composites cleanly).

## 7. Next
Dispatch #1 (access probe): cglabs confirms JRC public download + crops one RP over Kenya; reports
GFD reachability (GEE vs portal). Then macbook writes the JRC ingest (small) + the GFD ingest.
