# Rangeland vegetation ingest plan — MODIS NDVI (+ optional WaPOR / NPP)

_Scoping doc, 2026-08-13. For the KE-ENSO Explorer pastoralist/rangeland map panel.
Supersedes NPP_ingest_plan.md — NDVI is the better rangeland layer; NPP dropped unless a
carbon-magnitude story is wanted. Net-new source ingest, not yet built (see §5)._

## 1. Why NDVI, not NPP
- Notebook already has NDVI as **admin-zonal (WFP VAM)** — a number per district. The real upgrade
  is a **per-pixel NDVI raster** on the map panel, compositable by ENSO phase.
- NPP/PSN is a *different* metric (modelled carbon, gC/m²), derived from the **same** MODIS optical
  inputs as NDVI → strongly correlated, not a new signal. Adds carbon-magnitude framing only.
  Dropped for v1; revisit only if a carbon-productivity story is explicitly wanted.

## 2. Primary product — MODIS MOD13Q1 NDVI
| | value |
|---|---|
| Source | MODIS MOD13Q1 v061, GEE `MODIS/061/MOD13Q1` (band `NDVI`, scale 1e-4) |
| Native res | **250 m** (finer than NPP's 500 m) |
| Temporal | **16-day** composites |
| Record | **2000 → present (~26 yr)** — deep enough for ENSO phase-composites |
| Coverage | global (crop to Africa) |
| Products | (a) per-composite (16-day) OR (b) **seasonal** (OND/MAM = mean NDVI over the season's 16-day layers) OR (c) annual — mirrors the rainfall seasonal COGs |

Standard pastoral forage proxy (FEWS NET / WFP VAM use NDVI). Seasonal + long record = OND/MAM
greenness in El-Niño vs La-Niña years, aligned with the rainfall panel.

## 3. Heavy-map strategy — pyramids first, resample tiers only if needed
- Build 250 m NDVI COGs **with internal overviews** (`OVERVIEWS=AUTO`) → one file serves county
  (native) + continental (overview) via geotiff.js range reads. Solves zoom-out weight. Default.
- Separate coarse tiers (1 km / 5 km / 0.05°) ONLY if:
  - notebook needs **NDVI × rainfall pixel-math** on a shared grid → add a **0.05°-resampled** tier
    (matches the CHIRPS grid), or
  - native 250 m Africa COGs prove too big to store/serve (overviews fix rendering, not file size).
- Skip 1 km / 5 km unless proven heavy.

## 4. Optional finer-detail — WaPOR (defer)
WaPOR (100 m, Africa, 2009–, dekadal) NDVI/biomass — finer than 250 m for current condition, but
short record. Optional second source, **East-Africa crop first**. Not v1.

## 5. S3 layout (NEW type=vegetation — Brayden's catalogue call)
```
s3://digital-atlas/domain=climate/type=vegetation/source=modis-mod13q1/region=africa/
  processing=seasonal/variable=NDVI/season={SEASON}/NDVI_{SEASON}_{YYYY}_mean.tif
  processing=annual/variable=NDVI/NDVI_{YYYY}_mean.tif        # optional
```
COG conventions = the rainfall tiers (EPSG:4326, tiled, overviews ON, CORS `*`, range). New publish
tier in `6_publish_obs_to_s3.R`. Flag the `type=vegetation` path to Brayden (new type).

## 6. Ingest mechanism (net-new — the real work)
- GEE: `MODIS/061/MOD13Q1` → `.select("NDVI").multiply(1e-4)` → per-season composite
  (mean over the season's 16-day layers per year) → `Export.image` to GCS → download → COG w/
  overviews → publish tier.
- hazards_prototype has **no GEE today** → new dependency + service-account auth. Python ingest script.

## 7. Decisions to confirm BEFORE building
1. **Compute + auth:** does the bake node (cglabs) have **GEE access** (earthengine-api + service
   account)? If not, ingest runs on a GEE-enabled node / macbook one-off. → cglabs capability probe (gates all).
2. **Products:** seasonal (OND/MAM) NDVI is the clear v1. Also want annual? per-16-day? (seasonal recommended.)
3. **Co-registration:** need a 0.05° NDVI tier for NDVI×rainfall math, or is 250m+overviews enough? (overviews enough for v1.)
4. **S3 `type=vegetation`** convention — pick + inform Brayden.

## 8. Sequence
1. cglabs GEE capability probe — gates everything.
2. MOD13Q1 seasonal NDVI (OND/MAM, 2000–2025) → COG w/ overviews → publish tier → verify live.
3. (opt) WaPOR 100m East-Africa; (opt) 0.05° co-registered tier; (opt) NPP carbon layer.
4. Flag to Brayden for cataloguing.

_Open until §7 answered. No code yet._
