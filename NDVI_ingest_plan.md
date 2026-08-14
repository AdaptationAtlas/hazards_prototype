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

## 6. Ingest mechanism (net-new — NON-GEE, via NASA LP DAAC)
**No GEE** (dropped — the #1 probe showed it needs service-account provisioning we don't want; the
node has egress + rasterio + gdal, so a direct path runs there).
- **`earthaccess`** (pip) → search MOD13Q1 v061 by bbox+date → download HDF from LP DAAC.
- Extract "250m 16 days NDVI" subdataset (×1e-4) → mosaic MODIS tiles → **reproject Sinusoidal →
  EPSG:4326** → per-season mean over the window's 16-day layers → COG w/ overviews.
- Only "auth" = a **free NASA Earthdata Login** (`~/.netrc` or `EARTHDATA_*`) — far lighter than GEE.
- Python ingest script; publish via new `type=vegetation` tier in `6_publish_obs_to_s3.R`.

## 7. Decisions — RESOLVED
1. **Compute + auth:** ~~GEE~~ → **non-GEE via earthaccess/LP DAAC** (dispatch #2). Node ready
   (earthaccess 0.18.0 + egress + rasterio + gdal). **GATE = a free NASA Earthdata Login on cglabs**
   (`~/.netrc` / `EARTHDATA_*`) — the ONLY open blocker. Discovery is anon; download needs it.
2. **Products (KE-ENSO 2026-08-14):** ✅ **seasonal OND/MAM v1 + annual**. Skip raw 16-day.
3. **Co-registration (KE-ENSO):** ✅ **250 m + overviews enough — NO 0.05° pixel-math tier.**
4. **S3 `type=vegetation`** convention — pick + inform Brayden (still to flag).

## 8. Sequence
1. **Earthdata login on cglabs** — the one gate (see §7.1).
2. cglabs re-runs dispatch #2 step 3 → verified NDVI subdataset name + native CRS/res.
3. macbook writes ingest: MOD13Q1 → mosaic → reproject Sinusoidal→EPSG:4326 → **seasonal (OND/MAM)
   + annual** mean → COG w/ overviews → new `type=vegetation` publish tier.
4. Verify live; flag to Brayden for cataloguing.
5. (opt/deferred) WaPOR 100 m East-Africa; NPP carbon layer.

_Only open item: the Earthdata Login on cglabs. Everything else decided._
