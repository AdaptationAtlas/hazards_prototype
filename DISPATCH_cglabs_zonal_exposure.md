# DISPATCH — cglabs ⇄ macbook — pre-cooked flood × exposure zonal tables (per adm2)

Branch `develop`. Append-only; newest on top. cglabs runs, appends `### RESPONSE`, pushes.
**Authorship:** engine authored by **macbook / hazards_prototype**; **cglabs runs on-node** (owns the exposure data) + publishes. (Per the notebook's authorship-tracking ask.)

**Goal.** The KE-ENSO notebook can't intersect client-side (53 MB grid + 30 MB roads + 100 m pop + 111 m flood). Pre-compute the intersect server-side → small per-adm2 parquet tables (request: `atlas_nb-KE-enso/…/2026-09-01_request-precooked-exposure-tables.md`). New engine `R/observational/7_zonal_exposure.R` + publish **tier 16**.

**Products → `Data/exposure/intersect/`:**
- `exposure_gfm_seasonal.parquet` — adm2 × season × year (GFM observed flood). ~290 × (all seasons present) × 2018–2025.
- `exposure_jrc_rp.parquet` — adm2 × return-period (JRC modelled hazard). 290 × 7 RPs.
- `exposure_totals.parquet` — adm2 denominators (static).

**Final schema (macbook, refined from the notebook proposal):**
- keys: `adm2_pcode, adm1_pcode, adm2_name, adm1_name` (from IEBC COD-AB); A adds `season, year`; B adds `rp`.
- A metrics: `flooded_km2, observed_pct` (SAR coverage), `flooded_pct_observed`, `pop_exposed, pop_pct, pop_source`(=worldpop), `roads_km_exposed, health_n_exposed, schools_n_exposed, grid_km_exposed, grid_km_exposed_hv` (132/220 kV backbone).
- B metrics: `flood_prone_km2` (JRC depth>0) + same exposure cols (no season/year).
- totals: `pop_total, area_km2, roads_km_total, grid_km_total, health_n_total, schools_n_total`.
- Exposure rule: raster cell / asset geometry intersecting the flood mask; pop is pixel-sum weighted (mask resampled to the 100 m pop grid). WorldPop constrained = v1; GRID3 A/B deferred (add later via `pop_source`).

## [macbook / hazards_prototype · 2026-09-05 #1] ACTION -> cglabs: SMOKE first (time the line-intersect), then full + publish

Inputs expected under `<data>/exposure/` (the tier local_dirs): `admin_codab/ken_adm2.geojson`, `worldpop/population_2020.tif`, `osm_roads/kenya_roads.geojson`, `hotosm/{health,schools}.geojson`, `grid/kenya_power_grid.geojson`, `gfm_flood/seasonal/{flooded,nobs}/`, and `../flood_jrc/JRC/`. If any live elsewhere (you re-ran ingests with a custom `--out`), tell me and I'll fix the paths.

**STEP 1 — SMOKE (1 GFM season + 1 JRC RP → `intersect_smoke/`; report timing):**
```
SMOKE_ZONAL=1 Rscript R/observational/7_zonal_exposure.R
```
Report: the printed A-head + B-head, row counts, and the **per-step timings** — especially the grid line-intersection (141k features × the flood polygon). **If the grid `st_intersection` is the bottleneck** (say >1–2 min/raster → ~hours over the full GFM set), flag it and I'll optimise (pre-clip grid to the flood bbox / rasterised-length approximation) or drop `grid_km_exposed` to a v2. **Then HOLD** — let me eyeball the smoke output before the full run.

**STEP 2 — FULL (after macbook confirms smoke):**
```
nohup Rscript R/observational/7_zonal_exposure.R &> zonal_exposure.log &
```

**STEP 3 — PUBLISH:**
```
Rscript R/observational/6_publish_obs_to_s3.R --full --tier 16
```
→ 3 parquets to `domain=exposure/type=intersect/region=kenya/processing=analysis-ready/`. Count-verify (local==S3, 3==3) + 206 + CORS. Report the row counts + a sanity line (e.g. total pop_exposed for OND-2019 vs pop_total).

Append `### RESPONSE` with smoke timings + head, then (after full+publish) counts + verify, then push.
