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

## [macbook / hazards_prototype · 2026-09-08 #2] RATIFY 3 fixes + OPTIMISE line-exposure (st_intersection -> rasterised length). Re-smoke.

**Your 3 smoke fixes — RATIFIED, kept:**
1. `tag_adm2(pt=TRUE)` → `st_geometry(x)` (my bug: passed whole sf → 0 points tagged). Correct.
2. HOTOSM health/schools MIXED geometry (POINT+POLYGON footprints) → reduce to one representative point at load. Correct (1 point/facility semantics unchanged).
3. env `lwgeom` for `st_length` on lon/lat. Fine.

**Perf blocker FIXED (rasterised-length, your recommended option).** Replaced the per-raster `st_intersection(141k grid lines × flood polygon)` with `terra::rasterizeGeom(lines, grid, "length", "km")` computed **ONCE per grid** (GFM 111 m + JRC 90 m), then per-raster `roads/grid_km_exposed = zonal(len_percell × flood_mask, adm2)`. The hours-long vector op → a seconds-long raster op. Cell-resolution approximation (a line in a flooded cell counts its full in-cell length) — fine for a km-exposed metric. `grid_km_exposed_hv` (132/220 kV) via a separate length raster. Also **clamped `observed_pct`/`flooded_pct_observed` to ≤1** (your grid-mismatch note).

Expected: GFM ~6 min → seconds/raster; JRC ~8.35 h → seconds/raster; one-time `rasterizeGeom` of the 141k-line grid per grid (~1–2 min ×2). Full run should now be minutes, not 68 h.

**RE-SMOKE (same command), report timings:**
```
SMOKE_ZONAL=1 Rscript R/observational/7_zonal_exposure.R
```
Confirm GFM + JRC each drop to seconds and the roads/grid_km numbers are sane (grid_km_total ≈ 69,092 km; exposed ≤ total). **Then, if timings are good, GO for the full run + publish tier 16 WITHOUT waiting** (the perf risk is retired):
```
nohup Rscript R/observational/7_zonal_exposure.R &> zonal_exposure.log &
Rscript R/observational/6_publish_obs_to_s3.R --full --tier 16
```
Append `### RESPONSE` with re-smoke timings + full+publish counts + count-verify.

### RESPONSE — cglabs 2026-09-08 #2 — rasterised-length WORKS. Full run + tier-16 publish DONE. 🟢🟢

Per-raster cost fixed exactly as intended: **8.35 h → ~40 s/raster**. The residual cost is the one-time `rasterizeGeom` precompute (your ~1–2 min estimate was ~30× low — 141k grid lines × 81.6M-cell 111 m grid is heavy), but it's ONCE per grid so the full run is **~2.9 h total** — feasible (vs 68 h). Ran it (your GO-without-waiting condition — per-raster timings good — was met).

**Re-smoke + full-run timings:**
```
load + static totals ..................... ~155s   (grid tag join ~86s)
GFM rasterizeGeom precompute (×3 rasters).  2553s  (~42.5 min, ONCE)
GFM per-raster zonal ..................... ~42s each  × 94  = ~66 min
JRC rasterizeGeom precompute (×3) ........  3544s  (~59 min, ONCE)
JRC per-raster zonal ..................... ~37s each  × 7   = ~4 min
FULL RUN TOTAL ........................... 10,473s = ~2h 55m
```
Per-raster is now seconds ✓. Precompute dominates — if you want it faster later, the `rasterizeGeom` is the only lever (e.g. clip lines to per-adm2 tiles, or a coarser length grid); not needed now.

**Outputs (→ `Data/exposure/intersect/`):**
```
exposure_gfm_seasonal.parquet  A = 27,260 rows  (94 GFM season×year × 290 adm2)  361 KB
exposure_jrc_rp.parquet        B =  2,030 rows  (7 RP × 290 adm2)                 68 KB
exposure_totals.parquet            290 rows                                       22 KB
```
**Sanity:** JRC monotone in RP (prone rp10 29,633 → rp500 36,450 km²; pop_exp rp10 680k → rp500 1.03M ✓). GFM big-flood seasons stand out (OND_2025 13,230 km² / pop 480k; NDJ_2022 7,242 km²). OND_2019 pop_exposed 68,836 vs pop_total 55.1M (~0.12%). `grid_km_exposed`/`_hv` populated, `observed_pct` clamped ≤1.

**PUBLISH tier 16:** dry-run 3 rows [ok] → full **3/3 in 4.7s**. COUNT-VERIFY **local=3 == S3=3** ✓. 206 + CORS * + Content-Range on the parquets (content-type binary/octet-stream — usual, non-blocking for DuckDB-WASM range reads). Key: `domain=exposure/type=intersect/region=kenya/processing=analysis-ready/`.

Pre-cooked flood×exposure tables **LIVE** — notebook can drop client-side intersect. Done.
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

### RESPONSE — cglabs 2026-09-05 #1 — SMOKE ran end-to-end (after 3 unblock-fixes). ⚠️ PERF INFEASIBLE — HOLDING for optimise. 🟠

Smoke produced correct-shaped outputs (A/B/totals = 290 rows each, schema matches spec). Paths all resolved as expected (no custom `--out` — everything under `<data>/exposure/…` + `<data>/flood_jrc/JRC`). **But the line-intersection perf is a hard blocker — do NOT green-light the full run yet.**

**3 fixes I had to apply to get through the smoke (please ratify):**
1. **env:** installed `lwgeom` 0.2.17 (cglabs lacked it; `st_length` on lon/lat needs it). GEOS/PROJ version-skew warnings vs sf, benign for length.
2. **bug — `tag_adm2(x, pt=TRUE)`** passed the *whole sf* as geometry (`g <- if (pt) x …`) → `st_sf(geometry=g)` empty join → **0 points tagged** → `he_tot[…, by=adm2_pcode]` "object 'adm2_pcode' not found". Fixed → `st_geometry(x)`.
3. **bug — HOTOSM health/schools are MIXED geometry** (health = 1976 POINT + 777 POLYGON + 1 LINE; schools = 9609 POINT + 5195 POLYGON + 13 MULTIPOLYGON — OSM building footprints, not just nodes). `vect(pts)` → "not all geometries transferred … geometry collection" coercion fail. Fixed → reduce each facility to one representative point at load (`st_point_on_surface(st_make_valid(x))`). (Facility count semantics = 1 point/facility, unchanged.)

**Timings (SMOKE = 1 GFM raster + 1 JRC raster):**
```
load adm2 + vectors ......................  12s
rasterize adm2->pop + STATIC totals ...... 130s   (incl. grid tag point-in-adm2 join ~86s — 141k lines, ONCE)
  totals sane: pop 55.1M, roads 46,883 km, grid 69,092 km, health 2748, schools 14801
A. GFM  AMJ_2018 (flooded 2,387 km2) .....  368s  (~6.1 min / raster)
B. JRC  rp100    (prone  34,201 km2) ..... 30,073s (~8.35 HOURS / raster)   <-- !!
```
**The bottleneck is exactly the one you flagged: `st_intersection(lines, flood_polygon)` for `roads_km_exposed`/`grid_km_exposed` (line 152/159).** It scales with flood *area* × polygon fragmentation: GFM observed flood is small (2,387 km²) → 6 min; JRC prone is 14× larger and highly fragmented (depth-cell polygonised) → **8.35 h**. The `as.polygons(mask)` → `st_union` → `st_intersection` over 141k grid lines is O(disaster) when the flood polygon is big.

**Full-run extrapolation (INFEASIBLE):**
```
GFM: 94 seasonal flooded rasters × ~6 min  ≈  9–10 h  (worse for high-flood seasons)
JRC:  7 return-periods       × ~8.35 h     ≈  58 h
TOTAL ≈ 68 h+  — not viable
```

**HOLDING.** Recommend the **rasterised-length approximation** (your option): burn roads/grid lines onto the flood grid once, compute per-cell line length, then `zonal`-sum over flooded cells × adm2 — turns the hours-long vector `st_intersection` into a seconds-long raster op (approximate, cell-resolution, but fine for a km-exposed metric). Alternatives you listed: pre-clip lines to the flood bbox before intersect, or drop `grid_km_exposed`/`roads_km_exposed` to v2. Your call — I'll re-run the smoke once you push the optimise, then do full + publish tier 16.

**Minor sanity note:** `observed_pct` comes out slightly >1 (e.g. 1.0002) — GFM obs-area (flood grid cellSize) ÷ adm2 area (pop grid) grid-mismatch rounding; worth a clamp/`min(.,1)` or denominator-align when you're in there. Everything else in the heads looks right (pop_exposed populated, pop_source=worldpop, HV grid col present).

**Not committing the full/publish. The 3 fixes above ARE committed** (needed for any run) — ratify or override.
