# DISPATCH — cglabs ⇄ macbook · Pipeline spatial-coverage probe (global vs Africa)

_Append-only. Newest on top. cglabs runs, appends RESPONSE, pushes; macbook reads._

Workstream: establish, dataset by dataset, **what the hazards / hazards_prototype pipeline could cover
globally and what is hard-bounded to Africa**. Two halves: (1) upstream *source* footprints — done on
macbook, results below, no cglabs work needed; (2) *on-disk* footprints + whether outputs actually carry
data outside Africa — cglabs only, since the data lives there. Read-only probe; nothing is written or
re-baked.

---

## [macbook 2026-08-19 #1] ACTION → cglabs: run `scripts/probe_pipeline_coverage.sh`, answer Q1–Q6

### Steps
1. `git pull` develop.
2. Run the probe (read-only, no writes, ~minutes — it stats dirs and gdalinfo's one sample per dataset):
   ```
   cd $project_dir
   mkdir -p logs
   bash scripts/probe_pipeline_coverage.sh > logs/coverage_probe_$(date +%Y%m%d_%H%M%S).log 2>&1
   tail -5 logs/coverage_probe_*.log        # expect "PROBE COMPLETE"
   ```
   Env overrides if the roots differ from the defaults: `COMMON=` (default `/home/jovyan/common_data`),
   `HPROTO=` (default `$COMMON/hazards_prototype`), `PREMIUM=` (default `/home/jovyan/shared-data-premium`).
   Validated on macbook against real GDAL (both the FILE and DIR branches, negative-lon sample points OK).
3. Paste the log **whole** into the RESPONSE block — I want the raw extents, not a summary.
4. Answer Q1–Q6 below. These are the parts the log alone cannot settle.

### Why extent ≠ coverage (the thing to watch)
Every `04_indices` script does `xtd <- ext(rast(chirps_wrld/chirps-v2.0.1981.01.01.tif))` then
`crop(x, xtd)`. Cropping to an extent **wider** than the input is a no-op, so an index tif can report a
global extent while every cell outside the Africa input footprint is NA. The probe therefore samples
values at INDIA (78,22), BRAZIL (-47,-15), USA (-98,39), SE_ASIA (105,15) with KENYA (37,0) as control.
**A dataset only counts as global if those samples return numbers, not NA.**

### Q1 — nexgddp daily: is the on-disk data actually global?
`R/02_preprocess_data/preprocess_nex-gddp-cmip6_daily_data_v2_0.R` does unit conversion + `terra::rotate`
and **no crop**, so `$COMMON/nex-gddp-cmip6/{var}/{ssp}/{gcm}/` should be global 0.25°. Confirm from the
log, then tell me **which vars × ssps × gcms are complete on disk** (the script's `vrs` is currently
`c('pr','sfcWind')` — I need to know whether `tasmax`/`tasmin`/`rsds`/`hurs` are present global too, or
only for some GCMs).

### Q2 — nexgddp indices: global extent, or global *data*?
For `$COMMON/nex-gddp-cmip6_indices` and `$COMMON/atlas_nex-gddp_hazards/cmip6/indices`: report the
sample-point values per index. Specifically which of PTOT / TAVG / TMAX / TMIN / NTx35 / NDD / NDWS /
NDWL0 / NDWL50 / THI / HSH / WBGT / TAI / PET have **non-NA outside Africa**. If some do and some don't,
that difference is the answer to the whole question — name the split.

### Q3 — soils: the suspected hard cap on the water-balance indices
`fast_calc_NDWS/NDWL0/NDWL50` read `atlas_hazards/soils/{sscp_world,ssat_world}.tif`. Names say world.
Report their **extent, resolution and sample-point values**. If they are Africa-only, the water-balance
family cannot go global without new soils — that is a blocker I need named explicitly.

### Q4 — raw observed: what years are actually on disk?
- `$COMMON/chirps_wrld` — n files + first/last date on disk (source is global -180..180/-50..50, 1981–).
- `$COMMON/chirts/{Tmax,Tmin,RHum}` — n files + year range. Source CHIRTSdaily v1.0 is global but stops
  at **2016**; confirm what you hold.
- `$COMMON/ecmwf_agera5*` — is raw AgERA5 solar radiation held globally or Africa-cut? `rsds` feeds
  NDWL/TAI/PET, so an Africa-only AgERA5 caps those even when nexgddp is global.

### Q5 — cost of going global
From section G (`du -sh`): current size of `nex-gddp-cmip6`, `nex-gddp-cmip6_indices`, `chirps_wrld`,
`chirts`, `chirps_cmip6_africa`, the two indices trees. Plus **free space on the volume** (`df -h` on the
common_data mount). I want an order-of-magnitude on a global vs Africa index bake before anyone scopes one.

### Q6 — any other Africa-bound input inside 02→04 I have missed?
You know the tree better than the code does. Anything in the producer chain that is Africa-shaped by
construction (crop calendar, SoS, masks, water bodies, AEZ) and would block or silently blank a
non-Africa bake — list it with its path and extent.

### RESPONSE block (append, then push)
```
probe run: log = logs/coverage_probe_<stamp>.log   completed = yes/no
--- paste full log here ---

Q1 nexgddp daily global = yes/no   extent = ?   vars complete on disk = ?   ssps = ?   gcms = ?/18
Q2 indices with non-NA OUTSIDE Africa = ?        indices Africa-only-in-data = ?
Q3 sscp/ssat extent = ?   sample values outside Africa = ?   -> water balance global-capable = yes/no
Q4 chirps_wrld files/date range = ?   chirts vars/year range = ?   agera5 raw footprint = ?
Q5 du: nexgddp=? indices=? chirps_wrld=? chirts=? africa trees=?   free space = ?
Q6 other Africa-bound inputs = ?
```

### What macbook already established (source side — no cglabs work needed)
Verified today by HTTP + `gdalinfo /vsicurl` against data.chc.ucsb.edu, and by reading the pipeline code.

| Dataset (source) | URL path | Extent | Res | Span |
|---|---|---|---|---|
| CHIRPS v2.0 global daily | `products/CHIRPS-2.0/global_daily/tifs/p05` | -180..180, **-50..50** (7200×2000) | 0.05° | 1981– |
| CHIRPS v3.0 monthly **global** | `products/CHIRPS/v3.0/monthly/global/tifs` | -180..180, **-60..60** (7200×2400) | 0.05° | 1981.01–2026.07 |
| CHIRPS v3.0 monthly africa | `…/monthly/africa/tifs` | -20..55, -40..40 (1500×1600) | 0.05° | same |
| CHIRPS v3.0 monthly latam | `…/monthly/latam/tifs` | -120..-34, -60..35 | 0.05° | same |
| CHIRPS v3.0 other cadences | `v3.0/{daily/final,daily/prelim,pentads,dekads,2..6-monthly,annual}` | africa/global/latam each | 0.05° | 1981– |
| CHIRTSdaily v1.0 | `products/CHIRTSdaily/v1.0/global_tifs_p05/{Tmax,Tmin,RHum,HeatIndex,svp,vpd}` | -180..180, **-60..70** (7200×2600) | 0.05° | 1983–2016 |
| CHIRTS-ERA5 monthly (experimental) | `experimental/CHIRTS-ERA5/{tmax,tmin}/tifs/monthly` | -180..180, -60..70 | 0.05° | 1980.01–2026.07 |
| NEX-GDDP-CMIP6 | premium share NetCDF | global | 0.25° | hist 1950–2014, ssp 2015–2100 |

**Every upstream source is global.** Nothing in CHIRPS / CHIRTS / nexgddp is Africa-only at source —
the Africa restriction is ours, introduced at three places in our own code:

1. **`03_bias_correction/getDailyFutureData.R`** — `crop(ext(ref)) |> mask(ref)` against
   `atlas_hazards/roi/africa.tif`, writing `chirps_cmip6_africa` / `chirts_cmip6_africa` /
   `ecmwf_agera5_cmip6_africa`. So **climdat_source=atlas_delta is hard Africa-only**, by construction.
   `climdat_source=nexgddp` never passes through this step.
2. **`R/observational/1_get_chirps_chirts.R`** — PTOT points at the CHIRPS v3 **africa/** directory and
   everything is cropped to `metadata/base_raster_obs.tif` (= the CHIRPS africa grid, -20..55/-40..40).
   The global CHIRPS v3 monthly directory exists and is live (checked: HTTP 200, 7200×2400) — swapping
   `africa/` → `global/` is a one-line source change plus a new obs base raster. TMAX/TMIN already come
   off the **global** CHIRTS-ERA5 grid and are then cut down to Africa.
3. **Consumer exposure + economics** — `Data/mapspam/2020V1r2_SSA` (SSA only) and the FAOStat bulk
   downloads in `0_server_setup.R`, which are all `*_E_Africa.zip` (prices, production, VoP, trade).
   GLW4 is global. `atlas_data$boundaries$params$region` already offers `global` and `africa`; we use
   `region[[2]]` = africa.

Also already known, no probe needed:
- `metadata/base_rast_nexgddp.tif` (committed) = **1440×400, 0.25°, -180..180, -50..50 — global band**,
  and `0_server_setup.R` builds it as `crop(nexgddp pr tif, ext(-180,180,-50,50))`. The nexgddp consumer
  grid is already global-in-longitude; the 50°N/S clip comes from the CHIRPS v2 extent, not from Africa.
- `metadata/base_raster.tif` (atlas_delta) = 1663×1739, 0.05°, -25.35..57.8, -46.95..40 — Africa.
- `metadata/base_raster_obs.tif` = 1500×1600, 0.05°, -20..55, -40..40 — CHIRPS africa grid.
- **Stale, flag only:** `01_download_data/download_chirps.R` writes to
  `//catalogue/WFP_ClimateRiskPr1/1.Data/Chirps` (Windows UNC) with a hardcoded 2024-10-01..2025-01-31
  window, and `download_chirts.R` defaults to `years=1983:2016`. Neither is what feeds current bakes —
  consistent with the standing note that 01 is legacy. No action, just don't trust them as the coverage story.

Nothing here changes any bake. After your RESPONSE I'll fold both halves into a single coverage matrix
(per dataset: source footprint / on-disk footprint / data-present footprint / what blocks global) so the
"can the Atlas go beyond Africa" question has one document instead of a code read.
