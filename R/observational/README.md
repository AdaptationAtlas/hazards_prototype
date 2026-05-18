# R/observational — Atlas observational climate pipeline

End-to-end pipeline that turns UCSB CHC's monthly **CHIRPS v3** precipitation
and **CHIRTS-ERA5** temperature products into the on-disk artefacts the
Climate Rationale notebook reads for the observational track:

- pixel-level monthly GeoTIFFs (one per variable per month)
- per-pixel **drought index** (SPEI) at five accumulation scales
- **admin-aggregated** monthly parquet (mean + sd per polygon)
- **annual + seasonal** parquet (one row per zone × year × period × variable)
- **per-pixel climatology COGs** for map rendering (mean / min / max / sd over
  three reference windows, 13 calendar periods, all variables)

Five scripts in this folder, designed to run in order. Each is idempotent
(skip-if-present at output level), each has a `--smoke` and a `--full` mode,
each prints progress with `[HH:MM:SS]` timestamps that flush immediately so a
detached `nohup … > log.txt 2>&1 &` shows progress live in `tail -f log.txt`.

## What the data is

| Variable | Source | Native unit | Coverage | Notes |
|---|---|---|---|---|
| `PTOT` | CHIRPS v3 monthly Africa | mm/month | 1981-01 → present | Station-merged satellite precipitation. |
| `TMAX` | CHIRTS-ERA5 monthly | °C | 1980-01 → present | ERA5-blended monthly maximum temperature. |
| `TMIN` | CHIRTS-ERA5 monthly | °C | 1980-01 → present | ERA5-blended monthly minimum temperature. |
| `TAVG` | derived `(TMAX + TMIN) / 2` | °C | 1980-01 → present | Computed by script 1. |
| `SPEI-01 / 03 / 06 / 12 / 24` | derived | dimensionless | 1981-01 → present | Standardized Precipitation Evapotranspiration Index. Hargreaves PET (FAO-56) → CWB = P − PET → SPEI::spei() with log-Logistic / ub-pwm distribution fit on the 1991-2020 reference period. |

Everything lives at the **native CHIRPS 0.05° grid**, extent Africa
(−20°..55° lon, −40°..40° lat). The grid template is
`metadata/base_raster_obs.tif`, committed to the repo (40 KB) and built once
by script 1 on first smoke run.

## Pipeline narrative

```
                  raw .tif on CHC                              on-disk outputs
                  ────────────────                             ───────────────

 ┌──────────────────────────────────┐
 │ 1_get_chirps_chirts.R            │  scrape CHC directory,
 │                                  │  download monthly tifs,         Data/chirts_chirps_hist/
 │                                  │  sentinel-mask, crop+resample,  ├── PTOT/   PTOT-YYYY-MM.tif (+ _metadata.json)
 │                                  │  write COGs                     ├── TMAX/   TMAX-YYYY-MM.tif
 │                                  │                                 ├── TMIN/   TMIN-YYYY-MM.tif
 │                                  │  TAVG = (TMAX + TMIN) / 2       ├── TAVG/   TAVG-YYYY-MM.tif
 │                                  │                                 └── manifest.csv
 └──────────────────────────────────┘
                  │
                  ▼
 ┌──────────────────────────────────┐
 │ 2_calculate_obs_spei.R           │  Hargreaves PET (FAO-56)        Data/chirts_chirps_hist/
 │                                  │  CWB = PTOT − PET               ├── SPEI-01/  SPEI-01-YYYY-MM.tif
 │                                  │  SPEI::spei() per pixel,        ├── SPEI-03/  ...
 │                                  │  fit ref period 1991-2020       ├── SPEI-06/
 │                                  │                                 ├── SPEI-12/
 │                                  │  5 scales: 1, 3, 6, 12, 24      └── SPEI-24/
 └──────────────────────────────────┘
                  │
                  ▼
 ┌──────────────────────────────────┐
 │ 3_extract_obs_admin.R            │  zonal mean + sd per polygon    Data/chirts_chirps_hist/admin/
 │                                  │  for all 9 variables            ├── obs_monthly_adm0.parquet  (long: monthly)
 │                                  │  admin0 + admin1 by default     └── obs_monthly_adm1.parquet
 │                                  │  (admin2 opt-in flag)
 └──────────────────────────────────┘
                  │
                  ▼
 ┌──────────────────────────────────┐
 │ 4_aggregate_obs_admin_periods.R  │  collapse months within each    Data/chirts_chirps_hist/admin/
 │                                  │  period to one value/year using ├── obs_periods_adm0.parquet  (long: annual+seasonal)
 │                                  │  the variable's natural rule    └── obs_periods_adm1.parquet
 │                                  │  PTOT=sum, TMAX=max, TMIN=min,
 │                                  │  TAVG / SPEI=mean
 └──────────────────────────────────┘

 ┌──────────────────────────────────┐
 │ 5_make_obs_map_climatologies.R   │  step A: per-year aggregate     Data/chirts_chirps_hist/maps/
 │ (parallel branch, reads same     │          (within-window agg)    └── {variable}/
 │  monthly tifs as script 3)       │  step B: per-pixel mean/min/        ├── {VAR}_{period}_{clim}_mean.tif
 │                                  │          max/sd across years        ├── {VAR}_{period}_{clim}_min.tif
 │                                  │                                     ├── {VAR}_{period}_{clim}_max.tif
 │                                  │  9 vars × 13 periods ×              └── {VAR}_{period}_{clim}_sd.tif
 │                                  │  3 clim windows × 4 stats           (1,404 COGs)
 └──────────────────────────────────┘
```

## Key concepts

**Periods** (`period` column in admin parquets, `_{period}_` token in COG
filenames):

| Name | Months | Notes |
|---|---|---|
| `annual` | 1..12 | Full calendar year. Needs all 12 months present. |
| `JFM` `FMA` `MAM` `AMJ` `MJJ` `JJA` `JAS` `ASO` `SON` `OND` | 3-month rolling windows within a year | Need all 3 months present. |
| `NDJ` `DJF` | 3-month windows that **wrap year boundaries** | December rows are re-tagged to the year containing January, so DJF 2010 = Dec 2009 + Jan 2010 + Feb 2010. |

**Aggregation rule per variable** (within a period: months → one value):

| Variable | Rule | Why |
|---|---|---|
| `PTOT` | sum | Total rainfall (mm) over the period |
| `TMAX` | max | Warmest monthly maximum |
| `TMIN` | min | Coldest monthly minimum |
| `TAVG` | mean | Average daily mean temperature |
| `SPEI-*` | mean | SPEI is already standardised; arithmetic mean of standard scores |

**Climatology windows** (for the map COGs, `_{clim}_` token):

| Name | Years | Source |
|---|---|---|
| `atlas` | 1995-2014 | CMIP6 historical baseline used elsewhere in this pipeline |
| `wmo`   | 1991-2020 | WMO / IPCC AR6 reference period (also the SPEI fit window) |
| `full`  | 1981 → latest | Every available year |

**Standardization details (SPEI):** log-logistic distribution per pixel,
unbiased PWM fit, **reference period 1991-2020 hard-coded**. Tail pixel-months
can land at `±Inf` (CWB outside the fitted distribution); these are masked to
NA before any downstream zonal or climatology aggregation. The smoke run on
Kenya found ~0.018% of cell-months at Inf — benign tail effect.

## Running the pipeline

All scripts run from the project root via `Rscript`. They auto-detect the
correct `working_dir` per machine and (for `--full`) source
`R/0_server_setup.R`; `--smoke` uses a `bootstrap_minimal()` helper that
sidesteps the upstream pipeline's heavy startup downloads (mapspam, FAOSTAT,
etc.) so smoke runs in seconds.

```sh
# In order. Each --smoke is fast; each --full is heavy.
Rscript R/observational/1_get_chirps_chirts.R               --smoke
Rscript R/observational/1_get_chirps_chirts.R               --full   # ~30-60 min, ~10 GB
Rscript R/observational/2_calculate_obs_spei.R            --smoke
Rscript R/observational/2_calculate_obs_spei.R            --full   # ~1-2 h
Rscript R/observational/3_extract_obs_admin.R             --smoke
Rscript R/observational/3_extract_obs_admin.R             --full   # ~15-30 min
Rscript R/observational/4_aggregate_obs_admin_periods.R   --smoke
Rscript R/observational/4_aggregate_obs_admin_periods.R   --full   # <5 min
Rscript R/observational/5_make_obs_map_climatologies.R    --smoke
Rscript R/observational/5_make_obs_map_climatologies.R    --full   # ~1-2 h
```

Each `--smoke` runs inline verification checks (file present, schema,
distribution, range, COG integrity, PNG round-trip) and exits 0/1. Don't run
`--full` until the corresponding `--smoke` passes.

For long-running `--full` jobs, detach and tail:

```sh
nohup Rscript R/observational/5_make_obs_map_climatologies.R --full > /tmp/clim.log 2>&1 &
disown
tail -f /tmp/clim.log
```

## Output schemas

### Monthly admin parquet (`obs_monthly_adm{0,1}.parquet`)

| Column | Type | Notes |
|---|---|---|
| `iso3` | factor | ISO 3166-1 alpha-3 country code |
| `admin0_name` | factor | |
| `admin1_name` | factor | NA at adm0 |
| `admin2_name` | factor | NA at adm0 / adm1 |
| `gaul0_code`, `gaul1_code`, `gaul2_code` | int | GAUL 2024 codes (NA where not applicable) |
| `year` | int | |
| `month` | int | 1..12 |
| `variable` | factor | One of the 9 variables |
| `value_mean` | double | Spatial mean within polygon (zonal mean) |
| `value_sd` | double | Spatial sd within polygon at that month |

### Periods admin parquet (`obs_periods_adm{0,1}.parquet`)

Same schema as monthly, but `month` is replaced by `period` (factor with 13
levels: `annual`, `JFM`, ..., `DJF`). `value_mean` is aggregated by the
variable's rule (PTOT=sum, TMAX=max, TMIN=min, others=mean across months).
`value_sd` is the mean of monthly value_sds across the period — a
"typical spatial heterogeneity over the window".

### Climatology COGs (`maps/{variable}/{var}_{period}_{clim}_{stat}.tif`)

Per-pixel value. Float32 COG, DEFLATE / PREDICTOR=2 / BLOCKSIZE=512.
Filename encodes the four dimensions:

- `{variable}`: `PTOT`, `TMAX`, ..., `SPEI-24`
- `{period}`: `annual`, `JFM`, ..., `DJF`
- `{clim}`: `atlas_1995-2014`, `wmo_1991-2020`, `full_1981-<latest>`
- `{stat}`: `mean`, `min`, `max`, `sd`

## Dependencies

- 0_server_setup.R sets `project_dir`, `working_dir`, atlas_dirs, the GAUL
  boundary file list, and the lazy CRAN mirror. `--full` modes source it;
  `--smoke` modes don't.
- `metadata/base_raster_obs.tif` is committed and used by script 3 and
  script 5 for grid alignment.
- R packages: `terra`, `data.table`, `arrow`, `httr2`, `rvest`, `SPEI`,
  `jsonlite`, `glue`, `future`, `future.apply`, `furrr`, `progressr`,
  `digest`, `fs`, `sf`, `geoarrow`, `pacman`. Auto-installed via `pacman`
  on first run.

## Operational notes

- **Idempotency.** Re-running a script with all outputs already on disk
  exits in seconds; the `--smoke` verification block exercises the existing
  outputs without recomputing.
- **OS-aware parallel.** Where parallelism is used (`terra::app(cores=N)` in
  script 2, `furrr::future_map` in script 1), Linux runs forked `multicore`
  workers; Mac / Windows fall back to `multisession`.
- **Path resolution on CGlabs.** `0_server_setup.R` resolves `working_dir`
  from `project_dir` plus `climdat_source` ("atlas_delta" vs "nexgddp").
  Each script's `bootstrap_minimal()` auto-detects which of the two CGlabs
  paths contains the data and uses that one — so smoke and full modes pick
  up the same on-disk state regardless of which `climdat_source` is set in
  the global session.
- **Inf handling.** SPEI can emit `±Inf` at the tails. The downstream scripts
  (script 3, script 5) mask Inf to NA before zonal / climatology
  aggregations so a single tail pixel doesn't poison a whole polygon or
  climatology layer. The raw SPEI COGs themselves retain Inf as the
  faithful output of `SPEI::spei()`.

## Status as of this commit

- All 5 scripts: built, styler-clean, 0 lints, on `develop`.
- script 1 and script 2: smoke + full have been run on CGlabs successfully.
- script 3: smoke + full have been run on CGlabs successfully.
- script 4 and script 5: smoke + full pending on CGlabs.

Next dispatches (out of scope here): S3 publishing of these artefacts;
notebook consumption in `atlas_notebooks`.
