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
- **public S3 publish** of the admin parquets + climatology COGs + base raster

Six scripts in this folder, designed to run in order. Each is idempotent
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
                  │
                  ▼
 ┌──────────────────────────────────┐
 │ 6_publish_obs_to_s3.R            │  AtlasDataManageR::S3DirUploader  s3://digital-atlas/
 │ (reads outputs of 3, 4, 5 +      │  Hive-partitioned layout          ├── domain=climate/.../processing=admin-monthly/
 │  metadata/base_raster_obs.tif)   │                                   ├── domain=climate/.../processing=admin-periods/
 │                                  │  Tier 1: admin parquets + base    ├── domain=climate/.../processing=climatology/
 │                                  │  Tier 2: 1,404 climatology COGs   │   variable=.../period=.../clim=.../stat=...
 │                                  │  Tier 3: out of scope             └── domain=boundaries/.../processing=base-raster/
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

**Climatology windows** (for the map COGs, `_{clim}_` token in the filename):

| On-disk label | Years | S3 partition value (script 6) | Source |
|---|---|---|---|
| `1995-2014` | 1995-2014 | `atlas_1995-2014` | CMIP6 historical baseline used elsewhere in this pipeline |
| `1991-2020` | 1991-2020 | `wmo_1991-2020` | WMO / IPCC AR6 reference period (also the SPEI fit window) |
| `full`      | 1981 → latest | `full_record` | Every available year |

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
Rscript R/observational/6_publish_obs_to_s3.R              --dry-run
Rscript R/observational/6_publish_obs_to_s3.R              --smoke
Rscript R/observational/6_publish_obs_to_s3.R              --full   # ~10-30 min
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

### Parallelism & resource control

Scripts **1, 2, 3, 5, 6** accept the standard parallel flags. Script 4 runs
in seconds and doesn't need any.

| Flag | Default | Meaning |
|---|---|---|
| `--workers N` | (auto) | Explicit worker count. Overrides everything else. |
| `--cpu-fraction X` | `0.5` | Fraction of logical cores to use. |
| `--mem-fraction X` | `0.5` | Fraction of free RAM to use. |
| `--mem-budget G` | (auto) | Explicit memory budget in GB. Overrides `--mem-fraction`. |
| `--overwrite` | off | Rebuild outputs even when already on disk (scripts 3, 5, 6). |

Auto-resolution: `min(cpu_fraction × cores, mem_fraction × free_RAM /
per_worker_gb)`, clamped to each script's `max_workers`. Each script encodes
its own peak per-worker RAM estimate:

| Script | per-worker RAM | Why |
|---|---|---|
| 1 — download | ~0.5 GB | I/O-bound; small raster work area |
| 2 — SPEI | ~2 GB | `terra::app` streams blocks |
| 3 — admin extract | ~50 GB | Holds a 544-layer raster stack in memory for zonal passes |
| 5 — climatology | ~10 GB | One worker per variable; terra streams the per-year reductions |
| 6 — publish to S3 | ~0.2 GB | I/O-bound; small in-memory upload buffers |

At startup each script prints a banner so you can verify the resolved config
before the heavy work begins:

```
[extract] workers=6  per_worker_gb~30.0  cores=40/40 (logical/physical)  RAM=320.4 free / 377.0 total GB
```

Examples:

```sh
# Defaults: scales to 50% of cores, 50% of free RAM, takes the min.
Rscript R/observational/3_extract_obs_admin.R --full

# Pin a specific worker count.
Rscript R/observational/3_extract_obs_admin.R --full --workers 8

# Be polite on a shared node — use 25% of cores.
Rscript R/observational/3_extract_obs_admin.R --full --cpu-fraction 0.25

# Use 80% of cores AND 80% of free RAM (worker count = min of the two).
Rscript R/observational/2_calculate_obs_spei.R  --full --cpu-fraction 0.8 --mem-fraction 0.8
```

To inspect what's actually running while a script executes:

```sh
htop -p $(pgrep -f "3_extract_obs_admin")
ps -o pid,user,rss,vsz,pcpu,pmem,comm -p $(pgrep -f "3_extract_obs_admin")
```

The main R process at ~100% CPU is the parent; child R processes at 0% CPU
are forked workers (Linux) sharing memory via copy-on-write. RAM accounting
in `ps`/`htop` over-counts shared pages — true incremental cost per worker is
much less than the parent's RSS.

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
Filename is 4 underscore-tokens encoding the four dimensions:

- `{variable}`: `PTOT`, `TMAX`, ..., `SPEI-24`
- `{period}`: `annual`, `JFM`, ..., `DJF`
- `{clim}`: `1995-2014`, `1991-2020`, `full` (script 6 re-labels these to
  `atlas_1995-2014` / `wmo_1991-2020` / `full_record` when publishing to S3)
- `{stat}`: `mean`, `min`, `max`, `sd`

Example: `PTOT_annual_1991-2020_mean.tif`, `SPEI-03_NDJ_full_sd.tif`.

## Script 6 — publish to S3

Publishes the analysis-ready artefacts from scripts 3, 4, 5 + the base raster
to the public `digital-atlas` bucket using the Hive-partitioned layout that's
canonical for newer Atlas datasets (FAOSTAT, hazard × exposure, GLW4). Built
on `AtlasDataManageR::S3DirUploader`, so it reuses the same uploader Brayden
uses for hazard × exposure outputs.

**S3 path table:**

```
Tier 1 — admin parquets + base raster (5 files)

  s3://digital-atlas/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/
    processing=admin-monthly/variable=adm{0,1}_obs.parquet
    processing=admin-periods/variable=adm{0,1}_obs.parquet

  s3://digital-atlas/domain=boundaries/type=raster/source=chirps-grid/region=africa/
    processing=base-raster/base_raster_obs.tif

Tier 2 — climatology COGs (1,404 files)

  s3://digital-atlas/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/
    processing=climatology/
      variable={PTOT|TMAX|TMIN|TAVG|SPEI-01|SPEI-03|SPEI-06|SPEI-12|SPEI-24}/
      period={annual|JFM|...|DJF}/
      clim={atlas_1995-2014|wmo_1991-2020|full_record}/
      stat={mean|min|max|sd}/
      {VAR}_{period}_{clim}_{stat}.tif
```

Per-pixel monthly + SPEI COGs (Tier 3, ~13,500 files, ~50 GB) are **not**
published — they stay on Afrilabs/CGlabs only. Revisit if a downstream
consumer materialises.

**Run modes:**

| Mode | Effect |
|---|---|
| `--dry-run` | No network. Walks local files and writes `Data/chirts_chirps_hist/_publish_dry_run.csv` with `(tier, upload_id, local_path, size, s3_uri)` rows. Lets you eyeball every target path before committing to an upload. |
| `--smoke` | Uploads exactly one file (`obs_monthly_adm0.parquet`) and runs four inline checks: arrow round-trip, S3 listing, anonymous-read ACL, audit report. **Stop here, surface to a human reviewer before `--full`.** |
| `--full` | Uploads every file in the selected tier(s). Idempotent (default `overwrite = FALSE` skips files already on S3). |

**Flags:**

| Flag | Default | Meaning |
|---|---|---|
| `--tier {1\|2\|all}` | `all` | Restrict to Tier 1 only (admin + base raster, fast), Tier 2 only (climatology COGs, the bulk of the bytes), or both. `--smoke` always means Tier 1. |
| `--overwrite` | off | Re-upload files already on S3. |
| `--workers N` | (auto) | Explicit worker count for parallel upload. |
| `--cpu-fraction X`, `--mem-fraction X`, `--mem-budget G` | (auto) | Auto-resolution helpers; uploads are I/O-bound so per-worker RAM is ~0.2 GB and the cap is ~16 workers. |

**Verification protocol:**

1. `Rscript R/observational/6_publish_obs_to_s3.R --dry-run` and inspect the CSV.
2. `Rscript R/observational/6_publish_obs_to_s3.R --smoke` and confirm all 4 checks pass.
3. **Stop. Show the smoke URI + check results to a reviewer before running `--full`.**
4. After approval: `Rscript R/observational/6_publish_obs_to_s3.R --full`.

**AWS credentials:** required for `--smoke` and `--full`. The script checks
for `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` env vars OR
`~/.aws/credentials` at startup and exits with a clear message if both are
missing. `--dry-run` skips this check.

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
- Script 6 additionally needs `AtlasDataManageR`
  (`github.com/AdaptationAtlas/data-management/R/AtlasDataManageR`) and
  `s3fs`. AtlasDataManageR is auto-installed via `remotes::install_github`
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

- All 6 scripts: built, styler-clean, 0 lints.
- Scripts 1, 2, 3: smoke + full have been run on CGlabs successfully.
- Scripts 4, 5: smoke + full pending on CGlabs.
- Script 6: drafted on `feat/observational-publish-to-s3`; needs `--dry-run`
  + `--smoke` verification on CGlabs before `--full` runs.

Next dispatches (out of scope here): notebook consumption in
`atlas_notebooks` (CR-062 Phase A) — unblocked once script 6 `--full` lands
the S3 paths.
