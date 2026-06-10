# 📘 Atlas Hazard Layers – Processing Update (May 2025)

*Last updated: May 2025*\
*Project*: Africa Agriculture Adaptation Atlas (AAAA)\
*Prepared by*: Pete Steward ([p.steward\@cgiar.org](mailto:p.steward@cgiar.org){.email})\
*Storage*: All files are stored in the **CGLabs server common data folder** (not yet uploaded to S3)\
*Repository*: <https://github.com/AdaptationAtlas/hazards_prototype>

------------------------------------------------------------------------

## Table of Contents

-   [1) General Notes](#1-general-notes)
-   [Generic Hazard Simplification](#generic-hazard-simplification)
-   [2) Exposure and Value Datasets](#2-exposure-and-value-datasets)
    -   [2.1 Processed Livestock Data – GLW4](#21-processed-livestock-data--glw4)
    -   [2.2 Processed Crop Data – MapSPAM 2020 (SSA only)](#22-processed-crop-data--mapspam-2020-ssa-only)
    -   [2.3 Unified Exposure Tables](#23-unified-exposure-tables)
-   [3) Folder Summaries](#3-folder-summaries)
    -   [3.1 `hazard_risk/annual/`](#31-hazard_riskannual)
    -   [3.2 `hazard_timeseries_mean/annual/`](#32-hazard_timeseries_meanannual)
    -   [3.3 `hazard_timeseries_class/annual/`](#33-hazard_timeseries_classannual)
    -   [3.4 `hazard_timeseries_int/annual/`](#34-hazard_timeseries_intannual)
    -   [3.5 `hazard_timeseries_risk/annual/`](#35-hazard_timeseries_riskannual)
    -   [3.6 `hazard_timeseries_mean_month`](#36-hazard_timeseries_mean_month)
-   [4) Next Steps](#4-next-steps)

------------------------------------------------------------------------

## 1) General Notes

-   All outputs remain under `hazards_prototype/Data/`, with the original folder structure and seasonal subfolders unchanged.
-   Scripts 2 and 3 are now modular and can be controlled via in-script flags:
    -   [Script 2 – calculate_haz_freq.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/2_calculate_haz_freq.R)
    -   [Script 3 – freq_x_exposure.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/3_freq_x_exposure.R)
-   Scripts are launched using `Rscript` from terminal (not Rstudio).
-   Upload logic to S3 still needs to be implemented in Scripts 2 and 3. File structure should follow `metadata/data.json`.
-   Extractions now use zonal rather than vector based extractions.

### Generic Hazard Simplification {#generic-hazard-simplification}

-   **Previously**, generic hazards were duplicated across all crop-specific raster stacks, even though the hazard values were identical.
-   **Now**, this has been simplified: **only the `generic-crop` contains the generic hazard layers**.
-   In **Script 3 – Section 4**, when hazard frequency is intersected with exposure:
    -   The **generic hazard frequency** layer is intersected with **each crop's exposure** values.
    -   The `generic-crop` entry is intersected with the **total production value**.
-   These are the only files retained from this step.

## Conventions and Structure

-   File names use `_` to separate logical components and `-` only for compound terms (e.g. `generic-crop`).
-   Model names (e.g. `ACCESS-ESM1-5`) are included in all filenames.
-   `hazard_vars` combinations are embedded in filenames of interaction outputs.
-   All `.parquet` files now have `.json` documentation.
-   Parquet values are sorted and rounded to reduce file size and improve performance.

------------------------------------------------------------------------

## 2) Exposure and Value Datasets

This section describes the processed exposure datasets used in hazard × exposure intersections. All files are stored in subdirectories under `Data/`, organized by data source.

------------------------------------------------------------------------

### 2.1 Processed Livestock Data – GLW4

**Folder:** `Data/GLW4/processed/`

#### 📂 Raster files

| Filename | Description | Raster Layers (names) |
|------------------------|------------------------|------------------------|
| `livestock_number_number.tif` | Tropical & highland livestock counts | `cattle_tropical`, `sheep_highland`, etc. |
| `livestock_vop_intld2015.tif` | VOP in 2015 international dollars | Same layer structure as above |
| `livestock_vop_usd2015.tif` | VOP in 2015 USD | Same layer structure as above |

Each raster includes 12 layers:

-   **Tropical breeds:** `cattle_tropical`, `sheep_tropical`, `goats_tropical`, `pigs_tropical`, `poultry_tropical`, `total_tropical`
-   **Highland breeds:** `cattle_highland`, `sheep_highland`, `goats_highland`, `pigs_highland`, `poultry_highland`, `total_highland`

#### 📂 Parquet summary tables

| Filename | Description |
|------------------------------------|------------------------------------|
| `livestock_number_number_adm_sum.parquet` | Administrative sum of livestock numbers |
| `livestock_vop_intld2015_adm_sum.parquet` | Admin-level sum of VOP (int'l dollars) |
| `livestock_vop_usd2015_adm_sum.parquet` | Admin-level sum of VOP (USD) |

Each `.parquet` file contains these fields:

| Column        | Description                              |
|---------------|------------------------------------------|
| `iso3`        | Country ISO-3 code                       |
| `admin0_name` | Country name                             |
| `admin1_name` | Admin 1 region                           |
| `admin2_name` | Admin 2 district (if available)          |
| `crop`        | Livestock type (e.g., `cattle_tropical`) |
| `value`       | Count or value of production             |
| `exposure`    | `vop` or `number`                        |
| `unit`        | `number`, `usd`, or `intld`              |
| `stat`        | Aggregation type (usually `sum`)         |
| `tech`        | Not used in livestock (set as `NA`)      |

------------------------------------------------------------------------

### 2.2 Processed Crop Data – MapSPAM 2020 (SSA only)

**Folder:** `Data/mapspam/2020V1r2_SSA/processed/`

#### 📂 Raster files

Organized into folders by variable:

| Variable Folder          | Contents                                 |
|--------------------------|------------------------------------------|
| `variable=harv-area_ha/` | Harvested area rasters                   |
| `variable=phys-area_ha/` | Physical area rasters                    |
| `variable=prod_t/`       | Production (tonnes) rasters              |
| `variable=yield_kgha/`   | Yield rasters (kg/ha)                    |
| `variable=vop_intld15/`  | VOP (international dollars 2015) rasters |
| `variable=vop_usd2015/`  | VOP (USD 2015) rasters                   |

Each variable includes files like:

```         
spam_<variable>_<input>.tif
```

Where `<input>` is one of:

-   `all`, `irr`, `rf-all`, `rf-highinput`, `rf-lowinput`, `rf-subsistence`

Each raster contains 40+ crop-specific layers, including:

-   `wheat`, `maize`, `cassava`, `soybean`, `groundnut`, `arabica coffee`, `vegetables`, etc.

Use `terra::names(rast(file))` to inspect crop layers.

#### 📂 Parquet summary tables

Each raster variable includes multiple `.parquet` files with administrative aggregation. These are stored in folders like:

```         
Data/mapspam/2020V1r2_SSA/processed/variable=harv-area_ha/
```

Example filenames:

-   `spam_harv-area_ha_all_adm_sum.parquet`
-   `spam_vop_usd2015_rf-lowinput_adm_sum.parquet`

Each `.parquet` file includes:

| Column        | Description                                 |
|---------------|---------------------------------------------|
| `iso3`        | ISO-3 code                                  |
| `admin0_name` | Country                                     |
| `admin1_name` | Region                                      |
| `admin2_name` | District                                    |
| `crop`        | Crop name (e.g., `maize`, `tea`)            |
| `value`       | Harvest area / production / VOP             |
| `exposure`    | Exposure type (e.g., `harv-area`, `prod_t`) |
| `unit`        | `ha`, `t`, `usd`, etc.                      |
| `stat`        | Aggregation method (usually `sum`)          |
| `tech`        | Technology level (e.g., `all`, `irr`)       |

------------------------------------------------------------------------

### 2.3 Unified Exposure Tables

**Folder:** `Data/exposure/`

These harmonized files combine exposure information across both MapSPAM and GLW sources for hazard-risk overlays.

| Filename                   | Description                             |
|----------------------------|-----------------------------------------|
| `exposure_adm_sum.parquet` | All crop × exposure × tech combinations |
| `hpop_adm_sum.parquet`     | Human population exposure (GPW-derived) |

Schema for `exposure_adm_sum.parquet`:

| Column        | Description                       |
|---------------|-----------------------------------|
| `iso3`        | ISO-3 country code                |
| `admin0_name` | Country                           |
| `admin1_name` | Region                            |
| `admin2_name` | District                          |
| `crop`        | Crop or `generic-crop`            |
| `value`       | Exposure value                    |
| `exposure`    | e.g., `harv-area`, `prod_t`       |
| `unit`        | `ha`, `t`, `usd`, `number`        |
| `tech`        | e.g., `all`, `irr`, `rf-lowinput` |

------------------------------------------------------------------------

Let me know if you'd like to generate an automated index for all files or if Brayden needs a CSV of these folder structures.

## 3) Folder Summaries

### 3.1 `hazard_risk/annual/`

-   Parquet files summarizing hazard frequencies by administrative area.
-   Two types: `int` (interactions) and `solo` (single hazard variables).
-   Includes severity levels: `moderate`, `severe`, `extreme`.

**Parquet schema:**

| Column      | Description                         |
|-------------|-------------------------------------|
| iso3        | ISO country code                    |
| admin0_name | Country name                        |
| admin1_name | Admin 1 name                        |
| admin2_name | Admin 2 name                        |
| value       | Fraction of years meeting condition |
| scenario    | e.g. ssp126                         |
| model       | e.g. ACCESS-ESM1-5                  |
| timeframe   | e.g. 2021–2040                      |
| hazard      | e.g. heat, dry+wet                  |
| hazard_vars | Variable set used for condition     |
| crop        | Crop affected                       |
| severity    | Hazard threshold level              |

------------------------------------------------------------------------

### 3.2 `hazard_timeseries_mean/annual/`

-   Single-band `.tif` files: annual mean values over time windows.
-   Parquet summaries per model and timeframe with associated `.json`.

**Parquet schema:**

| Column      | Description            |
|-------------|------------------------|
| iso3        | ISO code               |
| admin0_name | Country                |
| admin1_name | Region                 |
| admin2_name | District               |
| value       | Mean hazard value      |
| scenario    | SSP (e.g., ssp245)     |
| model       | GCM or ensemble        |
| timeframe   | 20-year period         |
| hazard      | e.g., HSH-max-max      |
| stat        | Statistic (e.g., mean) |

------------------------------------------------------------------------

### 3.3 `hazard_timeseries_class/annual/`

-   Multi-band `.tif` files: classified hazard values by year.
-   Covers both **historic** (1995–2013) and **SSP scenario projections** (2021–2100).
-   No associated `.json` metadata.

**Filename pattern:**

```         
<scenario>_<model>_<timeframe>_<hazard>-<stat>-<threshold>.tif
```

Example:

```         
ssp245_MPI-ESM1-2-HR_2081-2100_PTOT-sum-L750.tif
```

------------------------------------------------------------------------

### 3.4 `hazard_timeseries_int/annual/`

-   Multi-layer `.tif` rasters of **hazard interactions**.
-   Each file = one combination of classified variables.
-   Layers represent interaction groupings (`dry`, `dry+heat`, etc.).

**Layer names example:**

```         
ssp126_MPI-ESM1-2-HR_2061-2080_dry+heat
```

------------------------------------------------------------------------

### 3.5 `hazard_timeseries_risk/annual/`

-   Single-layer `.tif` rasters representing **risk-weighted hazard values**.
-   Band name: `"mean"`

**Filename pattern:**

```         
<scenario>_<model>_<timeframe>_<hazard>-<stat>-<threshold>.tif
```

Example:

```         
ssp245_ACCESS-ESM1-5_2041-2060_PTOT-sum-L1700.tif
```

------------------------------------------------------------------------

### 3.6 `hazard_timeseries_mean_month`

Monthly hazard tables are generated by `R/2.1_create_monthly_haz_tables.R`. These represent a key intermediate product in the hazard processing pipeline. Outputs are saved in the `hazard_timeseries_mean_month/` folder in Parquet format.

#### Overview

The script follows a structured, multi-step workflow. Each step builds on the previous one to calculate meaningful summaries of climate hazard indicators, their anomalies, and temporal trends for subnational administrative units across Africa.

##### Step 1: Monthly Zonal Extraction (gridded → admin)

Raw gridded hazard data are first aggregated by **admin1 units** (e.g., counties, provinces) using zonal statistics (e.g., mean or sum) for each month, model, and scenario. This is controlled via the `extract_stat` parameter.

> 📁 These monthly admin-level summaries are not saved directly, but are used to build seasonal aggregations.

##### Step 2: Seasonal Aggregation (`season` column)

Each extracted monthly value is assigned to a rolling **3-month season** or annual period. The `season` column encodes these groupings (e.g., `DJF`, `MAM`, `Annual`). For each admin unit, model, and scenario, seasonal values are computed using: - **mean** (for temperature-type hazards), - or **sum** (for rainfall-type hazards).

> These aggregations are saved as `*_seasons.parquet` files.

##### Step 3: Anomaly Calculation (vs Baseline)

To quantify climate change signals, hazard values are compared to historical baselines (e.g., `1995`, `1981–2010`). The baseline is computed as the **average historical value** per admin × hazard × season grouping. Anomalies are then computed as:

```         
anomaly = value - baseline_value
```

This operation is applied to both future and baseline-period data to ensure internal consistency.

> Results with anomaly columns are saved as `*_anomaly-<baseline>_seasons.parquet`.

##### Step 4: Ensemble Statistics Across GCMs

To account for inter-model uncertainty, GCM-specific values are grouped and ensembled to compute: - **mean**, **min**, **max**, **SD** for `value` and `anomaly`.

These provide robust summaries of central tendency and model spread.

> Saved as `*_ensemble_seasons.parquet` (with `model` removed and `models` noted as a comma-separated list).

Additionally, a long-term average is computed **across years per model**, then ensembled again to obtain climatological summaries:

> Saved as `*_ensemble.parquet`.

##### Step 5: Trend Estimation

Time series of hazard values and anomalies are fitted with the **Theil–Sen estimator** per model × admin unit × hazard × season. This provides robust linear trends over time.

Metrics calculated include: - slope, intercept, confidence intervals, p-values, - estimated value at start/end year, - 5-year means at start and end, - 10-year change (`value_decade`), - mean changes in anomalies.

> Trend outputs are saved as: - `*_trends.parquet`: Model-specific trends\
> - `*_trends_ensemble.parquet`: Ensemble average trends\
> - `*_trends_ensemble_minimal.parquet`: Filtered subset for key hazards and stats

#### Output File Types

| Table Name Suffix | Description | Example Filename |
|------------------------|------------------------|------------------------|
| `_seasons.parquet` | Monthly values (or seasonal sums) for each model, year, and scenario. Includes anomalies vs baseline. | `haz_3months_adm_mean_2061-2080_anomaly-historic_seasons.parquet` |
| `_ensemble_seasons.parquet` | Same as above but ensembled across GCMs. Includes inter-model stats (mean, min, max, SD). | `haz_3months_adm_mean_2061-2080_anomaly-historic_ensemble_seasons.parquet` |
| `_ensemble.parquet` | Seasonal or annual values averaged over the entire time period. Represents long-term averages per GCM. | `haz_3months_adm_mean_2061-2080_anomaly-historic_ensemble.parquet` |
| `_trends.parquet` | Sen’s slope trend results per GCM per location. Includes slope, intercept, p-value, confidence interval. | `haz_3months_adm_mean_2061-2080_anomaly-historic_trends.parquet` |
| `_trends_ensemble.parquet` | Trend results averaged across GCMs. | `haz_3months_adm_mean_2061-2080_anomaly-historic_trends_ensemble.parquet` |
| `_trends_ensemble_minimal.parquet` | Filtered ensemble trend outputs for specific hazards and stats of interest. | `haz_3months_adm_mean_r2061-2080_anomaly-historic_trends_ensemble_minimal.parquet` |

#### Core Fields in All Tables

| Field Name | Description |
|------------------------------------|------------------------------------|
| `admin0_name` | Country name |
| `admin1_name` | First-level administrative unit |
| `scenario` | Emissions scenario (e.g., `ssp245`, `ssp126`) |
| `timeframe` | Future period label (e.g., `2021-2040`, `2041-2060`) |
| `model` | Climate model name (e.g., `MPI-ESM1-2-HR`); omitted in ensemble tables |
| `hazard` | Hazard variable (e.g., `PTOT`, `TAVG`, `HSH-max`) |
| `season` | 3-month window or annual label (e.g., `MAM`,`JFM`, `Annual`) |
| `baseline_name` | Name of the baseline used for computing anomalies (e.g., `1995-2014`, `1981-2024`) |
| `value` | Usually the monthly or seasonal hazard statistic (mean/sum depending on hazard) |
| `anomaly` | Difference between `value` and historical baseline average|

Additional fields in ensemble and trend tables include:

-   `mean`, `max`, `min`, `sd`: Statistical summaries across GCMs.
-   `value_slope`, `value_decade`, `value_diff`, etc.: Trend metrics using Sen’s slope method.

#### Notes

-   All tables are in `.parquet` format and are designed for efficient use with `arrow::read_parquet()` or in DuckDB.
-   Ensemble summaries are computed *after* anomalies are calculated to preserve variance structure across models.

------------------------------------------------------------------------

## 2026-06 Update — R/2.1 monthly pipeline (CR-119 fix, §3.4 trend speedup, ops lessons)

*Added 2026-06-10. Scope: `R/2.1_create_monthly_haz_tables.R`, producing
`hazard_timeseries_mean_month/*` for the NEX-GDDP-CMIP6 source on CGLabs.*

### CR-119 — `iso3` dropped from canonical outputs (FIXED)

The 2026-06-05 canonical publish broke every notebook reader: `iso3` was missing
from the schema (`Binder Error: column "iso3" not found`), files were ~14× too big,
and aggregate scans threw `TProtocolException: Invalid data`. Root causes + fixes:

-   **iso3 dropped** — the §3.3 *and* §3.4 aggregation `by=` clauses omitted `iso3`,
    and `write_parquet_pushdown` silently drops sort columns that aren't present.
    Fixed by adding `iso3` to the by-clauses in §3.3 (`data_ag`/`data_ag_ens`,
    commit `b83dd3f`) and §3.4 (`data_ex_trend_stats` + `data_ex_trend_stats_ens`,
    commit `9117450`). `iso3` is carried from the §3.2 seasons file and is 1:1 with
    `admin0_name`. **All `*_trends*.parquet` must be regenerated to gain iso3.**
-   **size** — the original diagnosis blamed the per-row `models` string. **This is wrong**
    (corrected 2026-06-10 via `parquet_metadata` footer reads, see
    `ISSUE_cr119_canonical_regression.md` + the climateRationale dispatch): `models`
    dict-encodes to ~0 MB. The real size driver on `ensemble_season_timeseries` is the
    **CR-060 quantile columns** + the 4 unused stat columns (`max/min/max_anomaly/min_anomaly`
    ≈ 45%). The fix is **per-iso3 hive partitioning + column pruning** on that file (a §3.3
    producer change), not `models` removal. Note also: **Future Projections reads
    `ensemble_season_timeseries`, not `*_trends*`** — the §3.4 trends regen here is orthogonal
    to FP (serves the future CR-117 consumer).
-   **Thrift corruption** — parallel §3.3 writers collided on a shared output path.
    §3.3 reverted to sequential `lapply` (single-digit minutes anyway). See
    `ISSUE_cr119_canonical_regression.md` for the full diagnosis + S3-versioning
    rollback procedure used as the acute fix.

### §3.4 trend computation — ~9 h/timeframe → minutes (speedups #1–#3)

Sen's-slope + Mann–Kendall trend fit over >10⁶ groups was the pipeline bottleneck.
Three numerically-identical speedups (validated to < `round3.4` by synthetic probes
`R/probe_*`):

-   **#1 baseline-invariant dedup** — `value`/`year` are identical across baselines
    for the same source file; only `intercept`/anomalies differ. §3.4 now iterates
    over distinct source `data` files (`source_groups`), computes the fit once, and
    recomputes only the baseline-dependent intercept per baseline via an `.EACHI` join.
-   **#2** — reuse the Theil–Sen fit from `yue_tfpw()` when TFPW isn't applied.
-   **#3 Rcpp single-pass kernel** (`R/trend_kernel.cpp` → `mk_sen_cpp`/`lag1_ac_cpp`)
    — replaces `trend::sens.slope` + `trend::mk.test` (two independent O(n²) Kendall
    passes) with one pairwise pass. **~63× per fit** (230µs→3.6µs @ n=24), ~13k
    groups/s on real data. Gated by `USE_TREND_KERNEL`; auto-falls-back to `trend::`
    if the kernel can't compile, or force off with `R21_DISABLE_TREND_KERNEL=1`.

See `ISSUE_sec3.4_trend_speedup.md` for full detail + validation table.

### ⚠️ Known bug — multisession §3.4 path is broken with the kernel

`future_lapply` over `source_groups` `FutureInterrupt`s immediately when the kernel is
on (suspected: concurrent per-worker `Rcpp::sourceCpp` racing the shared `R/.rcpp_cache`).
**Workaround (mandatory until fixed): set `R21_SEC3_4_SEQUENTIAL=1`** to force
`plan(sequential)`. Sequential + kernel ≈ 1.5–2.5 h for all 6 sources (vs ~a day on
`trend::`). Durable fix = package-ify the kernel (install as a tiny R package so workers
`library()` it instead of re-`sourceCpp`). Detail + local-test strategy in
`ISSUE_sec3.4_trend_speedup.md`.

### Running §3.4 standalone (current reliable recipe, CGLabs)

```bash
# pre-flight: confirm the node's I/O is healthy (it has wedged repeatedly under load)
uptime                                              # want single-digit load
timeout 5 cat ~/R/x86_64-pc-linux-gnu-library/4.5/Rcpp/include/Rcpp.h >/dev/null && echo ok
git checkout -- logs/ && git pull --rebase origin develop   # logs are tracked; drop local diffs first
R21_SEC3_4_SEQUENTIAL=1 FORCE_OVERWRITE=1 nohup bash scripts/r21_rerun.sh \
  --skip-sec2 --skip-sec3-1 --skip-sec3-2 --skip-sec3-3 > nohup.out 2>&1 &
```

### Ops lessons (CGLabs)

-   The shared CGLabs node (long uptime, noisy neighbours) intermittently **stalls I/O**:
    processes stick in uninterruptible `D` state, terminals freeze, even a tiny compile
    or `aws --version` hangs >1 h. This killed several runs (surfaced as
    `FutureInterruptError`, *not* a code bug). Recovery: Hub → Stop/Start My Server, and
    escalate to ops to drain/cordon the node if a restart reschedules onto the same one.
    **Always pre-flight node I/O health before a long run.**
-   `logs/*.log` are tracked + auto-committed, then appended after the commit, so every
    `git pull` blocks on "unstaged changes" → `git checkout -- logs/` first. If the pull
    silently fails, the runbook launches **old code** — a repeated source of wasted runs.
-   `Data/` resolves to the climdat-source `working_dir` (e.g.
    `/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/`), **not** the repo.

------------------------------------------------------------------------

## 4) Next Steps

-   [ ] Fix the multisession §3.4 path (package-ify the Rcpp kernel) to restore parallel speed
-   [ ] Republish the regenerated (iso3-bearing) trends canonical to S3
-   [ ] Add S3 upload logic to Scripts 2 and 3

    -   Confirm S3 inclusion for:
        -   `hazard_timeseries_int`
        -   `hazard_timeseries_risk`
    -   Do not include `hazard_timeseries_class`

-   [ ] Update script 0.6 (exposure extractions) to use zonal extractions
