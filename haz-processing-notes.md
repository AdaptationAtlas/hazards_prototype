# 📘 Atlas Hazard Layers – Processing Update (May 2025)

_Last updated: May 2025_  
_Project_: Africa Agriculture Adaptation Atlas (AAAA)  
_Prepared by_: Pete Steward (p.steward@cgiar.org)  
_Storage_: All files are stored in the **CGLabs server common data folder** (not yet uploaded to S3)  
_Repository_: https://github.com/AdaptationAtlas/hazards_prototype

---

## 1) General Notes

- All outputs remain under `hazards_prototype/Data/`, with the original folder structure and seasonal subfolders unchanged.
- Scripts 2 and 3 are now modular and can be controlled via in-script flags:
  - [Script 2 – calculate_haz_freq.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/2_calculate_haz_freq.R)
  - [Script 3 – freq_x_exposure.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/3_freq_x_exposure.R)
- Scripts are launched using `Rscript` from terminal (not Rstudio).
- Upload logic to S3 still needs to be implemented in Scripts 2 and 3. File structure should follow `metadata/data.json`.
- Extractions now use zonal rather than vector based extractions.
- Remaining steps:
  - Intersecting hazard frequency with exposure (now running for intdlr, usd and harv-area)
  - Extraction of hazard frequency x vop (code is already updated for zonal)
  - Haz_mean_month parquets, section 3 of script 3 - not updated yet. Do we use these files for anything?   

### Generic Hazard Simplification

- **Previously**, generic hazards were duplicated across all crop-specific raster stacks, even though the hazard values were identical.
- **Now**, this has been simplified: **only the `generic-crop` contains the generic hazard layers**.
- In **Script 3 – Section 4**, when hazard frequency is intersected with exposure:
  - The **generic hazard frequency** layer is intersected with **each crop's exposure** values.
  - The `generic-crop` entry is intersected with the **total production value**.
- These are the only files retained from this step.

## Conventions and Structure

- File names use `_` to separate logical components and `-` only for compound terms (e.g. `generic-crop`).
- Model names (e.g. `ACCESS-ESM1-5`) are included in all filenames.
- `hazard_vars` combinations are embedded in filenames of interaction outputs.
- All `.parquet` files now have `.json` documentation.
- Parquet values are sorted and rounded to reduce file size and improve performance.

---

## 2) Exposure and Value Datasets

This section describes the processed exposure datasets used in hazard × exposure intersections. All files are stored in subdirectories under `Data/`, organized by data source.

---

### 2.1 Processed Livestock Data – GLW4

**Folder:** `Data/GLW4/processed/`

#### 📂 Raster files

| Filename                                                   | Description                                 | Raster Layers (names)                      |
|------------------------------------------------------------|---------------------------------------------|--------------------------------------------|
| `livestock_number_number.tif`                              | Tropical & highland livestock counts        | `cattle_tropical`, `sheep_highland`, etc.  |
| `livestock_vop_intld2015.tif`                              | VOP in 2015 international dollars           | Same layer structure as above              |
| `livestock_vop_usd2015.tif`                                | VOP in 2015 USD                             | Same layer structure as above              |

Each raster includes 12 layers:

- **Tropical breeds:** `cattle_tropical`, `sheep_tropical`, `goats_tropical`, `pigs_tropical`, `poultry_tropical`, `total_tropical`
- **Highland breeds:** `cattle_highland`, `sheep_highland`, `goats_highland`, `pigs_highland`, `poultry_highland`, `total_highland`

#### 📂 Parquet summary tables

| Filename                                                   | Description                                |
|------------------------------------------------------------|--------------------------------------------|
| `livestock_number_number_adm_sum.parquet`                  | Administrative sum of livestock numbers    |
| `livestock_vop_intld2015_adm_sum.parquet`                  | Admin-level sum of VOP (int'l dollars)     |
| `livestock_vop_usd2015_adm_sum.parquet`                    | Admin-level sum of VOP (USD)               |

Each `.parquet` file contains these fields:

| Column         | Description                               |
|----------------|-------------------------------------------|
| `iso3`         | Country ISO-3 code                        |
| `admin0_name`  | Country name                              |
| `admin1_name`  | Admin 1 region                            |
| `admin2_name`  | Admin 2 district (if available)           |
| `crop`         | Livestock type (e.g., `cattle_tropical`)  |
| `value`        | Count or value of production              |
| `exposure`     | `vop` or `number`
| `unit`         | `number`, `usd`, or `intld`               |
| `stat`         | Aggregation type (usually `sum`)          |
| `tech`         | Not used in livestock (set as `NA`)       |

---

### 2.2 Processed Crop Data – MapSPAM 2020 (SSA only)

**Folder:** `Data/mapspam/2020V1r2_SSA/processed/`

#### 📂 Raster files

Organized into folders by variable:

| Variable Folder                    | Contents                                 |
|-----------------------------------|------------------------------------------|
| `variable=harv-area_ha/`          | Harvested area rasters                   |
| `variable=phys-area_ha/`          | Physical area rasters                    |
| `variable=prod_t/`                | Production (tonnes) rasters              |
| `variable=yield_kgha/`            | Yield rasters (kg/ha)                    |
| `variable=vop_intld15/`           | VOP (international dollars 2015) rasters|
| `variable=vop_usd2015/`           | VOP (USD 2015) rasters                   |

Each variable includes files like:

```
spam_<variable>_<input>.tif
```

Where `<input>` is one of:

- `all`, `irr`, `rf-all`, `rf-highinput`, `rf-lowinput`, `rf-subsistence`

Each raster contains 40+ crop-specific layers, including:

- `wheat`, `maize`, `cassava`, `soybean`, `groundnut`, `arabica coffee`, `vegetables`, etc.

Use `terra::names(rast(file))` to inspect crop layers.

#### 📂 Parquet summary tables

Each raster variable includes multiple `.parquet` files with administrative aggregation. These are stored in folders like:

```
Data/mapspam/2020V1r2_SSA/processed/variable=harv-area_ha/
```

Example filenames:

- `spam_harv-area_ha_all_adm_sum.parquet`
- `spam_vop_usd2015_rf-lowinput_adm_sum.parquet`

Each `.parquet` file includes:

| Column         | Description                                 |
|----------------|---------------------------------------------|
| `iso3`         | ISO-3 code                                  |
| `admin0_name`  | Country                                     |
| `admin1_name`  | Region                                      |
| `admin2_name`  | District                                    |
| `crop`         | Crop name (e.g., `maize`, `tea`)            |
| `value`        | Harvest area / production / VOP             |
| `exposure`     | Exposure type (e.g., `harv-area`, `prod_t`) |
| `unit`         | `ha`, `t`, `usd`, etc.                      |
| `stat`         | Aggregation method (usually `sum`)          |
| `tech`         | Technology level (e.g., `all`, `irr`)       |

---

### 2.3 Unified Exposure Tables

**Folder:** `Data/exposure/`

These harmonized files combine exposure information across both MapSPAM and GLW sources for hazard-risk overlays.

| Filename                                | Description                                 |
|-----------------------------------------|---------------------------------------------|
| `exposure_adm_sum.parquet`              | All crop × exposure × tech combinations     |
| `hpop_adm_sum.parquet`                  | Human population exposure (GPW-derived)     |

Schema for `exposure_adm_sum.parquet`:

| Column         | Description                        |
|----------------|------------------------------------|
| `iso3`         | ISO-3 country code                 |
| `admin0_name`  | Country                            |
| `admin1_name`  | Region                             |
| `admin2_name`  | District                           |
| `crop`         | Crop or `generic-crop`             |
| `value`        | Exposure value                     |
| `exposure`     | e.g., `harv-area`, `prod_t`        |
| `unit`         | `ha`, `t`, `usd`, `number`         |
| `tech`         | e.g., `all`, `irr`, `rf-lowinput`  |

---

Let me know if you'd like to generate an automated index for all files or if Brayden needs a CSV of these folder structures.

## 3) Folder Summaries

### 3.1 `hazard_risk/annual/`

- Parquet files summarizing hazard frequencies by administrative area.
- Two types: `int` (interactions) and `solo` (single hazard variables).
- Includes severity levels: `moderate`, `severe`, `extreme`.

**Parquet schema:**

| Column         | Description                          |
|----------------|--------------------------------------|
| iso3           | ISO country code                     |
| admin0_name    | Country name                         |
| admin1_name    | Admin 1 name                         |
| admin2_name    | Admin 2 name                         |
| value          | Fraction of years meeting condition  |
| scenario       | e.g. ssp126                          |
| model          | e.g. ACCESS-ESM1-5                   |
| timeframe      | e.g. 2021–2040                       |
| hazard         | e.g. heat, dry+wet                   |
| hazard_vars    | Variable set used for condition      |
| crop           | Crop affected                        |
| severity       | Hazard threshold level               |

---

### 3.2 `hazard_timeseries_mean/annual/`

- Single-band `.tif` files: annual mean values over time windows.
- Parquet summaries per model and timeframe with associated `.json`.

**Parquet schema:**

| Column       | Description                    |
|--------------|--------------------------------|
| iso3         | ISO code                       |
| admin0_name  | Country                        |
| admin1_name  | Region                         |
| admin2_name  | District                       |
| value        | Mean hazard value              |
| scenario     | SSP (e.g., ssp245)             |
| model        | GCM or ensemble                |
| timeframe    | 20-year period                 |
| hazard       | e.g., HSH-max-max              |
| stat         | Statistic (e.g., mean)         |

---

### 3.3 `hazard_timeseries_class/annual/`

- Multi-band `.tif` files: classified hazard values by year.
- Covers both **historic** (1995–2013) and **SSP scenario projections** (2021–2100).
- No associated `.json` metadata.

**Filename pattern:**

```
<scenario>_<model>_<timeframe>_<hazard>-<stat>-<threshold>.tif
```

Example:
```
ssp245_MPI-ESM1-2-HR_2081-2100_PTOT-sum-L750.tif
```

---

### 3.4 `hazard_timeseries_int/annual/`

- Multi-layer `.tif` rasters of **hazard interactions**.
- Each file = one combination of classified variables.
- Layers represent interaction groupings (`dry`, `dry+heat`, etc.).

**Layer names example:**
```
ssp126_MPI-ESM1-2-HR_2061-2080_dry+heat
```

---

### 3.5 `hazard_timeseries_risk/annual/`

- Single-layer `.tif` rasters representing **risk-weighted hazard values**.
- Band name: `"mean"`

**Filename pattern:**
```
<scenario>_<model>_<timeframe>_<hazard>-<stat>-<threshold>.tif
```

Example:
```
ssp245_ACCESS-ESM1-5_2041-2060_PTOT-sum-L1700.tif
```

---

## 4) To-Do / Next Steps

- [ ] Add S3 upload logic to Scripts 2 and 3
  - Confirm S3 inclusion for:
    - `hazard_timeseries_int`
    - `hazard_timeseries_risk`
  - Do not include `hazard_timeseries_class`
- [ ] Review .json documentation and see if this can be used to document the dataset
- [ ] Update script 0.6 (exposure extractions) to use zonal extractions
- [ ] Review and update script 2.1


---
