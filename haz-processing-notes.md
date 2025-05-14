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
- Scripts are launched using `Rscript` from terminal (not interactively).
- Upload logic to S3 still needs to be implemented in Scripts 2 and 3. File structure should follow `metadata/data.json`.

---

## 2) Conventions and Structure

- File names use `_` to separate logical components and `-` only for compound terms (e.g. `generic-crop`).
- Model names (e.g. `ACCESS-ESM1-5`) are included in all filenames.
- `hazard_vars` combinations are embedded in filenames of interaction outputs.
- All `.parquet` files now have `.json` documentation.
- Parquet values are sorted and rounded to reduce file size and improve performance.

---

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
- [ ] Confirm S3 inclusion for:
  - `hazard_timeseries_int`
  - `hazard_timeseries_risk`
- [ ] Auto-link parquet `.json` docs to data registry
- [ ] Validate presence of all expected GCM × SSP × timeframe combos

---