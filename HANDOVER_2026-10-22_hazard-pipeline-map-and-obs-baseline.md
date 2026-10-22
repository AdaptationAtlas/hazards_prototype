# Hazard pipeline: what is missing, the observed baseline, and a step-by-step map

**For:** Pete, planning the simplified baseline (CHIRPS v3 + CHIRTS-ERA5) hazard × exposure work, and
anyone joining the Carbon Credits for Adaptation analysis.
**State:** `develop` after the #13 publish (2026-10-16/19) and the #47 fix in flight, as of 2026-10-22.
Companion to `HANDOVER_2026-10-12_carbon-credits-adaptation-hazard-briefing.md`. That briefing
predates `period=annual` and the physical tiers going live; see `HANDOVER_2026-10-20.md` §1.
**Paths verified on the CGlabs node 2026-10-22** (read-only inventory; `DISPATCH_cglabs_pipeline_map_paths.md`
RESPONSE). §3.1 has the per-step evidence. Anything still marked **unverified** was not checked.

---

## 1. What is missing

### 1.1 Rangeland / pasture drought — nothing fit for purpose exists

**What livestock get today.** Livestock get exactly what crops get on the dry axis:
- generic `NDWS` > 15/20/25 days per month, or seasonal `PTOT` < 500/400/300 mm (`R/2_calculate_haz_freq.R:138-147`);
- now over two windows: the rainfed-maize `jagermeyr` window and, since #13, `annual` (12 months from
  the maize planting month).

**Measured on the live extreme tier.** For `cattle-tropical`, historic, in Kenya's arid counties,
"extreme" dry fires in 40-86 % of seasons. That is dryland climatology, not a drought signal (posted
on #31).

**What does not exist:**

| missing | why it matters for livestock | nearest thing we have |
|---|---|---|
| forage / biomass anomaly (NDVI anomaly, VCI, DMP) | the binding constraint in grazing systems | MODIS NDVI seasonal **means**, East Africa only, no anomaly (`eastafrica-ndvi-modis`) |
| rangeland water-balance index on a 12-month or rangeland-season window, with **percentile** severity | crop day-count thresholds saturate in drylands | SPEI-3/12 observed, Africa, 0.05° (`africa-spei-drought`). Not in the hazard chain. |
| consecutive failed seasons (run length) | the two-failed-seasons pathway to mortality and distress sales | per-season 0/1 single-hazard rasters exist on the node; nothing counts runs |
| grazing-system mask | decides where a rangeland index applies at all | GLPS directory declared (`R/0_server_setup.R:353-355`), unused. **Populated** (verified 2026-10-22): one global 10 km class raster (§4.2) |
| surface water / water-point access | water access is a separate livestock constraint | none; only a static lakes mask used to exclude water |
| heat **duration** (consecutive THI days) and night-time recovery | livestock heat load is cumulative | `THI_max` = the single hottest day per season; provenance open (#48) |

The plan is on #31 (Option C): observed NDVI anomaly as the target, a projectable index chosen by
skill against it, and a go/no-go in mid-January.

### 1.2 Perennial tree crops — the hazards are weak, and the live data shows how

These are the perennial crops (`SpamCodes.csv$long_season == TRUE`): arabica and robusta coffee,
cocoa, tea, oil palm, banana, plantain, coconut, cassava and sugarcane. Three separate weaknesses.

**(a) Window. Mostly fixed by `period=annual`, but the default is wrong.**
- Before #13 only the maize window was published. On it, cocoa showed 94 % severe dry and plantain
  96 % (#46).
- `period=annual` is now live. On the severe `prod_t` tier, historic, continental, cocoa's
  crop-specific dry falls to **0.075**.
- But nothing *routes* perennials to `annual`. Both periods are published for every crop, and a
  consumer reading `jagermeyr`, the default the V1 notebook uses, still gets the maize window for
  cocoa (#46, comment of 2026-10-12).
- The intended rule is "perennials read `annual`". It needs a publisher or consumer convention, or a
  combined period.

**(b) Heat. The threshold family decides the answer.** Severe heat marginal for each set, annual
period, historic, continental, `prod_t` severe tier:

| crop | EcoCrop Topt,max / Tabs,max (°C) | `NTx35` set | `NTxM` sets |
|---|---|---:|---:|
| arabica coffee | 28 / 34 | 0.000 | 0.28 |
| robusta coffee | 30 / 36 | 0.004 | **0.63** |
| cocoa | 32 / 38 | 0.000 | 0.34 |
| tea | 30 / 35 | 0.000 | 0.05 |
| oil palm | 35 / 38 | 0.002 | 0.002 |
| banana | 33 / 42 | 0.005 | 0.04 |
| maize (reference) | 33 / 47 | 0.087 | 0.21 |

- With the generic 35 °C, perennials, which grow in cooler or humid zones, show essentially **no**
  heat hazard.
- With EcoCrop's optimum maximum, robusta shows heat in 63 % of years.
- Neither is evidence-based. `NTxM` counts days above the top of the *optimal* range, which is not
  damage. Neither is tied to the sensitive stage: flowering and fruit set in coffee and cocoa.

**(c) Missing perennial-specific indices.** None of these is computed (agronomic candidates, not repo
facts):

| index | relevance |
|---|---|
| dry-season length and intensity: consecutive months < 100 mm, or the annual cumulative water deficit | cocoa, coffee, tea and oil palm respond to the length of the dry season, not to annual totals. Cocoa's dry class is 5-8 % on any total-based index. |
| lagged / multi-year deficit | oil palm and cocoa yields respond to deficits one to two years earlier; tree mortality after repeated drought |
| heat at flowering (a stage-specific window) | coffee flower abortion, cocoa pollination |
| cold / frost (`TMIN` low) | highland arabica and tea. `TMIN` is computed upstream and classed nowhere. |
| hot nights | coffee quality and respiration |

There is also a crop-coverage gap that is not about hazards. Nine SPAM composite groups (fruit,
vegetables, other cereals, and so on) carry **no hazard rows at all**: 62.4 B I$, about 19 % of
constant-dollar exposure (`HANDOVER_2026-10-20.md` §1, fact 2).

### 1.3 Annual crops — what is missing there too

- **`NDD` / dry spells.** `NDD` was dropped because "it cannot be projected with the delta method"
  (`R/2_calculate_haz_freq.R:126`). **That reason no longer applies.** The NEX-GDDP chain computes
  indices directly from daily GCM data, not by delta, so consecutive dry days within the season are
  computable. The upstream `calc_NDD.R` exists.
- **Planting-date and season onset/cessation.** There is one maize calendar for all crops and no
  second season.
- **Rainfall classes on irrigated production.** Exposure is `tech = all`. Wheat, rice, sugarcane and
  barley stay at 80-90 % crop-specific dry on any window (#46).
- **The `annual` period is wrong for annual crops.** Maize `NDWS` severe dry is 0.59 on `annual`
  against 0.26 on `jagermeyr`, because a 12-month mean of monthly stress days includes the dry
  season. So **neither period is right for every crop**, which is the case for a per-crop period rule.

---

## 2. Has the hazard chain been run on CHIRPS v3 + CHIRTS-ERA5? **No.**

| what | observed source | state |
|---|---|---|
| monthly `PTOT`, `TMAX`, `TMIN`, `TAVG` | CHIRPS v3 monthly + CHIRTS-ERA5 monthly | **built and published**, Africa 0.05° (`R/observational/1_get_chirps_chirts.R`; `africa-precipitation-monthly-seasonal`). **On node (verified 2026-10-22):** `DATA/chirts_chirps_hist/<VAR>/<VAR>-YYYY-MM.tif`. `PTOT` runs 1981-01 → 2026-04 (544 files); `TMAX`/`TMIN`/`TAVG` run 1980-01 → 2026-04 (556 each). `manifest.csv` records the sources as CHC `CHIRPS/v3.0/monthly/africa` and `CHIRTS-ERA5/{tmax,tmin}/tifs/monthly`, with `TAVG` derived. There are also 3-month `PTOT` sums, 1981-2025 (`seasonal/PTOT/PTOT_<MMM>_YYYY_sum.tif`, 541 files). The tree is about 31 GB, mtime 2026-05-15 → 2026-09-01. |
| SPEI-1/3/6/12/24 | same, Hargreaves PET, 1991-2020 fit | **built**; SPEI-3 and SPEI-12 published (`R/observational/2_calculate_obs_spei.R`; `africa-spei-drought`). On node: `DATA/chirts_chirps_hist/SPEI-NN/SPEI-NN-YYYY-MM.tif`, 1981-01 → 2026-04 (544 each). |
| per-pixel climatologies (mean/min/max/sd; 1995-2014, 1991-2020, full record) | same | **published**, 13 periods × 9 variables |
| admin monthly and period tables | same | **published** (`DATA/chirts_chirps_hist/admin/obs_{monthly,periods}_adm{0,1}.parquet`) |
| daily-derived hazard indices (`NTx*`, `NDWS`, `NDWL0`, `THI`, `HSH`, `NDD`) | needs CHIRPS v3 **daily** + CHIRTS-ERA5 **daily** | **not built.** Nothing in the repo ingests either daily product. The CDH catalog has records for both (`chirps-v3-daily`, `chirts-era5-daily`), and CHC serves them. **Verified 2026-10-22: neither daily product is on the node.** Nothing under `DATA/chirts_chirps_hist/` is daily. Two AgWise download scripts sit at the `{common_data}` root, dated 2026-05-28: `download_chirps_v3_rnl.R` (CHIRPS v3.0 daily `rnl`, 1981-2025) and `download_chirts_era5_temperature.R` (CHIRTS-ERA5 daily tmax/tmin). Both write to a relative `agwise-datasourcing/…/Landing/…` path, and no such landing directory exists within 4 levels of `/home/jovyan` or `{common_data}`. A copy elsewhere, for example on another node, is **unverified**. |
| threshold classes, frequency, interactions, × exposure (R/2, R/3) | — | **not run on any observed source** |

**The older observed baseline is a different product. Verified on node 2026-10-22.**
- The `atlas_delta` tree exists: `{common_data}/atlas_hazards/cmip6/indices` and `indices_seasonal`.
  - `indices/historical/` holds monthly indices for **1995-2014 only**: 64 index directories (`NTx20-50`,
    `HSM_NTx30-50`, `NDD`, `NDWS`, `NDWL0`, `NDWL50`, `PTOT`, `TAVG`, `TMAX`, `TMIN`, `THI`, `HSH`), about
    18.6 GB, mtime 2022-12 → 2024-04. `PRCPTOT` is the exception and runs 1981-2022.
  - `indices/historical-AgERA5/` holds 11 indices for 1981-2022 (`HSH`, `HSM_NTx35`, `NDD`, `NDWS`, `NTx35`,
    `NTx40`, `PTOT`, `TAVG`, `TMAX`, `TMIN`, `THI`), about 57 GB, mtime 2023-07 → 2025-08.
  - Delta futures cover 5 GCMs plus `ENSEMBLE` (ACCESS-ESM1-5, EC-Earth3, INM-CM5-0, MPI-ESM1-2-HR,
    MRI-ESM2-0) × 4 SSPs × 4 timeframes, as `<ssp>_<gcm>_<yyyy>_<yyyy>/`.
  - The whole `indices` tree is about 2.5 TB, measured as the summed sizes of 885 k files at depth ≤ 3.
  - `indices_seasonal/{annual,jagermeyr}` holds 11.1 k files, about 385 GB, mtime 2023-10 → 2025-04.
- The `historical/` source is **CHIRPS v2.0 + CHIRTS v1.0 by code only**. The upstream download scripts
  point at CHIRPS-2.0 and CHIRTSdaily v1.0 (`hazards_upstream/R/01_download_data/download_chirps.R:37`,
  `download_chirts.R:37`). The index GeoTIFFs carry no source metadata, so the source is **unverified**
  from the files themselves. The 1995-2014 window fits CHIRTS v1.0, which ends in 2016.
- This tree is what the 2024 `risk_prototype/` public files and the original delta-method futures
  came from. It is not v3/ERA5, not maintained, and not used by any current product.

**Raw observed trees on the node (verified 2026-10-22; sizes are summed file sizes from `find -printf`):**

| tree | product (from file metadata / names) | daily? | years | size |
|---|---|---|---|---|
| `{common_data}/chirps_wrld` (`chirps_raw`) | CHIRPS **v2.0**, quasi-global, `chirps-v2.0.YYYY.MM.DD.tif` | daily | 1981-2023 | ~0.9 TB, 15.7 k files |
| `{common_data}/chirps_af/netcdf` | CHIRPS **v2.0** (netCDF `version = "Version 2.0"`), Africa, `chirps_prec_YYYY.nc`, 365/366 layers | daily | 1981-2025 (`chirps_prec_2026.nc` is 0 bytes) | ~48 GB |
| `{common_data}/chirps_af/zarr`, `chirps_af/raw` | same v2.0, as zarr (made 2026-03) and daily tif | daily | 1981-2025 / 1981-2023 | not measured |
| `{common_data}/chirts` (`chirts_raw`) | CHIRTSdaily **v1.0** (netCDF `version = "Version 1.0"`, "downscaled to daily using ERA5"), `Tmax/`, `Tmin/`, **`RHum/`** as `<var>.YYYY.MM.DD.tif`, plus `netcdf/{tmax,tmin}/africa` | daily | 1983-2016 | ~2.8 TB, 37.5 k files |
| `{common_data}/chirts_africa/Tmax` | CHIRTS v1.0 Tmax, Africa subset | daily | 1983-2016 | not measured |

**No CHIRPS v3 daily and no CHIRTS-ERA5 daily on the node.** The only v3 / CHIRTS-ERA5 data present is the
monthly set in `DATA/chirts_chirps_hist/`.

**Input gaps for an observed run of the current indices:**

| index | needs | have it observed? |
|---|---|---|
| `PTOT`, `NDD` | daily or monthly precipitation | CHIRPS v3 daily: available at CHC, **not on node** (verified). Monthly v3: yes. CHIRPS v2.0 daily: yes, 1981-2025. |
| `NTx35`, `NTxM` | daily Tmax | CHIRTS-ERA5 daily: available at CHC (1983-), **not on node** (verified). CHIRTS v1.0 daily (1983-2016) and AgERA5 Tmax (1981-2022): on node. |
| `NDWS`, `NDWL0` | daily P, Tmax, Tmin, **solar radiation** (+ wind and RH for water-balance v2), soils | radiation and humidity are **not** in CHIRPS/CHIRTS. **AgERA5 daily solar radiation, RH, wind and Tmax/Tmin/Tmean, 1981-2022, are on node** (`{common_data}/ecmwf_agera5`; §4.2) |
| `THI` | daily Tmax + **relative humidity** | CHIRTS-ERA5 is Tmax/Tmin only. On node: AgERA5 RH 1981-2022, and CHIRTS v1.0 `RHum` 1983-2016 |
| SPEI | monthly P, Tmax, Tmin | **yes, already built** |

---

## 3. How the system works, step by step, and where each artefact is

Server roots, using `metadata/hosts.json` names. **All three were verified on the node on 2026-10-22.**
The node resolves to the `defaults.trees.nexgddp` templates with `{common_data}` = `/home/jovyan/common_data`
(the `cglabs` stanza has no `trees` override). Other hosts pin `working_dir` to their own `hazards_prototype`
directory, and only this node carries the `INDICES`/`SEASONAL` trees.
- `INDICES` = `{common_data}/atlas_nex-gddp_hazards/cmip6/indices`
- `SEASONAL` = `{common_data}/atlas_nex-gddp_hazards/cmip6/indices_seasonal`
- `DATA` = `{common_data}/nex-gddp-cimp6_hazards/Data`

`<period>` is `jagermeyr` or `annual`.

**The name token changes at step 2a:**
- **Steps 0-1** write `historical_ACCESS-CM2_1995_2014` and `ssp585_MRI-ESM2-0_2041_2060`, using `historical`
  and underscored years.
- **Steps 2a-3b** write `historic_ACCESS-CM2_1995-2014` and `ssp585_MRI-ESM2-0_2041-2060`, using `historic`
  and hyphenated years.

Scenarios on disk are `historical`/`historic` plus ssp126, ssp245, ssp370 and ssp585. The timeframes are
1995-2014 and 2021-2040, 2041-2060, 2061-2080 and 2081-2100, which gives the 17 scenario × timeframe
combinations in the 3c/4 stacks.

| # | step | code | output (server, **verified**) | content | public? |
|---|---|---|---|---|---|
| 0 | daily NEX-GDDP → **monthly index** | `hazards_upstream/R/04_indices/calc_*.R`, `fast_calc_*.R`; copied by `02_preprocess_data/saveNexGDDPindicesFollowing-atlas_hazards-structure.R` | `INDICES/<scen>_<gcm>_<yyyy>_<yyyy>/<IDX>/<IDX>-YYYY-MM.tif`, with the month **zero-padded**. `THI` and `HSH` carry two stats per month (`THI_max-YYYY-MM.tif`, …). | one raster per month; for example monthly count of NTx35 days, or monthly PTOT | no |
| 1 | monthly → **one value per season** | `R/1_make_timeseries.R` → `hazard_stacker` (`R/haz_functions.R:1296`) | `SEASONAL/<period>/<scen>_<gcm>_<yyyy>_<yyyy>_<IDX>_<stat>.tif`; `THI`/`HSH` are `_max_max` / `_mean_mean` | **the time series**: 19 layers, one per season; stat `sum` / `mean` / `max` from `haz_metadata.csv` | no |
| 2a | **threshold → binary 0/1** per season | `R/2` §1, `rast_class()` | `DATA/hazard_timeseries_class/<period>/<scen>_<gcm>_<yyyy-yyyy>_<IDX>-<stat>-<G\|L><thr>.tif`, for example `…_NDWS-mean-G20.tif` | 19 layers of 0/1 per index × threshold, **per GCM** | no (stale 2024 copies under `risk_prototype/`) |
| 2b | **frequency** = mean of 0/1 over 19 seasons | `R/2` §2, §2.1 | `DATA/hazard_timeseries_risk/<period>/` (same names) + `<scen>_ENSEMBLEmean_…` / `ENSEMBLEsd_…`; `annual/stats/haz_freq{,_ensemble}.parquet` | 1 layer, 0-1, per GCM and ensemble | no |
| 2c | index mean/sd (not frequency) | `R/2` §4 | `DATA/hazard_timeseries_mean/<period>/<scen>_<gcm>_<yyyy-yyyy>_<IDX>-<stat>_{mean,sd}.tif`, + `haz-means_adm_<gcm>.parquet`; `annual/{ptot,thi,ntx}_perc/*.parquet` | climatology of the index values | no |
| 3a | **interaction code** per season: dry×1 + heat×10 + wet×100 → {0, 1, 10, 100, 11, 101, 110, 111} | `R/2` §5.2 (`R/2_calculate_haz_freq.R:1574-1615`) | **never written**, held in memory only. Verified: no per-season code rasters on disk (§3.1) | — | — |
| 3b | **class frequency** = share of 19 seasons with each exact code; `any`, `none` = 1 − `any` | `R/2` §5.2, `int_risk()` | `DATA/hazard_timeseries_int/<period>/<scen>_<gcm>_<yyyy-yyyy>_<dry>+<heat>+<wet>.tif`, for example `…_NDWS-G20+NTx35-G14+NDWL0-G5.tif` | 9 layers (`any`, `none`, `dry`, `heat`, `wet`, `dry+heat`, `dry+wet`, `heat+wet`, `dry+heat+wet`), per GCM + ENSEMBLEmean/sd | no |
| 3c | **per-crop bundle**: all scenarios × timeframes for one crop, severity and set | `R/2` §5.3 | `DATA/hazard_risk/<period>/<crop>_<model>_<sev>_<set>_int.tif`, where `<model>` is each of the 18 GCMs **and** `ENSEMBLEmean`/`ENSEMBLEsd`, for example `maize_ENSEMBLEmean_severe_NDWS+NTxM+NDWL0_int.tif`. Also **solo** bundles `<crop>_<model>_<sev>.tif` (no set token). `NDWS+NTx35+NDWL0` exists only for `generic-crop`. | the stack R/3 reads: `_int` = 153 layers (17 scen×tf × 9 classes); solo = 51 layers (17 × dry/heat/wet) | no |
| 4 | **× exposure** per pixel | `R/3` §4.1 | `DATA/hazard_risk_{vop,vop_usd,ha,prod,n}/<period>/<crop>_<ENSEMBLEmean\|ENSEMBLEsd>_<sev>_<set>_int_<variable>.tif`, plus solo `<crop>_<model>_<sev>_<variable>.tif`. `<variable>` is `vop_intld15-2021`, `vop_nominal-usd-2021`, `harv-area_ha`, `prod_t` or `head_n`. **Ensemble only, no per-GCM files.** | class frequency × VoP / ha / t / head (153 / 51 layers, INT4S) | no |
| 5 | **zonal sum** to GAUL 2024 admin2 (+ adm1/adm0 sums) | `R/3` §4.2 | `DATA/hazard_risk_<v>/<period>/haz-freq-exp_<variable>_<ENSEMBLE\|ENSEMBLEmean\|ENSEMBLEsd>_int_adm_<sev>.parquet` + `.parquet.json` sidecar | the table | **yes**: `s3://digital-atlas/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=<v>/period=<p>/model=ENSEMBLEmean/severity=<s>/int=multi-hazard.parquet` (S3 side **not re-checked** in this inventory) |

#### 3.1 Verified on node, 2026-10-22 (read-only inventory)

**How it was measured.** Counts and sizes come from `find -maxdepth 1 -type f -printf '%s %T@ %f'` per
directory: summed apparent sizes in decimal GB. Layers come from `terra::nlyr()` / `names()` on the
example file. mtimes are file mtimes. Where two figures are given, they are `jagermeyr` / `annual`.

| # | example file (both periods unless noted) | files (jag / ann) | size GB (jag / ann) | layers | mtime oldest → newest (jag / ann) |
|---|---|---:|---:|---|---|
| 0 | `historical_ACCESS-CM2_1995_2014/NTx35/NTx35-1995-01.tif` (no period) | 3,494,034 in 324 `<scen>_<gcm>_<yyyy>_<yyyy>` dirs (18 GCMs × [hist 1981_2014, hist 1995_2014, 4 SSP × 4 tf]) × 42 index dirs | ~854 | 1 | 2025-07-03 → 2026-06-27 (4,320 files from 2026) |
| 1 | `historical_ACCESS-CM2_1995_2014_NDWS_mean.tif` | 13,158 / 13,491 (5 stray `.tmp` in annual) | 106.6 / 114.5 | 19, named by year (`1995` … `2013`) | 2025-07-07 → 2026-06-28 (mostly 2025-07/08; **not rebaked in #13**) |
| 2a | `historic_ACCESS-CM2_1995-2014_NDWS-mean-G20.tif` | 55,998 / 56,058 | 90.7 / 85.6 | 19, named by year, values 0/1 | jag all 2026-10-09; ann 2026-10-08 (+60 files from 2025-08-12) |
| 2b | `historic_ACCESS-CM2_1995-2014_NDWS-mean-G20.tif`, `historic_ENSEMBLEmean_1995-2014_NDWS-mean-G20.tif` | 62,220 / 62,290 | 10.2 / 9.7 | 1 | 2026-10-09 / 2026-10-08 |
| 2c | `historic_ACCESS-CM2_1995-2014_NDWS-mean_mean.tif` (+ `_sd`) | 29,278 / 32,378 | 16.7 / 18.7 | 1 | 2025-08-15 → 2026-10-09 / 2025-08-14 → 2026-10-08. 1,548 per period are 2025-08 leftovers in the old `historical_…` naming. |
| 3b | `historic_ACCESS-CM2_1995-2014_NDWS-G20+NTx35-G14+NDWL0-G5.tif` | 89,760 / 89,760 | 108.6 / 106.0 | 9 classes | 2026-05-27 → 2026-10-13. 56,100 are the 2026-10 #13 bake; 33,444 are 2026-05 combos (`PTOT+NTxNN+PTOT`, `…THI-max…`) not refreshed. |
| 3c | `maize_ENSEMBLEmean_severe_NDWS+NTxM+NDWL0_int.tif`, `maize_ENSEMBLEmean_severe.tif` (solo) | 8,100 / 8,100: 5,220 `_int` + 2,640 solo tif + 240 old `haz-freq_<gcm>_{int,solo}_adm_<sev>.parquet`/`.json` | 129.4 / 124.9 | 153 / 51 | tif all 2026-10 (→ 2026-10-13 / 2026-10-12); parquets 2025-08-25 |
| 4 | `maize_ENSEMBLEmean_severe_NDWS+NTxM+NDWL0_int_vop_intld15-2021.tif`, `cattle-highland_ENSEMBLEmean_severe_head_n.tif` | vop 1,002 / 1,002; vop_usd **0** / 986; ha 823 / 823; prod 823 / 823; n 199 / 199 | vop 2.7 / 2.4; vop_usd 0 / 1.7; ha 1.1 / 1.0; prod 1.2 / 1.1; n 0.4 / 0.4 | 153 (`_int`) / 51 (solo) | 2026-10-13 → 2026-10-16; vop_usd/annual 2026-10-22; ha/prod/n annual up to 2026-10-22 |
| 5 | `haz-freq-exp_prod_t_ENSEMBLEmean_int_adm_severe.parquet` | 9 parquet + 9 json per variable per period (3 models × 3 severities); vop_usd: jag 0, ann 1 (`ENSEMBLEmean`, `extreme`) | 13-164 MB each | — | 2026-10-13 → 2026-10-16; vop_usd 2026-10-22 (#47 fix in flight) |

**Invariant checks (dispatch Step A). All three pass.**
1. **2a and 3b are per-GCM, with the published 18.** The GCM token set in `hazard_timeseries_class` is exactly
   the 18 members in the `.parquet.json` sidecar `ensemble.members` (ACCESS-CM2 … TaiESM1), with no ensemble
   files. `hazard_timeseries_int` has the same 18 plus `ENSEMBLEmean`/`ENSEMBLEsd`. Steps 0 and 1 also carry
   exactly the 18.
2. **3a has no files.** A name search over `{common_data}/nex-gddp-cimp6_hazards`,
   `{common_data}/hazards_prototype` and `{common_data}/atlas_hazards` for `*code*`, `*interaction*`,
   `*int_season*`, `*season*int*` and `*_combo*` found only FAO `*Codes.csv`. Every sampled 3b file, both
   the 2026-05 and the 2026-10 bake, has the 9 class-frequency layers, not 19 per-season codes. Sampled
   2a files hold values 0-1. `hazard_risk/<period>/intermediate/` is empty. This check is by name and
   layer structure; not every file's values were scanned.
3. **3c `_int` bundles carry only the #13 sets.** The sets are `NDWS+NTx35+NDWL0` (60 per period, `generic-crop`
   only), `NDWS+NTxM+NDWL0` (1,980), `PTOT-L+NTxM+PTOT-G` (1,980), `NDWS+THI-max+NDWL0` (600) and
   `PTOT-L+THI-max+PTOT-G` (600). There is no `NTxS` set.
   - Step 4 carries `NDWS+NTx35+NDWL0` for 34 crops even though 3c has it only for `generic-crop`. The
     mechanism, presumably crops mapped to the generic bundle, is **unverified**.

**Directories under `DATA/` or `SEASONAL/` not in the table:**
- `chirts_chirps_hist/` — the observed CHIRPS v3 / CHIRTS-ERA5 monthly tree (§2).
- `hazard_timeseries_mean_month/` (145 entries) and `hazard_timeseries_mean_month_pre_issue26_bak/` (208) —
  monthly hazard admin means (`haz_3months_adm_mean_*.parquet`), the latter a pre-#26 backup.
- `exposure/`, `mapspam/`, `GLW4/`, `GLW4_2020/`, `fao/`, `atlas_pop/` — exposure inputs for 0.4.x.
- `boundaries/`, `boundariesintermediate/`, `hydrobasins/`, `afr_highlands/`, `base_rast.tif` — masks and
  geometry.
- `GLPS/`, `ggcmi/`, `cattle_heatstress/`, `sos/`, `solution_tables/` — ancillary inputs (§4.2).
- `ndvi_modis/`, `wrsi_fews/`, `flood_gfd/`, `flood_jrc/` — side datasets, not in R/1-R/3.
- `_parked_intld_fixes_20261004_173417/` (1 file), `_parked_issue47_20261021_164945/` (2,040 files, 5 GB of
  `glw_processed/` VoP), `_parked_r22_pre13_20261015_213020/` (10 files) — parked by earlier dispatches.
- `_acquisition/` (one jsonl).
- Empty: `hazard_exposure/`, `adaptive_capacity/`, `cropsuite_class/`, `isimip_timeseries{,_mean,_sd}/`,
  `roi/`.
- `SEASONAL/rcal_temp.tif` — one stray 2026-06-28 raster.
- `SEASONAL/annual/` also holds 332 `historical_<gcm>_1981_2014_*` stacks; `jagermeyr/` has none.

**Not checked:** the S3 copies for step 5, and the contents of `{common_data}/ecmwf_era5` beyond its
country folders (§4.2).

**Three structural facts to carry into any redesign:**
1. **The `110` / `001` codes never exist as files.** Step 3a builds the code for each season in memory
   and step 3b collapses it straight to a 20-year frequency. So a per-season compound-event series
   cannot be recovered without re-running §5.2. This is the main thing to change.
2. **Thresholds are applied per index × threshold, not per crop** (2a). Crop identity enters only at
   3c, when each crop's heat/dry/wet tokens pick their threshold files. So most of the cost is
   classifying every threshold any crop uses, about 200 min per period.
3. **§5.2 is the expensive step.** It took about 510 min per period in #13, because it runs every
   combination × scenario × GCM.

---

## 4. A simplified observed baseline (CHIRPS v3 + CHIRTS-ERA5): proposal

**Goal:** baseline exposure to a small set of heat and drought hazards for crops and livestock, from
observed data, quickly. Futures are not attempted here.

### 4.1 Design changes worth making while simplifying

1. **Store the per-season code, not only the frequency.** Write one `uint8` raster per year per
   crop-group: bit 1 = dry, bit 2 = heat, plus severity in the high bits, or one file per severity.
   - Frequency is then a mean over the files.
   - Interactions, consecutive-failure runs and any later mixed-severity class are all derived
     products, not re-runs.
   - This also gives the analyst (Jemal) the per-season series he asked for.
2. **Two hazard axes (dry, heat), not three.** Drop wet for the first version. Classes: none, dry,
   heat, dry+heat.
3. **Per-crop windows from the start:**
   - GGCMI Phase 3 has calendars for most staples, including maize, sorghum, millet, rice, wheat,
     soybean, groundnut, cassava, potato and beans;
   - perennials use 12 months;
   - livestock use 12 months or a rangeland season.
4. **Severity on fixed absolute thresholds, if these classes will ever be compared with NEX-GDDP
   futures.**
   - SPEI classes (−1, −1.5, −2) are standardised against 1991-2020, so baseline P is about
     16 / 7 / 2 % by construction. That is fine for a baseline-only map, and wrong for a
     baseline-vs-future comparison.
   - Also note that NEX-GDDP is bias-corrected to its own observational reference, not to
     CHIRPS/CHIRTS. A NEX-GDDP 1995-2014 frequency and a CHIRPS/CHIRTS 1995-2014 frequency will
     differ even with identical thresholds. That difference is worth measuring, and it is a check,
     not a defect.

### 4.2 A starter variable set, by how soon it can exist

| hazard | crops | livestock | data | effort |
|---|---|---|---|---|
| **drought — now** | SPEI-3 at the end of the crop window (≤ −1 / −1.5 / −2) | SPEI-12 at the end of the rangeland year, masked to grazing systems | **already built** (`R/observational/2`), 0.05° | about 1-2 weeks: classify + frequency + R/3 multiply |
| **drought — absolute** | seasonal PTOT below a percentile of 1995-2014, or `NDD` / max dry spell | 12-month PTOT percentile + consecutive-failure count | CHIRPS v3 monthly (have it); daily for `NDD` (ingest: v3 daily is **not on node**, verified) | +1-2 weeks; daily ingest is the long pole |
| **heat** | `NTx35` and a revised crop threshold (flowering-stage, not EcoCrop Topt) | THI needs RH. Interim: days Tmax > 35 °C as a crude proxy, labelled as such. | CHIRTS-ERA5 **daily** Tmax (ingest from CHC, 1983-; **not on node**, verified) | about 2-3 weeks incl. ingest |
| water balance (`NDWS`) | as now | — | needs radiation + soils. **AgERA5 daily radiation, RH, wind, Tmax/Tmin 1981-2022 are on node** (below) | not in a first version |
| THI properly | — | THI on Tmax + RH at Tmax | AgERA5 RH 1981-2022 on node (below) | not in a first version |

**Ancillary data on the node, verified 2026-10-22 (dispatch B4-B6):**

- **B4. ERA5 / AgERA5. This decides NDWS / THI on an observed baseline.**
  - `{common_data}/ecmwf_agera5` holds AgERA5 v1.0 daily for 1981-01 → 2022-12, about 1.4 TB (`du`). Each
    variable is a monthly zip, 504-505 per variable, plus an extracted daily `.nc` directory of about
    15.3 k files, for example `Solar-Radiation-Flux_C3S-glob-agric_AgERA5_19810101_final-v1.0.nc`. The
    variables are:
    - `solar_radiation_flux`
    - `2m_relative_humidity`
    - `10m_wind_speed` (24 h mean)
    - `2m_temperature-24_hour_{maximum,minimum,mean}`
  - `ecmwf_agera5/netcdf/` has per-variable folders (`Rainfall`, `RelativeHumidity`, `SolarRadiation`,
    `TemperatureMax/Mean/Min`, `WindSpeed`). Their contents were **not inspected**.
  - Smaller copies: `{common_data}/AgERA5_RH_2015_2022` and `AgERA5_Temp_2015_2022/Tmax` are daily
    2015-2022. `{common_data}/ET_SolRad` holds monthly `et_solrad_1..12`.
  - ERA5 proper: `{common_data}/ecmwf_era5/{raw,intermediate}` has six country folders only (EGY, MOZ, MWI,
    NGA, RWA, ZMB). Variables and years are **unverified**.
  - So radiation, humidity and wind exist observed, but on AgERA5's grid (0.1°) and only to 2022. An
    observed `NDWS` or `THI` would mix CHIRPS v3 / CHIRTS-ERA5 with AgERA5, or would need a fresh ERA5
    pull for 2023 onward. `atlas_delta/historical-AgERA5` already has AgERA5-based `NDWS` and `THI` for
    1981-2022 (§2), but on CHIRPS v2.0-era inputs.
- **B5. GLPS.** `DATA/GLPS/glps_gleam_61113_10km.tif` is the only file: one layer, global, 0.0833° (10 km),
  integer classes 1-15+ (the GLEAM livestock production systems), mtime 2025-06-30. An identical-size copy
  sits in `{common_data}/hazards_prototype/Data/GLPS/`. It is usable as a grazing mask. The class legend
  is **not on disk** and needs the GLPS documentation.
- **B6. GGCMI Phase 3 calendars.** `DATA/ggcmi/` holds 40 files,
  `<crop>_{rf,ir}_ggcmi_crop_calendar_phase3_v1.01.nc4`, one rainfed and one irrigated file for each of 20
  crops: `bar`, `bea`, `cas`, `cot`, `mai`, `mil`, `nut`, `pea`, `pot`, `rap`, `ri1`, `ri2`, `rye`, `sgb`,
  `sgc`, `sor`, `soy`, `sun`, `swh` and `wwh`. All are dated 2025-06-30.
  - Only `mai_rf_…` is read today (`R/1_make_timeseries.R:274`, `R/1.2_create_isimip_timeseries.R:30`).
  - Per-crop windows (§4.1 item 3) need no download for these 20 crops, including the second rice
    season (`ri2`).
  - `{common_data}/atlas_crop_calendar/raw_jagermeyr` appears to be the same set, judging by the
    identical directory size. That is **unverified** at file level.

**Recommendation:**
1. Start with **SPEI drought (crops: crop window; livestock: 12-month, grazing mask) × exposure** on the
   existing observed SPEI. It needs no new data.
2. Add **CHIRTS-ERA5 daily `NTx35`** as the heat axis once ingested.
3. Build it as a new, small script pair that writes per-season `uint8` codes. Do not push observed
   data through R/1-R/2, which carries the maize window, three-axis structure and in-memory codes
   that we are trying to leave behind.
4. Reuse R/3 §4 for the multiply and zonal sum. It only needs class-frequency rasters named the R/3
   way.
