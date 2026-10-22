# PLAN — a streamlined observed hazard pipeline (CHIRPS v3 + CHIRTS-ERA5)

**Status:** decisions recorded 2026-10-22; nothing is built yet.
**Inputs to this plan:**
- `HANDOVER_2026-10-22_hazard-pipeline-map-and-obs-baseline.md`, with the node-verified inventory
  (`DISPATCH_cglabs_pipeline_map_paths.md`);
- the CHC, FAO, JRC and USGS servers, checked anonymously on 2026-10-22.

**Goal:** a **present-day** observed baseline of hazard frequency × exposure for a small set of heat
and drought hazards, for crops and livestock. It must be comparable with the NEX-GDDP product, and it
must keep the per-season series that the current chain throws away. The historical baselines already
exist (NEX-GDDP 1995-2014; observed climatologies for 1995-2014, 1991-2020 and the full record).
What is missing is *now*.

## 0. Decisions (Pete, 2026-10-22)

| decision | choice |
|---|---|
| grid | **0.25°**, the NEX-GDDP hazard grid; R/3 reused as is |
| baseline years | **present day.** The historical baselines exist already. Proposed window **2006-2025**: 20 seasons, matching the 20-year structure of the product, with every input complete. **Confirm.** |
| NDWS / NDWL0 radiation and aggregation | **AgERA5**; keep the "mean of monthly counts" aggregation so definitions and thresholds match NEX-GDDP |
| livestock drought | research first → done, see §3. Recommendation: an observed **FPAR seasonal-anomaly** layer, plus a projectable index chosen by skill against it |

---

## 1. Inputs, all credential-free

| need | source | coverage | access (checked 2026-10-22) | notes |
|---|---|---|---|---|
| daily P | **CHIRPS v3 daily `rnl`, 0.25° `p25`** | 1981- | `data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final/rnl/p25/` | Daily timing comes from ERA5 within CHIRPS pentads; CHC warns of artificial wet runs. Affects dry-spell indices only. |
| daily Tmax, Tmin | **CHIRTS-ERA5 daily**, 0.05° → mean to 0.25° | 1959- | `data.chc.ucsb.edu/experimental/CHIRTS-ERA5/{tmax,tmin}/tifs/daily/` | ~27 MB/day global. Crop to Africa on read. |
| daily solar radiation (`rsds`) for NDWS/NDWL0 | **AgERA5 v2.0 Solar Radiation Flux**, FAO public mirror | 1979 → present, ~7-8 day lag | `storage.googleapis.com/fao-gismgr-c3s-data/DATA/C3S/MAPSET/AGERA5-SRF/C3S.AGERA5-SRF.YYYY-MM-DD.tif`; anonymous; checked 2005-01-01 and 2025-12-31 | Daily sum, J m⁻² d⁻¹. 0.1° → **area-weighted mean** to 0.25°. Nodata −9999. **Take the whole series from v2.0 and do not splice onto the node's v1.0 (1981-2022).** v1.0/v1.1 are being deprecated, and the ATBD says radiation processing is unchanged across versions. **Gate:** compare v1.0 vs v2.0 over 2006-2022 on the node. Licence: FAO page CC-BY-SA 4.0, CDS CC-BY; record both. |
| RH for THI | AgERA5 v2.0 RH (same FAO bucket) | 1979- | as above | CHIRTS-ERA5 lists a `vpd` product, but its files were not found. AgERA5 RH replaces the "Tmax + daily-mean RH" pairing (#48); compute RH at Tmax where possible. |
| soils | `sscp_world.tif`, `ssat_world.tif` | static | on the node (used by the NEX-GDDP NDWS) | |
| crop calendars | GGCMI Phase 3 v1.01, 20 crops × rf | static | on the node (`ggcmi_dir`) | perennials and livestock: 12 months |
| grazing mask | GLPS 10 km ∩ **ASAP rangeland mask v04** | static | GLPS on the node (legend to source); `agricultural-production-hotspots.ec.europa.eu/files/asap_mask_rangeland_v04.tif` (393 MB) | |
| rangeland vegetation | **JRC ASAP FPAR v9.0, 500 m, dekadal** (MODIS C6.1 + VIIRS C2, intercalibrated) | 2000-05 → present | `agricultural-production-hotspots.ec.europa.eu/data/indicators_fpar/fpar/`; anonymous; tiled GeoTIFF | Use raw `fpar/`, not ASAP's z-scores, so the baseline is ours. CC BY 4.0 (Seguini et al. 2026, ESSD 18:309). |
| cross-check vegetation | USGS/FEWS NET **eVMODIS NDVI**, 375 m, dekadal (VIIRS adjusted to eMODIS) | 2003 → present | `edcintl.cr.usgs.gov/.../africa/{east,west,southern,north}/dekadal/evmodis/ndvi/` | the index-insurance (IBLI/KLIP) lineage |

**Not used, and why:**
- **Copernicus DMP/NDVI 300 m:** needs a CDSE login, and the series starts in 2014.
- **WaPOR v3 NPP:** starts in 2018, and its model includes meteorology.
- **MOD17 GPP/NPP:** meteorology-driven, and MODIS ends in 2027.
- **FAO ASIS:** no anonymous bulk access, and it is an area-share index. Its grassland drought
  frequency (1984-2025) is kept as a sanity check only.
- **Raw ERA5 `ssrd`** (ARCO-ERA5 / NSF NCAR): anonymous fallback, but whole-series only. It is not
  AgERA5, so splicing it would put a step into PET.

**Why vegetation products with met inputs are excluded as validation targets.** DMP, WaPOR and MOD17
embed meteorological forcing, so agreement with SPEI or NDWS would be partly circular. FPAR and NDVI
are reflectance-based.

---

## 2. Climate-hazard pipeline (crops + livestock heat)

Each stage is one script, idempotent, with `--smoke` (a Kenya box) and `--full`, timestamped logging
(as in `R/observational/`).

| stage | does | output |
|---|---|---|
| **S1 ingest** | download, crop to Africa, aggregate to 0.25°: CHIRPS `p25` native; CHIRTS-ERA5 5×5 mean; AgERA5 SRF/RH area-weighted | daily cubes, one netCDF/zarr per variable-year (about 140 MB each). **Years 2005-2025** (2005 seeds the water-balance AVAIL for 2006). |
| **S2 daily → monthly index** | **reuse the upstream index code unchanged**: `calc_NTx.R`, `fast_calc_NDWS.R`/`NDWL0.R` with `wbkernel` and the AVAIL fix, `calc_THI.R`, `calc_PTOT.R`; input roots parameterised | `<IDX>-YYYY-MM.tif`, the NEX-GDDP step-0 layout, so the two are like-for-like |
| **S3 per-season value, per crop window** | crops: that crop's GGCMI rainfed calendar. Perennials and livestock: 12 months. Same stats as now (`haz_metadata.csv`). | one stack per index × window, one layer per season (20) |
| **S4 classify + encode** | fixed thresholds (`haz_classes.csv`), then **per season** `code = dry·1 + heat·2 + wet·4` (`uint8`) | **one code stack per crop-window × severity, saved** |
| **S5 frequencies** | class frequencies, `any`/`none`, marginals, with R/3 `_int` naming | rasters R/3 §4 reads unchanged |
| **S6 × exposure** | `R/3_freq_x_exposure.R` §4, all five tiers | admin parquets, same schema, `source=chirps-chirts-agera5`, `period=<crop-window>`, `timeframe=2006-2025` |

**v1 hazards:**
- **crops:** heat `NTx35`; dry `NDWS`; wet `NDWL0` (optional).
- **livestock heat:** `THI` (Tmax + AgERA5 RH).

Crop-specific heat thresholds wait for the #24/#25 agronomy review.

---

## 3. Rangeland drought (livestock) — from the research

**Observed present-day layer (V1):**
1. ASAP FPAR, dekadal, masked to **ASAP rangeland ∩ GLPS grazing systems**.
2. Per pixel and season, accumulate FPAR over the season, then convert to a cumulative z-score
   against a fixed baseline, **2006-2025** (zFPARc, our own computation).
   - Season windows come from per-pixel phenology (ASAP or ASIS phenology layers), or a 12-month
     rangeland year where seasons are not separable.
   - Bimodal pixels get two seasons. That is an improvement on the crop chain, which has one.
3. **Severity on empirical percentiles of the baseline:** moderate ≤ 20th percentile (the IBLI/KLIP
   trigger convention), severe ≤ 10th, extreme ≤ 5th.
   - Baseline P is then fixed by construction (0.20 / 0.10 / 0.05). That is correct for a percentile
     definition, and must be stated for the avoided-loss method.
   - A **consecutive-failure** class (≥ 2 seasons in a row at or below the 20th percentile) comes
     free from the per-season series.
4. Cross-checks:
   - eVMODIS NDVI cumulative z, same method;
   - ASIS grassland historic drought frequency;
   - FEWS rangeland WRSI (Kenya).
5. Output: a per-season code stack plus frequencies in R/3 naming, so `head_n` / livestock VoP exposure
   runs through S6.

**Projectable index (V2, for NEX-GDDP futures, #31 Option C):**
1. Over rangeland pixels, score observed SPEI-3/6/12 (already built) and a 12-month NDWS (from S2)
   against the zFPARc ≤ 20th-percentile events, 2006-2025: hit rate, false-alarm rate, Heidke skill.
2. Pick the timescale per agro-ecological zone.
3. **Go/no-go:** if no index shows useful skill, publish V1 only and state that projected livestock
   drought is not defensible.

---

## 4. Gates

- **AgERA5 v1.0 vs v2.0** radiation, 2006-2022, same grid: expect agreement within rounding (the ATBD
  says processing is unchanged). Any systematic difference is a stop.
- **S2 equivalence:** the observed index code is byte-identical to the NEX-GDDP upstream, with only the
  input roots differing (diff the scripts).
- **S5 partition:** classes sum to `any`, and `any` + `none` = 1 per pixel.
- **S6 G6b:** product any + none equals the exposure denominator per (iso3, crop).
- **FPAR continuity:** check for a step in zFPARc across the 2018-2023 MODIS → VIIRS intercalibration
  window before trusting trends or percentiles.
- **Observed vs NEX-GDDP:** report the difference in NTx35/NDWS frequency between this pipeline's
  1995-2014 (optional, same code) and the NEX-GDDP historic. NEX-GDDP is bias-corrected to its own
  reference, not CHIRPS/CHIRTS, so this is a reported diagnostic, not a gate.

## 5. Effort (estimates, one analyst plus node time)

| piece | estimate |
|---|---|
| S1 climate ingest: CHIRTS-ERA5 is the long pole (~600-700 GB global read for 2005-2025 × 2 vars, cropped on the fly). AgERA5 about 2 h at 16 workers; CHIRPS `p25` small. | about 1 week, mostly unattended |
| S2 parameterise upstream index scripts + smoke | about 1 week |
| S3-S5 new scripts + fixtures | about 1.5 weeks |
| S6 + gates | a few days |
| **first crops present-day baseline (NTx35 × NDWS, 2006-2025)** | **about 4 weeks** |
| livestock THI | +1 week |
| rangeland FPAR layer, V1 (~70-90 GB FPAR, phenology windows, percentiles) | +2-3 weeks, can run in parallel with S2-S5 |
| projectable-index skill test, V2 | +2 weeks after V1 |

## 6. Still to decide

1. Confirm the **2006-2025** window. 2005-2024 is the alternative if 2025 data are not yet final in
   every source.
2. Phenology source for rangeland seasons: ASAP / ASIS phenology, or our own from the FPAR
   climatology.
3. Percentile classes 20 / 10 / 5 (IBLI-aligned) vs z-score cut-offs.
4. Whether to also run **1995-2014** through the same code, for the observed-vs-NEX-GDDP diagnostic.
   It costs only node time.
