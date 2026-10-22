# Hazard pipeline: what is missing, the observed baseline, and a step-by-step map

**For:** Pete, planning the simplified baseline (CHIRPS v3 + CHIRTS-ERA5) hazard × exposure work, and
anyone joining the Carbon Credits for Adaptation analysis.
**State:** `develop` after the #13 publish (2026-10-16/19) and the #47 fix in flight, as of 2026-10-22.
Companion to `HANDOVER_2026-10-12_carbon-credits-adaptation-hazard-briefing.md`. That briefing
predates `period=annual` and the physical tiers going live; see `HANDOVER_2026-10-20.md` §1.

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
| grazing-system mask | decides where a rangeland index applies at all | GLPS directory declared (`R/0_server_setup.R:353-355`), unused, contents unchecked |
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
| monthly `PTOT`, `TMAX`, `TMIN`, `TAVG` | CHIRPS v3 monthly + CHIRTS-ERA5 monthly | **built and published**, Africa 0.05°, 1981-present (`R/observational/1_get_chirps_chirts.R`; `africa-precipitation-monthly-seasonal`) |
| SPEI-1/3/6/12/24 | same, Hargreaves PET, 1991-2020 fit | **built**; SPEI-3 and SPEI-12 published (`R/observational/2_calculate_obs_spei.R`; `africa-spei-drought`) |
| per-pixel climatologies (mean/min/max/sd; 1995-2014, 1991-2020, full record) | same | **published**, 13 periods × 9 variables |
| admin monthly and period tables | same | **published** |
| daily-derived hazard indices (`NTx*`, `NDWS`, `NDWL0`, `THI`, `HSH`, `NDD`) | needs CHIRPS v3 **daily** + CHIRTS-ERA5 **daily** | **not built.** Nothing in the repo ingests either daily product. The CDH catalog has records for both (`chirps-v3-daily`, `chirts-era5-daily`), and CHC serves them. |
| threshold classes, frequency, interactions, × exposure (R/2, R/3) | — | **not run on any observed source** |

**The older observed baseline is a different product.**
- The `atlas_delta` tree (`metadata/hosts.json`, `atlas_hazards/cmip6/indices`) holds daily-derived
  indices from **CHIRPS v2.0 + CHIRTS v1.0** (+ AgERA5). The upstream download scripts point at
  CHIRPS-2.0 and CHIRTSdaily v1.0 (`hazards_upstream/R/01_download_data/download_chirps.R:37`,
  `download_chirts.R:37`).
- CHIRTS v1.0 ends in 2016.
- This tree is what the 2024 `risk_prototype/` public files and the original delta-method futures
  came from. It is not v3/ERA5, not maintained, and not used by any current product.

**Input gaps for an observed run of the current indices:**

| index | needs | have it observed? |
|---|---|---|
| `PTOT`, `NDD` | daily or monthly precipitation | CHIRPS v3 daily: available at CHC, not ingested. Monthly: yes. |
| `NTx35`, `NTxM` | daily Tmax | CHIRTS-ERA5 daily: available at CHC (1983-), not ingested |
| `NDWS`, `NDWL0` | daily P, Tmax, Tmin, **solar radiation** (+ wind and RH for water-balance v2), soils | radiation and humidity are **not** in CHIRPS/CHIRTS; needs ERA5 / AgERA5 |
| `THI` | daily Tmax + **relative humidity** | CHIRTS-ERA5 is Tmax/Tmin only; RH needs ERA5 |
| SPEI | monthly P, Tmax, Tmin | **yes, already built** |

---

## 3. How the system works, step by step, and where each artefact is

Server roots, using `metadata/hosts.json` names:
- `INDICES` = `{common_data}/atlas_nex-gddp_hazards/cmip6/indices`
- `SEASONAL` = `{common_data}/atlas_nex-gddp_hazards/cmip6/indices_seasonal`
- `DATA` = `{common_data}/nex-gddp-cimp6_hazards/Data`

`<period>` is `jagermeyr` or `annual`. `<scen>_<gcm>_<yyyy-yyyy>` is, for example,
`historic_ACCESS-CM2_1995-2014` or `ssp585_MRI-ESM2-0_2041-2060`.

| # | step | code | output (server) | content | public? |
|---|---|---|---|---|---|
| 0 | daily NEX-GDDP → **monthly index** | `hazards_upstream/R/04_indices/calc_*.R`, `fast_calc_*.R`; copied by `02_preprocess_data/saveNexGDDPindicesFollowing-atlas_hazards-structure.R` | `INDICES/<scen>_<gcm>_<yyyy_yyyy>/<IDX>/<IDX>-YYYY-M.tif` | one raster per month; for example monthly count of NTx35 days, or monthly PTOT | no |
| 1 | monthly → **one value per season** | `R/1_make_timeseries.R` → `hazard_stacker` (`R/haz_functions.R:1296`) | `SEASONAL/<period>/<scen>_<gcm>_<yyyy_yyyy>_<IDX>_<stat>.tif` | **the time series**: 19 layers, one per season; stat `sum` / `mean` / `max` from `haz_metadata.csv` | no |
| 2a | **threshold → binary 0/1** per season | `R/2` §1, `rast_class()` | `DATA/hazard_timeseries_class/<period>/<scen>_<gcm>_<yyyy-yyyy>_<IDX>-<stat>-<G\|L><thr>.tif`, for example `…_NDWS-mean-G20.tif` | 19 layers of 0/1 per index × threshold, **per GCM** | no (stale 2024 copies under `risk_prototype/`) |
| 2b | **frequency** = mean of 0/1 over 19 seasons | `R/2` §2, §2.1 | `DATA/hazard_timeseries_risk/<period>/` (same names) + `<scen>_ENSEMBLEmean_…` / `ENSEMBLEsd_…` | 1 layer, 0-1, per GCM and ensemble | no |
| 2c | index mean/sd (not frequency) | `R/2` §4 | `DATA/hazard_timeseries_mean/<period>/…_mean.tif`, `_sd.tif` | climatology of the index values | no |
| 3a | **interaction code** per season: dry×1 + heat×10 + wet×100 → {0, 1, 10, 100, 11, 101, 110, 111} | `R/2` §5.2 (`R/2_calculate_haz_freq.R:1574-1615`) | **never written**, held in memory only | — | — |
| 3b | **class frequency** = share of 19 seasons with each exact code; `any`, `none` = 1 − `any` | `R/2` §5.2, `int_risk()` | `DATA/hazard_timeseries_int/<period>/<scen>_<gcm>_<yyyy-yyyy>_<dry>+<heat>+<wet>.tif`, for example `…_NDWS-G20+NTx35-G14+NDWL0-G5.tif` | 9 layers (`any`, `none`, `dry`, `heat`, `wet`, `dry+heat`, `dry+wet`, `heat+wet`, `dry+heat+wet`), per GCM + ENSEMBLEmean/sd | no |
| 3c | **per-crop bundle**: all scenarios × timeframes for one crop, severity and set | `R/2` §5.3 | `DATA/hazard_risk/<period>/<crop>_<model>_<sev>_<set>_int.tif`, for example `maize_ENSEMBLEmean_severe_NDWS+NTx35+NDWL0_int.tif` | the stack R/3 reads | no |
| 4 | **× exposure** per pixel | `R/3` §4.1 | `DATA/hazard_risk_{vop,vop_usd,ha,prod,n}/<period>/<same name>_<variable>.tif` | class frequency × VoP / ha / t / head | no |
| 5 | **zonal sum** to GAUL 2024 admin2 (+ adm1/adm0 sums) | `R/3` §4.2 | `haz-freq-exp_<variable>_<model>_int_adm_<sev>.parquet` | the table | **yes**: `s3://digital-atlas/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=<v>/period=<p>/model=ENSEMBLEmean/severity=<s>/int=multi-hazard.parquet` |

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
| **drought — absolute** | seasonal PTOT below a percentile of 1995-2014, or `NDD` / max dry spell | 12-month PTOT percentile + consecutive-failure count | CHIRPS v3 monthly (have it); daily for `NDD` (ingest) | +1-2 weeks; daily ingest is the long pole |
| **heat** | `NTx35` and a revised crop threshold (flowering-stage, not EcoCrop Topt) | THI needs RH. Interim: days Tmax > 35 °C as a crude proxy, labelled as such. | CHIRTS-ERA5 **daily** Tmax (ingest from CHC, 1983-) | about 2-3 weeks incl. ingest |
| water balance (`NDWS`) | as now | — | needs radiation + soils (ERA5/AgERA5) | not in a first version |
| THI properly | — | THI on Tmax + RH at Tmax | ERA5 humidity | not in a first version |

**Recommendation:**
1. Start with **SPEI drought (crops: crop window; livestock: 12-month, grazing mask) × exposure** on the
   existing observed SPEI. It needs no new data.
2. Add **CHIRTS-ERA5 daily `NTx35`** as the heat axis once ingested.
3. Build it as a new, small script pair that writes per-season `uint8` codes. Do not push observed
   data through R/1-R/2, which carries the maize window, three-axis structure and in-memory codes
   that we are trying to leave behind.
4. Reuse R/3 §4 for the multiply and zonal sum. It only needs class-frequency rasters named the R/3
   way.
