# Atlas hazard data for avoided-loss estimation — a briefing

**For:** Jemal S. Ahmed, Activity 2, Gates Carbon Credits for Adaptation (INV-089367)
**From:** Africa Agriculture Adaptation Atlas hazards pipeline, `AdaptationAtlas/hazards_prototype`
**State described:** branch `develop` as of 2026-10-12 (HEAD `6b6fc5b`). Nothing described here is on `main`.
**Live objects described:** the hazard-exposure tiers published 2026-10-06 and the exposure
denominator republished 2026-10-08, read directly off S3 on 2026-10-12.

Every claim cites a file as `path:line` relative to the repository root on `develop`. Where the
code and a metadata record disagree, this document follows the code and says so.

---

## 0. What you need to know first

1. **The Atlas does not publish a per-pixel frequency surface.** What is public is a table of
   *hazard frequency × exposure*, summed to GAUL 2024 admin units (§1.6). A usable
   P(hazard, severity) per commodity per admin unit can be recovered exactly from it as a ratio
   (§1.7). The per-pixel frequency rasters exist only on the processing server.
2. **Severity classes are fixed absolute thresholds, not percentiles** (§1.2). Nothing moves with
   the reference period, so a change in P between periods is a real change in exceedance, not an
   artefact of the class definition.
3. **Severity classes are cumulative, not bands.** "Moderate" means *at least* moderate, so it
   contains every severe and extreme season. Summing across severities double-counts (§1.4).
4. **Interaction classes are mutually exclusive and exhaustive.** Each season falls into exactly
   one of: none, dry, heat, wet, dry+heat, dry+wet, heat+wet, dry+heat+wet (§2). This is the
   Atlas's whole treatment of co-occurrence, and it is the piece most worth inheriting.
5. **One crop calendar is used for everything.** The season window is the GGCMI rainfed-*maize*
   planting-to-maturity window, applied to every crop and every livestock species (§3.2).
6. **The livestock drought layer is the generic crop soil-water-stress index**, with the crop
   thresholds and the maize season window (§4.4). Your concern about it is correct, and in two
   respects the situation is worse than you describe (§6.1).
7. **A republication is pending.** The full rebuild (issue #13) has finished its frequency stage
   but has not reached the exposure stage. When it publishes, one hazard set is renamed and one is
   added (§1.8). Pin the S3 versions in §7 for anything you report.

---

## 1. Hazard frequency

### 1.1 Climate inputs

- **Source:** NEX-GDDP-CMIP6 daily data. The indices are computed by the upstream half of the
  pipeline (`hazards_upstream/R/04_indices/`) from daily `pr`, `tasmax`, `tasmin`, `rsds` and
  `hurs` (for example `hazards_upstream/R/04_indices/fast_calc_NDWS.R:207-211`;
  `calc_THI.R:47-50`). They are written as monthly rasters: 240 files per hazard per 20-year
  window (`R/1_make_timeseries.R:384-396`).
- **Grid:** 0.25° (`metadata/base_rast_nexgddp.tif`: 1440 × 400 cells at 0.25°).
- **Scenarios:** `historic` and `ssp126`, `ssp245`, `ssp370`, `ssp585`.
- **Periods:** `historic` = 1995-2014, plus 2021-2040, 2041-2060, 2061-2080 and 2081-2100
  (`R/2_calculate_haz_freq.R:111-116`).
- **The baseline is 1995-2014**, the end of the CMIP6 historical run. It is not a WMO 1991-2020
  normal (`metadata/cdh/africa-hazard-exposure-nexgddp.yaml`, `note`, "BASELINE").
- **Ensemble:** 18 GCMs. The membership is stamped into each published tier's JSON sidecar
  (`<parquet key>.json`, block `ensemble`). As read on 2026-10-12 it is:
  ACCESS-CM2, ACCESS-ESM1-5, CanESM5, CMCC-ESM2, EC-Earth3, EC-Earth3-Veg-LR, GFDL-ESM4,
  INM-CM4-8, INM-CM5-0, IPSL-CM6A-LR, KACE-1-0-G, MIROC6, MPI-ESM1-2-HR, MPI-ESM1-2-LR,
  MRI-ESM2-0, NorESM2-LM, NorESM2-MM, TaiESM1.
  The publisher refuses to ship any membership other than 18 (`scripts/r3_publish_tiers.R:347-348`).
  Membership has varied between products in the past (issue #26), so read the sidecar rather than
  assuming it.

> `metadata/haz_metadata.csv` still describes the indices as computed from CHIRPS, CHIRTS and
> AgERA5. Those descriptions belong to the observational pipeline. The NEX-GDDP product uses the
> NEX-GDDP inputs listed above.

### 1.2 How a hazard season is defined

The chain has three steps.

1. **Seasonal aggregation** (`R/haz_functions.R:1296-1460`, function `hazard_stacker`, called from
   `R/1_make_timeseries.R:561`).
   - Each monthly index is reduced over the season window (§3.2) with the summary function listed
     in the `function` column of `metadata/haz_metadata.csv` (`R/haz_functions.R:1360, 1420-1434`).
   - This gives one value per pixel per season. A 20-year window yields **19 seasons**, because the
     last season would run past the end of the data (`R/haz_functions.R:1249-1252, 1411`).
2. **Classification** against a threshold. Each season becomes a 0/1 value
   (`R/haz_functions.R:1207-1219`; called at `R/2_calculate_haz_freq.R:862-868`).
   - The comparison is strict: `>` for "greater" hazards and `<` for "less" hazards.
3. **Frequency.** The mean of the 0/1 values over the 19 seasons, per GCM
   (`R/2_calculate_haz_freq.R:923`), then the mean across the 18 GCMs
   (`R/2_calculate_haz_freq.R:997`).

**What the summary function does to the units matters.** The seasonal value of a day-count index
is the *mean of its monthly counts* over the season months, not a season total:

| index | seasonal function | unit after aggregation | meaning of the seasonal value |
|---|---|---|---|
| `NDWS` (water-stress days) | mean | days per month | mean monthly number of days with AET/PET < 0.5 in the season months |
| `NDWL0` (waterlogging days) | mean | days per month | mean monthly number of waterlogging days |
| `NTx35`, `NTx40`, crop `NTx*` | mean | days per month | mean monthly number of days with Tmax above the threshold |
| `PTOT` | sum | mm per season | seasonal precipitation total |
| `THI_max` | max | °F-scale THI | hottest day's THI in the season (see §4.3 caveat) |
| `TAI` | mean, annual only | unitless | Thornthwaite aridity index (not windowed, `R/haz_functions.R:1374-1384`) |

Sources: `metadata/haz_metadata.csv` (`function` and `base_unit` columns), `R/haz_functions.R:1360`.

So the generic severe heat threshold "NTx35 > 14 days" means **more than 14 days above 35 °C per
month, averaged over the season months**. It does not mean more than 14 such days in the season.
The generated threshold table (`metadata/published_thresholds.md`) prints only "day", and the
Atlas front-end has quoted a different number for the same threshold (issue #20). Read the unit
as "days per month".

### 1.3 Severity thresholds

**All thresholds are fixed absolute values.** No percentile, anomaly or reference-period
quantity enters the classification anywhere (`R/haz_functions.R:1207-1219` compares the seasonal
value to a constant). There are two sources of thresholds.

**(a) Generic thresholds**, from `metadata/haz_classes.csv`, also rendered in
`metadata/published_thresholds.md`:

| hazard | index | moderate | severe | extreme |
|---|---|---|---|---|
| dry | `NDWS` | > 15 | > 20 | > 25 (days per month) |
| dry | `PTOT` (low) | < 500 | < 400 | < 300 (mm per season) |
| wet | `NDWL0` | > 2 | > 5 | > 8 (days per month) |
| wet | `PTOT` (high) | > 1200 | > 1600 | > 1800 (mm per season) |
| heat | `NTx35` | > 7 | > 14 | > 21 (days per month) |
| heat | `NTx40` | > 1 | > 5 | > 10 (days per month) |

Livestock heat thresholds are species- and zone-specific THI values (§4.3).

**(b) Crop-specific thresholds**, derived at run time from FAO EcoCrop (`metadata/ecocrop.csv`)
for each MapSPAM commodity (`R/2_calculate_haz_freq.R:153-284`):

| index | moderate | severe | extreme |
|---|---|---|---|
| `PTOT` low | < `Rain_Opt_Min` | < mean(`Rain_Abs_Min`, `Rain_Opt_Min`) | < `Rain_Abs_Min` |
| `PTOT` high | > `Rain_Opt_Max` | > ceil(mean(`Rain_Opt_Max`, `Rain_Abs_Max`)) | > `Rain_Abs_Max` |
| `NTxM` (days above `Temp_Opt_Max`) | > 7 | > 14 | > 21 |
| `NTxS` (days above ceil(mean(`Temp_Opt_Max`, `Temp_Abs_Max`))) | > 7 | > 14 | > 21 |
| `NTxE` (days above `Temp_Abs_Max`) | > 1 | > 5 | > 10 |

For maize this gives `NTxM` = 33 °C, `NTxS` = 40 °C, `NTxE` = 47 °C, and PTOT low thresholds of
600 / 500 / 400 mm. Where one MapSPAM commodity maps to several species, the thresholds are
averaged (`R/2_calculate_haz_freq.R:277-281`).

> **A unit mismatch in the crop-specific rainfall classes.** EcoCrop records rainfall limits as
> **annual** totals; this is stated in the CDH EcoCrop record
> (`cdh-catalog/records/ecocrop/ecocrop.yaml`, the `note` and the `ropmn` / `ropmx` descriptions).
> The code compares them with the **seasonal** PTOT sum over the maize window
> (`R/2_calculate_haz_freq.R:175-198`). A season total is smaller than an annual total, so the
> crop-specific dry class (`PTOT-L`) will fire more often than the agronomy intends, and the wet
> class less often. This is not flagged anywhere in the repository. Treat the crop-specific
> rainfall sets as prototype. Tracked as issue **#46**.

### 1.4 Severity classes are cumulative, not bands

Each tier is classified independently against its own threshold. Because the thresholds are
monotone (moderate < severe < extreme for `>` hazards, and the reverse for `<`), a season that
exceeds "extreme" also exceeds "severe" and "moderate". For one hazard:

    P(≥ moderate) ≥ P(≥ severe) ≥ P(≥ extreme)

**Band probabilities follow by differencing**, for single-hazard marginals only:

    P(moderate band) = P(≥ moderate) − P(≥ severe)

For an avoided-loss sum, an equivalent formulation needs no differencing:

    Σ_s P(≥ s) · [L(s) − L(s−1)],   where L(s) is the loss fraction at severity s and L(0) = 0

Over a single hazard this is exactly Σ over bands of P(band) · L(band). It does **not** carry
over cleanly to the interaction classes; see §2.3.

### 1.5 What exists behind the 20-year frequency

| level | exists? | where |
|---|---|---|
| per-season 0/1 classification, single hazard, per GCM | **yes, on the processing server only** | `Data/hazard_timeseries_class/<period>/`, one layer per season, written at `R/2_calculate_haz_freq.R:846-869` and retained |
| per-season interaction class, per GCM | **no** | collapsed to a 20-year mean inside the computation (`R/haz_functions.R:1239`; `R/2_calculate_haz_freq.R:1601`) and never saved |
| per-pixel 20-year frequency, per hazard and threshold, per GCM and ensemble mean/SD | yes, server only | `Data/hazard_timeseries_risk/<period>/` (`R/2_calculate_haz_freq.R:919-1013`) |
| per-pixel interaction-class frequency stacks per crop, per GCM and ensemble mean/SD | yes, server only | `Data/hazard_risk/<period>/<crop>_<model>_<sev>_<set>_int.tif` (`R/2_calculate_haz_freq.R:1811`) |
| admin-unit frequency × exposure tables | **yes, public** | §1.6 |

There is an old public copy of per-pixel frequency rasters under
`s3://digital-atlas/risk_prototype/data/hazard_risk/`, `…/hazard_timeseries_risk/` and
`…/hazard_timeseries_class/`, last written in 2024 by the manual script `R/push_to_s3.R:292-416`.
**Do not use it.** It predates every fix in the issue log (ensemble membership #26, NDWS
saturation, the THI threshold corrections) and nothing maintains it.

### 1.6 What is published

**Product:** `africa-hazard-exposure-nexgddp`, documented in
`metadata/cdh/africa-hazard-exposure-nexgddp.yaml`.
- Built by `R/3_freq_x_exposure.R` §4, published by `scripts/r3_publish_tiers.R`.
- Licence CC-BY-4.0.
- Key: `s3://digital-atlas/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=jagermeyr/model=ENSEMBLEmean/severity={moderate|severe|extreme}/int=multi-hazard.parquet`
  (public HTTPS on `digital-atlas.s3.amazonaws.com`).

**How a row is made** (`R/3_freq_x_exposure.R`):
1. Per pixel: `value = frequency × exposure` (L212, L215, L241). The 0.05° exposure is first
   mass-conservingly summed onto the 0.25° hazard grid (L133-152).
2. The products are summed over GAUL 2024 admin-2 zones, rasterised on the 0.25° grid with
   `touches = TRUE` (L328-334, L1444, L1466-1472).
3. Admin-1 and admin-0 rows are sums of the admin-2 rows (L1484-1500).

**Schema**, read from the live object:

| column | content |
|---|---|
| `iso3`, `admin0_name`, `admin1_name`, `admin2_name`, `gaul0_code`, `gaul1_code`, `gaul2_code` | location. **The same file holds admin-0, admin-1 and admin-2 rows.** A higher-level row has the lower-level names NULL. Filter to one level before summing. |
| `scenario`, `timeframe` | `historic`/`1995-2014`, or `ssp126…ssp585` × the four future windows |
| `model` | `ENSEMBLEmean` only |
| `severity` | `moderate`, `severe`, `extreme` (one file each) |
| `hazard_vars` | which three indices define dry, heat and wet (§2.1) |
| `hazard` | `any`, `none`, `dry`, `heat`, `wet`, `dry+heat`, `dry+wet`, `heat+wet`, `dry+heat+wet` |
| `crop` | 33 crops, `generic-crop`, and 10 livestock species-zones (§3.1, §4.1) |
| `value` | Σ over pixels of frequency × value of production, nominal 2021 USD |
| `exposure_var`, `exposure_unit` | `vop`, `nominal-usd-2021` |

Each file is about 62 MB and 66-68 million rows. Query it with predicate pushdown (DuckDB
`read_parquet` over HTTPS works) rather than loading it whole.

### 1.7 Recovering P(hazard, severity) per commodity — the route that works today

Because `value` is Σ_p f_p·V_p over the pixels p of a unit, and `any` + `none` = Σ_p V_p, the
ratio below is the **value-of-production-weighted mean frequency** of the class within the unit:

    P_u(class, sev) = value(class) / [ value(any) + value(none) ]

taken for one `crop`, `hazard_vars`, `scenario`, `timeframe`, `severity` and admin unit.

I verified the identities on the live severe tier for Kenya (admin-0, historic and SSP5-8.5
2041-2060):
- the seven exclusive classes sum to `any` to within the 3-decimal rounding of the frequencies
  (for example maize, `NDWS+NTx35+NDWL0`, historic: 466.60 M against 466.38 M);
- `any` + `none` is constant across scenarios for a given crop and set (1,117.78 M against
  1,117.82 M).

Three consequences:

- **Weighting by value is the right weighting for your formula.** Σ_u P_u × production_u reproduces
  the pixel-level Σ f_p × production_p, provided production is distributed within the unit like
  value (it is, at fixed prices). For a different weighting (area, or another production map)
  you need the rasters.
- **Divide within the same `hazard_vars` set.** Total value differs slightly between sets
  (maize, Kenya: 1,117.8 M for `NDWS+NTx35+NDWL0` against 1,145.5 M for `PTOT-L+NTxS+PTOT-G`),
  because the indices have different no-data masks.
- **Units with tiny exposure give unstable ratios.** The 0.25° grid cannot resolve about 2,100
  small admin-2 units that the 0.05° exposure grid can (CDH record note). Use admin-1 or admin-0
  where admin-2 exposure is small.

Baseline production for the same units, on the same grid, is in the exposure denominator
`crop-livestock_all_res-25.parquet` (§7). Its rows with `unit = t` are MapSPAM 2020 production in
tonnes, and its rows with `unit = ha` are harvested area. Select on `unit`: the table holds five
units.

### 1.8 Stability and what is about to change

| item | status |
|---|---|
| `NDWS+NTx35+NDWL0` (generic thresholds) | **Most stable.** This is what the live Atlas notebook reads. Its definition does not change in the #13 rebuild. |
| `PTOT-L+NTxS+PTOT-G` (crop-specific) | **Being retired.** `NTxS` averages the optimum with the survival limit, which gave maize a 40 °C threshold and 0 % heat exposure in Kenya (issue #25). It is replaced by `PTOT-L+NTxM+PTOT-G`, and a new `NDWS+NTxM+NDWL0` is added (`R/2_calculate_haz_freq.R:377-410`). Also subject to the EcoCrop rainfall unit issue in §1.3. Prototype. |
| livestock sets | Prototype; see §4. |
| the `annual` period, ensemble SD, per-GCM tables, and the constant-dollar, area, tonnage and head tiers | Computed or computable, **not published**. Planned in the #13 publish order (`HANDOVER_2026-10-07.md` §2D). |

The rebuild's frequency stage finished on the server on 2026-10-10. A follow-up run for the new
crop set stopped at a guard: a crop-name mismatch, `generic` against `generic-crop`
(`R/2_calculate_haz_freq.R:325` against `:489`), reported in `DISPATCH_cglabs_issue13_rebake.md`.
The exposure stage and the publish follow. No date is committed.

---

## 2. Interactions (co-occurring hazards)

### 2.1 Which interactions exist

There is one interaction structure: a **three-way dry × heat × wet** classification. Each published
`hazard_vars` value names the three indices, in the order dry + heat + wet
(`R/2_calculate_haz_freq.R:402-421, 573-577`):

| `hazard_vars` | dry | heat | wet | applies to | slots fixed or crop-specific |
|---|---|---|---|---|---|
| `NDWS+NTx35+NDWL0` | NDWS | NTx35 | NDWL0 | all crops | all generic; computed once and applied to every crop |
| `PTOT-L+NTxS+PTOT-G` *(live; being renamed `…NTxM…`)* | PTOT low | crop NTx | PTOT high | each crop | all crop-specific |
| `NDWS+NTxM+NDWL0` *(after #13)* | NDWS | crop NTx | NDWL0 | each crop | heat crop-specific, dry and wet generic |
| `NDWS+THI-max+NDWL0` | NDWS | THI_max | NDWL0 | livestock | heat species-specific, dry and wet generic |
| `PTOT-L+THI-max+PTOT-G` | PTOT low | THI_max | PTOT high | livestock | heat species-specific, PTOT generic |

Dry × heat, dry × wet, heat × wet and the triple are all classes within this one structure. No
other interaction is computed: HSH, TAVG, NTx40, TAI and cold are not in any set (HSH: see
`HANDOVER_2026-10-07.md`, "B7").

### 2.2 How the classes are built

For each GCM and each season (`R/2_calculate_haz_freq.R:1574-1615`):

1. The three 0/1 seasonal classifications at **one** severity are encoded as
   `dry × 1 + heat × 10 + wet × 100`. Each season therefore gets a code in
   {0, 1, 10, 100, 11, 101, 110, 111}.
2. For each code, a layer is 1 where the season has exactly that code and 0 otherwise
   (`int_risk`, `R/haz_functions.R:1236-1242`, which masks every other code to 0). That layer is
   averaged over the 19 seasons.
3. `any` = the mean of (code ≥ 1), and `none` = 1 − `any` (`R/2_calculate_haz_freq.R:1600-1610`).
4. Class frequencies are then averaged across the 18 GCMs (`R/2_calculate_haz_freq.R:1679`).
   A mean of partitions is still a partition, so the ensemble classes still sum to 1.

So:

- **The classes are mutually exclusive and exhaustive.** `dry` means *dry only*: dry
  ≥ threshold and heat and wet both below theirs. The seven non-empty classes sum to `any`.
- **Joint probabilities are empirical**, counted season by season within each GCM. Any dependence
  between the hazards that the GCM produces (dry seasons being hot, for example) is carried
  through. No correlation, copula or independence assumption is made or published.
- **All three hazards are classified at the same severity.** There is no "severe dry with
  moderate heat" class. A season that is severe-dry and only moderately hot falls in `dry` in the
  severe file and in `dry+heat` in the moderate file.
- **`dry+wet` is a real, non-empty class.** Dryness and wetness are separate indices summarised
  over the same window, so one season can have high mean stress days and high mean waterlogging
  days (an early dry spell and a late flood, for instance).

### 2.3 How interaction layers relate to single-hazard layers, and what double-counts

**Single-hazard marginals are sums of classes.** At one severity:

    P(dry) = P(dry) + P(dry+heat) + P(dry+wet) + P(dry+heat+wet)        [right-hand side: exclusive classes]
    P(dry ∩ heat) = P(dry+heat) + P(dry+heat+wet)

Single-hazard exposure is not published separately (`R/3_freq_x_exposure.R:1356-1370`, "solo
hazards … can be constructed from the interactions").

**These sums double-count:**

| summing over | why it double-counts |
|---|---|
| hazards' marginals: P(dry) + P(heat) + P(wet) | co-occurring seasons are counted two or three times. The exclusive classes are the correct thing to sum. |
| severity tiers | tiers are cumulative (§1.4) |
| `hazard_vars` sets | they are alternative definitions of the same three hazards, not additional hazards |
| `any` with any class | `any` is the sum of the seven classes |
| admin levels within one file | admin-0 and admin-1 rows are sums of the admin-2 rows |
| `generic-crop` with the individual crops | `generic-crop` is all crops' value combined, under the generic set |

**What we suggest you inherit.** Assign a loss fraction and a practice effectiveness to each
*exclusive class* at a chosen severity, and sum over the seven classes:

    avoided loss_u = production_u × Σ_{c ∈ 7 classes} P_u(c, s) × L(c, s) × E(practice, c, s)

This cannot double-count. It lets a compound class carry a loss that is not the sum of its parts
(L(dry+heat) need not equal L(dry) + L(heat)), which is the point of having the classes. Its limits:

- **Severity must be handled one tier at a time.** The incremental formulation in §1.4 is exact
  only for single-hazard marginals. Interaction classes are not nested across tiers: a season in
  `dry+heat` at severe can be `dry+heat+wet` at moderate. So across tiers you can either
  - run the calculation once per tier and report it as a sensitivity, or
  - use the marginal-increment form for each hazard and accept that the compound effect is then
    additive.
  There is no exact class-by-band decomposition without the per-season data, which is not saved
  (§1.5).
- **No uncertainty is published.** Each GCM's frequency is a count over 19 seasons, so it moves in
  steps of about 0.053. The across-GCM SD exists on the server (`ENSEMBLEsd`). The planned public
  `value_sd` is a sum of per-pixel SDs, not the SD of the aggregate (`R/3_freq_x_exposure.R:1613`),
  so it should not be read as an admin-level uncertainty.

---

## 3. Crop bundles

### 3.1 Commodities and coding

- **Coding:** MapSPAM. The commodity list is `metadata/SpamCodes.csv`: 55 rows, 36 of them single
  commodities (`compound = no`). The `Code` column holds the four-letter SPAM code (for example
  `acof`) and `Code_ifpri_2020` holds the IFPRI 2020 code.
- **Commodities that get hazard classes:** those with `compound = no` and a `Code`
  (`R/2_calculate_haz_freq.R:121-122`). That is **33 crops** in the live product: arabica-coffee,
  banana, barley, bean, cassava, chickpea, cocoa, coconut, cotton, cowpea, groundnut, lentil, maize,
  oilpalm, pearl-millet, pigeonpea, plantain, potato, rapeseed, rice, robusta-coffee, sesameseed,
  small-millet, sorghum, soybean, sugarbeet, sugarcane, sunflower, sweet-potato, tea, tobacco,
  wheat, yams.
- **Not in the hazard product:** MapSPAM's composite groups (other cereals, vegetables, fruits,
  "rest" and so on), and onion, rubber and tomatoes, which carry no `Code`.
- **Correction to the record.** `metadata/cdh/africa-hazard-exposure-nexgddp.yaml` says "Crop, 44
  present". The 44 is 33 crops + `generic-crop` + 10 livestock species-zones.

### 3.2 Season windows: one calendar for everything

- **The window comes from GGCMI Phase 3 crop calendar v1.01, rainfed maize**
  (`mai_rf_ggcmi_crop_calendar_phase3_v1.01.nc4`, `R/1_make_timeseries.R:274-286`). The planting
  and maturity day-of-year values are converted to months.
- **The published `period=jagermeyr`** is the window from the maize planting month to the maize
  maturity month, inclusive, wrapping across the year boundary (`R/haz_functions.R:1397-1402,
  1417-1434`). The name refers to Jägermeyr et al., the authors of the GGCMI calendar.
- **The same window is used for every crop and every livestock species.** There is no
  crop-specific calendar. Coffee, cassava, sugarcane, banana and cattle are all assessed over the
  rainfed-maize season.
- **One season per year.** The second rainy season in bimodal regions (East Africa's short rains,
  for example) is not represented. A start-of-season route with two seasons exists in code but is
  disabled for lack of a usable dataset (`R/1_make_timeseries.R:245-270, 353-355`).
- **The unpublished `period=annual`** is a 12-month window starting at the maize planting month
  (`R/haz_functions.R:1403-1405`). It is not a calendar year.
- So the **indices are computed within a growing season, but it is maize's season**, not the
  commodity's.

### 3.3 Exposure

- **Production data:** MapSPAM2020_AdaptationAtlas_SSA (DOI 10.7910/DVN/Z0HK7R, CC-BY-4.0,
  `metadata/cdh/mapspam2020-adaptation-atlas-ssa.yaml`). This is the Alliance's SSA release, not
  IFPRI's global SPAM 2020 v2r2. Technology `all` (rainfed and irrigated combined) throughout
  `R/3_freq_x_exposure.R` (L380, L400, L415, L428).
- **What is multiplied against frequency** in the published tier: crop **value of production in
  nominal 2021 USD**.
  - The value is MapSPAM production × a FAOSTAT implied farm-gate price, using the median of
    2019-2023, allocated and priced on the native 0.05° grid
    (`R/0.4.2_create_crop_vop_nominal_usd.R`; method in `docs/methods/nominal_price_method.md`).
  - It is summed onto the 0.25° hazard grid before the multiplication (`R/3_freq_x_exposure.R:133-152`).
- **Nominal, not deflated.** Do not compare values across periods as real terms. The Atlas
  standard of constant 2015 international dollars exists as a tier but is not published (§1.8).
- **Physical tiers.** Harvested area (ha), production (t) and livestock head are computed in R/3
  (`R/3_freq_x_exposure.R:980-1042`) but **not yet published** crossed with hazard. Their
  *denominators* are published, and those denominators are what you need for baseline production
  (§1.7, §7).

---

## 4. Livestock bundles

### 4.1 Species, dataset, masks

- **Species:** cattle, goats, sheep, pigs, poultry (`R/0.4.1_create_livestock_exposure.R:117`). No
  camels, buffalo or horses.
- **Dataset:** GLW 4 (Gridded Livestock of the World), 2020 reference year, about 10 km, CC-BY-4.0
  (`R/0.4.1_create_livestock_exposure.R:97-100`; CDH catalog record `glw4-2020`).
  - Densities are converted to head per pixel, then mass-conservingly summed onto the 0.05° or
    0.25° grid (L110, L118, L140).
  - The GLW4-2015 path is disabled; GLW3 is not used.
- **Production-system mask: none.** The Global Livestock Production Systems directory is declared
  (`R/0_server_setup.R:353-355`), but no live script reads it, and `metadata/LPS_legend_RGB.csv`
  is unused. The livestock mask is "species present" (L198, L210).
  - (`metadata/catalogue/glps.json` claims that 0.4.1 consumes GLPS. It does not.)
- **Highland/tropical split.** A binary highlands raster (`afr-highlands`, `R/0_server_setup.R:579`)
  splits each species into `-highland` and `-tropical` exposure (L212-225; `R/haz_functions.R:2128-2155`).
  - "Tropical" means "not highland". It is not a production system.
  - The split masks **exposure** only. The hazard layers for `cattle-highland` cover every pixel,
    and zoning takes effect only when frequency is multiplied by the zoned exposure.
- **Units.** Head per pixel per species. The `total` layer is in TLU (cattle 0.7, goats 0.1, sheep
  0.1, pigs 0.2, poultry 0.01; L610-612).
- **Value.** FAOSTAT national gross production value of meat, milk and eggs per species, spread by
  each pixel's share of national head (L262, L582).
  - **A pricing caveat in the nominal tier.** The nominal-USD livestock value multiplies production
    by the **global** median price for every country: `vop_usd_nominal := production_t *
    price_usd_global` (L483). The gap-filled national price computed on the same line
    (`price_usd_final`) is unused. This looks like a defect and has not been raised as an issue.
  - Livestock nominal values in the published tier carry it. For livestock, prefer head counts
    or the constant-dollar denominator. Tracked as issue **#47**.

### 4.2 Are livestock rows published?

**Yes**, although the CDH record says otherwise. The live `vop_nominal-usd21` parquets contain
rows for `cattle-highland`, `cattle-tropical`, `goats-*`, `pigs-*`, `poultry-*` and `sheep-*`, with
`exposure_var = vop`, under the two livestock `hazard_vars` sets.
- The record's statement "no published … livestock exposure tier" is wrong in substance. What is
  true is that there is no separate livestock *variable* (head counts crossed with hazard are not
  yet published).
- The CDH record will be corrected.

### 4.3 Livestock heat: it is a THI

- **Formula:** `THI = (1.8·T + 32) − (0.55 − 0.0055·RH)·(1.8·T − 26.8)`, from Rahimi et al. 2020
  (`hazards_upstream/R/04_indices/calc_THI.R:59`).
  - T is **daily Tmax** (`tasmax`). RH is **daily mean** relative humidity (`hurs`) (L47-50).
  - Daily-mean RH paired with Tmax overstates THI on the hottest hour. The Atlas's human heat index
    corrects RH to its value at Tmax; THI does not.
  - `THI_mean` is also computed from Tmax (L65), not from mean temperature.
    `metadata/THI_thresholds_provenance.md:15-17` says otherwise and is wrong on this point.
- **Monthly products:** the monthly maximum and the monthly mean of daily THI (L65-66).
- **Seasonal value:** `THI_max` uses `max`, so it is the season's highest daily THI.
- **Thresholds:** species × zone, in °F-scale THI (`metadata/haz_classes.csv:44-103`).
  Extreme values are from Thornton et al. 2021, *Global Change Biology* 27:5762 (Table 2).
  Moderate and severe are a curated mix, documented in `metadata/THI_thresholds_provenance.md`.

| species | tropical M / S / E | highland M / S / E |
|---|---|---|
| cattle | 74 / 82 / 94 | 72 / 79 / 89 |
| goats | 74 / 82 / 94 | 72 / 79 / 89 |
| sheep | 74 / 81 / 93 | 72 / 78 / 86 |
| pigs | 75 / 79 / 92 | 75 / 79 / 89 |
| poultry | 74 / 79 / 92 | 71 / 76 / 89 |

- **An unresolved provenance gap.** The current code does not produce the seasonal NEX-GDDP THI
  series that the frequency stage reads.
  - `R/1_make_timeseries.R:297-298` drops THI and HSH whenever the input path is the NEX-GDDP tree.
  - The THI files on the server come from an earlier run.
  - If that run used `hazard_stacker`, a long-standing slip at `R/haz_functions.R:1344`
    (`max = haz_files_mean`) means "THI_max" would be the seasonal max of *monthly-mean* THI. That
    would bias it low and understate heat exceedance.
  - This has not been checked on the files themselves. Until it is, treat the livestock heat
    classes as **prototype**. Note too that the two biases above (Tmax with mean RH up, possible
    monthly-mean down) point in opposite directions. Tracked as issue **#48**.

### 4.4 Livestock drought and wet hazards

The generic non-heat hazard rows are copied to every species-zone unchanged
(`R/2_calculate_haz_freq.R:138-147`). The livestock dry axis is therefore:

- `NDWS` > 15 / 20 / 25 mean water-stress days per month in the maize season (set
  `NDWS+THI-max+NDWL0`), **or**
- `PTOT` < 500 / 400 / 300 mm over the maize season (set `PTOT-L+THI-max+PTOT-G`).

These are the generic crop thresholds, over the crop window, with nothing specific to rangeland
(`metadata/haz_classes.csv:17-19, 23-25`). §6 takes this up.

### 4.5 Forage and rangeland condition

**Nothing about forage or rangeland condition enters the hazard chain.** Three related layers are
published, all observational, none projected, and all used only as display layers in a Kenya
notebook:

| layer | coverage | what it is | record |
|---|---|---|---|
| FEWS NET **rangeland** WRSI, end of season | Kenya, 0.1°, 2003- | the closest existing thing to a rangeland drought index | `metadata/cdh/eastafrica-wrsi-fews.yaml` |
| MODIS MOD13Q1 NDVI, seasonal means (MAM, OND) | East Africa window, 250 m, 2000-2025 | greenness; **no anomaly, no VCI** | `metadata/cdh/eastafrica-ndvi-modis.yaml` |
| SPEI-3 and SPEI-12 | Africa, 0.05°, 1981-, 1991-2020 base, Hargreaves PET | observational climatic water balance | `metadata/cdh/africa-spei-drought.yaml` |

---

## 5. Not covered, or not done here

**Of the four topics you asked about, these do not exist** (publicly, or at all where stated):

1. **Published per-pixel frequency surfaces**, per hazard × severity × commodity. They exist only on
   the processing server. The public 2024 copies are stale (§1.5).
2. **A published per-season time series.** Per-season single-hazard 0/1 rasters exist on the server
   only. Per-season *interaction* classes are not saved anywhere.
3. **Percentile-based or reference-period-relative severity classes.** All thresholds are fixed.
4. **A joint-probability or correlation model.** Only empirical co-occurrence classes exist.
   **Mixed-severity combinations** do not exist.
5. **Crop-specific growing seasons, or a second season.** One rainfed-maize window is used for all.
6. **Published uncertainty**: the ensemble SD, per-GCM values and per-year values.
7. **Hazard × physical exposure tables** (tonnes, hectares, head), and a constant-dollar tier.
   These are computed but not published. Only their denominators are public.
8. **A livestock production-system mask.** Only highland/tropical exists.
9. **Camels.**
10. **A livestock-specific drought index, or livestock-specific drought thresholds.**
11. **A forage or biomass anomaly layer** of any kind, observed or projected.
12. **Water-point or surface-water access.** The only water layer is a static lakes-and-wetlands mask
    used to *exclude* water cells (`hazards_upstream/R/02_preprocess_data/createAfricaWaterBodiesMask.R`).
13. **Population exposure** under this product.

---

## 6. The livestock drought problem

### 6.1 Do we agree? Yes, and it is worse than your description in two respects

You describe "drought indices built on crop water stress within a growing season". Mechanically,
the livestock drought layer is the following:

- **A generic crop index reused, not even a crop-specific one.**
  - `NDWS` is a single-layer bucket water balance with a Priestley-Taylor-type potential ET, run on
    NEX-GDDP daily inputs.
  - A day counts as stressed when the bucket's AET/PET ratio is below 0.5
    (`hazards_upstream/R/04_indices/fast_calc_NDWS.R:22-75, 165-174`; kernel `wbkernel/src/wb_kernel.cpp:99-147`).
  - It has no crop coefficient. Its soil capacity is built to a **maize rooting depth**, clamped to
    45-100 cm (`hazards_upstream/R/02_preprocess_data/createAfricaSoils.R:29, 105-122`). The link
    from that script to the exact file NDWS reads is not confirmed in the repository.
- **Integrated over the maize season, at rangeland pixels too.**
  - Where there is little or no maize, the GGCMI calendar's maize window still defines the season.
  - The rangeland rainy season, the dry season in which forage runs out, and the year-to-year
    carry-over are not represented.
- **Classified on crop benchmarks:** more than 15, 20 or 25 stress days per month on average.
- **Each season is an independent draw.** The bucket carries soil water from month to month
  (the AVAIL fix, `fast_calc_NDWS.R:140-150`). However, frequency is the mean of independent
  per-season 0/1 classes. There is no persistence term, no consecutive-failure class and no
  multi-season accumulation. The well-documented two-failed-seasons pathway to mortality and
  distress sales cannot appear.

The **alternative set** (`PTOT-L`) is no better. Fixed crop benchmarks saturate in drylands.
Measured on the live **extreme** tier for `cattle-tropical`, historic period, the marginal
probability of the dry class in Kenya's arid counties (the four dry-containing classes ÷ total) is:

| county | `NDWS+THI-max+NDWL0` | `PTOT-L+THI-max+PTOT-G` |
|---|---|---|
| Garissa | 0.47 | 0.40 |
| Mandera | 0.60 | 0.75 |
| Marsabit | 0.75 | 0.58 |
| Turkana | 0.72 | 0.43 |
| Wajir | 0.83 | 0.86 |

So "extreme" drought occurs in 40-86 % of seasons. That is the climatology of a dryland, not a
drought signal, and it says little about which seasons are bad for herds.

**One correction to your framing.** Within the Atlas these rows are not the result of a deliberate
"crop index applied to livestock" decision. They are generic rows copied to livestock so that the
interaction structure would be complete (`R/2_calculate_haz_freq.R:138-147`). Nobody has validated
them for livestock. The issue tracking the gap is hazards_prototype **#31** (open; the measurements
and plan below are posted there): "Rangeland
drought: integrate NDVI and pasture biomass to represent forage-based climate impact for
livestock". This document's recommendation is consistent with it.

Your point that **the error is invisible downstream** stands: the layer produces a well-formed
probability everywhere.

### 6.2 Answers to the specific questions

- **What the layer is mechanically:** a generic soil-water bucket index (or a seasonal rainfall
  total), over the rainfed-maize window, on crop thresholds. See §4.4 and §6.1.
- **Does a forage or biomass anomaly layer exist?** No. MODIS NDVI seasonal means exist for East
  Africa only, with no anomaly, and FEWS rangeland WRSI exists for Kenya only (§4.5).
  - **Could one be built?** Yes, as an *observed* layer. **It cannot be projected.** NDVI and biomass
    have no counterpart in NEX-GDDP, for the same reason the pipeline already excludes the dry-days
    index NDD from projections (`R/2_calculate_haz_freq.R:126`).
  - So under future scenarios a forage anomaly can only serve as a **calibration and validation
    target** for a meteorological index that *is* projectable. It cannot be the hazard itself.
- **Should livestock severity use different benchmarks?** Yes.
  - A rangeland drought class should be defined on the distribution of the index at each pixel,
    for example the 1-in-5, 1-in-10 and 1-in-20 dry season or 12-month period of the 1995-2014
    baseline, per GCM. That is an anchored percentile, **not** a rolling one.
  - Baseline P is then fixed by construction (0.2, 0.1, 0.05), and the future periods carry the
    signal. This changes your method in the way you anticipated: the baseline term becomes a
    design constant rather than an estimate.
  - Fixed absolute benchmarks transferred from crops have no physiological basis for grazing
    animals, and saturate in drylands.
  - A **consecutive-failure** class (two or more failed seasons in a row) belongs alongside them.
- **Is water-point or surface-water access represented?** No (§5, item 12). Observed surface-water
  history exists externally, for example JRC Global Surface Water, but it is not ingested. It is
  not projectable either, and access depends on infrastructure that no climate layer describes.

### 6.3 Options

Effort is in working weeks of one analyst, with node time noted separately. These are estimates,
not commitments.

**Option A — Reframe the existing projectable indices for rangeland.** About 4-6 weeks, plus about
2 days of node time.
- Restrict livestock drought to grazing systems using a production-system mask. The GLPS
  directory exists but must first be checked for contents.
- Compute the index over the 12-month window rather than the maize window. The `annual` period
  already does this from the maize planting month; a rangeland-specific window would be better.
- Use a multi-month water-balance or SPEI-type index from NEX-GDDP. `calc_PET.R` exists upstream.
- Define severity by baseline-anchored percentiles, and add a consecutive-failure class. The
  per-season 0/1 series this needs already exists on the server for single hazards; new code is
  needed for the run-length class.
- **Gives:** a coherent, projectable, rangeland-framed meteorological drought index.
- **Does not give:** any evidence that it tracks forage.

**Option B — Observed forage anomaly, historical only.** About 3-4 weeks.
- Extend the NDVI ingest from Kenya and East Africa to sub-Saharan Africa. The ingest is
  `python/ingest_ndvi_modis.py`; it needs NASA Earthdata credentials, and the current default
  bounding box is Kenya.
- Compute per-pixel seasonal anomalies and a VCI against a fixed baseline, masked to grazing
  systems.
- Candidate biomass products, not yet verified for coverage, latency or licence: Copernicus DMP,
  FEWS NET rangeland products, and WaPOR, which was considered and deferred in
  `archive/dispatches/NDVI_ingest_plan.md`.
- **Gives:** a defensible description of *past* rangeland drought.
- **Gives nothing for future scenarios.**

**Option C — B as the target for A: a validated projectable proxy.** About 3-4 months.
- Over rangeland pixels for 2000-2024, test which observed meteorological index (SPEI at 3, 6 or 12
  months, the 12-month water balance, rainfall percentile, consecutive-failure counts) best predicts
  the observed forage anomaly classes. Choose the index and thresholds on that evidence, then compute
  the same index from NEX-GDDP for the projections.
- **Do the validation with observed climate, not NEX-GDDP.** A GCM's historical run is not
  year-synchronous with observed weather, so its 1995-2014 seasons cannot be matched to NDVI
  seasons. The observational SPEI (CHIRPS v3 + CHIRTS-ERA5, `R/observational/2_calculate_obs_spei.R`)
  already exists Africa-wide, so most of the climate side of the validation is built.
- **Gives:** a projected livestock drought P, with a stated skill against observed forage, on
  benchmarks chosen for livestock. **This is the defensible product.**

**Option D — A forage process model driven by GCMs.** For example, a rangeland biomass model with
grazing pressure.
- More than six months, plus parameterisation data we do not hold.
- **Not achievable by March 2027.**

**Not addressable by any of these within the timeframe:** water-point access; the impact chain from
forage deficit to body condition, milk, conception, mortality and offtake (the loss fraction side,
which is yours); and carry-over beyond the persistence a consecutive-failure class captures.

### 6.4 Recommendation

**Do Option C, with Option A as its projectable half, and set a decision gate in mid-January 2027.**

- **November to mid-January:** the grazing mask, the SSA NDVI anomaly (B), and the validation
  analysis against observed SPEI and water-balance indices.
- **At the gate:**
  - If one index reaches useful skill against forage anomaly classes in rangeland pixels, compute it
    from NEX-GDDP with baseline-anchored percentile classes plus a consecutive-failure class (A).
    Run it through the frequency stage, and publish it as a separate livestock drought set by March.
    This is realistic if the work starts in November and the node is free (a frequency stage takes
    about a day and a half per period).
  - If no index has skill, **report the gap.** Publish the observed forage anomaly as a historical
    layer, and state that no projected livestock drought probability is defensible. We agree with
    you that this is better than a confident wrong number.
- **Until then, do not use either livestock dry axis in an external deliverable.** If a livestock
  heat-only figure is needed, the livestock `heat` class is computable, but read §4.3 first.

---

## 7. Products to use

| product | path (HTTPS base `https://digital-atlas.s3.amazonaws.com/`) | grid | units | licence | pin |
|---|---|---|---|---|---|
| Hazard frequency × crop and livestock value, moderate tier | `domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=jagermeyr/model=ENSEMBLEmean/severity=moderate/int=multi-hazard.parquet` | 0.25° hazard grid, aggregated to GAUL 2024 adm0/1/2 | nominal 2021 USD (frequency is unitless, 0-1) | CC-BY-4.0 | published 2026-10-06 19:53:58 UTC; `versionId=ahAbS.qWD4bnQNHgoIx0F4VgRrWVIWpS`; ETag `cd4d6673d3e84f3993ee9b01ce5d8b25` |
| … severe tier | same key, `severity=severe` | as above | as above | CC-BY-4.0 | 2026-10-06 19:53:30; `versionId=PqB5zDrtvImxm424NIZoM0mWFpoiaLwm`; ETag `f0193a5ca0d46324540720b0f1400893` |
| … extreme tier | same key, `severity=extreme` | as above | as above | CC-BY-4.0 | 2026-10-06 19:54:24; `versionId=UfKtgxwKTqXn8lMu4_9i8sghORnynOeb`; ETag `1c3f2e4b95cd67db324a27dc22e5a8ad` |
| Ensemble membership and field descriptions | each tier's key + `.json` | — | — | CC-BY-4.0 | read with the tier |
| Exposure denominator: production (t), area (ha), value, head | `domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/variable=crop-livestock_all_res-25.parquet` | 0.25° (the hazard grid), GAUL 2024 adm0/1/2 | select on `unit`: `t`, `ha`, `nominal-usd-2021`, `intld15-2021`, `number` | CC-BY-4.0 | 2026-10-08 07:35:52; `versionId=E_epSzh5nNIrf4wLXG20oXdCkBtiiJju`; ETag `4855965ece596288de3944a2c2e9af51` |
| Admin boundaries (join on `gaul2_code`) | `domain=boundaries/type=admin/source=gaul2024/region=africa/processing=analysis-ready/level=adm2/atlas_gaul24_a2_africa.parquet` | vector | — | CC-BY-4.0 (record `africa-admin-boundaries-gaul2024`) | 2025-08-19; `versionId=66c7MiKvkW8w_4q6wfQXly0wy7FRKEIa` |

**Pin by `versionId`.**
- The bucket is versioned, and a public read with `?versionId=<id>` works (checked 2026-10-12).
- The #13 republication will overwrite these keys in place. The pinned versions remain readable for
  at least 270 days.
- The denominator was republished on 2026-10-08. That changed its constant-dollar rows (+20 %); its
  `nominal-usd-2021`, `t` and `ha` rows were unchanged.

**Which rows to use, by stability:**
- **External deliverable, with the caveats stated:**
  - `hazard_vars = NDWS+NTx35+NDWL0` for crops, with the `none` / `any` ratio from §1.7;
  - the denominator's `t` rows for baseline production.
- **Prototype; internal use only:**
  - the crop-specific sets (`PTOT-L+NTx*+PTOT-G`, `NDWS+NTxM+NDWL0`);
  - both livestock sets;
  - livestock nominal-USD values.

**Example: P per class for one crop and unit** (DuckDB, `httpfs` loaded):

```sql
WITH t AS (
  SELECT * FROM read_parquet(
    'https://digital-atlas.s3.amazonaws.com/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=jagermeyr/model=ENSEMBLEmean/severity=severe/int=multi-hazard.parquet?versionId=PqB5zDrtvImxm424NIZoM0mWFpoiaLwm',
    hive_partitioning = false)
  WHERE crop = 'maize' AND hazard_vars = 'NDWS+NTx35+NDWL0'
    AND admin2_name IS NOT NULL                -- admin-2 rows only
)
SELECT gaul2_code, scenario, timeframe, hazard,
       value / SUM(value) FILTER (WHERE hazard IN ('any','none'))
                 OVER (PARTITION BY gaul2_code, scenario, timeframe) AS p
FROM t
WHERE hazard NOT IN ('any');                   -- 7 exclusive classes + none; they sum to 1
```

---

## 8. Corrections to the records that this review found

These are listed so that you are not misled by the records in the meantime. They will be fixed on
our side and are tracked together as issue **#49** (the days-per-month unit also on #20).

- `metadata/cdh/africa-hazard-exposure-nexgddp.yaml`:
  - "no … livestock exposure tier" is wrong: livestock value rows are published (§4.2);
  - "44 crops" is 33 crops + generic + 10 livestock (§3.1).
- `metadata/haz_metadata.csv`:
  - it describes CHIRPS/CHIRTS/AgERA5 inputs and a Steadman HSH that the NEX-GDDP chain does not
    use (§1.1);
  - it labels THI "cattle" although it is applied to all five species.
- `metadata/THI_thresholds_provenance.md`: it says THI_mean uses mean temperature; it uses Tmax
  (§4.3).
- `metadata/catalogue/glps.json` and `metadata/catalogue/nexgddp-indices-seasonal.json`: they claim
  consumers and producers that the code does not have (§4.1, §4.3).
- `metadata/published_thresholds.md`:
  - "day" means days per month, averaged over the season (§1.2);
  - it already lists the post-#13 hazard sets, which are not yet live (§1.8).
