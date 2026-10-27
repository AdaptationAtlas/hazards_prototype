# PLAN — a streamlined observed hazard pipeline (CHIRPS v3 + CHIRTS-ERA5 + AgERA5)

**Status:** v2, 2026-10-23. Revised after a five-way design review (efficiency, science, integration,
gates, provenance), using the reviewer definitions in `.claude/agents/`. §7 lists the findings and how
each is resolved. **Built so far:** upstream changes (a)-(c) (`5f60694`). **S1 ingest** (`python/obs/s1_ingest.py`, 2026-10-27): built, reviewed (integration + efficiency), macbook Kenya smoke PASS; node smoke/full run dispatched (`DISPATCH_cglabs_obs_s1_ingest.md`). **Rangeland V1 seasonal step built** (`R/obs/s6_rangeland_fpar.R`, selftest 11 assertions): seasons from the ASAP phenology modes (1-2 per cell, dekad indices 1-72), cumulative FPAR per season and start year, z against all start years, yearly score = worse season, severity by empirical per-cell percentiles Q20/Q10/Q05 (type 1; frozen thresholds written out; tie diagnostic), a consecutive-failure layer, and an R/3 livestock bundle per species-system with set `FPARz+THI-max+NDWL0` (dry = forage deficit, heat = THI from the S3 annual12 stack when present, wet = NDWL0). Decision §6 item 2 taken as ASAP phenology (same product family; a 12-month fallback is not needed because every rangeland cell has a season-1 phenology). **Note for the user:** with n start years the extreme tier is 1 year in n (0.05 only at n = 20). **Node findings folded into S1 (2026-10-27):** the FAO AgERA5 COGs are georeferenced half a cell off the C3S grid (corner as centre; measured by the v1-vs-v2 gate, which otherwise PASSED: v1.0 = v2.0 to float precision) — S1 now shifts by (−0.05, +0.05)°; `pr` ceiling 1,500 mm/d (TC Kenneth 647 mm over Cosmoledo is real); four stale pre-rewrite `AGERA5-RH06` objects → daily-mean RH uses ≥ 4 of 5 hours and flags the day; a stop-path deadlock fixed. **Rangeland FPAR ingest built** (`python/obs/fpar_ingest.py`): ASAP FPAR dekads → 0.25° rangeland-weighted means (area-weighted; the ASAP grid is half a pixel off the lattice), static rangeland fraction and phenology modes (season start/end dekads, 1-72), NRT dekads younger than 90 days skipped, per-file Last-Modified/ETag manifest; Kenya smoke PASS. **S6/FPAR integration review applied (2026-10-27):** S6 `--spinup` (default 1) so the seasons and timeframe label match S3-S5 (`2006-2025`), S6 **stops** without the S3 THI/NDWL0 stacks unless `--no-thi` (diagnostic bundles only, never to R/3); FPAR ingest writes a source tag (URL + Last-Modified + ETag) into every file and resumes by comparing it (a reprocessed ASAP dekad is rewritten), failures go to `manifests/fpar_failed_<stamp>.csv` (no cancelled-future rows in the data manifest), window shape/transform asserted against the mask window, `validate` reads the cutoff from the ingest manifest (`--cutoff` override) and checks manifest coverage. Selftests 7 (FPAR) and 12 (S6). Still open: registering `obs_fpar`/`obs_codes` in `R/00_paths.R` (coupled to the 34-key list in `0_server_setup.R`); GLPS split not implemented (bundles are per species-system name only; §3 item 1 said ASAP rangeland ∩ GLPS — the ingest uses ASAP rangeland only, R/3 then weights by GLW4 VoP). **Science review applied (2026-10-27, `hazard-science-reviewer`, measured against the live ASAP server):** (1) the operational ASAP archive is two product versions spliced — V062 Terra/Aqua MODIS (reprocessed 2023-09) to 2018-03-21, V070 MODIS/VIIRS (2024-03, VIIRS intercalibrated per pixel-dekad, Seguini et al. 2026 ESSD 18:309) from 2018-04-01 — so 12 of 19 baseline seasons are V062 and 7 V070; the ingest now records `Product-version`/`consolidation_period`/`file_creation` per file and manifest row, and the PASCAL F3 splice check (share of flagged seasons ≥ 2018 vs 7/19) is a **publish gate**; (2) phenology is a joint weighted mode of the (s1,e1,s2,e2,n) tuple (per-layer modes paired OND-first/MAM-first populations in mixed Kenyan cells); (3) `--min-age` 110 + stop on `consolidation_period != CF` (final rewrites measured 102-129 d after the dekad start); (4) tie diagnostic fixed (was blind at n = 10/20, moderate tier only) and extended to all tiers + distinct-value count; (6) `--no-thi` bundles go to `Data/hazard_risk_DIAGNOSTIC/`; (8) `baseline_moments.tif` (mean/sd per season) so a later period can be classified against the frozen thresholds; (9) the v05 phenology is a 1-108 three-year frame (strings fixed; code was right); the Kenya smoke then showed 24 of 882 two-season cells "OND-first" (s1 frame-year 1, s2 frame-year 2): S6 now pairs season 1 of y with season 2 of y+1 there (the OND(Y-1)+MAM(Y) pastoral year), a one-layer shift of the season-2 series; `Product-version` is only inside the `description` tag and is parsed from it; (10) flag values 251/254 excluded; (12) consecutive-failure benchmark is k(k−1)/(n(n−1)). **Design findings for the user (not code):** (5) the bundle's dry frequency is a design constant (4/19 = 0.211 moderate everywhere), so R/3's exposure = 0.211 × VoP carries no hazard information and must stay out of any cross-commodity "any hazard" sum; the information is in which years (`z_year`, `rel_deficit_year`) and the new magnitude layers `deficit_magnitude.tif` (relative deficit of a 1-in-5 year; worst year) — proposal: publish the magnitude layers as the rangeland product and the bundle only as a relative-event layer with that caveat in the sidecar; (7) non-stationarity (Sahel regreening) biases flagged years to the start of the record — F3 reports the per-year counts; a `--detrend` sensitivity is possible; (Q3) yearly score = min(z1, z2) is "any season failed" and is driven by the noisier short rains; alternatives (i) z of cum1 + cum2 (annual forage supply, carry-over) or (ii) z of 12-month cumulative FPAR from s1 (phenology-light) — reviewer recommends (i) or (ii) primary, min as sensitivity — **decision pending**; (14) `--spinup 1` drops a real 2005 season only for label alignment (n = 19 not 20) — say so in the sidecar; (11) dry+heat in the bundle is not same-window co-occurrence (ASAP season vs 12 months from the maize planting month). **R/3 + publisher plumbing (d)-(j), 2026-10-27, default-preserving:** `obs` climate tree in `hosts.json` / `00_paths.R` (working_dir = OBS_ROOT, so `Data/` is where S5 writes and R/3 reads); `timeframe_choices = ggcmi-crop` and `base_rast = metadata/base_rast_nexgddp.tif` when `ATLAS_CLIMDAT_SOURCE=obs`; `R3_MODEL_ALLOW` (default `ENSEMBLE|historic`) replaces the literal that silently dropped non-ensemble files; R/3 writes an `ensemble$provenance` block instead of a 1-member ensemble claim for obs; publisher `--source` with per-source scenario set / baseline / member expectations threaded into G3, G6b, G6 and the sidecar checks. Publisher fixture extended (43 assertions). R/3 also needs `Data/mapspam`, `Data/GLW4_2020` and `Data/boundaries` under OBS_ROOT: symlinks from the NEX working dir (dispatch step). **Cross-node reproducibility (2026-10-27):** PASCAL and the macbook produced byte-identical S1 smoke outputs (5/5 per-variable digests equal; `DISPATCH_pascal_obs_s1_repro.md`); note rasterio bundles its own GDAL (3.12.2 on both), independent of the env's `gdalinfo`. **S3-S5** (`R/obs/s3_s5_obs_hazards.R` + `metadata/obs_crop_windows.csv`): built with a synthetic end-to-end selftest (18 assertions); under review. Seasonal aggregation is its own small function (same rule as `hazard_stacker`, but the 24-month slice starts at month 1, so it is right whatever the minimum planting month in the box; `hazard_stacker` is only right when some cell plants in January). **Design change from the S1 review:** S1 writes on the **full 1440×400 NEX-GDDP lattice** (NaN outside Africa) so R/1-R/3 and `base_rast` work unchanged, and enforces a per-variable NaN-mask invariant across days. Changes (a)-(c) were node-verified on 2026-10-24: 744 outputs old == new with max |diff|  0, the gap census is clean, and `DAYS_CHECK=calendar` runs on all three NEX calendars (`DISPATCH_cglabs_obs_s2_equivalence.md`). Decisions D1-D5 recorded 2026-10-23 (§8). S0 probes answered 2026-10-24: CHIRTS and CHIRPS now come from the CDH Hugging Face Zarr cubes, and ingest runs on CGlabs (§5).

**Goal:** a **present-day** observed baseline of hazard frequency × exposure for a small set of heat
and drought hazards, for crops and livestock. It keeps the per-season series that the current chain
throws away. The historical baselines already exist (NEX-GDDP 1995-2014; observed climatologies).
What is missing is *now*.

## 0. Decisions so far (Pete, 2026-10-22)

| decision | choice |
|---|---|
| grid | 0.25°, the NEX-GDDP grid (`metadata/base_rast_nexgddp.tif`). CHIRPS v3 `p25` already aligns with it (verified). |
| baseline years | present day. Window **2006-2025 → 19 seasons**: a season that crosses the year boundary loses one, as in the NEX chain (handover §3 step 1). Ingest 2005-2025. |
| NDWS / NDWL0 | AgERA5 radiation; keep the "mean of monthly counts" aggregation |
| livestock drought | observed FPAR seasonal-anomaly layer, plus a projectable index chosen by skill against it (§3) |

---

## 1. Inputs

All remote sources were checked anonymously on 2026-10-22/23. **Licences are a blocker** (§8, D1).

| need | source and path | units → required units | licence (as found) | provenance handling |
|---|---|---|---|---|
| daily P | **CHIRPS v3 daily `rnl`, CDH Zarr v3 cube on Hugging Face**: `huggingface.co/datasets/aaguilar90/chirps-v3-daily-rnl/resolve/<rev>/chirps-v3-daily-rnl.zarr`. 0.05°, 60°N-60°S, 1981-01-01 → 2026-08-31, `int16 × 0.1` mm, Zstd, chunks 128 d × 300 × 300, lat north→south, ocean chunks absent (404). **Pin `rev` = `e9466d3109c6d087c6cf60c19ca6ae20873d358a`.** Fallbacks: CHC `products/CHIRPS/v3.0/daily/final/rnl/p25/` (throttled, see §5), or GEE `UCSB-CHC/CHIRPS/V3/DAILY_RNL`. | mm/day; aggregate 0.05 → 0.25° as an exact 5×5 mean | HF card: CC-BY-4.0 (the redistributor's statement); the CHC page says **both** "public domain" and CC-BY 4.0. Record all three. | one vintage (cube built 2026-10-08); pin the HF revision sha; manifest of revision + chunk list |
| daily Tmax, Tmin | **CHIRTS-ERA5 daily, CDH Zarr v3 cube on Hugging Face**: `huggingface.co/datasets/aaguilar90/chirts-era5-daily/resolve/<rev>/chirts-era5-daily.zarr`. 0.05°, 70°N-60°S, 1981-01-01 → 2026-10-03, `int16 × 0.1` °C, same chunking. **Pin `rev` = `c44a0b58772e32f7cbb2b5a740d5e6586ceeb054`.** Not on GEE (GEE has CHIRTS v1 1983-2016 only). | °C; 5×5 mean to 0.25° | HF card: CC-BY-4.0 (the redistributor's statement). Upstream CHC: **none stated, "experimental"**. Record both; ask CHC for terms and a DOI. | **one vintage for the whole series.** This avoids the CHC throttle (8-10 kB/s after ~100 MB, PASCAL S0 probe) and the 2023-snapshot/2026-pull seam the node mirror would have caused. Pin the revision sha. |
| daily radiation | AgERA5 v2.0 SRF, FAO mirror: `storage.googleapis.com/fao-gismgr-c3s-data/DATA/C3S/MAPSET/AGERA5-SRF/` (**10.4 MB/day** measured, 256-tiled COG; an Africa `-projwin` read is ~2 MB/day) | **J m⁻² d⁻¹ → ÷1e6 → MJ m⁻² d⁻¹**. `peest2` expects MJ: NEX `rsds` is converted in preprocessing (`hazards_upstream/R/02_preprocess_data/preprocess_nex-gddp-cmip6_daily_data.R:83`). | **CC-BY-SA-4.0** on the FAO catalogue (CDS: CC-BY) → share-alike propagates to NDWS/NDWL0 | archive rewritten Sep 2025; "v2.0" is inferred, not stamped in the files → manifest of GCS generation + md5Hash; gate on generation drift |
| RH for THI | AgERA5 **RH15** (RH at 15:00 local; the closest stand-in for RH at Tmax). Only RH06/09/12/15/18 exist. | % | per layer, FAO catalogue: RH06/09 CC-BY-SA, RH15 "other-open", RH18 CC-BY → treat as SA until clarified | as SRF |
| soils | `sscp_world.tif`, `ssat_world.tif` on the node | — | **unrecorded** → trace the source | |
| crop calendars | GGCMI Phase 3 v1.01, on the node | months | **null** in `metadata/catalogue/ggcmi-crop-calendars.json` → trace | |
| grazing mask | GLPS `glps_gleam_61113_10km.tif` (node) ∩ ASAP `asap_mask_rangeland_v04.tif` (393 MB, ETag recorded) | — | GLPS null (possibly FAO NC-SA); ASAP unverified | |
| rangeland vegetation | JRC ASAP FPAR, `agricultural-production-hotspots.ec.europa.eu/data/indicators_fpar/fpar/` (~305 MB/dekad global, single GeoTIFFs) | FPAR byte | **dataset licence unverified.** The CC-BY 4.0 cited earlier was the ESSD *article's*. Dataset DOI 10.2905/1aac79d8-0d68-4f1c-a40f-b6e362264e50. | rolling NRT: six estimates per dekad, consolidated ~90 days later → ingest dekads ≥ 90 days old only; per-file manifest; dated baseline snapshot |
| cross-check | USGS/FEWS eVMODIS NDVI, `edcintl.cr.usgs.gov/downloads/sciweb1/shared/fews/web/africa/east/dekadal/evmodis/ndvi/` | — | USGS public domain | East Africa only |

**Credential-free?** Yes for every remote source, as long as radiation comes from the FAO mirror. The
CC-BY route for radiation (CDS) needs a credential (D1).

---

## 2. Climate-hazard pipeline (crops + livestock heat)

**Isolation (integration and efficiency reviews).** The observed series runs as its **own climate
source and working tree**:
- a new `climdat_source = "obs"` in `metadata/hosts.json` / `R/0_server_setup.R`, with its own
  `working_dir`, `Data/` and `indices*` roots;
- so its `hazard_risk*` directories cannot mix with NEX-GDDP bundles (otherwise R/3's ensemble count
  would read 19 members and block the next NEX publish);
- the raw inputs get new raw keys (`chirps_v3_raw`, `chirts_era5_raw`, `agera5_raw`, `asap_fpar_raw`),
  not the existing `chirts_raw`.

| stage | does | output | changes to existing code |
|---|---|---|---|
| **S1 ingest** | **read the HF Zarr cubes in place** (terra `ZARR:` driver, which needs GDAL ≥ 3.11 - CGlabs has 3.11.3, PASCAL 3.4.3 - or Python xarray). Only the Africa chunks are fetched (CGlabs measured 18-27 MB/s aggregate on 8-16 streams, zero errors; `DISPATCH_obs_hf_probe.md`). Absent chunks → NA. **Reader caveats from that probe:** (i) GDAL's ZARR driver opens the cube **without georeferencing**, on 3.11.3 (CGlabs) and 3.13.3 (PASCAL) alike, so this is not a version issue and will not age out. Time is unusable through GDAL too (`DIM_time_VALUE=0` on every band) (extent `0,7200,0,2600`, no CRS), so S1 must set the grid explicitly (lon −180…180, lat 70…−60 for CHIRTS and 60…−60 for CHIRPS, 0.05°, EPSG:4326) or read the coordinate arrays with Python xarray (needs `zarr>=3` installed on the node). Otherwise `crop()` silently works in pixel space. (ii) `terra` and xarray already apply the `scale_factor` 0.1, so **do not scale again**. (iii) Time units are `days since 1980-01-01`, while the series starts 1981-01-01 (raw value 366). Select by date label, never by raw offset. (iv) xarray `chunks={}` needs `dask`; use `chunks=None` or add dask to the env. **Preferred reader: Python xarray** (real coordinates), writing the 0.25° daily GeoTIFFs that terra then reads. CHIRPS and CHIRTS-ERA5 each take an exact 5×5 mean to 0.25°. AgERA5 SRF ÷1e6 and RH15, area-weighted 0.1 → 0.25. | **daily tifs in the layout the upstream scripts read**: `<OBS_ROOT>/nex-gddp-cmip6/{pr,tasmax,tasmin,rsds,hurs}/historical/<obs-token>/<var>_YYYY-MM-DD.tif`, about 38k files, ~18 GB. Plus a manifest per source. | new script; symlink `chirps_wrld/` and `atlas_hazards/soils/` into `OBS_ROOT` |
| **S2 daily → monthly index** | upstream `fast_calc_NDWS.R`/`NDWL0.R`, `calc_NTx.R`, `calc_THI.R`, `calc_PTOT.R`, with `COMMON_DATA=<OBS_ROOT> GCMS=<obs-token> YRS=2005:2025` | `<IDX>-YYYY-MM.tif` | **(a)** water-balance seed months become a parameter (`AVAIL_SEED_MONTHS`). They are hard-coded `'1995-01','2021-01'` at `fast_calc_NDWS.R:137` / `NDWL0.R:137` / `fast_calc_waterbalance.R:80`, so a 2005 start fails and 2021-01 would silently reset the store mid-series. **(b)** `calc_NTx.R:31-32` (and THI/PTOT) skip on bare `file.exists`; switch them to `should_skip()` so `FORCE_OVERWRITE` works. **(c)** missing days are dropped silently (`fls[file.exists(fls)]`); add `stopifnot(length == days_in_month)`. |
| **S3 per-season value, per crop window** | per crop, its GGCMI rainfed calendar (SPAM ↔ GGCMI name map declared; crops without one fall back to maize, flagged). Perennials and livestock: 12 months. Same stats as now. | one stack per index × window, **19 layers**; an incomplete season is NA, never partial | new script |
| **S4 classify + encode** | fixed thresholds (`haz_classes.csv`), then per season **`code = dry·1 + heat·10 + wet·100`**, the R/2 encoding, so `int_risk()` is reusable. NA input → NA code, never 0. | code stack per crop-window × severity, saved | new script |
| **S5 frequencies** | `any = mean(code>0)`, `none = mean(code==0)` (independently), classes via `int_risk()`. **Per-crop bundles only, no `generic-crop`**: per-crop windows make every set crop-dependent. Written on the full `base_rast_nexgddp` grid with the R/3 `_int` naming and 9 layers. | `<crop>_<obs-token>_<sev>_<set>_int.tif` | new script |
| **S6 × exposure** | R/3 §4 in the obs tree | admin parquets | **(d)** an R/3 model allow-list per product. `ensemble_only` keeps only `ENSEMBLE|historic` (`R/3:495-496, 1077-1078, 1367-1368`), so obs files are silently dropped and the run still exits 0. Do **not** name obs files `ENSEMBLEmean`, which would be a false provenance claim. **(e)** an obs-provenance sidecar block in place of the 18-member ensemble block. **(f)** one new period value, `ggcmi-crop`, declared in `timeframe_choices`. |
| **publish** | `scripts/r3_publish_tiers.R` | `source=<obs-token>` (decide once: existing observed products use `chirps-chirts-era5`) | **(g)** a `--source` option (`S3_KEY_BASE` hard-codes `nex-gddp-cmip6`, `:294`). **(h)** a per-source spec: G3's scenario set and G6b's baseline token (both hard-code the five NEX scenarios and `historic`, `:388`, `R/checks/r3_tier_drift_vs_live.R:88,196`); membership checked only for NEX. **(i)** a new CDH record; do not edit the NEX one. |

**v1 hazards:** crops `NTx35` × `NDWS` (× `NDWL0` optional); livestock heat `THI` (D3).

---

## 3. Rangeland drought (livestock)

**Observed layer (V1):**
1. ASAP FPAR (≥ 90-day-old dekads), masked to ASAP rangeland ∩ GLPS.
2. Accumulate per pixel per season: ASAP phenology, or a 12-month rangeland year where seasons are
   not separable. Season-end must also anchor the SPEI accumulation in the skill test.
3. **Scale (D4).** Either compute at 500 m, block-wise, one season at a time (the full Africa stack is
   363 M px × 756 dekads, 2.2 TB as doubles), or aggregate seasonal sums to 0.25° first, about 3,000×
   less work. R/3 needs 0.25° in any case.
4. **Classes and what they mean** (science and gate reviews).
   - In-sample percentiles over 19-20 seasons give P = 4/20, 2/20, 1/20 **at every pixel**. P is then
     a design constant, not a measurement; the information is in *which* seasons fall in a class.
   - Severe and extreme are the record's bottom two seasons, and the bottom one.
   - For a baseline layer that is honest if it is stated. For comparison over time, freeze the baseline
     percentile **values** and apply them as absolute thresholds to other periods.
   - Do not add or compare percentile P with absolute-class crop P or with NEX-GDDP futures.
   - The "IBLI/KLIP 20th percentile" convention needs a citation. Early IBLI triggered on predicted
     mortality (Chantarat et al. 2013).
   - A consecutive-failure class (expected ~0.04/season under independence) is a separate product,
     pooled spatially. It has no slot in R/3's nine classes.
5. Cross-checks:
   - eVMODIS cumulative z-NDVI (East Africa). It is essential, not optional: FPAR integrates woody
     canopy, which damps herbaceous anomalies, so stratify by tree cover.
   - ASIS grassland drought frequency.
   - FEWS rangeland WRSI (Kenya).
6. R/3 packing: a 9-class `_int` stack with FPAR in the dry slot and the others empty. G6b uses the
   masked denominator for livestock, or sets `none = 1 − any` on the full grid.

**Projectable index (V2, #31 Option C).** Score SPEI-3/6/12 and 12-month NDWS against FPAR ≤ P20
events over rangeland:
- **Base-rate-matched** predictors (index ≤ its own P20).
- HSS, hit rate, and the false-alarm *ratio* (named, per Barnes et al. 2009).
- A 2006-15 / 2016-25 split or leave-one-season-out, with block bootstrap for spatial autocorrelation
  (Wilks 2016).
- **A pre-registered go/no-go:** HSS lower 95 % bound > 0, plus a skill threshold to set before
  looking.

State the stationarity assumption, given CO₂ greening.

---

### 3b. Caveats from the S3-S5 science review (2026-10-27) — carried into the product text

- **A 12-month "mean of monthly stress days" measures dry-season length, not drought.** In a unimodal
  climate with 7-8 dry months the annual mean exceeds 15/20 every year, so the dry class fires with
  P = 1 and zero interannual variance; conversely NDWL0 is diluted. This is the mechanism behind the
  maize 0.59-vs-0.26 and cocoa 94 %-vs-7.5 % findings, and the NEX `annual` product shares it (#24).
  **Falsifiable check on the first run:** the share of `annual12` land cells with dry P ∈ {0, 1}
  across the 19 seasons; if most semi-arid cells sit at 1, the layer carries aridity. **v1 ships it as
  is, labelled;** the fix (percentile classes of the 12-month products against their own record, or a
  wet-season-only dry axis) is the §3.4 rule and belongs with #24/#25.
- **Livestock dry axis (must reach the sidecar and the user):** NDWS is the Jones & Thornton (2009)
  crop bucket (Ea/Ep < 0.5, generic rooting depth) over a 12-month window; its frequency is a
  climatological aridity map, not forage shortfall; 15/20/25 days per month were never calibrated for
  grazing; forage (FPAR, §3) is the real constraint. `THI_max` is the max over 365 days, so moderate
  and severe can fire on a single day per year.
- **Rice:** the NDWL0 wet class is the production condition for lowland/irrigated rice (> 60 % of SSA
  rice area) and is not interpretable as a hazard (#25).
- **Seasons:** labelled by start year; `timeframe` 2006-2025 means seasons starting 2006-2024. The
  KNBS/SPAM harvest-year convention differs; state it in the sidecar.
- **Statistics:** one realisation × 19 seasons: SE of P at 0.1 is ~0.07; the extreme tier is 0-1
  seasons. 2006-2025 is non-stationary for heat: report continental NTx35 `any` for 2006-2015 vs
  2016-2024 and state any difference above sampling error.
- **Proxies** are flagged in `metadata/obs_crop_windows.csv` and must reach the sidecar. Chickpea and
  pigeonpea got their own `custom` windows (residual-moisture sowing; 8-month cycle). Still debatable:
  cowpea→bean, lentil→pea, sesame/tobacco/sweet potato→maize, yams→cassava (~8 months), wheat→spring
  wheat (misplaces South African winter wheat).
- **GGCMI artefacts:** barley carries 2-month and 12-month cells; the maize calendar is NA on ~1.7 %
  of land (Sahara margins, pastoral). S3 reports window-length counts and anchors the 12-month window
  at January where maize is NA.

## 4. Gates and fixtures

**Before any node run, five macbook fixtures** in the `R/checks/fixture_*.R` pattern. Each pairs an
honest input that passes with a corruption that must fail.

| fixture | checks |
|---|---|
| `fixture_obs_ingest_grid.R` | 2e7 J → 20 MJ; −9999 → NA, not ≈ −400 after averaging; ranges (rsds 0-40 MJ, Tmax ≥ Tmin, P ≥ 0, RH 0-100); `compareGeom` against `base_rast_nexgddp` and the soils grid |
| `fixture_obs_avail_continuity.R` | seed parameter works; AVAIL is continuous across a year boundary (Dec → Jan, including 2020 → 2021); a deleted AVAIL file stops the run; a missing day stops the run |
| `fixture_obs_season_stack.R` | a Nov-Mar window: 19 layers; every month read lies inside the ingest range; incomplete season = NA; season→year label |
| `fixture_obs_code_stack.R` | code ∈ {0, 1, 10, 11, 100, 101, 110, 111}; NA → NA; decoding reproduces the classes (swapped weights must fail); `any` and `none` computed independently sum to 1 |
| `fixture_obs_percentile_classes.R` | with ties and NAs, frequency = floor(p·n)/n; tied pixels (constant or zero FPAR) flagged |

**Node gates:**
- **S2 run-equivalence (replaces "byte-identical").** Run the parameterised S2 on one NEX-GDDP GCM
  historic year through `OBS_ROOT`-style paths. Max |new − existing step-0 tif| must be 0. `git diff`
  touches only the declared parameter lines.
- **S1 completeness:** 365/366 days per variable-year, with no gaps; manifest written.
- **S1 reader vs an independent reference:** for a few dates and both cubes, the 0.05° cube values over Africa equal the CHC GeoTIFF for the same day to within the int16 quantum (|diff| ≤ 0.05). PASCAL's 2015-06-15 CHIRTS Tmax is byte-identical to CHC's, so it can serve. Mean Africa land Tmax must be physically plausible (about 25-38 °C; 2015-06-15 measured 34.5). **PASSED on PASCAL 2026-10-24:** Tmax and Tmin on 2015-06-15 and 2020-12-31, 1,172,052 cells each, max |diff| exactly 0.05 (the quantum), with no cell over it and identical NA patterns, aligned by coordinates to 1e-4° (`DISPATCH_pascal_obs_env.md`). Re-run it on whichever node runs S1, on S1's own output. This catches both a georeferencing slip and a double scale factor.
- **PET sanity:** monthly-mean PET 2-8 mm/d over the smoke box. This catches a missed ÷1e6.
- **AgERA5 v1.0 vs v2.0**, on the native 0.1° grid, 2006-2022: |monthly bias| < ε per region, 99th
  percentile of |diff| < δ, and no trend in the difference. These are invariants, not "within
  rounding".
- **CHIRPS timing sensitivity:** NDWS/NDWL0 from `rnl` vs `sat` (IMERG-timed) on the smoke box. A mean
  difference above 1 day/month is material and must be reported.
- **FPAR continuity:** pseudo-invariant pixels (desert, evergreen forest). Pre/post VIIRS mean
  difference < k·σ and a Pettitt break test. Synthetic 0.03 step must fire; the clean series must not.
  Also, the share of ≤ P20 events in VIIRS years must match the share of VIIRS years (binomial).
- **S5 partition** and **S6 G6b** as above; G5 schema without `--allow-schema-drift`.
- Smoke and full runs use **separate roots**.

**Reported diagnostics (not gates):**
- observed 1995-2014 vs NEX-GDDP historic (D2): the share of pixels inside the 18-GCM spread;
- AgERA5 SRF vs the NEX historical `rsds` climatology. A difference above 5 % attributes the NDWS
  offset to radiation. NEX-GDDP is bias-corrected to GMFD, and ERA5 shortwave runs high under tropical
  cloud.

---

## 5. Volumes and effort (corrected)

| item | volume / node time |
|---|---|
| CHIRTS-ERA5 2005-2025, Tmax + Tmin, **from the HF cube** | Africa window ≈ 90 MB per 128-day slab per variable → ~60 slabs × 2 ≈ **11 GB**. A laptop measured 4-5.4 MB/s on 8 streams (≈ 35-45 min); node rate pending `DISPATCH_obs_hf_probe.md`. The CHC route was ~388 GiB, throttled to 8-10 kB/s on PASCAL and about 1 MB/s per stream on CGlabs, so it is not workable. |
| CHIRPS v3 2005-2025, from the HF cube | ≈ 55 MB per slab → **~3.3 GB** |
| AgERA5 SRF + RH15 | **~31 GB windowed** (≈ 2 MB/day Africa `-projwin` × 7,670 days × 2 variables; A5 measured on both nodes). That is the basis of the earlier 25-30 GB; a full download would be 10.4 MB/day ≈ 160 GB. CGlabs 8 streams ≈ 18-21 MB/s → about 30 min. |
| CHIRPS `p25` | small |
| S1 retained output | ~18 GB daily 0.25° tifs |
| S2-S5 compute | under ~1 h of node time; the stages are cheap at 116k cells |
| ASAP FPAR 2005-2025 | ~230 GB full, ~46-58 GB windowed |
| eVMODIS (East Africa) | to size |
| optional obs 1995-2014 (D2) | +~370 GiB CHIRTS transfer |

Without the 18-GCM × 5-scenario fan-out, compute is not the constraint: S2-S5 take under ~1 h of node
time. The elapsed time is engineering, verification and node round trips.

| work | realistic |
|---|---|
| upstream changes (a)-(c) + five fixtures | 1-2 days |
| S1 ingest (4 sources, manifests, completeness + PET gates) | 2-3 days |
| S3-S5 (windows, codes, frequencies) | 2 days |
| R/3 + publisher per-source changes (d)-(i) | 1-2 days |
| node: S2 run-equivalence, AgERA5 v1 vs v2, CHIRTS streaming (2-12 h), full run, gates | 3-5 days elapsed |
| **first present-day crops baseline** | **~2 weeks elapsed** |
| livestock THI (both variants) | +2-3 days |
| rangeland FPAR V1 | +1-1.5 weeks, partly in parallel |
| V2 skill test | +1 week |

**Dev strategy (Pete, 2026-10-23): stay on `develop`, additive.**
- New code lives in new files (`R/obs/`). The node workflow pulls `develop`, so no branch.
- **Every change to shared code** (upstream index scripts, R/3, the publisher, path registries) keeps
  today's NEX-GDDP behaviour as its default. A fixture proves that default before the change is
  pushed.
- The observed series is switched on only by `climdat_source=obs` and its env settings, and is inert
  otherwise.
- Change (c), stopping on missing input days, is a deliberate behaviour change for NEX-GDDP runs too.
  A gappy input now halts instead of undercounting. Announce it in the dispatch that ships it.
- Commit only our own paths; other sessions edit the same tree.

**No usable local copies (both nodes searched, 2026-10-26; `DISPATCH_obs_find_local_copies.md`).**
- No CHIRPS v3 and no CHIRTS-ERA5 for 2005-2025 on either node.
- No AgERA5 v2.0. CGlabs' local AgERA5 runs to 2025, but it is a v1.0 (to 2022) + v1.1.1 (2023+)
  stitch, and RH appears to change from 12 LT to 18 LT at 2023-01-01. Do **not** use it for the
  series. Keep FAO AgERA5 v2.0 for the whole span.
- Residual: on PASCAL three project roots were unreadable and two large trees hit the search cap.

**Where ingest runs (S0 probes, 2026-10-24): CGlabs.**
- It has GDAL 3.11.3, which `terra` needs to read Zarr v3. PASCAL has 3.4.3.
- 40 uncapped cores and 111 TB free.
- Faster GCS and JRC links: AgERA5 at 11-12 MB/s per stream; FPAR at 6.9 MB/s against PASCAL's 1.75.
- PASCAL's head start (its CHIRTS-ERA5 1991-2020 mirror, 2023 snapshot) no longer matters now that
  the HF cube supplies one vintage.
- Do not read PASCAL's `chirts_era5/tavg/`: it is a mislabelled East-Africa Tmax crop.
- **PASCAL is now a working fallback for the cube route** via the per-user `obs` conda env (GDAL 3.13.3, zarr 3.4.1, xarray; `server-environment.md` §5). Build conda envs there with `CONDA_FETCH_THREADS=1`: parallel fetches were reset twice, and conda still printed `done` and exited 0 without creating the env.

## 6. Order of work

1. D1-D5 decided.
2. Code changes (a)-(c) in the upstream scripts, plus the S2 run-equivalence probe on NEX-GDDP.
   - This proves the reused code before any observed data exists, and it is independently useful,
     because (b) and (c) are latent NEX defects too.
3. Five fixtures.
4. S1 ingest (manifests), with completeness and PET gates, then the AgERA5 v1 vs v2 gate.
5. S2 → S5 on the smoke box → full.
6. R/3 + publisher changes (d)-(i); G6b.
7. Rangeland V1 in parallel from step 4; V2 after V1.

---

## 7. Review findings (2026-10-23) and resolutions

| # | finding (reviewer) | resolution |
|---|---|---|
| 1 | AgERA5 J vs MJ: PET ~10⁶× (science, gates, efficiency) | ÷1e6 in S1; fixture + PET gate |
| 2 | AVAIL seed months hard-coded; 2021-01 silent reset (all) | change (a); continuity fixture |
| 3 | "byte-identical S2" cannot pass (science, gates, integration) | replaced by the run-equivalence probe |
| 4 | upstream scripts read daily tifs in a NEX layout; S1 planned yearly cubes (integration, efficiency) | S1 writes that layout under `OBS_ROOT` |
| 5 | R/3 drops non-ENSEMBLE files silently; NEX/obs mixing would poison the NEX sidecar (integration, efficiency) | separate obs tree; change (d) |
| 6 | publisher hard-codes source, scenarios, baseline token, 18-member check (integration) | changes (g)/(h) |
| 7 | the `generic-crop` expansion is invalid with per-crop windows (integration) | per-crop bundles only |
| 8 | plan encoding 1/2/4 vs R/2 1/10/100 (integration) | adopt 1/10/100 |
| 9 | 20 seasons stated; 19 real (integration) | 19 |
| 10 | missing days dropped silently; NTx ignores FORCE_OVERWRITE (gates, integration, efficiency) | changes (b)/(c); completeness gate; separate smoke roots |
| 11 | in-sample percentile P is constant by construction (science, gates) | stated in §3.4; frozen-value rule for comparisons; user note |
| 12 | daily CHIRPS timing affects NDWS/NDWL0, not just dry spells (science) | rnl vs sat sensitivity gate |
| 13 | obs vs NEX not like-for-like (GMFD reference, ERA5 SW bias, windows) (science) | reported diagnostics; D2 |
| 14 | THI humidity definition differs from NEX (science, gates, integration) | D3 |
| 15 | skill-test design underspecified (science, gates) | §3 V2 rewritten |
| 16 | FPAR: woody damping, VIIRS splice, NRT revisions, 230 GB, OOM at 500 m (science, provenance, efficiency) | eVMODIS essential; continuity gate; ≥ 90-day rule; D4 |
| 17 | share-alike licence via AgERA5; several inputs with no licence on record (provenance) | D1; licence column above; trace the soils, GLPS, GGCMI and FPAR licences |
| 18 | silently rewritten archives (FAO, CHIRPS p25, FPAR) (provenance) | per-source manifests; drift gates |
| 19 | CHIRTS volume overstated, FPAR understated (efficiency) | §5 corrected |

## 8. Decisions (Pete, 2026-10-23)

| # | decision |
|---|---|
| D1 | **Accept CC-BY-SA-4.0 for the NDWS/NDWL0 layers.** Radiation comes from the FAO AgERA5 mirror, credential-free. Record both FAO (CC-BY-SA-4.0) and CDS (CC-BY) terms. Layers that do not use AgERA5 (NTx35 from CHIRTS-ERA5 alone, for example) carry their own licence, still to be established for CHIRTS-ERA5. The product licence is declared **per layer**, never as one CC-BY-4.0 claim. |
| D2 | **Yes: run 1995-2014 through the observed code after the present-day run.** Reported diagnostic: observed vs NEX-GDDP historic, plus the radiation climatology comparison. About +370 GiB of CHIRTS streaming. |
| D3 | **Carry both THI variants**, labelled in the provenance: (i) NEX-like, Tmax with the AgERA5 daily-mean RH, *approximated as the mean of RH06/09/12/15/18*; (ii) Tmax with RH15 (≈ RH at Tmax). The NEX side is unchanged until #48. |
| D4 | **Aggregate seasonal FPAR sums to 0.25° first**, then take percentiles. |
| D5 | Source token defaults to `chirps-chirts-agera5` (radiation and RH are AgERA5), as distinct from the existing monthly products' `chirps-chirts-era5`. Change only before the first publish. |
