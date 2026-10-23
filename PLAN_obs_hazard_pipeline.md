# PLAN — a streamlined observed hazard pipeline (CHIRPS v3 + CHIRTS-ERA5 + AgERA5)

**Status:** v2, 2026-10-23. Revised after a five-way design review (efficiency, science, integration,
gates, provenance), using the reviewer definitions in `.claude/agents/`. §7 lists the findings and how
each is resolved. **Nothing is built yet.** Decisions D1-D5 recorded 2026-10-23 (§8).

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
| daily P | CHIRPS v3 daily `rnl`, 0.25° `p25`: `data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final/rnl/p25/` | mm/day | the CHC page says **both** "public domain" and CC-BY 4.0; record both | regenerated in place 2026-04-02 → snapshot date + checksum manifest |
| daily Tmax, Tmin | CHIRTS-ERA5 daily: `data.chc.ucsb.edu/experimental/CHIRTS-ERA5/{tmax,tmin}/tifs/daily/` (25.9 MiB/day, full-width strips; yearly netCDF also exists) | °C | **none stated, "experimental"**; ERA5-derived → ask CHC for terms and a DOI | per-file Last-Modified/size/md5 manifest; no version in the names |
| daily radiation | AgERA5 v2.0 SRF, FAO mirror: `storage.googleapis.com/fao-gismgr-c3s-data/DATA/C3S/MAPSET/AGERA5-SRF/` (12.8 MB/day, 256-tiled COG) | **J m⁻² d⁻¹ → ÷1e6 → MJ m⁻² d⁻¹**. `peest2` expects MJ: NEX `rsds` is converted in preprocessing (`hazards_upstream/R/02_preprocess_data/preprocess_nex-gddp-cmip6_daily_data.R:83`). | **CC-BY-SA-4.0** on the FAO catalogue (CDS: CC-BY) → share-alike propagates to NDWS/NDWL0 | archive rewritten Sep 2025; "v2.0" is inferred, not stamped in the files → manifest of GCS generation + md5Hash; gate on generation drift |
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
| **S1 ingest** | stream, crop to Africa, aggregate to 0.25°, **discard the raw data**: CHIRPS `p25` native; CHIRTS-ERA5 5×5 mean; AgERA5 SRF ÷1e6 and RH15, area-weighted 0.1 → 0.25 | **daily tifs in the layout the upstream scripts read**: `<OBS_ROOT>/nex-gddp-cmip6/{pr,tasmax,tasmin,rsds,hurs}/historical/<obs-token>/<var>_YYYY-MM-DD.tif`, about 38k files, ~18 GB. Plus a manifest per source. | new script; symlink `chirps_wrld/` and `atlas_hazards/soils/` into `OBS_ROOT` |
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
| CHIRTS-ERA5 2005-2025, Tmax + Tmin | **388 GiB read in full** (~260 GiB windowed, since full-width strips limit the saving). Streamed and discarded; ~2-12 h at 10-50 MB/s. |
| AgERA5 SRF + RH15 | ~25-30 GB windowed (197 GB full); ~2 h at 16 workers |
| CHIRPS `p25` | small |
| S1 retained output | ~18 GB daily 0.25° tifs |
| S2-S5 compute | under ~1 h of node time; the stages are cheap at 116k cells |
| ASAP FPAR 2005-2025 | ~230 GB full, ~46-58 GB windowed |
| eVMODIS (East Africa) | to size |
| optional obs 1995-2014 (D2) | +~370 GiB CHIRTS transfer |

| work (analyst) | estimate |
|---|---|
| code changes (a)-(i) + new S1/S3/S4/S5 scripts | ~2.5 weeks |
| five fixtures + node gates | ~1 week |
| first crops present-day baseline (NTx35 × NDWS, 2006-2025) | **~4-5 weeks total** |
| livestock THI | +1 week (depends on D3) |
| rangeland FPAR V1 | +2-3 weeks, in parallel |
| V2 skill test | +2 weeks after V1 |

---

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
