# Next full R/2 + R/3 rebake — consolidated checklist

Several fixes are deferred to bundle into the next full hazard rebake (amortise
the ~day of warmup). This is the pickup list so nothing is silently missed.
Run on cglabs (live Data/ + S3). Last updated **2026-09-17** (post issue-#9 publish).

## ⚠️ CRITICAL — §3 and §5.3 are toggle-only (FORCE_OVERWRITE does NOT enable them)
`run3` and `run5.3` are gated on explicit env toggles, NOT `FORCE_OVERWRITE`
(R/2 ~582/627). A plain `FORCE_OVERWRITE=1` bake will **skip the crop-stack (§3)
and per-crop interaction (`_int`, §5.3) producers** → `haz_risk/` would not refresh
and R/3 §4.1 (which reads those) consumes stale stacks. A full rebake that must
refresh `haz_risk` MUST export:
```
FORCE_OVERWRITE=1 RUN_R2_RUN3=1 RUN_R2_RUN5_3=1
```
(`RUN_R2_RUN5_2=1` too if the §5.2 combo tifs in `haz_time_int_dir` also need a
rebuild; §5.2 already runs under FORCE_OVERWRITE, so usually not separately needed.)

## R/3 run controls added 2026-09 (know these before launching section 4)
| Control | Default | Effect |
|---|---|---|
| `R3_CROP_VOP_USD` | `2015` | `2021` points crop nominal-USD at 0.4.2's `spam_vop_nominal-usd-2021_all`, matching the livestock side, the published label and the reference parquet. The 2026-09-16 publish used `2021`. The default is the legacy raster and is a **2015-vs-2021 currency-vintage mismatch inside the usd product** - set `2021` unless you have a reason not to. |
| `R3_ALLOW_41_FAILURES` | unset | Downgrades the 4.1 hard abort to a warning. Deliberate partial runs only; a silent 4.1 failure is what hid the #9 bug for months. |
| `SKIP_R3_4_1` | unset | Skip 4.1 when the tifs are already correct. |

**Gate before publishing anything:** `Rscript R/checks/usd_total_vs_reference.R` compares
`any + none` per (country, crop) against the 0.4.4 exposure reference. It splits rows into
material, immaterial and unmatched, because one ratio bound cannot serve all three - see the
header comment. `generic-crop` has no reference by design and is reported, not gated.

## CROSS-REPO — this rebake STARTS upstream in AdaptationAtlas/hazards (nexgddp)
The full chain is **2 repos**: `AdaptationAtlas/hazards` (nexgddp branch) produces
the per-year climate **indices** (NDWS, NDD, NTx, PTOT, … the analysis-ready rasters
+ the `indices_dir2`/haz_timeseries inputs) → `hazards_prototype` R/1 → R/2 → R/3
consume them. So a true full rebake is:
**(Stage 0) /hazards nexgddp — refresh/fix the indices → (Stage 1+) hazards_prototype.**
- **Stage 0 is now IN-REPO at `hazards_upstream/`** (vendored 2026-06-24 via full-history
  `git subtree add` from AdaptationAtlas/hazards `nexgddp`; update later with
  `git subtree pull --prefix=hazards_upstream <hazards> nexgddp`). The producer is a
  7-stage R pipeline: `hazards_upstream/R/{01_download_data, 02_preprocess_data,
  03_bias_correction, 04_indices, 05_final_maps, 06_metadata, 07_bucket_uploads}`.
  Its outputs (the climate indices) are what hazards_prototype R/1→R/2 consume via
  `indices_dir2`.
- **hazards#19 (NDWS-historic saturation) is fixed in `hazards_upstream/R/04_indices`**
  (the index-calc stage), NOT in this repo's R/. Stage 0 must re-run NDWS there before
  R/2 re-derives drought hazard, else the saturation re-propagates regardless of
  anything downstream. (Apply the #19 fix in 04_indices, re-run Stage 0 for NDWS,
  confirm historic NDWS no longer ~0.95/pixel, THEN proceed to R/2.)
- **Eventual goal (Pete): merge the two repos.** Until then, the cross-repo handoff
  (indices → consumption) + the 24 runtime `raw.githubusercontent.com/.../hazards_prototype`
  source URLs are the coupling to mind. (Merge scoping is a separate project.)

## ⛔ PRE-CONDITIONS / DEPENDENCIES (check BEFORE launching)
- **hazards#19 NDWS saturation — ✅ RESOLVED LIVE (verified 2026-09-18).** The
  de-saturated NDWS shipped inside the issue-#9 publish on 2026-09-16 and was
  verified on the published parquet (historic dry-union 0.1616 vs ~1.0 saturated;
  futures rise monotonically with forcing). The FIXED indices are what a rebake
  now re-derives from — this gate is CLEARED, do NOT re-run R/2/R/3 just for
  NDWS (see DISPATCH_cglabs_track1_ndws_resume.md: sidecar/metadata gaps only).
- **CR-115 / #11 disputed-territory dedup — CONDITIONAL.** If Brayden's convention
  (`data-management#3`) is SET by rebake time: wire `haz_functions.R::aggregate_disputedRegions()`
  into the adm0 admin-extraction (R/3 + R/observational) and apply, so disputed
  claimants stop producing duplicate adm0 rows. If NOT set: the dup rows persist
  (current behaviour) — don't implement producer dedup blind (Brayden also testing
  an H3/A5 grid-index alternative). See [[project-cr115-disputed-territory-convention]].
- **#10 Delta-Method exposure extraction (Brayden, bjyberg).** Only run for USD/INTL$
  2015. If this rebake is meant to use **new/2021 exposure data**, that extraction
  must run FIRST (Brayden owns) or R/3 multiplies hazard by stale exposure. Confirm
  which exposure vintage the bake targets.

## Items riding this rebake
1. **Poultry_highland THI Extreme 79→89 (#13).** Metadata already fixed
   (`haz_classes.csv` @16dce34). §3 + §5.3 regen with the corrected threshold +
   R/3 §4 across vop, vop_usd, ha, BOTH axes (annual + jagermeyr). Validate:
   poultry_highland Extreme exposure should DROP (89 is a higher bar than 79).
   Publish hazard_risk_vop family → close #13. (Background: archive/dispatches/DISPATCH_poultry_thi_rebake.md.)
2. **Desert PTOT mask (CR-093) — ✅ ALREADY DONE + PUBLISHED 2026-06-24.** R/2.2 masks
   baseline `PTOT < 100 mm/yr` (default on); shipped to domain=climate, ISSUE closed.
   A full bake re-runs R/2.2 → the mask carries automatically (no action). OPTIONAL:
   evaluate the compound cut (`PTOT_DELTA_MIN_MM=10`) — run R/2.2 default vs =10,
   compare arid-band %-area, ship only if Pete approves.
3. **§5.2 vectorize parity (USE_R2_5_2_VEC).** Default ON; the identity probe only
   ran on macbook terra. BEFORE the bake: `Rscript R/probe_r2_5_2_vec.R` on cglabs
   to confirm terra::mean/stdev parity. If it fails, set `USE_R2_5_2_VEC=0`.
4. **Tier D parallelism decision.** §5.2 now logs
   `5.2: N combinations x M scen_x_model | worker_n5.2=20`. If N << 20, the two-phase
   flatten in R/ISSUE_r2_5_2_parallelism.md is worth applying; if N≈15+, skip.
   (Tier A timers give per-section wall-clock to target.)
5. **Pattern B / exposure > VoP (#9, #12) — RESOLVED + PUBLISHED 2026-09-16. No longer rides this rebake.**
   **The earlier entry here was wrong and is corrected for the record.** It said "code review
   found NO grid/resample mismatch in our numerator". There was one. The crop nominal-USD
   exposure raster (`spam_vop_usd2015_all.tif`, SPAM 0.05 deg Africa) was multiplied against
   `_int` stacks on the 0.25 deg global grid, so `data * exposure` raised
   `[*] extents do not match` for **every** crop, the 4.1 retry wrapper swallowed it with
   `try(silent = TRUE)`, and the stale pre-existing tifs survived for 4.2 to read as current.
   Ratios >100% and the missing `hazard='none'` both traced to that. Fixed in R/3:
   `.align_exposure()` (aggregate when exactly nested, else `resample(method="sum")`),
   4.1 now **aborts** on genuine failures instead of warning, and commodity/variable pairs
   with no exposure surface (livestock x harvested area) return a classified non-fatal skip.
   Verified on the live product: 0 exceedances in 5,281 admin1 x crop pairs across AGO/KEN/NGA,
   NaN at adm0/adm1 = 0, adm0 = sum(adm1) exactly.
   **Lesson worth keeping: a clean R/3 exit is not evidence of a clean run.** Check
   `failed_risk_x_exposure_*.txt`, `skipped_not_in_exposure_*.txt`, per-variable 4.1 elapsed,
   and output mtimes against the run start.

6. **R/2.1 GCM pin (#26) — ✅ DONE, CLOSED 2026-09-20.** Re-baked (18 GCMs, both
   historic windows), published (19 S3 keys: 10 canonical + 4 trends + 5 variability),
   independently verified (duckdb httpfs on the live canonical key: `n_models`
   distinct = `{0, 18}` only, zero partial counts, 68,112 legitimate all-NaN rows
   where no GCM has valid extraction for a given admin1×hazard combo). No further
   action. Full trail: `archive/dispatches/DISPATCH_cglabs_issue26_r21_rebake.md`.

7. **VoP const-I$ publish ("Gap C", #30) — validated 2026-07 chain NEVER published.**
   Node QAQC went green 2026-07-08 (livestock 1.00 242/242, crop 0.99 36/50, outliers
   accepted) but `domain=hazard_exposure/.../variable=vop_intld15` on S3 is still the
   2025-06/07 vintage — live livestock measures 1.198 vs FAOStat const-I$
   (species-structured: poultry 1.370 … cattle 1.058). Publishing it = the ~7× cattle
   currency fix finally going live, a wholesale overwrite of ~3,345 objects.
   **BLOCKED on p.steward's #30 unit/allow-list decision** (`intld15` vs
   `intld15-2021` — the `0.4.4:345` allow-list drop is baked into the published
   artifact) or reference and product ship on different vintages. Gate to re-run
   pre- and post-publish: `R/checks/vop_align_live_gate.R` (live-artifact twin of
   `qaqc_vop_vs_faostat.R`, ~15 s, any machine). Detail: memory
   project_vop_currency_mismatch + #30 comment 2026-09-18.

## Order
0. Pre-conditions above (exposure vintage; CR-115 convention state).
1. R/2 (`FORCE_OVERWRITE=1 RUN_R2_RUN3=1 RUN_R2_RUN5_3=1`, both axes, nohup+log;
   run the terra-probe first).
2. R/3 §4.1+§4.2 (vop, vop_usd, ha, both axes).
3. R/2.2 (desert mask carries; optional compound eval).
4. Validators (`validate_cr093_real.R` for R/2.2; spot-check poultry Extreme drop).
5. Publish (hazard_risk_vop family + domain=climate) + add masking rationale to the
   `metadata/data.json` hazard_change record.
6. Close #13. Log the republish to Brayden on `data-management#2` (the A/B/C catalog
   list — append any new/changed keys per [[reference-atlas-stac-structure]]).

## POST-BAKE VALIDATION (CR-068 probes — run after publish)
`atlas_notebooks/scripts/probe_no_hazard_arithmetic_quick.sh <ISO3>` +
`probe_cross_parquet_vop_drift.sh <ISO3>` against canonical S3. Expect: exposure
ratios <=100%, `hazard='none'` rows present, NaN count -> 0 at adm0/adm1.
Baselines + interpretation in [[project-cr068-post-bake-probes]].

**Both probe scripts have bugs that read as data defects (verified 2026-09-17, live):**
- `probe_no_hazard_arithmetic_quick.sh` hardcodes `hazard_vars='NDWS+NTx35+NDWL0'`, a **crop**
  combination, so all ten livestock commodities report `no_hazard_row` when their data is
  present and healthy under the heat combinations.
- `probe_cross_parquet_vop_drift.sh` defines its admin1 sum as `admin1_name IS NOT NULL`,
  which also picks up admin2 rows. Admin2 legitimately carries ~1.9% NaN where a crop mask is
  empty, so the sum returns NaN and the probe reports a false admin0 mismatch. Restricted to
  true admin1 rows it matches admin0 to the dollar.

Both live in `atlas_notebooks`, reported on #9, not fixed from this repo. Prefer the
self-contained check: `any + none` from the product itself, rather than crossing to
`crop-livestock_all.parquet`, whose vintage differs and whose intld rows are affected by #30.
