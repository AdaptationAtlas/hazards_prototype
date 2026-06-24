# Next full R/2 + R/3 rebake — consolidated checklist

Several fixes are deferred to bundle into the next full hazard rebake (amortise
the ~day of warmup). This is the pickup list so nothing is silently missed.
Run on cglabs (live Data/ + S3). Last updated 2026-06-24 (issue sweep).

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

## CROSS-REPO — this rebake STARTS upstream in AdaptationAtlas/hazards (nexgddp)
The full chain is **2 repos**: `AdaptationAtlas/hazards` (nexgddp branch) produces
the per-year climate **indices** (NDWS, NDD, NTx, PTOT, … the analysis-ready rasters
+ the `indices_dir2`/haz_timeseries inputs) → `hazards_prototype` R/1 → R/2 → R/3
consume them. So a true full rebake is:
**(Stage 0) /hazards nexgddp — refresh/fix the indices → (Stage 1+) hazards_prototype.**
- **hazards#19 (NDWS-historic saturation) is fixed in /hazards, not here** — Stage 0
  must re-run the NDWS index on nexgddp before R/2 re-derives drought hazard, else
  the saturation re-propagates regardless of anything in this repo.
- Bringing the /hazards nexgddp workflow into the run plan is a TODO — its exact
  steps/branch state aren't captured here yet (different repo). Pull them in before
  scheduling the bake.
- **Eventual goal (Pete): merge the two repos.** Until then, the cross-repo handoff
  (indices → consumption) + the 24 runtime `raw.githubusercontent.com/.../hazards_prototype`
  source URLs are the coupling to mind. (Merge scoping is a separate project.)

## ⛔ PRE-CONDITIONS / DEPENDENCIES (check BEFORE launching)
- **hazards#19 — historic (1995-2014) NDWS rasters saturated (~0.95 every pixel).**
  UPSTREAM (`AdaptationAtlas/hazards`). A full rebake re-derives NDWS/NDWL0 hazard
  from these inputs → **re-propagates the saturation + the Luanda NaN signature**
  and the broken historic-vs-future comparability into the new products. Either
  (a) wait for the upstream fix, or (b) rebake but treat NDWS/NDWL0 **historic** as
  known-bad and flag it (don't silently republish saturated drought-historic).
  This is the single biggest gate on a clean full rebake.
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
   Publish hazard_risk_vop family → close #13. (Background: DISPATCH_poultry_thi_rebake.md.)
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
5. **Pattern B / exposure>VoP (#9, #12) — NOT a producer fix here.** Code review found
   NO grid/resample mismatch in our numerator; the gap is vs the STALE denominator
   `crop-livestock_all.parquet` (vintage 2026-01-22, Brayden's republish) ± boundary
   vintage. Probe-first (per #12), no blind re-bake. The rebake refreshes the
   numerator (hazard_exposure) but will NOT close #12 until the denominator is
   republished + the reconciliation probe passes. See [[project-cr068-post-bake-probes]].

## Order
0. Pre-conditions above (esp. hazards#19; exposure vintage; CR-115 convention state).
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
ratios ≤100%, `hazard='none'` rows present, NaN count → 0 (except the Luanda
residual until hazards#19 lands). Baselines + interpretation in
[[project-cr068-post-bake-probes]].
