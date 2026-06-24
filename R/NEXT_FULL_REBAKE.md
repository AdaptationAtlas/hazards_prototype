# Next full R/2 + R/3 rebake — consolidated checklist

Several fixes are deferred to bundle into the next full hazard rebake (amortise
the ~day of warmup). This is the pickup list so nothing is silently missed.
Run on cglabs (live Data/ + S3). Last updated 2026-06-24.

## ⚠️ CRITICAL — §3 and §5.3 are toggle-only (FORCE_OVERWRITE does NOT enable them)
`run3` and `run5.3` are gated on explicit env toggles, NOT `FORCE_OVERWRITE`
(R/2:582, 627). A plain `FORCE_OVERWRITE=1` bake will **skip the crop-stack (§3)
and per-crop interaction (`_int`, §5.3) producers** → `haz_risk/` would not refresh
and R/3 §4.1 (which reads those) would consume stale stacks. So a full rebake that
must refresh `haz_risk` MUST export:
```
FORCE_OVERWRITE=1 RUN_R2_RUN3=1 RUN_R2_RUN5_3=1
```
(`RUN_R2_RUN5_2=1` too if the §5.2 combo tifs in `haz_time_int_dir` also need a
rebuild; §5.2 already runs under FORCE_OVERWRITE, so usually not separately needed.)

## Items riding this rebake
1. **Poultry_highland THI Extreme 79→89 (#13).** Metadata already fixed
   (`haz_classes.csv` @16dce34). §3 + §5.3 regen with the corrected threshold +
   R/3 §4 across vop, vop_usd, ha, BOTH axes (annual + jagermeyr). Validate:
   poultry_highland Extreme exposure should DROP (89 is a higher bar than 79).
   Publish hazard_risk_vop family → close #13. (Background: DISPATCH_poultry_thi_rebake.md.)
2. **Desert PTOT mask (CR-093 follow-up).** R/2.2 SEC1 masks baseline
   `PTOT < PTOT_BASELINE_MIN_MM` (default 100 mm/yr) — active by default on the
   next R/2.2 run. Optionally evaluate the compound cut: run R/2.2 once default and
   once `PTOT_DELTA_MIN_MM=10`, compare arid-band %-area before shipping. Then
   re-validate (`validate_cr093_real.R`) + republish domain=climate
   (`CONFIRM=1 Rscript R/publish_cr093_r22.R`) → close ISSUE_cr093_nan_zeroprecip.
   NOTE: this is R/2.2-only and can be done independently/sooner if desired.
3. **§5.2 vectorize parity (USE_R2_5_2_VEC).** Default ON; the identity probe only
   ran on macbook terra. BEFORE the bake: `Rscript R/probe_r2_5_2_vec.R` on cglabs
   to confirm terra::mean/stdev parity. If it fails, set `USE_R2_5_2_VEC=0`.
4. **Tier D parallelism decision.** The §5.2 run now logs
   `5.2: N combinations x M scen_x_model | worker_n5.2=20`. Read it: if N << 20,
   the two-phase flatten in R/ISSUE_r2_5_2_parallelism.md is worth applying; if
   N≈15+, skip it. (Tier A timers also give per-section wall-clock to target.)
5. **CR-068 Pattern B** — only if a producer fix has landed by then (else leave;
   it's a tracked investigation, hazards_prototype#12).

## Order
R/2 (`FORCE_OVERWRITE=1 RUN_R2_RUN3=1 RUN_R2_RUN5_3=1`, both axes, nohup+log;
run the terra-probe first) → R/3 §4.1+§4.2 (vop, vop_usd, ha, both axes) →
R/2.2 (desert mask) → validators → publish (hazard_risk_vop family + domain=climate)
→ close #13 + ISSUE_cr093_nan_zeroprecip + add the masking rationale to the
metadata/data.json hazard_change record at republish time.
