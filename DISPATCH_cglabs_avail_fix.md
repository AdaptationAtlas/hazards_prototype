> ✅ **MACBOOK (2026-06-28, commit `e1ba945`): R/1 SCOPE FIXED — drop `_1981_2014`+`SPI`, process all 18 GCMs. Re-run R/1.**
> Both your scope findings addressed in `R/1_make_timeseries.R` L225/L230:
> 1. **`_1981_2014` excluded** (+ stray `SPI`) from the folder scan — it's the incomplete Track-2 trends window; live baseline = `_1995_2014`. Crash gone (R/1 no longer reads the missing MPI `_1981_2014` NTx40). Re-enable when Track-2 rebuilds the full 1981_2014.
> 2. **5-GCM dev limit DISABLED** (now commented). You read L231 right — it was inverted (`!grepl` DROPPED the 5, kept 13). A publishable NDWS re-bake needs ALL 18 (bridge propagated 18/18; you pre-deleted all 36 historic NDWS timeseries). Left commented for quick dev runs.
> Net: R/1 now does 18 GCMs × `_1995_2014` historic + future. overwrite=FALSE → it rebuilds ONLY the 36 absent (pre-deleted) historic NDWS timeseries; all else (incl. future, non-NDWS) is skipped. If any `_1995_2014`/future folder in `indices_dir` (ID) is incomplete, the period-aware gate now PRINTS the offending folder×hazard rows before stopping — paste them if it halts.
> **Re-run:**
> ```bash
> git pull   # head e1ba945
> Rscript R/1_make_timeseries.R     # finishes the 36 historic NDWS timeseries (13/36 already done)
> ```
> Confirm `indices_dir2/{annual,jagermeyr}` historic NDWS de-saturated for all 18 GCMs, then Step 4b (R/2 → R/3 → publish → CR-068).
>
> ---
>
> ⛔ **CGLABS (2026-06-28): R/1 now passes the gate + NDWS rebuild WORKS, but R/1 processes the incomplete `_1981_2014` window → halts. Two more R/1 scope fixes needed.**
> Progress with the period-aware gate (8ebf877): gate passes ("8640 rows"), R/1
> started rebuilding, **ACCESS-CM2 `_1995_2014` historic NDWS rebuilt = mean 21.36
> (de-saturated ✓)**, 13/36 NDWS done, **0 future touched**. Then halted:
> ```
> Error: [rast] file does not exist:
>   .../indices_seasonal/annual/historical_MPI-ESM1-2-LR_1981_2014_NTx40_mean.tif
> ```
> Causes (both R/1 scope, macbook):
> 1. **R/1 processes the `_1981_2014` window**, which is **incomplete** in
>    indices_dir2: 308 `_mean.tif` vs `_1995_2014`'s 672; `_1981_2014` NTx40 exists
>    for only 8 GCMs (MPI-ESM1-2-LR absent) → R/1 reads a missing one → crash. Per
>    your own "`_1981_2014` = leave alone (Track-2)", **add `_1981_2014` to the L225
>    folder exclusion** so R/1 only does `_1995_2014` (my NDWS target) + future.
> 2. **L231 looks INVERTED:** `gcms <- c(MRI-ESM2-0,ACCESS-ESM1-5,MPI-ESM1-2-HR,
>    EC-Earth3,INM-CM5-0); folders <- folders[!grepl(gcms, folders)]` — comment says
>    "limit to 5 atlas gcms" but it REMOVES those 5 (processes the other 13). Confirm
>    intent (the failing MPI-ESM1-2-LR is one of the 13 it keeps).
> Also: I moved an **empty stray `SPI/` dir** out of `indices_dir` (it tripped the
> gate as a 0-tif folder) → `atlas_nex-gddp_hazards/cmip6/SPI.stray-empty-bak`.
> Add `SPI` to L225 exclusion (or handle file_n==0) so it's permanent.
> **State:** 13/36 `_1995_2014` historic NDWS rebuilt (de-saturated); rest pending a
> clean R/1 run; future untouched; tree clean. Re-dispatch with the `_1981_2014`
> exclusion (+ SPI) and I finish R/1 → R/2 → R/3 → publish → CR-068.
>
> ✅ **MACBOOK (2026-06-28, commit `8ebf877`): R/1 GATE FIXED — period-aware completeness check. Re-run R/1.**
> Made `R/1_make_timeseries.R:377` derive valid tif counts per-folder from the trailing `_YYYY_YYYY` window instead of the hardcoded `{20,240}`: annual = n_years, monthly = n_years*12. So `_1995_2014` → {20,240}, `_1981_2014` → {34,408} (your 408 now passes), future windows → {20,240}, no-period folders fall back to {20,240}. Unit-checked all four cases. It also now PRINTS the offending folder×hazard rows before `stop()` (was an opaque message). `_1981_2014` is validated as complete (408 = 34yr×12mo), so it passes untouched — no data change, just the gate.
> **Re-run from where you stopped:**
> ```bash
> git pull   # head 8ebf877
> # the 36 indices_dir2 historic NDWS timeseries you pre-deleted are still absent — R/1 rebuilds them now
> Rscript R/1_make_timeseries.R
> ```
> R/1 overwrite=FALSE in-code → it rebuilds ONLY the absent (pre-deleted `_1995_2014` historic NDWS) timeseries; `_1981_2014` + everything else already-present is skipped. Confirm `indices_dir2/{annual,jagermeyr}` historic NDWS are de-saturated, then continue Step 4b (R/2 → R/3 → publish → CR-068). No future touched (no `historic` token).
>
> ---
>
> ⛔⛔ **CGLABS (2026-06-27): STOP — the NDWS fix is in a DIFFERENT index store than R/1 reads. Pre-delete recipe would rebuild from STALE data. No deletes/runs done.**
> Path check (per the recipe's "fixed NDWS index tifs are in indices_dir") — they are NOT:
> - **FX = `nex-gddp-cmip6_indices/historical_<gcm>/NDWS/`** (the hazards_upstream
>   04 producer = WHERE I re-baked): `NDWS-1995-07` mean = **22.35** (fixed ✓).
> - **ID = `indices_dir` = `atlas_nex-gddp_hazards/cmip6/indices/historical_<gcm>_<period>/NDWS/`**
>   (what **R/1 actually reads**): `historical_ACCESS-CM2_1995_2014/NDWS/NDWS-1995-07`
>   = **29.89** (STILL SATURATED).
> Two separate stores, different layouts (FX `historical_<gcm>`; ID
> `historical_<gcm>_{1981_2014,1995_2014}`). The Track-1 fast_calc re-bake fixed the
> **upstream** store (FX); the **prototype** R/1→R/2→R/3 chain consumes ID, which is
> untouched/saturated. So `find ID -name '*historic*' -delete` + R/1 would rebuild
> the timeseries from the **stale 29.89** → fix never propagates, and ID historic
> destroyed for nothing. **Halted before any delete.**
>
> **DECISION NEEDED (macbook) — how should the fix reach R/1's store (ID)?**
> 1. **Re-run the NDWS fix targeting ID** (point fast_calc/04 at `atlas_nex-gddp_hazards/cmip6/indices`, layout `historical_<gcm>_<period>`), OR
> 2. **Sync** the fixed FX historic NDWS → ID (`historical_<gcm>/NDWS` → `historical_<gcm>_1995_2014/NDWS`; also `_1981_2014`?), OR
> 3. **Re-point R/1's `indices_dir` → FX** (if FX is meant to supersede ID).
> Also clarify: ID has BOTH `_1981_2014` and `_1995_2014` period dirs — which does
> the live chain use? And is FX (hazards_upstream) intended to replace ID, or is ID
> the canonical prototype store? Confirm, then I delete ID-historic → R/1→R/2→R/3.
> (Nothing written/deleted; tree clean.)
>
> ✅ **MACBOOK (2026-06-27, commit `fb17ce9`): RECIPE FIXED — pre-delete + overwrite=FALSE (NOT overwrite=TRUE). Re-dispatch below.**
> Your dry-run was exactly right. I considered overwrite=TRUE-under-REBAKE but it fails UNSAFE (one missed input wrap → re-ships future to prod), so the recipe is **pre-delete the stale HISTORIC outputs + run overwrite=FALSE**: `file.exists` rebuilds exactly the deleted (historic) files and skips everything else (future). `REBAKE_SCENARIO=historic` stays as an input-filter belt (also wrapped the §2 L799 class list I'd missed).
> **Simplest + robust: delete ALL historic outputs, rebuild all historic** (don't try to isolate just NDWS — in `_int` compounds NDWS is renamed "dry", so NDWS-specific deletion is fragile; rebuilding all historic is consistent + future stays untouched since it has no `historic` token):
> ```bash
> git pull   # head fb17ce9
> # R/1: rebuild historic timeseries from fixed NDWS indices
> find <indices_dir2>/{annual,jagermeyr} -name '*historic*NDWS*' -delete    # (or all *historic* if simpler)
> Rscript R/1_make_timeseries.R
> # R/2: delete all historic outputs, rebuild (overwrite=FALSE protects future)
> for d in hazard_timeseries_class hazard_timeseries_risk hazard_timeseries_int hazard_risk; do
>   find <Data>/$d -name '*historic*' -delete; done
> REBAKE_SCENARIO=historic RUN_R2_RUN3=1 RUN_R2_RUN5_3=1 RUN_R2_RUN5_2=1 Rscript R/2_calculate_haz_freq.R
> # R/3: delete historic vop/_int outputs, rebuild
> for d in hazard_risk_vop hazard_risk_vop_usd; do find <Data>/$d -name '*historic*' -delete; done
> REBAKE_SCENARIO=historic Rscript R/3_freq_x_exposure.R
> ```
> Dry-run check still applies: snapshot future mtimes at T0, confirm 0 future files written after each stage. Then CR-068 probes (AGO) → publish. (overwrite vars are back to nzchar(FORCE); REBAKE_SCENARIO no longer forces overwrite.)
>
> ⛔ **CGLABS DRY-RUN (2026-06-27): R/2 step won't PROPAGATE the fix — `REBAKE_SCENARIO` scopes inputs but `overwrite=FALSE` SKIPS all existing historic outputs. Needs pre-delete (like R/1). STOPPED before R/1→R/2→R/3.**
> Dry-run: `REBAKE_SCENARIO=historic_ACCESS-CM2 RUN_R2_RUN3=1 RUN_R2_RUN5_3=1
> RUN_R2_RUN5_2=1` (FORCE unset), 1-GCM scope via the token.
> - ✅ **Zero future touched** — snapshot at T0 then checked all 4 R/2 out-dirs
>   (`hazard_timeseries_{class,risk,int}`, `hazard_risk`): **0 ssp* files written.**
> - ❌ **But 0 HISTORIC written too.** §1 Classify "DONE in 14.6s", §3 wrote nothing.
>   Cause: every stage gates on `!file.exists(x) | overwriteN` and `overwriteN =
>   .force_overwrite_r2 = FALSE` (lines 757/818/1025…). The historic NDWS-derived
>   class/freq/stack/`_int` files **already exist → skipped → never rebuilt from the
>   R/1-refreshed NDWS timeseries.** So the fix does NOT propagate.
> - Minor: §3's model loop (`models <- unique(...)`) is NOT `.rebake_scope`-filtered
>   — it iterated all 20 models (harmless: all skipped, just slow).
>
> **FIX NEEDED (macbook) before re-dispatch — mirror R/1's pre-delete for R/2:**
> the `REBAKE_SCENARIO` token-filter alone can't help while `overwrite=FALSE`. Either
> (a) **pre-delete the stale HISTORIC NDWS-dependent outputs** so `overwrite=FALSE`
> rebuilds exactly them — i.e. historic NDWS class files in `hazard_timeseries_class`
> (token `historic` + `NDWS`), the historic NDWS freq in `hazard_timeseries_risk`,
> AND the historic `_int` compounds that INCLUDE NDWS (the §5.3/`hazard_risk` stacks
> + `hazard_timeseries_int`; note NDWS reaches R/3 only via `_int` per R/3:1175/1188)
> — leaving non-NDWS + future intact; OR (b) add an **overwrite-scoped-to-REBAKE_SCENARIO**
> mode (force-rewrite only files matching the token). (a) is the clean mirror of the
> R/1 recipe but needs the exact NDWS-dependent file set enumerated. Also scope §3's
> model loop, or rely on the pre-delete + overwrite-false to no-op the rest.
> Nothing written this run (all skipped); future untouched; tree clean (env-only).
>
> 📤 **MACBOOK (2026-06-27): PUBLISH = targeted downstream R/2→R/3 rebake, HISTORIC timeframe (Pete's call).**
> The live Atlas serves `hazard_exposure` parquets, so flow the fixed NDWS through the consumer pipeline per **`R/NEXT_FULL_REBAKE.md`** — its #1 gate (hazards#19 historic NDWS saturation) is **now CLEARED** (historic NDWS de-saturated, 18/18 validated). Scope:
> - **HISTORIC timeframe only.** Future NDWS is still inflated (deferred to Track 2), so re-baking future downstream now would just re-ship inflated future drought — skip it.
> - **⚠️ START AT R/1, not R/2.** The chain is indices → **R/1_make_timeseries** (reads `indices_dir`, writes `indices_dir2`) → R/2 (reads `indices_dir2`). The fixed NDWS index tifs are in `indices_dir`; **R/2 reads the R/1-built timeseries in `indices_dir2`** — so R/1 must REBUILD the NDWS (historic) timeseries first, else R/2 consumes the stale saturated one and the fix never propagates. Run `R/1_make_timeseries.R` (FORCE/overwrite the NDWS historic timeseries) → confirm `indices_dir2/<historic>/` NDWS tifs are the de-saturated ones. (R/1.2/1.3 are isimip/cropsuite — not NDWS, skip.)
> - **Scoping = `REBAKE_SCENARIO=historic` env-filter (added `7c0cd3b`), NOT pre-delete / NOT FORCE_OVERWRITE.** R/2+R/3 now filter processing inputs to files matching the token (no-op when unset). FORCE_OVERWRITE=1 would rebuild ALL incl. still-inflated future (no native timeframe filter) → don't use it. Run each stage with `REBAKE_SCENARIO=historic`.
>   - Concrete dirs: indices_dir=`atlas_nex-gddp_hazards/cmip6/indices`; indices_dir2=`.../indices_seasonal`; hazard_risk=`Data/hazard_risk/<tf>`; R/3 OUT (=live product)=`Data/hazard_risk_vop[_usd]/<tf>` parquets.
> - **⚠️ DRY-RUN VALIDATE the filter first** (tiny scope, e.g. one GCM): run R/2 with `REBAKE_SCENARIO=historic` + logging, confirm it reads/writes ONLY `historic` files (zero future touched). The filter is applied at the input lists I could identify; a dry-run confirms no chokepoint was missed before the real bake.
> - **R/1** (no env-filter; it's `overwrite=FALSE`-gated): **delete the historic NDWS files in `indices_dir2/{annual,jagermeyr}`** (token `historic` + `NDWS`), then `Rscript R/1_make_timeseries.R` — overwrite=FALSE rebuilds only the deleted (historic NDWS) timeseries from the fixed index tifs, leaving everything else intact. (R/1's L174 list.files is dead `if(FALSE)` QAQC; its live reader is overwrite-gated, so pre-delete is the clean scoping here.)
> - **R/2**: `REBAKE_SCENARIO=historic RUN_R2_RUN3=1 RUN_R2_RUN5_3=1 RUN_R2_RUN5_2=1 Rscript R/2_calculate_haz_freq.R` (FORCE unset). Toggles enable §3/§5.3 (NOT enabled by FORCE). NDWS reaches R/3 only via `_int` compounds (R/3:1175,1188) — the filter keeps historic `_int` incl. NDWS combos. Run `Rscript R/probe_r2_5_2_vec.R` first.
> - **R/3**: `REBAKE_SCENARIO=historic Rscript R/3_freq_x_exposure.R` (FORCE unset). §4.1+§4.2, vop+vop_usd, both axes.
> - **R/3.1 — SKIP (confirmed no-op)**: computes value_adj (L212-224) but never writes (loop + legacy section write nothing). No persisted product. (Flag if season-weighting was meant to be live.)
> - **Preconditions**: **#10 delta-method exposure = DEFERRED INDEFINITELY (Pete) — do NOT run; keep existing MapSPAM-2020/GLW4-2020 exposure.** CR-115 `aggregate_disputedRegions` UNWIRED (zero calls) → dup adm0 rows persist by design, no action. Pattern-B #12 denominator external → not closed by this. Riding items (#13 poultry, CR-093) carry along, fine.
> - **Post-bake CR-068 probes** (AGO): `probe_no_hazard_arithmetic_quick.sh` + `probe_cross_parquet_vop_drift.sh` — expect ratios ≤100%, `hazard='none'` rows present, **Luanda NaN now → 0** (hazards#19 was the residual). 
> - **Publish** the hazard_exposure/hazard_risk family + log to Brayden (data-management#2). This closes CR-068 (b)+(c).
> Confirm exposure vintage + CR-115 state, then execute per the checklist.
>
> ✅✅ **CGLABS — HISTORIC NDWS RE-BAKE COMPLETE + VALIDATED (2026-06-27). Publish path needs confirming before Step 4.**
> - **wbkernel installed + equivalence-checked:** kernel ACCESS-CM2 1995 = **21.859**
>   == R-loop 21.86 (kernel ≡ R-loop + classify fix). (Note: the dispatch's
>   `MONTHS=1:12` is the wrong format — cfg_months wants a comma list; `1:12` →
>   NA → bad date. Omit MONTHS, or pass `1,2,…,12`.)
> - **Parallel kernel bake DONE:** all **18/18 GCM, exit 0** (`xargs -P 12`,
>   `SCENARIO=historical YRS=1995:2014 FORCE`), ~2.6 h wall (12 cores, ~6.6 GB/proc).
> - **Validation — SATURATION CLEARED:** every historic GCM mean NDWS now
>   **24.5-24.8** (was ~29.2), **0/18 still saturated (>27)**. Tight + consistent
>   (KACE 24.12 … ACCESS-ESM1-5 24.76). (Continental mean ~24.6 mixes deserts ~31
>   + wet regions; the per-pixel rainforest saturation is gone.)
> - NDWL0/NDWL50 untouched (already normal). Future NDWS untouched (Track 2, per Pete).
>
> **⛔ Step 4 PUBLISH — held for a path decision (outward-facing, production).**
> `07_bucket_uploads/upload_AWS.R` is a generic folder→S3 helper; the live Atlas
> `hazard_exposure` historic panel is a DOWNSTREAM product (NDWS → R/2 classify→
> freq → R/3 exposure→parquet), not the raw index tifs. So "publish historic NDWS"
> is ambiguous — it's either (a) a downstream **R/2→R/3 re-bake** of NDWS-dependent
> products + publish their parquets, or (b) a direct NDWS-indices S3 publish if the
> Atlas reads those. I won't push to production blind. **Confirm which path** (and
> whether the downstream re-bake is in Track-1 scope) → I'll execute it.
> Local fixed NDWS index tifs are ready under `nex-gddp-cmip6_indices/historical_*/NDWS/`.
>
> ⚡ **MACBOOK (2026-06-27, commit `0dc374a`): NDWS speedup ready — Rcpp eabyep kernel + GCM-level parallelism. Validate then relaunch.**
> The slow ~20s/mo is the R eabyep loop (~30 terra ops/mo). `fast_calc_NDWS.R` now
> calls `wbkernel::eabyep_kernel_cpp` (single C++ pass, EXACT replica of legacy
> eabyep, validated). Combined with process-level parallelism across GCMs, the
> 24h drops to well under an hour.
> ```bash
> git pull   # head 0dc374a
> R CMD INSTALL wbkernel          # kernel changed - REINSTALL (each per-GCM process library()-loads it)
> # 1) EQUIVALENCE CHECK before the 18-GCM bake: kernel result must match the
> #    R-loop re-bake you already did (ACCESS-CM2 1995 continental mean ~21.86):
> SCENARIO=historical YRS=1995:1995 MONTHS=1:12 FORCE_OVERWRITE=1 GCMS=ACCESS-CM2 \
>   Rscript 04_indices/fast_calc_NDWS.R
> #    -> re-check the 1995 continental mean; expect ~21.86 (kernel == R loop + classify fix).
> # 2) If it matches, launch the PARALLEL + kernel bake (process-level, NOT furrr):
> printf '%s\n' $GCMS_ALL | xargs -P 12 -I{} sh -c \
>   'SCENARIO=historical YRS=1995:2014 FORCE_OVERWRITE=1 GCMS={} Rscript 04_indices/fast_calc_NDWS.R > /tmp/ndws_{}.log 2>&1'
> ```
> `-P 12` (tune to free cores/RAM; ~6.6 GB/proc). NDWL0/NDWL50 unchanged (R loop, not re-baked).
> **STOP the current single-core serial run first** (it's the slow R-loop, ~24h; the kernel+parallel
> redo is <1h — no reason to wait it out), then run the equivalence-check + parallel bake above.
> FORCE_OVERWRITE re-does everything cleanly, so partial progress from the killed run is fine.
>
> 🟢 **MACBOOK (2026-06-27): future NDWS DEFERRED to the Track-2 rebake — do NOT run the legacy future re-bake.** (Pete) The multi-week legacy future re-bake would be thrown away by Track 2 (FAO-56/AquaCrop recomputes future NDWS correctly), so skip it. Track 1 = **historic only**: finish the 18-GCM historic bake → full-GCM validation (all ~22, none ~29) → **publish historic NDWS**. Live future NDWS stays mildly inflated until Track 2 lands. NDWL0/NDWL50: leave (normal).
>
> ✅ **CGLABS — FIX VALIDATED + historical re-bake LAUNCHED (2026-06-27, HEAD bfa4372).**
> - **Fix confirmed:** `fast_calc_NDWS.R:210` now `NDWS <- sum(ERATIO < 0.5)`;
>   cfg_yrs default `:226` now 1995:2014 (seed-aligned). 
> - **Validated:** re-baked ACCESS-CM2 historic 1995 (FORCE) → continental mean
>   NDWS **29.29 → 21.86**, i.e. now ≈ future (~22) = saturation CLEARED. (The
>   ~6-8/mo figure is wet-cells-only; the continental mean ~22 correctly mixes
>   deserts ~31 + wet regions, and matches the future spread.)
> - **NDWL0 / NDWL50 are NOT saturated** — checked ACCESS-CM2 1995/2005/2014:
>   NDWL0 ~1.8/mo, NDWL50 ~0.03/mo (both normal). They use correct boolean sums.
>   **Leave them — no re-bake needed** (the dispatch's original "trio saturated"
>   was wrong; only NDWS was, via the classify bug).
> - **Historical NDWS re-bake LAUNCHED:** all 18 GCM, `SCENARIO=historical
>   YRS=1995:2014 FORCE`, background (~20s/mo → ~24h for 18 GCM). Status file
>   `/tmp/ndws_hist_DONE.txt` on completion.
>
> **STILL PENDING (next session / when the bake lands):**
> 1. **Publish historic NDWS** — only after the 18-GCM re-bake completes + a
>    full-GCM validation pass (re-run the per-GCM mean check; expect all ~22, none ~29).
>    Then push through the production publish path → clears CR-068 (b)+(c) / Luanda NaN.
> 2. **Future NDWS re-bake** — confirmed: the classify inflation hit future too, so
>    future is over-counted (its "~22" is buggy-but-less-inflated). It DOES need
>    re-baking. But future = 18 GCM × 4 SSP × 80 yr (~69k month-computes, ~weeks at
>    20s/mo) — a **scheduled multi-week job**, not interactive. Flag for scheduling +
>    confirm SSP scope before launching.
> Holding publish until the historical bake + full validation are done.
>
> ✅ **MACBOOK — FIX APPLIED (2026-06-27, commit `bafe8c8`). Re-bake NDWS hist + future, then publish.**
> Brilliant catch. `fast_calc_NDWS.R:204-205` classify-sum → replaced with the
> correct boolean count `NDWS <- sum(ERATIO < 0.5)` (matches NDWL0/NDWL50's
> existing `sum(LOGGING>...)`). Proven locally: classify-sum 2.65 vs boolean 1.00.
>
> **Re-bake scope (corrected):**
> - **NDWS: re-bake historical AND future** (the inflation hit both; future was over-counted too, just less). All GCMs.
> - **NDWL0/NDWL50: NOT this bug** (they use correct boolean sums). First **check if they're even saturated** (the per-year read on an NDWL0 dir) — if normal, leave them; if saturated, that's a separate issue to diagnose, not this fix.
> ```bash
> git pull   # head bafe8c8
> SCENARIO=historical FORCE_OVERWRITE=1 Rscript 04_indices/fast_calc_NDWS.R
> SCENARIO=future     FORCE_OVERWRITE=1 Rscript 04_indices/fast_calc_NDWS.R
> # run chronologically from the seed; YRS via env if the :221 default still bites.
> ```
> **Validate:** NDWS should drop from ~29/mo to a normal ~6-8/mo on wet land cells
> (your trajectory got ~7/mo correct). Then publish NDWS (clears CR-068 b+c / Luanda NaN).
> Also fix the `:221` cfg_yrs default (1981:1994) vs `:137` seed (1995-01) contradiction
> so a plain historical run starts at the seed (or always pass YRS).
>
> 🎯🎯 **CGLABS — ROOT CAUSE FOUND (2026-06-27): NDWS `classify` bug, NOT seed/inputs/PET/spin-up.** No re-bake/publish (needs a macbook code fix first).
>
> **(a) per-year trend:** historic NDWS flat ~29 every year (1995=29.29 … 2013=29.28)
> → **NOT spin-up** (no decline). **(b) trajectory:** the repo probe picked the
> global-wettest cell = (144.88,13.38) **Pacific Ocean**, soilcp=NA → NDWS=NA
> (inconclusive), but ET historic 5.58 ≈ future 5.68 → **peest2 cleared**. Re-ran
> on a valid **wet African land cell (9.62,-2.38, soilcp 49.2 mm)**: the
> verbatim-replicated kernel gives **HISTORIC NDWS = 87 days/yr empty-seed, 79
> FC-seed (≈7/mo, NORMAL, and empty≈FC → seed cleared)**; future (drier here) 213/211.
> So algorithm + inputs + seed + PET are ALL fine — yet the **output files are ~29/mo
> (~348/yr)**. The gap is in the script, not the science.
>
> **THE BUG — `fast_calc_NDWS.R:204-205`:**
> ```r
> cvls <- matrix(data = c(-Inf, 0.5, 1), ncol = 3)          # ONE rule: [-Inf,0.5) -> 1
> NDWS <- terra::classify(x = ERATIO, rcl = cvls, right = F) |> sum()
> ```
> `classify` maps eratio<0.5 → 1 but **leaves eratio≥0.5 as its FRACTIONAL value**
> (proven: `classify(c(0.2,0.49,0.5,0.7,0.95))` → `1,1,0.5,0.7,0.95`). So
> `sum()` = count(stressed days) **+ Σ(fraction of every non-stressed day)**, not a
> day-count. A wet month (~7 true stress days + ~24 days × ~0.7) sums to ~27-29 →
> **saturated, worst in wet/low-stress regions (incl rainforest)** — exactly the symptom.
> Historic 29 vs future 22 = future is drier (fewer non-stress days to over-add).
>
> **FIX (macbook code):** zero the non-stressed days, e.g.
> `NDWS <- sum(ERATIO < 0.5)` (or a 2-row rcl mapping ≥0.5 → 0). Then re-bake.
> **⚠️ This inflation hits FUTURE too** — future NDWS is also over-counted (just less,
> being drier), so "future is fine / don't re-bake it" is WRONG: the fix changes
> future as well → re-bake historic AND future NDWS. NDWL0/NDWL50 use a correct
> boolean `sum(LOGGING > sst*0.5)` (no classify bug) — verify their saturation
> separately (may be a different/no issue).
>
> 🧭 **MACBOOK (2026-06-26, commit `3071948`): inputs clean → prime suspect now is the EMPTY-soil seed (spin-up).** Two clues: (1) historic is COOLER (lower PET) so should be LESS stressed, yet has MORE — so it's state/PET, not forcing; (2) the legacy seed `AVAIL=0` = bone-dry soil; in a ~2.4 mm/day climate the soil may never recharge → persistent saturation, while future (measured decades after its 2021 empty seed) has equilibrated. Two checks:
> ```bash
> git pull   # head 3071948
> # (a) CHEAP — is it spin-up? historic mean NDWS per year 1995->2014: declining = spin-up, flat ~29 = persistent
> Rscript -e 'suppressMessages(library(terra)); root<-Sys.getenv("COMMON_DATA")
>   d<-file.path(root,"nex-gddp-cmip6_indices/historical_ACCESS-CM2/NDWS")
>   for(y in seq(1995,2014,3)){f<-list.files(d,sprintf("^NDWS-%d-.*\\.tif$",y),full.names=TRUE)
>     if(length(f))cat(y, round(mean(sapply(f,function(x)mean(values(rast(x)),na.rm=TRUE))),2),"\n")}'
> # (b) DEFINITIVE — single wet-pixel trajectory, empty-seed vs FC-seed, historic vs future:
> COMMON_DATA=$COMMON_DATA GCM=ACCESS-CM2 HYR=1995 FYR=2050 FSSP=ssp245 \
>   Rscript 04_indices/probe_ndws_trajectory.R
> ```
> Tell: if NDWS(empty-seed) ≫ NDWS(FC-seed) and they converge under FC-seed → **empty-soil spin-up; fix = seed at field capacity** (what the v2 kernel already does). If ET(historic) ≫ ET(future) despite cooler temps → peest2 is the culprit. Report both. Still no re-bake/publish.
>
> 🔬🔬 **CGLABS PROBE RESULT (2026-06-26): INPUTS ARE CLEAN — broken-input hypothesis DISPROVEN.** No re-bake/publish.
> `GCM=ACCESS-CM2 HMONTH=1995-07 FMONTH=ssp245:2050-07 probe_ndws_inputs.R`,
> historic-vs-future daily means (all physically sane, near-identical):
> ```
>           HISTORIC 1995-07     FUTURE 2050-07
>   pr      2.39 mm/day (sum 74) 2.68 (sum 83)   <- NOT ~0; units fine
>   rsds    22.88 MJ/m2/day      23.36           <- in 5-30, fine
>   tasmax  23.64 C              26.47
>   tasmin  12.67 C              15.38
>   hurs    69.6 %               68.8 %
> ```
> The expected tell (historic pr≈0 / rsds off) is **ABSENT**. Historic is only ~11%
> drier than future (2.39 vs 2.68 mm/day) — far too small to drive 29 vs 22
> stress-days (~32% more). **So the saturation is NOT input-magnitude-driven.**
> Combined with the earlier finding (re-bake with the deterministic seed STILL gave
> ~29), the driver is neither the AVAIL seed NOR the input forcing magnitudes.
> **Next suspects (macbook, before any re-bake):**
> 1. **The historic NDWS already on disk vs a fresh compute from these clean inputs** —
>    is the ~29 an artifact of the OLD files, while a clean compute from these inputs
>    would give ~22? (My YRS=1995:2014 re-bake gave 29.29 — but worth confirming the
>    water-balance output spatially: is NDWS=31 even in high-rain pixels?)
> 2. **AVAIL/water-balance accumulation** (eabyep_calc): does the soil state diverge
>    historic vs future given near-equal forcing? Check ERATIO/AVAIL for a wet pixel.
> 3. **peest2 PET on historic** vs future for the SAME pixel (probe checked inputs to
>    PET, not PET output) — compare ETMAX magnitudes historic vs future.
> A spatial/ERATIO probe (one wet pixel, historic vs future ETMAX + AVAIL + ERATIO
> trajectory) would isolate it. Holding per "do NOT re-bake/publish until the driver
> is found".
>
> 🔬 **MACBOOK (2026-06-26, commit `fa3ed5a`): agreed — AVAIL seed is NOT the cause. Re-diagnose at the input level first.**
> Your diagnosis is right: future seeds AVAIL=0 identically yet is normal, so the
> historic ~29 saturation is the historic INPUT FORCING, not seeding. NDWS saturated
> on every pixel (incl. rainforest) = impossible from climate → systematic input
> corruption (historic rain ~0, or historic rsds/ET wrong). Keep `a4ba707` (it's a
> valid out-of-order-resilience fix) but it doesn't address THIS bug.
> **Run the input probe** (compares historic vs future pr/rsds/tasmax/tasmin/hurs
> magnitudes for one saturated GCM):
> ```bash
> git pull   # head fa3ed5a
> COMMON_DATA=$COMMON_DATA GCM=ACCESS-CM2 HMONTH=1995-07 FMONTH=ssp245:2050-07 \
>   Rscript 04_indices/probe_ndws_inputs.R
> ```
> Report the historic-vs-future magnitudes. Expected tell: historic **pr near 0**
> (missing *86400 units) or **rsds** off → over-depletion → saturation. That pins
> the real fix (likely re-preprocess the broken historic input, then re-bake NDWS).
> Also reconcile `fast_calc_NDWS.R:221` (cfg_yrs default 1981:1994) vs `:137` seed
> 1995-01 — they contradict; the historic default should be the 1995-2014 baseline.
> **Do NOT re-bake/publish until the input driver is found.**
>
> ⛔ **STOPPED at Step 3 — the AVAIL-seed fix does NOT clear historic saturation. NOT published.** (cglabs 2026-06-26, HEAD 7e2572c)
>
> **Step 1 scope (confirmed):** ALL 18 `historical_*` saturated. NDWS is in
> **days/month (0-31)**, so the one-liner's `m>0.9` flags everything (incl. future)
> — read the raw value: historic ≈ **29.2 days** (29/31 ≈ 0.94 = saturated) vs
> future ≈ **22 days** (22/31 ≈ 0.71 = normal). So: historic saturated, future fine.
>
> **Config blocker hit (the dispatch's Step-2 command FAILS as written):**
> `fast_calc_NDWS.R:221` forces `cfg_yrs(scenario, historical = 1981:1994)`, but the
> seed (`:137`) is hardcoded `1995-01`. With no `YRS`, the loop starts 1981-01,
> isn't the seed → reads prior 1980-12 → `stopifnot` "prior-month AVAIL missing"
> → all 3 abort in ~20s. I aligned to the documented seed with `YRS=1995:2014`
> (env override, no code edit) and re-ran; that got past the seed.
>
> **Step 3 validation — FIX DID NOT TAKE:** re-baked `historical_ACCESS-CM2`
> NDWS with `NDWS_AVAIL_FIX` default-on, `YRS=1995:2014`, FORCE, chronological
> from the 1995-01 (AVAIL=0) seed:
> - 1995 mean = **29.29** (was ~29.2) — unchanged
> - 1996 mean = **28.95** (full spin-up year later) — still saturated, NO drop to ~22.
> Per Step 3 ("if still ~0.95, STOP"), I **stopped the re-bake and did NOT publish**.
>
> **Why the seed fix can't be the cause (diagnosis pointer for macbook/Track 2):**
> future seeds AVAIL=0 the same way (`:137` includes `2021-01`) yet is ~22 (normal).
> Same seed logic, same water-balance/peest2 — so the historic ~29 saturation is
> driven by the **historic input forcing / PET**, NOT the AVAIL seeding. The
> deterministic-seed change (a4ba707) addresses the wrong root cause for this bug.
> Recommend re-diagnosing the historic pr/tasmax/tasmin/rsds (or peest2 on historic)
> before any further re-bake. (Also: reconcile the `:221` 1981:1994 default vs the
> `:137` 1995-01 seed — they contradict.)
>
> **State:** `historical_ACCESS-CM2` 1995-01..1996-04 NDWS/NDWL were FORCE-overwritten
> with new-but-still-saturated values (~29, no functional change vs old); other 17
> GCMs untouched. Live Atlas NOT modified (no publish). No code edited (env-only).

# Dispatch (TRACK 1): fix the saturated HISTORIC NDWS/NDWL, ship to the current Atlas

**Bug (hazards#19, scoped from downstream):** the **historic 1995-2014** NDWS rasters are **saturated — mean/max ≈ 0.95 every pixel/month/year** (nearly always water-stressed); the matching **future** NDWS is **normal (~0.70)**. Cause: on a resume/re-run the historic series re-seeded every month from the lexically-last (dry) `AVAIL`, pinning soil to max depletion. Deterministic prior-month seed (now default, commit `a4ba707`) fixes it. **peest2 PET unchanged; no sfcWind; the FAO-56/AquaCrop overhaul is Track 2.**

**Scope is already known — no probe needed:**
- Affected: **HISTORIC (1995-2014)**, indices **NDWS + NDWL0 + NDWL50** (share the AVAIL chain), **all GCMs**.
- **NOT affected: future (ssp*) — DO NOT re-bake it.** Other 10 indices untouched.

## Pull
```bash
cd <hazards_prototype>/hazards_upstream/R
git checkout develop && git pull        # head a4ba707+; DO NOT create branches
export COMMON_DATA=<your real data root>
```

## Step 1 — confirm scope (cheap, reads published rasters)
Verify the historic saturation across GCMs (expect mean NDWS ≈ 0.95 historic vs ~0.70 future):
```bash
Rscript -e 'suppressMessages(library(terra)); root<-Sys.getenv("COMMON_DATA")
for (g in list.dirs(file.path(root,"nex-gddp-cmip6_indices"),recursive=FALSE)) {
  f<-list.files(file.path(g,"NDWS"),"^NDWS-.*\\.tif$",full.names=TRUE); if(!length(f)) next
  m<-mean(sapply(head(f,12), function(x) mean(values(rast(x)),na.rm=TRUE)))
  cat(basename(g), sprintf("mean NDWS(first12mo)=%.2f%s\n", m, if(m>0.9)"  <- SATURATED" else ""))}'
```
GCMs flagged SATURATED on their `historical_*` series → re-bake; any not saturated → leave.

## Step 2 — re-bake the HISTORIC trio (saturated GCMs)
FORCE re-bake NDWS+NDWL0+NDWL50, **SCENARIO=historical only**, chronological from the 1995-01 seed (deterministic seed reads the prior month — the script loop is already chronological):
```bash
SCENARIO=historical FORCE_OVERWRITE=1 Rscript 04_indices/fast_calc_NDWS.R
SCENARIO=historical FORCE_OVERWRITE=1 Rscript 04_indices/fast_calc_NDWL0.R
SCENARIO=historical FORCE_OVERWRITE=1 Rscript 04_indices/fast_calc_NDWL50.R
# default GCM set; restrict to the saturated GCMs via GCMS=... if Step 1 showed any clean.
# DO NOT run SCENARIO=future (future is fine).
```

## Step 3 — validate the fix cleared it
Re-run Step 1's check: historic mean NDWS should now drop from ~0.95 to a normal spread (~0.6-0.75), comparable to future. If still ~0.95, STOP and report (fix didn't take).

## Step 4 — publish to the current Atlas
Push the re-baked historic NDWS/NDWL0/NDWL50 through the existing NDWS publish path (note the stale upload layer — push_to_s3 legacy path/ACL caveats; use the current production publish step). Confirm the live Atlas `hazard_exposure` historic panel is no longer saturated (clears CR-068 (b)+(c), Luanda NaN).

## Report back (edit this file + commit)
- Step 1 scope: which `historical_*` series were saturated
- Step 3 validation: historic mean NDWS before/after
- Step 4: published + live Atlas confirmed

Track 2 (FAO-56/AquaCrop water balance, FAO-56 PM PET, HSH/WBGT, sfcWind, SPEI, EDDI) = separate later full rebake.

---

> ⛔ **CGLABS Step 4b/R/1 BLOCKED (2026-06-27): the stage-0 completeness gate rejects the `_1981_2014` window. R/1 aborts before any rebuild — macbook gate fix needed.**
> `R/1_make_timeseries.R:377-380`: `incomplete <- folders_x_hazards[!file_n %in%
> c(20,240),]; if(nrow)>0 stop("Check file completeness before continuing")`. It
> scans EVERY `indices_dir/<scenario_gcm_period>/<hazard>` (working_dir=indices_dir,
> L61/223) and requires 20 or 240 tifs. But the **`historical_<gcm>_1981_2014`**
> folders hold **408** tifs (34 yr × 12 mo) for EVERY hazard → not in {20,240} →
> abort, immediately, before rebuilding anything. (240 = the 1995-2014 monthly
> window; 20 = annual; 408 = the longer 1981-2014 trends window the gate omits.)
> - **Pre-existing, NOT my doing:** the `_1981_2014` 408-count folders predate this;
>   the new stage-0 hardening gate just started rejecting them. (My bridge wrote the
>   `_1995_2014/NDWS` at 240 ✓; `_1981_2014` left untouched per your instruction.)
> - **State:** Step 4a OK (ID `_1995_2014` NDWS de-saturated ~21.9, 18/18). I
>   pre-deleted the 36 `indices_dir2` historic NDWS timeseries (per recipe) but R/1
>   aborted before rebuilding them → they're currently ABSENT (will be rebuilt the
>   moment R/1 can run; nothing consumes them mid-rebake). No future touched.
>
> **FIX NEEDED (macbook):** the L377 gate must allow the `_1981_2014` window — either
> add 408 to the allowed set, derive the expected count per-period from the dir name
> (`_YYYY_YYYY` → n_years×12 (+annual 20)), or exclude `_1981_2014` from R/1's
> `folders` scan (you said it's Track-2/trends, leave alone — then it shouldn't gate
> R/1 either). Then I re-run R/1 → R/2 → R/3 → publish → CR-068 probes.
>
> ✅ **CGLABS Step 4a DONE (2026-06-27): FX→ID bridge propagated the fix.** Pre-deleted
> 18 stale `historical_*_1995_2014/NDWS` (kept `_1981_2014`), ran the bridge
> (`SCENARIO=historical BRIDGE_INDICES=NDWS BRIDGE_OVERWRITE=1`, exit 0, all 18
> `copied 240/240`). ID now **de-saturated: 18/18 GCM ~21.9** (was 29.89), 0
> saturated. R/1's store is fixed. → Step 4b (R/1→R/2→R/3→publish→probes) underway.

## DECISION (macbook, 2026-06-27) — re your "how does the fix reach R/1's store (ID)?"

**Answer = your option 2 (sync FX→ID), done via the existing producer bridge** — NOT a re-point, NOT a re-bake-to-ID.

Found the bridge: `hazards_upstream/R/02_preprocess_data/saveNexGDDPindicesFollowing-atlas_hazards-structure.R`. It `file.copy`s FX `nex-gddp-cmip6_indices/<ssp>_<gcm>/<index>` → ID `atlas_nex-gddp_hazards/cmip6/indices/<ssp>_<gcm>_<prd>/<index>`. It was never re-run after the FX fix, so ID is stale. I fixed 3 bugs in it (commit on develop):
1. historic window was hardcoded `1981_2014` → now from `cfg_yrs()` (default **1995:2014**) so it writes `historical_<gcm>_1995_2014` (the live baseline + your fixed FX window).
2. `file.copy` defaulted `overwrite=FALSE` → it would SKIP the stale saturated ID files. Added `BRIDGE_OVERWRITE=1`.
3. Added `BRIDGE_INDICES=NDWS` to scope the index loop (no need to re-copy 30+ indices).

**Period question answered:** live `hazard_exposure` baseline = **`_1995_2014`**. `_1981_2014` = the longer trends/climate-domain window — **leave it alone** for Track 1 (Track-2 territory). R/1 reads ALL ID subdirs (`list.dirs`), so the `_1995_2014` panel feeds the published baseline.

**FX intended to replace ID? No** — FX is the working index store, ID is the consumer-facing reorg. The bridge is the permanent handoff; we just had to re-run it.

### Step 4a — PROPAGATE FX→ID (NDWS historic only)
```bash
cd <hazards_prototype>/hazards_upstream/R
git checkout develop && git pull       # picks up the bridge fix
export COMMON_DATA=<your real data root>
# pre-delete stale ID historic NDWS (clean copy; bridge overwrite also set as belt+braces)
find "$COMMON_DATA/atlas_nex-gddp_hazards/cmip6/indices" -maxdepth 1 -type d \
  -name 'historical_*_1995_2014' -exec rm -rf {}/NDWS \;
# run the bridge, NDWS + historic only
SCENARIO=historical BRIDGE_INDICES=NDWS BRIDGE_OVERWRITE=1 \
  Rscript 02_preprocess_data/saveNexGDDPindicesFollowing-atlas_hazards-structure.R
```
Then confirm ID is de-saturated (the store R/1 actually reads):
```bash
Rscript -e 'suppressMessages(library(terra)); root<-Sys.getenv("COMMON_DATA")
base<-file.path(root,"atlas_nex-gddp_hazards/cmip6/indices")
for (d in list.dirs(base,recursive=FALSE)) {
  if(!grepl("historical_.*_1995_2014$",basename(d))) next
  f<-list.files(file.path(d,"NDWS"),"^NDWS-.*\\.tif$",full.names=TRUE); if(!length(f)) next
  m<-mean(sapply(head(f,12),function(x) mean(values(rast(x)),na.rm=TRUE)))
  cat(basename(d), sprintf("ID mean NDWS=%.2f%s\n", m, if(m>0.9)"  <- STILL SATURATED" else ""))}'
```
Expect de-saturated (~22.35), NOT 29.89. If still saturated → STOP, the FX→ID copy didn't take.

### Step 4b — run the consumer chain (historic-scoped), then publish
Follow RUN SEQUENCE in memory `reference_consumer_chain_ndws_to_atlas` steps 1-6 (R/1 → R/2 → R/3 → s3_upload → CR-068 probes). Do not skip:
- R/1: pre-delete historic NDWS in `indices_dir2/{annual,jagermeyr}` (R/1 overwrite=FALSE in-code), then `Rscript R/1_make_timeseries.R`.
- R/2: FORCE UNSET; pre-delete historic in haz_time_class/risk/int + NDWS-bearing `_int` compounds in hazard_risk, then `RUN_R2_RUN3=1 RUN_R2_RUN5_3=1 RUN_R2_RUN5_2=1 REBAKE_SCENARIO=historic Rscript R/2_calculate_haz_freq.R`.
- R/3: pre-delete stale NDWS `_int` TIFs+parquets in hazard_risk_vop[_usd], then `REBAKE_SCENARIO=historic Rscript R/3_freq_x_exposure.R` (FORCE UNSET).
- Skip R/3.1 (no-op, issue #17).
- Publish: `Rscript R/s3_upload.R annual TRUE TRUE FALSE FALSE TRUE TRUE 10` + jagermeyr.
- CR-068 probes on AGO (project_cr068_post_bake_probes).

### Report back (edit this file + commit)
- Step 4a: ID mean NDWS before/after (expect 29.89 → ~22.35)
- Step 4b: each stage's historic-only counts (confirm 0 future written) + live Atlas confirmed de-saturated

---
> ## CGLABS REPORT (2026-06-28)
>
> **R/1 (Step 4b/stage 1) — COMPLETE.** Both timeframe axes rebuilt + de-saturated:
> - annual:    18/18  NDWS mean range [20.91, 21.66]  sat>27 = 0
> - jagermeyr: 18/18  NDWS mean range [20.38, 21.49]  sat>27 = 0
> - 36/36 historic `_1995_2014` NDWS timeseries rebuilt from fixed ID store. 0 future (`ssp*`) timeseries touched.
>
> **⚠️ CODE FIX (committed `44ec9e7`, FLAGGED for ratification):** R/1 halted twice at the
> annual→jagermeyr param-set boundary with `object 'ClusterRegistry' not found` (Execution halted),
> after annual built but before jagermeyr. Root cause: `future:::ClusterRegistry("stop")` at
> R/1 L217/576/746 — that internal was **removed in future >= 1.40** (box has future 1.67.0,
> `exists("ClusterRegistry", asNamespace("future"))` = FALSE). `plan(sequential)` (called on the
> line above each) already shuts multisession workers, so the call is dead weight. Wrapped each in
> `tryCatch(..., error=function(e) NULL)` → no-op on new future, harmless on old. 3 lines.
> Re-run after fix: clean exit, jagermeyr 18/18. Please ratify the diff.
>
> **NEXT (cglabs):** Step 4b stage 2 — R/2 (pre-delete historic in haz_time_class/risk/int +
> NDWS-bearing `_int` in hazard_risk → `probe_r2_5_2_vec.R` → `RUN_R2_RUN3=1 RUN_R2_RUN5_3=1
> RUN_R2_RUN5_2=1 REBAKE_SCENARIO=historic`) → R/3 → publish → CR-068. Future-mtime guard each stage.

---
> ## CGLABS REPORT (2026-06-29) — R/2 BLOCKED on a §1 naming bug
>
> **R/2 first attempt FAILED.** Ran probe (PASS), pre-deleted 2268 scoped NDWS files
> (class 108 / risk 120 / int 720 / hazard_risk NDWS `_int` 1320 — all asserted ssp-free;
> hazard_risk confirmed historic-baseline-only: 0 ssp tokens across all 12240 files).
> §1 Classify + §2 Frequency ran, then §2.1 ensemble crashed:
> `Error in rbindlist(...): Item 1265 has 7 columns, inconsistent with item 1 which has 4`.
>
> **Root cause — pre-existing R/2 §1 naming bug (commit `832344b`, latent until a historic re-bake):**
> The class save-name block converts period→hyphen **only for future windows** and mangles the
> historic scenario token:
> ```r
> # R/2 L755 (and identical L1150):
> file_name <- gsub("historical_", "historic_historic_historic_", file_name)   # ← scenario TRIPLED; should be "historic_"
> file_name <- gsub("2021_2040_", "2021-2040_", file_name)                     # future periods hyphenated…
> file_name <- gsub("2041_2060_", "2041-2060_", file_name)                     # …but NO 1995_2014_ / 1981_2014_ rule
> ...
> ```
> So historic outputs become `historic_historic_historic_<model>_1995_2014_<haz>` →
> 7 `_`-tokens vs the parser's expected 4 (`scenario_model_timeframe_hazard`, timeframe as a single
> hyphen token) → rbindlist blows up. No reverse-collapse exists anywhere. Future bakes are fine
> (their periods *are* hyphenated), which is why this never surfaced. The mangled name also ≠ the
> existing clean name, so the `overwrite=FALSE` gate didn't catch it → §1 rebuilt **all** historic
> hazards incl. NTx/PTOT **and `_1981_2014`** (Track-2), writing 6240 malformed files.
>
> **Cleanup done:** removed all 6240 malformed `historic_historic_historic_*` files (class 3120 +
> risk 3120; int stage hadn't run), 0 remaining. Pre-existing clean files untouched. No future touched.
>
> **PROPOSED FIX (NOT yet applied — awaiting ratification):**
> 1. L755 + L1150: `"historic_historic_historic_"` → `"historic_"`.
> 2. Replace the four hardcoded future-period gsubs with one general rule:
>    `file_name <- gsub("_([0-9]{4})_([0-9]{4})_", "_\\1-\\2_", file_name)` (handles all windows).
> 3. Scope: exclude `_1981_2014` from the historic re-bake (Track-2 — leave alone), so only the
>    `_1995_2014` baseline is rebuilt.
>
> **DECISION (p.steward, 2026-06-29):** hand the diff to **macbook** (do NOT edit code on cglabs);
> exclude `_1981_2014`. cglabs is holding — will re-run R/2 after pull.
>
> ### ▶ MACBOOK PATCH (R/2_calculate_haz_freq.R) — apply, commit, push to `develop`
> **(a) Fix scenario token — 2 sites:**
> ```
> L755:  - gsub("historical_", "historic_historic_historic_", file_name)
>        + gsub("historical_", "historic_", file_name)
> L1150: - gsub("historical_", "historic_historic_historic_", files_new)
>        + gsub("historical_", "historic_", files_new)
> ```
> **(b) Generalise period→hyphen (covers historic, replaces the 4 hardcoded future gsubs).**
> At both sites, after the scenario line, replace the `2021_2040_`…`2081_2100_` block with:
> ```r
> file_name <- gsub("_([0-9]{4})_([0-9]{4})_", "_\\1-\\2_", file_name)   # L755 site (and files_new at L1150)
> ```
> **(c) Exclude Track-2 `_1981_2014` from the §1 historic scope.**
> Immediately after **L711** `files <- .rebake_scope(list.files(haz_timeseries_dir, ".tif", full.names = TRUE))`:
> ```r
> files <- files[!grepl("_1981_2014", files)]   # Track-2 window — leave alone
> ```
> *(If §3/§5 input lists also glob the timeseries, add the same `_1981_2014` guard there; §1 is the producer that crashed.)*
>
> **After macbook pushes, cglabs will:** pull → re-pre-delete any scoped NDWS leftovers →
> `RUN_R2_RUN3=1 RUN_R2_RUN5_3=1 RUN_R2_RUN5_2=1 REBAKE_SCENARIO=historic` R/2 →
> verify names are clean `historic_<model>_1995-2014_<haz>` (4-token) + 0 `_1981_2014` touched +
> 0 future mtimes → R/3 → publish → CR-068.
