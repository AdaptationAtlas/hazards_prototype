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
