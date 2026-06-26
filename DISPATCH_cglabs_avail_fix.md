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
