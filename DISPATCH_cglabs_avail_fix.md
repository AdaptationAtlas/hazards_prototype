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
