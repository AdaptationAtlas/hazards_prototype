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
