# Dispatch (TRACK 1): ship the hazards#19 NDWS/NDWL fix to the CURRENT Atlas

**Goal:** fix the live NDWS/NDWL0/NDWL50 saturation bug NOW, on the existing legacy method (peest2 PET unchanged) — independent of the FAO-56/AquaCrop overhaul (Track 2, full rebake). **No sfcWind needed.** Head = origin/develop `a4ba707`.

## What changed
`fast_calc_NDWS/NDWL0/NDWL50.R` seeded soil moisture from the lexically-last `AVAIL-*.tif` → mis-seeds month N from the wrong month on resumed/out-of-order/gap-fill runs (hazards#19 historic saturation). The deterministic prior-month seed is now the **default** (`NDWS_AVAIL_FIX` default-on, commit a4ba707); the legacy lexical-last path is reachable only via `NDWS_AVAIL_FIX=0` (for the comparison below). PET method, formulas, soil — all unchanged.

## Pull
```bash
cd <hazards_prototype>/hazards_upstream/R
git checkout develop && git pull        # head a4ba707; DO NOT create branches
export COMMON_DATA=<your real data root>
```

## Step 1 — impact comparison (legacy vs fixed)
Runs each script both ways on baked GCM/months using the #19 trigger (recompute a mid-series month while later months exist), diffs, restores canonical data:
```bash
bash 04_indices/compare_avail_fix.sh                 # NDWS, ACCESS-ESM1-5, 1996-06
INDEX=NDWL0  bash 04_indices/compare_avail_fix.sh
INDEX=NDWL50 bash 04_indices/compare_avail_fix.sh
# a 2nd GCM/month is worth it: GCM=EC-Earth3 TGT_YR=2005 TGT_MN=09 bash 04_indices/compare_avail_fix.sh
```
Report the diff stats here (cells changed, mean shift, saturation direction). Pete reviews → approves the re-bake.

## Step 2 — re-bake the trio (after approval)
FORCE re-bake NDWS+NDWL0+NDWL50 across the full production scope. **Run months IN ORDER from the seed** (1995-01 historical / 2021-01 future) — the deterministic seed reads the prior month, so chronological order is required; the script's loop is already chronological, just don't shard months out of order.
```bash
# historical (seed 1995-01)
SCENARIO=historical FORCE_OVERWRITE=1 Rscript 04_indices/fast_calc_NDWS.R
SCENARIO=historical FORCE_OVERWRITE=1 Rscript 04_indices/fast_calc_NDWL0.R
SCENARIO=historical FORCE_OVERWRITE=1 Rscript 04_indices/fast_calc_NDWL50.R
# future (seed 2021-01) - repeat the three with SCENARIO=future
```
(Default GCM set; default fix on. fast_calc_NDWS/NDWL are the slow ones ~835 s/GCM — the Rcpp speedup is Track 2, not this fix.)

## Step 3 — publish to the current Atlas
Push the re-baked NDWS/NDWL0/NDWL50 through the existing NDWS publish path. NOTE the publish layer is the known-stale upload system (push_to_s3 legacy path; ACL caveats) — use the current production publish step for these indices and verify the live Atlas reads the new values.

## Report back (edit this file + commit)
- Step 1 impact stats (per index/GCM/month)
- Step 2 re-bake: completion + any errors with file:line
- Step 3 publish: confirmed live

Track 2 (FAO-56/AquaCrop water balance, FAO-56 PM PET, HSH/WBGT, sfcWind, SPEI, EDDI) is a SEPARATE later full rebake — do not mix into this fix.
