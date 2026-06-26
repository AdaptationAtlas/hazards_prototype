# Dispatch: hazards#19 AVAIL fix — show legacy-vs-fixed impact (pre-approval)

**Do NOT integrate / re-bake yet.** Goal: quantify the impact of the deterministic AVAIL fix on real data so Pete can approve before we flip it on. Head = origin/develop `c2508b7`.

## Background
`fast_calc_NDWS/NDWL0/NDWL50.R` seed soil moisture from the lexically-last `AVAIL-*.tif` → mis-seeds month N from a wrong month on resumed/out-of-order/gap-fill runs (hazards#19 historic saturation). Fix added behind `NDWS_AVAIL_FIX` (default OFF — live behaviour unchanged): seeds from the deterministic prior month, fails loud if it's missing.

## Pull
```bash
cd <hazards_prototype>/hazards_upstream/R
git checkout develop && git pull        # head c2508b7; DO NOT create branches
export COMMON_DATA=<your real data root>
```

## Run the impact comparison
Runs the SAME real script both ways on a baked GCM/month using the #19 trigger (recompute a mid-series month while later months exist), diffs the two NDWS rasters, restores canonical data afterward:
```bash
bash 04_indices/compare_avail_fix.sh                              # NDWS, ACCESS-ESM1-5, 1996-06
INDEX=NDWL0  bash 04_indices/compare_avail_fix.sh                 # repeat for NDWL0
INDEX=NDWL50 bash 04_indices/compare_avail_fix.sh                 # and NDWL50
# optionally a 2nd GCM / month: GCM=EC-Earth3 TGT_YR=2005 TGT_MN=09 ...
```
It prints: cells changed, % changed, delta (fixed−legacy) min/mean/max, and how many cells legacy over-counts (saturation direction), plus mean NDWS legacy vs fixed.

## Report back (edit this file + commit)
- the diff stats per index/GCM/month run
- your read: is the legacy-vs-fixed difference large (bug materially wrong) or negligible?
- confirm canonical data restored (script does this; spot-check one tif mtime unchanged)

Do not push code fixes. After Pete reviews the impact and approves, macbook flips `NDWS_AVAIL_FIX` to default-on (integrate) and dispatches the trio re-bake (NDWS+NDWL0+NDWL50, historical+future).
