# DISPATCH — poultry_highland THI partial rebake (issue #13)

For: cglabs Claude Code (live `Data/` + S3) — or run by hand in the cglabs terminal.
From: macbook session, 2026-06-24. Goal: re-bake ONLY the poultry_highland THI
products with the corrected Extreme threshold (79 → **89**, committed in
`metadata/haz_classes.csv` @16dce34) and republish.

## Why partial (not a full R/2 bake)
The threshold change only affects the per-crop crop-stack assembly for
`poultry_highland`. Classification is keyed by threshold VALUE — **G89 already
exists** (cattle/goats/pigs_highland Extreme=89), so §1 is a no-op. §2 (freq),
§4 (mean/sd), §5.2 (interactions) are crop-free — reusable. Only **R/2 §3**
(crop stacks, per-crop files) and **R/3 §4.1/§4.2** (VoP risk + parquet) need
regen. Overwrite stays OFF + we pre-delete only the poultry files, so only
poultry regenerates; the combined §4.2 parquet is deleted so it re-aggregates.

## PRE-FLIGHT (verify BEFORE any delete — do not skip)
```bash
git pull origin develop            # gets RUN_R2_RUN3 toggle + poultry threshold fix
# 1. confirm the corrected threshold is in metadata:
grep "poultry_highland" metadata/haz_classes.csv     # Extreme rows must read 89, not 79
# 2. confirm the axis + that §3 output (haz_risk) is the live producer R/3 reads:
Rscript -e 'source("R/0_server_setup.R");
  cat("axis dirs:\n"); print(list.dirs(atlas_dirs$data_dir$hazard_timeseries_class, recursive=FALSE, full.names=FALSE));
  hr <- file.path(atlas_dirs$data_dir$hazard_risk, "annual");
  cat("haz_risk/annual exists:", dir.exists(hr), "\n");
  cat("poultry-highland stacks present:\n"); print(head(list.files(hr, "poultry-highland"), 8));
  cat("G89 THI classified present (annual):\n");
  print(head(list.files(file.path(atlas_dirs$data_dir$hazard_timeseries_class,"annual"), "THI.*G89"), 4))'
```
**STOP and report if:** poultry rows still show 79; `haz_risk/annual` missing; no
`poultry-highland` stacks (means §3 isn't the live producer — escalate, don't guess);
or no `THI...G89` classified files (then §1 is NOT a no-op — different plan).

## STEP 1 — R/2 §3, poultry-scoped
```bash
HR=$(Rscript -e 'source("R/0_server_setup.R"); cat(file.path(atlas_dirs$data_dir$hazard_risk,"annual"))')
# verify-then-delete ONLY poultry-highland stacks (all severities; strictly only
# _extreme changed, but one crop is cheap — keep it simple):
ls "$HR"/poultry-highland_* | wc -l        # eyeball the count first
rm "$HR"/poultry-highland_*.tif
# run §3 ONLY, overwrite OFF (FORCE_OVERWRITE unset => overwrite3=FALSE =>
# only the deleted poultry files regenerate; every other crop is skipped):
SKIP_R2_RUN1=1 SKIP_R2_RUN2=1 SKIP_R2_RUN4=1 RUN_R2_RUN3=1 \
  nohup Rscript -e 'source("R/0_server_setup.R"); source(file.path(project_dir,"R","2_calculate_haz_freq.R"))' \
  > logs/poultry_r2_sec3_$(date +%Y%m%d_%H%M).log 2>&1 &
tail -f logs/poultry_r2_sec3_*.log         # watch the .sec2_start/done "3) Crop risk stacks" timers
# confirm poultry stacks rewritten:
ls -la "$HR"/poultry-highland_*.tif | head
```

## STEP 2 — R/3 §4.1 (poultry VoP tifs) + §4.2 (re-aggregate parquet)
```bash
VOP=$(Rscript -e 'source("R/0_server_setup.R"); cat(file.path(atlas_dirs$data_dir$hazard_risk_vop,"annual"))')
# delete poultry §4.1 tifs AND the §4.2 combined parquets (so both regen in one
# overwrite-OFF pass — §4.2 is all-crops so it must be rebuilt, not patched):
ls "$VOP"/poultry-highland_* | wc -l
rm "$VOP"/poultry-highland_*.tif
ls "$VOP"/*.parquet                        # eyeball the §4.2 parquet set
rm "$VOP"/*.parquet                         # combined-all-crops -> rebuild from (now-corrected) tifs
# run R/3 overwrite OFF: §4.1 regenerates only missing poultry tifs; §4.2 rebuilds
# the deleted parquets from ALL tifs (corrected poultry + existing others):
nohup Rscript -e 'source("R/0_server_setup.R"); source(file.path(project_dir,"R","3_freq_x_exposure.R"))' \
  > logs/poultry_r3_$(date +%Y%m%d_%H%M).log 2>&1 &
tail -f logs/poultry_r3_*.log
```
⚠️ Do NOT set `FORCE_OVERWRITE` in either step — that would re-run every crop
(full bake). Selective-delete + overwrite-off is the whole scoping mechanism.

## STEP 3 — validate (before/after)
```bash
# spot-check: poultry_highland Extreme exposure should DROP vs the old G79 run
# (89 is a higher bar than the old erroneous 79). Compare a known admin's
# poultry_highland extreme VoP in the new parquet vs the published one.
```

## STEP 4 — publish
Republish the hazard_risk_vop family (push_to_s3.R blocks 2.4-2.9:
hazard_timeseries_risk, _int, haz_risk, **hazard_risk_vop**, hazard_risk_vop_usd),
ACL `public-read`. Then close #13 + note the corrected threshold + version bump.

## Open confirmations for the cglabs session (live data)
1. Is `haz_risk/annual` the ONLY axis with poultry (jagermeyr too?). If livestock
   THI exists under `jagermeyr/`, repeat the delete+regen there.
2. Confirm `timeframe_choices` in R/3 (line ~432) covers the annual axis.
3. Confirm §3 is genuinely the live producer of the `haz_risk` files R/3 reads
   (not superseded by another path) before deleting.
4. Exact §4.2 parquet naming/location under `hazard_risk_vop/annual` — adjust the
   `rm *.parquet` glob if there are subdirs.
