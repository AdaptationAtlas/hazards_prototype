# DISPATCH — cglabs ⇄ macbook — issue #9: `hazard='none'` on EVERY combo + publish the notebook-facing hazard_exposure

Branch `develop`. Append-only; newest on top. cglabs runs, appends `### RESPONSE`, pushes.
**Authorship:** probe + publish script authored by **macbook / hazards_prototype**; **cglabs runs on-node** (owns the data) + publishes.
**Tracks:** [hazards_prototype#9](https://github.com/AdaptationAtlas/hazards_prototype/issues/9) (bjyberg). Supersedes the publish half of `DISPATCH_cglabs_avail_fix.md` (2026-07-09 "PUBLISH-READY, holding for go").

---

## [macbook / hazards_prototype · 2026-09-14 #1] PROBE → scoped §5.3 rebuild → FORCE R/3 → publish 3 tiers

### Why (verified from macbook 2026-09-10, all read-only)

- **The July full-refresh was never published.** Newest object under `s3://digital-atlas/domain=hazard_exposure/` is dated **2026-06-05**. The 2026-07-09 hold for "explicit publish go" was answered by Brayden on #9 (2026-08-18: "happy for the fix/publish to go live") and not actioned.
- **What the notebook actually reads** (grep of atlas_notebooks): `domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=jagermeyr/model=ENSEMBLEmean/severity={severe,moderate,extreme}/int=multi-hazard.parquet` (historic rows INSIDE the file) + reference `domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/variable=crop-livestock_all.parquet`.
  - **⚠️ `R/s3_upload.R` publishes a DIFFERENT product** (`source=atlas_cmip6/variable=vop_intld15|vop_usd15/.../interaction.parquet`). The July "publish sequence" would never have reached the notebook. **HOLD the s3_upload.R / derive route** (nex-gddp data under an `atlas_cmip6` label is a provenance question for p.steward). This dispatch publishes ONLY via `scripts/r3_publish_tiers.R`.
- **Live notebook files** (I read all 3 × 60.4 M rows): 4 combos (`NDWS+NTx35+NDWL0`, `NDWS+THI-max+NDWL0`, `PTOT-L+NTxS+PTOT-G`, `PTOT-L+THI-max+PTOT-G`), **zero `hazard='none'` rows**. So Brayden's "none only for NDWS+THI-max+NDWL0" is your LOCAL July product, not S3.
- **Where `none` went missing (hypothesis to prove with the probe):** `none` was added to §5.2 stacks in `41c1c00` (2026-05-26). Stage F ran §5.2 under `FORCE_OVERWRITE=1` on 2026-05-27/28 (44,880 tifs per timeframe) → **§5.2 stacks should all carry `none`**. §5.3 (per-crop `_int` combine) was never force-rebuilt; the July Track-1 run pre-deleted + rebuilt only the livestock-NDWS `_int` files (your "660/660"). So `hazard_risk/<tf>/*_int.tif` for the other combos predate 05-26 → no `none` → R/3 §4 inherits the gap. **If true, the fix is §5.3 (~1-3 h/tf), NOT a §5.2 re-run (~26 h).**
- **Reference `crop-livestock_all.parquet` (2026-01-22) is NOT refreshed here.** Its producer→canonical mapping is an unresolved drift (row counts, tech levels, derived `unit_full`; see atlas_notebooks `playbook/handovers/climateRationale/dispatches/2026-05-26_exposure-producer-drift.md`). Brayden's ask is total VOP = `any + none` from the hazard_exposure file itself, which this dispatch delivers; the reference stays as-is. `scripts/r3_publish_tiers.R --reference` exists but is opt-in and expected to FAIL its column gate until that drift is resolved — **do not pass it**.

### STEP 0 — sync
```bash
cd ~/atlas/hazards_prototype && git pull --ff-only origin develop && git log -1 --oneline   # expect the commit that adds this file
mkdir -p logs
```

### STEP 1 — PROBE (read-only, ~1-5 min). Paste the whole output.
```bash
Rscript R/probe_none_coverage.R |& tee logs/probe_none_$(date +%Y%m%d_%H%M%S).log
```
Per timeframe it prints: **A)** §5.2 stacks per combo + whether the historic ENSEMBLEmean stack and one per-GCM stack carry a `*_none` layer; **B)** §5.3 `_int` stacks per combo, how many ENSEMBLEmean `_int` carry `none`, mtimes; **C)** `hazard_vars × hazard` counts in your local `haz-freq-exp_*_ENSEMBLEmean_int_adm_<sev>.parquet` (usd + intld dirs); **VERDICT** = TIER 0 / A / B / C.
- **Expected: TIER A** (A ok, B not ok) for both `annual` and `jagermeyr`.
- **Also read B)'s "combos with §5.2 stacks but NO _int files" line** — that tells us whether the crop-NDWS combo (`NDWS+NTx35+NDWL0` / `NDWS+NTxS+NDWL0`) was deleted in July and never recombined (Brayden's CSV has only 3 combos).
- **STOP and report if the verdict is TIER B or TIER C, or if "combos with _int files but NO §5.2 stacks" is non-empty** (§5.3 would `stop()`); macbook re-scopes. Otherwise continue.

### STEP 2 — TIER A: rebuild §5.3 for ALL combos (both timeframes)
Scoping recipe = **pre-delete + overwrite=FALSE** (the R/2 convention; fails safe). Move, don't `rm`, so we can roll back until publish is verified.
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
WORKING=/home/jovyan/common_data/nex-gddp-cimp6_hazards        # = working_dir from 0_server_setup.R
for tf in annual jagermeyr; do
  d=$WORKING/Data/hazard_risk/$tf
  mkdir -p $d/_int_stale_$STAMP
  echo "$tf: $(ls $d/*_int.tif 2>/dev/null | wc -l) _int tifs before"
  find $d -maxdepth 1 -name '*_int.tif*' -exec mv -t $d/_int_stale_$STAMP/ {} +
  echo "$tf: $(ls $d/*_int.tif 2>/dev/null | wc -l) _int tifs after (expect 0) | stale parked: $(ls $d/_int_stale_$STAMP | wc -l)"
done
```
Run §5.3 only. `FORCE_OVERWRITE` **UNSET** (FORCE would rebuild §1/§2/§4/§5.2 = days). `REBAKE_SCENARIO` **UNSET** (§5.3 must read the full multi-scenario stack set — guard `5a566a5`). `RUN_R2_RUN5_2` unset → §5.2 does not run.
```bash
SKIP_R2_RUN1=1 SKIP_R2_RUN2=1 SKIP_R2_RUN4=1 RUN_R2_RUN5_3=1 \
nohup Rscript -e 'source("R/0_server_setup.R"); source("R/2_calculate_haz_freq.R")' \
  &> logs/r2_5_3_rebuild_$STAMP.log &
echo $! > logs/r2_5_3_rebuild_$STAMP.pid
```
(If your shell profile already sources setup for bare `Rscript R/2_…`, your usual form is fine — the env flags are what matter. The log header prints `run5.3 = TRUE overwrite5.3 = FALSE run5.2 = FALSE` — check that first.)

**Early kill-gate (≤15 min in):** once the first `_int.tif` files appear in `Data/hazard_risk/annual/`, check one ENSEMBLEmean file carries the layer:
```bash
f=$(ls -t $WORKING/Data/hazard_risk/annual/*_ENSEMBLEmean_*_int.tif | head -1)
Rscript -e "x<-names(terra::rast('$f')); cat(length(x),'layers;', sum(grepl('_none',x)),'none layers\n'); cat(head(grep('_none',x,value=TRUE),3),sep='\n')"
```
Expect `none layers > 0` (one per scenario×timeframe). **If 0 → kill the run** (`kill $(cat logs/r2_5_3_rebuild_$STAMP.pid)`), report.

**Done criteria:** log shows `5.3) …` complete for both timeframes; `check5.3` integrity → 0 failed; `ls Data/hazard_risk/<tf>/*_int.tif | wc -l` ≈ the parked count (report both numbers; a shortfall = combos §5.3 no longer builds → report which). Then **re-run STEP 1's probe** → expect `B) ALL ENSEMBLEmean _int stacks carry none: TRUE` and VERDICT **TIER 0** for both tf.

### STEP 3 — R/3 (FORCE, proven July path, ~12 h; `worker_n4.2 = 1` sequential is the OOM-safe floor)
```bash
FORCE_OVERWRITE=1 nohup Rscript -e 'source("R/0_server_setup.R"); source("R/3_freq_x_exposure.R")' \
  &> logs/r3_force_$STAMP.log &
```
Done criteria: exit 0; then `Rscript R/probe_none_coverage.R` section **C)** shows `none` for **every** `hazard_vars` in all 6 ENSEMBLEmean parquets (usd + intld × 3 sev) for `jagermeyr` (and `annual`). Paste the C) tables.
**Do NOT run `R/derive_historic_model_parquet.R` or `R/s3_upload.R`** — that is the held atlas_cmip6 route.

### STEP 4 — PUBLISH to the notebook path (3 tiers), gated
```bash
Rscript scripts/r3_publish_tiers.R --dry-run |& tee logs/publish_tiers_dry_$STAMP.log
```
Paste. Gates per tier: G1 file/size · **G2 every hazard_vars has `none` and n(none)==n(any)** · G3 scenarios = historic+4 ssp · G4 severity==tier · **G5 columns identical to live** (it downloads the live object for the backup anyway; prints whether live has `none` and its hazard_vars for the record).
- **Any gate FAIL → that tier is skipped, nothing uploaded. G5 FAIL (schema drift) → STOP, paste the column diff.** Do not pass `--allow-schema-drift` without macbook (notebook SQL depends on the columns).
If the dry run is clean for all three tiers:
```bash
Rscript scripts/r3_publish_tiers.R |& tee logs/publish_tiers_$STAMP.log
```
It backs up each live object to `s3://digital-atlas/sandbox/backup/issue9_<STAMP>/<key>` (download+upload, ACL public-read — never `s3_file_copy`), uploads, verifies remote size == local, and probes HTTPS range (expect **206**). **Do NOT pass `--reference`** (see "Why").

### STEP 5 — REPORT (`### RESPONSE` block here, push)
Probe verdicts (before/after), §5.3 file counts, R/3 exit + C) tables, publish log tail (3 × size match, 3 × 206, backup prefix). **macbook then runs the CR-068 probes against live** (`probe_no_hazard_arithmetic_quick.sh AGO`, `probe_cross_parquet_vop_drift.sh AGO`) and posts the numbers on #9. Only after that: `rm -r Data/hazard_risk/*/_int_stale_*`.

### Budget / don'ts
- probe 5 min · §5.3 ~1-3 h per tf · R/3 ~12 h · publish ~20 min. Total ≈ 1 working day of wall-clock; nothing needs you present except the two gates.
- Don't `FORCE_OVERWRITE=1` R/2. Don't run `s3_upload.R` / derive. Don't `rm` the parked `_int` until STEP 5. Don't touch `annual` vs `jagermeyr` scoping (R/2 loops both; notebook needs `jagermeyr`, s3_upload route would need `annual` too).
