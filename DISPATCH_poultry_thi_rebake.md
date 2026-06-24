> ⛔ **BLOCKED + DEFERRED — do NOT run this as a standalone partial.**
> (1) cglabs blocked the v1 plan as unsafe (see "CGLABS FINDINGS"); (2) macbook
> added the missing toggles + corrected the §3-ENSEMBLE read (see "MACBOOK
> RESPONSE"); (3) Pete DECIDED 2026-06-24 to **defer poultry-89 to the next full
> rebake** rather than run a grown-scope partial (vop+vop_usd+ha × both axes ≈
> full-bake blast radius anyway). #13 stays OPEN. This doc is retained as the
> poultry-specific background; the actionable pickup list is **R/NEXT_FULL_REBAKE.md**.

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

---

## CGLABS FINDINGS 2026-06-24 — why this is BLOCKED (evidence)

Verified against live `Data/` + the current R/2 / R/3 code. Nothing deleted/run.

**What R/3 §4.1 actually consumes** (the files the threshold fix must reach):
`R/3_freq_x_exposure.R` sets `ensemble_only4.1 <- TRUE` (line 398) and filters
its inputs to `grep("ENSEMBLE|historic", files)` (line 944). In
`haz_risk/<axis>/`, the poultry-highland files matching that are the **18
`ENSEMBLEmean/ENSEMBLEsd` single stacks** (+ their `_int` interaction stacks) —
NOT the per-GCM stacks.

**Three blockers:**
1. **§3 rebuilds the wrong subset.** `RUN_R2_RUN3=1` runs §3, which writes
   `haz_risk/<crop>_<model>_<sev>.tif` (line 996) where `models <-
   haz_freq_file_tab[,unique(model)]` (line 956) — **ENSEMBLE excluded**
   upstream. So §3 regenerates only the 162 per-GCM singles, never the 18
   ENSEMBLE singles or the `_int` stacks §4.1 reads. (`overwrite3` is correctly
   decoupled via `RUN_R2_RUN3`, so that part is fine — it's just the wrong files.)
2. **No producer for the consumed files in enabled code.** §5.2 writes
   interactions to `haz_time_int_dir`, NOT `haz_risk` (lines 1434/1523). The
   only `haz_risk` `_int` writer is §5.3 (line 1670), and `run5.3 <- FALSE`
   (line 623). There is no ensemble sub-step in §3. ⇒ The on-disk
   `haz_risk/poultry-highland_ENSEMBLEmean_*.tif` have **no locatable regen path
   in the current enabled pipeline**. Deleting them = permanent loss.
3. **§5.2 run gate ≡ overwrite gate.** `run5.2 <- .force_overwrite_r2` and
   `overwrite5.2 <- .force_overwrite_r2` (lines 605/609). There is no env to run
   §5.2 overwrite-off; enabling it (`FORCE_OVERWRITE=1`) forces a full,
   all-crops, both-axes bake (§5.2 alone was ~26h in the 2026-05 handover).

**So `rm poultry-highland_*.tif` + §3-only would delete the ENSEMBLE/`_int`
stacks and never rebuild them**, breaking R/3 §4.1. Pre-flight passing does not
make the plan safe — it only confirms inputs exist.

## What's actually needed before a poultry partial bake is possible
For the macbook session (owns R/2 control logic; no live data needed to do this):
1. **Locate / restore the ENSEMBLE-crop-stack producer.** Find what writes
   `haz_risk/<crop>_ENSEMBLEmean_<sev>.tif` (+ `_int`). Likely §5.3 (currently
   `run5.3 <- FALSE`) and/or a §3 ensemble sub-step that has been disabled. Until
   this is identified and runnable, NOTHING in `haz_risk` poultry can be safely
   deleted.
2. **Decouple run from overwrite for §5.2/§5.3**, mirroring the existing
   `RUN_R2_RUN3` pattern (line 586): add `RUN_R2_RUN5_2` / `RUN_R2_RUN5_3` env
   toggles so the section runs while `overwrite5.x` stays FALSE (file.exists-
   gated selective regen). The per-file gates at lines 1436/1672 already respect
   `!file.exists`, so only the run/overwrite wiring needs changing.
3. THEN a safe v2 dispatch can: delete poultry artifacts across §3 + §5.2/§5.3
   outputs + R/3 §4 (vop, vop_usd, ha; both axes), run §3 + §5.2 + §5.3
   overwrite-off, then R/3, then publish.

## Alternatives if the surgical path isn't worth the rework
- **Full `FORCE_OVERWRITE=1` R/2 + R/3 bake (both axes)** — the only currently-
  working way to propagate the threshold. Correct but ~a day and re-bakes/
  republishes every product, not just poultry (larger blast radius + review).
- **Defer** the poultry_highland 79→89 fix to the next scheduled full rebake;
  keep issue #13 open with this note.

Recommendation: hand to macbook for steps 1–2 (R/2 control + producer fix), then
re-dispatch a correct v2. Do not attempt a partial until then.

---

## MACBOOK RESPONSE 2026-06-24 — partial correction + toggles added

Good catch on the `_int` / ensemble-consumption gap — agreed the original v1
delete-set was unsafe. But **blocker #1/#2 ("§3 excludes ENSEMBLE → ENSEMBLE
crop stacks have no producer") looks wrong on a code re-read, and the difference
decides whether a partial is even possible:**

- `R/2:942` — the ENSEMBLE-exclusion on §3's inputs is **COMMENTED OUT**:
  `# haz_freq_files<-haz_freq_files[!grepl("ENSEMBLE",haz_freq_files)]`. So
  `haz_freq_files` (941, `list.files(haz_time_risk_dir, ".tif$")`) lists ALL freq
  tifs incl. the §2.1 ENSEMBLE outputs, `models <- unique(model)` (956) then
  includes `ENSEMBLEmean`/`ENSEMBLEsd`, and §3 writes
  `haz_risk/<crop>_ENSEMBLEmean_<sev>.tif` at line 996. ⇒ **§3 IS the producer of
  the ENSEMBLE singles** (when `RUN_R2_RUN3=1`), contradicting "no regen path."
- **The hinge is one live-disk fact I can't see but you can:** are there
  `*ENSEMBLE*` files in `haz_time_risk_dir/<axis>/`? If yes → §3 regenerates the
  ENSEMBLE singles (so deleting+regen them is safe). If §2.1 never wrote them →
  your read holds. Please run:
  `ls $(Rscript -e 'source("R/0_server_setup.R");cat(file.path(atlas_dirs$data_dir$hazard_timeseries_risk,"annual"))') | grep -c ENSEMBLE`

- You ARE right that the **`_int` stacks** come only from §5.3 (`haz_risk/..._int.tif`,
  line 1670) which was hardcoded `run5.3 <- FALSE`. That was the genuine gap.

**DONE this turn (R/2 control, your ask #2):** added run-decoupled toggles,
default-off, overwrite stays FALSE unless `FORCE_OVERWRITE` (so selective,
file.exists-gated regen is possible):
- `RUN_R2_RUN3=1` — §3 crop stacks (already added earlier)
- `RUN_R2_RUN5_2=1` — §5.2 interaction tifs (`run5.2` + `do5.2_main`)
- `RUN_R2_RUN5_3=1` — §5.3 per-crop `_int` stacks ← the missing producer

**v2 partial (once the ENSEMBLE-files live-check passes), scope = vop+vop_usd+ha,
both axes:**
1. delete poultry-highland artifacts in `haz_risk/<axis>/` — BOTH the
   `_ENSEMBLEmean/_ENSEMBLEsd_<sev>.tif` singles AND the `..._int.tif` stacks
   (and per-GCM if §4.1 ever reads non-ensemble — confirm via the §4.1 glob).
2. `RUN_R2_RUN3=1 RUN_R2_RUN5_3=1 SKIP_R2_RUN1=1 SKIP_R2_RUN2=1 SKIP_R2_RUN4=1`
   (FORCE_OVERWRITE unset) → §3 rebuilds the ENSEMBLE singles, §5.3 rebuilds the
   `_int` stacks (reads existing §5.2 combo tifs in `haz_time_int_dir`; if those
   are absent for poultry, add `RUN_R2_RUN5_2=1`). Run for both axes.
3. R/3 §4.1 + §4.2 across vop, vop_usd, ha, both axes; publish; validate.

**Live-data confirmations still needed (you):**
(a) the ENSEMBLE-files count above; (b) exactly what `grep("ENSEMBLE|historic")`
at R/3:944 globs for poultry — singles only, `_int` only, or both; (c) whether
§5.2 combo tifs for poultry exist in `haz_time_int_dir` (decides if `RUN_R2_RUN5_2`
is also needed).

**Honest trade-off:** with scope now = 3 VoP products × 2 axes + §3 + §5.3 + R/3,
the partial's blast radius approaches the full `FORCE_OVERWRITE` bake. If the
live-checks add more moving parts, the **full bake may be operationally simpler**
(one command, ~a day, but no selective-delete risk). Pete's call — flagged.
