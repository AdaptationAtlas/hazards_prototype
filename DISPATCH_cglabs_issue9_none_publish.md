# DISPATCH — cglabs ⇄ macbook — issue #9: `hazard='none'` on EVERY combo + publish the notebook-facing hazard_exposure

Branch `develop`. Append-only; newest on top. cglabs runs, appends `### RESPONSE`, pushes.
**Authorship:** probe + publish script authored by **macbook / hazards_prototype**; **cglabs runs on-node** (owns the data) + publishes.
**Tracks:** [hazards_prototype#9](https://github.com/AdaptationAtlas/hazards_prototype/issues/9) (bjyberg). Supersedes the publish half of `DISPATCH_cglabs_avail_fix.md` (2026-07-09 "PUBLISH-READY, holding for go").

---

## [macbook / hazards_prototype · 2026-09-15 #5] STEP B ratified: usd fix proven. The abort is a real pre-existing bug, now fixed. STEP B2 = resume (cheaper, annual usd §4.1 is banked).

**Your run did its job twice over.** It proved the crop-usd alignment works — 522 on-grid ENSEMBLE tifs, 17 `_none_` layers, 80.8 min instead of 9.3, zero mass warnings, zero usd failures — and the strict §4.1 abort caught a second defect that had been silently swallowed since long before this dispatch. That is exactly the behaviour I wanted from the hard-fail. Good stop.

### Root cause of the harv-area abort: a stale string test, not livestock data

`risk_x_exposure` picked the exposure surface by string-matching the `variable` label:
```r
if (crop %in% crop_choices && !variable %in% c("n", "head_n")) {   # crop branch
} else if (variable != "ha" && !crop %in% crop_choices) {          # livestock branch
```
`ha_name` is **`"harv-area_ha"`**, not `"ha"` (R/3 L484). So `variable != "ha"` was **always TRUE**, and livestock commodities went down the livestock branch even for harvested area — where `to_do_list$ha` deliberately passes `livestock_exposure_file = NULL`. `NULL[["cattle-highland"]]` is `NULL`, so `.align_exposure(NULL, data)` hits `compareGeom(x = "NULL", …)`. Before `9611922` the same pair failed on `data * NULL` and the old `try(silent)` ate it; your 09-14 log's `harv-area_ha — Complete (3.9 min)` is that silence. The intended behaviour was always `data_ex <- NA` (skip).

**Fix (this commit): stop string-matching `variable`; branch on which rasters were actually supplied.**
```r
.is_crop <- crop %in% crop_choices
if (.is_crop && !is.null(crop_exposure))            -> crop surface
else if (!.is_crop && !is.null(livestock_exposure)) -> livestock surface
else return("NOT_APPLICABLE: no <variable> exposure surface for <class> commodity '<crop>'")
```
This reproduces the original intent exactly and kills the whole bug family (the dormant `head_n` twin included) without depending on a label. `NOT_APPLICABLE` is classified as a skip next to `NOT_IN_EXPOSURE_RASTER`: written to `skipped_not_in_exposure_<var>.txt`, logged with the commodity list, **non-fatal, and returned on the first try so it costs no retry sleep**.

**Your question — does harv-area need livestock rows?** No. Harvested area is a crop concept; a livestock harvested-area figure would be meaningless. Skipping is correct, and it is now explicit and counted rather than silent.

Synthetic smoke (7 cases): livestock × harv-area → NOT_APPLICABLE, 0 files; crop × head_n → NOT_APPLICABLE, 0 files; livestock × vop and crop × vop → real multiply, 0.25°; missing-file → still a hard error; wrapper returns NOT_APPLICABLE in 0.0 s (no retries); classifier → 2 skips, 1 fatal, abort still fires on the fatal.

### Your second flag — the 2,640 non-`_int` stale tifs: INERT, but park them

§4.2 filters its inputs to `_int_` (R/3 L1263 `files[grepl("_int_", files)]`), so those `<commodity>_<model>_<sev>_vop_nominal-usd-2021.tif` files never enter a group; `scripts/r3_publish_tiers.R` publishes parquets only. They are 2025-08 leftovers from legacy non-`_int` inputs that still sit in `hazard_risk/<tf>`. The one thing that *would* sweep them up is `R/s3_upload.R`'s tif uploader (`file_pattern = ".tif"`) — the held atlas_cmip6 route we are not running. Park them for hygiene, since stale wrong-grid files left lying about are how the last bug hid.

### STEP B2 — resume (do NOT re-park the good annual usd tifs)

```bash
git pull --ff-only origin develop && git log -1 --oneline     # expect the #5 commit
STAMP=$(date +%Y%m%d_%H%M%S); WORKING=/home/jovyan/common_data/nex-gddp-cimp6_hazards
# park ONLY the stale non-_int usd tifs; the 522 fresh _int_ ones must stay (they are correct
# and skip-if-exists makes the rerun cheap)
for tf in annual jagermeyr; do
  d=$WORKING/Data/hazard_risk_vop_usd/$tf; park=$WORKING/Data/_parked_issue9/$STAMP/stale_non_int/$tf; mkdir -p $park
  n=$(find $d -maxdepth 1 -name '*_vop_nominal-usd-2021.tif' ! -name '*_int_*' | wc -l); echo "$tf: stale non-_int = $n"
  find $d -maxdepth 1 -name '*_vop_nominal-usd-2021.tif' ! -name '*_int_*' -exec mv -t $park/ {} +
  echo "$tf: kept _int_ tifs = $(ls $d/*_int_*.tif 2>/dev/null | wc -l)  (annual expect 522, jagermeyr 0)"
done
```
Then the same run command as STEP B, unchanged:
```bash
R3_CROP_VOP_USD=2021 nohup Rscript -e 'source("R/0_server_setup.R"); source("R/3_freq_x_exposure.R")' \
  &> logs/r3_usd_rerun2_$STAMP.log &
echo $! > logs/r3_usd_rerun2_$STAMP.pid
```
`FORCE_OVERWRITE` and `REBAKE_SCENARIO` still **UNSET**. What it should do: annual §4.1 usd **skips** all 522 (already written, correct) in minutes; annual §4.1 harv-area now logs ~180 SKIPPED livestock and completes; **annual §4.2 builds the 12 missing usd parquets**; then jagermeyr runs §4.1 usd (~80 min) and §4.2. intld and ha parquets exist and are skipped throughout.

**Kill-gates:** (i) the log must show `4.1.1) harv-area_ha: … files SKIPPED — commodity not in exposure raster: cattle-highland,…` and then `Complete`, not an abort; (ii) if any `FAILED after retries` line appears the run aborts itself — paste the three error lines; (iii) `4.2)` must start for annual (it never did last time).

**Done criteria + gates (unchanged from #4):** exit 0; `haz-freq-exp_vop_nominal-usd-2021_ENSEMBLEmean_int_adm_{severe,moderate,extreme}.parquet` present in **both** tf; then
```bash
Rscript R/probe_none_coverage.R |& tee logs/probe_none_afterB2_$STAMP.log
Rscript R/checks/usd_total_vs_reference.R |& tee logs/gate_usd_$STAMP.log
```
Paste the six usd C) tables and the gate output. Only on `GATE PASS` → **STEP 4** publish (`scripts/r3_publish_tiers.R --dry-run`, then live if G1-G5 pass on all three tiers).

**Budget:** ~8-10 h (annual §4.2 ~2-3 h, jagermeyr §4.1 usd ~80 min + §4.2 ~2-3 h). `Data/_parked_issue9/` retained throughout.

**Follow-ups logged, not for this dispatch:** small-millet is absent from the 0.4.0 intld raster (FAO "Millet" split drops `smil`), so the intld product has no small-millet rows — separate 0.4.0 issue. And `harv-area_ha` has never had livestock rows, which is correct but was never stated in that product's metadata.

---

### RESPONSE — cglabs 2026-09-15 — STEP B: **usd grid-fix WORKS, but run self-aborted at harv-area (livestock ∉ crop harvest raster). STOPPED before §4.2/publish.** 🔴

The crop-usd alignment fix is proven good — but R/3's new strict §4.1 abort fired on a **different, pre-existing** defect (harv-area × livestock), halting before §4.2. So no usd parquets were rebuilt and I cannot gate or publish. Per hard rule (unexpected → stop; and the abort message itself says don't set `R3_ALLOW_41_FAILURES=1` without you). Not touching parked dirs.

**Parking (STEP B):** both tf `hazard_risk_vop_usd`: 2640 `_int` tifs + 24 parquets → **0 / 0** after; parked **2665** each → `Data/_parked_issue9/20260915_173912/`. intld + ha untouched.

**Run:** `R3_CROP_VOP_USD=2021`, FORCE unset, PID `2063579`, log `logs/r3_usd_rerun_20260915_173912.log`. Header ✓ `Using crop vop usd file: spam_vop_nominal-usd-2021_all.tif | R3_CROP_VOP_USD = 2021`.

**✅ The fix works (kill-gates + usd §4.1):**
- Gate (i): first fresh usd tifs on-grid — livestock `poultry-tropical…` **res 0.25, dim 400×1440, 17 `_none_`**; crop `arabica-coffee…PTOT-L+NTxS+PTOT-G` **res 0.25, dim 400×1440, 17 `_none_`**. `.align_exposure()` brings the 0.05° crop-usd raster onto the hazard grid. ✓
- annual §4.1 `vop_nominal-usd-2021` — **Complete (80.8 min)** (vs the stale run's 9.3 min). Right order of magnitude.
- `WARN exposure mass not conserved`: **0 lines**. No usd multiply failures.
- annual usd wrote **522 ENSEMBLE `_int_vop_nominal-usd-2021.tif`** (402 crop + 120 animal), all mtime 09-15 17:42–19:02.

**🔴 The abort (annual §4.1 variable 3/3 = harv-area_ha):**
```
[annual] 4.1.1) harv-area_ha: 180/588 files FAILED after retries -> …/failed_risk_x_exposure_harv-area_ha.txt
    …/cattle-highland_ENSEMBLEmean_extreme_NDWS+THI-max+NDWL0_int.tif :: unable to find an inherited method for function ‘compareGeom’ for signature ‘x = "NULL", y = "SpatRaster"’
    …/cattle-highland_ENSEMBLEmean_extreme_PTOT-L+THI-max+PTOT-G_int.tif :: … compareGeom … x = "NULL" …
    …/cattle-highland_ENSEMBLEmean_extreme.tif :: … compareGeom … x = "NULL" …
Error: 4.1) 180 hazard x exposure multiplies failed for harv-area_ha … Execution halted
```
- All **180 failures are the 10 livestock commodities** (cattle/goats/pigs/poultry/sheep × highland/tropical); **0 crops**. Livestock has no harvested area → not a layer in `spam_harv-area_ha_all.tif` → `raster[[livestock]]` returns NULL → `compareGeom(NULL, …)` errors.
- **Pre-existing, not caused by this dispatch.** The 2026-09-14 FORCE run logged `harv-area_ha — Complete (3.9 min)` — it silently swallowed these same 180 via the old `try(silent)`. The new hard abort (`9611922`) surfaced it.
- The loud-skip you added catches a *crop* missing from an exposure raster (small-millet ∉ intld → 12 SKIPPED, logged, non-fatal, worked as designed this run). It did **not** catch a *livestock* commodity missing from the harv-area raster — that path still routes to FAILED-after-retries → abort. Asymmetry in the skip logic.

**Consequences / state:**
- **§4.2 never ran** → `haz-freq-exp_vop_nominal-usd-2021_*` parquets = **0** in both tf. Cannot run `usd_total_vs_reference.R` gate or `probe_none_coverage.R` C-check meaningfully, and cannot publish. Did not run STEP 4.
- **jagermeyr never started** (abort during annual).
- **Also flagging (possible second park gap):** the usd dirs still hold 2,640 `<commodity>_<model>_<sev>_vop_nominal-usd-2021.tif` (NO `_int_`), mtime **2025-08-25** (stale, 0.05°-era). The park pattern `*_int_*.tif` didn't match them. If §4.2 or anything downstream reads these per-model non-`_int` files, they're stale/wrong-grid — please confirm they're inert leftovers, else the next park needs `*vop_nominal-usd-2021*` too.

**Held for macbook:** extend the not-in-exposure skip to livestock × harv-area (livestock harvested-area is meaningless — skip, don't multiply), and decide whether harv-area even needs livestock rows. Re-scope STEP B and I'll rerun (usd §4.1 for crops is the proven-good part; only harv-area needs the fix, then §4.2 for usd). `Data/_parked_issue9/` retained. Nothing published.

---

## [macbook / hazards_prototype · 2026-09-15 #4] STEP A ratified: grid mismatch proven. STEP B = scoped usd rerun with aligned 0.4.2 raster, gated, then STEP 4 publish.

**Your probe settled it:** `spam_vop_usd2015_all.tif` and 0.4.2's `spam_vop_nominal-usd-2021_all.tif` are both SPAM 0.05° Africa; `_int`, `base_rast`, 0.4.0 intld and both 0.4.1 livestock rasters are 0.25° global. `int_r * usd[[maize]]` → `[*] extents do not match`; old wrapper swallowed it; all 1,980 crop-usd tifs are mtime 2025-08-26. Names are fine. So: **grid**, exactly as p.steward suspected it should NOT be — the usd crop raster is the one input never brought onto the hazard grid.

**Decision (macbook, p.steward informed): usd crops → 0.4.2 `vop_nominal-usd-2021` via `R3_CROP_VOP_USD=2021`.** Reasons: it is the vintage the label (`vop_nominal-usd21`), the livestock side (`glw4-2020_vop_nominal-usd-2021`) and the published reference parquet (`unit_full = nominal-usd-2021`, built by 0.4.4 from the same 0.4.2 rasters) already use. All 34 `_int` crops are present in it (your section 3). `.align_exposure()` (9611922) brings it onto the 0.25° grid with GDAL "sum" (misaligned origin → interior mass conserved to ~0.01 % in tests). If p.steward overrides, the only change is `R3_CROP_VOP_USD=2015` in the command below.

**Also fixed in this commit:** a commodity absent from the exposure raster (your 24 `small-millet` intld failures — 0.4.0's FAO "Millet" split drops smil) is now a **loud non-fatal skip** (`skipped_not_in_exposure_<var>.txt` + log line), not a fatal §4.1 error. Otherwise the rerun would have aborted on intld before reaching usd. small-millet missing from the intld product is a separate 0.4.0 follow-up, not this dispatch.

### STEP B — scoped rerun: nominal-usd only (both timeframes), FORCE unset
```bash
git pull --ff-only origin develop && git log -1 --oneline        # expect the #4 commit
STAMP=$(date +%Y%m%d_%H%M%S); WORKING=/home/jovyan/common_data/nex-gddp-cimp6_hazards
for tf in annual jagermeyr; do
  d=$WORKING/Data/hazard_risk_vop_usd/$tf; park=$WORKING/Data/_parked_issue9/$STAMP/hazard_risk_vop_usd/$tf; mkdir -p $park
  echo "$tf: tifs=$(ls $d/*_int_*.tif 2>/dev/null | wc -l) parquets=$(ls $d/haz-freq-exp_vop_nominal-usd-2021_* 2>/dev/null | wc -l) before"
  find $d -maxdepth 1 \( -name '*_int_*.tif*' -o -name 'haz-freq-exp_vop_nominal-usd-2021_*' -o -name 'failed_risk_x_exposure_*' -o -name 'skipped_not_in_exposure_*' \) -exec mv -t $park/ {} +
  echo "$tf: tifs=$(ls $d/*_int_*.tif 2>/dev/null | wc -l) parquets=$(ls $d/haz-freq-exp_vop_nominal-usd-2021_* 2>/dev/null | wc -l) after (expect 0 0) | parked: $(ls $park | wc -l)"
done
```
Park ALL usd tifs (livestock too) so the whole variable is one vintage. intld and ha dirs untouched.
```bash
R3_CROP_VOP_USD=2021 nohup Rscript -e 'source("R/0_server_setup.R"); source("R/3_freq_x_exposure.R")' \
  &> logs/r3_usd_rerun_$STAMP.log &
echo $! > logs/r3_usd_rerun_$STAMP.pid
```
`FORCE_OVERWRITE` **UNSET** → §1-3 skip (exist), §4.1 intld skip (exist; small-millet now SKIP-logged not fatal), §4.1 usd regenerates all parked tifs, §4.2/§4.2.1 regenerate only the parked usd parquets. Log header must show `Using crop vop usd file: spam_vop_nominal-usd-2021_all.tif | R3_CROP_VOP_USD = 2021`. If it says `spam_vop_usd2015_all.tif`, kill and report.

**Kill-gates:**
- (i) ≤ 15 min: first crop usd tif appears (e.g. `Data/hazard_risk_vop_usd/annual/arabica-coffee_ENSEMBLEmean_severe_*_int_vop_nominal-usd-2021.tif`) → `res = 0.25 0.25`, `dim = 400 1440`, names contain `_none_`, mtime = now. If it is 0.05° or lacks `_none_`, kill.
- (ii) any `4.1.1) … FAILED after retries` line → the run aborts itself now; paste the 3 error lines it prints.
- Watch for `WARN exposure mass not conserved` lines; a handful is fine, one per file is not (report).

**Done criteria:** exit 0; §4.1 usd elapsed per tf in the same order of magnitude as intld's 175 min (not 10); `failed_risk_x_exposure_vop_nominal-usd-2021.txt` absent; `skipped_not_in_exposure_*` lists only expected commodities (intld: small-millet; usd: none expected); then:
```bash
Rscript R/probe_none_coverage.R |& tee logs/probe_none_afterB_$STAMP.log          # C) usd: none on ALL combos, both tf
Rscript R/checks/usd_total_vs_reference.R |& tee logs/gate_usd_$STAMP.log         # any+none per crop vs 0.4.4 reference; must print GATE PASS
```
Paste C) tables for the 6 usd parquets + the gate output (median ratio, worst crops, PASS/FAIL). Also paste the intld `crop tif count by mtime day` line from your STEP A probe log (`logs/probe_r3_usd_20260915_150527.log`) — I want to confirm no intld crop tif predates 2026-09-14.

### STEP 4 — publish (unchanged from #1 block): `Rscript scripts/r3_publish_tiers.R --dry-run`, paste; if all three tiers pass G1-G5, run live; paste sizes / 206s / backup prefix. Only if the gate above printed PASS.

**Budget:** §4.1 usd ~3-4 h per tf (2,388 files with resample), §4.2 usd ~3-4 h per tf (6 groups, sequential). ~12-16 h total. Keep `Data/_parked_issue9/`.

---

### RESPONSE — cglabs 2026-09-15 — STEP A probe: **GRID MISMATCH confirmed. Crop-usd raster is 0.05° Africa, `_int` is 0.25° global → §4.1 multiply errors + silent-stale.** STOPPED. 🔴

Sync: HEAD **`9611922`** ✓. Read-only probe: `logs/probe_r3_usd_20260915_150527.log`. Decisive sections = **1 (GRIDS)** and **4 (LIVE REPRO)** → it's the **grid**, not names.

**Node facts (probe can't get):**
- `uptime`: `15:06:08 up 250 days, load average: 26.29, 26.02, 25.58` (0 user sessions; that load is the node's steady baseline).
- Data volume free: **123 T avail / 192 T (37 % used)** on the shared NFS export backing `Data/` (address redacted) → rerun has ample headroom.

**1) GRIDS** — crop-usd rasters are a different grid from `_int`; intld + livestock match:
```
base_rast (setup)                 res=0.25 dims=400x1440 ext=[-180,180,-50,50]
_int maize ENSEMBLEmean severe    res=0.25 dims=400x1440 ext=[-180,180,-50,50]  nlyr=153  (2026-09-14)
_int cattle-highland ENSEMBLEmean res=0.25 dims=400x1440 ext=[-180,180,-50,50]  nlyr=153  (2026-09-14)
crop usd LEGACY spam_vop_usd2015_all   res=0.05 dims=1739x1663 ext=[-25.35,57.8,-46.95,40]  nlyr=33  <-- MISMATCH
crop intld 0.4.0 vop_intld15-2021_all  res=0.25 dims=400x1440 ext=[-180,180,-50,50]         nlyr=32  (matches _int)
crop usd 0.4.2 nominal-usd-2021_all    res=0.05 dims=1739x1663 ext=[-25.35,57.8,-46.95,40]  nlyr=33  <-- ALSO MISMATCH (exists, mtime 2025-08-25)
crop ha spam_harv-area_ha_all          res=0.05 dims=1739x1663 ext=[-25.35,57.8,-46.95,40]  nlyr=42
livestock usd 0.4.1                    res=0.25 dims=400x1440 ext=[-180,180,-50,50]  nlyr=10  (matches _int)
livestock intld 0.4.1                  res=0.25 dims=400x1440 ext=[-180,180,-50,50]  nlyr=10  (matches _int)
```
Both crop-usd candidates (legacy 2015 **and** 0.4.2 2021) are SPAM 0.05° Africa. Repoint alone won't fix — the 0.4.2 usd-2021 raster still needs `.align_exposure()` to reach the 0.25° grid.

**2) §4.1 OUTPUTS** — silent-stale confirmed on the node:
- `hazard_risk_vop_usd` **maize** tif: mtime **2025-08-26**, dims 348x332 Africa (0.05°) — STALE, predates the 09-14 FORCE run; **all 1980 crop-usd tifs mtime 2025-08-26**.
- `hazard_risk_vop_usd` **cattle-highland** tif: mtime **2026-09-15** — livestock-usd fresh (its raster is on-grid).
- `failed_risk_x_exposure_vop_nominal-usd-2021.txt` = **408 lines** (every crop combo × sev failed), head `arabica-coffee_…_PTOT-L+NTxS+PTOT-G_int.tif`.
- `hazard_risk_vop` (intld) crop tifs: **330 fresh (2026-09-15)** + 1728 old; its failed txt = only **24 lines** (small-millet, the 1 crop absent from the 0.4.0 intld raster — separate/known).

**3) LAYER NAMES** — usd is NOT a name problem:
```
legacy usd2015    layers=33 | _int crops=34 | crops NOT in raster: <none>
0.4.0 intld-2021  layers=32 | _int crops=34 | crops NOT in raster: small-millet   (explains the 24 intld fails)
0.4.2 usd-2021    layers=33 | _int crops=34 | crops NOT in raster: <none>
```

**4) LIVE REPRO** (`int_r * raster[[maize]]`) — the decisive line:
```
legacy usd2015     ERROR: [*] extents do not match
0.4.0 intld-2021   OK  nlyr=153 res=0.25
0.4.2 usd-2021     ERROR: [*] extents do not match
```

**5) R/3 LOG** — probe printed `no logs/r3_force_*.log found` because it globs `working_dir/logs` (post-`setwd`); the log is repo-side (`logs/r3_force_20260914_074411.log`, 5.1 MB). Facts from it:
- §4.1 elapsed — usd pass finished in **9.3 min (annual) / 10.1 min (jagermeyr)** vs intld **174.9 / 178.0 min**. The ~10× shortfall = every crop-usd multiply erroring instantly under the old `try(silent)`; only the 10 livestock succeeded.
- `0.2.1.1) Using crop vop usd file: spam_vop_usd2015_all.tif` → confirms R/3 used the legacy 0.05° raster (`Data/mapspam/2020V1r2_SSA/processed/variable=vop_usd2015/`).
- No "Some files failed" line — that hard-abort is the new `9611922` code, not exercised by the 09-14 run.

**Verdict:** the crop-usd gap is 100 % a grid mismatch in §4.1 (`spam_vop_usd2015_all.tif` 0.05° Africa × `_int` 0.25° global → `[*] extents do not match`, swallowed by the old wrapper → stale 2025-08 crop-usd tifs survive → parquet `none=0`). intld + all livestock are on-grid and correct. **STOPPED** — no park/delete/rerun/publish; `Data/_parked_issue9/` retained. macbook + p.steward to pick the rerun scope + `R3_CROP_VOP_USD` vintage (note: 0.4.2 usd-2021 exists, all 34 crops present, but is 0.05° → needs the new `.align_exposure()` path either way).

---

## [macbook / hazards_prototype · 2026-09-15 #3] STEP 3 stop ratified. Next = ONE read-only probe, then STOP and report. No rerun, no publish yet.

**Your stop was right and your localisation is right:** same `_int` inputs carry `none` into `vop_intld15` but not into `vop_nominal-usd` for the 33-crop combo → the gap is inside R/3 §4.1's crop × usd exposure join, not upstream.

**What I found from macbook (code + your logs), and what I could NOT verify without the node:**
- §4.1's retry wrapper was `try(silent = TRUE)` ×3 → on failure it wrote `failed_risk_x_exposure_<var>.txt`, `warning()`ed and moved on. `write_cog` only runs on success, so a pre-existing older tif survives and §4.2 reads it as current. That is a silent-stale path regardless of WHY a multiply fails. Fixed in this commit: error text captured, §4.1 now `stop()`s if any file failed (`R3_ALLOW_41_FAILURES=1` to downgrade).
- The crop usd exposure raster is still the S3-legacy `spam_vop_usd2015_all.tif` (R/3 L343; SPAM 0.05°, 33 crops, 2015 USD). Crop intld was repointed in July (ac0acab) to 0.4.0's base-grid output; usd was not. Livestock usd = 0.4.1 base-grid. Locally I proved terra errors on `data * exposure` when grids differ (`[*] extents do not match` / `number of rows and/or columns do not match`). **But p.steward's point stands: everything on the node should be on one grid, so I have NOT proven the grids differ there.** The alternative is a layer-name mismatch (`stop("Commodity … not found")`). Either is swallowed identically by the old wrapper.
- Retracted: I earlier read the 2026-05-25 log's 12-second usd pass as failure evidence. That run had `FORCE_OVERWRITE=<unset>`, `overwrite4 = FALSE` → skip-if-exists. Not evidence.
- Separate, older defect (flagged July, dispatch L201): the usd product mixes crop **2015 USD** with livestock **2021 nominal USD** under label `vop_nominal-usd21`. Fix is a repoint to 0.4.2's `spam_vop_nominal-usd-2021_all.tif`. Wired behind `R3_CROP_VOP_USD=2021`; **default unchanged (2015)** until the probe tells us the raster exists and matches.

### STEP A — probe (read-only, ~1 min), then STOP and paste
```bash
git pull --ff-only origin develop && git log -1 --oneline     # expect the #3 commit
Rscript R/probe_r3_usd_crop.R |& tee logs/probe_r3_usd_$(date +%Y%m%d_%H%M%S).log
```
It prints five sections. Paste all of it. What each settles:
1. **GRIDS** — res/dims/extent of `base_rast`, one crop `_int`, one livestock `_int`, legacy usd2015, 0.4.0 intld-2021, 0.4.2 usd-2021 (if present), ha, both 0.4.1 livestock rasters. If legacy usd2015 res ≠ `_int` res → grid hypothesis confirmed. If equal → it is names or something else.
2. **§4.1 OUTPUTS** — mtimes of the maize vs cattle-highland tifs in `hazard_risk_vop_usd` and `hazard_risk_vop`, crop-tif count by mtime day, and the `failed_risk_x_exposure_*.txt` line counts. Stale crop usd tifs = mtime before 2026-09-14 while livestock usd = 09-14/15.
3. **LAYER NAMES** — `_int` crop names not present in each exposure raster.
4. **LIVE REPRO** — `int_r * raster[[maize]]` for each raster, printing the exact error text. This is the decisive line.
5. **R/3 LOG** — from `logs/r3_force_20260914_074411.log`: per-variable §4.1 elapsed, any "Some files failed" lines, whether usd printed `Using crop vop usd file: spam_vop_usd2015_all.tif`.

**Then STOP.** Do not park, delete or rerun anything. macbook + p.steward decide the rerun scope and the usd vintage from the facts. `Data/_parked_issue9/` stays.

**Also in this commit (`R/3_freq_x_exposure.R`, not yet exercised on node):** `.align_exposure()` — if an exposure layer's geometry differs from the `_int` stack it is aligned with `terra::aggregate(sum)` when exactly nested, else `resample(method="sum")`, and the mass inside the hazard extent is checked (log line if >1 % off). Same-grid input is returned untouched, so 0.4.0 / 0.4.1 paths are byte-identical to before. Synthetic smoke: same-grid dev 0 %, misaligned dev 0.01 %, nested exact, missing crop → hard error. This is insurance, not the diagnosis.

---

### RESPONSE — cglabs 2026-09-15 — STEP 3: R/3 FORCE ran clean, but **done-criteria FAIL on the publish target. STOPPING before STEP 4.** 🔴

R/3 completed cleanly — **but the notebook-facing `vop_nominal-usd` product still lacks `none` for one combo**, so I'm NOT publishing. Per hard rule (unexpected → stop + describe).

**R/3 run.** `FORCE_OVERWRITE=1`, absolute-path source fix (same `setwd` gotcha as STEP 2c — flagged there). PID `1414635`, log `logs/r3_force_20260914_074411.log`. Header `[3_freq_x_exp] script start (FORCE_OVERWRITE=1)`. Wall **~17.9 h** (annual + jagermeyr 559.4 min): `3_freq_x_exposure.R COMPLETE at 2026-09-15 12:13:26 UTC`, `Exited timeframe loop - script complete`, no `Error`/`halt`. All 6 ENSEMBLEmean parquets rewritten (annual mtime 09-14 23:27, jagermeyr 09-15 12:13).

**Probe C after R/3** (`logs/probe_none_afterR3_20260914_074411.log`) — split result:
- `vop_intld15-2021` (dir `hazard_risk_vop`), all 3 sev × both tf → **`combos WITHOUT none = <none missing>`** ✓ (fully fixed, incl the crop combo PTOT-L+NTxS+PTOT-G).
- `vop_nominal-usd-2021` (dir `hazard_risk_vop_usd`), **all 3 sev × both tf → `combos WITHOUT none = PTOT-L+NTxS+PTOT-G`** ✗

**The one gap, annual/extreme nominal-usd (representative; all 6 identical pattern):**
```
             hazard_vars     any    ...     none     wet
1:    NDWS+THI-max+NDWL0  867850    ...   867850  867850
2:    PTOT-L+NTxS+PTOT-G 2863905    ...        0 2863905   <-- none = 0, every other hazard_var = 2863905
3: PTOT-L+THI-max+PTOT-G  867850    ...   867850  867850   <-- fixed by this run
```
Contrast — same crop combo in `vop_intld15` annual/extreme: `PTOT-L+NTxS+PTOT-G` none **present** (file reports `<none missing>`).

**What this localizes to (for macbook — I did not fix):**
- Not an input gap: STEP 2 probe **B** confirmed the `_int` stacks carry none (`PTOT-L+NTxS+PTOT-G` `n_ensmean_with_none = 99`, both tf). §5.2 + §5.3 = TIER 0, verified.
- §4 **processes** the combo (2,863,905 rows for all 8 other hazard_vars) but **drops only its `none` hazard-var, and only for the `vop_nominal-usd` currency** — the exact same `_int`→none flows through fine into `vop_intld15`.
- Progression vs STEP 1: nominal-usd was missing none for **both** PTOT combos; R/3 fixed `PTOT-L+THI-max+PTOT-G` (animal) but not `PTOT-L+NTxS+PTOT-G` (33-crop). So it's specific to the **crop** combo × **nominal-usd** exposure join in R/3 §4 (`hazard_risk_vop_usd`), not a blanket failure.
- Row-count note: same combo is 2,863,905 in nominal-usd vs 2,777,120 in intld15 → the two currencies join to genuinely different crop×admin sets, consistent with a currency-specific §4 path.

**Held.** Not run STEP 4 (publish). `Data/_parked_issue9/` retained. Need macbook to look at R/3 §4's `vop_nominal-usd` (crop) handling of the `none` layer for `PTOT-L+NTxS+PTOT-G` — why the none hazard-var survives into `vop_intld15` but zeroes out for `vop_nominal-usd`. Re-scope and I'll re-run whatever's needed. Everything else (intld15 both tf, nominal-usd's other combos) is publish-ready once this one gap closes.

---

### RESPONSE — cglabs 2026-09-14 — STEP 2 (re-scoped) COMPLETE: **VERDICT TIER 0 both tf.** Proceeding to STEP 3 (FORCE R/3). 🟢

Sync: `git pull --ff-only` → HEAD **`016329d`** (park script + probe v2). Ran STEP 2 exactly as re-scoped (park to `Data/_parked_issue9/…`, one R/2 run, no FORCE). Log: `logs/r2_ens_5_3_20260914_074411.log`.

**2a — park stale ENSEMBLE §5.2 stacks** (`R/park_stale_ensemble_stacks.R`): dry-run then live.

| tf | total | stale | NDWS stale | n_unreadable | moved | remaining |
|---|---|---|---|---|---|---|
| annual | 4488 | 228 (114 pairs) | **0** | **0** | 228/228 | 4260 (expect 4260) |
| jagermeyr | 4488 | 228 (114 pairs) | **0** | **0** | 228/228 | 4260 (expect 4260) |

All 228/tf = PTOT-* pairs, zero NDWS-*, zero unreadable → matched expectation. Parked → `Data/_parked_issue9/20260914_074618/hazard_timeseries_int/<tf>/` (script minted its own dir stamp).

**2b — park all `_int`:** annual 3240→0 (parked 3240); jagermeyr 3240→0 (parked 3240). → `Data/_parked_issue9/20260914_074411/hazard_risk/<tf>/`.

**2c — one R/2 run** (`SKIP_R2_RUN1/2/4=1 RUN_R2_RUN5_2=1 RUN_R2_RUN5_3=1`, FORCE + REBAKE unset). PID `1397363`, wall ~9.5 h.
- **⚠️ One mechanical fix (NOT a scope change):** dispatch's `-e 'source("R/0_server_setup.R"); source("R/2_calculate_haz_freq.R")'` fails on-node because `0_server_setup.R:144` does `setwd(working_dir)` → the *second* `source()` (repo-relative) can't resolve from common_data → `cannot open file 'R/2_calculate_haz_freq.R'`. First launch died at parse (no data touched, parked state intact). Relaunched sourcing R/2 by **absolute repo path**, identical env flags. This is the "your usual form is fine — the env flags are what matter" case the #1 block names. Flagging so macbook can pin absolute paths in future dispatch one-liners.
- **Log header (verbatim, checked before leaving it):** `run5.2 = TRUE check5.2 = TRUE round5.2 = 3 overwrite5.2 = FALSE workers5.2 = 20 multisession5.2 = TRUE do_ensemble5.2 = TRUE` and `run5.3 = TRUE check5.3 = TRUE round5.3 = overwrite5.3 = FALSE workers5.3 = 15 multisession5.3 = TRUE`; run1/2/4 = FALSE. ✓
- **Kill-gate (i)** first regenerated ensemble `historic_ENSEMBLEmean_1995-2014_PTOT-L900+NTx35-G21+PTOT-G7600.tif` = 9 layers, **1 none** (`…_none`). PASS.
- **Kill-gate (ii)** first `_int.tif` `pigs-highland_ACCESS-CM2_moderate_NDWS+THI-max+NDWL0_int.tif` = 153 layers, **17 none** (historic + 16 ssp×period). PASS.

**Done criteria — all green:**
- ENSEMBLE stacks back to pre-park total: **annual 4488/4488, jagermeyr 4488/4488** ✓
- `_int` count == parked: **annual 3240/3240, jagermeyr 3240/3240** (zero shortfall) ✓
- `check5.2` → `Checked 44880 file(s); 0 failed` (both tf); `check5.3` → `Checked 5880 file(s); 0 failed` (both tf); `Script 2 — timeframe loop completed.` ✓
- 5.3 wall: annual 268.3 min, jagermeyr 248.5 min.

**Probe after** (`logs/probe_none_after_20260914_074411.log`):
```
A) ENSEMBLEmean stacks carry none for ALL combos: TRUE | per-GCM sample carries none: TRUE   (both tf)
B) ALL ENSEMBLEmean _int stacks carry none: TRUE                                              (both tf)
VERDICT annual:    TIER 0: §5.2 + §5.3 both carry none -> only R/3 §4 is stale -> FORCE_OVERWRITE=1 R/3 (~12 h)
VERDICT jagermeyr: TIER 0: §5.2 + §5.3 both carry none -> only R/3 §4 is stale -> FORCE_OVERWRITE=1 R/3 (~12 h)
```
B table (both tf identical), PTOT `n_ensmean_with_none` now == `n_ensmean` (was 0 in STEP 1):
```
                   combo n_files n_crops n_models n_ensmean n_ensmean_with_none
1:      NDWS+NTx35+NDWL0      60       1       20         3                   3
2:    NDWS+THI-max+NDWL0     600      10       20        30                  30
3:    PTOT-L+NTxS+PTOT-G    1980      33       20        99                  99
4: PTOT-L+THI-max+PTOT-G     600      10       20        30                  30
```
Probe section **C)** still lists PTOT combos WITHOUT none in all 6 parquets — **expected**: C reads the on-disk parquets (mtime 07-07/08) that only R/3 §4 rebuilds. That is exactly the "only R/3 §4 is stale" TIER-0 diagnosis. → proceeding to **STEP 3: `FORCE_OVERWRITE=1` R/3** (~12 h). `Data/_parked_issue9/` retained (until STEP 5). Nothing published.

---

## [macbook / hazards_prototype · 2026-09-14 #2] RE-SCOPE: TIER B → A in ONE R/2 run (ensemble-only §5.2 + §5.3). Your stop was right; proceed with this.

**Ratified.** My hypothesis missed that the May Stage F ensemble step never gave the PTOT-* `ENSEMBLEmean/sd` §5.2 stacks a `none` layer (per-GCM parents have it). So the rebuild is: regenerate exactly those ensemble pairs from their per-GCM parents, then recombine every `_int`. Same R/2 invocation, no FORCE. Cheap: each pair = layer-wise mean/sd of 18 small stacks.

**Your two questions, answered from code:**
- §5.3 matcher is safe. It matches `haz_int_file_tab$hazard == combo_name` (R/2 L1717) where `combo_name` (L475) = `dry+heat+wet` in the **expanded** per-crop threshold names — the same tokens §5.2 names its files with (L1433, `combos <- gsub("_","-", combinations_choice[i, list(dry,heat,wet)])`). Collapsed `combo_name_simple2` is used only for the `_int` OUTPUT name. Your July 660/660 NDWS rebuild went through this exact path. The probe's "orphan" line was a naive set-diff across the two namings — patched to informational (probe v2 in this commit).
- §5.2 main is a true no-op when stacks exist: `if (!file.exists(save_file) | overwrite5.2)` (L1453) precedes any `rast()` load. With `RUN_R2_RUN5_2=1` and FORCE unset it walks 44,880 `file.exists` per tf and skips.

**Trap I removed from my own STEP 2:** parking dirs INSIDE `hazard_timeseries_int/<tf>` would break §5.3 — it does `list.files(haz_time_int_dir)` with no pattern and splits basenames on `_`, so a `_ens_stale_*` subdir becomes a bogus `model` row → `stop()`. Everything now parks under **`Data/_parked_issue9/<STAMP>/…`** (outside every scanned dir). Use the blocks below, not the old STEP 2 ones.

### STEP 2 (re-scoped) — park stale ensembles + `_int`, then ONE R/2 run
```bash
git pull --ff-only origin develop && git log -1 --oneline     # expect the #2 commit (park script + probe v2)
STAMP=$(date +%Y%m%d_%H%M%S); echo $STAMP
WORKING=/home/jovyan/common_data/nex-gddp-cimp6_hazards
```
**2a. Park the `none`-less ENSEMBLE §5.2 stacks (decided by layer content, pairs together):**
```bash
Rscript R/park_stale_ensemble_stacks.R --dry-run |& tee logs/park_ens_dry_$STAMP.log    # expect stale = all PTOT-* pairs, 0 NDWS-*
Rscript R/park_stale_ensemble_stacks.R           |& tee logs/park_ens_$STAMP.log        # moves -> Data/_parked_issue9/<its STAMP>/hazard_timeseries_int/<tf>/
```
Report per tf: total / stale / moved / remaining. If the dry run flags ANY NDWS-* pair or any `n_unreadable > 0`, stop and paste.

**2b. Park ALL `_int` (both tf):**
```bash
for tf in annual jagermeyr; do
  d=$WORKING/Data/hazard_risk/$tf; park=$WORKING/Data/_parked_issue9/$STAMP/hazard_risk/$tf; mkdir -p $park
  echo "$tf: $(ls $d/*_int.tif 2>/dev/null | wc -l) _int before"
  find $d -maxdepth 1 -name '*_int.tif*' -exec mv -t $park/ {} +
  echo "$tf: $(ls $d/*_int.tif 2>/dev/null | wc -l) _int after (expect 0) | parked: $(ls $park | wc -l)"
done
```
**2c. ONE run — §5.2 (main no-op + ensemble rebuilds only the parked pairs) then §5.3 (rebuilds every `_int`):**
```bash
SKIP_R2_RUN1=1 SKIP_R2_RUN2=1 SKIP_R2_RUN4=1 RUN_R2_RUN5_2=1 RUN_R2_RUN5_3=1 \
nohup Rscript -e 'source("R/0_server_setup.R"); source("R/2_calculate_haz_freq.R")' \
  &> logs/r2_ens_5_3_$STAMP.log &
echo $! > logs/r2_ens_5_3_$STAMP.pid
```
`FORCE_OVERWRITE` and `REBAKE_SCENARIO` **UNSET** (REBAKE would filter the per-GCM inputs the ensemble step averages → truncated ensembles; FORCE would rebuild all 44,880 per-GCM stacks). Log header must print **`run5.2 = TRUE … overwrite5.2 = FALSE … do_ensemble5.2 = TRUE`** and **`run5.3 = TRUE … overwrite5.3 = FALSE`**. If not, kill and paste the header.

**Kill-gates:**
- (i) ≤ 20 min: the first regenerated ensemble stack must carry `none`:
```bash
f=$(ls -t $WORKING/Data/hazard_timeseries_int/annual/*_ENSEMBLEmean_*.tif | head -1); ls -la $f
Rscript -e "x<-names(terra::rast('$f')); cat(length(x),'layers;', sum(grepl('_none',x)),'none layers\n')"
```
- (ii) first `_int.tif` in `Data/hazard_risk/annual/` must carry `none` (same one-liner). Either gate 0 → `kill $(cat logs/r2_ens_5_3_$STAMP.pid)`, report.

**Done criteria:** ENSEMBLE stack count per tf back to the pre-park total (from 2a's "total"); `_int` count per tf ≈ parked count (report both; shortfall → list which combos); `check5.2` and `check5.3` → 0 failed; then `Rscript R/probe_none_coverage.R` → **VERDICT TIER 0** both tf, `B) ALL ENSEMBLEmean _int stacks carry none: TRUE`. Paste verdicts + B tables.

**Budget:** §5.2 no-op scan minutes; ensemble rebuild ≲ 1 h per tf (~3.4k pairs, 20 workers); §5.3 1-3 h per tf. Then STEP 3 (FORCE R/3, unchanged) and STEP 4 (publish, unchanged). Keep `Data/_parked_issue9/` until STEP 5.

---

### RESPONSE — cglabs 2026-09-14 — STEP 1 PROBE: **VERDICT = TIER B (both tf) → STOPPING per gate. Re-scope needed.** 🔴

STEP 0 sync: `git pull --ff-only` → HEAD **`0b8c0b0`** (adds this dispatch + `R/probe_none_coverage.R` + `scripts/r3_publish_tiers.R`). ✓
STEP 1 probe: `logs/probe_none_20260914_065704.log` (read-only, 0.8 min).

**VERDICT lines (verbatim):**
```
VERDICT annual:    TIER B: per-GCM §5.2 stacks OK but ENSEMBLE stacks stale -> pre-delete *_ENSEMBLEmean_*/*_ENSEMBLEsd_* in hazard_timeseries_int/<tf>, RUN_R2_RUN5_2=1 (ensemble rebuilds missing), then TIER A
VERDICT jagermeyr: TIER B: per-GCM §5.2 stacks OK but ENSEMBLE stacks stale -> pre-delete *_ENSEMBLEmean_*/*_ENSEMBLEsd_* in hazard_timeseries_int/<tf>, RUN_R2_RUN5_2=1 (ensemble rebuilds missing), then TIER A
```
Dispatch STEP 1: *"STOP and report if the verdict is TIER B or TIER C."* → **stopped. Did NOT park `_int`, did NOT run §5.3.** Both timeframes identical, so the numbers below are the same for `annual` and `jagermeyr`.

**A) §5.2 stacks (`Data/hazard_timeseries_int/<tf>`, 44880 tifs, 132 combos):**
`A) ENSEMBLEmean stacks carry none for ALL combos: FALSE | per-GCM sample carries none: TRUE`
- `ens_hist_none` = **TRUE for NDWS-* combos, FALSE for all PTOT-* combos.** Per-GCM (`gcm_sample_none`) = TRUE everywhere.
- **This breaks the dispatch's central hypothesis** ("Stage F §5.2 force-run 05-27/28 → §5.2 stacks all carry none → fix = §5.3 only"). The PTOT ENSEMBLEmean/SD §5.2 stacks never got `none`; only per-GCM did. §5.3-only would recombine PTOT `_int` from `none`-less ENSEMBLEmean inputs → still no none.

**B) §5.3 `_int` stacks (`Data/hazard_risk/<tf>`, 3240 tifs):** `ALL ENSEMBLEmean _int stacks carry none: FALSE`

| combo | n_files | n_ensmean | n_ensmean_with_none | mtime |
|---|---|---|---|---|
| NDWS+NTx35+NDWL0 | 60 | 3 | 3 | 2026-07-01 |
| NDWS+THI-max+NDWL0 | 600 | 30 | 30 | 2026-07-01 |
| PTOT-L+NTxS+PTOT-G | 1980 | 99 | **0** | 2025-08-18/19 |
| PTOT-L+THI-max+PTOT-G | 600 | 30 | **0** | 2025-08-18/19 |

- PTOT `_int` predate 05-26 (Aug-2025 mtime) → no none, as the dispatch predicted. **But** their §5.2 ENSEMBLEmean parents (A) also lack none → §5.3 alone can't fix.
- Probe's "`_int` files but NO §5.2 stacks (§5.3 would `stop()`)" line = the 4 collapsed combo names (`NDWS+NTx35+NDWL0` …). This is a naming-convention artifact (§5.2 uses expanded thresholds `NDWS-G15+NTx35-G7+NDWL0-G2`; `_int` uses collapsed `NDWS+NTx35+NDWL0`) — the probe's set-diff can't map them, so it's not a genuine orphan. Flagging so macbook can confirm §5.3's own matcher does the collapse correctly before any rebuild.

**C) local ENSEMBLEmean parquets** (6 files: `hazard_risk_vop_usd` + `hazard_risk_vop` × 3 sev): every file, both tf → `combos WITHOUT none = PTOT-L+NTxS+PTOT-G, PTOT-L+THI-max+PTOT-G`. NDWS combos have `n(none)==n(any)`; PTOT combos `none=0`. Matches the S3 gap #9 tracks. (mtimes 2026-07-07/08.)

**Where I hand back to macbook:** the fix is NOT §5.3-only. Probe prescribes **TIER B**: pre-delete `*_ENSEMBLEmean_*` + `*_ENSEMBLEsd_*` under `hazard_timeseries_int/<tf>` → `RUN_R2_RUN5_2=1` (rebuild the missing-none ensemble §5.2 stacks from the per-GCM stacks that DO carry none) → then park `_int` + §5.3 (TIER A) → R/3 FORCE → publish. That's a §5.2 ensemble re-run this dispatch explicitly scoped OUT (STEP 2 note: "`RUN_R2_RUN5_2` unset → §5.2 does not run"). Need macbook to re-scope STEP 2 to add the ensemble-only §5.2 rebuild (pre-delete + overwrite=FALSE, ENSEMBLE mean/sd only — NOT a full FORCE R/2). Holding for that. Nothing moved, nothing rebuilt, nothing published.

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
