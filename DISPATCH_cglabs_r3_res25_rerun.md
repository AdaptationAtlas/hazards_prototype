# Dispatch: R/3 re-run on the res-25 exposure rasters + hazard_exposure tier republish (#30 follow-up)

**Status:** code on `develop` (this commit and the two before it). Blocks A and B are runnable now.
Block C is the long one. **Block D writes to S3 and is GO-gated** - do not start it without the GO
line in this file.

**Why.** Issue #30 republished the exposure *reference* at both resolutions and regenerated the
exposure *rasters* with the resolution in every name. R/3 (`R/3_freq_x_exposure.R`) now reads only
the rasters tagged for its own grid (`res-25`), but it has not run since. The live hazard product
(`variable=vop_nominal-usd21`, three severity tiers, published 2026-09-16) was built from the 0.05°
0.4.2 crop nominal-USD raster aligned in-flight; that raster is now native 0.25°, so the usd product
will shift slightly (border-cell price assignment). `R/checks/usd_total_vs_reference.R` fails on its
usd side for exactly that reason (intld side 1.000). This dispatch re-bakes R/3 in full (usd + intld +
ha, both timeframes - p.steward 2026-09-26) and republishes the three usd tiers behind a new
value-drift gate.

**What changed in code (read before running):**
- `R3_CROP_VOP_USD` now **defaults to `2021`**. The old default (`2015`) pointed at the legacy
  `spam_vop_usd2015_all.tif`, which was retired into `_pre30_backup/` and deleted, so the old default
  is a guaranteed hard-stop. No env var needed for this run.
- `scripts/r3_publish_tiers.R` gains **G6**, a value-drift gate against the live tier
  (`R/checks/r3_tier_drift_vs_live.R`). G1-G5 never looked at a value. G6 compares admin0 historic
  `any + none` totals per (iso3, crop) in populations: continental per-crop total within 5 %,
  material pairs (live ≥ 1e6 USD) within 25 % with median within 3 %, small/border-heavy countries in
  their own table (50 %), livestock as a "must not move" control (2 %), zero flips, zero unmatched,
  row/scenario parity. It prints in `--dry-run`. `--allow-value-drift` demotes it to WARN; do not
  pass it unless a block here says so.
- `R/checks/probe_r3_res25_preflight.R` - read-only, seconds: resolves every R/3 exposure input the
  way R/3 does and says whether `.align_exposure()` will fire.

**Scoping.** R/3 has no country / tier / variable switch. Scope is by **parking**: move the outputs
to rebuild out of the way (`mv`, never `rm`), run with `FORCE_OVERWRITE` unset, and skip-if-exists
rebuilds only what is missing. Everything parked stays in `Data/_parked_r3_res25_<STAMP>/` until
told otherwise.

**Expectations are invariants, not figures.** Where a block says "count == parked count" or "ratio
essentially 1", that is the gate; a specific number in a log is an example, not a target. If
anything deviates from a stated expectation, **stop at that step and describe what you see.**

---

## Block A - read-only audit (< 5 min, paste everything)

```bash
cd <hazards_prototype> && git fetch origin && git checkout develop && git pull --ff-only && git log -1 --oneline
pgrep -af Rscript || echo "no Rscript running"
env | grep -E '^(FORCE_OVERWRITE|R3_|SKIP_R3|REBAKE_SCENARIO)' || echo "no R/3 env set (good)"
Rscript R/checks/probe_r3_res25_preflight.R
df -h <common_data mount>
```

**Expect:**
- preflight exits 0. Every one of the six inputs "resolves to exactly 1 file" and is `_res-25`
  (crop ha is the untagged native SPAM file - that is correct). Legacy `vop_usd2015_all` absent.
- `compareGeom(Data/base_rast.tif, metadata/base_rast_nexgddp.tif)` - **record the answer**. TRUE:
  R/3 multiplies directly. FALSE: `.align_exposure()` will resample every exposure raster (sum); that
  is allowed, but Block B's log must then show it and must not show `WARN exposure mass not conserved`.
- `_pre30_backup` **absent**. Its deletion was authorised in #30 but never recorded as done. If it is
  still there: **STOP**, report its size and entry count, do not delete it in this block.
- Inventory: paste the table. This is the record of what B and C park.

**STOP.** Paste. Block B may follow immediately if A is clean.

---

## Block B - probe: one tier through the real R/3 path (writes to node Data/, ~1-1.5 h)

Parks only jagermeyr · severe · usd; §4.1 rebuilds that third of the usd tifs, §4.2 builds exactly
one group. Everything else is skip-if-exists.

```bash
cd <hazards_prototype>
export STAMP=$(date +%Y%m%d_%H%M%S); echo $STAMP > logs/r3_res25_stamp.txt
Rscript -e '
  source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")
  stamp <- readLines("/home/jovyan/atlas/hazards_prototype/logs/r3_res25_stamp.txt")
  src <- file.path(atlas_dirs$data_dir$hazard_risk_vop_usd, "jagermeyr")
  dst <- file.path("Data", paste0("_parked_r3_res25_", stamp), "hazard_risk_vop_usd", "jagermeyr"); dir.create(dst, recursive = TRUE)
  f <- c(list.files(src, "_severe_.*_int_.*vop_nominal-usd-2021\\.tif$", full.names = TRUE),
         list.files(src, "^haz-freq-exp_vop_nominal-usd-2021_ENSEMBLE(mean)?_int_adm_severe\\.parquet(\\.json)?$", full.names = TRUE))
  cat("parking", length(f), "files ->", dst, "\n"); print(table(tools::file_ext(f)))
  ok <- file.rename(f, file.path(dst, basename(f))); stopifnot(all(ok))
  cat("remaining severe usd _int tifs in src:", length(list.files(src, "_severe_.*_int_.*vop_nominal-usd-2021\\.tif$")), "(expect 0)\n")'
nohup Rscript -e 'source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); source("/home/jovyan/atlas/hazards_prototype/R/3_freq_x_exposure.R")' \
  > logs/r3_res25_probe_$STAMP.log 2>&1 &
echo $! > logs/r3_res25_probe_$STAMP.pid
```

**T + 5 min - check the header, kill if wrong:**
```bash
grep -E 'Using crop vop usd file|Using crop vop intd file|Using livestock|hazard grid res|overwrite4|R3_CROP_VOP_USD|align|WARN' logs/r3_res25_probe_$STAMP.log | head -30
```
Expect `Using crop vop usd file: spam_vop_nominal-usd-2021_all_res-25.tif | R3_CROP_VOP_USD = 2021`,
every other exposure file `_res-25`, `hazard grid res 0.25x0.25 -> reading exposure rasters tagged res-25`,
`overwrite4= FALSE`, and either no `.align_exposure` lines or exactly the ones Block A predicted, with
**no** `WARN exposure mass not conserved`. Anything else: `kill $(cat logs/r3_res25_probe_$STAMP.pid)`,
un-park (move the files back), paste.

**At exit (wait for `script complete` / process gone):**
```bash
grep -E '4\.1\.1\) .*Complete|4\.2\) Extracting|FAILED|SKIPPED|WARN|Error|complete' logs/r3_res25_probe_$STAMP.log | tail -40
ls -la $(Rscript -e 'source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); cat(file.path(atlas_dirs$data_dir$hazard_risk_vop_usd,"jagermeyr"))' 2>/dev/null)/failed_* 2>/dev/null || echo "no failed_* (good)"
Rscript R/checks/usd_total_vs_reference.R --res 0.25 --severity severe
Rscript scripts/r3_publish_tiers.R --tiers severe --dry-run
```
**Expect (gates):**
- §4.1 usd: number of `_severe_..._int_..._vop_nominal-usd-2021.tif` now in the dir **== number parked**;
  `failed_risk_x_exposure_vop_nominal-usd-2021.txt` absent; the `skipped_not_in_exposure_*` list, if any,
  names only commodities the 2026-09-16 run also skipped. Elapsed in the tens of minutes, not single digits
  (single digits = instant failures swallowed).
- §4.2: `haz-freq-exp_vop_nominal-usd-2021_ENSEMBLEmean_int_adm_severe.parquet` + `.json` mtime > launch;
  sidecar `ensemble.n_members == 18`.
- `usd_total_vs_reference` **usd side PASS** (this is the number the whole item is about; intld unchanged, PASS).
- Publisher dry-run: G1-G5 ok; **G6 table printed** - paste it whole. Expect `total`, `material`,
  `livestock`, `flips`, `unmatched`, `parity` all ok; small-country table populated is fine.

**STOP.** Paste all of it. Do **not** start Block C until macbook has read the G6 table.

---

## Block C - full re-bake (usd + intld + ha, both timeframes; most of a day)

Only after Block B is clean. Parks everything else R/3 §4 writes (the probe's severe usd outputs stay;
they are final artefacts built by the same code from the same inputs).

```bash
cd <hazards_prototype>; export STAMP=$(cat logs/r3_res25_stamp.txt)
Rscript -e '
  source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")
  stamp <- readLines("/home/jovyan/atlas/hazards_prototype/logs/r3_res25_stamp.txt")
  root <- file.path("Data", paste0("_parked_r3_res25_", stamp))
  for (v in c("hazard_risk_vop_usd", "hazard_risk_vop", "hazard_risk_ha")) for (tf in timeframe_choices) {
    src <- file.path(atlas_dirs$data_dir[[v]], tf); if (!dir.exists(src)) next
    dst <- file.path(root, v, tf); dir.create(dst, recursive = TRUE, showWarnings = FALSE)
    f <- list.files(src, "\\.(tif|parquet|json|txt)$", full.names = TRUE)
    if (v == "hazard_risk_vop_usd" && tf == "jagermeyr") f <- f[!grepl("_severe_.*_int_|_int_adm_severe\\.parquet", basename(f))]   # keep the probe outputs
    ok <- file.rename(f, file.path(dst, basename(f))); stopifnot(all(ok))
    cat(sprintf("%-20s %-10s parked %5d | left behind %4d\n", v, tf, length(f), length(list.files(src))))
  }'
nohup Rscript -e 'source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); source("/home/jovyan/atlas/hazards_prototype/R/3_freq_x_exposure.R")' \
  > logs/r3_res25_full_$STAMP.log 2>&1 &
echo $! > logs/r3_res25_full_$STAMP.pid
```
"left behind" must be 0 everywhere except `hazard_risk_vop_usd/jagermeyr` (the probe's severe files).
Header check at T + 5 min exactly as Block B.

**Checkpoints (paste when you look):** `grep -E '4\.1\.1\) .*Complete|4\.2\) Extracting|FAILED|WARN|Error' logs/r3_res25_full_$STAMP.log`.
Per-variable §4.1 elapsed must be in the same order of magnitude as the last honest run (intld ~175 min
per timeframe, usd ~80-115 min); a variable completing in single-digit minutes is the silent-failure
signature and is a **STOP**. `failed_risk_x_exposure_*.txt` must not appear.

**At exit:**
```bash
Rscript R/checks/usd_total_vs_reference.R --res 0.25 --severity severe
Rscript R/checks/usd_total_vs_reference.R --res 0.25 --severity moderate
Rscript scripts/stamp_ensemble_membership.R --dry-run
# intld and ha are not published; gate them new-vs-parked with the same drift check at TIGHT bounds
# (their inputs are renames of the rasters the parked product was built from, so ratios must be essentially 1)
Rscript -e '
  source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); source("/home/jovyan/atlas/hazards_prototype/R/checks/r3_tier_drift_vs_live.R")
  stamp <- readLines("/home/jovyan/atlas/hazards_prototype/logs/r3_res25_stamp.txt"); root <- file.path("Data", paste0("_parked_r3_res25_", stamp))
  for (v in c(hazard_risk_vop = "vop_intld15-2021", hazard_risk_ha = "harv-area_ha")) for (s in c("severe", "moderate", "extreme")) {
    d <- names(v)[1]; fn <- sprintf("haz-freq-exp_%s_ENSEMBLEmean_int_adm_%s.parquet", v, s)
    res <- tier_drift(file.path(atlas_dirs$data_dir[[d]], "jagermeyr", fn), file.path(root, d, "jagermeyr", fn),
                      tol_pair = 0.02, tol_median = 0.01, tol_total = 0.01, tol_small = 0.05, tol_control = 0.02)
    cat(sprintf("\n== %s %s: %s\n", v, s, if (res$pass) "PASS" else "FAIL")); print(res$gates[result == "FAIL"])
    if (!res$pass) print_drift(res, label = fn) }'
Rscript scripts/r3_publish_tiers.R --dry-run
```
**Expect:** both gate sides PASS at both severities; 18 members everywhere; intld/ha drift **PASS at
the tight bounds** (they were not supposed to move - anything wider is a STOP and an explanation, not
a widened bound); publisher dry-run G1-G6 ok for all three tiers, G6 tables pasted.

**STOP. Await GO.**

---

## Block D - publish the three usd tiers (LIVE WRITE - GO-gated)

> GO line goes here, written by macbook after reading Block C's G6 tables.

```bash
cd <hazards_prototype>; export STAMP=$(cat logs/r3_res25_stamp.txt)
Rscript scripts/r3_publish_tiers.R 2>&1 | tee logs/publish_tiers_res25_$STAMP.log     # no --allow-*, no --skip-gates, no --reference
```
Then **verify from S3, not from the uploader** (s3fs may multipart, so ETag is not md5 - re-download):
```bash
Rscript -e '
  suppressPackageStartupMessages({library(arrow); library(dplyr); library(s3fs)}); source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")
  base <- "s3://digital-atlas/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=jagermeyr/model=ENSEMBLEmean/"
  for (s in c("severe","moderate","extreme")) {
    loc <- file.path(atlas_dirs$data_dir$hazard_risk_vop_usd, "jagermeyr", sprintf("haz-freq-exp_vop_nominal-usd-2021_ENSEMBLEmean_int_adm_%s.parquet", s))
    key <- paste0(base, "severity=", s, "/int=multi-hazard.parquet"); tmp <- tempfile(fileext = ".parquet"); s3_file_download(key, tmp)
    sm <- function(f) { d <- open_dataset(f); c(n = nrow(d), s = (d |> summarise(s = sum(value, na.rm = TRUE)) |> collect())$s) }
    a <- sm(loc); b <- sm(tmp)
    cat(sprintf("%-9s md5 %s | rows S3 %d local %d %s | sum(value) %s | sidecar %s\n", s,
      if (tools::md5sum(loc) == tools::md5sum(tmp)) "MATCH" else "MISMATCH", b["n"], a["n"], if (a["n"] == b["n"]) "==" else "!=",
      if (isTRUE(all.equal(a["s"], b["s"]))) "==" else "!=", if (s3_file_exists(paste0(key, ".json"))) "present" else "MISSING")) }'
for s in severe moderate extreme; do
  curl -sI -r 0-99 "https://digital-atlas.s3.amazonaws.com/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=jagermeyr/model=ENSEMBLEmean/severity=$s/int=multi-hazard.parquet" | grep -E '^HTTP|Last-Modified'
  curl -s "https://digital-atlas.s3.amazonaws.com/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=jagermeyr/model=ENSEMBLEmean/severity=$s/int=multi-hazard.parquet.json" | grep -o '"n_members":[0-9]*'; done
```
**Expect:** three `MATCH`, rows and sums `==`, sidecars present, `HTTP/1.1 206` ×3 with today's
`Last-Modified`, `"n_members":18` ×3. Then the CR-068 probes from `atlas_notebooks/scripts/`
(`probe_no_hazard_arithmetic_quick.sh AGO`, `probe_cross_parquet_vop_drift.sh AGO`): every ratio
≤ 100 %, adm0 == Σ adm1, zero NaN.

Paste all of it. Retain `Data/_parked_r3_res25_$STAMP/` and the `sandbox/backup/issue9_<STAMP>/`
objects the publisher wrote until told they can go. The CDH record edit
(`metadata/cdh/africa-hazard-exposure-nexgddp.yaml`, `updated:` + a note sentence) is macbook work.

---

## cglabs response — Blocks A + B run, STOP at G6 (cglabs 2026-09-27)

**STAMP = `20260926_185444`.** Repo at `b34fdaa`. Block C NOT started; nothing written to S3;
parked set intact at `Data/_parked_r3_res25_20260926_185444/`. `DISPATCH_cglabs_family_keys.md` untouched.

### Block A — clean
Preflight green on every substantive gate. The lone process-gate FAIL was two months-old zombie
babysitter shells (pids 2289981/2394838, R/2 jobs long finished, `until ! pgrep -f 2_calculate_haz_freq`
loops matching themselves); killed with your OK, gate then clean. `compareGeom(base_rast,
base_rast_nexgddp) = TRUE` → R/3 multiplies directly. All 6 inputs 1 file, res-25 except crop_ha
(native 0.05, geom FALSE → align predicted). Legacy usd2015 absent, `_pre30_backup` absent.

### Block B mechanics — clean
Launch 18:55 → complete 20:29 (**1h34m**). Header: `overwrite4=FALSE`, all inputs res-25,
`R3_CROP_VOP_USD=2021`, no WARN/Error/mass-not-conserved. jagermeyr vop_nominal-usd-2021 rebuild
**23.6 min** (not the single-digit silent-fail signature). §4.1 severe usd **174 tifs == 174 parked**;
no `failed_*`. §4.2 severe parquet/json mtime 20:06/20:07 > launch. No `.align_exposure` fired (ha was
skip-if-exists, outputs existed, so align never needed).

**Sidecar note (not blocking):** rebuilt severe sidecar has `ensemble.n_members = [0]`, `members = []`.
Not a regression — the parked *live* 2026-09-16 sidecar is `n_members = None`, and node-side moderate/
extreme siblings too. No node-side raw sidecar ever carries 18; that is stamped later by
`scripts/stamp_ensemble_membership.R` and verified on S3. R/3 L1295 writes `length(.ens_members)`,
empty at §4.2. The Block-B expectation "sidecar n_members == 18" cannot hold on raw node output.

`usd_total_vs_reference --res 0.25 --severity severe`: **usd PASS** (median 0.997, range [0.9598,1]),
intld PASS (1.000). But this samples only **AGO/KEN/NGA**.

### Block B GATE — G6 value drift vs LIVE: **FAIL. ABORTED. This is the STOP.**
G1–G5 ok. G6 severe FAIL — large, crop-specific, crop-side only:

| gate | result | detail |
|---|---|---|
| parity rows/vars/scen | ok | 67,952,655 ==; 4==4; scenarios match |
| spread (freq_any+freq_none=1) | **FAIL** | live 1.00 vs local **0.941** |
| unmatched | ok | 0 |
| flips | **FAIL** | 1 — ZWE wheat **112×** |
| total continental/crop (5%) | **FAIL** | 33 crops, ratio [0.659, **3.879**] |
| material pairs ±25% | **FAIL** | 629 pairs, **82 outside**, range [0.419, **112.3**] |
| material median (3%) | ok | median 1 |
| small-country ±50% | **FAIL** | 175 pairs, 15 outside |
| livestock control (2%) | **ok** | range **[1,1]** — untouched |

Worst continental crops: **wheat 3.88×, oilpalm 3.57×, plantain 1.74×**, cowpea 0.66×.
Worst pairs: ZWE wheat 112×, ZMB wheat 22.8×, oilpalm 5–21× across COD/CMR/TZA/MDG/GAB/AGO/CAF/COG.

**Read of it:**
1. Livestock control exactly 1 → multiply mechanics and livestock path fine; the **crop USD exposure** moved.
2. usd_total_vs_reference PASS = rebuilt product is internally consistent with the **new res-25 reference
   exposure**. G6 compares vs the **2026-09-16 live** product. So the res-25 crop-USD exposure raster
   differs from what live was built on — concentrated in wheat/oilpalm/plantain. Points at the **#30
   exposure rebake**, not R/3 logic.
3. The 3-country reference check missed it — worst offenders (ZWE/ZMB/COD/CMR) not in the sampled trio,
   and wheat/oilpalm minor there.
4. The **spread FAIL** (any+none = 0.941 locally) is separate and concerning — possible partition
   integrity issue, not just a value shift. Wants a macbook read.

**Not proceeding to C. Not passing `--allow-value-drift`.** Need your call on the G6 drift
(esp. wheat/oilpalm and the spread invariant) before the full re-bake.
