# Dispatch: Stage-0 Phase-2 — validation bake 02→06 (hazards_upstream)

**Status: migration validated on real data (smoke passed). Now run the validation bake.**
Head = origin/develop `fcfb7f0`. Two-session rule: macbook fixes code, cglabs runs (you have real `~/common_data`).

## Context
`hazards_prototype/hazards_upstream/R` = nexgddp index-producer pipeline. Phase-2 migrated stages 01–06 to source shared `R/00_setup.R` (`common_data_root()`, timestamped `.log()`, env run-controls `COMMON_DATA`/`GCMS`/`SCENARIO`/`SSPS`/`YRS`/`PRDS`/`MONTHS`/`FORCE_OVERWRITE`). Hardcoded `~/common_data` + `~/Repositories/hazards` paths removed. Smoke (meta_NDWS.R) passed on real data after the cross-stage `source()` fix; run-controls now honored by 05/06 calc.

## Pull
```bash
cd <hazards_prototype>/hazards_upstream/R
git checkout develop && git pull        # head fcfb7f0; DO NOT create branches (standing rule)
export COMMON_DATA=<your real data root>
```

## Decisions (Pete, 2026-06-25)
1. **Validation bake 02→06 on EXISTING data** — do NOT run 01_download.
2. **Skip CDS** — Atlas moving 01_download to AWS Open Data; 01 out of scope for this bake. 07_bucket_uploads stays deferred.
3. **Run-controls work** — `calc_LongTermStats`/`discreteMaps` honor `GCMS`/`SCENARIO` (commit 63c7362). Unset env = legacy full ensemble (verified byte-identical: gcm_list=6, block-2 stp=75 rows).

## 2a — fast scoped gate FIRST (mandatory before full bake)
Cheap end-to-end proof; all 5 stages exercised via the meta path:
```bash
GCMS=ACCESS-ESM1-5 SCENARIO=historical \
  Rscript 06_metadata/meta_NDWS.R 2>&1 | tee /tmp/bake_2a_meta_ndws.log
# expect: gcm_list = ACCESS-ESM1-5 + ENSEMBLE, block-2 stp = 3 rows -> finishes fast
```
PASS = no path/object errors, completes quickly with the expected scoped row count. If FAIL: capture exact error + file:line, STOP, report back. Do not patch blind.

## 2b — full validation bake 02→06 (only if 2a clean)
Normal full env (unset GCMS/SCENARIO = legacy full ensemble). Run the 02→06 stage scripts in order. Watch per-stage for residual hardcoded paths / missing-object errors. Logs timestamped — note per-stage elapsed.

## Report back (edit this file + commit)
- 2a: pass/fail + did it finish fast (rows/gcm count as expected)?
- 2b: per-stage status + any errors with file:line
- whether outputs landed under `$COMMON_DATA`

Do not push code fixes without flagging the diff first.

---
## Log (newest first)
- 2026-06-25 `fcfb7f0` (macbook) — trimmed CDS credential comment (env-read, CDS retiring).
- 2026-06-25 `63c7362` (macbook) — run-controls fix: 05/06 calc honor GCMS/SCENARIO; meta block-2 `setdiff` + guarded historical row; unset = byte-identical legacy.
- 2026-06-25 `96061c5` (cglabs) — SMOKE RE-RUN PASS: meta_NDWS sourced calc via repo-relative `getOption("hazards.r_root")`, real compute, no path/missing-fn errors. (Hit cglabs 400s test cap mid-compute — not a failure; calc was full-ensemble pre-63c7362.)
- 2026-06-25 `cfc4039` (macbook) — cross-stage source() fix: `00_setup.R` self-locates (`hazards_r_root()`) + stores in `hazards.r_root` option (survives `rm(list=ls())`); 14 meta source siblings via `getOption`.
- 2026-06-25 (cglabs) — SMOKE FAILED: meta_*.R sourced `~/Repositories/hazards` sibling-clone path (absent). 28 refs. Root cause: Phase-2 missed cross-stage source() paths. → fixed in cfc4039.
- earlier — 01–06 migrated to 00_setup.R (`4010c1a`/`058065f`/`a7bac48`/`85c5713`/`acaa0ea`); `5fbb877` dropped `rm(list=ls())` that wiped setup in 4× 02_preprocess.
- Non-blocking: cglabs box `libtiff.so.6: LIBTIFF_4.6.1 not found` GDAL warning on terra load — box env issue, didn't stop the run.
