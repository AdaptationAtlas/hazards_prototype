> ⛔ **SMOKE FAILED — STOPPED at Step 1 (cglabs 2026-06-25). Full bake NOT run.**
> `06_metadata/meta_NDWS.R:20` (and `:38`) sources a hardcoded sibling-clone
> path that doesn't exist here:
> ```
> Error: cannot open file
> '/home/jovyan/Repositories/hazards/R/05_final_maps/calc_LongTermStats.R'
> Calls: source -> file   (meta_NDWS.R:20)
> source("~/Repositories/hazards/R/05_final_maps/calc_LongTermStats.R")   # :20
> source("~/Repositories/hazards/R/05_final_maps/calc_discreteMaps.R")    # :38
> ```
> **Scope (systemic, macbook code fix):** ALL 14 `06_metadata/meta_*.R` source
> these two `~/Repositories/hazards/R/05_final_maps/calc_*.R` (28 refs) + 2 in
> `05_final_maps/calc_discreteMaps.R` (already `FIXME(stage0)`-flagged). Phase-2
> migrated `~/common_data` → `common_data_root()` but NOT these
> `~/Repositories/hazards` cross-stage `source()` paths. The targets live in THIS
> repo (`hazards_upstream/R/05_final_maps/`), so the fix is a repo-relative source
> (e.g. a `hazards_root()`/`here`-style helper in `00_setup.R`), not the absolute
> home path.
> **cglabs did NOT patch** (two-session rule: macbook fixes code). Re-dispatch
> after the source paths are migrated; I'll re-run smoke then Step 2.
> Minor/non-blocking: env `libtiff.so.6: LIBTIFF_4.6.1 not found` (GDAL warning)
> on terra load — didn't stop the run; flag for the box's GDAL/libtiff mismatch.

# Dispatch: Verify Stage-0 Phase-2 migration on real data (hazards_upstream 01–06)

## Context
`hazards_prototype/hazards_upstream/R` is the nexgddp index-producer pipeline. Phase-2 migrated stages 01–06 to source a shared `R/00_setup.R` (defines `common_data_root()`, timestamped `.log()`, and env run-controls: `COMMON_DATA`, `GCMS`, `SCENARIO`, `SSPS`, `YRS`, `PRDS`, `MONTHS`, `FORCE_OVERWRITE`). Hardcoded `~/common_data` paths replaced with `common_data_root()`.

Work was done on a local machine (no `~/common_data` data) and verified **only at the sourcing layer**. Your job: verify it runs on **real data**, then do the full bake if smoke passes.

## What's already pushed (origin/develop, up to `beef763`)
- `4010c1a` 01_download_data, `058065f` 02_preprocess_data, `a7bac48` 03_bias_correction, `85c5713` 05_final_maps, `acaa0ea` 06_metadata
- `5fbb877` — **critical fix**: 4× 02_preprocess scripts had `rm(list=ls())` AFTER sourcing setup, wiping helpers. Dropped. The 06_metadata migration was built to avoid this — setup sourced at line ~58, **after** the four `rm(list=ls())` at lines 6/36/51/56.
- `beef763` chore: track AGENTS.md + nexgddp_coverage.csv, gitignore references/

## Pull
```bash
cd <hazards_prototype>/hazards_upstream/R
git checkout develop && git pull        # DO NOT create branches (standing rule)
export COMMON_DATA=<your real data root>
```

## Step 1 — smoke test BEFORE any long run (mandatory)
Cheapest stage that exercises the migration end-to-end on real data:
```bash
GCMS=ACCESS-ESM1-5 SCENARIO=historical Rscript 06_metadata/meta_NDWS.R 2>&1 | tee /tmp/smoke_meta_ndws.log
```
PASS criteria:
- `common_data_root()` resolves to `$COMMON_DATA` (check `.log` lines / paths in output)
- timestamped `.log()` lines appear
- **no** `object 'common_data_root' not found` / `could not find function` (would mean a `rm(list=ls())` wiped setup — the 5fbb877 class of bug)
- no path-not-found on real dirs

If smoke FAILS: capture exact error + offending file:line, STOP, report back. Do not patch blind.

## Step 2 — full run (only if smoke passes)
Run 01→06 in order with your normal full-bake env. 07_bucket_uploads is **deferred** (separate upload-revision project) — do not run or migrate it.

Watch for any residual hardcoded path or missing-object error per stage. Logs are timestamped — note per-stage elapsed.

## Report back
- smoke result (pass/fail + log tail)
- full-run per-stage status + any errors with file:line
- whether outputs landed under `$COMMON_DATA` as expected

Do not push fixes without flagging the diff first.
