# Stage-0 Phase-2 rollout — shared infra (`00_setup.R`)

Phase 2 of the `UPGRADE_hazards_upstream.md` roadmap (ranks 3,4,5): one sourced
helper for logging + env run-controls + data-root resolution, dropping `warn=-1`
and `rm(list=ls())` across all stages. Started 2026-06-24 (macbook = code/docs).

## What landed
- **`R/00_setup.R`** — base-R-only shared helper. Source at the top of every script:
  ```r
  local({
    cargs <- commandArgs(FALSE)
    fa <- grep("^--file=", cargs, value = TRUE)
    base <- if (length(fa)) dirname(normalizePath(sub("^--file=", "", fa[1]))) else getwd()
    cand <- c(file.path(base, "..", "00_setup.R"), file.path(base, "00_setup.R"),
              "../00_setup.R", "00_setup.R")
    hit <- cand[file.exists(cand)][1]
    if (is.na(hit)) stop("00_setup.R not found from ", base)
    source(normalizePath(hit), local = FALSE)
  })
  ```
  Provides: `common_data_root()`, `.log()/.log_reset()`, `env_flag()/env_or()`,
  `parse_yrs()`, `cfg_gcms/ssps/scenario/yrs/prds/months()`, `should_skip()`,
  `ATLAS_GCMS` (18), `ATLAS_GCMS_BC` (5), `ATLAS_SSPS_FUTURE`, `ATLAS_PRDS`.
  Unit-tested locally (logging format + elapsed, env truthy parse, yrs range/csv,
  config defaults + overrides, should_skip gate) — all assertions pass.
- **Pilot migrated: `R/04_indices/fast_calc_NDWS.R`** — sources setup; `root <-
  common_data_root()`; CHIRPS mask via `file.path(root, ...)`; `warn=-1` dropped;
  run-config block → `cfg_scenario/ssps/yrs/gcms`; `if(!file.exists)` → `should_skip()`
  (honours `FORCE_OVERWRITE=1`); progress `cat()` → `.log()`. Parse + resolver +
  config-eval validated locally. **Behaviour-preserving by default** (see below).

## Behaviour-preservation contract (why this is safe to roll out)
- `COMMON_DATA` default = `~/common_data`; on cglabs `~` == `/home/jovyan`, so
  `common_data_root()` == the legacy literal `/home/jovyan/common_data` byte-for-byte.
- `cfg_yrs(scenario, historical=, future=)` takes **per-call defaults** — each script
  passes its own legacy literal, so no silent window change. fast_calc keeps `1981:1994`.
  (Baseline pass = `YRS=1995:2014`, per the documented CMIP6 baseline.)
- `cfg_gcms()` default == the verbatim 18-GCM vector.
- Setup sets `scipen=999` only; **warnings now surface** (legacy `warn=-1` intentionally
  dropped) — this is the one deliberate behaviour change; watch the first run for newly
  visible warnings.

## Per-script migration recipe (apply uniformly)
1. Replace the `options(warn=-1, scipen=999)` / `rm(list=ls())` / `g <- gc(...)`
   preamble with the `local({...})` setup-source block above. Keep `pacman::p_load(...)`.
2. `root <- '<.../common_data>'` → `root <- common_data_root()`.
3. Every hardcoded `'/home/jovyan/common_data/...'` or `'~/common_data/...'` literal →
   `file.path(root, '...')`.
4. Inline 18-GCM vector / scenario / yrs / ssps / prds → `cfg_*()` with the script's
   own legacy literal as the default arg (preserve behaviour).
5. `if (!file.exists(outfile))` output guards → `if (!should_skip(outfile))`.
6. Progress `cat(...)` → `.log(...)`.
7. Keep the Phase-1 `stopifnot(length(...)>0)` gates already added.

## Migration status
- **04_indices — DONE** (commit `c5b5f06`): all 13 scripts migrated (NDWS pilot +
  NDWL0/50 + 9 calc_* + QAQC). Parse-clean, no residual hardcoded roots.
  fast_calc AVAIL lexical-last lookup deliberately untouched (Phase 4 / hazards#19).
  calc_NTx kept its vectorized `[!file.exists(outfile)]` skip; QAQC kept inline QA
  config (BC-subset GCMs / 4-period prds).

## Remaining migration (queued — NOT done)
| stage | scripts | notes |
|---|---|---|
| 01_download_data | 7 | heterogeneous (JRV `library()` vs HAE pacman); `download_AgERA5.R` already de-hardcoded CDS key — keep |
| 02_preprocess_data | 14 | incl. the NEX-GDDP converter pair (pr2 unit bug = rank 6, Phase 3) |
| 03_bias_correction | 5 | `identifyCorruptedFiles.R` already OR→AND + SD-guard fixed (Phase 1) |
| 05_final_maps | 3 | sources sibling `~/Repositories/hazards/R/05_final_maps/*` — fix those self-refs to relative paths |
| 06_metadata | 14 | rank 7 says templatize to driver+config (Phase 3); a thin setup-source is still worth adding now |
| 07_bucket_uploads | 2 | **STALE/DEFERRED** — do not migrate (separate upload-revision project) |

## GATE before any long run (cglabs — REQUIRED)
Per roadmap sequencing step 2, validate the migration at RUNTIME on live Data/ at the
smallest scope (one GCM, one month = 1995-01, the historical AVAIL seed month — no
prior-AVAIL dependency). A self-checking harness does this and asserts every PASS
criterion, exiting non-zero on any failure:
```bash
bash hazards_upstream/R/04_indices/gate_phase2_ndws.sh   # default GCM ACCESS-ESM1-5 (confirmed present)
# override with another present GCM if needed:
GATE_GCM=MPI-ESM1-2-HR bash hazards_upstream/R/04_indices/gate_phase2_ndws.sh
```
The single-month scope is enabled by the new `MONTHS` run-control (`cfg_months()` in
00_setup.R; the harness sets `MONTHS=1 YRS=1995:1995 GCMS=<one>`). The harness checks:
1. timestamped `.log` run-config marker with `n_gcms=1`;
2. writes `NDWS-1995-01.tif` + `AVAIL-1995-01.tif`;
3. re-run without `FORCE_OVERWRITE` SKIPS (should_skip); with `FORCE_OVERWRITE=1`
   RECOMPUTES and overwrites with no "file exists" abort (overwrite=TRUE);
4. a no-input year (1850) FAILS LOUD via the Phase-1 `stopifnot` (non-zero exit).

**Exit 0 = PASS → safe to sweep 01/02/03/05/06.** Non-zero → stop and inspect the log.
