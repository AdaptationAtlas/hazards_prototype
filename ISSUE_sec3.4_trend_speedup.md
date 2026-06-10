# Sec 3.4 trend calc: ~9 h/timeframe → cut several-fold (4 changes, all validation-gated)

**Where:** `R/2.1_create_monthly_haz_tables.R` §3.4 (the `future_lapply` over `file_combos`,
the `data_ex_trend[, {...}, by=.(...)]` block, and `yue_tfpw()`).

**Why slow:** the per-group block runs for >10⁶ groups (admin1 × scenario × model ×
hazard × season), each a short ~20–34-yr series, and makes **three** heavy pure-R
non-parametric calls per group:
1. `trend::sens.slope(value)` inside `yue_tfpw()`
2. `trend::sens.slope(yw)` on the (maybe) pre-whitened series
3. `trend::mk.test(yw)`

Cost is dominated by R-call + `tryCatch` overhead and duplicated O(n²) Kendall work ×
millions of groups — not by per-series size.

Recommendations, ranked by impact × safety. **All must stay numerically identical** —
re-pass `05_trend-validation-reference.py` (currently 4/4) and diff the trend parquet of
one sample `file_combo` (slope/p/ci/lag1_ac within `round3.4`).

---

## 1. Stop recomputing baseline-invariant trends (biggest, low risk)
`file_combos` = futures × **baselines** (+ each baseline's historic). The trend fit acts on
`value`, and `value`+`year` are **identical across baselines** for the same data file.
Only `intercept = median(baseline_value − slope·year)` and the `anomaly_*` stats are
baseline-dependent; **slope, p_value, ci_low/high, lag1_ac, tfpw_applied are not.**

Today the full >10⁶-model fit runs **once per baseline** (e.g. 1981–2014 is processed as
combo 1/7 *and* 7/7 in the current log). With *B* baselines that's ~*B*× the expensive work.

**Fix:** compute the value-trend **once per distinct `data` file** (cache keyed on the data
path), then for each baseline reuse slope/p/ci/AC and recompute only the cheap
baseline-dependent intercept + anomaly stats. Expected: divide §3.4 fit cost by ~#baselines.

## 2. Don't fit Theil–Sen twice when TFPW is skipped (low risk)
`yue_tfpw()` already computes `trend::sens.slope(value)` as `ts0`. When `|r| ≤ 0.1` it
returns `y = value` (TFPW not applied — likely the majority of series), and §3.4 then calls
`sens.slope(value)` **again** on the identical series. Have `yue_tfpw()` return its `ts0`
(slope/intercept/CI) and reuse it when `applied == FALSE`. Saves one full O(n²) Sen fit for
every non-autocorrelated group.

## 3. One Kendall kernel for Sen-CI + MK-p (biggest raw-compute win, medium effort)
`sens.slope()` (slope + CI) and `mk.test()` (p) independently recompute Kendall's S / the
same pairwise comparisons. Replace both with a single function that does one pairwise pass →
Sen slope (median pairwise slope), Kendall S, MK variance/z/p, and Sen CI. Pure-R already
removes one O(n²) pass + one R-call/`tryCatch`; an **Rcpp** kernel (returns a list per group)
removes nearly all per-call overhead — realistically **5–20×** on §3.4. Adds an
`Rcpp`/`RcppArmadillo` build dep; gate behind the validation harness.

## 4. Load-balance the parallelism (low risk, big wall-clock win on many cores)
§3.4 parallelises with `future_lapply` over `file_combos` (~7–14 tasks of *wildly* uneven size
— 60 vs 4340 chunks). On a many-core box most cores idle and the single largest file gates
wall-clock. Options: (a) split each file's groups into chunks and fan those across all workers;
or (b) at minimum order `file_combos` largest-first and raise effective worker use. Combine
with #1 (fewer, dedup'd tasks) for the best balance.

### Minor
- `mk.test()` allocates a full `htest` object per group; a lean p-only calc avoids millions of
  allocations.
- Drop per-group `tryCatch` on the hot path once the kernel is total/NA-safe.

---

## Suggested rollout
1. Land **#2** then **#1** first (numerically identical, no new deps) — validate, measure.
2. Then **#3** (Rcpp kernel) behind the same validation — the largest remaining win.
3. **#4** independently (no numeric change).

Each step: re-run `05_trend-validation-reference.py` + diff one `file_combo`'s
`*_trends.parquet` against the current output before integrating.

---

## STATUS 2026-06-08 — #1 + #2 + #3 DONE (local, uncommitted), #4 skipped

- **#2** (reuse `ts0` when TFPW skipped): in `yue_tfpw()` + kernel path.
- **#1** (baseline-invariant fit dedup): §3.4 now parallelises over distinct source
  `data` file (`source_groups <- split(...)`), loops baselines inside one worker,
  computes value-fit ONCE, recomputes only `intercept` per baseline via `.EACHI`.
  Parallel-write-safe: each worker owns disjoint output paths; `stopifnot(!anyDuplicated)`
  guard added (CR-119 lesson).
- **#3** (Rcpp single-pass Sen+MK kernel, `R/trend_kernel.cpp` → `mk_sen_cpp`/`lag1_ac_cpp`):
  **~63× per fit** (230µs→3.6µs @ n=24). Gated by `USE_TREND_KERNEL`; falls back to
  `trend::` if compile fails or `R21_DISABLE_TREND_KERNEL=1`. Worker-safe: compiled into
  a throwaway env in main (populates shared on-disk Rcpp cache `R/.rcpp_cache`, gitignored),
  each multisession worker `.ensure_kernel()` loads fresh from cache (export-safe: global
  `.kernel_env` stays empty so no dead DLL pointer is serialised).
- **#4** (load-balance / within-source group-chunking): **skipped.** After #3 each combo is
  minutes not hours and the 6 sources already fan across 7 workers; chunking adds corruption
  surface for ~no wall-clock gain. Largest-first ordering moot when #groups ≤ #workers.

**Validation (synthetic, all PASS, all < `round3.4`):**
- `R/probe_trend_kernel_identity.R` — kernel vs `trend::` over 1400 series (max 1.1e-16)
- `R/probe_trend_kernel_yue_identity.R` — full TFPW path, both branches (max 2.3e-13)
- `R/probe_sec3_4_dedup_identity.R` — #1 dedup old-vs-new (max 0.0)
- `R/probe_sec3_4_integration.R` — full §3.4 by-block + `.EACHI` (max 1.1e-16)
- `R/probe_sec3_4_multisession.R` — workers load kernel from cache, 4 PIDs (max 0.0)

**STILL REQUIRED before production (Pete's controlled env, real data):**
1. `05_trend-validation-reference.py` → 4/4 (lives in `atlas_notebooks/.../context/`)
2. Diff one real `*_trends.parquet` combo (kernel vs `trend::` fallback) within `round3.4`
3. Confirm cglabs has a C++ toolchain (else it auto-falls-back to `trend::`, slow but correct)

---

## KNOWN BUG (2026-06-10): multisession §3.4 path is non-functional with the kernel

**Status: the parallel (multisession) §3.4 path is BROKEN and must NOT be used as-is.**
Workaround in place: env flag `R21_SEC3_4_SEQUENTIAL=1` forces `plan(sequential)` (commit
`5f8d97e`). All real runs must set it until the parallel path is fixed.

**Symptom:** with `USE_TREND_KERNEL` on, `future.apply::future_lapply` over `source_groups`
`FutureInterrupt`s **immediately**, every run, even on a healthy node:
```
Warning: Caught FutureInterruptError. Canceling all iterations ...
Error: Future ('future_lapply-1') of class MultisessionFuture interrupted, while running on 'localhost'
```
Zero combos complete. NOT a node/FS issue (reproduces on a healthy node; the single-process
diagnostic `diag_sec3_4_kernel_speed.R` runs fine; the OLD per-combo `trend::`-only multisession
§3.4 completed Jun-5). The delta is the **kernel machinery shipped to workers**.

**Root cause (suspected, not yet definitively pinned — bypassed rather than burn 1-per-run remote
debug cycles):** each worker calls `Rcpp::sourceCpp(..., cacheDir=R/.rcpp_cache, env=.kernel_env)`
to load the compiled `.so`. Leading suspects:
1. **Concurrent in-worker `sourceCpp` on a shared cacheDir** — N workers racing/locking the same
   cache artifacts; one trips → interrupt cascade. (A simplified local 4-worker probe did NOT
   reproduce — warm cache, fewer workers, faster FS — so it's env + full-path specific.)
2. `future` global-export/serialization of `.kernel_env` + the kernel function closures.
3. Worker death during kernel load surfacing as an interrupt.

**DURABLE FIX (do this to restore the parallel speedup):**
- **Package-ify the kernel.** Move `R/trend_kernel.cpp` into a tiny source package (e.g.
  `trendkernel/` with `src/` + `// [[Rcpp::export]]`), `R CMD INSTALL` it once. Then workers just
  `library(trendkernel)` — NO per-worker `sourceCpp`, no shared-cache race, no env serialization.
  This is the standard Rcpp-in-parallel pattern and should make multisession kernel-safe.
- Fallback if not packaging: give each worker a **private cacheDir** (e.g.
  `file.path(tempdir(), paste0("rcpp_", Sys.getpid()))`) so no two workers share cache files,
  OR pre-compile in main and have workers `dyn.load()` the existing `.so` directly instead of
  re-`sourceCpp`.
- Reproduce locally first: pre-warm cache, then 6 workers each `sourceCpp` simultaneously under a
  master `future_lapply`, to confirm which suspect fires before committing the fix.

Until then: **`R21_SEC3_4_SEQUENTIAL=1` is mandatory.** Sequential + kernel ≈ 1.5–2.5h for all 6
sources (vs ~a day on `trend::`), reliable.

---

## Strategy: reproduce + fix the multisession kernel bug LOCALLY (2026-06-10)

Goal: get the parallel §3.4 path working again (currently `R21_SEC3_4_SEQUENTIAL=1`
is the only safe mode). Do it entirely off CGLabs — the bug reproduces from the
*code path*, not the node, so a laptop/local box with R + Rcpp + future suffices.
The earlier local probe (`probe_sec3_4_multisession.R`) did NOT reproduce because it
was too simplified (warm cache, 4 workers, trivial closure). The plan tightens that.

### Phase A — reproduce the FutureInterrupt locally
Build `R/repro_sec3_4_multisession.R` that mirrors the REAL path as closely as possible:
1. Same global setup as §3.4: empty `.kernel_env`, `.ensure_kernel()`, `fit_value_kernel`,
   `USE_TREND_KERNEL`, `kernel_cpp`, `kernel_cache=R/.rcpp_cache`.
2. Synthetic data at realistic scale: ~6 "source groups", each ~0.3–1.5M groups × ~24 yrs
   (use the `diag` generator; n≈24/group). Optionally read the 125M real seasons file if
   migrated (`s3://digital-atlas/scratch/cr119-debug/`).
3. Force the failing config: `plan(multisession, workers=6)`, `future.seed=TRUE`, each
   worker calls `.ensure_kernel()` then `fit_value_kernel` over its group — i.e. concurrent
   `sourceCpp(cacheDir=shared)` across 6 fresh workers.
4. Run it 3× to confirm the interrupt is reproducible.
   - Try **cold cache** (delete `R/.rcpp_cache` first) AND **warm cache** — the cold/concurrent
     case is the prime suspect.

### Phase B — bisect the cause (toggle one variable at a time)
- **B1 private cacheDir:** each worker uses `cacheDir=file.path(tempdir(),paste0("rcpp_",Sys.getpid()))`.
  If the interrupt disappears → confirms suspect #1 (shared-cache `sourceCpp` race).
- **B2 dyn.load instead of sourceCpp:** main compiles once; workers `dyn.load()` the existing
  `.so` + wrap `.Call`, no `sourceCpp`. If clean → it's `sourceCpp`'s in-worker machinery.
- **B3 minimal globals:** strip the closure to just the kernel call (no write_parquet/json) to
  rule out global-export/serialization (suspect #2).

### Phase C — implement + validate the durable fix (package-ify)
1. Scaffold a tiny package `trendkernel/` (DESCRIPTION + `src/trend_kernel.cpp` with the same
   `// [[Rcpp::export]]` fns + `R/` wrappers). `Rcpp::compileAttributes()` then `R CMD INSTALL`.
2. §3.4: replace `.ensure_kernel()`/`sourceCpp` with `requireNamespace("trendkernel")` and call
   `trendkernel::mk_sen_cpp`/`lag1_ac_cpp`. Workers just `library(trendkernel)` — no per-worker
   compile, no shared-cache race, no env serialization.
3. Re-run Phase A harness with the package → must complete multisession, no interrupt, output
   numerically identical to the sequential/`trend::` path (reuse the identity probes).
4. Keep `R21_SEC3_4_SEQUENTIAL` as an escape hatch; keep `USE_TREND_KERNEL`/`R21_DISABLE_TREND_KERNEL`
   gates and the `trend::` fallback if the package isn't installed.

### Acceptance
- Phase A reproduces the interrupt locally (so we're fixing the real thing).
- Phase C: 6-worker multisession run completes locally across ≥3 repeats, results identical to
  sequential within `round3.4`, no `FutureInterruptError`.
- Then deploy to CGLabs (healthy-node pre-flight), parallel on, validate one real combo vs
  `_trendref`, and drop the mandatory `R21_SEC3_4_SEQUENTIAL`.

---

## Progress log 2026-06-10 (sequential rerun + two more bugs)

**Sequential path works.** With `R21_SEC3_4_SEQUENTIAL=1` + kernel, §3.4 completed (exit 0),
all 6 sources, iso3 present in all. First clean full run.

**Bug B — `value_decade` row duplication (FIXED, `728b1b6`).** `data_ex_trend_stats` used
`value_decade = 10 * slope`, but `slope` is the per-ROW merged column → returned a length-n
vector → data.table emitted one (identical) row PER YEAR per group: **~20-34× duplicate rows**
in every `*_trends.parquet` (1995-2014: 31.87M rows for 1.61M groups, ratio 19.7). Pre-existing,
not from the speedup work. Fix: `value_decade = 10 * slope[1]`. Confirmed ratio → 1.0 on the
regenerated file. Also ~halved §3.4 wall-time (234→119 min — writes were the bottleneck) since
20× fewer rows to write. Ensemble outputs were numerically unaffected (melt+agg collapsed dups).

**Timings (post-fix, sequential):** baselines ~6-8 min, futures ~25 min each, total ~119 min.

**⚠️ Bug C — kernel trends have NO slopes (OPEN, BLOCKS REPUBLISH).** The regenerated trends
have **`value_slope` = 100% NA and `value_pval` = 100% NA** (every group). The old `trend::`
ref had slope NA=0%, pval NA=8%. So the Rcpp-kernel path produces NA for every fit column on
REAL data — despite passing all synthetic probes (1e-16) and the integration probe. The non-fit
columns (`value_s5/e5/anomaly/diff`) are fine. Under investigation: isolating whether
`fit_value_kernel` returns NA at the `value_fit` stage (kernel fails on real data) or the bug is
downstream in the `.EACHI`/merge. **DO NOT republish until resolved** — the headline trend
output (slope, p-value) is currently all-NA. (Note: the `_trendref` reference is now only useful
for non-slope columns; for slope/pval the old file actually has values and the new one doesn't.)
