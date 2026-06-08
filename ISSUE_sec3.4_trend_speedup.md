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
