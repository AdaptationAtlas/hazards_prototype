# R/2 §5.2 — staged parallelism-grain optimization (Tier D, GATED)

Status: **DESIGNED, NOT APPLIED.** Apply only after the gate below passes on a real
cglabs run. Author: 2026-06-23 (CR-093/R2-optimization review).

## Problem
§5.2 (`run5.2`, interactions) is the only compute-heavy section that survives the
I/O-bound classification — but its parallelism grain is too coarse. The outer
`future.apply::future_lapply` parallelises over `combinations` only
(`R/2_calculate_haz_freq.R:1391`), while the per-model build (`for l in
scenarios_x_models`, 1407-1487) and the ensemble build (`for l in scenarios`,
1494-1544) run **sequentially inside each combination**.

`combinations` = unique `(dry, heat, wet)` hazard triples (R/2:459-464) — inherently
small (bounded by distinct hazard categories, est. ~6-15). `worker_n5.2 = 20`
(R/2:603). So if `nrow(combinations) < 20`, **20 − nrow(combinations) workers sit
idle** for the whole section while each busy worker grinds ~`nrow(scenarios_x_models)`
(~25-30) raster builds serially.

## ROI is count-dependent (why this is gated)
Phase-1 work = `nrow(combinations) × nrow(scenarios_x_models)` raster builds.
- Current makespan ≈ `nrow(scenarios_x_models)` units (each combo-worker serial).
- Flattened makespan ≈ `total / min(20, total)` units.
- If combinations ≈ 15 → ~25% faster (only 5 idle workers recovered).
- If combinations ≈ 8  → ~2.5× faster (12 idle workers recovered).

**Cannot compute `nrow(combinations)` without the real `haz_class` (data-derived).**
So the benefit is unknown until a real run. Restructuring the hottest, most complex
section blind violates [[feedback-validate-real-artifact-hard-gate]].

## GATE — apply only when BOTH hold (from the next Tier-A instrumented run)
1. `.sec2_done(... "5.2) Interactions")` elapsed confirms §5.2 is a material share of
   total wall-clock (if it's <10%, skip — not worth the risk).
2. `nrow(combinations) < worker_n5.2` by a meaningful margin (add a one-line
   `.log2(sprintf("5.2: %d combinations x %d scen_x_models", ...))` to read it).

## Design (two-phase, barrier forced by a real dependency)
The ensemble block (1494-1544) READS the per-model files written by the main block
(`ensemble_files <- file.path(haz_time_int_dir, paste0(scen_mod_time_choice, ...))`,
1508) — so phase 2 must not start until phase 1 for that (combination, scenario) is
on disk. A naive full flatten would break this. Correct shape:

```
# PHASE 1 — parallelise over the PRODUCT (combination x scenarios_x_models)
grid1 <- CJ(ci = seq_len(nrow(combinations_choice)), li = seq_len(nrow(scenarios_x_models)))
future_lapply(seq_len(nrow(grid1)), function(g) {
  i <- grid1$ci[g]; l <- grid1$li[g]
  # rebuild combos + combo_binary PER TASK (cheap; was shared mutable state at 1419 — must be task-local)
  ... existing 1395-1485 body for this (i, l) ...   # writes save_file (disjoint per i,l)
})
# BARRIER (future_lapply returns => all phase-1 files on disk)
# PHASE 2 — parallelise over (combination x scenarios)
grid2 <- CJ(ci = ..., li = seq_len(nrow(scenarios)))
future_lapply(..., function(g) { ... existing 1500-1543 ensemble body ... })  # reads phase-1 files
```

### Safety
- **Output paths disjoint** in both phases: phase-1 `save_file` keyed by
  `scen_mod_time_choice + combos`; phase-2 `save_file_mean/_sd` keyed by
  `scenario + ENSEMBLEmean/sd + time + combos`. No two tasks write the same path
  (satisfies the CR-119 disjoint-path rule, [[feedback-rcpp-future-datatable-lessons]]).
- **`combo_binary` must become task-local** — currently built once per combination
  (1404) then mutated with `lyr_names` per (scen,model) at 1419. In phase 1 each task
  is one (i,l), so build combo_binary inside the task from `combos`. No shared mutation.
- Keep behind a flag `USE_R2_5_2_FLAT` (default OFF) so the default path is the proven
  serial-inner loop until a real run validates the flattened output is identical
  (diff phase-1/phase-2 .tif vs a serial reference run for one combination).

## Already shipped alongside this (validated)
- Tier A: `.log2()` + per-section `.sec2_start/.sec2_done` elapsed + `pboptions(type="none")`.
- §5.2 `haz_sum` terra-vectorization behind `USE_R2_5_2_VEC` (default ON), identity-probed
  by `R/probe_r2_5_2_vec.R` (PASS: values + missingness + downstream any_haz identical).
- Tier B invariant hoists: NONE valid (§3 + §5.3 disabled via run3/run5.3=FALSE;
  §5.2 model_options already hoisted at R/2:1340).
