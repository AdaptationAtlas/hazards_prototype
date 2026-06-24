# ISSUE (CR-093 follow-up) — fix NaN/Inf % at SOURCE in the R/2 rebake

Owner: whoever runs the next **R/2** (`2_calculate_haz_freq.R`) rebake.
Opened: 2026-06-23. Severity: low (masked, not wrong-valued, in published R/2.2 outputs).

## STATUS 2026-06-24 — R/2.2 side CLOSED; root cause DEFERRED to R/2 rebake
- DONE (shipped): R/2.2 NA-cleans every non-finite payload value before write
  (commit 340a775). Re-ran full pipeline on live Data/ — nan=0 across all
  outputs, gate 10/10. Published to canonical domain=climate hazard-change keys
  (commit c9f1e73) and verified live over HTTPS (all 10 reachable + prunable,
  null-stat=0). So nothing non-finite ships today.
- STILL OPEN (this issue's real ask): the two ROOT CAUSES below live in the
  R/2-produced rasters, NOT in R/2.2. Cause 1 in particular is a *correctness*
  bug (desert false-"increase"), not cosmetic. Fix during the next R/2 rebake;
  the R/2.2 guards then become no-ops and stay as cheap defense.
- This issue stays OPEN until the R/2 rebake addresses cause 1 (+ optionally 2).

## Symptom
R/2.2 % / frequency outputs carry ~15–30k non-finite rows per by-model file
(NaN, a few Inf) before cleaning. R/2.2 now NA-cleans them at write
(`x[!is.finite(value), value := NA_real_]`, commit on develop), so published
parquets ship clean NULLs and the gate passes 10/10. That is a **band-aid in the
consumer** — the root cause lives in the R/2 rasters.

## Two distinct root causes (both in R/2-produced rasters)

1. **`100 * d / past` near-zero historic precip (PTOT % change, SEC1).**
   R/2.2 builds the precip-%-change raster as `round(100*(future-past)/past, 1)`.
   In hyper-arid cells `past ≈ 0` → `Inf`/`NaN`. Worse than cosmetic: `classify(>=5 -> 1)`
   turns `Inf` into a *counted* "increase" cell, so a desert going 0 -> trace mm
   reads as a precipitation increase. Examples: EGY New Valley / Al-Kharga Oasis.
   - FIX (source): in R/2, mask cells where the historic baseline precip is below
     a meaningful threshold (decide the threshold — e.g. annual `PTOT < X mm`)
     before the % change is derived, so those cells are NA not Inf. This needs a
     science call on X; that's why it's deferred to the rebake, not patched blind.
   - IMPLEMENTED 2026-06-24 (macbook, R/2.2 SEC1): `past[past < PTOT_BASELINE_MIN_MM] <- NA`
     before the % change, env-overridable, **default now = 100 mm/yr** (was unset).
     Surgical to the change product (R/2's mean raster untouched). Activates on the
     next R/2.2 run; current published outputs unchanged until then.
   - **THRESHOLD DECIDED 2026-06-24: `PTOT_BASELINE_MIN_MM = 100` (mm/yr).**
     Defensible range 50-250 (→50 to keep arid pastoral margins; →200-250 to
     describe only plausibly-cultivable land). Citable rationale (metadata-ready):
     > Relative precipitation change (100 × (future − historic)/historic) is masked
     > where the 20-year historic baseline PTOT < 100 mm/yr. Below this hyper-arid
     > threshold the denominator approaches zero, so trace-level differences within
     > model and observational noise produce spurious large percentage changes;
     > 100 mm/yr also corresponds to the conventional desert isohyet below which
     > rainfed agriculture is absent, so masking removes no decision-relevant signal.
     > This follows operational practice in major climate products (Copernicus C3S
     > masks cells below ~0.3 mm/day ≈ 110 mm/yr) and the UNEP hyper-arid/arid
     > aridity boundary. In masked cells, absolute change (Δmm/yr) is reported instead.
     Sources: UNEP(1992)/UNESCO aridity index (IPCC AR6 WGII CCP3); IPCC AR6 WGI
     SPM Fig SPM.5 (relative-vs-absolute in dry regions + robustness hatching);
     Copernicus C3S 0.3 mm/day mask; World Bank CCKP precip metadata; CMIP6 extremes
     ≥3-events/yr dry mask; FAO Sahelian isohyets (100 mm Saharan / 200 mm cropping).
   - **REFINEMENTS DRAFTED 2026-06-24 (gated, evaluate at rebake — validated on
     synthetic rasters, NOT yet on live Data/):**
     (a) **Compound classify threshold** — `PTOT_DELTA_MIN_MM` (default UNSET = off).
     When set, a cell counts as increase/decrease only if BOTH `|%| ≥ 5` AND
     `|Δmm| ≥ floor`. Targets the 100-200 mm band (±5% of 150 mm = ±7.5 mm, inside
     noise). cglabs evaluates at rebake with e.g. `PTOT_DELTA_MIN_MM=10`.
     (b) **Dual-metric / Δmm-everywhere** — DONE as a correction: `diff` (Δmm) is now
     computed from the UNMASKED baseline, so `ptot_diff_*.parquet` reports full
     coverage incl. deserts (only the `%` uses the masked baseline). ⚠️ This CHANGES
     `ptot_diff` in hyper-arid cells (was NA under the earlier shared-mask, now a
     valid Δmm) — intended (Δmm is the honest dryland metric, IPCC AR6 style), takes
     effect at the rebake.
     (c) model-agreement (≥66-80% sign agreement) robustness flag — NOT drafted;
     optional future enhancement.
   - **Evaluation at rebake:** run R/2.2 once with default (compound off) and once
     with `PTOT_DELTA_MIN_MM=10` (or 15); compare the increase/decrease %-area
     deltas in the arid band before choosing whether to ship the compound cut.
   - **TO ACTIVATE / CLOSE (cglabs, next R/2 rebake):** `Rscript -e
     'source("R/0_server_setup.R"); source(file.path(project_dir,"R","2.2_haz_change.R"))'`
     (default 100 now applies; override `PTOT_BASELINE_MIN_MM=50` etc if science
     revises) → `Rscript R/validate_cr093_real.R` → `CONFIRM=1 Rscript
     R/publish_cr093_r22.R`. Then this issue closes. (If science later prefers masking
     at R/2 source — alters the published mean product too — that's a broader call.)

2. **`x / total` and zonal mean over zero-valid-cell zones (all % + freq).**
   Tiny/islet admin units where `base_rast` (CHIRPS-grid, waterbody-masked) covers
   ~0 cells → `total = 0` → `0/0 = NaN` (SEC1/2/3 % area), or zonal `mean(na.rm=TRUE)`
   over an all-NA zone → `NaN` (SEC4 frequency). Pure zero-denominator, no science
   meaning — those admin units are simply unrepresented on the grid.
   - FIX (source/none): arguably fine to leave as NA (unit not on grid). If desired,
     drop zero-coverage zones from the boundary set, or document that admin units
     below grid resolution carry NULL.

## When this is fixed at source
Once R/2 masks low-baseline precip (cause 1) and zero-coverage zones resolve to
NA upstream, the R/2.2 `!is.finite(...) := NA` guards become no-ops and can stay
as cheap defensive lines. Do NOT remove them until the rebake is verified.

## Pointers
- R/2.2 change raster: `R/2.2_haz_change.R` SEC1 (~`round(100 * d / past, 1)`).
- R/2.2 NA-clean guards: search `is.finite` in `R/2.2_haz_change.R` (4 sites).
- Validator: `R/validate_cr093_real.R` (does NOT check value ranges — add a
  finite-range check here if you want the gate to catch regressions).
