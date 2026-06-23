# ISSUE (CR-093 follow-up) — fix NaN/Inf % at SOURCE in the R/2 rebake

Owner: whoever runs the next **R/2** (`2_calculate_haz_freq.R`) rebake.
Opened: 2026-06-23. Status: OPEN. Severity: low (cosmetic now; masked, not wrong-valued, in published R/2.2 outputs).

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
