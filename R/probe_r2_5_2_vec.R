# probe_r2_5_2_vec.R — numerical-identity probe for the §5.2 haz_sum
# vectorization (R/2_calculate_haz_freq.R). Confirms the new direct multi-layer
# terra add equals the old per-layer lapply-sum, cell-for-cell incl. NA, across
# multi-layer rasters. Pure terra — no Data/, runs locally.
#
#   Rscript R/probe_r2_5_2_vec.R   # expect "PROBE PASS"
suppressPackageStartupMessages(library(terra))
set.seed(1)

mk <- function(nl, na_frac = 0.1) {
  r <- rast(nrows = 20, ncols = 25, nlyrs = nl)
  v <- sample(0:1, ncell(r) * nl, replace = TRUE)          # classified 0/1
  v[sample(length(v), floor(length(v) * na_frac))] <- NA   # inject NA
  values(r) <- v
  r
}

# Functional equivalence (element-wise, attribute-insensitive). NOTE: terra's
# sum() emits an inconsistent missing sentinel (NaN for some NA inputs, NA for
# others) while `+` always gives NA — both are is.na()-missing and both are
# treated identically downstream (haz_sum >= 1 -> NA either way; mean(na.rm=TRUE)
# drops both), so we compare missingness via is.na(), not the NaN/NA sentinel.
ok <- TRUE
for (nl in c(1L, 3L, 12L, 24L)) {
  dry  <- mk(nl)
  heat <- mk(nl) * 10            # mirror the *10 applied in §5.2
  wet  <- mk(nl) * 100           # mirror the *100 applied in §5.2
  haz  <- list(dry = dry, heat = heat, wet = wet)

  # OLD kernel
  old <- terra::rast(lapply(1:nlyr(haz[[1]]), FUN = function(m) {
    sum(terra::rast(lapply(haz, "[[", m)))
  }))
  # NEW kernel
  new <- haz[["dry"]] + haz[["heat"]] + haz[["wet"]]

  ov <- values(old); nv <- values(new)
  same_dim  <- all(dim(old) == dim(new))
  same_na   <- sum(is.na(ov) != is.na(nv)) == 0                       # element-wise
  same_val  <- sum(ov[!is.na(ov)] != nv[!is.na(nv)]) == 0             # exact, no tol

  # Downstream check: any_haz (the value §5.2 actually derives from haz_sum).
  any_old <- terra::ifel(old >= 1 & old <= 999999, 1, 0)
  any_new <- terra::ifel(new >= 1 & new <= 999999, 1, 0)
  ao <- values(any_old); an <- values(any_new)
  same_any <- sum(is.na(ao) != is.na(an)) == 0 && sum(ao[!is.na(ao)] != an[!is.na(an)]) == 0

  cat(sprintf("nl=%2d  dim-ok=%s  na-pos=%s  val=%s  any_haz=%s\n",
              nl, same_dim, same_na, same_val, same_any))
  if (!(same_dim && same_na && same_val && same_any)) ok <- FALSE
}

if (ok) cat("haz_sum PASS — functionally identical (values + missingness + any_haz)\n") else {
  cat("haz_sum FAIL — kernels diverge; do NOT enable USE_R2_5_2_VEC\n"); quit(status = 1)
}

# --- §5.2 ensemble mean/sd ACROSS models (per layer) ---------------------------
# OLD: per-layer loop stacking the j-th layer across models -> mean / app(sd).
# NEW: do.call(terra::mean, ...) + do.call(terra::stdev, pop=FALSE) elementwise
# across model rasters, preserving layers. pop=FALSE == base sd (sample, n-1).
ens_ok <- TRUE
for (nl in c(1L, 3L, 12L)) {
  stk <- lapply(seq_len(5L), function(m) mk(nl))                  # 5 model rasters
  old_mean <- terra::rast(lapply(1:nlyr(stk[[1]]), function(j) mean(terra::rast(lapply(stk, "[[", j)), na.rm = TRUE)))
  old_sd   <- terra::rast(lapply(1:nlyr(stk[[1]]), function(j) terra::app(terra::rast(lapply(stk, "[[", j)), fun = sd, na.rm = TRUE)))
  new_mean <- do.call(terra::mean, c(stk, list(na.rm = TRUE)))
  new_sd   <- do.call(terra::stdev, c(stk, list(pop = FALSE, na.rm = TRUE)))
  dm <- max(abs(values(old_mean) - values(new_mean)), na.rm = TRUE)
  ds <- max(abs(values(old_sd) - values(new_sd)), na.rm = TRUE)
  na_m <- sum(is.na(values(old_mean)) != is.na(values(new_mean)))
  na_s <- sum(is.na(values(old_sd)) != is.na(values(new_sd)))
  cat(sprintf("ensemble nl=%2d  mean|d|=%.3g na=%d | sd|d|=%.3g na=%d\n", nl, dm, na_m, ds, na_s))
  if (!(dm == 0 && ds == 0 && na_m == 0 && na_s == 0)) ens_ok <- FALSE
}
if (ens_ok) cat("ensemble PASS — terra::mean/stdev identical to per-layer loop\n") else {
  cat("ensemble FAIL — do NOT enable USE_R2_5_2_VEC for ensemble\n"); quit(status = 1)
}

cat("PROBE PASS — all §5.2 vectorizations functionally identical\n")
