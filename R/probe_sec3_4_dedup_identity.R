# Numerical-identity probe for §3.4 Speedup #1 (baseline-invariant fit dedup).
# Asserts the NEW path (compute value-fit once per source, reuse across baselines,
# recompute only intercept via .EACHI) is numerically IDENTICAL to the OLD path
# (full per-(combo,baseline) fit incl. baseline_name in the by-key) on synthetic
# multi-baseline data where value/year are shared and only baseline_value differs.
# Runs in <2s, no pipeline deps. Exit 0 = identical.
suppressMessages({ library(data.table); library(trend) })

# --- yue_tfpw: copied verbatim from R/2.1_create_monthly_haz_tables.R lines 956-972 ---
yue_tfpw <- function(year, value, threshold = 0.1) {
  n <- length(value)
  if (n < 4L || !all(is.finite(value))) return(list(y = value, applied = FALSE, r = NA_real_))
  ts0 <- tryCatch(trend::sens.slope(value), error = function(e) NULL)
  if (is.null(ts0)) return(list(y = value, applied = FALSE, r = NA_real_))
  slope0     <- unname(ts0$estimates)
  intercept0 <- median(value - slope0 * year, na.rm = TRUE)
  detr       <- value - (slope0 * year + intercept0)
  d          <- detr - mean(detr, na.rm = TRUE)
  denom      <- sum(d * d, na.rm = TRUE)
  r          <- if (denom > 0) sum(d[-n] * d[-1L], na.rm = TRUE) / denom else 0.0
  if (abs(r) <= threshold) return(list(y = value, applied = FALSE, r = r, ts0 = ts0))
  wr <- c(detr[1L], detr[-1L] - r * detr[-n])
  list(y = wr + slope0 * year + intercept0, applied = TRUE, r = r, ts0 = NULL)
}

fit_keys <- c("admin0_name", "admin1_name", "scenario", "timeframe", "model", "hazard", "season")

# --- synthetic source: value/year identical across baselines; baseline_value differs ---
set.seed(42)
base_grid <- CJ(
  admin0_name = "Angola", admin1_name = c("R1", "R2"),
  scenario = c("ssp245", "ssp585"), timeframe = "2030s",
  model = c("m1", "m2"), hazard = c("PTOT", "TMAX"), season = c("DJF", "annual"),
  year = 2021:2044
)
# value depends ONLY on group identity + year (NOT baseline) — autocorrelation in some groups
base_grid[, gid := .GRP, by = fit_keys]
base_grid[, value := 50 + gid * 2 + 0.4 * (year - 2021) +
            ifelse(gid %% 3 == 0, cumsum(rnorm(.N, 0, 1)), rnorm(.N, 0, 3)), by = fit_keys]
base_grid[, anomaly := value - 60]

make_baseline <- function(dt, bl_name, bl_offset) {
  d <- copy(dt)
  d[, baseline_name := bl_name]
  # baseline_value differs per baseline → only intercept/anomaly should change
  d[, baseline_value := 60 + bl_offset + 0.1 * gid]
  d[]
}
b1 <- make_baseline(base_grid, "1981-2014", 0)
b2 <- make_baseline(base_grid, "1995-2014", 5)
baseline_files <- list(b1, b2)

# ---------- OLD path: full fit per (combo,baseline), baseline_name in by-key ----------
old_one <- function(d) {
  d[, {
    pw <- yue_tfpw(year, value); yw <- pw$y
    ts <- if (!pw$applied && !is.null(pw$ts0)) pw$ts0 else tryCatch(sens.slope(yw), error = function(e) NULL)
    if (is.null(ts)) list(slope=NA_real_, intercept=NA_real_, ci_low=NA_real_, ci_high=NA_real_,
                          p_value=NA_real_, tfpw_applied=FALSE, lag1_ac=pw$r)
    else { m <- unname(ts$estimates); intercept <- median(baseline_value - m * year)
      list(slope=m, intercept=intercept, ci_low=ts$conf.int[1], ci_high=ts$conf.int[2],
           p_value=tryCatch(mk.test(yw)$p.value, error=function(e) NA_real_),
           tfpw_applied=pw$applied, lag1_ac=pw$r) }
  }, by = c(fit_keys, "baseline_name")]
}
old_res <- rbindlist(lapply(baseline_files, old_one))

# ---------- NEW path: value-fit once, reuse + .EACHI intercept per baseline ----------
value_fit <- NULL
new_list <- list()
for (k in seq_along(baseline_files)) {
  d <- baseline_files[[k]]
  if (is.null(value_fit)) {
    value_fit <- d[, {
      pw <- yue_tfpw(year, value); yw <- pw$y
      ts <- if (!pw$applied && !is.null(pw$ts0)) pw$ts0 else tryCatch(sens.slope(yw), error = function(e) NULL)
      if (is.null(ts)) list(slope=NA_real_, ci_low=NA_real_, ci_high=NA_real_,
                            p_value=NA_real_, tfpw_applied=FALSE, lag1_ac=pw$r)
      else list(slope=unname(ts$estimates), ci_low=ts$conf.int[1], ci_high=ts$conf.int[2],
                p_value=tryCatch(mk.test(yw)$p.value, error=function(e) NA_real_),
                tfpw_applied=pw$applied, lag1_ac=pw$r)
    }, by = fit_keys]
  }
  ts_new <- d[value_fit, on = fit_keys,
    .(slope=i.slope, intercept=median(baseline_value - i.slope * year),
      ci_low=i.ci_low, ci_high=i.ci_high, p_value=i.p_value,
      tfpw_applied=i.tfpw_applied, lag1_ac=i.lag1_ac),
    by = .EACHI]
  ts_new[, baseline_name := baseline_files[[k]]$baseline_name[1]]
  new_list[[k]] <- ts_new
}
new_res <- rbindlist(new_list)

# ---------- compare ----------
cols <- c(fit_keys, "baseline_name", "slope", "intercept", "ci_low", "ci_high",
          "p_value", "tfpw_applied", "lag1_ac")
setkeyv(old_res, c(fit_keys, "baseline_name"))
setkeyv(new_res, c(fit_keys, "baseline_name"))
old_res <- old_res[, ..cols]; new_res <- new_res[, ..cols]

stopifnot(nrow(old_res) == nrow(new_res))
num_cols <- c("slope", "intercept", "ci_low", "ci_high", "p_value", "lag1_ac")
maxdiff <- 0
for (cc in num_cols) {
  dd <- max(abs(old_res[[cc]] - new_res[[cc]]), na.rm = TRUE)
  na_mismatch <- sum(is.na(old_res[[cc]]) != is.na(new_res[[cc]]))
  cat(sprintf("  %-12s max|diff|=%.3e  NA-mismatch=%d\n", cc, dd, na_mismatch))
  if (na_mismatch > 0) stop(sprintf("FAIL: NA pattern differs in %s", cc))
  maxdiff <- max(maxdiff, dd)
}
if (!identical(old_res$tfpw_applied, new_res$tfpw_applied)) stop("FAIL: tfpw_applied differs")

cat(sprintf("groups=%d  baselines=%d  rows=%d\n", uniqueN(base_grid$gid), 2L, nrow(new_res)))
# sanity: intercept SHOULD differ between baselines (proves intercept is baseline-dependent)
ic_spread <- new_res[, .(d = diff(range(intercept))), by = fit_keys][, max(d, na.rm = TRUE)]
cat(sprintf("max intercept spread across baselines = %.4f (expect > 0)\n", ic_spread))
stopifnot(ic_spread > 0)

if (maxdiff < 1e-9) cat(sprintf("PASS: new==old, max numeric diff %.2e; intercept correctly baseline-dependent\n", maxdiff)) else
  stop(sprintf("FAIL: max numeric diff %.3e exceeds 1e-9", maxdiff))
