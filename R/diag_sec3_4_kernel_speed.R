# Bounded diagnostic: is the §3.4 kernel value_fit fast enough at real scale, and
# does it crash on real data? Single process, no futures, subsamples to stay short.
# Run: Rscript R/diag_sec3_4_kernel_speed.R
suppressMessages({ library(data.table); library(arrow); library(Rcpp) })
ts <- function() format(Sys.time(), "%H:%M:%S")

D <- "/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/hazard_timeseries_mean_month"
# smallest baseline seasons file (3.2 output that §3.4 reads)
f <- file.path(D, "haz_3months_adm_mean_1995-2014_anomaly-historic_seasons.parquet")

cat(sprintf("[%s] compiling kernel (cache hit expected) ...\n", ts()))
.kernel_env <- new.env(parent = baseenv())
suppressMessages(Rcpp::sourceCpp("R/trend_kernel.cpp", cacheDir = "R/.rcpp_cache", env = .kernel_env))

fit_value_kernel <- function(year, value) {
  n <- length(value)
  if (n < 4L || !all(is.finite(value)))
    return(list(slope=NA_real_, ci_low=NA_real_, ci_high=NA_real_, p_value=NA_real_, tfpw_applied=FALSE, lag1_ac=NA_real_))
  ts0 <- .kernel_env$mk_sen_cpp(value); slope0 <- ts0$slope
  intercept0 <- median(value - slope0 * year, na.rm = TRUE)
  detr <- value - (slope0 * year + intercept0); r <- .kernel_env$lag1_ac_cpp(detr)
  if (abs(r) <= 0.1) return(list(slope=ts0$slope, ci_low=ts0$ci_low, ci_high=ts0$ci_high, p_value=ts0$p_value, tfpw_applied=FALSE, lag1_ac=r))
  wr <- c(detr[1L], detr[-1L] - r * detr[-n]); z <- wr + slope0 * year + intercept0
  tsz <- .kernel_env$mk_sen_cpp(z)
  list(slope=tsz$slope, ci_low=tsz$ci_low, ci_high=tsz$ci_high, p_value=tsz$p_value, tfpw_applied=TRUE, lag1_ac=r)
}

fit_keys <- c("admin0_name","admin1_name","scenario","timeframe","model","hazard","season")

t0 <- Sys.time()
dt <- data.table(read_parquet(f))
cat(sprintf("[%s] read %s rows in %.1fs; cols: %s\n", ts(), format(nrow(dt), big.mark=","),
            as.numeric(difftime(Sys.time(), t0, units="secs")), paste(names(dt), collapse=",")))
cat(sprintf("iso3 present in source: %s\n", "iso3" %in% names(dt)))
dt <- dt[is.finite(value) & is.finite(year)]
ng_total <- nrow(unique(dt[, ..fit_keys]))
cat(sprintf("total fit groups in this file: %s\n", format(ng_total, big.mark=",")))

# group-size sanity (n per series) — catches a collapsed key (n explosion → slow/segfault)
gsz <- dt[, .N, by = fit_keys]$N
cat(sprintf("group size n: min=%d median=%d max=%d  (expect ~20-40; huge max = collapsed key)\n",
            min(gsz), as.integer(median(gsz)), max(gsz)))

# time on a SUBSAMPLE (first 2 admin0_name) → groups/sec → extrapolate to full file
sub_adm <- head(unique(dt$admin0_name), 2)
sub <- dt[admin0_name %in% sub_adm]
ng_sub <- nrow(unique(sub[, ..fit_keys]))
cat(sprintf("[%s] timing value_fit on subsample: %s admin0, %s groups, %s rows ...\n",
            ts(), length(sub_adm), format(ng_sub, big.mark=","), format(nrow(sub), big.mark=",")))
t1 <- Sys.time()
vf <- sub[, fit_value_kernel(year, value), by = fit_keys]
el <- as.numeric(difftime(Sys.time(), t1, units = "secs"))
rate <- ng_sub / el
cat(sprintf("[%s] subsample value_fit: %.1fs for %s groups = %.0f groups/s\n",
            ts(), el, format(ng_sub, big.mark=","), rate))
cat(sprintf(">>> EXTRAPOLATED full-file value_fit: ~%.1f min (this is the SMALL baseline; futures are bigger)\n",
            ng_total / rate / 60))
cat(sprintf(">>> tfpw applied in subsample: %d / %d groups\n", sum(vf$tfpw_applied), nrow(vf)))
cat("if extrapolation is minutes -> kernel scales; if tens of min -> per-group R dispatch is the bottleneck (need vectorised C++ over all groups)\n")
