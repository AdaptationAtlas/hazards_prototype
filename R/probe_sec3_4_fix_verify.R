# Verify the Bug C fix end-to-end on REAL data before the long §3.4 rerun.
# Mirrors the FIXED §3.4 path verbatim: value_fit (kernel) -> .EACHI carrying the
# data's own baseline_name -> merge -> value_slope. Expect value_slope NA% ~0.5%
# (only the legit tiny/tied groups), NOT 100%. Tests a baseline AND a future file.
# Run on CGLabs: Rscript R/probe_sec3_4_fix_verify.R
suppressMessages({library(data.table); library(arrow)})
D <- "/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/hazard_timeseries_mean_month"
files <- c("haz_3months_adm_mean_1995-2014_anomaly-historic_seasons.parquet",
           "haz_3months_adm_mean_2081-2100_anomaly-historic_seasons.parquet")

fit_keys <- c("admin0_name","admin1_name","scenario","timeframe","model","hazard","season")
fk <- function(year, value) {
  n <- length(value)
  if (n < 4L || !all(is.finite(value)))
    return(list(slope=NA_real_, ci_low=NA_real_, ci_high=NA_real_, p_value=NA_real_,
                tfpw_applied=FALSE, lag1_ac=NA_real_))
  ts0 <- trendkernel::mk_sen_cpp(value); s0 <- ts0$slope
  i0  <- median(value - s0*year, na.rm=TRUE); detr <- value - (s0*year + i0)
  r   <- trendkernel::lag1_ac_cpp(detr)
  if (abs(r) <= 0.1)
    return(list(slope=ts0$slope, ci_low=ts0$ci_low, ci_high=ts0$ci_high, p_value=ts0$p_value,
                tfpw_applied=FALSE, lag1_ac=r))
  wr <- c(detr[1L], detr[-1L] - r*detr[-n]); z <- wr + s0*year + i0; tsz <- trendkernel::mk_sen_cpp(z)
  list(slope=tsz$slope, ci_low=tsz$ci_low, ci_high=tsz$ci_high, p_value=tsz$p_value,
       tfpw_applied=TRUE, lag1_ac=r)
}

fail <- FALSE
for (f in files) {
  dt <- as.data.table(read_parquet(file.path(D, f)))
  dt <- dt[is.finite(value) & is.finite(year)]
  value_fit <- dt[, fk(year, value), by = fit_keys]

  # FIXED .EACHI: carry baseline_name from the data (NOT names(baselines))
  trend_summary <- dt[value_fit, on = fit_keys,
    .(baseline_name = baseline_name[1L],
      slope = i.slope, intercept = median(baseline_value - i.slope*year),
      ci_low = i.ci_low, ci_high = i.ci_high, p_value = i.p_value,
      tfpw_applied = i.tfpw_applied, lag1_ac = i.lag1_ac),
    by = .EACHI]

  m <- merge(dt, trend_summary,
    by = c("admin0_name","admin1_name","scenario","timeframe","model","hazard","season","baseline_name"),
    all.x = TRUE, sort = FALSE)

  stats <- m[, .(value_slope = slope[1], value_pval = p_value[1]),
    by = .(iso3, admin0_name, admin1_name, scenario, model, timeframe, hazard, season, baseline_name)]

  slope_na <- mean(is.na(stats$value_slope)); pval_na <- mean(is.na(stats$value_pval))
  ok <- slope_na < 0.5
  if (!ok) fail <- TRUE
  cat(sprintf("[%s] %s  baseline_name=%s  groups=%s  value_slopeNA=%.2f%%  value_pvalNA=%.2f%%\n",
              sub("haz_3months_adm_mean_","",sub("_anomaly.*","",f)),
              if (ok) "PASS" else "FAIL", unique(dt$baseline_name)[1],
              format(nrow(stats), big.mark=","), 100*slope_na, 100*pval_na))
}
cat(sprintf("\n%s — Bug C fix on real data\n", if (fail) "OVERALL FAIL" else "OVERALL PASS"))
if (fail) quit(status = 1)
