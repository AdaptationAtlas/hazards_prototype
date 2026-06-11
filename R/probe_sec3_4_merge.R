# Pin the §3.4 100%-NA-slope to the trend_summary -> data_ex_trend merge (all.x=TRUE).
# value_fit is correct (worker probe: slopeNA 0.5%). The NA columns are exactly the
# RIGHT-side merge cols (slope/intercept/p_value), the survivors are LEFT-side
# (value/anomaly). That = a failed join key, prime suspect baseline_name.
# Run on CGLabs: Rscript R/probe_sec3_4_merge.R
suppressMessages({library(data.table); library(arrow)})
D  <- "/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/hazard_timeseries_mean_month"
sf <- file.path(D, "haz_3months_adm_mean_1995-2014_anomaly-historic_seasons.parquet")
dt <- as.data.table(read_parquet(sf))
dt <- dt[is.finite(value) & is.finite(year)]

merge_keys <- c("admin0_name","admin1_name","scenario","timeframe","model","hazard","season","baseline_name")
cat("seasons file columns:\n"); print(names(dt))
cat("\nhas baseline_name col:", "baseline_name" %in% names(dt), "\n")
if ("baseline_name" %in% names(dt)) {
  cat("unique baseline_name in seasons file: "); print(unique(dt$baseline_name))
  cat("class:", class(dt$baseline_name), "  any NA:", anyNA(dt$baseline_name), "\n")
}

# what the script assigns to trend_summary$baseline_name comes from names(baselines).
# Show the candidate scalar from the filename + the baselines object if discoverable.
cat("\nfilename baseline token: 1995-2014\n")

# Replicate the actual merge with the value built two ways and report match rate.
fit_keys <- c("admin0_name","admin1_name","scenario","timeframe","model","hazard","season")
fk <- function(year, value) {
  n <- length(value); if (n < 4L || !all(is.finite(value))) return(list(slope=NA_real_))
  list(slope = trendkernel::mk_sen_cpp(value)$slope)
}
value_fit <- dt[, fk(year, value), by = fit_keys]
cat(sprintf("\nvalue_fit slopeNA = %.2f%%  (this is correct/expected small)\n", 100*mean(is.na(value_fit$slope))))

bn_in_data <- if ("baseline_name" %in% names(dt)) unique(dt$baseline_name)[1] else NA
for (cand in unique(c(bn_in_data, "1995-2014", "historic", "1995_2014"))) {
  if (is.na(cand)) next
  ts <- copy(value_fit); ts[, baseline_name := cand]
  m  <- merge(dt, ts, by = merge_keys, all.x = TRUE)
  cat(sprintf("  merge with baseline_name='%s'  ->  slope NA after merge = %.1f%%\n",
              cand, 100*mean(is.na(m$slope))))
}
cat("\nIf the data's own baseline_name gives ~0%% but other candidates give 100%%,\n",
    "the bug is the scalar assigned to trend_summary$baseline_name not matching the data.\n")
