# Real-output validation for the §3.4 FUTURE trends (no _trendref exists for futures).
# Checks the actually-written parquet for the two things the run log doesn't prove:
#   (1) iso3 present and 1:1 with admin0_name (CR-119),
#   (2) value_slope / value_pval are NOT all-NA (old Bug C — Rcpp kernel returning NA).
# Reads only the needed columns. Run on CGLabs after the §3.4 rerun. Exit 0 = PASS.
suppressMessages(library(data.table))
suppressMessages(library(arrow))
D <- "/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/hazard_timeseries_mean_month"

tfs  <- c("2021-2040","2041-2060","2061-2080","2081-2100")
cols <- c("iso3","admin0_name","value_slope","value_pval")
fail <- FALSE

for (tf in tfs) {
  fn <- file.path(D, sprintf("haz_3months_adm_mean_%s_anomaly-historic_trends.parquet", tf))
  if (!file.exists(fn)) { cat(sprintf("[%s] MISSING %s\n", tf, fn)); fail <- TRUE; next }
  dt <- as.data.table(read_parquet(fn, col_select = all_of(cols)))
  has_iso3 <- "iso3" %in% names(dt)
  # iso3 1:1 with admin0_name (no blank/NA, no cross-mapping)
  iso3_ok <- has_iso3 &&
    !any(is.na(dt$iso3) | dt$iso3 == "") &&
    nrow(unique(dt[, .(iso3, admin0_name)])) == uniqueN(dt$admin0_name)
  slope_na <- mean(is.na(dt$value_slope))
  pval_na  <- mean(is.na(dt$value_pval))
  slope_ok <- slope_na < 1.0   # Bug C = 100% NA
  pval_ok  <- pval_na  < 1.0
  ok <- has_iso3 && iso3_ok && slope_ok && pval_ok
  if (!ok) fail <- TRUE
  cat(sprintf("[%s] %s  rows=%s iso3=%s iso3_1to1=%s slopeNA=%.1f%% pvalNA=%.1f%%\n",
              tf, if (ok) "PASS" else "FAIL", format(nrow(dt), big.mark=","),
              has_iso3, iso3_ok, 100*slope_na, 100*pval_na))
}
cat(sprintf("\n%s — §3.4 future trends real-output check\n", if (fail) "OVERALL FAIL" else "OVERALL PASS"))
if (fail) quit(status = 1)
