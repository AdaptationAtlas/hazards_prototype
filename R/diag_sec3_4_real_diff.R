# Real-data validation: new §3.4 trends (Rcpp KERNEL, deduped) vs the saved _trendref
# (pure trend:: output). Confirms the kernel == trend:: on actual data, within the 3-dp
# rounding (round3.4=3 → tol 1e-3). Run on CGLabs after the §3.4 rerun completes.
#
# Handles two wrinkles:
#  - _trendref is the OLD output: ~20x row-duplicated (value_decade bug) AND has no iso3.
#    -> dedup BOTH to one row per group key, and join on the non-iso3 key.
#  - dup rows are byte-identical, so dedup is lossless; value_decade matches (10*slope[1]
#    == 10*slope for the constant in-group slope).
suppressMessages(library(data.table))
D <- "/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/hazard_timeseries_mean_month"
key <- c("admin0_name","admin1_name","scenario","model","timeframe","hazard","season","baseline_name")
tol <- 1e-3   # 3-dp rounding
tfs <- c("1981-2014","1995-2014")

worst <- 0
for (tf in tfs) {
  fn  <- sprintf("haz_3months_adm_mean_%s_anomaly-historic_trends.parquet", tf)
  nf  <- file.path(D, fn); rf <- file.path(D, "_trendref", fn)
  if (!file.exists(rf)) { cat(sprintf("[%s] no _trendref — skip\n", tf)); next }
  new <- unique(data.table(arrow::read_parquet(nf)), by = key)   # kernel, already ratio 1
  ref <- unique(data.table(arrow::read_parquet(rf)), by = key)   # trend::, dedup the ~20x bloat
  cat(sprintf("[%s] new groups: %s  ref groups: %s\n", tf,
              format(nrow(new), big.mark=","), format(nrow(ref), big.mark=",")))
  num <- intersect(names(new), names(ref))
  num <- num[sapply(num, function(c) is.numeric(new[[c]]))]
  m <- merge(new, ref, by = key, suffixes = c(".n",".r"))
  cat(sprintf("       matched %s groups\n", format(nrow(m), big.mark=",")))
  tfmax <- 0
  for (c in num) {
    na_mm <- sum(is.na(m[[paste0(c,".n")]]) != is.na(m[[paste0(c,".r")]]))
    d <- suppressWarnings(max(abs(m[[paste0(c,".n")]] - m[[paste0(c,".r")]]), na.rm = TRUE))
    if (!is.finite(d)) d <- 0
    flag <- if (na_mm > 0) sprintf(" NA-mismatch=%d", na_mm) else ""
    cat(sprintf("       %-14s max|diff|=%.3e%s\n", c, d, flag))
    tfmax <- max(tfmax, d)
  }
  cat(sprintf("[%s] %s (max %.2e)\n", tf, if (tfmax < tol) "PASS" else "CHECK", tfmax))
  worst <- max(worst, tfmax)
}
cat(sprintf("\n%s — kernel vs trend:: on real data, max|diff|=%.2e (tol %.0e)\n",
            if (worst < tol) "OVERALL PASS" else "OVERALL CHECK", worst, tol))
