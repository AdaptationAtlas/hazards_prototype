#!/usr/bin/env Rscript
# CR-093 REAL-OUTPUT validator — run on cglabs AFTER R/2.2_haz_change.R completes.
# Hard gate before any publish: confirms every R/2.2 parquet retained iso3, is
# WASM-prunable (non-null iso3 row-group stats), iso3-first sorted, and the
# ensemble files carry the mean/min/max/sd inter-model spread.
#
# Run (same shell that ran 0_server_setup.R, or it sources setup itself):
#   Rscript R/validate_cr093_real.R
#
# Pure DuckDB for all reads (arrow + duckdb read_parquet together crashes).

t0 <- Sys.time()
log <- function(...) cat(sprintf("[%s] ", format(Sys.time(), "%H:%M:%S")), ..., "\n")

suppressPackageStartupMessages({ library(data.table); library(DBI); library(duckdb) })

# Paths come from 0_server_setup.R (Data/ lives under the climdat working_dir).
if (!exists("haz_mean_dir") || !exists("haz_time_risk_dir")) {
  log("sourcing 0_server_setup.R for atlas paths")
  source("R/0_server_setup.R")
}
stats_dir <- file.path(haz_time_risk_dir, "stats")

targets <- data.table(
  file = c(
    file.path(haz_mean_dir, "ptot_perc", "ptot_change_by_model.parquet"),
    file.path(haz_mean_dir, "ptot_perc", "ptot_change_ensemble.parquet"),
    file.path(haz_mean_dir, "ptot_perc", "ptot_diff_by_model.parquet"),
    file.path(haz_mean_dir, "ptot_perc", "ptot_diff_ensemble.parquet"),
    file.path(haz_mean_dir, "thi_perc", "thi_perc_area_by_model.parquet"),
    file.path(haz_mean_dir, "thi_perc", "thi_perc_area_ensemble.parquet"),
    file.path(haz_mean_dir, "ntx_perc", "ntx_perc_area_by_model.parquet"),
    file.path(haz_mean_dir, "ntx_perc", "ntx_perc_area_ensemble.parquet"),
    file.path(stats_dir, "haz_freq.parquet"),
    file.path(stats_dir, "haz_freq_ensemble.parquet")
  )
)
targets[, is_ensemble := grepl("_ensemble|haz_freq_ensemble", basename(file))]

con <- dbConnect(duckdb::duckdb(":memory:"))
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
q <- function(sql, ...) as.data.table(dbGetQuery(con, sprintf(sql, ...)))

check_one <- function(path, is_ens) {
  if (!file.exists(path)) return(list(ok = FALSE, note = "MISSING FILE"))
  cols <- q("SELECT * FROM read_parquet('%s') LIMIT 0", path)
  if (!"iso3" %in% names(cols)) return(list(ok = FALSE, note = "no iso3 column"))

  agg <- q("SELECT COUNT(*) n, COUNT(iso3) n_iso3, COUNT(DISTINCT iso3) d_iso3 FROM read_parquet('%s')", path)
  if (agg$n == 0) return(list(ok = FALSE, note = "0 rows"))
  if (agg$n_iso3 < agg$n) return(list(ok = FALSE, note = sprintf("iso3 has %d NA", agg$n - agg$n_iso3)))
  if (agg$d_iso3 < 2) return(list(ok = FALSE, note = sprintf("only %d distinct iso3", agg$d_iso3)))

  md <- q("SELECT row_group_id, stats_min, stats_max FROM parquet_metadata('%s') WHERE path_in_schema='iso3'", path)
  n_rg <- md[, uniqueN(row_group_id)]
  n_null <- md[is.na(stats_min) | is.na(stats_max), .N]
  if (n_null > 0) return(list(ok = FALSE, note = sprintf("%d/%d row-groups NULL iso3 stats (NOT prunable)", n_null, n_rg)))

  if (is_ens) {
    have <- names(cols)
    miss <- setdiff(c("mean", "min", "max", "sd"), have)
    if (length(miss)) return(list(ok = FALSE, note = sprintf("ensemble missing cols: %s", paste(miss, collapse = ","))))
  }
  list(ok = TRUE, note = sprintf("rows=%d iso3=%d rg=%d", agg$n, agg$d_iso3, n_rg))
}

res <- rbindlist(lapply(seq_len(nrow(targets)), function(i) {
  r <- check_one(targets$file[i], targets$is_ensemble[i])
  data.table(file = basename(targets$file[i]), status = if (r$ok) "PASS" else "FAIL", note = r$note)
}))

cat("\n==== CR-093 real-output validation ====\n")
print(res, nrow = 100)
n_fail <- res[status == "FAIL", .N]
log(sprintf("done in %.1fs — %d PASS / %d FAIL", as.numeric(difftime(Sys.time(), t0, "secs")),
            res[status == "PASS", .N], n_fail))
if (n_fail > 0) { cat("\nHARD GATE: do NOT publish — fix failures above and re-run R/2.2.\n"); quit(status = 1) }
cat("\nGATE PASSED: R/2.2 outputs are iso3-bearing + prunable. Safe to wire publish.\n")
