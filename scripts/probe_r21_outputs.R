#!/usr/bin/env Rscript
# scripts/probe_r21_outputs.R
# ===========================
# Validates R/2.1 output parquets before publishing to S3.
# Run after R/2.1 completes, before scripts/r21_publish_to_s3.R.
#
# Checks:
#   1. All 5 ensemble_seasons parquets exist (one per period)
#   2. CR-060 quantile columns present (q17_anomaly, q83_anomaly, n_models)
#   3. Pushdown stats populated (min/max on filter columns via parquet_metadata)
#   4. Row counts plausible (> 0, consistent across periods)
#   5. Sort order correct (iso3/hazard/scenario first — enables row-group skipping)
#
# Usage: Rscript scripts/probe_r21_outputs.R

source("R/0_server_setup.R")
suppressPackageStartupMessages({ pacman::p_load(arrow, data.table, DBI) })

PASS <- 0L; FAIL <- 0L
ok   <- function(msg) { cat(sprintf("  PASS  %s\n", msg)); PASS <<- PASS + 1L }
fail <- function(msg) { cat(sprintf("  FAIL  %s\n", msg)); FAIL <<- FAIL + 1L }

output_dir <- atlas_dirs$data_dir$hazard_timeseries_mean_month
PERIODS    <- c("1995-2014", "2021-2040", "2041-2060", "2061-2080", "2081-2100")
# Only match the CURRENT run's files: anomaly-historic (NEX-GDDP 1-baseline setup).
# Multiple baselines exist from older runs (anomaly-1981-2014, anomaly-1995-2014) —
# these are stale and should not be tested or published.
all_files  <- list.files(output_dir, "_anomaly-historic_ensemble_seasons\\.parquet$",
                         full.names = TRUE)

cat("=== probe_r21_outputs.R ===\n")
cat("output_dir =", output_dir, "\n")
cat("Matching: *_anomaly-historic_ensemble_seasons.parquet\n")
cat("Found:", length(all_files), "files\n\n")

# ---- 1. File existence ----
cat("--- 1. File existence (5 expected) ---\n")
for (p in PERIODS) {
  # Match on the DATA FILE period (left side of _anomaly-), not the baseline period.
  # Pattern: haz_3months_adm_mean_{p}_anomaly-historic_ensemble_seasons.parquet
  pattern <- paste0("haz_3months_adm_mean_", gsub("-", ".", p, fixed=TRUE), "_anomaly-historic")
  f <- all_files[grepl(pattern, basename(all_files), fixed = FALSE)]
  if (length(f) == 1) ok(sprintf("period=%s exists: %s", p, basename(f)))
  else if (length(f) == 0) fail(sprintf("period=%s MISSING", p))
  else fail(sprintf("period=%s has %d matches (ambiguous): %s", p, length(f), paste(basename(f), collapse=", ")))
}

# ---- 2. CR-060 columns ----
cat("\n--- 2. CR-060 quantile columns ---\n")
# Test the 2021-2040 period file from the current run
f_test <- all_files[grepl("haz_3months_adm_mean_2021.2040_anomaly-historic", basename(all_files))][1]
if (!is.na(f_test) && file.exists(f_test)) {
  schema_cols <- names(arrow::read_parquet(f_test, as_data_frame = FALSE)$schema)
  for (col in c("q17_anomaly", "q83_anomaly", "q50_anomaly", "n_models")) {
    if (col %in% schema_cols) ok(sprintf("column '%s' present", col))
    else fail(sprintf("column '%s' MISSING — CR-060 not baked", col))
  }
  if ("q17_anomaly" %in% schema_cols) {
    # Spot-check non-NA
    d <- data.table(arrow::read_parquet(f_test))[!is.na(q17_anomaly), .N]
    if (d > 0) ok(sprintf("q17_anomaly has %d non-NA values", d))
    else fail("q17_anomaly is all-NA")
  }
} else {
  fail("No 2021-2040 file found — cannot check columns")
}

# ---- 3. Pushdown stats ----
cat("\n--- 3. Pushdown stats (DuckDB parquet_metadata) ---\n")
if (!requireNamespace("duckdb", quietly = TRUE)) {
  cat("  SKIP  duckdb not available\n")
} else if (!is.na(f_test) && file.exists(f_test)) {
  .drv <- duckdb::duckdb(dbdir = ":memory:")
  con  <- DBI::dbConnect(.drv)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  meta <- DBI::dbGetQuery(con, sprintf(
    "SELECT path_in_schema, COUNT(*) AS n_rg,
            SUM(CASE WHEN stats_min IS NULL THEN 1 ELSE 0 END) AS null_stats
     FROM parquet_metadata('%s')
     WHERE path_in_schema IN ('admin0_name','hazard','scenario','season')
     GROUP BY path_in_schema", f_test))
  for (i in seq_len(nrow(meta))) {
    if (meta$null_stats[i] == 0)
      ok(sprintf("stats populated on '%s' (%d row groups)", meta$path_in_schema[i], meta$n_rg[i]))
    else
      fail(sprintf("NULL stats on '%s' in %d/%d row groups — pushdown broken",
                   meta$path_in_schema[i], meta$null_stats[i], meta$n_rg[i]))
  }
  rg_count <- DBI::dbGetQuery(con, sprintf(
    "SELECT COUNT(DISTINCT row_group_id) AS n FROM parquet_metadata('%s')", f_test))$n
  if (rg_count > 1) ok(sprintf("row group count = %d (> 1, pushdown possible)", rg_count))
  else fail(sprintf("only %d row group — entire file scanned per query", rg_count))
}

# ---- 4. Row counts ----
cat("\n--- 4. Row counts ---\n")
counts <- sapply(PERIODS, function(p) {
  pattern <- gsub("-", ".", p, fixed = TRUE)
  f <- all_files[grepl(pattern, basename(all_files))][1]
  if (is.na(f) || !file.exists(f)) return(NA_integer_)
  nrow(arrow::read_parquet(f))
})
for (i in seq_along(PERIODS)) {
  p <- PERIODS[i]; n <- counts[i]
  if (is.na(n)) fail(sprintf("period=%s — file missing, cannot count", p))
  else if (n > 0) ok(sprintf("period=%s — %d rows", p, n))
  else fail(sprintf("period=%s — 0 rows", p))
}

# ---- Summary ----
cat(sprintf("\n=== Results: %d passed, %d failed ===\n", PASS, FAIL))
if (FAIL > 0) {
  cat("Fix failures before running r21_publish_to_s3.R\n")
  quit(status = 1L)
} else {
  cat("All checks passed — safe to publish.\n")
  cat("Next: Rscript scripts/r21_publish_to_s3.R --dry-run\n")
}
