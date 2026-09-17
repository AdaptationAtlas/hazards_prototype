#!/usr/bin/env Rscript
# scripts/probe_r21_outputs.R
# ===========================
# Validates R/2.1 output parquets before publishing to S3.
# Run after R/2.1 completes, before scripts/r21_publish_to_s3.R.
#
# Checks:
#   1. All ensemble_seasons parquets exist — per baseline window found on disk
#      (issue #26: anomaly outputs are *_anomaly-<window>_*, one set per historic
#      window; both 1981-2014 and 1995-2014 are expected on a full bake), the
#      4 future periods + the baseline's own window each
#   2. CR-060 quantile columns present (q17_anomaly, q83_anomaly, n_models;
#      q50 was pruned by CR-119 and must NOT be present)
#   3. n_models is a single uniform value in every file (issue #26: the published
#      product previously mixed 18/13/5/0-member rows inside one file)
#   4. Pushdown stats populated (min/max on filter columns via parquet_metadata)
#   5. Row counts plausible (> 0)
#
# Usage: Rscript scripts/probe_r21_outputs.R

source("R/0_server_setup.R")
suppressPackageStartupMessages({ pacman::p_load(arrow, data.table, DBI) })

PASS <- 0L; FAIL <- 0L
ok   <- function(msg) { cat(sprintf("  PASS  %s\n", msg)); PASS <<- PASS + 1L }
fail <- function(msg) { cat(sprintf("  FAIL  %s\n", msg)); FAIL <<- FAIL + 1L }

output_dir     <- atlas_dirs$data_dir$hazard_timeseries_mean_month
FUTURE_PERIODS <- c("2021-2040", "2041-2060", "2061-2080", "2081-2100")

cat("=== probe_r21_outputs.R ===\n")
cat("output_dir =", output_dir, "\n")

# Stale-run guard: interim anomaly-historic names must be gone (issue #26).
stale <- list.files(output_dir, "_anomaly-historic_")
if (length(stale) > 0) {
  fail(sprintf("%d stale anomaly-historic files present (e.g. %s) — pre-delete before validating",
               length(stale), stale[1]))
}

all_files <- list.files(output_dir, "_anomaly-[0-9]{4}-[0-9]{4}_ensemble_seasons\\.parquet$",
                        full.names = TRUE)
BASELINES <- sort(unique(sub(".*_anomaly-([0-9]{4}-[0-9]{4})_.*", "\\1", basename(all_files))))
cat("Matching: *_anomaly-<window>_ensemble_seasons.parquet\n")
cat("Found:", length(all_files), "files | baselines:", paste(BASELINES, collapse = ", "), "\n\n")
if (length(BASELINES) == 0) { cat("No output files found — has R/2.1 run?\n"); quit(status = 1L) }

# ---- 1. File existence ----
cat("--- 1. File existence (5 per baseline) ---\n")
f_test <- NA_character_
for (b in BASELINES) {
  for (p in c(b, FUTURE_PERIODS)) {
    f <- file.path(output_dir,
                   sprintf("haz_3months_adm_mean_%s_anomaly-%s_ensemble_seasons.parquet", p, b))
    if (file.exists(f)) {
      ok(sprintf("baseline=%s period=%s exists", b, p))
      if (p == "2021-2040") f_test <- f
    } else fail(sprintf("baseline=%s period=%s MISSING (%s)", b, p, basename(f)))
  }
}

# ---- 2. CR-060 / CR-119 columns ----
cat("\n--- 2. Schema (CR-060 quantiles present, CR-119 prunes absent) ---\n")
if (!is.na(f_test) && file.exists(f_test)) {
  schema_cols <- names(arrow::read_parquet(f_test, as_data_frame = FALSE)$schema)
  for (col in c("q17_anomaly", "q83_anomaly", "n_models", "baseline_name", "iso3")) {
    if (col %in% schema_cols) ok(sprintf("column '%s' present", col))
    else fail(sprintf("column '%s' MISSING", col))
  }
  for (col in c("q50_anomaly", "q5_anomaly", "q95_anomaly", "max", "min", "models")) {
    if (!col %in% schema_cols) ok(sprintf("pruned column '%s' absent", col))
    else fail(sprintf("column '%s' present — CR-119 prune regressed", col))
  }
  if ("q17_anomaly" %in% schema_cols) {
    d <- data.table(arrow::read_parquet(f_test))[!is.na(q17_anomaly), .N]
    if (d > 0) ok(sprintf("q17_anomaly has %d non-NA values", d))
    else fail("q17_anomaly is all-NA")
  }
} else {
  fail("No 2021-2040 file found — cannot check columns")
}

# ---- 3. n_models uniformity (issue #26) ----
cat("\n--- 3. n_models uniformity per file ---\n")
n_models_seen <- integer(0)
for (f in all_files) {
  nm <- data.table(arrow::read_parquet(f, col_select = "n_models"))[, sort(unique(n_models))]
  if (length(nm) == 1 && nm > 0) {
    ok(sprintf("%s: n_models uniformly %d", basename(f), nm))
    n_models_seen <- union(n_models_seen, nm)
  } else {
    fail(sprintf("%s: MIXED/zero n_models {%s} — issue #26 failure mode",
                 basename(f), paste(nm, collapse = ",")))
  }
}
if (length(n_models_seen) == 1) {
  ok(sprintf("single ensemble size across all files: %d members", n_models_seen))
} else if (length(n_models_seen) > 1) {
  fail(sprintf("ensemble size differs ACROSS files: {%s}", paste(n_models_seen, collapse = ",")))
}

# ---- 4. Pushdown stats ----
cat("\n--- 4. Pushdown stats (DuckDB parquet_metadata) ---\n")
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
     WHERE path_in_schema IN ('iso3','admin0_name','hazard','scenario','season')
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

# ---- 5. Row counts ----
cat("\n--- 5. Row counts ---\n")
for (f in all_files) {
  n <- nrow(arrow::read_parquet(f, col_select = "n_models"))
  if (n > 0) ok(sprintf("%s — %d rows", basename(f), n))
  else fail(sprintf("%s — 0 rows", basename(f)))
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
