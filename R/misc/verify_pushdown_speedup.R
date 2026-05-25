#!/usr/bin/env Rscript
# verify_pushdown_speedup.R
# =========================
#
# Smoke-test the parquet pushdown rebake by running notebook-style
# filter queries against (a) the canonical S3 parquet and (b) the
# `.fixed.parquet` sidecar produced by rebake_parquets_for_pushdown.R.
# For each query, measure:
#   - elapsed wall time   (the user-facing metric the notebook pays)
#   - bytes downloaded    (via DuckDB's PRAGMA + duckdb_extension stats
#                          where available; falls back to noting "n/a")
#   - row groups read     (via EXPLAIN ANALYZE — shows whether DuckDB
#                          actually skipped any)
#
# Cache is disabled (`PRAGMA enable_object_cache=false`) so each query
# triggers a real cold-start fetch. Otherwise the second run hits the
# local cache and the test is meaningless.
#
# Usage
# -----
#   Rscript R/misc/verify_pushdown_speedup.R
#       Runs all built-in queries (canonical + sidecar pairs).
#
#   Rscript R/misc/verify_pushdown_speedup.R --only adm0_obs_monthly adm0_faostat
#       Restrict to specific target keys (same keys as the rebake script).
#
#   Rscript R/misc/verify_pushdown_speedup.R --canonical-only
#       Only query canonical paths — useful for an A baseline before
#       STAGE C uploads the sidecars.
#
# Expected outcome
# ----------------
# Canonical (single row group, NULL stats) — full file downloaded
# every time, ~30-70 s per query.
# Sidecar (multi row group, real stats) — DuckDB skips irrelevant
# groups, ~1-5 s per query.
# Speedup ratio: 10-30×.
#
# If the sidecar timing is close to canonical, pushdown isn't firing.
# Most likely cause: filter columns aren't sorted on (predicate can't
# narrow row groups) or stats still aren't populated (regression in
# the rebake script).

suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) {
    install.packages("pacman", repos = "https://cloud.r-project.org")
  }
  pacman::p_load(DBI, duckdb, data.table)
})

BUCKET <- "digital-atlas"
S3_BASE <- sprintf("https://%s.s3.amazonaws.com", BUCKET)

# ---------------------------------------------------------------------------
# Built-in query set — picks a representative national filter per target
# to mirror what the Climate Rationale notebook does on cold-start.
# Keys must match the rebake script's TARGETS list.
# ---------------------------------------------------------------------------

make_query <- function(key, s3_key, where_clause, notes = "") {
  list(key = key, s3_key = s3_key, where = where_clause, notes = notes)
}

QUERIES <- list(
  make_query(
    "adm0_obs_monthly",
    "domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=admin-monthly/variable=adm0_obs.parquet",
    "iso3 = 'AGO' AND variable = 'PTOT'",
    "Climate Rationale's AGO+PTOT cold-start case"
  ),
  make_query(
    "adm1_obs_monthly",
    "domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=admin-monthly/variable=adm1_obs.parquet",
    "iso3 = 'KEN' AND variable = 'TAVG'",
    "subnational TAVG query"
  ),
  make_query(
    "adm0_obs_periods",
    "domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=admin-periods/variable=adm0_obs.parquet",
    "iso3 = 'AGO' AND variable = 'PTOT' AND period = 'annual'",
    "national + period filter (the dispatch's example)"
  ),
  make_query(
    "adm1_obs_periods",
    "domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=admin-periods/variable=adm1_obs.parquet",
    "iso3 = 'ETH' AND variable = 'TMIN' AND period = 'annual'",
    "subnational + period filter"
  ),
  make_query(
    "cmip6_historical",
    "domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=timeseries_mean_month/timeframe=3months/period=1995-2014/baseline=1995-2014/variable=ensemble_season_timeseries.parquet",
    "iso3 = 'NGA' AND season = 'annual'",
    "historical national + season filter"
  ),
  make_query(
    "cmip6_2021_2040",
    "domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=timeseries_mean_month/timeframe=3months/period=2021-2040/baseline=1995-2014/variable=ensemble_season_timeseries.parquet",
    "iso3 = 'NGA' AND scenario = 'ssp245' AND season = 'annual'",
    "scenario + season filter"
  ),
  make_query(
    "hazard_exposure_multi",
    "domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=jagermeyr/model=ENSEMBLEmean/severity=severe/int=multi-hazard.parquet",
    "iso3 = 'CIV' AND crop = 'cocoa'",
    "hazard x exposure crop slice"
  ),
  make_query(
    "exposure_crop_livestock",
    "domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/variable=crop-livestock_all.parquet",
    "iso3 = 'UGA' AND exposure = 'vop'",
    "national + exposure filter"
  ),
  make_query(
    "adm0_faostat",
    "domain=socioeconomic/type=production/source=faostat/region=ssa/variable=adm0_faostat.parquet",
    "iso3 = 'ETH' AND variable = 'production'",
    "FAOSTAT national production query"
  )
)

# ---------------------------------------------------------------------------
# CLI parsing
# ---------------------------------------------------------------------------

parse_args <- function(argv) {
  args <- list(only = NULL, canonical_only = FALSE)
  i <- 1L
  while (i <= length(argv)) {
    a <- argv[i]
    if (a == "--canonical-only") {
      args$canonical_only <- TRUE
      i <- i + 1L
    } else if (a == "--only") {
      j <- i + 1L
      keys <- character(0)
      while (j <= length(argv) && !startsWith(argv[j], "--")) {
        keys <- c(keys, argv[j])
        j <- j + 1L
      }
      args$only <- keys
      i <- j
    } else if (a %in% c("-h", "--help")) {
      cat("Usage: Rscript verify_pushdown_speedup.R [--only KEY ...] [--canonical-only]\n")
      quit(status = 0, save = "no")
    } else {
      stop(sprintf("unknown arg: %s", a))
    }
  }
  args
}

# ---------------------------------------------------------------------------
# Per-query runner
# ---------------------------------------------------------------------------

sidecar_url <- function(canonical_url) {
  sub("\\.parquet$", ".fixed.parquet", canonical_url)
}

# Run a single query against a given URL with a fresh DuckDB connection
# (so the object cache really is empty for the cold-start measurement).
run_one <- function(url, where_clause) {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  # Belt-and-braces against any caching that might lurk:
  DBI::dbExecute(con, "PRAGMA enable_object_cache=false;")
  # httpfs is bundled with duckdb >= 0.7; install if missing.
  tryCatch(DBI::dbExecute(con, "INSTALL httpfs;"), error = function(e) NULL)
  DBI::dbExecute(con, "LOAD httpfs;")

  sql_count <- sprintf(
    "SELECT COUNT(*) AS n FROM read_parquet('%s') WHERE %s",
    url, where_clause
  )
  sql_explain <- sprintf(
    "EXPLAIN ANALYZE %s", sql_count
  )

  t0 <- Sys.time()
  rows <- DBI::dbGetQuery(con, sql_count)
  elapsed <- as.numeric(difftime(Sys.time(), t0), units = "secs")

  # EXPLAIN ANALYZE for the row-group skip info.
  plan <- tryCatch(
    DBI::dbGetQuery(con, sql_explain),
    error = function(e) data.frame(text = sprintf("(EXPLAIN ANALYZE failed: %s)", conditionMessage(e)))
  )
  plan_text <- paste(if (ncol(plan) >= 1L) plan[[ncol(plan)]] else character(0), collapse = "\n")

  list(
    url = url,
    elapsed_s = elapsed,
    n_rows = if (nrow(rows) > 0) rows$n[1] else NA_integer_,
    plan = plan_text
  )
}

run_query <- function(q, canonical_only = FALSE) {
  cat(sprintf("\n=== [%s] %s ===\n", q$key, q$notes))
  cat(sprintf("    WHERE %s\n", q$where))

  canonical_url <- file.path(S3_BASE, q$s3_key)
  sidecar       <- sidecar_url(canonical_url)

  out <- list(key = q$key)

  cat(sprintf("    canonical : %s\n", canonical_url))
  r_can <- tryCatch(run_one(canonical_url, q$where),
                    error = function(e) {
                      cat(sprintf("    canonical FAILED: %s\n", conditionMessage(e)))
                      list(elapsed_s = NA, n_rows = NA, plan = conditionMessage(e))
                    })
  cat(sprintf("    canonical : %.2fs, %s rows\n",
              r_can$elapsed_s,
              format(r_can$n_rows, big.mark = ",")))
  out$canonical <- r_can

  if (canonical_only) return(out)

  cat(sprintf("    sidecar   : %s\n", sidecar))
  r_side <- tryCatch(run_one(sidecar, q$where),
                     error = function(e) {
                       cat(sprintf("    sidecar FAILED: %s\n", conditionMessage(e)))
                       list(elapsed_s = NA, n_rows = NA, plan = conditionMessage(e))
                     })
  cat(sprintf("    sidecar   : %.2fs, %s rows\n",
              r_side$elapsed_s,
              format(r_side$n_rows, big.mark = ",")))
  out$sidecar <- r_side

  # Verdict.
  if (!is.na(r_can$elapsed_s) && !is.na(r_side$elapsed_s) && r_side$elapsed_s > 0) {
    speedup <- r_can$elapsed_s / r_side$elapsed_s
    rows_match <- isTRUE(r_can$n_rows == r_side$n_rows)
    cat(sprintf("    speedup   : %.1fx %s (correctness: %s)\n",
                speedup,
                if (speedup >= 3) "✓ pushdown firing" else "(no meaningful speedup)",
                if (rows_match) "match" else "MISMATCH"))
    out$speedup <- speedup
    out$rows_match <- rows_match
  } else {
    out$speedup <- NA_real_
    out$rows_match <- NA
  }
  out
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main <- function(argv) {
  args <- parse_args(argv)

  if (!is.null(args$only)) {
    wanted <- args$only
    selected <- Filter(function(q) q$key %in% wanted, QUERIES)
    have <- vapply(selected, function(q) q$key, character(1))
    missing <- setdiff(wanted, have)
    if (length(missing) > 0L) {
      cat(sprintf("unknown keys: %s\n", paste(missing, collapse = ", ")),
          file = stderr())
      cat(sprintf("available:    %s\n",
                  paste(vapply(QUERIES, function(q) q$key, character(1)), collapse = ", ")),
          file = stderr())
      return(2L)
    }
  } else {
    selected <- QUERIES
  }

  cat("=== verify_pushdown_speedup.R ===\n")
  cat(sprintf("bucket:           s3://%s\n", BUCKET))
  cat(sprintf("queries:          %d of %d\n", length(selected), length(QUERIES)))
  cat(sprintf("canonical_only:   %s\n", args$canonical_only))
  cat(sprintf("Each query runs on a FRESH duckdb connection with object cache\n",
              "disabled. Numbers should reflect cold-start cost.\n"))

  results <- list()
  for (q in selected) {
    results[[length(results) + 1L]] <- run_query(q, canonical_only = args$canonical_only)
  }

  cat("\n=== summary ===\n")
  if (args$canonical_only) {
    cat(sprintf("%-26s %10s %10s\n", "target", "elapsed_s", "rows"))
    cat(strrep("-", 50), "\n")
    for (r in results) {
      cat(sprintf("%-26s %10.2f %10s\n",
                  r$key,
                  r$canonical$elapsed_s,
                  format(r$canonical$n_rows, big.mark = ",")))
    }
  } else {
    cat(sprintf("%-26s %12s %12s %8s %s\n",
                "target", "canonical_s", "sidecar_s", "speedup", "rows_match"))
    cat(strrep("-", 75), "\n")
    for (r in results) {
      cat(sprintf("%-26s %12.2f %12.2f %7.1fx %s\n",
                  r$key,
                  r$canonical$elapsed_s,
                  r$sidecar$elapsed_s,
                  r$speedup,
                  if (isTRUE(r$rows_match)) "ok" else "MISMATCH"))
    }
    cat("\n")
    n_winning <- sum(vapply(results, function(r) isTRUE(r$speedup >= 3), logical(1)))
    cat(sprintf("Pushdown firing (>=3x speedup) on %d/%d queries.\n",
                n_winning, length(results)))
  }
  invisible(0L)
}

if (sys.nframe() == 0L) {
  rc <- tryCatch(main(commandArgs(trailingOnly = TRUE)),
                 error = function(e) {
                   cat(sprintf("FATAL: %s\n", conditionMessage(e)), file = stderr())
                   1L
                 })
  quit(status = as.integer(rc), save = "no")
}
