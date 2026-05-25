#!/usr/bin/env Rscript
# rebake_parquets_for_pushdown.R
# =================================
#
# R port of rebake_parquets_for_pushdown.py — one-off rescue job to
# rewrite Atlas parquets on S3 so DuckDB-WASM can push row-group-level
# predicates down. The current files have ONE row group and NULL stats
# on the filter keys (iso3, variable, period, ...), which forces
# DuckDB-WASM to download the entire compressed file on every cold-
# start query regardless of the WHERE clause. Pete observed a 69-second
# cold-start fetch for a 45-row national query on adm0_obs.parquet
# (see playbook/handovers/climateRationale/dispatches/
#  2026-05-22_recent-changes-followups.md and the companion
#  2026-05-25_pipeline-parquet-pushdown-rewrite.md).
#
# This script reads each canonical parquet from S3, rewrites it with
# ~100K-row row groups, sorted by the filter keys, with column
# statistics enabled, then writes the result to a SIDECAR S3 key
# (`<original>.fixed.parquet` alongside the original). It does NOT
# overwrite the canonical path — that swap is a manual step you do
# once you've A/B tested cold-start performance.
#
# Usage
# -----
#     # AWS creds via env vars or ~/.aws/credentials.
#     export AWS_ACCESS_KEY_ID=...
#     export AWS_SECRET_ACCESS_KEY=...
#
#     # Dry-run first (downloads, rebakes locally, prints stats, no upload):
#     Rscript scripts/rebake_parquets_for_pushdown.R --dry-run
#
#     # Real run — uploads each rebake to a `.fixed.parquet` sidecar S3 key:
#     Rscript scripts/rebake_parquets_for_pushdown.R
#
#     # Limit to a subset:
#     Rscript scripts/rebake_parquets_for_pushdown.R --only adm0_obs_monthly adm0_faostat
#
#     # Custom row-group size:
#     Rscript scripts/rebake_parquets_for_pushdown.R --row-group 64000
#
# Convention reference: project memory feedback-parquet-authoring-for-duckdb-wasm.
# Author: written 2026-05-25.

suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) {
    install.packages("pacman", repos = "https://cloud.r-project.org")
  }
  pacman::p_load(arrow, data.table, s3fs, DBI, duckdb)
})

BUCKET <- "digital-atlas"

# ---------------------------------------------------------------------------
# Target inventory — keep in sync with the .py companion and
# data/climateRationale/nbData.json.
# ---------------------------------------------------------------------------

make_target <- function(key, s3_key, sort_by, verify_stats_on, notes = "") {
  list(key = key, s3_key = s3_key, sort_by = sort_by,
       verify_stats_on = verify_stats_on, notes = notes)
}

TARGETS <- list(
  # observational (highest-impact targets) -----------------------------------
  make_target(
    "adm0_obs_monthly",
    "domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=admin-monthly/variable=adm0_obs.parquet",
    c("iso3", "variable", "year", "month"),
    c("iso3", "variable"),
    "Producer: hazards_prototype/R/observational/3_extract_obs_admin.R"
  ),
  make_target(
    "adm1_obs_monthly",
    "domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=admin-monthly/variable=adm1_obs.parquet",
    c("iso3", "admin1_name", "variable", "year", "month"),
    c("iso3", "variable"),
    "Producer: hazards_prototype/R/observational/3_extract_obs_admin.R"
  ),
  make_target(
    "adm0_obs_periods",
    "domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=admin-periods/variable=adm0_obs.parquet",
    c("iso3", "variable", "period", "year"),
    c("iso3", "variable", "period"),
    "Producer: hazards_prototype/R/observational/4_aggregate_obs_admin_periods.R"
  ),
  make_target(
    "adm1_obs_periods",
    "domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=admin-periods/variable=adm1_obs.parquet",
    c("iso3", "admin1_name", "variable", "period", "year"),
    c("iso3", "variable", "period"),
    "Producer: hazards_prototype/R/observational/4_aggregate_obs_admin_periods.R"
  ),
  # NEX-GDDP-CMIP6 ensemble timeseries (historical + 4 future periods) -------
  make_target(
    "cmip6_historical",
    "domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=timeseries_mean_month/timeframe=3months/period=1995-2014/baseline=1995-2014/variable=ensemble_season_timeseries.parquet",
    c("iso3", "admin1_name", "variable", "season", "year"),
    c("iso3", "variable", "season"),
    "Producer: hazards_prototype/R/1.x_*_timeseries.R"
  ),
  make_target(
    "cmip6_2021_2040",
    "domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=timeseries_mean_month/timeframe=3months/period=2021-2040/baseline=1995-2014/variable=ensemble_season_timeseries.parquet",
    c("iso3", "admin1_name", "variable", "season", "scenario", "year"),
    c("iso3", "variable", "season", "scenario"),
    "Producer: hazards_prototype/R/1.x_*_timeseries.R"
  ),
  make_target(
    "cmip6_2041_2060",
    "domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=timeseries_mean_month/timeframe=3months/period=2041-2060/baseline=1995-2014/variable=ensemble_season_timeseries.parquet",
    c("iso3", "admin1_name", "variable", "season", "scenario", "year"),
    c("iso3", "variable", "season", "scenario"),
    "Producer: hazards_prototype/R/1.x_*_timeseries.R"
  ),
  make_target(
    "cmip6_2061_2080",
    "domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=timeseries_mean_month/timeframe=3months/period=2061-2080/baseline=1995-2014/variable=ensemble_season_timeseries.parquet",
    c("iso3", "admin1_name", "variable", "season", "scenario", "year"),
    c("iso3", "variable", "season", "scenario"),
    "Producer: hazards_prototype/R/1.x_*_timeseries.R"
  ),
  make_target(
    "cmip6_2081_2100",
    "domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=timeseries_mean_month/timeframe=3months/period=2081-2100/baseline=1995-2014/variable=ensemble_season_timeseries.parquet",
    c("iso3", "admin1_name", "variable", "season", "scenario", "year"),
    c("iso3", "variable", "season", "scenario"),
    "Producer: hazards_prototype/R/1.x_*_timeseries.R"
  ),
  # hazard exposure & exposure -----------------------------------------------
  make_target(
    "hazard_exposure_multi",
    "domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=jagermeyr/model=ENSEMBLEmean/severity=severe/int=multi-hazard.parquet",
    c("iso3", "admin1_name", "crop", "scenario", "timeperiod"),
    c("iso3", "crop", "scenario"),
    "Producer: hazards_prototype/R/3_freq_x_exposure.R"
  ),
  make_target(
    "exposure_crop_livestock",
    "domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/variable=crop-livestock_all.parquet",
    c("iso3", "admin1_name", "exposure", "unit_full", "crop"),
    c("iso3", "exposure", "unit_full"),
    "Producer: hazards_prototype/R/0.4.4_process_exposure.R (renamed at publish time)"
  ),
  # FAOSTAT production timeseries --------------------------------------------
  make_target(
    "adm0_faostat",
    "domain=socioeconomic/type=production/source=faostat/region=ssa/variable=adm0_faostat.parquet",
    c("iso3", "variable", "commodity", "year"),
    c("iso3", "variable", "commodity"),
    "Producer: hazards_prototype/R/0.4.5_create_faostat_long.R"
  ),
  # externally-sourced (no producer mounted) ---------------------------------
  make_target(
    "a0_gdp",
    "domain=socioeconomic/type=economic/source=worldbank_gdp/region=ssa/variable=adm0_sectorGDP_usd2015.parquet",
    c("iso3", "year"),
    c("iso3"),
    "External producer (World Bank WDI pipeline not in mounted repos)."
  ),
  make_target(
    "a0_landuse",
    "domain=socioeconomic/type=economic/source=fao_landuse/region=ssa/variable=adm0_sectorLanduse.parquet",
    c("iso3", "year"),
    c("iso3"),
    "External producer (FAOSTAT land-use pipeline not in mounted repos)."
  ),
  make_target(
    "poverty",
    "domain=socioeconomic/type=economic/source=worldbank_gsap2023/region=africa/variable=adm01_pov-rates.parquet",
    c("iso3", "admin1_name"),
    c("iso3"),
    "External producer (World Bank GSAP 2023 pipeline not in mounted repos)."
  )
)

# ---------------------------------------------------------------------------
# S3 helpers — s3fs honours the default AWS credential chain.
# ---------------------------------------------------------------------------

s3_url <- function(key) sprintf("s3://%s/%s", BUCKET, key)

s3_head <- function(key) {
  url <- s3_url(key)
  tryCatch(s3fs::s3_file_info(url), error = function(e) NULL)
}

s3_download <- function(key, local_path) {
  s3fs::s3_file_download(s3_url(key), local_path, overwrite = TRUE)
  invisible(local_path)
}

s3_upload <- function(local_path, key) {
  s3fs::s3_file_upload(local_path, s3_url(key), overwrite = TRUE)
  invisible(key)
}

sidecar_key <- function(canonical_key) {
  if (!grepl("\\.parquet$", canonical_key)) {
    stop(sprintf("unexpected key (missing .parquet): %s", canonical_key))
  }
  sub("\\.parquet$", ".fixed.parquet", canonical_key)
}

# ---------------------------------------------------------------------------
# Verification — mirror the .py verify_stats() via DuckDB's
# parquet_metadata() (canonical surface, works across arrow versions).
# ---------------------------------------------------------------------------

verify_stats <- function(out_path, verify_on) {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  rg <- DBI::dbGetQuery(con, sprintf(
    "SELECT COUNT(DISTINCT row_group_id) AS n_groups
     FROM parquet_metadata('%s')",
    out_path
  ))
  n_groups <- rg$n_groups
  total_rows <- DBI::dbGetQuery(con, sprintf(
    "SELECT num_rows FROM parquet_file_metadata('%s')", out_path
  ))$num_rows

  problems <- character(0)
  if (n_groups < 2L) {
    problems <- c(problems,
      sprintf("only %d row group(s) — pushdown will not work", n_groups))
  }

  stats_summary <- vector("list", length(verify_on))
  names(stats_summary) <- verify_on

  schema_cols <- DBI::dbGetQuery(con, sprintf(
    "SELECT DISTINCT path_in_schema AS col FROM parquet_metadata('%s')",
    out_path
  ))$col

  for (col_name in verify_on) {
    if (!col_name %in% schema_cols) {
      problems <- c(problems, sprintf(
        "column %s not in schema — cannot verify stats", col_name))
      next
    }
    stats <- DBI::dbGetQuery(con, sprintf(
      "SELECT row_group_id, stats_min, stats_max
       FROM parquet_metadata('%s')
       WHERE path_in_schema = '%s'
       ORDER BY row_group_id",
      out_path, col_name
    ))
    null_stats <- is.na(stats$stats_min) | is.na(stats$stats_max)
    if (any(null_stats)) {
      problems <- c(problems, sprintf(
        "column %s: NULL min/max in %d/%d row groups",
        col_name, sum(null_stats), nrow(stats)))
    }
    stats_summary[[col_name]] <- stats
  }

  list(
    num_row_groups = n_groups,
    num_rows       = total_rows,
    stats_summary  = stats_summary,
    problems       = problems
  )
}

# ---------------------------------------------------------------------------
# Rebake core
# ---------------------------------------------------------------------------

reorder_table <- function(tbl, sort_by) {
  schema_names <- names(tbl)
  sort_cols <- intersect(sort_by, schema_names)
  missing <- setdiff(sort_by, schema_names)
  if (length(missing) > 0L) {
    cat(sprintf("    warn: sort columns not in schema, skipping: %s\n",
                paste(missing, collapse = ", ")))
  }
  if (length(sort_cols) == 0L) {
    cat("    warn: no sort columns matched schema — table written unsorted\n")
    return(tbl)
  }
  if (!inherits(tbl, "data.table")) tbl <- data.table::as.data.table(tbl)
  data.table::setorderv(tbl, sort_cols)
  tbl
}

rebake_one <- function(target, row_group_size, dry_run, tmpdir) {
  cat(sprintf("\n[%s] %s\n", target$key, target$s3_key))
  cat(sprintf("    notes: %s\n", target$notes))

  # 1. Existence check.
  head <- s3_head(target$s3_key)
  if (is.null(head) || nrow(head) == 0L) {
    cat("    !! missing on S3 — skipping\n")
    return(list(key = target$key, status = "missing"))
  }
  orig_bytes <- head$size[1]
  cat(sprintf("    head: %s bytes (%.2f MB)\n",
              format(orig_bytes, big.mark = ","),
              orig_bytes / 1024 / 1024))

  # 2. Download.
  in_local <- file.path(tmpdir, sprintf("%s.in.parquet", target$key))
  t0 <- Sys.time()
  s3_download(target$s3_key, in_local)
  cat(sprintf("    downloaded in %.2fs\n",
              as.numeric(difftime(Sys.time(), t0), units = "secs")))

  # 3. Read full table into memory.
  t0 <- Sys.time()
  tbl <- arrow::read_parquet(in_local, as_data_frame = FALSE)
  n_rows <- tbl$num_rows
  n_cols <- tbl$num_columns
  cat(sprintf("    read into arrow: %s rows, %d cols, %.2fs\n",
              format(n_rows, big.mark = ","), n_cols,
              as.numeric(difftime(Sys.time(), t0), units = "secs")))

  # 4. Inspect BEFORE row-group count.
  con <- DBI::dbConnect(duckdb::duckdb())
  before <- DBI::dbGetQuery(con, sprintf(
    "SELECT COUNT(DISTINCT row_group_id) AS n_groups FROM parquet_metadata('%s')",
    in_local
  ))$n_groups
  DBI::dbDisconnect(con, shutdown = TRUE)
  cat(sprintf("    BEFORE: %d row group(s), %s rows\n",
              before, format(n_rows, big.mark = ",")))

  # 5. Sort + factor → character coercion on verify columns.
  #    arrow writes R factors as dictionary-encoded; column stats are
  #    then against the dictionary indices, not the string values, so
  #    DuckDB's parquet_metadata() returns NULL stats_min/max. Coerce
  #    here so stats are written against the actual strings.
  t0 <- Sys.time()
  tbl_dt <- as.data.frame(tbl)  # convert via R DF; faster than arrow::compute for our sizes
  if (!inherits(tbl_dt, "data.table")) tbl_dt <- data.table::as.data.table(tbl_dt)
  for (col in intersect(target$verify_stats_on, names(tbl_dt))) {
    if (is.factor(tbl_dt[[col]])) {
      data.table::set(tbl_dt, j = col, value = as.character(tbl_dt[[col]]))
    }
  }
  tbl_sorted <- reorder_table(tbl_dt, target$sort_by)
  cat(sprintf("    sorted by [%s] in %.2fs\n",
              paste(target$sort_by, collapse = ", "),
              as.numeric(difftime(Sys.time(), t0), units = "secs")))

  # 6. Write to a local tmp file with the desired row group size + stats.
  out_local <- file.path(tmpdir, sprintf("%s.fixed.parquet", target$key))
  t0 <- Sys.time()
  arrow::write_parquet(
    tbl_sorted, out_local,
    compression       = "zstd",
    compression_level = 9L,
    chunk_size        = row_group_size,
    write_statistics  = TRUE,
    # Disable dictionary encoding so column stats are written against
    # the decoded string values; otherwise DuckDB's parquet_metadata()
    # returns NULL stats_min/max for iso3, variable, ... and the whole
    # pushdown purpose is defeated.
    use_dictionary    = FALSE
  )
  cat(sprintf("    wrote local rebake in %.2fs -> %s\n",
              as.numeric(difftime(Sys.time(), t0), units = "secs"),
              out_local))

  # 7. Verify.
  v <- verify_stats(out_local, target$verify_stats_on)
  cat(sprintf("    AFTER:  %d row group(s), %s rows\n",
              v$num_row_groups, format(v$num_rows, big.mark = ",")))
  for (col_name in names(v$stats_summary)) {
    stats <- v$stats_summary[[col_name]]
    if (is.null(stats) || nrow(stats) == 0L) {
      cat(sprintf("    stats[%s]: (no stats — verification will fail)\n", col_name))
    } else {
      mins <- head(sort(unique(as.character(stats$stats_min))), 3)
      maxs <- tail(sort(unique(as.character(stats$stats_max))), 3)
      cat(sprintf("    stats[%s]: %d groups · min~[%s] max~[%s]\n",
                  col_name, nrow(stats),
                  paste(mins, collapse = ", "),
                  paste(maxs, collapse = ", ")))
    }
  }
  if (length(v$problems) > 0L) {
    for (p in v$problems) cat(sprintf("    PROBLEM: %s\n", p))
    cat("    !! aborting this target — not uploading\n")
    return(list(key = target$key, status = "verify_failed", details = v))
  }

  new_size <- file.info(out_local)$size
  delta_pct <- (new_size - orig_bytes) / orig_bytes * 100
  cat(sprintf("    rebaked size: %s bytes (%+.1f%% vs original)\n",
              format(new_size, big.mark = ","), delta_pct))

  # 8. Upload to sidecar key (unless dry-run).
  out_key <- sidecar_key(target$s3_key)
  if (dry_run) {
    cat(sprintf("    DRY-RUN: would upload to %s\n", s3_url(out_key)))
    return(list(key = target$key, status = "dry_run", details = v))
  }

  s3_upload(out_local, out_key)
  cat(sprintf("    uploaded -> %s\n", s3_url(out_key)))
  list(key = target$key, status = "uploaded", details = v, sidecar_key = out_key)
}

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

parse_args <- function(argv) {
  args <- list(dry_run = FALSE, only = NULL, row_group = 100000L,
               tmpdir = "/tmp/atlas_parquet_rebake")
  i <- 1L
  while (i <= length(argv)) {
    a <- argv[i]
    if (a == "--dry-run") {
      args$dry_run <- TRUE
      i <- i + 1L
    } else if (a == "--only") {
      # consume tokens until next --flag or end
      j <- i + 1L
      keys <- character(0)
      while (j <= length(argv) && !startsWith(argv[j], "--")) {
        keys <- c(keys, argv[j])
        j <- j + 1L
      }
      args$only <- keys
      i <- j
    } else if (a == "--row-group") {
      args$row_group <- as.integer(argv[i + 1L])
      i <- i + 2L
    } else if (a == "--tmpdir") {
      args$tmpdir <- argv[i + 1L]
      i <- i + 2L
    } else if (a %in% c("-h", "--help")) {
      cat(readLines(commandArgs(trailingOnly = FALSE)[4]), sep = "\n")  # script-level doc
      quit(status = 0, save = "no")
    } else {
      stop(sprintf("unknown arg: %s", a))
    }
  }
  args
}

main <- function(argv) {
  args <- parse_args(argv)
  dir.create(args$tmpdir, recursive = TRUE, showWarnings = FALSE)

  if (!is.null(args$only)) {
    wanted <- args$only
    selected <- Filter(function(t) t$key %in% wanted, TARGETS)
    have <- vapply(selected, function(t) t$key, character(1))
    missing <- setdiff(wanted, have)
    if (length(missing) > 0L) {
      cat(sprintf("unknown target keys: %s\n", paste(missing, collapse = ", ")),
          file = stderr())
      cat(sprintf("available: %s\n",
                  paste(vapply(TARGETS, function(t) t$key, character(1)), collapse = ", ")),
          file = stderr())
      return(2L)
    }
  } else {
    selected <- TARGETS
  }

  cat("=== rebake_parquets_for_pushdown.R ===\n")
  cat(sprintf("bucket:        s3://%s\n", BUCKET))
  cat(sprintf("targets:       %d of %d\n", length(selected), length(TARGETS)))
  cat(sprintf("row_group:     %s rows\n", format(args$row_group, big.mark = ",")))
  cat(sprintf("dry_run:       %s\n", args$dry_run))
  cat(sprintf("tmpdir:        %s\n", args$tmpdir))
  cat(sprintf("AWS_PROFILE:   %s\n",
              ifelse(nzchar(Sys.getenv("AWS_PROFILE")),
                     Sys.getenv("AWS_PROFILE"), "<unset>")))

  results <- list()
  for (t in selected) {
    r <- tryCatch(
      rebake_one(t, args$row_group, args$dry_run, args$tmpdir),
      error = function(e) {
        cat(sprintf("    !! exception: %s\n", conditionMessage(e)))
        list(key = t$key, status = "exception", details = conditionMessage(e))
      }
    )
    results[[length(results) + 1L]] <- r
  }

  # Summary line per target.
  cat("\n=== summary ===\n")
  width <- max(nchar(vapply(results, function(r) r$key, character(1))))
  for (r in results) {
    cat(sprintf("  %-*s  %s\n", width, r$key, r$status))
  }

  # Manual-swap cheatsheet.
  uploaded <- Filter(function(r) r$status == "uploaded", results)
  if (length(uploaded) > 0L) {
    cat("\n=== manual-swap commands (after validating the .fixed files via DuckDB) ===\n")
    for (r in uploaded) {
      t <- Find(function(t) t$key == r$key, TARGETS)
      canonical <- s3_url(t$s3_key)
      sidecar   <- s3_url(r$sidecar_key)
      backup    <- paste0(canonical, ".preFix.bak")
      cat(sprintf("# %s\n", r$key))
      cat(sprintf("aws s3 mv %s %s\n", canonical, backup))
      cat(sprintf("aws s3 mv %s   %s\n", sidecar, canonical))
      cat("\n")
    }
  }

  n_failed <- sum(vapply(results,
                         function(r) r$status %in% c("verify_failed", "exception"),
                         logical(1)))
  if (n_failed > 0L) 1L else 0L
}

if (sys.nframe() == 0L) {
  rc <- main(commandArgs(trailingOnly = TRUE))
  quit(status = as.integer(rc), save = "no")
}
