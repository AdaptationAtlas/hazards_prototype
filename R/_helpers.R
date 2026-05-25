# Top-level shared helpers for hazards_prototype pipeline scripts.
#
# Source via:  source(file.path(project_dir, "R", "_helpers.R"))
# (the producer scripts call source() relative to project_dir after
# 0_server_setup.R has resolved that variable).

#' Write a parquet file in a way DuckDB-WASM can push predicates down on.
#'
#' Notebooks consumed via DuckDB-WASM can only skip work at the row-group
#' level, and they can only decide to skip a group from column statistics.
#' A single-row-group file with NULL stats forces the browser to download
#' the entire compressed parquet on every cold-start query. This helper
#' guarantees the four things that make a parquet pushdown-friendly:
#'
#' 1. Multiple row groups (target ~64K-128K rows per group).
#' 2. Sorted by the columns notebooks actually filter on.
#' 3. Column statistics enabled (default in recent arrow, but explicit
#'    here so we don't drift on a version bump).
#' 4. Verified post-write — row-group count > 1 and stats populated on
#'    every filter column. Raises an error if not.
#'
#' Convention reference: project memory
#'   feedback-parquet-authoring-for-duckdb-wasm
#' Diagnosis: atlas_notebooks
#'   playbook/handovers/climateRationale/dispatches/
#'   2026-05-22_recent-changes-followups.md  (Follow-up 1)
#'
#' @param tbl A data.frame / data.table / arrow Table.
#' @param out_path File path to write to.
#' @param sort_by Character vector of columns to sort by, in priority
#'                order. Columns not present in the schema are silently
#'                skipped (a warning is emitted if none match).
#' @param verify_stats_on Character vector of columns whose min/max
#'                       stats MUST be populated post-write. Defaults
#'                       to sort_by.
#' @param row_group_size Target row-group size in rows. Default 100,000.
#' @param compression Default "zstd" / level 9 to match existing files.
#' @param compression_level Numeric compression level for zstd.
#' @param ... Forwarded to arrow::write_parquet for any other arguments.
write_parquet_pushdown <- function(
  tbl,
  out_path,
  sort_by,
  verify_stats_on = sort_by,
  row_group_size  = 100000L,
  compression     = "zstd",
  compression_level = 9L,
  ...
) {
  stopifnot(is.character(sort_by), length(sort_by) >= 1L)

  # Coerce to data.table for the sort, then back to whatever arrow wants.
  if (!inherits(tbl, "data.table")) tbl <- data.table::as.data.table(tbl)

  # Coerce factor columns in verify_stats_on to character. arrow writes
  # factors as dictionary-encoded — and column statistics are stored
  # against the dictionary indices, not the decoded values, so
  # parquet_metadata() returns NULL stats_min/max for them. Forcing
  # character keeps stats populated against the actual strings.
  for (col in intersect(verify_stats_on, names(tbl))) {
    if (is.factor(tbl[[col]])) {
      data.table::set(tbl, j = col, value = as.character(tbl[[col]]))
    }
  }

  sort_cols_present <- intersect(sort_by, names(tbl))
  if (length(sort_cols_present) == 0L) {
    warning(sprintf(
      "write_parquet_pushdown: none of sort_by columns present in tbl: %s",
      paste(sort_by, collapse = ", ")
    ))
  } else {
    data.table::setorderv(tbl, sort_cols_present)
  }

  arrow::write_parquet(
    tbl,
    out_path,
    compression       = compression,
    compression_level = compression_level,
    chunk_size        = row_group_size,
    write_statistics  = TRUE,
    # Disable dictionary encoding: when ON, arrow writes column
    # statistics against the dictionary indices (integers), not the
    # decoded string values, so DuckDB's parquet_metadata() returns
    # NULL stats_min / stats_max on string filter columns. zstd-9
    # still compresses repeated strings ~well.
    use_dictionary    = FALSE,
    ...
  )

  # Verify via DuckDB's parquet_metadata() — canonical surface and
  # works across arrow R-package versions.
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  rg_check <- DBI::dbGetQuery(con, sprintf(
    "SELECT COUNT(DISTINCT row_group_id) AS n_groups
     FROM parquet_metadata('%s')",
    out_path
  ))
  if (rg_check$n_groups < 2L) {
    stop(sprintf(
      paste0("write_parquet_pushdown: %s ended up with %d row group(s); ",
             "chunk_size = %d may be too large for %d rows"),
      out_path, rg_check$n_groups, row_group_size, nrow(tbl)
    ))
  }
  for (col in verify_stats_on) {
    if (!col %in% names(tbl)) {
      warning(sprintf(
        "write_parquet_pushdown: verify_stats_on column %s not in schema; skipping",
        col
      ))
      next
    }
    stats_check <- DBI::dbGetQuery(con, sprintf(
      "SELECT row_group_id, stats_min, stats_max
       FROM parquet_metadata('%s')
       WHERE path_in_schema = '%s'",
      out_path, col
    ))
    null_stats <- is.na(stats_check$stats_min) | is.na(stats_check$stats_max)
    if (any(null_stats)) {
      stop(sprintf(
        paste0("write_parquet_pushdown: %s column %s has NULL stats in ",
               "%d/%d row groups — pushdown will be broken"),
        out_path, col, sum(null_stats), nrow(stats_check)
      ))
    }
  }
  message(sprintf(
    "write_parquet_pushdown: %s written (%d row groups, stats verified on %s)",
    out_path, rg_check$n_groups,
    paste(verify_stats_on, collapse = ", ")
  ))
  invisible(out_path)
}
