# Top-level shared helpers for hazards_prototype pipeline scripts.
#
# Source via:  source(file.path(project_dir, "R", "_helpers.R"))
# (the producer scripts call source() relative to project_dir after
# 0_server_setup.R has resolved that variable).

#' Write a parquet file in a way DuckDB-WASM can push predicates down on.
#'
#' Atlas notebooks consume parquets via DuckDB-WASM, which can only skip
#' work at the row-group level (per-row-group min/max column stats) AND
#' can only range-read at the column-chunk level (one fetch per
#' row-group × column needed). DuckDB-WASM has no per-page index
#' support, so the chunk IS the fetch unit; smaller row groups ⇒
#' smaller chunks ⇒ smaller per-fetch bytes.
#'
#' This helper:
#'
#' 1. Sorts by the columns notebooks filter on, so per-RG iso3 stats
#'    bracket contiguous ranges and most RGs get skipped on a single-
#'    country query.
#' 2. Writes via DuckDB-native COPY TO PARQUET — pyarrow's writer
#'    crashes DuckDB-WASM in the hive_partitioning=1 + multi-file UNION
#'    view shape used by the climateRationale notebook (root cause:
#'    byte-format-sensitive WASM parser). DuckDB writes a byte format
#'    its own WASM build can read. See 2026-05-27 dispatch
#'    parquet-pushdown-pipeline-ask.md for the failed-experiment trail.
#' 3. Targets ROW_GROUP_SIZE = 50,000 rows. Per the 2026-05-27
#'    parameter sweep at /tmp/parquet-pushdown-experiment/, this
#'    halves average compressed column-chunk size (~150KB→~76KB) vs
#'    the previous 100,000 default, with negligible (+0.3%) file-size
#'    cost. Going smaller balloons footer overhead without further
#'    chunk-size wins. DuckDB 1.5 has no DATA_PAGE_SIZE / WRITE_PAGE_-
#'    INDEX option, so ROW_GROUP_SIZE is the only knob.
#' 4. Verifies post-write: row-group count > 1, stats populated on every
#'    `verify_stats_on` column, and average column-chunk compressed size
#'    is below `max_avg_chunk_kb` (default 200 KB, the threshold above
#'    which WASM range-read latency starts to dominate).
#'
#' NB: this verification is necessary-but-not-sufficient. The
#' authoritative WASM perf verdict still has to come from loading the
#' promoted canonical in Chrome and confirming HAR-level byte transfer
#' for a `WHERE iso3 = 'X'` query is ~2-5% of file size. See the
#' pipeline-ask dispatch's verification-checklist step 3.
#'
#' Convention reference: project memory feedback-parquet-authoring-for-duckdb-wasm
#'
#' @param tbl A data.frame / data.table / arrow Table.
#' @param out_path File path to write to.
#' @param sort_by Character vector of columns to sort by, in priority
#'                order. Columns not present in the schema are silently
#'                skipped (a warning is emitted if none match).
#' @param verify_stats_on Character vector of columns whose min/max
#'                       stats MUST be populated post-write. Defaults
#'                       to sort_by.
#' @param row_group_size Rows per parquet row group. Default 50,000 —
#'                       sweet spot for WASM-side range-read efficiency.
#' @param compression Codec. Default "zstd".
#' @param compression_level Codec level. Default 9.
#' @param max_avg_chunk_kb Soft ceiling on the average column-chunk
#'                        compressed size (KB) before raising a warning.
#'                        Default 200 — chunks larger than this defeat
#'                        WASM's per-fetch latency budget on cold loads.
#'                        Set NULL to suppress the check.
#' @param ... Reserved for future extension; currently ignored.
write_parquet_pushdown <- function(
  tbl,
  out_path,
  sort_by,
  verify_stats_on   = sort_by,
  row_group_size    = 50000L,
  compression       = "zstd",
  compression_level = 9L,
  max_avg_chunk_kb  = 200,
  ...
) {
  stopifnot(is.character(sort_by), length(sort_by) >= 1L)

  if (!inherits(tbl, "data.table")) tbl <- data.table::as.data.table(tbl)

  sort_cols_present <- intersect(sort_by, names(tbl))
  if (length(sort_cols_present) == 0L) {
    warning(sprintf(
      "write_parquet_pushdown: none of sort_by columns present in tbl: %s",
      paste(sort_by, collapse = ", ")
    ))
  } else {
    data.table::setorderv(tbl, sort_cols_present)
  }

  # Explicit :memory: avoids stale lock files in working_dir.
  # Named .drv so it isn't GC'd before the first query (newer DuckDB).
  .drv <- duckdb::duckdb(dbdir = ":memory:")
  con <- DBI::dbConnect(.drv)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbWriteTable(con, "tbl_src", as.data.frame(tbl), overwrite = TRUE)
  order_by <- if (length(sort_cols_present) > 0L) {
    paste("ORDER BY", paste(sprintf("\"%s\"", sort_cols_present), collapse = ", "))
  } else {
    ""
  }
  DBI::dbExecute(con, sprintf(
    "COPY (SELECT * FROM tbl_src %s)
     TO '%s' (FORMAT PARQUET, COMPRESSION %s, COMPRESSION_LEVEL %d, ROW_GROUP_SIZE %d)",
    order_by, out_path, toupper(compression), compression_level, row_group_size
  ))

  rg_check <- DBI::dbGetQuery(con, sprintf(
    "SELECT COUNT(DISTINCT row_group_id) AS n_groups,
            COUNT(*)                     AS n_chunks,
            SUM(total_compressed_size)   AS bytes_compressed
     FROM parquet_metadata('%s')",
    out_path
  ))
  n_groups   <- rg_check$n_groups
  n_chunks   <- rg_check$n_chunks
  avg_chunk_kb <- if (n_chunks > 0) (rg_check$bytes_compressed / n_chunks) / 1024 else 0

  # Tables smaller than ~2× row_group_size naturally fall in a single
  # row group — pushdown verification is N/A there (the helper still
  # writes the file correctly, just can't subdivide). Skip the >1 RG
  # requirement in that case so small intermediates and lookup tables
  # don't error.
  small_table <- nrow(tbl) < (2L * row_group_size)
  if (!small_table && n_groups < 2L) {
    stop(sprintf(
      paste0("write_parquet_pushdown: %s ended up with %d row group(s); ",
             "row_group_size = %d may be too large for %d rows"),
      out_path, n_groups, row_group_size, nrow(tbl)
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
  if (!is.null(max_avg_chunk_kb) && avg_chunk_kb > max_avg_chunk_kb) {
    warning(sprintf(
      paste0("write_parquet_pushdown: %s avg column-chunk = %.1f KB, ",
             "exceeds soft ceiling %.0f KB. WASM cold-load may stall on ",
             "large per-fetch range requests. Consider lowering row_group_size."),
      out_path, avg_chunk_kb, max_avg_chunk_kb
    ))
  }
  # CR-119: verify row count matches input — catches thrift corruption where
  # a parallel write overwrote the footer leaving dangling row-group bodies.
  verify_n <- DBI::dbGetQuery(con, sprintf(
    "SELECT COUNT(*) AS n FROM read_parquet('%s')", out_path))$n
  if (verify_n != nrow(tbl)) {
    stop(sprintf(
      "write_parquet_pushdown: %s row count post-write %d != input %d (corruption or truncation)",
      out_path, verify_n, nrow(tbl)
    ))
  }
  message(sprintf(
    "write_parquet_pushdown: %s written (%d row groups, %d chunks, %.1f KB avg chunk, stats verified on %s, rows=%d)",
    out_path, n_groups, n_chunks, avg_chunk_kb,
    paste(verify_stats_on, collapse = ", "), verify_n
  ))
  invisible(out_path)
}

#' Write a Cloud-Optimized GeoTIFF using canonical pipeline settings.
#'
#' Canonical COG format for all hazards_prototype pipeline rasters:
#' ZSTD compression (level 9), no overviews (computation intermediates,
#' not map-tile serving), COG layout via terra's filetype = "COG".
#'
#' Use this instead of bare terra::writeRaster() calls so compression
#' settings stay consistent across R/2, R/3, and observational scripts.
#'
#' @param rast  SpatRaster to write.
#' @param filename  Output file path (must end in .tif).
#' @param overwrite Overwrite existing file. Default TRUE.
write_cog <- function(rast, filename, overwrite = TRUE) {
  terra::writeRaster(
    rast, filename,
    overwrite = overwrite,
    filetype  = "COG",
    gdal      = c("COMPRESS=ZSTD", "COMPRESS_LEVEL=9", "OVERVIEWS=NONE")
  )
}

#' Memory-aware worker count for parallel pipeline sections.
#'
#' Reads available RAM from /proc/meminfo (Linux) or sysctl (Mac), then
#' returns a worker count that respects both CPU and memory constraints.
#'
#' @param requested     Maximum workers the user wants (e.g. worker_n2).
#' @param n_tasks       Number of parallel tasks (caps workers — no point
#'                      spinning up more workers than tasks).
#' @param mem_per_worker_gb Estimated peak RAM per worker in GB. Conservative
#'                      default of 5 GB; lower for small-parquet sections,
#'                      higher for sec 3.4 trend computation.
#' @param safety_factor Fraction of available RAM to budget (default 0.7).
#'                      Leaves headroom for OS, R session, and data.table
#'                      internal parallelism.
#' @return Integer worker count, always >= 1.
safe_workers <- function(requested      = 20L,
                         n_tasks        = Inf,
                         mem_per_worker_gb = 5,
                         safety_factor  = 0.7) {
  avail_gb <- tryCatch({
    if (file.exists("/proc/meminfo")) {
      mem  <- readLines("/proc/meminfo", warn = FALSE)
      ln   <- grep("^MemAvailable:", mem, value = TRUE)
      if (length(ln)) as.numeric(sub(".*?(\\d+).*", "\\1", ln[1])) / 1024 / 1024
      else NA_real_
    } else {
      out <- system("sysctl -n hw.memsize", intern = TRUE)
      if (length(out)) as.numeric(out[1]) / 1024^3 else NA_real_
    }
  }, error = function(e) NA_real_)

  mem_limit <- if (!is.na(avail_gb)) {
    floor(avail_gb * safety_factor / mem_per_worker_gb)
  } else {
    requested   # fallback: trust the caller
  }

  n <- min(requested, mem_limit, n_tasks, parallel::detectCores(logical = TRUE))
  n <- max(1L, as.integer(n))

  cat(sprintf(
    "[safe_workers] requested=%d  avail_ram=%.0f GB  mem_per_worker=%.0f GB  limit=%d  -> %d workers\n",
    requested, if (!is.na(avail_gb)) avail_gb else -1, mem_per_worker_gb, mem_limit, n
  ))
  n
}
