# Shared helpers for the R/checks/* probe scripts.
#
# Source me after sourcing R/0_server_setup.R:
#   source("R/checks/_helpers.R")
#
# Exports:
#   log_step(msg, ...)        — timestamped + pid-tagged single line.
#   log_section(title)        — banner separator + log_step.
#   log_timer(expr, label)    — wraps an expression, logs elapsed time.
#   parse_cli(args, name, ..) — read --name <value> from commandArgs.
#   countries_aoi(geob, iso3) — subset geoboundaries to ISO3 set.
#   sample_files(files, n)    — deterministic stratified sample (by
#                                scenario / model / timeframe prefix).
#   summarize_log(rows)       — pretty-print a per-stage timing table.

log_step <- function(msg, ...) {
  ts <- format(Sys.time(), "%H:%M:%S")
  cat(sprintf("[%s][pid %d] %s\n", ts, Sys.getpid(), sprintf(msg, ...)))
  flush.console()
}

log_section <- function(title) {
  cat("\n", strrep("=", 70), "\n", sep = "")
  log_step("==== %s ====", title)
  cat(strrep("=", 70), "\n\n", sep = "")
}

# Tracks per-step elapsed seconds in a hidden environment for summary.
.checks_timer_env <- new.env(parent = emptyenv())
.checks_timer_env$rows <- list()

log_timer <- function(expr, label) {
  log_step("START  %s", label)
  t0 <- Sys.time()
  res <- force(expr)
  elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  log_step("END    %s  (%.1f s)", label, elapsed)
  .checks_timer_env$rows[[length(.checks_timer_env$rows) + 1L]] <-
    data.frame(label = label, secs = round(elapsed, 1))
  invisible(res)
}

#' Parse `--name value` from commandArgs(trailingOnly=TRUE)
parse_cli <- function(args, name,
                      type = c("character", "integer", "double", "logical"),
                      default = NA) {
  type <- match.arg(type)
  i <- match(paste0("--", name), args)
  if (is.na(i) || i == length(args)) return(default)
  v <- args[i + 1L]
  switch(type,
    integer   = as.integer(v),
    double    = as.numeric(v),
    logical   = as.logical(v),
    character = v
  )
}

#' Subset a SpatVector / sf of admin0 boundaries to a set of ISO3 codes.
#' Defaults to the full input if iso3 is NULL / "ALL" / "AFRICA".
countries_aoi <- function(geob, iso3) {
  if (is.null(iso3) || length(iso3) == 0L) return(geob)
  if (any(toupper(iso3) %in% c("ALL", "AFRICA"))) return(geob)
  sel <- geob[geob$iso3 %in% iso3, ]
  if (length(sel) == 0L) {
    warning("countries_aoi: no boundaries match iso3 = ",
            paste(iso3, collapse = ","), "; returning full input.")
    return(geob)
  }
  sel
}

#' Crop a raster to the AOI's extent before further computation. Sets
#' terra::window() so subsequent reads are windowed — much faster than
#' loading the full grid only to global() over a small ROI.
window_to_aoi <- function(r, aoi) {
  if (is.null(aoi) || length(aoi) == 0L) return(r)
  e <- terra::ext(aoi)
  # Snap-out so we never cut into the AOI boundary cells.
  r2 <- terra::crop(r, e, snap = "out")
  terra::mask(r2, aoi)
}

#' Stratified random sample of N filenames, stratified by the prefix
#' (everything before the first "_") so a `sample` retains coverage
#' across scenarios + models + timeframes.
sample_files <- function(files, n) {
  if (is.na(n) || n <= 0L || length(files) <= n) return(files)
  strata <- vapply(basename(files), function(b) {
    sub("_.*$", "", b)
  }, character(1))
  set.seed(1)
  unique_strata <- unique(strata)
  per <- ceiling(n / length(unique_strata))
  picks <- unlist(lapply(unique_strata, function(s) {
    pool <- files[strata == s]
    sample(pool, min(per, length(pool)))
  }))
  head(picks, n)
}

#' Render the timer summary at the end of a probe run.
summarize_log <- function() {
  if (length(.checks_timer_env$rows) == 0L) return(invisible())
  tab <- do.call(rbind, .checks_timer_env$rows)
  tab <- tab[order(-tab$secs), ]
  cat("\n", strrep("-", 70), "\n", sep = "")
  log_step("Stage timings (top to bottom = slowest first)")
  cat(strrep("-", 70), "\n", sep = "")
  print(tab, row.names = FALSE)
  total <- sum(tab$secs)
  log_step("TOTAL  %.1f s (%.1f min)", total, total / 60)
  invisible(tab)
}

# Memory snapshot (relies on _helpers.R's parent pipeline already
# loading utilities; we just expose a thin wrapper).
log_mem <- function(label = "") {
  if (requireNamespace("pryr", quietly = TRUE)) {
    mb <- as.numeric(pryr::mem_used()) / 1024 / 1024
    log_step("MEM   %.0f MB  %s", mb, label)
  } else {
    log_step("MEM   (install pryr for memory snapshot)  %s", label)
  }
}

# Loud end-of-script banner so `tail -f` / log files clearly show the
# probe terminated normally. Anything that runs after this banner is
# either an error trap or out-of-band cleanup — if you don't see this
# in the log, the script crashed silently.
log_complete <- function(label = "", extra = NULL) {
  cat("\n")
  cat(strrep("#", 70), "\n", sep = "")
  cat(strrep("#", 70), "\n", sep = "")
  log_step("##### COMPLETE: %s #####", label)
  if (!is.null(extra)) {
    for (line in extra) log_step("##### %s", line)
  }
  log_step("##### exit status 0; log file finalized %s #####",
           format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
  cat(strrep("#", 70), "\n", sep = "")
  cat(strrep("#", 70), "\n\n", sep = "")
  flush.console()
}
