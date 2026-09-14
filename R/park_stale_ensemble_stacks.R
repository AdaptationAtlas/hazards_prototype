#!/usr/bin/env Rscript
# R/park_stale_ensemble_stacks.R
# ==============================
# Issue #9, TIER B: the §5.2 ENSEMBLEmean / ENSEMBLEsd interaction stacks for the
# PTOT-* combos predate the hazard='none' layer (41c1c00), while their per-GCM
# parents carry it. R/2 §5.2's ensemble step rebuilds an ensemble pair only when
# `save_file_mean` is missing (or FORCE). This script PARKS (moves, never rm)
# every ENSEMBLEmean/ENSEMBLEsd stack whose layer names lack `_none` into a
# `Data/_parked_issue9/<STAMP>/hazard_timeseries_int/<tf>/` dir (OUTSIDE the tf dir) so that `RUN_R2_RUN5_2=1` (FORCE unset)
# regenerates exactly those from the per-GCM stacks. Pairs are moved together
# (§5.2 only checks the mean file's existence).
#
# Decided by LAYER CONTENT, not by combo-name pattern -> robust to naming.
# Header reads only; ~1-3 min for 2 x ~4.5k ensemble files.
#
# Usage (cglabs, repo root):
#   Rscript R/park_stale_ensemble_stacks.R --dry-run     # list + counts, nothing moved
#   Rscript R/park_stale_ensemble_stacks.R               # move
# Optional: PROBE_TIMEFRAMES=jagermeyr

t0 <- Sys.time()
.ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) cat(sprintf("[%s] [park-ens] %s\n", .ts(), sprintf(fmt, ...)))
args <- commandArgs(trailingOnly = TRUE)
DRY_RUN <- "--dry-run" %in% args
STAMP <- format(Sys.time(), "%Y%m%d_%H%M%S")

setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else
  file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
if (nzchar(Sys.getenv("ATLAS_SETUP_SKIP"))) {
  .log("ATLAS_SETUP_SKIP set - expecting atlas_dirs in the calling env"); stopifnot(exists("atlas_dirs"))
} else { .log("sourcing %s", setup); suppressMessages(suppressWarnings(source(setup))) }
suppressPackageStartupMessages({ pacman::p_load(terra, data.table) })

has_none <- function(f) {
  nm <- tryCatch(names(suppressWarnings(terra::rast(f))), error = function(e) NA_character_)
  if (length(nm) == 1 && is.na(nm)) return(NA)
  any(grepl("_none($|_)", nm))
}

int_root <- atlas_dirs$data_dir$hazard_timeseries_int
timeframes <- basename(list.dirs(int_root, recursive = FALSE))
tf_env <- Sys.getenv("PROBE_TIMEFRAMES", "")
timeframes <- timeframes[!grepl("^_|^errors$", timeframes)]
if (nzchar(tf_env)) timeframes <- intersect(timeframes, strsplit(tf_env, ",")[[1]])
.log("%s | root=%s | timeframes=%s", if (DRY_RUN) "DRY RUN" else "MOVING", int_root, paste(timeframes, collapse = ","))

totals <- list()
for (tf in timeframes) {
  d <- file.path(int_root, tf)
  ens <- list.files(d, "_ENSEMBLE(mean|sd)_.*\\.tif$", full.names = TRUE)
  .log("%s: %d ENSEMBLE(mean|sd) stacks", tf, length(ens))
  if (!length(ens)) next
  E <- data.table(file = ens, base = basename(ens))
  E[, key := sub("_ENSEMBLE(mean|sd)_", "_ENSEMBLE_", base)]         # pair key (mean+sd share it)
  E[, kind := ifelse(grepl("_ENSEMBLEmean_", base), "mean", "sd")]
  E[, none := vapply(file, has_none, logical(1))]
  # a pair is stale if EITHER member lacks none (or is unreadable)
  E[, stale := any(!(none %in% TRUE)), by = key]
  E[, combo := sub("^[^_]+_[^_]+_[^_]+_", "", tools::file_path_sans_ext(base))]
  summ <- E[, .(n = .N, n_stale = sum(stale), n_unreadable = sum(is.na(none))), by = .(combo_family = sub("[-+].*$", "", combo))][order(combo_family)]
  cat("  by combo family (first token):\n"); print(summ, nrows = 50)
  st <- E[stale == TRUE]
  .log("%s: %d stale stacks (%d pairs) to park | %d already carry none", tf, nrow(st), uniqueN(st$key), sum(E$none %in% TRUE))
  if (nrow(st)) {
    cat("  stale combos:", paste(sort(unique(st$combo)), collapse = "\n                "), "\n")
    # park OUTSIDE hazard_timeseries_int/<tf>: §5.3 does list.files(haz_time_int_dir) with no
    # pattern and splits basenames on "_" -> a subdir there becomes a bogus model row -> stop().
    park <- file.path(dirname(int_root), "_parked_issue9", STAMP, "hazard_timeseries_int", tf)
    if (DRY_RUN) .log("%s: [dry run] would move %d files -> %s", tf, nrow(st), park)
    else {
      dir.create(park, showWarnings = FALSE, recursive = TRUE)
      ok <- file.rename(st$file, file.path(park, st$base))
      .log("%s: moved %d/%d -> %s", tf, sum(ok), nrow(st), park)
      if (!all(ok)) stop("some moves failed in ", tf)
      .log("%s: remaining ENSEMBLE stacks in dir = %d (expect %d)", tf,
           length(list.files(d, "_ENSEMBLE(mean|sd)_.*\\.tif$")), nrow(E) - nrow(st))
    }
  }
  totals[[tf]] <- c(total = nrow(E), stale = nrow(st))
}
cat("\n"); for (tf in names(totals)) cat(sprintf("  %-10s total=%d stale=%d\n", tf, totals[[tf]]["total"], totals[[tf]]["stale"]))
.log("done in %.1f min%s", as.numeric(difftime(Sys.time(), t0, units = "mins")), if (DRY_RUN) " [DRY RUN - nothing moved]" else "")
