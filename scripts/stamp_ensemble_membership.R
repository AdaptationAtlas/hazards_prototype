#!/usr/bin/env Rscript
# scripts/stamp_ensemble_membership.R
# ===================================
# Issue #26 ask 4, applied to hazard_exposure without a re-bake.
#
# The published hazard_exposure parquet has no column that records which GCMs its
# ENSEMBLE aggregate is over, so an 18-member and a 5-member product are
# indistinguishable to a consumer. `R/3_freq_x_exposure.R` §4.2 now writes an
# `ensemble` block into the sidecar it produces, but only on a fresh bake, and a
# bake is hours. This adds the same block to the sidecars ALREADY on disk in
# seconds, so the stamp can ship with the next publish rather than the next bake.
#
# Membership is read off the per-GCM `_int` stacks sitting in each hazard_risk_vop*
# folder, taking the model token with the same anchored `tstrsplit` field-2 rule
# §4.2 applies to `model` - not a regex over GCM names, which carry their own
# hyphen traps (ACCESS-ESM1-5, MPI-ESM1-2-HR, TaiESM1). R/3 keeps its own inline
# copy of this derivation on purpose: it must not gain a `source()` of this file,
# because a missing-file error there would abort a multi-hour bake at §4.2.
#
# Run (cglabs, repo root, seconds, no heavy compute):
#   Rscript scripts/stamp_ensemble_membership.R --dry-run
#   Rscript scripts/stamp_ensemble_membership.R
# Options: --timeframe jagermeyr|annual (default both) --expect 18 --dry-run
# Exit 0 = every folder stamped at the expected member count; 1 = a mismatch.

suppressPackageStartupMessages({ library(data.table); library(jsonlite) })
t0   <- Sys.time()
.ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) { cat(sprintf("[%s] [stamp-ensemble] %s\n", .ts(), sprintf(fmt, ...))); flush.console() }
args <- commandArgs(trailingOnly = TRUE)
opt  <- function(x, d) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
DRY    <- "--dry-run" %in% args
EXPECT <- as.integer(opt("--expect", "18"))
TFS    <- strsplit(opt("--timeframe", "jagermeyr,annual"), ",")[[1]]

setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
.log("sourcing %s", setup)
suppressMessages(suppressWarnings(source(setup)))

# --- the derivation. Mirrors R/3_freq_x_exposure.R §4.2 exactly. -------------
ensemble_membership <- function(folder) {
  files <- list.files(folder, ".tif$", full.names = TRUE)
  sort(setdiff(
    unique(unlist(data.table::tstrsplit(
      basename(files[grepl("_int_", files)]), "_", keep = 2, fixed = TRUE))),
    c("ENSEMBLE", "ENSEMBLEmean", "ENSEMBLEsd", "historic")
  ))
}

roots <- unique(c(atlas_dirs$data_dir$hazard_risk_vop, atlas_dirs$data_dir$hazard_risk_vop_usd))
roots <- roots[!is.na(roots)]
.log("roots: %s", paste(roots, collapse = " | "))

rows <- list(); fail <- FALSE
for (root in roots) for (tf in TFS) {
  folder <- file.path(root, tf)
  if (!dir.exists(folder)) { .log("skip (absent): %s", folder); next }
  t1 <- Sys.time()
  members <- ensemble_membership(folder)
  sidecars <- list.files(folder, "\\.parquet\\.json$", full.names = TRUE)
  .log("%s/%s: %d members, %d sidecars", basename(root), tf, length(members), length(sidecars))
  if (!length(members)) {
    .log("  NONE FOUND - no per-GCM _int stacks in this folder, so membership is not derivable here. NOT stamping.")
    fail <- TRUE; next
  }
  .log("  members = %s", paste(members, collapse = ","))
  if (length(members) != EXPECT) {
    .log("  MISMATCH: %d members, expected %d - this folder's ENSEMBLE is a partial ensemble (#26)", length(members), EXPECT)
    fail <- TRUE
  }
  info <- list(n_members = length(members), members = members,
               derived_from = "model token of the per-GCM _int stacks present in the hazard_risk folder",
               folder = basename(folder),
               stamped_by = "scripts/stamp_ensemble_membership.R", stamped_at = format(Sys.time()))
  n_new <- 0L; n_upd <- 0L
  for (sc in sidecars) {
    a <- tryCatch(jsonlite::read_json(sc, simplifyVector = TRUE), error = function(e) NULL)
    if (is.null(a)) { .log("  unreadable sidecar, skipped: %s", basename(sc)); next }
    had <- !is.null(a$ensemble)
    a$ensemble <- info
    if (!DRY) jsonlite::write_json(a, sc, pretty = TRUE, auto_unbox = TRUE)
    if (had) n_upd <- n_upd + 1L else n_new <- n_new + 1L
  }
  .log("  %s%d sidecars stamped (%d new, %d updated) in %.1fs",
       if (DRY) "[dry run] would have " else "", n_new + n_upd, n_new, n_upd,
       as.numeric(difftime(Sys.time(), t1, units = "secs")))
  rows[[length(rows) + 1L]] <- data.table(root = basename(root), timeframe = tf,
                                          n_members = length(members), sidecars = n_new + n_upd,
                                          members = paste(members, collapse = ","))
}

cat("\n=============================== SUMMARY ===============================\n")
if (length(rows)) print(rbindlist(rows)) else cat("nothing found to stamp\n")
cat(sprintf("VERDICT: %s (expected %d members per folder)%s\n",
            if (!fail && length(rows)) "PASS" else "FAIL", EXPECT,
            if (DRY) "  [DRY RUN - nothing written]" else ""))
cat("=======================================================================\n")
.log("complete in %s", format(round(difftime(Sys.time(), t0, units = "secs"))))
quit(status = if (!fail && length(rows)) 0 else 1)
