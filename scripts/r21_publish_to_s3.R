#!/usr/bin/env Rscript
# scripts/r21_publish_to_s3.R
# ===========================
# Publish R/2.1 outputs to the canonical domain=climate S3 path consumed
# by the climateRationale notebook's Future Projections section.
#
# Background:
#   R/2.1 writes local parquets named *_anomaly-<baseline>_ensemble_seasons.parquet.
#   push_to_s3.R uses the LEGACY s3://digital-atlas/hazards/hazard_timeseries_mean_month
#   path; the notebook reads the NEWER domain=climate/... structure. No existing
#   publisher maps the local outputs to the correct path — this script does it.
#   See memory feedback_r21_publish_path for the full context.
#
# ALWAYS verify exact local filenames first:
#   list.files(atlas_dirs$data_dir$hazard_timeseries_mean_month,
#              "_ensemble_seasons\\.parquet$", full.names = TRUE)
#
# Usage (from project root after 0_server_setup.R):
#   Rscript scripts/r21_publish_to_s3.R [--dry-run]
#
# Convention: download+upload with ACL="public-read" (NOT s3_file_copy — strips ACL).

source("R/0_server_setup.R")
suppressPackageStartupMessages({ pacman::p_load(s3fs) })

args   <- commandArgs(trailingOnly = TRUE)
DRY_RUN <- "--dry-run" %in% args

BUCKET   <- "digital-atlas"
PREFIX   <- paste0(
  "domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/",
  "processing=timeseries_mean_month/timeframe=3months"
)
FUTURE_PERIODS <- c("2021-2040", "2041-2060", "2061-2080", "2081-2100")

output_dir <- atlas_dirs$data_dir$hazard_timeseries_mean_month

cat("=== R/2.1 -> S3 publish", if (DRY_RUN) "[DRY RUN]" else "", "===\n")
cat("output_dir =", output_dir, "\n\n")

# Issue #26: R/2.1 names anomaly outputs by the baseline WINDOW
# (*_anomaly-<window>_ensemble_seasons.parquet, e.g. anomaly-1995-2014), one
# set per historic window present (1981-2014 and 1995-2014 are both kept).
# The interim "anomaly-historic" naming is gone; if such files exist here they
# are stale collapsed-historic outputs and must not be published.
stale <- list.files(output_dir, "_anomaly-historic_", full.names = FALSE)
if (length(stale) > 0) {
  stop(length(stale), " stale anomaly-historic files present (e.g. ", stale[1],
       ") — pre-delete the output dir remnants before publishing (issue #26).")
}

all_files <- list.files(output_dir, "_anomaly-[0-9]{4}-[0-9]{4}_ensemble_seasons\\.parquet$", full.names = TRUE)
cat("Found anomaly-<window> ensemble_seasons parquets:", length(all_files), "\n")
if (length(all_files) == 0) stop("No anomaly-<window>_ensemble_seasons parquets found — has R/2.1 run?")

BASELINES <- sort(unique(sub(".*_anomaly-([0-9]{4}-[0-9]{4})_.*", "\\1", basename(all_files))))
cat("Baselines found:", paste(BASELINES, collapse = ", "), "\n\n")

for (b in BASELINES) {
  # Each baseline publishes the 4 future periods plus its own historic window
  # (period=<window>/baseline=<window> — anomaly of the window vs its own mean).
  for (p in c(b, FUTURE_PERIODS)) {
    fname   <- sprintf("haz_3months_adm_mean_%s_anomaly-%s_ensemble_seasons.parquet", p, b)
    local_f <- file.path(output_dir, fname)

    if (!file.exists(local_f)) {
      warning("No local file for period=", p, " baseline=", b, " (", fname, ") — skipping")
      next
    }
    s3_key <- sprintf("%s/period=%s/baseline=%s/variable=ensemble_season_timeseries.parquet",
                      PREFIX, p, b)
    s3_url <- sprintf("s3://%s/%s", BUCKET, s3_key)

    cat(sprintf("period=%s baseline=%s\n  local : %s\n  s3    : %s\n", p, b, fname, s3_url))

    if (!DRY_RUN) {
      s3fs::s3_file_upload(local_f, s3_url, ACL = "public-read", overwrite = TRUE)
      # Verify
      info <- s3fs::s3_file_info(s3_url)
      cat(sprintf("  -> uploaded %s bytes (mtime %s)\n", info$size, info$modification_time))
    } else {
      cat("  -> [dry run] skipped upload\n")
    }
    cat("\n")
  }
}

cat("=== PUBLISH", if (DRY_RUN) "DRY RUN" else "COMPLETE", "===\n")
if (!DRY_RUN) {
  cat("Notebook consumers will pick up new parquets on next cold-start.\n")
  cat("Run Stage 7 verify:\n")
  cat("  duckdb -c \"INSTALL httpfs; LOAD httpfs;\n")
  cat("  SELECT COUNT(*) FROM read_parquet('https://digital-atlas.s3.amazonaws.com/")
  cat(sprintf("%s/period=2021-2040/baseline=%s/variable=ensemble_season_timeseries.parquet')\n",
              PREFIX, BASELINES[length(BASELINES)]))
  cat("  WHERE iso3='AGO' AND season='annual';\"\n")
}
