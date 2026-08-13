# fix_spei_inf.R
# -----------------------------------------------------------------------------
# One-off cleanup: clamp non-finite (+/-Inf) pixels to NA in already-written
# SPEI COGs and re-embed correct STATISTICS tags. A few degenerate cells produce
# +/-Inf from the log-Logistic fit; GDAL then stamped -9999/-inf into the COG
# STATISTICS tags, breaking auto colour-scaling for consumers that trust
# embedded min/max (2026-08-13: 2 -Inf px of 2.4M). 2_calculate_obs_spei.R is
# now patched to clamp before write, but its skip-if-exists means existing files
# won't be recomputed — hence this in-place fix.
#
# Only rewrites files that actually contain Inf (cheap check first). Rewrites via
# write_seasonal_cog (COG + 3-step stat roundtrip = clean min/mean + overviews).
#
# RUN (cglabs): Rscript R/observational/fix_spei_inf.R            # default SPEI-03,SPEI-12
#               Rscript R/observational/fix_spei_inf.R SPEI-03 SPEI-06 SPEI-12
# Then delete the S3 keys for the touched scales and republish --tier 3.
# -----------------------------------------------------------------------------

log_step <- function(msg) { cat(format(Sys.time(), "[%H:%M:%S] "), msg, "\n", sep = ""); flush.console() }

project_dir <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else getwd()
source(file.path(project_dir, "R", "0_server_setup.R"))
source(file.path(project_dir, "R", "observational", "_seasonal_helpers.R"))  # write_seasonal_cog

args <- commandArgs(trailingOnly = TRUE)
scales <- if (length(args)) args else c("SPEI-03", "SPEI-12")
root <- atlas_dirs$data_dir$chirts_chirps_hist

total_scanned <- 0L; total_fixed <- 0L
for (sc in scales) {
  dir_sc <- file.path(root, sc)
  if (!dir.exists(dir_sc)) { log_step(sprintf("%s: dir missing, skip", sc)); next }
  fs <- list.files(dir_sc, pattern = sprintf("^%s-[0-9]{4}-[0-9]{2}\\.tif$", sc), full.names = TRUE)
  log_step(sprintf("%s: scanning %d COGs", sc, length(fs)))
  fixed <- 0L
  for (f in fs) {
    r <- terra::rast(f)
    n_inf <- terra::global(terra::ifel(is.infinite(r), 1, 0), "sum", na.rm = TRUE)[[1]]
    total_scanned <- total_scanned + 1L
    if (is.finite(n_inf) && n_inf > 0) {
      r <- terra::ifel(is.infinite(r), NA, r)
      write_seasonal_cog(r, f)                       # rewrite in place + real stats
      fixed <- fixed + 1L
      log_step(sprintf("  fixed %s (%d Inf px -> NA)", basename(f), as.integer(n_inf)))
    }
  }
  total_fixed <- total_fixed + fixed
  log_step(sprintf("%s: %d/%d files had Inf and were rewritten", sc, fixed, length(fs)))
}
log_step(sprintf("DONE: scanned %d, rewrote %d SPEI COGs across scales: %s",
  total_scanned, total_fixed, paste(scales, collapse = ", ")))
cat("\nNext: delete S3 keys for the touched scales, then republish:\n",
    "  for s in ", paste(scales, collapse = " "), "; do aws s3 rm --recursive \\\n",
    "    \"s3://digital-atlas/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=monthly/variable=$s/\"; done\n",
    "  Rscript R/observational/6_publish_obs_to_s3.R --full --tier 3\n", sep = "")
