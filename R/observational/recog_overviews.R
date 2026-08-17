# recog_overviews.R
# -----------------------------------------------------------------------------
# Re-write already-published monthly COGs that lack internal overviews, adding
# them (+ real embedded stats) so the Quarto dash can render them at zoomed-out
# extent. Needed because scripts 1/2/write_cog historically used OVERVIEWS=NONE
# (fixed forward to OVERVIEWS=AUTO on 2026-08-17, but existing files must be
# re-COGed in place). See feedback_cogs_need_overviews.
#
# Only rewrites files WITHOUT overviews (gdalinfo check) — skips ones already OK.
# Rewrites via write_seasonal_cog (COG + OVERVIEWS=AUTO + 3-step stat roundtrip).
#
# RUN (cglabs): Rscript R/observational/recog_overviews.R            # PTOT + SPEI-03 + SPEI-12
#               Rscript R/observational/recog_overviews.R PTOT
# Then delete the S3 keys for the touched vars and republish --tier 3.
# -----------------------------------------------------------------------------
log_step <- function(m) { cat(format(Sys.time(), "[%H:%M:%S] "), m, "\n", sep = ""); flush.console() }

project_dir <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else getwd()
source(file.path(project_dir, "R", "0_server_setup.R"))
source(file.path(project_dir, "R", "observational", "_seasonal_helpers.R"))  # write_seasonal_cog

args   <- commandArgs(trailingOnly = TRUE)
vars   <- if (length(args)) args else c("PTOT", "SPEI-03", "SPEI-12")
root   <- atlas_dirs$data_dir$chirts_chirps_hist

has_overviews <- function(path) {
  info <- suppressWarnings(system2("gdalinfo", shQuote(path), stdout = TRUE, stderr = FALSE))
  any(grepl("Overviews", info, ignore.case = TRUE))
}

total_scanned <- 0L; total_recog <- 0L
for (v in vars) {
  dir_v <- file.path(root, v)
  if (!dir.exists(dir_v)) { log_step(sprintf("%s: dir missing, skip", v)); next }
  fs <- list.files(dir_v, pattern = sprintf("^%s-[0-9]{4}-[0-9]{2}\\.tif$", v), full.names = TRUE)
  log_step(sprintf("%s: scanning %d COGs", v, length(fs)))
  n <- 0L
  for (f in fs) {
    total_scanned <- total_scanned + 1L
    if (has_overviews(f)) next
    write_seasonal_cog(terra::rast(f), f)   # rewrite in place: COG + overviews + stats
    n <- n + 1L
    if (n %% 50L == 0L) log_step(sprintf("  %s: re-COGed %d so far", v, n))
  }
  total_recog <- total_recog + n
  log_step(sprintf("%s: %d/%d lacked overviews and were re-COGed", v, n, length(fs)))
}
log_step(sprintf("DONE: scanned %d, re-COGed %d (added overviews) across: %s",
  total_scanned, total_recog, paste(vars, collapse = ", ")))
cat("\nNext: delete S3 keys for the touched vars, then republish:\n",
    "  for s in ", paste(vars, collapse = " "), "; do aws s3 rm --recursive \\\n",
    "    \"s3://digital-atlas/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=monthly/variable=$s/\"; done\n",
    "  Rscript R/observational/6_publish_obs_to_s3.R --full --tier 3\n", sep = "")
