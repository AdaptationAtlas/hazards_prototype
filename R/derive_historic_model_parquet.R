# =============================================================================
# derive_historic_model_parquet.R
# -----------------------------------------------------------------------------
# The live Atlas serves HISTORIC hazard_exposure from a separate `model=historic`
# partition; R/3 §4.2.1 only produces `model=ENSEMBLE` (a combined parquet whose
# rows span scenario ∈ {historic, ssp126/245/370/585}). s3_upload.R keys the S3
# `model=` partition off the FILENAME token (x[4]) with NO scenario→model remap,
# so nothing lands at `model=historic/` unless a file literally named with the
# `historic` model token exists.
#
# This derives it: for each combined `*_ENSEMBLE_int_adm_*.parquet`, take the
# scenario=="historic" rows, set model="historic", and write
# `*_historic_int_adm_*.parquet` → publishes to `model=historic/interaction.parquet`
# and supersedes the 2025-06-25 historic product with the new const-I$ values.
# Full-refresh: the ENSEMBLE parquet is still published as `model=ENSEMBLE`
# (all scenarios) for the future view; this only ADDS the historic partition.
#
# ⚠️ UNTESTED on real data (no ENSEMBLE parquet locally) — run after R/3 §4.2.1;
# verify row counts + a spot value before publish.
#
# RUN (cglabs, after R/3): Rscript R/derive_historic_model_parquet.R
# Idempotent (skip if output exists unless FORCE_OVERWRITE). ~2-5 min.
# =============================================================================

.dlog <- function(msg) {
  cat(sprintf("[%s] [derive-historic] %s\n", format(Sys.time(), "%H:%M:%S"), msg))
  flush.console()
}
suppressWarnings(suppressMessages({ library(arrow); library(data.table) }))
source(file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R"))
source(file.path(Sys.getenv("project_dir"), "R", "_helpers.R"))   # write_parquet_pushdown

overwrite <- nzchar(Sys.getenv("FORCE_OVERWRITE"))
timeframes <- c("annual", "jagermeyr")
vop_dirs <- c(
  atlas_dirs$data_dir$hazard_risk_vop,
  atlas_dirs$data_dir$hazard_risk_vop_usd
)

# sort key = full identity (matches R/3 §4.2.1 co-sort); scenario dropped from the
# derived file is fine — all rows are historic — but keep the columns intact.
sort_key <- c("iso3", "admin0_name", "hazard", "crop", "scenario")

n_written <- 0L
for (vd in vop_dirs) {
  for (tf in timeframes) {
    dir_tf <- file.path(vd, tf)
    if (!dir.exists(dir_tf)) next
    en_files <- list.files(dir_tf, "_ENSEMBLE_int_adm_.*\\.parquet$", full.names = TRUE)
    .dlog(sprintf("%s: %d ENSEMBLE parquets", dir_tf, length(en_files)))
    for (f in en_files) {
      out <- file.path(dirname(f), gsub("_ENSEMBLE_int_adm_", "_historic_int_adm_", basename(f)))
      if (file.exists(out) && !overwrite) { .dlog(sprintf("  skip (exists): %s", basename(out))); next }
      dt <- as.data.table(arrow::read_parquet(f))
      if (!"scenario" %in% names(dt)) stop("no scenario column in ", basename(f))
      hist <- dt[scenario == "historic"]
      if (!nrow(hist)) { .dlog(sprintf("  WARN 0 historic rows in %s — skipping", basename(f))); next }
      hist[, model := "historic"]
      .dlog(sprintf("  %s: %d historic / %d total rows -> %s", basename(f), nrow(hist), nrow(dt), basename(out)))
      write_parquet_pushdown(hist, out,
        sort_by = intersect(sort_key, names(hist)),
        verify_stats_on = intersect(c("iso3", "hazard", "crop"), names(hist)))
      # carry the attribute json if present
      if (file.exists(paste0(f, ".json"))) {
        aj <- jsonlite::read_json(paste0(f, ".json"))
        aj$model <- "historic"
        jsonlite::write_json(aj, paste0(out, ".json"))
      }
      n_written <- n_written + 1L
    }
  }
}
.dlog(sprintf("done — wrote %d model=historic parquets", n_written))
cat("\nVERIFY before publish: model=historic row count == ENSEMBLE scenario==historic count;\n",
    "spot-check a (iso3,hazard,crop) value matches; then publish (s3_upload UPLOAD_PARQUET).\n", sep = "")
