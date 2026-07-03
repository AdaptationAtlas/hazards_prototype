# =============================================================================
# validate_glw_vop_vintage.R  (Track-1 publish GATE — VOP-base sanity)
# -----------------------------------------------------------------------------
# WHY: the rebuilt historic hazard_exposure is ~7x the 2025-06-25 live values.
# cattle-highland is LIVESTOCK, so exposure = GLW heads x price. The pipeline
# switched livestock source GLW4 -> GLW4-2020 in 0.4.1 (commits 35375cf
# 2025-07-24 + 69d7b84 2025-09-05) AFTER the live bake, and the switch dropped
# the explicit `_Da.tif` selector (now globs all .tif). A real 2015->2020
# change is ~10% for Angola cattle, NOT 7x — so the jump is most likely a
# unit/selection artifact, not a genuine vintage refresh.
#
# DECISIVE TEST: sum AGO admin0 cattle HEADS from both vintages and compare to
# FAOStat (~5M cattle, Angola, 2020). Native mask+sum == resampled sum because
# the pipeline resample is mass-conserving, so this reflects what 0.4.1 ingests.
#
#   * GLW4-2020 total ~= FAOStat (~5M)  -> base is REAL, 7x is legit -> proceed.
#   * GLW4-2020 ~= 7x FAOStat           -> UNITS BUG in GLW4-2020 rasters/
#                                          selection -> fix 0.4.1 + re-bake VOP
#                                          BEFORE any publish (rebuild is wrong).
#
# Also prints native resolution + per-pixel max + global sum for each vintage —
# a per-km2-vs-per-pixel mismatch shows up as a large res/scale difference.
#
# RUN (cglabs):  Rscript R/validate_glw_vop_vintage.R
# Read-only. Writes nothing. ~1-2 min (mostly setup + raster read).
# =============================================================================

.vlog <- function(msg) {
  cat(sprintf("[%s] [validate-glw] %s\n", format(Sys.time(), "%H:%M:%S"), msg))
  flush.console()
}

suppressWarnings(suppressMessages({
  library(terra)
  library(data.table)
  library(arrow)
  library(sf)
}))

.vlog("sourcing 0_server_setup.R (paths, atlas_dirs, geo files, base raster)")
source(file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R"))

ISO <- "AGO"
FAOSTAT_AGO_CATTLE_2020 <- 5.0e6   # ~5M head (FAOStat live-animals, Angola, 2020) — anchor, verify

# --- Angola admin0 polygon (mirror 0.4.1 L82-87) -----------------------------
.vlog("loading admin0 boundaries")
geob <- arrow::read_parquet(geo_files_local[1]) |>
  sf::st_as_sf() |>
  terra::vect()
geob <- terra::aggregate(geob, "iso3")
ago <- geob[geob$iso3 == ISO, ]
if (nrow(ago) == 0) stop("AGO polygon not found in ", geo_files_local[1])

# --- helper: pick the cattle layer + report AGO sum + unit hints -------------
report_vintage <- function(label, r_cattle) {
  res_m  <- terra::res(r_cattle)
  gtot   <- terra::global(r_cattle, "sum", na.rm = TRUE)[1, 1]
  pmax   <- terra::global(r_cattle, "max", na.rm = TRUE)[1, 1]
  ago_tot <- terra::extract(r_cattle, ago, fun = "sum", na.rm = TRUE, ID = FALSE)[1, 1]
  .vlog(sprintf("%s | native res=%.5f deg | per-pixel max=%.1f | GLOBAL sum=%.3g | AGO cattle=%.0f (%.2fM)",
                label, res_m[1], pmax, gtot, ago_tot, ago_tot / 1e6))
  ago_tot
}

# --- GLW4-2020 (what the rebuild used) ---------------------------------------
.vlog(sprintf("GLW4-2020 dir: %s", atlas_dirs$data_dir$GLW4_2020))
new_files <- list.files(atlas_dirs$data_dir$GLW4_2020, ".tif$", full.names = TRUE)
.vlog(sprintf("GLW4-2020 files (%d): %s", length(new_files),
              paste(basename(new_files), collapse = ", ")))
new_stk <- terra::rast(new_files)
new_codes <- unlist(tstrsplit(names(new_stk), "_", keep = 1))
new_cattle_idx <- which(new_codes == "Ct")
if (length(new_cattle_idx) != 1) stop("GLW4-2020: expected exactly 1 'Ct' cattle layer, found ", length(new_cattle_idx))
ago_new <- report_vintage("GLW4-2020 (rebuild)", new_stk[[new_cattle_idx]])

# --- GLW4 original 2015 _Da (what the 2025-06-25 live product used) ----------
old_cattle_path <- file.path(atlas_dirs$data_dir$GLW4, "5_Ct_2015_Da.tif")
ago_old <- NA_real_
if (file.exists(old_cattle_path)) {
  .vlog(sprintf("GLW4 (old 2015 _Da): %s", old_cattle_path))
  ago_old <- report_vintage("GLW4 2015 _Da (live)", terra::rast(old_cattle_path))
} else {
  .vlog(sprintf("GLW4 old cattle file NOT present (%s) — old vintage may be purged; rely on FAOStat anchor", old_cattle_path))
}

# --- verdict -----------------------------------------------------------------
cat("\n=============================== VERDICT ===============================\n")
cat(sprintf("FAOStat anchor (AGO cattle, 2020)   : ~%.2fM head\n", FAOSTAT_AGO_CATTLE_2020 / 1e6))
cat(sprintf("GLW4-2020 AGO cattle (rebuild)      : %.2fM head  (%.1fx FAOStat)\n",
            ago_new / 1e6, ago_new / FAOSTAT_AGO_CATTLE_2020))
if (!is.na(ago_old)) {
  cat(sprintf("GLW4 2015 _Da AGO cattle (live base): %.2fM head  (%.1fx FAOStat)\n",
              ago_old / 1e6, ago_old / FAOSTAT_AGO_CATTLE_2020))
  cat(sprintf("GLW4-2020 / GLW4-2015 ratio         : %.2fx\n", ago_new / ago_old))
}
cat("\nINTERPRETATION:\n")
cat(" - GLW4-2020 ~= FAOStat (0.7-1.5x)  -> head-count REAL; 7x exposure is a legit VOP-base\n")
cat("                                       refresh -> proceed to publish-scope decision.\n")
cat(" - GLW4-2020 ~= 5-8x FAOStat        -> UNITS BUG in GLW4-2020 rasters/selection\n")
cat("                                       (per-km2 vs per-pixel, or wrong product) -> FIX\n")
cat("                                       0.4.1 + re-bake VOP BEFORE publish.\n")
cat(" - Also compare native res / per-pixel max between vintages: a big gap = unit change.\n")
cat("======================================================================\n")
