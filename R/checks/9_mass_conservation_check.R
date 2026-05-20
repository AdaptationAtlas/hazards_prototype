# Mass-conservation probe for the bilinear-resample bug (issue #9).
#
# Loads the GLW + Worldpop source rasters at their native resolution,
# applies BOTH the current density / bilinear-resample / density-back
# pattern AND the proposed method="sum" replacement, and reports
# country-level mass ratios (dst / src) for each. Mass-conserving
# resamples have ratio ≈ 1.000.
#
# Workspace convention: source R/0_server_setup.R so all paths, the
# atlas base raster, and the geoboundaries vector resolve via the
# canonical project_dir / working_dir setup (CGlabs / Aflabs / Mac /
# Windows all handled there).
#
# Usage (from project_dir):
#   Rscript R/checks/9_mass_conservation_check.R [--country AGO,NGA,CIV]

# ----- Workspace bootstrap (same pattern as observational pipeline) -------
project_dir <- if (nzchar(Sys.getenv("project_dir"))) {
  Sys.getenv("project_dir")
} else {
  getwd()
}
setwd(project_dir)
source("R/0_server_setup.R")
# 0_server_setup.R now exposes: working_dir, base_rast, base_rast_path,
# geoboundaries (SpatVector, iso3-aggregated), glw_dir, hpop_dir,
# boundaries_dir, atlas_dirs, Cglabs, ...

suppressPackageStartupMessages({
  pacman::p_load(terra, data.table, jsonlite)
})

# ----- CLI ---------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
country_arg <- {
  i <- match("--country", args)
  if (!is.na(i) && i < length(args)) args[i + 1L] else "AGO"
}
countries <- strsplit(country_arg, ",")[[1]]

cat("\n=== Mass-conservation probe (issue #9) ===\n")
cat(sprintf("  project_dir = %s\n", project_dir))
cat(sprintf("  working_dir = %s\n", getwd()))
cat(sprintf("  Cglabs      = %s\n", isTRUE(Cglabs)))
cat(sprintf("  countries   = %s\n", paste(countries, collapse = ", ")))
if (is.null(base_rast)) {
  stop("base_rast is NULL — 0_server_setup.R did not load a base raster. ",
       "Run 1_make_timeseries.R first or check climdat_source.")
}
cat(sprintf("  base_rast   = res=%s, ext=%s\n\n",
            paste(round(res(base_rast), 4), collapse = "x"),
            paste(round(as.vector(ext(base_rast)), 2), collapse = ",")))

# ----- Generic probe helper -----------------------------------------------
probe_one <- function(src_rast, label, countries, base_rast, vect_all) {
  # bilinear: density / resample / density-back (CURRENT code path)
  src_density <- src_rast / terra::cellSize(src_rast, unit = "ha")
  dst_bilin <- terra::resample(src_density, base_rast) # default = bilinear
  dst_bilin <- dst_bilin * terra::cellSize(dst_bilin, unit = "ha")
  # method = "sum": mass-conserving (PROPOSED fix)
  dst_sum <- terra::resample(src_rast, base_rast, method = "sum")

  rbindlist(lapply(countries, function(iso) {
    aoi <- vect_all[vect_all$iso3 == iso, ]
    if (nrow(aoi) == 0L) {
      return(data.table(layer = label, iso3 = iso,
                        src = NA_real_, bilinear = NA_real_, sum = NA_real_,
                        ratio_bilin = NA_real_, ratio_sum = NA_real_))
    }
    m_src <- terra::global(terra::mask(terra::crop(src_rast, aoi), aoi),
                           "sum", na.rm = TRUE)[, 1]
    m_bil <- terra::global(terra::mask(terra::crop(dst_bilin, aoi), aoi),
                           "sum", na.rm = TRUE)[, 1]
    m_sum <- terra::global(terra::mask(terra::crop(dst_sum, aoi), aoi),
                           "sum", na.rm = TRUE)[, 1]
    data.table(layer = label, iso3 = iso,
               src = m_src, bilinear = m_bil, sum = m_sum,
               ratio_bilin = m_bil / m_src, ratio_sum = m_sum / m_src)
  }))
}

results <- list()

# ----- (1) GLW cattle ----------------------------------------------------
glw_file <- file.path(glw_dir, "5_Ct_2015_Da.tif")
if (file.exists(glw_file)) {
  cat("[1] GLW cattle (5_Ct_2015_Da.tif)\n")
  src_glw <- terra::rast(glw_file)
  src_glw <- terra::crop(src_glw, geoboundaries)
  results$glw_cattle <- probe_one(src_glw, "GLW cattle", countries,
                                   base_rast, geoboundaries)
  print(results$glw_cattle); cat("\n")
} else {
  cat("[1] GLW cattle SKIPPED — not at", glw_file, "\n\n")
}

# ----- (2) Worldpop hpop -------------------------------------------------
hpop_files <- list.files(hpop_dir, "\\.tif$", full.names = TRUE)
hpop_files <- hpop_files[!grepl("intermediate|processed", hpop_files)]
if (length(hpop_files) > 0L) {
  cat(sprintf("[2] Worldpop hpop (%s)\n", basename(hpop_files[1])))
  src_hpop <- terra::rast(hpop_files[1])
  if (terra::nlyr(src_hpop) > 1L) src_hpop <- src_hpop[[1]]
  src_hpop <- terra::crop(src_hpop, geoboundaries)
  results$hpop <- probe_one(src_hpop, "Worldpop hpop", countries,
                             base_rast, geoboundaries)
  print(results$hpop); cat("\n")
} else {
  cat("[2] Worldpop hpop SKIPPED — no .tif under", hpop_dir, "\n\n")
}

# ----- Summary -----------------------------------------------------------
cat("=========== SUMMARY ===========\n")
all <- rbindlist(results, fill = TRUE)
print(all)
cat("\n")
cat(sprintf("Bilinear ratios outside [0.995, 1.005]: %d / %d rows\n",
            sum(abs(all$ratio_bilin - 1) > 0.005, na.rm = TRUE),
            sum(!is.na(all$ratio_bilin))))
cat(sprintf("Sum ratios outside     [0.995, 1.005]: %d / %d rows\n",
            sum(abs(all$ratio_sum - 1) > 0.005, na.rm = TRUE),
            sum(!is.na(all$ratio_sum))))
cat("\n")
if (any(abs(all$ratio_bilin - 1) > 0.005, na.rm = TRUE)) {
  cat("DECISION: bilinear NOT mass-conserving. Proceed to Stage 2 of dispatch.\n")
} else {
  cat("DECISION: bilinear is mass-conserving within tolerance. ",
      "Diagnosis was wrong - escalate.\n", sep = "")
}

# ----- JSON record -------------------------------------------------------
out_dir <- file.path(project_dir, "metadata", "checks")
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
out_path <- file.path(out_dir, "9_mass_conservation_check.json")
jsonlite::write_json(
  list(timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
       project_dir = project_dir,
       Cglabs = isTRUE(Cglabs),
       countries = countries,
       results = all),
  out_path, auto_unbox = TRUE, pretty = TRUE
)
cat(sprintf("Wrote report to %s\n", out_path))
