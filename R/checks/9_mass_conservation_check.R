# Mass-conservation probe for the bilinear-resample bug (issue #9).
#
# Loads the GLW + Worldpop source rasters at their native resolution,
# applies BOTH the current density / bilinear-resample / density-back
# pattern AND the proposed method="sum" replacement, and reports
# country-level mass ratios (dst / src) for each. Mass-conserving
# resamples have ratio ≈ 1.000.
#
# Designed to run on CGlabs (paths under /home/jovyan/*). Falls back to
# the Mac dev layout when running locally if the data is staged.
#
# Usage:
#   Rscript R/checks/9_mass_conservation_check.R [--country AGO,NGA,CIV]

suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) {
    install.packages("pacman", repos = "https://cloud.r-project.org")
  }
  pacman::p_load(terra, data.table, sf, geoarrow, arrow, jsonlite)
})

# Path resolution (mirrors 0_server_setup.R's branch logic) ----------------
project_dir <- if (nzchar(Sys.getenv("project_dir"))) {
  Sys.getenv("project_dir")
} else {
  getwd()
}
setwd(project_dir)

cglabs <- project_dir == "/home/jovyan/atlas/hazards_prototype"

if (cglabs) {
  data_root <- "/home/jovyan/common_data/hazards_prototype/Data"
} else {
  data_root <- normalizePath(file.path(project_dir, "..",
                                       "common_data", "hazards_prototype", "Data"),
                             mustWork = FALSE)
}

base_rast_path <- if (cglabs) {
  "/home/jovyan/common_data/nex-gddp-cmip6/pr/ssp126/ACCESS-CM2/pr_2021-01-01.tif"
} else {
  file.path(project_dir, "metadata", "base_raster.tif")
}

glw_dir   <- file.path(data_root, "GLW4")
hpop_dir  <- file.path(data_root, "atlas_pop")
boundaries_dir <- file.path(data_root, "..", "boundaries")
boundaries_files <- c(
  list.files(boundaries_dir, "admin0", full.names = TRUE, recursive = TRUE),
  list.files(file.path(project_dir, "metadata"), "admin0", full.names = TRUE)
)

# CLI ----------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
country_arg <- {
  i <- match("--country", args)
  if (!is.na(i) && i < length(args)) args[i + 1L] else "AGO"
}
countries <- strsplit(country_arg, ",")[[1]]

cat("Mass-conservation probe — issue #9\n")
cat(sprintf("  project_dir = %s\n", project_dir))
cat(sprintf("  data_root   = %s\n", data_root))
cat(sprintf("  countries   = %s\n", paste(countries, collapse = ", ")))
cat(sprintf("  cglabs      = %s\n\n", cglabs))

# Boundaries ---------------------------------------------------------------
boundary_pq <- Sys.glob(file.path(boundaries_dir, "*admin0*.parquet"))[1]
if (is.na(boundary_pq) || !file.exists(boundary_pq)) {
  stop("admin0 boundary parquet not found under: ", boundaries_dir,
       " — set boundaries_dir manually before running.")
}
geob <- arrow::read_parquet(boundary_pq)
geob <- sf::st_as_sf(geob, crs = 4326)
geob <- aggregate(geob[, "iso3"], by = list(iso3 = geob$iso3), FUN = first)
boundaries_vect <- terra::vect(geob)

# Base raster --------------------------------------------------------------
if (!file.exists(base_rast_path)) {
  stop("base raster not found: ", base_rast_path)
}
base_rast <- terra::rast(base_rast_path)[[1]]
cat(sprintf("base_rast: res=%s, ext=%s\n",
            paste(round(res(base_rast), 4), collapse = "x"),
            paste(round(as.vector(ext(base_rast)), 2), collapse = ",")))

# Generic probe helper -----------------------------------------------------
probe_one <- function(src_rast, label, countries, base_rast, vect_all) {
  # bilinear: density / resample / density-back (current code path)
  src_density <- src_rast / terra::cellSize(src_rast, unit = "ha")
  dst_bilin <- terra::resample(src_density, base_rast)               # default method
  dst_bilin <- dst_bilin * terra::cellSize(dst_bilin, unit = "ha")
  # method="sum": mass-conserving (proposed fix)
  dst_sum <- terra::resample(src_rast, base_rast, method = "sum")

  out <- rbindlist(lapply(countries, function(iso) {
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
    data.table(
      layer       = label,
      iso3        = iso,
      src         = m_src,
      bilinear    = m_bil,
      sum         = m_sum,
      ratio_bilin = m_bil / m_src,
      ratio_sum   = m_sum / m_src
    )
  }))
  out
}

results <- list()

# (1) GLW cattle (5_Ct_2015_Da.tif) ---------------------------------------
glw_file <- file.path(glw_dir, "5_Ct_2015_Da.tif")
if (file.exists(glw_file)) {
  cat("\n[1] GLW cattle (5_Ct_2015_Da.tif)\n")
  src_glw <- terra::rast(glw_file)
  src_glw <- terra::crop(src_glw, boundaries_vect)
  results$glw_cattle <- probe_one(src_glw, "GLW cattle", countries,
                                   base_rast, boundaries_vect)
  print(results$glw_cattle)
} else {
  cat("\n[1] GLW cattle: SKIPPED, file not found:", glw_file, "\n")
}

# (2) Worldpop hpop (any *.tif in hpop_dir) -------------------------------
hpop_files <- list.files(hpop_dir, ".tif$", full.names = TRUE)
hpop_files <- hpop_files[!grepl("intermediate|processed", hpop_files)]
if (length(hpop_files) > 0L) {
  cat("\n[2] Worldpop hpop\n")
  src_hpop <- terra::rast(hpop_files[1])
  if (terra::nlyr(src_hpop) > 1L) src_hpop <- src_hpop[[1]]
  src_hpop <- terra::crop(src_hpop, boundaries_vect)
  results$hpop <- probe_one(src_hpop, "Worldpop hpop", countries,
                             base_rast, boundaries_vect)
  print(results$hpop)
} else {
  cat("\n[2] Worldpop hpop: SKIPPED, no .tif under", hpop_dir, "\n")
}

# (3) Summary --------------------------------------------------------------
cat("\n============ SUMMARY ============\n")
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
  cat("DECISION: bilinear is NOT mass-conserving on at least one layer/country.",
      "Proceed to Stage 2 of dispatch.\n")
} else {
  cat("DECISION: bilinear is mass-conserving within tolerance.",
      "Diagnosis was wrong — escalate before applying Stage 2 fix.\n")
}

# Write a JSON report for the dispatch record -----------------------------
out_dir <- file.path(project_dir, "metadata", "checks")
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
jsonlite::write_json(
  list(
    timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    cglabs = cglabs,
    countries = countries,
    results = all
  ),
  file.path(out_dir, "9_mass_conservation_check.json"),
  auto_unbox = TRUE, pretty = TRUE
)
cat(sprintf("Wrote report to %s\n",
            file.path(out_dir, "9_mass_conservation_check.json")))
