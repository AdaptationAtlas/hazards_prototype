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
# 0_server_setup.R calls setwd(working_dir) — source the helper by
# absolute path so it doesn't try to resolve against the new cwd.
source(file.path(project_dir, "R/checks/_helpers.R"))
# 0_server_setup.R exposes: working_dir, base_rast, base_rast_path,
# geo_files_local (admin0/admin1/admin2 parquets), glw_dir, hpop_dir,
# boundaries_dir, atlas_dirs, Cglabs, ...
# geoboundaries itself is NOT exported by 0_server_setup — each pipeline
# script builds it from geo_files_local[1] (the admin0 parquet) using
# the same incantation. Replicate here.

suppressPackageStartupMessages({
  pacman::p_load(terra, data.table, jsonlite, arrow, geoarrow, sf)
})

geoboundaries <- arrow::read_parquet(geo_files_local[1])
geoboundaries <- geoboundaries |> sf::st_as_sf() |> terra::vect()
geoboundaries <- terra::aggregate(geoboundaries, "iso3")

# ----- CLI ---------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
country_arg <- {
  i <- match("--country", args)
  if (!is.na(i) && i < length(args)) args[i + 1L] else "AGO"
}
countries <- strsplit(country_arg, ",")[[1]]

log_section("Mass-conservation probe (issue #9)")
log_step("project_dir = %s", project_dir)
log_step("working_dir = %s", getwd())
log_step("Cglabs      = %s", isTRUE(Cglabs))
log_step("countries   = %s", paste(countries, collapse = ", "))
if (is.null(base_rast)) {
  stop("base_rast is NULL — 0_server_setup.R did not load a base raster. ",
       "Run 1_make_timeseries.R first or check climdat_source.")
}
log_step("base_rast   = res=%s, ext=%s",
         paste(round(res(base_rast), 4), collapse = "x"),
         paste(round(as.vector(ext(base_rast)), 2), collapse = ","))

# Crop source rasters to the union of selected-country AOIs BEFORE
# probe — avoids reading the whole global grid when we only care about
# a few countries' totals. Big speedup on Worldpop especially.
aoi_subset <- countries_aoi(geoboundaries, countries)
log_step("aoi_subset has %d feature(s); ext = %s",
         length(aoi_subset),
         paste(round(as.vector(terra::ext(aoi_subset)), 2), collapse = ", "))

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
  log_step("[1] GLW cattle (%s)", basename(glw_file))
  results$glw_cattle <- log_timer({
    src_glw <- terra::rast(glw_file)
    # Crop to the selected-country AOI union FIRST so the resample +
    # global() chain operates on a small subset rather than the global
    # grid. The probe_one() helper still crops per-country internally.
    src_glw <- window_to_aoi(src_glw, aoi_subset)
    log_mem("after GLW load + crop")
    probe_one(src_glw, "GLW cattle", countries,
              base_rast, geoboundaries)
  }, label = "probe GLW cattle")
  print(results$glw_cattle)
  cat("\n")
} else {
  log_step("[1] GLW cattle SKIPPED — not at %s", glw_file)
}

# ----- (2) Worldpop hpop -------------------------------------------------
hpop_files <- list.files(hpop_dir, "\\.tif$", full.names = TRUE)
hpop_files <- hpop_files[!grepl("intermediate|processed", hpop_files)]
if (length(hpop_files) > 0L) {
  log_step("[2] Worldpop hpop (%s)", basename(hpop_files[1]))
  results$hpop <- log_timer({
    src_hpop <- terra::rast(hpop_files[1])
    if (terra::nlyr(src_hpop) > 1L) src_hpop <- src_hpop[[1]]
    src_hpop <- window_to_aoi(src_hpop, aoi_subset)
    log_mem("after hpop load + crop")
    probe_one(src_hpop, "Worldpop hpop", countries,
              base_rast, geoboundaries)
  }, label = "probe Worldpop hpop")
  print(results$hpop)
  cat("\n")
} else {
  log_step("[2] Worldpop hpop SKIPPED — no .tif under %s", hpop_dir)
}

# ----- Summary -----------------------------------------------------------
log_section("SUMMARY")
all <- rbindlist(results, fill = TRUE)
print(all)
cat("\n")
log_step("Bilinear ratios outside [0.995, 1.005]: %d / %d rows",
         sum(abs(all$ratio_bilin - 1) > 0.005, na.rm = TRUE),
         sum(!is.na(all$ratio_bilin)))
log_step("Sum ratios outside     [0.995, 1.005]: %d / %d rows",
         sum(abs(all$ratio_sum - 1) > 0.005, na.rm = TRUE),
         sum(!is.na(all$ratio_sum)))
if (any(abs(all$ratio_bilin - 1) > 0.005, na.rm = TRUE)) {
  log_step("DECISION: bilinear NOT mass-conserving. Proceed to Stage 2 of dispatch.")
} else {
  log_step("DECISION: bilinear is mass-conserving within tolerance. Diagnosis was wrong - escalate.")
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
log_step("Wrote report to %s", out_path)
summarize_log()
log_complete("Issue #9 mass-conservation baseline probe",
             c(sprintf("JSON report: %s", out_path)))
