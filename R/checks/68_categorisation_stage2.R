# CR-068 Stage 2 — walk upstream to identify the bug's root cause.
#
# Stage 1 (R/checks/68_categorisation_stage1.R) confirmed Candidate 1:
# historic NDWS-mean-G15 / G20 are saturated (mean = 1.000) over AGO
# while future means are lower (~0.88). Physically impossible — AGO
# has wet seasons. Bug must be in Step 1 of R/2_calculate_haz_freq.R
# (the classifier) or upstream in R/1_make_timeseries.R (the source
# monthly-index rasters that feed the classifier).
#
# This probe inspects the SOURCE rasters (unclassified day-count TIFs
# under hazard_timeseries_mean/) and runs a file-name collision audit
# at the SAME physical directory level (the L654 rename concern from
# the dispatch).
#
# Workspace convention: source R/0_server_setup.R for paths.
#
# Usage:
#   Rscript R/checks/68_categorisation_stage2.R
#   Rscript R/checks/68_categorisation_stage2.R --countries AGO

project_dir <- if (nzchar(Sys.getenv("project_dir"))) {
  Sys.getenv("project_dir")
} else {
  getwd()
}
setwd(project_dir)
source("R/0_server_setup.R")
source(file.path(project_dir, "R/checks/_helpers.R"))

suppressPackageStartupMessages({
  pacman::p_load(terra, data.table, jsonlite, arrow, geoarrow, sf)
})

geoboundaries <- arrow::read_parquet(geo_files_local[1]) |>
  sf::st_as_sf() |>
  terra::vect()
geoboundaries <- terra::aggregate(geoboundaries, "iso3")

args_all <- commandArgs(trailingOnly = TRUE)
country_arg <- parse_cli(args_all, "countries", "character", default = "AGO")
countries_iso <- strsplit(country_arg, ",")[[1]]
aoi <- countries_aoi(geoboundaries, countries_iso)

log_section("CR-068 Stage 2 — walk upstream")
log_step("project_dir = %s", project_dir)
log_step("Cglabs      = %s", isTRUE(Cglabs))
log_step("countries   = %s", paste(countries_iso, collapse = ","))

results <- list()

# =========================================================================
# Section A — Inspect the unclassified NDWS source raster for historic
# =========================================================================
# If values are 0-31 day-counts -> classifier threshold issue.
# If values are already 0/1     -> classifier was run twice (re-binarised).
# If values are nonsensical     -> bug is in 1_make_timeseries.R upstream.
log_section("Section A — NDWS source TIF inspection (historic vs ssp245)")

mean_dir <- atlas_dirs$data_dir$hazard_timeseries_mean
if (is.null(mean_dir) || !dir.exists(mean_dir)) {
  log_step("hazard_timeseries_mean dir not resolvable, skipping Section A")
} else {
  log_step("mean_dir = %s", mean_dir)
  # The actual layout is per-period (jagermeyr/...) not per-scenario.
  # Files are named <scenario>_<model>_<timeframe>_<index>.tif.
  # Find NDWS source files for historic + ssp245 by recursive glob +
  # filename-pattern.
  all_src <- list.files(mean_dir, pattern = "NDWS.*\\.tif$",
                        full.names = TRUE, recursive = TRUE)
  all_src <- all_src[!grepl("ENSEMBLE", all_src)]
  log_step("found %d NDWS source files (all scenarios)", length(all_src))
  for (scenario_dir in c("historic", "ssp245")) {
    log_section(sprintf("Section A.%s", scenario_dir))
    src_files <- all_src[grepl(paste0("^", scenario_dir, "_"),
                               basename(all_src))]
    log_step("found %d NDWS source files for %s",
             length(src_files), scenario_dir)
    if (length(src_files) == 0L) {
      log_step("(no matching files; check filename pattern under %s)",
               mean_dir)
      next
    }

    # Pick the first file as a representative sample.
    f <- src_files[1]
    log_step("inspecting: %s", basename(f))
    r <- terra::rast(f)
    r_aoi <- window_to_aoi(r, aoi)

    summary_vec <- terra::global(r_aoi,
                                 fun = c("min", "mean", "max"),
                                 na.rm = TRUE)
    log_step("  layers = %d", terra::nlyr(r_aoi))
    log_step("  per-pixel summary (over AOI, all layers):")
    print(summary_vec)

    # Sample 1000 random pixel-month values and show their distribution
    vals <- terra::values(r_aoi, dataframe = FALSE)
    vals <- vals[!is.na(vals)]
    if (length(vals) > 1000) vals <- sample(vals, 1000)
    log_step("  value sample (n=%d): min=%.2f, q05=%.2f, median=%.2f, q95=%.2f, max=%.2f",
             length(vals),
             min(vals),
             quantile(vals, 0.05),
             median(vals),
             quantile(vals, 0.95),
             max(vals))

    # Pattern interpretation:
    # - 0-31 with integer-ish distribution -> day-counts (Step 1 classifier
    #   should compare against threshold)
    # - {0, 1} only                        -> already binary (re-binarised
    #   into the _class output, which would still be 1 not saturate-bug)
    # - Very large values or non-physical  -> upstream timeseries problem
    is_binary <- all(vals %in% c(0, 1))
    is_daycount <- (min(vals) >= 0) && (max(vals) <= 31.5)
    log_step("  diagnosis: is_binary=%s, is_daycount=%s",
             is_binary, is_daycount)

    results[[paste0("ndws_src_", scenario_dir)]] <- list(
      file       = basename(f),
      n_layers   = terra::nlyr(r_aoi),
      summary    = summary_vec,
      is_binary  = is_binary,
      is_daycount = is_daycount,
      val_min    = min(vals),
      val_median = median(vals),
      val_max    = max(vals)
    )
  }
}

# =========================================================================
# Section B — Inspect the classified NDWS raster for historic vs future
# =========================================================================
# Same file pair from Section A, post-classification. Confirms whether
# the *classified* file is 0/1 (correct binary) and computes the
# fraction-saturated to ground-truth Stage 1's finding.
log_section("Section B — NDWS classified TIF inspection (historic vs ssp245)")

class_dir <- atlas_dirs$data_dir$hazard_timeseries_class
if (is.null(class_dir) || !dir.exists(class_dir)) {
  log_step("hazard_timeseries_class dir not resolvable, skipping Section B")
} else {
  log_step("class_dir = %s", class_dir)
  for (scenario_dir in c("historic", "ssp245")) {
    log_section(sprintf("Section B.%s", scenario_dir))
    scn_dir <- file.path(class_dir, scenario_dir)
    # Class dir might not be partitioned by scenario; recursive scan.
    candidates <- list.files(class_dir, pattern = "NDWS.*G15\\.tif$",
                             full.names = TRUE, recursive = TRUE)
    candidates <- candidates[grepl(scenario_dir, basename(candidates))]
    candidates <- candidates[!grepl("ENSEMBLE", candidates)]
    if (length(candidates) == 0L) {
      log_step("(no NDWS-G15 class file found for %s)", scenario_dir)
      next
    }
    f <- candidates[1]
    log_step("inspecting: %s", basename(f))
    r <- terra::rast(f)
    r_aoi <- window_to_aoi(r, aoi)
    summary_vec <- terra::global(r_aoi,
                                 fun = c("min", "mean", "max"),
                                 na.rm = TRUE)
    log_step("  layers = %d", terra::nlyr(r_aoi))
    log_step("  per-pixel summary (binary 0/1 expected):")
    print(summary_vec)

    # Per-pixel fraction of months flagged. Aggregating with mean()
    # collapses layers into a single fraction-of-time-flagged surface.
    pixel_frac <- terra::global(terra::mean(r_aoi), "mean", na.rm = TRUE)[, 1]
    log_step("  global mean (pixel × time fraction flagged) = %.4f", pixel_frac)
    results[[paste0("ndws_class_", scenario_dir)]] <- list(
      file = basename(f),
      n_layers = terra::nlyr(r_aoi),
      summary = summary_vec,
      mean_fraction = pixel_frac
    )
  }
}

# =========================================================================
# Section C — File-name collision audit at the SAME-DIRECTORY level
# =========================================================================
# Stage 1's audit collected basenames recursively across the whole tree.
# Here we group by *directory* to surface real collisions (two files at
# the same path).
log_section("Section C — per-directory collision audit (the L654 rename concern)")

if (!is.null(class_dir) && dir.exists(class_dir)) {
  all_class <- list.files(class_dir, recursive = TRUE, full.names = TRUE)
  all_class <- all_class[!grepl("ENSEMBLE", all_class)]
  df <- data.table(path = all_class,
                   dir  = dirname(all_class),
                   bn   = basename(all_class))
  per_dir_dups <- df[, .(n = .N, paths = list(path)),
                     by = .(dir, bn)][n > 1]
  log_step("dirs with same-name files: %d", nrow(per_dir_dups))
  if (nrow(per_dir_dups) > 0L) {
    log_step("first 10 collisions:")
    print(per_dir_dups[1:min(10L, .N), .(dir, bn, n)])
    results$collisions_per_dir <- per_dir_dups
  } else {
    log_step("  no same-directory collisions found — Stage 1's 'collisions'")
    log_step("  were cross-subdir basename matches (benign)")
  }
}

# =========================================================================
# JSON record
# =========================================================================
out_dir <- file.path(project_dir, "metadata", "checks")
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
out_path <- file.path(out_dir, "68_categorisation_stage2.json")
jsonlite::write_json(
  c(list(timestamp  = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
         project_dir = project_dir,
         Cglabs     = isTRUE(Cglabs),
         countries  = countries_iso),
    results),
  out_path, auto_unbox = TRUE, pretty = TRUE
)
log_step("Wrote report to %s", out_path)
summarize_log()

log_section("STOP — Stage 2 report")
log_step("Read the per-pixel summary + is_binary / is_daycount diagnostics")
log_step("above to determine where the saturation enters the pipeline:")
log_step("  - is_daycount = TRUE && historic > future : Step 1 classifier")
log_step("    threshold likely wrong, OR raw NDWS values are saturated.")
log_step("  - is_binary = TRUE                          : classifier ran")
log_step("    twice OR raw timeseries already encodes binary 0/1.")
log_step("  - non-physical values                       : bug is upstream")
log_step("    in R/1_make_timeseries.R historic-scenario read path.")

log_complete("CR-068 Stage 2 probe", c(
  sprintf("JSON report: %s", out_path),
  "Diagnosis lines above — paste back for Stage 3 fix"
))
