# CR-068 Stage 3 — probe TAVG + PTOT source rasters (raw NEX-GDDP inputs).
#
# Stage 2 found historic NDWS source at AGO has collapsed distribution
# (28-30 days/month, every pixel) vs ssp245 NDWS source (16-30 days/month,
# physical spread). NDWS is a DERIVED index, so the next question is:
# does the collapse exist in the raw inputs (TAVG, PTOT) too, or only
# in the NDWS derivation step?
#
# Decision branches:
#   - If historic TAVG + PTOT both show degenerate spread relative to
#     ssp245: bug is at the raw NEX-GDDP-CMIP6 historical data level
#     (could be 1_make_timeseries.R historic read path, or upstream
#     NEX-GDDP files themselves).
#   - If TAVG + PTOT look normal but only NDWS is degenerate: bug is
#     in the NDWS derivation code (likely in 1_make_timeseries.R Step
#     1 or wherever NDWS is computed from PR + Tmax).
#
# Workspace convention: source R/0_server_setup.R for paths.
#
# Usage:
#   Rscript R/checks/68_categorisation_stage3.R
#   Rscript R/checks/68_categorisation_stage3.R --countries AGO,NGA

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

log_section("CR-068 Stage 3 — probe raw TAVG + PTOT source rasters")
log_step("project_dir = %s", project_dir)
log_step("Cglabs      = %s", isTRUE(Cglabs))
log_step("countries   = %s", paste(countries_iso, collapse = ","))

mean_dir <- atlas_dirs$data_dir$hazard_timeseries_mean
if (is.null(mean_dir) || !dir.exists(mean_dir)) {
  stop("hazard_timeseries_mean dir not resolvable: ", mean_dir)
}
log_step("mean_dir = %s", mean_dir)

# Helper — inspect first matching file for (scenario_prefix, var_token)
inspect_one <- function(label, prefix_re, var_token) {
  log_section(sprintf("%s — %s", label, var_token))
  pat <- sprintf("%s.*\\.tif$", var_token)
  files <- list.files(mean_dir, pattern = pat,
                      full.names = TRUE, recursive = TRUE)
  files <- files[!grepl("ENSEMBLE", files)]
  files <- files[grepl(prefix_re, basename(files))]
  log_step("found %d source files (regex %s, var %s)",
           length(files), prefix_re, var_token)
  if (length(files) == 0L) {
    log_step("(first 5 actual basenames: %s)",
             paste(head(basename(list.files(mean_dir,
                                            pattern = pat,
                                            full.names = TRUE,
                                            recursive = TRUE)), 5),
                   collapse = ", "))
    return(NULL)
  }
  f <- files[1]
  log_step("inspecting: %s", basename(f))
  r <- terra::rast(f)
  r_aoi <- window_to_aoi(r, aoi)
  summary_vec <- terra::global(r_aoi,
                               fun = c("min", "mean", "max"),
                               na.rm = TRUE)
  log_step("  layers = %d", terra::nlyr(r_aoi))
  log_step("  per-pixel summary (over AOI):")
  print(summary_vec)
  vals <- terra::values(r_aoi, dataframe = FALSE)
  vals <- vals[!is.na(vals)]
  if (length(vals) > 1000) vals <- sample(vals, 1000)
  qq <- quantile(vals, c(0.05, 0.5, 0.95))
  spread <- max(vals) - min(vals)
  log_step("  value sample n=%d: min=%.3f, q05=%.3f, median=%.3f, q95=%.3f, max=%.3f, spread=%.3f",
           length(vals), min(vals), qq[1], qq[2], qq[3], max(vals), spread)
  list(
    file = basename(f),
    var = var_token,
    n_layers = terra::nlyr(r_aoi),
    summary = summary_vec,
    val_min = min(vals),
    val_median = qq[2],
    val_max = max(vals),
    val_spread = spread
  )
}

results <- list()

# Probe 3 source variables for both historic + ssp245.
# Convention: historic source files use either `historic_` or `historical_`
# prefix; the rename happens at the _class/ step.
for (var_token in c("PTOT", "TAVG", "NDWS")) {
  results[[paste0(var_token, "_historic")]] <- inspect_one(
    "Section: historic", "^(historic|historical)_", var_token
  )
  results[[paste0(var_token, "_ssp245")]] <- inspect_one(
    "Section: ssp245", "^ssp245_", var_token
  )
}

# Comparison summary
log_section("Cross-scenario spread comparison")
cmp_wide <- NULL
cmp <- rbindlist(lapply(names(results), function(k) {
  r <- results[[k]]
  if (is.null(r)) return(NULL)
  # Split key into (var, scenario) by RIGHT-most underscore so multi-
  # underscore var tokens (e.g. SPEI-12_historic) don't collide with
  # the regex-based extraction the previous version used.
  parts <- strsplit(k, "_(?=[^_]+$)", perl = TRUE)[[1]]
  data.table(key = k, var = parts[1], scenario = parts[2],
             min = r$val_min, median = r$val_median, max = r$val_max,
             spread = r$val_spread)
}))
if (nrow(cmp) > 0L) {
  tryCatch({
    cmp_wide <- dcast(cmp, var ~ scenario,
                      value.var = c("min", "median", "max", "spread"))
    log_step("min / median / max / spread by variable, historic vs ssp245:")
    print(cmp_wide)
    if (all(c("spread_historic", "spread_ssp245") %in% names(cmp_wide))) {
      cmp_wide[, ratio_spread := round(spread_historic / spread_ssp245, 3)]
      log_step("spread ratio historic / ssp245:")
      print(cmp_wide[, .(var, spread_historic, spread_ssp245, ratio_spread)])
    }
    log_step("Watch ratio of historic spread to ssp245 spread:")
    log_step("  ratio < 0.5 = historic distribution collapsed (degenerate)")
    log_step("  ratio ~ 1   = historic is fine; bug is downstream in NDWS deriv")
  }, error = function(e) {
    log_step("dcast/summary failed: %s", conditionMessage(e))
    log_step("falling back to long-form table (per-section data still in JSON):")
    print(cmp)
  })
}

# JSON record
out_dir <- file.path(project_dir, "metadata", "checks")
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
out_path <- file.path(out_dir, "68_categorisation_stage3.json")
jsonlite::write_json(
  c(list(timestamp  = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
         project_dir = project_dir,
         Cglabs     = isTRUE(Cglabs),
         countries  = countries_iso,
         comparison = if (nrow(cmp) > 0L) cmp_wide else NULL),
    results),
  out_path, auto_unbox = TRUE, pretty = TRUE
)
log_step("Wrote report to %s", out_path)
summarize_log()

log_complete("CR-068 Stage 3 probe (TAVG + PTOT + NDWS source spread)",
             c(sprintf("JSON report: %s", out_path),
               "Branch on the spread-ratio per variable:",
               "  PTOT + TAVG ratio < 0.5 -> raw NEX-GDDP historic data bug",
               "  Only NDWS ratio < 0.5   -> bug in NDWS derivation step"))
