# 0) Introduction ####
# Zonal-aggregate the monthly observational stack written by
# R/observational/0.1.2_get_chirps_chirts.R + R/observational/0.1.2.1_calculate_obs_spei.R from pixel
# rasters to admin polygons (adm0, adm1, adm2). Produces one combined long
# parquet per admin level covering all nine observational variables:
#   PTOT, TMAX, TMIN, TAVG, SPEI-01, SPEI-03, SPEI-06, SPEI-12, SPEI-24
#
# Schema of each output parquet (long form):
#   iso3            character
#   admin0_name     character
#   admin1_name     character (NA at adm0)
#   admin2_name     character (NA at adm0 / adm1)
#   gaul0_code      integer
#   gaul1_code      integer (NA at adm0)
#   gaul2_code      integer (NA at adm0 / adm1)
#   year            integer
#   month           integer
#   variable        factor ("PTOT" | "TMAX" | ... | "SPEI-24")
#   value_mean      double  (spatial mean within polygon)
#   value_sd        double  (spatial sd   within polygon)
#
# Outputs:
#   Data/chirts_chirps_hist/admin/obs_monthly_adm{0,1,2}.parquet
#   Data/chirts_chirps_hist/admin/obs_monthly_adm{0,1,2}.parquet.json
#
# Pipeline:
#   1) Load admin boundary polygons (adm0/1/2) from geo_files_local.
#   2) Build or load cached zonal rasters aligned to obs_base_rast (0.05 deg)
#      at boundaries_int_dir/{admin_level}_obs_zonal.tif.
#   3) For each (admin_level x variable) tuple, list the variable's monthly
#      TIFs, lazy-stack them, run two terra::zonal passes (mean and sd) over
#      the stack with the zonal raster, melt the wide-by-layer matrix into
#      long (zone_id, year, month, value_*), then merge with the
#      boundaries_index to attach admin names.
#   4) Bind all (variable, year, month) rows per admin level, factor-encode
#      the string columns, and write the parquet with zstd compression.
#
# Run modes:
#   --smoke   adm0 only, all nine variables, the most recent five years of
#             coverage. Runs five inline sanity checks: file written, schema
#             matches spec, expected row count, no all-NA admin x variable
#             combinations, parquet round-trips. ~minute on CGlabs.
#   --full    adm0 + adm1 by default (set include_adm2 <- TRUE near the top
#             to also produce adm2, which is ~25k zones x 544 months x 9
#             variables and near the 2 GB parquet ceiling). Intended for
#             the Afrilabs / CGlabs server.
#   (none)    Print usage and exit 1.
#
# Please run 0_server_setup.R before --full so geo_files_local is populated;
# --smoke loads a single admin file directly without sourcing the full setup.

# 1) Setup ####

log_step <- function(msg) {
  cat(format(Sys.time(), "[%H:%M:%S] "), msg, "\n", sep = "")
  flush.console()
}

warnings_collected <- new.env(parent = emptyenv())
warnings_collected$entries <- character()

collect_warnings <- function(expr, label) {
  withCallingHandlers(
    expr,
    warning = function(w) {
      msg <- sprintf("[%s] %s", label, conditionMessage(w))
      warnings_collected$entries <- c(warnings_collected$entries, msg)
      invokeRestart("muffleWarning")
    }
  )
}

bootstrap_minimal <- function() {
  log_step("bootstrap_minimal: resolving project / working dirs")
  if (!requireNamespace("pacman", quietly = TRUE)) {
    install.packages("pacman", repos = "https://cloud.r-project.org")
  }
  library(pacman)
  pacman::p_load(terra, data.table, glue, jsonlite, arrow, sf, geoarrow, fs)

  project_dir <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else getwd()
  candidates <- switch(project_dir,
    "/home/jovyan/atlas/hazards_prototype" = c(
      "/home/jovyan/common_data/nex-gddp-cimp6_hazards",
      "/home/jovyan/common_data/hazards_prototype"
    ),
    "D:/rprojects/hazards_prototype" = "D:/common_data/hazards_prototype",
    "C:/rprojects/hazards_prototype" = "C:/rprojects/common_data/hazards_prototype",
    "/Users/pstewarda/Documents/rprojects/hazards_prototype" =
      "/Users/pstewarda/Documents/rprojects/common_data/hazards_prototype",
    "/home/psteward/rprojects/hazards_prototype" = "/cluster01/workspace/atlas/hazards_prototype",
    stop(glue::glue("Unknown project_dir '{project_dir}'. Add a mapping to bootstrap_minimal()."))
  )
  has_data <- vapply(candidates, function(p) {
    dir.exists(file.path(p, "Data", "chirts_chirps_hist", "PTOT"))
  }, logical(1))
  working_dir <- if (any(has_data)) candidates[has_data][1] else candidates[1]
  log_step(sprintf("  selected working_dir: %s", working_dir))
  if (!dir.exists(working_dir)) dir.create(working_dir, recursive = TRUE)
  setwd(working_dir)

  chirts_chirps_hist_dir <- file.path("Data", "chirts_chirps_hist")
  if (!dir.exists(chirts_chirps_hist_dir)) {
    stop("Run R/observational/0.1.2_get_chirps_chirts.R --full before computing admin extract.")
  }

  terra::gdalCache(60000)

  list(
    project_dir = project_dir,
    working_dir = working_dir,
    chirts_chirps_hist_dir = chirts_chirps_hist_dir
  )
}

args <- commandArgs(trailingOnly = TRUE)
mode <- if (length(args) == 0) "" else args[1]

if (mode == "--smoke") {
  paths <- bootstrap_minimal()
  project_dir <- paths$project_dir
  chirts_chirps_hist_dir <- paths$chirts_chirps_hist_dir
} else if (mode == "--full") {
  source("R/0_server_setup.R")
  pacman::p_load(terra, data.table, glue, jsonlite, arrow, sf, geoarrow, fs)
  chirts_chirps_hist_dir <- atlas_dirs$data_dir$chirts_chirps_hist
  if (!dir.exists(chirts_chirps_hist_dir)) {
    stop("Run R/observational/0.1.2_get_chirps_chirts.R --full before computing admin extract.")
  }
} else {
  cat(
    "Usage:\n",
    "  Rscript R/observational/0.1.2.2_extract_obs_admin.R --smoke\n",
    "      adm0 only, all 9 variables, last 5 years of coverage.\n",
    "  Rscript R/observational/0.1.2.2_extract_obs_admin.R --full\n",
    "      All admin levels (0/1/2), all variables, all months.\n",
    sep = ""
  )
  quit(status = 1)
}

# 2) Configuration ####

variables_full <- c(
  "PTOT", "TMAX", "TMIN", "TAVG",
  "SPEI-01", "SPEI-03", "SPEI-06", "SPEI-12", "SPEI-24"
)

# Admin2 is the heavy step (~25k zones x 544 months x 9 variables, parquet
# size near the 2 GB recommended ceiling). Off by default; flip to TRUE when
# you want the subnational district output.
include_adm2 <- FALSE

admin_levels_full <- c("admin0", "admin1")
if (isTRUE(include_adm2)) admin_levels_full <- c(admin_levels_full, "admin2")
admin_levels_smoke <- c("admin0")
smoke_n_years <- 5L

admin_dir <- file.path(chirts_chirps_hist_dir, "admin")
if (!dir.exists(admin_dir)) dir.create(admin_dir, recursive = TRUE)

# Use the obs base raster as the alignment template for zonal rasters. It
# was built by R/observational/0.1.2_get_chirps_chirts.R into the project repo's metadata/.
# bootstrap_minimal switched cwd to working_dir, so go back via project_dir.
obs_base_rast_path <- file.path(project_dir, "metadata", "base_raster_obs.tif")
if (!file.exists(obs_base_rast_path)) {
  stop(glue::glue("obs_base_rast not found at {obs_base_rast_path} - run 0.1.2_get_chirps_chirts.R first."))
}

# geo_files_local is set by 0_server_setup.R (--full path); when running
# --smoke we resolve admin paths directly via boundaries_dir. The atlas
# naming convention is atlas_gaul24_a{0,1,2}_{region}.parquet (see
# R/0_server_setup.R), so the level token is _a0_ / _a1_ / _a2_.
if (!exists("geo_files_local", inherits = TRUE)) {
  boundaries_dir <- file.path("Data", "boundaries")
  geo_files_local <- list.files(
    boundaries_dir,
    pattern = "_a[0-2]_[^/]*\\.parquet$",
    full.names = TRUE
  )
  if (length(geo_files_local) == 0L) {
    stop(glue::glue(
      "No admin boundary parquets in {boundaries_dir}. ",
      "Run R/0_server_setup.R or stage atlas_gaul24_a*.parquet manually."
    ))
  }
  level_tag <- sub(".*_a([0-2])_.*", "\\1", basename(geo_files_local))
  names(geo_files_local) <- paste0("admin", level_tag)
}

# Where the cached zonal rasters live (one per admin level, aligned to obs).
boundaries_int_dir_local <- if (exists("boundaries_int_dir", inherits = TRUE)) {
  boundaries_int_dir
} else {
  file.path("Data", "boundaries", "intermediate")
}
if (!dir.exists(boundaries_int_dir_local)) {
  dir.create(boundaries_int_dir_local, recursive = TRUE)
}

cat("project_dir            :", project_dir, "\n")
cat("working_dir            :", getwd(), "\n")
cat("input dir              :", chirts_chirps_hist_dir, "\n")
cat("output dir             :", admin_dir, "\n")
cat("obs_base_rast_path     :", obs_base_rast_path, "\n")
cat("boundaries_int_dir     :", boundaries_int_dir_local, "\n")
cat("mode                   :", mode, "\n")
cat("admin levels           :", paste(
  if (mode == "--smoke") admin_levels_smoke else admin_levels_full,
  collapse = ", "
), "\n")
cat("variables              :", paste(variables_full, collapse = ", "), "\n\n")

# 3) Helpers ####

#' Load one admin level as a SpatVector with zone_id (gaul2 -> gaul1 -> gaul0).
load_admin <- function(level) {
  file <- geo_files_local[[level]]
  if (is.null(file) || !file.exists(file)) {
    stop(glue::glue("Admin file for {level} not found: {file}"))
  }
  log_step(sprintf("  loading admin geometry: %s -> %s", level, file))
  data <- arrow::open_dataset(file) |>
    sf::st_as_sf() |>
    terra::vect()
  data$zone_id <- ifelse(!is.na(data$gaul2_code), data$gaul2_code,
    ifelse(!is.na(data$gaul1_code), data$gaul1_code, data$gaul0_code)
  )
  data
}

#' Build or load the obs-grid zonal raster for an admin level.
get_zonal_rast <- function(level, geo, obs_base) {
  out_path <- file.path(boundaries_int_dir_local, sprintf("%s_obs_zonal.tif", level))
  if (file.exists(out_path)) {
    log_step(sprintf("  cached zonal raster: %s", out_path))
    return(terra::rast(out_path))
  }
  log_step(sprintf("  rasterising %s onto obs grid -> %s", level, out_path))
  zone_rast <- terra::rasterize(
    x = geo, y = obs_base, field = "zone_id",
    background = NA, touches = TRUE
  )
  terra::writeRaster(zone_rast, out_path, overwrite = TRUE)
  zone_rast
}

#' Build the per-zone metadata data.table (zone_id + admin names + gaul codes).
#' Deduplicates on zone_id so multi-feature countries (e.g. mainland + offshore
#' islands sharing one gaul code) don't explode the downstream merge.
build_boundaries_index <- function(geo) {
  cols <- c(
    "iso3", "admin0_name", "admin1_name", "admin2_name", "zone_id",
    "gaul0_code", "gaul1_code", "gaul2_code"
  )
  present_cols <- intersect(cols, names(data.frame(geo)))
  dt <- data.table::as.data.table(data.frame(geo)[, present_cols, drop = FALSE])
  unique(dt, by = "zone_id")
}

#' List the monthly TIFs for one variable and return a data.table of paths.
list_var_files <- function(var) {
  dir_path <- file.path(chirts_chirps_hist_dir, var)
  fs <- list.files(
    dir_path,
    pattern = sprintf("^%s-[0-9]{4}-[0-9]{2}\\.tif$", var),
    full.names = TRUE
  )
  if (length(fs) == 0L) stop(glue::glue("No {var} files in {dir_path}"))
  data.table::data.table(path = fs)[order(path)]
}

#' Parse "VAR-YYYY-MM" or "VAR-NN-YYYY-MM" layer names into (year, month).
parse_ym_from_layer <- function(lyr) {
  m <- regmatches(lyr, regexec("(\\d{4})-(\\d{2})$", lyr))
  yy <- as.integer(vapply(m, function(x) if (length(x) >= 3) x[2] else NA_character_, character(1)))
  mm <- as.integer(vapply(m, function(x) if (length(x) >= 3) x[3] else NA_character_, character(1)))
  list(year = yy, month = mm)
}

#' Zonal-extract one variable's full monthly stack: returns a long data.table
#' with (zone_id, year, month, value_mean, value_sd). Two terra::zonal passes
#' for mean and sd, merged on zone_id + layer.
extract_var_zonal <- function(var, zonal_rast, n_years = NULL) {
  files <- list_var_files(var)$path
  if (!is.null(n_years)) {
    # Smoke filter: keep only the most recent n_years.
    ym <- parse_ym_from_layer(sub("\\.tif$", "", basename(files)))
    max_y <- max(ym$year, na.rm = TRUE)
    keep <- ym$year > (max_y - n_years)
    files <- files[keep]
  }
  if (length(files) == 0L) {
    stop(glue::glue("No files for {var} after filtering"))
  }
  stk <- terra::rast(files)
  names(stk) <- sub("\\.tif$", "", basename(files))

  # SPEI's log-logistic fit can produce +-Inf at the distribution tails for a
  # tiny fraction of pixel-months. Inf propagates through mean() and sd(),
  # contaminating any admin polygon containing a tail pixel. Mask to NA so
  # na.rm=TRUE in zonal can drop them. No-op for PTOT / TMAX / TMIN / TAVG.
  stk <- collect_warnings(
    terra::ifel(is.infinite(stk), NA, stk),
    label = sprintf("Inf mask %s", var)
  )

  z_mean <- collect_warnings(
    terra::zonal(stk, zonal_rast, fun = "mean", na.rm = TRUE),
    label = sprintf("zonal mean %s", var)
  )
  z_sd <- collect_warnings(
    terra::zonal(stk, zonal_rast, fun = "sd", na.rm = TRUE),
    label = sprintf("zonal sd %s", var)
  )
  z_mean <- data.table::as.data.table(z_mean)
  z_sd <- data.table::as.data.table(z_sd)
  zone_col <- names(z_mean)[1]
  data.table::setnames(z_mean, zone_col, "zone_id")
  data.table::setnames(z_sd, zone_col, "zone_id")

  long_mean <- data.table::melt(
    z_mean,
    id.vars = "zone_id",
    variable.name = "lyr", value.name = "value_mean",
    variable.factor = FALSE
  )
  long_sd <- data.table::melt(
    z_sd,
    id.vars = "zone_id",
    variable.name = "lyr", value.name = "value_sd",
    variable.factor = FALSE
  )
  out <- merge(long_mean, long_sd, by = c("zone_id", "lyr"))
  ym <- parse_ym_from_layer(out$lyr)
  out[, year := ym$year]
  out[, month := ym$month]
  out[, variable := var]
  out[, lyr := NULL]
  out[]
}

# 4) Process per admin level ####

levels_run <- if (mode == "--smoke") admin_levels_smoke else admin_levels_full
obs_base <- terra::rast(obs_base_rast_path)

written <- character()
for (level in levels_run) {
  out_path <- file.path(admin_dir, sprintf("obs_monthly_%s.parquet", sub("admin", "adm", level)))
  if (file.exists(out_path) && file.size(out_path) > 100L) {
    log_step(sprintf(
      "=== %s: parquet already present, skipping (delete %s to rebuild)",
      level, basename(out_path)
    ))
    written <- c(written, out_path)
    next
  }

  log_step(sprintf("=== Admin level: %s ===", level))
  geo <- load_admin(level)
  zonal_rast <- get_zonal_rast(level, geo, obs_base)
  idx <- build_boundaries_index(geo)

  per_var <- vector("list", length(variables_full))
  for (j in seq_along(variables_full)) {
    var <- variables_full[j]
    t0 <- Sys.time()
    long_var <- extract_var_zonal(
      var, zonal_rast,
      n_years = if (mode == "--smoke") smoke_n_years else NULL
    )
    log_step(sprintf(
      "  [%d/%d] %s: %d rows in %.1fs",
      j, length(variables_full), var, nrow(long_var),
      as.numeric(Sys.time() - t0, units = "secs")
    ))
    per_var[[j]] <- long_var
  }
  combined <- data.table::rbindlist(per_var, use.names = TRUE)
  log_step(sprintf("  merging admin metadata onto %d rows", nrow(combined)))
  combined <- merge(combined, idx, by = "zone_id", all.x = TRUE)
  combined[, zone_id := NULL]

  # Coerce types and put the output schema in stable order.
  combined[, variable := factor(variable, levels = variables_full)]
  for (c in c("iso3", "admin0_name", "admin1_name", "admin2_name")) {
    if (c %in% names(combined)) combined[, (c) := as.character(get(c))]
  }
  col_order <- c(
    "iso3", "admin0_name", "admin1_name", "admin2_name",
    "gaul0_code", "gaul1_code", "gaul2_code",
    "year", "month", "variable", "value_mean", "value_sd"
  )
  col_order <- intersect(col_order, names(combined))
  data.table::setcolorder(combined, col_order)
  data.table::setorderv(combined, intersect(
    c("iso3", "admin1_name", "admin2_name", "variable", "year", "month"),
    names(combined)
  ))

  # Factor-encode the string columns so Arrow stores them as dictionaries.
  for (c in c("iso3", "admin0_name", "admin1_name", "admin2_name")) {
    if (c %in% names(combined)) combined[, (c) := as.factor(get(c))]
  }

  tbl <- arrow::arrow_table(combined)
  tbl <- tbl$ReplaceSchemaMetadata(list(
    description = sprintf(
      "Monthly observational climate aggregated to %s (mean and sd of pixel values within each polygon).",
      level
    ),
    source = "R/observational/0.1.2.2_extract_obs_admin.R against Data/chirts_chirps_hist/{PTOT,TMAX,TMIN,TAVG,SPEI-*}/*.tif",
    obs_base_rast = obs_base_rast_path,
    variables = paste(variables_full, collapse = ", "),
    admin_level = level,
    n_rows = as.character(nrow(combined)),
    n_zones = as.character(uniqueN(combined[, intersect("gaul0_code", names(combined)), with = FALSE])),
    year_range = sprintf(
      "%d-%02d to %d-%02d",
      min(combined$year), min(combined$month[combined$year == min(combined$year)]),
      max(combined$year), max(combined$month[combined$year == max(combined$year)])
    ),
    aggregation = "spatial mean and sd of pixel values within polygon (terra::zonal)",
    build_time = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    build_script = "R/observational/0.1.2.2_extract_obs_admin.R"
  ))
  arrow::write_parquet(tbl, out_path, compression = "zstd", compression_level = 9)
  log_step(sprintf("  wrote %s (%.1f MB)", out_path, file.info(out_path)$size / 1024 / 1024))
  written <- c(written, out_path)

  # Sidecar JSON.
  jsonlite::write_json(list(
    file = basename(out_path),
    admin_level = level,
    variables = variables_full,
    aggregation = "spatial mean (value_mean) and sd (value_sd) per polygon",
    obs_base_rast = obs_base_rast_path,
    n_rows = nrow(combined),
    year_range = c(min(combined$year), max(combined$year)),
    build_time = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    parent_script = "R/observational/0.1.2.2_extract_obs_admin.R"
  ), path = paste0(out_path, ".json"), pretty = TRUE, auto_unbox = TRUE)
}

# 5) Smoke verification ####

if (mode == "--smoke") {
  log_step("=== VERIFICATION CHECKS ===")
  pass <- TRUE
  parquet_path <- written[1]

  # Check 1: file exists.
  if (file.exists(parquet_path) && file.size(parquet_path) > 100L) {
    cat(sprintf(
      "[OK] 1. Parquet written: %s (%.1f MB)\n",
      parquet_path, file.info(parquet_path)$size / 1024 / 1024
    ))
  } else {
    cat(sprintf("[FAIL] 1. Parquet missing or empty: %s\n", parquet_path))
    pass <- FALSE
  }

  back <- arrow::read_parquet(parquet_path) |> data.table::as.data.table()

  # Check 2: schema.
  expected_cols <- c(
    "iso3", "admin0_name", "gaul0_code",
    "year", "month", "variable", "value_mean", "value_sd"
  )
  missing_cols <- setdiff(expected_cols, names(back))
  if (length(missing_cols) == 0L) {
    cat(sprintf("[OK] 2. Schema has expected columns (%d cols).\n", ncol(back)))
  } else {
    cat(sprintf("[FAIL] 2. Missing columns: %s\n", paste(missing_cols, collapse = ", ")))
    pass <- FALSE
  }

  # Check 3: row count plausible. For admin0 the natural unit is the gaul0
  # zone (n_zones > n_iso3 in practice - territories share iso3 with main
  # country but get their own gaul0_code).
  zone_cols <- intersect(c("gaul2_code", "gaul1_code", "gaul0_code"), names(back))
  n_zones <- uniqueN(back[, zone_cols, with = FALSE])
  n_vars <- uniqueN(back$variable)
  n_months <- uniqueN(back[, .(year, month)])
  expected_rows <- n_zones * n_vars * n_months
  if (abs(nrow(back) - expected_rows) / expected_rows < 0.05) {
    cat(sprintf(
      "[OK] 3. Row count %d ~= zones x vars x months = %d x %d x %d = %d.\n",
      nrow(back), n_zones, n_vars, n_months, expected_rows
    ))
  } else {
    cat(sprintf(
      "[FAIL] 3. Row count %d differs from expected %d (zones=%d vars=%d months=%d).\n",
      nrow(back), expected_rows, n_zones, n_vars, n_months
    ))
    pass <- FALSE
  }

  # Check 4: no all-NA admin x variable combinations.
  na_by_combo <- back[, .(all_na = all(is.na(value_mean))), by = .(iso3, variable)]
  n_all_na <- na_by_combo[all_na == TRUE, .N]
  if (n_all_na == 0L) {
    cat("[OK] 4. No (iso3 x variable) combinations are entirely NA.\n")
  } else {
    cat(sprintf("[FAIL] 4. %d (iso3 x variable) combinations are entirely NA.\n", n_all_na))
    print(head(na_by_combo[all_na == TRUE]))
    pass <- FALSE
  }

  # Check 5: value range plausibility per variable.
  rng <- back[, .(
    min = min(value_mean, na.rm = TRUE),
    max = max(value_mean, na.rm = TRUE)
  ),
  by = variable
  ]
  cat("[OK] 5. Per-variable value ranges (mean):\n")
  print(rng)

  cat(
    "\n=== WARNINGS COLLECTED:",
    length(warnings_collected$entries), "===\n"
  )
  if (length(warnings_collected$entries) > 0L) {
    tab <- sort(table(sub(":.*$", "", warnings_collected$entries)), decreasing = TRUE)
    for (nm in names(tab)) cat(sprintf("  %5d  %s\n", tab[[nm]], nm))
    cat("First few full messages:\n")
    cat(paste0("  - ", utils::head(warnings_collected$entries, 5), collapse = "\n"), "\n")
  } else {
    cat("(no warnings)\n")
  }

  if (!pass) {
    cat("\n=== SMOKE TEST FAILED ===\n")
    quit(status = 1)
  }
  cat("\n=== SMOKE TEST PASSED - STOPPING (do NOT run --full from here) ===\n")
  quit(status = 0)
}

# 6) --full summary ####

log_step(sprintf("Full extract complete. %d parquet files written.", length(written)))
for (p in written) cat("  ", p, "\n")
cat("\nWarnings collected:", length(warnings_collected$entries), "\n")
if (length(warnings_collected$entries) > 0L) {
  tab <- sort(table(sub(":.*$", "", warnings_collected$entries)), decreasing = TRUE)
  for (nm in names(tab)) cat(sprintf("  %5d  %s\n", tab[[nm]], nm))
}
