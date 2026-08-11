# _seasonal_helpers.R
# -----------------------------------------------------------------------------
# Pure functions for building per-year SEASONAL (tri-month) aggregate rasters
# from the monthly obs store `{root}/{VAR}/{VAR}-YYYY-MM.tif`.
#
# These MIRROR the definitions inside 5_make_obs_map_climatologies.R (season
# lists, agg rule, per-year stack, COG-stats roundtrip). Script 5 currently
# keeps its own inline copies and computes the per-year stack only to reduce it
# to climatology mean/min/max/sd (the per-year layers are discarded). This file
# exposes the same logic so 5b can PERSIST the per-year seasonal totals.
#
# DEDUP DEBT: script 5 should eventually source this file instead of its inline
# copies. Kept separate for now to avoid editing the publish-critical script 5
# without a real-data re-validation. Keep the two in sync until then.
# -----------------------------------------------------------------------------

suppressWarnings(suppressMessages({
  library(data.table); library(terra); library(glue)
}))

# 12 tri-month windows + annual. NDJ/DJF span the calendar-year boundary.
.seasons <- list(
  annual = 1:12,
  JFM = c(1, 2, 3), FMA = c(2, 3, 4), MAM = c(3, 4, 5), AMJ = c(4, 5, 6),
  MJJ = c(5, 6, 7), JJA = c(6, 7, 8), JAS = c(7, 8, 9), ASO = c(8, 9, 10),
  SON = c(9, 10, 11), OND = c(10, 11, 12), NDJ = c(11, 12, 1),
  DJF = c(12, 1, 2)
)

# Per-variable rule for combining the months WITHIN a window.
.agg_rule <- list(
  PTOT = "sum", TMAX = "max", TMIN = "min", TAVG = "mean",
  `SPEI-01` = "mean", `SPEI-03` = "mean", `SPEI-06` = "mean",
  `SPEI-12` = "mean", `SPEI-24` = "mean"
)

# COG creation options — matches script 5's climatology COGs (overviews ON so
# the notebook can render zoomed-out without pulling native res).
.cog_gdal_opts <- c(
  "COMPRESS=DEFLATE", "PREDICTOR=2",
  "OVERVIEWS=AUTO", "OVERVIEW_RESAMPLING=AVERAGE", "BLOCKSIZE=512"
)

.parse_ym <- function(path) {
  m <- regmatches(basename(path), regexec("(\\d{4})-(\\d{2})\\.tif$", basename(path)))[[1]]
  c(year = as.integer(m[2]), month = as.integer(m[3]))
}

# List the monthly tifs for one variable under `root_dir/{var}/`.
.list_var_files <- function(var, root_dir) {
  dir_path <- file.path(root_dir, var)
  fs <- list.files(dir_path,
    pattern = sprintf("^%s-[0-9]{4}-[0-9]{2}\\.tif$", var), full.names = TRUE)
  if (length(fs) == 0L) stop(glue("No {var} files in {dir_path}"))
  ym <- t(vapply(fs, .parse_ym, integer(2)))
  data.table(path = fs, year = ym[, 1], month = ym[, 2])[order(year, month)]
}

# (year, months_needed) index for a window. NDJ/DJF: December belongs to the
# PREVIOUS calendar year, so DJF-1998 = Dec1997 + Jan1998 + Feb1998.
.build_period_year_index <- function(period, year_lo, year_hi) {
  months <- .seasons[[period]]
  lapply(seq.int(year_lo, year_hi), function(y) {
    pairs <- if (period %in% c("NDJ", "DJF")) {
      data.table(m = months, y_src = ifelse(months == 12L, y - 1L, y))
    } else {
      data.table(m = months, y_src = y)
    }
    list(year = y, months_needed = pairs)
  })
}

.month_agg <- function(rule) {
  switch(rule,
    sum = function(stk) sum(stk), max = function(stk) max(stk),
    min = function(stk) min(stk), mean = function(stk) mean(stk),
    stop(glue("Unknown agg rule '{rule}'")))
}

# Per-year seasonal stack: one layer per calendar year that has ALL required
# months present (incomplete seasons — e.g. current partial year — are dropped).
seasonal_yearly_stack <- function(var, root_dir, period, year_lo, year_hi, bbox = NULL) {
  files <- .list_var_files(var, root_dir)
  reducer <- .month_agg(.agg_rule[[var]])
  pyx <- .build_period_year_index(period, year_lo, year_hi)
  layers <- list()
  for (entry in pyx) {
    needed <- entry$months_needed
    fmask <- files[needed, on = c(year = "y_src", month = "m"), nomatch = 0L]
    if (nrow(fmask) != nrow(needed)) next          # incomplete window -> skip year
    stk <- terra::rast(fmask$path)
    if (!is.null(bbox)) stk <- terra::crop(stk, terra::ext(bbox))
    stk <- terra::ifel(is.infinite(stk), NA, stk)  # SPEI tails
    layer <- reducer(stk)
    names(layer) <- as.character(entry$year)
    layers[[as.character(entry$year)]] <- layer
  }
  if (length(layers) == 0L) return(NULL)
  terra::rast(layers)
}

# Write a SpatRaster layer as a COG, then embed real STATISTICS_MEAN/STDDEV via
# the 3-step GDAL roundtrip (mirrors compute_cog_stats in script 5 — terra's COG
# writer stamps -9999 stat sentinels that gdalinfo -stats won't overwrite).
write_seasonal_cog <- function(layer, out_path) {
  terra::writeRaster(layer, out_path, overwrite = TRUE,
    filetype = "COG", gdal = .cog_gdal_opts)
  if (!nzchar(Sys.which("gdal_translate")) || !nzchar(Sys.which("gdal_edit.py"))) {
    warning("gdal_translate/gdal_edit.py missing; skipping stats embed for ", basename(out_path))
    return(invisible(NULL))
  }
  s1 <- paste0(out_path, ".s1.tif"); s3 <- paste0(out_path, ".s3.tif")
  on.exit(unlink(c(s1, paste0(s1, ".aux.xml"), s3, paste0(s3, ".aux.xml"))), add = TRUE)
  if (system2("gdal_translate", c("-q", "-of", "GTiff", shQuote(out_path), shQuote(s1)),
      stdout = FALSE, stderr = FALSE) != 0L || !file.exists(s1)) {
    warning("stats step1 failed for ", basename(out_path)); return(invisible(NULL))
  }
  if (system2("gdal_edit.py", c("-unsetstats", shQuote(s1)), stdout = FALSE, stderr = FALSE) != 0L) {
    warning("stats step2 failed for ", basename(out_path)); return(invisible(NULL))
  }
  if (system2("gdal_translate", c("-q", "-of", "COG", "-co", "COMPRESS=DEFLATE",
      "-co", "PREDICTOR=2", "-co", "BLOCKSIZE=512", "-co", "OVERVIEWS=AUTO",
      "-co", "OVERVIEW_RESAMPLING=AVERAGE", "-stats", shQuote(s1), shQuote(s3)),
      stdout = FALSE, stderr = FALSE) != 0L || !file.exists(s3)) {
    warning("stats step3 failed for ", basename(out_path)); return(invisible(NULL))
  }
  if (!file.rename(s3, out_path)) warning("stats rename failed for ", basename(out_path))
  invisible(NULL)
}

# The 12 tri-month windows (excludes 'annual').
seasonal_windows <- function() setdiff(names(.seasons), "annual")
