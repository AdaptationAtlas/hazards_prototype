# 0) Introduction ####
# Produce per-pixel climatology COGs for the observational stack so the
# notebook can render maps without doing on-the-fly rasterstats.
#
# Inputs (already on disk after R/observational/1_get_chirps_chirts.R and
# R/observational/2_calculate_obs_spei.R --full):
#   Data/chirts_chirps_hist/{PTOT,TMAX,TMIN,TAVG,SPEI-01,03,06,12,24}/*.tif
#
# Outputs:
#   Data/chirts_chirps_hist/maps/{variable}/{variable}_{period}_{clim}_{stat}.tif
#   Data/chirts_chirps_hist/maps/{variable}/_metadata.json
#
# Dimensions:
#   variables   9   PTOT TMAX TMIN TAVG SPEI-01 SPEI-03 SPEI-06 SPEI-12 SPEI-24
#   periods    13   annual + JFM FMA MAM AMJ MJJ JJA JAS ASO SON OND NDJ DJF
#   climatology 3   1995-2014  1991-2020  full
#   stats       4   mean min max sd
# Total: 9 * 13 * 3 * 4 = 1,404 COGs
#
# Two-step reduction per (variable, period, climatology):
#   Step A  per-year aggregate the months in the period using the variable's
#           natural rule:
#             PTOT                                  -> sum
#             TMAX                                  -> max
#             TMIN                                  -> min
#             TAVG, SPEI-01 / 03 / 06 / 12 / 24     -> mean
#           A year is included only if ALL months in the period are present;
#           DJF / NDJ wrap year boundaries and attach to the year containing
#           January.
#   Step B  reduce the multi-year stack pixel-wise to mean, min, max, sd in
#           one terra::app pass that masks +-Inf pixel-years to NA (SPEI tail
#           values otherwise contaminate the aggregate).
#
# Run modes:
#   --smoke   PTOT only; annual + JFM + JJA; one climatology (1991-2020);
#             Kenya bbox. Six inline checks plus a PNG of one output. ~2 min
#             on CGlabs.
#   --full    All 9 variables, all 13 periods, all 3 climatologies, full
#             Africa extent. ~1-2 hours on CGlabs.
#   (none)    Usage + exit 1.
#
# Outputs are idempotent: per-(variable, period, climatology, stat) the COG
# is skipped if it already exists and has sane size.
#
# Please run R/0_server_setup.R before --full; --smoke uses
# bootstrap_minimal() which sidesteps the heavy pipeline startup downloads.

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
  pacman::p_load(terra, data.table, glue, jsonlite, fs)

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
    stop(glue::glue("Unknown project_dir '{project_dir}'. Add a mapping."))
  )
  has_data <- vapply(candidates, function(p) {
    dir.exists(file.path(p, "Data/chirts_chirps_hist/PTOT"))
  }, logical(1))
  working_dir <- if (any(has_data)) candidates[has_data][1] else candidates[1]
  log_step(sprintf("  selected working_dir: %s", working_dir))
  if (!dir.exists(working_dir)) dir.create(working_dir, recursive = TRUE)
  setwd(working_dir)

  chirts_chirps_hist_dir <- file.path("Data", "chirts_chirps_hist")
  terra::gdalCache(60000)
  list(
    project_dir = project_dir, working_dir = working_dir,
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
  pacman::p_load(terra, data.table, glue, jsonlite, fs)
  chirts_chirps_hist_dir <- atlas_dirs$data_dir$chirts_chirps_hist
} else {
  cat(
    "Usage:\n",
    "  Rscript R/observational/5_make_obs_map_climatologies.R --smoke\n",
    "      PTOT, annual + JFM + JJA, 1991-2020 climatology, Kenya bbox.\n",
    "  Rscript R/observational/5_make_obs_map_climatologies.R --full\n",
    "      All 9 vars x 13 periods x 3 climatologies x 4 stats = 1,404 COGs.\n",
    sep = ""
  )
  quit(status = 1)
}

# 2) Configuration ####

variables_full <- c(
  "PTOT", "TMAX", "TMIN", "TAVG",
  "SPEI-01", "SPEI-03", "SPEI-06", "SPEI-12", "SPEI-24"
)

# Per-variable rule for combining the months within a period (Step A).
agg_rule <- list(
  PTOT      = "sum",  TMAX      = "max",  TMIN      = "min",  TAVG      = "mean",
  `SPEI-01` = "mean", `SPEI-03` = "mean", `SPEI-06` = "mean", `SPEI-12` = "mean",
  `SPEI-24` = "mean"
)

seasons <- list(
  annual = 1:12,
  JFM = c(1, 2, 3), FMA = c(2, 3, 4), MAM = c(3, 4, 5), AMJ = c(4, 5, 6),
  MJJ = c(5, 6, 7), JJA = c(6, 7, 8), JAS = c(7, 8, 9), ASO = c(8, 9, 10),
  SON = c(9, 10, 11), OND = c(10, 11, 12), NDJ = c(11, 12, 1),
  DJF = c(12, 1, 2)
)

# Climatology windows (inclusive year bounds; NA in upper = "to latest available").
climatologies <- list(
  `1995-2014` = c(1995L, 2014L),
  `1991-2020` = c(1991L, 2020L),
  full        = c(NA_integer_, NA_integer_)
)

# COG creation options.
# - OVERVIEWS=AUTO + OVERVIEW_RESAMPLING=AVERAGE: pyramid pre-baked so
#   browser clients (notebook geotiff.js HTTP Range) can fetch ~5 KB
#   at continental zoom instead of full-resolution ~3.5 MB. Per
#   dispatches/2026-05-21_observational-cog-extent-bug-plus-optimizations.md
#   the single biggest perf win for the upcoming observational map view.
# - PREDICTOR=2 is fine for our use; PREDICTOR=3 (Float32 predictor)
#   may compress 10-20% better on smooth climatology fields — worth an
#   A/B at the per-variable level if disk footprint matters.
# - BLOCKSIZE=512 matches the existing publish layout.
# - Statistics: terra's COG writer copies min/max but writes MEAN/STDDEV
#   = -9999 sentinels. We post-process with `gdalinfo -stats` after
#   writeRaster() to populate real stats in the .aux.xml sidecar
#   (CR-076 part 2). See compute_cog_stats() further down.
cog_gdal_opts <- c(
  "COMPRESS=DEFLATE",
  "PREDICTOR=2",
  "OVERVIEWS=AUTO",
  "OVERVIEW_RESAMPLING=AVERAGE",
  "BLOCKSIZE=512"
)
maps_dir <- file.path(chirts_chirps_hist_dir, "maps")
if (!dir.exists(maps_dir)) dir.create(maps_dir, recursive = TRUE)

# Smoke scope
smoke_vars <- c("PTOT")
smoke_periods <- c("annual", "JFM", "JJA")
smoke_clim <- "1991-2020"
smoke_bbox <- c(xmin = 33.5, xmax = 42.0, ymin = -5.0, ymax = 5.5)

# Full scope
variables_run <- if (mode == "--smoke") smoke_vars else variables_full
periods_run <- if (mode == "--smoke") smoke_periods else names(seasons)
climatologies_run <- if (mode == "--smoke") climatologies[smoke_clim] else climatologies

cat("project_dir          :", project_dir, "\n")
cat("working_dir          :", getwd(), "\n")
cat("input dir            :", chirts_chirps_hist_dir, "\n")
cat("output dir           :", maps_dir, "\n")
cat("mode                 :", mode, "\n")
cat("variables            :", paste(variables_run, collapse = ", "), "\n")
cat("periods              :", paste(periods_run, collapse = ", "), "\n")
cat("climatology windows  :", paste(names(climatologies_run), collapse = ", "), "\n\n")

# 3) Helpers ####

#' Parse YYYY and MM from a {var}-YYYY-MM.tif filename.
parse_ym <- function(path) {
  m <- regmatches(basename(path), regexec("(\\d{4})-(\\d{2})\\.tif$", basename(path)))[[1]]
  c(year = as.integer(m[2]), month = as.integer(m[3]))
}

#' List the monthly tifs for one variable as a data.table (path, year, month).
list_var_files <- function(var) {
  dir_path <- file.path(chirts_chirps_hist_dir, var)
  fs <- list.files(
    dir_path,
    pattern = sprintf("^%s-[0-9]{4}-[0-9]{2}\\.tif$", var),
    full.names = TRUE
  )
  if (length(fs) == 0L) stop(glue::glue("No {var} files in {dir_path}"))
  ym <- t(vapply(fs, parse_ym, integer(2)))
  data.table::data.table(path = fs, year = ym[, 1], month = ym[, 2])[order(year, month)]
}

#' Build the (year, month) windows for a given period. Returns a data.table
#' with rows: year (the calendar year of attribution), and a list-column
#' 'months_needed' of (year, month) pairs needed for that period-year.
build_period_year_index <- function(period, year_lo, year_hi) {
  months <- seasons[[period]]
  years <- seq.int(year_lo, year_hi)
  out <- lapply(years, function(y) {
    # For NDJ / DJF the December belongs to the PREVIOUS calendar year.
    pairs <- if (period %in% c("NDJ", "DJF")) {
      data.table::data.table(
        m = months,
        y_src = ifelse(months == 12L, y - 1L, y)
      )
    } else {
      data.table::data.table(m = months, y_src = y)
    }
    list(year = y, months_needed = pairs)
  })
  out
}

#' Per-variable aggregation function for combining months WITHIN a period.
month_agg <- function(rule) {
  switch(rule,
    sum  = function(stk) sum(stk),
    max  = function(stk) max(stk),
    min  = function(stk) min(stk),
    mean = function(stk) mean(stk),
    stop(glue::glue("Unknown agg rule '{rule}'"))
  )
}

#' Compute the yearly-summary stack for one (variable, period, climatology):
#' one layer per calendar year that has all required months present.
yearly_summary_stack <- function(var, period, year_lo, year_hi, bbox = NULL) {
  files <- list_var_files(var)
  rule <- agg_rule[[var]]
  reducer <- month_agg(rule)
  pyx <- build_period_year_index(period, year_lo, year_hi)

  layers <- list()
  for (entry in pyx) {
    needed <- entry$months_needed
    fmask <- files[needed, on = c(year = "y_src", month = "m"), nomatch = 0L]
    if (nrow(fmask) != nrow(needed)) next
    stk <- terra::rast(fmask$path)
    if (!is.null(bbox)) stk <- terra::crop(stk, terra::ext(bbox))
    # SPEI tail values: mask +-Inf to NA so they don't corrupt the reducer.
    stk <- terra::ifel(is.infinite(stk), NA, stk)
    layer <- collect_warnings(
      reducer(stk),
      label = sprintf("%s %s %d Step-A", var, period, entry$year)
    )
    names(layer) <- as.character(entry$year)
    layers[[as.character(entry$year)]] <- layer
  }
  if (length(layers) == 0L) {
    return(NULL)
  }
  terra::rast(layers)
}

#' Reduce a yearly stack to (mean, min, max, sd) per pixel in one app pass.
reduce_to_stats <- function(yearly_stk) {
  fun <- function(x) {
    v <- x[is.finite(x)]
    if (length(v) < 3L) {
      return(rep(NA_real_, 4))
    }
    c(mean(v), min(v), max(v), stats::sd(v))
  }
  out <- collect_warnings(
    terra::app(yearly_stk, fun = fun),
    label = "Step-B reduce"
  )
  names(out) <- c("mean", "min", "max", "sd")
  out
}

#' Post-process a freshly-written COG to populate real STATISTICS_MEAN /
#' STATISTICS_STDDEV (CR-076 part 2). terra::writeRaster(filetype = "COG")
#' copies min/max from the source raster but writes -9999 sentinels for
#' mean / stddev because GDAL's COG driver does NOT compute them during
#' creation. `gdalinfo -stats` forces a full-pass scan and writes the
#' real values into the .aux.xml sidecar; downstream readers (including
#' the notebook's geotiff.js client) pick them up via the standard
#' PAM lookup. Adds one extra full read per file — small cost relative
#' to the per-COG bake time.
compute_cog_stats <- function(path) {
  if (!nzchar(Sys.which("gdalinfo"))) {
    return(invisible(NULL))   # GDAL CLI not on PATH; silently skip.
  }
  res <- suppressWarnings(system2("gdalinfo",
    args = c("-stats", "-mm", shQuote(path)),
    stdout = FALSE, stderr = FALSE))
  if (res != 0L) {
    warning(sprintf("gdalinfo -stats failed (rc=%d) for %s",
                    res, basename(path)))
  }
  invisible(NULL)
}

#' Resolve the year range for a climatology window, given the variable's
#' available years (so 'full' truncates correctly).
resolve_clim_years <- function(clim_bounds, available_years) {
  lo <- if (is.na(clim_bounds[1])) min(available_years) else max(clim_bounds[1], min(available_years))
  hi <- if (is.na(clim_bounds[2])) max(available_years) else min(clim_bounds[2], max(available_years))
  c(lo, hi)
}

# 4) Main loop ####

written <- character()
# Parallelism setup: workload scales nicely across variables - each handles
# its own (periods x climwindows x stats) sequentially. Per-worker peak RSS
# observed ~10 GB. Worker count auto-scales from cgroup-aware free RAM.
source(file.path(project_dir, "R", "observational", "_helpers.R"))
pacman::p_load(future, future.apply, furrr)

overwrite <- parse_overwrite_flag(args)
if (overwrite) log_step("--overwrite set: existing outputs will be rebuilt")

per_worker_gb <- 10
workers <- resolve_workers(args, per_worker_gb = per_worker_gb,
                           max_workers = length(variables_run))
print_resource_banner(workers, per_worker_gb, label = "climatology")

backend <- parse_cli_flag(args, "backend", "character")
if (is.null(backend)) {
  backend <- if (.Platform$OS.type == "unix" &&
    !grepl("darwin", R.version$os, ignore.case = TRUE)) "multicore" else "multisession"
}

#' Compute all (period x climwindow x stat) COGs for one variable. Returns
#' the vector of output paths written. Self-contained so it serialises
#' cleanly to PSOCK workers when --backend multisession is used.
process_variable <- function(var) {
  # CRITICAL: smoke runs use a Kenya bbox (smoke_bbox) and previously wrote
  # to the SAME output path as the full bake — overwriting Africa-wide
  # outputs with a Kenya crop. Caused the CR-076 / "4 PTOT_annual_1991-2020
  # files at 170x210 px" bug surfaced 2026-05-21. Namespace the smoke
  # outputs under a `_smoke/` subtree so they can NEVER collide with the
  # production bake. The smoke validation block at the bottom of this
  # script reads from this subdir; the publish layer never sees it.
  out_var_dir <- if (mode == "--smoke") {
    file.path(maps_dir, "_smoke", var)
  } else {
    file.path(maps_dir, var)
  }
  if (!dir.exists(out_var_dir)) dir.create(out_var_dir, recursive = TRUE)
  files <- list_var_files(var)
  available_years <- sort(unique(files$year))
  written_local <- character()

  for (clim_name in names(climatologies_run)) {
    bounds <- resolve_clim_years(climatologies_run[[clim_name]], available_years)
    log_step(sprintf("=== %s / %s (%d-%d) ===", var, clim_name, bounds[1], bounds[2]))
    for (period in periods_run) {
      expected <- file.path(
        out_var_dir,
        sprintf("%s_%s_%s_%s.tif", var, period, clim_name,
                c("mean", "min", "max", "sd"))
      )
      if (!overwrite && all(file.exists(expected)) &&
          all(file.size(expected) > 100L)) {
        log_step(sprintf("  %s / %s: all 4 stats present, skipping", var, period))
        written_local <- c(written_local, expected)
        next
      }

      t0 <- Sys.time()
      bbox <- if (mode == "--smoke") smoke_bbox else NULL
      yearly_stk <- yearly_summary_stack(var, period, bounds[1], bounds[2], bbox = bbox)
      if (is.null(yearly_stk) || terra::nlyr(yearly_stk) < 3L) {
        log_step(sprintf("  %s / %s: insufficient yearly summaries (%s); skipping",
          var, period,
          if (is.null(yearly_stk)) "0" else terra::nlyr(yearly_stk)))
        next
      }

      stats_stk <- reduce_to_stats(yearly_stk)
      for (k in seq_len(4)) {
        stat_name <- c("mean", "min", "max", "sd")[k]
        out_path <- file.path(out_var_dir,
          sprintf("%s_%s_%s_%s.tif", var, period, clim_name, stat_name))
        collect_warnings(
          terra::writeRaster(stats_stk[[k]], out_path,
            overwrite = TRUE, filetype = "COG", gdal = cog_gdal_opts),
          label = sprintf("write %s", basename(out_path))
        )
        # CR-076 part 2: populate real STATISTICS_MEAN / STDDEV via
        # gdalinfo -stats sidecar. Small extra pass per file (~1 s).
        compute_cog_stats(out_path)
        written_local <- c(written_local, out_path)
      }

      log_step(sprintf("  %s / %s (n_years=%d) -> 4 COGs in %.1fs",
        var, period, terra::nlyr(yearly_stk),
        as.numeric(Sys.time() - t0, units = "secs")))
    }
  }

  # Per-variable sidecar
  jsonlite::write_json(
    list(
      variable = var,
      aggregation_rule_within_period = agg_rule[[var]],
      across_year_stats = c("mean", "min", "max", "sd"),
      climatologies = lapply(names(climatologies_run), function(n) {
        b <- climatologies_run[[n]]
        list(name = n,
             start = if (is.na(b[1])) "min_available" else b[1],
             end   = if (is.na(b[2])) "max_available" else b[2])
      }),
      periods = periods_run,
      inf_handling = "+-Inf masked to NA before any reducer",
      format = "Cloud-Optimized GeoTIFF (DEFLATE PREDICTOR=2 BLOCKSIZE=512)",
      build_time = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
      parent_script = "R/observational/5_make_obs_map_climatologies.R"
    ),
    path = file.path(out_var_dir, "_metadata.json"),
    pretty = TRUE, auto_unbox = TRUE
  )
  written_local
}

# Dispatch
if (workers > 1L) {
  if (backend == "multicore") {
    future::plan(future::multicore, workers = workers)
  } else {
    future::plan(future::multisession, workers = workers)
  }
} else {
  future::plan(future::sequential)
  backend <- "sequential"
}
log_step(sprintf(
  "parallel climatology across %d variables, %d workers (%s)",
  length(variables_run), workers, backend
))

results <- furrr::future_map(variables_run, process_variable,
  # stdout = FALSE so worker cat() lines stream to the parent's stdout
  # (and thus to the nohup log) in real time, rather than being captured
  # in memory and only released when future_map() returns.
  .options = furrr::furrr_options(seed = TRUE, stdout = FALSE))
future::plan(future::sequential)
written <- c(written, unlist(results))

# 5) Smoke verification ####

if (mode == "--smoke") {
  log_step("=== VERIFICATION CHECKS ===")
  pass <- TRUE

  expected_files <- file.path(
    maps_dir, "PTOT",
    sprintf(
      "PTOT_%s_%s_%s.tif", rep(smoke_periods, each = 4),
      smoke_clim, c("mean", "min", "max", "sd")
    )
  )
  present <- file.exists(expected_files) & file.size(expected_files) > 100L
  if (all(present)) {
    cat(sprintf("[OK] 1. All %d expected COGs present.\n", length(expected_files)))
  } else {
    cat(sprintf(
      "[FAIL] 1. %d/%d expected COGs missing.\n",
      sum(!present), length(expected_files)
    ))
    cat("Missing:\n")
    cat(paste0("  ", expected_files[!present], collapse = "\n"), "\n")
    pass <- FALSE
  }

  smp <- expected_files[present][seq_len(min(6L, sum(present)))]
  ok <- vapply(smp, function(f) {
    tryCatch(
      {
        terra::rast(f) + 0
        TRUE
      },
      error = function(e) FALSE
    )
  }, logical(1))
  if (all(ok)) {
    cat(sprintf(
      "[OK] 2. COG integrity: %d / %d sampled files parse cleanly.\n",
      sum(ok), length(ok)
    ))
  } else {
    cat(sprintf("[FAIL] 2. COG integrity: %d failed.\n", sum(!ok)))
    pass <- FALSE
  }

  # Each PTOT_annual_*_mean should be roughly 12x a PTOT_JFM_*_mean (because
  # JFM is 3 months and annual is 12, both summed). Loose check: ratio in [2, 8].
  ann_mean <- terra::rast(file.path(
    maps_dir, "PTOT",
    sprintf("PTOT_annual_%s_mean.tif", smoke_clim)
  ))
  jfm_mean <- terra::rast(file.path(
    maps_dir, "PTOT",
    sprintf("PTOT_JFM_%s_mean.tif", smoke_clim)
  ))
  ratio <- terra::global(ann_mean, "mean", na.rm = TRUE)[1, 1] /
    terra::global(jfm_mean, "mean", na.rm = TRUE)[1, 1]
  if (is.finite(ratio) && ratio >= 2 && ratio <= 8) {
    cat(sprintf("[OK] 3. annual / JFM PTOT ratio = %.2f (plausible for Kenya).\n", ratio))
  } else {
    cat(sprintf("[FAIL] 3. annual / JFM PTOT ratio = %.3f (outside [2, 8]).\n", ratio))
    pass <- FALSE
  }

  # sd should be > 0 in non-NA regions.
  sd_r <- terra::rast(file.path(
    maps_dir, "PTOT",
    sprintf("PTOT_annual_%s_sd.tif", smoke_clim)
  ))
  sd_min <- terra::global(sd_r, "min", na.rm = TRUE)[1, 1]
  sd_max <- terra::global(sd_r, "max", na.rm = TRUE)[1, 1]
  if (is.finite(sd_min) && sd_min >= 0 && sd_max > 0) {
    cat(sprintf("[OK] 4. PTOT annual SD range: %.2f .. %.2f mm.\n", sd_min, sd_max))
  } else {
    cat(sprintf(
      "[FAIL] 4. PTOT annual SD range suspicious: %.3f .. %.3f.\n",
      sd_min, sd_max
    ))
    pass <- FALSE
  }

  # max >= mean >= min should hold.
  max_r <- terra::rast(file.path(
    maps_dir, "PTOT",
    sprintf("PTOT_annual_%s_max.tif", smoke_clim)
  ))
  min_r <- terra::rast(file.path(
    maps_dir, "PTOT",
    sprintf("PTOT_annual_%s_min.tif", smoke_clim)
  ))
  bad <- terra::values(max_r) < terra::values(ann_mean) |
    terra::values(min_r) > terra::values(ann_mean)
  n_bad <- sum(bad, na.rm = TRUE)
  if (n_bad == 0L) {
    cat("[OK] 5. min <= mean <= max holds across all pixels.\n")
  } else {
    cat(sprintf("[FAIL] 5. %d pixels violate min <= mean <= max.\n", n_bad))
    pass <- FALSE
  }

  png_path <- file.path(maps_dir, "_map_smoke_PTOT_annual_mean.png")
  grDevices::png(png_path, width = 1200, height = 900, res = 120)
  terra::plot(ann_mean, main = sprintf("PTOT annual mean %s (smoke)", smoke_clim))
  grDevices::dev.off()
  png_sz <- file.info(png_path)$size
  if (!is.na(png_sz) && png_sz > 1024) {
    cat(sprintf("[OK] 6. PNG round-trip: %s (%d KB).\n", png_path, round(png_sz / 1024)))
  } else {
    cat(sprintf("[FAIL] 6. PNG too small: %s.\n", png_path))
    pass <- FALSE
  }

  cat("\n=== WARNINGS COLLECTED:", length(warnings_collected$entries), "===\n")
  if (length(warnings_collected$entries) > 0L) {
    tab <- sort(table(sub(":.*$", "", warnings_collected$entries)), decreasing = TRUE)
    for (nm in names(tab)) cat(sprintf("  %5d  %s\n", tab[[nm]], nm))
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

log_step(sprintf("Full build complete. %d COGs written.", length(written)))
cat("\nWarnings collected:", length(warnings_collected$entries), "\n")
if (length(warnings_collected$entries) > 0L) {
  tab <- sort(table(sub(":.*$", "", warnings_collected$entries)), decreasing = TRUE)
  for (nm in names(tab)) cat(sprintf("  %5d  %s\n", tab[[nm]], nm))
}
# Final flush so the wrap-up summary lands in the log file even if R exits
# right after this (otherwise the buffered final lines can be lost).
flush.console()
