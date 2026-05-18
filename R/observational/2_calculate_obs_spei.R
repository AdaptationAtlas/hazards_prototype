# 0) Introduction ####
# Compute monthly SPEI (Standardized Precipitation Evapotranspiration Index)
# at pixel level over the CHIRPS / CHIRTS-ERA5 observational stack written by
# R/observational/1_get_chirps_chirts.R. SPEI is produced at scales 1, 3, 6, 12, 24,
# fit on the 1991-2020 reference period.
#
# Pipeline:
#   1) Build a 12-layer monthly extraterrestrial radiation raster Ra(lat, m)
#      from the obs_base_rast latitude (Hargreaves 1985 closed-form).
#   2) For every available month, compute Hargreaves PET (mm / month) from
#      TMAX, TMIN, TAVG, Ra, and days_in_month.
#   3) Build the climatic water balance CWB = PTOT - PET as a multilayer stack.
#   4) For each scale in {1, 3, 6, 12, 24}, run terra::app over CWB calling
#      SPEI::spei() pixel-wise with the 1991-2020 reference period; write one
#      COG per (scale, year, month) into Data/chirts_chirps_hist/SPEI-{NN}/.
#
# Inputs (already on disk after R/observational/1_get_chirps_chirts.R --full):
#   Data/chirts_chirps_hist/{PTOT,TMAX,TMIN,TAVG}/{VAR}-YYYY-MM.tif
#
# Outputs:
#   Data/chirts_chirps_hist/SPEI-{01,03,06,12,24}/SPEI-{scale}-YYYY-MM.tif
#   Data/chirts_chirps_hist/SPEI-{scale}/_metadata.json
#
# Run modes:
#   --smoke   Single scale (3), bbox cropped to Kenya, 1985-2020 window.
#             Runs sanity checks: PET reasonable range, SPEI ~ N(0,1) over
#             the reference period, no all-NA pixels in a populated region.
#             Reference period MUST sit inside the smoke window.
#   --full    All five scales over the full Africa extent, every available
#             month. Heavy - run on Afrilabs / CGlabs, not the Mac.
#   (none)    Print usage + exit 1.
#
# Progress + warnings: every meaningful step is wrapped in log_step() (which
# flushes immediately) and any captured warnings get collected into
# warnings_collected and dumped at the end.
#
# Please run 0_server_setup.R before executing in --full mode; --smoke uses
# the minimal bootstrap helper to skip the heavy startup downloads.

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
  pacman::p_load(
    terra, data.table, glue, jsonlite, fs,
    future, future.apply, progressr, SPEI
  )

  project_dir <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else getwd()
  # Candidate working_dirs per machine. On CGlabs the same project_dir feeds
  # two climdat_source paths (atlas_delta vs nexgddp); pick whichever already
  # has chirts_chirps_hist/PTOT/ on disk.
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
    stop("Run R/observational/1_get_chirps_chirts.R --full before computing SPEI.")
  }

  terra::gdalCache(60000)
  options(timeout = 600)

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
  pacman::p_load(terra, data.table, glue, jsonlite, fs, future, future.apply, progressr, SPEI)
  chirts_chirps_hist_dir <- atlas_dirs$data_dir$chirts_chirps_hist
  if (!dir.exists(chirts_chirps_hist_dir)) {
    stop("Run R/observational/1_get_chirps_chirts.R --full before computing SPEI.")
  }
} else {
  cat(
    "Usage:\n",
    "  Rscript R/observational/2_calculate_obs_spei.R --smoke\n",
    "      Single scale (3), Kenya bbox, 1985-2020. Quick correctness check.\n",
    "  Rscript R/observational/2_calculate_obs_spei.R --full\n",
    "      All scales (1/3/6/12/24), full Africa, every available month.\n",
    sep = ""
  )
  quit(status = 1)
}

# 2) Configuration ####

scales_full <- c(1L, 3L, 6L, 12L, 24L)
scales_smoke <- c(3L)
ref_start_ym <- c(1991L, 1L)
ref_end_ym <- c(2020L, 12L)
smoke_bbox <- c(xmin = 33.5, xmax = 42.0, ymin = -5.0, ymax = 5.5)
smoke_years <- 1985L:2020L

cog_gdal_opts <- c(
  "COMPRESS=DEFLATE",
  "PREDICTOR=2",
  "OVERVIEWS=NONE",
  "BLOCKSIZE=512"
)
source("R/observational/_helpers.R")
# terra::app per-pixel SPEI fit streams blocks; memory footprint per worker is
# modest. 2 GB / worker is conservative for full Africa.
per_worker_gb <- 2
n_cores_spei <- resolve_workers(args, per_worker_gb = per_worker_gb, max_workers = 16L)
n_cores_pet  <- n_cores_spei
print_resource_banner(n_cores_spei, per_worker_gb, label = "spei")

# Mid-month day-of-year for monthly mean Ra (FAO-56 Tab. A2.2 approximation).
mid_month_doy <- c(15L, 46L, 75L, 105L, 135L, 162L, 198L, 228L, 258L, 288L, 318L, 344L)

cat("project_dir          :", project_dir, "\n")
cat("working_dir          :", getwd(), "\n")
cat("input dir            :", chirts_chirps_hist_dir, "\n")
cat("mode                 :", mode, "\n")
cat("scales               :", paste(if (mode == "--smoke") scales_smoke else scales_full, collapse = ", "), "\n")
cat(
  "reference period     :",
  sprintf("%04d-%02d", ref_start_ym[1], ref_start_ym[2]), "to",
  sprintf("%04d-%02d", ref_end_ym[1], ref_end_ym[2]), "\n\n"
)

# 3) Helpers ####

#' Parse a chirts_chirps_hist tif into a (year, month) integer pair.
parse_ym <- function(path) {
  m <- regmatches(basename(path), regexec("([0-9]{4})-([0-9]{2})\\.tif$", basename(path)))[[1]]
  c(year = as.integer(m[2]), month = as.integer(m[3]))
}

#' List the variable directory and return a data.table with parsed year, month.
list_var_files <- function(var, hist_dir = chirts_chirps_hist_dir) {
  dir_path <- file.path(hist_dir, var)
  fs <- list.files(dir_path, pattern = sprintf("^%s-[0-9]{4}-[0-9]{2}\\.tif$", var), full.names = TRUE)
  if (length(fs) == 0L) stop(glue::glue("No {var} files in {dir_path}"))
  ym <- t(vapply(fs, parse_ym, integer(2)))
  dt <- data.table::data.table(path = fs, year = ym[, 1], month = ym[, 2])
  dt[order(year, month)]
}

#' Days in a (year, month).
days_in_month <- function(year, month) {
  d <- as.Date(sprintf("%04d-%02d-15", year, month))
  next_month <- as.Date(sprintf("%04d-%02d-01", year + (month == 12), (month %% 12) + 1))
  prev_first <- as.Date(sprintf("%04d-%02d-01", year, month))
  as.integer(next_month - prev_first)
}

#' Build a 12-layer monthly Ra raster (MJ / m^2 / day) for the given template,
#' using the FAO-56 Allen et al. (1998) closed-form solution. Latitude is read
#' from the template's geometry.
build_ra_raster <- function(template) {
  log_step("Building 12-layer Ra (extraterrestrial radiation) raster")
  lat_deg <- terra::init(template, "y")
  phi <- lat_deg * (pi / 180)
  gsc <- 0.0820 # solar constant MJ / (m^2 * min)
  ra_list <- lapply(seq_len(12), function(m) {
    j <- mid_month_doy[m]
    dr <- 1 + 0.033 * cos(2 * pi * j / 365)
    dec <- 0.409 * sin(2 * pi * j / 365 - 1.39)
    # Clamp tan(phi) * tan(dec) to [-1, 1] before acos for polar safety.
    arg <- terra::clamp(-tan(phi) * tan(dec), -1, 1, values = TRUE)
    omega_s <- acos(arg)
    ra <- (24 * 60 / pi) * gsc * dr *
      (omega_s * sin(phi) * sin(dec) + cos(phi) * cos(dec) * sin(omega_s))
    names(ra) <- sprintf("Ra-%02d", m)
    ra
  })
  out <- terra::rast(ra_list)
  log_step(sprintf(
    "  Ra range: %.2f .. %.2f MJ/m^2/day",
    terra::global(out, "min", na.rm = TRUE)[1, 1],
    terra::global(out, "max", na.rm = TRUE)[1, 1]
  ))
  out
}

#' Hargreaves monthly PET (mm / month). Inputs are SpatRaster aligned on the
#' same grid; ra_m is the calendar-month Ra layer (mm-equivalent applied here).
#'   PET_day = 0.0023 * 0.408 * Ra * (Tavg + 17.8) * sqrt(Tmax - Tmin)
#'   PET_mon = PET_day * days_in_month
hargreaves_pet_month <- function(tmax, tmin, tavg, ra_m, n_days) {
  # sqrt of negative (very rare with sentinel-masked CHIRTS) becomes NA.
  delta_t <- tmax - tmin
  delta_t <- terra::clamp(delta_t, lower = 0, values = TRUE)
  pet_day <- 0.0023 * 0.408 * ra_m * (tavg + 17.8) * sqrt(delta_t)
  pet_day * n_days
}

#' Build a fully self-contained per-pixel SPEI closure. terra::app(cores = N)
#' spawns PSOCK workers that don't see the parent's global env, so the
#' function passed to app() must carry its own captured args AND its own
#' package handle. force() resolves the args eagerly into the closure env;
#' SPEI is loaded inside the closure on first call per worker.
make_spei_fn <- function(scale, ts_start, ref_start, ref_end) {
  force(scale)
  force(ts_start)
  force(ref_start)
  force(ref_end)
  function(x) {
    if (sum(!is.na(x)) < (scale + 24L)) {
      return(rep(NA_real_, length(x)))
    }
    if (!"SPEI" %in% loadedNamespaces()) {
      requireNamespace("SPEI", quietly = TRUE)
    }
    ts_x <- stats::ts(x, start = ts_start, frequency = 12L)
    out <- tryCatch(
      suppressWarnings(
        SPEI::spei(ts_x,
          scale = scale, ref.start = ref_start, ref.end = ref_end,
          na.rm = TRUE, verbose = FALSE
        )
      ),
      error = function(e) NULL
    )
    if (is.null(out)) {
      return(rep(NA_real_, length(x)))
    }
    as.numeric(out$fitted)
  }
}

# 4) Build aligned monthly stacks for PTOT / TMAX / TMIN / TAVG ####

log_step("Listing input files for PTOT / TMAX / TMIN / TAVG")
inv <- list(
  PTOT = list_var_files("PTOT"),
  TMAX = list_var_files("TMAX"),
  TMIN = list_var_files("TMIN"),
  TAVG = list_var_files("TAVG")
)
for (v in names(inv)) {
  log_step(sprintf(
    "  %s: %d files (%d-%d to %d-%d)",
    v, nrow(inv[[v]]),
    inv[[v]]$year[1], inv[[v]]$month[1],
    inv[[v]]$year[nrow(inv[[v]])], inv[[v]]$month[nrow(inv[[v]])]
  ))
}

# Restrict to (year, month) tuples present in ALL four variables, then attach
# the per-variable path columns. Plain merges keep the data.table semantics
# obvious.
common <- inv$PTOT[, .(year, month, PTOT = path)]
common <- merge(common, inv$TMAX[, .(year, month, TMAX = path)], by = c("year", "month"))
common <- merge(common, inv$TMIN[, .(year, month, TMIN = path)], by = c("year", "month"))
common <- merge(common, inv$TAVG[, .(year, month, TAVG = path)], by = c("year", "month"))
data.table::setorder(common, year, month)
log_step(sprintf(
  "Common monthly coverage: %d months (%d-%02d to %d-%02d)",
  nrow(common),
  common$year[1], common$month[1],
  common$year[nrow(common)], common$month[nrow(common)]
))

if (mode == "--smoke") {
  common <- common[year %in% smoke_years]
  log_step(sprintf(
    "  Smoke: filtered to %d months in %d..%d",
    nrow(common), min(smoke_years), max(smoke_years)
  ))
}
log_step(sprintf("Final monthly index: %d rows", nrow(common)))

# 5) Decide which scales need computing (idempotent skip) ####

scales_run <- if (mode == "--smoke") scales_smoke else scales_full
ts_start <- c(common$year[1], common$month[1])

#' For a given scale, return the vector of expected output paths.
expected_spei_paths <- function(scale, common, hist_dir) {
  out_dir <- file.path(hist_dir, sprintf("SPEI-%02d", scale))
  file.path(out_dir, sprintf("SPEI-%02d-%04d-%02d.tif", scale, common$year, common$month))
}

log_step("Checking existing SPEI outputs per scale")
scales_to_compute <- integer()
for (scale in scales_run) {
  expected <- expected_spei_paths(scale, common, chirts_chirps_hist_dir)
  present <- file.exists(expected) & file.size(expected) > 100L
  if (all(present)) {
    log_step(sprintf("  scale %d: all %d outputs present - will skip", scale, length(expected)))
  } else {
    log_step(sprintf("  scale %d: %d/%d missing - will compute",
      scale, sum(!present), length(expected)))
    scales_to_compute <- c(scales_to_compute, scale)
  }
}

# 6) Compute Hargreaves PET and CWB stacks (only if anything needs computing) ####

if (length(scales_to_compute) > 0L) {
  log_step("Loading input stacks (lazy)")
  ptot_stk <- terra::rast(common$PTOT)
  tmax_stk <- terra::rast(common$TMAX)
  tmin_stk <- terra::rast(common$TMIN)
  tavg_stk <- terra::rast(common$TAVG)

  if (mode == "--smoke") {
    bb <- terra::ext(smoke_bbox)
    log_step(sprintf(
      "Cropping to smoke bbox: lon [%.2f, %.2f] lat [%.2f, %.2f]",
      smoke_bbox["xmin"], smoke_bbox["xmax"], smoke_bbox["ymin"], smoke_bbox["ymax"]
    ))
    ptot_stk <- terra::crop(ptot_stk, bb)
    tmax_stk <- terra::crop(tmax_stk, bb)
    tmin_stk <- terra::crop(tmin_stk, bb)
    tavg_stk <- terra::crop(tavg_stk, bb)
  }

  ra_stk <- build_ra_raster(ptot_stk[[1]])

  log_step(sprintf("Computing monthly Hargreaves PET for %d months", nrow(common)))
  t_pet <- Sys.time()
  pet_layers <- vector("list", nrow(common))
  for (i in seq_len(nrow(common))) {
    m <- common$month[i]
    y <- common$year[i]
    n <- days_in_month(y, m)
    pet_layers[[i]] <- collect_warnings(
      hargreaves_pet_month(tmax_stk[[i]], tmin_stk[[i]], tavg_stk[[i]], ra_stk[[m]], n),
      label = sprintf("PET %04d-%02d", y, m)
    )
    if (i %% 60L == 0L || i == nrow(common)) {
      log_step(sprintf(
        "  PET %d/%d (%04d-%02d) [%.1fs elapsed]",
        i, nrow(common), y, m, as.numeric(Sys.time() - t_pet, units = "secs")
      ))
    }
  }
  pet_stk <- terra::rast(pet_layers)
  log_step(sprintf(
    "PET stack: %d layers, range %.2f..%.2f mm/month",
    terra::nlyr(pet_stk),
    terra::global(pet_stk, "min", na.rm = TRUE)[1, 1],
    terra::global(pet_stk, "max", na.rm = TRUE)[1, 1]
  ))

  log_step("Computing climatic water balance CWB = PTOT - PET")
  cwb_stk <- ptot_stk - pet_stk
  log_step(sprintf(
    "  CWB range: %.2f..%.2f mm/month",
    terra::global(cwb_stk, "min", na.rm = TRUE)[1, 1],
    terra::global(cwb_stk, "max", na.rm = TRUE)[1, 1]
  ))
} else {
  log_step("All scales already complete - skipping PET / CWB compute entirely")
}

cwb_starts_in_ref <- common$year[1] < ref_start_ym[1] ||
  (common$year[1] == ref_start_ym[1] && common$month[1] <= ref_start_ym[2])
cwb_ends_after_ref <- common$year[nrow(common)] > ref_end_ym[1] ||
  (common$year[nrow(common)] == ref_end_ym[1] && common$month[nrow(common)] >= ref_end_ym[2])
if (!(cwb_starts_in_ref && cwb_ends_after_ref)) {
  stop(glue::glue(
    "Reference period {ref_start_ym[1]}-{ref_start_ym[2]:02d}..{ref_end_ym[1]}-{ref_end_ym[2]:02d} ",
    "is outside the CWB coverage {common$year[1]}-{common$month[1]:02d}..",
    "{common$year[nrow(common)]}-{common$month[nrow(common)]:02d}"
  ))
}

written_paths <- character()
for (scale in scales_run) {
  out_dir <- file.path(chirts_chirps_hist_dir, sprintf("SPEI-%02d", scale))
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

  if (!(scale %in% scales_to_compute)) {
    log_step(sprintf("=== SPEI scale = %d (already complete, skipping compute) ===", scale))
    written_paths <- c(written_paths, expected_spei_paths(scale, common, chirts_chirps_hist_dir))
    next
  }

  log_step(sprintf("=== SPEI scale = %d ===", scale))
  t_scale <- Sys.time()
  spei_fn <- make_spei_fn(scale, ts_start, ref_start_ym, ref_end_ym)
  spei_stk <- collect_warnings(
    terra::app(cwb_stk, fun = spei_fn, cores = n_cores_spei),
    label = sprintf("SPEI-%02d app", scale)
  )
  log_step(sprintf("  SPEI computed in %.1fs", as.numeric(Sys.time() - t_scale, units = "secs")))

  # Write one COG per month.
  for (i in seq_len(nrow(common))) {
    out_name <- sprintf("SPEI-%02d-%04d-%02d.tif", scale, common$year[i], common$month[i])
    out_path <- file.path(out_dir, out_name)
    collect_warnings(
      terra::writeRaster(
        spei_stk[[i]], out_path,
        overwrite = TRUE,
        filetype = "COG",
        gdal = cog_gdal_opts
      ),
      label = sprintf("write %s", out_name)
    )
    written_paths <- c(written_paths, out_path)
    if (i %% 60L == 0L || i == nrow(common)) {
      log_step(sprintf(
        "  wrote %d/%d (%04d-%02d)",
        i, nrow(common), common$year[i], common$month[i]
      ))
    }
  }

  # JSON sidecar.
  jsonlite::write_json(list(
    variable = sprintf("SPEI-%02d", scale),
    method = "Hargreaves PET (FAO-56 Allen 1998) -> CWB -> SPEI::spei (log-Logistic, ub-pwm)",
    reference_period = sprintf(
      "%04d-%02d to %04d-%02d",
      ref_start_ym[1], ref_start_ym[2], ref_end_ym[1], ref_end_ym[2]
    ),
    scale_months = scale,
    distribution = "log-Logistic",
    fit = "ub-pwm (unbiased probability-weighted moments)",
    n_files = nrow(common),
    year_range = sprintf(
      "%04d-%02d to %04d-%02d",
      common$year[1], common$month[1],
      common$year[nrow(common)], common$month[nrow(common)]
    ),
    format = "Cloud-Optimized GeoTIFF (DEFLATE PREDICTOR=2 BLOCKSIZE=512)",
    parent_script = "R/observational/2_calculate_obs_spei.R",
    date_created = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    notes = "Pixel-level SPEI from CHIRPS v3 + CHIRTS-ERA5 monthly stack."
  ), path = file.path(out_dir, "_metadata.json"), pretty = TRUE, auto_unbox = TRUE)
}

# 7) Smoke verification ####

if (mode == "--smoke") {
  log_step("=== VERIFICATION CHECKS ===")
  pass <- TRUE

  scale <- scales_smoke[1]
  out_dir <- file.path(chirts_chirps_hist_dir, sprintf("SPEI-%02d", scale))
  out_files <- list.files(out_dir, pattern = "^SPEI-.*\\.tif$", full.names = TRUE)
  expected <- nrow(common)

  # Check 1: file count.
  if (length(out_files) != expected) {
    cat(sprintf("[FAIL] 1. File count: %d / %d expected.\n", length(out_files), expected))
    pass <- FALSE
  } else {
    cat(sprintf("[OK] 1. File count: %d SPEI-%02d COGs written.\n", length(out_files), scale))
  }

  # Check 2: COG integrity for a sample.
  smp <- out_files[seq(1, length(out_files), length.out = min(6L, length(out_files)))]
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
    cat(sprintf("[OK] 2. COG integrity: %d / %d sampled files parse cleanly.\n", sum(ok), length(ok)))
  } else {
    cat(sprintf("[FAIL] 2. COG integrity: %d / %d failed.\n", sum(!ok), length(ok)))
    pass <- FALSE
  }

  # Check 3: per-pixel SPEI distribution over the reference period ~ N(0,1).
  # SPEI is standardized PER PIXEL, so the temporal ref-period mean per pixel
  # should be ~ 0 and temporal SD ~ 1. Filter !is.finite() before averaging:
  # SPEI::spei() can legitimately emit +-Inf for pixel-months at the tails of
  # the fitted log-logistic distribution; those values should not poison
  # the aggregate.
  ref_files <- out_files[as.integer(sub("^.*-(\\d{4})-\\d{2}\\.tif$", "\\1", basename(out_files))) %in%
    ref_start_ym[1]:ref_end_ym[1]]
  if (length(ref_files) >= 24L) {
    ref_stk <- terra::rast(ref_files)
    finite_mean <- function(x) {
      x <- x[is.finite(x)]
      if (length(x) == 0L) return(NA_real_)
      mean(x)
    }
    finite_sd <- function(x) {
      x <- x[is.finite(x)]
      if (length(x) < 2L) return(NA_real_)
      sd(x)
    }
    per_pixel_mean <- terra::app(ref_stk, finite_mean)
    per_pixel_sd <- terra::app(ref_stk, finite_sd)
    ref_mean <- terra::global(per_pixel_mean, "mean", na.rm = TRUE)[1, 1]
    ref_sd <- terra::global(per_pixel_sd, "mean", na.rm = TRUE)[1, 1]
    per_pixel_inf <- terra::app(ref_stk, function(x) sum(is.infinite(x)))
    inf_cells <- terra::global(per_pixel_inf, "sum", na.rm = TRUE)[1, 1]
    inf_pct <- 100 * inf_cells / (terra::nlyr(ref_stk) * terra::ncell(ref_stk))
    cat(sprintf("    Inf cells in ref-period stack: %d (%.4f%% of cell-months)\n", inf_cells, inf_pct))
    if (is.finite(ref_mean) && is.finite(ref_sd) &&
      abs(ref_mean) < 0.15 && abs(ref_sd - 1) < 0.20) {
      cat(sprintf("[OK] 3. Per-pixel ref SPEI ~ N(0,1): mean=%.3f sd=%.3f.\n", ref_mean, ref_sd))
    } else {
      cat(sprintf("[FAIL] 3. Per-pixel ref SPEI distribution off: mean=%.3f sd=%.3f.\n", ref_mean, ref_sd))
      pass <- FALSE
    }
  } else {
    cat(sprintf("[FAIL] 3. Not enough reference-period files (%d) for distribution check.\n", length(ref_files)))
    pass <- FALSE
  }

  # Check 4: not-all-NA in populated region (sample center pixel).
  smp_r <- terra::rast(out_files[length(out_files) %/% 2])
  center_v <- terra::extract(smp_r, cbind(
    mean(smoke_bbox[c("xmin", "xmax")]),
    mean(smoke_bbox[c("ymin", "ymax")])
  ))[1, 1]
  if (!is.na(center_v)) {
    cat(sprintf("[OK] 4. Center-pixel value finite: %.3f.\n", center_v))
  } else {
    cat("[FAIL] 4. Center-pixel SPEI is NA.\n")
    pass <- FALSE
  }

  # Check 5: PET range plausible (Africa monthly PET ~ 30..400 mm). Only
  # available when PET was actually built this run; idempotent re-runs that
  # skip the compute path won't have pet_stk in scope.
  if (exists("pet_stk", inherits = FALSE)) {
    pet_min <- terra::global(pet_stk, "min", na.rm = TRUE)[1, 1]
    pet_max <- terra::global(pet_stk, "max", na.rm = TRUE)[1, 1]
    if (pet_min >= 0 && pet_max < 600) {
      cat(sprintf("[OK] 5. PET range plausible: %.1f .. %.1f mm/month.\n", pet_min, pet_max))
    } else {
      cat(sprintf("[FAIL] 5. PET range suspicious: %.1f .. %.1f mm/month.\n", pet_min, pet_max))
      pass <- FALSE
    }
  } else {
    cat("[SKIP] 5. PET range check skipped (compute path skipped this run).\n")
  }

  # Check 6: PNG validation plot.
  png_path <- file.path(chirts_chirps_hist_dir, sprintf("_spei_smoke_%02d.png", scale))
  grDevices::png(png_path, width = 1200, height = 900, res = 120)
  terra::plot(smp_r, main = paste("SPEI-", scale, basename(out_files[length(out_files) %/% 2])))
  grDevices::dev.off()
  png_size <- file.info(png_path)$size
  if (!is.na(png_size) && png_size > 1024) {
    cat(sprintf("[OK] 6. PNG round-trip: %s (%d KB).\n", png_path, round(png_size / 1024)))
  } else {
    cat(sprintf("[FAIL] 6. PNG round-trip too small: %s.\n", png_path))
    pass <- FALSE
  }

  cat(
    "\n=== WARNINGS COLLECTED:",
    length(warnings_collected$entries), "===\n"
  )
  if (length(warnings_collected$entries) > 0L) {
    tab <- sort(table(sub(":.*$", "", warnings_collected$entries)), decreasing = TRUE)
    cat("Top warning labels:\n")
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

# 8) --full summary ####

log_step(sprintf(
  "Full bake complete. %d COGs written across %d scales.",
  length(written_paths), length(scales_run)
))
cat("\nWarnings collected:", length(warnings_collected$entries), "\n")
if (length(warnings_collected$entries) > 0L) {
  tab <- sort(table(sub(":.*$", "", warnings_collected$entries)), decreasing = TRUE)
  for (nm in names(tab)) cat(sprintf("  %5d  %s\n", tab[[nm]], nm))
}
