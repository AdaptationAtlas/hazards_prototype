# 0) Introduction ####
# Download monthly observational climate data from UCSB CHC and bake it into
# Cloud-Optimized GeoTIFFs aligned to a CHIRPS-native observational base raster.
#
# Sources:
#   - CHIRPS v3 monthly precipitation (Africa)     -> PTOT
#   - CHIRTS-ERA5 monthly Tmax (global, Africa cut) -> TMAX
#   - CHIRTS-ERA5 monthly Tmin (global, Africa cut) -> TMIN
#   - Derived (TMAX + TMIN) / 2                    -> TAVG
#
# Inputs:
#   - HTTPS directory listings at data.chc.ucsb.edu (scraped with rvest).
#   - metadata/base_raster_obs.tif (built on first run from the first CHIRPS
#     v3 monthly tile; cached on disk thereafter).
#
# Outputs (under Data/chirts_chirps_hist/):
#   - manifest.csv             - per-file download/processing log
#   - PTOT/PTOT-YYYY-MM.tif    - one COG per month (likewise for TMAX/TMIN/TAVG)
#   - {VAR}/_metadata.json     - variable-level JSON sidecar
#
# Run modes (via commandArgs):
#   --smoke   PTOT only, 2023-01..2023-12; runs six inline verification checks
#             at the end and exits 0/1.
#   --full    All variables, all months in the configured ranges. Use the
#             Afrilabs server - this is heavier than what laptops handle well.
#   (none)    Print usage and exit 1.
#
# Dependencies: requires R/0_server_setup.R to have been sourced (project_dir,
# working_dir, chirts_chirps_hist_dir, base_rast, set_parallel_plan,
# check_tif_integrity, terra::gdalCache).
#
# Please run 0_server_setup.R before executing this script.

# 1) Setup ####

#' Stream a progress line that flushes immediately (so background runs report
#' progress as they happen instead of buffering everything to the end).
log_step <- function(msg) {
  cat(format(Sys.time(), "[%H:%M:%S] "), msg, "\n", sep = "")
  flush.console()
}

#' Resolve project_dir -> working_dir using the same per-environment mapping
#' that 0_server_setup.R uses, without firing off the upstream pipeline
#' downloads (mapspam, FAOSTAT, etc.) that 0_server_setup.R also performs.
#' Used by --smoke so it can run in seconds. --full still sources the full
#' setup script.
bootstrap_minimal <- function() {
  log_step("bootstrap_minimal: resolving project / working dirs")
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  library(pacman)
  pacman::p_load(
    terra, data.table, glue, httr2, rvest, jsonlite, digest, fs,
    future, future.apply, furrr, progressr
  )

  project_dir <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else getwd()
  working_dir <- switch(project_dir,
    "/home/jovyan/atlas/hazards_prototype" = "/home/jovyan/common_data/hazards_prototype",
    "D:/rprojects/hazards_prototype" = "D:/common_data/hazards_prototype",
    "C:/rprojects/hazards_prototype" = "C:/rprojects/common_data/hazards_prototype",
    "/Users/pstewarda/Documents/rprojects/hazards_prototype" =
      "/Users/pstewarda/Documents/rprojects/common_data/hazards_prototype",
    "/home/psteward/rprojects/hazards_prototype" = "/cluster01/workspace/atlas/hazards_prototype",
    stop(glue::glue("Unknown project_dir '{project_dir}'. Add a mapping to bootstrap_minimal()."))
  )
  if (!dir.exists(working_dir)) dir.create(working_dir, recursive = TRUE)
  setwd(working_dir)

  chirts_chirps_hist_dir <- file.path("Data", "chirts_chirps_hist")
  if (!dir.exists(chirts_chirps_hist_dir)) dir.create(chirts_chirps_hist_dir, recursive = TRUE)

  # Only source the helpers we actually need (set_parallel_plan, check_tif_integrity).
  source(file.path(project_dir, "R", "haz_functions.R"))

  terra::gdalCache(60000)
  options(timeout = 600)

  list(
    project_dir            = project_dir,
    working_dir            = working_dir,
    chirts_chirps_hist_dir = chirts_chirps_hist_dir
  )
}

# Parse run mode early so we can choose the cheap or full bootstrap path.
args <- commandArgs(trailingOnly = TRUE)
mode <- if (length(args) == 0) "" else args[1]

if (mode == "--smoke") {
  paths <- bootstrap_minimal()
  project_dir <- paths$project_dir
  chirts_chirps_hist_dir <- paths$chirts_chirps_hist_dir
} else {
  source("R/0_server_setup.R")
  pacman::p_load(
    terra, data.table, glue, httr2, rvest, jsonlite, digest, fs,
    future, future.apply, furrr, progressr
  )
  chirts_chirps_hist_dir <- atlas_dirs$data_dir$chirts_chirps_hist
  if (!dir.exists(chirts_chirps_hist_dir)) dir.create(chirts_chirps_hist_dir, recursive = TRUE)
}

# 2) Sources ####
sources <- list(
  PTOT = list(
    base_url = "https://data.chc.ucsb.edu/products/CHIRPS/v3.0/monthly/africa/tifs/",
    filename_re = "^chirps-v3\\.0\\.(\\d{4})\\.(\\d{2})\\.tif$",
    version = "CHIRPS v3.0",
    unit = "mm/month",
    description = "Monthly precipitation total (CHIRPS v3 station-merged, Africa).",
    sentinel_mask = function(r) {
      r[r < 0] <- NA
      r
    }
  ),
  TMAX = list(
    base_url = "https://data.chc.ucsb.edu/experimental/CHIRTS-ERA5/tmax/tifs/monthly/",
    filename_re = "^CHIRTS-ERA5\\.monthly_Tmax\\.(\\d{4})\\.(\\d{2})\\.tif$",
    version = "CHIRTS-ERA5 (experimental)",
    unit = "degC",
    description = "Monthly maximum temperature (CHIRTS-ERA5, native global grid).",
    sentinel_mask = function(r) terra::classify(r, cbind(-Inf, -100, NA), right = FALSE)
  ),
  TMIN = list(
    base_url = "https://data.chc.ucsb.edu/experimental/CHIRTS-ERA5/tmin/tifs/monthly/",
    filename_re = "^CHIRTS-ERA5\\.monthly_Tmin\\.(\\d{4})\\.(\\d{2})\\.tif$",
    version = "CHIRTS-ERA5 (experimental)",
    unit = "degC",
    description = "Monthly minimum temperature (CHIRTS-ERA5, native global grid).",
    sentinel_mask = function(r) terra::classify(r, cbind(-Inf, -100, NA), right = FALSE)
  )
)

# COG write options - dispatch-specified encoding.
cog_gdal_opts <- c(
  "COMPRESS=DEFLATE",
  "PREDICTOR=2",
  "OVERVIEWS=NONE",
  "BLOCKSIZE=512"
)

# Observational base raster path (committed alongside the existing base_raster.tif).
obs_base_rast_path <- file.path(project_dir, "metadata", "base_raster_obs.tif")
manifest_path <- file.path(chirts_chirps_hist_dir, "manifest.csv")
manifest_cols <- c(
  "variable", "year", "month", "source_url", "source_size_bytes",
  "local_path", "downloaded_at", "sha256"
)

# OS-aware parallel backend: Linux uses fork-based multicore; Mac/Windows use multisession.
on_linux_server <- .Platform$OS.type == "unix" && !grepl("darwin", R.version$os, ignore.case = TRUE)

# 3) Helpers ####

#' Scrape the CHC directory listing and return a data.table with the parsed
#' year and month columns for one variable.
list_remote_tifs <- function(var) {
  cfg <- sources[[var]]
  hrefs <- rvest::read_html(cfg$base_url) |>
    rvest::html_nodes("a") |>
    rvest::html_attr("href")
  tifs <- hrefs[grepl(cfg$filename_re, hrefs)]
  if (length(tifs) == 0L) {
    stop(glue::glue("No TIFs matched filename_re for {var} at {cfg$base_url}"))
  }
  m <- regmatches(tifs, regexec(cfg$filename_re, tifs))
  yy <- as.integer(vapply(m, `[`, character(1), 2L))
  mm <- as.integer(vapply(m, `[`, character(1), 3L))
  data.table::data.table(
    variable   = var,
    year       = yy,
    month      = mm,
    source_url = paste0(cfg$base_url, tifs),
    filename   = tifs
  )[order(year, month)]
}

#' Download a single URL to `dest` with exponential-backoff retries.
download_with_retry <- function(url, dest, max_attempts = 3L) {
  req <- httr2::request(url) |>
    httr2::req_retry(
      max_tries = max_attempts,
      backoff = function(i) c(1, 5, 15)[min(i, 3L)]
    )
  resp <- httr2::req_perform(req, path = dest)
  if (httr2::resp_status(resp) >= 400) {
    stop(glue::glue("Download failed [{httr2::resp_status(resp)}]: {url}"))
  }
  invisible(dest)
}

#' Read the manifest from disk; return an empty manifest with correct columns
#' when the file does not yet exist.
read_manifest <- function() {
  if (file.exists(manifest_path)) {
    return(data.table::fread(manifest_path, colClasses = list(character = manifest_cols)))
  }
  empty <- data.table::data.table(matrix(character(), ncol = length(manifest_cols)))
  data.table::setnames(empty, manifest_cols)
  empty
}

#' Append/replace a single row in the manifest CSV.
append_manifest_row <- function(row) {
  current <- read_manifest()
  current <- current[!(variable == row$variable & year == row$year & month == row$month)]
  current <- rbind(current, row, use.names = TRUE, fill = TRUE)
  data.table::setorderv(current, c("variable", "year", "month"))
  data.table::fwrite(current, manifest_path)
}

#' Build (or load) the observational base raster from a CHIRPS template tile.
get_obs_base_rast <- function(template_path) {
  if (file.exists(obs_base_rast_path)) {
    return(terra::rast(obs_base_rast_path))
  }
  r <- terra::rast(template_path)
  r <- sources$PTOT$sentinel_mask(r)
  r[!is.na(r)] <- 1
  res_deg <- mean(terra::res(r))
  if (abs(res_deg - 0.05) > 1e-4) {
    stop(glue::glue("Unexpected obs base resolution: {res_deg} deg (expected ~0.05)"))
  }
  if (!dir.exists(dirname(obs_base_rast_path))) {
    dir.create(dirname(obs_base_rast_path), recursive = TRUE)
  }
  terra::writeRaster(
    r, obs_base_rast_path,
    overwrite = TRUE,
    filetype = "COG",
    gdal = cog_gdal_opts
  )
  cat("Built observational base raster:", obs_base_rast_path, "\n")
  terra::rast(obs_base_rast_path)
}

#' Process one downloaded raster: sentinel-mask, crop+resample to obs_base_rast,
#' write COG. Returns the output path.
process_raster <- function(raw_path, var, out_path, obs_base_rast) {
  r <- terra::rast(raw_path)
  r <- sources[[var]]$sentinel_mask(r)
  r <- terra::crop(r, obs_base_rast)
  r <- terra::resample(r, obs_base_rast, method = "bilinear")
  if (!dir.exists(dirname(out_path))) {
    dir.create(dirname(out_path), recursive = TRUE)
  }
  terra::writeRaster(
    r, out_path,
    overwrite = TRUE,
    filetype = "COG",
    gdal = cog_gdal_opts
  )
  out_path
}

#' Compute SHA256 hex digest of a file.
sha256_file <- function(path) digest::digest(file = path, algo = "sha256")

#' Process a single (var, year, month) tuple: download, transform, COG-write,
#' record in manifest. Returns the manifest row as a 1-row data.table.
process_one <- function(var, year, month, source_url, obs_base_rast,
                        out_dir, keep_raw = FALSE, raw_dir = NULL) {
  out_name <- sprintf("%s-%04d-%02d.tif", var, year, month)
  out_path <- file.path(out_dir, out_name)
  raw_path <- if (keep_raw) {
    file.path(raw_dir, basename(source_url))
  } else {
    tempfile(fileext = ".tif")
  }

  download_with_retry(source_url, raw_path)
  src_size <- file.info(raw_path)$size

  process_raster(raw_path, var, out_path, obs_base_rast)

  if (!keep_raw) unlink(raw_path)

  data.table::data.table(
    variable          = var,
    year              = sprintf("%04d", year),
    month             = sprintf("%02d", month),
    source_url        = source_url,
    source_size_bytes = as.character(src_size),
    local_path        = out_path,
    downloaded_at     = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    sha256            = sha256_file(out_path)
  )
}

#' Write the variable-level JSON metadata sidecar.
write_var_metadata <- function(var, rows, out_dir) {
  cfg <- sources[[var]]
  year_min <- min(as.integer(rows$year))
  year_max <- max(as.integer(rows$year))
  jsonlite::write_json(
    list(
      variable        = var,
      source          = cfg$base_url,
      version         = cfg$version,
      description     = cfg$description,
      unit            = cfg$unit,
      n_files         = nrow(rows),
      year_range      = paste(year_min, year_max, sep = "-"),
      format          = "Cloud-Optimized GeoTIFF (COG, DEFLATE PREDICTOR=2, BLOCKSIZE=512)",
      base_raster     = obs_base_rast_path,
      parent_script   = "R/0.6_download_chirps_chirts.R",
      date_created    = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
      notes           = "Cropped + resampled (bilinear) to obs_base_rast; sentinel values masked to NA."
    ),
    path = file.path(out_dir, "_metadata.json"),
    pretty = TRUE,
    auto_unbox = TRUE
  )
}

#' Compute and write TAVG-{YYYY}-{MM}.tif from matching TMAX + TMIN COGs.
derive_tavg <- function(year, month, obs_base_rast, tavg_dir) {
  tmax_path <- file.path(chirts_chirps_hist_dir, "TMAX", sprintf("TMAX-%04d-%02d.tif", year, month))
  tmin_path <- file.path(chirts_chirps_hist_dir, "TMIN", sprintf("TMIN-%04d-%02d.tif", year, month))
  if (!file.exists(tmax_path) || !file.exists(tmin_path)) {
    return(NULL)
  }
  out_path <- file.path(tavg_dir, sprintf("TAVG-%04d-%02d.tif", year, month))
  r <- (terra::rast(tmax_path) + terra::rast(tmin_path)) / 2
  terra::writeRaster(r, out_path, overwrite = TRUE, filetype = "COG", gdal = cog_gdal_opts)
  data.table::data.table(
    variable          = "TAVG",
    year              = sprintf("%04d", year),
    month             = sprintf("%02d", month),
    source_url        = "derived from TMAX + TMIN",
    source_size_bytes = NA_character_,
    local_path        = out_path,
    downloaded_at     = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    sha256            = sha256_file(out_path)
  )
}

# 4) Run-mode handling ####

usage <- function() {
  cat(
    "Usage:\n",
    "  Rscript R/0.6_download_chirps_chirts.R --smoke\n",
    "      PTOT only, 2023-01..2023-12, runs verification checks, exits.\n",
    "  Rscript R/0.6_download_chirps_chirts.R --full\n",
    "      All variables, all months in the configured ranges.\n",
    sep = ""
  )
}

if (!mode %in% c("--smoke", "--full")) {
  usage()
  quit(status = 1)
}

cat("project_dir          :", project_dir, "\n")
cat("working_dir          :", getwd(), "\n")
cat("output dir           :", chirts_chirps_hist_dir, "\n")
cat("obs_base_rast_path   :", obs_base_rast_path, "\n")
cat("mode                 :", mode, "\n\n")

# 5) Smoke mode ####
if (mode == "--smoke") {
  log_step("=== SMOKE TEST: PTOT 2023-01..2023-12 ===")

  # 5.1) URL pattern resolution check for ALL three vars (CHIRPS, TMAX, TMIN) -
  # the dispatch wants this on every run, not just --full.
  log_step("Step 5.1: scraping remote directory listings for PTOT/TMAX/TMIN")
  url_counts <- vapply(names(sources), function(v) {
    n <- nrow(list_remote_tifs(v))
    log_step(sprintf("  %s: %d TIFs available", v, n))
    n
  }, integer(1))
  if (any(url_counts == 0)) {
    stop("URL pattern resolution failed - one or more variables returned zero TIFs.")
  }

  # 5.2) Pick the 12 PTOT months we need.
  ptot_remote <- list_remote_tifs("PTOT")
  smoke_target <- ptot_remote[year == 2023L]
  if (nrow(smoke_target) != 12L) {
    stop(glue::glue("Expected 12 PTOT files for 2023; found {nrow(smoke_target)}."))
  }
  log_step(sprintf("Step 5.2: selected %d files for smoke download", nrow(smoke_target)))

  # 5.3) Build obs_base_rast (download the first file as a template).
  ptot_dir <- file.path(chirts_chirps_hist_dir, "PTOT")
  if (!dir.exists(ptot_dir)) dir.create(ptot_dir, recursive = TRUE)

  log_step("Step 5.3: downloading template tile for obs_base_rast")
  template_raw <- tempfile(fileext = ".tif")
  download_with_retry(smoke_target$source_url[1], template_raw)
  obs_base_rast <- get_obs_base_rast(template_raw)
  unlink(template_raw)
  log_step(sprintf(
    "  obs_base_rast extent=[%s] res=[%s]",
    paste(round(as.vector(terra::ext(obs_base_rast)), 3), collapse = ", "),
    paste(round(terra::res(obs_base_rast), 4), collapse = ", ")
  ))

  # 5.4) Download + process the 12 months sequentially so progress is visible
  # in the log as each month completes. Skip months already present + valid.
  log_step("Step 5.4: downloading + processing 12 PTOT months (sequential, visible progress)")
  manifest_now <- read_manifest()
  rows <- vector("list", nrow(smoke_target))
  for (i in seq_len(nrow(smoke_target))) {
    t0 <- Sys.time()
    out_name <- sprintf("PTOT-%04d-%02d.tif", smoke_target$year[i], smoke_target$month[i])
    out_path <- file.path(ptot_dir, out_name)
    have_row <- manifest_now[
      variable == "PTOT" &
        year == sprintf("%04d", smoke_target$year[i]) &
        month == sprintf("%02d", smoke_target$month[i])
    ]
    if (file.exists(out_path) && nrow(have_row) == 1L &&
      !inherits(try(terra::rast(out_path) + 0, silent = TRUE), "try-error")) {
      rows[[i]] <- have_row
      log_step(sprintf("  [%d/%d] %s skipped (already present + valid)", i, nrow(smoke_target), out_name))
    } else {
      rows[[i]] <- process_one(
        var           = "PTOT",
        year          = smoke_target$year[i],
        month         = smoke_target$month[i],
        source_url    = smoke_target$source_url[i],
        obs_base_rast = terra::rast(obs_base_rast_path),
        out_dir       = ptot_dir
      )
      append_manifest_row(rows[[i]])
      log_step(sprintf(
        "  [%d/%d] %s in %.1fs",
        i, nrow(smoke_target), out_name, as.numeric(Sys.time() - t0, units = "secs")
      ))
    }
  }
  rows <- data.table::rbindlist(rows, use.names = TRUE, fill = TRUE)

  write_var_metadata("PTOT", rows, ptot_dir)
  log_step("Step 5.4 complete: all 12 PTOT COGs written + manifest updated per-file")

  # 5.5) Verification checks.
  cat("\n=== VERIFICATION CHECKS ===\n")
  pass <- TRUE

  # Check 1: URL pattern resolution (already done above; restate for the log).
  cat("[OK] 1. URL pattern resolution: all 3 source listings return >0 TIFs.\n")

  # Check 2: Manifest round-trip.
  m_back <- read_manifest()
  if (nrow(m_back[variable == "PTOT" & year == "2023"]) != 12L) {
    cat(
      "[FAIL] 2. Manifest round-trip: expected 12 PTOT 2023 rows, got",
      nrow(m_back[variable == "PTOT" & year == "2023"]), "\n"
    )
    pass <- FALSE
  } else {
    cat("[OK] 2. Manifest round-trip: 12 PTOT 2023 rows persisted and read back.\n")
  }

  # Check 3: COG integrity for the 12 smoke outputs. Walk each manifest row
  # path directly - earlier the glob+dir scan masked a zero-files vacuous pass.
  integ <- vapply(rows$local_path, function(f) {
    if (!file.exists(f)) {
      return(FALSE)
    }
    tryCatch(
      {
        terra::rast(f) + 0
        TRUE
      },
      error = function(e) FALSE
    )
  }, logical(1))
  if (length(integ) != 12L || any(!integ)) {
    cat("[FAIL] 3. COG integrity: ", sum(!integ), " of ", length(integ),
      " smoke COGs failed (expected 12).\n",
      sep = ""
    )
    pass <- FALSE
  } else {
    cat("[OK] 3. COG integrity: all 12 smoke COGs parse cleanly.\n")
  }

  # Check 4: Sentinel masking - PTOT must have no negative values.
  smoke_r <- terra::rast(rows$local_path[1])
  smoke_min <- min(terra::values(smoke_r), na.rm = TRUE)
  if (smoke_min < 0) {
    cat("[FAIL] 4. Sentinel masking: PTOT minimum = ", smoke_min, " < 0.\n", sep = "")
    pass <- FALSE
  } else {
    cat("[OK] 4. Sentinel masking: PTOT minimum = ", round(smoke_min, 3), " >= 0.\n", sep = "")
  }

  # Check 5: Grid alignment (extent + resolution).
  ext_match <- all(as.vector(terra::ext(smoke_r)) == as.vector(terra::ext(obs_base_rast)))
  res_match <- all(terra::res(smoke_r) == terra::res(obs_base_rast))
  if (!ext_match || !res_match) {
    cat("[FAIL] 5. Grid alignment: extent_match=", ext_match, " res_match=", res_match, "\n", sep = "")
    pass <- FALSE
  } else {
    cat("[OK] 5. Grid alignment: extent and resolution match obs_base_rast.\n")
  }

  # Check 6: Round-trip PNG plot.
  png_path <- file.path(chirts_chirps_hist_dir, "_smoke_test.png")
  grDevices::png(png_path, width = 1200, height = 900, res = 120)
  terra::plot(smoke_r, main = paste("PTOT", basename(rows$local_path[1])))
  grDevices::dev.off()
  png_size <- file.info(png_path)$size
  if (is.na(png_size) || png_size <= 1024) {
    cat("[FAIL] 6. PNG round-trip: ", png_path, " size = ", png_size, " bytes.\n", sep = "")
    pass <- FALSE
  } else {
    cat("[OK] 6. PNG round-trip: ", png_path, " (", round(png_size / 1024), " KB).\n", sep = "")
  }

  # 5.6) Print smoke summary.
  cat("\n=== SMOKE OUTPUTS ===\n")
  cat("COGs (12):\n")
  cat(paste0("  ", rows$local_path, collapse = "\n"), "\n")
  cat("Validation PNG :", png_path, "\n")
  cat("Manifest head  :\n")
  print(read_manifest()[, .(variable, year, month, source_size_bytes, local_path)][1:12])
  cat("\nJSON sidecar contents:\n")
  cat(readLines(file.path(ptot_dir, "_metadata.json")), sep = "\n")
  cat("\n\n")

  if (!pass) {
    cat("=== SMOKE TEST FAILED ===\n")
    quit(status = 1)
  }
  cat("=== SMOKE TEST PASSED - STOPPING (do NOT run --full from here) ===\n")
  quit(status = 0)
}

# 6) Full mode ####
if (mode == "--full") {
  cat("=== FULL BAKE ===\n\n")

  # 6.1) Confirm all three remote listings, build obs_base_rast if missing.
  listings <- lapply(names(sources), list_remote_tifs)
  names(listings) <- names(sources)
  for (v in names(listings)) {
    cat(glue::glue(
      "Remote listing {v}: {nrow(listings[[v]])} files; ",
      "{min(listings[[v]]$year)}-{min(listings[[v]]$month)} to ",
      "{max(listings[[v]]$year)}-{max(listings[[v]]$month)}"
    ), "\n")
  }

  if (!file.exists(obs_base_rast_path)) {
    template_raw <- tempfile(fileext = ".tif")
    download_with_retry(listings$PTOT$source_url[1], template_raw)
    get_obs_base_rast(template_raw)
    unlink(template_raw)
  }

  # 6.2) Determine what to download (skip rows already in manifest with valid COG).
  existing <- read_manifest()
  to_do <- data.table::rbindlist(lapply(names(listings), function(v) {
    dt <- copy(listings[[v]])
    dt[, c("year_c", "month_c") := list(sprintf("%04d", year), sprintf("%02d", month))]
    dt[, out_path := file.path(chirts_chirps_hist_dir, v, sprintf("%s-%s-%s.tif", v, year_c, month_c))]
    done <- existing[variable == v, paste(year, month, sep = "-")]
    dt[, key := paste(year_c, month_c, sep = "-")]
    dt[!(key %in% done) | !file.exists(out_path)]
  }))
  cat("Downloads pending:", nrow(to_do), "\n\n")

  # 6.3) Download / process loop (one parallel pass).
  source(file.path(project_dir, "R", "observational", "_helpers.R"))
  # Downloads are I/O-bound; per-worker RAM is small (~0.5 GB raster work area).
  per_worker_gb_dl <- 0.5
  n_dl <- resolve_workers(args, per_worker_gb = per_worker_gb_dl, max_workers = 16L)
  print_resource_banner(n_dl, per_worker_gb_dl, label = "download")
  set_parallel_plan(n_cores = n_dl, use_multisession = !on_linux_server)
  progressr::handlers("progress")

  rows_full <- progressr::with_progress({
    p <- progressr::progressor(steps = nrow(to_do))
    furrr::future_map(seq_len(nrow(to_do)), function(i) {
      v <- to_do$variable[i]
      y <- to_do$year[i]
      m <- to_do$month[i]
      p(sprintf("%s %s-%s", v, sprintf("%04d", y), sprintf("%02d", m)))
      tryCatch(
        {
          out_dir <- file.path(chirts_chirps_hist_dir, v)
          process_one(
            var = v, year = y, month = m,
            source_url = to_do$source_url[i],
            obs_base_rast = terra::rast(obs_base_rast_path),
            out_dir = out_dir
          )
        },
        error = function(e) {
          message(glue::glue("FAILED {v} {y}-{m}: {e$message}"))
          NULL
        }
      )
    }, .options = furrr::furrr_options(seed = TRUE))
  })
  future::plan(future::sequential)

  rows_full <- data.table::rbindlist(Filter(Negate(is.null), rows_full))
  for (i in seq_len(nrow(rows_full))) append_manifest_row(rows_full[i])

  # 6.4) Per-variable JSON sidecars.
  manifest_now <- read_manifest()
  for (v in c("PTOT", "TMAX", "TMIN")) {
    vrows <- manifest_now[variable == v]
    if (nrow(vrows) > 0L) {
      write_var_metadata(v, vrows, file.path(chirts_chirps_hist_dir, v))
    }
  }

  # 6.5) TAVG derivation (only for months where BOTH TMAX and TMIN exist).
  tmax_keys <- manifest_now[variable == "TMAX", paste(year, month, sep = "-")]
  tmin_keys <- manifest_now[variable == "TMIN", paste(year, month, sep = "-")]
  both <- intersect(tmax_keys, tmin_keys)
  tavg_keys <- manifest_now[variable == "TAVG", paste(year, month, sep = "-")]
  todo_tavg <- setdiff(both, tavg_keys)

  if (length(todo_tavg) > 0L) {
    tavg_dir <- file.path(chirts_chirps_hist_dir, "TAVG")
    if (!dir.exists(tavg_dir)) dir.create(tavg_dir, recursive = TRUE)

    set_parallel_plan(n_cores = if (on_linux_server) 16L else 10L, use_multisession = !on_linux_server)
    progressr::handlers("progress")
    tavg_rows <- progressr::with_progress({
      p <- progressr::progressor(steps = length(todo_tavg))
      furrr::future_map(todo_tavg, function(ym) {
        parts <- strsplit(ym, "-", fixed = TRUE)[[1]]
        y <- as.integer(parts[1])
        m <- as.integer(parts[2])
        p(sprintf("TAVG %s", ym))
        derive_tavg(y, m, terra::rast(obs_base_rast_path), tavg_dir)
      }, .options = furrr::furrr_options(seed = TRUE))
    })
    future::plan(future::sequential)
    tavg_rows <- data.table::rbindlist(Filter(Negate(is.null), tavg_rows))
    for (i in seq_len(nrow(tavg_rows))) append_manifest_row(tavg_rows[i])
    write_var_metadata("TAVG", read_manifest()[variable == "TAVG"], tavg_dir)
  }

  cat("\nFull bake complete. Manifest rows by variable:\n")
  print(read_manifest()[, .N, by = variable][order(variable)])
}
