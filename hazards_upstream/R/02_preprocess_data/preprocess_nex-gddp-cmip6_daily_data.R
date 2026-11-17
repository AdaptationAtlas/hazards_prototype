# Nex-GDDP-CMIP6 to Atlas data structure
# By: H. Achicanoy
# Alliance Bioversity-International & CIAT, 2025

# R options
# Shared Stage-0 setup: data root, timestamped .log(), env run-controls (warn=-1 dropped).
local({
  cargs <- commandArgs(FALSE)
  fa <- grep("^--file=", cargs, value = TRUE)
  base <- if (length(fa)) dirname(normalizePath(sub("^--file=", "", fa[1]))) else getwd()
  cand <- c(file.path(base, "..", "00_setup.R"), file.path(base, "00_setup.R"),
            "../00_setup.R", "00_setup.R")
  hit <- cand[file.exists(cand)][1]
  if (is.na(hit)) stop("00_setup.R not found from ", base)
  source(normalizePath(hit), local = FALSE)
})
if (!require(pacman)) {install.packages('pacman'); library(pacman)} else {library(pacman)}
pacman::p_load(tidyverse, terra)   # xts/tsbox were loaded and never used; furrr/future dropped (see below)

grep2 <- Vectorize(grep, 'pattern')

# Root directory
root <- common_data_root()

## Conversion factors
# Precipitation: pr * 86400
# Temperatures: tasmax, tasmin - 273.15
# Solar radiation: rsds * 86400 / 1000000
# Rotate

# Get daily transformed data
get_daily_data <- function (vr, ssp, gcm) {
  
  # Input directory
  indir <- file.path(root,'nex-gddp-cmip6_raw',vr,ssp,gcm)
  # Output directory
  outdir <- file.path(root,'nex-gddp-cmip6',vr,ssp,gcm)
  dir.create(path = outdir, F, recursive = T)
  
  # Files in input directory
  fls <- list.files(path = indir, pattern = '.nc$', full.names = T)
  
  # One process, plain lapply. The v2_0 variant found furrr::future_map writing 0 tifs on cglabs
  # (worker lost the enclosing gcm/vr/outdir, error swallowed, exit 0). The download dominates wall
  # time, so nothing meaningful is lost; what is gained is that a failure here is an error.
  lapply(fls, function(fl) {
    t0 <- Sys.time()
    # Read annual raster
    r <- terra::rast(fl)
    # Daily dates, DATE ONLY (#58 P10). terra::time() returns a Date on some GDAL builds and a POSIXct
    # with a time of day on others (PASCAL, GDAL 3.4.3: "1995-01-01 12:00:00"). The old
    # as.character(time(r)) then produced names with a colon, which GDAL cannot create, and which
    # 04 (names built from seq(as.Date(), by='day')) could never find anyway.
    all_dts <- format(terra::time(r), '%Y-%m-%d')
    if (gcm == 'KACE-1-0-G') {
      dts <- all_dts[!duplicated(all_dts)]                     # Remove duplicated dates
      yr  <- unique(lubridate::year(dts))
      dts <- dts[!dts %in% paste0(yr, '-02-29')]                # Remove Feb 29 if leap year
      r <- r[[match(x = dts, table = all_dts)]]
    } else {
      dts <- all_dts
    }
    
    # Create output filenames
    out_files <- file.path(outdir, paste0(vr,'_',dts,'.tif'))
    
    # Check which files need to be processed
    to_process <- !file.exists(out_files)
    
    if (any(to_process)) {
      # Wrap raster for parallel processing
      r_wrapped <- terra::wrap(r)
      
      # Apply unit transformations
      r_unwrapped <- terra::unwrap(r_wrapped)
      if (vr == 'pr') {
        r_unwrapped <- r_unwrapped * 86400
      } else if (vr %in% c('tasmax','tasmin')) {
        r_unwrapped <- r_unwrapped - 273.15
      } else if (vr == 'rsds') {
        r_unwrapped <- r_unwrapped * 86400 / 1000000
      }
      
      # Rotate rasters
      r_unwrapped <- terra::rotate(r_unwrapped)
      
      # Write only the files that don't exist
      terra::writeRaster(x = r_unwrapped[[to_process]], filename = out_files[to_process], overwrite = T)
    }
    .log(sprintf('%s: %d day(s), %d written, %d present, %.0f s', basename(fl), length(dts), sum(to_process),
                 sum(!to_process), as.numeric(difftime(Sys.time(), t0, units = 'secs'))))
    # The gate: every expected daily tif exists after this file. A silent partial write is a stop.
    missing <- out_files[!file.exists(out_files)]
    if (length(missing)) stop(sprintf('%s: %d expected daily tif(s) not written (first: %s)',
                                      basename(fl), length(missing), basename(missing[1])), call. = FALSE)
    invisible(NULL)
  })
  gc(F, T, T)
  
  return(cat('Done.\n'))
  
}

# Run config, env-overridable via 00_setup.R (issue #58 phase C): SCENARIO / SSPS / VARS / GCMS.
# Defaults reproduce the legacy literals (historical, 6 vars, 18 GCMs).
scenario <- cfg_scenario('historical')
ssps <- cfg_ssps(scenario)
vrs  <- cfg_vars()
gcms <- cfg_gcms()
.log('Run config: scenario=', scenario, ' | ssps=', paste(ssps, collapse = ','), ' | vars=', paste(vrs, collapse = ','),
     ' | gcms=', length(gcms))

stp <- base::expand.grid(gcm = gcms, ssp = ssps, vr = vrs, stringsAsFactors = F) |>
  base::as.data.frame(); rm(vrs, ssps, gcms)

1:nrow(stp) |>
  purrr::map(.f = function(j) {
    vr  <- stp$vr[j]; ssp <- stp$ssp[j]; gcm <- stp$gcm[j]
    get_daily_data(vr = vr, ssp = ssp, gcm = gcm)
    cat(vr,ssp,gcm,'ready...\n')
  })
