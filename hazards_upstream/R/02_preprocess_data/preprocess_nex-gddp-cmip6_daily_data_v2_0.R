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
pacman::p_load(tidyverse, terra, furrr, future, xts, tsbox)

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
  indir <- file.path('/home/jovyan/shared-data-premium/nex-gddp-cmip6_raw',vr,ssp,gcm)
  # Output directory
  outdir <- file.path(root,'nex-gddp-cmip6',vr,ssp,gcm)
  dir.create(path = outdir, F, recursive = T)
  
  # Files in input directory
  fls <- list.files(path = indir, pattern = '.nc$', full.names = T)
  
  # Process files. NOTE: furrr::future_map produced 0 tifs on cglabs (worker
  # failed to capture the enclosing gcm/vr/outdir, error swallowed -> [[1]] NULL,
  # exit 0). terra objects also don't serialise across future workers. Plain
  # lapply (lexical scope, same process) is correct; the download dominates wall
  # time, so we don't lose meaningful speed here.
  lapply(fls, function(fl) {

    # Read annual raster
    r <- terra::rast(fl)
    # Get daily dates
    if (gcm == 'KACE-1-0-G') {
      
      dts <- as.character(terra::time(r))
      dts <- dts[-which(duplicated(dts))] # Remove duplicated dates
      yr  <- unique(lubridate::year(dts))
      leap_yr <- grep(pattern = paste0(yr,'-02-29'), x = dts)
      if (length(leap_yr) > 0) {dts <- dts[-leap_yr]} # Remove Feb 29 if leap year
      r <- r[[match(x = dts, table = as.character(time(r)))]]
      
    } else {
      
      dts <- as.character(terra::time(r))
      
    }
    
    # Create output filenames
    out_files <- file.path(outdir, paste0(vr,'_',dts,'.tif'))
    
    # Check which files need to be processed
    to_process <- !file.exists(out_files)
    
    if (any(to_process)) {
      # Apply unit transformations (no wrap/unwrap - in-process, no serialisation).
      # pr: kg m-2 s-1 -> mm/day; tas: K -> degC; rsds: W m-2 -> MJ m-2 day-1.
      # hurs (%) and sfcWind (m/s) pass through unchanged.
      if (vr == 'pr') {
        r <- r * 86400
      } else if (vr %in% c('tasmax','tasmin')) {
        r <- r - 273.15
      } else if (vr == 'rsds') {
        r <- r * 86400 / 1000000
      }

      # Rotate rasters
      r <- terra::rotate(r)

      # Write only the files that don't exist
      terra::writeRaster(x = r[[to_process]], filename = out_files[to_process], overwrite = T)
    }

  })
  gc(F, T, T)
  
  return(cat('Done.\n'))
  
}

scenario <- cfg_scenario("historical")   # SCENARIO env: historical (default) | future

if (scenario == 'future'){
  ssps <- c('ssp126','ssp245','ssp370','ssp585')
} else {
  if (scenario == 'historical') {
    ssps <- 'historical'
  }
}
vrs <- c('pr','sfcWind') # ,'hurs' 'tasmax','tasmin','rsds' -- sfcWind added for FAO-56 PM ET0 (PET); no unit conversion
gcms <- cfg_gcms()   # GCMS env (default = ATLAS_GCMS, the 18); was hardcoded

stp <- base::expand.grid(gcm = gcms, ssp = ssps, vr = vrs, stringsAsFactors = F) |>
  base::as.data.frame(); rm(vrs, ssps, gcms)

1:nrow(stp) |>
  purrr::map(.f = function(j) {
    vr  <- paste0(stp$vr[j],'2'); ssp <- stp$ssp[j]; gcm <- stp$gcm[j]
    get_daily_data(vr = vr, ssp = ssp, gcm = gcm)
    cat(vr,ssp,gcm,'ready...\n')
  })
