# Compute number of soil water stress days (NDWS)
# By: H. Achicanoy & A. Mendez
# Alliance Bioversity International & CIAT, 2025

# R options
# args <- commandArgs(trailingOnly = T)
# Shared Stage-0 setup: data root, timestamped .log(), env run-controls, run config.
# (sets scipen; warnings left at default so they surface - legacy warn=-1 dropped)
local({
  cargs <- commandArgs(FALSE)
  fa <- grep("^--file=", cargs, value = TRUE)
  base <- if (length(fa)) dirname(normalizePath(sub("^--file=", "", fa[1]))) else getwd()
  cand <- c(file.path(base, "..", "00_setup.R"), file.path(base, "00_setup.R"),
            "../00_setup.R", "00_setup.R")
  hit <- cand[file.exists(cand)][1]
  if (is.na(hit)) stop("00_setup.R not found from ", base)
  source(normalizePath(hit), local = FALSE)   # local=FALSE -> defs land in .GlobalEnv
})
suppressMessages(library(pacman))
suppressMessages(pacman::p_load(tidyverse,terra,gtools,lubridate,compiler,raster,ncdf4))

peest2 <- function(srad, tmin, tmean, tmax){
  
  # Convert rasters to matrices for faster processing
  srad_m  <- terra::values(srad)
  tmin_m  <- terra::values(tmin)
  tmean_m <- terra::values(tmean)
  tmax_m  <- terra::values(tmax)
  
  # Constants
  albedo  <- 0.2
  vpd_cte <- 0.7
  a_eslope <- 611.2
  b_eslope <- 17.67
  c_eslope <- 243.5
  
  # Pre-allocate matrices
  n_cells <- ncol(srad_m)
  n_layers <- nrow(srad_m)
  rn <- matrix(0, nrow = n_layers, ncol = n_cells)
  eslope <- matrix(0, nrow = n_layers, ncol = n_cells)
  et_max <- matrix(0, nrow = n_layers, ncol = n_cells)
  
  # Optimized calculations
  rn <- (1-albedo) * srad_m
  rm(srad_m)
  
  temp_denom <- tmean_m + c_eslope
  eslope <- (a_eslope * b_eslope * c_eslope * exp(b_eslope * tmean_m/temp_denom)) / (temp_denom^2)
  rm(tmean_m, temp_denom)
  
  vpd <- vpd_cte * (0.61120 * (exp((17.67*tmax_m)/(tmax_m+243.5)) - 
                                 exp((17.67*tmin_m)/(tmin_m+243.5))))
  rm(tmin_m, tmax_m)
  
  # Constants
  pt_const <- 1.26
  pt_fact  <- 1
  vpd_ref  <- 1
  psycho   <- 62
  rho_w    <- 997
  rlat_ht  <- 2.26E6
  
  # Final calculation
  pt_coef <- 1 + (pt_fact*pt_const-1) * vpd / vpd_ref
  conversion_factor <- 1E6 * 100 / (rlat_ht * rho_w) * 10
  et_max <- pt_coef * rn * eslope/(eslope+psycho) * conversion_factor
  
  # Convert back to raster
  et_max_rast <- tmin
  terra::values(et_max_rast) <- et_max
  
  return(et_max_rast)
  
}

root <- common_data_root()

# Get CHIRPS extent
msk <- terra::rast(file.path(root, 'chirps_wrld', 'chirps-v2.0.1981.01.01.tif'))
xtd <- terra::ext(msk); rm(msk)

# Soil variables
scp <- terra::rast(paste0(root,'/atlas_hazards/soils/sscp_world.tif'))
sst <- terra::rast(paste0(root,'/atlas_hazards/soils/ssat_world.tif'))

# NDWS function
calc_ndws <- function(yr, mn){
  
  outfile <- paste0(out_dir,'/NDWS-',yr,'-',mn,'.tif')

  if (!should_skip(outfile)) {   # honours FORCE_OVERWRITE=1
    
    dir.create(dirname(outfile),F,T)
    
    # Sequence of dates
    last_day <- lubridate::days_in_month(as.Date(paste0(yr,'-',mn,'-01')))
    dts <- seq(from = as.Date(paste0(yr,'-',mn,'-01')), to = as.Date(paste0(yr,'-',mn,'-',last_day)), by = 'day')
    
    .log('NDWS compute: ', yr, '-', mn)
    
    # Files
    pr_fls <- paste0(pr_pth,'/pr_', dts,'.tif')
    pr_fls <- pr_fls[file.exists(pr_fls)]
    tx_fls <- paste0(tx_pth, '/tasmax_', dts, '.tif')
    tx_fls <- tx_fls[file.exists(tx_fls)]
    tm_fls <- paste0(tm_pth, '/tasmin_', dts, '.tif')
    tm_fls <- tm_fls[file.exists(tm_fls)]
    sr_fls <- paste0(sr_pth, '/rsds_', dts, '.tif')
    sr_fls <- sr_fls[file.exists(sr_fls)]
    # Fail loud if any input variable has zero available days (roadmap rank 5)
    stopifnot(length(pr_fls) > 0, length(tx_fls) > 0,
              length(tm_fls) > 0, length(sr_fls) > 0)
    
    # Read variables
    prc <- terra::rast(pr_fls)
    prc <- prc |> terra::crop(xtd)
    tmx <- terra::rast(tx_fls)
    tmx <- tmx |> terra::crop(xtd)
    tmn <- terra::rast(tm_fls)
    tmn <- tmn |> terra::crop(xtd)
    tav <- (tmx + tmn)/2
    srd <- terra::rast(sr_fls)
    srd <- srd |> terra::crop(xtd)
    
    # Maximum evapotranspiration
    ETMAX <<- peest2(srad = srd, tmin = tmn, tmean = tav, tmax = tmx)
    rm(list = c('tmn','tmx','tav','srd'))
    
    scp <- scp |> terra::resample(ETMAX[[1]])
    scp <- scp |> terra::crop(xtd)
    sst <- sst |> terra::resample(ETMAX[[1]])
    sst <- sst |> terra::crop(xtd)
    
    # Compute water balance model
    date <- paste0(yr,'-',mn)
    if(date %in% c('1995-01','2021-01')){
      AVAIL <<- ETMAX[[1]] * 0
      AVAIL[!is.na(AVAIL)] <- 0
    } else {
      avail_fl <- list.files(path = dirname(outfile), pattern = 'AVAIL-')
      avail_fl <- avail_fl[grep(pattern = '\\.tif', avail_fl)]
      avail_fl <- avail_fl[length(avail_fl)]
      AVAIL <<- terra::rast(paste0(dirname(outfile),'/',avail_fl))
      AVAIL <<- AVAIL[[terra::nlyr(AVAIL)]]
    }
    eabyep_calc <- function(soilcp = scp, soilsat = ssat, avail = AVAIL, rain = prc[[1]], evap = ETMAX[[1]]){
      
      avail   <- min(avail, soilcp)
      
      # ERATIO
      percwt <- min(avail/soilcp*100, 100)
      percwt <- max(percwt, 1)
      eratio <- min(percwt/(97-3.868*sqrt(soilcp)), 1)
      
      demand  <- eratio * evap
      result  <- avail + rain - demand
      logging <- result - soilcp
      logging <- max(logging, 0)
      logging <- min(logging, soilsat)
      # runoff  <- result - logging + soilcp
      avail   <- min(soilcp, result)
      avail   <- max(avail, 0)
      # runoff  <- max(runoff, 0)
      
      out     <- list(Availability = c(AVAIL, avail),
                      # Demand       = demand,
                      Eratio       = eratio
                      # Logging      = logging
      )
      return(out)
    }
    ceabyep_calc <- compiler::cmpfun(eabyep_calc)
    
    watbal <- vector('list', terra::nlyr(ETMAX))
    for(j in 1:terra::nlyr(ETMAX)){
      water_balance <- ceabyep_calc(soilcp  = scp,
                                    soilsat = sst,
                                    avail   = AVAIL[[terra::nlyr(AVAIL)]],
                                    rain    = prc[[j]],
                                    evap    = ETMAX[[j]])
      # Update AVAIL with deep copy to avoid memory leaks
      AVAIL <- terra::deepcopy(water_balance$Availability)
      # Store result and clean temporary objects
      watbal[[j]] <- water_balance
      rm(water_balance)
    }
    
    ERATIO <- watbal |> purrr::map('Eratio') |> terra::rast()
    
    # Calculate number of soil water stress days
    cvls <- matrix(data = c(-Inf, 0.5, 1), ncol = 3) # Classification values
    NDWS <- terra::classify(x = ERATIO, rcl = cvls, right = F) |> sum()
    terra::writeRaster(NDWS, outfile, overwrite = TRUE)
    terra::writeRaster(AVAIL, paste0(dirname(outfile),'/AVAIL-',yr,'-',mn,'.tif'), overwrite = TRUE)
    
    ## Clean up
    rm(list = c('prc','ETMAX','AVAIL','watbal','ERATIO','LOGGING','NDWL0'))
    gc(F, T, T)
  }
}

# Runs
# Run config - env-overridable via 00_setup.R (SCENARIO / SSPS / YRS / GCMS).
# Historical window kept at the legacy 1981:1994 default (the documented CMIP6
# baseline is 1995:2014 - run the baseline pass with YRS=1995:2014).
scenario <- cfg_scenario("historical")
ssps     <- cfg_ssps(scenario)
yrs      <- cfg_yrs(scenario, historical = 1981:1994)
gcms     <- cfg_gcms()
.log('Run config: scenario=', scenario, ' | ssps=', paste(ssps, collapse=','),
     ' | yrs=', min(yrs), ':', max(yrs), ' | n_gcms=', length(gcms))

for (gcm in gcms) {
  
  for (ssp in ssps) {
    
    cmb <- paste0(ssp,'_',gcm)
    .log('To process: ', cmb)
    mns <- cfg_months()   # MONTHS env restricts the inner loop (fast GATE runs)
    stp <- base::expand.grid(yrs, mns, stringsAsFactors = F) |> base::as.data.frame(); rm(mns)
    names(stp) <- c('yrs','mns')
    stp <- stp |> dplyr::arrange(yrs, mns) |> base::as.data.frame()
    
    pr_pth <- paste0(root,'/nex-gddp-cmip6/pr/',ssp,'/',gcm)     # Precipitation
    tm_pth <- paste0(root,'/nex-gddp-cmip6/tasmin/',ssp,'/',gcm) # Minimum temperature
    tx_pth <- paste0(root,'/nex-gddp-cmip6/tasmax/',ssp,'/',gcm) # Maximum temperature
    sr_pth <- paste0(root,'/nex-gddp-cmip6/rsds/',ssp,'/',gcm)   # Solar radiation
    out_dir <- paste0(root,'/nex-gddp-cmip6_indices/',cmb,'/NDWS') # Output directory
    
    1:nrow(stp) |>
      purrr::map(.f = function(i){
        calc_ndws(yr = stp$yrs[i], mn = stp$mns[i]); gc(F, T, T)
        if (i%%5 == 0) {
          tmpfls <- list.files(tempdir(), full.names = T)
          1:length(tmpfls) |> purrr::map(.f = function(k) {system(paste0('rm -f ',tmpfls[k]))})
        }
      })
    
  }
  
}

# ssps  <- args[1]
# gcms  <- args[2]
# prds  <- args[3]
#c('ACCESS-ESM1-5','EC-Earth3','INM-CM5-0','MPI-ESM1-2-HR','MRI-ESM2-0')
# Rscript fast_calc_NDWL50.R 'ssp245' 'ACCESS-ESM1-5' 'EC-Earth3'                   screen id = ndwl50
# Rscript fast_calc_NDWL50.R 'ssp245' 'INM-CM5-0' 'MPI-ESM1-2-HR'  'MRI-ESM2-0'     screen id = ndwl50_2