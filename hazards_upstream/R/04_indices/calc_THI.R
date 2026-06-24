# Compute heat stress livestock (cattle) (THI)
# By: H. Achicanoy
# Alliance Bioversity International & CIAT, 2025

# R options
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
  source(normalizePath(hit), local = FALSE)
})
suppressMessages(library(pacman))
suppressMessages(pacman::p_load(tidyverse,terra,gtools,lubridate))

root <- common_data_root()

# Extent CHIRPS
msk <- terra::rast(file.path(root,'chirps_wrld/chirps-v2.0.1981.01.01.tif'))
xtd <- terra::ext(msk)

# THI function
calc_thi <- function(yr, mn){
  
  outfile1 <- paste0(out_dir,'/THI_mean-',yr,'-',mn,'.tif')
  outfile2 <- paste0(out_dir,'/THI_max-',yr,'-',mn,'.tif')
  outfile3 <- paste0(out_dir,'/daily/THI_daily-',yr,'-',mn,'.tif')
  
  file.exists(c(outfile1,outfile2,outfile3))
  
  cat(outfile2,'\n')
  
  if(!should_skip(c(outfile1,outfile2,outfile3))){
    
    dir.create(dirname(outfile3),F,T)
    
    # Sequence of dates
    last_day <- lubridate::days_in_month(as.Date(paste0(yr,'-',mn,'-01'))) # Last day of the month
    dts <- seq(from = as.Date(paste0(yr,'-',mn,'-01')), to = as.Date(paste0(yr,'-',mn,'-',last_day)), by = 'day')
    
    # Files
    tx_fls <- paste0(tx_pth,'/tasmax_',dts,'.tif')
    tx_fls <- tx_fls[file.exists(tx_fls)]
    stopifnot(length(tx_fls) > 0)
    rh_fls <- paste0(rh_pth,'/hurs_',dts,'.tif')
    rh_fls <- rh_fls[file.exists(rh_fls)]
    stopifnot(length(rh_fls) > 0)
    
    # Read daily maximum temperature and relative humidity data
    tmx <- terra::rast(tx_fls) |> terra::crop(xtd)
    rhm <- terra::rast(rh_fls) |> terra::crop(xtd)
    
    thr_hum_idx <- function(tmax, rhum){
      thi = (1.8 * tmax + 32) - ((0.55 - 0.0055 * rhum) * (1.8 * tmax - 26.8))
      return(thi)
    }
    cthr_hum_idx <- compiler::cmpfun(thr_hum_idx)
    # Calculate human heat stress
    THI <- cthr_hum_idx(tmax = tmx, rhum = rhm)
    THI_avg <- mean(THI)
    THI_max <- max(THI)
    
    # Write output
    terra::writeRaster(THI_avg, outfile1, overwrite = T)
    terra::writeRaster(THI_max, outfile2, overwrite = T)
    terra::writeRaster(THI, outfile3, overwrite = T)
    
    # Clean-up
    rm(tmx,rhm,THI,THI_avg,THI_max); gc(F, T, T)
  }
}

# Runs
# Run config - env-overridable via 00_setup.R (SCENARIO / SSPS / YRS / GCMS).
# Historical window kept at the legacy 1981:1994 default (documented baseline is
# 1995:2014 - run the baseline pass with YRS=1995:2014).
scenario <- cfg_scenario("historical")
ssps     <- cfg_ssps(scenario)
yrs      <- cfg_yrs(scenario, historical = 1981:1994)
gcms     <- cfg_gcms()
.log('Run config: scenario=', scenario, ' | yrs=', min(yrs), ':', max(yrs), ' | n_gcms=', length(gcms))

for (gcm in gcms) {
  
  for (ssp in ssps) {
    
    ## Parameters
    cmb <- paste0(ssp, '_', gcm)
    mnt <- sprintf('%02.0f',1:12)
    stp <- base::expand.grid(yrs, mnt, stringsAsFactors = F) |> setNames(c('yrs', 'mnt')) |> dplyr::arrange(yrs, mnt) |> base::as.data.frame(); rm(mnt)
    
    ## Setup in/out files
    tx_pth <- paste0(root, '/nex-gddp-cmip6/tasmax/', ssp, '/', gcm) # Daily maximum temperatures
    rh_pth <- paste0(root, '/nex-gddp-cmip6/hurs/', ssp, '/', gcm) # Daily relative humidity
    out_dir <- paste0(root,'/nex-gddp-cmip6_indices/',ssp,'_',gcm,'/THI')
    
    1:nrow(stp) |> purrr::map(.f = function(i){calc_thi(yr = stp$yrs[i], mn = stp$mnt[i])}); gc(F, T, T)
    tmpfls <- list.files(tempdir(), full.names = T)
    1:length(tmpfls) |> purrr::map(.f = function(k) {system(paste0('rm -f ', tmpfls[k]))})
    cat('----Finish----\n')
    
  }
  
}
