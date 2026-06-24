# Heat stress generic crop and threshold (i.e., NTx40)
# By: H. Achicanoy, F. Castro-Llanos
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

# Get CHIRPS extent
msk <- terra::rast(file.path(root, 'chirps_wrld', 'chirps-v2.0.1981.01.01.tif'))
xtd <- terra::ext(msk); rm(msk)

# NTx function
calc_ntx <- function(yr, mn, thr = 40) {
  
  outfile <- paste0(out_dir,'/NTx',thr,'/NTx',thr,'-',yr,'-',mn,'.tif') 
  thr <- thr[!file.exists(outfile)]
  outfile <- outfile[!file.exists(outfile)]
  
  if (length(outfile) > 0) {
    
    cat('...processing n=', length(outfile), 'files for yr=', yr, '/ mn=', mn, '\n')
    
    # Create directories
    1:length(outfile) |> purrr::map(.f = function(j){dir.create(dirname(outfile[j]),F,T)})
    
    # Sequence of dates
    last_day <- lubridate::days_in_month(as.Date(paste0(yr,'-',mn,'-01')))
    dts <- seq(from = as.Date(paste0(yr,'-',mn,'-01')), to = as.Date(paste0(yr,'-',mn,'-',last_day)), by = 'day')
    
    # Files
    fls <- paste0(tx_pth,'/tasmax','_',dts,'.tif')
    fls <- fls[file.exists(fls)]
    stopifnot(length(fls) > 0)

    # Read daily maximum temperature data
    tmx <- terra::rast(fls)
    tmx <- terra::crop(tmx, xtd)
    
    # Calculate heat stress generic crop
    for (j in 1:length(thr)) {
      cat('...processing threshold thr=',thr[j],'\n')
      ntx <- sum(tmx > thr[j])
      terra::writeRaster(x = ntx, filename = outfile[j], overwrite = T)
    }
    
    # Clean-up
    rm(tmx, ntx); gc(F, T, T)
    
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
    stp <- base::expand.grid(yrs, mnt, stringsAsFactors = F) |> base::as.data.frame() |> setNames(c('yrs', 'mnt')) |> dplyr::arrange(yrs, mnt) |> base::as.data.frame(); rm(mnt)
    
    ## Setup in/out files
    tx_pth  <- paste0(root,'/nex-gddp-cmip6/tasmax/',ssp,'/',gcm)
    thr <- 20:50
    out_dir <- paste0(root,'/nex-gddp-cmip6_indices/',ssp,'_',gcm) 
    
    1:nrow(stp) |> purrr::map(.f = function(i){calc_ntx(yr = stp$yrs[i], mn = stp$mnt[i], thr = thr)}); gc(F, T, T)
    tmpfls <- list.files(tempdir(), full.names = T)
    1:length(tmpfls) |> purrr::map(.f = function(k) {system(paste0('rm -f ', tmpfls[k]))})
    cat('----Finish----\n')
    
  }
  
}
