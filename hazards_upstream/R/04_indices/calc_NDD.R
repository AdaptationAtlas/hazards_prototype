# Compute number of dry days (NDD)
# By: H. Achicanoy, F. Castro
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

# NDD function
calc_ndd <- function(yr, mn){
  
  outfile <- paste0(out_dir,'/NDD-',yr,'-',mn,'.tif')
  cat(outfile,'\n')
  
  if(!should_skip(outfile)){
    
    dir.create(dirname(outfile),F,T)
    
    # Sequence of dates
    last_day <- lubridate::days_in_month(as.Date(paste0(yr,'-',mn,'-01')))
    dts <- seq(from = as.Date(paste0(yr,'-',mn,'-01')), to = as.Date(paste0(yr,'-',mn,'-',last_day)), by = 'day')
    
    # Files
    fls <- paste0(pr_pth,'/','pr_',dts,'.tif')
    fls <- fls[file.exists(fls)]
    stopifnot(length(fls) > 0)

    # Read daily precipitation data
    prc <- terra::rast(fls)
    prc <- terra::crop(prc, xtd)
    
    # Calculate number of dry days
    ndd <- sum(prc < 1)
    terra::writeRaster(x = ndd, filename = outfile, overwrite = T)
    
    # Clean-up
    rm(prc, ndd); gc(F, T, T)
    
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
    cmb <- paste0(ssp,'_',gcm)
    mnt <- cfg_months()   # honour MONTHS env (was hardcoded 1:12)
    stp <- base::expand.grid(yrs, mnt, stringsAsFactors = F) |> base::as.data.frame() |> setNames(c('yrs', 'mnt')) |> dplyr::arrange(yrs, mnt) |> base::as.data.frame(); rm(mnt)
    
    ## Setup in/out files
    pr_pth  <- paste0(root,'/nex-gddp-cmip6/pr/',ssp,'/',gcm)
    out_dir <- paste0(root,'/nex-gddp-cmip6_indices/',ssp,'_',gcm,'/NDD')
    
    1:nrow(stp) |> purrr::map(.f = function(i){calc_ndd(yr = stp$yrs[i], mn = stp$mnt[i])}); gc(F, T, T)
    tmpfls <- list.files(tempdir(), full.names = T)
    1:length(tmpfls) |> purrr::map(.f = function(k) {system(paste0('rm -f ', tmpfls[k]))})
    cat('----Finish----\n')
    
  }
  
}
