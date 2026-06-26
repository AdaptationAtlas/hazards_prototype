# Compute human heat stress (HSH) = NWS (Rothfusz) Heat Index.
# By: H. Achicanoy; revised 2025 (Stage-0): use the full NOAA Heat Index from
# Tmax + RH-at-Tmax (heat_index.R), replacing the prior daily-mean-temperature
# formula that omitted the Steadman fallback and the low/high-RH adjustments.
# RHx (RH at Tmax) derived from daily-mean hurs under the FAO-56 conserved-
# vapour-pressure assumption (see heat_index.R / rhx_from_daily).
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
source(file.path(getOption("hazards.r_root"), "04_indices/heat_index.R"))

root <- common_data_root()

# Extent CHIRPS
msk <- terra::rast(file.path(root,'chirps_wrld/chirps-v2.0.1981.01.01.tif'))
xtd <- terra::ext(msk)

# HSH function
calc_hsh <- function(yr, mn){
  
  outfile1 <- paste0(out_dir,'/HSH_mean-',yr,'-',mn,'.tif')
  outfile2 <- paste0(out_dir,'/HSH_max-',yr,'-',mn,'.tif')
  outfile3 <- paste0(out_dir,'/daily/HSH_daily-',yr,'-',mn,'.tif')
  
  cat(outfile2,'\n')
  
  if(!should_skip(c(outfile1,outfile2,outfile3))){
    
    dir.create(dirname(outfile3),F,T)
    
    # Sequence of dates
    last_day <- lubridate::days_in_month(as.Date(paste0(yr,'-',mn,'-01'))) # Last day of the month
    dts <- seq(from = as.Date(paste0(yr,'-',mn,'-01')), to = as.Date(paste0(yr,'-',mn,'-',last_day)), by = 'day')
    
    # Files - keep only days present for all three variables (aligned stacks)
    ex <- function(p,v) file.exists(paste0(p,'/',v,'_',dts,'.tif'))
    keep <- ex(tx_pth,'tasmax') & ex(tn_pth,'tasmin') & ex(rh_pth,'hurs')
    stopifnot("no complete tasmax/tasmin/hurs days for month" = any(keep))
    dd <- dts[keep]
    rdc <- function(p,v) terra::crop(terra::rast(paste0(p,'/',v,'_',dd,'.tif')), xtd)
    tmx <- rdc(tx_pth,'tasmax'); tmn <- rdc(tn_pth,'tasmin'); rhm <- rdc(rh_pth,'hurs')

    # NWS Heat Index from Tmax + RH-at-Tmax (RHx). Full NOAA algorithm in
    # heat_index.R - fixes the prior bug (daily-mean T, no Steadman/low-RH/high-RH).
    rhx <- rhx_from_daily(tmx, tmn, rhm)
    HI  <- heat_index_nws(tmx, rhx)          # daily Heat Index, degC
    HI_avg <- mean(HI)
    HI_max <- max(HI)

    terra::writeRaster(HI_avg, outfile1, overwrite = T)
    terra::writeRaster(HI_max, outfile2, overwrite = T)
    terra::writeRaster(HI, outfile3, overwrite = T)

    # Clean-up
    rm(tmx, tmn, rhm, rhx, HI, HI_avg, HI_max); gc(F, T, T)
    
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
    mnt <- cfg_months()   # honour MONTHS env (was hardcoded 1:12)
    stp <- base::expand.grid(yrs, mnt, stringsAsFactors = F) |> setNames(c('yrs', 'mnt')) |> dplyr::arrange(yrs, mnt) |> base::as.data.frame(); rm(mnt)
    
    ## Setup in/out files
    tx_pth <- paste0(root,'/nex-gddp-cmip6/tasmax/',ssp,'/',gcm) # Daily maximum temperatures
    tn_pth <- paste0(root,'/nex-gddp-cmip6/tasmin/',ssp,'/',gcm) # Daily minimum temperatures
    rh_pth <- paste0(root,'/nex-gddp-cmip6/hurs/',ssp,'/',gcm)   # Daily relative humidity
    out_dir <- paste0(root,'/nex-gddp-cmip6_indices/',ssp,'_',gcm,'/HSH')
    
    1:nrow(stp) |> purrr::map(.f = function(i){calc_hsh(yr = stp$yrs[i], mn = stp$mnt[i])}); gc(F, T, T)
    tmpfls <- list.files(tempdir(), full.names = T)
    1:length(tmpfls) |> purrr::map(.f = function(k) {system(paste0('rm -f ', tmpfls[k]))})
    cat('----Finish----\n')
    
  }
  
}
