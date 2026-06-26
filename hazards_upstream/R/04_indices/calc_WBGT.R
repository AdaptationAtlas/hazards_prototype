# Compute Wet Bulb Globe Temperature (WBGT, shaded/indoor) heat stress.
# Daily WBGTmax = wbgt_chc(Tmax, RH-at-Tmax) (heat_index.R): NWS Heat Index ->
# WBGTmax(C) = -0.0034*HI(F)^2 + 0.96*HI(F) - 34 (Williams et al. 2024 Sci.Data
# Eq.6; Bernard & Iheanacho 2015) - the same daily derivation CHC uses for
# CHIRTS-ERA5 / CHC-CMIP6 WBGTmax, here on NEX-GDDP. RHx from daily-mean hurs
# under the FAO-56 conserved-vapour-pressure assumption (rhx_from_daily).
# Outputs monthly mean + max of daily WBGTmax, the daily stack, and day-counts
# above CHC harm thresholds 28 / 30 / 32 C.

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
msk <- terra::rast(file.path(root,'chirps_wrld/chirps-v2.0.1981.01.01.tif'))
xtd <- terra::ext(msk)

calc_wbgt <- function(yr, mn){

  o_mean <- paste0(out_dir,'/WBGT_mean-',yr,'-',mn,'.tif')
  o_max  <- paste0(out_dir,'/WBGT_max-',yr,'-',mn,'.tif')
  o_dly  <- paste0(out_dir,'/daily/WBGT_daily-',yr,'-',mn,'.tif')
  o_d28  <- paste0(out_dir,'/WBGT_days28-',yr,'-',mn,'.tif')
  o_d30  <- paste0(out_dir,'/WBGT_days30-',yr,'-',mn,'.tif')
  o_d32  <- paste0(out_dir,'/WBGT_days32-',yr,'-',mn,'.tif')
  cat(o_max,'\n')

  if(!should_skip(c(o_mean,o_max,o_dly,o_d28,o_d30,o_d32))){

    dir.create(dirname(o_dly),F,T)

    last_day <- lubridate::days_in_month(as.Date(paste0(yr,'-',mn,'-01')))
    dts <- seq(as.Date(paste0(yr,'-',mn,'-01')), as.Date(paste0(yr,'-',mn,'-',last_day)), by='day')

    ex <- function(p,v) file.exists(paste0(p,'/',v,'_',dts,'.tif'))
    keep <- ex(tx_pth,'tasmax') & ex(tn_pth,'tasmin') & ex(rh_pth,'hurs')
    stopifnot("no complete tasmax/tasmin/hurs days for month" = any(keep))
    dd <- dts[keep]
    rdc <- function(p,v) terra::crop(terra::rast(paste0(p,'/',v,'_',dd,'.tif')), xtd)
    tmx <- rdc(tx_pth,'tasmax'); tmn <- rdc(tn_pth,'tasmin'); rhm <- rdc(rh_pth,'hurs')

    rhx  <- rhx_from_daily(tmx, tmn, rhm)
    WBGT <- wbgt_chc(tmx, rhx)               # daily WBGTmax, degC

    terra::writeRaster(mean(WBGT), o_mean, overwrite = T)
    terra::writeRaster(max(WBGT),  o_max,  overwrite = T)
    terra::writeRaster(WBGT,       o_dly,  overwrite = T)
    terra::writeRaster(sum(WBGT > 28), o_d28, overwrite = T)
    terra::writeRaster(sum(WBGT > 30), o_d30, overwrite = T)
    terra::writeRaster(sum(WBGT > 32), o_d32, overwrite = T)

    rm(tmx, tmn, rhm, rhx, WBGT); gc(F, T, T)
  }
}

# Run config - env-overridable via 00_setup.R (SCENARIO / SSPS / YRS / GCMS).
scenario <- cfg_scenario("historical")
ssps     <- cfg_ssps(scenario)
yrs      <- cfg_yrs(scenario, historical = 1995:2014)
gcms     <- cfg_gcms()
.log('Run config: scenario=', scenario, ' | yrs=', min(yrs), ':', max(yrs), ' | n_gcms=', length(gcms))

for (gcm in gcms) {
  for (ssp in ssps) {
    mnt <- cfg_months()   # honour MONTHS env
    stp <- base::expand.grid(yrs, mnt, stringsAsFactors = F) |> setNames(c('yrs','mnt')) |> dplyr::arrange(yrs, mnt) |> base::as.data.frame(); rm(mnt)
    tx_pth <- paste0(root,'/nex-gddp-cmip6/tasmax/',ssp,'/',gcm)
    tn_pth <- paste0(root,'/nex-gddp-cmip6/tasmin/',ssp,'/',gcm)
    rh_pth <- paste0(root,'/nex-gddp-cmip6/hurs/',ssp,'/',gcm)
    out_dir <- paste0(root,'/nex-gddp-cmip6_indices/',ssp,'_',gcm,'/WBGT')
    1:nrow(stp) |> purrr::map(.f = function(i){calc_wbgt(yr = stp$yrs[i], mn = stp$mnt[i])}); gc(F, T, T)
    tmpfls <- list.files(tempdir(), full.names = T)
    1:length(tmpfls) |> purrr::map(.f = function(k) {system(paste0('rm -f ', tmpfls[k]))})
    cat('----Finish----\n')
  }
}
