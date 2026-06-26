# Water-balance indices v2: NDWS + NDWL0 + NDWL50 in ONE pass.
# Replaces the three separate fast_calc_NDWS/NDWL0/NDWL50.R scripts (each of which
# recomputed PET + the daily water balance independently) with a single
# computation:
#   PET   = FAO-56 Penman-Monteith ET0 (et0_fao56.R)  -- replaces CIAT peest2
#   balance = wb_kernel_cpp (Rcpp single-pass EABYEP)  -- replaces the R day loop
#   AVAIL seed = deterministic prior month (hazards#19) -- replaces lexical-last
# Indices: NDWS = days ERATIO<0.5 ; NDWL0 = days LOGGING>0 ; NDWL50 = days
#          LOGGING>0.5*soilsat. The legacy scripts are kept for the old-vs-new
#          impact comparison; this is the v2 producer.
# Needs sfcWind (FAO-56 PM wind term). Honors GCMS/SCENARIO/SSPS/YRS/MONTHS.

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
suppressMessages(pacman::p_load(tidyverse, terra, lubridate, wbkernel))
source(file.path(getOption("hazards.r_root"), "04_indices/et0_fao56.R"))

WIND_10_TO_2 <- 4.87 / log(67.8 * 10 - 5.42)   # FAO-56 Eq.47 (z=10m) -> 0.748

root <- common_data_root()
msk <- terra::rast(file.path(root, 'chirps_wrld', 'chirps-v2.0.1981.01.01.tif'))
xtd <- terra::ext(msk)
scp0 <- terra::rast(paste0(root,'/atlas_hazards/soils/sscp_world.tif'))   # soil water capacity
sst0 <- terra::rast(paste0(root,'/atlas_hazards/soils/ssat_world.tif'))   # extra saturation capacity

calc_wb <- function(yr, mn){

  out_ndws  <- paste0(nd_dir,'/NDWS-',yr,'-',mn,'.tif')
  out_ndwl0 <- paste0(n0_dir,'/NDWL0-',yr,'-',mn,'.tif')
  out_ndwl5 <- paste0(n5_dir,'/NDWL50-',yr,'-',mn,'.tif')
  out_avail <- paste0(av_dir,'/AVAIL-',yr,'-',mn,'.tif')
  cat(out_ndws,'\n')

  if (should_skip(c(out_ndws, out_ndwl0, out_ndwl5))) return(invisible())
  for (d in c(nd_dir,n0_dir,n5_dir,av_dir)) dir.create(d, F, T)

  last_day <- lubridate::days_in_month(as.Date(paste0(yr,'-',mn,'-01')))
  dts <- seq(as.Date(paste0(yr,'-',mn,'-01')), as.Date(paste0(yr,'-',mn,'-',last_day)), by='day')

  ex  <- function(p,v) file.exists(paste0(p,'/',v,'_',dts,'.tif'))
  keep <- ex(pr_pth,'pr') & ex(tx_pth,'tasmax') & ex(tn_pth,'tasmin') &
          ex(rs_pth,'rsds') & ex(rh_pth,'hurs') & ex(ws_pth,'sfcWind')
  stopifnot("no complete met days for month" = any(keep))
  dts <- dts[keep]; Js <- lubridate::yday(dts)
  rdc <- function(p,v) terra::crop(terra::rast(paste0(p,'/',v,'_',dts,'.tif')), xtd)

  prc <- rdc(pr_pth,'pr')
  tmx <- rdc(tx_pth,'tasmax'); tmn <- rdc(tn_pth,'tasmin')
  rs  <- rdc(rs_pth,'rsds');   rh  <- rdc(rh_pth,'hurs')
  u2  <- rdc(ws_pth,'sfcWind') * WIND_10_TO_2
  lat <- terra::init(prc[[1]], 'y')

  # Daily FAO-56 PM ET0 stack (mm/day)
  et0 <- terra::rast(lapply(seq_along(dts), function(j)
    et0_fao56(tmax=tmx[[j]], tmin=tmn[[j]], rs=rs[[j]], u2=u2[[j]],
              lat_deg=lat, J=Js[j], elev=0, rh=rh[[j]])))

  # Soil to the met grid
  scp <- terra::crop(terra::resample(scp0, prc[[1]]), xtd)
  sst <- terra::crop(terra::resample(sst0, prc[[1]]), xtd)

  # Deterministic prior-month AVAIL seed (hazards#19); seed month -> 0.
  if (paste0(yr,'-',mn) %in% c('1995-01','2021-01')) {
    avail0 <- terra::ifel(is.na(scp), NA, 0)
  } else {
    pdate <- as.Date(paste0(yr,'-',mn,'-01')) - 1
    prior <- paste0(av_dir,'/AVAIL-',format(pdate,'%Y'),'-',format(pdate,'%m'),'.tif')
    stopifnot("prior-month AVAIL missing - run months in order from the seed" = file.exists(prior))
    avail0 <- terra::rast(prior)
  }

  k <- wb_kernel_cpp(rain = terra::values(prc), evap = terra::values(et0),
                     soilcp = terra::values(scp)[,1], soilsat = terra::values(sst)[,1],
                     avail0 = terra::values(avail0)[,1])

  sst_v <- terra::values(sst)[,1]
  ndws  <- terra::setValues(prc[[1]], rowSums(k$eratio  < 0.5))
  ndwl0 <- terra::setValues(prc[[1]], rowSums(k$logging > 0))
  ndwl5 <- terra::setValues(prc[[1]], rowSums(k$logging > 0.5 * sst_v))
  avf   <- terra::setValues(prc[[1]], k$avail_final)

  terra::writeRaster(ndws,  out_ndws,  overwrite=TRUE)
  terra::writeRaster(ndwl0, out_ndwl0, overwrite=TRUE)
  terra::writeRaster(ndwl5, out_ndwl5, overwrite=TRUE)
  terra::writeRaster(avf,   out_avail, overwrite=TRUE)
  rm(prc,tmx,tmn,rs,rh,u2,lat,et0,scp,sst,avail0,k); gc(F,T,T)
}

# Run config
scenario <- cfg_scenario("historical")
ssps     <- cfg_ssps(scenario)
yrs      <- cfg_yrs(scenario, historical = 1995:2014)
gcms     <- cfg_gcms()
mnt_all  <- cfg_months()
.log('Run config: scenario=', scenario, ' | yrs=', min(yrs), ':', max(yrs), ' | n_gcms=', length(gcms))

for (gcm in gcms) {
  for (ssp in ssps) {
    # process chronologically so the deterministic AVAIL seed always finds prior month
    stp <- base::expand.grid(yrs, mnt_all, stringsAsFactors=F) |>
      setNames(c('yrs','mnt')) |> dplyr::arrange(yrs, mnt) |> base::as.data.frame()
    pr_pth <- paste0(root,'/nex-gddp-cmip6/pr/',ssp,'/',gcm)
    tx_pth <- paste0(root,'/nex-gddp-cmip6/tasmax/',ssp,'/',gcm)
    tn_pth <- paste0(root,'/nex-gddp-cmip6/tasmin/',ssp,'/',gcm)
    rs_pth <- paste0(root,'/nex-gddp-cmip6/rsds/',ssp,'/',gcm)
    rh_pth <- paste0(root,'/nex-gddp-cmip6/hurs/',ssp,'/',gcm)
    ws_pth <- paste0(root,'/nex-gddp-cmip6/sfcWind/',ssp,'/',gcm)
    base_out <- paste0(root,'/nex-gddp-cmip6_indices/',ssp,'_',gcm)
    nd_dir <- paste0(base_out,'/NDWS'); n0_dir <- paste0(base_out,'/NDWL0')
    n5_dir <- paste0(base_out,'/NDWL50'); av_dir <- paste0(base_out,'/AVAIL')
    for (i in 1:nrow(stp)) calc_wb(stp$yrs[i], stp$mnt[i])
    gc(F,T,T); cat('----Finish----\n')
  }
}
