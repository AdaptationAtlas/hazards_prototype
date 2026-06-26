# Water-balance indices v2: NDWS + NDWL0 + NDWL50 in ONE FAO-56/AquaCrop pass.
# Peer-reviewed: FAO-56 root-zone balance + water-stress coeff Ks (Allen et al.
# 1998 Ch.8; Pereira et al. 2026) for drought; AquaCrop aeration-stress coeff
# Ks_aer (Raes et al. 2009) for waterlogging. PET = FAO-56 Penman-Monteith ET0
# (et0_fao56.R). Single C++ pass via wbkernel::wb_kernel_cpp. Replaces the legacy
# eabyep heuristic + the three separate fast_calc_NDWS/NDWL0/NDWL50 scripts.
#   NDWS   = days Ks < 0.5
#   NDWL0  = days waterlogged (soil water above the anaerobiosis point)
#   NDWL50 = days Ks_aer <= 0.5 (severe aeration stress)
# Crop-agnostic reference: Kc=1, depletion p=0.5, anaerobiosis A=5 vol%
# (a_mm = 0.05*Zr*1000 = 0.5*Zr_cm), aeration lag 4 d. Needs sfcWind + soil
# rasters sscp/ssat/sroot (TAW mm, sat-excess mm, rooting depth cm). Honors
# GCMS/SCENARIO/SSPS/YRS/MONTHS; processes months chronologically (AVAIL seed).

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
P_DEPLETION  <- 0.5                            # RAW = p*TAW (crop-agnostic)
AER_LAG      <- 4                              # saturated days before full aeration stress

root <- common_data_root()
msk <- terra::rast(file.path(root, 'chirps_wrld', 'chirps-v2.0.1981.01.01.tif'))
xtd <- terra::ext(msk)
# Soil: TAW (mm), saturation-excess (mm), rooting depth (cm). a_mm = 0.5*Zr_cm.
taw0  <- terra::rast(paste0(root,'/atlas_hazards/soils/sscp_world.tif'))
ssat0 <- terra::rast(paste0(root,'/atlas_hazards/soils/ssat_world.tif'))
zr0   <- terra::rast(paste0(root,'/atlas_hazards/soils/sroot_world.tif'))   # rooting depth, cm

calc_wb <- function(yr, mn){

  out_ndws  <- paste0(nd_dir,'/NDWS-',yr,'-',mn,'.tif')
  out_ndwl0 <- paste0(n0_dir,'/NDWL0-',yr,'-',mn,'.tif')
  out_ndwl5 <- paste0(n5_dir,'/NDWL50-',yr,'-',mn,'.tif')
  out_swb   <- paste0(sw_dir,'/SWB-',yr,'-',mn,'.tif')        # S (water rel. to FC), next-month seed
  cat(out_ndws,'\n')

  if (should_skip(c(out_ndws, out_ndwl0, out_ndwl5))) return(invisible())
  for (d in c(nd_dir,n0_dir,n5_dir,sw_dir)) dir.create(d, F, T)

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
  taw  <- terra::crop(terra::resample(taw0,  prc[[1]]), xtd)
  ssat <- terra::crop(terra::resample(ssat0, prc[[1]]), xtd)
  a_mm <- 0.5 * terra::crop(terra::resample(zr0, prc[[1]]), xtd)   # 0.05*Zr(m)*1000 = 0.5*Zr(cm)

  # Initial soil water S (rel. to FC): seed month -> at field capacity (S=0);
  # else carry the prior month's S (deterministic, in chronological order).
  if (paste0(yr,'-',mn) %in% c('1995-01','2021-01')) {
    s0 <- terra::ifel(is.na(taw), NA, 0)
  } else {
    pdate <- as.Date(paste0(yr,'-',mn,'-01')) - 1
    prior <- paste0(sw_dir,'/SWB-',format(pdate,'%Y'),'-',format(pdate,'%m'),'.tif')
    stopifnot("prior-month SWB missing - run months in order from the seed" = file.exists(prior))
    s0 <- terra::rast(prior)
  }

  k <- wb_kernel_cpp(rain = terra::values(prc), et0 = terra::values(et0),
                     taw = terra::values(taw)[,1], ssat = terra::values(ssat)[,1],
                     a_mm = terra::values(a_mm)[,1], s0 = terra::values(s0)[,1],
                     p = P_DEPLETION, aer_lag = AER_LAG)

  ndws  <- terra::setValues(prc[[1]], rowSums(k$ks    < 0.5))
  ndwl0 <- terra::setValues(prc[[1]], rowSums(k$wl    > 0))
  ndwl5 <- terra::setValues(prc[[1]], rowSums(k$ksaer <= 0.5))
  swb   <- terra::setValues(prc[[1]], k$s_final)

  terra::writeRaster(ndws,  out_ndws,  overwrite=TRUE)
  terra::writeRaster(ndwl0, out_ndwl0, overwrite=TRUE)
  terra::writeRaster(ndwl5, out_ndwl5, overwrite=TRUE)
  terra::writeRaster(swb,   out_swb,   overwrite=TRUE)
  rm(prc,tmx,tmn,rs,rh,u2,lat,et0,taw,ssat,a_mm,s0,k); gc(F,T,T)
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
    n5_dir <- paste0(base_out,'/NDWL50'); sw_dir <- paste0(base_out,'/SWB')
    for (i in 1:nrow(stp)) calc_wb(stp$yrs[i], stp$mnt[i])
    gc(F,T,T); cat('----Finish----\n')
  }
}
