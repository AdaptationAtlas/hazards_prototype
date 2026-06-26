# Compute monthly potential evapotranspiration (PET) = FAO-56 Penman-Monteith ET0
# Allen et al. (1998), FAO Irrigation & Drainage Paper 56. Canonical reference ET0,
# replacing the non-standard CIAT peest2 as the published Atlas PET.
#
# Inputs (preprocessed daily NEX-GDDP-CMIP6 tifs):
#   tasmax, tasmin [degC]; rsds [MJ m-2 day-1] (preprocess already did *86400/1e6);
#   hurs [%, daily mean]; sfcWind [m/s @10m]. Latitude from the grid; DOY per day.
# Output: PET-<yr>-<mn>.tif = monthly sum of daily ET0 [mm/month].

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
suppressMessages(pacman::p_load(tidyverse, terra, lubridate))

# Audited FAO-56 PM ET0 function (sibling script), located repo-relative.
source(file.path(getOption("hazards.r_root"), "04_indices/et0_fao56.R"))

root <- common_data_root()
msk <- terra::rast(file.path(root, 'chirps_wrld', 'chirps-v2.0.1981.01.01.tif'))
xtd <- terra::ext(msk)

# Wind 10 m -> 2 m (FAO-56 Eq. 47): u2 = uz * 4.87 / ln(67.8*z - 5.42), z = 10 m.
WIND_10_TO_2 <- 4.87 / log(67.8 * 10 - 5.42)

calc_pet <- function(yr, mn){

  outfile <- paste0(out_dir,'/PET-',yr,'-',mn,'.tif')
  cat(outfile,'\n')

  if(!should_skip(outfile)){

    dir.create(dirname(outfile), F, T)

    last_day <- lubridate::days_in_month(as.Date(paste0(yr,'-',mn,'-01')))
    dts <- seq(from = as.Date(paste0(yr,'-',mn,'-01')),
               to   = as.Date(paste0(yr,'-',mn,'-',last_day)), by = 'day')

    # Keep only days present for ALL five variables, so the daily stacks stay aligned.
    ex <- function(pth, var) file.exists(paste0(pth,'/',var,'_',dts,'.tif'))
    keep <- ex(tx_pth,'tasmax') & ex(tn_pth,'tasmin') & ex(rs_pth,'rsds') &
            ex(rh_pth,'hurs')   & ex(ws_pth,'sfcWind')
    stopifnot(any(keep))
    dts <- dts[keep]
    Js  <- lubridate::yday(dts)

    rdc <- function(pth, var) terra::crop(terra::rast(paste0(pth,'/',var,'_',dts,'.tif')), xtd)
    tmx <- rdc(tx_pth,'tasmax'); tmn <- rdc(tn_pth,'tasmin')
    rs  <- rdc(rs_pth,'rsds')                       # already MJ m-2 day-1
    rh  <- rdc(rh_pth,'hurs')
    u2  <- rdc(ws_pth,'sfcWind') * WIND_10_TO_2     # 10 m -> 2 m
    lat <- terra::init(tmx[[1]], 'y')               # latitude per cell [deg]

    # Daily ET0, summed to the monthly total. elev = 0 (sea-level P/gamma) for now;
    # a DEM can refine the pressure term later (modest effect).
    pet <- NULL
    for(j in 1:terra::nlyr(tmx)){
      et0 <- et0_fao56(tmax = tmx[[j]], tmin = tmn[[j]], rs = rs[[j]], u2 = u2[[j]],
                       lat_deg = lat, J = Js[j], elev = 0, rh = rh[[j]])
      pet <- if(is.null(pet)) et0 else pet + et0
    }
    terra::writeRaster(x = pet, filename = outfile, overwrite = T)

    rm(tmx, tmn, rs, rh, u2, lat, pet); gc(F, T, T)
  }
}

# Run config - env-overridable via 00_setup.R (SCENARIO / SSPS / YRS / GCMS / MONTHS).
scenario <- cfg_scenario("historical")
ssps     <- cfg_ssps(scenario)
yrs      <- cfg_yrs(scenario, historical = 1995:2014)
gcms     <- cfg_gcms()
mnt_all  <- cfg_months()
.log('Run config: scenario=', scenario, ' | yrs=', min(yrs), ':', max(yrs),
     ' | n_gcms=', length(gcms))

for (gcm in gcms) {
  for (ssp in ssps) {

    stp <- base::expand.grid(yrs, mnt_all, stringsAsFactors = F) |>
      setNames(c('yrs','mnt')) |> dplyr::arrange(yrs, mnt) |> base::as.data.frame()

    tx_pth  <- paste0(root,'/nex-gddp-cmip6/tasmax/',ssp,'/',gcm)
    tn_pth  <- paste0(root,'/nex-gddp-cmip6/tasmin/',ssp,'/',gcm)
    rs_pth  <- paste0(root,'/nex-gddp-cmip6/rsds/',ssp,'/',gcm)
    rh_pth  <- paste0(root,'/nex-gddp-cmip6/hurs/',ssp,'/',gcm)
    ws_pth  <- paste0(root,'/nex-gddp-cmip6/sfcWind/',ssp,'/',gcm)
    out_dir <- paste0(root,'/nex-gddp-cmip6_indices/',ssp,'_',gcm,'/PET')

    1:nrow(stp) |> purrr::map(.f = function(i){ calc_pet(yr = stp$yrs[i], mn = stp$mnt[i]) }); gc(F, T, T)
    tmpfls <- list.files(tempdir(), full.names = T)
    1:length(tmpfls) |> purrr::map(.f = function(k){ system(paste0('rm -f ', tmpfls[k])) })
    cat('----Finish----\n')
  }
}
