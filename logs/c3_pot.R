suppressMessages({library(terra); library(data.table); library(sf); library(arrow)})
OR <- "/home/jovyan/common_data/atlas_obs_hazards"
SP <- file.path(OR,"Data/mapspam/2020V1r2_SSA/processed")
SE <- file.path(OR,"Data/obs_seasonal/chirps-chirts-agera5_2006-2025")
HR <- file.path(OR,"Data/hazard_risk/ggcmi-crop")
V  <- rast(file.path(SP,"variable=vop_nominal-usd-2021/spam_vop_nominal-usd-2021_all_res-25.tif"))
.t <- read_parquet(file.path(OR,"Data/boundaries/atlas_gaul24_a2_africa.parquet"))
.gc <- if ("geometry" %in% names(.t)) "geometry" else tail(names(.t),1)
a2 <- terra::vect(sf::st_sf(.t[, setdiff(names(.t), .gc)],
        geometry=sf::st_as_sfc(structure(.t[[.gc]], class="WKB"), EWKB=TRUE), crs=4326))
pol <- a2[a2$gaul2_code == 105522, ]; e <- ext(pol) + 0.5
Vc <- crop(V, e); m <- rasterize(pol, Vc[[1]], touches=TRUE)

# GGCMI potato rainfed calendar -> window length in months, on the 0.25 grid
cal <- rast(file.path("/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/ggcmi",
                      "pot_rf_ggcmi_crop_calendar_phase3_v1.01.nc4"))
cat("calendar layers:", paste(names(cal), collapse=", "), "\n")
pl <- cal[["planting_day"]]; hv <- cal[["maturity_day"]]
pm <- ceiling(pl/30.42); hm <- ceiling(hv/30.42)
nmo <- ifel(hm >= pm, hm - pm + 1, 12 - pm + 1 + hm)
nmo <- resample(crop(nmo, e), Vc[[1]], method="near")

g <- function(p, lyr) resample(crop(rast(file.path(SE,p))[[lyr]], e), Vc[[1]], method="near")
d <- data.table(
  inu  = values(m)[,1],
  vop  = values(Vc[["potato"]])[,1],
  months_pot = values(nmo)[,1],
  ndws_pot   = values(g("ggcmi-pot/NDWS_mean.tif",1))[,1],
  ntx_pot    = values(g("ggcmi-pot/NTx35_mean.tif",1))[,1],
  ndws_ann   = values(g("annual12/NDWS_mean.tif",1))[,1],
  ntx_ann    = values(g("annual12/NTx35_mean.tif",1))[,1],
  bundle     = values(resample(crop(rast(file.path(HR,"potato_chirps-chirts-agera5_moderate_NDWS+NTx35+NDWL0_int.tif"))[[1]], e), Vc[[1]], method="near"))[,1])
d <- d[!is.na(inu)]
cat("\n== Kambia cells (touches=TRUE), potato ==\n")
print(d[, .(vop=round(vop), months_pot, ndws_pot=round(ndws_pot,2), ntx_pot=round(ntx_pot,2),
            ndws_ann=round(ndws_ann,2), bundle=round(bundle,4))])
cat("\nvop-bearing cells:", nrow(d[!is.na(vop)&vop>0]),
    "| of them bundle NA:", nrow(d[!is.na(vop)&vop>0&is.na(bundle)]),
    "| of those: months<3:", nrow(d[!is.na(vop)&vop>0&is.na(bundle)&!is.na(months_pot)&months_pot<3]),
    "| NDWS NA & NTx ok:", nrow(d[!is.na(vop)&vop>0&is.na(bundle)&is.na(ndws_pot)&!is.na(ntx_pot)]), "\n")
