suppressMessages({library(terra); library(data.table); library(sf); library(arrow)})
OR <- "/home/jovyan/common_data/atlas_obs_hazards"
SP <- file.path(OR,"Data/mapspam/2020V1r2_SSA/processed")
HR <- file.path(OR,"Data/hazard_risk/ggcmi-crop")
AN <- file.path(OR,"Data/obs_seasonal/chirps-chirts-agera5_2006-2025/annual12")
V <- rast(file.path(SP,"variable=vop_nominal-usd-2021/spam_vop_nominal-usd-2021_all_res-25.tif"))
P <- rast(file.path(SP,"variable=prod_t/spam_prod_t_all.tif"))
ndws <- rast(file.path(AN,"NDWS_mean.tif")); ntx <- rast(file.path(AN,"NTx35_mean.tif"))
.a2f <- file.path(OR,"Data/boundaries/atlas_gaul24_a2_africa.parquet")
.t <- arrow::read_parquet(.a2f)
.gc <- if ("geometry" %in% names(.t)) "geometry" else tail(names(.t),1)
.g <- sf::st_as_sfc(structure(.t[[.gc]], class="WKB"), EWKB=TRUE)
.sf <- sf::st_sf(.t[, setdiff(names(.t), .gc)], geometry=.g, crs=4326)
a2 <- terra::vect(.sf)
nm <- grep("gaul2|admin2", names(a2), value=TRUE, ignore.case=TRUE)
jobs <- list(list(u=105522,n="Kambia (SLE)",   cr=c("banana","potato","yams","cotton")),
             list(u=105570,n="Badhaadhe (SOM)",cr=c("plantain","cowpea")),
             list(u=104396,n="Lago (MOZ)",     cr=c("robusta coffee")),
             list(u=106299,n="Namayingo (UGA)",cr=c("lentil")))
cat("a2 fields:", paste(nm, collapse=", "), "\n")
res <- rbindlist(lapply(jobs, function(j) {
  pol <- a2[a2$gaul2_code == j$u, ]
  if (!nrow(pol)) { cat("NO POLYGON", j$n, "\n"); return(NULL) }
  e <- ext(pol) + 0.5
  Vc <- crop(V, e); m <- rasterize(pol, Vc[[1]], touches=TRUE)
  nd <- crop(ndws, e); nt <- crop(ntx, e)
  nd <- resample(nd, Vc[[1]], method="near"); nt <- resample(nt, Vc[[1]], method="near")
  fa <- round(0.25 / res(P)[1])
  Pa <- aggregate(crop(P, e), fact=fa, fun="sum", na.rm=TRUE); Pa <- resample(Pa, Vc[[1]], method="near")
  rbindlist(lapply(j$cr, function(cp) {
    if (!(cp %in% names(Vc))) { cat("no vop layer", cp, "\n"); return(NULL) }
    cb <- gsub(" ", "-", cp)
    bf <- file.path(HR, sprintf("%s_chirps-chirts-agera5_moderate_NDWS+NTx35+NDWL0_int.tif", cb))
    if (!file.exists(bf)) { cat("no bundle", cp, "\n"); return(NULL) }
    B <- resample(crop(rast(bf)[[1]], e), Vc[[1]], method="near")
    d <- data.table(inu=values(m)[,1], vop=values(Vc[[cp]])[,1],
                    prod=if (cp %in% names(Pa)) values(Pa[[cp]])[,1] else NA_real_,
                    bun=values(B)[,1], nd=values(nd)[,1], nt=values(nt)[,1])
    d <- d[!is.na(inu)]
    dv <- d[!is.na(vop) & vop > 0]
    data.table(unit=j$n, crop=cp,
      cells_in_unit=nrow(d), vop_cells=nrow(dv), vop_total=round(sum(dv$vop)),
      vop_on_bundleNA=nrow(dv[is.na(bun)]),
      vop_val_on_bundleNA=round(sum(dv[is.na(bun)]$vop)),
      share_lost=round(sum(dv[is.na(bun)]$vop)/sum(dv$vop),4),
      vop_no_prod=nrow(dv[is.na(prod) | prod==0]),
      bNA_ntx_ok_ndws_NA=nrow(dv[is.na(bun) & !is.na(nt) & is.na(nd)]),
      bNA_ntx_NA=nrow(dv[is.na(bun) & is.na(nt)]))
  }))
}))
print(res)
