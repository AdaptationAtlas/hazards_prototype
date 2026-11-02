suppressMessages({library(arrow); library(dplyr); library(data.table)})
OR  <- "/home/jovyan/common_data/atlas_obs_hazards"
NWD <- "/home/jovyan/common_data/nex-gddp-cimp6_hazards"
units <- data.table(iso3=c("SLE","SOM","MOZ","UGA"),
                    admin2_name=c("Kambia","Badhaadhe","Lago","Namayingo"),
                    gaul2_code=c(105522,105570,104396,106299))
spec <- list(
  vop_usd=list(tier="Data/hazard_risk_vop_usd/ggcmi-crop/haz-freq-exp_vop_nominal-usd-2021_chirps-chirts-agera5_int_adm_moderate.parquet",
               expo="Data/exposure/vop_nominal-usd-2021_adm_sum_spam20_glw420_res-25.parquet", var="vop"),
  prod_t =list(tier="Data/hazard_risk_prod/ggcmi-crop/haz-freq-exp_prod_t_chirps-chirts-agera5_int_adm_moderate.parquet",
               expo="Data/exposure/exposure_adm_sum_spam20-20_glw420-20_res-25.parquet", var="prod"),
  ha     =list(tier="Data/hazard_risk_ha/ggcmi-crop/haz-freq-exp_harv-area_ha_chirps-chirts-agera5_int_adm_moderate.parquet",
               expo="Data/exposure/exposure_adm_sum_spam20-20_glw420-20_res-25.parquet", var="harv-area"))
crops <- c("banana","potato","yams","cotton","plantain","cowpea","lentil","robusta-coffee")
res <- rbindlist(lapply(names(spec), function(v) {
  s <- spec[[v]]; tf <- file.path(OR,s$tier)
  if (!file.exists(tf)) { cat("MISSING:",tf,"\n"); return(NULL) }
  P <- open_dataset(tf) |> filter(scenario=="observed", hazard %in% c("any","none"),
        gaul2_code %in% units$gaul2_code, crop %in% crops) |>
       select(gaul2_code,crop,hazard_vars,value) |> collect() |> as.data.table()
  Pt <- P[, .(T=sum(value,na.rm=TRUE)), by=.(gaul2_code,crop,hazard_vars)][, .(T=max(T)), by=.(gaul2_code,crop)]
  .w <- s$var
  E <- open_dataset(file.path(NWD,s$expo)) |> filter(!is.na(admin2_name), exposure==.w,
        gaul2_code %in% units$gaul2_code, crop %in% crops) |>
       select(gaul2_code,crop,tech,value) |> collect() |> as.data.table()
  Et <- E[(tech=="all"|is.na(tech)) & is.finite(value), .(E=sum(value)), by=.(gaul2_code,crop)]
  m <- merge(Et[E>0], Pt, by=c("gaul2_code","crop"), all.x=TRUE); m[is.na(T), T:=0]
  m[, `:=`(variable=v, ratio=T/E)]; m }))
res <- merge(res, units, by="gaul2_code")
cat("== per unit x crop x variable: E, T, ratio ==\n")
print(dcast(res, admin2_name+crop ~ variable, value.var="ratio", fun.aggregate=function(z) round(z[1],4)))
cat("\n== how many crops in each unit are short, by variable (ratio < 0.98, E>1000) ==\n")
print(res[E>1000, .(crops_E=.N, short=sum(ratio<0.98), min_ratio=round(min(ratio),4)), by=.(admin2_name,variable)][order(admin2_name,variable)])
