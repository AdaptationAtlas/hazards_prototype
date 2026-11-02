suppressMessages({library(arrow); library(dplyr); library(data.table)})
OR  <- "/home/jovyan/common_data/atlas_obs_hazards"
NWD <- "/home/jovyan/common_data/nex-gddp-cimp6_hazards"
tier <- file.path(OR,"Data/hazard_risk_vop_usd/ggcmi-crop",
  "haz-freq-exp_vop_nominal-usd-2021_chirps-chirts-agera5_int_adm_moderate.parquet")
expo <- file.path(NWD,"Data/exposure","vop_nominal-usd-2021_adm_sum_spam20_glw420_res-25.parquet")
pr <- data.table(
  iso3=c("SLE","SLE","SLE","SLE","SOM","SOM","UGA","MOZ"),
  crop=c("banana","potato","yams","cotton","plantain","cowpea","lentil","robusta-coffee"))
isos <- unique(pr$iso3); crops <- unique(pr$crop)
P <- open_dataset(tier) |> filter(scenario=="observed", hazard %in% c("any","none"),
       iso3 %in% isos, crop %in% crops, !is.na(admin2_name)) |>
  select(iso3,crop,gaul2_code,admin2_name,hazard_vars,hazard,value) |> collect() |> as.data.table()
Pt <- P[, .(T=sum(value,na.rm=TRUE)), by=.(iso3,crop,gaul2_code,admin2_name,hazard_vars)][
        , .(T=max(T)), by=.(iso3,crop,gaul2_code,admin2_name)]
E <- open_dataset(expo) |> filter(!is.na(admin2_name), exposure=="vop",
       iso3 %in% isos, crop %in% crops) |>
  select(iso3,crop,gaul2_code,admin2_name,tech,value) |> collect() |> as.data.table()
Et <- E[(tech=="all"|is.na(tech)) & is.finite(value), .(E=sum(value)), by=.(iso3,crop,gaul2_code,admin2_name)]
J <- merge(Et[E>0], Pt, by=c("iso3","crop","gaul2_code","admin2_name"), all.x=TRUE)
J[is.na(T), T:=0][, short := E-T]
out <- J[pr, on=.(iso3,crop)]
cat("== per pair: where the shortfall sits (units ordered by shortfall) ==\n")
for (i in seq_len(nrow(pr))) {
  p <- pr[i]; x <- out[iso3==p$iso3 & crop==p$crop][order(-short)]
  tot <- sum(x$E); tt <- sum(x$T)
  cat(sprintf("\n-- %s %s : E=%.0f T=%.0f ratio=%.4f ; %d units with E>0, %d short\n",
      p$iso3,p$crop,tot,tt,tt/tot,nrow(x),nrow(x[short>1])))
  print(head(x[, .(admin2_name,gaul2_code,E=round(E),T=round(T),short=round(short),ratio=round(T/E,4))],5))
}
cat("\n== shortfall by admin2 unit, summed over the 8 pairs ==\n")
print(out[, .(pairs=.N, short=round(sum(short)), E=round(sum(E))), by=.(iso3,admin2_name,gaul2_code)][order(-short)][1:12])
