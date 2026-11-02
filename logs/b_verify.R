suppressMessages({library(arrow); library(dplyr); library(data.table)})
source("/home/jovyan/atlas/hazards_prototype/R/checks/r3_tier_drift_vs_live.R")
OR  <- "/home/jovyan/common_data/atlas_obs_hazards"
NWD <- "/home/jovyan/common_data/nex-gddp-cimp6_hazards"
expo <- file.path(NWD,"Data/exposure","exposure_adm_sum_spam20-20_glw420-20_res-25.parquet")
nine <- c("vegetables","tropical-fruit","rest-of-crops","other-roots","temperate-fruit",
          "other-cereals","other-pulses","other-oil-crops","other-fibre-crops")
out <- rbindlist(lapply(c("severe","moderate","extreme"), function(tier) {
  f <- file.path(OR,"Data/hazard_risk_n/ggcmi-crop",
        sprintf("haz-freq-exp_head_n_chirps-chirts-agera5_int_adm_%s.parquet", tier))
  rbindlist(lapply(c("default nine","nine + total-*"), function(lab) {
    np <- if (lab=="default nine") nine else c(nine,"total-tropical","total-highland")
    r <- tier_vs_exposure(f, expo, "number", no_product_crops=np,
                          baseline="observed", scenarios="observed")
    data.table(tier=tier, no_product_crops=lab, pass=r$pass, gates=r$gates,
               material=nrow(r$material),
               breach=nrow(r$material[abs(ratio-1)>0.02]),
               missing_product=nrow(r$missing_product),
               unmodelled=nrow(r$unmodelled),
               g3=r$gates[gate=="no material exposure pair missing from the product", paste0(value," / ",pass)])
  }))
}))
print(out)
