suppressMessages({library(arrow); library(dplyr); library(data.table)})
source("/home/jovyan/atlas/hazards_prototype/R/checks/r3_tier_drift_vs_live.R")
OR  <- "/home/jovyan/common_data/atlas_obs_hazards"
NWD <- "/home/jovyan/common_data/nex-gddp-cimp6_hazards"
r <- tier_vs_exposure(
  file.path(OR,"Data/hazard_risk_n/ggcmi-crop","haz-freq-exp_head_n_chirps-chirts-agera5_int_adm_moderate.parquet"),
  file.path(NWD,"Data/exposure","exposure_adm_sum_spam20-20_glw420-20_res-25.parquet"),
  "number", baseline="observed", scenarios="observed")
mp <- r$missing_product
cat("missing_product rows:", nrow(mp), "\n")
print(mp[, .(pairs=.N, iso3=uniqueN(iso3), E=round(sum(E))), by=crop][order(-pairs)])
cat("\nany non-total crop among them?\n"); print(mp[!grepl("^total-", crop), .N])
