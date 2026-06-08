# Synthetic probe: confirm iso3 survives the sec 3.3 ensemble aggregation chain.
# Mirrors R/2.1_create_monthly_haz_tables.R lines 767-791 exactly (data_ag -> data_ag_ens).
# No pipeline deps, no file IO. Runs in <1s. Exit 0 = iso3 present in both outputs.
suppressMessages(library(data.table))

round3.3 <- 2

# Tiny data_anomaly with the same column set the live log printed (line 140):
# iso3,admin0_name,admin1_name,hazard,season,scenario,model,timeframe,year,
# suspect_value_flag,value,n_value,baseline_value,baseline_name,anomaly
set.seed(1)
grid <- CJ(
  iso3        = c("AGO", "KEN"),
  admin1_name = c("R1", "R2"),
  scenario    = c("ssp245", "ssp585"),
  model       = c("m1", "m2", "m3"),
  hazard      = c("PTOT", "TMAX"),
  season      = c("DJF", "annual"),
  timeframe   = "2030s",
  year        = 2021:2025
)
grid[, admin0_name   := fifelse(iso3 == "AGO", "Angola", "Kenya")]
grid[, baseline_name := "1995-2014"]
grid[, value         := rnorm(.N, 100, 10)]
grid[, anomaly       := rnorm(.N, 0, 2)]
grid[, baseline_value := 100]
data_anomaly <- grid

stopifnot("iso3" %in% names(data_anomaly))

# --- sec 3.3 block (verbatim by-clauses from script lines 767-791) ---
data_ag <- data_anomaly[, list(
  mean         = mean(value, na.rm = TRUE),
  mean_anomaly = mean(anomaly, na.rm = TRUE)
),
by = c("iso3", "admin0_name", "admin1_name", "scenario", "timeframe", "model", "hazard", "season", "baseline_name")
]

cat("data_ag cols:", paste(names(data_ag), collapse = ","), "\n")
if (!"iso3" %in% names(data_ag)) stop("FAIL: iso3 dropped from data_ag")

data_ag_ens <- data_ag[, list(
  mean_mean    = mean(mean, na.rm = TRUE),
  min_mean     = min(mean, na.rm = TRUE),
  max_mean     = max(mean, na.rm = TRUE),
  median_mean  = median(mean, na.rm = TRUE),
  mean_anomaly = mean(mean_anomaly, na.rm = TRUE),
  max_anomaly  = max(mean_anomaly, na.rm = TRUE),
  min_anomaly  = min(mean_anomaly, na.rm = TRUE),
  sd_anomaly   = sd(mean_anomaly, na.rm = TRUE),
  q17_anomaly  = quantile(mean_anomaly, 0.17, na.rm = TRUE),
  q83_anomaly  = quantile(mean_anomaly, 0.83, na.rm = TRUE),
  n_models     = sum(!is.na(mean_anomaly))
),
by = c("iso3", "admin0_name", "admin1_name", "scenario", "timeframe", "hazard", "season", "baseline_name")
]

cat("data_ag_ens cols:", paste(names(data_ag_ens), collapse = ","), "\n")
if (!"iso3" %in% names(data_ag_ens)) stop("FAIL: iso3 dropped from data_ag_ens")

# iso3 should be 1:1 with admin0_name (no cross-contamination from the regroup)
chk <- unique(data_ag_ens[, .(iso3, admin0_name)])
if (nrow(chk) != uniqueN(data_ag_ens$admin0_name)) stop("FAIL: iso3<->admin0 mapping broke")
print(chk)

cat("PASS: iso3 present in data_ag and data_ag_ens; mapping intact\n")
