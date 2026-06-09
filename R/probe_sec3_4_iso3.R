# Probe: iso3 survives the §3.4 trend-stats + ensemble aggregations (CR-119 in §3.4).
# Mirrors R/2.1 lines ~1151-1257 verbatim (by-clauses incl iso3). Exit 0 = iso3 present
# in both the per-model trends table and the ensemble table, 1:1 with admin0_name.
suppressMessages(library(data.table))
round3.4 <- 3

# data_ex_trend_m-like input: has iso3 (from §3.2 seasons file) + per-model trend cols
set.seed(3)
d <- CJ(iso3 = c("AGO", "KEN"), admin1_name = c("R1", "R2"),
        scenario = c("ssp245", "ssp585"), model = c("m1", "m2", "m3"),
        timeframe = "2030s", hazard = c("PTOT", "TMAX"), season = c("DJF", "annual"),
        year = 2021:2030)
d[, admin0_name := fifelse(iso3 == "AGO", "Angola", "Kenya")]
d[, baseline_name := "1995-2014"]
d[, value := rnorm(.N, 100, 5)][, anomaly := rnorm(.N, 0, 2)]
d[, slope := 0.3][, intercept := 90][, p_value := 0.04]

# --- §3.4.2 per-model trend stats (with CR-119 iso3 in by-clause) ---
stats <- d[, .(
  value_slope = slope[1], value_start = min(year) * slope[1] + intercept[1],
  value_s5 = mean(value[1:5]), anomaly_s5 = mean(anomaly[1:5]),
  value_end = max(year) * slope[1] + intercept[1], value_e5 = mean(tail(value, 5)),
  anomaly_e5 = mean(tail(anomaly, 5)), value_decade = 10 * slope, value_pval = p_value[1]
), by = .(iso3, admin0_name, admin1_name, scenario, model, timeframe, hazard, season, baseline_name)
][, value_diff := value_e5 - value_s5][, anomaly_diff := anomaly_e5 - anomaly_s5]

stopifnot("iso3 in per-model trends" = "iso3" %in% names(stats))
cat("per-model trends cols:", paste(names(stats), collapse = ","), "\n")

# --- melt (id.vals ignored → auto-detect keeps char iso3 as id) ---
ens <- melt(stats, id.vals = c("admin0_name","admin1_name","scenario","model","timeframe","variable","season"),
            variable.name = "stat")
stopifnot("iso3 survives melt" = "iso3" %in% names(ens))

# --- §3.7.1 ensemble across models (with CR-119 iso3 in by-clause) ---
ens <- ens[, list(mean = mean(value, na.rm = TRUE), max = max(value, na.rm = TRUE),
                  min = min(value, na.rm = TRUE), sd = sd(value, na.rm = TRUE)),
           by = list(iso3, admin0_name, admin1_name, scenario, timeframe, season, hazard, stat)]
stopifnot("iso3 in ensemble trends" = "iso3" %in% names(ens))
cat("ensemble trends cols:", paste(names(ens), collapse = ","), "\n")

# mapping intact (iso3 1:1 admin0)
chk <- unique(ens[, .(iso3, admin0_name)])
stopifnot("iso3<->admin0 1:1" = nrow(chk) == uniqueN(ens$admin0_name))
print(chk)
cat("PASS: iso3 present in §3.4 per-model trends AND ensemble trends; mapping intact\n")
