#!/usr/bin/env Rscript
# CR-119 probe: validate the §3.3 column-prune on the REAL aggregation + REAL
# write path BEFORE the long §3.3 rerun. Runs the exact data_anomaly_ens
# aggregation (post-prune) on the smallest real anomaly-historic seasons file,
# writes via the real write_parquet_pushdown, and hard-asserts:
#   - output schema == expected pruned set (no max/min/q5/q50/q95(_anomaly))
#   - q17/q83(_anomaly), n_models present
#   - iso3 present, 0% NA, and row-group stats NON-NULL (prunability + guard)
#   - row count == distinct group count
# Usage (cglabs):  Rscript R/probe_sec3_3_prune.R [path_to_seasons_parquet]
suppressMessages({
  library(data.table); library(arrow); library(duckdb); library(DBI)
})
t0 <- Sys.time()
ts <- function(...) cat(sprintf("[%s] ", format(Sys.time(), "%H:%M:%S")), ..., "\n", sep = "")
el <- function() sprintf("%.1fs", as.numeric(difftime(Sys.time(), t0, units = "secs")))

# real write helper (the part most likely to surprise: iso3 stats verify)
source("R/_helpers.R")
ts("sourced R/_helpers.R")

round3.3 <- 3L
DEFAULT_DIR <- "/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/hazard_timeseries_mean_month"
args <- commandArgs(trailingOnly = TRUE)
in_file <- if (length(args) >= 1) args[[1]] else
  file.path(DEFAULT_DIR, "haz_3months_adm_mean_1995-2014_anomaly-historic_seasons.parquet")
if (!file.exists(in_file)) stop(sprintf("input seasons file not found: %s", in_file))
ts("input:", in_file, sprintf("(%.1f MB)", file.size(in_file) / 1e6))

data_anomaly <- as.data.table(read_parquet(in_file))
ts("read", format(nrow(data_anomaly), big.mark = ","), "rows |", el())
stopifnot("iso3" %in% names(data_anomaly), "value" %in% names(data_anomaly), "anomaly" %in% names(data_anomaly))

# ---- EXACT aggregation copied from R/2.1 §3.3 (post-prune, L733-748) ----
data_anomaly_ens <- data_anomaly[, list(
  mean     = mean(value, na.rm = TRUE),
  sd       = sd(value, na.rm = TRUE),
  q17      = quantile(value, 0.17, na.rm = TRUE),
  q83      = quantile(value, 0.83, na.rm = TRUE),
  n_models = sum(!is.na(value)),
  mean_anomaly = mean(anomaly, na.rm = TRUE),
  sd_anomaly   = sd(anomaly, na.rm = TRUE),
  q17_anomaly  = quantile(anomaly, 0.17, na.rm = TRUE),
  q83_anomaly  = quantile(anomaly, 0.83, na.rm = TRUE)
),
by = c("iso3", "admin0_name", "admin1_name", "scenario", "timeframe", "year", "hazard", "season", "baseline_name")
]
num_cols <- names(data_anomaly_ens)[sapply(data_anomaly_ens, is.numeric)]
data_anomaly_ens[, (num_cols) := lapply(.SD, round, digits = round3.3), .SDcols = num_cols]
ts("aggregated ->", format(nrow(data_anomaly_ens), big.mark = ","), "ensemble rows |", el())

# ---- schema asserts ----
expected <- c("iso3","admin0_name","admin1_name","scenario","timeframe","year","hazard","season","baseline_name",
              "mean","sd","q17","q83","n_models","mean_anomaly","sd_anomaly","q17_anomaly","q83_anomaly")
forbidden <- c("max","min","q5","q50","q95","max_anomaly","min_anomaly","q5_anomaly","q50_anomaly","q95_anomaly","models")
got <- names(data_anomaly_ens)
extra   <- setdiff(got, expected)
missing <- setdiff(expected, got)
hit_forbidden <- intersect(got, forbidden)
if (length(missing)) stop("MISSING expected cols: ", paste(missing, collapse = ", "))
if (length(extra))   stop("UNEXPECTED extra cols: ", paste(extra, collapse = ", "))
if (length(hit_forbidden)) stop("FORBIDDEN cols present: ", paste(hit_forbidden, collapse = ", "))
ts("PASS schema: exactly", length(expected), "cols, no pruned cols present")

# ---- value sanity ----
isoNA <- 100 * mean(is.na(data_anomaly_ens$iso3))
q17NA <- 100 * mean(is.na(data_anomaly_ens$q17))
q83NA <- 100 * mean(is.na(data_anomaly_ens$q83))
ngrp  <- nrow(unique(data_anomaly[, .(iso3, admin0_name, admin1_name, scenario, timeframe, year, hazard, season, baseline_name)]))
ts(sprintf("iso3 NA%%=%.3f  q17 NA%%=%.3f  q83 NA%%=%.3f  rows=%d  distinct-groups=%d",
           isoNA, q17NA, q83NA, nrow(data_anomaly_ens), ngrp))
if (isoNA > 0)            stop(sprintf("iso3 has %.3f%% NA — would break pruning", isoNA))
if (nrow(data_anomaly_ens) != ngrp) stop("row count != distinct group count (dup/collapse bug)")

# ---- REAL write path + prunability guard ----
out <- tempfile(fileext = ".parquet")
write_parquet_pushdown(
  data_anomaly_ens, out,
  sort_by         = c("iso3", "admin0_name", "hazard", "scenario", "season", "year", "timeframe", "admin1_name"),
  verify_stats_on = c("iso3", "admin0_name", "hazard", "scenario", "season")
)
ts("write_parquet_pushdown OK (iso3 stats verified by helper) |", el())

# explicit double-check: iso3 row-group stats non-null
con <- dbConnect(duckdb::duckdb(dbdir = ":memory:"))
md <- dbGetQuery(con, sprintf(
  "SELECT COUNT(*) n, SUM(CASE WHEN stats_min IS NULL OR stats_max IS NULL THEN 1 ELSE 0 END) nullstats
   FROM parquet_metadata('%s') WHERE path_in_schema='iso3'", out))
dbDisconnect(con, shutdown = TRUE)
ts(sprintf("iso3 row-groups=%d  null-stat groups=%d", md$n, md$nullstats))
if (md$nullstats > 0) stop("iso3 has NULL row-group stats — pruning broken")
file.remove(out)

ts("ALL PASS — prune safe to rerun §3.3 |", el())
