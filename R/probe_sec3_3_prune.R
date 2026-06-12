#!/usr/bin/env Rscript
# CR-119 probe: validate the §3.3 prune + Rcpp quantile kernel on REAL data
# BEFORE the long §3.3 rerun.
#   1. compile R/quantile_kernel.cpp (the real kernel)
#   2. run the FAST kernel path on the smallest real anomaly-historic seasons
#      file (sort -> .grp -> ens_stats_cpp -> cbind keys); time it
#   3. assert output schema == pruned set (no max/min/q5/q50/q95(_anomaly))
#   4. assert iso3 0% NA, rows == distinct-groups
#   5. real write_parquet_pushdown + iso3 non-null row-group stats
#   6. REAL-DATA equivalence: on a small subset, kernel == stats::quantile path
# Usage (cglabs):  Rscript R/probe_sec3_3_prune.R [path_to_seasons_parquet]
suppressMessages({
  library(data.table); library(arrow); library(duckdb); library(DBI); library(Rcpp)
})
t0 <- Sys.time()
ts <- function(...) cat(sprintf("[%s] ", format(Sys.time(), "%H:%M:%S")), ..., "\n", sep = "")
el <- function() sprintf("%.1fs", as.numeric(difftime(Sys.time(), t0, units = "secs")))

source("R/_helpers.R")
sourceCpp("R/quantile_kernel.cpp")
stopifnot(exists("ens_stats_cpp"))
ts("compiled kernel + sourced helpers")

round3.3 <- 3L
by_cols <- c("iso3","admin0_name","admin1_name","scenario","timeframe","year","hazard","season","baseline_name")
DEFAULT_DIR <- "/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/hazard_timeseries_mean_month"
args <- commandArgs(trailingOnly = TRUE)
in_file <- if (length(args) >= 1) args[[1]] else
  file.path(DEFAULT_DIR, "haz_3months_adm_mean_1995-2014_anomaly-historic_seasons.parquet")
if (!file.exists(in_file)) stop(sprintf("input seasons file not found: %s", in_file))
ts("input:", in_file, sprintf("(%.1f MB)", file.size(in_file) / 1e6))

data_anomaly <- as.data.table(read_parquet(in_file))
ts("read", format(nrow(data_anomaly), big.mark = ","), "rows |", el())
stopifnot("iso3" %in% names(data_anomaly), "value" %in% names(data_anomaly), "anomaly" %in% names(data_anomaly))

# ---- FAST kernel path (exactly as in R/2.1 §3.3) ----
tk <- Sys.time()
setorderv(data_anomaly, by_cols)
data_anomaly[, .grp := .GRP, by = by_cols]
keys <- unique(data_anomaly[, ..by_cols])
ens_stats <- ens_stats_cpp(data_anomaly$value, data_anomaly$anomaly, data_anomaly$.grp, nrow(keys))
data_anomaly_ens <- cbind(keys, as.data.table(ens_stats))
num_cols <- names(data_anomaly_ens)[sapply(data_anomaly_ens, is.numeric)]
data_anomaly_ens[, (num_cols) := lapply(.SD, round, digits = round3.3), .SDcols = num_cols]
ts(sprintf("KERNEL path -> %s ensemble rows in %.1fs | total %s",
           format(nrow(data_anomaly_ens), big.mark = ","),
           as.numeric(difftime(Sys.time(), tk, units = "secs")), el()))

# ---- schema asserts ----
expected <- c("iso3","admin0_name","admin1_name","scenario","timeframe","year","hazard","season","baseline_name",
              "mean","sd","q17","q83","n_models","mean_anomaly","sd_anomaly","q17_anomaly","q83_anomaly")
forbidden <- c("max","min","q5","q50","q95","max_anomaly","min_anomaly","q5_anomaly","q50_anomaly","q95_anomaly","models")
got <- names(data_anomaly_ens)
if (length(setdiff(expected, got))) stop("MISSING cols: ", paste(setdiff(expected, got), collapse = ", "))
if (length(setdiff(got, expected))) stop("UNEXPECTED cols: ", paste(setdiff(got, expected), collapse = ", "))
if (length(intersect(got, forbidden))) stop("FORBIDDEN cols: ", paste(intersect(got, forbidden), collapse = ", "))
ts("PASS schema: exactly", length(expected), "cols, no pruned cols")

isoNA <- 100 * mean(is.na(data_anomaly_ens$iso3))
ts(sprintf("iso3 NA%%=%.3f  q17 NA%%=%.3f  rows=%d  distinct-groups=%d",
           isoNA, 100 * mean(is.na(data_anomaly_ens$q17)), nrow(data_anomaly_ens), nrow(keys)))
if (isoNA > 0) stop(sprintf("iso3 %.3f%% NA — breaks pruning", isoNA))
if (nrow(data_anomaly_ens) != nrow(keys)) stop("row count != distinct groups")

# ---- real write + prunability ----
out <- tempfile(fileext = ".parquet")
write_parquet_pushdown(
  data_anomaly_ens, out,
  sort_by         = c("iso3", "admin0_name", "hazard", "scenario", "season", "year", "timeframe", "admin1_name"),
  verify_stats_on = c("iso3", "admin0_name", "hazard", "scenario", "season")
)
con <- dbConnect(duckdb::duckdb(dbdir = ":memory:"))
md <- dbGetQuery(con, sprintf(
  "SELECT COUNT(*) n, SUM(CASE WHEN stats_min IS NULL OR stats_max IS NULL THEN 1 ELSE 0 END) nullstats
   FROM parquet_metadata('%s') WHERE path_in_schema='iso3'", out))
dbDisconnect(con, shutdown = TRUE); file.remove(out)
ts(sprintf("write OK | iso3 row-groups=%d null-stat=%d", md$n, md$nullstats))
if (md$nullstats > 0) stop("iso3 NULL row-group stats — pruning broken")

# ---- REAL-DATA equivalence: kernel == stats::quantile on a small subset ----
s1 <- data_anomaly$scenario[1]; h1 <- data_anomaly$hazard[1]
sub <- data_anomaly[scenario == s1 & hazard == h1]
setorderv(sub, by_cols); sub[, .grp := .GRP, by = by_cols]
ksub <- cbind(unique(sub[, ..by_cols]),
              as.data.table(ens_stats_cpp(sub$value, sub$anomaly, sub$.grp, sub[, uniqueN(.grp)])))
qsub <- sub[, list(
  mean=mean(value,na.rm=TRUE), sd=sd(value,na.rm=TRUE),
  q17=quantile(value,0.17,na.rm=TRUE), q83=quantile(value,0.83,na.rm=TRUE),
  n_models=sum(!is.na(value)),
  mean_anomaly=mean(anomaly,na.rm=TRUE), sd_anomaly=sd(anomaly,na.rm=TRUE),
  q17_anomaly=quantile(anomaly,0.17,na.rm=TRUE), q83_anomaly=quantile(anomaly,0.83,na.rm=TRUE)
), by = by_cols]
setkeyv(ksub, by_cols); setkeyv(qsub, by_cols); m <- ksub[qsub]
worst <- 0; patt_ok <- TRUE
for (c in c("mean","sd","q17","q83","mean_anomaly","sd_anomaly","q17_anomaly","q83_anomaly")) {
  x <- m[[c]]; y <- m[[paste0("i.", c)]]
  fin <- is.finite(x) & is.finite(y)                      # skip NA/NaN/Inf (Inf-Inf=NaN)
  d <- if (any(fin)) max(abs(x[fin] - y[fin])) else 0
  worst <- max(worst, d)
  patt_ok <- patt_ok && identical(which(!is.finite(x)), which(!is.finite(y)))  # non-finite align
}
nmok <- all(m$n_models == m[["i.n_models"]])
ts(sprintf("EQUIV subset (%s/%s, %d groups): max|kernel-quantile|(finite)=%.2e  non-finite-match=%s  n_models exact=%s",
           s1, h1, nrow(ksub), worst, patt_ok, nmok))
if (worst > 1e-6 || !nmok || !patt_ok) stop("kernel != stats::quantile on real subset")

ts("ALL PASS — kernel + prune safe to rerun §3.3 |", el())
