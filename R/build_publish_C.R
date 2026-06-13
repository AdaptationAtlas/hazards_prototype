#!/usr/bin/env Rscript
# CR-120: build + publish C (per-GCM interannual variability) — ensemble_season_variability.
# Standalone (no §3.4 rerun): computes IAV from EXISTING files — per-model per-year `value`
# from `*_anomaly-historic_seasons.parquet` + per-model `value_slope` from the member
# `*_anomaly-historic_trends.parquet` (the SAME Theil-Sen slope B uses).
#
# Per (GCM x iso3 x admin1 x scenario x season x hazard x period):
#   detrend: resid = value - value_slope*(year - mean(year)) ; iav_sd = sd(resid)
# Baseline window (1995-2014, scenario=historic) computed too (IAV not baseline-invariant).
# Ensemble across GCMs -> long rows:
#   stat=iav_sd    : mean/sd of per-GCM iav_sd (absolute);            pct_gcms_increase = NA
#   stat=iav_delta : mean/sd of per-GCM (iav_sd_future - iav_sd_base); pct = frac GCMs delta>0
# delta computed PER GCM first (future-baseline matched on iso3/admin/model/hazard/season,
# scenario-free baseline), then ensembled — never mean(future)-mean(baseline).
#
# Schema: iso3, admin0_name, admin1_name, scenario, timeframe, season, hazard, stat, mean, sd, pct_gcms_increase
# Keys:   .../period={1995-2014,4 futures}/baseline=1995-2014/variable=ensemble_season_variability.parquet
#
# Usage (cglabs):
#   Rscript R/build_publish_C.R            # DRY-RUN: compute + write to /tmp + assert + plan
#   CONFIRM=1 Rscript R/build_publish_C.R  # upload (public-read)
suppressMessages({ library(arrow); library(data.table); library(duckdb); library(DBI) })
t0 <- Sys.time()
ts <- function(...) cat(sprintf("[%s] ", format(Sys.time(), "%H:%M:%S")), ..., "\n", sep = "")

DIR  <- "/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/hazard_timeseries_mean_month"
BASE <- "s3://digital-atlas/domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=timeseries_mean_month/timeframe=3months"
BASELINE_PERIOD <- "1995-2014"
FUTURES <- c("2021-2040", "2041-2060", "2061-2080", "2081-2100")
BASELINE_KEY <- "1995-2014"
VARIABLE <- "ensemble_season_variability"
# member-level grouping (one Theil-Sen fit per GCM per series)
MKEYS <- c("iso3","admin0_name","admin1_name","scenario","model","timeframe","hazard","season")
# keys to match a future GCM-series to its baseline GCM-series (scenario- & timeframe-free)
XKEYS <- c("iso3","admin0_name","admin1_name","model","hazard","season")
# ensemble grouping (drop model)
EKEYS <- c("iso3","admin0_name","admin1_name","scenario","timeframe","season","hazard")
CONFIRM <- nzchar(Sys.getenv("CONFIRM"))
stamp <- format(Sys.time(), "%Y%m%d-%H%M%S")

source(file.path(Sys.getenv("project_dir", getwd()), "R", "_helpers.R"))

AWS <- Sys.getenv("AWS_BIN", "")
if (!nzchar(AWS)) {
  cand <- c("aws/dist/aws", path.expand("~/atlas/hazards_prototype/aws/dist/aws"), "aws")
  hit <- cand[file.exists(cand) | nchar(Sys.which(cand)) > 0]
  AWS <- if (length(hit)) hit[1] else "aws"
}
aws <- function(args) {
  rc <- system2(AWS, args, stdout = TRUE, stderr = TRUE)
  st <- attr(rc, "status"); if (!is.null(st) && st != 0) stop("aws failed: ", paste(rc, collapse = "\n"))
  rc
}
s3_exists <- function(key) {
  r <- suppressWarnings(system2(AWS, c("s3", "ls", key), stdout = TRUE, stderr = TRUE))
  length(r) > 0 && is.null(attr(r, "status"))
}
ts("mode:", if (CONFIRM) "CONFIRM" else "DRY-RUN", "| aws:", AWS, "| variable:", VARIABLE)

# --- per-GCM iav_sd for one period: detrend value by its stored Theil-Sen slope, sd of residual ---
compute_iav <- function(period) {
  sfile <- file.path(DIR, sprintf("haz_3months_adm_mean_%s_anomaly-historic_seasons.parquet", period))
  tfile <- file.path(DIR, sprintf("haz_3months_adm_mean_%s_anomaly-historic_trends.parquet", period))
  if (!file.exists(sfile)) stop("missing seasons: ", sfile)
  if (!file.exists(tfile)) stop("missing member trends: ", tfile)
  s <- as.data.table(read_parquet(sfile, col_select = c(MKEYS, "year", "value")))
  tr <- as.data.table(read_parquet(tfile, col_select = c(MKEYS, "value_slope")))
  tr <- unique(tr, by = MKEYS)                      # one slope per GCM-series
  s[tr, value_slope := i.value_slope, on = MKEYS]
  # detrend with the stored slope (intercept irrelevant to sd); centre year within series
  s[, resid := value - value_slope * (year - mean(year)), by = MKEYS]
  iav <- s[, .(iav_sd = sd(resid, na.rm = TRUE)), by = MKEYS]   # GForce sd, fast
  ts(sprintf("  iav %-10s: %d GCM-series  (slope-NA series=%d)",
             period, nrow(iav), tr[is.na(value_slope), .N]))
  iav
}

ts("=== baseline IAV (", BASELINE_PERIOD, ") ===")
base_iav <- compute_iav(BASELINE_PERIOD)
# baseline is scenario=historic; key it scenario-free for the per-GCM delta match
base_x <- base_iav[, .(iav_sd_base = mean(iav_sd, na.rm = TRUE)), by = XKEYS]  # collapse any scenario dup

tmpdir <- file.path(tempdir(), "publishC"); dir.create(tmpdir, showWarnings = FALSE)
plan <- list()

# --- baseline file: absolute iav_sd only (no delta) ---
base_ens <- base_iav[, .(mean = mean(iav_sd, na.rm = TRUE), sd = sd(iav_sd, na.rm = TRUE)),
                     by = EKEYS][, `:=`(stat = "iav_sd", pct_gcms_increase = NA_real_)]
setcolorder(base_ens, c(EKEYS, "stat", "mean", "sd", "pct_gcms_increase"))
outb <- file.path(tmpdir, sprintf("%s_%s.parquet", VARIABLE, BASELINE_PERIOD))
write_parquet_pushdown(base_ens, outb,
  sort_by = c("iso3","admin0_name","admin1_name","hazard","scenario","season","stat"),
  verify_stats_on = c("iso3","admin0_name","hazard","scenario"))
plan[[BASELINE_PERIOD]] <- list(P = BASELINE_PERIOD, out = outb,
  target = sprintf("%s/period=%s/baseline=%s/variable=%s.parquet", BASE, BASELINE_PERIOD, BASELINE_KEY, VARIABLE))
ts(sprintf("  baseline file: %d rows -> %.1f MB", nrow(base_ens), file.size(outb)/1e6))

# --- futures: iav_sd + iav_delta + pct_gcms_increase ---
for (P in FUTURES) {
  ts("=== future ", P, " ===")
  fut <- compute_iav(P)
  fut[base_x, iav_sd_base := i.iav_sd_base, on = XKEYS]   # match GCM to its baseline series
  fut[, delta := iav_sd - iav_sd_base]                    # per-GCM change
  ens <- fut[, .(
    iav_sd_mean    = mean(iav_sd, na.rm = TRUE),
    iav_sd_sd      = sd(iav_sd, na.rm = TRUE),
    iav_delta_mean = mean(delta, na.rm = TRUE),
    iav_delta_sd   = sd(delta, na.rm = TRUE),
    pct_increase   = mean(delta > 0, na.rm = TRUE)
  ), by = EKEYS]
  long <- rbindlist(list(
    ens[, .(iso3,admin0_name,admin1_name,scenario,timeframe,season,hazard,
            stat = "iav_sd",    mean = iav_sd_mean,    sd = iav_sd_sd,    pct_gcms_increase = NA_real_)],
    ens[, .(iso3,admin0_name,admin1_name,scenario,timeframe,season,hazard,
            stat = "iav_delta", mean = iav_delta_mean, sd = iav_delta_sd, pct_gcms_increase = pct_increase)]
  ))
  out <- file.path(tmpdir, sprintf("%s_%s.parquet", VARIABLE, P))
  write_parquet_pushdown(long, out,
    sort_by = c("iso3","admin0_name","admin1_name","hazard","scenario","season","stat"),
    verify_stats_on = c("iso3","admin0_name","hazard","scenario"))
  plan[[P]] <- list(P = P, out = out,
    target = sprintf("%s/period=%s/baseline=%s/variable=%s.parquet", BASE, P, BASELINE_KEY, VARIABLE))
  ts(sprintf("  %s: %d rows -> %.1f MB | pct_increase range %.2f..%.2f",
             P, nrow(long), file.size(out)/1e6,
             min(ens$pct_increase, na.rm = TRUE), max(ens$pct_increase, na.rm = TRUE)))
}

# --- asserts ---
chk <- as.data.table(read_parquet(plan[[FUTURES[1]]]$out))
stopifnot(
  "iso3" %in% names(chk),
  setequal(unique(chk$stat), c("iav_sd","iav_delta")),
  100*mean(is.na(chk$iso3)) == 0,
  all(is.na(chk[stat=="iav_sd", pct_gcms_increase])),          # pct only on delta
  all(!is.na(chk[stat=="iav_delta", pct_gcms_increase])),
  all(chk[stat=="iav_delta", pct_gcms_increase] >= 0 & chk[stat=="iav_delta", pct_gcms_increase] <= 1)
)
bchk <- as.data.table(read_parquet(plan[[BASELINE_PERIOD]]$out))
stopifnot(setequal(unique(bchk$stat), "iav_sd"))               # baseline = iav_sd only
ts("=== asserts PASS; files in", tmpdir, "===")

if (!CONFIRM) { ts("DRY-RUN complete — re-run with CONFIRM=1 to upload | total", sprintf("%.0fs", as.numeric(difftime(Sys.time(),t0,units="secs")))); quit(status = 0) }

for (p in plan) {
  if (s3_exists(p$target)) {
    bak <- sub("\\.parquet$", sprintf(".preFix-%s.bak", stamp), p$target)
    ts("backup:", basename(p$target)); aws(c("s3","cp", p$target, bak, "--acl","public-read","--only-show-errors"))
  } else ts("new key:", p$target)
  ts("upload:", basename(p$out)); aws(c("s3","cp", p$out, p$target, "--acl","public-read","--only-show-errors"))
}
ts("=== verify uploaded iso3 prunability ===")
for (p in plan) {
  con <- dbConnect(duckdb::duckdb(dbdir=":memory:"))
  https <- sub("^s3://([^/]+)/", "https://\\1.s3.amazonaws.com/", p$target)
  msg <- tryCatch({ q <- dbGetQuery(con, sprintf("SELECT COUNT(*) ng, SUM(CASE WHEN stats_min IS NULL OR stats_max IS NULL THEN 1 ELSE 0 END) nn FROM parquet_metadata('%s') WHERE path_in_schema='iso3'", https)); sprintf("rg=%d null-stat=%d", q$ng, q$nn) }, error = function(e) paste("verify-skip:", conditionMessage(e)))
  dbDisconnect(con, shutdown = TRUE); ts("  ", p$P, msg)
}
ts("DONE — C published to", length(plan), "keys | total", sprintf("%.0fs", as.numeric(difftime(Sys.time(),t0,units="secs"))))
