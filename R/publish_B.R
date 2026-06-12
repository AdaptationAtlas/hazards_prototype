#!/usr/bin/env Rscript
# CR-119 / CR-117: publish B (slim trends ensemble) to canonical domain=climate keys.
# Source = §3.4 anomaly-historic *_trends_ensemble.parquet (fresh, iso3-bearing).
# Ships TREND MAGNITUDE only: stat in {value_slope, value_decade}, keep
# iso3/admin/scenario/timeframe/season/hazard/stat + mean (ensemble mean) + sd
# (across-GCM spread). 4 future periods, baseline=1995-2014. DRY-RUN by default.
#
# value_slope is fit per-GCM (fit_keys incl. model, L1104) THEN ensembled
# (§3.7.1 mean/sd across models) — ensembling-is-last, NOT slope-of-the-mean.
#
# SIGNIFICANCE DEFERRED (notebook dispatch 2026-06-12): mean-of-pvals across GCMs
# is statistically weak — value_pval is NOT published. The significance layer needs
# an AR6-style agreement metric (pct_gcms_sig = frac GCMs with sig slope; pct_sign_pos
# = frac slope>0), computed per-GCM in the §3.4 producer (§3.7.1). Tracked for CR-117,
# to land when the metric is settled alongside the sandbox trend-map prototype.
#
# Trend metrics are baseline-invariant (anomaly = value - const) => one variant
# (anomaly-historic) suffices. Not consumed by climateRationale; for CR-117 trend maps.
#
# Usage (cglabs):
#   Rscript R/publish_B.R            # dry-run: checks + plan, writes slim files to /tmp, no upload
#   CONFIRM=1 Rscript R/publish_B.R  # back up any existing key + upload (public-read)
suppressMessages({ library(arrow); library(data.table); library(duckdb); library(DBI) })
ts <- function(...) cat(sprintf("[%s] ", format(Sys.time(), "%H:%M:%S")), ..., "\n", sep = "")

DIR  <- "/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/hazard_timeseries_mean_month"
BASE <- "s3://digital-atlas/domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=timeseries_mean_month/timeframe=3months"
PERIODS <- c("2021-2040", "2041-2060", "2061-2080", "2081-2100")  # 4 futures
BASELINE_KEY <- "1995-2014"
KEEP_STATS <- c("value_slope", "value_decade")   # value_pval deferred — see header (mean-pval weak)
KEEP_COLS  <- c("iso3","admin0_name","admin1_name","scenario","timeframe","season","hazard","stat","mean","sd")
VARIABLE   <- "ensemble_season_trends"
CONFIRM <- nzchar(Sys.getenv("CONFIRM"))
stamp <- format(Sys.time(), "%Y%m%d-%H%M%S")

source(file.path(Sys.getenv("project_dir", getwd()), "R", "_helpers.R"))

AWS <- Sys.getenv("AWS_BIN", "")
if (!nzchar(AWS)) {
  cand <- c("aws/dist/aws", path.expand("~/atlas/hazards_prototype/aws/dist/aws"), "aws")
  hit <- cand[file.exists(cand) | nchar(Sys.which(cand)) > 0]
  AWS <- if (length(hit)) hit[1] else "aws"
}
ts("mode:", if (CONFIRM) "CONFIRM (will upload)" else "DRY-RUN", "| aws:", AWS, "| variable:", VARIABLE)

aws <- function(args) {
  rc <- system2(AWS, args, stdout = TRUE, stderr = TRUE)
  st <- attr(rc, "status"); if (!is.null(st) && st != 0) stop("aws failed: ", paste(rc, collapse = "\n"))
  rc
}
s3_exists <- function(key) {
  r <- suppressWarnings(system2(AWS, c("s3", "ls", key), stdout = TRUE, stderr = TRUE))
  length(r) > 0 && is.null(attr(r, "status"))
}

tmpdir <- file.path(tempdir(), "publishB"); dir.create(tmpdir, showWarnings = FALSE)

ts("=== pre-flight + slim (4 files) ===")
plan <- lapply(PERIODS, function(P) {
  local  <- file.path(DIR, sprintf("haz_3months_adm_mean_%s_anomaly-historic_trends_ensemble.parquet", P))
  target <- sprintf("%s/period=%s/baseline=%s/variable=%s.parquet", BASE, P, BASELINE_KEY, VARIABLE)
  if (!file.exists(local)) stop("missing local trends_ensemble: ", local)
  dt <- as.data.table(read_parquet(local))
  if (!"iso3" %in% names(dt)) stop(local, " has no iso3 column (stale pre-CR-119 file?)")
  if (!"stat" %in% names(dt)) stop(local, " has no stat column")
  miss <- setdiff(KEEP_STATS, unique(dt$stat))
  if (length(miss)) stop(local, " missing stats: ", paste(miss, collapse = ", "))
  slim <- dt[stat %in% KEEP_STATS, ..KEEP_COLS]
  isona <- 100 * mean(is.na(slim$iso3))
  if (isona > 0) stop(local, sprintf(" iso3 %.3f%% NULL", isona))
  out <- file.path(tmpdir, sprintf("%s_%s.parquet", VARIABLE, P))
  write_parquet_pushdown(
    slim, out,
    sort_by         = c("iso3", "admin0_name", "admin1_name", "hazard", "scenario", "season", "stat"),
    verify_stats_on = c("iso3", "admin0_name", "hazard", "scenario")
  )
  ts(sprintf("  OK %-10s %d rows -> %.1f MB slim  -> %s", P, nrow(slim), file.size(out)/1e6, target))
  list(P = P, slim = out, target = target)
})
ts("=== pre-flight PASS; slim files written to", tmpdir, "===")

if (!CONFIRM) { ts("DRY-RUN complete — re-run with CONFIRM=1 to upload."); quit(status = 0) }

for (p in plan) {
  if (s3_exists(p$target)) {
    bak <- sub("\\.parquet$", sprintf(".preFix-%s.bak", stamp), p$target)
    ts("backup:", basename(p$target), "->", basename(bak))
    aws(c("s3", "cp", p$target, bak, "--acl", "public-read", "--only-show-errors"))
  } else ts("new key (no backup):", p$target)
  ts("upload:", basename(p$slim), "->", p$target)
  aws(c("s3", "cp", p$slim, p$target, "--acl", "public-read", "--only-show-errors"))
}

ts("=== verify uploaded iso3 prunability ===")
for (p in plan) {
  con <- dbConnect(duckdb::duckdb(dbdir = ":memory:"))
  https <- sub("^s3://([^/]+)/", "https://\\1.s3.amazonaws.com/", p$target)
  msg <- tryCatch({
    q <- dbGetQuery(con, sprintf(
      "SELECT COUNT(*) ng, SUM(CASE WHEN stats_min IS NULL OR stats_max IS NULL THEN 1 ELSE 0 END) nn
       FROM parquet_metadata('%s') WHERE path_in_schema='iso3'", https))
    sprintf("rg=%d null-stat=%d", q$ng, q$nn)
  }, error = function(e) paste("verify-skip:", conditionMessage(e)))
  dbDisconnect(con, shutdown = TRUE)
  ts("  ", p$P, msg)
}
ts("DONE — B (slim trends ensemble) published to", length(plan), "future keys.")
