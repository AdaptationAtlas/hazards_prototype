#!/usr/bin/env Rscript
# CR-093: publish the 10 R/2.2_haz_change.R outputs to the canonical
# domain=climate keys (same convention as republish_A/publish_B/build_publish_C).
# DRY-RUN by default.
#
# Target layout (one flat variable= object per product):
#   s3://digital-atlas/domain=climate/type=hazard-indices/source=nex-gddp-cmip6/
#     region=africa/processing=hazard-change/timeframe=<axis>/variable=<name>.parquet
#
# Pre-flight per file (hard stop on fail) — the same contract the gate enforces:
#   iso3 column present, iso3 0% NA, iso3 row-group stats NON-NULL (prunable),
#   >=1 row, >=2 distinct iso3.
# On CONFIRM=1: back up any existing S3 object (.preFix-<stamp>.bak, public-read),
# upload local -> canonical key (ACL=public-read), then verify uploaded iso3 stats.
#
# Usage (cglabs, after R/2.2 + R/validate_cr093_real.R pass):
#   Rscript R/publish_cr093_r22.R            # dry-run: checks + prints plan, no upload
#   CONFIRM=1 Rscript R/publish_cr093_r22.R  # back up + upload
#   R22_TIMEFRAME=jagermeyr Rscript R/publish_cr093_r22.R   # other axis
suppressMessages({ library(arrow); library(duckdb); library(DBI) })
ts <- function(...) cat(sprintf("[%s] ", format(Sys.time(), "%H:%M:%S")), ..., "\n", sep = "")

# Paths from 0_server_setup.R; derive the annual-axis dirs as R/2.2 does
# (server_setup only assigns _dir vars for non-timeframe subdirs).
if (!exists("atlas_dirs")) { ts("sourcing 0_server_setup.R for atlas paths"); source("R/0_server_setup.R") }
r22_timeframe     <- Sys.getenv("R22_TIMEFRAME", unset = "annual")
haz_mean_dir      <- file.path(atlas_dirs$data_dir$hazard_timeseries_mean, r22_timeframe)
haz_time_risk_dir <- file.path(atlas_dirs$data_dir$hazard_timeseries_risk, r22_timeframe)
stats_dir         <- file.path(haz_time_risk_dir, "stats")

BASE <- sprintf(paste0("s3://digital-atlas/domain=climate/type=hazard-indices/",
                       "source=nex-gddp-cmip6/region=africa/processing=hazard-change/timeframe=%s"),
                r22_timeframe)
CONFIRM <- nzchar(Sys.getenv("CONFIRM"))
stamp   <- format(Sys.time(), "%Y%m%d-%H%M%S")

# Local product -> canonical variable= leaf name. Stem is the variable= value.
LOCAL <- c(
  file.path(haz_mean_dir, "ptot_perc", "ptot_change_by_model.parquet"),
  file.path(haz_mean_dir, "ptot_perc", "ptot_change_ensemble.parquet"),
  file.path(haz_mean_dir, "ptot_perc", "ptot_diff_by_model.parquet"),
  file.path(haz_mean_dir, "ptot_perc", "ptot_diff_ensemble.parquet"),
  file.path(haz_mean_dir, "thi_perc",  "thi_perc_area_by_model.parquet"),
  file.path(haz_mean_dir, "thi_perc",  "thi_perc_area_ensemble.parquet"),
  file.path(haz_mean_dir, "ntx_perc",  "ntx_perc_area_by_model.parquet"),
  file.path(haz_mean_dir, "ntx_perc",  "ntx_perc_area_ensemble.parquet"),
  file.path(stats_dir,                 "haz_freq.parquet"),
  file.path(stats_dir,                 "haz_freq_ensemble.parquet")
)

# aws binary: cglabs keeps it off PATH
AWS <- Sys.getenv("AWS_BIN", "")
if (!nzchar(AWS)) {
  cand <- c("aws/dist/aws", path.expand("~/atlas/hazards_prototype/aws/dist/aws"), "aws")
  AWS <- cand[which(nchar(Sys.which(cand)) > 0 | file.exists(cand))][1]
  if (is.na(AWS)) AWS <- "aws"
}
ts("mode:", if (CONFIRM) "CONFIRM (will upload)" else "DRY-RUN",
   "| axis:", r22_timeframe, "| aws:", AWS)
ts("BASE:", BASE)

check_file <- function(path) {
  if (!file.exists(path)) stop("missing local file: ", path)
  if (!"iso3" %in% names(open_dataset(path)$schema)) stop(path, " has no iso3 column")
  con <- dbConnect(duckdb::duckdb(dbdir = ":memory:")); on.exit(dbDisconnect(con, shutdown = TRUE))
  agg <- dbGetQuery(con, sprintf(
    "SELECT COUNT(*) n, COUNT(DISTINCT iso3) d,
            100.0*SUM(CASE WHEN iso3 IS NULL THEN 1 ELSE 0 END)/COUNT(*) isona
     FROM read_parquet('%s')", path))
  md <- dbGetQuery(con, sprintf(
    "SELECT COUNT(*) ng, SUM(CASE WHEN stats_min IS NULL OR stats_max IS NULL THEN 1 ELSE 0 END) nn
     FROM parquet_metadata('%s') WHERE path_in_schema='iso3'", path))
  if (agg$n < 1)        stop(path, " 0 rows")
  if (agg$d < 2)        stop(path, sprintf(" only %d distinct iso3", agg$d))
  if (agg$isona > 0)    stop(path, sprintf(" iso3 %.3f%% NULL", agg$isona))
  if (md$ng < 1)        stop(path, " no iso3 column in parquet metadata")
  if (md$nn > 0)        stop(path, sprintf(" iso3 NULL row-group stats in %d/%d groups — not prunable", md$nn, md$ng))
  list(ng = md$ng, d = agg$d, size_mb = file.size(path) / 1e6)
}

aws <- function(args) {
  rc <- system2(AWS, args, stdout = TRUE, stderr = TRUE)
  status <- attr(rc, "status"); if (!is.null(status) && status != 0) stop("aws failed: ", paste(rc, collapse = "\n"))
  rc
}
s3_exists <- function(key) {
  r <- suppressWarnings(system2(AWS, c("s3", "ls", key), stdout = TRUE, stderr = TRUE))
  length(r) > 0 && is.null(attr(r, "status"))
}

ts("=== pre-flight checks (all", length(LOCAL), "files) ===")
plan <- lapply(LOCAL, function(local) {
  stem   <- sub("\\.parquet$", "", basename(local))
  target <- sprintf("%s/variable=%s.parquet", BASE, stem)
  info   <- check_file(local)
  ts(sprintf("  OK %-26s %7.1f MB  rg=%-4d iso3=%d  -> %s", stem, info$size_mb, info$ng, info$d, target))
  list(local = local, target = target, stem = stem)
})
ts("=== pre-flight PASS for all", length(plan), "files ===")

if (!CONFIRM) {
  ts("DRY-RUN complete — re-run with CONFIRM=1 to back up + upload.")
  quit(status = 0)
}

for (p in plan) {
  if (s3_exists(p$target)) {
    bak <- sub("\\.parquet$", sprintf(".preFix-%s.bak", stamp), p$target)
    ts("backup:", basename(p$target), "->", basename(bak))
    aws(c("s3", "cp", p$target, bak, "--acl", "public-read", "--only-show-errors"))
  } else ts("no existing object to back up:", basename(p$target))
  ts("upload:", basename(p$local), "->", p$target)
  aws(c("s3", "cp", p$local, p$target, "--acl", "public-read", "--only-show-errors"))
}

ts("=== verify uploaded iso3 prunability ===")
for (p in plan) {
  con   <- dbConnect(duckdb::duckdb(dbdir = ":memory:"))
  https <- sub("^s3://([^/]+)/", "https://\\1.s3.amazonaws.com/", p$target)
  ok <- tryCatch({
    q <- dbGetQuery(con, sprintf(
      "SELECT COUNT(*) ng, SUM(CASE WHEN stats_min IS NULL OR stats_max IS NULL THEN 1 ELSE 0 END) nn
       FROM parquet_metadata('%s') WHERE path_in_schema='iso3'", https))
    sprintf("rg=%d null-stat=%d", q$ng, q$nn)
  }, error = function(e) paste("verify-skip:", conditionMessage(e)))
  dbDisconnect(con, shutdown = TRUE)
  ts("  ", p$stem, ok)
}
ts("DONE — CR-093 R/2.2 published to canonical hazard-change keys. Verify in a REAL browser next.")
