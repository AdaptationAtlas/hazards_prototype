#!/usr/bin/env Rscript
# CR-119: republish A (ensemble_season_timeseries) to the 5 canonical
# domain=climate keys the climateRationale notebook reads. DRY-RUN by default.
#
# Pre-flight per file (hard stop on fail): exact pruned schema (iso3 + q17/q83,
# NO max/min/q5/q50/q95), iso3 0% NA, iso3 row-group stats NON-NULL (prunable).
# On CONFIRM=1: back up the existing S3 object (.preFix-<stamp>.bak, public-read)
# then upload local -> canonical key with ACL=public-read; verify uploaded iso3 stats.
#
# Usage (cglabs):
#   Rscript R/republish_A.R            # dry-run: checks + prints plan, no upload
#   CONFIRM=1 Rscript R/republish_A.R  # back up + upload
suppressMessages({ library(arrow); library(duckdb); library(DBI) })
ts <- function(...) cat(sprintf("[%s] ", format(Sys.time(), "%H:%M:%S")), ..., "\n", sep = "")

DIR  <- "/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/hazard_timeseries_mean_month"
BASE <- "s3://digital-atlas/domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=timeseries_mean_month/timeframe=3months"
PERIODS <- c("1995-2014", "2021-2040", "2041-2060", "2061-2080", "2081-2100")  # historic + 4 futures
BASELINE_KEY <- "1995-2014"   # anomaly-historic baseline == 1995-2014
CONFIRM <- nzchar(Sys.getenv("CONFIRM"))
stamp <- format(Sys.time(), "%Y%m%d-%H%M%S")

# aws binary: cglabs keeps it off PATH
AWS <- Sys.getenv("AWS_BIN", "")
if (!nzchar(AWS)) {
  cand <- c("aws/dist/aws", path.expand("~/atlas/hazards_prototype/aws/dist/aws"), "aws")
  AWS <- cand[which(nchar(Sys.which(cand)) > 0 | file.exists(cand))][1]
  if (is.na(AWS)) AWS <- "aws"
}
ts("mode:", if (CONFIRM) "CONFIRM (will upload)" else "DRY-RUN", "| aws:", AWS)

EXPECTED <- c("iso3","admin0_name","admin1_name","scenario","timeframe","year","hazard","season","baseline_name",
              "mean","sd","q17","q83","n_models","mean_anomaly","sd_anomaly","q17_anomaly","q83_anomaly")
FORBIDDEN <- c("max","min","q5","q50","q95","max_anomaly","min_anomaly","q5_anomaly","q50_anomaly","q95_anomaly","models")

check_file <- function(path) {
  if (!file.exists(path)) stop("missing local file: ", path)
  sch <- names(open_dataset(path)$schema)
  if (length(setdiff(EXPECTED, sch))) stop(path, " MISSING cols: ", paste(setdiff(EXPECTED, sch), collapse=", "))
  if (length(setdiff(sch, EXPECTED))) stop(path, " UNEXPECTED cols: ", paste(setdiff(sch, EXPECTED), collapse=", "))
  if (length(intersect(sch, FORBIDDEN))) stop(path, " FORBIDDEN cols: ", paste(intersect(sch, FORBIDDEN), collapse=", "))
  con <- dbConnect(duckdb::duckdb(dbdir=":memory:")); on.exit(dbDisconnect(con, shutdown=TRUE))
  q <- dbGetQuery(con, sprintf(
    "SELECT COUNT(*) ng, SUM(CASE WHEN stats_min IS NULL OR stats_max IS NULL THEN 1 ELSE 0 END) nn
     FROM parquet_metadata('%s') WHERE path_in_schema='iso3'", path))
  isona <- dbGetQuery(con, sprintf(
    "SELECT 100.0*SUM(CASE WHEN iso3 IS NULL THEN 1 ELSE 0 END)/COUNT(*) p FROM read_parquet('%s')", path))$p
  if (q$ng < 1) stop(path, " no iso3 column in parquet metadata")
  if (q$nn > 0) stop(path, sprintf(" iso3 NULL row-group stats in %d/%d groups — not prunable", q$nn, q$ng))
  if (isona > 0) stop(path, sprintf(" iso3 %.3f%% NULL", isona))
  list(ng = q$ng, isona = isona, size_mb = file.size(path)/1e6)
}

aws <- function(args) {
  rc <- system2(AWS, args, stdout = TRUE, stderr = TRUE)
  status <- attr(rc, "status"); if (!is.null(status) && status != 0) stop("aws failed: ", paste(rc, collapse="\n"))
  rc
}
s3_exists <- function(key) {
  r <- suppressWarnings(system2(AWS, c("s3", "ls", key), stdout = TRUE, stderr = TRUE))
  length(r) > 0 && is.null(attr(r, "status"))
}

ts("=== pre-flight checks (all 5 files) ===")
plan <- lapply(PERIODS, function(P) {
  local  <- file.path(DIR, sprintf("haz_3months_adm_mean_%s_anomaly-historic_ensemble_seasons.parquet", P))
  target <- sprintf("%s/period=%s/baseline=%s/variable=ensemble_season_timeseries.parquet", BASE, P, BASELINE_KEY)
  info <- check_file(local)
  ts(sprintf("  OK %-10s %.1f MB  rg=%d  -> %s", P, info$size_mb, info$ng, target))
  list(P = P, local = local, target = target)
})
ts("=== pre-flight PASS for all", length(plan), "files ===")

if (!CONFIRM) {
  ts("DRY-RUN complete — re-run with CONFIRM=1 to back up + upload.")
  quit(status = 0)
}

for (p in plan) {
  if (s3_exists(p$target)) {
    bak <- sub("\\.parquet$", sprintf(".preFix-%s.bak", stamp), p$target)
    ts("backup:", p$target, "->", basename(bak))
    aws(c("s3", "cp", p$target, bak, "--acl", "public-read", "--only-show-errors"))
  } else ts("no existing object to back up:", p$target)
  ts("upload:", basename(p$local), "->", p$target)
  aws(c("s3", "cp", p$local, p$target, "--acl", "public-read", "--only-show-errors"))
}

ts("=== verify uploaded iso3 prunability ===")
for (p in plan) {
  con <- dbConnect(duckdb::duckdb(dbdir=":memory:"))
  https <- sub("^s3://([^/]+)/", "https://\\1.s3.amazonaws.com/", p$target)
  ok <- tryCatch({
    q <- dbGetQuery(con, sprintf(
      "SELECT COUNT(*) ng, SUM(CASE WHEN stats_min IS NULL OR stats_max IS NULL THEN 1 ELSE 0 END) nn
       FROM parquet_metadata('%s') WHERE path_in_schema='iso3'", https))
    sprintf("rg=%d null-stat=%d", q$ng, q$nn)
  }, error = function(e) paste("verify-skip:", conditionMessage(e)))
  dbDisconnect(con, shutdown = TRUE)
  ts("  ", p$P, ok)
}
ts("DONE — A republished to 5 canonical keys. Verify in a REAL browser next.")
