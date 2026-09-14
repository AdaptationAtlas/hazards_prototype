#!/usr/bin/env Rscript
# scripts/r3_publish_tiers.R
# ==========================
# Publish the notebook-facing hazard_exposure parquets (all three severity tiers)
# and, optionally, the exposure reference parquet the notebook divides by.
# Generalises scripts/r3_publish_moderate_extreme.R (CR-091) to include `severe`
# and adds hard gates so a half-baked product cannot reach the live Atlas.
#
# WHAT THE NOTEBOOK READS (verified 2026-09-10 against atlas_notebooks):
#   s3://digital-atlas/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/
#     processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=jagermeyr/
#     model=ENSEMBLEmean/severity={severe,moderate,extreme}/int=multi-hazard.parquet
#   s3://digital-atlas/domain=exposure/type=combined/source=glw4-2020_spam2020AA/
#     region=ssa/processing=atlas-harmonized/variable=crop-livestock_all.parquet
#   NOTE: R/s3_upload.R publishes a DIFFERENT product (source=atlas_cmip6/vop_intld15).
#
# LOCAL SOURCES (working_dir-relative after 0_server_setup.R):
#   <hazard_risk_vop_usd>/<timeframe>/haz-freq-exp_vop_nominal-usd-2021_ENSEMBLEmean_int_adm_<tier>.parquet
#   <exposure_dir>/exposure_adm_sum_spam20-20_glw420-20.parquet            (--reference)
#
# GATES (per tier; any FAIL aborts that tier, nothing uploaded):
#   G1 local file exists and is > 10 MB
#   G2 every hazard_vars has hazard='none' rows and n(none) == n(any)   (issue #9 ask)
#   G3 scenarios = historic + ssp126/245/370/585 (historic rows live INSIDE this file)
#   G4 severity column == tier
#   G5 column names identical to the live object (schema drift breaks notebook SQL)
# Reference (--reference, OPT-IN, currently DEFERRED): columns + distinct(exposure/unit/stat)
#   identical to live, rows within 25 %. Expected to FAIL on the column gate until the
#   producer->canonical drift is resolved (0.4.4 emits no `unit_full`; row counts/tech
#   levels differ) - see atlas_notebooks .../2026-05-26_exposure-producer-drift.md.
#
# Convention: back up the live object to sandbox/backup/issue9_<STAMP>/... then
# upload with ACL="public-read" (download+upload, NEVER s3_file_copy - strips ACL).
#
# Usage:
#   Rscript scripts/r3_publish_tiers.R --dry-run                 # gates + backup plan, no writes
#   Rscript scripts/r3_publish_tiers.R                           # severe,moderate,extreme
#   Rscript scripts/r3_publish_tiers.R --tiers severe            # subset
#   Rscript scripts/r3_publish_tiers.R --reference               # tiers + exposure reference
#   Rscript scripts/r3_publish_tiers.R --reference-only
#   flags: --timeframe jagermeyr (default) | --allow-schema-drift (G5 -> warn) | --skip-gates

t0 <- Sys.time()
.ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) cat(sprintf("[%s] [publish-tiers] %s\n", .ts(), sprintf(fmt, ...)))
.elapsed <- function(from) sprintf("%.1f min", as.numeric(difftime(Sys.time(), from, units = "mins")))

args <- commandArgs(trailingOnly = TRUE)
flag <- function(x) x %in% args
opt  <- function(x, default) { i <- match(x, args); if (is.na(i) || i == length(args)) default else args[i + 1] }
DRY_RUN     <- flag("--dry-run")
SKIP_GATES  <- flag("--skip-gates")
ALLOW_DRIFT <- flag("--allow-schema-drift")
DO_REF      <- flag("--reference") || flag("--reference-only")
DO_TIERS    <- !flag("--reference-only")
TF          <- opt("--timeframe", "jagermeyr")
TIERS       <- strsplit(opt("--tiers", "severe,moderate,extreme"), ",")[[1]]
STAMP       <- format(Sys.time(), "%Y%m%d_%H%M%S")

setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else
  file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
if (nzchar(Sys.getenv("ATLAS_SETUP_SKIP"))) {
  .log("ATLAS_SETUP_SKIP set - not sourcing setup; expecting atlas_dirs (+ exposure_dir) in the calling env")
  stopifnot(exists("atlas_dirs"))
} else {
  .log("sourcing %s", setup)
  suppressMessages(suppressWarnings(source(setup)))
}
suppressPackageStartupMessages({ pacman::p_load(s3fs, arrow, dplyr, data.table) })

BUCKET  <- "digital-atlas"
S3_BASE <- sprintf(paste0(
  "domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/",
  "variable=vop_nominal-usd21/period=%s/model=ENSEMBLEmean"), TF)
REF_KEY <- paste0("domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/",
                  "processing=atlas-harmonized/variable=crop-livestock_all.parquet")
local_tier_dir <- file.path(atlas_dirs$data_dir$hazard_risk_vop_usd, TF)
local_ref_dir  <- if (exists("exposure_dir")) exposure_dir else atlas_dirs$data_dir$exposure
local_ref      <- file.path(local_ref_dir, "exposure_adm_sum_spam20-20_glw420-20.parquet")

cat(sprintf("\n=== issue #9 publish: tiers=%s | timeframe=%s | reference=%s | %s ===\n",
            paste(TIERS, collapse = ","), TF, DO_REF, if (DRY_RUN) "[DRY RUN]" else "LIVE WRITE"))
.log("local tier dir = %s", local_tier_dir)
.log("local reference = %s", local_ref)

s3_exists <- function(url) !is.null(tryCatch(suppressWarnings(s3fs::s3_file_info(url)), error = function(e) NULL))
download_live <- function(url) {
  tmp <- tempfile(fileext = ".parquet"); t1 <- Sys.time()
  s3fs::s3_file_download(url, tmp); .log("  downloaded live copy (%.0f MB) in %s", file.size(tmp) / 1e6, .elapsed(t1)); tmp
}
backup_then_upload <- function(local_f, s3_key, label) {
  s3_url  <- sprintf("s3://%s/%s", BUCKET, s3_key)
  bak_url <- sprintf("s3://%s/sandbox/backup/issue9_%s/%s", BUCKET, STAMP, s3_key)
  live_tmp <- NULL
  if (s3_exists(s3_url)) {
    .log("  live object exists -> backup %s", bak_url)
    live_tmp <- download_live(s3_url)
    if (!DRY_RUN) { s3fs::s3_file_upload(live_tmp, bak_url, ACL = "public-read", overwrite = TRUE); .log("  backup written") }
    else .log("  [dry run] backup upload skipped")
  } else .log("  no live object at %s (first publish)", s3_url)
  list(s3_url = s3_url, live_tmp = live_tmp)
}
finish_upload <- function(local_f, s3_url) {
  if (DRY_RUN) { .log("  [dry run] would upload %s (%.0f MB) -> %s", basename(local_f), file.size(local_f) / 1e6, s3_url); return(invisible(TRUE)) }
  t1 <- Sys.time()
  s3fs::s3_file_upload(local_f, s3_url, ACL = "public-read", overwrite = TRUE)
  info <- s3fs::s3_file_info(s3_url)
  ok <- isTRUE(as.numeric(info$size) == file.size(local_f))
  .log("  uploaded in %s: remote %s bytes vs local %s bytes -> %s", .elapsed(t1), info$size, file.size(local_f), if (ok) "SIZE MATCH" else "SIZE MISMATCH")
  https <- sub("^s3://([^/]+)/", "https://\\1.s3.amazonaws.com/", s3_url)
  rc <- tryCatch(system2("curl", c("-s", "-o", "/dev/null", "-w", "%{http_code}", "-H", "'Range: bytes=0-0'", shQuote(https)), stdout = TRUE), error = function(e) NA)
  .log("  HTTPS range probe %s -> HTTP %s (expect 206)", https, paste(rc, collapse = ""))
  if (!ok) stop("size mismatch after upload: ", s3_url)
  invisible(ok)
}
schema_of <- function(f) sort(names(arrow::open_dataset(f)$schema))

## ---------------------------------------------------------------- tiers ----
if (DO_TIERS) for (tier in TIERS) {
  t_tier <- Sys.time()
  cat(sprintf("\n--- %s ---\n", toupper(tier)))
  local_f <- file.path(local_tier_dir, sprintf("haz-freq-exp_vop_nominal-usd-2021_ENSEMBLEmean_int_adm_%s.parquet", tier))
  s3_key  <- sprintf("%s/severity=%s/int=multi-hazard.parquet", S3_BASE, tier)

  # G1
  if (!file.exists(local_f)) { .log("  G1 FAIL: missing %s (has R/3 §4.2 run for %s?)", local_f, TF); next }
  if (file.size(local_f) < 10e6) { .log("  G1 FAIL: %s is only %.1f MB", basename(local_f), file.size(local_f) / 1e6); next }
  .log("  G1 ok: %s (%.0f MB, mtime %s)", basename(local_f), file.size(local_f) / 1e6, format(file.mtime(local_f), "%Y-%m-%d %H:%M"))
  ds <- arrow::open_dataset(local_f)

  if (!SKIP_GATES) {
    # G2 none coverage
    cnt <- ds |> dplyr::count(hazard_vars, hazard) |> dplyr::collect() |> as.data.table()
    wide <- dcast(cnt, hazard_vars ~ hazard, value.var = "n", fill = 0L)
    print(wide, nrows = 50)
    g2 <- all(c("none", "any") %in% names(wide)) && all(wide$none > 0) && all(wide$none == wide$any)
    .log("  G2 %s: every hazard_vars has none rows and n(none)==n(any)", if (g2) "ok" else "FAIL")
    # G3 scenarios
    sc <- ds |> dplyr::count(scenario) |> dplyr::collect() |> as.data.table()
    g3 <- setequal(sc$scenario, c("historic", "ssp126", "ssp245", "ssp370", "ssp585"))
    .log("  G3 %s: scenarios = %s", if (g3) "ok" else "FAIL", paste(sort(sc$scenario), collapse = ","))
    # G4 severity
    sv <- ds |> dplyr::distinct(severity) |> dplyr::collect()
    g4 <- identical(sort(sv$severity), tier)
    .log("  G4 %s: severity column = %s", if (g4) "ok" else "FAIL", paste(sv$severity, collapse = ","))
    if (!(g2 && g3 && g4)) { .log("  ABORT %s: gate failure, nothing uploaded", tier); next }
  }

  bu <- backup_then_upload(local_f, s3_key, tier)
  if (!SKIP_GATES && !is.null(bu$live_tmp)) {
    # G5 schema vs live
    live_cols <- schema_of(bu$live_tmp); loc_cols <- schema_of(local_f)
    if (identical(live_cols, loc_cols)) .log("  G5 ok: %d columns identical to live", length(loc_cols))
    else {
      .log("  G5 %s: local-only = [%s] live-only = [%s]", if (ALLOW_DRIFT) "WARN (allowed)" else "FAIL",
           paste(setdiff(loc_cols, live_cols), collapse = ","), paste(setdiff(live_cols, loc_cols), collapse = ","))
      if (!ALLOW_DRIFT) { .log("  ABORT %s: schema drift (use --allow-schema-drift after checking the notebook SQL)", tier); next }
    }
    live_n <- arrow::open_dataset(bu$live_tmp) |> dplyr::count(hazard_vars, hazard) |> dplyr::collect()
    .log("  info: live has none rows? %s | live hazard_vars = %s", "none" %in% live_n$hazard,
         paste(sort(unique(live_n$hazard_vars)), collapse = ", "))
  }
  finish_upload(local_f, bu$s3_url)
  .log("  %s done in %s", tier, .elapsed(t_tier))
}

## ------------------------------------------------------------ reference ----
if (DO_REF) {
  cat("\n--- EXPOSURE REFERENCE (crop-livestock_all) ---\n")
  if (!file.exists(local_ref)) { .log("  FAIL: missing %s (has 0.4.4 run?)", local_ref) }
  else {
    .log("  local %s (%.0f MB, mtime %s)", basename(local_ref), file.size(local_ref) / 1e6, format(file.mtime(local_ref), "%Y-%m-%d %H:%M"))
    bu <- backup_then_upload(local_ref, REF_KEY, "reference")
    ok <- TRUE
    if (!SKIP_GATES) {
      if (is.null(bu$live_tmp)) { .log("  FAIL: no live reference to compare against - refusing to publish blind"); ok <- FALSE }
      else {
        lc <- schema_of(bu$live_tmp); oc <- schema_of(local_ref)
        if (!identical(lc, oc)) { .log("  FAIL columns: local-only=[%s] live-only=[%s]", paste(setdiff(oc, lc), collapse = ","), paste(setdiff(lc, oc), collapse = ",")); ok <- FALSE }
        else .log("  ok: %d columns identical", length(oc))
        dl <- arrow::open_dataset(bu$live_tmp); dr <- arrow::open_dataset(local_ref)
        for (col in c("exposure", "unit", "stat")) if (col %in% oc) {
          a <- sort((dl |> dplyr::distinct(!!rlang::sym(col)) |> dplyr::collect())[[col]])
          b <- sort((dr |> dplyr::distinct(!!rlang::sym(col)) |> dplyr::collect())[[col]])
          if (setequal(a, b)) .log("  ok: distinct(%s) identical = %s", col, paste(a, collapse = ","))
          else { .log("  FAIL distinct(%s): live=[%s] local=[%s]", col, paste(a, collapse = ","), paste(b, collapse = ",")); ok <- FALSE }
        }
        nl <- nrow(dl); nr <- nrow(dr)
        if (abs(nr - nl) / nl <= 0.25) .log("  ok: rows local %d vs live %d", nr, nl)
        else { .log("  FAIL rows: local %d vs live %d (>25%% apart)", nr, nl); ok <- FALSE }
      }
    }
    if (ok) finish_upload(local_ref, bu$s3_url) else .log("  ABORT reference: gate failure, nothing uploaded")
  }
}

.log("complete in %s%s", .elapsed(t0), if (DRY_RUN) " [DRY RUN - nothing written]" else "")
