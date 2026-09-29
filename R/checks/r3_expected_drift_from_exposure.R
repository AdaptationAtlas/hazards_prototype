#!/usr/bin/env Rscript
# R/checks/r3_expected_drift_from_exposure.R
# ==========================================
# How much did each (iso3, crop) EXPOSURE input move between the exposure the live
# hazard product was built from and the exposure this bake uses? R/3 multiplies
# hazard frequency by exposure, so the product must move by the same factor; G6
# (R/checks/r3_tier_drift_vs_live.R, via scripts/r3_publish_tiers.R --drift-expected)
# applies its bounds NET of these ratios. Without them, a legitimate input change
# (the 2026-09 producer-price fix + the 2026-05 FAOSTAT refresh moved ~140 material
# pairs by more than 25 %) reads as product drift and blocks a correct publish.
#
# old = the exposure table the live product's inputs correspond to. For the usd
#       tier that is the 2025-11-03 S3 object variable=vop_nominal-usd-2021.parquet
#       (built from the same 2025-08 0.4.2 rasters as the 2026-09-16 product; 0.05 deg
#       zonal), downloaded here. --old <parquet> overrides (e.g. a parked twin).
# new = this bake's exposure table: <exposure_dir>/vop_nominal-usd-2021_adm_sum_spam20_glw420_<tag>.parquet
#       (--new overrides; --res picks the tag, default 0.25 = the hazard grid).
# Ratio = new / old at admin0, tech = all (livestock rows have NA tech), finite, old > 0.
# Pairs on one side only get no row (G6 then expects 1 and reports them as unmatched).
#
# Usage (node, seconds; arrow + s3fs):
#   Rscript R/checks/r3_expected_drift_from_exposure.R --res 0.25 --out logs/r3_expected_drift_usd_res-25.csv
#   Rscript R/checks/r3_expected_drift_from_exposure.R --res 0.25 --old <parquet> --new <parquet> --out <csv>

t0 <- Sys.time()
.ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) cat(sprintf("[%s] [expected-drift] %s\n", .ts(), sprintf(fmt, ...)))
args <- commandArgs(trailingOnly = TRUE)
opt <- function(x, d) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
RES <- opt("--res", "0.25"); RES_TAG <- sprintf("res-%02d", round(as.numeric(RES) * 100))
OLD <- opt("--old", ""); NEW <- opt("--new", ""); OUT <- opt("--out", sprintf("logs/r3_expected_drift_usd_%s.csv", RES_TAG))
OLD_KEY <- "s3://digital-atlas/domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/variable=vop_nominal-usd-2021.parquet"
suppressPackageStartupMessages({ library(arrow); library(dplyr); library(data.table) })
if (!nzchar(NEW) || !nzchar(OLD)) {
  setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
  .log("sourcing %s", setup); suppressMessages(suppressWarnings(source(setup)))
}
if (!nzchar(NEW)) { ref_dir <- if (exists("exposure_dir")) exposure_dir else atlas_dirs$data_dir$exposure; NEW <- file.path(ref_dir, sprintf("vop_nominal-usd-2021_adm_sum_spam20_glw420_%s.parquet", RES_TAG)) }
if (!nzchar(OLD)) {
  # NOTE: the S3 key was REPUBLISHED on 2026-09-28 with the corrected values. The
  # 2025-11-03 object the live product corresponds to is the one the publisher backed
  # up; pass it with --old, or let this fall back to the current key only if you know
  # it still is the old one. Refuse silently guessing: require --old unless --old-live-key.
  if (!"--old-live-key" %in% args) stop("--old is required: the S3 key ", OLD_KEY, " was republished 2026-09-28; point --old at the backed-up 2025-11-03 object (sandbox/backup/issue9_<STAMP>/.../variable=vop_nominal-usd-2021.parquet) or pass --old-live-key to use the current key knowingly")
  suppressPackageStartupMessages(library(s3fs)); OLD <- tempfile(fileext = ".parquet"); s3fs::s3_file_download(OLD_KEY, OLD); .log("downloaded %s", OLD_KEY)
}
stopifnot(file.exists(OLD), file.exists(NEW))
rd <- function(f) arrow::open_dataset(f) |> dplyr::filter(is.na(admin1_name), exposure == "vop") |> dplyr::select(iso3, crop, tech, value) |> dplyr::collect() |> as.data.table()
o <- rd(OLD)[(tech == "all" | is.na(tech)) & is.finite(value) & value > 0, .(old = sum(value)), by = .(iso3, crop)]
n <- rd(NEW)[(tech == "all" | is.na(tech)) & is.finite(value) & value > 0, .(new = sum(value)), by = .(iso3, crop)]
m <- merge(o, n, by = c("iso3", "crop"))
m[, ratio_expected := new / old]
.log("old %s (%d pairs) | new %s (%d pairs) | matched %d", basename(OLD), nrow(o), basename(NEW), nrow(n), nrow(m))
.log("ratio_expected: median %.3f | 5-95%% [%.3f, %.3f] | %d pairs beyond +/-25%% (old >= 1e6: %d)",
     median(m$ratio_expected), quantile(m$ratio_expected, 0.05), quantile(m$ratio_expected, 0.95),
     sum(abs(m$ratio_expected - 1) > 0.25), m[old >= 1e6, sum(abs(ratio_expected - 1) > 0.25)])
cat("\n  largest expected moves (old >= 1e6):\n"); print(m[old >= 1e6][order(-abs(log(ratio_expected)))][1:min(15, .N), .(iso3, crop, old = signif(old, 4), new = signif(new, 4), ratio_expected = signif(ratio_expected, 4))], nrows = 15)
dir.create(dirname(OUT), showWarnings = FALSE, recursive = TRUE)
fwrite(m[, .(iso3, crop, old = signif(old, 6), new = signif(new, 6), ratio_expected = signif(ratio_expected, 6))], OUT)
.log("wrote %s (%d rows) in %.1f s", OUT, nrow(m), as.numeric(difftime(Sys.time(), t0, units = "secs")))
