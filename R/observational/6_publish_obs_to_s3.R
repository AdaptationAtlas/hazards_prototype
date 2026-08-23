# 0) Introduction ####
# Publish the observational pipeline's analysis-ready artefacts to the
# public digital-atlas S3 bucket using the Hive-partitioned layout that's
# canonical for newer Atlas datasets (FAOSTAT, hazard x exposure, GLW4).
#
# Sources (produced by R/observational/3, 4, 5 + the obs base raster from 1):
#   Data/chirts_chirps_hist/admin/obs_monthly_adm{0,1}.parquet
#   Data/chirts_chirps_hist/admin/obs_periods_adm{0,1}.parquet
#   Data/chirts_chirps_hist/maps/{VAR}/{VAR}_{period}_{clim}_{stat}.tif   (1,404 COGs)
#   metadata/base_raster_obs.tif
#
# S3 targets:
#
#   Tier 1 (admin parquets + base raster, ~5 files):
#     s3://digital-atlas/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/
#       processing=admin-monthly/variable=adm0_obs.parquet
#       processing=admin-monthly/variable=adm1_obs.parquet
#       processing=admin-periods/variable=adm0_obs.parquet
#       processing=admin-periods/variable=adm1_obs.parquet
#     s3://digital-atlas/domain=boundaries/type=raster/source=chirps-grid/region=africa/
#       processing=base-raster/base_raster_obs.tif
#
#   Tier 2 (climatology COGs, 1,404 files, ~few GB):
#     s3://digital-atlas/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/
#       processing=climatology/
#         variable={PTOT|TMAX|TMIN|TAVG|SPEI-01|SPEI-03|SPEI-06|SPEI-12|SPEI-24}/
#         period={annual|JFM|FMA|MAM|AMJ|MJJ|JJA|JAS|ASO|SON|OND|NDJ|DJF}/
#         clim={atlas_1995-2014|wmo_1991-2020|full_record}/
#         stat={mean|min|max|sd}/
#         {VAR}_{period}_{clim}_{stat}.tif
#
# Local climatology filenames are 4-token: `{VAR}_{period}_{clim}_{stat}.tif`
# where clim is the bare year-range ("1995-2014" / "1991-2020" / "full").
# The name_fn below translates those into the descriptive S3 partition values
# above. No retro-rename of the 1,404 on-disk COGs.
#
# Tier 3 (per-pixel monthly + SPEI COGs, ~13,500 files, ~50 GB) is explicitly
# out of scope for this dispatch and stays on Afrilabs only.
#
# Run modes:
#   --dry-run   Walk local files for the selected tiers and write a
#               (tier, upload_id, local_path, local_size_bytes, s3_uri) CSV.
#               No network, no AWS credentials required. Use this to eyeball
#               path shapes before any real upload. Exits 0 unless a name_fn
#               throws (e.g. unexpected climatology filename shape).
#   --smoke     Upload ONE file (obs_monthly_adm0.parquet) and run four
#               inline checks: arrow round-trip, S3 listing, anonymous-read
#               ACL, $save_report() audit log. Always Tier 1 regardless of
#               --tier. Stops after the checks; do NOT run --full from here.
#   --full      Upload every file selected by --tier. Per-tier upload via
#               AtlasDataManageR::S3DirUploader$upload_files_parallel + a
#               $save_report() audit log. Idempotent (overwrite = FALSE) so
#               re-runs after a partial failure are cheap.
#   --tier N    Restrict to Tier 1, 2, or 'all' (default). Ignored by --smoke.
#   --overwrite Re-upload files that already exist on S3.
#
# Parallel flags (workers, cpu-fraction, mem-fraction, mem-budget) follow the
# pipeline-wide convention in R/observational/_helpers.R. Uploads are
# I/O-bound, so per-worker RAM is small (~0.2 GB).
#
# Dependencies:
#   AtlasDataManageR (github.com/AdaptationAtlas/data-management/R/AtlasDataManageR)
#   arrow, data.table, s3fs, glue, jsonlite, fs, future, future.apply
#
# Please run R/0_server_setup.R before --full / --smoke; --dry-run uses
# bootstrap_minimal() and sidesteps the heavy pipeline startup.

# 1) Setup ####

log_step <- function(msg) {
  cat(format(Sys.time(), "[%H:%M:%S] "), msg, "\n", sep = "")
  flush.console()
}

bootstrap_minimal <- function() {
  log_step("bootstrap_minimal: resolving project / working dirs")
  if (!requireNamespace("pacman", quietly = TRUE)) {
    install.packages("pacman", repos = "https://cloud.r-project.org")
  }
  library(pacman)
  pacman::p_load(data.table, glue, jsonlite, fs, arrow)

  project_dir <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else getwd()
  candidates <- switch(project_dir,
    "/home/jovyan/atlas/hazards_prototype" = c(
      "/home/jovyan/common_data/nex-gddp-cimp6_hazards",
      "/home/jovyan/common_data/hazards_prototype"
    ),
    "D:/rprojects/hazards_prototype" = "D:/common_data/hazards_prototype",
    "C:/rprojects/hazards_prototype" = "C:/rprojects/common_data/hazards_prototype",
    "/Users/pstewarda/Documents/rprojects/hazards_prototype" =
      "/Users/pstewarda/Documents/rprojects/common_data/hazards_prototype",
    "/home/psteward/rprojects/hazards_prototype" = "/cluster01/workspace/atlas/hazards_prototype",
    stop(glue::glue("Unknown project_dir '{project_dir}'. Add a mapping."))
  )
  has_data <- vapply(candidates, function(p) {
    dir.exists(file.path(p, "Data", "chirts_chirps_hist"))
  }, logical(1))
  working_dir <- if (any(has_data)) candidates[has_data][1] else candidates[1]
  log_step(sprintf("  selected working_dir: %s", working_dir))
  if (!dir.exists(working_dir)) dir.create(working_dir, recursive = TRUE)
  setwd(working_dir)

  chirts_chirps_hist_dir <- file.path("Data", "chirts_chirps_hist")
  list(
    project_dir = project_dir,
    working_dir = working_dir,
    chirts_chirps_hist_dir = chirts_chirps_hist_dir
  )
}

args <- commandArgs(trailingOnly = TRUE)
mode <- {
  modes <- intersect(args, c("--dry-run", "--smoke", "--full"))
  if (length(modes) == 0L) "" else modes[1]
}

usage <- function() {
  cat(
    "Usage:\n",
    "  Rscript R/observational/6_publish_obs_to_s3.R --dry-run [--tier N]\n",
    "      No network. Writes _publish_dry_run.csv with (local, s3) path pairs.\n",
    "  Rscript R/observational/6_publish_obs_to_s3.R --smoke\n",
    "      Uploads one file + runs 4 inline checks. Requires AWS credentials.\n",
    "  Rscript R/observational/6_publish_obs_to_s3.R --full [--tier N] [--overwrite]\n",
    "      Uploads every file in the selected tiers. Requires AWS credentials.\n",
    "\n",
    "Flags:\n",
    "  --tier {1|2|3|4|5|all} Default 'all' (Tier 1+2). Tier 3 = monthly COGs;\n",
    "                      Tier 4 = seasonal-sum COGs; Tier 5 = MODIS NDVI COGs.\n",
    "                      Tiers 3/4/5 are OPT-IN ONLY (--tier 3|4|5), not in 'all'.\n",
    "                      Ignored by --smoke (always Tier 1).\n",
    "  --overwrite         Re-upload files already on S3.\n",
    sep = ""
  )
}

if (!nzchar(mode)) {
  usage()
  # Load helpers just for the parallel-flags usage block, if available.
  pd <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else getwd()
  helpers <- file.path(pd, "R", "observational", "_helpers.R")
  if (file.exists(helpers)) {
    source(helpers)
    cat("\nParallel flags (uploads are I/O-bound, ~0.2 GB per worker):\n")
    cat(parallel_flags_usage(), "\n", sep = "")
  }
  quit(status = 1)
}

if (mode == "--dry-run") {
  paths <- bootstrap_minimal()
  project_dir <- paths$project_dir
  chirts_chirps_hist_dir <- paths$chirts_chirps_hist_dir
} else if (mode == "--smoke") {
  paths <- bootstrap_minimal()
  project_dir <- paths$project_dir
  chirts_chirps_hist_dir <- paths$chirts_chirps_hist_dir
} else if (mode == "--full") {
  source("R/0_server_setup.R")
  pacman::p_load(data.table, glue, jsonlite, fs, arrow)
  chirts_chirps_hist_dir <- atlas_dirs$data_dir$chirts_chirps_hist
}

source(file.path(project_dir, "R", "observational", "_helpers.R"))
pacman::p_load(future, future.apply)

# Resolve --tier (default all). --smoke always means Tier 1, one file.
tier_arg <- parse_cli_flag(args, "tier", "character")
if (is.null(tier_arg) || is.na(tier_arg)) tier_arg <- "all"
if (!tier_arg %in% c("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "all")) {
  stop(glue::glue("--tier must be 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, or all (got '{tier_arg}')"))
}
# Tiers 3-7 are opt-in only: NOT included in 'all' (large raster / new-domain uploads).
do_tier1 <- mode == "--smoke" || tier_arg %in% c("1", "all")
do_tier2 <- mode != "--smoke" && tier_arg %in% c("2", "all")
do_tier3 <- mode != "--smoke" && tier_arg == "3"
do_tier4 <- mode != "--smoke" && tier_arg == "4"
do_tier5 <- mode != "--smoke" && tier_arg == "5"
do_tier6 <- mode != "--smoke" && tier_arg == "6"
do_tier7 <- mode != "--smoke" && tier_arg == "7"
do_tier8 <- mode != "--smoke" && tier_arg == "8"
do_tier9 <- mode != "--smoke" && tier_arg == "9"
do_tier10 <- mode != "--smoke" && tier_arg == "10"
do_tier11 <- mode != "--smoke" && tier_arg == "11"

overwrite <- parse_overwrite_flag(args)
# AtlasDataManageR 0.0.0.9000 (currently installed) does NOT expose an
# `overwrite` arg on either S3DirUploader$new() or upload_files_parallel().
# The flag is parsed for forward-compatibility but emits a warning so the
# user knows it's not honoured at the package level. If you need true
# re-upload semantics, delete the target S3 keys first with the AWS CLI.
if (overwrite) {
  log_step(paste(
    "WARNING: --overwrite is not honoured by AtlasDataManageR 0.0.0.9000",
    "(no overwrite arg on S3DirUploader). Upload will follow the package",
    "default (typically: skip-if-exists). Delete S3 keys manually if you",
    "need a forced re-upload."
  ))
}

# Upload workers are I/O-bound; per-worker RAM is small.
per_worker_gb <- 0.2
workers <- resolve_workers(args, per_worker_gb = per_worker_gb, max_workers = 16L)
print_resource_banner(workers, per_worker_gb, label = "publish")

# 2) AWS credential check ####

aws_creds_present <- function() {
  has_env <- nzchar(Sys.getenv("AWS_ACCESS_KEY_ID")) &&
    nzchar(Sys.getenv("AWS_SECRET_ACCESS_KEY"))
  has_file <- file.exists("~/.aws/credentials") ||
    file.exists(file.path(Sys.getenv("HOME"), ".aws", "credentials"))
  has_env || has_file
}

if (mode != "--dry-run") {
  if (!aws_creds_present()) {
    cat(
      "ERROR: AWS credentials not found.\n",
      "  Set AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY in the environment,\n",
      "  or populate ~/.aws/credentials. Run with --dry-run to verify path\n",
      "  shapes without AWS access.\n",
      sep = ""
    )
    quit(status = 1)
  }
}

# 3) AtlasDataManageR install + load ####

if (mode != "--dry-run") {
  if (!requireNamespace("AtlasDataManageR", quietly = TRUE)) {
    log_step("Installing AtlasDataManageR from GitHub")
    if (!requireNamespace("remotes", quietly = TRUE)) {
      install.packages("remotes", repos = "https://cloud.r-project.org")
    }
    remotes::install_github("AdaptationAtlas/data-management/R/AtlasDataManageR")
  }
  library(AtlasDataManageR)
  pacman::p_load(s3fs)
}

# 4) Configuration ####

bucket <- "digital-atlas"

# Local directories
admin_dir   <- file.path(chirts_chirps_hist_dir, "admin")
maps_dir    <- file.path(chirts_chirps_hist_dir, "maps")
metadata_dir <- file.path(project_dir, "metadata")

# Hive-partition prefixes (no leading slash; AtlasDataManageR composes the
# full s3://bucket/<s3_dir>/<name_fn output>).
climate_root <- "domain=climate/type=observational/source=chirps-chirts-era5/region=africa"
prefix_admin_monthly <- paste0(climate_root, "/processing=admin-monthly")
prefix_admin_periods <- paste0(climate_root, "/processing=admin-periods")
prefix_climatology   <- paste0(climate_root, "/processing=climatology")
prefix_monthly       <- paste0(climate_root, "/processing=monthly")
prefix_seasonal      <- paste0(climate_root, "/processing=seasonal")
# NDVI is a NEW domain path (type=vegetation, not CHIRPS observational). Region =
# east-africa (the 4 MOD13Q1 tiles / Kenya footprint, NOT all Africa). processing=
# and season= are emitted by name_fn_ndvi (seasonal vs annual differ).
prefix_ndvi <- "domain=climate/type=vegetation/source=modis-mod13q1/region=east-africa"
# JRC GloFAS return-period flood hazard (new type=flood). region=east-africa (Kenya crop).
prefix_flood <- "domain=climate/type=flood/source=jrc-glofas/region=east-africa"
# Global Flood Database observed per-year flood occurrence (type=flood, different source).
prefix_gfd <- "domain=climate/type=flood/source=global-flood-db/region=east-africa"
# FEWS NET CHIRPS-ETos WRSI (crop/pasture water satisfaction; new type=agriculture).
prefix_wrsi <- "domain=climate/type=agriculture/source=fews-wrsi/region=east-africa"
# KE-39 exposure: WorldPop population (domain=exposure, new type=population).
prefix_pop <- "domain=exposure/type=population/source=worldpop-constrained-2020/region=east-africa"
# KE-39 admin backbone: IEBC COD-AB (official) GeoJSON — domain=boundaries (mirrors gaul2024 layout).
prefix_codab <- "domain=boundaries/type=admin/source=iebc-codab/region=kenya/processing=analysis-ready"
# KE-39 population (2nd surface): GRID3/WOPR bottom-up (KNBS microcensus) — vs worldpop constrained.
prefix_grid3 <- "domain=exposure/type=population/source=grid3/region=east-africa"
prefix_base_raster   <- "domain=boundaries/type=raster/source=chirps-grid/region=africa/processing=base-raster"

# Translate the on-disk climatology label (bare year-range) to the
# descriptive S3 partition value. Script 5 emits "1995-2014" / "1991-2020"
# / "full"; downstream notebook queries see the named windows.
clim_translate <- function(local_clim) {
  switch(local_clim,
    `1995-2014` = "atlas_1995-2014",
    `1991-2020` = "wmo_1991-2020",
    full        = "full_record",
    stop(sprintf("Unknown climatology label '%s'", local_clim))
  )
}

# Name functions
name_fn_admin_monthly <- function(x) {
  base <- tools::file_path_sans_ext(basename(x))     # obs_monthly_adm0
  adm  <- sub(".*_(adm[01])$", "\\1", base)          # adm0
  sprintf("variable=%s_obs.parquet", adm)
}

name_fn_admin_periods <- function(x) {
  base <- tools::file_path_sans_ext(basename(x))     # obs_periods_adm0
  adm  <- sub(".*_(adm[01])$", "\\1", base)
  sprintf("variable=%s_obs.parquet", adm)
}

name_fn_base_raster <- function(x) basename(x)

# Per-pixel monthly COG (Tier 3). On-disk name: {VAR}-YYYY-MM.tif (e.g.
# PTOT-2015-11.tif). Africa extent — the notebook window-reads to the county
# via geotiff.js range requests, so no per-country crop here. S3 leaf:
#   variable={VAR}/{VAR}-YYYY-MM.tif
name_fn_monthly <- function(x) {
  fname <- basename(x)
  base  <- tools::file_path_sans_ext(fname)
  var   <- sub("-[0-9]{4}-[0-9]{2}$", "", base)      # PTOT-2015-11 -> PTOT ; SPEI-03-2015-11 -> SPEI-03
  # vectorized guard: uploader calls name_fn on the whole path vector (CGLABS 2026-08-11)
  bad <- !grepl("^.+-[0-9]{4}-[0-9]{2}$", base)
  if (any(bad)) {
    stop(sprintf("Unexpected monthly filename shape (expected {VAR}-YYYY-MM.tif): %s",
                 paste(fname[bad], collapse = ", ")))
  }
  sprintf("variable=%s/%s", var, fname)
}

# Per-year seasonal (tri-month) sum COG (Tier 4). On-disk name:
# {VAR}_{SEASON}_{YYYY}_sum.tif (e.g. PTOT_OND_2015_sum.tif). Africa extent.
# S3 leaf: variable={VAR}/season={SEASON}/{fname}. name_fn is vectorized (the
# uploader passes the whole path vector — see name_fn_monthly note).
name_fn_seasonal <- function(x) {
  fname <- basename(x)
  base  <- tools::file_path_sans_ext(fname)
  parts <- tstrsplit(base, "_", fixed = TRUE)            # {VAR} | SEASON | YYYY | {sum|mean}
  bad <- !grepl("^.+_[A-Z]{3}_[0-9]{4}_(sum|mean|min|max)$", base)  # VAR may contain '-' (SPEI-03)
  if (any(bad)) {
    stop(sprintf("Unexpected seasonal filename shape (expected {VAR}_{SEASON}_YYYY_{sum|mean}.tif): %s",
                 paste(fname[bad], collapse = ", ")))
  }
  sprintf("variable=%s/season=%s/%s", parts[[1]], parts[[2]], fname)
}

# MODIS NDVI COG (Tier 5, type=vegetation). On-disk names:
#   seasonal: NDVI_{SEASON}_{YYYY}_mean.tif   annual: NDVI_{YYYY}_mean.tif
# S3 leaf (under prefix_ndvi): processing={seasonal|annual}/variable=NDVI/[season={S}/]{fname}
name_fn_ndvi <- function(x) {
  fname <- basename(x)
  base  <- tools::file_path_sans_ext(fname)
  seas <- grepl("^NDVI_[A-Z]{3}_[0-9]{4}_mean$", base)
  ann  <- grepl("^NDVI_[0-9]{4}_mean$", base)
  bad  <- !(seas | ann)
  if (any(bad)) {
    stop(sprintf("Unexpected NDVI filename (expected NDVI_{SEASON}_YYYY_mean or NDVI_YYYY_mean): %s",
                 paste(fname[bad], collapse = ", ")))
  }
  ifelse(seas,
    sprintf("processing=seasonal/variable=NDVI/season=%s/%s",
            sub("^NDVI_([A-Z]{3})_.*", "\\1", base), fname),
    sprintf("processing=annual/variable=NDVI/%s", fname))
}

# JRC flood-depth COG (Tier 6, type=flood). On-disk: flood-depth_rp{RP}.tif ->
# S3 leaf: processing=return-period/variable=flood-depth/rp={RP}/{fname}
name_fn_flood <- function(x) {
  fname <- basename(x)
  base  <- tools::file_path_sans_ext(fname)
  bad <- !grepl("^flood-depth_rp[0-9]+$", base)
  if (any(bad)) {
    stop(sprintf("Unexpected flood filename (expected flood-depth_rp{RP}.tif): %s",
                 paste(fname[bad], collapse = ", ")))
  }
  rp <- sub("^flood-depth_rp([0-9]+)$", "\\1", base)
  sprintf("processing=return-period/variable=flood-depth/rp=%s/%s", rp, fname)
}

# GFD per-year flood occurrence COG (Tier 7, type=flood). On-disk: flooded_{YYYY}.tif ->
# S3 leaf: processing=annual/variable=flooded/{fname}
name_fn_gfd <- function(x) {
  fname <- basename(x)
  base  <- tools::file_path_sans_ext(fname)
  bad <- !grepl("^flooded_[0-9]{4}$", base)
  if (any(bad)) {
    stop(sprintf("Unexpected GFD filename (expected flooded_YYYY.tif): %s",
                 paste(fname[bad], collapse = ", ")))
  }
  sprintf("processing=annual/variable=flooded/%s", fname)
}

# FEWS WRSI COG (Tier 8, type=agriculture). On-disk: wrsi_{crop}_{SEASON}_{YYYY}.tif ->
# S3 leaf: processing=seasonal/variable=wrsi/crop={crop}/season={SEASON}/{fname}
name_fn_wrsi <- function(x) {
  fname <- basename(x)
  base  <- tools::file_path_sans_ext(fname)
  bad <- !grepl("^wrsi_(cropland|rangeland)_[A-Z]{3}_[0-9]{4}$", base)
  if (any(bad)) {
    stop(sprintf("Unexpected WRSI filename (expected wrsi_{cropland|rangeland}_{SEASON}_YYYY.tif): %s",
                 paste(fname[bad], collapse = ", ")))
  }
  crop   <- sub("^wrsi_([a-z]+)_.*", "\\1", base)
  season <- sub("^wrsi_[a-z]+_([A-Z]{3})_.*", "\\1", base)
  sprintf("processing=seasonal/variable=wrsi/crop=%s/season=%s/%s", crop, season, fname)
}

# WorldPop population COG (Tier 9, domain=exposure/type=population). On-disk:
# population_{YYYY}.tif -> S3 leaf: processing=constrained/variable=count/{fname}
name_fn_worldpop <- function(x) {
  fname <- basename(x)
  base  <- tools::file_path_sans_ext(fname)
  bad <- !grepl("^population_[0-9]{4}$", base)
  if (any(bad)) {
    stop(sprintf("Unexpected WorldPop filename (expected population_YYYY.tif): %s",
                 paste(fname[bad], collapse = ", ")))
  }
  sprintf("processing=constrained/variable=count/%s", fname)
}

# IEBC COD-AB admin GeoJSON (Tier 10, domain=boundaries/type=admin). On-disk:
# ken_adm{1,2}.geojson -> S3 leaf: level=adm{N}/{fname}. (Not a .tif -> skips overview gate.)
name_fn_codab <- function(x) {
  fname <- basename(x)
  base  <- tools::file_path_sans_ext(fname)
  bad <- !grepl("^ken_adm[12]$", base)
  if (any(bad)) {
    stop(sprintf("Unexpected COD-AB filename (expected ken_adm{1,2}.geojson): %s",
                 paste(fname[bad], collapse = ", ")))
  }
  lvl <- sub("^ken_(adm[12])$", "\\1", base)
  sprintf("level=%s/%s", lvl, fname)
}

# GRID3/WOPR bottom-up population COG (Tier 11, type=population/source=grid3). On-disk:
# population_{YYYY}.tif -> S3 leaf: processing=bottom-up/variable=count/{fname}
name_fn_grid3 <- function(x) {
  fname <- basename(x)
  base  <- tools::file_path_sans_ext(fname)
  bad <- !grepl("^population_[0-9]{4}$", base)
  if (any(bad)) {
    stop(sprintf("Unexpected GRID3 filename (expected population_YYYY.tif): %s",
                 paste(fname[bad], collapse = ", ")))
  }
  sprintf("processing=bottom-up/variable=count/%s", fname)
}

name_fn_climatology <- function(x) {
  fname <- basename(x)
  base  <- tools::file_path_sans_ext(fname)
  parts <- strsplit(base, "_", fixed = TRUE)[[1]]
  if (length(parts) != 4L) {
    stop(sprintf(
      "Unexpected climatology filename shape (expected 4 underscore-tokens): %s",
      fname
    ))
  }
  var    <- parts[1]
  period <- parts[2]
  clim   <- parts[3]
  stat   <- parts[4]
  sprintf("variable=%s/period=%s/clim=%s/stat=%s/%s",
          var, period, clim_translate(clim), stat, fname)
}

# Tier-1 + Tier-2 upload specs. Each entry has the inputs S3DirUploader
# needs, plus the file_pattern used by --dry-run to walk locally.
tier1_specs <- list(
  list(
    upload_id     = "obs-admin-monthly",
    local_dir     = admin_dir,
    s3_dir        = prefix_admin_monthly,
    file_pattern  = "^obs_monthly_adm[01]\\.parquet$",
    name_fn       = name_fn_admin_monthly,
    recursive     = FALSE,
    tier          = 1L
  ),
  list(
    upload_id     = "obs-admin-periods",
    local_dir     = admin_dir,
    s3_dir        = prefix_admin_periods,
    file_pattern  = "^obs_periods_adm[01]\\.parquet$",
    name_fn       = name_fn_admin_periods,
    recursive     = FALSE,
    tier          = 1L
  ),
  list(
    upload_id     = "obs-base-raster",
    local_dir     = metadata_dir,
    s3_dir        = prefix_base_raster,
    file_pattern  = "^base_raster_obs\\.tif$",
    name_fn       = name_fn_base_raster,
    recursive     = FALSE,
    tier          = 1L
  )
)

tier2_specs <- list(
  list(
    upload_id     = "obs-climatology",
    local_dir     = maps_dir,
    s3_dir        = prefix_climatology,
    file_pattern  = "\\.tif$",
    name_fn       = name_fn_climatology,
    recursive     = TRUE,
    tier          = 2L
  )
)

# Tier 3 (per-pixel monthly COGs). Opt-in ONLY (--tier 3) — 544+ PTOT files,
# large one-time upload; deliberately NOT part of --tier all. local_dir is the
# monthly variable store sibling of admin/ + maps/ (e.g. chirts_chirps_hist/PTOT).
# Per-pixel monthly COGs, one spec per variable. PTOT is already published;
# SPEI-03/SPEI-12 added (obs pipeline computes them; agg for SPEI is a monthly
# value, not a sum). A var whose local dir is absent simply yields 0 files.
# Confirm the on-disk dir names match (e.g. "SPEI-03" not "SPEI-3") before --full.
monthly_vars <- c("PTOT", "SPEI-03", "SPEI-12")
tier3_specs <- lapply(monthly_vars, function(v) list(
  upload_id     = paste0("obs-monthly-", tolower(v)),
  local_dir     = file.path(chirts_chirps_hist_dir, v),
  s3_dir        = prefix_monthly,
  file_pattern  = sprintf("^%s-[0-9]{4}-[0-9]{2}\\.tif$", v),
  name_fn       = name_fn_monthly,
  recursive     = FALSE,
  tier          = 3L
))

# Tier 4 (per-year seasonal tri-month sum COGs, from 5b). Opt-in ONLY (--tier 4)
# — ~540 PTOT files. local_dir is the seasonal store (recursive over variable
# subdirs: seasonal/{VAR}/{VAR}_{SEASON}_YYYY_sum.tif).
tier4_specs <- list(
  list(
    upload_id     = "obs-seasonal",
    local_dir     = file.path(chirts_chirps_hist_dir, "seasonal"),
    s3_dir        = prefix_seasonal,
    file_pattern  = "^.+_[A-Z]{3}_[0-9]{4}_(sum|mean|min|max)\\.tif$",
    name_fn       = name_fn_seasonal,
    recursive     = TRUE,
    tier          = 4L
  )
)

# Tier 5 (MODIS NDVI COGs, type=vegetation). Opt-in ONLY (--tier 5). local_dir is
# the NDVI store: <Data>/ndvi_modis/NDVI (sibling of chirts_chirps_hist under Data/).
tier5_specs <- list(
  list(
    upload_id     = "obs-ndvi-modis",
    local_dir     = file.path(dirname(chirts_chirps_hist_dir), "ndvi_modis", "NDVI"),
    s3_dir        = prefix_ndvi,
    file_pattern  = "^NDVI_([A-Z]{3}_)?[0-9]{4}_mean\\.tif$",
    name_fn       = name_fn_ndvi,
    recursive     = FALSE,
    tier          = 5L
  )
)

# Tier 6 (JRC flood-depth return-period COGs, type=flood). Opt-in ONLY (--tier 6).
tier6_specs <- list(
  list(
    upload_id     = "obs-flood-jrc",
    local_dir     = file.path(dirname(chirts_chirps_hist_dir), "flood_jrc", "JRC"),
    s3_dir        = prefix_flood,
    file_pattern  = "^flood-depth_rp[0-9]+\\.tif$",
    name_fn       = name_fn_flood,
    recursive     = FALSE,
    tier          = 6L
  )
)

# Tier 7 (GFD per-year flood-occurrence COGs, type=flood). Opt-in ONLY (--tier 7).
tier7_specs <- list(
  list(
    upload_id     = "obs-flood-gfd",
    local_dir     = file.path(dirname(chirts_chirps_hist_dir), "flood_gfd", "GFD"),
    s3_dir        = prefix_gfd,
    file_pattern  = "^flooded_[0-9]{4}\\.tif$",
    name_fn       = name_fn_gfd,
    recursive     = FALSE,
    tier          = 7L
  )
)

# Tier 8 (FEWS WRSI COGs, type=agriculture). Opt-in ONLY (--tier 8).
tier8_specs <- list(
  list(
    upload_id     = "obs-wrsi-fews",
    local_dir     = file.path(dirname(chirts_chirps_hist_dir), "wrsi_fews", "WRSI"),
    s3_dir        = prefix_wrsi,
    file_pattern  = "^wrsi_(cropland|rangeland)_[A-Z]{3}_[0-9]{4}\\.tif$",
    name_fn       = name_fn_wrsi,
    recursive     = FALSE,
    tier          = 8L
  )
)

# Tier 9 (WorldPop population COG, domain=exposure/type=population). Opt-in ONLY (--tier 9).
tier9_specs <- list(
  list(
    upload_id     = "exposure-worldpop",
    local_dir     = file.path(dirname(chirts_chirps_hist_dir), "exposure", "worldpop"),
    s3_dir        = prefix_pop,
    file_pattern  = "^population_[0-9]{4}\\.tif$",
    name_fn       = name_fn_worldpop,
    recursive     = FALSE,
    tier          = 9L
  )
)

# Tier 10 (IEBC COD-AB admin GeoJSON, domain=boundaries/type=admin). Opt-in ONLY (--tier 10).
tier10_specs <- list(
  list(
    upload_id     = "exposure-admin-codab",
    local_dir     = file.path(dirname(chirts_chirps_hist_dir), "exposure", "admin_codab"),
    s3_dir        = prefix_codab,
    file_pattern  = "^ken_adm[12]\\.geojson$",
    name_fn       = name_fn_codab,
    recursive     = FALSE,
    tier          = 10L
  )
)

# Tier 11 (GRID3/WOPR bottom-up population COG). Opt-in ONLY (--tier 11).
tier11_specs <- list(
  list(
    upload_id     = "exposure-grid3",
    local_dir     = file.path(dirname(chirts_chirps_hist_dir), "exposure", "grid3"),
    s3_dir        = prefix_grid3,
    file_pattern  = "^population_[0-9]{4}\\.tif$",
    name_fn       = name_fn_grid3,
    recursive     = FALSE,
    tier          = 11L
  )
)

active_specs <- c(
  if (do_tier1) tier1_specs else list(),
  if (do_tier2) tier2_specs else list(),
  if (do_tier3) tier3_specs else list(),
  if (do_tier4) tier4_specs else list(),
  if (do_tier5) tier5_specs else list(),
  if (do_tier6) tier6_specs else list(),
  if (do_tier7) tier7_specs else list(),
  if (do_tier8) tier8_specs else list(),
  if (do_tier9) tier9_specs else list(),
  if (do_tier10) tier10_specs else list(),
  if (do_tier11) tier11_specs else list()
)

cat("project_dir          :", project_dir, "\n")
cat("working_dir          :", getwd(), "\n")
cat("admin dir            :", admin_dir, "\n")
cat("maps dir             :", maps_dir, "\n")
cat("metadata dir         :", metadata_dir, "\n")
cat("mode                 :", mode, "\n")
cat("tier                 :", tier_arg, "\n")
cat("tier 1 enabled       :", do_tier1, "\n")
cat("tier 2 enabled       :", do_tier2, "\n")
cat("tier 3 enabled       :", do_tier3, "\n")
cat("tier 4 enabled       :", do_tier4, "\n")
cat("tier 5 enabled       :", do_tier5, "\n")
cat("tier 6 enabled       :", do_tier6, "\n")
cat("tier 7 enabled       :", do_tier7, "\n")
cat("tier 8 enabled       :", do_tier8, "\n")
cat("tier 9 enabled       :", do_tier9, "\n")
cat("tier 10 enabled      :", do_tier10, "\n")
cat("tier 11 enabled      :", do_tier11, "\n")
cat("overwrite            :", overwrite, "\n")
cat("workers              :", workers, "\n\n")

# 5) Helpers ####

#' Walk one upload spec's local files, applying name_fn to each.
#' Returns a data.table with (tier, upload_id, local_path,
#' local_size_bytes, s3_uri). Used by --dry-run AND by --full as the
#' authoritative source of which files an uploader would touch.
walk_spec <- function(spec) {
  files <- list.files(
    spec$local_dir,
    pattern = spec$file_pattern,
    full.names = TRUE,
    recursive = spec$recursive
  )
  if (length(files) == 0L) {
    return(data.table::data.table(
      tier = integer(0), upload_id = character(0),
      local_path = character(0), local_size_bytes = double(0),
      s3_uri = character(0)
    ))
  }
  s3_leaves <- vapply(files, spec$name_fn, character(1))
  data.table::data.table(
    tier = spec$tier,
    upload_id = spec$upload_id,
    local_path = files,
    local_size_bytes = file.info(files)$size,
    s3_uri = sprintf("s3://%s/%s/%s", bucket, spec$s3_dir, s3_leaves)
  )
}

#' Does a raster carry internal overviews? (gdalinfo "Overviews:" line.)
cog_has_overviews <- function(path) {
  info <- suppressWarnings(system2("gdalinfo", shQuote(path), stdout = TRUE, stderr = FALSE))
  # A COG smaller than one 512 block CAN'T carry overviews (nothing to downsample)
  # and doesn't need them — the full image already IS the zoomed-out view. Treat
  # such tiny rasters as compliant (e.g. WRSI Kenya = 80x102 @ 0.1 deg). Otherwise
  # require a real "Overviews:" line.
  if (any(grepl("Overviews:", info, fixed = TRUE))) return(TRUE)
  m <- regmatches(info, regexec("Size is ([0-9]+), ([0-9]+)", info))
  hit <- m[vapply(m, length, integer(1)) == 3L]
  if (length(hit)) {
    dims <- as.integer(hit[[1]][2:3])
    if (max(dims) <= 512L) return(TRUE)   # sub-tile: overviews impossible + unneeded
  }
  FALSE
}

#' PUBLISH GATE — every .tif COG we publish MUST carry internal overviews, or the
#' Quarto dash can't render it at zoomed-out extent (see feedback_cogs_need_overviews).
#' Hard-stops the run before any upload if a COG lacks overviews. The base reference
#' raster (obs-base-raster) is exempt (not a map layer). Override: ALLOW_NO_OVERVIEWS=1.
assert_cog_overviews <- function(specs) {
  if (nzchar(Sys.getenv("ALLOW_NO_OVERVIEWS"))) {
    log_step("overview gate: SKIPPED (ALLOW_NO_OVERVIEWS set)")
    return(invisible(NULL))
  }
  missing <- character(0)
  for (spec in specs) {
    if (identical(spec$upload_id, "obs-base-raster")) next
    w <- walk_spec(spec)
    tifs <- w$local_path[grepl("\\.tif$", w$local_path)]
    for (f in tifs) if (!cog_has_overviews(f)) missing <- c(missing, f)
  }
  if (length(missing) > 0L) {
    stop(sprintf(
      paste0("PUBLISH GATE FAILED: %d COG(s) lack internal overviews — the dash requires ",
             "pyramids (feedback_cogs_need_overviews). Re-COG with OVERVIEWS=AUTO (or run ",
             "R/observational/recog_overviews.R), then re-publish. Override with ",
             "ALLOW_NO_OVERVIEWS=1.\nFirst offenders:\n  %s"),
      length(missing), paste(utils::head(missing, 8), collapse = "\n  ")))
  }
  log_step(sprintf("overview gate: PASS — all COGs across %d spec(s) have overviews", length(specs)))
}

#' Build an S3DirUploader for one spec.
#' AtlasDataManageR 0.0.0.9000 does not expose an `overwrite` arg; behaviour
#' falls back to the package default (typically skip-if-exists, which gives
#' free idempotency on re-runs). See the `--overwrite` warning emitted up
#' near the CLI parsing block for forced-overwrite workarounds.
build_uploader <- function(spec) {
  AtlasDataManageR::S3DirUploader$new(
    upload_id    = spec$upload_id,
    local_dir    = spec$local_dir,
    s3_dir       = spec$s3_dir,
    bucket       = bucket,
    file_pattern = spec$file_pattern,
    name_fn      = spec$name_fn,
    public       = TRUE,
    recursive    = spec$recursive
  )
}

# 6) Dry-run mode ####

if (mode == "--dry-run") {
  log_step("=== DRY RUN ===")
  rows <- data.table::rbindlist(lapply(active_specs, walk_spec))
  out_csv <- file.path(chirts_chirps_hist_dir, "_publish_dry_run.csv")
  data.table::fwrite(rows, out_csv)
  log_step(sprintf("wrote %s (%d rows)", out_csv, nrow(rows)))

  if (nrow(rows) == 0L) {
    cat("(no local files matched any active spec - nothing to publish)\n")
    quit(status = 0)
  }

  summary <- rows[, .(
    n_files = .N,
    total_mb = sum(local_size_bytes, na.rm = TRUE) / 1024 / 1024
  ), by = .(tier, upload_id)]
  cat("\n=== Dry-run summary ===\n")
  print(summary)

  # Pre-flight overview check (warn only in dry-run; --full hard-stops on it).
  tif_rows <- rows[grepl("\\.tif$", local_path) & upload_id != "obs-base-raster"]
  if (nrow(tif_rows) > 0L) {
    no_ov <- tif_rows$local_path[!vapply(tif_rows$local_path, cog_has_overviews, logical(1))]
    if (length(no_ov) > 0L) {
      cat(sprintf("\n[!] OVERVIEW WARNING: %d/%d COG(s) lack internal overviews — --full will BLOCK.\n    Re-COG with OVERVIEWS=AUTO (or recog_overviews.R). e.g. %s\n",
        length(no_ov), nrow(tif_rows), basename(no_ov[1])))
    } else {
      cat(sprintf("\n[ok] overview check: all %d COG(s) have overviews.\n", nrow(tif_rows)))
    }
  }

  cat("\n=== Sample S3 paths (head 3 + tail 2 per upload_id) ===\n")
  for (uid in unique(rows$upload_id)) {
    sub <- rows[upload_id == uid]
    cat(sprintf("\n[%s] (%d files)\n", uid, nrow(sub)))
    head_n <- min(3L, nrow(sub))
    tail_n <- min(2L, max(0L, nrow(sub) - head_n))
    for (i in seq_len(head_n)) cat("  ", sub$s3_uri[i], "\n", sep = "")
    if (tail_n > 0L) {
      cat("  ...\n")
      for (i in seq.int(nrow(sub) - tail_n + 1L, nrow(sub))) cat("  ", sub$s3_uri[i], "\n", sep = "")
    }
  }

  cat(sprintf("\nFull list at: %s\n", out_csv))
  quit(status = 0)
}

# 7) Smoke mode ####

if (mode == "--smoke") {
  log_step("=== SMOKE TEST: upload obs_monthly_adm0.parquet + 4 inline checks ===")

  smoke_local <- file.path(admin_dir, "obs_monthly_adm0.parquet")
  if (!file.exists(smoke_local)) {
    cat(sprintf(
      "ERROR: smoke target missing: %s\n  Run R/observational/3_extract_obs_admin.R --full first.\n",
      smoke_local
    ))
    quit(status = 1)
  }
  smoke_s3 <- sprintf("s3://%s/%s/%s",
    bucket, prefix_admin_monthly, name_fn_admin_monthly(smoke_local))
  log_step(sprintf("  local : %s (%.1f MB)", smoke_local, file.info(smoke_local)$size / 1024 / 1024))
  log_step(sprintf("  s3    : %s", smoke_s3))

  # Tighter file_pattern than the Tier-1 monthly spec so the smoke uploads
  # adm0 only - the regular spec matches both adm0 and adm1.
  smoke_spec <- list(
    upload_id    = "obs-admin-monthly-smoke",
    local_dir    = admin_dir,
    s3_dir       = prefix_admin_monthly,
    file_pattern = "^obs_monthly_adm0\\.parquet$",
    name_fn      = name_fn_admin_monthly,
    recursive    = FALSE,
    tier         = 1L
  )
  uploader <- build_uploader(smoke_spec)
  uploader$upload_files_parallel(workers)
  uploader$save_report()
  log_step("upload complete")

  cat("\n=== VERIFICATION CHECKS ===\n")
  pass <- TRUE

  # Check 1: round-trip via s3fs::s3_file_download + arrow::read_parquet
  tmp <- tempfile(fileext = ".parquet")
  ok_download <- tryCatch({
    s3fs::s3_file_download(smoke_s3, tmp)
    TRUE
  }, error = function(e) {
    cat("  download error: ", conditionMessage(e), "\n", sep = "")
    FALSE
  })
  if (ok_download && file.exists(tmp) && file.info(tmp)$size > 100L) {
    local_tbl  <- arrow::read_parquet(smoke_local)
    remote_tbl <- arrow::read_parquet(tmp)
    same_rows <- nrow(local_tbl) == nrow(remote_tbl)
    same_cols <- identical(sort(names(local_tbl)), sort(names(remote_tbl)))
    if (same_rows && same_cols) {
      cat(sprintf("[OK] 1. Round-trip: %d rows x %d cols match.\n",
        nrow(local_tbl), ncol(local_tbl)))
    } else {
      cat(sprintf("[FAIL] 1. Round-trip mismatch (rows %d vs %d; cols match=%s).\n",
        nrow(local_tbl), nrow(remote_tbl), same_cols))
      pass <- FALSE
    }
  } else {
    cat("[FAIL] 1. Round-trip download failed.\n")
    pass <- FALSE
  }

  # Check 2: listing the processing=admin-monthly partition returns the
  # uploaded path (or contains it among any pre-existing objects).
  listing_prefix <- sprintf("s3://%s/%s/", bucket, prefix_admin_monthly)
  listed <- tryCatch(
    s3fs::s3_dir_ls(listing_prefix, refresh = TRUE),
    error = function(e) {
      cat("  list error: ", conditionMessage(e), "\n", sep = "")
      character(0)
    }
  )
  if (smoke_s3 %in% listed) {
    cat(sprintf("[OK] 2. S3 listing contains the uploaded object (%d total objects in partition).\n",
      length(listed)))
  } else {
    cat(sprintf("[FAIL] 2. Uploaded object not in S3 listing of %s.\n", listing_prefix))
    cat("Listed:\n")
    cat(paste0("  ", utils::head(listed, 5), collapse = "\n"), "\n")
    pass <- FALSE
  }

  # Check 3: anonymous read (public ACL). Use a separate s3fs handle with
  # no credentials; if AtlasDataManageR didn't tag the object public, this
  # fails.
  ok_anon <- tryCatch({
    fs_anon <- s3fs::S3FileSystem$new(anonymous = TRUE)
    info <- fs_anon$file_info(sub("^s3://", "", smoke_s3))
    isTRUE(info$size > 100)
  }, error = function(e) {
    cat("  anonymous read error: ", conditionMessage(e), "\n", sep = "")
    FALSE
  })
  if (ok_anon) {
    cat("[OK] 3. Anonymous read succeeded (object is public).\n")
  } else {
    cat("[FAIL] 3. Anonymous read failed - check public ACL on the object.\n")
    pass <- FALSE
  }

  # Check 4: $save_report() audit log exists and references the upload.
  # AtlasDataManageR writes the report file to the current working directory
  # by default, named with the upload_id. We don't know its exact name a
  # priori, so look for any file matching upload_id (case-insensitive).
  report_candidates <- list.files(
    ".", pattern = smoke_spec$upload_id,
    recursive = TRUE, full.names = TRUE, ignore.case = TRUE
  )
  if (length(report_candidates) > 0L) {
    cat(sprintf("[OK] 4. Upload report file present: %s\n", report_candidates[1]))
  } else {
    cat(sprintf("[WARN] 4. Could not locate $save_report() output for upload_id '%s' in working_dir.\n",
      smoke_spec$upload_id))
    # WARN not FAIL - the report writer's path may differ across AtlasDataManageR versions.
  }

  if (!pass) {
    cat("\n=== SMOKE TEST FAILED ===\n")
    quit(status = 1)
  }
  cat("\n=== SMOKE TEST PASSED - STOPPING (do NOT run --full from here) ===\n")
  cat(sprintf("Smoke URI: %s\n", smoke_s3))
  quit(status = 0)
}

# 8) Full mode ####

if (mode == "--full") {
  log_step("=== FULL PUBLISH ===")

  # PUBLISH GATE: block the upload if any COG lacks overviews (dash requirement).
  assert_cog_overviews(active_specs)

  # Pre-flight: print what each spec WOULD upload, before doing it.
  for (spec in active_specs) {
    walk <- walk_spec(spec)
    log_step(sprintf("  spec '%s': %d files, %.1f MB total",
      spec$upload_id, nrow(walk),
      sum(walk$local_size_bytes, na.rm = TRUE) / 1024 / 1024))
  }

  summary <- data.table::data.table(
    upload_id = character(0),
    tier = integer(0),
    n_files = integer(0),
    elapsed_s = double(0)
  )
  for (spec in active_specs) {
    log_step(sprintf("=== Uploading: %s (tier %d) ===", spec$upload_id, spec$tier))
    t0 <- Sys.time()
    uploader <- build_uploader(spec)
    # Idempotency: S3DirUploader's default overwrite=FALSE skips objects
    # already on S3. --overwrite flips that.
    uploader$upload_files_parallel(workers)
    uploader$save_report()
    walk <- walk_spec(spec)
    elapsed <- as.numeric(Sys.time() - t0, units = "secs")
    log_step(sprintf("  done: %d files in %.1fs", nrow(walk), elapsed))
    summary <- rbind(summary, data.table::data.table(
      upload_id = spec$upload_id, tier = spec$tier,
      n_files = nrow(walk), elapsed_s = elapsed
    ))
  }

  cat("\n=== Full publish summary ===\n")
  print(summary)
  cat(sprintf("\nTotal files: %d, total elapsed: %.1fs\n",
    sum(summary$n_files), sum(summary$elapsed_s)))
}
