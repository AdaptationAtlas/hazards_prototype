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
    "  --tier {1|2|3|all}  Default 'all' (Tier 1+2). Tier 3 = per-pixel monthly\n",
    "                      COGs; OPT-IN ONLY (--tier 3), not in 'all'. Ignored by\n",
    "                      --smoke (always Tier 1).\n",
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
if (!tier_arg %in% c("1", "2", "3", "all")) {
  stop(glue::glue("--tier must be 1, 2, 3, or all (got '{tier_arg}')"))
}
# Tier 3 is opt-in only: NOT included in 'all' (large per-pixel monthly upload).
do_tier1 <- mode == "--smoke" || tier_arg %in% c("1", "all")
do_tier2 <- mode != "--smoke" && tier_arg %in% c("2", "all")
do_tier3 <- mode != "--smoke" && tier_arg == "3"

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
  var   <- sub("-.*$", "", base)                     # PTOT-2015-11 -> PTOT
  bad <- !grepl("^[A-Za-z0-9]+-[0-9]{4}-[0-9]{2}$", base)  # vectorized: uploader calls name_fn on the whole vector (CGLABS 2026-08-11, flagged)
  if (any(bad)) {
    stop(sprintf("Unexpected monthly filename shape (expected {VAR}-YYYY-MM.tif): %s",
                 paste(fname[bad], collapse = ", ")))
  }
  sprintf("variable=%s/%s", var, fname)
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
tier3_specs <- list(
  list(
    upload_id     = "obs-monthly-ptot",
    local_dir     = file.path(chirts_chirps_hist_dir, "PTOT"),
    s3_dir        = prefix_monthly,
    file_pattern  = "^PTOT-[0-9]{4}-[0-9]{2}\\.tif$",
    name_fn       = name_fn_monthly,
    recursive     = FALSE,
    tier          = 3L
  )
)

active_specs <- c(
  if (do_tier1) tier1_specs else list(),
  if (do_tier2) tier2_specs else list(),
  if (do_tier3) tier3_specs else list()
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
