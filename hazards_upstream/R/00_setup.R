# =============================================================================
# Stage-0 (hazards_upstream) shared setup / logging / config helper
# By: Stage-0 workout (Phase 2), 2026-06-24
#
# Source at the TOP of every 01-07 script:
#     source(file.path(dirname(sys.frame(1)$ofile %||% "."), "../00_setup.R"))
#   or, more simply, with an absolute/relative path from the script's stage dir:
#     source("../00_setup.R")
#
# Provides, base-R only (no package deps so it loads cheaply everywhere):
#   * common_data_root()  - single source of truth for the data root, env-overridable
#   * .log() / .log_reset()- timestamped logging with total + per-step elapsed
#   * env_flag() / env_or()- read SKIP_* / FORCE_OVERWRITE / DRY_RUN / config envs
#   * cfg_gcms/ssps/scenario/yrs/prds - canonical run config, env-overridable
#   * should_skip()        - file.exists + FORCE_OVERWRITE gate
#
# Design note: this is a STANDALONE producer setup. It intentionally does NOT
# source hazards_prototype's 0_server_setup.R / atlas_dirs - the vendored subtree
# stays loosely coupled until the repo merge is scoped. The COMMON_DATA default
# (~/common_data) reproduces the legacy hardcoded root byte-for-byte on cglabs
# (where ~ == /home/jovyan), so sourcing this changes NO path behaviour by default.
# =============================================================================

`%||%` <- function(a, b) if (is.null(a) || length(a) == 0L || (length(a) == 1L && is.na(a))) b else a

# ---- data root --------------------------------------------------------------
# Override with COMMON_DATA=/some/path. Default = ~/common_data (legacy behaviour).
common_data_root <- function(check = FALSE) {
  root <- Sys.getenv("COMMON_DATA", unset = path.expand("~/common_data"))
  root <- path.expand(root)
  if (check) stopifnot("COMMON_DATA root does not exist" = dir.exists(root))
  root
}

# ---- repo R-dir (for cross-stage source() of sibling scripts) ---------------
# Self-locates the directory holding THIS 00_setup.R (= hazards_upstream/R) at
# source() time, so scripts can source sibling stage scripts repo-relative
# instead of a hardcoded ~/Repositories/hazards sibling-clone path that only
# exists on one box. Scans the call stack for the source()-set `ofile`.
.HAZARDS_R_ROOT <- local({
  d <- NULL
  for (i in seq_len(sys.nframe())) {
    of <- sys.frame(i)$ofile
    if (!is.null(of)) { d <- dirname(normalizePath(of)); break }
  }
  if (is.null(d)) d <- getwd()
  d
})
# Stored as an option too: options survive rm(list=ls()), so scripts that wipe
# globalenv between sections can still resolve sibling paths via getOption().
options(hazards.r_root = .HAZARDS_R_ROOT)
hazards_r_root <- function() getOption("hazards.r_root", .HAZARDS_R_ROOT)

# ---- environment readers ----------------------------------------------------
# Truthy: 1/true/t/yes/y/on (case-insensitive). Everything else (incl. unset) -> default.
env_flag <- function(name, default = FALSE) {
  v <- Sys.getenv(name, unset = NA_character_)
  if (is.na(v) || v == "") return(default)
  tolower(trimws(v)) %in% c("1", "true", "t", "yes", "y", "on")
}

env_or <- function(name, default) {
  v <- Sys.getenv(name, unset = NA_character_)
  if (is.na(v) || v == "") default else v
}

# Parse a year spec from an env string: "1995:2014" (range) or "1995,1996,2000" (csv).
parse_yrs <- function(spec) {
  spec <- trimws(spec)
  if (grepl(":", spec, fixed = TRUE)) {
    p <- as.integer(strsplit(spec, ":", fixed = TRUE)[[1]])
    stopifnot(length(p) == 2L, !any(is.na(p)))
    return(p[1]:p[2])
  }
  out <- as.integer(strsplit(spec, ",", fixed = TRUE)[[1]])
  stopifnot(!any(is.na(out)))
  out
}

# ---- timestamped logging ----------------------------------------------------
# [2026-06-24 14:03:11] +  12.4s (Δ  3.1s) INFO  | message
# Per the project rule: every log line carries a timestamp + total + per-step elapsed.
.atlas_log_state <- new.env(parent = emptyenv())
.atlas_log_state$t0    <- Sys.time()
.atlas_log_state$tlast <- .atlas_log_state$t0

.log_reset <- function() {
  now <- Sys.time()
  .atlas_log_state$t0    <- now
  .atlas_log_state$tlast <- now
  invisible(now)
}

.log <- function(..., level = "INFO") {
  now      <- Sys.time()
  el_total <- as.numeric(difftime(now, .atlas_log_state$t0,    units = "secs"))
  el_step  <- as.numeric(difftime(now, .atlas_log_state$tlast, units = "secs"))
  .atlas_log_state$tlast <- now
  msg <- paste0(..., collapse = "")
  cat(sprintf("[%s] +%6.1fs (Δ%5.1fs) %-5s | %s\n",
              format(now, "%Y-%m-%d %H:%M:%S"), el_total, el_step, level, msg))
  flush.console()
  invisible(NULL)
}

# ---- canonical run config ---------------------------------------------------
# Full 18-GCM ensemble (verbatim from the 04_indices scripts).
ATLAS_GCMS <- c('ACCESS-CM2','ACCESS-ESM1-5','CanESM5','CMCC-ESM2','EC-Earth3',
                'EC-Earth3-Veg-LR','GFDL-ESM4','INM-CM4-8','INM-CM5-0','IPSL-CM6A-LR',
                'KACE-1-0-G','MIROC6','MPI-ESM1-2-HR','MPI-ESM1-2-LR','MRI-ESM2-0',
                'NorESM2-LM','NorESM2-MM','TaiESM1')

# Bias-correction QA subset (from 03/identifyCorruptedFiles.R).
ATLAS_GCMS_BC <- c('ACCESS-ESM1-5','MPI-ESM1-2-HR','EC-Earth3','INM-CM5-0','MRI-ESM2-0')

ATLAS_SSPS_FUTURE <- c('ssp126','ssp245','ssp370','ssp585')
ATLAS_PRDS        <- c('2021_2040','2041_2060')   # period-string convention (03)

# GCMS env override: comma-separated list, else full ensemble.
cfg_gcms <- function(default = ATLAS_GCMS) {
  v <- Sys.getenv("GCMS", unset = NA_character_)
  if (is.na(v) || v == "") return(default)
  trimws(strsplit(v, ",", fixed = TRUE)[[1]])
}

# SCENARIO env override: 'historical' (default) or 'future'.
cfg_scenario <- function(default = "historical") {
  s <- tolower(env_or("SCENARIO", default))
  stopifnot("SCENARIO must be 'historical' or 'future'" = s %in% c("historical", "future"))
  s
}

# SSPs by scenario, YRS-overridable for future.
cfg_ssps <- function(scenario = cfg_scenario()) {
  if (scenario == "historical") "historical" else {
    v <- Sys.getenv("SSPS", unset = NA_character_)
    if (is.na(v) || v == "") ATLAS_SSPS_FUTURE else trimws(strsplit(v, ",", fixed = TRUE)[[1]])
  }
}

# Year vector by scenario. Override with YRS= (range "a:b" or csv). Defaults are
# per-call so each script preserves its own legacy window - pass historical=/future=
# to match the literal the script used. The documented CMIP6 baseline is 1995:2014
# (WMO 1991-2020 is impossible from CMIP6 historical, which ends 2014).
cfg_yrs <- function(scenario = cfg_scenario(), historical = 1995:2014, future = 2021:2100) {
  v <- Sys.getenv("YRS", unset = NA_character_)
  if (!is.na(v) && v != "") return(parse_yrs(v))
  if (scenario == "historical") historical else future
}

cfg_prds <- function() {
  v <- Sys.getenv("PRDS", unset = NA_character_)
  if (is.na(v) || v == "") ATLAS_PRDS else trimws(strsplit(v, ",", fixed = TRUE)[[1]])
}

# Months as zero-padded "01".."12". Override MONTHS="1" or MONTHS="1,7" (csv ints)
# to restrict the inner month loop - used for fast single-month GATE runs.
cfg_months <- function(default = sprintf('%02.0f', 1:12)) {
  v <- Sys.getenv("MONTHS", unset = NA_character_)
  if (is.na(v) || v == "") return(default)
  sprintf('%02.0f', as.integer(trimws(strsplit(v, ",", fixed = TRUE)[[1]])))
}

# ---- skip / overwrite gate --------------------------------------------------
# TRUE => skip (output present AND not forced). FORCE_OVERWRITE=1 forces recompute.
should_skip <- function(outfile, force = env_flag("FORCE_OVERWRITE", FALSE)) {
  all(file.exists(outfile)) && !force
}

# scipen only; do NOT silence warnings (legacy scripts set warn=-1 - dropped on migration).
options(scipen = 999)
