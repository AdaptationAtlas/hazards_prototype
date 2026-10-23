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
# exists on one box. Scans the call stack for the frame whose source()-set
# `ofile` is THIS file (00_setup.R) - NOT just any outer ofile: a sibling
# script (e.g. 05_final_maps/calc_*.R) that re-sources this file leaves its own
# ofile higher in the stack, and matching that would mis-root to its subdir and
# double the path on the next getOption() resolve.
.HAZARDS_R_ROOT <- local({
  d <- NULL
  for (i in seq_len(sys.nframe())) {
    of <- sys.frame(i)$ofile
    if (!is.null(of) && basename(of) == "00_setup.R") {
      d <- dirname(normalizePath(of)); break
    }
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

# ---- input completeness gate --------------------------------------------------
# The daily readers used to drop missing input files silently (`fls[file.exists(fls)]`), so a month
# with gaps under-counted day-count indices (NTx, NDWS, ...) and biased means. DAYS_CHECK sets what a
# gap does:
#   warn     (default) legacy behaviour - continue on the days present - plus a WARN line per month
#            that names the gap. NEX-GDDP runs are unchanged by default: the on-disk calendar layout
#            of the 360-day / no-leap GCMs has not been censused yet (DISPATCH_obs_s0_probe.md).
#   calendar stop on any gap the series' own calendar does not explain. The calendar is inferred
#            from the files in that directory: a missing 31st is a calendar gap only if the
#            directory holds no 31st-of-month file at all (360-day GCM); a missing 29-Feb only if
#            it holds no 29-Feb at all (no-leap GCM). For NEX-GDDP once the gap census is clean.
#   strict   stop on any missing day. For Gregorian observed inputs (CHIRPS v3 / CHIRTS-ERA5 / AgERA5).
# Under calendar/strict a stop aborts the whole script (drivers have no tryCatch): run one GCM per
# process.
cfg_days_check <- function() {
  m <- tolower(env_or("DAYS_CHECK", "warn"))
  stopifnot("DAYS_CHECK must be warn, calendar or strict" = m %in% c("warn", "calendar", "strict"))
  m
}
.dir_cal_cache <- new.env(parent = emptyenv())
.dir_calendar <- function(dir) {
  if (!is.null(.dir_cal_cache[[dir]])) return(.dir_cal_cache[[dir]])
  f <- list.files(dir, pattern = "\\.tif$")
  r <- c(has31 = any(grepl("-[0-9]{2}-31\\.tif$", f)), has0229 = any(grepl("-02-29\\.tif$", f)))
  assign(dir, r, envir = .dir_cal_cache)
  r
}
# TRUE where a missing file is explained by its directory's calendar. Dates are read from the
# FILE NAME only, so a date-like token elsewhere in the path cannot confuse it.
.calendar_gap <- function(ids) {
  b <- basename(ids)
  m <- regexpr("[0-9]{4}-[0-9]{2}-[0-9]{2}", b)
  out <- rep(FALSE, length(ids))
  hit <- m > 0
  if (!any(hit)) return(out)
  d <- regmatches(b, m)
  md <- substr(d, 6, 10)
  for (k in which(hit)) {
    cal <- .dir_calendar(dirname(ids[k]))
    j <- which(which(hit) == k)
    out[k] <- (md[j] == "02-29" && !cal[["has0229"]]) || (substr(md[j], 4, 5) == "31" && !cal[["has31"]])
  }
  out
}
.missing_days_gate <- function(ok, ids, label) {
  if (all(ok)) return(invisible(TRUE))
  mode <- cfg_days_check()
  miss <- ids[!ok]
  if (mode != "strict" && all(.calendar_gap(miss))) {
    .log(sprintf("%s: %d calendar-gap day(s) absent (no 29-Feb / no 31st in this series) - expected",
                 label, length(miss)), level = "INFO")
    return(invisible(TRUE))
  }
  msg <- sprintf("%s: %d of %d daily inputs missing (first missing: %s)",
                 label, sum(!ok), length(ok), basename(miss[1]))
  if (mode == "warn") {
    .log(msg, " - DAYS_CHECK=warn (default): continuing on the days present", level = "WARN")
    return(invisible(FALSE))
  }
  stop(msg, " [DAYS_CHECK=", mode, "]. DAYS_CHECK=warn continues on the days present.", call. = FALSE)
}
# Expected daily files -> the files to read (all of them, or under warn the ones present).
require_days <- function(fls, label = "input") {
  ok <- file.exists(fls)
  .missing_days_gate(ok, fls, label)
  fls[ok]
}
# Same gate for the aligned-stack pattern (`keep` = days present for every variable). `ref_dir` is
# one input directory, used to read the series calendar.
require_all_days <- function(keep, dts, label = "input", ref_dir = ".") {
  .missing_days_gate(keep, file.path(ref_dir, paste0("x_", as.character(dts), ".tif")), label)
  invisible(keep)
}

# ---- water-balance seed months ----------------------------------------------
# Months at which the soil-water store (AVAIL / SWB) is reset to its initial state instead of
# being carried from the prior month. Default = the two NEX-GDDP run starts (historical 1995-01,
# future 2021-01). A run starting elsewhere (e.g. an observed 2005-2025 series) must set
# AVAIL_SEED_MONTHS="2005-01" - otherwise it stops at the first month (prior AVAIL missing) and,
# worse, would silently reset the store at 2021-01 mid-series.
cfg_avail_seed_months <- function(default = c("1995-01", "2021-01")) {
  v <- Sys.getenv("AVAIL_SEED_MONTHS", unset = NA_character_)
  if (is.na(v) || v == "") return(default)
  s <- trimws(strsplit(v, ",", fixed = TRUE)[[1]])
  stopifnot("AVAIL_SEED_MONTHS must be YYYY-MM[,YYYY-MM...]" = all(grepl("^[0-9]{4}-[0-9]{2}$", s)))
  s
}

# scipen only; do NOT silence warnings (legacy scripts set warn=-1 - dropped on migration).
options(scipen = 999)
