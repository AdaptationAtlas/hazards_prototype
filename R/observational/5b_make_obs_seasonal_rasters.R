# 5b_make_obs_seasonal_rasters.R
# -----------------------------------------------------------------------------
# Per-year SEASONAL (tri-month) SUM rasters for all 12 rolling windows.
#
# Persists the per-year seasonal-total stack that 5_make_obs_map_climatologies.R
# computes and then DISCARDS (script 5 keeps only climatology mean/min/max/sd).
# For PTOT the window rule is `sum`, so each output pixel = total rainfall over
# the 3 months of that window in that year.
#
# Input : Data/chirts_chirps_hist/{VAR}/{VAR}-YYYY-MM.tif   (the monthly store,
#         same files the Tier-3 monthly publish uses -> guaranteed consistent)
# Output: Data/chirts_chirps_hist/seasonal/{VAR}/{VAR}_{SEASON}_{YYYY}_sum.tif
# Publish: R/observational/6_publish_obs_to_s3.R --tier 4  (processing=seasonal)
#
# PURPOSE: give the KE-ENSO notebook a precalculated seasonal COG so it can A/B
# the render path (fetch 1 seasonal COG) vs the client-side sum (fetch 3 monthly
# COGs + add in-browser). Africa extent — notebook window-reads to the county.
#
# Windows (12): JFM FMA MAM AMJ MJJ JJA JAS ASO SON OND NDJ DJF.
# NDJ/DJF span the year boundary; December is attributed to the PREVIOUS year
# (DJF-1998 = Dec1997+Jan1998+Feb1998) — matches script 5 / the climatology COGs.
# Incomplete windows (e.g. current partial year, or the first NDJ/DJF lacking a
# prior December) are dropped automatically.
#
# RUN (cglabs):
#   Rscript R/observational/5b_make_obs_seasonal_rasters.R --full [--var PTOT] [--overwrite]
#   Rscript R/observational/5b_make_obs_seasonal_rasters.R --smoke   # 3 windows, Kenya bbox
# Idempotent: skips a COG that already exists (unless --overwrite).
# ~12 windows x ~45 years ~= 540 COGs for PTOT; sequential ~30-40 min.
# -----------------------------------------------------------------------------

log_step <- function(msg) {
  cat(format(Sys.time(), "[%H:%M:%S] "), msg, "\n", sep = "")
  flush.console()
}

args <- commandArgs(trailingOnly = TRUE)
mode <- {
  m <- intersect(args, c("--smoke", "--full"))
  if (length(m) == 0L) "" else m[1]
}
if (!nzchar(mode)) {
  cat("Usage:\n",
      "  Rscript R/observational/5b_make_obs_seasonal_rasters.R --full [--var PTOT] [--overwrite]\n",
      "  Rscript R/observational/5b_make_obs_seasonal_rasters.R --smoke\n", sep = "")
  quit(status = 1)
}

# --var (default PTOT). Only sum-rule vars make sense as "seasonal totals"; the
# helper still honours each var's agg_rule if you pass another.
get_flag_val <- function(flag, default) {
  hit <- grep(paste0("^", flag, "$"), args)
  if (length(hit) && length(args) > hit[1]) args[hit[1] + 1L] else default
}
VAR <- get_flag_val("--var", "PTOT")
overwrite <- "--overwrite" %in% args

project_dir <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else getwd()
source(file.path(project_dir, "R", "0_server_setup.R"))
source(file.path(project_dir, "R", "observational", "_seasonal_helpers.R"))

root <- atlas_dirs$data_dir$chirts_chirps_hist
# --smoke writes Kenya-cropped COGs to a SEPARATE dir so it can never
# contaminate the full Africa-extent product. (Bug 2026-08-13: smoke + full
# shared `seasonal/` and --full's skip-if-exists left the smoke crop in place
# for JFM/OND/DJF -> published mixed-extent COGs. Tier-4 publish only reads
# `seasonal/`, so the smoke dir is never uploaded.)
out_base <- if (mode == "--smoke") "seasonal_smoke" else "seasonal"
out_dir <- file.path(root, out_base, VAR)
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

# Filename stat suffix reflects the per-var window aggregation: PTOT -> "sum",
# SPEI/TAVG -> "mean", TMAX -> "max", TMIN -> "min" (from _seasonal_helpers agg_rule).
agg_suffix <- .agg_rule[[VAR]]
if (is.null(agg_suffix)) stop("no agg_rule for var '", VAR, "' in _seasonal_helpers.R")

# Scope
smoke_windows <- c("JFM", "OND", "DJF")                       # incl a cross-boundary
smoke_bbox    <- c(xmin = 33.5, xmax = 42.0, ymin = -5.0, ymax = 5.5)  # Kenya-ish
windows <- if (mode == "--smoke") smoke_windows else seasonal_windows()
bbox    <- if (mode == "--smoke") smoke_bbox else NULL

files <- .list_var_files(VAR, root)
year_lo <- min(files$year); year_hi <- max(files$year)

log_step(sprintf("5b seasonal | var=%s mode=%s | root=%s", VAR, mode, root))
log_step(sprintf("  years %d-%d | windows: %s | out=%s",
  year_lo, year_hi, paste(windows, collapse = ","), out_dir))

n_written <- 0L; n_skipped <- 0L
for (period in windows) {
  t0 <- Sys.time()
  stk <- seasonal_yearly_stack(VAR, root, period, year_lo, year_hi, bbox = bbox)
  if (is.null(stk)) {
    log_step(sprintf("  %s: no complete years -> skip", period)); next
  }
  yrs <- names(stk)
  w <- 0L; s <- 0L
  for (k in seq_along(yrs)) {
    out_path <- file.path(out_dir, sprintf("%s_%s_%s_%s.tif", VAR, period, yrs[k], agg_suffix))
    if (!overwrite && file.exists(out_path) && file.size(out_path) > 100L) {
      s <- s + 1L; next
    }
    write_seasonal_cog(stk[[k]], out_path)
    w <- w + 1L
  }
  n_written <- n_written + w; n_skipped <- n_skipped + s
  log_step(sprintf("  %s: %d years (%d written, %d skipped) in %.1fs",
    period, length(yrs), w, s, as.numeric(Sys.time() - t0, units = "secs")))
}
log_step(sprintf("DONE seasonal %s: %d COGs written, %d skipped -> %s",
  VAR, n_written, n_skipped, out_dir))
cat("\nNext: publish with  Rscript R/observational/6_publish_obs_to_s3.R --full --tier 4\n")
