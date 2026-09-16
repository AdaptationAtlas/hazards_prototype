# 7b_relevel_exposure_pop.R
# -----------------------------------------------------------------------------
# Re-level the published flood x exposure tables onto a different official population denominator
# WITHOUT re-running 7_zonal_exposure.R (issue #28).
#
# WHY: the zonal engine costs ~2.9 h, almost all of it the one-time rasterizeGeom precompute for the
# road and grid line lengths — none of which changes when only the population denominator changes.
# Everything the levelling needs is already in the tables: the raw gridded pixel sums
# (pop_total_grid, pop_exposed_grid) and the county key (adm1_pcode). So switching between the 2019
# census and a KNBS projection year is seconds of table arithmetic, not hours of raster work.
#
#   pop_total   = pop_total_grid   x pop_scale_census x pop_growth_county
#   pop_exposed = pop_exposed_grid x (the same two factors)
#   pop_pct     unchanged (they cancel)
# where pop_scale_census = census county total / gridded county total (level fix) and
# pop_growth_county = target county total / census county total (change over time). Both are written
# out as columns, so the method is readable off the published table. Full statement of the method,
# and of the fixed-share assumption it rests on, in R/observational/_population_helpers.R.
#
# Reads and rewrites, in <exposure>/intersect/ :
#   exposure_gfm_seasonal.parquet  exposure_jrc_rp.parquet  exposure_totals.parquet
# Tables written by the pre-#28 engine (no *_grid columns) are upgraded in place: their existing
# pop_total / pop_exposed ARE the gridded sums, so they are copied into the _grid columns first.
#
# DRY RUN BY DEFAULT — prints what would change and writes nothing. Set APPLY=1 to rewrite.
#   POP_SOURCE=knbs-projection POP_YEAR=2025 Rscript R/observational/7b_relevel_exposure_pop.R
#   POP_SOURCE=knbs-projection POP_YEAR=2025 APPLY=1 Rscript R/observational/7b_relevel_exposure_pop.R
#   POP_METHOD=county-level ...                 published projection level (default: county-growth)
#   POP_BASE_YEAR=2020 ...                      growth base year for county-growth
#   IN_DIR=/some/other/intersect ...            override the tables directory
#   EXP_ROOT=/some/exposure ...                 name the exposure root and skip 0_server_setup.R
# Republish afterwards: R/observational/6_publish_obs_to_s3.R --full --tier 16 --overwrite
# -----------------------------------------------------------------------------
t0 <- Sys.time()
log_step <- function(m) cat(format(Sys.time(), "[%H:%M:%S] "),
                            sprintf("(+%5.1fs) ", as.numeric(difftime(Sys.time(), t0, units = "secs"))),
                            m, "\n", sep = "")

suppressPackageStartupMessages({library(data.table); library(arrow)})

project_dir <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else getwd()
# EXP_ROOT names the exposure root directly and skips 0_server_setup.R — for tests and for hosts
# without the pipeline's data layout. Unset (the normal case) resolves it the usual way.
exp_root <- Sys.getenv("EXP_ROOT", "")
if (!nzchar(exp_root)) {
  source(file.path(project_dir, "R", "0_server_setup.R"))
  exp_root <- file.path(dirname(chirts_chirps_hist_dir), "exposure")
}
source(file.path(project_dir, "R", "observational", "_population_helpers.R"))

in_dir          <- Sys.getenv("IN_DIR", file.path(exp_root, "intersect"))
POP_SOURCE      <- Sys.getenv("POP_SOURCE", "knbs-census-2019")
POP_YEAR        <- Sys.getenv("POP_YEAR", "")
POP_METHOD      <- Sys.getenv("POP_METHOD", "county-growth")
POP_BASE_YEAR   <- as.integer(Sys.getenv("POP_BASE_YEAR", POP_PROJECTION_BASE_YEAR))
POP_GRID_SOURCE <- "worldpop-constrained-2020"
APPLY           <- Sys.getenv("APPLY") == "1"

files <- c(A = "exposure_gfm_seasonal.parquet", B = "exposure_jrc_rp.parquet",
           totals = "exposure_totals.parquet")
paths <- file.path(in_dir, files)
missing <- files[!file.exists(paths)]
if (length(missing)) stop("missing input table(s) in ", in_dir, ": ", paste(missing, collapse = ", "))
log_step(sprintf("re-levelling %s | POP_SOURCE=%s%s POP_METHOD=%s | %s", in_dir, POP_SOURCE,
                 if (nzchar(POP_YEAR)) paste0(" POP_YEAR=", POP_YEAR) else "", POP_METHOD,
                 if (APPLY) "APPLY" else "DRY RUN (set APPLY=1 to write)"))

tabs <- lapply(paths, function(p) as.data.table(read_parquet(p)))
names(tabs) <- names(files)

# legacy tables (pre-#28): the existing pop columns are the raw gridded sums
upgrade_legacy <- function(dt, exposed = TRUE) {
  if (!"pop_total_grid" %in% names(dt) && "pop_total" %in% names(dt)) {
    dt[, pop_total_grid := as.numeric(pop_total)]
  }
  if (exposed && !"pop_exposed_grid" %in% names(dt) && "pop_exposed" %in% names(dt)) {
    dt[, pop_exposed_grid := as.numeric(pop_exposed)]
  }
  dt
}
tabs$A <- upgrade_legacy(tabs$A)
tabs$B <- upgrade_legacy(tabs$B)
tabs$totals <- upgrade_legacy(tabs$totals, exposed = FALSE)
for (nm in names(tabs)) {
  need <- c("adm1_pcode", "pop_total_grid")
  miss <- setdiff(need, names(tabs[[nm]]))
  if (length(miss)) stop(sprintf("table %s lacks %s — cannot re-level; re-run 7_zonal_exposure.R",
                                 files[[nm]], paste(miss, collapse = ", ")))
}

# gridded county totals come from the per-adm2 denominators, which are static
grid_adm1 <- unique(tabs$totals[, .(adm2_pcode, adm1_pcode, pop_total_grid)])[
  , .(grid_pop = sum(pop_total_grid, na.rm = TRUE)), by = adm1_pcode]
knbs <- knbs_county_totals(exp_root, POP_SOURCE, POP_YEAR, POP_METHOD, POP_BASE_YEAR)
pop_label  <- knbs$label
pop_method <- knbs$method
scale_dt <- pop_scale_table(grid_adm1, knbs$totals, pop_label, log_step)

relevel <- function(dt, exposed = TRUE) {
  drop <- intersect(c("pop_scale_adm1", "pop_scale_census", "pop_growth_county"), names(dt))
  if (length(drop)) dt[, (drop) := NULL]
  dt <- scale_dt[dt, on = "adm1_pcode"]
  dt[, `:=`(pop_total = pop_total_grid * pop_scale_adm1,
            pop_source = pop_label, pop_method = pop_method,
            pop_grid_source = POP_GRID_SOURCE)]
  if (exposed) {
    dt[, `:=`(pop_exposed = pop_exposed_grid * pop_scale_adm1,
              pop_pct = fifelse(pop_total_grid > 0, pop_exposed_grid / pop_total_grid, NA_real_))]
  }
  # the join puts the key columns last; restore the engine's leading key order
  setcolorder(dt, intersect(c("adm2_pcode", "adm1_pcode", "adm2_name", "adm1_name"), names(dt)))
  dt[]
}
before <- vapply(tabs, function(d) if ("pop_exposed" %in% names(d)) sum(d$pop_exposed, na.rm = TRUE)
                 else sum(d$pop_total, na.rm = TRUE), numeric(1))
tabs$A <- relevel(tabs$A)
tabs$B <- relevel(tabs$B)
tabs$totals <- relevel(tabs$totals, exposed = FALSE)
after <- vapply(tabs, function(d) if ("pop_exposed" %in% names(d)) sum(d$pop_exposed, na.rm = TRUE)
                else sum(d$pop_total, na.rm = TRUE), numeric(1))

for (nm in names(tabs)) {
  log_step(sprintf("  %-28s %12.0f -> %12.0f  (x%.4f)", files[[nm]], before[[nm]], after[[nm]],
                   after[[nm]] / before[[nm]]))
}
nat <- sum(tabs$totals$pop_total)
log_step(sprintf("  national pop_total now %.0f [%s / %s]", nat, pop_label, pop_method))
if (!is.null(knbs$totals) && abs(nat - sum(knbs$totals$knbs_pop)) > 1) {
  stop(sprintf("national total %.0f does not match the %s table (%.0f)", nat, pop_label,
               sum(knbs$totals$knbs_pop)))
}

if (!APPLY) {
  log_step("DRY RUN — nothing written. Re-run with APPLY=1 to rewrite the tables in place.")
  quit(save = "no")
}
for (nm in names(tabs)) write_parquet(tabs[[nm]], file.path(in_dir, files[[nm]]))
log_step(sprintf("WROTE -> %s (%s)", in_dir, paste(files, collapse = ", ")))
cat("\nNext: republish with  Rscript R/observational/6_publish_obs_to_s3.R --full --tier 16 --overwrite\n")
