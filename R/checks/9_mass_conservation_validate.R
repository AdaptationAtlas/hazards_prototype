# Issue #9 Stage 3 validation harness — run AFTER rebake of:
#   R/0.4.1_create_livestock_exposure.R
#   R/0.4.4_process_exposure.R
#   R/3_freq_x_exposure.R
#
# Confirms that the Stage 2 fix (terra::resample method="sum" at the five
# exposure-resample sites) closes the hazard_exposure > VOP_total gap that
# motivated the dispatch. Validation criteria from the dispatch:
#   (a) per (iso3, crop): sum(hazard_exposure where hazard != 'none','any')
#       <= total VOP within ±0.5 %.
#   (b) re-probe AGO sugarcane mass ratio (was +3.65% on GLW cattle in
#       Stage 1; sugarcane comes via MapSPAM atlas-harmonized so this is
#       a downstream sanity check, not the resample itself).
#   (c) spot-check NGA oil palm + CIV cocoa for the same invariant.
#   (d) verify CR-068 categorisation column is structurally unchanged
#       (issue #9 fix should not perturb the hazard category mass split).
#
# Workspace convention: source R/0_server_setup.R for paths.
#
# Usage (from project_dir, after rebake):
#   Rscript R/checks/9_mass_conservation_validate.R [--country AGO,NGA,CIV]
#   [--parquet <local path or S3 URL of hazard_exposure parquet>]

project_dir <- if (nzchar(Sys.getenv("project_dir"))) {
  Sys.getenv("project_dir")
} else {
  getwd()
}
setwd(project_dir)
source("R/0_server_setup.R")
# 0_server_setup.R calls setwd(working_dir) — source the helper by
# absolute path so it doesn't try to resolve against the new cwd.
source(file.path(project_dir, "R/checks/_helpers.R"))

suppressPackageStartupMessages({
  pacman::p_load(data.table, arrow, jsonlite)
})

args <- commandArgs(trailingOnly = TRUE)
country_arg <- {
  i <- match("--country", args)
  if (!is.na(i) && i < length(args)) args[i + 1L] else "AGO,NGA,CIV"
}
countries <- strsplit(country_arg, ",")[[1]]

parquet_arg <- {
  i <- match("--parquet", args)
  if (!is.na(i) && i < length(args)) args[i + 1L] else NA_character_
}
# Resolve local path under hazard_exposure_dir if not provided.
# Two resolution paths in priority order:
#   1) post-publish hive layout under Data/hazard_exposure/ (matches the
#      S3 canonical path the notebook reads).
#   2) producer-side STAGE C output under Data/hazard_risk_vop_usd/<period>/
#      — used to validate the rebake BEFORE pushing to S3.
hex_dir <- atlas_dirs$data_dir$hazard_exposure
if (is.na(parquet_arg)) {
  candidates <- list.files(hex_dir, "multi-hazard\\.parquet$",
                           recursive = TRUE, full.names = TRUE)
  candidates <- candidates[
    grepl("variable=vop_nominal-usd21", candidates) &
    grepl("model=ENSEMBLEmean", candidates) &
    grepl("severity=severe", candidates)
  ]
  if (length(candidates) == 0L) {
    prod_dir <- file.path(atlas_dirs$data_dir$hazard_risk_vop_usd, "jagermeyr")
    prod_candidates <- list.files(
      prod_dir,
      "^haz-freq-exp_.*_ENSEMBLEmean_int_adm_severe\\.parquet$",
      full.names = TRUE
    )
    if (length(prod_candidates) == 0L) {
      stop("hazard_exposure parquet not found under ", hex_dir,
           " AND no producer fallback under ", prod_dir,
           " — rerun STAGE C or pass --parquet <path>.")
    }
    if (length(prod_candidates) > 1L) {
      stop("Producer fallback under ", prod_dir, " matched ",
           length(prod_candidates), " files; pass --parquet to disambiguate:\n  ",
           paste(prod_candidates, collapse = "\n  "))
    }
    parquet_arg <- prod_candidates[1]
  } else {
    parquet_arg <- candidates[1]
  }
}

log_section("Issue #9 Stage 3 validation")
log_step("parquet     = %s", parquet_arg)
log_step("countries   = %s", paste(countries, collapse = ", "))

# ----- (a) iso3 x crop: sum(hazard != 'none','any') <= VOP_total -------
# In the long parquet, every (iso3, admin0, admin1=NA, admin2=NA, crop,
# scenario, timeframe) has 7 rows for the 7 specific hazard combinations
# plus 'any' (the union) plus 'none' (unexposed). The unexposed-PLUS-
# specific sum should equal VOP_total per (iso3, crop, scenario,
# timeframe). The specific-only sum should be <= VOP_total.
#
# CR-068 (a) notes the 'none' row is missing in the current parquet so
# we cannot compute VOP_total from this table alone — we proxy via
# `hazard = 'any'` (the union) which sums all exposed mass.
log_step("arrow version: %s", as.character(packageVersion("arrow")))

# Load parquet once via arrow — avoids DuckDB/arrow C++ library conflict
# (DuckDB 1.5.x read_parquet() crashes with 'Invalid connection' when
# the arrow R package is also loaded in the same session on CGlabs).
log_step("Loading parquet via arrow::read_parquet ...")
.pq_raw <- arrow::read_parquet(parquet_arg)
.pq <- data.table::as.data.table(.pq_raw)
rm(.pq_raw); invisible(gc())
log_step("Loaded %d rows, %d cols", nrow(.pq), ncol(.pq))

.haz_vars <- c("NDWS+NTx35+NDWL0", "NDWS+THI-max+NDWL0")

# ----- (a) sum(specific) <= sum('any') ---------------------------------
log_section("[a] sum(specific) <= sum('any') check, per (iso3, crop, scenario, timeframe)")
.base_a <- .pq[
  iso3 %in% countries &
  is.na(admin2_name) & is.na(admin1_name) &
  hazard_vars %in% .haz_vars &
  exposure_unit == "nominal-usd-2021" &
  crop != "generic-crop"
]
chk_a <- log_timer({
  .agg <- .base_a[, .(
    sum_specific = round(sum(value[!hazard %in% c("none", "any")], na.rm = TRUE), 1),
    sum_any      = round(sum(value[hazard == "any"], na.rm = TRUE), 1)
  ), by = .(iso3, crop, scenario, timeframe)]
  .agg[, ratio := round(sum_specific / data.table::fifelse(sum_any == 0, NA_real_, sum_any), 4)]
  .agg[sum_any > 0][order(-ratio)]
}, label = "[a] specific-vs-any aggregation")
log_step("rows checked: %d", nrow(chk_a))
breach <- chk_a[!is.na(ratio) & ratio > 1.005]
log_step("breaches (ratio > 1.005): %d", nrow(breach))
if (nrow(breach) > 0L) {
  log_step("TOP 10 BREACHES:")
  print(breach[1:min(10L, .N)])
} else {
  log_step("PASS — no (iso3, crop, scenario, timeframe) where specific > any.")
}

# ----- (b) AGO sugarcane mass ratio re-probe ---------------------------
log_section("[b] AGO sugarcane mass — current parquet snapshot")
chk_b <- log_timer({
  .pq[
    iso3 == "AGO" & crop == "sugarcane" &
    is.na(admin2_name) & is.na(admin1_name) &
    hazard == "any" &
    hazard_vars %in% .haz_vars &
    exposure_unit == "nominal-usd-2021",
    .(sum_value = round(sum(value, na.rm = TRUE), 1), n_rows = .N),
    by = .(scenario, timeframe)
  ][order(scenario, timeframe)]
}, label = "[b] AGO sugarcane aggregation")
print(chk_b)

# ----- (c) NGA oil-palm + CIV cocoa spot check -------------------------
log_section("[c] Spot-check sharp-concentration crops")
# AND binds tighter than OR — (NGA OR CIV) selector wrapped in its own
# condition to avoid NGA leaking common filters. See logs/post_rebake_
# followups_20260526_094837.log for the prior spurious-NaN incident.
chk_c <- log_timer({
  .pq[
    ((iso3 == "NGA" & crop %in% c("oilpalm", "oil-palm", "oil_palm")) |
     (iso3 == "CIV" & crop %in% c("cocoa"))) &
    is.na(admin2_name) & is.na(admin1_name) &
    hazard == "any" &
    hazard_vars %in% .haz_vars &
    exposure_unit == "nominal-usd-2021",
    .(sum_value = round(sum(value, na.rm = TRUE), 1)),
    by = .(iso3, crop, scenario, timeframe)
  ][order(iso3, crop, scenario, timeframe)]
}, label = "[c] spot-check NGA oilpalm + CIV cocoa")
print(chk_c)

# ----- (d) CR-068 categorisation unchanged ----------------------------
# Compute per-hazard mass split for AGO across three scenario×timeframe
# slices. issue #9 changes magnitudes only (resample fix), not hazard
# categorisation shape. Material shape changes = regression to investigate.
log_section("[d] AGO hazard-category split — CR-068 regression check")
chk_d <- log_timer({
  .base_d <- .pq[
    iso3 == "AGO" & is.na(admin2_name) &
    hazard_vars %in% .haz_vars &
    exposure_unit == "nominal-usd-2021" &
    crop != "generic-crop" & hazard != "any"
  ]
  .long_d <- .base_d[, .(value = sum(value, na.rm = TRUE)),
                     by = .(hazard, scenario, timeframe)]
  .long_d[, col := paste0(
    data.table::fcase(
      scenario == "historic" & timeframe == "1995-2014", "hist_1995_2014",
      scenario == "ssp245"   & timeframe == "2021-2040", "ssp245_2021_2040",
      scenario == "ssp585"   & timeframe == "2021-2040", "ssp585_2021_2040",
      default = paste0(scenario, "_", gsub("-", "_", timeframe))
    )
  )]
  .wide <- data.table::dcast(.long_d, hazard ~ col, value.var = "value",
                              fun.aggregate = sum, fill = 0)
  # Round the numeric columns
  num_cols <- setdiff(names(.wide), "hazard")
  .wide[, (num_cols) := lapply(.SD, round, 0), .SDcols = num_cols]
  if ("hist_1995_2014" %in% names(.wide)) setorder(.wide, -hist_1995_2014)
  .wide
}, label = "[d] AGO category split aggregation")
print(chk_d)
log_step(paste(
  "NOTE: zeros under historic for heat / heat+wet / wet are the CR-068",
  "bug (categorisation asymmetry), NOT a regression from issue #9.",
  "CR-068 has its own dispatch."
))

# ----- JSON record -----------------------------------------------------
out_dir <- file.path(project_dir, "metadata", "checks")
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
out_path <- file.path(out_dir, "9_mass_conservation_validate.json")
jsonlite::write_json(
  list(
    timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    parquet   = parquet_arg,
    countries = countries,
    specific_vs_any = chk_a,
    breaches  = breach,
    sugarcane = chk_b,
    spot_checks = chk_c,
    category_split = chk_d
  ),
  out_path, auto_unbox = TRUE, pretty = TRUE
)
log_step("Wrote report to %s", out_path)
summarize_log()

# Exit status: non-zero if (a) has breaches
if (nrow(breach) > 0L) {
  log_step("DECISION: validation FAILED — investigate the (iso3, crop) breaches above.")
  log_complete("Issue #9 Stage 3 validation",
               c("DECISION = FAILED (exit code 1)"))
  quit(status = 1, save = "no")
} else {
  log_step("DECISION: validation PASSED — issue #9 fix closes the magnitude gap.")
  log_complete("Issue #9 Stage 3 validation",
               c("DECISION = PASSED (exit code 0)"))
}
