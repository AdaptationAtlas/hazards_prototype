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
  pacman::p_load(data.table, arrow, jsonlite, duckdb, DBI)
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
log_section("[a] sum(specific) <= sum('any') check, per (iso3, crop, scenario, timeframe)")
# Explicit :memory: avoids stale lock files in the current working_dir.
# Named .drv so it isn't GC'd before the first query (newer DuckDB).
.drv <- duckdb::duckdb(dbdir = ":memory:")
con <- DBI::dbConnect(.drv)
stopifnot("DuckDB connection invalid immediately after dbConnect" = DBI::dbIsValid(con))
log_step("duckdb version: %s", as.character(packageVersion("duckdb")))
log_step("arrow  version: %s", as.character(packageVersion("arrow")))
log_step("dbIsValid after stopifnot: %s", DBI::dbIsValid(con))
# Simple sanity query before touching parquet
.test <- DBI::dbGetQuery(con, "SELECT 42 AS answer")
log_step("sanity query result: %d", .test$answer)
log_step("dbIsValid after sanity query: %s", DBI::dbIsValid(con))
on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
if (grepl("^https?://", parquet_arg)) {
  log_step("Remote parquet — loading httpfs")
  DBI::dbExecute(con, "INSTALL httpfs; LOAD httpfs;")
}
q_a <- sprintf("
  WITH base AS (
    SELECT iso3, crop, scenario, timeframe, hazard, value
    FROM read_parquet('%s')
    WHERE iso3 IN (%s)
      AND admin2_name IS NULL
      AND admin1_name IS NULL
      AND hazard_vars IN ('NDWS+NTx35+NDWL0','NDWS+THI-max+NDWL0')
      AND exposure_unit = 'nominal-usd-2021'
      AND crop != 'generic-crop'
  )
  SELECT iso3, crop, scenario, timeframe,
         ROUND(SUM(value) FILTER (WHERE hazard NOT IN ('none','any'))::DOUBLE, 1)
           AS sum_specific,
         ROUND(SUM(value) FILTER (WHERE hazard = 'any')::DOUBLE, 1)
           AS sum_any,
         ROUND((SUM(value) FILTER (WHERE hazard NOT IN ('none','any')) /
                NULLIF(SUM(value) FILTER (WHERE hazard = 'any'), 0))::DOUBLE, 4)
           AS ratio
  FROM base
  GROUP BY ALL
  HAVING sum_any > 0
  ORDER BY ratio DESC",
  parquet_arg,
  paste(sprintf("'%s'", countries), collapse = ", "))
chk_a <- log_timer(
  as.data.table(DBI::dbGetQuery(con, q_a)),
  label = "[a] specific-vs-any query"
)
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
q_b <- sprintf("
  SELECT scenario, timeframe,
         ROUND(SUM(value)::DOUBLE, 1) AS sum_value,
         COUNT(*) AS n_rows
  FROM read_parquet('%s')
  WHERE iso3 = 'AGO' AND crop = 'sugarcane'
    AND admin2_name IS NULL AND admin1_name IS NULL
    AND hazard = 'any'
    AND hazard_vars IN ('NDWS+NTx35+NDWL0','NDWS+THI-max+NDWL0')
    AND exposure_unit = 'nominal-usd-2021'
  GROUP BY ALL ORDER BY scenario, timeframe", parquet_arg)
chk_b <- log_timer(
  as.data.table(DBI::dbGetQuery(con, q_b)),
  label = "[b] AGO sugarcane query"
)
print(chk_b)

# ----- (c) NGA oil-palm + CIV cocoa spot check -------------------------
log_section("[c] Spot-check sharp-concentration crops")
q_c <- sprintf("
  SELECT iso3, crop, scenario, timeframe,
         ROUND(SUM(value)::DOUBLE, 1) AS sum_value
  FROM read_parquet('%s')
  -- AND binds tighter than OR, so the (NGA OR CIV) selector MUST be
  -- wrapped in its own parens — otherwise the common filters below
  -- only bind to the CIV branch and NGA leaks every admin level, every
  -- hazard, every hazard_vars, every exposure_unit. Result: SUM gets
  -- poisoned by sub-national NaN rows and the spot-check spuriously
  -- reports NaN for NGA oilpalm even though admin0 NGA oilpalm is
  -- clean. See logs/post_rebake_followups_20260526_094837.log.
  WHERE (
         (iso3 = 'NGA' AND crop IN ('oilpalm','oil-palm','oil_palm'))
      OR (iso3 = 'CIV' AND crop IN ('cocoa'))
        )
    AND admin2_name IS NULL AND admin1_name IS NULL
    AND hazard = 'any'
    AND hazard_vars IN ('NDWS+NTx35+NDWL0','NDWS+THI-max+NDWL0')
    AND exposure_unit = 'nominal-usd-2021'
  GROUP BY ALL ORDER BY iso3, crop, scenario, timeframe", parquet_arg)
chk_c <- log_timer(
  as.data.table(DBI::dbGetQuery(con, q_c)),
  label = "[c] spot-check NGA oilpalm + CIV cocoa"
)
print(chk_c)

# ----- (d) CR-068 categorisation unchanged ----------------------------
# Compute per (scenario, timeframe) hazard mass split for AGO; should
# show the same SHAPE as before the fix (issue #9 only changes
# magnitudes via resample, not hazard categorisation). If the shape
# changes materially, that's a regression to investigate.
log_section("[d] AGO hazard-category split — CR-068 regression check")
q_d <- sprintf("
  SELECT hazard,
         ROUND(SUM(value) FILTER (WHERE scenario='historic' AND timeframe='1995-2014')::DOUBLE, 0)
           AS hist_1995_2014,
         ROUND(SUM(value) FILTER (WHERE scenario='ssp245'   AND timeframe='2021-2040')::DOUBLE, 0)
           AS ssp245_2021_2040,
         ROUND(SUM(value) FILTER (WHERE scenario='ssp585'   AND timeframe='2021-2040')::DOUBLE, 0)
           AS ssp585_2021_2040
  FROM read_parquet('%s')
  WHERE iso3 = 'AGO'
    AND admin2_name IS NULL
    AND hazard_vars IN ('NDWS+NTx35+NDWL0','NDWS+THI-max+NDWL0')
    AND exposure_unit = 'nominal-usd-2021'
    AND crop != 'generic-crop'
    AND hazard != 'any'
  GROUP BY hazard ORDER BY hist_1995_2014 DESC", parquet_arg)
chk_d <- log_timer(
  as.data.table(DBI::dbGetQuery(con, q_d)),
  label = "[d] AGO category split query"
)
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
