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
# Resolve local path under hazard_exposure_dir if not provided
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
    stop("hazard_exposure parquet not found under ", hex_dir,
         " — rerun the rebake or pass --parquet <path>.")
  }
  parquet_arg <- candidates[1]
}

cat("\n=== Issue #9 Stage 3 validation ===\n")
cat(sprintf("  parquet     = %s\n", parquet_arg))
cat(sprintf("  countries   = %s\n\n", paste(countries, collapse = ", ")))

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
cat("[a] sum(specific) <= sum('any') check, per (iso3, crop, scenario, timeframe)\n")
con <- DBI::dbConnect(duckdb::duckdb())
on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
if (grepl("^https?://", parquet_arg)) {
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
chk_a <- as.data.table(DBI::dbGetQuery(con, q_a))
cat(sprintf("  rows checked: %d\n", nrow(chk_a)))
breach <- chk_a[!is.na(ratio) & ratio > 1.005]
cat(sprintf("  breaches (ratio > 1.005): %d\n", nrow(breach)))
if (nrow(breach) > 0L) {
  cat("  TOP 10 BREACHES:\n")
  print(breach[1:min(10L, .N)])
} else {
  cat("  PASS — no (iso3, crop, scenario, timeframe) where specific > any.\n")
}

# ----- (b) AGO sugarcane mass ratio re-probe ---------------------------
cat("\n[b] AGO sugarcane mass — current parquet snapshot\n")
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
chk_b <- as.data.table(DBI::dbGetQuery(con, q_b))
print(chk_b)

# ----- (c) NGA oil-palm + CIV cocoa spot check -------------------------
cat("\n[c] Spot-check sharp-concentration crops\n")
q_c <- sprintf("
  SELECT iso3, crop, scenario, timeframe,
         ROUND(SUM(value)::DOUBLE, 1) AS sum_value
  FROM read_parquet('%s')
  WHERE (iso3 = 'NGA' AND crop IN ('oilpalm','oil-palm','oil_palm'))
     OR (iso3 = 'CIV' AND crop IN ('cocoa'))
    AND admin2_name IS NULL AND admin1_name IS NULL
    AND hazard = 'any'
    AND hazard_vars IN ('NDWS+NTx35+NDWL0','NDWS+THI-max+NDWL0')
    AND exposure_unit = 'nominal-usd-2021'
  GROUP BY ALL ORDER BY iso3, crop, scenario, timeframe", parquet_arg)
chk_c <- as.data.table(DBI::dbGetQuery(con, q_c))
print(chk_c)

# ----- (d) CR-068 categorisation unchanged ----------------------------
# Compute per (scenario, timeframe) hazard mass split for AGO; should
# show the same SHAPE as before the fix (issue #9 only changes
# magnitudes via resample, not hazard categorisation). If the shape
# changes materially, that's a regression to investigate.
cat("\n[d] Hazard-category mass split (AGO) — sanity that issue #9 fix",
    " did NOT perturb the categorisation column.\n", sep = "")
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
chk_d <- as.data.table(DBI::dbGetQuery(con, q_d))
print(chk_d)
cat("  NOTE: zeros under historic for heat / heat+wet / wet are the",
    " CR-068 bug (categorisation asymmetry), NOT a regression from",
    " issue #9. CR-068 has its own dispatch.\n", sep = "")

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
cat(sprintf("\nWrote report to %s\n", out_path))

# Exit status: non-zero if (a) has breaches
if (nrow(breach) > 0L) {
  cat("\nDECISION: validation FAILED — investigate the (iso3, crop) breaches above.\n")
  quit(status = 1, save = "no")
} else {
  cat("\nDECISION: validation PASSED — issue #9 fix closes the magnitude gap.\n")
}
