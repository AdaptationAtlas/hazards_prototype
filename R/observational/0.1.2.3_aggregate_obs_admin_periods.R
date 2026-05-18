# 0) Introduction ####
# Aggregate the monthly admin-level observational parquet
# (obs_monthly_adm{0,1}.parquet from R/observational/0.1.2.2_extract_obs_admin.R) into a
# single combined annual + 3-month-seasonal parquet per admin level:
#
#   Data/chirts_chirps_hist/admin/obs_periods_adm{0,1}.parquet
#
# Periods (long form 'period' column):
#   annual            full calendar year (Jan-Dec)
#   JFM FMA MAM AMJ MJJ JJA JAS ASO SON OND NDJ DJF   12 overlapping 3-month
#                                                     seasons; DJF crosses
#                                                     year boundary and
#                                                     attaches to the year
#                                                     containing January.
#
# Per-variable aggregation rule across the months in each period:
#   PTOT                                  -> sum   (total rainfall mm)
#   TMAX                                  -> max   (warmest monthly max)
#   TMIN                                  -> min   (coldest monthly min)
#   TAVG, SPEI-01, SPEI-03, ..., SPEI-24  -> mean
#
# value_mean from the monthly parquet is aggregated according to the rule
# above. value_sd (spatial sd within polygon at each month) is aggregated as
# the simple mean across the months in the period - that gives a 'typical
# spatial heterogeneity' over the season rather than trying to propagate
# variance algebraically (which doesn't have a clean closed form when the
# spatial sd is reported per-layer).
#
# A 3-month season requires data for ALL three of its months; partial windows
# yield NA for that (zone, year, period). The annual period requires all 12.
#
# Run modes:
#   --smoke   adm0 only; runs five inline checks (file written, expected
#             period set, row count plausible, no all-NA admin x period x
#             variable combinations, value-range printout).
#   --full    adm0 + adm1.
#   (none)    Usage + exit 1.
#
# Please run 0_server_setup.R before --full; --smoke uses bootstrap_minimal()
# and skips the pipeline startup downloads.

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
  pacman::p_load(data.table, arrow, glue, jsonlite)

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
    file.exists(file.path(p, "Data/chirts_chirps_hist/admin/obs_monthly_adm0.parquet"))
  }, logical(1))
  working_dir <- if (any(has_data)) candidates[has_data][1] else candidates[1]
  log_step(sprintf("  selected working_dir: %s", working_dir))
  if (!dir.exists(working_dir)) dir.create(working_dir, recursive = TRUE)
  setwd(working_dir)

  chirts_chirps_hist_dir <- file.path("Data", "chirts_chirps_hist")
  list(
    project_dir = project_dir, working_dir = working_dir,
    chirts_chirps_hist_dir = chirts_chirps_hist_dir
  )
}

args <- commandArgs(trailingOnly = TRUE)
mode <- if (length(args) == 0) "" else args[1]

if (mode == "--smoke") {
  paths <- bootstrap_minimal()
  project_dir <- paths$project_dir
  chirts_chirps_hist_dir <- paths$chirts_chirps_hist_dir
} else if (mode == "--full") {
  source("R/0_server_setup.R")
  pacman::p_load(data.table, arrow, glue, jsonlite)
  chirts_chirps_hist_dir <- atlas_dirs$data_dir$chirts_chirps_hist
} else {
  cat(
    "Usage:\n",
    "  Rscript R/observational/0.1.2.3_aggregate_obs_admin_periods.R --smoke\n",
    "      adm0 only; verification checks; exit 0/1.\n",
    "  Rscript R/observational/0.1.2.3_aggregate_obs_admin_periods.R --full\n",
    "      adm0 + adm1.\n",
    sep = ""
  )
  quit(status = 1)
}

# 2) Configuration ####

admin_dir <- file.path(chirts_chirps_hist_dir, "admin")
if (!dir.exists(admin_dir)) {
  stop(glue::glue("admin dir not found: {admin_dir} - run 0.1.2.2 first"))
}

levels_full <- c("adm0", "adm1")
levels_smoke <- c("adm0")
levels_run <- if (mode == "--smoke") levels_smoke else levels_full

# Variable aggregation rule across the months in a period.
agg_rule <- list(
  PTOT      = "sum",
  TMAX      = "max",
  TMIN      = "min",
  TAVG      = "mean",
  `SPEI-01` = "mean",
  `SPEI-03` = "mean",
  `SPEI-06` = "mean",
  `SPEI-12` = "mean",
  `SPEI-24` = "mean"
)

# Seasons: ordered list of 3-month month-vectors. DJF wraps - the months
# are c(12, 1, 2) but we attach the season to the year that contains
# January (so JFM and DJF share a year_value).
seasons <- list(
  annual = 1:12,
  JFM = c(1, 2, 3), FMA = c(2, 3, 4), MAM = c(3, 4, 5), AMJ = c(4, 5, 6),
  MJJ = c(5, 6, 7), JJA = c(6, 7, 8), JAS = c(7, 8, 9), ASO = c(8, 9, 10),
  SON = c(9, 10, 11), OND = c(10, 11, 12), NDJ = c(11, 12, 1),
  DJF = c(12, 1, 2)
)

cat("project_dir  :", project_dir, "\n")
cat("working_dir  :", getwd(), "\n")
cat("input/output :", admin_dir, "\n")
cat("mode         :", mode, "\n")
cat("admin levels :", paste(levels_run, collapse = ", "), "\n\n")

# 3) Helpers ####

#' Aggregate one variable's monthly rows to one row per (zone, year, period).
#' Returns a long data.table with value_mean (per agg_rule) and value_sd
#' (mean of monthly value_sds).
aggregate_one <- function(dt_var, var) {
  rule <- agg_rule[[var]]
  if (is.null(rule)) stop(glue::glue("No aggregation rule for variable '{var}'"))

  agg_fun <- switch(rule,
    sum  = function(x) if (any(is.na(x))) NA_real_ else sum(x),
    max  = function(x) if (any(is.na(x))) NA_real_ else max(x),
    min  = function(x) if (any(is.na(x))) NA_real_ else min(x),
    mean = function(x) if (any(is.na(x))) NA_real_ else mean(x),
    stop(glue::glue("Unknown agg rule '{rule}'"))
  )

  # NA-strict by design: a 3-month season needs all three months; a missing
  # month makes the season NA. Same for annual needing all 12.
  zone_cols <- intersect(
    c(
      "iso3", "admin0_name", "admin1_name", "admin2_name",
      "gaul0_code", "gaul1_code", "gaul2_code"
    ),
    names(dt_var)
  )

  out_list <- lapply(names(seasons), function(period) {
    months <- seasons[[period]]
    expected_n <- length(months)
    dt_p <- dt_var[month %in% months]
    if (nrow(dt_p) == 0L) {
      return(NULL)
    }
    # DJF / NDJ: assign December rows to the next calendar year so the
    # season aligns to the year containing January.
    if (period %in% c("NDJ", "DJF")) {
      dt_p <- copy(dt_p)
      dt_p[month == 12L, year := year + 1L]
      if (period == "NDJ") dt_p <- dt_p[month %in% c(11, 12, 1)]
    }
    by_cols <- c(zone_cols, "year")
    agg <- dt_p[, .(
      value_mean = agg_fun(value_mean),
      value_sd   = if (any(is.na(value_sd))) NA_real_ else mean(value_sd),
      n_months   = .N
    ), by = by_cols]
    # Drop incomplete windows: need exactly expected_n months present.
    agg <- agg[n_months == expected_n]
    agg[, n_months := NULL]
    agg[, period := period]
    agg
  })
  out <- data.table::rbindlist(out_list, use.names = TRUE)
  out[, variable := var]
  out
}

# 4) Process per admin level ####

written <- character()
for (lvl in levels_run) {
  in_path <- file.path(admin_dir, sprintf("obs_monthly_%s.parquet", lvl))
  out_path <- file.path(admin_dir, sprintf("obs_periods_%s.parquet", lvl))

  if (!file.exists(in_path)) {
    stop(glue::glue("Missing input: {in_path}. Run R/observational/0.1.2.2_extract_obs_admin.R first."))
  }
  if (file.exists(out_path) && file.size(out_path) > 100L) {
    log_step(sprintf(
      "=== %s: %s already present, skipping (delete to rebuild)",
      lvl, basename(out_path)
    ))
    written <- c(written, out_path)
    next
  }

  log_step(sprintf("=== %s ===", lvl))
  log_step(sprintf("  reading %s", in_path))
  monthly <- arrow::read_parquet(in_path) |> data.table::as.data.table()
  # Convert factor columns back to character for grouping.
  for (c in c("iso3", "admin0_name", "admin1_name", "admin2_name", "variable")) {
    if (c %in% names(monthly) && is.factor(monthly[[c]])) {
      monthly[, (c) := as.character(get(c))]
    }
  }
  log_step(sprintf(
    "  monthly rows: %d; variables: %s",
    nrow(monthly), paste(unique(monthly$variable), collapse = ", ")
  ))

  vars <- intersect(names(agg_rule), unique(monthly$variable))
  per_var <- vector("list", length(vars))
  for (j in seq_along(vars)) {
    v <- vars[j]
    t0 <- Sys.time()
    per_var[[j]] <- aggregate_one(monthly[variable == v], v)
    log_step(sprintf(
      "  [%d/%d] %s -> %d rows in %.1fs",
      j, length(vars), v, nrow(per_var[[j]]),
      as.numeric(Sys.time() - t0, units = "secs")
    ))
  }
  combined <- data.table::rbindlist(per_var, use.names = TRUE, fill = TRUE)

  combined[, variable := factor(variable, levels = names(agg_rule))]
  combined[, period := factor(period, levels = names(seasons))]
  for (c in c("iso3", "admin0_name", "admin1_name", "admin2_name")) {
    if (c %in% names(combined)) combined[, (c) := as.factor(get(c))]
  }
  col_order <- c(
    "iso3", "admin0_name", "admin1_name", "admin2_name",
    "gaul0_code", "gaul1_code", "gaul2_code",
    "year", "period", "variable", "value_mean", "value_sd"
  )
  col_order <- intersect(col_order, names(combined))
  data.table::setcolorder(combined, col_order)
  data.table::setorderv(combined, intersect(
    c("iso3", "admin1_name", "year", "period", "variable"), names(combined)
  ))

  tbl <- arrow::arrow_table(combined)
  tbl <- tbl$ReplaceSchemaMetadata(list(
    description = sprintf(
      "Annual + 12 3-month-seasonal observational aggregates per %s polygon.", lvl
    ),
    source = sprintf(
      "R/observational/0.1.2.3_aggregate_obs_admin_periods.R from %s", basename(in_path)
    ),
    aggregation_rule = paste(
      sprintf("%s=%s", names(agg_rule), unlist(agg_rule)),
      collapse = ", "
    ),
    periods = paste(names(seasons), collapse = ", "),
    n_rows = as.character(nrow(combined)),
    build_time = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    build_script = "R/observational/0.1.2.3_aggregate_obs_admin_periods.R"
  ))
  arrow::write_parquet(tbl, out_path, compression = "zstd", compression_level = 9)
  log_step(sprintf(
    "  wrote %s (%.1f MB)",
    out_path, file.info(out_path)$size / 1024 / 1024
  ))
  written <- c(written, out_path)

  jsonlite::write_json(list(
    file = basename(out_path),
    admin_level = lvl,
    periods = names(seasons),
    aggregation_rule = agg_rule,
    n_rows = nrow(combined),
    build_time = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    parent_script = "R/observational/0.1.2.3_aggregate_obs_admin_periods.R"
  ), path = paste0(out_path, ".json"), pretty = TRUE, auto_unbox = TRUE)
}

# 5) Smoke verification ####

if (mode == "--smoke") {
  log_step("=== VERIFICATION CHECKS ===")
  pass <- TRUE
  out_path <- written[1]

  if (file.exists(out_path) && file.size(out_path) > 100L) {
    cat(sprintf(
      "[OK] 1. Parquet written: %s (%.1f MB)\n",
      out_path, file.info(out_path)$size / 1024 / 1024
    ))
  } else {
    cat(sprintf("[FAIL] 1. Parquet missing: %s\n", out_path))
    pass <- FALSE
  }

  back <- arrow::read_parquet(out_path) |> data.table::as.data.table()

  periods_found <- as.character(unique(back$period))
  if (setequal(periods_found, names(seasons))) {
    cat(sprintf("[OK] 2. All %d periods present.\n", length(seasons)))
  } else {
    cat(sprintf(
      "[FAIL] 2. Period set mismatch. Found: %s\n",
      paste(periods_found, collapse = ", ")
    ))
    pass <- FALSE
  }

  zone_cols <- intersect(c("gaul2_code", "gaul1_code", "gaul0_code"), names(back))
  n_zones <- uniqueN(back[, zone_cols, with = FALSE])
  n_periods <- uniqueN(back$period)
  n_vars <- uniqueN(back$variable)
  n_years <- uniqueN(back$year)
  expected_max <- n_zones * n_periods * n_vars * n_years
  # Annual + 12 seasons; not every year-period combo will be complete at the
  # edges of coverage (esp. DJF at the very first year). Expect actual to be
  # close to but below expected_max.
  ratio <- nrow(back) / expected_max
  if (ratio > 0.85 && ratio <= 1.0) {
    cat(sprintf(
      "[OK] 3. Row count %d (%.1f%% of %d zones x periods x vars x years).\n",
      nrow(back), 100 * ratio, expected_max
    ))
  } else {
    cat(sprintf(
      "[FAIL] 3. Row count %d is %.1f%% of max %d - too low or over.\n",
      nrow(back), 100 * ratio, expected_max
    ))
    pass <- FALSE
  }

  na_combos <- back[, .(all_na = all(is.na(value_mean))),
    by = .(variable, period)
  ][all_na == TRUE]
  if (nrow(na_combos) == 0L) {
    cat("[OK] 4. No (variable x period) combinations are entirely NA.\n")
  } else {
    cat(sprintf(
      "[FAIL] 4. %d (variable x period) combinations all-NA:\n",
      nrow(na_combos)
    ))
    print(na_combos)
    pass <- FALSE
  }

  rng <- back[, .(
    min = min(value_mean, na.rm = TRUE),
    max = max(value_mean, na.rm = TRUE)
  ), by = .(variable, period)][period %in% c("annual", "JFM", "JJA")]
  cat("[OK] 5. Sample value ranges (annual, JFM, JJA):\n")
  print(rng)

  if (!pass) {
    cat("\n=== SMOKE TEST FAILED ===\n")
    quit(status = 1)
  }
  cat("\n=== SMOKE TEST PASSED ===\n")
  quit(status = 0)
}

log_step(sprintf("Full aggregation complete. %d parquet files written.", length(written)))
for (p in written) cat("  ", p, "\n")
