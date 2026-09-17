# _population_helpers.R — official population denominators for the KE-39 exposure tables (issue #28).
#
# Source me after 0_server_setup.R:
#   source(file.path(project_dir, "R", "observational", "_population_helpers.R"))
#
# WHY THIS EXISTS. Both gridded population surfaces run ~17% above the enumerated KNBS 2019 census
# (WorldPop constrained ~55.2 M, GRID3 ~55.9 M, census 47,564,296), so an absolute headcount taken
# straight off a grid is ~17% above what a Kenyan user checks it against. And KNBS projects forward
# (53.33 M in 2025, 57.81 M in 2030) while the grid is a 2020 snapshot. Both corrections are county
# (adm1) facts, because county is the only level where KNBS and COD-AB agree unit-for-unit: COD-AB
# adm2 is 290 IEBC constituencies, the census reports 345 KNBS sub-counties, and only 183 names
# match — see python/ingest_population_knbs_census.py.
#
# THE METHOD, STATED IN FULL. Every published sub-county population is
#
#     pop_adm2 = pop_adm2_grid  x  pop_scale_census  x  pop_growth_county
#                (100 m share)     (level fix)          (change over time)
#
#   pop_scale_census  = census_county_2019 / gridded_county
#         Replaces the LEVEL of the gridded surface with the enumerated census, county by county.
#         Constant in time. ~0.855 nationally, but MEASURED 0.324-1.394 across counties on the
#         real WorldPop grid (cglabs, 2026-09-17) - a much wider spread than the national figure
#         suggests, so aggregate change for any subset depends on WHERE that subset's people are.
#   pop_growth_county = target_county_total / census_county_2019
#         The county's proportional change over time. 1.0 for the census itself.
#
# The share of a county's people living in each sub-county is held FIXED at the gridded 2020
# distribution. Every sub-county in a county therefore moves by exactly its county's percentage
# change: this carries no sub-county-specific growth (a fast-urbanising sub-county and a
# depopulating one in the same county grow at the same rate here). That is a deliberate constraint,
# not an oversight — KNBS does not project below county, so any sub-county differential would be
# invented. Two ways to set pop_growth_county, chosen with POP_METHOD:
#
#   POP_METHOD=county-growth  (default for projections; census-anchored)
#       pop_growth_county = projection_county(year) / projection_county(base year, default 2020)
#       The enumerated census stays the level; KNBS supplies only the SHAPE of change. National 2025
#       lands at ~51.96 M rather than the published 53.33 M, because it does not import the ~2.6%
#       step KNBS puts between census night (Aug 2019) and its own 2020 base — the census total is
#       carried forward as if it were the base-year level. Conservative and census-anchored.
#
#   POP_METHOD=county-level   (matches the published projection exactly)
#       pop_growth_county = projection_county(year) / census_county_2019
#       The county total IS the official published projection, so national 2025 = 53,330,978 —
#       the number a Kenyan counterpart would quote. The 2019-to-base-year step is included, which
#       also means the census anchor is superseded by KNBS's own base-level revision.
#
# Exports:
#   read_pop_tbl(stem)                        - read <stem>.parquet, or .csv (pyarrow-less hosts)
#   knbs_county_totals(exp_root, source, year, method, base_year)
#                                             - per-county target totals + the decomposition
#   pop_scale_table(grid_adm1, knbs, label, log_fn)
#                                             - adm1_pcode, pop_scale_adm1, pop_scale_census,
#                                               pop_growth_county
#
# Used by 7_zonal_exposure.R (compute) and 7b_relevel_exposure_pop.R (re-level published tables
# without re-running the ~3 h zonal engine).

suppressPackageStartupMessages({library(data.table); library(arrow)})

POP_PROJECTION_BASE_YEAR <- 2020L   # first year of the KNBS Vol XVI projection series

#' Read an ingest output, whichever form it was written in.
#' The python ingests write parquet, or CSV when pyarrow is missing on the host.
read_pop_tbl <- function(stem) {
  hits <- paste0(stem, c(".parquet", ".csv"))
  hits <- hits[file.exists(hits)]
  if (!length(hits)) return(NULL)
  if (grepl("\\.parquet$", hits[1])) as.data.table(read_parquet(hits[1])) else fread(hits[1])
}

#' Census county totals (the level anchor). Errors with the ingest command if absent.
census_county_totals <- function(exp_root) {
  stem <- file.path(exp_root, "knbs_census", "population_knbs_census_adm1")
  tbl <- read_pop_tbl(stem)
  if (is.null(tbl)) {
    stop("no KNBS census table at ", stem,
         " — run: python3 python/ingest_population_knbs_census.py")
  }
  tbl[, .(adm1_pcode, census_pop = as.numeric(pop_total))]
}

#' Target county totals for the requested denominator, with the method decomposed.
#'
#' @param exp_root   exposure root holding knbs_census/ and knbs_projections/
#' @param pop_source "knbs-census-2019", "knbs-projection" or "grid"
#' @param pop_year   projection year (required for "knbs-projection"): 2020-2035, 2040 or 2045
#' @param pop_method "county-growth" (census-anchored, default) or "county-level" (published total)
#' @param base_year  growth base year for "county-growth" (default 2020, the series base)
#' @return list(totals = data.table(adm1_pcode, census_pop, growth_ratio, knbs_pop) or NULL for
#'              "grid", label = character, method = character)
knbs_county_totals <- function(exp_root, pop_source = "knbs-census-2019", pop_year = "",
                               pop_method = "county-growth",
                               base_year = POP_PROJECTION_BASE_YEAR) {
  if (pop_source == "grid") {
    return(list(totals = NULL, label = "worldpop-constrained-2020", method = "none"))
  }
  if (pop_source == "knbs-census-2019") {
    cen <- census_county_totals(exp_root)
    cen[, `:=`(growth_ratio = 1, knbs_pop = census_pop)]
    return(list(totals = cen[], label = "knbs-census-2019", method = "county-level"))
  }
  if (pop_source == "knbs-projection") {
    if (!nzchar(pop_year)) {
      stop("POP_SOURCE=knbs-projection needs POP_YEAR (2020-2035, 2040 or 2045)")
    }
    if (!pop_method %in% c("county-growth", "county-level")) {
      stop("POP_METHOD must be county-growth or county-level (got '", pop_method, "')")
    }
    stem <- file.path(exp_root, "knbs_projections", "population_knbs_projections_adm1_totals")
    tbl <- read_pop_tbl(stem)
    if (is.null(tbl)) {
      stop("POP_SOURCE=knbs-projection but no projection table at ", stem,
           " — run: python3 python/ingest_population_knbs_projections.py")
    }
    yr <- as.integer(pop_year)
    sel <- tbl[year == yr, .(adm1_pcode, proj_pop = as.numeric(pop_total))]
    if (!nrow(sel)) {
      stop(sprintf("POP_YEAR=%s is not a published projection year (have %s)", pop_year,
                   paste(sort(unique(tbl$year)), collapse = ", ")))
    }
    cen <- census_county_totals(exp_root)
    out <- sel[cen, on = "adm1_pcode"]
    if (out[is.na(proj_pop), .N]) {
      stop(sprintf("no %d projection for %d counties: %s", yr, out[is.na(proj_pop), .N],
                   paste(out[is.na(proj_pop), adm1_pcode], collapse = ", ")))
    }
    if (pop_method == "county-level") {
      # target IS the published projection; growth absorbs the 2019-to-base-year step
      out[, `:=`(knbs_pop = proj_pop, growth_ratio = proj_pop / census_pop)]
      label <- paste0("knbs-projection-", yr)
    } else {
      base <- tbl[year == base_year, .(adm1_pcode, base_pop = as.numeric(pop_total))]
      if (!nrow(base)) {
        stop(sprintf("POP_BASE_YEAR=%d is not a published projection year (have %s)", base_year,
                     paste(sort(unique(tbl$year)), collapse = ", ")))
      }
      out <- base[out, on = "adm1_pcode"]
      if (out[is.na(base_pop) | base_pop <= 0, .N]) {
        stop("missing or zero base-year projection for: ",
             paste(out[is.na(base_pop) | base_pop <= 0, adm1_pcode], collapse = ", "))
      }
      # census stays the level; KNBS supplies only the shape of change since the base year
      out[, `:=`(growth_ratio = proj_pop / base_pop)]
      out[, knbs_pop := census_pop * growth_ratio]
      label <- sprintf("knbs-projection-%d", yr)
    }
    return(list(totals = out[, .(adm1_pcode, census_pop, growth_ratio, knbs_pop)],
                label = label,
                method = if (pop_method == "county-level") "county-level"
                         else sprintf("county-growth-from-%d", base_year)))
  }
  stop("POP_SOURCE must be knbs-census-2019, knbs-projection or grid (got '", pop_source, "')")
}

#' Per-county factors, decomposed so the method is readable off the published table.
#'   pop_scale_census  = census county total / gridded county total  (level fix, no time component)
#'   pop_growth_county = target county total / census county total   (change over time; 1 = census)
#'   pop_scale_adm1    = the product actually applied to the gridded figures
#'
#' @param grid_adm1 data.table(adm1_pcode, grid_pop) - gridded population summed per county
#' @param knbs      data.table(adm1_pcode, census_pop, growth_ratio, knbs_pop) or NULL
#' @param label     denominator label, for the log line
#' @param log_fn    logger taking one string
pop_scale_table <- function(grid_adm1, knbs, label, log_fn = message) {
  if (is.null(knbs)) {
    return(grid_adm1[, .(adm1_pcode, pop_scale_adm1 = 1, pop_scale_census = 1,
                         pop_growth_county = 1)])
  }
  scale_dt <- knbs[grid_adm1, on = "adm1_pcode"]
  missing <- scale_dt[is.na(knbs_pop), adm1_pcode]
  if (length(missing)) {
    stop(sprintf("no %s total for %d counties: %s", label, length(missing),
                 paste(missing, collapse = ", ")))
  }
  if (scale_dt[grid_pop <= 0, .N]) {
    stop("gridded county total is zero for: ",
         paste(scale_dt[grid_pop <= 0, adm1_pcode], collapse = ", "))
  }
  scale_dt[, `:=`(pop_scale_census  = census_pop / grid_pop,
                  pop_growth_county = knbs_pop / census_pop,
                  pop_scale_adm1    = knbs_pop / grid_pop)]
  log_fn(sprintf("  pop denominator: %s | national %.0f (grid %.0f, x%.3f) | county factors %.3f-%.3f",
                 label, sum(scale_dt$knbs_pop), sum(scale_dt$grid_pop),
                 sum(scale_dt$knbs_pop) / sum(scale_dt$grid_pop),
                 min(scale_dt$pop_scale_adm1), max(scale_dt$pop_scale_adm1)))
  log_fn(sprintf("    decomposed: census/grid x%.4f (constant) x county growth %.4f-%.4f (national %.4f)",
                 sum(scale_dt$census_pop) / sum(scale_dt$grid_pop),
                 min(scale_dt$pop_growth_county), max(scale_dt$pop_growth_county),
                 sum(scale_dt$knbs_pop) / sum(scale_dt$census_pop)))
  scale_dt[, .(adm1_pcode, pop_scale_adm1, pop_scale_census, pop_growth_county)]
}
