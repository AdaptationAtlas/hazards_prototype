# 2026-09-15_validate_knbs_denominator.R — exercise the issue-#28 denominator logic against the REAL repo code and the
# REAL KNBS tables. The helper is sourced as-is; the two blocks inside 7_zonal_exposure.R that the
# helper cannot cover (totals assembly, section A assembly) are EXTRACTED from the script itself, so
# this probe fails if that code drifts. Stubs stand in only for terra/sf.
suppressPackageStartupMessages({library(data.table); library(arrow)})

#
# Needs only the two KNBS ingest outputs, not the pipeline data — point EXP_ROOT at the directory
# holding knbs_census/ and knbs_projections/ (on cglabs: <working_dir>/Data/exposure):
#   EXP_ROOT=$PWD/Data/exposure Rscript scripts/2026-09-15_validate_knbs_denominator.R
REPO   <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else getwd()
SCRIPT <- file.path(REPO, "R/observational/7_zonal_exposure.R")
SP     <- Sys.getenv("EXP_ROOT", "")
if (!nzchar(SP)) stop("set EXP_ROOT to the exposure root holding knbs_census/ and knbs_projections/")
src    <- readLines(SCRIPT)

grab <- function(from, to) {
  i <- grep(from, src)[1]; j <- grep(to, src); j <- j[j > i][1]
  stopifnot(!is.na(i), !is.na(j))
  cat(sprintf("  extracted lines %d-%d\n", i, j))
  src[i:j]
}
blk_totals <- grab('^setnames\\(totals, "pop_total", "pop_total_grid"\\)',
                   'sum\\(totals\\$health_n_total\\), sum\\(totals\\$schools_n_total\\)\\)\\)')
blk_A <- grab("^A <- totals\\[, \\.\\(adm2_pcode, area_km2", "^A <- A\\[, \\.\\.Acols\\]")

log_step <- function(m) cat("   [script] ", m, "\n", sep = "")
source(file.path(REPO, "R/observational/_population_helpers.R"))
exp_root <- SP                       # holds knbs_census/ + knbs_projections/ from the ingests

# 47 counties x 2 synthetic adm2; gridded pops deliberately ~17% over the census
census <- read_pop_tbl(file.path(SP, "knbs_census", "population_knbs_census_adm1"))
stopifnot(!is.null(census))
set.seed(1)
key_dt <- rbindlist(lapply(seq_len(nrow(census)), function(i) data.table(
  adm1_pcode = census$adm1_pcode[i], adm2_pcode = paste0(census$adm1_pcode[i], c("a", "b")),
  adm2_name = c("a", "b"), adm1_name = census$adm1_name[i])))
key_dt[, adm2_idx := .I]
frac <- runif(nrow(census), 0.3, 0.7)
grid_county <- census$pop_total * 1.17
pop_by_idx <- data.table(adm2_idx = key_dt$adm2_idx,
                         pop_total = as.numeric(rbind(grid_county * frac, grid_county * (1 - frac))))
grid_adm1 <- pop_by_idx[key_dt, on = "adm2_idx"][, .(grid_pop = sum(pop_total, na.rm = TRUE)),
                                                 by = adm1_pcode]

cat("\n-- helper: census --\n")
POP_GRID_SOURCE <- "worldpop-constrained-2020"
knbs <- knbs_county_totals(exp_root, "knbs-census-2019", "")
pop_label  <- knbs$label
pop_method <- knbs$method
scale_dt <- pop_scale_table(grid_adm1, knbs$totals, pop_label, log_step)
stopifnot(nrow(scale_dt) == 47, all(abs(scale_dt$pop_scale_adm1 - 1 / 1.17) < 1e-9),
          all(scale_dt$pop_growth_county == 1),                       # census = no time factor
          all(abs(scale_dt$pop_scale_census - scale_dt$pop_scale_adm1) < 1e-12))

cat("\n-- totals block (extracted from 7_zonal_exposure.R) --\n")
totals <- pop_by_idx[key_dt, on = "adm2_idx"]
totals[, `:=`(area_km2 = 100, roads_km_total = 1, grid_km_total = 1,
              health_n_total = 1L, schools_n_total = 1L)]
eval(parse(text = paste(blk_totals, collapse = "\n")))
nat <- sum(totals$pop_total)
cat(sprintf("   national pop_total %.0f vs census 47,564,296 (diff %.2f)\n", nat, nat - 47564296))
stopifnot(abs(nat - 47564296) < 1,
          all(abs(totals$pop_total - totals$pop_total_grid * totals$pop_scale_adm1) < 1e-6),
          unique(totals$pop_source) == "knbs-census-2019")

cat("\n-- section A block (extracted from 7_zonal_exposure.R) --\n")
A <- key_dt[, .(adm2_pcode, adm1_pcode, adm2_name, adm1_name)]
A[, `:=`(season = "MAM", year = 2024L, flooded_km2 = 5, observed_km2 = 80,
         pop_exposed = totals$pop_total_grid * 0.1,
         roads_km_exposed = 1, health_n_exposed = 1L, schools_n_exposed = 1L,
         grid_km_exposed = 1, grid_km_exposed_hv = 1)]
eval(parse(text = paste(blk_A, collapse = "\n")))
cat(sprintf("   A national pop_exposed %.0f (grid %.0f, ratio %.4f)\n",
            sum(A$pop_exposed), sum(A$pop_exposed_grid), sum(A$pop_exposed) / sum(A$pop_exposed_grid)))
stopifnot(all(abs(A$pop_pct - 0.1) < 1e-9),                     # share is scale-invariant
          all(abs(A$pop_exposed - A$pop_exposed_grid / 1.17) < 1e-6),
          abs(sum(A$pop_exposed) - 0.1 * 47564296) < 1,
          all(c("pop_exposed_grid", "pop_total_grid", "pop_scale_adm1", "pop_scale_census",
                "pop_growth_county", "pop_method", "pop_grid_source") %in% names(A)))

cat("\n-- helper: the two growth methods --\n")
# county-level: the county total IS the published projection
lvl <- knbs_county_totals(exp_root, "knbs-projection", "2025", "county-level")
stopifnot(lvl$label == "knbs-projection-2025", lvl$method == "county-level",
          abs(sum(lvl$totals$knbs_pop) - 53330978) < 50)
cat(sprintf("   county-level  2025 national %.0f (published 53,330,978 before county rounding)\n",
            sum(lvl$totals$knbs_pop)))
# county-growth: census level carried forward by each county's own % change since the base year
gro <- knbs_county_totals(exp_root, "knbs-projection", "2025", "county-growth")
stopifnot(gro$method == "county-growth-from-2020",
          sum(gro$totals$knbs_pop) < sum(lvl$totals$knbs_pop),      # excludes the 2019->2020 step
          abs(sum(gro$totals$knbs_pop) - 51964059) < 2000)
cat(sprintf("   county-growth 2025 national %.0f (census 47,564,296 x county growth)\n",
            sum(gro$totals$knbs_pop)))
# growth is genuinely per-county, not one national factor
stopifnot(diff(range(gro$totals$growth_ratio)) > 0.05)
cat(sprintf("   county growth ratios 2025 span %.4f-%.4f (national %.4f)\n",
            min(gro$totals$growth_ratio), max(gro$totals$growth_ratio),
            sum(gro$totals$knbs_pop) / sum(gro$totals$census_pop)))
# base year is a fixed point: growth = 1, so the census level comes back exactly
b20 <- knbs_county_totals(exp_root, "knbs-projection", "2020", "county-growth")
stopifnot(all(abs(b20$totals$growth_ratio - 1) < 1e-12),
          abs(sum(b20$totals$knbs_pop) - 47564296) < 1)
# the published decomposition multiplies back to the applied factor
sg <- pop_scale_table(grid_adm1, gro$totals, gro$label, log_step)
stopifnot(all(abs(sg$pop_scale_census * sg$pop_growth_county - sg$pop_scale_adm1) < 1e-12))

cat("\n-- helper: grid + error paths --\n")
kg <- knbs_county_totals(exp_root, "grid", "")
stopifnot(is.null(kg$totals), kg$label == "worldpop-constrained-2020",
          all(pop_scale_table(grid_adm1, kg$totals, kg$label, log_step)$pop_scale_adm1 == 1))
err <- function(expr) tryCatch({expr; ""}, error = function(e) conditionMessage(e))
stopifnot(grepl("POP_SOURCE must be", err(knbs_county_totals(exp_root, "nonsense", ""))),
          grepl("needs POP_YEAR", err(knbs_county_totals(exp_root, "knbs-projection", ""))),
          grepl("not a published projection year", err(knbs_county_totals(exp_root, "knbs-projection", "2037"))),
          grepl("POP_METHOD must be", err(knbs_county_totals(exp_root, "knbs-projection", "2025", "sideways"))),
          grepl("POP_BASE_YEAR=2037 is not a published", err(knbs_county_totals(
            exp_root, "knbs-projection", "2025", "county-growth", 2037L))),
          grepl("no knbs-census-2019 total for", err(pop_scale_table(
            rbind(grid_adm1, data.table(adm1_pcode = "KE099", grid_pop = 10)),
            knbs$totals, "knbs-census-2019", log_step))))

cat("\nPROBE PASSED\n")
