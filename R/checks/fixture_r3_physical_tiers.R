#!/usr/bin/env Rscript
# R/checks/fixture_r3_physical_tiers.R
# ====================================
# #41 / rebake item 8 (decided in for #13, 2026-10-07). Synthetic probe of the two physical hazard
# tiers R/3 now produces - crop production tonnes (prod_t) and livestock head counts (head_n) -
# on a 4 x 4 hazard grid with a nested 20 x 20 exposure grid. No data needed, seconds. Fails
# loudly on any assertion.
#
# Why these tiers: R/3 multiplies hazard frequency by VALUE rasters, so every price decision forces
# a 0.4.x -> R/3 re-bake at both resolutions (the 2026-10 price pass cost four node re-runs). R/3 is
# linear and prices are national, so frequency x tonnes summed per unit x the national price is the
# same number. With a physical tier published, a price revision is a raster multiply or a table join.
#
# What is actually at risk, and so what is asserted here:
#   1. the §4.1 dispatch. It picks the exposure surface from WHICH RASTER WAS SUPPLIED, not by
#      matching the variable name (issue #9, 2026-09-15). prod_t is crop-only and head_n is
#      livestock-only, so each must send the other commodity class down the classified
#      NOT_APPLICABLE skip rather than into compareGeom(NULL, ...).
#   2. the grid. SPAM prod_t is native 0.05 deg and untagged while the hazard grid is 0.25 deg, so
#      prod_t takes .align_exposure()'s resample path. Tonnage must survive it - the physical tier
#      is only worth having if it conserves mass.
#   3. the unit token. §4.2 derives the parquet's exposure_unit as field 2 of the variable name.
#
# Usage: Rscript R/checks/fixture_r3_physical_tiers.R
suppressPackageStartupMessages({ library(data.table); library(terra) })
root <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else {
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd()
}
source(file.path(root, "R", "_helpers.R"))
ok <- function(cond, msg) { if (!isTRUE(cond)) stop("FIXTURE FAIL: ", msg) else cat("  ok  ", msg, "\n") }

## Pull the two functions under test out of R/3 without running it (R/3's top level needs setup,
## the live Data/ tree and hours of inputs). Same trick as R/checks/probe_040_allocation.R.
r3 <- file.path(root, "R", "3_freq_x_exposure.R")
want <- c(".align_exposure", "risk_x_exposure")
got <- character(0)
for (e in parse(r3)) {
  if (is.call(e) && length(e) >= 3 && as.character(e[[1]])[1] %in% c("<-", "=") &&
      is.name(e[[2]]) && as.character(e[[2]]) %in% want) { eval(e, envir = globalenv()); got <- c(got, as.character(e[[2]])) }
}
ok(identical(sort(got), sort(want)), "both §4.1 functions lifted out of R/3_freq_x_exposure.R")

## ---------------------------------------------------------------- grids + exposure
hz <- rast(nrows = 4, ncols = 4, xmin = 0, xmax = 1, ymin = 0, ymax = 1, crs = "EPSG:4326")   # hazard, 0.25-shaped
fine <- rast(nrows = 20, ncols = 20, xmin = 0, xmax = 1, ymin = 0, ymax = 1, crs = "EPSG:4326")  # exposure, 0.05-shaped
mk <- function(g, v) { r <- rast(g); values(r) <- v; r }

# Totals chosen to divide evenly across the cells: the tiers carry round_n = 0, so a product that
# does not land on whole numbers per cell loses a little to rounding and conservation holds only to
# that, not exactly. Real head counts and tonnages are rounded the same way.
MAIZE_T <- 4000; WHEAT_T <- 1000; CATTLE_N <- 960
crop_prod <- c(mk(fine, MAIZE_T / 400), mk(fine, WHEAT_T / 400)); names(crop_prod) <- c("maize", "wheat")
lvst_no   <- c(mk(hz, CATTLE_N / 16));                            names(lvst_no)   <- "cattle"
tmp <- file.path(tempdir(), "r3_phys"); dir.create(tmp, showWarnings = FALSE)
prod_path <- file.path(tmp, "spam_prod_t_all.tif");  writeRaster(crop_prod, prod_path, overwrite = TRUE)
lvst_path <- file.path(tmp, "livestock_number_number_res-25.tif"); writeRaster(lvst_no, lvst_path, overwrite = TRUE)

## hazard frequency = 1 everywhere, so the product's total IS the exposure total and any mass lost
## on the regrid shows up directly in the assertion below.
in_dir <- file.path(tmp, "in"); dir.create(in_dir, showWarnings = FALSE)
haz <- function(commodity) {
  f <- file.path(in_dir, sprintf("%s_ENSEMBLEmean_historic_NDWS_severe_int.tif", commodity))
  writeRaster(mk(hz, 1), f, overwrite = TRUE); f
}
f_maize <- haz("maize"); f_cattle <- haz("cattle")
crop_choices <- c("maize", "wheat", "generic-crop")
out <- function(sub) { d <- file.path(tmp, sub); dir.create(d, showWarnings = FALSE); d }
tot <- function(f) global(rast(f), "sum", na.rm = TRUE)[1, 1]

## ---------------------------------------------------------------- prod_t: crop-only
d_prod <- out("prod")
r1 <- risk_x_exposure(file = f_maize, save_dir = d_prod, variable = "prod_t", overwrite = TRUE,
                      crop_exposure_path = prod_path, livestock_exposure_path = NULL,
                      crop_choices = crop_choices, round_n = 0)
f_out <- file.path(d_prod, "maize_ENSEMBLEmean_historic_NDWS_severe_int_prod_t.tif")
ok(file.exists(f_out), "prod_t: a crop commodity multiplies and writes")
ok(abs(tot(f_out) / MAIZE_T - 1) < 1e-6,
   sprintf("prod_t: tonnage survives the 0.05 -> 0.25 regrid (%.0f t in, %.0f t out)", MAIZE_T, tot(f_out)))

r2 <- risk_x_exposure(file = f_cattle, save_dir = d_prod, variable = "prod_t", overwrite = TRUE,
                      crop_exposure_path = prod_path, livestock_exposure_path = NULL,
                      crop_choices = crop_choices, round_n = 0)
ok(is.character(r2) && grepl("^NOT_APPLICABLE", r2),
   "prod_t: a livestock commodity returns the classified skip, not an error (livestock have no tonnage surface)")
ok(!file.exists(file.path(d_prod, "cattle_ENSEMBLEmean_historic_NDWS_severe_int_prod_t.tif")),
   "prod_t: the skipped livestock pair writes no file, so §4.2 cannot read a phantom tier")

## ---------------------------------------------------------------- head_n: livestock-only
d_n <- out("n")
r3a <- risk_x_exposure(file = f_cattle, save_dir = d_n, variable = "head_n", overwrite = TRUE,
                       crop_exposure_path = NULL, livestock_exposure_path = lvst_path,
                       crop_choices = crop_choices, round_n = 0)
f_n <- file.path(d_n, "cattle_ENSEMBLEmean_historic_NDWS_severe_int_head_n.tif")
ok(file.exists(f_n) && abs(tot(f_n) / CATTLE_N - 1) < 1e-6, "head_n: a livestock commodity multiplies and conserves head count")
r3b <- risk_x_exposure(file = f_maize, save_dir = d_n, variable = "head_n", overwrite = TRUE,
                       crop_exposure_path = NULL, livestock_exposure_path = lvst_path,
                       crop_choices = crop_choices, round_n = 0)
ok(is.character(r3b) && grepl("^NOT_APPLICABLE", r3b), "head_n: a crop commodity returns the classified skip (crops have no head count)")

## ---------------------------------------------------------------- the regrid itself
a <- .align_exposure(crop_prod$maize, hz, "fixture")
ok(compareGeom(a, hz, stopOnError = FALSE), ".align_exposure lands the exposure on the hazard grid")
ok(abs(global(a, "sum", na.rm = TRUE)[1, 1] / MAIZE_T - 1) < 1e-6,
   ".align_exposure conserves mass on the exactly-nested path (aggregate, not GDAL warp)")
ok(identical(.align_exposure(mk(hz, 2), hz, "same"), mk(hz, 2)), "an exposure already on the hazard grid is returned untouched")

## ---------------------------------------------------------------- the §4.2 unit token
u <- function(v) unlist(data.table::tstrsplit(v, "_", keep = 2))
ok(identical(u("prod_t"), "t") && identical(u("head_n"), "n"),
   "§4.2 derives exposure_unit = field 2 of the variable name: prod_t -> t, head_n -> n")
ok(identical(u("harv-area_ha"), "ha") && identical(u("vop_nominal-usd-2021"), "nominal-usd-2021"),
   "the same rule still gives the established tiers their units")

## ---------------------------------------------------------------- the wiring in R/3 itself
src <- readLines(r3)
ok(any(grepl("^do_prod <- TRUE", src)) && any(grepl("^do_n <- TRUE", src)), "both physical tiers are switched ON in R/3")
ok(any(grepl("to_do_list\\$prod <- list", src)), "R/3 builds a to_do_list entry for the production tier")
ok(any(grepl("hazard_risk_prod", readLines(file.path(root, "R", "0_server_setup.R")))) &&
   any(grepl("hazard_risk_prod", readLines(file.path(root, "R", "00_paths.R")))),
   "hazard_risk_prod is registered in BOTH directory registries (a missing key is a NULL path and ensure_dir writes to the cwd)")

cat("\nALL R/3 PHYSICAL-TIER FIXTURE ASSERTIONS PASSED\n")
