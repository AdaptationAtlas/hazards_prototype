#!/usr/bin/env Rscript
# R/checks/probe_054_intld_guard.R
# ================================
# Read-only measurement for issue #54: FAOSTAT's "Gross Production Value (constant 2014-2016
# thousand I$)" holds a placeholder for a few livestock rows (LBY and SLE cattle meat = 1, LSO
# chicken meat = 2, SWZ sheep meat = 0; flag E, every year), so R/0.4.1 §4.2 filed those countries'
# constant-I$ VoP at about $1,000 or $0 against a nominal VoP in the tens of millions. Since
# d4d7c3f, §4.2 runs R/price_fill.R::livestock_intld_guard(): a row whose nominal / constant-I$
# ratio is beyond BASIS_BAND_DEFAULT x the item's cross-country median, or whose constant-I$ is <= 0
# while nominal > 0, takes nominal / item-median ratio (intld_source = "basis fallback").
#
# This probe runs 0.4.1 sections a-b, 1, 2.3 (the glw2atlas map only) and 3-4.2 exactly as the
# script does (the GLW raster sections are skipped; nothing in the evaluated range writes), then
# compares, per (country, product) and year set:
#   OLD  what §4.2 computed before the guard: median(constant-I$ over the window) x 1000
#   NEW  what §4.2 computes now (the guard applied), with intld_source
#
# GATE 0 (reproduction): OLD must equal the `value` column of the CSV the last 0.4.1 run wrote,
#   <glw_pro_dir>/fao_vop/glw4-2020_vop_intld15-2021-prices-used.csv, per (iso3, atlas_name).
#   If it does not, the probe is not measuring what is published: STOP and report.
#
# Why national factors are enough: §5 distributes each country's VoP over its GLW pixels by share
# and splits highland/tropical by mask, so every published intld livestock row (reference and
# vop_intld15-21 tier) of a flagged (iso3, glw3 class) scales by NEW/OLD, up to per-pixel rounding,
# and every other row is untouched. The gate R/checks/gate_054_reference_diff.R reads this probe's
# CSV for the expected NEW per (iso3, glw3_name).
#
# Usage (cglabs, repo root, a few minutes; EXPOSURE_RES only picks the grid section b loads):
#   EXPOSURE_RES=0.25 Rscript R/checks/probe_054_intld_guard.R [--top 30] [--out /abs/path.csv]
# --out writes the per-(year set, iso3, product) table; default writes nothing.

t0 <- Sys.time()
.ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) cat(sprintf("[%s] [probe-054] %s\n", .ts(), sprintf(fmt, ...)))
args <- commandArgs(trailingOnly = TRUE)
opt  <- function(x, d) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
TOP <- as.integer(opt("--top", "30"))
OUT <- opt("--out", "")
if (!nzchar(Sys.getenv("EXPOSURE_RES"))) Sys.setenv(EXPOSURE_RES = "0.25")

setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
.log("sourcing %s", setup); suppressMessages(suppressWarnings(source(setup)))
suppressPackageStartupMessages({ library(data.table); library(terra) })
source(file.path(project_dir, "R", "price_fill.R"))
if (!exists("livestock_intld_guard")) stop("R/price_fill.R has no livestock_intld_guard(): this checkout predates the #54 fix (d4d7c3f)")

script <- file.path(project_dir, "R", "0.4.1_create_livestock_exposure.R")
src <- readLines(script)
cut <- function(re) { i <- grep(re, src)[1]; if (is.na(i)) stop("section marker not found in 0.4.1: ", re); i }
c2  <- cut("^# 2\\) Load GLW4 data"); c23 <- cut("^## 2\\.3\\) map GLW values"); c24 <- cut("^# 2\\.4\\) Load high/low mask")
c3  <- cut("^# 3\\) Load FAOstat data"); c43 <- cut("^## 4\\.3\\) Not Run")
stopifnot(c2 < c23, c23 < c24, c24 < c3, c3 < c43)
if (!any(grepl("livestock_intld_guard\\(", src[c3:c43]))) stop("0.4.1 §4.2 does not call livestock_intld_guard(): this checkout predates the #54 fix")
run_lines <- function(a, b) {
  for (e in parse(text = src[a:b], keep.source = FALSE)) {
    txt <- paste(deparse(e), collapse = " ")
    if (grepl("0_server_setup", txt, fixed = TRUE)) next     # already sourced, and cwd has moved
    if (grepl("fwrite|writeRaster|ensure_dir", txt)) stop("probe would evaluate a write: ", substr(txt, 1, 120))
    eval(e, envir = globalenv())
  }
}
.log("evaluating %s lines 1-%d, %d-%d, %d-%d (GLW raster sections skipped)", basename(script), c2 - 1, c23, c24 - 1, c3, c43 - 1)
run_lines(1, c2 - 1)
SCALARS <- c("dataset_name", "glw_pro_dir")   # plain name constants from the skipped band (see probe_041)
for (nm in SCALARS) {
  li <- c2 - 1 + grep(paste0("^", nm, " <- "), src[c2:(c23 - 1)])
  if (length(li) != 1) stop("expected one top-level `", nm, " <- ` in 0.4.1 lines ", c2, "-", c23 - 1, "; found ", length(li))
  .log("from skipped section 2: line %d  %s", li, src[li]); run_lines(li, li)
}
run_lines(c23, c24 - 1); run_lines(c3, c43 - 1)
stopifnot(exists("nominal_usd"), exists("intd_2015"), exists("prod_merge"), exists("year_sets"), exists("glw2atlas"))
if (!"intld_source" %in% names(intd_2015[[1]])) stop("§4.2 output has no intld_source column: the guard did not run")

## FAO inputs -------------------------------------------------------------------
.log("--- FAO inputs")
for (f in c(vop_file, prod_file)) .log("%-48s mtime %s  size %.0f MB", basename(f), format(file.mtime(f), "%Y-%m-%d"), file.size(f) / 1e6)
.log("guard constants: BASIS_BAND_DEFAULT = %g, BASIS_MIN_N_DEFAULT = %g", BASIS_BAND_DEFAULT, BASIS_MIN_N_DEFAULT)

## Per year set: OLD vs NEW ------------------------------------------------------------
tabs <- rbindlist(lapply(seq_along(year_sets), function(i) {
  old <- prod_merge[year %in% year_sets[[i]], .(intld_old = median(value_intd15, na.rm = TRUE) * 1000), .(atlas_name, iso3)]
  new <- as.data.table(intd_2015[[i]])[, .(iso3, atlas_name, vop_usd_nominal, intld_new = vop_intd15, intld_source, basis_ratio, basis_median)]
  d <- merge(old, new, by = c("iso3", "atlas_name"), all = TRUE)
  d <- merge(d, as.data.table(nominal_usd[[i]])[, .(iso3, atlas_name, production_t, price_source)], by = c("iso3", "atlas_name"), all.x = TRUE)
  d <- merge(d, glw2atlas, by = "atlas_name", all.x = TRUE)
  d[, factor := intld_new / intld_old]
  d[, year_set := names(year_sets)[i]]
  d
}))
p <- tabs[year_set == "y2021"]
bn <- function(x) sprintf("%.3f B", x / 1e9)

## GATE 0: OLD reproduces what 0.4.1 last wrote ------------------------------------------
live_csv <- file.path(glw_pro_dir, "fao_vop", paste0(dataset_name, "_vop_intld15-2021-prices-used.csv"))
if (!file.exists(live_csv)) stop("GATE 0: live prices-used CSV not found: ", live_csv)
.log("--- GATE 0: OLD vs %s (mtime %s)", basename(live_csv), format(file.mtime(live_csv), "%Y-%m-%d %H:%M"))
live <- unique(fread(live_csv)[, .(iso3, atlas_name, value_live = value)])
if ("intld_source" %in% names(fread(live_csv, nrows = 1))) .log("NOTE: the live CSV already carries intld_source - 0.4.1 has run with the guard since the last publish; GATE 0 then compares OLD against guarded values and will FAIL on the flagged rows by design")
g0 <- merge(p[, .(iso3, atlas_name, intld_old)], live, by = c("iso3", "atlas_name"), all = TRUE)
g0[, rel := abs(intld_old - value_live) / pmax(abs(value_live), 1)]
bad0 <- g0[(is.na(intld_old) != is.na(value_live)) | (!is.na(rel) & rel > 1e-9)]
.log("GATE 0: %d rows; live total %s, probe OLD total %s; %d rows differ", nrow(g0), bn(sum(g0$value_live, na.rm = TRUE)), bn(sum(g0$intld_old, na.rm = TRUE)), nrow(bad0))
if (nrow(bad0) > 0) {
  print(bad0[order(-rel)][1:min(TOP, .N)])
  stop("GATE 0 FAILED: OLD does not reproduce the published livestock constant-I$ VoP. Stop and report.")
}
.log("GATE 0 PASS: OLD reproduces the published values")

## The measurement, y2021 ----------------------------------------------------------------
.log("--- y2021 totals (Africa, all countries)")
tot <- p[, .(old = sum(intld_old, na.rm = TRUE), new = sum(intld_new, na.rm = TRUE), nominal = sum(vop_usd_nominal, na.rm = TRUE))]
.log("livestock constant-I$: OLD %s | NEW %s (NEW/OLD %.5f, +%s) | nominal for scale %s", bn(tot$old), bn(tot$new), tot$new / tot$old, bn(tot$new - tot$old), bn(tot$nominal))
.log("--- intld_source, y2021")
print(p[, .(n = .N, intld_bn = round(sum(intld_new, na.rm = TRUE) / 1e9, 4)), by = intld_source][order(-n)])
.log("--- item medians of nominal / constant-I$ (the guard's reference), y2021")
print(unique(p[!is.na(basis_median), .(atlas_name, basis_median = signif(basis_median, 4))])[order(atlas_name)])
.log("--- flagged rows (basis fallback), y2021")
fl <- p[intld_source == "basis fallback"][order(atlas_name, iso3)]
print(fl[, .(iso3, atlas_name, glw3_name, production_t, nominal_M = round(vop_usd_nominal / 1e6, 2), intld_old, intld_new_M = round(intld_new / 1e6, 2), basis_ratio = signif(basis_ratio, 3), basis_median = signif(basis_median, 3), factor = signif(factor, 4))])
.log("--- invariants")
.log("every non-flagged row unchanged: %s (%d rows differ)", all(p[intld_source != "basis fallback" | is.na(intld_source), isTRUE(all.equal(intld_old, intld_new)) || (is.na(intld_old) & is.na(intld_new))]), p[!(intld_source %in% "basis fallback") & !((is.na(intld_old) & is.na(intld_new)) | abs(intld_old - intld_new) <= 1e-9 * pmax(abs(intld_old), 1)), .N])
.log("every flagged row: nominal > 0, intld_new > 0, intld_new == nominal / basis_median: %s", all(fl$vop_usd_nominal > 0 & fl$intld_new > 0 & abs(fl$intld_new - fl$vop_usd_nominal / fl$basis_median) < 1e-6))
.log("flagged rows with intld_old <= 0 or NA: %d; with intld_old > 0 (ratio beyond band): %d", fl[intld_old <= 0 | is.na(intld_old), .N], fl[intld_old > 0, .N])
.log("rows with intld NA on both sides (no production): %d", p[is.na(intld_old) & is.na(intld_new), .N])
.log("--- per (iso3, glw3 class) factor NEW/OLD for the reference diff gate (the four pairs expected: LBY cattle, SLE cattle, LSO poultry, SWZ sheep)")
fac <- p[, .(old = sum(intld_old, na.rm = TRUE), new = sum(intld_new, na.rm = TRUE)), by = .(iso3, glw3_name)][, factor := new / old]
print(fac[abs(factor - 1) > 1e-9 | !is.finite(factor)][order(iso3, glw3_name)])
.log("--- other year sets")
for (ys in setdiff(unique(tabs$year_set), "y2021")) {
  q <- tabs[year_set == ys]
  .log("%s: OLD %s -> NEW %s; flagged: %s", ys, bn(sum(q$intld_old, na.rm = TRUE)), bn(sum(q$intld_new, na.rm = TRUE)),
       paste(q[intld_source == "basis fallback", paste(iso3, atlas_name)], collapse = ", "))
}
if (nzchar(OUT)) { dir.create(dirname(OUT), showWarnings = FALSE, recursive = TRUE); fwrite(tabs[order(year_set, atlas_name, iso3)], OUT); .log("wrote %s (%d rows)", OUT, nrow(tabs)) }
.log("done in %.1f min", as.numeric(difftime(Sys.time(), t0, units = "mins")))
