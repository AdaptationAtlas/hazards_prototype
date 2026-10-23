#!/usr/bin/env Rscript
# R/checks/fixture_intld_guard.R
# ==============================
# Synthetic replay of issue #54 through R/price_fill.R::livestock_intld_guard() AND through the real
# text of R/0.4.1_create_livestock_exposure.R section 4.2, run BEFORE the node touches 0.4.1 (no
# data needed, seconds). Fails loudly on any assertion.
#   #54      FAOSTAT constant-I$ GPV = 1 thousand I$ (LBY, SLE cattle meat), 2 (LSO chicken), 0 (SWZ
#            sheep) against real production; the row takes nominal / item-median ratio instead
#   untouched a normal row keeps FAOSTAT's value; an item with < min_n usable rows is never changed
#            even when a ratio is wild; both-NA rows stay NA; intld_source states what happened
# Usage: Rscript R/checks/fixture_intld_guard.R
suppressPackageStartupMessages(library(data.table))
root <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else { fa <- grep("^--file=", commandArgs(FALSE), value = TRUE); if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd() }
source(file.path(root, "R", "price_fill.R"))
ok <- function(cond, msg) { if (!isTRUE(cond)) stop("FIXTURE FAIL: ", msg) else cat("  ok  ", msg, "\n") }

## upstream-shaped: one (iso3, product) row per window, nominal from 4.1, constant-I$ from FAOSTAT x 1000
good <- c("KEN", "TZA", "UGA", "ETH", "NGA", "ZAF")
tb <- rbindlist(list(
  data.table(iso3 = good, atlas_name = "cattle_meat", vop_intd15 = c(1040e6, 500e6, 300e6, 900e6, 1200e6, 2000e6),
             vop_usd_nominal = c(1060e6, 400e6, 210e6, 720e6, 1080e6, 1400e6)),              # ratios 1.02 0.80 0.70 0.80 0.90 0.70; with the two placeholders (27,500 / 48,200) the 8-row median is 0.85
  data.table(iso3 = c("LBY", "SLE"), atlas_name = "cattle_meat", vop_intd15 = c(1000, 1000), vop_usd_nominal = c(27.5e6, 48.2e6)),  # the #54 rows
  data.table(iso3 = good[1:5], atlas_name = "sheep_meat", vop_intd15 = c(100e6, 50e6, 20e6, 80e6, 60e6),
             vop_usd_nominal = c(110e6, 50e6, 24e6, 80e6, 66e6)),                               # median 1.10
  data.table(iso3 = "SWZ", atlas_name = "sheep_meat", vop_intd15 = 0, vop_usd_nominal = 0.85e6),  # constant-I$ = 0, production 166 t
  data.table(iso3 = good[1:5], atlas_name = "poultry_meat", vop_intd15 = c(100e6, 50e6, 20e6, 80e6, 60e6),
             vop_usd_nominal = c(170e6, 85e6, 34e6, 136e6, 102e6)),                             # median 1.70
  data.table(iso3 = "LSO", atlas_name = "poultry_meat", vop_intd15 = 2000, vop_usd_nominal = 1.0e6),
  data.table(iso3 = c("KEN", "TZA", "UGA"), atlas_name = "goat_milk", vop_intd15 = c(10e6, 5e6, 1000), vop_usd_nominal = c(17e6, 8.5e6, 3e6)),  # n = 3 < min_n: no reference
  data.table(iso3 = "SSD", atlas_name = "cattle_meat", vop_intd15 = NA_real_, vop_usd_nominal = NA_real_)   # no production
))

## 1) the function ---------------------------------------------------------------------------
ig <- livestock_intld_guard(tb)
d <- ig$data
print(d[order(atlas_name, iso3), .(iso3, atlas_name, vop_usd_nominal, vop_intd15, basis_ratio = signif(basis_ratio, 3), basis_median = signif(basis_median, 3), intld_source)])
g <- function(i, a, col) d[iso3 == i & atlas_name == a][[col]]
ok(nrow(d) == nrow(tb) && !anyDuplicated(d[, .(iso3, atlas_name)]), "one row per (iso3, product), none added or lost")
ok(isTRUE(all.equal(ig$medians[atlas_name == "cattle_meat", basis_median], 0.85)), "cattle meat reference = cross-country MEDIAN ratio 0.85 (a median shrugs off the two placeholder ratios; on the real 50-country table they move it by 0.001)")
ok(identical(sort(paste(ig$flagged$iso3, ig$flagged$atlas_name)), sort(c("LBY cattle_meat", "SLE cattle_meat", "LSO poultry_meat", "SWZ sheep_meat"))),
   "#54: exactly LBY and SLE cattle meat, LSO poultry meat and SWZ sheep meat flagged")
ok(isTRUE(all.equal(g("LBY", "cattle_meat", "vop_intd15"), 27.5e6 / 0.85)) && g("LBY", "cattle_meat", "intld_source") == "basis fallback",
   "LBY cattle meat: 1,000 I$ -> nominal / 0.85 = 32.4 M, source basis fallback")
ok(isTRUE(all.equal(g("SLE", "cattle_meat", "vop_intd15"), 48.2e6 / 0.85)), "SLE cattle meat: 1,000 I$ -> 56.7 M")
ok(isTRUE(all.equal(g("SWZ", "sheep_meat", "vop_intd15"), 0.85e6 / 1.10)) && g("SWZ", "sheep_meat", "intld_source") == "basis fallback",
   "SWZ sheep meat: constant-I$ 0 with nominal > 0 is a placeholder, not zero production -> 0.77 M")
ok(isTRUE(all.equal(g("LSO", "poultry_meat", "vop_intd15"), 1.0e6 / 1.70)), "LSO poultry meat: 2,000 I$ -> 0.59 M")
ok(g("KEN", "cattle_meat", "vop_intd15") == 1040e6 && g("KEN", "cattle_meat", "intld_source") == "fao gpv constant-I$",
   "KEN cattle meat untouched, source fao gpv constant-I$")
ok(all(d[intld_source == "fao gpv constant-I$", vop_intd15] == tb[match(paste(d[intld_source == "fao gpv constant-I$", iso3], d[intld_source == "fao gpv constant-I$", atlas_name]), paste(iso3, atlas_name)), vop_intd15]),
   "every 'fao gpv constant-I$' row carries FAOSTAT's value unchanged (the source column is tested against the values)")
ok(all(d[intld_source == "basis fallback", abs(vop_intd15 - vop_usd_nominal / basis_median) < 1e-6]),
   "every 'basis fallback' row equals nominal / item median (the claim matches the value)")
ok(g("UGA", "goat_milk", "vop_intd15") == 1000 && g("UGA", "goat_milk", "intld_source") == "fao gpv constant-I$",
   "goat milk (3 rows < min_n 5): wild UGA ratio NOT changed, no reference")
ok(is.na(g("SSD", "cattle_meat", "vop_intd15")) && is.na(g("SSD", "cattle_meat", "intld_source")), "SSD both-NA row stays NA with NA source")
ok(all(d[intld_source == "fao gpv constant-I$" & atlas_name == "cattle_meat" & !iso3 %in% c("LBY", "SLE"), basis_ratio <= 4 * 0.85 & basis_ratio >= 0.85 / 4]),
   "no in-band cattle row flagged (band 4)")

## 2) the real text of 0.4.1 section 4.2, on the same inputs --------------------------------------
src <- readLines(file.path(root, "R", "0.4.1_create_livestock_exposure.R"))
a <- grep("^## 4\\.2\\) iusd2015", src); b <- grep("^## 4\\.3\\)", src)
ok(length(a) == 1 && length(b) == 1 && a < b, "0.4.1 section 4.2 located")
s42 <- src[a:(b - 1)]
ok(any(grepl("livestock_intld_guard\\(", s42[!grepl("^\\s*#", s42)])), "0.4.1 4.2 calls livestock_intld_guard()")
env <- new.env(parent = globalenv())
yrs <- 2019:2023
pm <- tb[, .(iso3, atlas_name)][, .(year = yrs), by = .(iso3, atlas_name)]
pm <- merge(pm, tb[, .(iso3, atlas_name, value_intd15 = vop_intd15 / 1000)], by = c("iso3", "atlas_name"))   # FAOSTAT thousand I$, same every year
env$prod_merge <- pm
env$year_sets <- list(y2021 = yrs)
env$nominal_usd <- list(tb[, .(iso3, atlas_name, vop_usd_nominal)])
env$.log041 <- function(msg) cat("[0.4.1]", msg, "\n")
env$dataset_name <- "glw4-2020"
for (e in parse(text = s42, keep.source = FALSE)) eval(e, envir = env)
y21 <- env$intd_2015[[1]]
ok(all(c("iso3", "atlas_name", "vop_intd15", "vop_usd_nominal", "intld_source", "year") %in% names(y21)) && all(y21$year == "y2021"),
   "4.2 output carries vop_intd15 (what section 5 reads for intld15) plus intld_source")
cmp <- merge(y21[, .(iso3, atlas_name, v041 = vop_intd15)], d[, .(iso3, atlas_name, vfun = vop_intd15)], by = c("iso3", "atlas_name"))
ok(nrow(cmp) == nrow(d) && isTRUE(all.equal(cmp$v041, cmp$vfun)), "0.4.1's own 4.2 output == the function's output, row for row")
ok(isTRUE(all.equal(y21[iso3 == "LBY" & atlas_name == "cattle_meat", vop_intd15], 27.5e6 / 0.85)), "#54 end to end: 0.4.1 4.2 gives LBY cattle meat 32.4 M I$, not 1,000")
ok(y21[iso3 == "KEN" & atlas_name == "cattle_meat", vop_intd15] == 1040e6, "end to end: KEN unchanged")

cat("\nALL INTLD-GUARD FIXTURE ASSERTIONS PASSED\n")
