#!/usr/bin/env Rscript
# R/checks/fixture_livestock_price.R
# ==================================
# Synthetic replay of issue #47 through R/price_fill.R::livestock_nominal_price() AND through the
# real text of R/0.4.1_create_livestock_exposure.R section 4.1, run BEFORE the node touches 0.4.1
# (no data needed, seconds). Fails loudly on any assertion.
#   #47      0.4.1 priced every country's livestock at the world median producer price; each
#            country must now carry its own price (KEN cattle meat 3,000, not the world 4,340)
#   clip     a currency artefact (TZA 2021, 46x world) is dropped before the window median
#   fills    longer-series own price, neighbours MEDIAN (not mean), iso3 outside `regions`
#            (singleton region -> continent median), goat milk at the world median by decision
# Usage: Rscript R/checks/fixture_livestock_price.R
suppressPackageStartupMessages(library(data.table))
root <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else { fa <- grep("^--file=", commandArgs(FALSE), value = TRUE); if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd() }
source(file.path(root, "R", "price_fill.R"))
ok <- function(cond, msg) { if (!isTRUE(cond)) stop("FIXTURE FAIL: ", msg) else cat("  ok  ", msg, "\n") }

yrs <- 2019:2023; long <- 2014:2023
neighbors <- list(KEN = c("TZA", "UGA", "ETH"), TZA = c("KEN", "UGA", "MOZ"), UGA = c("KEN", "TZA"), ETH = c("KEN"),
                  ZAF = c("MOZ"), MOZ = c("ZAF", "TZA", "KEN"))
regions <- list(East = c("KEN", "TZA", "UGA", "ETH"), South = c("ZAF", "MOZ"))   # SSD deliberately absent

## upstream-shaped inputs: FAO yearly rows, production in every year, prices where FAO has them
ctry <- c("KEN", "TZA", "UGA", "ETH", "MOZ", "ZAF", "SSD")
pm <- CJ(iso3 = ctry, atlas_name = c("cattle_meat", "goat_milk"), year = 2014:2023)
pm[, production_t := 1000][iso3 == "KEN" & atlas_name == "cattle_meat" & year %in% yrs, production_t := c(1000, 1100, 1200, 1300, 1400)]
pm[, price_usd := NA_real_]
pm[iso3 == "KEN" & atlas_name == "cattle_meat" & year %in% yrs, price_usd := 3000]
pm[iso3 == "TZA" & atlas_name == "cattle_meat" & year == 2019, price_usd := 2500]
pm[iso3 == "TZA" & atlas_name == "cattle_meat" & year == 2021, price_usd := 200000]      # artefact, 46x world
pm[iso3 == "UGA" & atlas_name == "cattle_meat" & year == 2016, price_usd := 3500]        # longer series only
pm[iso3 == "ZAF" & atlas_name == "cattle_meat" & year %in% yrs, price_usd := 6000]
pm[iso3 == "KEN" & atlas_name == "goat_milk" & year %in% yrs, price_usd := 300]           # own, but item uses world
wp <- rbind(CJ(iso3 = c("W1", "W2", "W3"), atlas_name = "cattle_meat", year = 2014:2023)[, price_usd := c(4000, 4340, 5000)[match(iso3, c("W1", "W2", "W3"))]],
            CJ(iso3 = c("W1", "W2", "W3"), atlas_name = "goat_milk", year = 2014:2023)[, price_usd := c(700, 800, 900)[match(iso3, c("W1", "W2", "W3"))]])

## 1) the function ---------------------------------------------------------------------------
lp <- livestock_nominal_price(pm, wp[, .(atlas_name, year, price_usd)], years = yrs, long_years = long, neighbors = neighbors, regions = regions)
d <- lp$data
print(d[order(atlas_name, iso3), .(iso3, atlas_name, production_t, price_usd, price_usd_global, price_usd_final, price_source, vop_usd_nominal)])
g <- function(i, a, col) d[iso3 == i & atlas_name == a][[col]]
ok(nrow(d) == length(ctry) * 2 && !anyDuplicated(d[, .(iso3, atlas_name)]), "one row per (iso3, product) in the window")
ok(all(d[atlas_name == "cattle_meat", price_usd_global] == 4340), "world median per item over the window = 4340 (median of per-country world rows)")
ok(g("KEN", "cattle_meat", "price_usd_final") == 3000 && g("KEN", "cattle_meat", "price_source") == "fao producer price",
   "#47: KEN cattle meat priced at its OWN 3000, not the world 4340")
ok(g("KEN", "cattle_meat", "vop_usd_nominal") == 1200 * 3000, "vop = window-median production (1200) x own price, not x world")
ok(nrow(lp$dropped) == 1 && lp$dropped$iso3 == "TZA" && lp$dropped$year == 2021, "TZA 2021 artefact (46x world) clipped, nothing else")
ok(g("TZA", "cattle_meat", "price_usd_final") == 2500, "TZA own = 2500 after the clip (unclipped window median would be 101,250)")
ok(g("UGA", "cattle_meat", "price_usd_final") == 3500 && g("UGA", "cattle_meat", "price_source") == "fao producer price longer-series", "UGA: longer-series own price")
ok(g("ETH", "cattle_meat", "price_usd_final") == 3000 && g("ETH", "cattle_meat", "price_source") == "neighbours median", "ETH: neighbour fill from KEN")
ok(g("MOZ", "cattle_meat", "price_usd_final") == 3000, "MOZ: neighbours MEDIAN of 6000/2500/3000 = 3000 (a mean would give 3833)")
ok(identical(lp$singletons, "SSD") && g("SSD", "cattle_meat", "price_usd_final") == 3250 && g("SSD", "cattle_meat", "price_source") == "continent median",
   "SSD (not in regions): singleton region, falls to the continent median of own prices (3000/2500/3500/6000 -> 3250)")
ok(all(d[atlas_name == "goat_milk", price_usd_final] == 800) && all(d[atlas_name == "goat_milk", price_source] == "world median (item override)"),
   "goat milk at the world median for every country, own price or not (pre-existing decision)")
ok(all(is.finite(d$vop_usd_nominal)) && all(d$vop_usd_nominal == d$production_t * d$price_usd_final), "every row priced, vop = production x price_usd_final")

## 2) the real text of 0.4.1 section 4.1, on the same inputs --------------------------------------
src <- readLines(file.path(root, "R", "0.4.1_create_livestock_exposure.R"))
a <- grep("^## 4\\.1\\) Nominal usd", src); b <- grep("^## 4\\.2\\)", src)
ok(length(a) == 1 && length(b) == 1 && a < b, "0.4.1 section 4.1 located")
s41 <- src[a:(b - 1)]
ok(!any(grepl("production_t \\* price_usd_global", s41[!grepl("^\\s*#", s41)])), "0.4.1 4.1 no longer multiplies production by the world price")
env <- new.env(parent = globalenv())
env$prod_merge <- copy(pm)
env$prod_price_global <- copy(wp)
env$african_neighbors <- neighbors; env$regions <- regions
env$project_dir <- root; env$dataset_name <- "glw4-2020"
env$.log041 <- function(msg) cat("[0.4.1]", msg, "\n")
for (e in parse(text = s41, keep.source = FALSE)) eval(e, envir = env)
nu <- env$nominal_usd
ok(identical(names(nu), c("glw4-2020_vop_nominal-usd-2021", "glw4-2020_vop_nominal-usd-2015", "glw4-2020_vop_nominal-usd-2020")), "0.4.1 names the three year-set tables as before")
y21 <- nu[["glw4-2020_vop_nominal-usd-2021"]]
ok(all(c("iso3", "atlas_name", "vop_usd_nominal", "price_usd_final", "price_source", "year") %in% names(y21)) && all(y21$year == "y2021"),
   "the y2021 table carries the columns 4.2 and section 5 read (vop_usd_nominal) plus the price audit columns")
cmp <- merge(y21[, .(iso3, atlas_name, v041 = vop_usd_nominal)], d[, .(iso3, atlas_name, vfun = vop_usd_nominal)], by = c("iso3", "atlas_name"))
ok(nrow(cmp) == nrow(d) && isTRUE(all.equal(cmp$v041, cmp$vfun)), "0.4.1's own 4.1 output == the function's output, row for row")
ok(y21[iso3 == "KEN" & atlas_name == "cattle_meat", vop_usd_nominal] == 1200 * 3000, "#47 end to end: 0.4.1 4.1 prices KEN at its own price")
y15 <- nu[["glw4-2020_vop_nominal-usd-2015"]]
ok(y15[iso3 == "UGA" & atlas_name == "cattle_meat", price_source] == "fao producer price" && all(is.finite(y15$vop_usd_nominal)),
   "y2015 window: UGA's 2016 price is in-window own; every row priced")

cat("\nALL LIVESTOCK-PRICE FIXTURE ASSERTIONS PASSED\n")
