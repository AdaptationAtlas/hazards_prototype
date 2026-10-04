#!/usr/bin/env Rscript
# R/checks/fixture_vop_allocate.R
# ================================
# Synthetic replay of #38 / #39 (and the classify() ID leak) through R/vop_allocate.R, on a
# 6 x 6 grid with three countries. No data needed, seconds. Fails loudly on any assertion.
#   A  "Millet" 100 over pearl 10 t + small 30 t      -> 25 / 75 (#38); coffee 200 over 5 + 15 -> 50 / 150
#      "Bananas" 300, FAO prod 1000 t, no Plantains; SPAM banana 1 t + plantain 999 t (Nigeria-shaped)
#                                                     -> pooled: 0.3 / 299.7 (#39 banana)
#      "Rape or colza seed" 10 + "Mustard seed" 5     -> rapeseed layer 15 (items summed into the crop)
#   B  wheat 80, FAO 1000 t, SPAM 50 t (5 %)          -> guarded NA (#39 Sudan-shaped)
#      maize 50, SPAM 0 t                             -> guarded NA (nothing to distribute onto)
#      sorghum 60, no FAO production row               -> allocated, reason logged (coverage not judged)
#   C  maize: SPAM 20 t, NO GPV row                   -> NA, and NOT the admin ID (classify leak)
# Usage: Rscript R/checks/fixture_vop_allocate.R
suppressPackageStartupMessages({ library(data.table); library(terra) })
root <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else { fa <- grep("^--file=", commandArgs(FALSE), value = TRUE); if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd() }
source(file.path(root, "R", "vop_allocate.R"))
ok <- function(cond, msg) { if (!isTRUE(cond)) stop("FIXTURE FAIL: ", msg) else cat("  ok  ", msg, "\n") }

## grid: 6 x 6, country A = columns 1-2 (ID 1), B = 3-4 (ID 2), C = 5-6 (ID 3)
admin <- rast(nrows = 6, ncols = 6, xmin = 0, xmax = 6, ymin = 0, ymax = 6, crs = "EPSG:4326")
values(admin) <- rep(c(1, 1, 2, 2, 3, 3), 6)
zones <- data.table(ID = 1:3, iso3 = c("AAA", "BBB", "CCC"))
mk <- function(f) { r <- rast(admin); values(r) <- f(values(admin)[, 1]); r }   # f: ID vector -> values
spread <- function(total, id, n_cells = 12) function(ids) ifelse(ids == id, total / n_cells, 0)
layers <- list(
  "pearl millet"   = mk(spread(10, 1)),  "small millet" = mk(spread(30, 1)),
  "arabica coffee" = mk(spread(5, 1)),   "robusta coffee" = mk(spread(15, 1)),
  "banana"         = mk(spread(1, 1)),   "plantain" = mk(spread(999, 1)),
  "rapeseed"       = mk(spread(40, 1)),
  "wheat"          = mk(spread(50, 2)),
  "maize"          = mk(function(ids) ifelse(ids == 3, 20 / 12, 0)),
  "sorghum"        = mk(spread(100, 2)))
spam_all <- rast(layers); names(spam_all) <- names(layers)
# an "unmapped" layer that must be dropped with a log line, not an error
spam_all$`rest of crops` <- mk(spread(7, 2))

spam2fao <- fread(file.path(root, "metadata", "SPAM2010_FAO_crops.csv"))
groups <- vop_item_groups(spam2fao, names(spam_all))
print(groups[group %in% c("acof+rcof", "pmil+smil", "banpl", "rape")])
ok(identical(sort(groups[group == "pmil+smil", unique(layer)]), c("pearl millet", "small millet")), "millet: pearl + small share one group from the fixed mapping table")
ok(identical(sort(groups[group == "acof+rcof", unique(layer)]), c("arabica coffee", "robusta coffee")), "coffee: arabica + robusta share one group (no hand-written split needed)")
ok(identical(sort(groups[group == "banpl", unique(item)]), c("Bananas", "Plantains and cooking bananas")) && identical(sort(groups[group == "banpl", unique(layer)]), c("banana", "plantain")), "musa: Bananas + Plantains pooled onto banana + plantain")
ok(identical(sort(groups[group == "rape", unique(item)]), c("Mustard seed", "Rape or colza seed")), "rapeseed: two FAO items summed into one SPAM crop")
ok(all(groups[group == "whea", layer] == "wheat") && nrow(groups[layer == "rest of crops"]) > 0, "single-item crops map one to one; 'rest of crops' maps to its many items")

## national tables
spam_prod <- as.data.table(zonal(spam_all, admin, fun = "sum", na.rm = TRUE)); setnames(spam_prod, names(spam_prod)[1], "ID")
spam_prod <- melt(spam_prod, id.vars = "ID", variable.name = "layer", value.name = "prod_t"); spam_prod <- merge(spam_prod, zones, by = "ID")[, .(iso3, layer, prod_t)]
gpv <- rbind(data.table(iso3 = "AAA", Item = c("Millet", "Coffee, green", "Bananas", "Rape or colza seed", "Mustard seed"), value = c(100, 200, 300, 10, 5)),
             data.table(iso3 = "BBB", Item = c("Wheat", "Maize (corn)", "Sorghum"), value = c(80, 50, 60)),
             data.table(iso3 = "ZZZ", Item = "Wheat", value = 999))   # a FAO country not on the grid: must be ignored
fao_prod <- rbind(data.table(iso3 = "AAA", Item = c("Millet", "Coffee, green", "Bananas", "Rape or colza seed", "Mustard seed"), prod_t = c(40, 20, 1000, 30, 10)),
                  data.table(iso3 = "BBB", Item = c("Wheat", "Maize (corn)"), prod_t = c(1000, 500)))
alloc <- vop_allocation_table(gpv, fao_prod, spam_prod, groups, min_coverage = 0.10)
print(alloc)
a <- function(i, g) alloc[iso3 == i & group == g]
ok(a("AAA", "pmil+smil")$value_alloc == 100 && a("AAA", "pmil+smil")$reason == "ok", "A millet allocated (coverage 1.0)")
ok(a("AAA", "banpl")$gpv == 300 && a("AAA", "banpl")$n_items_valued == 1 && abs(a("AAA", "banpl")$coverage - 1) < 1e-9 && !a("AAA", "banpl")$guarded, "A musa: pooled coverage judged on banana + plantain together (1000 / 1000), not on banana alone (1 / 1000)")
ok(a("AAA", "rape")$gpv == 15 && a("AAA", "rape")$fao_prod_t == 40, "A rapeseed: GPV and production summed over the two items")
ok(a("BBB", "whea")$guarded && is.na(a("BBB", "whea")$value_alloc) && grepl("< 10%", a("BBB", "whea")$reason) && abs(a("BBB", "whea")$coverage - 0.05) < 1e-9, "B wheat guarded: SPAM covers 5 % of FAO production")
ok(a("BBB", "maiz")$guarded && a("BBB", "maiz")$reason == "SPAM has no production for the group", "B maize guarded: nothing in SPAM to distribute onto")
ok(!a("BBB", "sorg")$guarded && a("BBB", "sorg")$value_alloc == 60 && grepl("not judged", a("BBB", "sorg")$reason), "B sorghum allocated with 'coverage not judged' (no FAO production row)")
ok(nrow(alloc[iso3 == "CCC"]) == 0 && nrow(alloc[iso3 == "ZZZ"]) == 1, "C has no GPV row (nothing to allocate); the off-grid FAO country stays in the table but gets no raster")
ok(a("ZZZ", "whea")$reason == "country outside the SPAM release" && a("ZZZ", "whea")$guarded, "a country with no SPAM production at all is labelled outside the release (North Africa), not a coverage failure")
ok(a("BBB", "maiz")$reason == "SPAM has no production for the group", "a covered country missing one crop keeps the group-level reason")

## rasters
vop <- vop_allocate_rasters(spam_all, admin, zones, alloc, groups, spam_prod)
print(names(vop))
z <- as.data.table(zonal(vop, admin, fun = "sum", na.rm = TRUE)); setnames(z, names(z)[1], "ID"); z <- merge(z, zones, by = "ID")
v <- function(i, l) z[iso3 == i][[l]]
ok(abs(v("AAA", "pearl millet") - 25) < 1e-9 && abs(v("AAA", "small millet") - 75) < 1e-9, "#38: Millet 100 split 25 / 75 by SPAM share (was 100 / nothing)")
ok(abs(v("AAA", "arabica coffee") - 50) < 1e-9 && abs(v("AAA", "robusta coffee") - 150) < 1e-9, "coffee 200 split 50 / 150 - identical to the old hand-written split")
ok(abs(v("AAA", "banana") - 0.3) < 1e-9 && abs(v("AAA", "plantain") - 299.7) < 1e-9, "#39 banana: Bananas 300 lands 0.3 on banana and 299.7 on plantain (was 300 on 1 t of banana, nothing on plantain)")
ok(abs(v("AAA", "rapeseed") - 15) < 1e-9, "rapeseed 15 on the rapeseed layer")
ok((is.na(v("BBB", "wheat")) || v("BBB", "wheat") == 0) && all(is.na(values(vop$wheat)[values(admin)[, 1] == 2])), "#39 Sudan-shaped: guarded wheat is NA on B's cells, not 80 spread over 50 t")
ok(all(is.na(values(vop$maize)[values(admin)[, 1] == 3])), "classify leak: C has SPAM maize but no GPV -> NA, not the admin ID (3) spread as value")
ok(all(is.na(values(vop$maize)[values(admin)[, 1] == 2])), "B maize (SPAM 0 t) -> NA, no 0/0")
ok(abs(v("BBB", "sorghum") - 60) < 1e-9, "B sorghum 60 allocated")
ok(!"rest of crops" %in% names(vop) || all(is.na(values(vop$`rest of crops`))), "layer whose items have no GPV carries no value")
ok(all(c("wheat", "maize") %in% names(vop)), "fully guarded / unvalued crops still have a (NA) layer so the layer set is data-independent")
chk <- vop_check_totals(vop, admin, zones, alloc, groups)
print(chk)
ok(all(chk[iso3 != "ZZZ", ok]), "conservation: every allocated (country, group) zonal sum == allocated GPV; every guarded one is 0 / NA")

## #40: a polygon smaller than a cell owns no cell centre -> rasterize() without touches gives NA
isl <- vect("POLYGON ((4.2 4.2, 4.4 4.2, 4.4 4.4, 4.2 4.4, 4.2 4.2))", crs = "EPSG:4326"); isl$iso3 <- "SYC"; isl$price <- 500
r0 <- rasterize(isl, admin, field = "price")
r1 <- rasterize(isl, admin, field = "price", touches = TRUE)
ok(all(is.na(values(r0))) && sum(!is.na(values(r1))) == 1 && values(r1)[!is.na(values(r1))] == 500, "#40: a sub-cell polygon (Seychelles-shaped) rasterises to nothing without touches and to its one touched cell with touches = TRUE")
cat("\nALL VOP-ALLOCATE FIXTURE ASSERTIONS PASSED\n")
