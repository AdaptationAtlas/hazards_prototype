#!/usr/bin/env Rscript
# R/checks/fixture_price_fill.R
# ==============================
# Synthetic replay of the 2026-09/10 price defects through R/price_fill.R, run BEFORE the
# node touches 0.4.2 (no data needed, seconds). Fails loudly on any assertion.
#   ZWE wheat 2022   67,170 USD/t in BOTH FAO series (official rate on a hyperinflating
#                    currency) -> clipped from both; the 2021 own value stands
#   SDN wheat        low-side exchange-rate artefact in current US$ GPV (56 -> 1.6 USD/t over
#                    2019-23): all but one year clipped, the survivor is still 1/5 of the
#                    item's nominal/intld median -> basis fallback
#   KEN coffee       auction green-bean price, 5x the item's nominal/intld median -> basis fallback
#   BDI coffee       cherry price, 1/5 of the item median -> basis fallback
#   + a producer-price-only country, a longer-series-only country, a pure fill, an item with
#     too few own-priced countries for the guard, and a filled price outside the band (info only)
# Usage: Rscript R/checks/fixture_price_fill.R
suppressPackageStartupMessages(library(data.table))
root <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else { fa <- grep("^--file=", commandArgs(FALSE), value = TRUE); if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd() }
source(file.path(root, "R", "price_fill.R"))
ok <- function(cond, msg) { if (!isTRUE(cond)) stop("FIXTURE FAIL: ", msg) else cat("  ok  ", msg, "\n") }

yrs <- 2019:2023
neighbors <- list(ZWE = c("ZMB", "ZAF", "MOZ"), ZMB = c("ZWE", "MOZ", "TZA"), ZAF = c("ZWE", "MOZ"), MOZ = c("ZWE", "ZMB", "ZAF", "TZA"),
                  KEN = c("TZA", "UGA", "ETH"), ETH = c("KEN", "SDN"), SDN = c("ETH", "EGY"), EGY = c("SDN"), TZA = c("KEN", "UGA", "MOZ", "ZMB", "RWA", "BDI"),
                  UGA = c("KEN", "TZA", "RWA"), RWA = c("UGA", "TZA", "BDI"), BDI = c("RWA", "TZA"), CIV = c("GHA"), GHA = c("CIV"), CMR = c("NGA"), NGA = c("CMR"))
regions <- list(East = c("KEN", "ETH", "TZA", "UGA", "RWA", "BDI"), South = c("ZWE", "ZMB", "ZAF", "MOZ"), North = c("SDN", "EGY"), West = c("CIV", "GHA", "NGA", "CMR"))
isos <- unique(unlist(regions))

## 1) raw yearly series ---------------------------------------------------------------------
# wheat: world implied ~300, world producer ~290
whea <- rbind(
  data.table(iso3 = "ZWE", year = c(2021, 2022), gpv_k = c(421 * 250, 67170 * 230), prod_t = c(250e3, 230e3), pp = c(421, 67170)),   # artefact 2022 in BOTH series
  data.table(iso3 = "SDN", year = yrs, gpv_k = c(56, 34, 10.6, NA, 1.6) * c(726, 718, 676, 476, 378), prod_t = c(726, 718, 676, 476, 378) * 1e3, pp = NA_real_),   # low-side FX, no producer price
  data.table(iso3 = "ZAF", year = yrs, gpv_k = c(280, 290, 310, 380, 330) * 2000, prod_t = 2000e3, pp = c(280, 290, 310, 380, 330)),
  data.table(iso3 = "MOZ", year = yrs, gpv_k = NA_real_, prod_t = 20e3, pp = c(400, 410, 420, 450, 430)),                                   # producer price only
  data.table(iso3 = "KEN", year = yrs, gpv_k = c(350, 360, 370, 420, 400) * 300, prod_t = 300e3, pp = NA_real_),
  data.table(iso3 = "ETH", year = yrs, gpv_k = c(330, 340, 360, 410, 390) * 5000, prod_t = 5000e3, pp = NA_real_),
  data.table(iso3 = "EGY", year = yrs, gpv_k = c(300, 310, 320, 400, 350) * 9000, prod_t = 9000e3, pp = c(300, 310, 320, 400, 350)),
  data.table(iso3 = "TZA", year = 2015:2016, gpv_k = c(310, 320) * 100, prod_t = 100e3, pp = NA_real_),                                      # longer-series only
  data.table(iso3 = "ZMB", year = yrs, gpv_k = NA_real_, prod_t = 200e3, pp = NA_real_))[, atlas_name := "whea"]
# constant-I$ GPV (thousand I$) such that nominal/intld sits ~1.2 for the sound countries; SDN's is sound
whea_intld <- data.table(iso3 = c("ZWE", "SDN", "ZAF", "MOZ", "KEN", "ETH", "EGY", "TZA", "ZMB"),
                         intld_k = c(421 * 250 / 1.2, 160188, 310 * 2000 / 1.2, 420 * 20 / 1.2, 370 * 300 / 1.2, 360 * 5000 / 1.2, 320 * 9000 / 1.2, 315 * 100 / 1.2, 300 * 200 / 1.2))[, atlas_name := "whea"]
# coffee: world implied ~900; item median nominal/intld ~0.6 (constant-I$ coffee price is high vs most farm-gate prices)
coff <- rbind(
  data.table(iso3 = "KEN", year = yrs, gpv_k = 4269 * 40, prod_t = 40e3, pp = 4269),      # auction green bean
  data.table(iso3 = "BDI", year = yrs, gpv_k = 268 * 12, prod_t = 12e3, pp = NA_real_),    # cherry
  data.table(iso3 = "ETH", year = yrs, gpv_k = 1200 * 450, prod_t = 450e3, pp = NA_real_),
  data.table(iso3 = "UGA", year = yrs, gpv_k = 1300 * 300, prod_t = 300e3, pp = 1300),
  data.table(iso3 = "TZA", year = yrs, gpv_k = 1100 * 60, prod_t = 60e3, pp = NA_real_),
  data.table(iso3 = "CIV", year = yrs, gpv_k = 1000 * 100, prod_t = 100e3, pp = 1000),
  data.table(iso3 = "CMR", year = yrs, gpv_k = 1250 * 30, prod_t = 30e3, pp = NA_real_),
  data.table(iso3 = "RWA", year = yrs, gpv_k = NA_real_, prod_t = 25e3, pp = NA_real_))[, atlas_name := "coff"]
coff_intld <- data.table(iso3 = c("KEN", "BDI", "ETH", "UGA", "TZA", "CIV", "CMR", "RWA"),
                         intld_k = c(60000, 28837, 1200 * 450 / 0.6, 1300 * 300 / 0.6, 1100 * 60 / 0.6, 1000 * 100 / 0.6, 1250 * 30 / 0.6, 2000 * 25 / 0.6 * 6))[, atlas_name := "coff"]
# tea: only 2 own-priced countries -> guard must NOT fire even though they disagree 20x
teas <- rbind(data.table(iso3 = "KEN", year = yrs, gpv_k = 2000 * 500, prod_t = 500e3, pp = 2000),
              data.table(iso3 = "RWA", year = yrs, gpv_k = 400 * 30, prod_t = 30e3, pp = NA_real_),
              data.table(iso3 = "UGA", year = yrs, gpv_k = NA_real_, prod_t = 60e3, pp = NA_real_))[, atlas_name := "teas"]
teas_intld <- data.table(iso3 = c("KEN", "RWA", "UGA"), intld_k = c(2000 * 500 / 1.5, 400 * 30 / 1.5 * 20, 1500 * 60 / 1.5))[, atlas_name := "teas"]
raw <- rbind(whea, coff, teas)
raw[, price_implied := implied_price(gpv_k, prod_t)]
world_impl <- rbind(CJ(atlas_name = "whea", year = 2014:2023)[, price_usd := c(rep(300, 5), 250, 260, 300, 380, 300)],
                    CJ(atlas_name = "coff", year = 2014:2023)[, price_usd := 900], CJ(atlas_name = "teas", year = 2014:2023)[, price_usd := 1500])
world_pp <- copy(world_impl)[, price_usd := price_usd * 0.95]

## 2) clip ----------------------------------------------------------------------------------
ci <- clip_prices_to_world_band(raw[, .(iso3, atlas_name, year, price_usd = price_implied)], world_impl, band = 5)
cp <- clip_prices_to_world_band(raw[, .(iso3, atlas_name, year, price_usd = pp)], world_pp, band = 5)
cat("implied clipped:\n"); print(ci$dropped); cat("producer clipped:\n"); print(cp$dropped)
ok(nrow(ci$dropped[iso3 == "ZWE"]) == 1 && ci$dropped[iso3 == "ZWE", year] == 2022, "ZWE wheat 2022 clipped from the implied series")
ok(nrow(cp$dropped[iso3 == "ZWE"]) == 1 && cp$dropped[iso3 == "ZWE", year] == 2022, "ZWE wheat 2022 clipped from the producer series")
ok(nrow(ci$dropped[iso3 == "SDN"]) == 3, "SDN wheat: three low-side years clipped (2020, 2021, 2023), 2019 survives")
ok(nrow(ci$dropped[iso3 %in% c("KEN", "BDI") & atlas_name == "coff"]) == 0, "KEN auction and BDI cherry coffee survive the world clip (they are inside 5x) - the clip is not what catches a basis problem")

## 3) own price per window --------------------------------------------------------------------
own <- own_price_window(ci$kept, cp$kept, years = yrs, long_years = (min(yrs) - 5):max(yrs))
print(own[order(atlas_name, iso3)])
g <- function(i, c) own[iso3 == i & atlas_name == c]
ok(g("ZWE", "whea")$price_usd == 421 && g("ZWE", "whea")$price_source == "fao gpv implied", "ZWE wheat own = 421 (2021), implied")
ok(g("SDN", "whea")$price_usd == 56 && g("SDN", "whea")$price_source == "fao gpv implied", "SDN wheat own = 56 from the one surviving year (the guard must deal with it)")
ok(g("MOZ", "whea")$price_source == "fao producer price" && g("MOZ", "whea")$price_usd == 420, "MOZ wheat: producer price is the first fallback")
ok(g("TZA", "whea")$price_source == "fao gpv implied longer-series" && g("TZA", "whea")$price_usd == 315, "TZA wheat: longer series when the window is empty")
ok(is.na(g("ZMB", "whea")$price_usd) && is.na(g("ZMB", "whea")$price_source), "ZMB wheat: nothing own -> left to the fill chain")
ok(g("ZAF", "whea")$price_usd == 310, "ZAF wheat: implied window median 310 (implied == producer where both exist)")

## 4) fill chain + basis guard, as 0.4.2 section 3 does it -----------------------------------
grid <- CJ(iso3 = isos, atlas_name = c("whea", "coff", "teas"))
d <- merge(grid, raw[year %in% yrs, .(production_t = median(prod_t, na.rm = TRUE)), by = .(iso3, atlas_name)], by = c("iso3", "atlas_name"), all.x = TRUE)
d <- merge(d, rbind(whea_intld, coff_intld, teas_intld)[, .(iso3, atlas_name, value_intd15 = intld_k)], by = c("iso3", "atlas_name"), all.x = TRUE)
d <- merge(d, own, by = c("iso3", "atlas_name"), all.x = TRUE)
own_src <- d[, .(iso3, atlas_name, own_source = price_source)]; d[, price_source := NULL]
wi <- world_impl[year %in% yrs, .(price_world = median(price_usd)), by = atlas_name]
d <- fill_price_robust(d, value_field = "price_usd", group_field = "atlas_name", neighbors = neighbors, regions = regions, world = wi)
d <- merge(d, own_src, by = c("iso3", "atlas_name")); d[price_source == "own", price_source := own_source][, own_source := NULL]
ok(all(d[is.na(price_usd) & !is.na(production_t), price_source] %in% c("neighbours median", "region median", "continent median", "world median")), "every row without an own price is filled and says so")
ok(d[iso3 == "ZMB" & atlas_name == "whea", price_source] == "neighbours median" && d[iso3 == "ZMB" & atlas_name == "whea", price_usd_final] < 500, "ZMB wheat: neighbours median, no artefact inherited")
bg <- apply_basis_guard(d, band = 4, min_n = 5)
cat("basis medians:\n"); print(bg$medians); cat("flagged:\n"); print(bg$flagged); cat("fills outside band (info):\n"); print(bg$info_fills)
r <- bg$data; h <- function(i, c) r[iso3 == i & atlas_name == c]
ok(all(c("KEN", "BDI") %in% bg$flagged[atlas_name == "coff", iso3]), "KEN auction and BDI cherry coffee flagged by the basis guard")
ok(h("KEN", "coff")$price_source == "basis fallback" && h("KEN", "coff")$price_usd_final < 1500 && h("KEN", "coff")$price_usd_final > 800, "KEN coffee -> item-median factor x constant-I$ value (a few hundred to ~1,000 USD/t, not 4,269)")
ok(h("BDI", "coff")$price_source == "basis fallback" && h("BDI", "coff")$price_usd_final > 1000, "BDI coffee cherry -> green-bean-equivalent price from its constant-I$ value")
ok("SDN" %in% bg$flagged[atlas_name == "whea", iso3] && h("SDN", "whea")$price_source == "basis fallback" && abs(h("SDN", "whea")$price_usd_final - 1.2 * 160188e3 / 676e3) / (1.2 * 160188e3 / 676e3) < 0.05,
   "SDN wheat: the one surviving low-side year is caught by the guard -> ~284 USD/t from its constant-I$ value")
ok(!"ZWE" %in% bg$flagged$iso3 && h("ZWE", "whea")$price_usd_final == 421, "ZWE wheat untouched by the guard (own 421 sits at the item median)")
ok(nrow(bg$flagged[atlas_name == "teas"]) == 0 && h("RWA", "teas")$price_source == "fao gpv implied", "tea: 2 own-priced countries < min_n -> guard skipped even at 20x")
ok(all(!bg$flagged$price_source %in% c("neighbours median", "region median", "continent median", "world median")), "no filled price was changed by the guard")
ok(nrow(bg$info_fills) >= 1 && "RWA" %in% bg$info_fills[atlas_name == "coff", iso3], "RWA coffee (filled, intld inconsistent) reported as info, not corrected")
ok(all(is.finite(r[!is.na(production_t), price_usd_final]) & r[!is.na(production_t), price_usd_final] > 0), "every priced row finite and positive")
ok(all(c("basis_ratio", "basis_median") %in% names(r)), "audit columns present")
cat("\nALL PRICE-FILL FIXTURE ASSERTIONS PASSED\n")
