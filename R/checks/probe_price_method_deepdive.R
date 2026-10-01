#!/usr/bin/env Rscript
# R/checks/probe_price_method_deepdive.R
# ======================================
# Evidence probe (read-only) for the nominal-price method decision (2026-10-01):
# should 0.4.2 price SPAM tonnage with FAOSTAT PRODUCER PRICES (USD/t, today) or
# with the IMPLIED price = FAOSTAT gross production value (current thousand US$) /
# FAOSTAT production (t), per country x item x year?
#
# Five questions, all answered from the FAOSTAT bulk CSVs in fao_dir for the SPAM
# crop set and the 2019-2023 window 0.4.2 uses:
#   1 coverage   share of (iso3, crop) pairs with an OWN value, and the production
#                share they represent (what the fill chain would still have to fill)
#   2 agreement  where both exist, implied / producer-price ratio distribution
#   3 outliers   pairs outside [1/band, band] x the WORLD median, per method
#   4 stability  within-pair year-to-year spread (CV) 2019-2023, per method
#   5 cross-basis implied-nominal / constant-I$ GPV spread vs producer-price-nominal /
#                constant-I$ GPV spread (the gate band the two methods would need)
#   + the named pairs behind the 2026-09 defects (ZWE wheat, RWA oil palm, ZMB wheat,
#     SDN wheat/sorghum, NGA banana, KEN maize, BDI/RWA arabica).
#
# Usage: Rscript R/checks/probe_price_method_deepdive.R [--fao-dir <dir>] [--band 5] [--out <csv>]
#        (without --fao-dir it sources 0_server_setup.R for fao_dir)

suppressPackageStartupMessages({ library(data.table); library(countrycode) })
args <- commandArgs(trailingOnly = TRUE)
opt <- function(x, d) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
FAO <- opt("--fao-dir", ""); BAND <- as.numeric(opt("--band", "5")); OUT <- opt("--out", "")
WIN <- 2019:2023
if (!nzchar(FAO)) {
  setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
  suppressMessages(suppressWarnings(source(setup))); FAO <- fao_dir
}
root <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else getwd()
cat(sprintf("[deepdive] fao_dir = %s | window %d-%d | band %gx\n", FAO, min(WIN), max(WIN), BAND))

## crop set, exactly as 0.4.2 builds it ---------------------------------------
codes <- fread(file.path(root, "metadata", "SpamCodes.csv"))[!is.na(Code) & Code != ""][, code_low := tolower(Code)]
crops <- tolower(codes[compound == "no", Code])
s2f <- fread(file.path(root, "metadata", "SPAM2010_FAO_crops.csv"))[short_spam2010 %in% crops & name_fao != "Mustard seed" & !(short_spam2010 %in% c("rcof", "smil", "pmil", "acof"))]
s2f[short_spam2010 == "rape", name_fao_val := "Rape or colza seed"]
items <- rbind(s2f[, .(atlas = short_spam2010, Item = name_fao_val)], data.table(atlas = c("coff", "mill"), Item = c("Coffee, green", "Millet")))

## loaders -----------------------------------------------------------------------
rd <- function(f, element, unit = NULL, value_name) {
  d <- fread(file.path(FAO, f), encoding = "Latin-1")
  d <- d[Element == element]
  if (!is.null(unit)) d <- d[Unit == unit]
  d[, iso3 := countrycode(as.numeric(gsub("'", "", `Area Code (M49)`)), origin = "un", destination = "iso3c", warn = FALSE)]
  d <- d[!is.na(iso3) & Item %in% items$Item & !Area %in% c("Ethiopia PDR", "Sudan (former)")]
  ycols <- grep("^Y[0-9]{4}$", names(d), value = TRUE)
  m <- melt(d[, c("iso3", "Item", ycols), with = FALSE], id.vars = c("iso3", "Item"), variable.name = "year", value.name = value_name)
  m[, year := as.integer(sub("Y", "", year))][!is.na(get(value_name))]
}
pp   <- rd("Prices_E_Africa_NOFLAG.csv", "Producer Price (USD/tonne)", NULL, "pp_usd_t")
gpvc <- rd("Value_of_Production_E_Africa_NOFLAG.csv", "Gross Production Value (current thousand US$)", NULL, "gpv_cur_kusd")
gpvi <- rd("Value_of_Production_E_Africa_NOFLAG.csv", "Gross Production Value (constant 2014-2016 thousand I$)", NULL, "gpv_intld_k")
prod <- rd("Production_Crops_Livestock_E_Africa_NOFLAG.csv", "Production", "t", "prod_t")
d <- Reduce(function(a, b) merge(a, b, by = c("iso3", "Item", "year"), all = TRUE), list(prod, pp, gpvc, gpvi))
d <- merge(d, items, by = "Item")
d[, implied_usd_t := fifelse(!is.na(prod_t) & prod_t > 0 & !is.na(gpv_cur_kusd), gpv_cur_kusd * 1000 / prod_t, NA_real_)]
w <- d[year %in% WIN]
cat(sprintf("[deepdive] rows in window: %d | countries %d | items %d\n", nrow(w), uniqueN(w$iso3), uniqueN(w$Item)))

## world medians (all-countries price file; all-area-groups VoP + production for 'World')
ppw <- fread(file.path(FAO, "Prices_E_All_Data_(Normalized).csv"), encoding = "Latin-1")[Element == "Producer Price (USD/tonne)" & Item %in% items$Item & Year %in% WIN, .(pp_world = median(Value, na.rm = TRUE)), by = Item]
gw <- fread(file.path(FAO, "Value_of_Production_E_All_Area_Groups_NOFLAG.csv"), encoding = "Latin-1")[Area == "World" & Element == "Gross Production Value (current thousand US$)" & Item %in% items$Item]
pw <- fread(file.path(FAO, "Production_Crops_Livestock_E_All_Area_Groups_NOFLAG.csv"), encoding = "Latin-1")[Area == "World" & Element == "Production" & Unit == "t" & Item %in% items$Item]
ycols <- paste0("Y", WIN)
implw <- merge(melt(gw[, c("Item", ycols), with = FALSE], id.vars = "Item", value.name = "g"), melt(pw[, c("Item", ycols), with = FALSE], id.vars = "Item", value.name = "p"), by = c("Item", "variable"))[, .(impl_world = median(g * 1000 / p, na.rm = TRUE)), by = Item]
world <- merge(ppw, implw, by = "Item", all = TRUE)
cat("\n== world reference prices (USD/t, 2019-23 median): producer-price file vs World GPV/production ==\n")
print(world[, .(Item, pp_world = signif(pp_world, 4), impl_world = signif(impl_world, 4), ratio = signif(impl_world / pp_world, 3))][order(ratio)], nrows = 60)

## 1 coverage ---------------------------------------------------------------------
pairs <- w[, .(n_pp = sum(!is.na(pp_usd_t)), n_impl = sum(!is.na(implied_usd_t)), prod = median(prod_t, na.rm = TRUE)), by = .(iso3, atlas, Item)]
pairs <- pairs[!is.na(prod) & prod > 0]
cov <- pairs[, .(pairs = .N,
                 pp_pairs = sum(n_pp > 0), impl_pairs = sum(n_impl > 0), both = sum(n_pp > 0 & n_impl > 0), neither = sum(n_pp == 0 & n_impl == 0),
                 pp_prod_share = sum(prod[n_pp > 0]) / sum(prod), impl_prod_share = sum(prod[n_impl > 0]) / sum(prod))]
cat("\n== 1 coverage: (iso3, crop) pairs with production > 0 in the window ==\n"); print(cov)
cat(sprintf("   producer price covers %.0f%% of pairs (%.0f%% of production); implied price covers %.0f%% of pairs (%.0f%% of production); neither %d pairs\n",
            100 * cov$pp_pairs / cov$pairs, 100 * cov$pp_prod_share, 100 * cov$impl_pairs / cov$pairs, 100 * cov$impl_prod_share, cov$neither))
cat("   countries with NO producer price at all in the window but implied prices: ",
    paste(setdiff(pairs[n_impl > 0, unique(iso3)], pairs[n_pp > 0, unique(iso3)]), collapse = ","), "\n")

## 2 agreement where both exist ----------------------------------------------------
both <- w[!is.na(pp_usd_t) & !is.na(implied_usd_t) & pp_usd_t > 0 & implied_usd_t > 0][, ratio := implied_usd_t / pp_usd_t]
cat(sprintf("\n== 2 agreement: %d country-item-years with BOTH values; implied / producer price ==\n", nrow(both)))
cat(sprintf("   median %.3f | IQR [%.3f, %.3f] | within 10%%: %.0f%% | within 25%%: %.0f%% | beyond 2x either way: %.1f%%\n",
            median(both$ratio), quantile(both$ratio, .25), quantile(both$ratio, .75), 100 * mean(abs(both$ratio - 1) <= .10), 100 * mean(abs(both$ratio - 1) <= .25), 100 * mean(both$ratio > 2 | both$ratio < .5)))
bc <- both[, .(n = .N, med = signif(median(ratio), 3), beyond2x = sum(ratio > 2 | ratio < .5)), by = iso3][order(-beyond2x, -abs(log(med)))]
cat("   countries where the two disagree most (systematic = exchange-rate or valuation basis, not noise):\n"); print(bc[1:min(12, .N)], nrows = 12)

## 3 outliers vs world ------------------------------------------------------------
pw3 <- merge(w, world, by = "Item")
o_pp   <- pw3[!is.na(pp_usd_t), .(out = pp_usd_t / pp_world > BAND | pp_usd_t / pp_world < 1 / BAND)]
o_impl <- pw3[!is.na(implied_usd_t), .(out = implied_usd_t / impl_world > BAND | implied_usd_t / impl_world < 1 / BAND)]
cat(sprintf("\n== 3 outliers beyond %gx the world median: producer price %d of %d own values (%.1f%%) | implied %d of %d (%.1f%%) ==\n",
            BAND, sum(o_pp$out), nrow(o_pp), 100 * mean(o_pp$out), sum(o_impl$out), nrow(o_impl), 100 * mean(o_impl$out)))
cat("   worst implied-price outliers (would still be clipped):\n")
print(pw3[!is.na(implied_usd_t)][order(-abs(log(implied_usd_t / impl_world)))][1:10, .(iso3, Item, year, implied_usd_t = signif(implied_usd_t, 4), impl_world = signif(impl_world, 4), ratio = signif(implied_usd_t / impl_world, 3))], nrows = 10)
cat("   worst producer-price outliers:\n")
print(pw3[!is.na(pp_usd_t)][order(-abs(log(pp_usd_t / pp_world)))][1:10, .(iso3, Item, year, pp_usd_t = signif(pp_usd_t, 4), pp_world = signif(pp_world, 4), ratio = signif(pp_usd_t / pp_world, 3))], nrows = 10)

## 4 stability ---------------------------------------------------------------------
st <- w[, .(cv_pp = if (sum(!is.na(pp_usd_t)) >= 3) sd(pp_usd_t, na.rm = TRUE) / mean(pp_usd_t, na.rm = TRUE) else NA_real_,
            cv_impl = if (sum(!is.na(implied_usd_t)) >= 3) sd(implied_usd_t, na.rm = TRUE) / mean(implied_usd_t, na.rm = TRUE) else NA_real_), by = .(iso3, Item)]
cat(sprintf("\n== 4 stability (within-pair CV over 2019-23, pairs with >= 3 years): producer price median CV %.3f (n=%d) | implied median CV %.3f (n=%d) ==\n",
            median(st$cv_pp, na.rm = TRUE), sum(!is.na(st$cv_pp)), median(st$cv_impl, na.rm = TRUE), sum(!is.na(st$cv_impl))))

## 5 cross-basis spread --------------------------------------------------------------
cb <- w[!is.na(gpv_intld_k) & gpv_intld_k > 0 & !is.na(prod_t) & prod_t > 0]
cb[, nominal_pp := pp_usd_t * prod_t / 1000]                     # thousand USD, producer-price method
cb[, nominal_impl := gpv_cur_kusd]                               # thousand USD, implied method (= FAO current GPV by construction)
cb5 <- cb[, .(r_pp = median(nominal_pp / gpv_intld_k, na.rm = TRUE), r_impl = median(nominal_impl / gpv_intld_k, na.rm = TRUE)), by = .(iso3, Item)]
q <- function(x) { x <- x[is.finite(x)]; sprintf("median %.2f | 5-95%% [%.2f, %.2f] | beyond [1/5,5]: %.1f%% (n=%d)", median(x), quantile(x, .05), quantile(x, .95), 100 * mean(x > 5 | x < .2), length(x)) }
cat("\n== 5 cross-basis nominal / constant-I$ per (iso3, item), window medians ==\n")
cat("   producer-price nominal / intld : ", q(cb5$r_pp), "\n")
cat("   implied (GPV current) / intld  : ", q(cb5$r_impl), "\n")

## 6 the fair comparison: each method AFTER the world-band clip (what 0.4.2 would actually use) ----
clip <- function(x, ref) fifelse(!is.na(x) & !is.na(ref) & x / ref <= BAND & x / ref >= 1 / BAND, x, NA_real_)
w6 <- merge(w, world, by = "Item")
w6[, pp_c := clip(pp_usd_t, pp_world)][, impl_c := clip(implied_usd_t, impl_world)]
p6 <- w6[, .(pp = median(pp_c, na.rm = TRUE), impl = median(impl_c, na.rm = TRUE), prod = median(prod_t, na.rm = TRUE), intld = median(gpv_intld_k, na.rm = TRUE)), by = .(iso3, Item)][!is.na(prod) & prod > 0]
cat(sprintf("\n== 6 after the %gx world-band clip, (iso3, item) pairs with production > 0: %d ==\n", BAND, nrow(p6)))
cat(sprintf("   own value survives the clip: producer price %d pairs (%.0f%% of production) | implied %d pairs (%.0f%% of production) | implied adds %d pairs the producer price lacks; %d pairs have neither (fill chain)\n",
            sum(!is.na(p6$pp)), 100 * sum(p6$prod[!is.na(p6$pp)]) / sum(p6$prod), sum(!is.na(p6$impl)), 100 * sum(p6$prod[!is.na(p6$impl)]) / sum(p6$prod),
            sum(is.na(p6$pp) & !is.na(p6$impl)), sum(is.na(p6$pp) & is.na(p6$impl))))
cat(sprintf("   clipped away: producer price %d of %d own values (%.1f%%) | implied %d of %d (%.1f%%)\n",
            w6[!is.na(pp_usd_t), sum(is.na(pp_c))], w6[, sum(!is.na(pp_usd_t))], 100 * w6[!is.na(pp_usd_t), mean(is.na(pp_c))],
            w6[!is.na(implied_usd_t), sum(is.na(impl_c))], w6[, sum(!is.na(implied_usd_t))], 100 * w6[!is.na(implied_usd_t), mean(is.na(impl_c))]))
p6[, r_pp := pp * prod / 1000 / intld][, r_impl := impl * prod / 1000 / intld]
cat("   cross-basis nominal / intld after clip - producer price:", q(p6$r_pp), "\n")
cat("   cross-basis nominal / intld after clip - implied      :", q(p6$r_impl), "\n")
cat("   countries whose implied prices are mostly clipped away (exchange-rate regimes FAO's current US$ cannot carry):\n")
print(w6[!is.na(implied_usd_t), .(n = .N, clipped = sum(is.na(impl_c)), share = round(mean(is.na(impl_c)), 2)), by = iso3][clipped > 0][order(-share, -clipped)][1:min(10, .N)], nrows = 10)
cat("   pairs the implied method adds (no producer price, implied survives clip) - production-weighted top 15:\n")
print(p6[is.na(pp) & !is.na(impl)][order(-prod)][1:min(15, .N), .(iso3, Item, prod_t = signif(prod, 4), implied_usd_t = signif(impl, 4))], nrows = 15)

## named pairs -----------------------------------------------------------------------
named <- data.table(iso3 = c("ZWE", "ZMB", "RWA", "SDN", "SDN", "NGA", "KEN", "BDI", "RWA", "ETH"), Item = c("Wheat", "Wheat", "Oil palm fruit", "Wheat", "Sorghum", "Bananas", "Maize (corn)", "Coffee, green", "Coffee, green", "Wheat"))
cat("\n== named pairs behind the 2026-09 defects: producer price vs implied, per year ==\n")
np <- merge(w, named, by = c("iso3", "Item"))[order(iso3, Item, year), .(iso3, Item, year, prod_t = signif(prod_t, 4), pp_usd_t = signif(pp_usd_t, 4), implied_usd_t = signif(implied_usd_t, 4), gpv_cur_kusd = signif(gpv_cur_kusd, 4), gpv_intld_k = signif(gpv_intld_k, 4))]
print(np, nrows = 80)
if (!nzchar(OUT)) OUT <- file.path(tempdir(), "price_method_deepdive.csv")
fwrite(w[, .(iso3, atlas, Item, year, prod_t, pp_usd_t, gpv_cur_kusd, gpv_intld_k, implied_usd_t)], OUT)
cat(sprintf("\n[deepdive] window table written to %s (%d rows)\n", OUT, nrow(w)))
