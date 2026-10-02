#!/usr/bin/env Rscript
# R/checks/probe_price_stale_slc.R
# =================================
# Evidence probe (read-only) behind the basis guard's low-side catches (2026-10-02): is FAOSTAT's
# implied price (GPV current US$ / production) for a (country, item) a MEASUREMENT or a stale
# imputation converted at a depreciating official exchange rate?
#
# Test: FAO also publishes GPV in current STANDARD LOCAL CURRENCY (SLC). SLC per tonne is the local
# price FAO actually used. If it is flat (or falls) while the country's GDP deflator multiplies,
# FAO carried a stale local price forward; dividing it by the official rate then produces a USD
# price that falls every year - Sudan millet 2,742 SDG/t in 2019, 2020, 2021 while the deflator
# went x37. A genuine series moves with the price level. Per pair: SLC/t 2019 -> 2023 ratio,
# GDP deflator (FAO Deflators file, SLC 2015 prices) ratio, real SLC/t ratio = the first over the
# second, plus whether a producer price exists at all (none exists for the 8 pairs in question).
#
# Usage: Rscript R/checks/probe_price_stale_slc.R [--pairs "Nigeria:Groundnuts, excluding shelled;Sudan:Millet"] [--out csv]
suppressPackageStartupMessages(library(data.table))
args <- commandArgs(trailingOnly = TRUE)
opt <- function(x, d) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
OUT <- opt("--out", "")
setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
suppressMessages(suppressWarnings(source(setup)))
WIN <- 2019:2023; yc <- paste0("Y", WIN)
s2f <- fread(file.path(project_dir, "metadata/SPAM2010_FAO_crops.csv")); items <- unique(c(s2f$name_fao_val, "Maize (corn)"))
v <- fread(file.path(fao_dir, "Value_of_Production_E_Africa.csv"), encoding = "Latin-1")
pr <- fread(file.path(fao_dir, "Production_Crops_Livestock_E_Africa_NOFLAG.csv"), encoding = "Latin-1")[Element == "Production" & Unit == "t" & Item %in% items, c("Area", "Item", yc), with = FALSE]
pp <- fread(file.path(fao_dir, "Prices_E_Africa_NOFLAG.csv"), encoding = "Latin-1")[Element == "Producer Price (USD/tonne)" & Item %in% items, c("Area", "Item", yc), with = FALSE]
de <- fread(def_file, encoding = "Latin-1")[Item == "GDP Deflator" & Element == "Value Standard Local Currency, 2015 prices" & Year %in% WIN, .(Area, year = Year, defl = Value)]
long <- function(d, nm) melt(d, id.vars = c("Area", "Item"), variable.name = "year", value.name = nm)[, year := as.integer(sub("Y", "", year))]
m <- Reduce(function(a, b) merge(a, b, by = c("Area", "Item", "year"), all = TRUE),
            list(long(v[Element == "Gross Production Value (current thousand SLC)" & Item %in% items, c("Area", "Item", yc), with = FALSE], "slc_k"),
                 long(v[Element == "Gross Production Value (current thousand US$)" & Item %in% items, c("Area", "Item", yc), with = FALSE], "usd_k"),
                 long(pr, "prod"), long(pp, "pp_usd")))
m <- merge(m, de, by = c("Area", "year"), all.x = TRUE)
m[, `:=`(slc_t = slc_k * 1000 / prod, usd_t = usd_k * 1000 / prod, fx = slc_k / usd_k)]
m <- m[is.finite(prod) & prod > 0]
first_last <- function(x, y) { ok <- is.finite(x); if (sum(ok) < 2) NA_real_ else x[ok][which.max(y[ok])] / x[ok][which.min(y[ok])] }
pair <- m[, .(n_slc = sum(is.finite(slc_t)), has_pp = any(is.finite(pp_usd)), prod_kt = median(prod) / 1e3,
              usd_t_med = median(usd_t, na.rm = TRUE), usd_t_first = usd_t[which.min(year[is.finite(usd_t)])], usd_t_last = usd_t[is.finite(usd_t)][which.max(year[is.finite(usd_t)])],
              slc_t_first = slc_t[is.finite(slc_t)][which.min(year[is.finite(slc_t)])], slc_t_last = slc_t[is.finite(slc_t)][which.max(year[is.finite(slc_t)])],
              slc_ratio = first_last(slc_t, year), defl_ratio = first_last(defl, year), fx_first = fx[is.finite(fx)][which.min(year[is.finite(fx)])], fx_last = fx[is.finite(fx)][which.max(year[is.finite(fx)])]), by = .(Area, Item)]
pair[, real_slc_ratio := slc_ratio / defl_ratio]
pair[, verdict := fifelse(!is.finite(real_slc_ratio), "no SLC series", fifelse(real_slc_ratio < 0.5, "STALE local price (real SLC/t fell > 50 %)", fifelse(real_slc_ratio > 2, "local price outran the deflator (check)", "moves with the price level")))]
cat(sprintf("[stale-slc] %d (country, item) pairs with production | real SLC/t ratio %d-%d: median %.2f, 5-95%% [%.2f, %.2f] | STALE (< 0.5): %d pairs, %.1f%% of production | with a producer price: %d\n",
            nrow(pair), min(WIN), max(WIN), median(pair$real_slc_ratio, na.rm = TRUE), quantile(pair$real_slc_ratio, .05, na.rm = TRUE), quantile(pair$real_slc_ratio, .95, na.rm = TRUE),
            pair[grepl("STALE", verdict), .N], 100 * pair[grepl("STALE", verdict), sum(prod_kt)] / pair[, sum(prod_kt)], sum(pair$has_pp)))
cat("\nSTALE pairs by country (n, production share of the country's pairs):\n")
print(pair[, .(n_stale = sum(grepl("STALE", verdict)), n = .N, prod_share_stale = round(sum(prod_kt[grepl("STALE", verdict)]) / sum(prod_kt), 2), defl_ratio = round(defl_ratio[1], 2)), by = Area][n_stale > 0][order(-n_stale)], nrows = 60)
sel <- opt("--pairs", "Nigeria:Groundnuts, excluding shelled;Nigeria:Oil palm fruit;Angola:Cassava, fresh;Angola:Bananas;Angola:Maize (corn);Sudan:Millet;Sudan:Wheat;Sudan:Bananas")
sp <- rbindlist(lapply(strsplit(strsplit(sel, ";")[[1]], ":"), function(x) data.table(Area = x[1], Item = x[2])))
cat("\nnamed pairs:\n")
print(merge(sp, pair, by = c("Area", "Item"), sort = FALSE)[, .(Area, Item = substr(Item, 1, 16), prod_kt = round(prod_kt), has_pp, usd_t = paste0(round(usd_t_first), "->", round(usd_t_last)), slc_t = paste0(round(slc_t_first), "->", round(slc_t_last)), slc_x = round(slc_ratio, 2), defl_x = round(defl_ratio, 2), real_x = round(real_slc_ratio, 3), fx = paste0(round(fx_first), "->", round(fx_last)), verdict)], nrows = 20)
cat("\nyearly detail, named pairs:\n")
print(merge(sp, m, by = c("Area", "Item"), sort = FALSE)[order(Area, Item, year), .(Area, Item = substr(Item, 1, 16), year, prod_kt = round(prod / 1e3), slc_t = round(slc_t), usd_t = round(usd_t, 1), fx_official_implied = round(fx, 1), defl = round(defl), pp_usd)], nrows = 60)
if (!nzchar(OUT)) OUT <- file.path(tempdir(), "price_stale_slc.csv")
fwrite(pair, OUT); cat(sprintf("\n[stale-slc] pair table written to %s\n", OUT))
