#!/usr/bin/env Rscript
# R/checks/probe_042_price_fill.R
# ================================
# Read-only probe of R/0.4.2_create_crop_vop_nominal_usd.R's price inference.
#
# 2026-09-26: the first G6 run showed the rebuilt crop nominal-USD exposure had
# moved against the live product by crop-specific factors that are IDENTICAL at
# both resolutions (wheat 3.9x, oilpalm 3.6x, plantain 1.7x, cowpea 0.66x; ZWE
# wheat 112x, ZMB wheat 23x, oilpalm 20.4x in TZA/BDI/COD/MDG and 5.8x in eight
# Central African countries, cassava 2.11x across Central Africa). Region-shaped
# constants are the signature of add_nearby()'s MEAN-based neighbour / region /
# continent fill propagating one outlier producer price. This probe runs 0.4.2
# sections 1.1-1.9 and 3 (FAO loads + price inference) exactly as the script
# does, then prints where every price came from and which ones are outliers
# against the world price. It writes nothing and never reaches section 4.
#
# Usage (cglabs, repo root, ~1-3 min; EXPOSURE_RES only picks the base grid):
#   EXPOSURE_RES=0.25 Rscript R/checks/probe_042_price_fill.R [--crops whea,oilp,plnt,cass,cowp] [--top 30]

t0 <- Sys.time()
.ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) cat(sprintf("[%s] [probe-042] %s\n", .ts(), sprintf(fmt, ...)))
args <- commandArgs(trailingOnly = TRUE)
opt  <- function(x, d) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
CROPS <- strsplit(opt("--crops", "whea,oilp,plnt,cass,cowp"), ",")[[1]]
TOP   <- as.integer(opt("--top", "30"))
if (!nzchar(Sys.getenv("EXPOSURE_RES"))) Sys.setenv(EXPOSURE_RES = "0.25")

setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
.log("sourcing %s", setup); suppressMessages(suppressWarnings(source(setup)))
suppressPackageStartupMessages({ library(data.table); library(terra) })

script <- file.path(project_dir, "R", "0.4.2_create_crop_vop_nominal_usd.R")
src <- readLines(script)
cut_2 <- grep("^## 2\\) Load spam", src)[1]; cut_3 <- grep("^## 3\\) Infer missing prices", src)[1]; cut_4 <- grep("^## 4\\) Multiply", src)[1]
stopifnot(!is.na(cut_2), !is.na(cut_3), !is.na(cut_4), cut_2 < cut_3, cut_3 < cut_4)
.log("evaluating %s lines 1-%d (sections 1.x) and %d-%d (section 3); section 2 (rasters) and 4 (writes) skipped", basename(script), cut_2 - 1, cut_3, cut_4 - 1)
run_lines <- function(a, b) {
  ex <- parse(text = src[a:b], keep.source = FALSE)
  for (e in ex) {
    txt <- paste(deparse(e), collapse = " ")
    if (grepl("0_server_setup", txt, fixed = TRUE)) next          # already sourced, and cwd has moved
    if (grepl("fwrite(", txt, fixed = TRUE)) { .log("skipped (read-only): %s", substr(txt, 1, 70)); next }   # section 3 writes the audit CSV the cross-basis gate reads; a probe must not touch it
    eval(e, envir = globalenv())
  }
}
run_lines(1, cut_2 - 1)
run_lines(cut_3, cut_4 - 1)
stopifnot(exists("prod_merge"), exists("price_usd_list"))

## FAO inputs actually read -------------------------------------------------
.log("--- FAO inputs")
for (f in c(path_vop_africa_fao, path_prices_fao, fao_econ_file_world, path_prod_africa_fao)) if (exists(basename(f)) || file.exists(f))
  .log("%-45s mtime %s  size %.0f MB", basename(f), format(file.mtime(f), "%Y-%m-%d"), file.size(f) / 1e6)
yrs <- prod_merge[!is.na(price_usd), .(n_prices = .N), by = year][order(year)]
.log("producer-price observations per year (Africa file): %s", paste(sprintf("%d:%d", yrs$year, yrs$n_prices), collapse = " "))

## The y2021 fill table ------------------------------------------------------
p <- copy(price_usd_list[["nominal-usd-2021"]])
p[, source := price_source]   # 2026-10-01: the script names the source per row (implied / producer / fills / basis fallback)
p[, ratio_world := price_usd_final / price_usd_global]
.log("--- fill sources, y2021 (%d country x crop rows)", nrow(p))
print(p[, .N, by = source][order(-N)])
if ("value_intd15" %in% names(p)) {
  .log("--- basis fallbacks (own price replaced by the item-median nominal/intld factor x constant-I$ value)")
  print(p[source == "basis fallback", .(iso3, atlas_name, price_usd_own = signif(price_usd, 4), price_usd_final = signif(price_usd_final, 4), basis_ratio, basis_median, production_t = signif(production_t, 4))][order(atlas_name, iso3)], nrows = 80)
}

## Outliers vs world -----------------------------------------------------------
.log("--- top %d prices vs the world median (|log ratio|), all crops", TOP)
show <- function(d) d[, .(iso3, atlas_name, source, price_usd_final = signif(price_usd_final, 4), price_usd_global = signif(price_usd_global, 4),
                          ratio_world = signif(ratio_world, 3), production_t = signif(production_t, 4))]
print(show(p[is.finite(ratio_world)][order(-abs(log(ratio_world)))][1:min(TOP, .N)]), nrows = TOP)

## The suspect crops in full ------------------------------------------------------
for (cr in CROPS) {
  .log("--- %s: every country, y2021 fill", cr)
  d <- p[atlas_name == cr][order(-ratio_world)]
  print(show(d), nrows = 100)
  # raw own-price series for the countries whose final price is > 3x or < 1/3 world
  odd <- d[is.finite(ratio_world) & (ratio_world > 3 | ratio_world < 1 / 3), iso3]
  if (length(odd)) {
    .log("%s: raw FAO USD/t series for %s", cr, paste(odd, collapse = ","))
    raw <- dcast(prod_merge[atlas_name == cr & iso3 %in% odd & year >= 2014, .(iso3, year, price_usd = signif(price_usd, 4))], iso3 ~ year, value.var = "price_usd")
    print(raw, nrows = 60)
  }
  # region / neighbour medians that would be inherited by the fill
  .log("%s: region medians (over member countries' own prices)", cr)
  print(p[atlas_name == cr, .(price_usd_region = signif(unique(price_usd_region), 4)[1], price_usd_continent = signif(unique(price_usd_continent), 4)[1],
                              price_usd_global = signif(unique(price_usd_global), 4)[1]),
          by = .(region = sapply(iso3, function(i) { r <- names(regions)[sapply(regions, function(X) i %in% X)]; if (length(r)) r[1] else NA_character_ }))])
}
.log("done in %.1f min (read-only; nothing written)", as.numeric(difftime(Sys.time(), t0, units = "mins")))
