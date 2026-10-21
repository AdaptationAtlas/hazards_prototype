#!/usr/bin/env Rscript
# R/checks/probe_041_livestock_price.R
# ====================================
# Read-only measurement for issue #47: R/0.4.1_create_livestock_exposure.R §4.1 builds a
# gap-filled national price `price_usd_final` (own -> longer-series median -> neighbours ->
# region -> goat/sheep-milk world override -> continent -> world) and then computes
#   vop_usd_nominal := production_t * price_usd_global
# so every country's livestock nominal-USD value is priced at the world median producer
# price and `price_usd_final` is never used. Present since a8ca8ee (2025-07-10).
#
# This probe runs 0.4.1 sections a-b, 1, 2.3 (the glw2atlas map only) and 3-4.2 exactly as the
# script does (sections 2.0-2.2 and 2.4-2.5 load GLW rasters and are skipped; nothing in the
# evaluated range writes), then prices each (country, product) three ways:
#   A  as published   production_t x price_usd_global                       (line 483 today)
#   B  as coded       production_t x price_usd_final                        (the one-token fix)
#   C  crop method    production_t x R/price_fill.R price: own producer prices clipped to
#                     [1/5, 5] x the world median per item-year BEFORE averaging, window
#                     median (longer series as fallback), then MEDIAN neighbours -> region ->
#                     continent -> world fills; same goat/sheep-milk world override as B
# B inherits add_nearby()'s MEAN fills and unclipped producer prices -- the two defects the
# crop side hit in 2026-09 (ZWE wheat 115x world; see R/price_fill.R header). C shows what a
# fix that reuses the crop-side cleaning would give.
#
# GATE 0 (reproduction): A must equal the `value` column of the CSV the last 0.4.1 run wrote,
#   <glw_pro_dir>/fao_vop/glw4-2020_vop_nominal-usd-2021-prices-used.csv, per (iso3, atlas_name).
#   If it does not, the probe is not measuring what is published: STOP and report.
#
# Why national ratios are enough: §5 distributes each country's VoP over its GLW pixels by
# share (glw_prop) and splits highland/tropical by mask, so every published livestock row
# (exposure reference and vop_nominal-usd21 hazard tier) scales by exactly
# B/A (or C/A) for its (iso3, glw3 class), up to per-pixel rounding. No raster run is needed
# to size the effect.
#
# Usage (cglabs, repo root, a few minutes; EXPOSURE_RES only picks the grid section b loads):
#   EXPOSURE_RES=0.25 Rscript R/checks/probe_041_livestock_price.R [--top 30] [--out /abs/path.csv]
# --out writes the per-(year set, iso3, product) table; default writes nothing.

t0 <- Sys.time()
.ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) cat(sprintf("[%s] [probe-041] %s\n", .ts(), sprintf(fmt, ...)))
args <- commandArgs(trailingOnly = TRUE)
opt  <- function(x, d) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
TOP <- as.integer(opt("--top", "30"))
OUT <- opt("--out", "")
if (!nzchar(Sys.getenv("EXPOSURE_RES"))) Sys.setenv(EXPOSURE_RES = "0.25")

setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
.log("sourcing %s", setup); suppressMessages(suppressWarnings(source(setup)))
suppressPackageStartupMessages({ library(data.table); library(terra) })
source(file.path(project_dir, "R", "price_fill.R"))

script <- file.path(project_dir, "R", "0.4.1_create_livestock_exposure.R")
src <- readLines(script)
cut <- function(re) { i <- grep(re, src)[1]; if (is.na(i)) stop("section marker not found in 0.4.1: ", re); i }
c2  <- cut("^# 2\\) Load GLW4 data"); c23 <- cut("^## 2\\.3\\) map GLW values"); c24 <- cut("^# 2\\.4\\) Load high/low mask")
c3  <- cut("^# 3\\) Load FAOstat data"); c43 <- cut("^## 4\\.3\\) Not Run")
stopifnot(c2 < c23, c23 < c24, c24 < c3, c3 < c43)
run_lines <- function(a, b) {
  for (e in parse(text = src[a:b], keep.source = FALSE)) {
    txt <- paste(deparse(e), collapse = " ")
    if (grepl("0_server_setup", txt, fixed = TRUE)) next     # already sourced, and cwd has moved
    if (grepl("fwrite|writeRaster|ensure_dir", txt)) stop("probe would evaluate a write: ", substr(txt, 1, 120))
    eval(e, envir = globalenv())
  }
}
.log("evaluating %s lines 1-%d, %d-%d, %d-%d (GLW raster sections skipped)", basename(script), c2 - 1, c23, c24 - 1, c3, c43 - 1)
run_lines(1, c2 - 1); run_lines(c23, c24 - 1); run_lines(c3, c43 - 1)
stopifnot(exists("nominal_usd"), exists("intd_2015"), exists("prod_merge"), exists("prod_price_global"), exists("glw2atlas"))

## FAO inputs -------------------------------------------------------------------
.log("--- FAO inputs")
for (f in c(vop_file, econ_file, fao_econ_file_world, prod_file))
  .log("%-48s mtime %s  size %.0f MB", basename(f), format(file.mtime(f), "%Y-%m-%d"), file.size(f) / 1e6)

## Per year set: A, B, C ------------------------------------------------------------
MILK <- c("goat_milk", "sheep_milk")
tabs <- lapply(seq_along(year_sets), function(i) {
  yrs <- year_sets[[i]]; long <- (min(yrs) - 5):max(yrs)
  d <- copy(nominal_usd[[i]])
  # C: clip own producer prices per item-year against the world median, then window medians
  clip <- clip_prices_to_world_band(prod_merge[, .(iso3, atlas_name, year = as.integer(year), price_usd)],
                                    prod_price_global[, .(atlas_name, year = as.integer(year), price_usd)])
  k <- clip$kept
  own <- merge(k[year %in% yrs,  .(p_win  = if (all(is.na(price_usd))) NA_real_ else median(price_usd, na.rm = TRUE)), .(iso3, atlas_name)],
               k[year %in% long, .(p_long = if (all(is.na(price_usd))) NA_real_ else median(price_usd, na.rm = TRUE)), .(iso3, atlas_name)],
               by = c("iso3", "atlas_name"), all = TRUE)
  own[, price_usd := fifelse(!is.na(p_win), p_win, p_long)]
  rc <- merge(d[, .(iso3, atlas_name, production_t)], own[, .(iso3, atlas_name, price_usd)], by = c("iso3", "atlas_name"), all.x = TRUE)
  w  <- unique(d[, .(atlas_name, price_world = price_usd_global)])
  # fill_price_robust() stops on an iso3 absent from `regions`; give any such country a singleton
  # region (its region median excludes itself, so it falls through to continent) and say so.
  miss <- setdiff(rc$iso3, unlist(regions)); regs <- regions
  if (length(miss)) { .log("%s: %d iso3 not in regions, singleton regions: %s", names(year_sets)[i], length(miss), paste(miss, collapse = ",")); for (m in miss) regs[[paste0("_", m)]] <- m }
  rc <- fill_price_robust(rc, value_field = "price_usd", group_field = "atlas_name",
                          neighbors = african_neighbors, regions = regs, world = w, stat = "median")
  rc[atlas_name %in% MILK, `:=`(price_usd_final = price_usd_global, price_source = "milk: world override")]
  d <- merge(d, rc[, .(iso3, atlas_name, price_C = price_usd_final, source_C = price_source)], by = c("iso3", "atlas_name"), all.x = TRUE)
  # B's source, in the chain order line 480-483 applies it
  d[, source_B := fcase(atlas_name %in% MILK, "milk: world override",
                        !is.na(price_usd), "own (window median)", !is.na(price_median), "own (longer series)",
                        !is.na(price_usd_neighbors), "neighbours mean", !is.na(price_usd_region), "region mean",
                        !is.na(price_usd_continent), "continent mean", !is.na(price_usd_global), "world median", default = "none")]
  d[, `:=`(vop_A = vop_usd_nominal, vop_B = production_t * price_usd_final, vop_C = production_t * price_C)]
  d <- merge(d, intd_2015[[i]][, .(iso3, atlas_name, vop_intd15)], by = c("iso3", "atlas_name"), all.x = TRUE)
  d <- merge(d, glw2atlas, by = "atlas_name", all.x = TRUE)
  d[, year_set := names(year_sets)[i]]
  list(d = d, dropped = clip$dropped)
})
names(tabs) <- names(year_sets)
p  <- tabs[["y2021"]]$d
bn <- function(x) sprintf("%.3f B", x / 1e9)

## GATE 0: A reproduces what 0.4.1 last wrote ------------------------------------------
live_csv <- file.path(glw_pro_dir, "fao_vop", paste0(dataset_name, "_vop_nominal-usd-2021-prices-used.csv"))
if (!file.exists(live_csv)) stop("GATE 0: live prices-used CSV not found: ", live_csv)
.log("--- GATE 0: A vs %s (mtime %s)", basename(live_csv), format(file.mtime(live_csv), "%Y-%m-%d %H:%M"))
live <- unique(fread(live_csv)[, .(iso3, atlas_name, value_live = value)])
g0 <- merge(p[, .(iso3, atlas_name, vop_A)], live, by = c("iso3", "atlas_name"), all = TRUE)
g0[, rel := abs(vop_A - value_live) / pmax(abs(value_live), 1)]
n_bad <- g0[(is.na(vop_A) != is.na(value_live)) | (!is.na(rel) & rel > 1e-9), .N]
.log("GATE 0: %d rows; live total %s, probe A total %s; %d rows differ", nrow(g0),
     bn(sum(g0$value_live, na.rm = TRUE)), bn(sum(g0$vop_A, na.rm = TRUE)), n_bad)
if (n_bad > 0) {
  print(g0[(is.na(vop_A) != is.na(value_live)) | (!is.na(rel) & rel > 1e-9)][order(-rel)][1:min(TOP, .N)])
  stop("GATE 0 FAILED: probe A does not reproduce the published livestock nominal VoP. Stop and report.")
}
.log("GATE 0 PASS: probe A reproduces the published values")

## Size of the effect, y2021 (feeds vop_nominal-usd21 and the reference's nominal-usd-2021 rows)
.log("--- y2021 totals (Africa, all countries)")
tot <- p[, .(A = sum(vop_A, na.rm = TRUE), B = sum(vop_B, na.rm = TRUE), C = sum(vop_C, na.rm = TRUE), intld = sum(vop_intd15, na.rm = TRUE))]
.log("livestock nominal: A %s | B %s (B/A %.3f) | C %s (C/A %.3f) | constant-I$ for scale %s",
     bn(tot$A), bn(tot$B), tot$B / tot$A, bn(tot$C), tot$C / tot$A, bn(tot$intld))
.log("--- by GLW class (the published tier's species rows, before highland/tropical split)")
print(p[, .(A_bn = sum(vop_A, na.rm = TRUE) / 1e9, B_bn = sum(vop_B, na.rm = TRUE) / 1e9, C_bn = sum(vop_C, na.rm = TRUE) / 1e9), by = glw3_name][
  , `:=`(B_over_A = round(B_bn / A_bn, 3), C_over_A = round(C_bn / A_bn, 3))][order(-A_bn)])
.log("--- by product")
print(p[, .(A_bn = sum(vop_A, na.rm = TRUE) / 1e9, B_bn = sum(vop_B, na.rm = TRUE) / 1e9, C_bn = sum(vop_C, na.rm = TRUE) / 1e9,
            n_ctry = sum(!is.na(production_t))), by = atlas_name][, `:=`(B_over_A = round(B_bn / A_bn, 3), C_over_A = round(C_bn / A_bn, 3))][order(-A_bn)])

## Per (country, GLW class) scale factors = what each published livestock row would move by
cls <- p[, .(A = sum(vop_A, na.rm = TRUE), B = sum(vop_B, na.rm = TRUE), C = sum(vop_C, na.rm = TRUE)), by = .(iso3, glw3_name)][A > 0]
cls[, `:=`(rB = B / A, rC = C / A)]
q <- function(x) paste(sprintf("%.2f", quantile(x, c(0, .1, .25, .5, .75, .9, 1), na.rm = TRUE)), collapse = " / ")
.log("--- per (iso3, GLW class) row factor, %d rows; quantiles min/10/25/50/75/90/max", nrow(cls))
.log("B/A: %s", q(cls$rB)); .log("C/A: %s", q(cls$rC))
for (b in c(1.1, 1.5, 2)) .log("rows moving > %.0f%% either way: B %d (%.0f%% of A value), C %d (%.0f%% of A value)", 100 * (b - 1),
  cls[rB > b | rB < 1 / b, .N], 100 * cls[rB > b | rB < 1 / b, sum(A)] / sum(cls$A),
  cls[rC > b | rC < 1 / b, .N], 100 * cls[rC > b | rC < 1 / b, sum(A)] / sum(cls$A))
.log("--- top %d (iso3, GLW class) rows by |B - A| USD", TOP)
print(cls[order(-abs(B - A))][1:min(TOP, .N), .(iso3, glw3_name, A_mn = round(A / 1e6), B_mn = round(B / 1e6), C_mn = round(C / 1e6), rB = round(rB, 2), rC = round(rC, 2))], nrows = TOP)

## Where the B and C prices come from, and the outliers each would publish ---------------------
.log("--- price sources, y2021 (country x product rows with production)")
pp <- p[!is.na(production_t) & production_t > 0]
print(merge(pp[, .(B_n = .N, B_pct_A = round(100 * sum(vop_A, na.rm = TRUE) / sum(pp$vop_A, na.rm = TRUE), 1)), by = .(source = source_B)],
            pp[, .(C_n = .N), by = .(source = source_C)], by = "source", all = TRUE))
.log("--- C: own producer prices clipped (outside [1/5, 5] x world median for item-year), y2021 run")
print(tabs[["y2021"]]$dropped, nrows = 60)
pp[, `:=`(ratio_B = price_usd_final / price_usd_global, ratio_C = price_C / price_usd_global)]
.log("--- top %d B prices vs world (|log ratio|) -- what the one-token fix would publish", TOP)
print(pp[is.finite(ratio_B)][order(-abs(log(ratio_B)))][1:min(TOP, .N),
  .(iso3, atlas_name, source_B, price_B = signif(price_usd_final, 4), price_world = signif(price_usd_global, 4), ratio_B = signif(ratio_B, 3),
    price_C = signif(price_C, 4), ratio_C = signif(ratio_C, 3), production_t = signif(production_t, 4))], nrows = TOP)
## Basis check against FAOSTAT's own constant-I$ GPV: nominal/intld per row vs the product median
pp[, `:=`(fB = vop_B / vop_intd15, fC = vop_C / vop_intd15)]
pp[, `:=`(fB_rel = fB / median(fB[is.finite(fB)]), fC_rel = fC / median(fC[is.finite(fC)])), by = atlas_name]
.log("--- basis: rows whose nominal/constant-I$ factor is > 3x or < 1/3 the product median (B %d, C %d)",
     pp[is.finite(fB_rel) & (fB_rel > 3 | fB_rel < 1/3), .N], pp[is.finite(fC_rel) & (fC_rel > 3 | fC_rel < 1/3), .N])
print(pp[is.finite(fB_rel) & (fB_rel > 3 | fB_rel < 1/3)][order(-abs(log(fB_rel)))][1:min(TOP, .N),
  .(iso3, atlas_name, source_B, fB_rel = signif(fB_rel, 3), fC_rel = signif(fC_rel, 3), vop_A_mn = round(vop_A / 1e6), vop_B_mn = round(vop_B / 1e6))], nrows = TOP)

## Other year sets (not published in a tier; reported for completeness) ------------------------
for (ys in setdiff(names(tabs), "y2021")) {
  d <- tabs[[ys]]$d
  .log("%s: A %s | B %s (%.3f) | C %s (%.3f)", ys, bn(sum(d$vop_A, na.rm = TRUE)), bn(sum(d$vop_B, na.rm = TRUE)),
       sum(d$vop_B, na.rm = TRUE) / sum(d$vop_A, na.rm = TRUE), bn(sum(d$vop_C, na.rm = TRUE)), sum(d$vop_C, na.rm = TRUE) / sum(d$vop_A, na.rm = TRUE))
}

if (nzchar(OUT)) {
  all_rows <- rbindlist(lapply(tabs, `[[`, "d"), fill = TRUE)
  fwrite(all_rows[, .(year_set, iso3, atlas_name, glw3_name, production_t, price_usd_global, price_usd_final, source_B, price_C, source_C,
                      vop_A, vop_B, vop_C, vop_intd15)], OUT)
  .log("wrote %s (%d rows)", OUT, nrow(all_rows))
}
.log("done in %.1f min", as.numeric(difftime(Sys.time(), t0, units = "mins")))
