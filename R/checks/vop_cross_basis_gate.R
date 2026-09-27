#!/usr/bin/env Rscript
# R/checks/vop_cross_basis_gate.R
# ===============================
# Per-crop, per-country sanity gate across the two value-of-production bases that
# 0.4.4 writes into one table: nominal-usd-2021 (0.4.2, producer prices x SPAM
# production) against intld15-2021 (0.4.0, FAOSTAT gross production value in
# constant 2015 international dollars distributed by production share).
#
# Their ratio is a price-level-times-deflator factor and is NOT expected to be 1,
# nor tight: on the 2026-09-25 objects the material-pair median was 1.3 with a
# 5-95 % range of 0.2-4.0, because FAOSTAT's I$ valuation and its producer prices
# disagree per crop in ordinary ways. What the ratio DOES catch is a broken
# input on one side: 2026-09-27 the contaminated nominal-USD prices showed as
# ZWE wheat 122x, COD/BDI/MDG/TZA oilpalm 41-47x, ZMB wheat 45x, while every
# livestock pair sat at ~1. The millet split (#38) shows the other way round
# (intld inflated: KEN pearl-millet ~1/4000). Neither ALL-CROPS national gate
# can see these; this one can.
#
# First run on the live res-25 object (2026-09-27) also surfaced two PRE-EXISTING
# defects that the old-vs-new drift check could not (they were already in the
# 2025-11 publication and the live hazard product): Sudan's nominal-USD values are
# ~1/5000 of intld across a dozen crops (a producer-price currency artefact in the
# other direction), and Nigeria banana ~1/1000. The world-median clip in
# R/price_fill.R removes those prices too, so after the 0.4.2 fix these pairs are
# EXPECTED to move by three to four orders of magnitude against the last good
# publication. This gate, not the old-vs-new comparison, is the arbiter for them.
#
# Populations + bounds (defaults; flags override):
#   material    intld >= --min-ref (1e6): every pair within [1/--band, --band] (10)
#   per-crop    continental median of material pairs within [1/--band-crop, --band-crop] (5)
#   immaterial  reported, not gated
#   one-sided   value on one basis only -> reported (small-millet is expected here, #38)
#
# Usage (node, seconds; arrow only):
#   Rscript R/checks/vop_cross_basis_gate.R --res 0.25 [--band 10] [--band-crop 5] [--min-ref 1e6]
#   Rscript R/checks/vop_cross_basis_gate.R --file <any 0.4.4 combined table>
# exit 0 PASS, 1 FAIL.

t0 <- Sys.time()
.ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) cat(sprintf("[%s] [gate-cross-basis] %s\n", .ts(), sprintf(fmt, ...)))
args <- commandArgs(trailingOnly = TRUE)
opt <- function(x, d) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
RES <- opt("--res", "0.25"); RES_TAG <- sprintf("res-%02d", round(as.numeric(RES) * 100))
BAND <- as.numeric(opt("--band", "10")); BAND_CROP <- as.numeric(opt("--band-crop", "5")); MIN_REF <- as.numeric(opt("--min-ref", "1e6"))
MIN_N_CROP <- as.integer(opt("--min-n-crop", "3"))   # a per-crop median needs a few countries to mean anything (rapeseed has one)
FILE <- opt("--file", "")
suppressPackageStartupMessages({ library(arrow); library(dplyr); library(data.table) })
if (!nzchar(FILE)) {
  setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
  .log("sourcing %s", setup); suppressMessages(suppressWarnings(source(setup)))
  ref_dir <- if (exists("exposure_dir")) exposure_dir else atlas_dirs$data_dir$exposure
  FILE <- file.path(ref_dir, sprintf("exposure_adm_sum_spam20-20_glw420-20_%s.parquet", RES_TAG))
}
if (!file.exists(FILE)) { .log("MISSING %s", FILE); quit(status = 1) }
.log("table = %s (mtime %s)", FILE, format(file.mtime(FILE), "%Y-%m-%d %H:%M"))

d <- arrow::open_dataset(FILE) |>
  dplyr::filter(is.na(admin1_name), exposure == "vop", unit %in% c("nominal-usd-2021", "intld15-2021")) |>
  dplyr::select(iso3, crop, unit, tech, value) |> dplyr::collect() |> as.data.table()
d <- d[(tech == "all" | is.na(tech)) & is.finite(value)]
w <- dcast(d[, .(value = sum(value)), by = .(iso3, crop, unit)], iso3 + crop ~ unit, value.var = "value")
setnames(w, c("nominal-usd-2021", "intld15-2021"), c("nominal", "intld"), skip_absent = TRUE)
for (c in c("nominal", "intld")) if (!c %in% names(w)) w[[c]] <- NA_real_
w[, livestock := grepl("cattle|sheep|goats|pigs|poultry|total-", crop)]
w[, ratio := nominal / intld]
one_sided <- w[(is.na(nominal) | nominal == 0) != (is.na(intld) | intld == 0)]
both <- w[!is.na(nominal) & !is.na(intld) & nominal > 0 & intld > 0]
material <- both[intld >= MIN_REF]
immaterial <- both[intld < MIN_REF]
per_crop <- material[, .(n = .N, med = median(ratio), lo = min(ratio), hi = max(ratio)), by = .(crop, livestock)][order(-abs(log(med)))]
show <- function(x) x[, .(iso3, crop, nominal = signif(nominal, 4), intld = signif(intld, 4), ratio = signif(ratio, 3))]

.log("pairs: %d both bases | %d material (intld >= %s) | %d immaterial | %d one-sided", nrow(both), nrow(material), format(MIN_REF, big.mark = ","), nrow(immaterial), nrow(one_sided))
.log("material ratio nominal/intld: median %s | 5-95%% [%s, %s] | livestock median %s",
     signif(median(material$ratio), 3), signif(quantile(material$ratio, 0.05), 3), signif(quantile(material$ratio, 0.95), 3),
     if (any(material$livestock)) signif(median(material[livestock == TRUE, ratio]), 3) else NA)
out_pair <- material[ratio > BAND | ratio < 1 / BAND][order(-abs(log(ratio)))]
out_crop <- per_crop[n >= MIN_N_CROP & (med > BAND_CROP | med < 1 / BAND_CROP)]
cat("\nper-crop medians (worst first):\n"); print(per_crop[1:min(15, .N)][, .(crop, n, med = signif(med, 3), lo = signif(lo, 3), hi = signif(hi, 3))], nrows = 15)
if (nrow(out_pair)) { .log("FAIL: %d material pairs outside [1/%g, %g]:", nrow(out_pair), BAND, BAND); print(show(out_pair)[1:min(25, .N)], nrows = 25) } else .log("ok: every material pair within [1/%g, %g]", BAND, BAND)
if (nrow(out_crop)) { .log("FAIL: %d crops whose material median is outside [1/%g, %g]: %s", nrow(out_crop), BAND_CROP, BAND_CROP, paste(out_crop$crop, collapse = ",")) } else .log("ok: every per-crop median within [1/%g, %g]", BAND_CROP, BAND_CROP)
if (nrow(one_sided)) { .log("info: %d one-sided pairs (value on one basis only; small-millet expected, #38): crops %s", nrow(one_sided), paste(sort(unique(one_sided$crop)), collapse = ",")) }
pass <- nrow(out_pair) == 0 && nrow(out_crop) == 0
.log("GATE %s in %.1f min", if (pass) "PASS" else "FAIL", as.numeric(difftime(Sys.time(), t0, units = "mins")))
quit(status = if (pass) 0 else 1)
