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

# Which SIDE is broken? Both bases share the SPAM footprint, so the implied nominal
# price nominal / SPAM tonnage is comparable across countries for one crop; if a
# pair's implied price sits inside a band around the crop's median, the nominal side
# is sound and the out-of-band ratio is the intld side (0.4.0 distributing a full
# FAO value over a negligible SPAM footprint: Sudan, 0.05 Mt in SPAM for the whole
# country; Nigeria banana, 1.5 kt against 6.5 Mt plantain; the #38 millet split).
# Those are reported as a named residual, not a FAIL, unless --fail-on-intld-side.
PRICE_BAND_SIDE <- as.numeric(opt("--price-band", "5"))
FAIL_INTLD <- "--fail-on-intld-side" %in% args
# The reference price must be INDEPENDENT of the table: when most countries of a
# crop are contaminated (oil palm, 17 of 21 on the 2026-09-25 object) the crop
# median is contaminated too and the bad pairs look sound. Use the world median
# price per crop from 0.4.2's fill-source audit CSV
# (mapspam_pro_dir/fao_prices/crop_price_nominal-usd-2021-t_fill-sources_<tag>.csv,
# auto-detected after a setup-sourced run; --world-prices <csv> overrides); fall
# back to the crop median only for crops it does not cover, and say so.
WORLD_CSV <- opt("--world-prices", "")
if (!nzchar(WORLD_CSV) && exists("mapspam_pro_dir")) {
  cand <- file.path(mapspam_pro_dir, "fao_prices", sprintf("crop_price_nominal-usd-2021-t_fill-sources_%s.csv", RES_TAG))
  if (file.exists(cand)) WORLD_CSV <- cand
}
repo_root <- local({
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  d <- if (exists("project_dir")) project_dir else if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd()
  d
})
world_price <- NULL
if (nzchar(WORLD_CSV) && file.exists(WORLD_CSV)) {
  wp <- fread(WORLD_CSV)[!is.na(price_usd_global), .(price_world = median(price_usd_global, na.rm = TRUE)), by = atlas_name]
  codes <- fread(file.path(repo_root, "metadata", "SpamCodes.csv"))[!is.na(Code) & Code != "", .(atlas_name = tolower(Code), crop = gsub(" ", "-", tolower(Fullname)))]
  # 0.4.2 prices millet and coffee as one FAO item each; both SPAM crops inherit that price
  codes <- rbind(codes, data.table(atlas_name = c("mill", "mill", "coff", "coff"), crop = c("pearl-millet", "small-millet", "arabica-coffee", "robusta-coffee")))
  world_price <- merge(wp, codes, by = "atlas_name", allow.cartesian = TRUE)[, .(crop, price_world)]
  world_price <- world_price[!duplicated(crop)]
  .log("world price reference: %s (%d crops)", WORLD_CSV, nrow(world_price))
} else .log("WARN: no world-price reference (pass --world-prices <0.4.2 fill-sources csv>); falling back to the crop median implied price, which is blind to a majority-contaminated crop")
d <- arrow::open_dataset(FILE) |>
  dplyr::filter(is.na(admin1_name), exposure %in% c("vop", "prod"), unit %in% c("nominal-usd-2021", "intld15-2021", "t")) |>
  dplyr::select(iso3, crop, exposure, unit, tech, value) |> dplyr::collect() |> as.data.table()
d <- d[(tech == "all" | is.na(tech)) & is.finite(value)]
d[, key := fifelse(exposure == "prod", "prod_t", unit)]
w <- dcast(d[, .(value = sum(value)), by = .(iso3, crop, key)], iso3 + crop ~ key, value.var = "value")
setnames(w, c("nominal-usd-2021", "intld15-2021"), c("nominal", "intld"), skip_absent = TRUE)
for (c in c("nominal", "intld", "prod_t")) if (!c %in% names(w)) w[[c]] <- NA_real_
w[, livestock := grepl("cattle|sheep|goats|pigs|poultry|total-", crop)]
w[, ratio := nominal / intld]
w[, implied_price := nominal / prod_t]
w[is.finite(implied_price) & prod_t > 0, price_med := median(implied_price[implied_price > 0], na.rm = TRUE), by = crop]
if (!is.null(world_price)) { w <- merge(w, world_price, by = "crop", all.x = TRUE); w[!is.na(price_world), price_med := price_world]; w[, price_world := NULL] }
w[, nominal_side_ok := livestock | (is.finite(implied_price) & implied_price > 0 & implied_price <= PRICE_BAND_SIDE * price_med & implied_price >= price_med / PRICE_BAND_SIDE)]
one_sided <- w[(is.na(nominal) | nominal == 0) != (is.na(intld) | intld == 0)]
both <- w[!is.na(nominal) & !is.na(intld) & nominal > 0 & intld > 0]
material <- both[intld >= MIN_REF]
immaterial <- both[intld < MIN_REF]
per_crop <- material[, .(n = .N, med = median(ratio), lo = min(ratio), hi = max(ratio)), by = .(crop, livestock)][order(-abs(log(med)))]
show <- function(x) x[, .(iso3, crop, nominal = signif(nominal, 4), intld = signif(intld, 4), ratio = signif(ratio, 3),
                          spam_t = signif(prod_t, 3), implied_usd_t = signif(implied_price, 3), ref_usd_t = signif(price_med, 3))]

.log("pairs: %d both bases | %d material (intld >= %s) | %d immaterial | %d one-sided", nrow(both), nrow(material), format(MIN_REF, big.mark = ","), nrow(immaterial), nrow(one_sided))
.log("material ratio nominal/intld: median %s | 5-95%% [%s, %s] | livestock median %s",
     signif(median(material$ratio), 3), signif(quantile(material$ratio, 0.05), 3), signif(quantile(material$ratio, 0.95), 3),
     if (any(material$livestock)) signif(median(material[livestock == TRUE, ratio]), 3) else NA)
out_pair <- material[ratio > BAND | ratio < 1 / BAND][order(-abs(log(ratio)))]
out_nominal <- out_pair[nominal_side_ok == FALSE]
out_intld   <- out_pair[nominal_side_ok == TRUE]
# 2026-10-05 (cglabs C3 stop): a pair 0.4.0 allocated NO constant-I$ value to nationally (no FAO GPV row
# for it, or guarded) can still show intld value in this table on the 0.25 deg grid - neighbours' value
# in border cells that 0.4.4's zones assign to it (the accepted #18 one-cell-one-zone allocation): TCD
# yams (no FAO record, 461 kt in SPAM, ratio 184), BEN bean. Classified from 0.4.0's allocation audit
# CSV (an input to the table, independent of it) and reported, not failed.
ALLOC_CSV <- opt("--allocation", "")
if (!nzchar(ALLOC_CSV) && exists("mapspam_pro_dir")) {
  cand <- file.path(mapspam_pro_dir, "fao_prices", sprintf("crop_vop_intld15-2021_allocation_%s.csv", RES_TAG))
  if (file.exists(cand)) ALLOC_CSV <- cand
}
spill <- out_intld[0]
if (nzchar(ALLOC_CSV) && file.exists(ALLOC_CSV) && nrow(out_intld)) {
  al <- fread(ALLOC_CSV)
  codes_all <- fread(file.path(repo_root, "metadata", "SpamCodes.csv"))[!is.na(Code) & Code != "", .(code = tolower(Code), crop = gsub(" ", "-", tolower(Fullname)))]
  al <- al[, .(code = unlist(strsplit(sub("^banpl$", "bana+plnt", group), "\\+"))), by = .(iso3, group, value_alloc)]
  al <- merge(al, codes_all, by = "code")
  allocated <- al[!is.na(value_alloc) & value_alloc > 0, .(iso3, crop)]
  out_intld[, nationally_allocated := paste(iso3, crop) %in% allocated[, paste(iso3, crop)]]
  spill <- out_intld[nationally_allocated == FALSE]
  out_intld <- out_intld[nationally_allocated == TRUE]
  .log("allocation audit: %s (%d allocated pairs)", ALLOC_CSV, nrow(allocated))
  if (nrow(spill)) { .log("info: %d out-of-band pairs have NO national constant-I$ allocation (border spill on this grid, #18) - reported, not gated: %s",
                          nrow(spill), paste(spill[, paste0(iso3, ":", crop)], collapse = ",")); print(show(spill), nrows = 25) }
} else if (nrow(out_intld)) .log("WARN: no allocation audit CSV (pass --allocation <0.4.0 crop_vop_intld15-2021_allocation_<tag>.csv>); border-spill pairs cannot be told apart")
out_crop <- per_crop[n >= MIN_N_CROP & (med > BAND_CROP | med < 1 / BAND_CROP)]
cat("\nper-crop medians (worst first):\n"); print(per_crop[1:min(15, .N)][, .(crop, n, med = signif(med, 3), lo = signif(lo, 3), hi = signif(hi, 3))], nrows = 15)
if (nrow(out_nominal)) {
  .log("FAIL: %d material pairs outside [1/%g, %g] with the NOMINAL side off (implied USD/t outside %gx the crop median):", nrow(out_nominal), BAND, BAND, PRICE_BAND_SIDE)
  print(show(out_nominal)[1:min(25, .N)], nrows = 25)
} else {
  .log("ok: no material pair outside [1/%g, %g] has its nominal side off", BAND, BAND)
}
if (nrow(out_intld)) {
  .log("%s: %d material pairs outside [1/%g, %g] with a SOUND nominal side -> intld-side defect (FAO value over a negligible SPAM footprint, or the #38 millet split); countries %s",
       if (FAIL_INTLD) "FAIL" else "residual", nrow(out_intld), BAND, BAND, paste(sort(unique(out_intld$iso3)), collapse = ","))
  print(show(out_intld)[1:min(25, .N)], nrows = 25)
}
if (nrow(out_crop)) { .log("FAIL: %d crops whose material median is outside [1/%g, %g]: %s", nrow(out_crop), BAND_CROP, BAND_CROP, paste(out_crop$crop, collapse = ",")) } else .log("ok: every per-crop median within [1/%g, %g]", BAND_CROP, BAND_CROP)
if (nrow(one_sided)) { .log("info: %d one-sided pairs (value on one basis only; small-millet expected, #38): crops %s", nrow(one_sided), paste(sort(unique(one_sided$crop)), collapse = ",")) }
pass <- nrow(out_nominal) == 0 && nrow(out_crop) == 0 && (!FAIL_INTLD || nrow(out_intld) == 0)
.log("GATE %s in %.1f min", if (pass) "PASS" else "FAIL", as.numeric(difftime(Sys.time(), t0, units = "mins")))
quit(status = if (pass) 0 else 1)
