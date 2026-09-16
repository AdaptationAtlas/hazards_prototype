#!/usr/bin/env Rscript
# R/checks/usd_total_vs_reference.R
# =================================
# Issue #9 STEP B gate. For the R/3 ENSEMBLEmean hazard_exposure parquet, the per-
# commodity total VOP is value(any) + value(none) (freq_any + freq_none = 1 per
# pixel). That total must agree with the standalone exposure reference produced by
# 0.4.4 from the SAME exposure rasters. Disagreement = wrong raster vintage, a
# stale §4.1 tif, or a broken none layer. Zonal differences (R/3 at 0.25 deg vs
# 0.4.4) keep it from being exact; tolerance is deliberately loose.
#
# Compares, at admin0, scenario = historic:
#   usd  : haz-freq-exp_vop_nominal-usd-2021_ENSEMBLEmean_int_adm_<sev>  vs
#          exposure_dir/vop_nominal-usd-2021_adm_sum_spam20_glw420.parquet
#   intld: haz-freq-exp_vop_intld15-2021_ENSEMBLEmean_int_adm_<sev>      vs
#          exposure_dir/exposure_adm_sum_spam20-20_glw420-20.parquet (unit intld15)
# PASS = median ratio in [0.90, 1.10] AND every MATERIAL crop ratio in [0.50, 2.00],
# where material means the reference value clears MIN_REF (default 1e5, env GATE_MIN_REF).
# A ratio test is meaningless when the denominator is a rounding error: AGO coconut has a
# sub-dollar national reference, so total/ref swings wildly on noise and failed the bound
# for no reason (2026-09-16). Immaterial pairs are still reported, and still FAIL if the
# product claims real value where the reference has none (total > GATE_MAX_ABS, default 1e6)
# - that is the dangerous direction and the one worth aborting a publish over.
#
# Usage (cglabs, repo root, ~1-2 min): Rscript R/checks/usd_total_vs_reference.R
#   [--timeframe jagermeyr] [--severity severe] [--iso3 AGO,KEN,NGA]
# arrow only (no duckdb: the two clash on CGlabs).

t0 <- Sys.time()
.ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) cat(sprintf("[%s] [gate-usd] %s\n", .ts(), sprintf(fmt, ...)))
args <- commandArgs(trailingOnly = TRUE)
opt <- function(x, d) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
TF  <- opt("--timeframe", "jagermeyr"); SEV <- opt("--severity", "severe"); ISO <- strsplit(opt("--iso3", "AGO,KEN,NGA"), ",")[[1]]
MIN_REF <- as.numeric(Sys.getenv("GATE_MIN_REF", "1e5"))   # below this a national crop total is noise
MAX_ABS <- as.numeric(Sys.getenv("GATE_MAX_ABS", "1e6"))   # product value allowed against a noise reference
setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
if (nzchar(Sys.getenv("ATLAS_SETUP_SKIP"))) { .log("ATLAS_SETUP_SKIP set"); stopifnot(exists("atlas_dirs")) } else { .log("sourcing %s", setup); suppressMessages(suppressWarnings(source(setup))) }
suppressPackageStartupMessages({ pacman::p_load(arrow, dplyr, data.table) })
ref_dir <- if (exists("exposure_dir")) exposure_dir else atlas_dirs$data_dir$exposure

haz_total <- function(pq) {
  arrow::open_dataset(pq) |>
    dplyr::filter(iso3 %in% ISO, is.na(admin1_name), scenario == "historic", hazard %in% c("any", "none")) |>
    dplyr::select(iso3, crop, hazard_vars, hazard, value) |> dplyr::collect() |> as.data.table()
}
ref_total <- function(pq, unit_keep) {
  d <- arrow::open_dataset(pq) |>
    dplyr::filter(iso3 %in% ISO, is.na(admin1_name), exposure == "vop") |>
    dplyr::select(iso3, crop, unit, tech, value) |> dplyr::collect() |> as.data.table()
  d <- d[unit %in% unit_keep & (tech == "all" | is.na(tech))]
  d[, .(ref = sum(value, na.rm = TRUE)), by = .(iso3, crop)]
}
overall <- TRUE
for (spec in list(
  list(lab = "usd",   dir = atlas_dirs$data_dir$hazard_risk_vop_usd, var = "vop_nominal-usd-2021", ref = file.path(ref_dir, "vop_nominal-usd-2021_adm_sum_spam20_glw420.parquet"), units = c("nominal-usd-2021", "usd")),
  list(lab = "intld", dir = atlas_dirs$data_dir$hazard_risk_vop,     var = "vop_intld15-2021",     ref = file.path(ref_dir, "exposure_adm_sum_spam20-20_glw420-20.parquet"),    units = c("intld15", "intld15-2021", "intld15-2020")))) {
  cat(sprintf("\n=== %s | %s | %s | %s ===\n", spec$lab, TF, SEV, paste(ISO, collapse = ",")))
  pq <- file.path(spec$dir, TF, sprintf("haz-freq-exp_%s_ENSEMBLEmean_int_adm_%s.parquet", spec$var, SEV))
  if (!file.exists(pq)) { .log("%s: MISSING %s", spec$lab, pq); overall <- FALSE; next }
  if (!file.exists(spec$ref)) { .log("%s: MISSING reference %s", spec$lab, spec$ref); overall <- FALSE; next }
  .log("%s: hazard parquet %s (mtime %s) | reference %s (mtime %s)", spec$lab, basename(pq), format(file.mtime(pq), "%Y-%m-%d %H:%M"), basename(spec$ref), format(file.mtime(spec$ref), "%Y-%m-%d %H:%M"))
  h <- haz_total(pq)
  if (!nrow(h)) { .log("%s: no historic adm0 any/none rows", spec$lab); overall <- FALSE; next }
  # one total per (iso3, crop): any+none is identical across hazard_vars by construction -> take the first combo
  ht <- h[, .(total = sum(value, na.rm = TRUE), n_haz = .N), by = .(iso3, crop, hazard_vars)][, .SD[1], by = .(iso3, crop)]
  ht[, has_none := iso3 %in% h[hazard == "none", iso3] & crop %in% h[hazard == "none", crop]]
  none_by_combo <- h[, .(n_none = sum(hazard == "none"), n_any = sum(hazard == "any")), by = hazard_vars]
  print(none_by_combo)
  r <- ref_total(spec$ref, spec$units)
  m <- merge(ht, r, by = c("iso3", "crop"), all = TRUE)
  m[, ratio := total / ref]
  cat(sprintf("  crops in hazard only: %s\n  crops in reference only: %s\n",
              paste(m[is.na(ref), unique(crop)], collapse = ",") , paste(m[is.na(total), unique(crop)], collapse = ",")))
  m[, material := !is.na(ref) & ref >= MIN_REF]
  mm <- m[material == TRUE & !is.na(ratio) & is.finite(ratio)]
  imm <- m[material == FALSE]
  # signif(), not round(): a sub-dollar reference printed as "0" is what made AGO coconut
  # look like a 0/0 degenerate rather than the tiny-denominator case it actually is.
  show <- function(d) d[, .(iso3, crop, total = signif(total, 4), ref = signif(ref, 4), ratio = signif(ratio, 4))]
  if (!nrow(mm)) { .log("%s: no material pairs (ref >= %.3g) — cannot gate", spec$lab, MIN_REF); overall <- FALSE; next }
  med <- median(mm$ratio); rng <- range(mm$ratio)
  .log("%s: %d material pairs (ref >= %.3g) | median ratio %.3f | range [%.4g, %.4g]", spec$lab, nrow(mm), MIN_REF, med, rng[1], rng[2])
  print(show(mm[order(ratio)])[c(1:min(5, .N), max(1, .N - 4):.N)])
  out_of_band <- mm[ratio < 0.5 | ratio > 2.0]
  if (nrow(out_of_band)) { .log("%s: %d material pairs outside [0.5, 2] —", spec$lab, nrow(out_of_band)); print(show(out_of_band[order(-abs(log(ratio)))])[1:min(15, .N)]) }
  if (nrow(imm)) {
    .log("%s: %d pairs below the materiality floor (reported, not ratio-gated)", spec$lab, nrow(imm))
    print(show(imm[order(-total)])[1:min(8, .N)])
  }
  invented <- imm[!is.na(total) & total > MAX_ABS]
  if (nrow(invented)) { .log("%s: %d immaterial pairs where the product claims > %.3g against a noise reference —", spec$lab, nrow(invented), MAX_ABS); print(show(invented[order(-total)])[1:min(10, .N)]) }
  pass <- med >= 0.90 && med <= 1.10 && nrow(out_of_band) == 0 && nrow(invented) == 0 && all(none_by_combo$n_none == none_by_combo$n_any)
  .log("%s: %s", spec$lab, if (pass) "PASS" else sprintf("FAIL (median %.3f, %d material pairs out of band, %d invented-value pairs, n(none)==n(any) %s)",
       med, nrow(out_of_band), nrow(invented), all(none_by_combo$n_none == none_by_combo$n_any)))
  overall <- overall && pass
}
.log("GATE %s in %.1f min", if (overall) "PASS" else "FAIL", as.numeric(difftime(Sys.time(), t0, units = "mins")))
quit(status = if (overall) 0 else 1)
