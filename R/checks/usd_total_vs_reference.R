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
#          exposure_dir/exposure_adm_sum_spam20-20_glw420-20.parquet (unit intld15-2021,
#          or the pre-#30-fix `intld15` - the gate reports which it matched)
# PASS = median ratio in [0.90, 1.10] AND every MATERIAL crop ratio in [0.50, 2.00],
# where material means the reference value clears MIN_REF (default 1e5, env GATE_MIN_REF).
# A ratio test is meaningless when the denominator is a rounding error: AGO coconut has a
# sub-dollar national reference, so total/ref swings wildly on noise and failed the bound
# for no reason (2026-09-16). Immaterial pairs are still reported, and still FAIL if the
# product claims real value where the reference has none (total > GATE_MAX_ABS, default 1e6)
# - that is the dangerous direction and the one worth aborting a publish over.
#
# Usage (cglabs, repo root, ~1-2 min): Rscript R/checks/usd_total_vs_reference.R
#   [--timeframe jagermeyr] [--severity severe] [--iso3 AGO,KEN,NGA] [--res 0.25|0.05]
#   [--basis admin2|admin0]   -- which zonal basis to gate on; see BASIS below. admin2 is the
#                                like-for-like one and the default since 2026-10-07.
# arrow only (no duckdb: the two clash on CGlabs).

t0 <- Sys.time()
.ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) cat(sprintf("[%s] [gate-usd] %s\n", .ts(), sprintf(fmt, ...)))
args <- commandArgs(trailingOnly = TRUE)
opt <- function(x, d) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
TF  <- opt("--timeframe", "jagermeyr"); SEV <- opt("--severity", "severe"); ISO <- strsplit(opt("--iso3", "AGO,KEN,NGA"), ",")[[1]]
# `--iso3 all` gates every country. The three-country default is a smoke sample and
# is BLIND to a country-specific input defect: 2026-09-26 it passed while wheat in
# ZWE/ZMB and oilpalm across Central Africa had moved 5-112x (G6 caught it). admin0
# rows only, so the full set costs seconds more.
ISO_ALL <- identical(tolower(ISO), "all")
# Reference resolution (p.steward 2026-09-23): 0.4.4 writes every table once per zonal grid,
# suffixed res-05 / res-25. The hazard product is on the 0.25 deg NEX-GDDP grid, so the
# like-for-like comparison is res-25; --res 0.05 compares against the Atlas exposure grid instead.
RES <- opt("--res", "0.25"); RES_TAG <- sprintf("res-%02d", round(as.numeric(RES) * 100))
# Zonal basis (2026-10-07, handover §2 A4). Until now the gate took `is.na(admin1_name)` on both
# sides, which LOOKS like-for-like and is not: the product's adm0 row is formed from the lower
# levels, while 0.4.4 zones admin0 directly off its own rasterisation. At 0.25 deg with
# touches = TRUE a cell on a border can fall in one country's admin0 zone and in a neighbouring
# country's admin2 zone (#18), so the two disagreed over 31 border cells and the gate carried a
# residual it could not explain - a gate that fails a correct run.
#   admin2 (default) compares the admin2 rows of BOTH tables, summed per (iso3, crop). Both were
#     rasterised from the same Geographies onto the same base_rast, so the border assignment is
#     identical on both sides and the residual is not there to explain away.
#   admin0 keeps the old behaviour for comparison.
# Either way the gate now MEASURES the gap between the two bases and prints it, instead of leaving
# it as an unexplained ratio.
BASIS <- opt("--basis", "admin2")
if (!BASIS %in% c("admin0", "admin2")) stop("--basis must be admin0 or admin2, got '", BASIS, "'")
MIN_REF <- as.numeric(Sys.getenv("GATE_MIN_REF", "1e5"))   # below this a national crop total is noise
MAX_ABS <- as.numeric(Sys.getenv("GATE_MAX_ABS", "1e6"))   # product value allowed against a noise reference
# Rows in the product that are not SPAM commodities and so can never have a reference row.
# `generic-crop` is the synthetic all-crop aggregate (R/3 risk_x_exposure multiplies by
# sum(crop_exposure)); its total is the sum over crops by construction, so comparing it to a
# single reference commodity is a category error, not a finding.
NO_REF <- strsplit(Sys.getenv("GATE_NO_REF", "generic-crop"), ",")[[1]]
setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
if (nzchar(Sys.getenv("ATLAS_SETUP_SKIP"))) { .log("ATLAS_SETUP_SKIP set"); stopifnot(exists("atlas_dirs")) } else { .log("sourcing %s", setup); suppressMessages(suppressWarnings(source(setup))) }
suppressPackageStartupMessages({ pacman::p_load(arrow, dplyr, data.table) })
ref_dir <- if (exists("exposure_dir")) exposure_dir else atlas_dirs$data_dir$exposure

iso_filter <- function(d) if (ISO_ALL) d else dplyr::filter(d, iso3 %in% ISO)
haz_total <- function(pq, basis = BASIS) {
  d <- arrow::open_dataset(pq) |> iso_filter() |>
    dplyr::filter(scenario == "historic", hazard %in% c("any", "none"))
  d <- if (identical(basis, "admin2")) dplyr::filter(d, !is.na(admin2_name)) else dplyr::filter(d, is.na(admin1_name))
  d |> dplyr::select(iso3, crop, hazard_vars, hazard, value) |> dplyr::collect() |> as.data.table()
}
# `unit_keep` is ordered PREFERRED VINTAGE FIRST. Accepting several vintages keeps
# the gate runnable across a migration, but silently accepting one is how #30 went
# unnoticed: the gate and the product were comparing unlike vintages and nothing
# said so. So say which vintage was actually matched, and warn when it is not the
# decided one (p.steward 2026-09-18: the vintage stays in the name).
ref_total <- function(pq, unit_keep, basis = BASIS) {
  d0 <- arrow::open_dataset(pq) |> iso_filter() |> dplyr::filter(exposure == "vop")
  d0 <- if (identical(basis, "admin2")) dplyr::filter(d0, !is.na(admin2_name)) else dplyr::filter(d0, is.na(admin1_name))
  d <- d0 |> dplyr::select(iso3, crop, unit, tech, value) |> dplyr::collect() |> as.data.table()
  present <- d[, sort(unique(unit))]
  matched <- intersect(unit_keep, present)
  if (!length(matched)) {
    .log("  reference units present = [%s]; NONE of the accepted units [%s] is there",
         paste(present, collapse = ","), paste(unit_keep, collapse = ","))
  } else {
    .log("  reference unit matched = %s (accepted %s | present %s)",
         paste(matched, collapse = ","), paste(unit_keep, collapse = ","), paste(present, collapse = ","))
    if (!identical(matched, unit_keep[1])) {
      .log("  WARNING: matched on `%s`, not the decided vintage `%s` - gate and product may be on unlike vintages (#30)",
           paste(matched, collapse = ","), unit_keep[1])
    }
  }
  d <- d[unit %in% unit_keep & (tech == "all" | is.na(tech))]
  d[, .(ref = sum(value, na.rm = TRUE)), by = .(iso3, crop)]
}
overall <- TRUE
for (spec in list(
  list(lab = "usd",   dir = atlas_dirs$data_dir$hazard_risk_vop_usd, var = "vop_nominal-usd-2021", ref = file.path(ref_dir, sprintf("vop_nominal-usd-2021_adm_sum_spam20_glw420_%s.parquet", RES_TAG)), units = c("nominal-usd-2021", "usd")),
  list(lab = "intld", dir = atlas_dirs$data_dir$hazard_risk_vop,     var = "vop_intld15-2021",     ref = file.path(ref_dir, sprintf("exposure_adm_sum_spam20-20_glw420-20_%s.parquet", RES_TAG)),    units = c("intld15-2021", "intld15", "intld15-2020")))) {
  cat(sprintf("\n=== %s | %s | %s | %s | reference %s | basis %s ===\n", spec$lab, TF, SEV, paste(ISO, collapse = ","), RES_TAG, BASIS))
  pq <- file.path(spec$dir, TF, sprintf("haz-freq-exp_%s_ENSEMBLEmean_int_adm_%s.parquet", spec$var, SEV))
  if (!file.exists(pq)) { .log("%s: MISSING %s", spec$lab, pq); overall <- FALSE; next }
  if (!file.exists(spec$ref)) { .log("%s: MISSING reference %s", spec$lab, spec$ref); overall <- FALSE; next }
  .log("%s: hazard parquet %s (mtime %s) | reference %s (mtime %s)", spec$lab, basename(pq), format(file.mtime(pq), "%Y-%m-%d %H:%M"), basename(spec$ref), format(file.mtime(spec$ref), "%Y-%m-%d %H:%M"))
  h <- haz_total(pq)
  if (!nrow(h)) { .log("%s: no historic %s any/none rows", spec$lab, BASIS); overall <- FALSE; next }
  # one total per (iso3, crop): any+none is identical across hazard_vars by construction, so sum
  # over the basis's units within a combo and then take the first combo.
  ht <- h[, .(total = sum(value, na.rm = TRUE), n_haz = .N), by = .(iso3, crop, hazard_vars)][, .SD[1], by = .(iso3, crop)]
  ht[, has_none := iso3 %in% h[hazard == "none", iso3] & crop %in% h[hazard == "none", crop]]
  none_by_combo <- h[, .(n_none = sum(hazard == "none"), n_any = sum(hazard == "any")), by = hazard_vars]
  print(none_by_combo)
  r <- ref_total(spec$ref, spec$units)
  m <- merge(ht, r, by = c("iso3", "crop"), all = TRUE)
  m[, ratio := total / ref]

  # Measure the two zonal bases against each other, on whichever one is not being gated. This is
  # #18's 31 border cells as a number rather than a story, and it is the quantity that used to leak
  # into the gated ratio. Informational: both bases are legitimate views of the same product.
  other <- if (identical(BASIS, "admin2")) "admin0" else "admin2"
  xb <- tryCatch({
    h2 <- haz_total(pq, other)
    p2 <- h2[, .(total = sum(value, na.rm = TRUE)), by = .(iso3, crop, hazard_vars)][, .SD[1], by = .(iso3, crop)]
    r2 <- ref_total(spec$ref, spec$units, other)
    list(prod = merge(ht[, .(iso3, crop, t_gated = total)], p2[, .(iso3, crop, t_other = total)], by = c("iso3", "crop")),
         ref  = merge(r[, .(iso3, crop, r_gated = ref)],    r2[, .(iso3, crop, r_other = ref)],   by = c("iso3", "crop")))
  }, error = function(e) { .log("%s: cross-basis measurement unavailable (%s)", spec$lab, conditionMessage(e)); NULL })
  if (!is.null(xb)) {
    xb$prod[, d := t_other / t_gated]; xb$ref[, d := r_other / r_gated]
    .log("%s: zonal-basis check — gated on %s, compared with %s. product %s totals differ by median %.5f (max |1-d| %.4f over %d pairs); reference by median %.5f (max %.4f)",
         spec$lab, BASIS, other, other,
         median(xb$prod[is.finite(d), d]), max(abs(1 - xb$prod[is.finite(d), d])), nrow(xb$prod),
         median(xb$ref[is.finite(d), d]),  max(abs(1 - xb$ref[is.finite(d), d])))
    .wp <- xb$prod[is.finite(d)][order(-abs(1 - d))][1:min(5, .N)]
    if (nrow(.wp)) { cat(sprintf("  worst %s-vs-%s product pairs (d = %s / %s):\n", BASIS, other, other, BASIS)); print(.wp[, .(iso3, crop, t_gated = signif(t_gated, 4), t_other = signif(t_other, 4), d = signif(d, 6))]) }
  }
  cat(sprintf("  crops in hazard only: %s\n  crops in reference only: %s\n",
              paste(m[is.na(ref), unique(crop)], collapse = ",") , paste(m[is.na(total), unique(crop)], collapse = ",")))
  # Three populations, because one bound cannot serve all three:
  #   unmatched  - no reference row at all (ref NA). Cannot be compared. Reported.
  #   immaterial - reference present but negligible. Ratio is noise; the invented-value
  #                check still applies, since claiming value against ~nothing is suspicious.
  #   material   - reference is real. Ratio bound applies.
  m[, unmatched := is.na(ref)]
  m[, material := !unmatched & ref >= MIN_REF]
  mm  <- m[material == TRUE & !is.na(ratio) & is.finite(ratio)]
  imm <- m[unmatched == FALSE & material == FALSE]
  unm <- m[unmatched == TRUE]
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
  if (nrow(unm)) {
    expected <- unm[crop %in% NO_REF]; unexpected <- unm[!crop %in% NO_REF]
    if (nrow(expected)) {
      .log("%s: %d pair(s) with no reference by design (%s) — reported, not gated", spec$lab, nrow(expected), paste(NO_REF, collapse = ","))
      print(show(expected[order(-total)]))
      # Eyeball check only: the aggregate should be the same order as the sum over real crops.
      # Not gated, because the reference crop set and the raster crop set are known to differ.
      for (cr in NO_REF) for (ii in unique(expected[crop == cr, iso3]))
        .log("   %s %s total %.4g vs sum of material crop totals %.4g (informational)", ii, cr,
             expected[crop == cr & iso3 == ii, total][1], sum(mm[iso3 == ii, total], na.rm = TRUE))
    }
    if (nrow(unexpected)) {
      .log("%s: %d pair(s) in the product with NO reference row — coverage mismatch, reported, not gated", spec$lab, nrow(unexpected))
      print(show(unexpected[order(-total)])[1:min(10, .N)])
    }
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
