#!/usr/bin/env Rscript
# R/checks/r3_tier_drift_vs_live.R
# =================================
# Value-drift gate for the R/3 hazard_exposure tiers (publisher gate G6).
#
# The publisher's G1-G5 check size, none/any parity, scenarios, severity and
# column names. None of them looks at a value, so a product that has shifted
# (new exposure grid, wrong raster grep-matched, a stale tif that survived
# parking) would ship unobserved. This compares a freshly baked tier against the
# tier currently live and says, in populations, how much moved and where.
#
# What is compared (both files, identical filter):
#   scenario == "historic", admin0 rows (admin1_name NA), hazard in {any, none}.
#   T(iso3, crop, hazard_vars) = sum(any + none) = total exposed value of that
#   crop in that country, because freq_any + freq_none = 1 per pixel. T must be
#   the same for every hazard_vars of a (iso3, crop); the spread is reported.
#   T is then collapsed to (iso3, crop) and the two sides are full-joined.
#
# Populations (a single ratio bound cannot serve all of them - #9 lesson):
#   material    live T >= min_live and iso3 not in small_iso3   -> gated on tol_pair, median on tol_median
#   small       iso3 in small_iso3 (narrow / border-heavy)       -> own table, gated on tol_small only
#   immaterial  live T <  min_live                               -> reported, gated on flips only
#   unmatched   (iso3, crop) present on one side only            -> must be empty
#   flips       material on one side, ~0 on the other            -> must be empty (the dangerous direction)
#   livestock   crop matches cattle|sheep|goats|pigs|poultry     -> control: inputs are renames, ratio ~ 1 (tol_control)
#   totals      continental sum per crop                        -> gated on tol_total
#   parity      row count, distinct(hazard_vars), distinct(scenario) identical
#
# Why these defaults (2026-09-26, p.steward): the 0.4.2 crop nominal-USD raster
# moved from 0.05 deg (aligned in-flight by R/3) to native 0.25 deg. Production is
# mass-conserved either way; what changes is which country's price a border cell
# gets. That nets out continentally (tol_total 5 %), is small for most countries
# (tol_pair 25 %, median 3 %), and can be large for narrow countries (GMB, SWZ,
# RWA...), which is why they are reported in their own table rather than used to
# widen the main bound. A FAIL on the material population, on livestock, or a
# flip is a defect, not an expected shift.
#
# arrow + data.table only: duckdb and arrow cannot both be attached on CGlabs.
#
# Usage - standalone (node, ~1 min; also how intld / ha tiers are gated new vs parked):
#   Rscript R/checks/r3_tier_drift_vs_live.R --local <new.parquet> --live <old.parquet>
#     [--tol-pair 0.25] [--tol-median 0.03] [--tol-total 0.05] [--tol-small 0.5]
#     [--tol-control 0.02] [--min-live 1e6] [--small-iso3 GMB,SWZ,...]
#   exit 0 = PASS, 1 = FAIL, 2 = usage.
# Usage - from scripts/r3_publish_tiers.R: source() this file (defines tier_drift()
#   and print_drift(); the CLI block below only runs when this file is the script).

suppressPackageStartupMessages({ library(arrow); library(dplyr); library(data.table) })

DRIFT_SMALL_ISO3_DEFAULT <- c("GMB", "SWZ", "LSO", "BDI", "RWA", "DJI", "GNQ", "GNB",
                              "TGO", "BEN", "SLE", "CPV", "COM", "STP", "MUS", "SYC")
DRIFT_LIVESTOCK_RE <- "cattle|sheep|goats|pigs|poultry"
# `generic-crop` is the synthetic all-crop aggregate: it has no row in the exposure
# reference (usd_total_vs_reference exempts it as "no reference by design"), so with an
# --drift-exposure basis it falls back to live-vs-local and legitimately moves with the
# summed price corrections. Report it, never gate it. Env-overridable, mirrors GATE_NO_REF.
DRIFT_NO_REF <- strsplit(Sys.getenv("DRIFT_NO_REF", "generic-crop"), ",")[[1]]

.drift_ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.drift_log <- function(fmt, ...) cat(sprintf("[%s] [gate-drift] %s\n", .drift_ts(), sprintf(fmt, ...)))

# admin0 historic any+none totals per (iso3, crop, hazard_vars), plus parity facts.
.drift_read <- function(f) {
  ds <- arrow::open_dataset(f)
  d <- ds |>
    dplyr::filter(scenario == "historic", is.na(admin1_name), hazard %in% c("any", "none")) |>
    dplyr::select(iso3, crop, hazard_vars, hazard, value) |>
    dplyr::collect() |> as.data.table()
  if (!nrow(d)) stop("no admin0 historic any/none rows in ", f)
  t_hv <- d[, .(T = sum(value, na.rm = TRUE)), by = .(iso3, crop, hazard_vars)]
  # T is the same for every hazard_vars WHERE THE COMBINATION IS DEFINED for that
  # crop; some combinations carry no rows or NaN for some crops (the live product
  # has this too: first Block B run measured a "spread" of 1.00 on live), so take
  # the max over hazard_vars as the pair's total and report the spread among the
  # non-zero ones for information only.
  spread <- t_hv[T > 0, .(spread = (max(T) - min(T)) / max(T), n_hv = .N), by = .(iso3, crop)]
  t_ic <- t_hv[, .(T = max(T)), by = .(iso3, crop)]
  list(
    T = t_ic,
    max_spread = if (nrow(spread)) spread[, max(spread)] else 0,
    n_partial = if (nrow(spread)) spread[n_hv < length(unique(d$hazard_vars)), .N] else 0L,
    n_rows = nrow(ds),
    hazard_vars = sort(unique(d$hazard_vars)),
    scenarios = sort((ds |> dplyr::distinct(scenario) |> dplyr::collect())$scenario)
  )
}

# Read a 0.4.4 exposure basis as admin0 totals per (iso3, crop), for one `exposure` value.
.drift_read_exposure <- function(f, exposure_var) {
  stopifnot(file.exists(f))
  .want <- exposure_var
  ex <- arrow::open_dataset(f) |>
    dplyr::filter(is.na(admin1_name), exposure == .want) |>
    dplyr::select(iso3, crop, tech, value) |> dplyr::collect() |> as.data.table()
  if (!nrow(ex)) {
    have <- arrow::open_dataset(f) |> dplyr::distinct(exposure, unit) |> dplyr::collect() |> as.data.table()
    stop(sprintf("exposure_var '%s' matches no rows in %s. Pairs present: %s",
                 exposure_var, basename(f), paste(sprintf("%s/%s", have$exposure, have$unit), collapse = ", ")))
  }
  ex[(tech == "all" | is.na(tech)) & is.finite(value), .(E = sum(value)), by = .(iso3, crop)]
}

# G6b — the FIRST-PUBLISH gate (added 2026-10-07 with the #41 physical tiers and the intld / ha
# routes). G6 judges a product against the live object at its key, so a key that has never been
# published cannot be gated by it at all: `tier_drift` is simply skipped and the first version of a
# brand-new tier would reach the Atlas ungated on value. That is precisely the publish this bake is
# making four of.
#
# The invariant that does not need a live object: R/3 multiplies hazard frequency by exposure, and
# freq_any + freq_none = 1 per pixel, so a tier's admin0 historic any+none total per (iso3, crop) IS
# the exposure total it was built from. Judge the product against that input, on the same grid -
# the G6 lesson of 2026-09-30, and a reference independent of the table it judges.
#
# Populations are split as elsewhere, because one ratio bound cannot serve all three: `material`
# (exposure clears min_exposure; the bound applies), `immaterial` (noise-level exposure, where a
# ratio swings wildly - reported, and only an invented value is caught), and `unmatched` (in one
# table and not the other - always reported, and a product pair with no exposure row at all is a
# FAIL, since there was nothing to multiply).
tier_vs_exposure <- function(local_f, exposure_f, exposure_var,
                             tol_pair = 0.02, min_exposure = 1e3, max_invented = 1e6,
                             no_ref = DRIFT_NO_REF) {
  stopifnot(file.exists(local_f))
  b <- .drift_read(local_f)
  e <- .drift_read_exposure(exposure_f, exposure_var)
  j <- merge(b$T, e, by = c("iso3", "crop"), all = TRUE)
  j[, noref := crop %in% no_ref]
  j[, ratio := T / E]
  unmatched_no_expo <- j[!noref & (is.na(E) | E <= 0) & !is.na(T) & T > max_invented]
  unmatched_no_prod <- j[!noref & (is.na(T)) & !is.na(E) & E > min_exposure]
  mat <- j[!noref & !is.na(T) & !is.na(E) & E > min_exposure]
  imm <- j[!noref & !is.na(T) & !is.na(E) & E <= min_exposure]
  worst <- if (nrow(mat)) mat[which.max(abs(ratio - 1))] else mat
  gates <- data.table::data.table(
    gate = c("material pairs within tol", "no product pair invents value without exposure",
             "no material exposure pair missing from the product", "product has the full scenario set"),
    value = c(if (nrow(mat)) sprintf("max |ratio-1| = %.4f over %d pairs", max(abs(mat$ratio - 1)), nrow(mat)) else "no material pairs",
              sprintf("%d pair(s)", nrow(unmatched_no_expo)),
              sprintf("%d pair(s)", nrow(unmatched_no_prod)),
              paste(b$scenarios, collapse = ",")),
    pass = c(nrow(mat) > 0 && max(abs(mat$ratio - 1)) <= tol_pair,
             nrow(unmatched_no_expo) == 0L,
             nrow(unmatched_no_prod) == 0L,
             setequal(b$scenarios, c("historic", "ssp126", "ssp245", "ssp370", "ssp585"))))
  list(pass = all(gates$pass), gates = gates, material = mat[order(-abs(ratio - 1))],
       immaterial = imm, invented = unmatched_no_expo, missing_product = unmatched_no_prod,
       noref = j[noref == TRUE], worst = worst,
       basis = sprintf("%s, exposure == '%s'", basename(exposure_f), exposure_var),
       params = list(tol_pair = tol_pair, min_exposure = min_exposure, max_invented = max_invented))
}

print_tier_vs_exposure <- function(res, label = "", n_worst = 12, log = .drift_log) {
  log("G6b %s: product vs the exposure input it was built from — basis %s", label, res$basis)
  cat("\n  gates:\n"); print(res$gates, nrows = 20)
  cols <- c("iso3", "crop", "E", "T", "ratio")
  if (nrow(res$material))        { cat("\n  material pairs, worst", n_worst, "of", nrow(res$material), "(ratio = product / exposure, expect 1):\n"); print(head(res$material[, ..cols], n_worst), nrows = n_worst) }
  if (nrow(res$invented))        { cat("\n  PRODUCT VALUE WITH NO EXPOSURE ROW:\n"); print(res$invented[, ..cols], nrows = 50) }
  if (nrow(res$missing_product)) { cat("\n  EXPOSURE PRESENT, NO PRODUCT ROW:\n"); print(res$missing_product[, ..cols], nrows = 50) }
  if (nrow(res$noref))           { cat("\n  no-reference crops (reported, not gated):\n"); print(res$noref[, ..cols], nrows = 20) }
  cat(sprintf("\n  immaterial pairs (exposure <= %s): %d, reported only\n", format(res$params$min_exposure, big.mark = ","), nrow(res$immaterial)))
  invisible(res$pass)
}

# expected: optional data.table (iso3, crop, ratio_expected) - the ratio by which the
# EXPOSURE input of that pair changed between what the live product was built from
# and what the new product was built from (new / old, admin0, tech = all). R/3 is a
# linear multiply of hazard frequency by exposure, so the product must move by the
# same factor: every bound below is then applied to ratio / ratio_expected, and the
# continental total to the exposure-weighted expected total. Pairs absent from the
# table expect 1. Produced on the node by R/checks/r3_expected_drift_from_exposure.R.
# exposure_new: optional path to the exposure table THIS bake multiplied by (0.4.4 §3.2
# twin on the SAME zonal grid as the product, e.g. vop_nominal-usd-2021_adm_sum_..._res-25).
# Because freq_any + freq_none = 1 per pixel, the product's admin0 any+none total IS the
# exposure total: T_local / exposure_new must be ~1 for every pair, whatever the exposure
# did against the live product. This is the clean form of the "expected move" idea - the
# CSV route (`expected`, kept for the CLI) mixes in the zonal-grid difference between a
# 0.05 deg old table and the 0.25 deg live product, which for small countries dwarfs the
# price change (2026-09-30: RWA/GAB coffee "expected" 87x/8x). When exposure_new is given
# the bounds below apply to T_local / exposure_new; the raw live-vs-local drift is still
# printed for information.
# exposure_var: which `exposure` rows of exposure_new are this tier's basis. The single-unit vop
#   twins (0.4.4 §3.2 / §3.3) hold only "vop", so the default was fine while vop_nominal-usd21 was
#   the only published tier. The physical tiers (#41, 2026-10-07) take their basis from the
#   multi-unit reference, where they are "prod" (prod_t), "harv-area" (harv-area_ha) and "number"
#   (head_n) - filtering those on "vop" would match no rows and G6 would quietly degrade to the
#   live-product basis, which is the one thing a new tier does not have. Matching nothing is
#   therefore an error here, and the error names the (exposure, unit) pairs the file does hold.
# flip_allow: "ISO3:crop" pairs whose live -> ~0 flip is explained and accepted (reported, not FAIL).
tier_drift <- function(local_f, live_f,
                       tol_pair = 0.25, tol_median = 0.03, tol_total = 0.05,
                       tol_small = 0.5, tol_control = 0.02,
                       min_live = 1e6, small_iso3 = DRIFT_SMALL_ISO3_DEFAULT,
                       flip_frac = 0.01, expected = NULL, exposure_new = NULL, flip_allow = character(0),
                       exposure_var = "vop") {
  stopifnot(file.exists(local_f), file.exists(live_f))
  a <- .drift_read(live_f); b <- .drift_read(local_f)
  j <- merge(a$T, b$T, by = c("iso3", "crop"), all = TRUE, suffixes = c("_live", "_local"))
  j[, ratio_raw := T_local / T_live]
  j[, ratio_expected := 1]
  j[, livestock := grepl(DRIFT_LIVESTOCK_RE, crop)]
  basis <- "live product (every pair expects 1)"
  if (!is.null(exposure_new)) {
    # A basis that matches nothing would quietly fall back to the live-product basis, so
    # .drift_read_exposure() errors instead, naming the (exposure, unit) pairs the file does hold.
    ex <- .drift_read_exposure(exposure_new, exposure_var)
    data.table::setnames(ex, "E", "E_new")
    j <- merge(j, ex, by = c("iso3", "crop"), all.x = TRUE)
    # expected move = what the exposure input actually is now, relative to the live product
    j[!is.na(E_new) & T_live > 0, ratio_expected := E_new / T_live]
    basis <- sprintf("exposure input %s, exposure == '%s' (bounds on T_local / exposure_new)", basename(exposure_new), exposure_var)
  } else if (!is.null(expected)) {
    e <- as.data.table(expected)[is.finite(ratio_expected) & ratio_expected > 0, .(iso3, crop, ratio_expected)]
    j <- merge(j[, !"ratio_expected"], e, by = c("iso3", "crop"), all.x = TRUE)
    j[is.na(ratio_expected), ratio_expected := 1]
    basis <- "expected-move csv (bounds on ratio_raw / ratio_expected)"
  }
  # Livestock rasters never change in a crop-price fix and are not in 0.4.2; the control is
  # always judged raw (live == local), never net of any expected move.
  j[livestock == TRUE, ratio_expected := 1]
  j[, ratio := ratio_raw / ratio_expected]
  j[, small := iso3 %in% small_iso3]
  j[, noref := crop %in% DRIFT_NO_REF]

  unmatched <- j[is.na(T_live) | is.na(T_local)]
  m <- j[!is.na(T_live) & !is.na(T_local)]
  # A flip: real value on one side, essentially nothing on the other.
  flips_all <- m[(T_live >= min_live & T_local <= flip_frac * T_live) |
                 (T_local >= min_live & T_live <= flip_frac * T_local)]
  flips_all[, allowed := paste0(iso3, ":", crop) %in% flip_allow]
  flips <- flips_all[allowed == FALSE]
  # An allowed flip is explained elsewhere; keep it out of every gated population so it
  # does not fail the continental total or the pair band on top of being reported.
  m <- m[!paste0(iso3, ":", crop) %in% flips_all[allowed == TRUE, paste0(iso3, ":", crop)]]
  material   <- m[T_live >= min_live & !small & !livestock & !noref]
  smallpop   <- m[T_live >= min_live & small & !livestock & !noref]
  norefpop   <- m[T_live >= min_live & noref]
  immaterial <- m[T_live <  min_live & !noref]
  control    <- m[livestock & T_live >= min_live]
  totals <- m[livestock == FALSE & noref == FALSE, .(T_live = sum(T_live), T_local = sum(T_local), T_expected = sum(T_live * ratio_expected)), by = crop][T_live >= min_live]
  totals[, ratio_raw := T_local / T_live]
  totals[, ratio := T_local / T_expected]   # net of the expected input move

  g <- function(gate, population, ok, detail) data.table(gate = gate, population = population,
                                                         result = ifelse(ok, "ok", "FAIL"), detail = detail)
  med <- if (nrow(material)) median(material$ratio) else NA_real_
  gates <- rbindlist(list(
    g("parity", "rows",        a$n_rows == b$n_rows, sprintf("live %d vs local %d", a$n_rows, b$n_rows)),
    g("parity", "hazard_vars", setequal(a$hazard_vars, b$hazard_vars),
      sprintf("live %d vs local %d distinct", length(a$hazard_vars), length(b$hazard_vars))),
    g("parity", "scenarios",   setequal(a$scenarios, b$scenarios), paste(b$scenarios, collapse = ",")),
    g("spread", "T across defined hazard_vars (informational)", TRUE,
      sprintf("max relative spread among non-zero combos live %.2e local %.2e; pairs missing a combo live %d local %d",
              a$max_spread, b$max_spread, a$n_partial, b$n_partial)),
    g("unmatched", "(iso3, crop) one side only", nrow(unmatched) == 0, sprintf("%d pairs", nrow(unmatched))),
    g("flips", sprintf("material one side, <= %.0f%% other", 100 * flip_frac), nrow(flips) == 0,
      sprintf("%d pairs%s", nrow(flips), if (any(flips_all$allowed)) sprintf(" (+%d allowed: %s)", sum(flips_all$allowed), paste(flips_all[allowed == TRUE, paste0(iso3, ":", crop)], collapse = ",")) else "")),
    g("total", sprintf("continental per crop (live >= %s)", format(min_live, big.mark = ",")),
      nrow(totals) > 0 && all(abs(totals$ratio - 1) <= tol_total),
      sprintf("%d crops; ratio range [%s, %s]; tol %.0f%%", nrow(totals), signif(min(totals$ratio), 4), signif(max(totals$ratio), 4), 100 * tol_total)),
    g("material", "pairs within tol_pair", nrow(material) > 0 && all(abs(material$ratio - 1) <= tol_pair),
      sprintf("%d pairs; %d outside +/-%.0f%%; range [%s, %s]", nrow(material), sum(abs(material$ratio - 1) > tol_pair), 100 * tol_pair,
              signif(min(material$ratio), 4), signif(max(material$ratio), 4))),
    g("material", "median within tol_median", !is.na(med) && abs(med - 1) <= tol_median,
      sprintf("median %s; tol %.0f%%", signif(med, 4), 100 * tol_median)),
    g("small-country", "pairs within tol_small", nrow(smallpop) == 0 || all(abs(smallpop$ratio - 1) <= tol_small),
      sprintf("%d pairs (%s); %d outside +/-%.0f%%", nrow(smallpop), paste(sort(unique(smallpop$iso3)), collapse = ","),
              if (nrow(smallpop)) sum(abs(smallpop$ratio - 1) > tol_small) else 0L, 100 * tol_small)),
    g("livestock", "control ~ 1 (inputs are renames)", nrow(control) == 0 || all(abs(control$ratio - 1) <= tol_control),
      sprintf("%d pairs; range [%s, %s]; tol %.0f%%", nrow(control),
              if (nrow(control)) signif(min(control$ratio), 4) else NA, if (nrow(control)) signif(max(control$ratio), 4) else NA, 100 * tol_control)),
    g("no-reference", sprintf("%s: reported, not gated", paste(DRIFT_NO_REF, collapse = ",")), TRUE,
      sprintf("%d pairs (synthetic all-crop aggregate, no exposure row); range [%s, %s]", nrow(norefpop),
              if (nrow(norefpop)) signif(min(norefpop$ratio), 4) else NA, if (nrow(norefpop)) signif(max(norefpop$ratio), 4) else NA))
  ))
  n_exp <- m[ratio_expected != 1, .N]
  gates <- rbind(gates, g("basis", "what the bounds are judged against", TRUE,
                          sprintf("%s; %d pairs carry an expected move != 1", basis, n_exp)))
  list(pass = all(gates$result == "ok"), gates = gates,
       material = material[order(abs(ratio - 1), decreasing = TRUE)],
       small = smallpop[order(abs(ratio - 1), decreasing = TRUE)],
       immaterial = immaterial, unmatched = unmatched, flips = flips, flips_allowed = flips_all[allowed == TRUE],
       noref = norefpop[order(abs(ratio - 1), decreasing = TRUE)],
       livestock = control[order(abs(ratio - 1), decreasing = TRUE)], totals = totals[order(abs(ratio - 1), decreasing = TRUE)],
       params = list(tol_pair = tol_pair, tol_median = tol_median, tol_total = tol_total, tol_small = tol_small,
                     tol_control = tol_control, min_live = min_live, small_iso3 = small_iso3))
}

# Prints with signif(), never round(): round() showed 0.4 as 0 once and sent a
# diagnosis the wrong way for a round trip.
print_drift <- function(res, label = "", n_worst = 10, log = .drift_log) {
  fmt <- function(d) { d <- copy(d); for (c in c("T_live", "T_local", "ratio", "ratio_raw", "ratio_expected")) if (c %in% names(d)) d[[c]] <- signif(d[[c]], 4); d }
  log("G6 value drift %s: %s", label, if (res$pass) "PASS" else "FAIL")
  print(res$gates, nrows = 50)
  if (nrow(res$totals))   { cat("\n  continental totals per crop (worst first):\n"); print(head(fmt(res$totals), n_worst), nrows = n_worst) }
  cols <- c("iso3", "crop", "T_live", "T_local", "ratio_raw", "ratio_expected", "ratio")
  if (nrow(res$material)) { cat("\n  material pairs, worst", n_worst, "of", nrow(res$material), "(ratio = raw / expected):\n"); print(head(fmt(res$material[, ..cols]), n_worst), nrows = n_worst) }
  if (nrow(res$small))    { cat("\n  small-country pairs (informational population), worst", n_worst, ":\n"); print(head(fmt(res$small[, ..cols]), n_worst), nrows = n_worst) }
  if (nrow(res$livestock)){ cat("\n  livestock control, worst", n_worst, ":\n"); print(head(fmt(res$livestock[, ..cols]), n_worst), nrows = n_worst) }
  if (!is.null(res$noref) && nrow(res$noref)) { cat("\n  no-reference (generic-crop; reported, not gated), worst", n_worst, ":\n"); print(head(fmt(res$noref[, ..cols]), n_worst), nrows = n_worst) }
  if (nrow(res$flips))    { cat("\n  FLIPS:\n"); print(fmt(res$flips[, ..cols]), nrows = 50) }
  if (nrow(res$flips_allowed)) { cat("\n  flips ALLOWED by name (explained elsewhere):\n"); print(fmt(res$flips_allowed[, ..cols]), nrows = 50) }
  if (nrow(res$unmatched)){ cat("\n  UNMATCHED:\n"); print(fmt(res$unmatched[, .(iso3, crop, T_live, T_local)]), nrows = 50) }
  cat(sprintf("\n  immaterial pairs (live < %s): %d, not gated except for flips\n", format(res$params$min_live, big.mark = ","), nrow(res$immaterial)))
  invisible(res$pass)
}

# ---------------------------------------------------------------- CLI --------
if (sys.nframe() == 0L || nzchar(Sys.getenv("DRIFT_CLI"))) {
  args <- commandArgs(trailingOnly = TRUE)
  opt <- function(x, d) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
  local_f <- opt("--local", ""); live_f <- opt("--live", ""); exp_f <- opt("--expected", ""); expo_f <- opt("--exposure", ""); allow_f <- opt("--allow-flips", "")
  expo_var <- opt("--exposure-var", "vop")
  if (!nzchar(local_f) || !nzchar(live_f)) { cat("usage: --local <new.parquet> --live <old.parquet> [--exposure <0.4.4 twin this bake used>] [--exposure-var vop|prod|harv-area|number] [--expected <iso3,crop,ratio_expected csv>] [--allow-flips ISO3:crop,...] [--tol-pair --tol-median --tol-total --tol-small --tol-control --min-live --small-iso3]\n"); quit(status = 2) }
  t0 <- Sys.time()
  .drift_log("local = %s", local_f); .drift_log("live  = %s", live_f)
  expected <- if (nzchar(exp_f)) { .drift_log("expected input moves from %s", exp_f); data.table::fread(exp_f) } else NULL
  exposure_new <- if (nzchar(expo_f)) { .drift_log("exposure input = %s", expo_f); expo_f } else NULL
  flip_allow <- if (nzchar(allow_f)) strsplit(allow_f, ",")[[1]] else character(0)
  res <- tier_drift(local_f, live_f,
                    tol_pair = as.numeric(opt("--tol-pair", "0.25")), tol_median = as.numeric(opt("--tol-median", "0.03")),
                    tol_total = as.numeric(opt("--tol-total", "0.05")), tol_small = as.numeric(opt("--tol-small", "0.5")),
                    tol_control = as.numeric(opt("--tol-control", "0.02")), min_live = as.numeric(opt("--min-live", "1e6")),
                    small_iso3 = strsplit(opt("--small-iso3", paste(DRIFT_SMALL_ISO3_DEFAULT, collapse = ",")), ",")[[1]],
                    expected = expected, exposure_new = exposure_new, flip_allow = flip_allow,
                    exposure_var = expo_var)
  print_drift(res, label = basename(local_f))
  .drift_log("done in %.1f min", as.numeric(difftime(Sys.time(), t0, units = "mins")))
  quit(status = if (res$pass) 0 else 1)
}
