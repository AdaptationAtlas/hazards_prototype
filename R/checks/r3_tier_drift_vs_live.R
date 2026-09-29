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

# expected: optional data.table (iso3, crop, ratio_expected) - the ratio by which the
# EXPOSURE input of that pair changed between what the live product was built from
# and what the new product was built from (new / old, admin0, tech = all). R/3 is a
# linear multiply of hazard frequency by exposure, so the product must move by the
# same factor: every bound below is then applied to ratio / ratio_expected, and the
# continental total to the exposure-weighted expected total. Pairs absent from the
# table expect 1. Produced on the node by R/checks/r3_expected_drift_from_exposure.R.
tier_drift <- function(local_f, live_f,
                       tol_pair = 0.25, tol_median = 0.03, tol_total = 0.05,
                       tol_small = 0.5, tol_control = 0.02,
                       min_live = 1e6, small_iso3 = DRIFT_SMALL_ISO3_DEFAULT,
                       flip_frac = 0.01, expected = NULL) {
  stopifnot(file.exists(local_f), file.exists(live_f))
  a <- .drift_read(live_f); b <- .drift_read(local_f)
  j <- merge(a$T, b$T, by = c("iso3", "crop"), all = TRUE, suffixes = c("_live", "_local"))
  j[, ratio_raw := T_local / T_live]
  j[, ratio_expected := 1]
  if (!is.null(expected)) {
    e <- as.data.table(expected)[is.finite(ratio_expected) & ratio_expected > 0, .(iso3, crop, ratio_expected)]
    j <- merge(j[, !"ratio_expected"], e, by = c("iso3", "crop"), all.x = TRUE)
    j[is.na(ratio_expected), ratio_expected := 1]
  }
  # Every bound is applied to the product's move net of its input's move.
  j[, ratio := ratio_raw / ratio_expected]
  j[, livestock := grepl(DRIFT_LIVESTOCK_RE, crop)]
  j[, small := iso3 %in% small_iso3]

  unmatched <- j[is.na(T_live) | is.na(T_local)]
  m <- j[!is.na(T_live) & !is.na(T_local)]
  # A flip: real value on one side, essentially nothing on the other.
  flips <- m[(T_live >= min_live & T_local <= flip_frac * T_live) |
             (T_local >= min_live & T_live <= flip_frac * T_local)]
  material   <- m[T_live >= min_live & !small & !livestock]
  smallpop   <- m[T_live >= min_live & small & !livestock]
  immaterial <- m[T_live <  min_live]
  control    <- m[livestock & T_live >= min_live]
  totals <- m[livestock == FALSE, .(T_live = sum(T_live), T_local = sum(T_local), T_expected = sum(T_live * ratio_expected)), by = crop][T_live >= min_live]
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
    g("flips", sprintf("material one side, <= %.0f%% other", 100 * flip_frac), nrow(flips) == 0, sprintf("%d pairs", nrow(flips))),
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
              if (nrow(control)) signif(min(control$ratio), 4) else NA, if (nrow(control)) signif(max(control$ratio), 4) else NA, 100 * tol_control))
  ))
  n_exp <- if (is.null(expected)) 0L else m[ratio_expected != 1, .N]
  gates <- rbind(gates, g("expected", "pairs with an expected input move", TRUE,
                          if (n_exp) sprintf("%d material+immaterial pairs carry ratio_expected != 1; bounds applied net of it", n_exp) else "none supplied (every pair expects 1)"))
  list(pass = all(gates$result == "ok"), gates = gates,
       material = material[order(abs(ratio - 1), decreasing = TRUE)],
       small = smallpop[order(abs(ratio - 1), decreasing = TRUE)],
       immaterial = immaterial, unmatched = unmatched, flips = flips,
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
  if (nrow(res$flips))    { cat("\n  FLIPS:\n"); print(fmt(res$flips[, ..cols]), nrows = 50) }
  if (nrow(res$unmatched)){ cat("\n  UNMATCHED:\n"); print(fmt(res$unmatched[, .(iso3, crop, T_live, T_local)]), nrows = 50) }
  cat(sprintf("\n  immaterial pairs (live < %s): %d, not gated except for flips\n", format(res$params$min_live, big.mark = ","), nrow(res$immaterial)))
  invisible(res$pass)
}

# ---------------------------------------------------------------- CLI --------
if (sys.nframe() == 0L || nzchar(Sys.getenv("DRIFT_CLI"))) {
  args <- commandArgs(trailingOnly = TRUE)
  opt <- function(x, d) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
  local_f <- opt("--local", ""); live_f <- opt("--live", ""); exp_f <- opt("--expected", "")
  if (!nzchar(local_f) || !nzchar(live_f)) { cat("usage: --local <new.parquet> --live <old.parquet> [--expected <iso3,crop,ratio_expected csv>] [--tol-pair --tol-median --tol-total --tol-small --tol-control --min-live --small-iso3]\n"); quit(status = 2) }
  t0 <- Sys.time()
  .drift_log("local = %s", local_f); .drift_log("live  = %s", live_f)
  expected <- if (nzchar(exp_f)) { .drift_log("expected input moves from %s", exp_f); data.table::fread(exp_f) } else NULL
  res <- tier_drift(local_f, live_f,
                    tol_pair = as.numeric(opt("--tol-pair", "0.25")), tol_median = as.numeric(opt("--tol-median", "0.03")),
                    tol_total = as.numeric(opt("--tol-total", "0.05")), tol_small = as.numeric(opt("--tol-small", "0.5")),
                    tol_control = as.numeric(opt("--tol-control", "0.02")), min_live = as.numeric(opt("--min-live", "1e6")),
                    small_iso3 = strsplit(opt("--small-iso3", paste(DRIFT_SMALL_ISO3_DEFAULT, collapse = ",")), ",")[[1]],
                    expected = expected)
  print_drift(res, label = basename(local_f))
  .drift_log("done in %.1f min", as.numeric(difftime(Sys.time(), t0, units = "mins")))
  quit(status = if (res$pass) 0 else 1)
}
