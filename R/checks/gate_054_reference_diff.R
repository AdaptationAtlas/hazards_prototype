#!/usr/bin/env Rscript
# R/checks/gate_054_reference_diff.R
# ==================================
# Post-0.4.4 gate for issues #54 + #52: the rebuilt exposure tables against the copies parked BEFORE
# the rebuild, on the same grid. The two changes are:
#   #54  four livestock constant-I$ (iso3, species) pairs take a basis fallback (LBY and SLE cattle,
#        LSO poultry, SWZ sheep: FAOSTAT placeholders 1 / 2 / 0 thousand I$). exposure == "vop",
#        unit == "intld15-2021", those pairs only.
#   #52  the two TLU aggregates (crop total-highland / total-tropical) move from exposure = "number",
#        unit = "number" to exposure = "tlu", unit = "tlu", values untouched.
# So:
#   1. same row set once the old TLU rows are read as (tlu, tlu);
#   2. every row that is NOT a #54 pair is value-identical (NaN == NaN): crops, nominal, prod,
#      harv-area, phys-area, number, the relabelled TLU rows, every other intld row;
#   3. each #54 pair's new admin2 sum (both systems of the species) is within --tol of the probe's
#      NEW national value (R/checks/probe_054_intld_guard.R --out CSV, year_set y2021). The OLD side
#      is ~1,000 split over pixels and rounded to integers, so a ratio against it is noise; the gate
#      is on the NEW level. Pairs the probe did not flag must not move (rule 2);
#   4. the new intld livestock admin2 total is within --tol-total of old + the probe's (NEW - OLD).
# Runs on the combined reference, the nominal twin (expects NO change at all) and the intld twin.
# Read-only. arrow only (no duckdb).
# Usage (cglabs, repo root):
#   Rscript R/checks/gate_054_reference_diff.R --old-dir <parked dir> --res 0.25 --probe <probe csv> [--tol 0.02] [--tol-total 0.01]
# --new-dir <dir> overrides exposure_dir (the fixture uses it; then 0_server_setup.R is not sourced).

.log <- function(fmt, ...) cat(sprintf("[%s] [gate-054-ref] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), sprintf(fmt, ...)))
args <- commandArgs(trailingOnly = TRUE)
opt  <- function(x, d) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
OLD_DIR <- opt("--old-dir", ""); RES <- opt("--res", ""); PROBE <- opt("--probe", ""); NEW_DIR <- opt("--new-dir", "")
TOL <- as.numeric(opt("--tol", "0.02")); TOL_TOTAL <- as.numeric(opt("--tol-total", "0.01"))
if (!nzchar(OLD_DIR) || !RES %in% c("0.25", "0.05") || !nzchar(PROBE)) { cat("usage: --old-dir <dir> --res 0.25|0.05 --probe <csv> [--new-dir <dir>] [--tol 0.02] [--tol-total 0.01]\n"); quit(status = 2) }
tag <- if (RES == "0.25") "res-25" else "res-05"
suppressPackageStartupMessages({ library(data.table); library(arrow) })
if (nzchar(NEW_DIR)) { exposure_dir <- NEW_DIR } else {
  setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
  suppressMessages(suppressWarnings(source(setup)))
}
if (!file.exists(PROBE)) stop("--probe not found: ", PROBE)
probe <- fread(PROBE)[year_set == "y2021"]
need <- c("iso3", "glw3_name", "intld_old", "intld_new", "intld_source")
if (!all(need %in% names(probe))) stop("probe CSV lacks ", paste(setdiff(need, names(probe)), collapse = ", "))
fac <- probe[, .(old_nat = sum(intld_old, na.rm = TRUE), new_nat = sum(intld_new, na.rm = TRUE), flagged = any(intld_source %in% "basis fallback")), by = .(iso3, glw3_name)]
moved <- fac[flagged == TRUE]
delta_nat <- probe[, sum(intld_new, na.rm = TRUE) - sum(intld_old, na.rm = TRUE)]
.log("probe: %d (iso3, class) pairs flagged: %s; national NEW - OLD = %.3f M", nrow(moved), paste(moved[, paste(iso3, glw3_name)], collapse = ", "), delta_nat / 1e6)
if (nrow(moved) == 0) stop("the probe flagged no pair: nothing for this gate to judge")

LS_RE <- "^(cattle|sheep|goats|pigs|poultry)-(highland|tropical)$"
TLU_RE <- "^total-(highland|tropical)$"
KEY <- c("iso3", "admin0_name", "admin1_name", "admin2_name", "crop", "exposure", "unit", "tech", "stat")
same <- function(a, b) (is.na(a) & is.na(b)) | (!is.na(a) & !is.na(b) & abs(a - b) <= 1e-9 * pmax(abs(b), 1))

judge <- function(fname, expect_moved = TRUE, expect_tlu = TRUE) {
  fo <- file.path(OLD_DIR, fname); fn <- file.path(exposure_dir, fname)
  if (!file.exists(fo) || !file.exists(fn)) { .log("FAIL %s: missing (old %s, new %s)", fname, file.exists(fo), file.exists(fn)); return(FALSE) }
  o <- as.data.table(read_parquet(fo)); n <- as.data.table(read_parquet(fn))
  k <- intersect(KEY, intersect(names(o), names(n)))
  .log("%s: old %d rows, new %d rows; key %s", fname, nrow(o), nrow(n), paste(k, collapse = ","))
  # #52: read the old TLU rows as the new slot so the row sets can be compared
  tlu_old <- 0L; tlu_new <- 0L
  if (all(c("exposure", "unit", "crop") %in% k)) {
    tlu_old <- o[exposure == "number" & grepl(TLU_RE, crop), .N]
    o[exposure == "number" & grepl(TLU_RE, crop), `:=`(exposure = "tlu", unit = "tlu")]
    tlu_new <- n[exposure == "tlu", .N]
    if ("unit_full" %in% names(n)) { bad_uf <- n[exposure == "tlu" & unit_full != "tlu", .N]; if (bad_uf) .log("  FAIL: %d tlu rows carry unit_full != tlu", bad_uf) }
  }
  dup <- c(old = anyDuplicated(o, by = k) > 0, new = anyDuplicated(n, by = k) > 0)
  j <- merge(o[, c(k, "value"), with = FALSE], n[, c(k, "value"), with = FALSE], by = k, all = TRUE, suffixes = c("_old", "_new"))
  unmatched <- j[is.na(value_old) & !is.na(value_new) | !is.na(value_old) & is.na(value_new)]
  rowset_ok <- nrow(o) == nrow(n) && nrow(j) == nrow(o) && !any(dup)
  if (!"unit" %in% names(j)) j[, unit := if (grepl("nominal-usd", fname)) "nominal-usd-2021" else "intld15-2021"]
  if (!"exposure" %in% names(j)) j[, exposure := "vop"]
  j[, glw3_name := sub("-(highland|tropical)$", "", crop)]
  j[, moved_by_54 := exposure == "vop" & unit == "intld15-2021" & grepl(LS_RE, crop) & paste(iso3, glw3_name) %in% moved[, paste(iso3, glw3_name)]]
  other <- j[moved_by_54 == FALSE]
  other_bad <- other[!same(value_new, value_old)]
  tlu_ok <- if (expect_tlu) tlu_old > 0 && tlu_new == tlu_old && n[exposure == "tlu" & unit != "tlu", .N] == 0 && j[exposure == "tlu" & !same(value_new, value_old), .N] == 0 else tlu_old == 0 && tlu_new == 0
  ls <- j[moved_by_54 == TRUE & !is.na(admin2_name) & (is.na(tech) | tech == "all")]
  pair <- ls[, .(old = sum(value_old[is.finite(value_old)]), new = sum(value_new[is.finite(value_new)])), by = .(iso3, glw3_name)]
  pair <- merge(pair, moved[, .(iso3, glw3_name, new_nat, old_nat)], by = c("iso3", "glw3_name"), all = TRUE)
  pair[, off := new / new_nat - 1]
  pair_bad <- pair[is.na(new) | !is.finite(off) | abs(off) > TOL]
  tot <- j[exposure == "vop" & unit == "intld15-2021" & grepl(LS_RE, crop) & !is.na(admin2_name) & (is.na(tech) | tech == "all"),
           .(old = sum(value_old[is.finite(value_old)]), new = sum(value_new[is.finite(value_new)]))]
  tot_ok <- abs((tot$new - (tot$old + delta_nat)) / pmax(tot$new, 1)) <= TOL_TOTAL
  .log("  rows: %s (old %d / new %d / joined %d, unmatched-value %d, duplicate keys old %s new %s)", if (rowset_ok && nrow(unmatched) == 0) "ok" else "FAIL", nrow(o), nrow(n), nrow(j), nrow(unmatched), dup[["old"]], dup[["new"]])
  if (nrow(unmatched)) print(head(unmatched[, .N, by = .(exposure, unit, crop)][order(-N)], 20))
  .log("  #52 TLU slot: %s (old number/total-* rows %d -> new tlu rows %d, values identical)", if (tlu_ok) "ok" else "FAIL", tlu_old, tlu_new)
  .log("  not-#54 rows identical: %s (%d rows, %d differ)", if (nrow(other_bad) == 0) "ok" else "FAIL", nrow(other), nrow(other_bad))
  if (nrow(other_bad)) print(head(other_bad[, .N, by = .(exposure, unit, crop)][order(-N)], 30))
  if (!expect_moved) {
    g <- c(rowset = rowset_ok && nrow(unmatched) == 0, tlu = tlu_ok, others_identical = nrow(other_bad) == 0, no_moved_rows = nrow(ls) == 0 || ls[!same(value_new, value_old), .N] == 0)
    .log("  #54 pairs present but unchanged (this table must not move): %s", if (g[["no_moved_rows"]]) "ok" else "FAIL")
    return(all(g))
  }
  g <- c(rowset = rowset_ok && nrow(unmatched) == 0, tlu = tlu_ok, others_identical = nrow(other_bad) == 0, pairs = nrow(pair_bad) == 0 && nrow(pair) == nrow(moved), total = tot_ok)
  .log("  #54 pairs vs probe NEW: %s (%d pairs; %d off by > %.0f%%)", if (g[["pairs"]]) "ok" else "FAIL", nrow(pair), nrow(pair_bad), 100 * TOL)
  print(pair[, .(iso3, glw3_name, old_adm2 = old, new_adm2 = new, new_nat, off = signif(off, 3))])
  .log("  intld livestock total: %s (old %.4f B -> new %.4f B; expected old + %.3f M)", if (tot_ok) "ok" else "FAIL", tot$old / 1e9, tot$new / 1e9, delta_nat / 1e6)
  all(g)
}
res <- c(judge(sprintf("exposure_adm_sum_spam20-20_glw420-20_%s.parquet", tag)),
         judge(sprintf("vop_nominal-usd-2021_adm_sum_spam20_glw420_%s.parquet", tag), expect_moved = FALSE, expect_tlu = FALSE),
         judge(sprintf("vop_intld15-2021_adm_sum_spam20_glw420_%s.parquet", tag), expect_tlu = FALSE))
.log("%s (%s): %d/%d tables pass", if (all(res)) "GATE 054-REF PASS" else "GATE 054-REF FAIL", tag, sum(res), length(res))
quit(status = if (all(res)) 0 else 1)
