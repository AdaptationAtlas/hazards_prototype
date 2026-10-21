#!/usr/bin/env Rscript
# R/checks/gate_047_reference_diff.R
# ==================================
# Post-0.4.4 gate for issue #47: the rebuilt exposure tables against the copies parked BEFORE the
# rebuild, on the same grid. #47 reprices livestock nominal USD only, so:
#   1. same row set (no row added or lost);
#   2. every row that is NOT (exposure == "vop", unit == "nominal-usd-2021", livestock species) is
#      value-identical (NaN == NaN) - crops, intld, prod, harv-area, phys-area, number, TLU;
#   3. livestock nominal rows move, per (iso3, species), by the factor the probe measured nationally
#      (C/A per iso3 x GLW class, R/checks/041_outputs/...): 0.4.1 spreads one national value over
#      pixels by GLW share and splits highland/tropical by mask, so every admin2 row of that country
#      and species scales by that one factor, up to per-pixel rounding. Pairs with old >= --min-old
#      must sit within --tol of it;
#   4. the new livestock nominal admin2 total is within --tol-total of old x (C total / A total).
# Runs on the combined reference AND the nominal twin (the G6 basis). Read-only. arrow only (no duckdb).
# Usage (cglabs, repo root):
#   Rscript R/checks/gate_047_reference_diff.R --old-dir <parked dir> --res 0.25 [--tol 0.02] [--min-old 1e6] [--tol-total 0.01]

.log <- function(fmt, ...) cat(sprintf("[%s] [gate-047-ref] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), sprintf(fmt, ...)))
args <- commandArgs(trailingOnly = TRUE)
opt  <- function(x, d) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
OLD_DIR <- opt("--old-dir", ""); RES <- opt("--res", "")
TOL <- as.numeric(opt("--tol", "0.02")); MIN_OLD <- as.numeric(opt("--min-old", "1e6")); TOL_TOTAL <- as.numeric(opt("--tol-total", "0.01"))
if (!nzchar(OLD_DIR) || !RES %in% c("0.25", "0.05")) { cat("usage: --old-dir <dir> --res 0.25|0.05 [--tol 0.02] [--min-old 1e6] [--tol-total 0.01]\n"); quit(status = 2) }
tag <- if (RES == "0.25") "res-25" else "res-05"

setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
suppressMessages(suppressWarnings(source(setup)))
suppressPackageStartupMessages({ library(data.table); library(arrow) })

probe <- fread(file.path(project_dir, "R", "checks", "041_outputs", "issue47_livestock_price_20261021_125145.csv"))[year_set == "y2021"]
fac <- probe[, .(A = sum(vop_A, na.rm = TRUE), C = sum(vop_C, na.rm = TRUE)), by = .(iso3, glw3_name)][A > 0, .(iso3, glw3_name, f_exp = C / A)]
total_factor <- probe[, sum(vop_C, na.rm = TRUE) / sum(vop_A, na.rm = TRUE)]
.log("probe factors: %d (iso3, class) pairs; continental C/A = %.4f", nrow(fac), total_factor)

LS_RE <- "^(cattle|sheep|goats|pigs|poultry)-(highland|tropical)$"
KEY <- c("iso3", "admin0_name", "admin1_name", "admin2_name", "crop", "exposure", "unit", "tech", "stat")
same <- function(a, b) (is.na(a) & is.na(b)) | (!is.na(a) & !is.na(b) & abs(a - b) <= 1e-9 * pmax(abs(b), 1))

judge <- function(fname, expect_moved = TRUE) {
  fo <- file.path(OLD_DIR, fname); fn <- file.path(exposure_dir, fname)
  if (!file.exists(fo) || !file.exists(fn)) { .log("FAIL %s: missing (old %s, new %s)", fname, file.exists(fo), file.exists(fn)); return(FALSE) }
  o <- as.data.table(read_parquet(fo)); n <- as.data.table(read_parquet(fn))
  k <- intersect(KEY, intersect(names(o), names(n)))
  .log("%s: old %d rows (mtime %s), new %d rows (mtime %s); key %s", fname, nrow(o), format(file.mtime(fo), "%m-%d %H:%M"),
       nrow(n), format(file.mtime(fn), "%m-%d %H:%M"), paste(k, collapse = ","))
  dup <- c(old = anyDuplicated(o, by = k) > 0, new = anyDuplicated(n, by = k) > 0)
  j <- merge(o[, c(k, "value"), with = FALSE], n[, c(k, "value"), with = FALSE], by = k, all = TRUE, suffixes = c("_old", "_new"))
  unmatched <- j[is.na(value_old) & !is.na(value_new) | !is.na(value_old) & is.na(value_new)]
  rowset_ok <- nrow(o) == nrow(n) && nrow(j) == nrow(o) && !any(dup)
  if (!"unit" %in% names(j)) j[, unit := if (grepl("nominal-usd", fname)) "nominal-usd-2021" else NA_character_]
  if (!"exposure" %in% names(j)) j[, exposure := "vop"]
  j[, moved_by_47 := exposure == "vop" & unit == "nominal-usd-2021" & grepl(LS_RE, crop)]
  other <- j[moved_by_47 == FALSE]
  other_bad <- other[!same(value_new, value_old)]
  ls <- j[moved_by_47 == TRUE & !is.na(admin2_name) & (is.na(tech) | tech == "all")]
  ls[, glw3_name := sub("-(highland|tropical)$", "", crop)]
  pair <- ls[, .(old = sum(value_old[is.finite(value_old)]), new = sum(value_new[is.finite(value_new)])), by = .(iso3, crop, glw3_name)]
  pair <- merge(pair, fac, by = c("iso3", "glw3_name"), all.x = TRUE)
  pair[, ratio := new / old][, off := ratio / f_exp - 1]
  mat <- pair[old >= MIN_OLD]
  mat_bad <- mat[is.na(f_exp) | !is.finite(off) | abs(off) > TOL]
  tot_ratio <- sum(pair$new) / sum(pair$old)
  tot_ok <- abs(tot_ratio / total_factor - 1) <= TOL_TOTAL
  if (!expect_moved) {   # e.g. the constant-I$ twin: #47 must not touch a single row of it
    g <- c(rowset = rowset_ok && nrow(unmatched) == 0, others_identical = nrow(other_bad) == 0, no_moved_rows = nrow(ls) == 0)
    .log("  rows: %s | all rows identical: %s (%d differ) | livestock nominal rows present: %d (expect 0)",
         if (g[["rowset"]]) "ok" else "FAIL", if (g[["others_identical"]]) "ok" else "FAIL", nrow(other_bad), nrow(ls))
    if (nrow(other_bad)) print(head(other_bad[, .N, by = .(crop)][order(-N)], 30))
    return(all(g))
  }
  g <- c(rowset = rowset_ok && nrow(unmatched) == 0, others_identical = nrow(other_bad) == 0, livestock_pairs = nrow(mat_bad) == 0 && nrow(mat) > 0, livestock_total = tot_ok)
  .log("  rows: %s (old %d / new %d / joined %d, unmatched-value %d, duplicate keys old %s new %s)", if (g[["rowset"]]) "ok" else "FAIL", nrow(o), nrow(n), nrow(j), nrow(unmatched), dup[["old"]], dup[["new"]])
  .log("  not-#47 rows identical: %s (%d rows, %d differ)", if (g[["others_identical"]]) "ok" else "FAIL", nrow(other), nrow(other_bad))
  if (nrow(other_bad)) print(head(other_bad[, .N, by = .(exposure, unit, crop)][order(-N)], 30))
  .log("  livestock nominal pairs vs probe factor: %s (%d pairs >= %.0e; %d off by > %.0f%%; worst %s)", if (g[["livestock_pairs"]]) "ok" else "FAIL",
       nrow(mat), MIN_OLD, nrow(mat_bad), 100 * TOL, if (nrow(mat)) signif(max(abs(mat$off), na.rm = TRUE), 3) else NA)
  if (nrow(mat_bad)) print(head(mat_bad[order(-abs(off))], 30))
  .log("  livestock nominal total: %s (old %.3f B -> new %.3f B, ratio %.4f vs probe %.4f)", if (tot_ok) "ok" else "FAIL",
       sum(pair$old) / 1e9, sum(pair$new) / 1e9, tot_ratio, total_factor)
  all(g)
}
res <- c(judge(sprintf("exposure_adm_sum_spam20-20_glw420-20_%s.parquet", tag)),
         judge(sprintf("vop_nominal-usd-2021_adm_sum_spam20_glw420_%s.parquet", tag)),
         judge(sprintf("vop_intld15-2021_adm_sum_spam20_glw420_%s.parquet", tag), expect_moved = FALSE))
.log("%s (%s): %d/%d tables pass", if (all(res)) "GATE 047-REF PASS" else "GATE 047-REF FAIL", tag, sum(res), length(res))
quit(status = if (all(res)) 0 else 1)
