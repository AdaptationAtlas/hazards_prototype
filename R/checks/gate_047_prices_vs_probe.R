#!/usr/bin/env Rscript
# R/checks/gate_047_prices_vs_probe.R
# ===================================
# Post-0.4.1 gate for issue #47. The fixed 0.4.1 §4.1 (R/price_fill.R::livestock_nominal_price)
# must reproduce, row for row, the C column the read-only probe measured on the node BEFORE the fix
# (R/checks/041_outputs/issue47_livestock_price_20261021_125145.csv, committed 9540f97). That CSV is
# a fixed artefact written by different code (the probe's own composition of clip + fills), so it
# is an independent reference for what the new code writes:
#   nominal  <glw_pro_dir>/fao_vop/glw4-2020_vop_nominal-usd-<Y>-prices-used.csv  value == vop_C,
#            price_usd_final == price_C, for Y in 2021, 2015, 2020
#   intld    <glw_pro_dir>/fao_vop/glw4-2020_vop_intld15-<Y>-prices-used.csv     value == vop_intd15
#            (the constant-I$ side must NOT move)
# and each CSV must be newer than --since (the rerun's start), so a skipped write cannot pass.
# Read-only. Exit 0 = PASS, 1 = FAIL.
# Usage (cglabs, repo root, seconds):
#   Rscript R/checks/gate_047_prices_vs_probe.R --since "2026-10-22 09:00" [--probe R/checks/041_outputs/issue47_...csv] [--tol 1e-9]

t0 <- Sys.time()
.log <- function(fmt, ...) cat(sprintf("[%s] [gate-047] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), sprintf(fmt, ...)))
args <- commandArgs(trailingOnly = TRUE)
opt  <- function(x, d) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
SINCE <- opt("--since", "")
TOL   <- as.numeric(opt("--tol", "1e-9"))
if (!nzchar(SINCE)) { cat("usage: --since 'YYYY-MM-DD HH:MM' [--probe <csv>] [--tol 1e-9]\n"); quit(status = 2) }
since <- as.POSIXct(SINCE)

setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
suppressMessages(suppressWarnings(source(setup)))
suppressPackageStartupMessages(library(data.table))
probe_f <- opt("--probe", file.path(project_dir, "R", "checks", "041_outputs", "issue47_livestock_price_20261021_125145.csv"))
stopifnot(file.exists(probe_f))
probe <- fread(probe_f)
.log("reference: %s (%d rows, year sets %s)", basename(probe_f), nrow(probe), paste(sort(unique(probe$year_set)), collapse = ","))
fao_vop <- file.path(glw2020_pro_dir, "fao_vop")

rel <- function(a, b) abs(a - b) / pmax(abs(b), 1)
check <- function(kind, y) {
  f <- file.path(fao_vop, sprintf("glw4-2020_vop_%s-%s-prices-used.csv", kind, y))
  if (!file.exists(f)) { .log("FAIL %s %s: file missing: %s", kind, y, f); return(FALSE) }
  fresh <- file.mtime(f) >= since
  new <- unique(fread(f)[, intersect(c("iso3", "atlas_name", "value", "price_usd_final", "price_source"), names(fread(f, nrows = 0))), with = FALSE])
  ref <- probe[year_set == paste0("y", y)]
  if (kind == "nominal-usd") {
    j <- merge(new, ref[, .(iso3, atlas_name, ref_value = vop_C, ref_price = price_C)], by = c("iso3", "atlas_name"), all = TRUE)
    if (!"price_usd_final" %in% names(j)) { .log("FAIL %s %s: no price_usd_final column - is this the pre-#47 0.4.1 output?", kind, y); return(FALSE) }
    bad <- j[(is.na(value) != is.na(ref_value)) | (!is.na(value) & rel(value, ref_value) > TOL) |
             (is.na(price_usd_final) != is.na(ref_price)) | (!is.na(price_usd_final) & rel(price_usd_final, ref_price) > TOL)]
  } else {
    j <- merge(new, ref[, .(iso3, atlas_name, ref_value = vop_intd15)], by = c("iso3", "atlas_name"), all = TRUE)
    bad <- j[(is.na(value) != is.na(ref_value)) | (!is.na(value) & rel(value, ref_value) > TOL)]
  }
  pass <- fresh && nrow(bad) == 0
  .log("%s %s %s: %d rows, total new %.3f B vs reference %.3f B, %d differ; mtime %s (%s --since)", if (pass) "ok  " else "FAIL",
       kind, y, nrow(j), sum(j$value, na.rm = TRUE) / 1e9, sum(j$ref_value, na.rm = TRUE) / 1e9, nrow(bad),
       format(file.mtime(f), "%Y-%m-%d %H:%M"), if (fresh) "after" else "BEFORE")
  if (nrow(bad)) print(head(bad[order(-rel(value, ref_value))], 30))
  if (kind == "nominal-usd" && "price_source" %in% names(new)) print(new[!is.na(value), .(n = .N, value_bn = round(sum(value) / 1e9, 3)), by = price_source][order(-n)])
  pass
}
res <- c(sapply(c("2021", "2015", "2020"), function(y) check("nominal-usd", y)),
         sapply(c("2021", "2015", "2020"), function(y) check("intld15", y)))
.log("%s: %d/%d checks pass (%.1f s)", if (all(res)) "GATE 047 PASS" else "GATE 047 FAIL", sum(res), length(res), as.numeric(difftime(Sys.time(), t0, units = "secs")))
quit(status = if (all(res)) 0 else 1)
