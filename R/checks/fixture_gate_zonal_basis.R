#!/usr/bin/env Rscript
# R/checks/fixture_gate_zonal_basis.R
# ===================================
# Handover 2026-10-07 §2 A4. Off-node fixture for the zonal-basis fix in
# R/checks/usd_total_vs_reference.R. Synthetic parquets, no data, seconds.
#
# The defect: the gate took `is.na(admin1_name)` on both sides, which LOOKS like-for-like. It is
# not. R/3's adm0 row is formed from the lower levels, while 0.4.4 zones admin0 directly off its
# own rasterisation; at 0.25 deg with touches = TRUE a border cell can fall in one country's admin0
# zone and a neighbour's admin2 zone (#18). So the two tables disagreed over 31 border cells and
# the gate carried a residual it had no way to explain - a gate that fails a correct run, which is
# the failure mode AGENTS.md puts first.
#
# The fix compares the admin2 rows of BOTH tables, summed per (iso3, crop). Both were rasterised
# from the same Geographies onto the same base_rast, so the border assignment is identical on both
# sides and there is no residual left to wave through.
#
# This fixture builds exactly that asymmetry - the product's adm0 equals its admin2 sum, the
# reference's adm0 is independently zoned and 1 % larger for one pair - and asserts that the old
# basis sees the residual while the new one does not. It also asserts the new basis still FAILS a
# genuinely shifted product, so the fix removes a false failure without removing the gate.
#
# Usage: Rscript R/checks/fixture_gate_zonal_basis.R
suppressPackageStartupMessages({ library(data.table); library(arrow); library(dplyr) })
root <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else {
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd()
}
ok <- function(cond, msg) { if (!isTRUE(cond)) stop("FIXTURE FAIL: ", msg) else cat("  ok  ", msg, "\n") }
GATE <- file.path(root, "R", "checks", "usd_total_vs_reference.R")

SCEN <- c("historic", "ssp126", "ssp245", "ssp370", "ssp585")
HV   <- c("NDWS+NTx35", "NDWS+NTx35+NDWL0")
# KEN maize is the border pair: the reference's independently-zoned admin0 total is 1 % above what
# its own admin2 rows sum to. The product's adm0 is the admin2 sum, as R/3 builds it.
PAIRS <- data.table(iso3 = c("KEN", "KEN", "NGA", "AGO"), crop = c("maize", "wheat", "maize", "maize"),
                    E = c(4e6, 2e6, 9e6, 3e6), adm0_bump = c(1.01, 1, 1, 1))

build <- function(dir_root, product_shift = 1) {
  d_usd <- file.path(dir_root, "hazard_risk_vop_usd", "jagermeyr")
  d_int <- file.path(dir_root, "hazard_risk_vop", "jagermeyr")
  d_exp <- file.path(dir_root, "exposure")
  for (d in c(d_usd, d_int, d_exp)) dir.create(d, recursive = TRUE, showWarnings = FALSE)
  mk_haz <- function(f) {
    g <- CJ(i = seq_len(nrow(PAIRS)), scenario = SCEN, hazard_vars = HV, hazard = c("any", "none"), unique = TRUE)
    g[, `:=`(iso3 = PAIRS$iso3[i], crop = PAIRS$crop[i], E = PAIRS$E[i] * product_shift)]
    g[, value := ifelse(hazard == "any", 0.3, 0.7) * E]
    # two admin2 units per pair; the adm0 row is their sum, which is how R/3 forms it
    a2 <- rbindlist(lapply(1:2, function(k) g[, .(iso3, admin0_name = iso3, admin1_name = "p1",
                           admin2_name = sprintf("d%d", k), crop, scenario, hazard_vars, hazard,
                           severity = "severe", value = value / 2)]))
    a0 <- g[, .(iso3, admin0_name = iso3, admin1_name = NA_character_, admin2_name = NA_character_,
                crop, scenario, hazard_vars, hazard, severity = "severe", value)]
    write_parquet(rbind(a0, a2), f)
  }
  mk_haz(file.path(d_usd, "haz-freq-exp_vop_nominal-usd-2021_ENSEMBLEmean_int_adm_severe.parquet"))
  mk_haz(file.path(d_int, "haz-freq-exp_vop_intld15-2021_ENSEMBLEmean_int_adm_severe.parquet"))
  mk_ref <- function(f, unit) {
    # admin2 rows sum to E; the independently-zoned adm0 row carries the border cell as well
    a2 <- rbindlist(lapply(1:2, function(k) PAIRS[, .(iso3, admin0_name = iso3, admin1_name = "p1",
                    admin2_name = sprintf("d%d", k), crop, tech = "all", exposure = "vop", unit = unit, value = E / 2)]))
    a0 <- PAIRS[, .(iso3, admin0_name = iso3, admin1_name = NA_character_, admin2_name = NA_character_,
                    crop, tech = "all", exposure = "vop", unit = unit, value = E * adm0_bump)]
    write_parquet(rbind(a0, a2), f)
  }
  mk_ref(file.path(d_exp, "vop_nominal-usd-2021_adm_sum_spam20_glw420_res-25.parquet"), "nominal-usd-2021")
  mk_ref(file.path(d_exp, "exposure_adm_sum_spam20-20_glw420-20_res-25.parquet"), "intld15-2021")
  dir_root
}

# Run the real gate in a child process, with a tiny driver that supplies atlas_dirs. The gate ends
# in quit(status), so the exit code IS its verdict - which is also what a dispatch reads.
run_gate <- function(dir_root, basis) {
  drv <- file.path(dir_root, sprintf("drive_%s.R", basis))
  writeLines(c(
    sprintf('base <- "%s"', dir_root),
    'atlas_dirs <- list(data_dir = list(hazard_risk_vop_usd = file.path(base, "hazard_risk_vop_usd"),',
    '                                   hazard_risk_vop = file.path(base, "hazard_risk_vop"),',
    '                                   exposure = file.path(base, "exposure")))',
    'exposure_dir <- file.path(base, "exposure")',
    'Sys.setenv(ATLAS_SETUP_SKIP = "1", GATE_MIN_REF = "1e5")',
    sprintf('source("%s")', GATE)), drv)
  out <- suppressWarnings(system2("Rscript", c(drv, "--iso3", "all", "--basis", basis),
                                  stdout = TRUE, stderr = TRUE))
  list(status = attr(out, "status") %||% 0L, out = paste(out, collapse = "\n"))
}
`%||%` <- function(a, b) if (is.null(a)) b else a

tmp <- file.path(tempdir(), "zbasis"); dir.create(tmp, showWarnings = FALSE)

## ------------------------------------------------- a correct product, the two bases disagree
good <- build(file.path(tmp, "good"))
r_a2 <- run_gate(good, "admin2")
r_a0 <- run_gate(good, "admin0")
cat(sub("(?s).*(=== usd.*?PASS|=== usd.*?FAIL[^\n]*).*", "\\1", r_a2$out, perl = TRUE), "\n")
ok(r_a2$status == 0L, "admin2 basis PASSES a correct product (both sides zoned the same way, ratio 1)")
ok(grepl("range \\[1, 1\\]", r_a2$out), "and the material ratios are exactly 1, not merely inside a loose band")
ok(grepl("zonal-basis check", r_a2$out) &&
   grepl("product admin0 totals differ by median 1.00000 \\(max \\|1-d\\| 0.0000", r_a2$out) &&
   grepl("reference by median 1.00000 \\(max 0.0100\\)", r_a2$out),
   "the gap is MEASURED and attributed: 0 on the product (its adm0 IS the admin2 sum) and 1 % on the reference (independently zoned) — #18's border cells as a number, not a story")
ok(grepl("0.990", r_a0$out),
   "the old admin0 basis sees the border-cell residual it cannot explain — the false failure this fix removes")

## ------------------------------------------------- a genuinely shifted product still fails
bad <- build(file.path(tmp, "bad"), product_shift = 3)
r_bad <- run_gate(bad, "admin2")
ok(r_bad$status != 0L, "admin2 basis still FAILS a product shifted 3x — the fix removes a false failure, not the gate")
ok(grepl("outside \\[0.5, 2\\]", r_bad$out), "and reports the pairs that are out of band")

## ------------------------------------------------- the option is validated
r_junk <- run_gate(good, "admin9")
ok(r_junk$status != 0L && grepl("--basis must be admin0 or admin2", r_junk$out),
   "an unknown --basis aborts rather than silently falling back to one of them")

cat("\nALL ZONAL-BASIS FIXTURE ASSERTIONS PASSED\n")
