#!/usr/bin/env Rscript
# R/checks/exposure_pair_drift.R
# ===============================
# Old-vs-new report for a 0.4.4 combined exposure table (crop-livestock_all): per (iso3, crop,
# exposure, unit) at admin0, tech = all, the ratio new / old, in populations, with the movers
# named. A REPORT, not a gate (exit 0): the dispatch states which movers are expected and the
# cross-basis gate is the arbiter of whether a side is sound (vop_cross_basis_gate.R). The
# reference a gate judges against must be independent of the table it judges; the old table is
# not, so this only says WHAT moved.
#
# Usage (node, seconds; arrow only):
#   Rscript R/checks/exposure_pair_drift.R --old <parked parquet> --new <new parquet> [--min-ref 1e6] [--out <csv>]
t0 <- Sys.time()
.ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) cat(sprintf("[%s] [pair-drift] %s\n", .ts(), sprintf(fmt, ...)))
args <- commandArgs(trailingOnly = TRUE)
opt <- function(x, d) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
OLD <- opt("--old", ""); NEW <- opt("--new", ""); MIN_REF <- as.numeric(opt("--min-ref", "1e6")); OUT <- opt("--out", "")
if (!nzchar(OLD) || !nzchar(NEW)) stop("--old and --new are required")
for (f in c(OLD, NEW)) if (!file.exists(f)) stop("missing ", f)
suppressPackageStartupMessages({ library(arrow); library(dplyr); library(data.table) })
rd <- function(f) {
  d <- arrow::open_dataset(f) |> dplyr::filter(is.na(admin1_name)) |> dplyr::select(iso3, crop, exposure, unit, tech, value) |> dplyr::collect() |> as.data.table()
  d <- d[(tech == "all" | is.na(tech))]
  d[, .(value = sum(value, na.rm = TRUE), n = .N, all_na = all(is.na(value))), by = .(iso3, crop, exposure, unit)]
}
.log("old = %s (mtime %s)", OLD, format(file.mtime(OLD), "%Y-%m-%d %H:%M")); .log("new = %s (mtime %s)", NEW, format(file.mtime(NEW), "%Y-%m-%d %H:%M"))
o <- rd(OLD); n <- rd(NEW)
m <- merge(o[, .(iso3, crop, exposure, unit, old = value, old_na = all_na)], n[, .(iso3, crop, exposure, unit, new = value, new_na = all_na)], by = c("iso3", "crop", "exposure", "unit"), all = TRUE)
m[, livestock := grepl("cattle|sheep|goats|pigs|poultry|total-", crop)]
m[, status := fifelse(is.na(old) | isTRUE(old_na), fifelse(is.na(new) | isTRUE(new_na), "absent both", "appears"),
              fifelse(is.na(new) | isTRUE(new_na), "disappears", fifelse(old == 0 & new == 0, "zero both", fifelse(old == 0, "from zero", fifelse(new == 0, "to zero", "both")))))]
m[status == "both", ratio := new / old]
.log("pairs: %s", paste(sprintf("%s=%d", names(table(m$status)), as.integer(table(m$status))), collapse = " | "))
lv <- m[livestock == TRUE & status == "both"]
.log("livestock control (must not move): %d pairs, max |ratio - 1| = %s", nrow(lv), if (nrow(lv)) signif(max(abs(lv$ratio - 1)), 3) else NA)
if (nrow(lv) && max(abs(lv$ratio - 1)) > 1e-6) print(lv[abs(ratio - 1) > 1e-6][order(-abs(ratio - 1))][1:min(10, .N), .(iso3, crop, exposure, unit, old = signif(old, 4), new = signif(new, 4), ratio = signif(ratio, 4))])
cat("\nper unit (crop rows), continental total new/old and material-pair ratio distribution (old >= min-ref):\n")
pu <- m[livestock == FALSE, .(n_both = sum(status == "both"), appears = sum(status == "appears" | status == "from zero"), disappears = sum(status == "disappears" | status == "to zero"),
                              total_ratio = signif(sum(new[status %in% c("both", "appears", "from zero")], na.rm = TRUE) / sum(old[status %in% c("both", "disappears", "to zero")], na.rm = TRUE), 4),
                              med = signif(median(ratio[status == "both" & old >= MIN_REF], na.rm = TRUE), 3),
                              q05 = signif(quantile(ratio[status == "both" & old >= MIN_REF], .05, na.rm = TRUE), 3), q95 = signif(quantile(ratio[status == "both" & old >= MIN_REF], .95, na.rm = TRUE), 3),
                              beyond_2x = sum(status == "both" & old >= MIN_REF & (ratio > 2 | ratio < 0.5), na.rm = TRUE)), by = .(exposure, unit)][order(exposure, unit)]
print(pu, nrows = 20)
show <- function(d) d[, .(iso3, crop, unit, old = signif(old, 4), new = signif(new, 4), ratio = signif(ratio, 3))]
for (u in m[exposure == "vop" & livestock == FALSE, sort(unique(unit))]) {
  mv <- m[exposure == "vop" & unit == u & status == "both" & old >= MIN_REF & (ratio > 2 | ratio < 0.5)][order(-abs(log(ratio)))]
  cat(sprintf("\n[%s] material movers beyond 2x: %d (showing up to 40)\n", u, nrow(mv))); if (nrow(mv)) print(show(mv)[1:min(40, .N)], nrows = 40)
  ap <- m[exposure == "vop" & unit == u & status %in% c("appears", "from zero")][order(-new)]
  cat(sprintf("[%s] pairs that APPEAR (NA/0 before): %d; crops %s; largest: %s\n", u, nrow(ap), paste(sort(unique(ap$crop)), collapse = ","), if (nrow(ap)) paste(ap[1:min(8, .N), sprintf("%s:%s=%s", iso3, crop, signif(new, 3))], collapse = " ") else ""))
  dp <- m[exposure == "vop" & unit == u & status %in% c("disappears", "to zero")][order(-old)]
  cat(sprintf("[%s] pairs that DISAPPEAR (NA/0 now): %d; countries %s; largest: %s\n", u, nrow(dp), paste(sort(unique(dp$iso3)), collapse = ","), if (nrow(dp)) paste(dp[1:min(8, .N), sprintf("%s:%s=%s", iso3, crop, signif(old, 3))], collapse = " ") else ""))
  pc <- m[exposure == "vop" & unit == u & livestock == FALSE & status %in% c("both", "appears", "disappears", "from zero", "to zero"), .(r = { so <- sum(old, na.rm = TRUE); if (so > 0) sum(new, na.rm = TRUE) / so else NA_real_ }), by = crop][is.finite(r)][order(r)]
  cat(sprintf("[%s] per-crop continental ratio, lowest 6: %s | highest 6: %s\n", u, paste(pc[1:min(6, .N), sprintf("%s %.2f", crop, r)], collapse = ", "), paste(pc[max(1, .N - 5):.N, sprintf("%s %.2f", crop, r)], collapse = ", ")))
}
pr <- m[exposure == "prod" & livestock == FALSE & status == "both"]
.log("production rows (prod_t, same SPAM input): %d pairs, max |ratio - 1| = %s (res-25 may differ at the coast and SYC through touches = TRUE)", nrow(pr), if (nrow(pr)) signif(max(abs(pr$ratio - 1), na.rm = TRUE), 3) else NA)
if (!nzchar(OUT)) OUT <- file.path(tempdir(), sprintf("exposure_pair_drift_%s.csv", format(Sys.time(), "%Y%m%d_%H%M%S")))
fwrite(m, OUT); .log("per-pair table written to %s (%d rows) in %.1f min", OUT, nrow(m), as.numeric(difftime(Sys.time(), t0, units = "mins")))
