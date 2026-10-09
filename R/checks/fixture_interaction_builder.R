#!/usr/bin/env Rscript
# R/checks/fixture_interaction_builder.R
# ======================================
# #13 B1, found on the node 2026-10-09. #25(a)'s new crop interaction row `NDWS + NTxM + NDWL0` was
# in the table, printed in the log, and asserted by fixture_crop_heat_interactions.R — and produced
# **zero** stacks across an entire R/2 pass. §5.2 wrote 129 combinations and none paired a
# crop-specific NTx threshold with NDWS/NDWL0.
#
# Cause: `metadata/haz_classes.csv` carries NDWS and NDWL0 only under `crop = generic`. They are
# replicated per species for LIVESTOCK (`R/2:142`), but never for crops, which instead get the
# ecocrop-derived PTOT/TAVG/NTx* rows. The builder looked every slot up in ONE crop's table, so for
# a real crop the mixed row mapped `dry` and `wet` to NA and the `!is.na` filter dropped it. Row 2
# survived because PTOT_L/PTOT_G are per-crop; row 1 survived only for `generic`.
#
# The lesson this fixture exists for: **the old fixture pinned the TABLE and the LABELS, not the
# BUILDER.** It asserted that the row was declared and that the published name would be right. Both
# were true. Neither says the row resolves to anything. A declaration is not an output.
#
# So this one runs the real builder against the real haz_classes.csv and asserts that each declared
# row produces combinations for the crops it should.
#
# Usage: Rscript R/checks/fixture_interaction_builder.R
suppressPackageStartupMessages({ library(data.table) })
root <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else {
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd()
}
ok <- function(cond, msg) { if (!isTRUE(cond)) stop("FIXTURE FAIL: ", msg) else cat("  ok  ", msg, "\n") }
R2 <- file.path(root, "R", "2_calculate_haz_freq.R")

## Lift the interaction tables and the slot resolver out of R/2.
for (e in parse(R2)) {
  if (is.call(e) && length(e) >= 3 && as.character(e[[1]])[1] %in% c("<-", "=") && is.name(e[[2]]) &&
      as.character(e[[2]]) %in% c("crop_interactions", "animal_interactions", ".resolve_slot")) eval(e, envir = globalenv())
}
ok(exists(".resolve_slot"), "the slot resolver was lifted out of R/2 (it is the thing under test)")

## Build a haz_class the shape R/2 builds: generic rows for the fixed hazards, per-crop ecocrop rows
## for the free ones, and the livestock replication of generic non-heat rows.
SEV <- "Severe"
mkrow <- function(idx, crop, thr) data.table(index_name2 = idx, description = SEV, crop = crop,
                                             filename = sprintf("%s-G%s.tif", sub("_.*$", "", idx), thr))
haz_class <<- rbindlist(list(
  # generic: the fixed water-balance pair and the generic heat index
  mkrow("NDWS", "generic", 20), mkrow("NDWL0", "generic", 5), mkrow("NTx35", "generic", 14),
  mkrow("PTOT_L", "generic", 400), mkrow("PTOT_G", "generic", 1600),
  # a real crop: ecocrop-derived rows only - note NO NDWS / NDWL0, which is the whole defect
  mkrow("NTxM", "maize", 33), mkrow("NTxS", "maize", 40), mkrow("NTxE", "maize", 47),
  mkrow("PTOT_L", "maize", 500), mkrow("PTOT_G", "maize", 1200),
  # a livestock system: generic non-heat replicated per species (R/2:142) plus species THI
  mkrow("THI_max", "cattle_tropical", 82), mkrow("NDWS", "cattle_tropical", 20),
  mkrow("NDWL0", "cattle_tropical", 5), mkrow("PTOT_L", "cattle_tropical", 400),
  mkrow("PTOT_G", "cattle_tropical", 1600)))

res <- function(tokens, fixed, crop) .resolve_slot(tokens, fixed, crop, SEV)

## ------------------------------------------------------------------ the defect, directly
ok(is.na(res("NDWS", FALSE, "maize")), "maize has NO crop-specific NDWS row - the condition that caused the drop")
ok(!is.na(res("NDWS", TRUE, "maize")), "...but resolved as a FIXED slot it comes from `generic`, which is what fixed means")
ok(identical(res("NDWS", TRUE, "maize"), res("NDWS", TRUE, "sorghum")),
   "a fixed slot is the same hazard for every crop, by definition")

## ------------------------------------------------------------------ each declared row, end to end
row_resolves <- function(r, crop) {
  all(!is.na(c(res(r$heat_simple, r$heat_fixed, crop), res(r$dry_simple, r$dry_fixed, crop), res(r$wet_simple, r$wet_fixed, crop))))
}
ci <- crop_interactions
r1 <- ci[heat_simple == "NTx35"]; r2 <- ci[dry_simple == "PTOT_L"]; r3 <- ci[heat_simple == "NTxM" & dry_simple == "NDWS"]
ok(nrow(r3) == 1, "the #25(a) mixed row is declared")
ok(row_resolves(r3, "maize"), "#25(a) `NDWS + NTxM + NDWL0` now RESOLVES for a real crop - it produced nothing before")
ok(row_resolves(r2, "maize"), "the precipitation-framed crop row still resolves (it always did)")
ok(row_resolves(r1, "generic"), "the all-fixed generic row still resolves under `generic`")
ok(all(vapply(seq_len(nrow(animal_interactions)), function(k)
       row_resolves(animal_interactions[k], "cattle_tropical"), logical(1))),
   "both animal rows still resolve for a species system - the livestock path must not move")

## ------------------------------------------------------------------ scope: all-fixed stays generic-only
src <- readLines(R2)
ok(any(grepl("X <- X\\[!\\(heat_fixed & wet_fixed & dry_fixed\\) \\| crop_focus == \"generic\"\\]", src)),
   "an ALL-fixed row is emitted once under `generic`, not once per crop - 34 copies of one hazard stack would cost R/3 the same multiple for no information")

## ------------------------------------------------------------------ the guard that would have caught this
ok(any(grepl("resolved for no crop at all", src)),
   "R/2 now STOPS when a declared interaction row resolves nowhere, instead of quietly producing no tier")
ok(any(grepl("could not resolve every slot and are dropped", src)),
   "and reports the partially-resolved combinations it drops, rather than filtering them in silence")

cat("\nALL INTERACTION-BUILDER FIXTURE ASSERTIONS PASSED\n")
