#!/usr/bin/env Rscript
# R/checks/fixture_crop_heat_interactions.R
# =========================================
# #25, decided into the #13 rebake on 2026-10-07. Off-node fixture for the crop heat wiring in
# R/2_calculate_haz_freq.R §0.2.2.1 / §0.2.4. Synthetic, seconds, no data.
#
# What was wrong. §0.2.2.1 derives THREE crop heat index families from ecocrop and classifies all
# three, but `crop_interactions` names only one of them, and only families named there reach a
# compound `_int` stack - which is the only thing published. The one named was NTxS,
# ceil((Temp_Opt_Max + Temp_Abs_Max)/2): for maize that averages the optimum (33 C) with the
# SURVIVAL limit (47 C) and gets 40 C. Nowhere in Kenya that grows maize sees 14 days above 40 C,
# so the crop-specific pathway reported 0.0 % heat-exposed maize value in both historic and SSP585.
# 15 of 35 crops had a crop-specific "severe" threshold hotter than the generic NTx35.
#
# Three changes, asserted here because they move published values and the bake is a day per stage:
#   (b) the crop heat family is NTxM (= Temp_Opt_Max) rather than NTxS
#   (a) a third row gives crops crop-specific heat with the WATER-BALANCE framing, which only
#       livestock had; previously switching heat definition forced switching drought definition too
#   (c) a commodity with no ecocrop match is reported loudly instead of scrolling past
#
# Usage: Rscript R/checks/fixture_crop_heat_interactions.R
suppressPackageStartupMessages({ library(data.table) })
root <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else {
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd()
}
ok <- function(cond, msg) { if (!isTRUE(cond)) stop("FIXTURE FAIL: ", msg) else cat("  ok  ", msg, "\n") }
R2 <- file.path(root, "R", "2_calculate_haz_freq.R")

## Lift the two interaction tables out of R/2 without running it (its top level needs setup and the
## live Data/ tree). Everything asserted below is evaluated from the real file.
for (e in parse(R2)) {
  if (is.call(e) && length(e) >= 3 && as.character(e[[1]])[1] %in% c("<-", "=") &&
      is.name(e[[2]]) && as.character(e[[2]]) %in% c("crop_interactions", "animal_interactions")) {
    eval(e, envir = globalenv())
  }
}
ok(exists("crop_interactions") && exists("animal_interactions"), "both interaction tables lifted out of R/2_calculate_haz_freq.R")
print(crop_interactions)

## ------------------------------------------------------------------ (b) the heat family
ok(!("NTxS" %in% crop_interactions$heat_simple),
   "(b) NTxS is no longer wired: the opt/absolute-max midpoint is a survival limit, not a yield threshold")
ok("NTxM" %in% crop_interactions$heat_simple,
   "(b) NTxM (= Temp_Opt_Max) is the crop-specific heat family")
ok("NTx35" %in% crop_interactions$heat_simple,
   "the generic NTx35 set is untouched - the live V1 notebook reads it")

## ------------------------------------------------------------------ (a) the framing matrix
key <- function(d) paste(d$dry_simple, d$heat_simple, d$wet_simple, sep = "+")
ck <- key(crop_interactions)
ok(nrow(crop_interactions) == 3, "(a) crops now have three interaction rows, not two")
ok("NDWS+NTx35+NDWL0" %in% ck, "generic heat with the water-balance framing (unchanged, set 1)")
ok("PTOT_L+NTxM+PTOT_G" %in% ck, "crop-specific heat with the precipitation framing (set 2, heat family moved)")
ok("NDWS+NTxM+NDWL0" %in% ck, "(a) crop-specific heat with the WATER-BALANCE framing - the row crops were missing")
# the point of (a): generic vs crop-specific heat must now be a ONE-variable comparison
.wb <- crop_interactions[dry_simple == "NDWS" & wet_simple == "NDWL0"]
ok(nrow(.wb) == 2 && setequal(.wb$heat_simple, c("NTx35", "NTxM")),
   "(a) two sets now differ ONLY in the heat index, so heat can be compared without also swapping drought")
# livestock already had this; crops now match it
ak <- key(animal_interactions)
ok(length(unique(animal_interactions$heat_simple)) == 1 && nrow(animal_interactions) == 2,
   "livestock already had both framings with species-specific heat - this is the matrix crops now have")
ok(all(c("NDWS+THI_max+NDWL0", "PTOT_L+THI_max+PTOT_G") %in% ak), "animal interactions are unchanged")

## ------------------------------------------------------------------ the fixed/free guard
# R/2 stops if any hazard appears as both fixed (shared across crops) and free (crop-specific),
# because the simplified names would collide across different thresholds. The new row keeps NDWS and
# NDWL0 fixed and NTxM free, so it must pass - assert that rather than discover it an hour into a run.
fixed <- unique(c(crop_interactions[heat_fixed == TRUE, heat_simple], crop_interactions[wet_fixed == TRUE, wet_simple],
                  crop_interactions[dry_fixed == TRUE, dry_simple], animal_interactions[heat_fixed == TRUE, heat_simple],
                  animal_interactions[wet_fixed == TRUE, wet_simple], animal_interactions[dry_fixed == TRUE, dry_simple]))
free <- unique(c(crop_interactions[heat_fixed == FALSE, heat_simple], crop_interactions[wet_fixed == FALSE, wet_simple],
                 crop_interactions[dry_fixed == FALSE, dry_simple], animal_interactions[heat_fixed == FALSE, heat_simple],
                 animal_interactions[wet_fixed == FALSE, wet_simple], animal_interactions[dry_fixed == FALSE, dry_simple]))
cat("  fixed:", paste(sort(fixed), collapse = ","), "| free:", paste(sort(free), collapse = ","), "\n")
ok(!any(fixed %in% free), "the fixed-vs-free guard passes with the new row (it would stop() an hour into R/2 otherwise)")
ok(all(c("NDWS", "NDWL0") %in% fixed) && "NTxM" %in% free,
   "and for the right reason: the water-balance pair stays fixed while the crop heat index stays free")

## ------------------------------------------------------------------ published hazard_vars labels
# The published `hazard_vars` value is built from these tokens (combo_name_simple2 in R/2), with "_"
# replaced by "-". Changing the heat family therefore RENAMES the crop-specific set rather than
# silently changing its meaning under the old label - a provenance label is a claim.
pub <- gsub("_", "-", ck)
ok(setequal(pub, c("NDWS+NTx35+NDWL0", "PTOT-L+NTxM+PTOT-G", "NDWS+NTxM+NDWL0")),
   "the published hazard_vars set is {generic/water-balance, crop-specific/precip, crop-specific/water-balance}")
ok(!("PTOT-L+NTxS+PTOT-G" %in% pub),
   "the old crop-specific label is RETIRED, not reused for a different definition - consumers see the change")

## ------------------------------------------------------------------ (c) the no-match report
src <- readLines(R2)
ok(any(grepl("\\.ec_no_match <- NULL", src)) && any(grepl("\\.ec_no_match <<- rbind", src)),
   "(c) ecocrop misses are collected rather than printed and forgotten")
ok(any(grepl("NO ecocrop match", src)) && !any(grepl('print\\(paste0\\(i, "-", j, " \\| ", crop, " - ERROR NO MATCH"\\)\\)', src)),
   "(c) and reported once, in full, after the loop - the scrolling print() is gone")

cat("\nALL CROP-HEAT INTERACTION FIXTURE ASSERTIONS PASSED\n")
