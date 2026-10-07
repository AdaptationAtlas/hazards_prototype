#!/usr/bin/env Rscript
# R/checks/fixture_fao_code_join.R
# ================================
# B5 (2026-10-07). Synthetic replay of the FAOSTAT rename through prepare_fao_data(), the helper
# the QAQC denominator is built from. No data needed, seconds. Fails loudly on any assertion.
#
# The bug: `metadata/SPAM2010_FAO_crops.csv` records the FAO item name as of when it was written.
# FAOSTAT renames items between releases ("Vegetables fresh nes" -> "Other vegetables, fresh
# n.e.c."), the name no longer matched, and the item's value left the table in silence - a missing
# item is indistinguishable from an item the country does not grow, so nothing could catch it.
# 59 of the 127 FAO codes in the composite groups had moved: about 8 % of all-crop FAO GPV.
#
# Why this fixture, and not just the 0.4.0 one: 0.4.0 allocates the value, but
# `R/qaqc_vop_vs_faostat.R` builds the FAO denominator that JUDGES the allocation, through
# prepare_fao_data(). If only the producer moved to codes, the denominator would stay short by the
# same 8 % and a correct re-bake would read as a crop over-allocation - a gate failing a good run,
# the failure mode AGENTS.md calls out. Both sides key on the code; this proves both behaviours.
#
# Usage: Rscript R/checks/fixture_fao_code_join.R
suppressPackageStartupMessages({ library(data.table); library(countrycode) })
root <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else {
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd()
}
source(file.path(root, "R", "haz_functions.R"))
ok <- function(cond, msg) { if (!isTRUE(cond)) stop("FIXTURE FAIL: ", msg) else cat("  ok  ", msg, "\n") }

## A FAOSTAT-shaped bulk file. Kenya (M49 404) and Nigeria (566). Three items:
##   56  maize      - FAOSTAT now says "Maize (corn)", the mapping table agrees      -> matches either way
##   358 cabbages   - FAOSTAT now says "Cabbages", the mapping table says "Cabbages and other
##                    brassicas" (the shape of the real rename)                      -> name join DROPS it
##   463 veg nes    - FAOSTAT now says "Other vegetables, fresh n.e.c.", the mapping table says
##                    "Vegetables fresh nes" (the real #B5 example)                  -> name join DROPS it
fao <- data.table(
  `Area Code (M49)` = c("'404", "'404", "'404", "'566", "'566", "'566"),
  Area              = rep(c("Kenya", "Nigeria"), each = 3),
  `Item Code`       = rep(c(56, 358, 463), 2),
  Item              = rep(c("Maize (corn)", "Cabbages", "Other vegetables, fresh n.e.c."), 2),
  Element           = "Gross Production Value (constant 2014-2016 thousand I$)",
  Y2021             = c(1000, 200, 300, 2000, 400, 600))
csv <- file.path(tempdir(), "fixture_fao_bulk.csv")
fwrite(fao, csv)

## the mapping table's spellings: two of the three are stale
map_name <- c(maiz = "Maize (corn)", vege = "Cabbages and other brassicas", vege2 = "Vegetables fresh nes")
map_code <- c(maiz = 56L,            vege = 358L,                          vege2 = 463L)

args <- list(file = csv, elements = "Gross Production Value (constant 2014-2016 thousand I$)",
             remove_countries = character(0), keep_years = 2021, atlas_iso3 = c("KEN", "NGA"))

by_name <- do.call(prepare_fao_data, c(args, list(lps2fao = map_name)))
by_code <- do.call(prepare_fao_data, c(args, list(lps2fao = map_code, by = "code")))
tot <- function(d) sum(d$Y2021, na.rm = TRUE)
cat("\nname join:\n"); print(by_name[order(iso3, atlas_name)])
cat("\ncode join:\n"); print(by_code[order(iso3, atlas_name)])

ok(identical(sort(unique(by_name$atlas_name)), "maiz"),
   "name join: only the item whose FAOSTAT name still matches the mapping table survives")
ok(identical(sort(unique(by_code$atlas_name)), c("maiz", "vege", "vege2")),
   "code join: all three items match, whatever FAOSTAT currently calls them")
ok(tot(by_name) == 3000 && tot(by_code) == 4500,
   "code join recovers the renamed items' value (3000 -> 4500 thousand I$ here; 53.6 B I$ on the real file)")
ok(abs(tot(by_code) / tot(by_name) - 1.5) < 1e-9,
   "the gap is a silent under-count, not an error: the name join returned a complete-looking table")

## the default must stay "name" - every livestock caller in 0.4.1 keys on species names
by_default <- do.call(prepare_fao_data, c(args, list(lps2fao = map_name)))
ok(identical(by_default, by_name), "by = 'name' is the default, so the livestock callers are untouched")

## a file with no item-code column must say so, not silently return nothing
nocode <- copy(fao); nocode[, `Item Code` := NULL]
csv2 <- file.path(tempdir(), "fixture_fao_bulk_nocode.csv"); fwrite(nocode, csv2)
e <- tryCatch({ do.call(prepare_fao_data, c(modifyList(args, list(file = csv2)), list(lps2fao = map_code, by = "code"))); NULL },
              error = function(e) conditionMessage(e))
ok(!is.null(e) && grepl("item-code column", e), "a FAOSTAT file with no item-code column aborts with a named reason")

cat("\nALL FAO CODE-JOIN FIXTURE ASSERTIONS PASSED\n")
