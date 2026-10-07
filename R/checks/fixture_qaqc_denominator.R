#!/usr/bin/env Rscript
# R/checks/fixture_qaqc_denominator.R
# ===================================
# B5 A4.1, found on the node 2026-10-07. The crop QAQC read a median ratio of 1.17 against a product
# that was independently verified correct (gridded total / 0.4.0's allocated value = 0.996). The
# product was right; the GATE's denominator was short by ~20 %.
#
# Cause: `fao_gpv_i()` melted the per-item FAO table to (iso3, atlas_name, year) and took
# `median(gpv_i_k)` over all of it. The crop map is MANY items to one atlas_name - `vege` carries 26
# FAO items, `rest` 21 - so for a composite group that is the median of N items x 5 years, i.e. one
# typical item standing in for the whole group. ZAF `temf`: 11 items summing to 2.70 B, median item
# 0.032 B. 0.4.0 instead medians each item's year window and then SUMS the items into the group.
#
# It only surfaced now because before the B5 code join the name join matched 1-5 items per composite,
# so the collapse cost ~3 % and the gate read 1.03. Recovering 57 renamed codes put 11-26 items in
# each composite and the same defect read as a 17 % product error. A gate that fails a correct run -
# the same family as the G6 lesson, and the reason AGENTS.md puts that failure mode first.
#
# Asserted here because a gate that is wrong is worse than no gate: it reads as assurance.
#
# Usage: Rscript R/checks/fixture_qaqc_denominator.R
suppressPackageStartupMessages({ library(data.table) })
root <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else {
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd()
}
source(file.path(root, "R", "vop_allocate.R"))
ok <- function(cond, msg) { if (!isTRUE(cond)) stop("FIXTURE FAIL: ", msg) else cat("  ok  ", msg, "\n") }

## A composite group, shaped like `temf`: one big item and several small ones, over a 5-year window.
## Values are deliberately uneven across years so that median-over-years is not just the mean.
YRS <- paste0("Y", 2019:2023)
row <- function(code, group, base) as.data.table(c(
  list(iso3 = "ZAF", item_code = code, atlas_name = group),
  as.list(setNames(base * c(0.9, 1.0, 1.1, 1.0, 1.2), YRS))))
items <- rbindlist(list(
  row(101L, "temf", 2000), row(102L, "temf", 40),
  row(103L, "temf", 30),   row(104L, "temf", 20),
  row(201L, "maiz", 5000)))

# what 0.4.0 does: median across the window PER ITEM, then sum the items into the group
per_item_median <- items[, .(v = median(unlist(.SD), na.rm = TRUE)), by = .(iso3, item_code, atlas_name), .SDcols = YRS]
truth <- per_item_median[, .(gpv = sum(v)), by = .(iso3, atlas_name)]

# the old gate behaviour: median over every (item, year) cell
melted <- melt(items, id.vars = c("iso3", "item_code", "atlas_name"), measure.vars = YRS, value.name = "g")
old <- melted[, .(gpv = median(g, na.rm = TRUE)), by = .(iso3, atlas_name)]

j <- merge(truth, old, by = c("iso3", "atlas_name"), suffixes = c("_truth", "_old"))
print(j)

ok(j[atlas_name == "maiz", abs(gpv_truth - gpv_old) < 1e-9],
   "a SINGLE-item group is identical either way - which is why livestock, mapped 1:1, never showed this")
ok(j[atlas_name == "temf", gpv_old] < j[atlas_name == "temf", gpv_truth] / 10,
   "a COMPOSITE group collapses to one typical item under the old median: here under a tenth of the true sum")
ok(j[atlas_name == "temf", abs(gpv_truth - 2090) < 1], "the correct denominator is the sum of the per-item medians (2000 + 40 + 30 + 20)")

## the live gate must now use the 0.4.0 order, and must not have kept the old one for crops
q <- readLines(file.path(root, "R", "qaqc_vop_vs_faostat.R"))
ok(any(grepl('collapse = "item_median_then_sum"', q)), "the crop denominator asks for the per-item median then sum")
ok(any(grepl('fao_gpv_i\\(spam_map, by = "code", collapse = "item_median_then_sum", pins =', q)),
   "and the crop call passes both the collapse rule and the quantity pins")
ok(any(grepl('collapse = c\\("median", "item_median_then_sum"\\)', q)),
   "the default stays \"median\", so the 1:1 livestock path is untouched")

## the pin half: 0.4.0 pins the GPV it allocates, so the gate's reference must pin it the same way,
## or a pinned pair reads low by construction (CAF coffee read 0.657 against its own pinned basis)
gpv  <- data.table(iso3 = c("CAF", "ZAF"), item_code = c(656L, 101L), value = c(1760, 2000))
prod <- data.table(iso3 = c("CAF", "ZAF"), item_code = c(656L, 101L), prod_t = c(298000, 900))
pins <- data.table(iso3 = "CAF", item_code = 656L, prod_t = 8512, scale_gpv = TRUE, status = "applied")
res <- vop_apply_quantity_pins(gpv, prod, pins)
ok(nrow(res$log) == 1 && isTRUE(res$log$matched) && is.finite(res$log$ratio),
   "the pin resolves a finite ratio when the production table is supplied")
ok(abs(res$gpv[iso3 == "CAF", value] - 1760 * 8512 / 298000) < 1e-6,
   "and scales the denominator by prod_pinned / prod_FAO, exactly as 0.4.0 scales what it allocates")
ok(res$gpv[iso3 == "ZAF", value] == 2000, "an unpinned item is untouched")

## the trap that made the first attempt a no-op: no production table means no ratio
res0 <- vop_apply_quantity_pins(gpv, NULL, pins)
ok(nrow(res0$log) == 1 && !is.finite(res0$log$ratio) && res0$gpv[iso3 == "CAF", value] == 1760,
   "WITHOUT the production table the ratio is NA and the GPV is unchanged - a silent no-op, which the gate now refuses")
ok(any(grepl("a quantity pin produced no ratio", q)),
   "and the gate stops on exactly that rather than reporting an unpinned denominator as if it were pinned")

cat("\nALL QAQC-DENOMINATOR FIXTURE ASSERTIONS PASSED\n")
