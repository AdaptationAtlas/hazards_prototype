#!/usr/bin/env Rscript
# R/checks/fixture_gate_054.R
# ===========================
# Runs R/checks/gate_054_reference_diff.R end to end on synthetic parked (old) and rebuilt (new)
# exposure tables plus a synthetic probe CSV, through its real CLI. The gate must PASS on a correct
# #54 + #52 rebuild and FAIL on each of: a non-target row that moved, the TLU rows left in the
# head-count slot, a livestock intld pair that moved without the probe flagging it, and the nominal
# twin changing. A gate that cannot fail is not a gate. Needs arrow. Seconds.
# Usage: Rscript R/checks/fixture_gate_054.R
suppressPackageStartupMessages({ library(data.table); library(arrow) })
root <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else { fa <- grep("^--file=", commandArgs(FALSE), value = TRUE); if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd() }
gate <- file.path(root, "R", "checks", "gate_054_reference_diff.R")
ok <- function(cond, msg) { if (!isTRUE(cond)) stop("FIXTURE FAIL: ", msg) else cat("  ok  ", msg, "\n") }
td <- file.path(tempdir(), paste0("g054_", format(Sys.time(), "%H%M%S"))); dir.create(td)
OLD <- file.path(td, "old"); NEW <- file.path(td, "new"); dir.create(OLD); dir.create(NEW)

## the reference, old shape: 3 countries x 2 admin2, species + TLU under number, two vop units, a crop
sp <- c("cattle", "sheep", "goats", "pigs", "poultry"); sys <- c("highland", "tropical")
base <- CJ(iso3 = c("KEN", "LBY", "SWZ"), admin2_name = c("a", "b"), crop = c(paste0(rep(sp, each = 2), "-", sys), "total-highland", "total-tropical"))
base[, `:=`(admin0_name = iso3, admin1_name = "x", tech = NA_character_, stat = "sum")]
num <- copy(base)[, `:=`(exposure = "number", unit = "number", value = 1000)]
num[grepl("^total-", crop), value := 123]
intld <- copy(base)[!grepl("^total-", crop)][, `:=`(exposure = "vop", unit = "intld15-2021", value = 5e6)]
intld[iso3 == "LBY" & grepl("^cattle-", crop), value := 250]          # the placeholder, split over 2 admin2 x 2 systems = 1,000 national
intld[iso3 == "SWZ" & grepl("^sheep-", crop), value := 0]
nom <- copy(base)[!grepl("^total-", crop)][, `:=`(exposure = "vop", unit = "nominal-usd-2021", value = 6e6)]
crop <- CJ(iso3 = c("KEN", "LBY", "SWZ"), admin2_name = c("a", "b"), crop = "maize")[, `:=`(admin0_name = iso3, admin1_name = "x", tech = "all", stat = "sum", exposure = "prod", unit = "t", value = 777)]
old_ref <- rbind(num, intld, nom, crop, fill = TRUE)[, unit_full := unit]

## probe: LBY cattle and SWZ sheep flagged, with national NEW; KEN untouched
probe <- rbindlist(list(
  data.table(year_set = "y2021", iso3 = "LBY", atlas_name = "cattle_meat", glw3_name = "cattle", intld_old = 1000, intld_new = 48e6, intld_source = "basis fallback"),
  data.table(year_set = "y2021", iso3 = "SWZ", atlas_name = "sheep_meat", glw3_name = "sheep", intld_old = 0, intld_new = 0.8e6, intld_source = "basis fallback"),
  data.table(year_set = "y2021", iso3 = "KEN", atlas_name = "cattle_meat", glw3_name = "cattle", intld_old = 20e6, intld_new = 20e6, intld_source = "fao gpv constant-I$")))
pf <- file.path(td, "probe.csv"); fwrite(probe, pf)

## the correct rebuild: TLU relabelled, LBY cattle intld -> 48 M over 4 rows, SWZ sheep -> 0.8 M over 4 rows, all else identical
new_ref <- copy(old_ref)
new_ref[exposure == "number" & grepl("^total-", crop), `:=`(exposure = "tlu", unit = "tlu", unit_full = "tlu")]
new_ref[exposure == "vop" & unit == "intld15-2021" & iso3 == "LBY" & grepl("^cattle-", crop), value := 48e6 / 4]
new_ref[exposure == "vop" & unit == "intld15-2021" & iso3 == "SWZ" & grepl("^sheep-", crop), value := 0.8e6 / 4]
twin <- function(x, u) x[exposure == "vop" & unit == u]
wr <- function(dir, ref) {
  write_parquet(ref, file.path(dir, "exposure_adm_sum_spam20-20_glw420-20_res-25.parquet"))
  write_parquet(twin(ref, "nominal-usd-2021"), file.path(dir, "vop_nominal-usd-2021_adm_sum_spam20_glw420_res-25.parquet"))
  write_parquet(twin(ref, "intld15-2021"), file.path(dir, "vop_intld15-2021_adm_sum_spam20_glw420_res-25.parquet"))
}
wr(OLD, old_ref)
run <- function(new_dir) { out <- suppressWarnings(system2("Rscript", c(gate, "--old-dir", OLD, "--res", "0.25", "--probe", pf, "--new-dir", new_dir), stdout = TRUE, stderr = TRUE)); list(status = attr(out, "status"), out = out) }
pass <- function(r) is.null(r$status) && any(grepl("GATE 054-REF PASS", r$out))

wr(NEW, new_ref); r <- run(NEW)
if (!pass(r)) cat(r$out, sep = "\n")
ok(pass(r), "a correct #54 + #52 rebuild PASSES (TLU relabelled, two flagged pairs at the probe's NEW level, every other row identical)")

## failure 1: a non-target row moved (KEN maize)
N1 <- file.path(td, "n1"); dir.create(N1); x <- copy(new_ref); x[crop == "maize" & iso3 == "KEN" & admin2_name == "a", value := 778]; wr(N1, x)
ok(!pass(run(N1)), "FAILS when a non-target row (KEN maize prod) moved by one tonne")
## failure 2: TLU rows left under number
N2 <- file.path(td, "n2"); dir.create(N2); x <- copy(new_ref); x[exposure == "tlu", `:=`(exposure = "number", unit = "number", unit_full = "number")]; wr(N2, x)
ok(!pass(run(N2)), "FAILS when the TLU rows are still filed under exposure = number")
## failure 3: a livestock intld pair moved that the probe did not flag (KEN cattle)
N3 <- file.path(td, "n3"); dir.create(N3); x <- copy(new_ref); x[exposure == "vop" & unit == "intld15-2021" & iso3 == "KEN" & grepl("^cattle-", crop), value := 5.1e6]; wr(N3, x)
ok(!pass(run(N3)), "FAILS when an unflagged livestock intld pair (KEN cattle) moved")
## failure 4: a flagged pair lands 5 % off the probe's NEW
N4 <- file.path(td, "n4"); dir.create(N4); x <- copy(new_ref); x[exposure == "vop" & unit == "intld15-2021" & iso3 == "LBY" & grepl("^cattle-", crop), value := 48e6 * 1.05 / 4]; wr(N4, x)
ok(!pass(run(N4)), "FAILS when a flagged pair lands 5 % off the probe's national NEW value (tol 2 %)")
## failure 5: the nominal twin changed
N5 <- file.path(td, "n5"); dir.create(N5); x <- copy(new_ref); x[exposure == "vop" & unit == "nominal-usd-2021" & iso3 == "LBY" & crop == "cattle-tropical" & admin2_name == "a", value := 6e6 + 1]; wr(N5, x)
ok(!pass(run(N5)), "FAILS when the nominal twin moved by one dollar")

unlink(td, recursive = TRUE)
cat("\nALL GATE-054 FIXTURE ASSERTIONS PASSED\n")
