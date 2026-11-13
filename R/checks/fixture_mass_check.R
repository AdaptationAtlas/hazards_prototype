#!/usr/bin/env Rscript
# R/checks/fixture_mass_check.R
# =============================
# Issue #58: R/mass_check.R stops a sum-resample that does not conserve mass. No data needed, seconds.
#   pass     a source with a NoData mask, aggregated 3x3 to a 0.25 deg BAND that covers part of the
#            globe, passes; the old global-vs-band comparison would have read 0.95 on a correct host
#   x2 / x1.6  the destination doubled or x1.6 (what GDAL 3.4.3 did on PASCAL) stops, naming the
#            layer and the ratio
#   just over / under  0.6 % off stops at the default 0.5 % tolerance, 0.3 % passes
#   warn     ATLAS_MASS_CHECK=warn turns the stop into a warning
#   empty    a layer with no source mass passes when the destination is empty too, stops when not
#   wired    every terra::resample(..., method = "sum") in R/0.4.1 is followed by mass_check() on the
#            next line, and the old global-mass warnings are gone
# Usage: Rscript R/checks/fixture_mass_check.R
suppressPackageStartupMessages(library(terra))
root <- if (nzchar(Sys.getenv("ATLAS_PROJECT_DIR"))) Sys.getenv("ATLAS_PROJECT_DIR") else { fa <- grep("^--file=", commandArgs(FALSE), value = TRUE); if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd() }
ok <- function(cond, msg) { if (!isTRUE(cond)) stop("FIXTURE FAIL: ", msg) else cat("  ok  ", msg, "\n") }
source(file.path(root, "R", "mass_check.R"))
err <- function(expr) tryCatch({ suppressMessages(expr); NA_character_ }, error = function(e) conditionMessage(e))

## upstream-shaped: GLW-like 1/12 deg global counts, NoData over "ocean", two layers
set.seed(58)
src <- rast(nrows = 2160, ncols = 4320, xmin = -180, xmax = 180, ymin = -90, ymax = 90, nlyrs = 2)
v <- matrix(rexp(ncell(src) * 2, 1 / 50), ncol = 2)
v[sample(ncell(src), ncell(src) * 0.7), ] <- NA            # 70 % NoData, as the GLW land mask
values(src) <- v
names(src) <- c("cattle", "goats")
band <- ext(-180, 180, -50, 50)                             # base_rast_nexgddp's extent
dst <- crop(aggregate(src, fact = 3, fun = "sum", na.rm = TRUE), band)   # the correct answer, no GDAL warper

r <- suppressMessages(mass_check(src, dst, "fx"))
ok(all(abs(r - 1) < 1e-9), sprintf("correct 3x3 aggregate onto a partial band passes (ratios %s)", paste(round(r, 6), collapse = ",")))
old <- global(dst, "sum", na.rm = TRUE)[, 1] / global(src, "sum", na.rm = TRUE)[, 1]
ok(all(old < 0.9), sprintf("the old global-vs-band comparison reads %s on the same correct data, which is why it warned everywhere", paste(round(old, 3), collapse = ",")))

e2 <- err(mass_check(src, dst * 2, "fx-x2"))
ok(!is.na(e2) && grepl("NOT conserved", e2) && grepl("cattle=2.0000", e2) && grepl("GDAL", e2), "x2 destination (GDAL 3.4.3 at 0.25 deg) stops, naming layer, ratio and GDAL version")
ok(!is.na(err(mass_check(src, dst * 1.6, "fx-x1.6"))), "x1.6 destination (GDAL 3.4.3 at 0.05 deg) stops")
ok(!is.na(err(mass_check(src, dst * 1.006, "fx-over"))), "0.6 % over stops at the default 0.5 % tolerance")
ok(is.na(err(mass_check(src, dst * 0.997, "fx-under"))), "0.3 % under passes")

Sys.setenv(ATLAS_MASS_CHECK = "warn")
w <- tryCatch({ suppressMessages(mass_check(src, dst * 2, "fx-warn")); "none" }, warning = function(w) "warned", error = function(e) "stopped")
Sys.unsetenv("ATLAS_MASS_CHECK")
ok(identical(w, "warned"), "ATLAS_MASS_CHECK=warn downgrades the stop to a warning")

src0 <- src; src0[["goats"]] <- 0
dst0 <- crop(aggregate(src0, fact = 3, fun = "sum", na.rm = TRUE), band)
ok(is.na(err(mass_check(src0, dst0, "fx-empty"))), "a layer with no source mass and an empty destination passes")
dst0b <- dst0; dst0b[["goats"]] <- dst0b[["goats"]] + 1
ok(!is.na(err(mass_check(src0, dst0b, "fx-gain"))), "a layer that gains mass from nothing stops")

## the real script: every sum-resample is checked on the next line
s <- readLines(file.path(root, "R", "0.4.1_create_livestock_exposure.R"))
code <- sub("#.*$", "", s)
hits <- grep('method = "sum"', code)
ok(length(hits) == 4, sprintf("0.4.1 has the expected 4 sum-resamples (found %d)", length(hits)))
ok(all(grepl("^\\s*mass_check\\(", code[hits + 1])), "each is followed on the next line by mass_check()")
ok(any(grepl('source\\(.*"mass_check.R"', code)), "0.4.1 sources R/mass_check.R")
ok(!any(grepl("mass not conserved on", code)), "the old global-mass warnings are gone")

cat("\nALL MASS-CHECK FIXTURE ASSERTIONS PASSED\n")
