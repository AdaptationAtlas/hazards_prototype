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
#   wired    every terra::resample(..., method = "sum") in R/0.4.0, 0.4.1, 0.4.4 and haz_functions.R
#            is followed by mass_check() on the next line, and the old global-mass warnings are gone
#   terra    a real terra::resample(method = "sum") of a masked source onto a coarser, partial grid
#            passes on this host (the ExtractHaz / hpop shape), and .mass_check_fn() loads
#            mass_check() without putting it in the caller's environment
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

## the real scripts: every sum-resample is checked on the next line
wired <- list(
  list(f = "0.4.0_create_crop_vop_intld15.R",  n = 1, call = "^\\s*mass_check\\(",                src = TRUE),
  list(f = "0.4.1_create_livestock_exposure.R", n = 4, call = "^\\s*mass_check\\(",                src = TRUE),
  list(f = "0.4.4_process_exposure.R",          n = 3, call = "^\\s*mass_check\\(",                src = TRUE),
  list(f = "haz_functions.R",                   n = 3, call = "^\\s*\\.mass_check_fn\\(\\)\\(", src = FALSE))
for (w in wired) {
  code <- sub("#.*$", "", readLines(file.path(root, "R", w$f)))
  hits <- grep('method *= *"sum"', code)
  ok(length(hits) == w$n, sprintf("%s has the expected %d sum-resample(s) (found %d)", w$f, w$n, length(hits)))
  ok(all(grepl(w$call, code[hits + 1])), sprintf("%s: each is followed on the next line by the mass check", w$f))
  if (w$src) ok(any(grepl('source\\(.*"mass_check.R"', code)), sprintf("%s sources R/mass_check.R", w$f))
  ok(!any(grepl("mass not conserved on", code)), sprintf("%s: the old global-mass warnings are gone", w$f))
}

## a real terra sum-resample: masked source onto a coarser grid over part of its extent
m <- rast(nrows = 600, ncols = 600, xmin = 0, xmax = 30, ymin = -15, ymax = 15)   # 0.05 deg
values(m) <- rexp(ncell(m), 1 / 20)
m[sample(ncell(m), ncell(m) * 0.4)] <- NA
zone <- rast(nrows = 80, ncols = 80, xmin = 5, xmax = 25, ymin = -10, ymax = 10)  # 0.25 deg, partial
mr <- resample(m, zone, method = "sum")
rr <- suppressMessages(mass_check(m, mr, "fx-terra"))
ok(abs(rr - 1) < 0.005, sprintf("terra::resample(method = 'sum') on GDAL %s conserves mass onto a partial grid (ratio %.6f)", terra::gdal(), rr))

## .mass_check_fn() from haz_functions.R, in a session where mass_check() is not defined
e <- new.env()
# haz_functions.R has a non-ASCII identifier (mem_\u0394), which a C locale cannot parse
if (!isTRUE(l10n_info()$`UTF-8`)) for (loc in c("C.UTF-8", "en_US.UTF-8")) if (nzchar(suppressWarnings(Sys.setlocale("LC_CTYPE", loc)))) break
source(file.path(root, "R", "haz_functions.R"), local = e, encoding = "UTF-8")
Sys.setenv(project_dir = root)
fn <- local(.mass_check_fn(), envir = e)
ok(is.function(fn) && !exists("mass_check", envir = e, inherits = FALSE), ".mass_check_fn() loads mass_check() without defining it in the caller's environment")
ok(!is.na(err(fn(src, dst * 2, "fx-fn"))), "the function it returns stops on a doubled destination")

cat("\nALL MASS-CHECK FIXTURE ASSERTIONS PASSED\n")
