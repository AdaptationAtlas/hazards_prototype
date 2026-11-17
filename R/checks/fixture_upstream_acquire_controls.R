#!/usr/bin/env Rscript
# R/checks/fixture_upstream_acquire_controls.R
# ============================================
# Issue #58 phase C: the two canonical NEX-GDDP acquisition scripts in hazards_upstream now honour the
# 00_setup.R run controls (GCMS / SCENARIO / SSPS / YRS / VARS) and DRY_RUN, so one GCM-year can be
# fetched and converted on a new host without editing the scripts.
#   cfg_vars     default = the six NEX variables; VARS=pr,tasmax honoured; an unknown name stops
#   download     DRY_RUN=1 with a seeded, correctly scoped manifest lists the files and exits 0 with
#                nothing fetched; a manifest that does not cover the scope stops; the manifest file
#                name carries the scope so a slice never reuses another slice's probe
#   preprocess   the REAL script converts a synthetic 0-360 .nc for one GCM into daily tifs: file names
#                from the .nc time axis, pr x 86400, tas - 273.15, rsds x 86400/1e6, hurs untouched,
#                extent rotated to -180..180; a second run writes nothing (skip-if-present); VARS and
#                GCMS scope what is converted
#   static       no 18-GCM literal, no furrr/future, no xts/tsbox in either script
# Needs terra + ncdf4 (writeCDF). ~30 s. No network.
# Usage: Rscript R/checks/fixture_upstream_acquire_controls.R
suppressPackageStartupMessages(library(terra))
root <- if (nzchar(Sys.getenv("ATLAS_PROJECT_DIR"))) Sys.getenv("ATLAS_PROJECT_DIR") else {
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd()
}
ok  <- function(cond, msg) { if (!isTRUE(cond)) stop("FIXTURE FAIL: ", msg) else cat("  ok  ", msg, "\n") }
err <- function(expr) inherits(try(expr, silent = TRUE), "try-error")
UP  <- file.path(root, "hazards_upstream", "R")
DL  <- file.path(UP, "01_download_data", "download_manual_nex_gddpCMIP6.R")
PP  <- file.path(UP, "02_preprocess_data", "preprocess_nex-gddp-cmip6_daily_data.R")
for (v in c("GCMS", "SCENARIO", "SSPS", "YRS", "VARS", "DRY_RUN", "FORCE_OVERWRITE")) Sys.unsetenv(v)
withenv <- function(vars, expr) {
  old <- vapply(names(vars), function(n) Sys.getenv(n, unset = NA), character(1))
  do.call(Sys.setenv, as.list(vars))
  on.exit(for (n in names(old)) if (is.na(old[[n]])) Sys.unsetenv(n) else do.call(Sys.setenv, setNames(list(old[[n]]), n)))
  force(expr)
}
run <- function(script, env) {
  st <- suppressWarnings(system2("Rscript", script, env = paste0(names(env), "=", env), stdout = TRUE, stderr = TRUE))
  list(status = if (is.null(attr(st, "status"))) 0L else attr(st, "status"), log = st)
}

## ---- A. cfg_vars ---------------------------------------------------------------------------------
cat("A. cfg_vars (00_setup.R)\n")
e <- new.env()
withenv(c(COMMON_DATA = tempdir()), sys.source(file.path(UP, "00_setup.R"), envir = e))
ok(identical(e$cfg_vars(), c("pr", "tasmax", "tasmin", "hurs", "rsds", "sfcWind")), "default = the six NEX variables, legacy order")
ok(identical(withenv(c(VARS = "pr, tasmax"), e$cfg_vars()), c("pr", "tasmax")), "VARS=pr,tasmax honoured (whitespace tolerated)")
ok(err(withenv(c(VARS = "pr,tmax"), e$cfg_vars())), "an unknown variable name stops")

## ---- B. download script under DRY_RUN -------------------------------------------------------------
cat("B. download_manual_nex_gddpCMIP6.R (DRY_RUN, seeded manifest)\n")
CD <- file.path(tempdir(), "fx_phaseC"); unlink(CD, recursive = TRUE); dir.create(CD)
wd <- file.path(CD, "nex-gddp-cmip6_raw"); dir.create(wd)
man <- function(gcm, vars, yrs) {
  g <- expand.grid(gcm = gcm, ssp = "historical", var = vars, yr = yrs, stringsAsFactors = FALSE)
  g$pth_dir <- sprintf("https://example.invalid/NEX-GDDP-CMIP6/%s/historical/r1i1p1f1/%s", g$gcm, g$var)
  g$file <- sprintf("%s_day_%s_historical_r1i1p1f1_gn_%d_v1.1.nc", g$var, g$gcm, g$yr)
  g
}
# the scoped manifest name the script derives for GCMS=FXGCM VARS=pr,tasmax YRS=1995
mf <- file.path(wd, "cmip6_short-baseline_files_to_download_1gcm_2var_1995-1995.csv")
write.csv(man("FXGCM", c("pr", "tasmax"), 1995), mf, row.names = FALSE)
r <- run(DL, c(COMMON_DATA = CD, GCMS = "FXGCM", VARS = "pr,tasmax", YRS = "1995", DRY_RUN = "1"))
ok(r$status == 0 && any(grepl("DRY_RUN: would fetch 2 file", r$log)), "DRY_RUN with a matching scoped manifest: lists 2 files, exit 0")
ok(any(grepl("scoped by env", r$log)) && any(grepl("pr_day_FXGCM", r$log)) && any(grepl("tasmax_day_FXGCM", r$log)), "the plan names both files and says the run is scoped")
ok(!length(list.files(wd, "\\.nc$", recursive = TRUE)), "nothing was downloaded")
write.csv(man("FXGCM", "pr", 1995), mf, row.names = FALSE)              # stale: one var short
r <- run(DL, c(COMMON_DATA = CD, GCMS = "FXGCM", VARS = "pr,tasmax", YRS = "1995", DRY_RUN = "1"))
ok(r$status != 0 && any(grepl("holds 1 row\\(s\\) for this scope but 2 are expected", r$log)), "a manifest that does not cover the scope stops (no silent partial fetch)")
r <- run(DL, c(COMMON_DATA = CD, GCMS = "FXGCM", VARS = "pr,snow", YRS = "1995", DRY_RUN = "1"))
ok(r$status != 0 && any(grepl("VARS: unknown variable", r$log)), "VARS with an unknown name stops before anything else")

## ---- C. preprocess script, end to end on a synthetic .nc ---------------------------------------
cat("C. preprocess_nex-gddp-cmip6_daily_data.R (real script, synthetic .nc)\n")
days <- as.Date("1995-01-01") + 0:4
mk_nc <- function(var, scale) {
  r <- rast(nrows = 4, ncols = 8, xmin = 0, xmax = 360, ymin = -90, ymax = 90, nlyrs = length(days))
  values(r) <- rep(scale * seq_along(days), each = 32)        # layer i holds scale*i everywhere
  time(r) <- days
  d <- file.path(wd, var, "historical", "FXGCM"); dir.create(d, FALSE, TRUE)
  writeCDF(r, file.path(d, sprintf("%s_day_FXGCM_historical_r1i1p1f1_gn_1995_v1.1.nc", var)), varname = var, overwrite = TRUE)
}
mk_nc("pr", 1e-5); mk_nc("tasmax", 300); mk_nc("rsds", 100); mk_nc("hurs", 50)
mk_nc("tasmin", 290)                                          # present on disk, excluded by VARS below
out <- function(var) file.path(CD, "nex-gddp-cmip6", var, "historical", "FXGCM", paste0(var, "_", days, ".tif"))
r <- run(PP, c(COMMON_DATA = CD, GCMS = "FXGCM", VARS = "pr,tasmax,rsds,hurs"))
ok(r$status == 0, paste("preprocess exits 0", if (r$status != 0) paste(tail(r$log, 5), collapse = " | ") else ""))
ok(all(file.exists(out("pr"))) && all(file.exists(out("tasmax"))) && all(file.exists(out("rsds"))) && all(file.exists(out("hurs"))),
   "5 daily tifs per requested variable, named <var>_<date>.tif from the .nc time axis")
ok(!any(file.exists(out("tasmin"))), "tasmin, on disk but outside VARS, is not converted")
v <- function(var, i) values(rast(out(var)[i]))[1, 1]
near <- function(a, b) { r <- isTRUE(all.equal(as.numeric(a), as.numeric(b), tolerance = 1e-5)); if (!r) cat("    got", format(a, digits = 10), "expected", format(b, digits = 10), "\n"); r }   # .nc stores float32
ok(near(v("pr", 3), 3e-5 * 86400), "pr: kg m-2 s-1 x 86400 -> mm/day")
ok(near(v("tasmax", 2), 600 - 273.15), "tasmax: K - 273.15 -> degC")
ok(near(v("rsds", 4), 400 * 86400 / 1e6), "rsds: W m-2 x 86400/1e6 -> MJ m-2 day-1")
ok(near(v("hurs", 5), 250), "hurs passes through unchanged")
ok(isTRUE(all.equal(as.numeric(as.vector(ext(rast(out("pr")[1])))), c(-180, 180, -90, 90))), "0..360 longitudes rotated to -180..180")
ok(any(grepl("5 day\\(s\\), 5 written, 0 present", r$log)), "per-file log line carries day count, written, present and seconds")
m <- file.mtime(out("pr")[1])
r <- run(PP, c(COMMON_DATA = CD, GCMS = "FXGCM", VARS = "pr"))
ok(r$status == 0 && file.mtime(out("pr")[1]) == m && any(grepl("0 written, 5 present", r$log)), "second run writes nothing (skip-if-present) and says so")
r <- run(PP, c(COMMON_DATA = CD, GCMS = "OTHER", VARS = "pr"))
ok(r$status == 0 && !dir.exists(file.path(CD, "nex-gddp-cmip6", "pr", "historical", "OTHER", "x")), "a GCM with no raw files converts nothing and does not fail")

## ---- D. static -----------------------------------------------------------------------------------
cat("D. static\n")
for (f in c(DL, PP)) {
  code <- sub("#.*$", "", readLines(f, warn = FALSE))
  ok(!any(grepl("'ACCESS-CM2','ACCESS-ESM1-5','CanESM5'", code)), paste(basename(f), ": no 18-GCM literal"))
  ok(any(grepl("cfg_gcms\\(\\)", code)) && any(grepl("cfg_vars\\(\\)", code)) && any(grepl("cfg_scenario\\(", code)),
     paste(basename(f), ": uses cfg_gcms / cfg_vars / cfg_scenario"))
}
ppc <- sub("#.*$", "", readLines(PP, warn = FALSE))
ok(!any(grepl("furrr|future_map|tsbox|xts", ppc)), "preprocess: no furrr/future_map (silent-zero-output path) and no unused xts/tsbox")
dlc <- sub("#.*$", "", readLines(DL, warn = FALSE))
ok(any(grepl("env_flag\\('DRY_RUN'", dlc)), "download: DRY_RUN gate present")

unlink(CD, recursive = TRUE)
cat("\nALL UPSTREAM ACQUIRE-CONTROLS FIXTURE ASSERTIONS PASSED\n")
