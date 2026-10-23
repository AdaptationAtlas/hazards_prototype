#!/usr/bin/env Rscript
# R/checks/fixture_upstream_run_params.R
# ======================================
# PLAN_obs_hazard_pipeline.md changes (a)-(c) to the upstream index scripts
# (hazards_upstream/R/00_setup.R + 04_indices/*):
#   (a) water-balance seed months were hard-coded c('1995-01','2021-01') -> AVAIL_SEED_MONTHS env
#       (default unchanged). A run starting elsewhere stopped at month 1, and would have silently
#       RESET the soil store at 2021-01 mid-series.
#   (b) calc_NTx.R skipped on bare file.exists, ignoring FORCE_OVERWRITE -> should_skip().
#   (c) every daily reader dropped missing input days silently -> DAYS_CHECK gate:
#         warn (default, legacy-compatible: continue + WARN), calendar (stop on gaps the series'
#         own calendar does not explain, inferred per directory), strict (stop on any gap).
#
# A declaration is not an output (fixture_interaction_builder.R): part B runs the REAL scripts end
# to end on a 4x4 synthetic grid in a temp COMMON_DATA and asserts on what they write. Each check
# pairs an honest input with a corruption that must fail. DEFAULTS MUST REPRODUCE TODAY'S NEX-GDDP
# BEHAVIOUR; the node-side run-equivalence probe (old vs new code on real NEX-GDDP years) is the
# independent check of that, this fixture is the fast one.
#
# Not run end to end here: fast_calc_waterbalance.R (v2, unrun, needs sroot/TAW/elevation inputs) and
# calc_TAI.R (needs the ET_SolRad ESRI-grid climatology + envirem); their seed and gate edits are
# covered by the static checks in C.
#
# Usage: Rscript R/checks/fixture_upstream_run_params.R      (~2-4 min on a laptop)
suppressPackageStartupMessages({ library(terra) })
root <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else {
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd()
}
ok  <- function(cond, msg) { if (!isTRUE(cond)) stop("FIXTURE FAIL: ", msg) else cat("  ok  ", msg, "\n") }
err <- function(expr) inherits(try(expr, silent = TRUE), "try-error")
`%||%` <- function(a, b) if (is.null(a)) b else a
UP  <- file.path(root, "hazards_upstream", "R")
IDX <- file.path(UP, "04_indices")
withenv <- function(vars, expr) {                       # set env vars for one expression, then restore
  old <- vapply(names(vars), function(n) Sys.getenv(n, unset = NA), character(1))
  do.call(Sys.setenv, as.list(vars))
  on.exit(for (n in names(old)) if (is.na(old[[n]])) Sys.unsetenv(n) else do.call(Sys.setenv, setNames(list(old[[n]]), n)))
  force(expr)
}
for (v in c("DAYS_CHECK", "AVAIL_SEED_MONTHS", "FORCE_OVERWRITE")) Sys.unsetenv(v)

## ---- A. helpers, in isolation -------------------------------------------------------------
cat("A. helpers (00_setup.R)\n")
e <- new.env()
withenv(c(COMMON_DATA = tempdir()), sys.source(file.path(UP, "00_setup.R"), envir = e))
ok(identical(e$cfg_avail_seed_months(), c("1995-01", "2021-01")), "seed default unchanged: 1995-01, 2021-01")
ok(identical(withenv(c(AVAIL_SEED_MONTHS = "2005-01"), e$cfg_avail_seed_months()), "2005-01"), "AVAIL_SEED_MONTHS=2005-01 honoured")
ok(err(withenv(c(AVAIL_SEED_MONTHS = "2005-1"), e$cfg_avail_seed_months())), "malformed AVAIL_SEED_MONTHS is rejected")
ok(e$cfg_days_check() == "warn", "DAYS_CHECK default is warn (legacy-compatible)")
ok(err(withenv(c(DAYS_CHECK = "yes"), e$cfg_days_check())), "unknown DAYS_CHECK value is rejected")

# One directory per calendar type. The ROOT path contains a date on purpose: only the file name may be parsed.
base <- file.path(tempdir(), "obs_2026-10-22"); unlink(base, recursive = TRUE)
mkdir <- function(name, dates) { d <- file.path(base, name); dir.create(d, FALSE, TRUE)
  invisible(file.create(file.path(d, paste0("pr_", dates, ".tif")))); d }
yr2  <- seq(as.Date("2007-01-01"), as.Date("2008-12-31"), by = "day")
greg <- mkdir("greg", yr2)                                              # has 31sts and a 29-Feb
c360 <- mkdir("c360", yr2[format(yr2, "%d") != "31"])                   # no 31st anywhere
nolp <- mkdir("nolp", yr2[format(yr2, "%m-%d") != "02-29"])             # no 29-Feb anywhere
F <- function(d, from, to) file.path(d, paste0("pr_", seq(as.Date(from), as.Date(to), by = "day"), ".tif"))
mar_g <- F(greg, "2008-03-01", "2008-03-31"); mar_3 <- F(c360, "2008-03-01", "2008-03-31")
feb_g <- F(greg, "2008-02-01", "2008-02-29"); feb_n <- F(nolp, "2008-02-01", "2008-02-29")
ok(identical(e$require_days(mar_g, "pr"), mar_g), "complete month passes unchanged")
invisible(file.remove(mar_g[15]))
ok(length(e$require_days(mar_g, "pr")) == 30, "default warn: a mid-month gap continues on 30 days (legacy behaviour kept)")
ok(err(withenv(c(DAYS_CHECK = "calendar"), e$require_days(mar_g, "pr"))), "calendar: a mid-month gap STOPS")
ok(err(withenv(c(DAYS_CHECK = "strict"), e$require_days(mar_g, "pr"))), "strict: a mid-month gap STOPS")
invisible(file.create(mar_g[15])); invisible(file.remove(mar_g[31]))
ok(err(withenv(c(DAYS_CHECK = "calendar"), e$require_days(mar_g, "pr"))), "calendar: a missing 31st STOPS on a Gregorian series (it has other 31sts)")
ok(length(withenv(c(DAYS_CHECK = "calendar"), e$require_days(mar_3, "pr"))) == 30, "calendar: a missing 31st is expected on a 360-day series")
ok(err(withenv(c(DAYS_CHECK = "strict"), e$require_days(mar_3, "pr"))), "strict: a missing 31st STOPS even on a 360-day series")
invisible(file.remove(F(c360, "2008-03-10", "2008-03-10")))
ok(err(withenv(c(DAYS_CHECK = "calendar"), e$require_days(mar_3, "pr"))), "calendar: a 31st + a mid-month gap together STOP")
ok(length(withenv(c(DAYS_CHECK = "calendar"), e$require_days(feb_n, "pr"))) == 28, "calendar: a missing 29-Feb is expected on a no-leap series")
ok(err(withenv(c(DAYS_CHECK = "strict"), e$require_days(feb_n, "pr"))), "strict: a missing 29-Feb STOPS on a no-leap series")
lp <- seq(as.Date("2004-01-01"), as.Date("2008-12-31"), by = "day")
leap <- mkdir("leap", lp); invisible(file.remove(file.path(leap, "pr_2008-02-29.tif")))   # keeps 2004-02-29
ok(err(withenv(c(DAYS_CHECK = "calendar"), e$require_days(F(leap, "2008-02-01", "2008-02-29"), "pr"))),
   "calendar: a missing 29-Feb STOPS on a leap-aware series (2004-02-29 is on disk)")
invisible(file.remove(feb_g[29])); rm(list = ls(e$.dir_cal_cache), envir = e$.dir_cal_cache)
# greg now holds no 29-Feb at all (2007 is not a leap year and 2008-02-29 was just removed), so a fresh
# listing reads it as no-leap: the inference is evidence-based and cannot tell a lone missing 29-Feb
# from a no-leap calendar. Pin that limit rather than hide it.
ok(length(withenv(c(DAYS_CHECK = "calendar"), e$require_days(feb_g, "pr"))) == 28,
   "calendar: a series with no 29-Feb on disk reads as no-leap (documented limit of the inference)")
keep <- c(rep(TRUE, 10), FALSE, rep(TRUE, 20)); dts <- seq(as.Date("2007-01-01"), as.Date("2007-01-31"), by = "day")
ok(err(withenv(c(DAYS_CHECK = "calendar"), e$require_all_days(keep, dts, "tx/tn/rh", greg))), "require_all_days (aligned stacks): calendar stops on a mid-month gap")
ok(!err(withenv(c(DAYS_CHECK = "strict"), e$require_all_days(rep(TRUE, 31), dts, "tx/tn/rh", greg))), "require_all_days passes a complete month (even under strict)")

## ---- B. the real scripts, end to end ---------------------------------------------------------
cat("B. real scripts on a synthetic 4x4 grid\n")
CD <- file.path(tempdir(), "fx_common_data"); unlink(CD, recursive = TRUE); dir.create(CD)
tmpl <- rast(nrows = 4, ncols = 4, xmin = 34, xmax = 35, ymin = 0, ymax = 1, crs = "EPSG:4326")
w <- function(r, f) { dir.create(dirname(f), FALSE, TRUE); writeRaster(r, f, overwrite = TRUE) }
w(setValues(tmpl, 1), file.path(CD, "chirps_wrld", "chirps-v2.0.1981.01.01.tif"))
w(setValues(tmpl, 100), file.path(CD, "atlas_hazards", "soils", "sscp_world.tif"))
w(setValues(tmpl, 150), file.path(CD, "atlas_hazards", "soils", "ssat_world.tif"))
set.seed(42)
met <- list(pr = function(d) setValues(tmpl, if (format(d, "%Y-%m") == "2005-12") 40 + runif(16) else rgamma(16, 0.6, 0.15)),
            tasmax = function(d) setValues(tmpl, 31 + rnorm(16)), tasmin = function(d) setValues(tmpl, 18 + rnorm(16)),
            rsds = function(d) setValues(tmpl, 20 + rnorm(16)), hurs = function(d) setValues(tmpl, 60 + rnorm(16, 0, 5)),
            sfcWind = function(d) setValues(tmpl, 2 + runif(16)))
mkgcm <- function(gcm, days, vars = names(met)) for (v in vars) for (i in seq_along(days))
  w(met[[v]](days[i]), file.path(CD, "nex-gddp-cmip6", v, "historical", gcm, paste0(v, "_", days[i], ".tif")))
mkgcm("OBSTEST", seq(as.Date("2005-01-01"), as.Date("2006-12-31"), by = "day"))      # Gregorian, wet Dec 2005
q1 <- seq(as.Date("2005-01-01"), as.Date("2005-03-31"), by = "day")
mkgcm("CAL360", q1[format(q1, "%d") != "31"], "tasmax")                               # 360-day-like: no 31sts
mkgcm("OBS95", seq(as.Date("1995-01-01"), as.Date("1995-01-31"), by = "day"), c("pr", "tasmax", "tasmin", "rsds"))
run <- function(script, gcm, env = character()) {
  envs <- c(COMMON_DATA = CD, SCENARIO = "historical", GCMS = gcm, env)
  st <- suppressWarnings(system2("Rscript", file.path(IDX, script), env = paste0(names(envs), "=", envs), stdout = TRUE, stderr = TRUE))
  list(status = attr(st, "status") %||% 0L, log = st)
}
O   <- function(gcm, ...) file.path(CD, "nex-gddp-cmip6_indices", paste0("historical_", gcm), ...)
mon <- function(gcm, idx, y, m) O(gcm, idx, sprintf("%s-%d-%02d.tif", idx, y, m))
avl <- function(gcm, idx, y, m) O(gcm, idx, sprintf("AVAIL-%d-%02d.tif", y, m))
vals <- function(f) values(rast(f))[, 1]

# B1. seeds: defaults are unchanged and loud on a non-seed start, for all three water-balance scripts;
#     the default 1995-01 seed still starts a run.
for (s in c("NDWS", "NDWL0", "NDWL50")) {
  r <- run(paste0("fast_calc_", s, ".R"), "OBSTEST", c(YRS = "2005", MONTHS = "1"))
  ok(r$status != 0 && any(grepl("prior-month AVAIL missing", r$log)), sprintf("%s: default seeds stop loudly on a 2005-01 start", s))
  unlink(O("OBSTEST", s), recursive = TRUE)
  r <- run(paste0("fast_calc_", s, ".R"), "OBSTEST", c(YRS = "2005", MONTHS = "1,2,3", AVAIL_SEED_MONTHS = "2005-01"))
  ok(r$status == 0 && all(file.exists(sapply(1:3, function(m) mon("OBSTEST", s, 2005, m)))), sprintf("%s: AVAIL_SEED_MONTHS=2005-01 runs Jan-Mar", s))
  unlink(O("OBSTEST", s), recursive = TRUE)
}
r <- run("fast_calc_NDWS.R", "OBS95", c(YRS = "1995", MONTHS = "1"))
ok(r$status == 0 && file.exists(mon("OBS95", "NDWS", 1995, 1)), "default seed 1995-01 still starts a NEX-style run (no env set)")

# B2/B3. carry vs reset: 2005-01 seed over 2005-2006, then the same with an extra 2006-01 seed.
r <- run("fast_calc_NDWS.R", "OBSTEST", c(YRS = "2005:2006", AVAIL_SEED_MONTHS = "2005-01"))
f24 <- unlist(lapply(c(2005, 2006), function(y) sapply(1:12, function(m) mon("OBSTEST", "NDWS", y, m))))
ok(r$status == 0 && all(file.exists(f24)), "NDWS 2005-2006: 24 monthly files")
nd <- unlist(lapply(f24, vals))
ok(all(nd >= 0 & nd <= 31) && any(nd > 0) && any(nd < 28), "NDWS values are day counts and not degenerate (catches a J-vs-MJ radiation error)")
carry05 <- lapply(1:12, function(m) vals(avl("OBSTEST", "NDWS", 2005, m))); carry06 <- vals(avl("OBSTEST", "NDWS", 2006, 1))
ok(mean(carry05[[12]]) > 0, "wet December 2005 leaves a non-empty store (so reset-vs-carry is a real test)")
keepdir <- paste0(O("OBSTEST", "NDWS"), "_carry"); invisible(file.rename(O("OBSTEST", "NDWS"), keepdir))
r <- run("fast_calc_NDWS.R", "OBSTEST", c(YRS = "2005:2006", AVAIL_SEED_MONTHS = "2005-01,2006-01"))
reset05 <- lapply(1:12, function(m) vals(avl("OBSTEST", "NDWS", 2005, m))); reset06 <- vals(avl("OBSTEST", "NDWS", 2006, 1))
ok(r$status == 0 && identical(carry05, reset05), "an extra 2006-01 seed leaves 2005 bit-identical")
ok(!isTRUE(all.equal(carry06, reset06)), "...and resets the store at 2006-01 (what the old hard-coded 2021-01 did mid-series)")
unlink(O("OBSTEST", "NDWS"), recursive = TRUE); invisible(file.rename(keepdir, O("OBSTEST", "NDWS")))

# B4. DAYS_CHECK inside the real script. Remove 2005-03-15 from EVERY input: the case the legacy code
#     silently under-counted. (A day missing from only one variable already crashed the NDWS kernel.)
hold <- function(day, gcm = "OBSTEST", vars = names(met)) {
  f <- file.path(CD, "nex-gddp-cmip6", vars, "historical", gcm, paste0(vars, "_", day, ".tif"))
  invisible(file.rename(f, paste0(f, ".hold"))); function() invisible(file.rename(paste0(f, ".hold"), f)) }
nd_dir <- O("OBSTEST", "NDWS")
restore <- hold("2005-03-15"); unlink(nd_dir, recursive = TRUE)
r <- run("fast_calc_NDWS.R", "OBSTEST", c(YRS = "2005", MONTHS = "1,2,3", AVAIL_SEED_MONTHS = "2005-01"))
ok(r$status == 0 && file.exists(mon("OBSTEST", "NDWS", 2005, 3)) && any(grepl("WARN.*daily inputs missing", r$log)),
   "default (warn): completes on the days present AND logs the gap - legacy behaviour, now visible")
for (m in c("calendar", "strict")) {
  unlink(nd_dir, recursive = TRUE)
  r <- run("fast_calc_NDWS.R", "OBSTEST", c(YRS = "2005", MONTHS = "1,2,3", AVAIL_SEED_MONTHS = "2005-01", DAYS_CHECK = m))
  ok(r$status != 0 && any(grepl("daily inputs missing", r$log)), sprintf("DAYS_CHECK=%s: real NDWS stops on the 2005-03-15 gap", m))
}
restore()
restore <- hold("2005-03-31"); unlink(nd_dir, recursive = TRUE)
r <- run("fast_calc_NDWS.R", "OBSTEST", c(YRS = "2005", MONTHS = "1,2,3", AVAIL_SEED_MONTHS = "2005-01", DAYS_CHECK = "calendar"))
ok(r$status != 0 && any(grepl("daily inputs missing", r$log)), "DAYS_CHECK=calendar: a missing 31st stops on a Gregorian series")
restore()
r <- run("calc_NTx.R", "CAL360", c(YRS = "2005", MONTHS = "1,2,3", DAYS_CHECK = "calendar"))
ok(r$status == 0 && file.exists(O("CAL360", "NTx30", "NTx30-2005-03.tif")), "DAYS_CHECK=calendar: a 360-day series (no 31sts) runs NTx Jan-Mar")
unlink(O("CAL360"), recursive = TRUE)
r <- run("calc_NTx.R", "CAL360", c(YRS = "2005", MONTHS = "1", DAYS_CHECK = "strict"))
ok(r$status != 0 && any(grepl("daily inputs missing", r$log)), "DAYS_CHECK=strict: the same 360-day series stops (every Gregorian day required)")

# B5. NTx honours FORCE_OVERWRITE (was bare file.exists)
nt30 <- O("OBSTEST", "NTx30", "NTx30-2005-01.tif")
r <- run("calc_NTx.R", "OBSTEST", c(YRS = "2005", MONTHS = "1"))
ok(r$status == 0 && file.exists(nt30), "NTx runs and writes NTx30-2005-01")
Sys.setFileTime(nt30, as.POSIXct("2000-01-01")); t0 <- file.mtime(nt30)
r <- run("calc_NTx.R", "OBSTEST", c(YRS = "2005", MONTHS = "1"))
ok(r$status == 0 && file.mtime(nt30) == t0, "without FORCE_OVERWRITE an existing NTx file is skipped (default unchanged)")
r <- run("calc_NTx.R", "OBSTEST", c(YRS = "2005", MONTHS = "1", FORCE_OVERWRITE = "1"))
ok(r$status == 0 && file.mtime(nt30) > t0, "FORCE_OVERWRITE=1 rewrites the NTx file (was ignored)")

# B6. every other gated reader runs on honest input and stops on a gap under DAYS_CHECK=calendar.
others <- list(list(s = "calc_THI.R",  m = "1", dir = "THI"),
               list(s = "calc_PTOT.R", m = "1", dir = "PTOT"),
               list(s = "calc_HSH.R",  m = "1", dir = "HSH"),
               list(s = "calc_WBGT.R", m = "1", dir = "WBGT"),
               list(s = "calc_PET.R",  m = "1", dir = "PET")
               )   # calc_TAI.R not run: needs the ET_SolRad ESRI-grid climatology + envirem; covered by C
for (x in others) {
  env <- c(YRS = "2005", DAYS_CHECK = "calendar"); if (nzchar(x$m)) env <- c(env, MONTHS = x$m)
  r <- run(x$s, "OBSTEST", env)
  od <- O("OBSTEST", x$dir)
  ok(r$status == 0 && length(list.files(od, "\\.tif$", recursive = TRUE)) > 0, sprintf("%s: honest input runs and writes output", x$s))
  unlink(od, recursive = TRUE)
  restore <- hold("2005-01-15")
  r <- run(x$s, "OBSTEST", env)
  ok(r$status != 0 && any(grepl("daily inputs missing", r$log)), sprintf("%s: a 2005-01-15 gap stops under DAYS_CHECK=calendar", x$s))
  restore(); unlink(od, recursive = TRUE)
}

## ---- C. static: nothing left on the old silent paths ----------------------------------------
cat("C. static\n")
fs  <- list.files(IDX, "\\.R$", full.names = TRUE)
src <- setNames(lapply(fs, function(f) suppressWarnings(readLines(f))), basename(fs))
ok(!any(unlist(lapply(src, function(l) grepl("c\\('1995-01','2021-01'\\)", l)))), "no hard-coded c('1995-01','2021-01') seed list remains")
ok(!any(unlist(lapply(src, function(l) grepl("fls <- [a-z_]*fls\\[file\\.exists\\(", l)))), "no silent `fls[file.exists(fls)]` daily-input filter remains")
ungated <- unlist(lapply(names(src), function(n) {
  l <- src[[n]]; i <- grep("<- dts\\[keep\\]", l)
  bad <- i[!vapply(i, function(k) any(grepl("require_all_days\\(", l[max(1, k - 3):k])), logical(1))]
  if (length(bad)) paste0(n, ":", bad) else NULL }))
ok(length(ungated) == 0, paste0("every `dts[keep]` aligned-stack filter is preceded by require_all_days()",
                                if (length(ungated)) paste(" - UNGATED:", paste(ungated, collapse = ", ")) else ""))

unlink(c(CD, base), recursive = TRUE)
cat("\nALL UPSTREAM RUN-PARAMETER FIXTURE ASSERTIONS PASSED\n")
