#!/usr/bin/env Rscript
# R/checks/probe_r3_res25_preflight.R
# ===================================
# "Will R/3 read what we think it will read?" - answered in seconds, before a
# day-long bake. Read-only: sources 0_server_setup.R for the paths, opens rasters
# for their geometry, writes nothing.
#
# Since issue #30 the exposure rasters exist at two resolutions with the tag in
# every name, and R/3 reads the tag matching its own hazard grid
# (res_tag_of(base_rast)). This probe resolves every exposure input exactly as
# R/3 L380-440 does (same grep patterns, same file.path()), and asserts:
#   1. each input resolves to exactly ONE file (a _res-05 twin must not double-match)
#   2. each input's grid vs base_rast: compareGeom TRUE means R/3 multiplies
#      directly; FALSE means .align_exposure() will resample (sum) - allowed, but
#      it must be predicted here, not discovered in the log
#   3. Data/base_rast.tif (R/3's grid) vs metadata/base_rast_nexgddp.tif (what
#      0.4.0/0.4.1/0.4.2 rasterised onto) - same question, stated once
#   4. the retired legacy raster (vop_usd2015_all) is absent and _pre30_backup is gone
#   5. the environment cannot silently change the run: FORCE_OVERWRITE,
#      R3_ALLOW_41_FAILURES, SKIP_R3_4_1 unset; R3_CROP_VOP_USD unset or 2021
#   6. no other Rscript is running (a second bake into the same dirs is how
#      skip-if-exists produces a half-stale product)
#   7. inventory of the R/3 output dirs, so the RESPONSE records what was parked
#
# Exit 0 = every check ok; 1 = at least one FAIL. WARN lines do not fail.
#
# Usage (cglabs, repo root, seconds):
#   Rscript R/checks/probe_r3_res25_preflight.R
#   Rscript -e 'source("/abs/hazards_prototype/R/checks/probe_r3_res25_preflight.R")'

t0 <- Sys.time()
.ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) cat(sprintf("[%s] [r3-preflight] %s\n", .ts(), sprintf(fmt, ...)))
n_fail <- 0L
ok   <- function(cond, fmt, ...) { if (isTRUE(cond)) .log("ok   %s", sprintf(fmt, ...)) else { n_fail <<- n_fail + 1L; .log("FAIL %s", sprintf(fmt, ...)) }; invisible(cond) }
warn <- function(fmt, ...) .log("WARN %s", sprintf(fmt, ...))

setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
if (nzchar(Sys.getenv("ATLAS_SETUP_SKIP"))) { .log("ATLAS_SETUP_SKIP set"); stopifnot(exists("atlas_dirs")) } else { .log("sourcing %s", setup); suppressMessages(suppressWarnings(source(setup))) }
suppressPackageStartupMessages(library(terra))

## 5) environment ---------------------------------------------------------------
.log("--- environment")
for (v in c("FORCE_OVERWRITE", "R3_ALLOW_41_FAILURES", "SKIP_R3_4_1", "REBAKE_SCENARIO")) ok(!nzchar(Sys.getenv(v)), "%s unset (got '%s')", v, Sys.getenv(v))
.vint <- Sys.getenv("R3_CROP_VOP_USD", "")
ok(.vint %in% c("", "2021"), "R3_CROP_VOP_USD unset or 2021 (got '%s'); the default is 2021 since 2026-09-26", .vint)
.log("climdat_source = %s | working dir = %s", climdat_source, getwd())

## 6) nothing else running ------------------------------------------------------
.log("--- processes")
procs <- tryCatch(suppressWarnings(system2("pgrep", c("-af", "Rscript"), stdout = TRUE)), error = function(e) character(0))
procs <- procs[!grepl(sprintf("^%d ", Sys.getpid()), procs) & !grepl("probe_r3_res25_preflight", procs)]
ok(length(procs) == 0, "no other Rscript running (%d found)", length(procs)); for (p in procs) .log("     %s", p)

## 1-3) grids and inputs, resolved as R/3 does -----------------------------------
.log("--- hazard grid")
ok(file.exists(base_rast_path), "base_rast_path exists: %s", base_rast_path)
base_rast <- terra::rast(base_rast_path)
tag <- res_tag_of(base_rast)
.log("base_rast res %s -> R/3 reads tag %s | ext %s", paste(terra::res(base_rast), collapse = "x"), tag, paste(round(as.vector(terra::ext(base_rast)), 3), collapse = ","))
ok(tag == "res-25", "R/3 tag is res-25 on this host (got %s)", tag)
nex <- file.path(project_dir, "metadata", "base_rast_nexgddp.tif")
if (file.exists(nex)) {
  same <- terra::compareGeom(base_rast, terra::rast(nex), stopOnError = FALSE)
  .log("%s compareGeom(Data/base_rast.tif, metadata/base_rast_nexgddp.tif) = %s -> %s", if (same) "ok  " else "WARN", same,
       if (same) "R/3 multiplies exposure directly" else ".align_exposure() WILL resample every exposure raster (sum, mass-conserving); expect its log lines, and a WARN only if dev > 1%")
} else warn("metadata/base_rast_nexgddp.tif not found under project_dir")

.log("--- exposure inputs (R/3 L380-440 patterns)")
files <- list.files(mapspam_pro_dir, ".tif$", recursive = TRUE, full.names = TRUE)
.log("mapspam_pro_dir = %s (%d tifs)", mapspam_pro_dir, length(files))
inputs <- list(
  crop_vop_intld = grep(paste0("vop_intld15-2021_all_", tag, "\\.tif$"), files, value = TRUE),
  crop_vop_usd   = grep(paste0("variable=vop_nominal-usd-2021/spam_vop_nominal-usd-2021_all_", tag, "\\.tif$"), files, value = TRUE),
  crop_ha        = grep("harv-area_ha_all", files, value = TRUE),
  livestock_no   = file.path(glw2020_pro_dir, paste0("livestock_number_number_", tag, ".tif")),
  livestock_vop  = file.path(glw2020_pro_dir, "variable=vop_intld15-2021", paste0("glw4-2020_vop_intld15-2021_", tag, ".tif")),
  livestock_usd  = file.path(glw2020_pro_dir, "variable=vop_nominal-usd-2021", paste0("glw4-2020_vop_nominal-usd-2021_", tag, ".tif"))
)
for (nm in names(inputs)) {
  f <- inputs[[nm]]; f <- f[file.exists(f)]
  if (!ok(length(f) == 1, "%-15s resolves to exactly 1 file (%d): %s", nm, length(f), paste(basename(f), collapse = " | "))) next
  r <- terra::rast(f)
  same <- terra::compareGeom(r, base_rast, stopOnError = FALSE)
  .log("     %-15s res %s nlyr %d | compareGeom(base_rast) = %s%s", nm, paste(terra::res(r), collapse = "x"), terra::nlyr(r), same,
       if (same) "" else "  <- .align_exposure() will resample this one")
}
# A _res-05 twin sitting next to the _res-25 file is expected; the tag-anchored
# greps above must still have matched one. Say how many twins exist.
.log("     tagged rasters under mapspam_pro_dir: res-25 = %d, res-05 = %d, untagged = %d",
     sum(grepl("_res-25\\.tif$", files)), sum(grepl("_res-05\\.tif$", files)), sum(!grepl("_res-(05|25)\\.tif$", files)))

## 4) retired things are gone ----------------------------------------------------
.log("--- retired artefacts")
legacy <- c(grep("vop_usd2015_all", files, value = TRUE),
            list.files(glw2020_pro_dir, "vop_usd2015_all", recursive = TRUE, full.names = TRUE))
ok(length(legacy) == 0, "legacy vop_usd2015_all raster absent (%d found)", length(legacy)); for (l in legacy) .log("     %s", l)
.ref_dir <- if (exists("exposure_dir")) exposure_dir else atlas_dirs$data_dir$exposure
pre30 <- file.path(.ref_dir, "_pre30_backup")
ok(!dir.exists(pre30), "%s absent (deletion authorised in #30)%s", pre30, if (dir.exists(pre30)) sprintf(" - present, %d entries, STOP and report", length(list.files(pre30, recursive = TRUE))) else "")

## 7) inventory of R/3 output dirs -------------------------------------------------
.log("--- R/3 output inventory (what Block B/C park)")
for (v in c("hazard_risk_vop_usd", "hazard_risk_vop", "hazard_risk_ha")) for (tf in timeframe_choices) {
  d <- file.path(atlas_dirs$data_dir[[v]], tf)
  if (!dir.exists(d)) { .log("     %-20s %-10s (missing dir)", v, tf); next }
  tifs <- list.files(d, "\\.tif$"); pq <- list.files(d, "\\.parquet$"); txt <- list.files(d, "^(failed|skipped)_.*\\.txt$")
  newest <- if (length(tifs) + length(pq)) format(max(file.mtime(list.files(d, "\\.(tif|parquet)$", full.names = TRUE))), "%Y-%m-%d %H:%M") else "-"
  .log("     %-20s %-10s int tifs %5d | other tifs %5d | parquets %2d | failed/skipped txt %d | newest %s",
       v, tf, sum(grepl("_int_", tifs)), sum(!grepl("_int_", tifs)), length(pq), length(txt), newest)
  for (t in txt) .log("        %s (%d lines)", t, length(readLines(file.path(d, t))))
}

.log("complete in %.1f s: %d FAIL", as.numeric(difftime(Sys.time(), t0, units = "secs")), n_fail)
if (!interactive()) quit(status = if (n_fail == 0L) 0 else 1)
