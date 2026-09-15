#!/usr/bin/env Rscript
# R/probe_r3_usd_crop.R
# =====================
# Issue #9 STEP 3 follow-up. READ-ONLY. Answers, on the node, why R/3 §4.1 leaves
# the nominal-usd CROP tifs without hazard='none' while intld15 crops and usd
# livestock are fine. Hypothesis under test: the crop usd exposure raster
# (`spam_vop_usd2015_all.tif`, S3-legacy SPAM) is on a different grid from the
# `_int` stacks, `data * exposure` errors, the §4.1 retry wrapper swallows it and
# the stale pre-`none` tif survives. The alternative is a layer-name mismatch.
# Either way this prints the facts; it changes nothing.
#
# Run (cglabs, repo root, ~1 min):  Rscript R/probe_r3_usd_crop.R
# Optional: PROBE_TIMEFRAME=jagermeyr (default) | PROBE_CROP=maize (default)

.ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) cat(sprintf("[%s] [probe-usd] %s\n", .ts(), sprintf(fmt, ...)))
setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
if (nzchar(Sys.getenv("ATLAS_SETUP_SKIP"))) { .log("ATLAS_SETUP_SKIP set"); stopifnot(exists("atlas_dirs")) } else { .log("sourcing %s", setup); suppressMessages(suppressWarnings(source(setup))) }
suppressPackageStartupMessages({ pacman::p_load(terra, data.table) })

TF   <- Sys.getenv("PROBE_TIMEFRAME", "jagermeyr")
CROP <- Sys.getenv("PROBE_CROP", "maize")
geom <- function(path, label) {
  if (!length(path) || is.na(path[1]) || !file.exists(path[1])) { .log("%-34s MISSING (%s)", label, ifelse(length(path), path, "<none>")); return(invisible(NULL)) }
  r <- tryCatch(suppressWarnings(terra::rast(path)), error = function(e) NULL)
  if (is.null(r)) { .log("%-34s UNREADABLE %s", label, path); return(invisible(NULL)) }
  .log("%-34s res=%s dims=%s ext=[%s] nlyr=%d mtime=%s  %s", label, paste(round(terra::res(r), 4), collapse = "x"),
       paste(dim(r)[1:2], collapse = "x"), paste(round(as.vector(terra::ext(r)), 3), collapse = ","), terra::nlyr(r),
       format(file.mtime(path), "%Y-%m-%d %H:%M"), basename(path))
  invisible(r)
}
cat("\n=== 1) GRIDS ===\n")
base <- geom(base_rast_path, "base_rast (setup)")
risk_dir <- file.path(atlas_dirs$data_dir$hazard_risk, TF)
int_f <- list.files(risk_dir, paste0("^", CROP, "_ENSEMBLEmean_severe_.*_int\\.tif$"), full.names = TRUE)
int_r <- geom(int_f[1], sprintf("_int %s ENSEMBLEmean severe", CROP))
lv_int <- list.files(risk_dir, "^cattle-highland_ENSEMBLEmean_severe_.*_int\\.tif$", full.names = TRUE)
geom(lv_int[1], "_int cattle-highland ENSEMBLEmean")
ms <- list.files(mapspam_pro_dir, ".tif$", recursive = TRUE, full.names = TRUE)
usd15 <- geom(grep("vop_usd2015_all", ms, value = TRUE)[1], "crop usd LEGACY spam_vop_usd2015_all")
int21 <- geom(grep("vop_intld15-2021_all", ms, value = TRUE)[1], "crop intld 0.4.0 vop_intld15-2021_all")
usd21 <- geom(grep("variable=vop_nominal-usd-2021/spam_vop_nominal-usd-2021_all\\.tif$", ms, value = TRUE)[1], "crop usd 0.4.2 nominal-usd-2021_all")
geom(grep("harv-area_ha_all", ms, value = TRUE)[1], "crop ha spam_harv-area_ha_all")
geom(file.path(glw2020_pro_dir, "variable=vop_nominal-usd-2021", "glw4-2020_vop_nominal-usd-2021.tif"), "livestock usd 0.4.1")
geom(file.path(glw2020_pro_dir, "variable=vop_intld15-2021", "glw4-2020_vop_intld15-2021.tif"), "livestock intld 0.4.1")
.log("n mapspam tifs matching vop_usd2015_all: %d | nominal-usd-2021_all: %d", length(grep("vop_usd2015_all", ms)), length(grep("nominal-usd-2021_all", ms)))

cat("\n=== 2) §4.1 OUTPUTS (stale or fresh?) ===\n")
for (v in c("hazard_risk_vop_usd", "hazard_risk_vop")) {
  d <- file.path(atlas_dirs$data_dir[[v]], TF)
  crop_t <- list.files(d, paste0("^", CROP, "_ENSEMBLEmean_severe_.*_int_.*\\.tif$"), full.names = TRUE)
  lv_t   <- list.files(d, "^cattle-highland_ENSEMBLEmean_severe_.*_int_.*\\.tif$", full.names = TRUE)
  geom(crop_t[1], sprintf("%s %s tif", v, CROP)); geom(lv_t[1], sprintf("%s cattle-highland tif", v))
  ft <- list.files(d, "^failed_risk_x_exposure_.*\\.txt$", full.names = TRUE)
  if (length(ft)) for (f in ft) { l <- readLines(f); .log("%s: %s has %d lines (mtime %s); head: %s", v, basename(f), length(l), format(file.mtime(f), "%Y-%m-%d %H:%M"), paste(head(basename(l), 2), collapse = " | ")) } else .log("%s: no failed_risk_x_exposure_*.txt", v)
  all_t <- list.files(d, "_int_.*\\.tif$", full.names = TRUE)
  if (!length(all_t)) { .log("%s: no _int_*.tif in %s", v, d); next }
  mt <- file.mtime(all_t); crops <- !grepl("^(cattle|sheep|goats|pigs|poultry)", basename(all_t))
  rng <- function(z) if (length(z)) sprintf("%s .. %s", format(min(z), "%Y-%m-%d"), format(max(z), "%Y-%m-%d")) else "<none>"
  .log("%s: %d tifs | crop tifs mtime %s | livestock tifs mtime %s", v, length(all_t), rng(mt[crops]), rng(mt[!crops]))
  # mtime histogram by day for crop tifs: stale vintages show up as separate days
  tab <- sort(table(format(mt[crops], "%Y-%m-%d")), decreasing = TRUE)
  .log("%s: crop tif count by mtime day: %s", v, paste(sprintf("%s=%d", names(tab), tab), collapse = ", "))
}

cat("\n=== 3) LAYER NAMES: _int crops vs exposure rasters ===\n")
crop_choices <- unique(unlist(tstrsplit(basename(list.files(risk_dir, "_int\\.tif$")), "_", keep = 1)))
crop_choices <- crop_choices[!grepl("cattle|sheep|goats|pigs|poultry", crop_choices)]
nm <- function(r) if (is.null(r)) character(0) else gsub("_| ", "-", names(r))
for (x in list(list("legacy usd2015", usd15), list("0.4.0 intld-2021", int21), list("0.4.2 usd-2021", usd21))) {
  miss <- setdiff(setdiff(crop_choices, "generic-crop"), nm(x[[2]]))
  .log("%-18s layers=%d | _int crops=%d | crops NOT in raster: %s", x[[1]], length(nm(x[[2]])), length(crop_choices), if (length(miss)) paste(miss, collapse = ",") else "<none>")
}

cat("\n=== 4) LIVE REPRO: data * exposure for one crop file, per raster ===\n")
if (!is.null(int_r)) for (x in list(list("legacy usd2015", usd15), list("0.4.0 intld-2021", int21), list("0.4.2 usd-2021", usd21))) {
  r <- x[[2]]; if (is.null(r)) { .log("%-18s skipped (raster missing)", x[[1]]); next }
  names(r) <- gsub("_| ", "-", names(r))
  if (!CROP %in% names(r)) { .log("%-18s %s not a layer -> R/3 would stop('Commodity not found')", x[[1]], CROP); next }
  res <- tryCatch({ y <- int_r * r[[CROP]]; sprintf("OK nlyr=%d res=%s", terra::nlyr(y), paste(terra::res(y), collapse = "x")) }, error = function(e) paste("ERROR:", conditionMessage(e)))
  .log("%-18s %s", x[[1]], res)
}

cat("\n=== 5) R/3 LOG of the 2026-09-14 FORCE run ===\n")
lg <- sort(list.files("logs", "^r3_force_.*\\.log$", full.names = TRUE), decreasing = TRUE)[1]
if (!is.na(lg)) { l <- readLines(lg); .log("log %s (%d lines)", lg, length(l))
  for (pat in c("4\\.1\\.1\\) variable", "FAILED after retries|Some files failed|failed_risk_x_exposure", "Warning|warnings", "Using crop vop usd file")) { h <- grep(pat, l, value = TRUE); .log("  /%s/: %d lines", pat, length(h)); for (z in head(h, 8)) cat("     ", substr(z, 1, 160), "\n") }
} else .log("no logs/r3_force_*.log found")
.log("probe done")
