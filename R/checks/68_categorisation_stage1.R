# CR-068 Stage 1 probe — historic vs future hazard-categorisation asymmetry.
#
# Reads the per-(scenario × model × timeframe × hazard) classified binary
# rasters under atlas_dirs$data_dir$hazard_timeseries_class and reports
# per-pixel mean value. The classifier emits binary 0/1 layers, so the
# mean = fraction of pixel-months flagged as a hazard event. Saturated
# historic means (~0.7–1.0) for one of NDWS-G19 / NTx35 / NDWL0 will
# point to Candidate 1 (e.g. wrong threshold or unclassified raw counts
# read as binary) or 2 of the dispatch hypothesis.
#
# Also triage's the SSP370 missing-periods side-finding: counts rows in
# the published parquet by (scenario, timeframe) for AGO at admin0.
#
# Workspace convention: source R/0_server_setup.R for all paths.
#
# Usage (from project_dir):
#   Rscript R/checks/68_categorisation_stage1.R
#
# To override the classified-raster directory (e.g. if 0_server_setup.R's
# climdat_source resolves to a working_dir that doesn't have the data):
#   Rscript R/checks/68_categorisation_stage1.R \
#     --class-dir /home/jovyan/common_data/hazards_prototype/Data/hazard_timeseries_class

project_dir <- if (nzchar(Sys.getenv("project_dir"))) {
  Sys.getenv("project_dir")
} else {
  getwd()
}
setwd(project_dir)
source("R/0_server_setup.R")

suppressPackageStartupMessages({
  pacman::p_load(terra, data.table, jsonlite, duckdb, DBI)
})

cat("\n=== CR-068 Stage 1 probe — categorisation asymmetry ===\n")
cat(sprintf("  project_dir = %s\n", project_dir))
cat(sprintf("  Cglabs      = %s\n", isTRUE(Cglabs)))

# --class-dir override (in case climdat_source picked the wrong tree)
args_all <- commandArgs(trailingOnly = TRUE)
class_dir_override <- {
  i <- match("--class-dir", args_all)
  if (!is.na(i) && i < length(args_all)) args_all[i + 1L] else NA_character_
}

class_dir <- if (!is.na(class_dir_override)) {
  class_dir_override
} else {
  atlas_dirs$data_dir$hazard_timeseries_class
}
if (is.null(class_dir) || !dir.exists(class_dir)) {
  # Try the sibling working_dir (atlas_delta <-> nexgddp swap) before giving up
  candidates <- c(
    "/home/jovyan/common_data/hazards_prototype/Data/hazard_timeseries_class",
    "/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/hazard_timeseries_class"
  )
  hit <- candidates[dir.exists(candidates)]
  if (length(hit) > 0L) {
    class_dir <- hit[1]
    cat(sprintf("  (resolved class_dir from candidate fallback: %s)\n", class_dir))
  } else {
    cat("\nERROR: hazard_timeseries_class dir not found at:\n")
    cat(sprintf("  - %s (resolved from atlas_dirs)\n",
                atlas_dirs$data_dir$hazard_timeseries_class))
    cat("  - CGlabs candidates:\n")
    cat(paste0("    ", candidates), sep = "\n")
    stop("Run R/2_calculate_haz_freq.R Step 1 first, OR pass --class-dir <path>.")
  }
}
cat(sprintf("  class_dir   = %s\n\n", class_dir))

# ----- A. Classified-raster per-pixel mean diagnostic -------------------
# Reads (scenario × model × timeframe × hazard) binary rasters; mean ≈
# fraction of pixel-months flagged as the hazard. Skip ENSEMBLE composites
# so we see per-GCM behaviour; aggregate at the end by (scenario, hazard).
cat("[A] Sampling classified rasters...\n")
hazard_dirs <- list.dirs(class_dir, recursive = FALSE)

sample <- rbindlist(lapply(hazard_dirs, function(d) {
  files <- list.files(
    d,
    pattern    = "(NDWS-G[0-9]+|NTx35-G[0-9]+|NDWL0-G[0-9]+)\\.tif$",
    full.names = TRUE,
    recursive  = TRUE
  )
  files <- files[!grepl("ENSEMBLE", files)]
  if (length(files) == 0L) return(NULL)
  rbindlist(lapply(files, function(f) {
    r <- terra::rast(f)
    parts <- strsplit(basename(f), "_")[[1]]
    data.table(
      scenario   = parts[1],
      model      = parts[2],
      timeframe  = parts[3],
      hazard     = sub("\\.tif$", "", paste(parts[-(1:3)], collapse = "_")),
      n_layers   = terra::nlyr(r),
      mean_value = as.numeric(
        terra::global(terra::mean(r), "mean", na.rm = TRUE)[, 1]),
      file       = basename(f)
    )
  }))
}), fill = TRUE)

if (nrow(sample) == 0L) {
  stop("No classified rasters found under ", class_dir,
       " — has R/2_calculate_haz_freq.R Step 1 been run?")
}

setorder(sample, hazard, scenario, timeframe)

cat(sprintf("  scanned %d classified rasters across %d hazard dirs\n",
            nrow(sample), length(hazard_dirs)))

# Aggregate over models — focus on (scenario, timeframe, hazard) cell
agg <- sample[, list(
  n_files   = .N,
  mean_avg  = mean(mean_value, na.rm = TRUE),
  mean_min  = min(mean_value, na.rm = TRUE),
  mean_max  = max(mean_value, na.rm = TRUE)
), by = list(scenario, timeframe, hazard)]
setorder(agg, hazard, scenario, timeframe)

cat("\n  Mean of per-pixel hazard fraction by (scenario, timeframe, hazard):\n")
print(agg)

# Highlight saturated layers (mean > 0.7 = candidate 1)
cat("\n  Saturated layers (mean > 0.7) — Candidate 1 evidence:\n")
print(agg[mean_avg > 0.7])

cat("\n  Near-zero layers (mean < 0.005) — Candidate 2 evidence:\n")
print(agg[mean_avg < 0.005])

# ----- B. Threshold sanity check ----------------------------------------
if (exists("Thresholds_U")) {
  cat("\n[B] Thresholds_U for the 3 fixed-threshold hazards:\n")
  print(Thresholds_U[grep("NDWS|NTx35|NDWL0", index_name2)])
} else {
  cat("\n[B] Thresholds_U not in scope after sourcing 0_server_setup. ",
      "Run script 2 setup blocks 1-3 manually if you need the table.\n", sep = "")
}

# ----- C. File-name collision check -------------------------------------
cat("\n[C] File-name collision audit (rename at 2_calculate_haz_freq.R:649)\n")
class_files <- list.files(class_dir, recursive = TRUE, full.names = TRUE)
class_files <- class_files[!grepl("ENSEMBLE", class_files)]
bn <- basename(class_files)
dup <- bn[duplicated(bn)]
if (length(dup) > 0L) {
  cat("  COLLISIONS FOUND:\n")
  print(unique(dup))
} else {
  cat(sprintf("  no collisions across %d files\n", length(class_files)))
}

# ----- D. SSP370 missing periods (side finding) -------------------------
cat("\n[D] SSP370 missing-periods triage (AGO admin0)\n")
parquet_url <- paste0(
  "https://digital-atlas.s3.amazonaws.com/",
  "domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/",
  "processing=hazard-risk-exposure/variable=vop_nominal-usd21/",
  "period=jagermeyr/model=ENSEMBLEmean/severity=severe/int=multi-hazard.parquet"
)
ssp370_check <- tryCatch({
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(con, "INSTALL httpfs; LOAD httpfs;")
  DBI::dbGetQuery(con, sprintf("
    SELECT scenario, timeframe,
           COUNT(*) AS n_rows,
           ROUND(SUM(value)::DOUBLE, 0) AS sum_value
    FROM read_parquet('%s')
    WHERE iso3 = 'AGO' AND admin2_name IS NULL AND hazard != 'any'
    GROUP BY ALL
    ORDER BY scenario, timeframe", parquet_url))
}, error = function(e) {
  cat("  S3 fetch failed:", conditionMessage(e), "\n")
  NULL
})
if (!is.null(ssp370_check)) print(ssp370_check)

# ----- E. JSON report ---------------------------------------------------
out_dir <- file.path(project_dir, "metadata", "checks")
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
out_path <- file.path(out_dir, "68_categorisation_stage1.json")
jsonlite::write_json(
  list(
    timestamp  = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    project_dir = project_dir,
    Cglabs     = isTRUE(Cglabs),
    raster_means = agg,
    saturated  = agg[mean_avg > 0.7],
    near_zero  = agg[mean_avg < 0.005],
    collisions = if (length(dup) > 0L) unique(dup) else character(0),
    ssp370_per_period = ssp370_check
  ),
  out_path, auto_unbox = TRUE, pretty = TRUE
)
cat(sprintf("\nWrote report to %s\n", out_path))

cat("\n=== STOP for Pete (Stage 1 only) ===\n")
cat("Interpret the means table above:\n")
cat("  Candidate 1 (NDWS-G19 saturated historic): hist mean ≈ 0.7-1.0",
    " AND future means ≈ 0.1-0.4 for NDWS-G19.\n", sep = "")
cat("  Candidate 2 (NTx35 / NDWL0 historic ≈ 0):  historic means near zero",
    " while future means well above zero.\n", sep = "")
cat("  Candidate 3 (upstream data anomaly):       all three hazards behave",
    " anomalously in historic relative to future.\n", sep = "")
