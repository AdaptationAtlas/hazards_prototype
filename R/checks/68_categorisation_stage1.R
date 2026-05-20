# CR-068 Stage 1 probe — historic vs future hazard-categorisation asymmetry.
#
# Primary diagnostic: scan hazard_timeseries_class/ (Step 1 binary outputs)
# for per-pixel hazard fractions to identify saturated/zero hazards
# (Candidates 1/2/3 of the dispatch).
#
# Fallback diagnostic (Stage 1-bis): if hazard_timeseries_class/ is empty
# because Step 1 wasn't kept on local disk after the bake, scan
# hazard_timeseries_int/ (Step 5.2 combo outputs) and compare historic vs
# future combo means. Less precise (combos not individual hazards) but
# tells us whether the parquet asymmetry already exists at the _int/
# stage (-> bug in Step 1 or Step 5.2) or comes later
# (-> bug in 3_freq_x_exposure.R).
#
# Workspace convention: source R/0_server_setup.R for all paths.
#
# Usage (from project_dir):
#   Rscript R/checks/68_categorisation_stage1.R [--class-dir <path>] [--int-dir <path>]

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

args_all <- commandArgs(trailingOnly = TRUE)
get_arg <- function(name) {
  i <- match(paste0("--", name), args_all)
  if (!is.na(i) && i < length(args_all)) args_all[i + 1L] else NA_character_
}

# ----- Resolve class_dir + int_dir (try resolved path then fallbacks) ----
resolve_dir <- function(label, resolved, candidates) {
  override <- get_arg(label)
  if (!is.na(override)) {
    if (!dir.exists(override)) {
      warning(sprintf("--%s override '%s' does not exist", label, override))
      return(NA_character_)
    }
    return(override)
  }
  if (!is.null(resolved) && dir.exists(resolved)) return(resolved)
  hit <- candidates[dir.exists(candidates)]
  if (length(hit) > 0L) return(hit[1])
  NA_character_
}

class_candidates <- c(
  "/home/jovyan/common_data/hazards_prototype/Data/hazard_timeseries_class",
  "/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/hazard_timeseries_class"
)
int_candidates <- c(
  "/home/jovyan/common_data/hazards_prototype/Data/hazard_timeseries_int",
  "/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/hazard_timeseries_int"
)

class_dir <- resolve_dir("class-dir",
                         atlas_dirs$data_dir$hazard_timeseries_class,
                         class_candidates)
int_dir <- resolve_dir("int-dir",
                       atlas_dirs$data_dir$hazard_timeseries_int,
                       int_candidates)

cat(sprintf("  class_dir   = %s\n", if (is.na(class_dir)) "(not found)" else class_dir))
cat(sprintf("  int_dir     = %s\n\n", if (is.na(int_dir))   "(not found)" else int_dir))

results <- list()

# =========================================================================
# Section A — Per-hazard binary classification means (PRIMARY diagnostic)
# =========================================================================
# Scans Step 1 output for saturated (>0.7) or near-zero (<0.005) hazards.
# Sample patterns broadened from the dispatch's NDWS-G19/NTx35/NDWL0 to
# match any hazard-with-threshold code (e.g. PTOT-L1500, NTx33-G7).
if (!is.na(class_dir)) {
  cat("[A] Sampling classified rasters...\n")
  hazard_dirs <- list.dirs(class_dir, recursive = FALSE)
  if (length(hazard_dirs) == 0L) hazard_dirs <- class_dir

  # Widened regex to match any classified raster whose filename ends with
  # the canonical threshold suffix `-G<num>.tif` or `-L<num>.tif`. Examples
  # from CGlabs: ssp585_EC-Earth3_2081-2100_NTx35-mean-G14.tif (multi-
  # segment hazard token), ssp585_EC-Earth3_2081-2100_THI-max-max-G92.tif.
  # Earlier strict pattern ([A-Z][A-Za-z0-9]+-[GL][0-9]+) only matched
  # single-segment tokens like NDWS-G19 from the dispatch example.
  pattern_a <- "-[GL][0-9]+\\.tif$"
  sample <- rbindlist(lapply(hazard_dirs, function(d) {
    files <- list.files(d, pattern = pattern_a,
                        full.names = TRUE, recursive = TRUE)
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

  if (is.null(sample) || nrow(sample) == 0L) {
    cat("  No matching rasters found under class_dir — skipping Section A\n\n")
  } else {
    setorder(sample, hazard, scenario, timeframe)
    cat(sprintf("  scanned %d classified rasters\n", nrow(sample)))
    agg_a <- sample[, list(
      n_files  = .N,
      mean_avg = mean(mean_value, na.rm = TRUE),
      mean_min = min(mean_value, na.rm = TRUE),
      mean_max = max(mean_value, na.rm = TRUE)
    ), by = list(scenario, timeframe, hazard)]
    setorder(agg_a, hazard, scenario, timeframe)
    cat("\n  Mean hazard fraction by (scenario, timeframe, hazard):\n")
    print(agg_a)

    cat("\n  Saturated (mean > 0.7) — Candidate 1 evidence:\n")
    print(agg_a[mean_avg > 0.7])

    cat("\n  Near-zero (mean < 0.005) — Candidate 2 evidence:\n")
    print(agg_a[mean_avg < 0.005])

    results$class_means <- agg_a
  }
} else {
  cat("[A] SKIPPED — class_dir not available\n\n")
}

# =========================================================================
# Section B — Threshold sanity check (if Thresholds_U exists in scope)
# =========================================================================
if (exists("Thresholds_U")) {
  cat("\n[B] Thresholds_U for the fixed-threshold hazards:\n")
  print(Thresholds_U[grep("NDWS|NTx|NDWL|PTOT", index_name2)])
} else {
  cat("\n[B] Thresholds_U not in scope after sourcing 0_server_setup.\n")
}

# =========================================================================
# Section C — File-name collision audit (the L654 rename)
# =========================================================================
if (!is.na(class_dir)) {
  cat("\n[C] File-name collision audit (rename at 2_calculate_haz_freq.R:654)\n")
  class_files <- list.files(class_dir, recursive = TRUE, full.names = TRUE)
  class_files <- class_files[!grepl("ENSEMBLE", class_files)]
  bn <- basename(class_files)
  dup <- bn[duplicated(bn)]
  if (length(dup) > 0L) {
    cat("  COLLISIONS FOUND:\n")
    print(unique(dup))
    results$collisions <- unique(dup)
  } else {
    cat(sprintf("  no collisions across %d files\n", length(class_files)))
  }
}

# =========================================================================
# Section D — SSP370 missing-periods (side finding) against published parquet
# =========================================================================
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
    SELECT scenario, timeframe, COUNT(*) AS n_rows,
           ROUND(SUM(value)::DOUBLE, 0) AS sum_value
    FROM read_parquet('%s')
    WHERE iso3 = 'AGO' AND admin2_name IS NULL AND hazard != 'any'
    GROUP BY ALL ORDER BY scenario, timeframe", parquet_url))
}, error = function(e) {
  cat("  S3 fetch failed:", conditionMessage(e), "\n")
  NULL
})
if (!is.null(ssp370_check)) {
  print(ssp370_check)
  results$ssp370_per_period <- ssp370_check
}

# =========================================================================
# Section E — Stage 1-bis: combo (_int/) means, historic vs future
# =========================================================================
# Runs when class_dir was empty so we still get diagnostic signal. Reads
# every _int/<period>/*.tif and computes the per-pixel mean (= fraction of
# months where the combo is active). Side-by-side historic vs future
# means tell us if the asymmetry exists at the _int/ stage.
if (!is.na(int_dir)) {
  cat("\n[E] Stage 1-bis — combo (_int/) means, historic vs future\n")
  int_files <- list.files(int_dir, "\\.tif$",
                          full.names = TRUE, recursive = TRUE)
  int_files <- int_files[grepl("ENSEMBLEmean", int_files)]
  cat(sprintf("  scanned %d ENSEMBLEmean combo files\n", length(int_files)))

  if (length(int_files) > 0L) {
    int_sample <- rbindlist(lapply(int_files, function(f) {
      parts <- strsplit(basename(f), "_")[[1]]
      # Expected: <scenario>_<model>_<timeframe>_<combo>.tif
      tryCatch({
        r <- terra::rast(f)
        data.table(
          scenario   = parts[1],
          model      = parts[2],
          timeframe  = parts[3],
          combo      = sub("\\.tif$", "", paste(parts[-(1:3)], collapse = "_")),
          n_layers   = terra::nlyr(r),
          mean_value = as.numeric(
            terra::global(terra::mean(r), "mean", na.rm = TRUE)[, 1]),
          file       = basename(f)
        )
      }, error = function(e) NULL)
    }), fill = TRUE)

    if (!is.null(int_sample) && nrow(int_sample) > 0L) {
      setorder(int_sample, combo, scenario, timeframe)
      cat("\n  Combo means (top 30 rows):\n")
      print(int_sample[1:min(30L, .N), list(scenario, timeframe, combo, mean_value)])

      # Historic vs future side-by-side (pick a few combos that exist
      # in both historic and at least one future scenario)
      hist_combos <- int_sample[grepl("historic", scenario), unique(combo)]
      asym <- int_sample[combo %in% hist_combos,
                         list(mean_hist = mean(mean_value[grepl("historic", scenario)],
                                               na.rm = TRUE),
                              mean_fut  = mean(mean_value[!grepl("historic", scenario)],
                                               na.rm = TRUE),
                              n_files   = .N),
                         by = combo]
      asym[, ratio := mean_hist / pmax(mean_fut, 1e-9)]
      setorder(asym, -ratio)
      cat("\n  Historic-vs-future combo ratio (top 15 most-divergent):\n")
      print(asym[1:min(15L, .N)])

      cat(sprintf(
        "\n  combos where mean_hist > 1.5x mean_fut: %d\n",
        sum(asym$ratio > 1.5, na.rm = TRUE)))
      cat(sprintf(
        "  combos where mean_hist < 0.5x mean_fut: %d\n",
        sum(asym$ratio < 0.5, na.rm = TRUE)))

      results$int_means <- int_sample
      results$int_asymmetry <- asym
    }
  }
} else {
  cat("\n[E] SKIPPED — int_dir not available\n")
}

# =========================================================================
# JSON record
# =========================================================================
out_dir <- file.path(project_dir, "metadata", "checks")
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
out_path <- file.path(out_dir, "68_categorisation_stage1.json")
jsonlite::write_json(
  c(list(timestamp  = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
         project_dir = project_dir,
         Cglabs     = isTRUE(Cglabs),
         class_dir  = class_dir,
         int_dir    = int_dir),
    results),
  out_path, auto_unbox = TRUE, pretty = TRUE
)
cat(sprintf("\nWrote report to %s\n", out_path))

cat("\n=== STOP for Pete (Stage 1 only) ===\n")
cat("Interpretation guide:\n")
cat("  Section A populated AND saturated/near-zero rows present:\n",
    "    -> Candidate 1 (saturated) or 2 (near-zero) — root cause is\n",
    "       wrong threshold or unclassified raw counts at Step 1.\n", sep = "")
cat("  Section A SKIPPED (class_dir empty), Section E shows large\n",
    "    historic-vs-future ratio asymmetry per combo:\n",
    "    -> bug is at or before Step 5.2 (likely in Step 1 inputs that\n",
    "       weren't kept on local disk after Brayden's bake).\n", sep = "")
cat("  Section A SKIPPED, Section E shows historic combo means ~= future:\n",
    "    -> bug is downstream of _int/ (in 3_freq_x_exposure.R or the\n",
    "       parquet build), and the local rasters are fine.\n", sep = "")
