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
#   Rscript R/checks/68_categorisation_stage1.R
#     [--class-dir <path>] [--int-dir <path>]
#     [--countries AGO[,NGA,CIV,ETH]]
#     [--sample N]                 (cap files per section for fast smoke)
#
# Subset behaviour:
#   --countries: crop every raster to the union of these admin0 AOIs
#                before global mean. Default = whole geoboundaries
#                extent (slow on >1000 files). Strongly recommended.
#   --sample N : stratified random pick of N files per section. Default
#                = no cap. Useful for smoke runs on a CGlabs probe.

project_dir <- if (nzchar(Sys.getenv("project_dir"))) {
  Sys.getenv("project_dir")
} else {
  getwd()
}
setwd(project_dir)
source("R/0_server_setup.R")
# 0_server_setup.R calls setwd(working_dir) — switch to a working_dir
# under /home/jovyan/common_data/*. Source the helper by absolute path
# so it doesn't fall through to the now-cwd.
source(file.path(project_dir, "R/checks/_helpers.R"))

suppressPackageStartupMessages({
  pacman::p_load(terra, data.table, jsonlite, duckdb, DBI,
                 future, future.apply, furrr, progressr)
})

# Per-file scan with windowed-read + global() mean. Pulled out as a
# top-level function so future_map workers can call it (closures over
# .GlobalEnv don't always serialize cleanly across multisession).
scan_one_class <- function(f, aoi_wrap) {
  tryCatch({
    aoi_local <- terra::unwrap(aoi_wrap)
    r <- terra::rast(f)
    r <- window_to_aoi(r, aoi_local)
    parts <- strsplit(basename(f), "_")[[1]]
    m <- as.numeric(terra::global(terra::mean(r), "mean", na.rm = TRUE)[, 1])
    data.table(
      scenario   = parts[1],
      model      = parts[2],
      timeframe  = parts[3],
      hazard     = sub("\\.tif$", "", paste(parts[-(1:3)], collapse = "_")),
      n_layers   = terra::nlyr(r),
      mean_value = m,
      file       = basename(f),
      status     = if (is.nan(m) || is.na(m)) "nan" else "ok"
    )
  }, error = function(e) {
    parts <- strsplit(basename(f), "_")[[1]]
    data.table(
      scenario   = if (length(parts) >= 1) parts[1] else NA_character_,
      model      = if (length(parts) >= 2) parts[2] else NA_character_,
      timeframe  = if (length(parts) >= 3) parts[3] else NA_character_,
      hazard     = if (length(parts) >= 4)
                     sub("\\.tif$", "", paste(parts[-(1:3)], collapse = "_"))
                   else NA_character_,
      n_layers   = NA_integer_,
      mean_value = NA_real_,
      file       = basename(f),
      status     = paste0("err: ", conditionMessage(e))
    )
  })
}

scan_one_int <- function(f, aoi_wrap) {
  tryCatch({
    aoi_local <- terra::unwrap(aoi_wrap)
    r <- terra::rast(f)
    r <- window_to_aoi(r, aoi_local)
    parts <- strsplit(basename(f), "_")[[1]]
    m <- as.numeric(terra::global(terra::mean(r), "mean", na.rm = TRUE)[, 1])
    data.table(
      scenario   = parts[1],
      model      = parts[2],
      timeframe  = parts[3],
      combo      = sub("\\.tif$", "", paste(parts[-(1:3)], collapse = "_")),
      n_layers   = terra::nlyr(r),
      mean_value = m,
      file       = basename(f),
      status     = if (is.nan(m) || is.na(m)) "nan" else "ok"
    )
  }, error = function(e) {
    parts <- strsplit(basename(f), "_")[[1]]
    data.table(
      scenario = parts[1], model = parts[2], timeframe = parts[3],
      combo = NA_character_, n_layers = NA_integer_,
      mean_value = NA_real_, file = basename(f),
      status = paste0("err: ", conditionMessage(e))
    )
  })
}

# Reconstruct geoboundaries (0_server_setup.R does not export it).
geoboundaries <- arrow::read_parquet(geo_files_local[1]) |>
  sf::st_as_sf() |>
  terra::vect()
geoboundaries <- terra::aggregate(geoboundaries, "iso3")

log_section("CR-068 Stage 1 probe — categorisation asymmetry")
log_step("project_dir = %s", project_dir)
log_step("Cglabs      = %s", isTRUE(Cglabs))

args_all <- commandArgs(trailingOnly = TRUE)
get_arg <- function(name) {
  i <- match(paste0("--", name), args_all)
  if (!is.na(i) && i < length(args_all)) args_all[i + 1L] else NA_character_
}

# Subset controls (logged so the run is self-documenting).
countries_arg <- get_arg("countries")
countries_iso <- if (is.na(countries_arg)) NULL else strsplit(countries_arg, ",")[[1]]
sample_n      <- as.integer(parse_cli(args_all, "sample", "integer", default = NA_integer_))
n_workers     <- as.integer(parse_cli(args_all, "workers", "integer", default = 1L))
log_step("--countries = %s", if (is.null(countries_iso)) "ALL (full extent — slow)"
         else paste(countries_iso, collapse = ","))
log_step("--sample N  = %s", if (is.na(sample_n)) "ALL files" else sample_n)
log_step("--workers   = %d (1 = sequential)", n_workers)

aoi <- countries_aoi(geoboundaries, countries_iso)
log_step("aoi has %d feature(s); ext = %s",
         length(aoi), paste(round(as.vector(terra::ext(aoi)), 2), collapse = ", "))

# terra SpatVector objects don't survive multisession serialization;
# wrap once and pass the PackedSpatVector to each worker.
aoi_wrap <- terra::wrap(aoi)

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
log_section("Section A — class-layer per-pixel hazard means")

if (!is.na(class_dir)) {
  pattern_a <- "-[GL][0-9]+\\.tif$"
  files_a <- list.files(class_dir, pattern = pattern_a,
                        full.names = TRUE, recursive = TRUE)
  files_a <- files_a[!grepl("ENSEMBLE", files_a)]
  log_step("found %d class rasters before sampling", length(files_a))
  files_a <- sample_files(files_a, sample_n)
  log_step("after --sample: %d files to scan", length(files_a))

  if (length(files_a) == 0L) {
    log_step("No matching rasters found under class_dir — skipping Section A")
  } else {
    sample <- log_timer({
      if (n_workers > 1L) {
        log_step("parallel scan with %d workers (terra::wrap'd AOI)", n_workers)
        future::plan(future::multisession, workers = n_workers)
        on.exit(future::plan(future::sequential), add = TRUE)
        rbindlist(furrr::future_map(files_a, scan_one_class,
                                    aoi_wrap = aoi_wrap,
                                    .options = furrr::furrr_options(
                                      seed = TRUE, stdout = FALSE)),
                  fill = TRUE)
      } else {
        n_total <- length(files_a)
        progress_every <- max(1L, n_total %/% 20L)
        rbindlist(lapply(seq_along(files_a), function(i) {
          f <- files_a[i]
          if (i %% progress_every == 1L || i == n_total) {
            log_step("  [class A] %d / %d  %s", i, n_total, basename(f))
          }
          scan_one_class(f, aoi_wrap)
        }), fill = TRUE)
      }
    }, label = "Section A — read + global() per file")

    # Drop NaN/error rows from the aggregate but report the tally.
    n_total_a    <- nrow(sample)
    n_nan_a      <- sum(sample$status == "nan", na.rm = TRUE)
    n_err_a      <- sum(grepl("^err", sample$status), na.rm = TRUE)
    sample_ok    <- sample[status == "ok"]
    log_step("scanned %d class rasters: %d ok | %d NaN | %d errors",
             n_total_a, nrow(sample_ok), n_nan_a, n_err_a)
    if (n_err_a > 0L) {
      log_step("first 5 error messages:")
      print(unique(sample[grepl("^err", status), .(file, status)])[1:min(5L, .N)])
    }

    setorder(sample_ok, hazard, scenario, timeframe)
    agg_a <- sample_ok[, list(
      n_files  = .N,
      mean_avg = mean(mean_value, na.rm = TRUE),
      mean_min = min(mean_value, na.rm = TRUE),
      mean_max = max(mean_value, na.rm = TRUE)
    ), by = list(scenario, timeframe, hazard)]
    setorder(agg_a, hazard, scenario, timeframe)
    cat("\n  Mean hazard fraction by (scenario, timeframe, hazard) — top 30:\n")
    print(agg_a[order(-mean_avg)][1:min(30L, .N)])
    cat("\n  Saturated (mean > 0.7) — Candidate 1 evidence:\n")
    print(agg_a[mean_avg > 0.7])
    cat("\n  Near-zero (mean < 0.005) — Candidate 2 evidence:\n")
    print(agg_a[mean_avg < 0.005])
    results$class_means   <- agg_a
    results$class_status  <- list(total = n_total_a, ok = nrow(sample_ok),
                                  nan = n_nan_a, err = n_err_a)
  }
} else {
  log_step("[A] SKIPPED — class_dir not available")
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
log_section("Section E — Stage 1-bis: combo (_int/) means, historic vs future")

if (!is.na(int_dir)) {
  int_files <- list.files(int_dir, "\\.tif$",
                          full.names = TRUE, recursive = TRUE)
  int_files <- int_files[grepl("ENSEMBLEmean", int_files)]
  log_step("found %d ENSEMBLEmean combo files before sampling",
           length(int_files))
  int_files <- sample_files(int_files, sample_n)
  log_step("after --sample: %d files to scan", length(int_files))

  if (length(int_files) > 0L) {
    int_sample_raw <- log_timer({
      if (n_workers > 1L) {
        log_step("parallel scan with %d workers (terra::wrap'd AOI)", n_workers)
        future::plan(future::multisession, workers = n_workers)
        on.exit(future::plan(future::sequential), add = TRUE)
        rbindlist(furrr::future_map(int_files, scan_one_int,
                                    aoi_wrap = aoi_wrap,
                                    .options = furrr::furrr_options(
                                      seed = TRUE, stdout = FALSE)),
                  fill = TRUE)
      } else {
        n_total <- length(int_files)
        progress_every <- max(1L, n_total %/% 20L)
        rbindlist(lapply(seq_along(int_files), function(i) {
          f <- int_files[i]
          if (i %% progress_every == 1L || i == n_total) {
            log_step("  [int E] %d / %d  %s", i, n_total, basename(f))
          }
          scan_one_int(f, aoi_wrap)
        }), fill = TRUE)
      }
    }, label = "Section E — read + global() per file")

    # Drop NaN/error rows from the aggregate but keep the tally
    n_total_e <- nrow(int_sample_raw)
    n_nan_e   <- sum(int_sample_raw$status == "nan", na.rm = TRUE)
    n_err_e   <- sum(grepl("^err", int_sample_raw$status), na.rm = TRUE)
    int_sample <- int_sample_raw[status == "ok"]
    log_step("scanned %d int rasters: %d ok | %d NaN | %d errors",
             n_total_e, nrow(int_sample), n_nan_e, n_err_e)

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
log_step("Wrote report to %s", out_path)
summarize_log()

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
