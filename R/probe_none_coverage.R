#!/usr/bin/env Rscript
# R/probe_none_coverage.R
# =======================
# Issue #9 (bjyberg): hazard='none' present for only ONE hazard combination in
# the hazard_exposure product. This READ-ONLY probe locates where 'none' is
# missing along the R/2 §5.2 -> R/2 §5.3 -> R/3 §4 chain so the rebuild can be
# scoped to the minimum (a full §5.2 re-run is ~26 h; §5.3 alone is ~1-3 h).
#
# Timeline that makes this probe necessary:
#   2026-05-26 41c1c00  'none' layer added to §5.2 interaction stacks
#   2026-05-27/28       Stage F ran §5.2 under FORCE_OVERWRITE=1 (both timeframes)
#   2026-07-xx          §5.3 rebuilt ONLY for the livestock NDWS combos (pre-delete)
#   -> hypothesis: §5.2 stacks all carry 'none'; §5.3 _int stacks for the
#      non-NDWS combos are stale (pre-05-26) -> R/3 output has 'none' for one combo.
#
# Checks per timeframe:
#   A) §5.2 stacks  hazard_timeseries_int/<tf>/<scenario>_<model>_<timeframe>_<combo>.tif
#      per combo: n files; does the historic ENSEMBLEmean stack carry a *_none
#      layer; does one per-GCM stack carry it.
#   B) §5.3 stacks  hazard_risk/<tf>/<crop>_<model>_<sev>_<combo>_int.tif
#      per combo: n files, n ENSEMBLEmean files, n of those with a *_none layer.
#   C) R/3 §4.2     hazard_risk_vop_usd/<tf>/haz-freq-exp_*_ENSEMBLEmean_int_adm_<sev>.parquet
#      hazard_vars x hazard row counts (arrow, nothing materialised).
# Verdict per timeframe -> which tier to rebuild.
#
# Run (cglabs, repo root):  Rscript R/probe_none_coverage.R            (~1-5 min)
# Optional: PROBE_TIMEFRAMES=jagermeyr  to restrict.

t0 <- Sys.time()
.ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) cat(sprintf("[%s] [probe-none] %s\n", .ts(), sprintf(fmt, ...)))
.elapsed <- function(from) sprintf("%.1f min", as.numeric(difftime(Sys.time(), from, units = "mins")))

setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else
  file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
if (nzchar(Sys.getenv("ATLAS_SETUP_SKIP"))) {
  .log("ATLAS_SETUP_SKIP set - not sourcing setup; expecting atlas_dirs (+ exposure_dir) in the calling env")
  stopifnot(exists("atlas_dirs"))
} else {
  .log("sourcing %s", setup)
  suppressMessages(suppressWarnings(source(setup)))
}
suppressPackageStartupMessages({ pacman::p_load(terra, arrow, dplyr, data.table) })
.log("working dir = %s", getwd())

layer_names <- function(f) {
  tryCatch(names(suppressWarnings(terra::rast(f))), error = function(e) NA_character_)
}
has_none <- function(f) {
  nm <- layer_names(f)
  if (length(nm) == 1 && is.na(nm)) return(NA)
  any(grepl("_none($|_)", nm))
}

int_root  <- atlas_dirs$data_dir$hazard_timeseries_int
risk_root <- atlas_dirs$data_dir$hazard_risk
vusd_root <- atlas_dirs$data_dir$hazard_risk_vop_usd
vint_root <- atlas_dirs$data_dir$hazard_risk_vop
.log("dirs: int=%s | risk=%s | vop_usd=%s | vop=%s", int_root, risk_root, vusd_root, vint_root)

timeframes <- basename(list.dirs(int_root, recursive = FALSE))
tf_env <- Sys.getenv("PROBE_TIMEFRAMES", "")
if (nzchar(tf_env)) timeframes <- intersect(timeframes, strsplit(tf_env, ",")[[1]])
if (!length(timeframes)) { .log("no timeframe dirs under %s — nothing to probe", int_root); quit(status = 2) }
.log("timeframes: %s", paste(timeframes, collapse = ", "))

verdicts <- list()

for (tf in timeframes) {
  t_tf <- Sys.time()
  cat("\n", strrep("=", 78), "\n", sep = ""); .log("TIMEFRAME %s", tf); cat(strrep("=", 78), "\n")

  ## ---- A) §5.2 interaction stacks -------------------------------------------
  int_dir <- file.path(int_root, tf)
  a_files <- list.files(int_dir, "\\.tif$", full.names = TRUE)
  .log("A) §5.2 stacks in %s: %d tifs", int_dir, length(a_files))
  A_ens_ok <- A_gcm_ok <- NA
  a_combos <- character(0)
  if (length(a_files)) {
    tok <- strsplit(tools::file_path_sans_ext(basename(a_files)), "_", fixed = TRUE)
    ok4 <- lengths(tok) == 4L
    if (any(!ok4)) .log("A) WARN %d filenames do not split into 4 tokens (ignored), e.g. %s",
                        sum(!ok4), basename(a_files[!ok4][1]))
    A <- rbindlist(lapply(tok[ok4], as.list)); setnames(A, c("scenario", "model", "timeframe", "combo"))
    A[, file := a_files[ok4]]
    a_combos <- sort(unique(A$combo))
    a_tab <- A[, .(n_files = .N, n_models = uniqueN(model),
                   n_ensmean = sum(model == "ENSEMBLEmean")), by = combo][order(combo)]
    a_tab[, ens_hist_none := vapply(combo, function(cb) {
      f <- A[combo == cb & model == "ENSEMBLEmean" & scenario == "historic", file]
      if (!length(f)) f <- A[combo == cb & model == "ENSEMBLEmean", file]
      if (!length(f)) return(NA); has_none(f[1]) }, logical(1))]
    a_tab[, gcm_sample_none := vapply(combo, function(cb) {
      f <- A[combo == cb & !grepl("^ENSEMBLE", model) & scenario == "historic", file]
      if (!length(f)) f <- A[combo == cb & !grepl("^ENSEMBLE", model), file]
      if (!length(f)) return(NA); has_none(f[1]) }, logical(1))]
    print(a_tab, nrows = 100)
    A_ens_ok <- all(a_tab$ens_hist_none %in% TRUE)
    A_gcm_ok <- all(a_tab$gcm_sample_none %in% TRUE)
    .log("A) ENSEMBLEmean stacks carry none for ALL combos: %s | per-GCM sample carries none: %s", A_ens_ok, A_gcm_ok)
  }

  ## ---- B) §5.3 per-crop _int stacks -----------------------------------------
  risk_dir <- file.path(risk_root, tf)
  b_files <- list.files(risk_dir, "_int\\.tif$", full.names = TRUE)
  .log("B) §5.3 _int stacks in %s: %d tifs", risk_dir, length(b_files))
  B_ok <- NA; b_combos <- character(0)
  if (length(b_files)) {
    rx <- "^(.+?)_([^_]+)_(moderate|severe|extreme)_(.+)_int$"
    base <- tools::file_path_sans_ext(basename(b_files))
    m <- regmatches(base, regexec(rx, base))
    okm <- lengths(m) == 5L
    if (any(!okm)) .log("B) WARN %d _int filenames did not match <crop>_<model>_<sev>_<combo>_int (ignored), e.g. %s",
                        sum(!okm), base[!okm][1])
    B <- rbindlist(lapply(m[okm], function(x) as.list(x[2:5]))); setnames(B, c("crop", "model", "sev", "combo"))
    B[, file := b_files[okm]]
    b_combos <- sort(unique(B$combo))
    ens <- B[model == "ENSEMBLEmean"]
    .log("B) checking *_none layer on %d ENSEMBLEmean _int stacks (header reads)…", nrow(ens))
    ens[, none := vapply(file, has_none, logical(1))]
    b_tab <- merge(
      B[, .(n_files = .N, n_crops = uniqueN(crop), n_models = uniqueN(model)), by = combo],
      ens[, .(n_ensmean = .N, n_ensmean_with_none = sum(none %in% TRUE),
              mtime_min = format(min(file.mtime(file)), "%Y-%m-%d"),
              mtime_max = format(max(file.mtime(file)), "%Y-%m-%d")), by = combo],
      by = "combo", all = TRUE)[order(combo)]
    print(b_tab, nrows = 100)
    B_ok <- nrow(ens) > 0 && all(ens$none %in% TRUE)
    .log("B) ALL ENSEMBLEmean _int stacks carry none: %s", B_ok)
    only_a <- setdiff(a_combos, b_combos); only_b <- setdiff(b_combos, a_combos)
    if (length(only_a)) .log("B) combos with §5.2 stacks but NO _int files (deleted/never combined): %s", paste(only_a, collapse = ", "))
    if (length(only_b)) .log("B) combos with _int files but NO §5.2 stacks (§5.3 would stop()): %s", paste(only_b, collapse = ", "))
  }

  ## ---- C) R/3 §4.2 ENSEMBLEmean parquets ------------------------------------
  for (root in c(vusd_root, vint_root)) {
    pq <- list.files(file.path(root, tf), "_ENSEMBLEmean_int_adm_.*\\.parquet$", full.names = TRUE)
    .log("C) %s/%s: %d ENSEMBLEmean parquets", root, tf, length(pq))
    for (f in pq) {
      cnt <- tryCatch(
        arrow::open_dataset(f) |> dplyr::count(hazard_vars, hazard) |> dplyr::collect() |> as.data.table(),
        error = function(e) { .log("C) arrow count failed on %s: %s", basename(f), conditionMessage(e)); NULL })
      if (is.null(cnt)) next
      wide <- dcast(cnt, hazard_vars ~ hazard, value.var = "n", fill = 0L)
      cat(sprintf("\n  %s  (mtime %s, %.0f MB)\n", basename(f), format(file.mtime(f), "%Y-%m-%d %H:%M"), file.size(f) / 1e6))
      print(wide, nrows = 50)
      miss <- if ("none" %in% names(wide)) wide[none == 0, hazard_vars] else wide$hazard_vars
      .log("C) %s: combos WITHOUT none = %s", basename(f), if (length(miss)) paste(miss, collapse = ", ") else "<none missing>")
    }
  }

  ## ---- Verdict ----------------------------------------------------------------
  v <- if (isTRUE(B_ok)) {
    "TIER 0: §5.2 + §5.3 both carry none -> only R/3 §4 is stale -> FORCE_OVERWRITE=1 R/3 (~12 h)"
  } else if (isTRUE(A_ens_ok)) {
    "TIER A: §5.2 stacks OK, §5.3 _int stale -> pre-delete hazard_risk/<tf>/*_int.tif + RUN_R2_RUN5_3=1 (no FORCE), then FORCE R/3"
  } else if (isTRUE(A_gcm_ok)) {
    "TIER B: per-GCM §5.2 stacks OK but ENSEMBLE stacks stale -> pre-delete *_ENSEMBLEmean_*/*_ENSEMBLEsd_* in hazard_timeseries_int/<tf>, RUN_R2_RUN5_2=1 (ensemble rebuilds missing), then TIER A"
  } else {
    "TIER C: per-GCM §5.2 stacks lack none -> full §5.2 rebuild needed (FORCE, ~26 h both tf) -> then §5.3 -> R/3"
  }
  verdicts[[tf]] <- v
  .log("VERDICT %s: %s", tf, v)
  .log("timeframe %s done in %s", tf, .elapsed(t_tf))
}

cat("\n", strrep("-", 78), "\n", sep = "")
for (tf in names(verdicts)) cat(sprintf("  %-10s %s\n", tf, verdicts[[tf]]))
.log("probe complete in %s", .elapsed(t0))
