#!/usr/bin/env Rscript
# R/obs/s6_rangeland_fpar.R
# =========================
# Rangeland drought V1 (PLAN_obs_hazard_pipeline.md §3, decisions D4 + §3.4): JRC ASAP FPAR, aggregated by
# python/obs/fpar_ingest.py to 0.25 deg rangeland-weighted dekadal means, turned into a per-season forage-deficit
# class and an R/3 livestock bundle.
#
#   1. seasons from the ASAP phenology modes (static/pheno_s{1,2}, pheno_e{1,2}, pheno_n): dekad indices in the v05
#      1..108 three-year frame (37-72 = year 2, 73-108 = year 3). A season starting in year y covers dekads
#      [((S-1) %% 36)+1 .. +L-1] counted from dekad 1 of y, L = E - S + 1 (NA if E < S or L > 36), so a season that
#      ends in the following calendar year is summed across the year boundary. Cells with pheno_n == 1 have one season.
#   2. cumulative FPAR over each season, per start year (na.rm = FALSE: an incomplete season is NA)
#   3. per season k, the standardised anomaly z_k(y) against the mean/sd of ALL start years; the yearly score
#      z(y) = min over seasons (the worse season of a bimodal year)
#   4. severity by EMPIRICAL PERCENTILES of z over the start years, per cell:
#        moderate  z <= Q(0.20)      severe  z <= Q(0.10)      extreme  z <= Q(0.05)     (quantile type 1: no interpolation)
#      so baseline P is ceiling(n p)/n = 4/19, 2/19, 1/19 for 19 start years BY CONSTRUCTION (PLAN §3b: a design
#      constant, not a measurement - the information is in WHICH years and in the magnitude layers rel_deficit_year /
#      worst_year_deficit / deficit_q20, written alongside; frozen
#      thresholds are written out so other periods can be classified against them)
#   5. consecutive failure: moderate in y and y-1 (a separate layer, not an R/3 class)
#   6. an R/3 bundle per livestock species-system: dry = FPAR deficit class, heat = THI_max (from the S3 annual12
#      stack if present, else 0), wet = NDWL0 annual12 (if present, else 0); set name `FPARz+THI-max+NDWL0`
#
# Inputs  <OBS_ROOT>/Data/obs_fpar/dekads/fpar_rl_YYYYMMDD.tif, <OBS_ROOT>/Data/obs_fpar/static/pheno_*.tif
#         optional <OBS_ROOT>/Data/obs_seasonal/<thi_token>_<timeframe>/annual12/{THI_max_max,NDWL0_mean}.tif
# Outputs <OBS_ROOT>/Data/obs_fpar/seasonal/<timeframe>/{cum_s1,cum_s2,z_year}.tif   (layer per start year)
#         <OBS_ROOT>/Data/obs_fpar/seasonal/<timeframe>/thresholds_z.tif              (3 layers: q20, q10, q05)
#         <OBS_ROOT>/Data/obs_fpar/seasonal/<timeframe>/consecutive_failure.tif         (layer per year, 0/1)
#         <OBS_ROOT>/Data/obs_codes/<token>_<timeframe>/<species>_<token>_FPARz+THI-max+NDWL0_<sev>_codes.tif
#         <OBS_ROOT>/Data/hazard_risk/<period>/<species>_<token>_<sev>_FPARz+THI-max+NDWL0_int.tif
#
# Usage: Rscript R/obs/s6_rangeland_fpar.R --obs-root <abs> --years 2005:2025 [--token chirps-chirts-agera5]
#          [--thi-token ...] [--period ggcmi-crop] [--rl-min 0.1] [--spinup 1] [--no-thi] [--overwrite]   |   --selftest
#   --spinup: drop the first N start years so the seasons and the timeframe label match S3-S5 (default 1).
#   --no-thi: allow a DIAGNOSTIC run without the S3 THI/NDWL0 stacks (heat/wet slots 0); the bundles then go under
#            Data/hazard_risk_DIAGNOSTIC/<period>/, which R/3 never reads.
suppressPackageStartupMessages({ library(terra); library(data.table) })
.t0 <- Sys.time()
lg <- function(...) cat(sprintf("[%s | +%7.1fs] [S6] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
                                as.numeric(difftime(Sys.time(), .t0, units = "secs")), paste0(...)))
stopf <- function(...) stop("[S6] STOP: ", sprintf(...), call. = FALSE)
repo <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else {
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd()
}
S35 <- file.path(repo, "R", "obs", "s3_s5_obs_hazards.R")
sys.source(S35, envir = (s35 <- new.env()))        # CLI guard keeps it from running; we reuse its helpers
SET_NAME <- "FPARz+THI-max+NDWL0"
SEVS <- c(moderate = 0.20, severe = 0.10, extreme = 0.05)
LIVESTOCK <- as.vector(outer(c("cattle", "goats", "pigs", "poultry", "sheep"), c("highland", "tropical"), paste, sep = "-"))

# ---- dekad stack -----------------------------------------------------------------------------------------
dekad_dates <- function(years) as.Date(sprintf("%d-%02d-%02d", rep(years, each = 36), rep(rep(1:12, each = 3), length(years)), rep(c(1, 11, 21), 12 * length(years))))
load_dekads <- function(obs_root, years) {
  d <- dekad_dates(years)
  f <- file.path(obs_root, "Data", "obs_fpar", "dekads", sprintf("fpar_rl_%s.tif", format(d, "%Y%m%d")))
  miss <- !file.exists(f)
  if (any(miss)) stopf("%d dekad file(s) missing (first %s). The ingest skips dekads younger than its --min-age; choose --years accordingly", sum(miss), basename(f[miss][1]))
  r <- rast(f); names(r) <- format(d, "%Y%m%d"); r
}

# ---- seasons ----------------------------------------------------------------------------------------------
season_windows <- function(S, E) {
  # S, E: dekad-index rasters (1..72). Returns list(first = dekad-of-year of the start, last = first + L - 1), NA where invalid
  L <- E - S + 1
  first <- ((S - 1) %% 36) + 1
  last <- first + L - 1
  bad <- is.na(S) | is.na(E) | L < 1 | L > 36
  list(first = ifel(bad, NA, first), last = ifel(bad, NA, last))
}
cum_season <- function(dek, win, years) {
  # per start year y: sum of FPAR over [first .. last] counted from dekad 1 of y, within a 72-dekad slice
  n <- length(years) - 1
  if (n < 1) stopf("need at least 2 data years")
  out <- lapply(seq_len(n), function(m) {
    x <- dek[[(1 + 36 * (m - 1)):(72 + 36 * (m - 1))]]
    s <- rapp(x, first = win$first, last = win$last, fun = sum, na.rm = FALSE)
    names(s) <- years[m]; s
  })
  rast(out)
}
zscore <- function(cum) {                      # per cell, over layers (start years); sd 0 or < 2 values -> NA
  m <- mean(cum, na.rm = TRUE); s <- stdev(cum, na.rm = TRUE, pop = FALSE)
  z <- (cum - m) / s
  ifel(s > 0, z, NA)
}
qthresholds <- function(z, probs = SEVS) {
  # empirical quantiles per cell (type 1: an observed value, no interpolation), NA if any layer NA
  app(z, function(v) if (anyNA(v)) rep(NA_real_, length(probs)) else as.numeric(quantile(v, probs = probs, type = 1, names = FALSE)))
}

# ---- driver -------------------------------------------------------------------------------------------------
run <- function(obs_root, years, token, thi_token = token, period = "ggcmi-crop", rl_min = 0.1, overwrite = FALSE, scenario = "observed",
                spinup = 1L, no_thi = FALSE) {
  overwrite <- overwrite || tolower(Sys.getenv("FORCE_OVERWRITE", "")) %in% c("1", "true", "t", "yes", "y", "on")
  fdir <- file.path(obs_root, "Data", "obs_fpar"); sdir <- file.path(fdir, "static")
  # the same season set and timeframe label as S3-S5 (min(years)+spinup .. max(years); start years after the spin-up),
  # so the THI/NDWL0 stacks line up and one parquet never carries two timeframes
  timeframe <- sprintf("%d-%d", min(years) + spinup, max(years)); all_start <- years[-length(years)]
  sea_keep <- seq_along(all_start); if (spinup > 0) sea_keep <- sea_keep[-seq_len(spinup)]   # x[-integer(0)] is empty, hence the guard
  if (!length(sea_keep)) stopf("spinup=%d leaves no seasons", spinup)
  start_years <- all_start[sea_keep]
  odir <- file.path(fdir, "seasonal", timeframe); dir.create(odir, FALSE, TRUE)
  lg(sprintf("obs_root=%s years=%d-%d start years=%d token=%s rl_min=%s", obs_root, min(years), max(years), length(start_years), token, rl_min))
  dek <- load_dekads(obs_root, years)
  rl <- rast(file.path(sdir, "rl_frac.tif"))
  if (!compareGeom(dek, rl, stopOnError = FALSE)) stopf("dekads and static layers are not on the same grid")
  keep <- rl >= rl_min
  dek <- mask(dek, keep, maskvalues = FALSE)
  ph <- lapply(c("s1", "e1", "s2", "e2", "n"), function(k) rast(file.path(sdir, sprintf("pheno_%s.tif", k)))); names(ph) <- c("s1", "e1", "s2", "e2", "n")
  # 1-2. cumulative FPAR per season
  w1 <- season_windows(ph$s1, ph$e1); cum1 <- cum_season(dek, w1, years)[[sea_keep]]
  two <- ph$n >= 2
  w2 <- season_windows(ifel(two, ph$s2, NA), ifel(two, ph$e2, NA)); cum2 <- cum_season(dek, w2, years)[[sea_keep]]
  writeRaster(cum1, file.path(odir, "cum_s1.tif"), overwrite = TRUE); writeRaster(cum2, file.path(odir, "cum_s2.tif"), overwrite = TRUE)
  len1 <- values(w1$last - w1$first + 1)[, 1]; len2 <- values(w2$last - w2$first + 1)[, 1]
  lg(sprintf("season 1: length dekads %s..%s on %d cells; season 2 on %d cells (%s..%s)", min(len1, na.rm = TRUE), max(len1, na.rm = TRUE), sum(!is.na(len1)),
             sum(!is.na(len2)), if (any(!is.na(len2))) min(len2, na.rm = TRUE) else "-", if (any(!is.na(len2))) max(len2, na.rm = TRUE) else "-"))
  n_rl <- sum(values(keep)[, 1], na.rm = TRUE); n_nosea <- sum(values(keep & is.na(w1$first))[, 1], na.rm = TRUE)
  lg(sprintf("rangeland cells (rl >= %s): %d; without a valid season-1 window (dropped silently from every layer): %d", rl_min, n_rl, n_nosea))
  if (n_nosea > 0.01 * n_rl) lg("WARN: more than 1 % of rangeland cells have no valid season window - inspect the phenology modes")
  # "OND-first" cells encode s1 in frame-year 1 and s2 in frame-year 2 (season 2 is the NEXT calendar year's MAM):
  # the modulo window sums each season in its own calendar year, so for those cells the pastoral year y pairs
  # season 1 of y with season 2 of y + 1 (the KNBS OND(Y-1)+MAM(Y) convention) - a one-layer shift of the season-2 series
  nxt <- !is.na(ph$s2) & (floor((ph$s2 - 1) / 36) > floor((ph$s1 - 1) / 36))
  n_nxt <- sum(values(nxt)[, 1], na.rm = TRUE)
  lg(sprintf("two-season cells whose season 2 falls in the following calendar year (season-2 series shifted by one start year there): %d", n_nxt))
  shift_next <- function(x) { y <- c(x[[-1]], setValues(x[[1]], NA)); names(y) <- names(x); ifel(nxt, y, x) }
  # 3. z per season, yearly score = worse season
  z1 <- zscore(cum1); z2 <- shift_next(zscore(cum2))
  zy <- ifel(is.na(z2), z1, min(z1, z2)); names(zy) <- start_years
  writeRaster(zy, file.path(odir, "z_year.tif"), overwrite = TRUE)
  # baseline moments: what a later period needs to be classified against these frozen thresholds (mean/sd per season)
  mom <- c(mean(cum1, na.rm = TRUE), stdev(cum1, na.rm = TRUE, pop = FALSE), mean(cum2, na.rm = TRUE), stdev(cum2, na.rm = TRUE, pop = FALSE),
           sum(!is.na(cum1)))
  names(mom) <- c("mean_s1", "sd_s1", "mean_s2", "sd_s2", "n_years"); writeRaster(mom, file.path(odir, "baseline_moments.tif"), overwrite = TRUE)
  # magnitude layers (the frequencies below are design constants; these carry the size of the deficit):
  #   rel_deficit_year = min over seasons of (cum - mean) / mean per start year; worst_year_deficit = its minimum over years;
  #   deficit_q20 = its 20th percentile (type 1) = the relative deficit of a 1-in-5 year
  rd1 <- (cum1 - mom[["mean_s1"]]) / mom[["mean_s1"]]; rd2 <- shift_next((cum2 - mom[["mean_s2"]]) / mom[["mean_s2"]])
  rdy <- ifel(is.na(rd2), rd1, min(rd1, rd2)); names(rdy) <- start_years
  writeRaster(rdy, file.path(odir, "rel_deficit_year.tif"), overwrite = TRUE)
  mag <- c(min(rdy, na.rm = TRUE), app(rdy, function(v) if (anyNA(v)) NA_real_ else as.numeric(quantile(v, 0.2, type = 1, names = FALSE))))
  names(mag) <- c("worst_year_deficit", "deficit_q20"); writeRaster(mag, file.path(odir, "deficit_magnitude.tif"), overwrite = TRUE)
  lg(sprintf("relative deficit of a 1-in-5 year (deficit_q20): median %.3f over rangeland cells; worst year: median %.3f",
             median(values(mag[["deficit_q20"]])[, 1], na.rm = TRUE), median(values(mag[["worst_year_deficit"]])[, 1], na.rm = TRUE)))
  # 4. percentile thresholds (frozen) and classes
  thr <- qthresholds(zy); names(thr) <- paste0("q", sprintf("%02d", round(100 * SEVS)))
  writeRaster(thr, file.path(odir, "thresholds_z.tif"), overwrite = TRUE)
  fail <- lapply(names(SEVS), function(sv) { f <- ifel(zy <= thr[[paste0("q", sprintf("%02d", round(100 * SEVS[[sv]])))]], 1, 0); names(f) <- start_years; f })
  names(fail) <- names(SEVS)
  n_y <- length(start_years); pf <- values(mean(fail$moderate))[, 1]
  # without ties P = ceiling(n p)/n exactly; one tie at the quantile makes it (k+1)/n, so test against that (all three tiers)
  tied <- vapply(names(SEVS), function(sv) { k <- ceiling(n_y * SEVS[[sv]]); sum(values(mean(fail[[sv]]))[, 1] >= (k + 1) / n_y - 1e-9, na.rm = TRUE) }, numeric(1))
  n_dist <- app(zy, function(v) if (anyNA(v)) NA_real_ else length(unique(v))); few <- sum(values(n_dist)[, 1] < n_y, na.rm = TRUE)
  lg(sprintf("baseline P(moderate) per cell: min %.3f median %.3f max %.3f over %d rangeland cells (by construction %d/%d = %.3f); cells with a tie at the quantile: moderate %d severe %d extreme %d; cells with fewer than %d distinct z: %d",
             min(pf, na.rm = TRUE), median(pf, na.rm = TRUE), max(pf, na.rm = TRUE), sum(!is.na(pf)), ceiling(n_y * SEVS[["moderate"]]), n_y, ceiling(n_y * SEVS[["moderate"]]) / n_y,
             tied[["moderate"]], tied[["severe"]], tied[["extreme"]], n_y, few))
  if (max(tied) > 0.01 * sum(!is.na(pf))) lg("WARN: more than 1 % of rangeland cells have tied z at a class quantile (constant or zero FPAR; the extreme tier doubles on a tie at the minimum) - inspect before use")
  # 5. consecutive failure (moderate in y and y-1)
  cf <- fail$moderate[[-1]] * fail$moderate[[-nlyr(fail$moderate)]]; names(cf) <- start_years[-1]
  writeRaster(cf, file.path(odir, "consecutive_failure.tif"), overwrite = TRUE)
  k_m <- ceiling(n_y * SEVS[["moderate"]])
  lg(sprintf("consecutive-failure frequency: domain mean %.3f (no-persistence expectation with exactly %d of %d flagged: k(k-1)/(n(n-1)) = %.3f)",
             mean(values(mean(cf))[, 1], na.rm = TRUE), k_m, n_y, k_m * (k_m - 1) / (n_y * (n_y - 1))))
  # 6. R/3 bundles per livestock species-system
  sea <- file.path(obs_root, "Data", "obs_seasonal", paste0(thi_token, "_", timeframe), "annual12")
  hc <- s35$load_thresholds(file.path(repo, "metadata", "haz_classes.csv"))
  thi <- if (file.exists(file.path(sea, "THI_max_max.tif"))) rast(file.path(sea, "THI_max_max.tif")) else NULL
  ndwl <- if (file.exists(file.path(sea, "NDWL0_mean.tif"))) rast(file.path(sea, "NDWL0_mean.tif")) else NULL
  if (is.null(thi) || is.null(ndwl)) {
    if (!no_thi) stopf("S3 annual12 stacks not found under %s (THI_max_max.tif / NDWL0_mean.tif). Run S3-S5 first, or pass --no-thi for a DIAGNOSTIC run whose heat/wet slots are 0", sea)
    lg("--no-thi: heat and wet slots are 0 in these bundles (DIAGNOSTIC ONLY, not for R/3/publish)")
  } else {
    if (!all(as.character(start_years) %in% names(thi))) stopf("THI stack seasons %s..%s do not cover %s..%s", names(thi)[1], tail(names(thi), 1), start_years[1], tail(start_years, 1))
    thi <- thi[[as.character(start_years)]]; ndwl <- ndwl[[as.character(start_years)]]
  }
  codes_dir <- file.path(obs_root, "Data", "obs_codes", paste0(token, "_", timeframe)); dir.create(codes_dir, FALSE, TRUE)
  # --no-thi bundles never share the R/3 input directory with real ones (R/3 cannot tell them apart by name)
  out_dir <- file.path(obs_root, "Data", if (is.null(thi)) "hazard_risk_DIAGNOSTIC" else "hazard_risk", period); dir.create(out_dir, FALSE, TRUE)
  prefix <- paste(scenario, token, timeframe, sep = "_"); n_out <- 0
  for (sp in LIVESTOCK) for (sv in names(SEVS)) {
    bundle_f <- file.path(out_dir, sprintf("%s_%s_%s_%s_int.tif", sp, token, sv, SET_NAME))
    if (file.exists(bundle_f) && !overwrite) next
    dry <- fail[[sv]]
    heat <- if (is.null(thi)) dry * 0 else { th <- s35$threshold_for(hc, "THI_max", sv, "livestock", sp); s35$classify(thi, th$direction, th$threshold) }
    wet <- if (is.null(ndwl)) dry * 0 else { th <- s35$threshold_for(hc, "NDWL0", sv, "livestock", sp); s35$classify(ndwl, th$direction, th$threshold) }
    codes <- s35$code_stack(dry, heat, wet); names(codes) <- start_years
    writeRaster(codes, file.path(codes_dir, sprintf("%s_%s_%s_%s_codes.tif", sp, token, SET_NAME, sv)), overwrite = TRUE, datatype = "INT1U", NAflag = 255)
    b <- s35$freq_from_codes(codes, prefix); names(b) <- paste0(names(b), "_", SET_NAME, "_", sp, "_", sv)
    s35$gate_bundle(b, basename(bundle_f))
    writeRaster(b, bundle_f, overwrite = TRUE, filetype = "COG", gdal = c("OVERVIEWS=NONE", "COMPRESS=ZSTD")); n_out <- n_out + 1
  }
  lg(sprintf("done: %d bundles (%s) under %s", n_out, SET_NAME, out_dir)); invisible(out_dir)
}

# ---- selftest ---------------------------------------------------------------------------------------------
selftest <- function() {
  ok <- function(c, m) if (!isTRUE(c)) stop("SELFTEST FAIL: ", m) else cat("  ok  ", m, "\n")
  td <- file.path(tempdir(), "s6_selftest"); unlink(td, recursive = TRUE)
  base <- rast(nrows = 3, ncols = 4, xmin = 30, xmax = 31, ymin = 0, ymax = 0.75, crs = "EPSG:4326")
  years <- 2005:2015                                  # 10 start years
  dd <- file.path(td, "Data", "obs_fpar", "dekads"); sd <- file.path(td, "Data", "obs_fpar", "static"); dir.create(dd, FALSE, TRUE); dir.create(sd, FALSE, TRUE)
  # FPAR = 50 (+ a tiny year trend so no two years tie) except: cell (1,1) season-1 dekads in 2008 = 10 (one bad year);
  # cell (1,2) is a no-season cell; cell (3,4) is constant (sd 0 -> NA)
  dates <- dekad_dates(years)
  for (i in seq_along(dates)) {
    y <- as.integer(format(dates[i], "%Y")); dk <- ((i - 1) %% 36) + 1
    r <- setValues(base, 50 + (y - 2005) * 0.01); r[3, 4] <- 50
    if (y == 2008 && dk %in% 10:15) r[1, 1] <- 10            # 6 dekads x (50 -> 10)
    if (y == 2011 && dk %in% 10:15) r[1, 3] <- 20            # a milder bad year in cell (1,3)
    if (y == 2012 && dk %in% 10:15) r[1, 3] <- 20            # ... two in a row -> consecutive failure
    if (y == 2010 && dk %in% 8:11) r[3, 1] <- 10             # OND-first cell (3,1): its season 2 (MAM 2010) fails -> pastoral year 2009
    writeRaster(r, file.path(dd, sprintf("fpar_rl_%s.tif", format(dates[i], "%Y%m%d"))), overwrite = TRUE)
  }
  writeRaster(setValues(base, 1), file.path(sd, "rl_frac.tif"), overwrite = TRUE)
  s1 <- setValues(base, 10L); e1 <- setValues(base, 15L); n <- setValues(base, 1L)    # season 1 = dekads 10..15
  s1[1, 2] <- NA; e1[1, 2] <- NA; n[1, 2] <- NA                                      # no phenology
  s1[2, 1] <- 70L; e1[2, 1] <- 75L                                                   # start > 36: dekad 34, crossing into the next year
  s2 <- setValues(base, NA); e2 <- setValues(base, NA)
  s1[3, 1] <- 30L; e1[3, 1] <- 33L; s2[3, 1] <- 44L; e2[3, 1] <- 47L; n[3, 1] <- 2L   # OND-first: s1 frame-year 1, s2 frame-year 2 (= MAM of y+1)
  writeRaster(s1, file.path(sd, "pheno_s1.tif"), overwrite = TRUE); writeRaster(e1, file.path(sd, "pheno_e1.tif"), overwrite = TRUE)
  writeRaster(s2, file.path(sd, "pheno_s2.tif"), overwrite = TRUE); writeRaster(e2, file.path(sd, "pheno_e2.tif"), overwrite = TRUE)
  writeRaster(n, file.path(sd, "pheno_n.tif"), overwrite = TRUE)
  # no S3 stacks here: must STOP without --no-thi, run with it (spinup = 0 so all 10 start years are kept, as the assertions expect)
  r <- try(run(td, years, "obstest", rl_min = 0.5, overwrite = TRUE, spinup = 0L), silent = TRUE)
  ok(inherits(r, "try-error") && grepl("annual12 stacks not found", r), "without the S3 THI/NDWL0 stacks S6 stops unless --no-thi")
  out <- run(td, years, "obstest", rl_min = 0.5, overwrite = TRUE, spinup = 0L, no_thi = TRUE)
  od <- file.path(td, "Data", "obs_fpar", "seasonal", "2005-2015")
  cum <- rast(file.path(od, "cum_s1.tif"))
  ok(nlyr(cum) == 10 && identical(names(cum), as.character(2005:2014)), "10 start years, labelled")
  ok(abs(values(cum[[1]])[1, 1] - 300) < 1e-6 && abs(values(cum[["2008"]])[1, 1] - 60) < 1e-6, "cumulative FPAR over dekads 10-15: 300 normally, 60 in the bad year")
  ok(is.na(values(cum[[1]])[2, 1]), "no-phenology cell is NA")
  zy9 <- values(rast(file.path(od, "z_year.tif")))[9, ]
  ok(names(zy9)[which.min(zy9)] == "2009" && is.na(zy9[["2014"]]) == FALSE,
     "OND-first cell: a failed MAM 2010 (season 2, frame-year 2) scores pastoral year 2009 = OND 2009 + MAM 2010; the last year keeps season 1 only")
  ok(abs(values(cum[[1]])[5, 1] - (300 + 0.01 * 3)) < 1e-3, "start dekad 70 (= 34) crossing into the next year sums 3 dekads of y and 3 of y+1 (float32 tolerance)")
  zy <- rast(file.path(od, "z_year.tif")); thr <- rast(file.path(od, "thresholds_z.tif"))
  ok(values(zy[["2008"]])[1, 1] < -2 && all(values(zy)[1, -4] > 0), "z: the bad year is far below, the others slightly above the mean")
  ok(is.na(values(zy[[1]])[12, 1]), "a cell with constant FPAR (sd 0) is NA, not 0")
  bund <- rast(file.path(out, "cattle-tropical_obstest_moderate_FPARz+THI-max+NDWL0_int.tif"))
  v <- values(bund)[1, ]; nm <- sub("observed_obstest_2005-2015_", "", names(bund)); nm <- sub("_FPARz.*", "", nm)
  ok(nlyr(bund) == 9 && abs(v[nm == "dry"] - 0.2) < 1e-6 && abs(v[nm == "none"] - 0.8) < 1e-6,
     "moderate = Q20 of 10 years -> 2 of 10 years are 'dry' (P = 0.2 by construction), heat/wet 0 without THI/NDWL0 stacks")
  be <- rast(file.path(out, "cattle-tropical_obstest_extreme_FPARz+THI-max+NDWL0_int.tif")); ve <- values(be)[1, ]
  ok(abs(ve[nm == "dry"] - 0.1) < 1e-6, "extreme = Q05 of 10 years -> 1 of 10 (type-1 quantile picks the worst observed year)")
  cf <- rast(file.path(od, "consecutive_failure.tif"))
  ok(values(cf[["2012"]])[3, 1] == 1 && values(cf[["2012"]])[1, 1] == 0, "consecutive failure: cell (1,3) fails 2011 and 2012 -> 1 in 2012; cell (1,1) does not")
  ok(length(list.files(out, "FPARz.*_int\\.tif$")) == 30, "10 species-systems x 3 severities = 30 bundles")
  # spinup = 1: timeframe and seasons as S3-S5 label them (2006-2015, start years 2006-2014)
  run(td, years, "obstest", rl_min = 0.5, overwrite = TRUE, spinup = 1L, no_thi = TRUE)
  zs <- rast(file.path(td, "Data", "obs_fpar", "seasonal", "2006-2015", "z_year.tif"))
  ok(nlyr(zs) == 9 && names(zs)[1] == "2006", "spinup = 1 drops the first start year and labels the timeframe 2006-2015 like S3-S5")
  ok(grepl("hazard_risk_DIAGNOSTIC", out) && !dir.exists(file.path(td, "Data", "hazard_risk")), "--no-thi bundles go to hazard_risk_DIAGNOSTIC/, never to the R/3 input dir")
  mg <- rast(file.path(od, "deficit_magnitude.tif")); mo <- rast(file.path(od, "baseline_moments.tif"))
  v1 <- values(mg)[1, ]; ok(all(c("worst_year_deficit", "deficit_q20") %in% names(mg)) && v1[["worst_year_deficit"]] <= v1[["deficit_q20"]] && v1[["worst_year_deficit"]] < 0,
                            "magnitude layers: worst-year deficit <= 1-in-5 deficit, worst < 0 (Q20 may be > 0 when one bad year drags the mean down)")
  ok(nlyr(mo) == 5 && values(mo)[1, "n_years"] == 10, "baseline_moments carries mean/sd per season and the year count")
  unlink(td, recursive = TRUE); cat("\nALL S6 SELFTEST ASSERTIONS PASSED\n")
}

if (sys.nframe() == 0) {
  a <- commandArgs(TRUE); getopt <- function(k, d = NULL) { i <- match(k, a); if (is.na(i)) d else a[i + 1] }
  if ("--selftest" %in% a) { selftest(); quit(status = 0) }
  yrs <- getopt("--years", "2005:2025"); yrs <- if (grepl(":", yrs)) { p <- as.integer(strsplit(yrs, ":")[[1]]); p[1]:p[2] } else as.integer(strsplit(yrs, ",")[[1]])
  obs_root <- getopt("--obs-root"); if (is.null(obs_root) || !startsWith(obs_root, "/")) stopf("--obs-root must be absolute")
  tok <- getopt("--token", "chirps-chirts-agera5")
  run(obs_root, yrs, tok, thi_token = getopt("--thi-token", tok), period = getopt("--period", "ggcmi-crop"),
      rl_min = as.numeric(getopt("--rl-min", "0.1")), overwrite = "--overwrite" %in% a,
      spinup = as.integer(getopt("--spinup", "1")), no_thi = "--no-thi" %in% a)
}
