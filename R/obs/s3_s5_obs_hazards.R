#!/usr/bin/env Rscript
# R/obs/s3_s5_obs_hazards.R
# =========================
# Observed hazard pipeline, stages S3-S5 (PLAN_obs_hazard_pipeline.md §2):
#   S3  monthly index (S2 output)  -> one value per season, per crop WINDOW (GGCMI rainfed calendar of that
#       crop; perennials and livestock: 12 months from the maize planting month, the NEX `annual` rule)
#   S4  classify each season at each severity (metadata/haz_classes.csv) and encode the season as
#       code = dry*1 + heat*10 + wet*100  (the R/2 encoding; NA if any input is NA)  -> SAVED, one layer per season
#   S5  class frequencies over the seasons -> the 9-layer `_int` bundle R/3 §4 reads, one file per crop
#
# Inputs  <OBS_ROOT>/nex-gddp-cmip6_indices/historical_<token>/<IDX>/<IDX>-YYYY-MM.tif   (S2, upstream scripts)
#         metadata/obs_crop_windows.csv, metadata/haz_classes.csv, metadata/haz_metadata.csv, GGCMI calendars
# Outputs <OBS_ROOT>/obs_seasonal/<window>/<IDX>_<stat>.tif                 (S3; layer per season, named by start year)
#         <OBS_ROOT>/obs_codes/<crop>_<set>_<sev>_codes.tif                  (S4; INT1U, 255 = NA)
#         <OBS_ROOT>/hazard_risk/<period>/<crop>_<token>_<sev>_<set>_int.tif (S5; R/3 naming and layer order)
#
# Layer name contract (R/3 §4.2 splits on "_" into 9 fields after appending "_<exposure_var>_<exposure_unit>"):
#   <scenario>_<token>_<timeframe>_<class>_<set>_<crop>_<sev>   e.g. observed_chirps-chirts-agera5_2006-2025_dry+heat_NDWS+NTx35+NDWL0_maize_severe
# so token, crop, set and sev must contain no "_".
#
# Seasons: with data years Y1..Yn there are n-1 seasons (a season may run into the next year), labelled by the
# start year. --spinup drops the first season(s): the water balance is seeded at Y1-01, so season Y1 starts from
# an artificial field-capacity store.
#
# Usage:
#   Rscript R/obs/s3_s5_obs_hazards.R --obs-root <abs> --token chirps-chirts-agera5 --years 2005:2025 \
#           [--spinup 1] [--period ggcmi-crop] [--ggcmi-dir <dir>] [--crops maize,cassava] [--sets crop,livestock] [--overwrite]
#   Rscript R/obs/s3_s5_obs_hazards.R --selftest        # synthetic end-to-end, ~20 s, no data needed
suppressPackageStartupMessages({ library(terra); library(data.table) })
.t0 <- Sys.time()
lg <- function(...) cat(sprintf("[%s | +%7.1fs] [S3-5] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
                                as.numeric(difftime(Sys.time(), .t0, units = "secs")), paste0(...)))
stopf <- function(...) stop("[S3-5] STOP: ", sprintf(...), call. = FALSE)

repo <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else {
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd()
}

# ---- config ------------------------------------------------------------------------------------------
SETS <- list(
  crop      = list(name = "NDWS+NTx35+NDWL0",   dry = "NDWS", heat = "NTx35",   wet = "NDWL0", who = "crop"),
  livestock = list(name = "NDWS+THI-max+NDWL0", dry = "NDWS", heat = "THI_max", wet = "NDWL0", who = "livestock")
)
SEVS <- c("moderate", "severe", "extreme")
CLASS_CODES <- c(dry = 1, heat = 10, wet = 100, `dry+heat` = 11, `dry+wet` = 101, `heat+wet` = 110, `dry+heat+wet` = 111)
IDX_FILE_PREFIX <- c(NDWS = "NDWS-", NTx35 = "NTx35-", NDWL0 = "NDWL0-", THI_max = "THI_max-")
IDX_DIR <- c(NDWS = "NDWS", NTx35 = "NTx35", NDWL0 = "NDWL0", THI_max = "THI")

load_thresholds <- function(path) {
  hc <- fread(path)[, .(index_name, description = tolower(description), direction, crop, threshold)]
  hc[crop == "generic", crop := "generic"]
  hc
}
threshold_for <- function(hc, idx, sev, who, crop) {
  # crops: generic rows for every index; livestock: species-system row for THI, generic for the rest
  key <- if (who == "livestock" && idx == "THI_max") gsub("-", "_", crop) else "generic"
  r <- hc[index_name == idx & description == sev & crop == key]
  if (!nrow(r) && key != "generic") r <- hc[index_name == idx & description == sev & crop == "generic"]
  if (nrow(r) != 1) stopf("threshold for %s/%s/%s resolved to %d rows", idx, sev, key, nrow(r))
  list(direction = r$direction, threshold = r$threshold)
}

# ---- S3: seasonal aggregation --------------------------------------------------------------------------
# Same rule as R/haz_functions.R hazard_stacker (plant..maturity inclusive, wrap +12, per-index stat), but the
# 24-month slice is taken from month 1 of the start year, so it is correct whatever the minimum planting month
# in the raster (hazard_stacker's slice starts at that minimum, which is only right when some cell plants in
# January). THI files are chosen by exact prefix, so THI_max never reads the THI_mean files.
month_files <- function(idx_dir, idx, years) {
  f <- list.files(idx_dir, pattern = paste0("^", IDX_FILE_PREFIX[[idx]], "[0-9]{4}-[0-9]{2}\\.tif$"), full.names = TRUE)
  tab <- data.table(file = f, ym = sub("\\.tif$", "", sub(paste0("^", IDX_FILE_PREFIX[[idx]]), "", basename(f))))
  tab[, `:=`(year = as.integer(substr(ym, 1, 4)), month = as.integer(substr(ym, 6, 7)))]
  want <- CJ(year = years, month = 1:12)
  tab <- merge(want, tab, by = c("year", "month"), all.x = TRUE)
  if (anyNA(tab$file)) stopf("%s: %d monthly file(s) missing (first %04d-%02d)", idx, sum(is.na(tab$file)),
                            tab[is.na(file)][1, year], tab[is.na(file)][1, month])
  setorder(tab, year, month); tab
}
season_stack <- function(files_tab, plant, harvest, stat, years) {
  r <- rast(files_tab$file)                           # 12*n layers, chronological
  stopifnot(nlyr(r) == 12 * length(years))
  fun <- switch(stat, mean = mean, sum = sum, max = max, min = min, stopf("unknown stat %s", stat))
  out <- lapply(seq_len(length(years) - 1), function(m) {
    x <- r[[(1 + 12 * (m - 1)):(24 + 12 * (m - 1))]]   # month 1 of year m .. month 12 of year m+1
    s <- rapp(x, first = plant, last = harvest, fun = fun, na.rm = TRUE)
    names(s) <- years[m]; s
  })
  rast(out)
}
make_calendar <- function(window, ggcmi_code, ggcmi_dir, base, maize_plant) {
  # returns list(plant, harvest) month rasters on `base`; harvest already wrapped (+12 when < plant)
  if (window == "ggcmi") {
    f <- file.path(ggcmi_dir, sprintf("%s_rf_ggcmi_crop_calendar_phase3_v1.01.nc4", ggcmi_code))
    if (!file.exists(f)) stopf("GGCMI calendar missing: %s", f)
    cc <- rast(f)
    if (!all(c("planting_day", "maturity_day") %in% names(cc))) cc <- rast(list(rast(f, subds = "planting_day"), rast(f, subds = "maturity_day")))
    cc <- cc[[c("planting_day", "maturity_day")]]
    cc <- resample(crop(cc, base), base, method = "near")
    # exactly R/1_make_timeseries.R:279-284 (no -1; 2024 origin), so windows match the NEX chain like for like
    plant <- as.integer(format(as.Date(values(cc$planting_day)[, 1], origin = "2024-01-01"), "%m"))
    matur <- as.integer(format(as.Date(values(cc$maturity_day)[, 1], origin = "2024-01-01"), "%m"))
    p <- setValues(base, plant); h <- setValues(base, matur)
  } else if (window == "annual12") {
    p <- maize_plant; h <- ((p + 10) %% 12) + 1          # 12 months inclusive from the planting month
  } else stopf("unknown window %s", window)
  h <- ifel(h < p, h + 12, h)
  list(plant = p, harvest = h)
}

# ---- S4/S5 -----------------------------------------------------------------------------------------------
classify <- function(s, direction, threshold) {
  if (direction %in% c(">", "G")) ifel(s > threshold, 1, 0) else ifel(s < threshold, 1, 0)
}
code_stack <- function(dry01, heat01, wet01) dry01 + 10 * heat01 + 100 * wet01     # NA propagates
freq_from_codes <- function(codes, lyr_prefix) {
  valid <- !is.na(codes)
  nval <- sum(valid)                                        # seasons with data, per cell
  any_f  <- sum(ifel(valid, codes > 0, 0)) / nval           # independent of none
  none_f <- sum(ifel(valid, codes == 0, 0)) / nval
  cls <- lapply(names(CLASS_CODES), function(k) sum(ifel(valid, codes == CLASS_CODES[[k]], 0)) / nval)
  out <- c(any_f, none_f, rast(cls))
  out <- ifel(nval == 0, NA, out)
  names(out) <- paste0(lyr_prefix, "_", c("any", "none", names(CLASS_CODES)))
  out
}
gate_bundle <- function(b, what) {
  v <- values(b); ok <- rowSums(is.na(v)) == 0
  if (nlyr(b) != 9) stopf("%s: %d layers, expected 9", what, nlyr(b))
  if (!all(vapply(strsplit(names(b), "_"), length, 1L) == 7)) stopf("%s: layer names must have exactly 7 '_' fields", what)
  if (any(abs(rowSums(v[ok, 3:9, drop = FALSE]) - v[ok, 1]) > 1e-6)) stopf("%s: classes do not sum to any", what)
  if (any(abs(v[ok, 1] + v[ok, 2] - 1) > 1e-6)) stopf("%s: any + none != 1", what)
  invisible(TRUE)
}

# ---- driver -------------------------------------------------------------------------------------------------
run <- function(obs_root, token, years, spinup, period, ggcmi_dir, crops_sel, sets_sel, overwrite,
                base = NULL, windows_csv = file.path(repo, "metadata", "obs_crop_windows.csv"),
                hc_csv = file.path(repo, "metadata", "haz_classes.csv"), hm_csv = file.path(repo, "metadata", "haz_metadata.csv"),
                scenario = "observed") {
  if (grepl("_", token)) stopf("token must not contain '_': %s", token)
  if (is.null(base)) base <- rast(file.path(repo, "metadata", "base_rast_nexgddp.tif"))
  idx_root <- file.path(obs_root, "nex-gddp-cmip6_indices", paste0("historical_", token))
  if (!dir.exists(idx_root)) stopf("S2 output dir missing: %s", idx_root)
  hc <- load_thresholds(hc_csv)
  hm <- unique(fread(hm_csv)[, .(variable.code, stat = `function`)])
  win <- fread(windows_csv)
  if (grepl("_", paste(win$crop, collapse = ""))) stopf("obs_crop_windows.csv: crop names must use '-' not '_'")
  if (!is.null(crops_sel)) win <- win[crop %in% crops_sel]
  sets <- SETS[sets_sel]
  season_years <- years[-length(years)]                      # start years of the n-1 seasons
  keep <- seq_len(length(season_years))[-seq_len(spinup)]
  if (!length(keep)) stopf("spinup=%d leaves no seasons", spinup)
  timeframe <- sprintf("%d-%d", min(years) + spinup, max(years))
  lg(sprintf("obs_root=%s token=%s years=%d-%d seasons kept=%d (%s..%s) timeframe=%s period=%s crops=%d sets=%s",
             obs_root, token, min(years), max(years), length(keep), season_years[keep[1]], season_years[tail(keep, 1)],
             timeframe, period, nrow(win), paste(names(sets), collapse = ",")))
  # grid gate: every S2 file must sit on the base lattice
  probe <- rast(month_files(file.path(idx_root, "NDWS"), "NDWS", years)$file[1])
  if (!compareGeom(probe, base, stopOnError = FALSE)) stopf("S2 output is not on the base lattice (%s)", basename(sources(probe)))

  # maize planting month (needed for every annual12 window)
  mz <- make_calendar("ggcmi", "mai", ggcmi_dir, base, NULL)
  # --- S3: one seasonal stack per (window, index)
  win[, wkey := ifelse(window == "ggcmi", paste0("ggcmi-", ggcmi_code), "annual12")]
  idx_needed <- unique(unlist(lapply(sets, function(s) c(s$dry, s$heat, s$wet))))
  s3 <- list()
  for (wk in unique(win$wkey)) {
    w0 <- win[wkey == wk][1]
    cal <- if (wk == "annual12") make_calendar("annual12", NA, ggcmi_dir, base, mz$plant) else make_calendar("ggcmi", w0$ggcmi_code, ggcmi_dir, base, NULL)
    for (idx in idx_needed) {
      stat <- hm[variable.code == idx, stat]; if (length(stat) != 1) stopf("stat for %s not unique in haz_metadata.csv", idx)
      f <- file.path(obs_root, "obs_seasonal", wk, sprintf("%s_%s.tif", idx, stat)); dir.create(dirname(f), FALSE, TRUE)
      if (!file.exists(f) || overwrite) {
        t1 <- Sys.time()
        s <- season_stack(month_files(file.path(idx_root, IDX_DIR[[idx]]), idx, years), cal$plant, cal$harvest, stat, years)
        writeRaster(s, f, overwrite = TRUE, filetype = "COG", gdal = c("OVERVIEWS=NONE", "COMPRESS=ZSTD"))
        lg(sprintf("S3 %s/%s: %d seasons in %.1fs", wk, idx, nlyr(s), as.numeric(difftime(Sys.time(), t1, units = "secs"))))
      }
      s3[[paste(wk, idx)]] <- rast(f)[[keep]]
    }
  }
  # --- S4 + S5 per crop, set, severity
  dir.create(file.path(obs_root, "obs_codes"), FALSE, TRUE)
  out_dir <- file.path(obs_root, "hazard_risk", period); dir.create(out_dir, FALSE, TRUE)
  n_out <- 0
  for (i in seq_len(nrow(win))) {
    cr <- win$crop[i]; wk <- win$wkey[i]
    is_lv <- grepl("-(highland|tropical)$", cr)
    for (sn in names(sets)) {
      st <- sets[[sn]]
      if ((st$who == "livestock") != is_lv) next
      for (sev in SEVS) {
        bundle_f <- file.path(out_dir, sprintf("%s_%s_%s_%s_int.tif", cr, token, sev, st$name))
        codes_f <- file.path(obs_root, "obs_codes", sprintf("%s_%s_%s_codes.tif", cr, st$name, sev))
        if (file.exists(bundle_f) && file.exists(codes_f) && !overwrite) next
        cl <- lapply(c(dry = st$dry, heat = st$heat, wet = st$wet), function(idx) {
          th <- threshold_for(hc, idx, sev, st$who, cr); classify(s3[[paste(wk, idx)]], th$direction, th$threshold)
        })
        codes <- code_stack(cl$dry, cl$heat, cl$wet); names(codes) <- names(s3[[paste(wk, st$dry)]])
        writeRaster(codes, codes_f, overwrite = TRUE, datatype = "INT1U", NAflag = 255, filetype = "COG", gdal = c("OVERVIEWS=NONE"))
        b <- freq_from_codes(codes, paste(scenario, token, timeframe, sep = "_"))
        names(b) <- paste0(names(b), "_", st$name, "_", cr, "_", sev)
        gate_bundle(b, basename(bundle_f))
        writeRaster(b, bundle_f, overwrite = TRUE, filetype = "COG", gdal = c("OVERVIEWS=NONE", "COMPRESS=ZSTD"))
        n_out <- n_out + 1
      }
    }
    lg(sprintf("S4/S5 %s (%s) done", cr, wk))
  }
  lg(sprintf("done: %d bundles under %s", n_out, out_dir))
  invisible(out_dir)
}

# ---- selftest ---------------------------------------------------------------------------------------------
selftest <- function() {
  ok <- function(c, m) if (!isTRUE(c)) stop("SELFTEST FAIL: ", m) else cat("  ok  ", m, "\n")
  td <- file.path(tempdir(), "s35_selftest"); unlink(td, recursive = TRUE); dir.create(td, recursive = TRUE)
  base <- rast(nrows = 4, ncols = 5, xmin = 30, xmax = 31.25, ymin = 0, ymax = 1, crs = "EPSG:4326")
  token <- "obstest"; years <- 2005:2008                                  # 3 seasons; spinup 1 -> seasons 2006, 2007
  idx_root <- file.path(td, "nex-gddp-cmip6_indices", paste0("historical_", token))
  # calendar: plant month 3, maturity 7 everywhere (so the window is Mar-Jul; plant_min != 1 on purpose)
  gg <- file.path(td, "ggcmi"); dir.create(gg)
  cc <- sds(setValues(base, 31 + 28 + 1), setValues(base, 31 + 28 + 31 + 30 + 31 + 30 + 1)); names(cc) <- c("planting_day", "maturity_day")
  for (code in c("mai", "bea")) suppressWarnings(writeCDF(cc, file.path(gg, sprintf("%s_rf_ggcmi_crop_calendar_phase3_v1.01.nc4", code)), overwrite = TRUE))
  # monthly inputs. Season 2006 (kept): NDWS 22 in Mar-Jul, 99 in Aug (outside window -> must not count);
  # NTx35 15 in Mar-Jul; NDWL0 0; THI_max 85. Season 2007: everything 0. 2005 (spin-up): 50; 2008: 0.
  val <- function(idx, y, m) {
    if (y == 2005) return(50)                      # spin-up year: must be dropped
    if (y == 2008) return(0)                       # only reached by the 12-month window of season 2007
    if (y == 2007) return(0)
    switch(idx, NDWS = if (m %in% 3:7) 22 else if (m == 8) 99 else 0, NTx35 = if (m %in% 3:7) 15 else 0,
           NDWL0 = 0, THI_max = if (m %in% 3:7) 85 else 60)
  }
  for (idx in names(IDX_DIR)) for (y in years) for (m in 1:12) {
    d <- file.path(idx_root, IDX_DIR[[idx]]); dir.create(d, FALSE, TRUE)
    r <- setValues(base, val(idx, y, m)); r[1, 1] <- NA                          # one NA cell throughout
    writeRaster(r, file.path(d, sprintf("%s%04d-%02d.tif", IDX_FILE_PREFIX[[idx]], y, m)), overwrite = TRUE)
  }
  # a decoy THI_mean file must not be picked up as THI_max
  writeRaster(setValues(base, 999), file.path(idx_root, "THI", "THI_mean-2006-04.tif"), overwrite = TRUE)
  # window table: maize (ggcmi), bean (ggcmi proxy), coffee (annual12), cattle-tropical, cattle-highland (livestock)
  wcsv <- file.path(td, "windows.csv")
  fwrite(data.table(crop = c("maize", "bean", "arabica-coffee", "cattle-tropical", "cattle-highland"),
                    window = c("ggcmi", "ggcmi", "annual12", "annual12", "annual12"),
                    ggcmi_code = c("mai", "bea", "", "", ""), proxy = FALSE, note = ""), wcsv)
  out <- run(td, token, years, spinup = 1, period = "ggcmi-crop", ggcmi_dir = gg, crops_sel = NULL,
             sets_sel = c("crop", "livestock"), overwrite = TRUE, base = base, windows_csv = wcsv)
  # --- assertions
  s3 <- rast(file.path(td, "obs_seasonal", "ggcmi-mai", "NDWS_mean.tif"))
  ok(nlyr(s3) == 3 && identical(names(s3), c("2005", "2006", "2007")), "S3: 3 seasons from 4 years, labelled by start year")
  ok(abs(values(s3[[2]])[2, 1] - 22) < 1e-6, "S3: window Mar-Jul mean = 22 (the 99 in Aug is outside the window and ignored)")
  ok(is.na(values(s3[[2]])[1, 1]), "S3: NA input cell stays NA")
  a12 <- rast(file.path(td, "obs_seasonal", "annual12", "NDWS_mean.tif"))
  ok(abs(values(a12[[2]])[2, 1] - (5 * 22 + 99) / 12) < 1e-6, "S3: annual12 window = 12 months from the planting month (Mar-Feb) includes Aug")
  thi <- rast(file.path(td, "obs_seasonal", "annual12", "THI_max_max.tif"))
  ok(abs(values(thi[[2]])[2, 1] - 85) < 1e-6, "S3: THI_max uses max and ignores the THI_mean decoy (999)")
  codes <- rast(file.path(td, "obs_codes", "maize_NDWS+NTx35+NDWL0_severe_codes.tif"))
  ok(nlyr(codes) == 2 && identical(names(codes), c("2006", "2007")), "S4: spin-up season dropped; 2 code layers named by year")
  ok(values(codes)[2, 1] == 11 && values(codes)[2, 2] == 0, "S4: severe codes = 11 (dry+heat) in 2006, 0 in 2007")
  ok(is.na(values(codes)[1, 1]), "S4: NA cell -> NA code (never 0 = none)")
  ext <- rast(file.path(td, "obs_codes", "maize_NDWS+NTx35+NDWL0_extreme_codes.tif"))
  ok(values(ext)[2, 1] == 0, "S4: extreme (NDWS>25, NTx35>21) does not fire on 22/15")
  b <- rast(file.path(out, "maize_obstest_severe_NDWS+NTx35+NDWL0_int.tif"))
  ok(nlyr(b) == 9 && names(b)[1] == "observed_obstest_2006-2008_any_NDWS+NTx35+NDWL0_maize_severe", "S5: 9 layers, R/3 layer-name contract (7 fields)")
  v <- values(b)[2, ]; names(v) <- sub(".*2006-2008_", "", names(v)); names(v) <- sub("_NDWS.*", "", names(v))
  ok(abs(v[["any"]] - 0.5) < 1e-9 && abs(v[["none"]] - 0.5) < 1e-9 && abs(v[["dry+heat"]] - 0.5) < 1e-9 && v[["dry"]] == 0,
     "S5: any 0.5, none 0.5, dry+heat 0.5, dry-only 0 (classes are exclusive)")
  ok(all(is.na(values(b)[1, ])), "S5: NA cell is NA in every class")
  # livestock use the 12-month window: NDWS mean = (5*22 + 99)/12 = 17.4 -> moderate (>15) yes, severe (>20) no
  ct <- rast(file.path(out, "cattle-tropical_obstest_severe_NDWS+THI-max+NDWL0_int.tif")); vv <- values(ct)[2, ]
  ok(abs(vv[grep("_heat_", names(vv), fixed = TRUE)] - 0.5) < 1e-9 && vv[grep("_dry+heat_", names(vv), fixed = TRUE)] == 0,
     "S5 livestock severe: THI 85 > cattle_tropical 82 but 12-month NDWS 17.4 < 20 -> heat only (window-dependent)")
  cm <- rast(file.path(out, "cattle-tropical_obstest_moderate_NDWS+THI-max+NDWL0_int.tif")); vm <- values(cm)[2, ]
  ok(abs(vm[grep("_dry+heat_", names(vm), fixed = TRUE)] - 0.5) < 1e-9, "S5 livestock moderate: NDWS 17.4 > 15 and THI 85 > 74 -> dry+heat")
  ce <- rast(file.path(out, "cattle-tropical_obstest_extreme_NDWS+THI-max+NDWL0_int.tif")); ve <- values(ce)[2, ]
  ok(ve[grep("_none_", names(ve))] == 1, "S5 livestock: extreme (THI > 94) never fires -> none = 1")
  ok(file.exists(file.path(out, "bean_obstest_moderate_NDWS+NTx35+NDWL0_int.tif")) && file.exists(file.path(out, "arabica-coffee_obstest_extreme_NDWS+NTx35+NDWL0_int.tif")),
     "S5: per-crop bundles exist for every crop x severity (no generic-crop)")
  ok(length(list.files(out, "_int\\.tif$")) == 5 * 3, "S5: 5 commodities x 3 severities = 15 bundles")
  unlink(td, recursive = TRUE)
  cat("\nALL S3-S5 SELFTEST ASSERTIONS PASSED\n")
}

# ---- CLI -----------------------------------------------------------------------------------------------------
if (sys.nframe() == 0) {
  a <- commandArgs(TRUE)
  getopt <- function(k, d = NULL) { i <- match(k, a); if (is.na(i)) d else a[i + 1] }
  if ("--selftest" %in% a) { selftest(); quit(status = 0) }
  yrs <- getopt("--years", "2005:2025"); yrs <- if (grepl(":", yrs)) { p <- as.integer(strsplit(yrs, ":")[[1]]); p[1]:p[2] } else as.integer(strsplit(yrs, ",")[[1]])
  obs_root <- getopt("--obs-root"); if (is.null(obs_root) || !startsWith(obs_root, "/")) stopf("--obs-root must be absolute")
  ggcmi_dir <- getopt("--ggcmi-dir", Sys.getenv("GGCMI_DIR", NA)); if (is.na(ggcmi_dir)) stopf("--ggcmi-dir or GGCMI_DIR required")
  crops_sel <- getopt("--crops"); if (!is.null(crops_sel)) crops_sel <- strsplit(crops_sel, ",")[[1]]
  run(obs_root, getopt("--token", "chirps-chirts-agera5"), yrs, as.integer(getopt("--spinup", "1")), getopt("--period", "ggcmi-crop"),
      ggcmi_dir, crops_sel, strsplit(getopt("--sets", "crop,livestock"), ",")[[1]], "--overwrite" %in% a)
}
