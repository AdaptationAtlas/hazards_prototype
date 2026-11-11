#!/usr/bin/env Rscript
# R/obs/s7_trends_anomalies.R
# ===========================
# Observed pipeline, stage S7 (PLAN_obs_hazard_pipeline.md, "S7 design", Pete 2026-11-05): trends, normals and
# anomalies of the per-season hazard indices of the LONG observed series (CHIRPS v3 / CHIRTS-ERA5 / AgERA5).
#
# Input   <OBS_ROOT>/Data/obs_seasonal/<token>_<timeframe>/<window>/<IDX>_<stat>.tif   (S3; one layer per season,
#         named by start year; the first layer is the spin-up season and is dropped by --years)
# Per (window, index) stack it writes, under <OBS_ROOT>/Data/obs_trends/<token>_<first>-<last>/<window>/:
#   <IDX>_<stat>_trend.tif        slope_per_decade (Theil-Sen), p_mk (Mann-Kendall, Hamed-Rao autocorrelation
#                                 correction), sig_fdr (1 = significant at FDR 0.05, Benjamini-Hochberg over the
#                                 valid cells of this stack), n_years
#   <IDX>_<stat>_normal_<N1>-<N2>.tif   mean, sd, p10, p50, p90 over the normal period (default WMO 1991-2020)
#   <IDX>_<stat>_anomaly_diff.tif one layer per season: value - normal mean (index units)
#   <IDX>_<stat>_anomaly_z.tif    one layer per season: (value - normal mean) / normal sd
# and, with --boundaries-dir, admin tables (area-weighted means of the cell values, GAUL 2024 admin0/1/2):
#   admin_series_<level>.parquet  one row per unit x window x index x season: value, anomaly_diff, anomaly_z
#   admin_trends_<level>.parquet  one row per unit x window x index: slope_per_decade, p_mk, sig_fdr, normal mean/sd
# Admin statistics are computed on the unit's own mean series (not averaged pixel statistics).
#
# Normal and anomaly cells need >= --min-normal-years valid seasons in the normal period (default 25 of 30);
# trend cells need >= --min-trend-years valid seasons (default 30). Otherwise NA.
#
# Usage:
#   Rscript R/obs/s7_trends_anomalies.R --obs-root <abs> --token chirps-chirts-agera5 --timeframe 1982-2025 \
#           --years 1982:2024 [--normal 1991:2020] [--windows ggcmi-mai,annual12] [--indices NDWS_mean,THI_max_max]
#           [--boundaries-dir <dir with *a0*/*a1*/*a2* GAUL parquets>] [--cores 8] [--overwrite]
#   Rscript R/obs/s7_trends_anomalies.R --selftest
suppressPackageStartupMessages({ library(terra); library(data.table) })
.t0 <- Sys.time()
lg <- function(...) cat(sprintf("[%s | +%7.1fs] [S7] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
                                as.numeric(difftime(Sys.time(), .t0, units = "secs")), paste0(...)))
stopf <- function(...) stop("[S7] STOP: ", sprintf(...), call. = FALSE)

# ---- statistics on one series -------------------------------------------------------------------------------
# Theil-Sen slope (per year) and Mann-Kendall p with the Hamed & Rao (1998) variance correction for serial
# correlation, using only the significant lags (|r_k| > 1.96/sqrt(n)) of the rank autocorrelation of the
# Sen-detrended series. Returns c(slope, p, n); NA where fewer than min_n valid values.
mk_sen <- function(x, t, min_n = 30L) {
  ok <- !is.na(x); x <- x[ok]; t <- t[ok]; n <- length(x)
  if (n < min_n) return(c(NA_real_, NA_real_, n))
  dx <- outer(x, x, "-"); dt <- outer(t, t, "-"); up <- upper.tri(dx)
  slope <- stats::median(-dx[up] / -dt[up])                       # (x_j - x_i)/(t_j - t_i), i < j
  S <- sum(sign(-dx[up]))                                         # sum over i<j of sign(x_j - x_i)
  tp <- table(x); tp <- tp[tp > 1]
  v0 <- (n * (n - 1) * (2 * n + 5) - sum(tp * (tp - 1) * (2 * tp + 5))) / 18
  xd <- x - slope * t
  fac <- 1
  if (stats::sd(xd) > 0) {
    r <- rank(xd); r <- r - mean(r)
    ac <- stats::acf(r, lag.max = n - 1, plot = FALSE, demean = FALSE)$acf[-1]
    k <- seq_along(ac); sigk <- abs(ac) > 1.96 / sqrt(n)
    if (any(sigk)) fac <- 1 + 2 / (n * (n - 1) * (n - 2)) * sum(((n - k) * (n - k - 1) * (n - k - 2) * ac)[sigk])
  }
  v <- v0 * max(fac, 1e-6)
  z <- if (S > 0) (S - 1) / sqrt(v) else if (S < 0) (S + 1) / sqrt(v) else 0
  c(slope, 2 * stats::pnorm(-abs(z)), n)
}
digest_key <- function(x) {                     # short stable key without extra packages
  b <- utf8ToInt(paste(x, collapse = "|")); h <- 0
  for (i in b) h <- (h * 31 + i) %% 2147483647
  sprintf("%010.0f", h)
}
normal_stats <- function(x, min_n) {
  x <- x[!is.na(x)]
  if (length(x) < min_n) return(rep(NA_real_, 5))
  c(mean(x), stats::sd(x), stats::quantile(x, c(0.1, 0.5, 0.9), type = 7, names = FALSE))
}
bh_sig <- function(p, alpha = 0.05) { out <- rep(NA_real_, length(p)); ok <- !is.na(p); out[ok] <- as.numeric(stats::p.adjust(p[ok], "BH") <= alpha); out }

# ---- one stack ------------------------------------------------------------------------------------------------
do_stack <- function(f, out_dir, years, normal, min_normal, min_trend, cores, overwrite) {
  base <- sub("\\.tif$", "", basename(f))
  outs <- file.path(out_dir, paste0(base, c("_trend.tif", sprintf("_normal_%d-%d.tif", min(normal), max(normal)), "_anomaly_diff.tif", "_anomaly_z.tif")))
  if (all(file.exists(outs)) && !overwrite) { lg("skip (exists): ", basename(out_dir), "/", base); return(invisible(outs)) }
  r <- rast(f); nm <- names(r)
  miss <- setdiff(as.character(years), nm); if (length(miss)) stopf("%s lacks seasons %s", f, paste(head(miss, 5), collapse = ","))
  r <- r[[as.character(years)]]
  ni <- match(as.character(normal), names(r)); if (anyNA(ni)) stopf("normal period %d-%d not inside --years", min(normal), max(normal))
  t <- as.numeric(years)
  # local copies: with cores > 1 terra::app ships the closure to PSOCK workers, which see its enclosing frame but not
  # this script's global functions (CGlabs 2026-11-11: "could not find function mk_sen")
  mk <- mk_sen; ns <- normal_stats
  tr <- app(r, function(v) mk(v, t, min_trend), cores = cores)
  names(tr) <- c("slope_per_year", "p_mk", "n_years")
  slope_dec <- tr[["slope_per_year"]] * 10; names(slope_dec) <- "slope_per_decade"
  sig <- setValues(tr[["p_mk"]], bh_sig(values(tr[["p_mk"]])[, 1])); names(sig) <- "sig_fdr"
  trend <- c(slope_dec, tr[["p_mk"]], sig, tr[["n_years"]])
  nrm <- app(r[[ni]], function(v) ns(v, min_normal), cores = cores); names(nrm) <- c("mean", "sd", "p10", "p50", "p90")
  adiff <- r - nrm[["mean"]]; names(adiff) <- names(r)
  az <- ifel(nrm[["sd"]] > 0, adiff / nrm[["sd"]], NA); names(az) <- names(r)
  dir.create(out_dir, FALSE, TRUE)
  wr <- function(x, p) writeRaster(x, p, overwrite = TRUE, filetype = "COG", gdal = c("COMPRESS=ZSTD", "OVERVIEWS=AUTO"))
  wr(trend, outs[1]); wr(nrm, outs[2]); wr(adiff, outs[3]); wr(az, outs[4])
  vs <- values(sig)[, 1]; vp <- values(tr[["p_mk"]])[, 1]
  lg(sprintf("%s/%s: %d seasons; trend on %d cells, %.1f %% p<0.05, %.1f %% FDR-significant; median slope %.3g per decade",
             basename(out_dir), base, nlyr(r), sum(!is.na(vp)), 100 * mean(vp < 0.05, na.rm = TRUE), 100 * mean(vs == 1, na.rm = TRUE),
             stats::median(values(slope_dec)[, 1], na.rm = TRUE)))
  invisible(outs)
}

# ---- admin aggregation: cell x unit weights computed once per level -----------------------------------------
read_units <- function(boundaries_dir) {
  fl <- list.files(boundaries_dir, "\\.parquet$", full.names = TRUE)
  pick <- function(tag) { x <- grep(tag, basename(fl)); if (length(x) != 1) stopf("need exactly one %s parquet in %s", tag, boundaries_dir); fl[x] }
  lapply(c(admin0 = "a0", admin1 = "a1", admin2 = "a2"), function(tag) {
    v <- terra::vect(sf::st_as_sf(arrow::read_parquet(pick(paste0("_", tag, "_")), as_data_frame = TRUE)))
    v
  })
}
unit_weights <- function(v, template) {
  w <- as.data.table(terra::extract(template, v, cells = TRUE, weights = TRUE, ID = TRUE))
  w <- w[!is.na(cell) & weight > 0, .(ID, cell, weight)]
  w[, weight := weight * terra::cellSize(template, unit = "km")[cell][, 1]]       # area weights (cells shrink with latitude)
  w
}
admin_means <- function(r, w) {
  m <- values(r)[w$cell, , drop = FALSE]
  dt <- data.table(ID = w$ID, weight = w$weight, m)
  vc <- setdiff(names(dt), c("ID", "weight"))
  dt[, lapply(.SD, function(x) { ok <- !is.na(x); if (!any(ok)) NA_real_ else sum(x[ok] * weight[ok]) / sum(weight[ok]) }), by = ID, .SDcols = vc]
}
admin_stack <- function(f, units, wts, window, years, normal, min_normal, min_trend) {
  r <- rast(f)[[as.character(years)]]; idx <- sub("\\.tif$", "", basename(f))
  res <- list(series = list(), trends = list())
  for (lv in names(units)) {
    a <- admin_means(r, wts[[lv]])
    meta <- as.data.table(units[[lv]])[a$ID]
    keep <- intersect(c("iso3", "admin0_name", "admin1_name", "admin2_name", "gaul0_code", "gaul1_code", "gaul2_code"), names(meta))
    mat <- as.matrix(a[, -1]); ni <- match(as.character(normal), colnames(mat))
    nrm <- t(apply(mat[, ni, drop = FALSE], 1, normal_stats, min_n = min_normal))
    tr <- t(apply(mat, 1, mk_sen, t = as.numeric(years), min_n = min_trend))
    trends <- data.table(level = lv, meta[, ..keep], window = window, index = idx,
                         slope_per_decade = tr[, 1] * 10, p_mk = tr[, 2], n_years = tr[, 3],
                         normal_mean = nrm[, 1], normal_sd = nrm[, 2])
    trends[, sig_fdr := bh_sig(p_mk)]
    long <- data.table(rid = seq_len(nrow(mat)), mat)
    long <- melt(long, id.vars = "rid", variable.name = "year", value.name = "value", variable.factor = FALSE)
    long[, `:=`(year = as.integer(year), anomaly_diff = value - nrm[rid, 1])]
    long[, anomaly_z := fifelse(nrm[rid, 2] > 0, anomaly_diff / nrm[rid, 2], NA_real_)]
    series <- cbind(data.table(level = lv), meta[long$rid, ..keep], data.table(window = window, index = idx), long[, .(year, value, anomaly_diff, anomaly_z)])
    res$series[[lv]] <- series; res$trends[[lv]] <- trends
  }
  res
}

# ---- driver ----------------------------------------------------------------------------------------------------
run <- function(obs_root, token, timeframe, years, normal = 1991:2020, windows = NULL, indices = NULL,
                boundaries_dir = NULL, cores = 1L, overwrite = FALSE, min_normal = 25L, min_trend = 30L, units = NULL) {
  overwrite <- overwrite || tolower(Sys.getenv("FORCE_OVERWRITE", "")) %in% c("1", "true", "t", "yes", "y", "on")
  src <- file.path(obs_root, "Data", "obs_seasonal", paste0(token, "_", timeframe))
  if (!dir.exists(src)) stopf("no seasonal stacks at %s", src)
  if (!all(normal %in% years)) stopf("normal period must lie inside --years")
  dst <- file.path(obs_root, "Data", "obs_trends", sprintf("%s_%d-%d", token, min(years), max(years)))
  wins <- list.dirs(src, full.names = FALSE, recursive = FALSE); if (!is.null(windows)) wins <- intersect(wins, windows)
  if (!length(wins)) stopf("no windows selected under %s", src)
  lg(sprintf("src=%s windows=%d years=%d-%d (%d) normal=%d-%d cores=%d", src, length(wins), min(years), max(years), length(years), min(normal), max(normal), cores))
  wts <- NULL
  if (is.null(units) && !is.null(boundaries_dir)) units <- read_units(boundaries_dir)
  if (!is.null(units)) {
    tmpl <- rast(list.files(file.path(src, wins[1]), "\\.tif$", full.names = TRUE)[1])[[1]]
    # the cell x unit weights take ~36 min for GAUL admin2 on the Africa lattice; cache them per level, keyed by the
    # grid and the unit geometry, so a second token / rerun reuses them (CGlabs 2026-11-06)
    cache_dir <- file.path(obs_root, "Data", "obs_trends", "_admin_weights"); dir.create(cache_dir, FALSE, TRUE)
    key_of <- function(v) digest_key(c(as.character(ext(tmpl)), res(tmpl), nrow(tmpl), ncol(tmpl), nrow(v), round(sum(expanse(v, unit = "km")), 3)))
    wts <- lapply(names(units), function(lv) {
      cf <- file.path(cache_dir, sprintf("%s_%s.rds", lv, key_of(units[[lv]])))
      if (file.exists(cf)) { lg("admin weights from cache: ", basename(cf)); return(readRDS(cf)) }
      t1 <- Sys.time(); w <- unit_weights(units[[lv]], tmpl); saveRDS(w, cf)
      lg(sprintf("admin weights %s: %d unit-cell pairs in %.1f min (cached)", lv, nrow(w), as.numeric(difftime(Sys.time(), t1, units = "mins")))); w
    })
    names(wts) <- names(units)
    lg(sprintf("admin units: %s", paste(sprintf("%s %d", names(units), vapply(units, function(v) as.integer(nrow(v)), integer(1))), collapse = ", ")))
  }
  ser <- list(); trn <- list()
  for (w in wins) {
    fl <- list.files(file.path(src, w), "\\.tif$", full.names = TRUE)
    if (!is.null(indices)) fl <- fl[sub("\\.tif$", "", basename(fl)) %in% indices]
    for (f in fl) {
      do_stack(f, file.path(dst, w), years, normal, min_normal, min_trend, cores, overwrite)
      if (!is.null(units)) { a <- admin_stack(f, units, wts, w, years, normal, min_normal, min_trend); ser[[length(ser) + 1]] <- a$series; trn[[length(trn) + 1]] <- a$trends }
    }
  }
  if (!is.null(units)) {
    for (lv in names(units)) {
      s <- rbindlist(lapply(ser, `[[`, lv), fill = TRUE); tt <- rbindlist(lapply(trn, `[[`, lv), fill = TRUE)
      arrow::write_parquet(s, file.path(dst, sprintf("admin_series_%s.parquet", lv)))
      arrow::write_parquet(tt, file.path(dst, sprintf("admin_trends_%s.parquet", lv)))
      lg(sprintf("admin %s: %d series rows, %d trend rows; %.1f %% of units FDR-significant", lv, nrow(s), nrow(tt), 100 * mean(tt$sig_fdr == 1, na.rm = TRUE)))
    }
  }
  lg("done: ", dst); invisible(dst)
}

# ---- selftest --------------------------------------------------------------------------------------------------
selftest <- function() {
  ok <- function(c, m) if (!isTRUE(c)) stop("SELFTEST FAIL: ", m) else cat("  ok  ", m, "\n")
  # 1. MK on a textbook case: x = 1..10 -> S = 45, var = 125, Z = 44/sqrt(125) = 3.935, p = 8.3e-5; Sen slope 1
  m <- mk_sen(1:10, 1:10, min_n = 5)
  ok(abs(m[1] - 1) < 1e-12 && abs(m[2] - 2 * pnorm(-44 / sqrt(125))) < 1e-12, "MK/Sen: x = 1..10 gives slope 1 and the textbook p (no autocorrelation correction on a constant residual)")
  set.seed(1); xn <- rnorm(43)
  ok(mk_sen(xn, 1982:2024)[2] > 0.05, "MK: white noise is not significant")
  ok(is.na(mk_sen(c(1:20, rep(NA, 23)), 1982:2024)[1]), "MK: fewer than 30 valid seasons gives NA")
  # 2. end-to-end on a synthetic root: 3x3 cells, layers 1981..2024 (1981 = spin-up, dropped)
  td <- file.path(tempdir(), "s7_selftest"); unlink(td, recursive = TRUE)
  src <- file.path(td, "Data", "obs_seasonal", "obstest_1982-2025", "ggcmi-mai"); dir.create(src, recursive = TRUE)
  base <- rast(nrows = 3, ncols = 3, xmin = 30, xmax = 30.75, ymin = 0, ymax = 0.75, crs = "EPSG:4326")
  yrs <- 1981:2024; set.seed(2)
  lay <- lapply(yrs, function(y) {
    v <- rep(10, 9) + rnorm(9, 0, 0.3)
    v[1] <- 5 + 0.2 * (y - 1981) + rnorm(1, 0, 0.3)       # cell 1: +0.2 per year = +2 per decade
    v[5] <- 10 + rnorm(1, 0, 1)                           # cell 5: noise only
    v[9] <- NA                                            # cell 9: no data
    if (y == 1981) v[] <- 999                             # spin-up layer must never be read
    setValues(base, v)
  })
  s <- rast(lay); names(s) <- yrs
  writeRaster(s, file.path(src, "NDWS_mean.tif"), overwrite = TRUE)
  out <- run(td, "obstest", "1982-2025", 1982:2024, normal = 1991:2020)
  od <- file.path(out, "ggcmi-mai")
  tr <- rast(file.path(od, "NDWS_mean_trend.tif")); nr <- rast(file.path(od, "NDWS_mean_normal_1991-2020.tif"))
  az <- rast(file.path(od, "NDWS_mean_anomaly_z.tif")); ad <- rast(file.path(od, "NDWS_mean_anomaly_diff.tif"))
  v <- values(tr)
  ok(abs(v[1, "slope_per_decade"] - 2) < 0.15 && v[1, "p_mk"] < 1e-6 && v[1, "sig_fdr"] == 1, "trend: the +2/decade cell is recovered and FDR-significant")
  ok(v[5, "p_mk"] > 0.05 && v[5, "sig_fdr"] == 0, "trend: the noise cell is not significant")
  ok(is.na(v[9, "slope_per_decade"]) && v[1, "n_years"] == 43, "trend: NA cell stays NA; 43 seasons used (1981 spin-up dropped)")
  x1 <- values(s)[1, as.character(1991:2020)]
  ok(abs(values(nr)[1, "mean"] - mean(x1)) < 1e-4 && abs(values(nr)[1, "sd"] - sd(x1)) < 1e-4, "normal: mean and sd over exactly 1991-2020")
  ok(nlyr(az) == 43 && names(az)[1] == "1982" && abs(values(az)[1, "2024"] - (values(s)[1, "2024"] - mean(x1)) / sd(x1)) < 1e-4 &&
       abs(values(ad)[1, "2024"] - (values(s)[1, "2024"] - mean(x1))) < 1e-4, "anomalies: 43 seasons, z and diff against the 1991-2020 normal")
  ok(max(values(s[["1981"]]), na.rm = TRUE) == 999 && max(abs(values(az)), na.rm = TRUE) < 50, "the 999 spin-up layer never reaches the outputs")
  r2 <- try(run(td, "obstest", "1982-2025", 1982:2025), silent = TRUE)
  ok(inherits(r2, "try-error") && grepl("lacks seasons", r2), "asking for a season the stack does not hold stops")
  # 3. admin aggregation: two units (west column / rest); area weights on a regular 0.25 grid near the equator
  w1 <- data.table(ID = c(1, 1, 1, 2, 2), cell = c(1, 4, 7, 2, 5), weight = 1)
  am <- admin_means(s[[c("2000", "2001")]], w1)
  ok(abs(am[ID == 2, `2000`] - mean(values(s)[c(2, 5), "2000"])) < 1e-6, "admin: unit mean = weighted mean of its cells")
  # 4. the full admin path on real polygons: west column (cells 1,4,7) = unit A; the other six cells = unit B
  pa <- vect("POLYGON((30 0, 30.25 0, 30.25 0.75, 30 0.75, 30 0))", crs = "EPSG:4326")
  pb <- vect("POLYGON((30.25 0, 30.75 0, 30.75 0.75, 30.25 0.75, 30.25 0))", crs = "EPSG:4326")
  uv <- rbind(pa, pb); uv$iso3 <- c("AAA", "BBB"); uv$admin0_name <- c("A", "B")
  units <- list(admin0 = uv); wts <- lapply(units, unit_weights, template = s[[1]])
  ok(setequal(wts$admin0[ID == 1, cell], c(1, 4, 7)) && all(abs(wts$admin0$weight / max(wts$admin0$weight) - 1) < 0.01),
     "admin weights: unit A holds cells 1/4/7, full-cell area weights (equal near the equator)")
  ad2 <- admin_stack(file.path(src, "NDWS_mean.tif"), units, wts, "ggcmi-mai", 1982:2024, 1991:2020, 25L, 30L)
  sa <- ad2$series$admin0; ta <- ad2$trends$admin0
  wA <- wts$admin0[ID == 1][order(cell)]; cA <- values(s)[wA$cell, as.character(1982:2024)]
  serA <- colSums(cA * wA$weight) / sum(wA$weight)            # area-weighted, as the code does (cells differ by < 1e-4 here)
  ok(nrow(sa) == 2 * 43 && abs(sa[iso3 == "AAA" & year == 2000, value] - serA["2000"]) < 1e-4,
     "admin series: 43 seasons per unit; unit value = area-weighted mean of its cells")
  ok(abs(ta[iso3 == "AAA", slope_per_decade] - mk_sen(serA, 1982:2024)[1] * 10) < 1e-6 &&
       abs(ta[iso3 == "AAA", normal_mean] - mean(serA[as.character(1991:2020)])) < 1e-6,
     "admin trend and normal are computed on the unit's own mean series")
  ok(abs(sa[iso3 == "AAA" & year == 2024, anomaly_z] - (serA["2024"] - mean(serA[as.character(1991:2020)])) / sd(serA[as.character(1991:2020)])) < 1e-6,
     "admin anomaly z against the unit's own 1991-2020 normal")
  # 5. the FULL run() with admin units (the path that crashed on the node): tables written, weights cached and reused
  out2 <- run(td, "obstest", "1982-2025", 1982:2024, normal = 1991:2020, units = units, overwrite = TRUE)
  st <- arrow::read_parquet(file.path(out2, "admin_series_admin0.parquet")); tt <- arrow::read_parquet(file.path(out2, "admin_trends_admin0.parquet"))
  cache <- list.files(file.path(td, "Data", "obs_trends", "_admin_weights"), "\\.rds$")
  ok(nrow(st) == 2 * 43 && nrow(tt) == 2 && length(cache) == 1, "run() with admin units writes the series and trend tables and caches the weights")
  out3 <- capture.output(run(td, "obstest", "1982-2025", 1982:2024, normal = 1991:2020, units = units, cores = 2L, overwrite = TRUE))
  ok(any(grepl("admin weights from cache", out3)), "a second run reuses the cached admin weights")
  tr2 <- rast(file.path(out2, "ggcmi-mai", "NDWS_mean_trend.tif"))
  ok(isTRUE(all.equal(values(tr2), values(tr)[, names(tr2)], tolerance = 1e-9)),
     "cores = 2 (PSOCK workers, as on the node with --cores 16) gives the same trend raster as cores = 1")
  unlink(td, recursive = TRUE)
  cat("\nALL S7 SELFTEST ASSERTIONS PASSED\n")
}

if (sys.nframe() == 0) {
  a <- commandArgs(TRUE)
  getopt <- function(k, d = NULL) { i <- match(k, a); if (is.na(i)) d else a[i + 1] }
  if ("--selftest" %in% a) { selftest(); quit(status = 0) }
  rng <- function(s) { p <- as.integer(strsplit(s, ":")[[1]]); p[1]:p[2] }
  lst <- function(k) { x <- getopt(k); if (is.null(x)) NULL else strsplit(x, ",")[[1]] }
  obs_root <- getopt("--obs-root"); if (is.null(obs_root) || !startsWith(obs_root, "/")) stopf("--obs-root must be absolute")
  run(obs_root, getopt("--token", "chirps-chirts-agera5"), getopt("--timeframe", "1982-2025"), rng(getopt("--years", "1982:2024")),
      normal = rng(getopt("--normal", "1991:2020")), windows = lst("--windows"), indices = lst("--indices"),
      boundaries_dir = getopt("--boundaries-dir"), cores = as.integer(getopt("--cores", "1")), overwrite = "--overwrite" %in% a)
}
