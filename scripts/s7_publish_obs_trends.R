#!/usr/bin/env Rscript
# scripts/s7_publish_obs_trends.R
# ===============================
# Publish the observed S7 products (R/obs/s7_trends_anomalies.R; PLAN_obs_hazard_pipeline.md "S7 design",
# Pete 2026-11-05): per-cell trend, normal and anomaly COGs for every (window, index) stack, plus the admin0/1/2
# `trends` and `series` tables, for both tokens (main, and the -rh15 THI sensitivity variant).
# First publish of a versioned key set. A version is never overwritten: if any target key exists the run stops
# (with --resume, an existing key is skipped only when its size and MD5 match the local file).
#
# KEYS (s3://digital-atlas/; layout proposed 2026-11-14, version segment as approved by Brayden for domain=soil):
#   domain=climate/type=hazard-indices/source=<token>/region=africa/processing=<p>/version=<v>/
#     window=<w>/variable=<IDX>_<stat>/<IDX>_<stat>_<w>_<product>_<period>_<v>.tif
#     <p> = trends    product trend         period 1982-2024  bands slope_per_decade, p_mk, sig_fdr, n_years
#           normals   product normal        period 1991-2020  bands mean, sd, p10, p50, p90
#           anomalies product anomaly-diff  period 1982-2024  one band per season (start year)
#           anomalies product anomaly-z     period 1982-2024  one band per season (start year)
#   domain=climate/type=hazard-indices/source=<token>/region=africa/processing=trends/version=<v>/
#     admin_trends_<level>_<period>_<v>.parquet
#   domain=climate/type=hazard-indices/source=<token>/region=africa/processing=anomalies/version=<v>/
#     admin_series_<level>_<period>_<v>.parquet
#   every object has a `<key>.json` provenance sidecar (bands or field_descriptions, caveats, checksums, code commit);
#   one manifest per version: domain=climate/type=hazard-indices/source=<main token>/region=africa/
#     manifest_trends-normals-anomalies_<period>_<v>.csv  (uploaded LAST: its presence means the set is complete)
#
# LOCAL SOURCES: <obs-root>/Data/obs_trends/<token>_<period>/<window>/<IDX>_<stat>_{trend,normal_<N>,anomaly_diff,anomaly_z}.tif
#                <obs-root>/Data/obs_trends/<token>_<period>/admin_{trends,series}_admin{0,1,2}.parquet
#   gate P4 also reads the S7 INPUT stacks <obs-root>/Data/obs_seasonal/<token>_<timeframe>/<window>/<IDX>_<stat>.tif
#
# GATES (all run in every mode; any FAIL stops the run before a single upload):
#   P1 inventory: per token, stacks == --expect-stacks, each stack has all four COGs, all six tables, keys unique;
#      stray .tif files in a window dir are listed (not published)
#   P2 COG: LAYOUT=COG, overviews on band 1 when the raster is wider/taller than 512, EPSG:4326, band names as
#      above, the four files of a stack on one grid and on the input stack's grid
#   P3 trend + normal values: p_mk in (0, 1] and >= the plain MK p of a perfectly monotonic series of that cell's n_years (3.7e-21 at 43), sig_fdr in {0,1},
#      slope / p_mk / sig_fdr share one NA mask, n_years in [30, n seasons] where p_mk is set; normal sd >= 0,
#      p10 <= p50 <= p90, one NA mask across the five normal bands. BH recomputed from the float32 p_mk is a REPORT
#      (rounding can move a borderline cell); the table gate P5 recomputes it exactly.
#   P4 anomalies against the S7 INPUT stack (independent of the S7 outputs except the normal, which U4 gated
#      identical): anomaly_diff = value - normal mean, anomaly_z = anomaly_diff / normal sd (NA where sd is not > 0),
#      NA masks equal, within float32 tolerance. U4 (M-20261119-s7-1) did not gate these 228 files.
#   P5 tables: expected columns; p_mk in (0, 1], none below the monotonic floor for its n_years, none exactly 0; sig_fdr == BH(p_mk) at 0.05 within
#      (level, window, index, weighting), exact; weighting in {area, crop_production, livestock_tlu}, livestock_tlu
#      only on annual12; series: 43 seasons per unit x window x index x weighting, anomaly_diff = value -
#      normal_mean of the trends table, anomaly_z = anomaly_diff / normal_sd (cross-table, 1e-9).
#   P6 S3: no target key exists (--resume: an existing key must match size and MD5, and is skipped).
#
# Usage (on CGlabs; background anything but --selftest):
#   Rscript scripts/s7_publish_obs_trends.R --selftest
#   Rscript scripts/s7_publish_obs_trends.R --obs-root $OL                # dry run (default): gates, staging, no S3 write
#   Rscript scripts/s7_publish_obs_trends.R --obs-root $OL --publish      # gates, then upload (public-read)
#   Rscript scripts/s7_publish_obs_trends.R --obs-root $OL --publish --resume
#   Rscript scripts/s7_publish_obs_trends.R --obs-root $OL --verify       # read back every key; writes pins + files CSV
# Options: --tokens a,b  --expect-stacks a=95,b=19  --years 1982:2024  --normal 1991:2020  --timeframe 1982-2025
#          --version v1.0  --staging <dir> (default <obs-root>/Data/obs_trends/_publish_<version>)
suppressPackageStartupMessages({ library(terra); library(data.table) })
.ts <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) cat(sprintf("[%s] [s7-publish] %s\n", .ts(), sprintf(fmt, ...)))
stopf <- function(...) stop("[s7-publish] STOP: ", sprintf(...), call. = FALSE)

BUCKET   <- "digital-atlas"
KEY_ROOT <- "domain=climate/type=hazard-indices"
CODE_URL <- "https://github.com/AdaptationAtlas/hazards_prototype"
CDH_ID   <- "africa-hazard-season-trends-observed"
PROBE_KEY <- paste0("domain=hazard_exposure/source=chirps-chirts-agera5/region=ssa/processing=hazard-risk-exposure/",
                    "variable=harv-area_ha/period=ggcmi-crop/model=chirps-chirts-agera5/severity=extreme/int=multi-hazard.parquet")
# Smallest p a series of n seasons can reach: plain Mann-Kendall (no ties) on a perfectly monotonic series,
# S = n(n-1)/2. The inflate-only Hamed-Rao factor can only raise p above it. 3.7e-21 at n = 43. (The 5e-17 in
# M-20261119-s7-1 came from a test series whose float residuals carried a factor > 1; it is not a hard floor.)
mk_floor <- function(n) 2 * stats::pnorm(-(n * (n - 1) / 2 - 1) / sqrt(n * (n - 1) * (2 * n + 5) / 18)) * (1 - 1e-6)
UNIT_KEYS <- c("iso3", "admin0_name", "admin1_name", "admin2_name", "gaul0_code", "gaul1_code", "gaul2_code")
PRODUCTS <- data.table(
  stem       = c("trend", "normal", "anomaly_diff", "anomaly_z"),
  product    = c("trend", "normal", "anomaly-diff", "anomaly-z"),
  processing = c("trends", "normals", "anomalies", "anomalies"))

# ---- text that must reach the sidecars and the CDH record (HANDOVER_2026-11-19 §2.1) ---------------------------
CAVEATS <- c(
  season_label = paste("Seasons are labelled by their START year. A season that starts in one year and ends in the next",
                       "carries the first year: eastern Kenya plants in October, so season 2010 is the OND-2010 failure."),
  normal       = "The normal is the WMO 1991-2020 reference period; a cell or unit needs at least 25 valid seasons of the 30, otherwise NA.",
  chirps       = paste("CHIRPS blends a station network whose density changes over time, so part of a rainfall trend",
                       "(PTOT, and the soil-water indices NDWS and NDWL0 that use rainfall) may be an artefact of the network, not climate."),
  chirts_coast = paste("CHIRTS-ERA5 daily temperatures were repaired at coastal-edge pixels above physical caps (57 C Tmax,",
                       "43 C Tmin; median of the valid 5x5 neighbours), mostly on the Red Sea, Gulf of Aden and Gulf coasts."),
  soil_fill    = paste("NDWS and NDWL0 use soil layers whose coverage gaps were filled by inverse-distance interpolation",
                       "within 0.5 degrees (hazards_prototype issue #56); the projected NEX-GDDP products use the unfilled layers."),
  islands      = "Island states outside the ingest bounding box (Mauritius, Cabo Verde, Seychelles) have no values (issue #57).")
P_MK_TEXT <- paste("Two-sided Mann-Kendall p-value of the trend over the seasons 1982-2024, with the Hamed-Rao (1998)",
                   "autocorrelation correction applied inflate-only: the test variance is widened for positive serial",
                   "correlation and never narrowed, so p is never smaller than the uncorrected Mann-Kendall p.")
SIG_TEXT_RASTER <- paste("1 = trend significant at a false discovery rate of 0.05 (Benjamini-Hochberg over the valid cells of",
                         "this one window x index stack), 0 = not significant. The control is within one stack only.")
SIG_TEXT_TABLE <- paste("1 = trend significant at a false discovery rate of 0.05 (Benjamini-Hochberg over all units of one",
                        "admin level x window x index x weighting), 0 = not significant. The control is within that group only.")
WEIGHTING_TEXT <- paste(
  "How the cell values are averaged into the unit. area: cell area times boundary coverage, so every land cell counts and",
  "non-producing land dominates. crop_production: MapSPAM 2020 production (t) of the crops that use this window.",
  "livestock_tlu: GLW4 2020 head counts in Tropical Livestock Units (cattle 0.7, sheep and goats 0.1, pigs 0.2,",
  "poultry 0.01), annual12 window only. READ crop windows by crop_production and the livestock (annual12) window by",
  "livestock_tlu; use area only for a land-area view.")
WINDOW_TEXT <- paste("Season window. ggcmi-<crop> = that crop's GGCMI rainfed growing season per cell; annual12 = 12 months",
                     "from the cell's maize planting month (livestock, and crops without a GGCMI calendar); custom-mai_plant-8",
                     "and custom-mai_maturity-4 = 8 months from maize planting and 4 months to maize maturity.")
INDEX_TEXT <- paste("Hazard index and the statistic taken over the window's months: NDWS soil water-stress days, NDWL0",
                    "waterlogging days, NTx35 days above 35 C (each per month; _mean = mean over the window's months);",
                    "PTOT_sum = total precipitation over the window (mm); THI_max_max = highest monthly maximum",
                    "livestock temperature-humidity index (unitless).")
FIELD_DOCS <- list(
  level = "Admin level of the row: admin0 (country), admin1, admin2 (FAO GAUL 2024).",
  iso3 = "ISO 3166-1 alpha-3 country code.", admin0_name = "Country name (GAUL 2024).",
  admin1_name = "First-level unit name (GAUL 2024).", admin2_name = "Second-level unit name (GAUL 2024).",
  gaul0_code = "GAUL 2024 admin0 code.", gaul1_code = "GAUL 2024 admin1 code.", gaul2_code = "GAUL 2024 admin2 code.",
  window = WINDOW_TEXT, index = INDEX_TEXT, weighting = WEIGHTING_TEXT,
  slope_per_decade = "Theil-Sen slope of the unit's seasonal series over 1982-2024, in index units per decade.",
  p_mk = P_MK_TEXT, sig_fdr = SIG_TEXT_TABLE,
  n_years = "Valid seasons in the trend (of 43); slope and p are NA below 30.",
  normal_mean = "Mean of the unit's seasonal values over the WMO 1991-2020 normal (NA below 25 valid seasons).",
  normal_sd = "Standard deviation over the WMO 1991-2020 normal.",
  year = "Season, labelled by its start year (1982-2024).",
  value = "The unit's seasonal index value (weighted mean of its cells, per `weighting`).",
  anomaly_diff = "value - normal_mean, in index units.",
  anomaly_z = "anomaly_diff / normal_sd; NA where normal_sd is not > 0.")
BAND_DOCS <- list(
  trend = list(slope_per_decade = "Theil-Sen slope over seasons 1982-2024, index units per decade.", p_mk = P_MK_TEXT,
               sig_fdr = SIG_TEXT_RASTER, n_years = "Valid seasons in the trend (of 43); slope and p are NA below 30."),
  normal = list(mean = "Mean over the WMO 1991-2020 normal.", sd = "Standard deviation over the normal.",
                p10 = "10th percentile over the normal.", p50 = "Median over the normal.", p90 = "90th percentile over the normal."),
  `anomaly-diff` = list(`<season>` = "One band per season (named by its start year): value - normal mean, index units."),
  `anomaly-z` = list(`<season>` = "One band per season (named by its start year): (value - normal mean) / normal sd; NA where sd is not > 0."))

# ---- keys and inventory -----------------------------------------------------------------------------------------
yr_tag <- function(y) sprintf("%d-%d", min(y), max(y))
key_of <- function(token, processing, version, file, window = NA_character_, variable = NA_character_) {
  parts <- c(KEY_ROOT, paste0("source=", token), "region=africa", paste0("processing=", processing), paste0("version=", version))
  if (!is.na(window)) parts <- c(parts, paste0("window=", window))
  if (!is.na(variable)) parts <- c(parts, paste0("variable=", variable))
  paste(c(parts, file), collapse = "/")
}
raster_file <- function(variable, window, product, period, version) sprintf("%s_%s_%s_%s_%s.tif", variable, window, product, period, version)
manifest_key <- function(token, years, version)
  sprintf("%s/source=%s/region=africa/manifest_trends-normals-anomalies_%s_%s.csv", KEY_ROOT, token, yr_tag(years), version)

inventory <- function(trend_root, tokens, years, normal, version) {
  yr <- yr_tag(years); nr <- yr_tag(normal); rows <- list(); strays <- character()
  stem_re <- sprintf("_(trend|normal_%s|anomaly_diff|anomaly_z)\\.tif$", nr)
  for (tok in tokens) {
    d <- file.path(trend_root, paste0(tok, "_", yr))
    if (!dir.exists(d)) stopf("no S7 output dir %s", d)
    wins <- sort(list.dirs(d, full.names = FALSE, recursive = FALSE)); wins <- wins[!grepl("^_", wins)]
    for (w in wins) {
      tifs <- list.files(file.path(d, w), "\\.tif$")
      strays <- c(strays, file.path(tok, w, tifs[!grepl(stem_re, tifs)]))
      for (b in sort(unique(sub(stem_re, "", tifs[grepl(stem_re, tifs)])))) for (i in seq_len(nrow(PRODUCTS))) {
        pr <- PRODUCTS[i]
        lf <- file.path(d, w, sprintf("%s_%s.tif", b, if (pr$stem == "normal") paste0("normal_", nr) else pr$stem))
        per <- if (pr$stem == "normal") nr else yr
        rows[[length(rows) + 1]] <- dtk(token = tok, kind = "raster", window = w, variable = b, level = NA_character_,
          product = pr$product, processing = pr$processing, local = lf,
          key = key_of(tok, pr$processing, version, raster_file(b, w, pr$product, per, version), w, b))
      }
    }
    for (kd in c("trends", "series")) for (lv in c("admin0", "admin1", "admin2")) {
      pr <- if (kd == "trends") "trends" else "anomalies"
      rows[[length(rows) + 1]] <- dtk(token = tok, kind = "table", window = NA_character_, variable = NA_character_, level = lv,
        product = paste0("admin-", kd), processing = pr, local = file.path(d, sprintf("admin_%s_%s.parquet", kd, lv)),
        key = key_of(tok, pr, version, sprintf("admin_%s_%s_%s_%s.parquet", kd, lv, yr, version)))
    }
  }
  inv <- rbindlist(rows); inv[, exists := file.exists(local)]
  attr(inv, "strays") <- strays
  inv
}

# ---- gates --------------------------------------------------------------------------------------------------------
bh <- function(p, alpha = 0.05) { out <- rep(NA_real_, length(p)); ok <- !is.na(p); out[ok] <- as.numeric(stats::p.adjust(p[ok], "BH") <= alpha); out }
gate_inventory <- function(inv, expect) {
  ok <- TRUE
  for (tok in unique(inv$token)) {
    st <- unique(inv[token == tok & kind == "raster", .(window, variable)])
    miss <- inv[token == tok & !exists]
    nt <- inv[token == tok & kind == "table" & exists, .N]
    e <- expect[[tok]]
    pass <- (is.null(e) || nrow(st) == e) && nrow(miss) == 0 && nt == 6
    .log("P1 %s %s | stacks %d (expected %s) | files missing %d | tables %d of 6", tok, if (pass) "PASS" else "FAIL",
         nrow(st), if (is.null(e)) "-" else e, nrow(miss), nt)
    if (nrow(miss)) for (f in head(miss$local, 10)) .log("   missing %s", f)
    ok <- ok && pass
  }
  du <- inv[duplicated(key), key]
  if (length(du)) { .log("P1 FAIL | %d duplicate keys, e.g. %s", length(du), du[1]); ok <- FALSE }
  st <- attr(inv, "strays")
  .log("P1 stray .tif in window dirs (not published): %d%s", length(st), if (length(st)) paste0(", e.g. ", st[1]) else "")
  ok
}
band_names <- function(product, years) switch(product, trend = c("slope_per_decade", "p_mk", "sig_fdr", "n_years"),
                                              normal = c("mean", "sd", "p10", "p50", "p90"), as.character(years))
gate_cog <- function(inv, years, src_of) {
  bad <- 0L; n <- 0L
  for (s in split(inv[kind == "raster"], by = c("token", "window", "variable"))) {
    rs <- lapply(s$local, rast); ref <- rast(src_of(s$token[1], s$window[1], s$variable[1]))
    geo_ok <- all(vapply(rs, function(r) isTRUE(compareGeom(r, ref, stopOnError = FALSE)), logical(1)))
    for (i in seq_len(nrow(s))) {
      n <- n + 1L; f <- s$local[i]; r <- rs[[i]]; info <- terra::describe(f)
      b1 <- grep("^Band 1 ", info); b2 <- grep("^Band 2 ", info)
      seg <- if (length(b1)) info[b1:(if (length(b2)) b2 - 1L else length(info))] else character()
      need_ov <- max(dim(r)[1:2]) > 512
      chk <- c(cog = any(grepl("LAYOUT=COG", info)), overviews = !need_ov || any(grepl("Overviews:", seg)),
               crs = isTRUE(terra::same.crs(r, "EPSG:4326")), bands = identical(names(r), band_names(s$product[i], years)),
               grid = geo_ok)
      if (!all(chk)) { bad <- bad + 1L; if (bad <= 10) .log("   P2 FAIL %s: %s", basename(f), paste(names(chk)[!chk], collapse = ",")) }
    }
  }
  .log("P2 %s | %d COGs checked, %d failing", if (bad == 0) "PASS" else "FAIL", n, bad)
  bad == 0
}
gate_values <- function(inv, years, min_trend = 30) {
  bad <- 0L; bh_mis <- 0; minp <- Inf
  for (s in split(inv[kind == "raster" & product %in% c("trend", "normal")], by = c("token", "window", "variable"))) {
    tag <- paste(s$token[1], s$window[1], s$variable[1], sep = "/")
    tr <- values(rast(s[product == "trend", local])); nm <- values(rast(s[product == "normal", local]))
    p <- tr[, "p_mk"]; sg <- tr[, "sig_fdr"]; sl <- tr[, "slope_per_decade"]; ny <- tr[, "n_years"]; pv <- p[!is.na(p)]
    if (length(pv)) minp <- min(minp, pv)
    tol <- 1e-6 * (1 + abs(nm[, "p50"]))
    chk <- c(p_range = all(pv > 0 & pv <= 1), p_floor = all(p[!is.na(p)] >= mk_floor(ny[!is.na(p)])), sig01 = all(sg[!is.na(sg)] %in% c(0, 1)),
             na_mask = identical(is.na(p), is.na(sg)) && identical(is.na(p), is.na(sl)),
             n_years = all(ny[!is.na(p)] >= min_trend & ny[!is.na(p)] <= length(years)),
             sd_pos = all(nm[, "sd"] >= 0, na.rm = TRUE),
             pct_order = all(nm[, "p10"] <= nm[, "p50"] + tol & nm[, "p50"] <= nm[, "p90"] + tol, na.rm = TRUE),
             normal_mask = all(rowSums(is.na(nm)) %in% c(0, ncol(nm))))
    bh_mis <- bh_mis + sum(bh(p) != sg, na.rm = TRUE)
    if (!all(chk)) { bad <- bad + 1L; if (bad <= 10) .log("   P3 FAIL %s: %s", tag, paste(names(chk)[!chk], collapse = ",")) }
  }
  .log("P3 %s | stacks failing %d | min p_mk %.3g | report: sig_fdr vs BH recomputed from float32 p_mk differs on %d cells",
       if (bad == 0) "PASS" else "FAIL", bad, minp, as.integer(bh_mis))
  bad == 0
}
gate_anomaly <- function(inv, years, src_of) {
  bad <- 0L; n <- 0L; worst_d <- 0; worst_z <- 0
  for (s in split(inv[kind == "raster"], by = c("token", "window", "variable"))) {
    n <- n + 1L; tag <- paste(s$token[1], s$window[1], s$variable[1], sep = "/")
    x <- values(rast(src_of(s$token[1], s$window[1], s$variable[1]))[[as.character(years)]])
    nm <- values(rast(s[product == "normal", local])); m <- nm[, "mean"]; sdv <- nm[, "sd"]
    d <- values(rast(s[product == "anomaly-diff", local])); z <- values(rast(s[product == "anomaly-z", local]))
    ed <- x - m
    ez_na <- is.na(ed) | is.na(sdv) | !(sdv > 0)
    rd <- abs(d - ed) / (1e-5 * (1 + abs(x) + abs(m)))                         # float32 tolerance, as a ratio
    rz <- abs(z - d / sdv) / (1e-6 * (abs(x) + abs(m)) / sdv + 1e-5 * (1 + abs(z)))
    md <- suppressWarnings(max(rd, na.rm = TRUE)); mz <- suppressWarnings(max(rz[!ez_na], na.rm = TRUE))
    if (is.finite(md)) worst_d <- max(worst_d, md); if (is.finite(mz)) worst_z <- max(worst_z, mz)
    chk <- c(diff_mask = !any(xor(is.na(d), is.na(ed))), diff = !is.finite(md) || md <= 1,
             z_mask = !any(xor(is.na(z), ez_na)), z = !is.finite(mz) || mz <= 1)
    if (!all(chk)) { bad <- bad + 1L; if (bad <= 10) .log("   P4 FAIL %s: %s", tag, paste(names(chk)[!chk], collapse = ",")) }
  }
  .log("P4 %s | %d stacks (%d anomaly COGs) against their input stacks | failing %d | worst error / tolerance: diff %.3g, z %.3g",
       if (bad == 0) "PASS" else "FAIL", n, 2L * n, bad, worst_d, worst_z)
  bad == 0
}
# data.table(key = ...) would bind `key` to data.table's own argument (setkey), not a column
dtk <- function(...) as.data.table(list(...))
read_pq <- function(f) as.data.table(arrow::read_parquet(f))
gate_tables <- function(inv, years) {
  ok <- TRUE
  for (tok in unique(inv$token)) for (lv in c("admin0", "admin1", "admin2")) {
    tt <- read_pq(inv[token == tok & product == "admin-trends" & level == lv, local])
    ss <- read_pq(inv[token == tok & product == "admin-series" & level == lv, local])
    uk <- intersect(UNIT_KEYS, names(tt)); gk <- c("level", "window", "index", "weighting")
    need_t <- c("level", "window", "index", "weighting", "slope_per_decade", "p_mk", "sig_fdr", "n_years", "normal_mean", "normal_sd")
    need_s <- c("level", "window", "index", "weighting", "year", "value", "anomaly_diff", "anomaly_z")
    pv <- tt[!is.na(p_mk), p_mk]
    tt[, sig_exp := bh(p_mk), by = gk]
    cnt <- ss[, .(n = .N, y0 = min(year), y1 = max(year)), by = c(uk, gk)]
    j <- merge(ss, tt[, c(uk, gk, "normal_mean", "normal_sd"), with = FALSE][, .hit := TRUE], by = c(uk, gk), all.x = TRUE)
    ez <- j[, fifelse(normal_sd > 0, anomaly_diff / normal_sd, NA_real_)]
    chk <- c(columns = all(need_t %in% names(tt)) && all(need_s %in% names(ss)) && all(uk %in% names(ss)),
             unique_keys = !anyDuplicated(tt, by = c(uk, gk)),
             p_range = all(pv > 0 & pv <= 1), p_floor = tt[!is.na(p_mk), all(p_mk >= mk_floor(n_years))],
             sig_bh = tt[, isTRUE(all.equal(sig_fdr, sig_exp))],
             weighting = all(tt$weighting %in% c("area", "crop_production", "livestock_tlu")) &&
               all(tt[weighting == "livestock_tlu", window] == "annual12"),
             seasons = all(cnt$n == length(years) & cnt$y0 == min(years) & cnt$y1 == max(years)),
             series_in_trends = !anyNA(j$.hit) && nrow(cnt) == nrow(tt),
             anomaly_diff = identical(is.na(j$anomaly_diff), is.na(j$value - j$normal_mean)) &&
               isTRUE(j[!is.na(anomaly_diff), all(abs(anomaly_diff - (value - normal_mean)) <= 1e-9 * (1 + abs(value)))]),
             anomaly_z = identical(is.na(j$anomaly_z), is.na(ez)) && isTRUE(all(abs(j$anomaly_z - ez) <= 1e-9 * (1 + abs(ez)), na.rm = TRUE)))
    .log("P5 %s %s %s | trends %s rows, series %s rows | min p_mk %.3g | sig_fdr 1: %d%s", tok, lv, if (all(chk)) "PASS" else "FAIL",
         format(nrow(tt), big.mark = ","), format(nrow(ss), big.mark = ","), if (length(pv)) min(pv) else NA,
         tt[sig_fdr == 1, .N], if (all(chk)) "" else paste0(" | failing: ", paste(names(chk)[!chk], collapse = ",")))
    ok <- ok && all(chk)
    rm(tt, ss, j); invisible(gc())
  }
  ok
}
# ---- CDH file inventories (cdh-inventory CSVs next to the record; one per COG data entry) ------------------------
https_of <- function(key) sprintf("https://%s.s3.amazonaws.com/%s", BUCKET, key)
inv_path <- function(dir, processing) file.path(dir, sprintf("%s-files-%s.csv", CDH_ID, processing))
# x: key, token, processing, product, window, variable (+ sha256 to add the checksum column, at --verify)
write_inventories <- function(x, dir) {
  for (p in c("trends", "normals", "anomalies")) {
    y <- x[processing == p]
    d <- data.table(href = https_of(y$key), source = y$token, window = y$window, index = y$variable)
    if (p == "anomalies") d[, variable := sub("-", "_", y$product)]
    if ("sha256" %in% names(y)) d[, checksum := paste0("sha256:", y$sha256)]
    setorderv(d, intersect(c("source", "window", "index", "variable"), names(d)))
    fwrite(d, inv_path(dir, p))
  }
  invisible(dir)
}
# P1b: the COG keys on disk must be exactly the inventories the CDH record points to (record and data cannot diverge)
gate_record <- function(inv, dir) {
  f <- inv_path(dir, c("trends", "normals", "anomalies"))
  if (!all(file.exists(f))) { .log("P1b FAIL | inventory missing: %s", paste(basename(f[!file.exists(f)]), collapse = ",")); return(FALSE) }
  rec <- sub(sprintf("^https://%s\\.s3\\.amazonaws\\.com/", BUCKET), "", unlist(lapply(f, function(z) fread(z)$href)))
  have <- inv[kind == "raster", key]
  miss <- setdiff(rec, have); extra <- setdiff(have, rec)
  ok <- !length(miss) && !length(extra)
  .log("P1b %s | CDH inventories list %d COGs; on disk %d | in record not on disk %d%s | on disk not in record %d%s",
       if (ok) "PASS" else "FAIL", length(rec), length(have), length(miss), if (length(miss)) paste0(" (e.g. ", miss[1], ")") else "",
       length(extra), if (length(extra)) paste0(" (e.g. ", extra[1], ")") else "")
  ok
}
md5_of <- function(f) unname(tools::md5sum(f))
sha256_of <- function(f) {
  cmd <- if (nzchar(Sys.which("sha256sum"))) c("sha256sum", "") else c("shasum", "-a 256")
  out <- system2(cmd[1], c(if (nzchar(cmd[2])) cmd[2], shQuote(f)), stdout = TRUE)
  sub("\\s.*$", "", out[1])
}
gate_s3 <- function(inv, s3_head, resume, probe_key = NULL) {
  # a key that is live (the observed hazard x exposure tier, 2026-11-04) must be found: proves credentials and reach
  if (!is.null(probe_key) && is.null(s3_head(probe_key))) { .log("P6 FAIL | probe key not found, S3 not readable: %s", probe_key); return(list(ok = FALSE, skip = character())) }
  hits <- 0L; matched <- 0L; skip <- character()
  for (i in seq_len(nrow(inv))) {
    h <- s3_head(inv$key[i]); if (is.null(h)) next
    hits <- hits + 1L
    same <- isTRUE(h$size == file.size(inv$local[i])) && isTRUE(!grepl("-", h$etag) && h$etag == md5_of(inv$local[i]))
    if (same) { matched <- matched + 1L; skip <- c(skip, inv$key[i]) }
    else if (hits - matched <= 5) .log("   P6 key exists and differs from local: %s", inv$key[i])
  }
  pass <- hits == 0L || (resume && hits == matched)
  .log("P6 %s | target keys already on S3: %d (identical to local: %d)%s", if (pass) "PASS" else "FAIL", hits, matched,
       if (hits && !resume) " | a version is never overwritten" else "")
  list(ok = pass, skip = skip)
}

# ---- sidecars and manifest -------------------------------------------------------------------------------------
git_head <- function(project_dir) {
  h <- tryCatch(system2("git", c("-C", shQuote(project_dir), "rev-parse", "--short=7", "HEAD"), stdout = TRUE, stderr = FALSE), error = function(e) "")
  if (length(h) && nzchar(h[1])) h[1] else "unknown"
}
sidecar <- function(row, years, normal, version, commit, md5, sha, grid = NULL, fields = NULL) {
  base <- list(
    dataset = CDH_ID, version = version, key = row$key, source = row$token,
    source_description = paste("Observed hazard indices from CHIRPS v3 (rainfall), CHIRTS-ERA5 (temperature) and AgERA5",
                               "(humidity, radiation), 0.25 degree, seasons 1982-2024", if (grepl("rh15$", row$token))
                               "(THI variant: relative humidity at 15:00)" else ""),
    processing = row$processing, product = row$product,
    period = if (row$product == "normal") yr_tag(normal) else yr_tag(years),
    normal_period = yr_tag(normal), date_created = format(Sys.Date()),
    code = list(stage = "R/obs/s7_trends_anomalies.R", publisher = "scripts/s7_publish_obs_trends.R",
                url = sprintf("%s/tree/%s", CODE_URL, commit), commit = commit),
    checksums = list(md5 = md5, sha256 = sha), caveats = as.list(CAVEATS))
  if (row$kind == "raster") {
    base$window <- row$window; base$variable <- row$variable
    base$bands <- BAND_DOCS[[row$product]]; base$grid <- grid
    base$window_description <- WINDOW_TEXT; base$index_description <- INDEX_TEXT
  } else {
    base$admin_level <- row$level; base$field_descriptions <- FIELD_DOCS[intersect(names(FIELD_DOCS), fields)]
    base$read_note <- paste("Pick one weighting per query (filter on `weighting`): crop windows by crop_production, the livestock",
                            "window (annual12, livestock rows) by livestock_tlu. Read with hive partitioning off, or the",
                            "`version=` path segment becomes a column.")
  }
  base
}
stage <- function(inv, staging, years, normal, version, project_dir) {
  commit <- git_head(project_dir); rows <- vector("list", nrow(inv))
  dir.create(staging, FALSE, TRUE)
  for (i in seq_len(nrow(inv))) {
    r <- inv[i]; md5 <- md5_of(r$local); sha <- sha256_of(r$local)
    if (r$kind == "raster") {
      x <- rast(r$local)
      grid <- list(crs = "EPSG:4326", res = res(x), extent = as.vector(ext(x)), dims = dim(x)[1:2], datatype = datatype(x)[1],
                   layout = "COG, ZSTD, internal overviews")
      sc <- sidecar(r, years, normal, version, commit, md5, sha, grid = grid)
      meta <- list(dtype = datatype(x)[1], nlayers = nlyr(x), res = res(x)[1])
    } else {
      fields <- names(arrow::open_dataset(r$local)$schema)
      sc <- sidecar(r, years, normal, version, commit, md5, sha, fields = fields)
      meta <- list(dtype = "parquet", nlayers = NA_integer_, res = NA_real_)
    }
    scf <- file.path(staging, paste0(r$key, ".json")); dir.create(dirname(scf), FALSE, TRUE)
    jsonlite::write_json(sc, scf, auto_unbox = TRUE, pretty = TRUE, digits = NA)
    rows[[i]] <- dtk(key = r$key, sidecar = paste0(r$key, ".json"), bytes = file.size(r$local), md5 = md5, sha256 = sha,
                            source = r$token, processing = r$processing, product = r$product, window = r$window,
                            variable = r$variable, level = r$level, dtype = meta$dtype, nlayers = meta$nlayers,
                            res = meta$res, crs = "EPSG:4326", code_commit = commit)
  }
  man <- rbindlist(rows)
  mf <- file.path(staging, "manifest.csv"); fwrite(man, mf)
  .log("staged %d sidecars + manifest under %s (code %s; %.2f GB of objects)", nrow(man), staging, commit, sum(man$bytes) / 1e9)
  man
}

# ---- S3 ---------------------------------------------------------------------------------------------------------
s3_head_paws <- function() {
  svc <- paws.storage::s3()
  function(key) tryCatch({
    h <- svc$head_object(Bucket = BUCKET, Key = key)
    one <- function(x) if (length(x)) as.character(x)[1] else NA_character_     # absent header -> NA, never a dropped column
    list(size = as.numeric(h$ContentLength), etag = gsub('"', "", one(h$ETag)), version_id = one(h$VersionId),
         last_modified = format(as.POSIXct(h$LastModified, tz = "UTC"), "%Y-%m-%dT%H:%M:%OS3Z"))
  }, error = function(e) {
    # only a 404 means "no such key"; anything else (403, network, credentials) must not read as "absent"
    if (grepl("404|Not Found|NoSuchKey", conditionMessage(e))) NULL else stopf("S3 HEAD %s failed: %s", key, conditionMessage(e))
  })
}
upload_one <- function(local, key, content_type, s3_head) {
  url <- sprintf("s3://%s/%s", BUCKET, key)
  s3fs::s3_file_upload(local, url, overwrite = FALSE, ACL = "public-read", ContentType = content_type,
                       CacheControl = "public, max-age=31536000, immutable")
  h <- s3_head(key)
  if (is.null(h) || !isTRUE(h$size == file.size(local))) stopf("size mismatch after upload: %s", key)
  h
}
publish <- function(inv, man, staging, skip, s3_head, manifest_key) {
  ct <- function(k) if (grepl("\\.tif$", k)) "image/tiff; application=geotiff; profile=cloud-optimized" else
    if (grepl("\\.parquet$", k)) "application/vnd.apache.parquet" else if (grepl("\\.json$", k)) "application/json" else "text/csv"
  prog <- file.path(staging, "progress.csv"); t0 <- Sys.time(); done <- 0L
  ord <- c(which(inv$kind == "raster"), which(inv$kind == "table"))
  for (i in ord) {
    r <- inv[i]
    if (!(r$key %in% skip)) { upload_one(r$local, r$key, ct(r$key), s3_head); done <- done + 1L }
    sk <- paste0(r$key, ".json")
    if (is.null(s3_head(sk))) upload_one(file.path(staging, sk), sk, ct(sk), s3_head)
    fwrite(dtk(key = r$key, at = .ts()), prog, append = file.exists(prog))
    if (done %% 50 == 0 && done) .log("  %d objects uploaded (%.1f min)", done, as.numeric(difftime(Sys.time(), t0, units = "mins")))
  }
  upload_one(file.path(staging, "manifest.csv"), manifest_key, "text/csv", s3_head)
  .log("PUBLISHED | %d objects uploaded now, %d skipped (resume) | + %d sidecars | manifest %s | %.1f min",
       done, length(skip), nrow(inv), manifest_key, as.numeric(difftime(Sys.time(), t0, units = "mins")))
}
verify <- function(man, s3_head, manifest_key, project_dir, version, years, record_dir) {
  keys <- c(man$key, man$sidecar, manifest_key); bad <- 0L; pins <- vector("list", length(keys)); t0 <- Sys.time()
  md5_by_key <- setNames(man$md5, man$key); bytes_by_key <- setNames(man$bytes, man$key)
  for (i in seq_along(keys)) {
    k <- keys[i]; h <- s3_head(k)
    if (is.null(h)) { bad <- bad + 1L; .log("   VERIFY missing %s", k); next }
    if (k %in% man$key) {
      ok_size <- isTRUE(h$size == bytes_by_key[[k]])
      ok_md5 <- if (!grepl("-", h$etag)) h$etag == md5_by_key[[k]] else {
        tmp <- tempfile(); s3fs::s3_file_download(sprintf("s3://%s/%s", BUCKET, k), tmp)
        m <- md5_of(tmp); unlink(tmp); m == md5_by_key[[k]]
      }
      https <- sprintf("https://%s.s3.amazonaws.com/%s", BUCKET, k)
      rc <- tryCatch(system2("curl", c("-s", "-o", "/dev/null", "-w", "%{http_code}", "-r", "0-0", shQuote(https)), stdout = TRUE), error = function(e) NA)
      ok_get <- isTRUE(rc[1] %in% c("200", "206"))
      if (!(ok_size && ok_md5 && ok_get)) { bad <- bad + 1L; if (bad <= 10) .log("   VERIFY FAIL %s: size %s md5 %s public GET %s", k, ok_size, ok_md5, rc[1]) }
    }
    pins[[i]] <- dtk(family = sub("^.*source=([^/]+)/region=africa/(processing=([^/]+))?.*$", "\\1_\\3", k), key = k,
                            size_bytes = h$size, last_modified = h$last_modified, etag = h$etag, version_id = h$version_id)
    if (i %% 200 == 0) .log("  verified %d of %d (%.1f min)", i, length(keys), as.numeric(difftime(Sys.time(), t0, units = "mins")))
  }
  pin_f <- file.path(project_dir, "metadata", "cdh", "pins", sprintf("atlas-hazards-observed-trends-%s_s3-versions.csv", version))
  fwrite(rbindlist(pins), pin_f)
  # the CDH inventories, rewritten with the SHA-256 of every COG (same rows as before: P1b checked them)
  write_inventories(man[processing %in% c("trends", "normals", "anomalies") & grepl("\\.tif$", key),
                        .(key, token = source, processing, product, window, variable, sha256)], record_dir)
  fi_f <- basename(inv_path(record_dir, "{trends,normals,anomalies}"))
  .log("VERIFY %s | %d keys (%d objects + sidecars + manifest), failing %d | pins %s | file index %s",
       if (bad == 0) "PASS" else "FAIL", length(keys), nrow(man), bad, pin_f, fi_f)
  bad == 0
}

# ---- main -------------------------------------------------------------------------------------------------------
run <- function(obs_root, tokens, expect, years, normal, timeframe, version, staging, mode, resume, project_dir,
                s3_head = NULL, src_root = NULL, record_dir = file.path(project_dir, "metadata", "cdh"), probe_key = NULL) {
  trend_root <- file.path(obs_root, "Data", "obs_trends")
  src_root <- if (is.null(src_root)) file.path(obs_root, "Data", "obs_seasonal") else src_root
  src_of <- function(tok, w, v) file.path(src_root, paste0(tok, "_", timeframe), w, paste0(v, ".tif"))
  .log("=== mode %s%s | tokens %s | version %s | seasons %s, normal %s ===", mode, if (resume) " (resume)" else "",
       paste(tokens, collapse = ","), version, yr_tag(years), yr_tag(normal))
  inv <- inventory(trend_root, tokens, years, normal, version)
  if (mode == "verify") {
    mf <- file.path(staging, "manifest.csv"); if (!file.exists(mf)) stopf("no staged manifest %s", mf)
    return(invisible(verify(fread(mf, colClasses = list(character = c("window", "variable", "level"))), s3_head,
                            manifest_key(tokens[1], years, version), project_dir, version, years, record_dir)))
  }
  g <- c(P1 = gate_inventory(inv, expect) && gate_record(inv, record_dir))
  if (!g["P1"]) stopf("P1 inventory failed; nothing else checked")
  g <- c(g, P2 = gate_cog(inv, years, src_of), P3 = gate_values(inv, years), P4 = gate_anomaly(inv, years, src_of),
         P5 = gate_tables(inv, years))
  s3g <- if (is.null(s3_head)) list(ok = NA, skip = character()) else gate_s3(inv, s3_head, resume, probe_key)
  g <- c(g, P6 = s3g$ok)
  if (!all(g, na.rm = TRUE)) stopf("gate(s) failed: %s; nothing uploaded", paste(names(g)[!is.na(g) & !g], collapse = ","))
  .log("GATES PASS %s", paste(names(g), collapse = " "))
  man <- stage(inv, staging, years, normal, version, project_dir)
  if (mode == "publish") publish(inv, man, staging, s3g$skip, s3_head, manifest_key(tokens[1], years, version))
  else .log("DRY RUN | would upload %d objects + %d sidecars + manifest (%.2f GB) | first key %s | manifest %s",
            nrow(man), nrow(man), sum(man$bytes) / 1e9, man$key[1], manifest_key(tokens[1], years, version))
  invisible(list(inv = inv, man = man, gates = g))
}

project_root <- function() {
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  if (length(fa)) dirname(dirname(normalizePath(sub("^--file=", "", fa[1])))) else getwd()
}

selftest <- function() {
  ok <- function(c, m) if (!isTRUE(c)) stop("SELFTEST FAIL: ", m, call. = FALSE) else cat("  ok  ", m, "\n")
  fails <- function(expr, gate) { e <- tryCatch({ capture.output(expr); NULL }, error = function(e) e)
    inherits(e, "error") && grepl(gate, conditionMessage(e)) }   # stopped, and by the gate named
  pr <- project_root(); s7 <- file.path(pr, "R", "obs", "s7_trends_anomalies.R")
  if (!file.exists(s7)) stop("selftest needs ", s7)
  # 1. key scheme
  ok(identical(key_of("chirps-chirts-agera5", "trends", "v1.0", raster_file("NDWS_mean", "ggcmi-mai", "trend", "1982-2024", "v1.0"), "ggcmi-mai", "NDWS_mean"),
               "domain=climate/type=hazard-indices/source=chirps-chirts-agera5/region=africa/processing=trends/version=v1.0/window=ggcmi-mai/variable=NDWS_mean/NDWS_mean_ggcmi-mai_trend_1982-2024_v1.0.tif"),
     "key: trend COG")
  ok(identical(key_of("chirps-chirts-agera5-rh15", "anomalies", "v1.0", "admin_series_admin2_1982-2024_v1.0.parquet"),
               "domain=climate/type=hazard-indices/source=chirps-chirts-agera5-rh15/region=africa/processing=anomalies/version=v1.0/admin_series_admin2_1982-2024_v1.0.parquet"),
     "key: admin series table, rh15 token")
  # 2. synthetic S7 run: two tokens, 3x3 cells, layers 1981..2024 (1981 = spin-up), produced by the REAL S7 script
  td <- file.path(tempdir(), "s7pub_selftest"); unlink(td, recursive = TRUE)
  base <- rast(nrows = 3, ncols = 3, xmin = 30, xmax = 30.75, ymin = 0, ymax = 0.75, crs = "EPSG:4326")
  yrs <- 1981:2024; set.seed(3)
  for (tok in c("tA", "tB")) {
    src <- file.path(td, "Data", "obs_seasonal", paste0(tok, "_1982-2025"), "ggcmi-mai"); dir.create(src, recursive = TRUE)
    s <- rast(lapply(yrs, function(y) { v <- 10 + rnorm(9, 0, 0.5); v[1] <- 5 + 0.2 * (y - 1981); v[9] <- NA; v[5] <- 7; setValues(base, v) }))
    names(s) <- yrs; writeRaster(s, file.path(src, "NDWS_mean.tif"), overwrite = TRUE)
    rc <- system2(file.path(R.home("bin"), "Rscript"), c(shQuote(s7), "--obs-root", shQuote(td), "--token", tok, "--timeframe", "1982-2025",
                                                       "--years", "1982:2024", "--cores", "1"), stdout = FALSE, stderr = FALSE)
    if (rc != 0) stop("selftest: S7 run failed for ", tok)
    # admin tables consistent with S7's own definitions (two units, area + crop_production)
    d <- file.path(td, "Data", "obs_trends", paste0(tok, "_1982-2024"))
    for (lv in c("admin0", "admin1", "admin2")) {
      g <- CJ(iso3 = c("AAA", "BBB"), weighting = c("area", "crop_production"))
      ser <- g[, .(year = 1982:2024, value = 10 + 0.05 * (1982:2024 - 1982) * (iso3 == "AAA") + sin(1982:2024)), by = .(iso3, weighting)]
      ser[, `:=`(level = lv, admin0_name = iso3, window = "ggcmi-mai", index = "NDWS_mean")]
      nrm <- ser[year %in% 1991:2020, .(normal_mean = mean(value), normal_sd = sd(value)), by = .(iso3, weighting)]
      ser <- merge(ser, nrm, by = c("iso3", "weighting"))
      ser[, `:=`(anomaly_diff = value - normal_mean)][, anomaly_z := anomaly_diff / normal_sd]
      tr <- ser[, .(slope_per_decade = 0.5, p_mk = if (iso3[1] == "AAA") 1e-6 else 0.4, n_years = 43L), by = .(level, iso3, admin0_name, window, index, weighting)]
      tr <- merge(tr, nrm, by = c("iso3", "weighting")); tr[, sig_fdr := bh(p_mk), by = .(level, window, index, weighting)]
      ser[, c("normal_mean", "normal_sd") := NULL]
      arrow::write_parquet(tr, file.path(d, sprintf("admin_trends_%s.parquet", lv)))
      arrow::write_parquet(ser, file.path(d, sprintf("admin_series_%s.parquet", lv)))
    }
  }
  exp <- list(tA = 1L, tB = 1L); stg <- file.path(td, "staging"); rec <- file.path(td, "rec"); dir.create(rec)
  write_inventories(inventory(file.path(td, "Data", "obs_trends"), c("tA", "tB"), 1982:2024, 1991:2020, "v1.0")[kind == "raster"], rec)
  go <- function(...) run(td, c("tA", "tB"), exp, 1982:2024, 1991:2020, "1982-2025", "v1.0", stg, "dry-run", FALSE, pr, ..., record_dir = rec)
  res <- NULL; capture.output(res <- go(s3_head = function(k) NULL))
  ok(all(res$gates), "clean S7 output passes P1-P6 (P6 with no key on S3)")
  pm <- values(rast(res$inv[product == "trend" & token == "tA", local]))[1, "p_mk"]
  ok(abs(pm / mk_floor(43) - 1) < 1e-5, "a perfectly monotonic 43-season cell sits on the MK floor (3.7e-21) and passes P3")
  ok(nrow(res$man) == 2 * (4 + 6) && file.exists(file.path(stg, "manifest.csv")), "manifest: 4 COGs + 6 tables per token")
  sc <- jsonlite::read_json(file.path(stg, paste0(res$man[product == "admin-trends" & level == "admin1" & source == "tA", key], ".json")))
  ok(grepl("crop_production", sc$field_descriptions$weighting) && grepl("livestock_tlu", sc$field_descriptions$weighting) &&
       grepl("inflate-only", sc$field_descriptions$p_mk) && grepl("Benjamini-Hochberg", sc$field_descriptions$sig_fdr) &&
       all(c("season_label", "normal", "chirps", "chirts_coast", "soil_fill", "islands") %in% names(sc$caveats)),
     "table sidecar carries the weighting guidance, p_mk / sig_fdr text and all six caveats")
  scr <- jsonlite::read_json(file.path(stg, paste0(res$man[product == "trend" & source == "tA", key], ".json")))
  ok(identical(scr$checksums$md5, res$man[product == "trend" & source == "tA", md5]) && length(scr$bands) == 4, "COG sidecar: bands + md5")
  # 3. P6: an existing key stops the run; --resume skips it only when identical
  k1 <- res$man$key[1]; f1 <- res$inv[key == k1, local]
  same <- function(k) if (k == k1) list(size = file.size(f1), etag = md5_of(f1)) else NULL
  diff <- function(k) if (k == k1) list(size = 1, etag = "x") else NULL
  ok(fails(go(s3_head = function(k) NULL, probe_key = "live/key"), "P6"), "P6: S3 that cannot see a known live key stops the run (no false \"absent\")")
  ok(fails(go(s3_head = same), "P6"), "P6: an existing key stops a non-resume run, even if identical")
  ok(!fails(run(td, c("tA", "tB"), exp, 1982:2024, 1991:2020, "1982-2025", "v1.0", stg, "dry-run", TRUE, pr, s3_head = same, record_dir = rec), "."),
     "P6 --resume: an identical existing key is skipped")
  ok(fails(run(td, c("tA", "tB"), exp, 1982:2024, 1991:2020, "1982-2025", "v1.0", stg, "dry-run", TRUE, pr, s3_head = diff, record_dir = rec), "P6"),
     "P6 --resume: an existing key that differs stops the run")
  # 4. each gate catches its defect (restore after each)
  dA <- file.path(td, "Data", "obs_trends", "tA_1982-2024")
  fd <- file.path(dA, "ggcmi-mai", "NDWS_mean_anomaly_diff.tif"); bk <- tempfile(fileext = ".tif"); file.copy(fd, bk)
  x <- rast(fd) * 1; v <- values(x); v[1, 5] <- v[1, 5] + 0.01; x <- setValues(x, v)
  writeRaster(x, fd, overwrite = TRUE, filetype = "COG", gdal = c("COMPRESS=ZSTD", "OVERVIEWS=AUTO"))
  ok(fails(go(s3_head = function(k) NULL), "P4"), "P4: one anomaly_diff cell off by 0.01 stops the run")
  writeRaster(rast(bk) * 1, fd, overwrite = TRUE, filetype = "GTiff")
  ok(fails(go(s3_head = function(k) NULL), "P2"), "P2: a plain GeoTIFF (not COG) stops the run")
  file.copy(bk, fd, overwrite = TRUE)
  fz <- file.path(dA, "ggcmi-mai", "NDWS_mean_anomaly_z.tif"); file.rename(fz, paste0(fz, ".bak"))
  ok(fails(go(s3_head = function(k) NULL), "P1"), "P1: a missing anomaly_z COG stops the run")
  file.rename(paste0(fz, ".bak"), fz)
  ft <- file.path(dA, "admin_trends_admin1.parquet"); tb <- read_pq(ft); tb0 <- copy(tb)
  tb[iso3 == "BBB" & weighting == "area", p_mk := 0]; arrow::write_parquet(tb, ft)
  ok(fails(go(s3_head = function(k) NULL), "P5"), "P5: a p_mk of exactly 0 stops the run")
  tb <- copy(tb0); tb[iso3 == "BBB" & weighting == "area", sig_fdr := 1]; arrow::write_parquet(tb, ft)
  ok(fails(go(s3_head = function(k) NULL), "P5"), "P5: a sig_fdr that is not BH(p_mk) stops the run")
  arrow::write_parquet(tb0, ft)
  fs <- file.path(dA, "admin_series_admin0.parquet"); sb <- read_pq(fs); sb0 <- copy(sb)
  sb[1, anomaly_diff := anomaly_diff + 1e-3]; arrow::write_parquet(sb, fs)
  ok(fails(go(s3_head = function(k) NULL), "P5"), "P5: a series anomaly_diff that is not value - normal_mean stops the run")
  arrow::write_parquet(sb0, fs)
  ok(fails(run(td, c("tA", "tB"), list(tA = 2L, tB = 1L), 1982:2024, 1991:2020, "1982-2025", "v1.0", stg, "dry-run", FALSE, pr,
               s3_head = function(k) NULL, record_dir = rec), "P1"), "P1: a stack count that differs from --expect-stacks stops the run")
  fi <- inv_path(rec, "normals"); ib <- fread(fi); fwrite(ib[-1], fi)
  ok(fails(go(s3_head = function(k) NULL), "P1"), "P1b: a COG on disk that the CDH inventory does not list stops the run")
  fwrite(ib, fi)
  iv <- fread(inv_path(rec, "anomalies"))
  ok(nrow(iv) == 4 && setequal(iv$variable, c("anomaly_diff", "anomaly_z")) && all(c("href", "source", "window", "index") %in% names(iv)),
     "inventory columns: href + one per dimension (+ variable for the anomaly entry)")
  capture.output(res2 <- go(s3_head = function(k) NULL))
  ok(all(res2$gates), "restored output passes again")
  unlink(td, recursive = TRUE)
  cat("ALL S7 PUBLISH SELFTEST ASSERTIONS PASSED\n")
}

if (sys.nframe() == 0L) {
  args <- commandArgs(TRUE)
  flag <- function(x) x %in% args
  getopt <- function(x, d = NULL) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
  rng <- function(s) { p <- as.integer(strsplit(s, ":")[[1]]); p[1]:p[2] }
  if (flag("--selftest")) { selftest(); quit(status = 0) }
  obs_root <- getopt("--obs-root"); if (is.null(obs_root)) stopf("--obs-root <abs> is required")
  obs_root <- normalizePath(obs_root, mustWork = TRUE)
  tokens <- strsplit(getopt("--tokens", "chirps-chirts-agera5,chirps-chirts-agera5-rh15"), ",")[[1]]
  ex <- strsplit(strsplit(getopt("--expect-stacks", "chirps-chirts-agera5=95,chirps-chirts-agera5-rh15=19"), ",")[[1]], "=")
  expect <- setNames(lapply(ex, function(z) as.integer(z[2])), vapply(ex, `[`, "", 1))
  years <- rng(getopt("--years", "1982:2024")); normal <- rng(getopt("--normal", "1991:2020"))
  version <- getopt("--version", "v1.0")
  staging <- getopt("--staging", file.path(obs_root, "Data", "obs_trends", paste0("_publish_", version)))
  mode <- if (flag("--publish")) "publish" else if (flag("--verify")) "verify" else "dry-run"
  pr <- project_root()
  # S3 credentials: the node's setup (absolute path: it setwd()s). Every path in this script is absolute.
  suppressMessages(suppressWarnings(source(file.path(pr, "R", "0_server_setup.R"))))
  run(obs_root, tokens, expect, years, normal, getopt("--timeframe", "1982-2025"), version, staging, mode, flag("--resume"), pr,
      s3_head = s3_head_paws(), probe_key = PROBE_KEY)
}
