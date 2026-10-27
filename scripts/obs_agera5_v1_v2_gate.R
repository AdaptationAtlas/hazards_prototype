#!/usr/bin/env Rscript
# scripts/obs_agera5_v1_v2_gate.R
# ===============================
# PLAN_obs_hazard_pipeline.md §4 gate (DISPATCH_cglabs_obs_s1_ingest.md, I4): is AgERA5 v1.0 solar radiation the
# same series as the AgERA5 v2.0 SRF that S1 ingests? Read-only; writes two CSVs under logs/.
#
#   v1.0  {common_data}/ecmwf_agera5/solar_radiation_flux/Solar-Radiation-Flux_C3S-glob-agric_AgERA5_<yyyymmdd>_final-v1.0.nc
#         netCDF4 (read with ncdf4: the node's GDAL has no HDF5 plugin), J m-2 d-1, 3600 x 1801, centres on
#         whole 0.1 deg (lon -180..179.9, lat 90..-90)
#   v2.0  /vsicurl/ FAO GISMGR mirror COG C3S.AGERA5-SRF.<yyyy-mm-dd>.tif, J m-2 d-1, 3600 x 1800, edges at
#         -180 / 90, i.e. centres on x.x5 deg
# The two grids differ by half a cell, so there is no coordinate-exact pairing. Day 1 is compared under the four
# index pairings (v1 row i or i+1, col j or j+1 for COG row i / col j); the pairing with the smallest mean |diff|
# is used for every day, and the other three are reported. A near-zero best pairing also tells us which way the
# mirror relabelled the grid (relevant to S1's 0.1 -> 0.25 area-average geolocation).
#
# Days: the 15th of every month 2006-2022 (204 days). Window: Africa, -18..52 E, 35 S..38 N (700 x 730 cells).
# Per year: mean bias v2 - v1 (MJ m-2 d-1, mean of the 12 daily domain means), p99 and max of |diff| over the 12
# days pooled; trend = OLS slope of the yearly mean bias on year.
# Invariants (dispatch): |mean bias| < 0.1 MJ every year, p99 |diff| < 0.5 MJ every year, |trend| <= 0.02 MJ/yr.
#
# Run (from the repo root, in the background):
#   COMMON_DATA=<common_data> GATE_WORKERS=8 nohup Rscript scripts/obs_agera5_v1_v2_gate.R > logs/obs_s1_gate_<STAMP>.log 2>&1 &

suppressPackageStartupMessages({ library(terra); library(ncdf4); library(parallel) })

cd      <- Sys.getenv("COMMON_DATA")
if (!nzchar(cd)) stop("set COMMON_DATA to the node's common_data root")
workers <- as.integer(Sys.getenv("GATE_WORKERS", "8"))
v1_dir  <- file.path(cd, "ecmwf_agera5", "solar_radiation_flux")
v2_url  <- "/vsicurl/https://storage.googleapis.com/fao-gismgr-c3s-data/DATA/C3S/MAPSET/AGERA5-SRF/C3S.AGERA5-SRF.%s.tif"
stamp   <- format(Sys.time(), "%Y%m%d_%H%M%S")
out_day <- file.path("logs", sprintf("obs_s1_gate_days_%s.csv", stamp))
out_yr  <- file.path("logs", sprintf("obs_s1_gate_years_%s.csv", stamp))

AFR <- c(xmin = -18, xmax = 52, ymin = -35, ymax = 38)
j0  <- round((AFR[["xmin"]] + 180) / 0.1)       # COG col offset (0-based) of the window
i0  <- round((90 - AFR[["ymax"]]) / 0.1)        # COG row offset (0-based)
nc_ <- round((AFR[["xmax"]] - AFR[["xmin"]]) / 0.1)
nr_ <- round((AFR[["ymax"]] - AFR[["ymin"]]) / 0.1)
NODATA <- -9999

terra::setGDALconfig("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
terra::setGDALconfig("GDAL_HTTP_MAX_RETRY", "5")
terra::setGDALconfig("GDAL_HTTP_RETRY_DELAY", "2")

msg <- function(...) cat(sprintf("[%s] [gate] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), paste0(...)))

days <- seq(as.Date("2006-01-15"), as.Date("2022-12-15"), by = "month")
stopifnot(length(days) == 204L)

v1_file <- function(d) file.path(v1_dir, sprintf("Solar-Radiation-Flux_C3S-glob-agric_AgERA5_%s_final-v1.0.nc", format(d, "%Y%m%d")))
miss <- days[!file.exists(vapply(days, v1_file, ""))]
if (length(miss)) stop("v1.0 files missing for: ", paste(miss, collapse = ", "))

# v2.0 window as a [row, col] matrix in J m-2 d-1, NA for nodata
read_v2 <- function(d) {
  r <- terra::rast(sprintf(v2_url, format(d, "%Y-%m-%d")))
  stopifnot(terra::ncol(r) == 3600, terra::nrow(r) == 1800, all.equal(terra::res(r), c(0.1, 0.1)),
            abs(terra::xmin(r) + 180) < 1e-9, abs(terra::ymax(r) - 90) < 1e-9)
  w <- terra::crop(r, terra::ext(AFR[["xmin"]], AFR[["xmax"]], AFR[["ymin"]], AFR[["ymax"]]))
  m <- terra::as.matrix(w, wide = TRUE)
  stopifnot(nrow(m) == nr_, ncol(m) == nc_)
  m[m == NODATA] <- NA
  m
}

# v1.0 window shifted by (di, dj) rows/cols relative to the COG index, as a [row, col] matrix
read_v1 <- function(d, di, dj) {
  nc <- ncdf4::nc_open(v1_file(d)); on.exit(ncdf4::nc_close(nc))
  lat <- ncdf4::ncvar_get(nc, "lat"); lon <- ncdf4::ncvar_get(nc, "lon")
  stopifnot(length(lon) == 3600, length(lat) == 1801, abs(lat[1] - 90) < 1e-6, abs(lon[1] + 180) < 1e-6)
  a <- ncdf4::ncvar_get(nc, "Solar_Radiation_Flux", start = c(j0 + dj + 1, i0 + di + 1, 1),
                        count = c(nc_, nr_, 1), collapse_degen = TRUE)
  m <- t(a)                                        # [lon, lat] -> [lat (north first), lon]
  m[!is.finite(m) | m == NODATA] <- NA
  m
}

day_stats <- function(d, di, dj) {
  v2 <- read_v2(d) / 1e6; v1 <- read_v1(d, di, dj) / 1e6
  diff <- v2 - v1; ok <- is.finite(diff)
  list(row = data.frame(date = d, di = di, dj = dj, n_valid = sum(ok),
                        n_mask_mismatch = sum(xor(is.finite(v1), is.finite(v2))),
                        v1_mean = mean(v1[ok]), v2_mean = mean(v2[ok]), mean_bias = mean(diff[ok]),
                        mean_abs = mean(abs(diff[ok])), p99_abs = unname(quantile(abs(diff[ok]), 0.99)),
                        max_abs = max(abs(diff[ok]))),
       absdiff = abs(diff[ok]))
}

msg("v1.0 dir ", sub(cd, "{common_data}", v1_dir, fixed = TRUE), "; ", length(days), " days; window ", nc_, "x", nr_,
    "; workers ", workers, "; terra ", as.character(packageVersion("terra")), " GDAL ", terra::gdal())

# ---- 1. pairing on day 1 --------------------------------------------------------------------------------
pairs <- expand.grid(di = 0:1, dj = 0:1)
p1 <- do.call(rbind, lapply(seq_len(nrow(pairs)), function(k) day_stats(days[1], pairs$di[k], pairs$dj[k])$row))
msg("pairing test on ", format(days[1]), " (v1 row = COG row + di, v1 col = COG col + dj):")
print(p1[, c("di", "dj", "n_valid", "mean_bias", "mean_abs", "p99_abs", "max_abs")], row.names = FALSE, digits = 4)
best <- p1[which.min(p1$mean_abs), c("di", "dj")]
msg("using pairing di=", best$di, " dj=", best$dj)

# ---- 2. all days, per year -------------------------------------------------------------------------------
yrs <- sort(unique(as.integer(format(days, "%Y"))))
day_rows <- list(); yr_rows <- list()
for (y in yrs) {
  dd <- days[as.integer(format(days, "%Y")) == y]
  res <- mclapply(dd, function(d) {
    for (k in 1:3) {                               # transient /vsicurl errors
      r <- try(day_stats(d, best$di, best$dj), silent = TRUE)
      if (!inherits(r, "try-error")) return(r)
      Sys.sleep(2^k)
    }
    stop("day ", d, ": ", r)
  }, mc.cores = workers, mc.preschedule = FALSE)
  bad <- vapply(res, inherits, TRUE, "try-error")
  if (any(bad)) stop("year ", y, ": ", paste(unlist(res[bad]), collapse = " | "))
  dr <- do.call(rbind, lapply(res, `[[`, "row")); day_rows[[length(day_rows) + 1]] <- dr
  pooled <- unlist(lapply(res, `[[`, "absdiff"))
  yr_rows[[length(yr_rows) + 1]] <- data.frame(year = y, n_days = nrow(dr), mean_bias = mean(dr$mean_bias),
                                               p99_abs = unname(quantile(pooled, 0.99)), max_abs = max(pooled),
                                               mean_abs = mean(pooled), v1_mean = mean(dr$v1_mean), v2_mean = mean(dr$v2_mean))
  msg(sprintf("%d: %2d days  mean_bias %+.4f  p99|d| %.4f  max|d| %.3f  (v1 %.2f, v2 %.2f MJ)", y, nrow(dr),
              yr_rows[[length(yr_rows)]]$mean_bias, yr_rows[[length(yr_rows)]]$p99_abs, yr_rows[[length(yr_rows)]]$max_abs,
              yr_rows[[length(yr_rows)]]$v1_mean, yr_rows[[length(yr_rows)]]$v2_mean))
}
D <- do.call(rbind, day_rows); Y <- do.call(rbind, yr_rows)
write.csv(D, out_day, row.names = FALSE); write.csv(Y, out_yr, row.names = FALSE)

slope <- unname(coef(lm(mean_bias ~ year, data = Y))[["year"]])
msg(sprintf("trend of yearly mean bias: %+.5f MJ/yr", slope))
msg("max day mask mismatch: ", max(D$n_mask_mismatch), " cells; min n_valid: ", min(D$n_valid))

inv <- c(`|mean bias| < 0.1 MJ every year` = all(abs(Y$mean_bias) < 0.1),
         `p99 |diff| < 0.5 MJ every year`  = all(Y$p99_abs < 0.5),
         `|trend| <= 0.02 MJ/yr`           = abs(slope) <= 0.02)
for (k in names(inv)) msg(if (inv[[k]]) "PASS  " else "FAIL  ", k)
msg("csv: ", out_day, " , ", out_yr)
msg(if (all(inv)) "AGERA5 V1.0 vs V2.0 GATE PASSED" else "AGERA5 V1.0 vs V2.0 GATE FAILED")
quit(status = if (all(inv)) 0L else 1L)
