# Please run 0_server_setup.R before executing this script

# Note - this entire script can be generalized.
# We would need to pull in the stat from haz_meta to automate the "_mean-G" text creation (e.g. 2.2)
# Would also need to automate the G/L coding
# Livestock and crops could combined, but the highland/tropical split for livestock would need to be incorporated
# The entire workflow here could be generalized to generate area merge with total area. The resulting table could then
# Be wrangled to give % and % change from baseline.

# First run server_setup script
# 0) Load R functions & packages ####
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

load_and_install_packages <- function(packages) {
  for (package in packages) {
    if (!require(package, character.only = TRUE)) {
      install.packages(package)
      library(package, character.only = TRUE)
    }
  }
}

# List of packages to be loaded
packages <- c(
  "arrow",
  "geoarrow",
  "sf",
  "terra",
  "data.table",
  "doFuture",
  "future.apply",
  "exactextractr",
  "parallel",
  "pbapply",
  "DBI",
  "duckdb"
)

# Call the function to install and load packages
load_and_install_packages(packages)

# CR-093: write_parquet_pushdown() (iso3-first sort + row-group stats) lives in
# _helpers.R. 0_server_setup.R sets project_dir and must be run first.
source(file.path(project_dir, "R", "_helpers.R"))

# ----------------------------------------------------------------------------
# CR-093: run controls + timestamped logging (best practice from the R/2.1 saga)
# ----------------------------------------------------------------------------
# Timestamped logger with elapsed-since-start, so every line shows duration.
.t0_22 <- Sys.time()
.log22 <- function(...) {
  el <- as.numeric(difftime(Sys.time(), .t0_22, units = "secs"))
  cat(sprintf("[%s | +%6.1fs] [2.2] %s\n",
              format(Sys.time(), "%Y-%m-%d %H:%M:%S"), el, paste0(...)))
  flush.console()
}
# Silence pbapply spinners under nohup (handlers("void") does NOT cover pbapply).
# 0_server_setup.R sets this when run interactively; set it here too so a
# non-interactive run of 2.2 alone stays quiet.
if (!interactive()) pbapply::pboptions(type = "none")

# Section run controls: each heavy section runs by default; export
# SKIP_R22_SEC{1,2,3,4}=1 to skip it (e.g. stage-test SEC1 alone, validate,
# then run the rest). Setup above (functions, paths, Geographies) is OUTSIDE
# every guard and always executes.
run_sec1 <- !nzchar(Sys.getenv("SKIP_R22_SEC1")) # 1) PTOT % area change + diff
run_sec2 <- !nzchar(Sys.getenv("SKIP_R22_SEC2")) # 2) THI livestock heat % area
run_sec3 <- !nzchar(Sys.getenv("SKIP_R22_SEC3")) # 3) NTx crop heat % area
run_sec4 <- !nzchar(Sys.getenv("SKIP_R22_SEC4")) # 4) NDWS/NDWL0 drought/wet freq
.log22(sprintf("run controls: SEC1=%s SEC2=%s SEC3=%s SEC4=%s",
               run_sec1, run_sec2, run_sec3, run_sec4))

# Helper: write an R/2.2 parquet with iso3-first pruning + a row-count log line.
write_chg_parquet <- function(tbl, path) {
  # CR-093: gaul*/zone_id are extraction join keys, not published columns —
  # strip them so every R/2.2 output keeps the canonical iso3 + admin*_name
  # schema the notebook expects (by-model tables carry gaul*; ensembles already
  # drop them via the name-keyed group-by).
  drop_cols <- intersect(c("zone_id", "gaul0_code", "gaul1_code", "gaul2_code"), names(tbl))
  if (length(drop_cols)) tbl <- tbl[, !drop_cols, with = FALSE]
  write_parquet_pushdown(tbl, path, sort_by = chg_sort_by, verify_stats_on = "iso3")
  .log22(sprintf("wrote %s (%d rows, %d iso3)",
                 basename(path), nrow(tbl),
                 length(unique(tbl[["iso3"]]))))
}

merge_admin_extract <- function(data_ex) {
  # CR-093: pass-through retained for call-site compatibility. The new
  # admin_extract(boundaries_zonal, boundaries_index) API (R/haz_functions.R)
  # ALREADY returns the final long data.table — geographic identifiers merged in
  # (incl. gaul0/1/2_code) and melted to (variable, value), stacked across admin
  # levels (admin0 rows carry NA for deeper admin*_name). zone_id is dropped
  # inside admin_extract. The gaul* codes are kept on purpose: they are the only
  # collision-free key for the per-admin area joins below (admin names are NOT
  # unique in GAUL2024). write_chg_parquet() strips gaul* before write so the
  # published schema stays iso3 + admin*_name + payload.
  data_ex[]
}

# CR-093: parse a risk-dir layer name into scenario/model/timeframe/severity.
# Risk filenames are scenario_model_timeframe_<HAZ-stat-Gthr>_severity, where
# only the bookend underscores separate fields — the hazard token and the years
# are dash-delimited (e.g. ssp126_ACCESS-CM2_2021-2040_NTx35-mean-G14_severe,
# historic_ACCESS-CM2_1995-2014_THI-max-max-G82_extreme). So a plain `_`-split
# gives exactly: [1]=scenario (historic|sspNNN — already a single token, no
# `historical`->3-token expansion needed), [2]=model, [3]=timeframe, last=
# severity. Taking ncol() for severity is robust to any extra middle fields.
.parse_risk_vars <- function(v) {
  m <- do.call("cbind", tstrsplit(v, "_"))
  m <- m[, c(1, 2, 3, ncol(m)), drop = FALSE]
  colnames(m) <- c("scenario", "model", "timeframe", "severity")
  m
}

# 0.1) Set up workspace #####
# CR-093: shared iso3-first sort order for all R/2.2 parquet outputs so the
# notebook's per-country reads prune by row-group stats (write_parquet_pushdown
# intersects with the columns actually present per output; iso3 is the guard).
chg_sort_by <- c("iso3", "admin0_name", "admin1_name", "admin2_name", "scenario", "timeframe")
haz_class <- fread(haz_class_url)
haz_class[, direction2 := "G"][direction == "<", direction2 := "L"]
haz_meta <- fread(haz_meta_url)
# Make cell size raster
base_cellsize <- terra::cellSize(base_rast, unit = "km")

# 0.2) Load admin boundaries #####
# CR-093: migrate to the admin_extract(boundaries_zonal, boundaries_index) API
# (R/haz_functions.R), mirroring R/2.1:111-132. Build a zone_id per polygon,
# rasterize each admin level to a cached <name>_zonal.tif in boundaries_int_dir
# (shared with R/2.1), and keep a per-level index keyed by zone_id for the
# post-extract merge. admin_extract then does zonal() + merge + melt internally.
Geographies <- lapply(seq_along(geo_files_local), FUN = function(i) {
  file <- geo_files_local[i]
  data <- arrow::open_dataset(file) |>
    sf::st_as_sf() |>
    terra::vect()
  data$zone_id <- ifelse(!is.na(data$gaul2_code), data$gaul2_code,
    ifelse(!is.na(data$gaul1_code), data$gaul1_code, data$gaul0_code)
  )
  data
})
names(Geographies) <- names(geo_files_local)

boundaries_zonal <- lapply(seq_along(Geographies), FUN = function(i) {
  file_path <- file.path(boundaries_int_dir, paste0(names(Geographies)[i], "_zonal.tif"))
  if (!file.exists(file_path)) {
    zone_rast <- terra::rasterize(
      x = Geographies[[i]],
      y = base_rast,
      field = "zone_id",
      background = NA, # cells not covered by any polygon become NA
      touches = TRUE
    )
    terra::writeRaster(zone_rast, file_path, overwrite = TRUE)
  }
  file_path
})
names(boundaries_zonal) <- names(Geographies)

boundaries_index <- lapply(seq_along(Geographies), FUN = function(i) {
  d <- data.frame(Geographies[[i]])[, c("iso3", "admin0_name", "admin1_name", "admin2_name", "zone_id", "gaul0_code", "gaul1_code", "gaul2_code")]
  # CR-093: zone_id must be 1:1 with attributes. admin_extract() merges the
  # zonal values onto this index BY zone_id, so duplicate zone_id rows (GAUL2024
  # ships a few multipart/duplicate polygons at each admin level — 4 per level
  # here) would DOUBLE the extracted rows and turn the area joins below into a
  # cartesian blow-up. R/2.1 sidesteps this by extracting admin0+admin1 only;
  # R/2.2 also does admin2, so dedup the index to one attribute row per zone.
  d[!duplicated(d$zone_id), ]
})
names(boundaries_index) <- names(Geographies)

# CR-093: input dirs. These were previously LEAKED GLOBALS from
# R/2_calculate_haz_freq.R (defined only inside its per-timeframe loop,
# R/2:656-664), so R/2.2 only worked when run after R/2 in the same session.
# Define them here, self-contained, at the `annual` axis (the parent dirs also
# carry a `jagermeyr` axis; annual is the notebook-consumed PTOT/THI/NTx/NDWS
# product). Override the axis with R22_TIMEFRAME=jagermeyr if ever needed.
r22_timeframe <- Sys.getenv("R22_TIMEFRAME", unset = "annual")
haz_mean_dir       <- file.path(atlas_dirs$data_dir$hazard_timeseries_mean, r22_timeframe)
haz_time_risk_dir  <- file.path(atlas_dirs$data_dir$hazard_timeseries_risk, r22_timeframe)
haz_timeseries_dir <- file.path(indices_dir2, r22_timeframe)
.log22(sprintf("input dirs @ axis '%s': mean=%s | risk=%s | indices=%s",
               r22_timeframe, haz_mean_dir, haz_time_risk_dir, haz_timeseries_dir))
stopifnot(dir.exists(haz_mean_dir), dir.exists(haz_time_risk_dir))

# 1) % area of precipitation increase or decrease by admin vect ####
if (run_sec1) {
.log22("SEC1 (PTOT % area change + diff) — START")
.t_sec1 <- Sys.time()
# Create save folder
haz_mean_ptot_dir <- file.path(haz_mean_dir, "ptot_perc")
if (!dir.exists(haz_mean_ptot_dir)) {
  dir.create(haz_mean_ptot_dir)
}

# Load annual precip rasters

files <- list.files(haz_mean_dir, ".tif", full.names = TRUE)
files <- grep("PTOT", files, value = TRUE)
files <- files[!grepl("change", files)]
# CR-093: drop producer ENSEMBLE rasters before pairing (parity with SEC3/SEC4).
# .extract_gcm maps ENSEMBLEmean files to GCM="ENSEMBLEmean"; if a historic
# ENSEMBLEmean ever existed it would pair with the future one and leak a
# model="ENSEMBLEmean" row into the by-model output (the ensemble is recomputed
# separately as change_ens). Defensive — current Data/ has no historic ENSEMBLE.
files <- files[!grepl("ENSEMBLE", files)]
# CR-093 FIX: PTOT % area change is mean-only — the mean dir also holds the
# matching `PTOT-sum_sd` rasters (one per mean), which are meaningless here and
# would otherwise double every row (and inject NaN/out-of-range % from sd
# rasters). Anchor on the `_mean.tif` stat SUFFIX, not a bare `_mean` substring:
# the parent dir is `hazard_timeseries_mean`, so an unanchored grep on the full
# path matches every file and drops nothing.
files <- grep("_mean[.]tif$", files, value = TRUE)
# CR-093 FIX: keep only the canonical hyphen-year form (`YYYY-YYYY`). The mean
# dir also holds stale underscore-year (`YYYY_YYYY`) duplicates of the
# historic files; including both double-counts. Per [[feedback-r2-filename-parsing-pitfalls]]
# the producer should not be re-run just to fix names — filter on read.
files <- grep("[0-9]{4}-[0-9]{4}", files, value = TRUE)
files_hist <- grep("historic", files, value = TRUE)
files_fut <- files[!files %in% files_hist]

# CR-093 FIX: pair historic->future by GCM (the shared token), not by the whole
# historic basename. The future names share only the GCM (years + scenario
# differ). Filename grammar is documented in [[feedback-r2-filename-parsing-pitfalls]]:
# scenario_model_timeframe_<haz>[_stat], `_`-split, GCMs dashed, years YYYY-YYYY.
# TWO historic prefix conventions coexist in this dir for the SAME 18 GCMs:
# the canonical `historic_historic_historic_<GCM>_...` (post-rename, R/2:1117)
# AND stale `historical_<GCM>_...` leftovers. .extract_gcm strips BOTH (plus the
# ssp future prefix) so every historic file resolves to its bare GCM token; we
# then dedup historic files by GCM (keeping the first, i.e. the canonical
# `historic_historic_historic_` form sorts ahead of `historical_`) so a GCM is
# never paired — and counted — twice.
.extract_gcm <- function(x) {
  b <- sub("^(historic_historic_historic|historical|ssp[0-9]+)_", "", basename(x))  # drop scenario prefix
  sub("_[0-9]{4}-[0-9]{4}_.*$", "", b)                                    # drop _years_<rest>
}
hist_gcm <- .extract_gcm(files_hist)
.n_hist_raw <- length(files_hist)
.dup <- duplicated(hist_gcm)
files_hist <- files_hist[!.dup]
hist_gcm   <- hist_gcm[!.dup]
fut_gcm    <- .extract_gcm(files_fut)

.log22(sprintf("SEC1: %d historic PTOT file(s) -> %d after GCM dedup, %d future file(s); %d distinct GCMs",
               .n_hist_raw, length(files_hist), length(files_fut), length(unique(hist_gcm))))
stopifnot(length(files_hist) > 0L, length(files_fut) > 0L)

ptot_pairs <- lapply(seq_along(files_hist), function(i) {
  file_hist <- files_hist[i]
  files_fut_ss <- files_fut[fut_gcm == hist_gcm[i]]
  if (length(files_fut_ss) == 0L) {
    .log22(sprintf("SEC1: WARN no future match for historic GCM '%s' (%s) — skipped",
                   hist_gcm[i], basename(file_hist)))
    return(NULL)
  }
  future <- terra::rast(files_fut_ss)
  past <- terra::rast(file_hist)
  d <- future - past
  ch <- round(100 * d / past, 1)
  names(ch) <- gsub(".tif", "", basename(files_fut_ss))
  names(d) <- gsub(".tif", "", basename(files_fut_ss))
  list(change = ch, diff = d)
})
ptot_pairs <- Filter(Negate(is.null), ptot_pairs)
if (length(ptot_pairs) == 0L) {
  stop("SEC1: no historic PTOT file matched any future file — check naming/token ",
       "logic before publishing (pre-existing matching logic, see CR-093 note).")
}
change <- terra::rast(lapply(ptot_pairs, `[[`, "change"))
diff <- terra::rast(lapply(ptot_pairs, `[[`, "diff"))
.log22(sprintf("SEC1: built change/diff stacks — %d layers from %d/%d historic file(s)",
               terra::nlyr(change), length(ptot_pairs), length(files_hist)))


# Increasing area
change_inc <- terra::classify(change, rcl = data.frame(from = c(-999999999, 5), to = c(5, 99999999999), becomes = c(0, 1)))
change_inc <- change_inc * base_cellsize
# Decreasing area
change_dec <- terra::classify(change, rcl = data.frame(from = c(-999999999, -5), to = c(-5, 99999999999), becomes = c(1, 0)))
change_dec <- change_dec * base_cellsize

# Sum areas by admin vectors
base_areas <- admin_extract(base_cellsize,
  boundaries_zonal,
  boundaries_index,
  FUN = "sum"
)

change_inc <- admin_extract(change_inc,
  boundaries_zonal,
  boundaries_index,
  FUN = "sum"
)

change_dec <- admin_extract(change_dec,
  boundaries_zonal,
  boundaries_index,
  FUN = "sum"
)

diff <- admin_extract(diff,
  boundaries_zonal,
  boundaries_index,
  FUN = "mean"
)

# Tabulate data
change_inc <- merge_admin_extract(change_inc)[, direction := "increase_5"]
change_dec <- merge_admin_extract(change_dec)[, direction := "decrease_5"]

base_areas <- merge_admin_extract(base_areas)[, direction := "total"]
setnames(base_areas, "value", "total")

diff <- merge_admin_extract(diff)

# Work out percentage change
change <- rbind(change_inc, change_dec)
# CR-093: join the per-admin total area by gaul0/1/2_code, NOT by admin name —
# admin names are not unique in GAUL2024 (the gaul codes are, after the zone_id
# dedup of boundaries_index), so a name join would be many-to-many.
change <- merge(change, base_areas[, list(gaul0_code, gaul1_code, gaul2_code, total)],
  by = c("gaul0_code", "gaul1_code", "gaul2_code"), all.x = TRUE)
change[, value := round(100 * value / total, 1)][, total := NULL]
# CR-093: NA-clean non-finite % (NaN from 0/0 in zones with no covered base
# cells; Inf where the upstream change raster blew up on near-zero historic
# precip — see ISSUE_cr093_nan_zeroprecip.md, for the R/2 rebake to fix at
# source). NA is a clean prunable NULL; ensemble means already use na.rm.
change[!is.finite(value), value := NA_real_]

# Wrangle variable name. Layer name = future basename, e.g.
# "ssp126_ACCESS-CM2_2021-2040_PTOT-sum_mean". CR-093 FIX: strip the variable
# +ext-stat+stat suffix (`_PTOT-sum_mean`/`_PTOT_sum_mean`), then `_`-split into
# scenario_model_timeframe — GCMs use dashes so they stay one field (grammar:
# [[feedback-r2-filename-parsing-pitfalls]]). Year regex already anchored (4b28977).
# CR-093 FIX: parse each table from its OWN $variable. `change` is
# rbind(increase, decrease) so it has 2x the rows of `diff`; reusing change's
# var_names for diff cbind'd mismatched row counts.
.parse_ptot_vars <- function(v) {
  v <- gsub("([0-9]{4})_([0-9]{4})", "\\1-\\2", v, perl = TRUE)
  v <- sub("_PTOT[-_]sum(_(mean|sd|sum))?$", "", v)
  m <- do.call("cbind", tstrsplit(v, "_"))
  colnames(m) <- c("scenario", "model", "timeframe")
  m
}

change <- cbind(change, .parse_ptot_vars(change$variable))[, variable := "PTOT"][, stat := "perc_change"]
diff <- cbind(diff, .parse_ptot_vars(diff$variable))[, variable := "PTOT"][, stat := "diff"]


# Generate ensemble data from models
change_ens <- change[!grepl("ENSEMBLE", model)]
change_ens <- change_ens[, list(mean = mean(value, na.rm = TRUE), min = min(value, na.rm = TRUE), max = max(value, na.rm = TRUE), sd = sd(value, na.rm = TRUE)),
  by = list(iso3, admin0_name, admin1_name, admin2_name, scenario, timeframe, direction, variable, stat)
]

diff_ens <- diff[!grepl("ENSEMBLE", model)]
diff_ens <- diff_ens[, list(mean = mean(value, na.rm = TRUE), min = min(value, na.rm = TRUE), max = max(value, na.rm = TRUE), sd = sd(value, na.rm = TRUE)),
  by = list(iso3, admin0_name, admin1_name, admin2_name, scenario, timeframe, variable, stat)
]

# save results — CR-093: iso3-first pruning + row-count log via write_chg_parquet.
write_chg_parquet(change, file.path(haz_mean_ptot_dir, "ptot_change_by_model.parquet"))
write_chg_parquet(change_ens, file.path(haz_mean_ptot_dir, "ptot_change_ensemble.parquet"))
write_chg_parquet(diff, file.path(haz_mean_ptot_dir, "ptot_diff_by_model.parquet"))
write_chg_parquet(diff_ens, file.path(haz_mean_ptot_dir, "ptot_diff_ensemble.parquet"))

.log22(sprintf("SEC1 (PTOT) — DONE in %.1fs",
               as.numeric(difftime(Sys.time(), .t_sec1, units = "secs"))))
} # end run_sec1

# 2) % area of severe or extreme crop or livestock heat stress ####
# 2.1) Livestock #####
if (run_sec2) {
.log22("SEC2 (THI livestock heat % area) — START")
.t_sec2 <- Sys.time()
# set save location
haz_mean_thi_dir <- file.path(haz_mean_dir, "thi_perc")
if (!dir.exists(haz_mean_thi_dir)) {
  dir.create(haz_mean_thi_dir)
}

# list data files. CR-093: risk files are dash-delimited (`THI-max-...`); the
# old underscore pattern `THI_max` matched nothing. Drop ENSEMBLE for parity
# with SEC3/SEC4 (the by-model output must not carry model="ENSEMBLEmean").
files <- list.files(haz_time_risk_dir, "THI-max", full.names = TRUE)
files <- files[!grepl("ENSEMBLE", files)]

# get severity thresholds. haz_class still keys on the underscore index_name
# "THI_max", but the on-disk code is dashed: THI-max-max-G<threshold>.
cat_thresholds <- haz_class[index_name == "THI_max" &
  description %in% c("Severe", "Extreme") &
  crop %in% c("cattle_highland", "cattle_tropical"), list(index_name, crop, description, threshold)][, code := paste0("THI-max-max-G", threshold)]

# get highland/lowland mask
highlands <- terra::rast(afr_highlands_file)
highlands <- terra::resample(highlands, base_rast, method = "near")
tropical <- classify(highlands, data.frame(from = c(0, 1), to = c(1, 0)))

# subset data files to
data <- pblapply(seq_len(nrow(cat_thresholds)), FUN = function(i) {
  files_ss <- grep(cat_thresholds[i, code], files, value = TRUE)
  data <- terra::rast(files_ss)

  # Apply highland/lowland mask
  if (cat_thresholds[i, grepl("tropical", crop)]) {
    data <- data * tropical
  } else {
    data <- data * highlands
  }

  names(data) <- paste0(gsub(".tif", "", basename(files_ss)), "_", cat_thresholds[i, tolower(description)])

  data
})

data_sev <- data[grep("Severe", cat_thresholds$description)]
data_sev <- data_sev[[1]] + data_sev[[2]]

data_ext <- data[grep("Extreme", cat_thresholds$description)]
data_ext <- data_ext[[1]] + data_ext[[2]]

data <- c(data_sev, data_ext)
data <- data * base_cellsize

# Extract by admin area
base_areas <- admin_extract(base_cellsize,
  boundaries_zonal,
  boundaries_index,
  FUN = "sum"
)

data <- admin_extract(data,
  boundaries_zonal,
  boundaries_index,
  FUN = "sum"
)

# Tabulate data
data <- merge_admin_extract(data)
base_areas <- merge_admin_extract(base_areas)
setnames(base_areas, "value", "total")

# Work out percentage change
# CR-093: join total area by gaul code (unique), not admin name (not unique).
data <- merge(data, base_areas[, list(gaul0_code, gaul1_code, gaul2_code, total)],
  by = c("gaul0_code", "gaul1_code", "gaul2_code"), all.x = TRUE)
data[, value := round(100 * value / total, 1)][, total := NULL]
# CR-093: NA-clean non-finite % (0/0 in zero-area zones). See ISSUE_cr093_nan_zeroprecip.md.
data[!is.finite(value), value := NA_real_]

# Wrangle variable name. CR-093: parse the dash-delimited risk layer name
# (scenario_model_timeframe_THI-max-max-Gthr_severity) directly.
var_names <- .parse_risk_vars(data$variable)

data <- cbind(data, var_names)[, hazard := "THI"][, variable := "perc_area"][, crop := "cattle"]

# Generate ensemble data from models
data_ens <- data[, list(
  mean = mean(value, na.rm = TRUE),
  min = min(value, na.rm = TRUE),
  max = max(value, na.rm = TRUE),
  sd = round(sd(value, na.rm = TRUE), 1)
),
by = list(iso3, admin0_name, admin1_name, admin2_name, scenario, timeframe, variable, severity, crop)
]

write_chg_parquet(data, file.path(haz_mean_thi_dir, "thi_perc_area_by_model.parquet"))
write_chg_parquet(data_ens, file.path(haz_mean_thi_dir, "thi_perc_area_ensemble.parquet"))

.log22(sprintf("SEC2 (THI) — DONE in %.1fs",
               as.numeric(difftime(Sys.time(), .t_sec2, units = "secs"))))
} # end run_sec2

# 2.2) Crops #####
if (run_sec3) {
.log22("SEC3 (NTx crop heat % area) — START")
.t_sec3 <- Sys.time()

haz_mean_ntx_dir <- file.path(haz_mean_dir, "ntx_perc")
if (!dir.exists(haz_mean_ntx_dir)) {
  dir.create(haz_mean_ntx_dir)
}

# choose hazards
haz_choices <- c("NTx35", "NTx40")
# choose crops
crop_choices <- "generic"
# choose severity classes
sev_classes <- c("Severe", "Extreme")


choices <- expand.grid(haz = haz_choices, crop = crop_choices, stringsAsFactors = FALSE)

# CR-093: iterate ROWS of choices (seq_along() on a data.frame walks COLUMNS;
# it only worked before because nrow happened to equal ncol).
data <- rbindlist(lapply(seq_len(nrow(choices)), FUN = function(j) {
  haz <- as.character(choices$haz[j])
  crop_focus <- as.character(choices$crop[j])

  # list data files
  files <- list.files(haz_time_risk_dir, haz, full.names = TRUE)
  files <- files[!grepl("ENSEMBLE", files)]


  # get severity thresholds. CR-093: on-disk code is dashed (NTx35-mean-G14).
  cat_thresholds <- haz_class[index_name == haz &
    description %in% sev_classes &
    crop %in% crop_focus, list(index_name, crop, description, threshold)][, code := paste0(haz, "-mean-G", threshold)] # mean -> stat, should be generalized via haz_meta

  data <- terra::rast(lapply(seq_along(sev_classes), FUN = function(i) {
    files_ss <- grep(cat_thresholds[description == sev_classes[i], code], files, value = TRUE)
    data <- terra::rast(files_ss)
    names(data) <- paste0(gsub(".tif", "", basename(files_ss)), "_", tolower(sev_classes[i]))
    data
  }))

  data <- data * base_cellsize

  # Extract by admin area
  base_areas <- admin_extract(base_cellsize,
    boundaries_zonal,
    boundaries_index,
    FUN = "sum"
  )

  data <- admin_extract(data,
    boundaries_zonal,
    boundaries_index,
    FUN = "sum",
    max_cells_in_memory = 3 * 10^8
  )

  # Tabulate data
  data <- merge_admin_extract(data)
  setnames(data, "value", "area")
  base_areas <- merge_admin_extract(base_areas)
  setnames(base_areas, "value", "total_area")

  # Work out percentage change
  # CR-093: join total area by gaul code (unique), not admin name (not unique).
  data <- merge(data, base_areas[, list(gaul0_code, gaul1_code, gaul2_code, total_area)],
    by = c("gaul0_code", "gaul1_code", "gaul2_code"), all.x = TRUE)
  data[, perc := round(100 * area / total_area, 1)]
  # CR-093: NA-clean non-finite % (0/0 in zero-area zones). See ISSUE_cr093_nan_zeroprecip.md.
  data[!is.finite(perc), perc := NA_real_]

  # Wrangle variable name. CR-093: parse the dash-delimited risk layer name
  # (scenario_model_timeframe_NTxNN-mean-Gthr_severity) directly.
  var_names <- .parse_risk_vars(data$variable)

  data <- cbind(data, var_names)[, hazard := haz][, crop := crop_focus]

  data
}))

# 2.3) Generate ensemble data from models ####
data_ens <- data
setnames(data_ens, "perc", "value")

data_ens <- data[, list(
  mean = mean(value, na.rm = TRUE),
  min = min(value, na.rm = TRUE),
  max = max(value, na.rm = TRUE),
  sd = round(sd(value, na.rm = TRUE), 1)
),
by = list(iso3, admin0_name, admin1_name, admin2_name, scenario, timeframe, hazard, severity, crop)
][, variable := "perc_area"]


# CR-093: historic scenario token is "historic" (risk-dir prefix), not "historical".
data_ens[scenario == "historic", c("min", "max", "sd") := NA]

write_chg_parquet(data, file.path(haz_mean_ntx_dir, "ntx_perc_area_by_model.parquet"))
write_chg_parquet(data_ens, file.path(haz_mean_ntx_dir, "ntx_perc_area_ensemble.parquet"))

.log22(sprintf("SEC3 (NTx) — DONE in %.1fs",
               as.numeric(difftime(Sys.time(), .t_sec3, units = "secs"))))
} # end run_sec3

# 3) Extreme drought or wet spells ####
if (run_sec4) {
.log22("SEC4 (NDWS/NDWL0 drought/wet frequency) — START")
.t_sec4 <- Sys.time()
# choose hazards
haz_choices <- c("NDWS", "NDWL0")
# choose crops
crop_choices <- "generic"
# choose severity classes
sev_classes <- c("Severe", "Extreme")

choices <- expand.grid(haz = haz_choices, crop = crop_choices, stringsAsFactors = FALSE)
extract_fun <- "mean"

# CR-093: iterate ROWS of choices (seq_along() walks columns).
data <- rbindlist(lapply(seq_len(nrow(choices)), FUN = function(j) {
  haz <- as.character(choices$haz[j])
  crop_focus <- as.character(choices$crop[j])

  # list data files
  files <- list.files(haz_time_risk_dir, haz, full.names = TRUE)
  files <- files[!grepl("ENSEMBLE", files)]

  # get stat
  stat <- haz_meta[code == haz, `function`]

  # get severity thresholds. CR-093: on-disk code is dashed (NDWS-mean-G20).
  cat_thresholds <- haz_class[index_name == haz &
    description %in% sev_classes &
    crop %in% crop_focus, list(index_name, crop, description, threshold, direction2)][, code := paste0(haz, "-", stat, "-", direction2, threshold)]

  data <- terra::rast(lapply(seq_along(sev_classes), FUN = function(i) {
    files_ss <- grep(cat_thresholds[description == sev_classes[i], code], files, value = TRUE)
    data <- terra::rast(files_ss)
    names(data) <- paste0(gsub(".tif", "", basename(files_ss)), "_", tolower(sev_classes[i]))
    data
  }))

  data <- admin_extract(data,
    boundaries_zonal,
    boundaries_index,
    FUN = extract_fun,
    max_cells_in_memory = 3 * 10^8
  )

  # Tabulate data
  data <- merge_admin_extract(data)

  # Wrangle variable name. CR-093: parse the dash-delimited risk layer name
  # (scenario_model_timeframe_<haz>-mean-Gthr_severity) directly.
  var_names <- .parse_risk_vars(data$variable)

  data <- cbind(data, var_names)[, hazard := haz][, crop := crop_focus][, variable := "frequency"]

  data
}))

years_hist <- terra::nlyr(terra::rast(list.files(haz_timeseries_dir, "hist", full.names = TRUE)[1]))
years_scen <- terra::nlyr(terra::rast(list.files(haz_timeseries_dir, "ssp245", full.names = TRUE)[1]))

data2 <- data.table::copy(data)
# CR-093: historic scenario token is "historic" (risk-dir prefix), not "historical".
data2 <- data2[scenario != "historic", value := round(value * years_scen, 0)][scenario == "historic", value := round(value * years_hist, 0)][, variable := "frequency_n"]

data[, value := round(value, 2)]

data <- rbind(data, data2)
# CR-093: NA-clean non-finite freq/freq_n (zonal mean over zones with no valid
# cells -> NaN). See ISSUE_cr093_nan_zeroprecip.md.
data[!is.finite(value), value := NA_real_]

data[hazard == "NDWS", hazard_user := "drought"][hazard == "NDWL0", hazard_user := "wet"]

data_ens <- data[, list(
  mean = round(mean(value, na.rm = TRUE), 2),
  min = min(value, na.rm = TRUE),
  max = max(value, na.rm = TRUE),
  sd = round(sd(value, na.rm = TRUE), 1)
),
by = list(iso3, admin0_name, admin1_name, admin2_name, scenario, timeframe, hazard, hazard_user, severity, crop, variable)
]

data_ens[scenario == "historic", c("min", "max", "sd") := NA]

haz_time_risk_stats_dir <- file.path(haz_time_risk_dir, "stats")
if (!dir.exists(haz_time_risk_stats_dir)) {
  dir.create(haz_time_risk_stats_dir)
}

write_chg_parquet(data, file.path(haz_time_risk_stats_dir, "haz_freq.parquet"))
write_chg_parquet(data_ens, file.path(haz_time_risk_stats_dir, "haz_freq_ensemble.parquet"))

.log22(sprintf("SEC4 (haz_freq) — DONE in %.1fs",
               as.numeric(difftime(Sys.time(), .t_sec4, units = "secs"))))
} # end run_sec4

.log22(sprintf("R/2.2 complete — total %.1fs",
               as.numeric(difftime(Sys.time(), .t0_22, units = "secs"))))
