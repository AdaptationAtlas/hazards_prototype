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
  write_parquet_pushdown(tbl, path, sort_by = chg_sort_by, verify_stats_on = "iso3")
  .log22(sprintf("wrote %s (%d rows, %d iso3)",
                 basename(path), nrow(tbl),
                 length(unique(tbl[["iso3"]]))))
}

merge_admin_extract <- function(data_ex) {
  # Define a mapping of administrative level names to short codes.
  levels <- c(admin0 = "adm0", admin1 = "adm1", admin2 = "adm2")

  # Process the extracted data to format it for analysis or further processing.
  data_ex <- rbindlist(lapply(seq_along(levels), FUN = function(i) {
    level <- levels[i]

    # Convert the data to a data.table and remove specific columns.
    # CR-093: retain iso3 (notebook filters on it + enables iso3-first
    # row-group pruning); only admin_name is dropped here.
    data <- data.table(data.frame(data_ex[[names(level)]]))
    data <- data[, !c("admin_name")]

    # Determine the administrative level being processed and adjust the data accordingly.
    # CR-093: iso3 leads the id.vars so it survives the melt at every level.
    admin <- c("iso3", "admin0_name")
    if (level %in% c("adm1", "adm2")) {
      admin <- c(admin, "admin1_name")
      data <- suppressWarnings(data[, !"a1_a0"])
    }

    if (level == "adm2") {
      admin <- c(admin, "admin2_name")
      data <- suppressWarnings(data[, !"a2_a1_a0"])
    }

    # Adjust column names and reshape the data.
    colnames(data) <- gsub("_nam$", "_name", colnames(data))
    data <- data.table(melt(data, id.vars = admin))

    data
  }), fill = TRUE)


  # Return the processed or read data.
  data_ex
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
Geographies <- lapply(seq_along(geo_files_local), FUN = function(i) {
  file <- geo_files_local[i]
  data <- arrow::open_dataset(file)
  data <- data |>
    sf::st_as_sf() |>
    terra::vect()
  data
})
names(Geographies) <- names(geo_files_local)

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
# CR-093 FIX: keep only the canonical hyphen-year form (`YYYY-YYYY`). The mean
# dir also holds ~18 stale underscore-year (`YYYY_YYYY`) duplicates of the
# historic files; including both double-counts. Per [[feedback-r2-filename-parsing-pitfalls]]
# the producer should not be re-run just to fix names — filter on read.
files <- grep("[0-9]{4}-[0-9]{4}", files, value = TRUE)
files_hist <- grep("historic", files, value = TRUE)
files_fut <- files[!files %in% files_hist]

# CR-093 FIX: pair historic->future by GCM (the shared token), not by the whole
# historic basename. The old `gsub("historical_", "")` matched nothing: the real
# historic prefix is `historic_historic_historic_` and future names share only
# the GCM (years + scenario differ). Filename grammar is documented in
# [[feedback-r2-filename-parsing-pitfalls]]: scenario_model_timeframe_<haz>[_stat],
# `_`-split, GCMs dashed, years YYYY-YYYY, historic scenario = 3 tokens.
.extract_gcm <- function(x) {
  b <- sub("^(historic_historic_historic|ssp[0-9]+)_", "", basename(x))  # drop scenario prefix
  sub("_[0-9]{4}-[0-9]{4}_.*$", "", b)                                    # drop _years_<rest>
}
hist_gcm <- .extract_gcm(files_hist)
fut_gcm  <- .extract_gcm(files_fut)

.log22(sprintf("SEC1: %d historic PTOT file(s), %d future file(s); %d distinct GCMs",
               length(files_hist), length(files_fut), length(unique(hist_gcm))))
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
  Geographies = Geographies,
  FUN = "sum"
)

change_inc <- admin_extract(change_inc,
  Geographies = Geographies,
  FUN = "sum"
)

change_dec <- admin_extract(change_dec,
  Geographies = Geographies,
  FUN = "sum"
)

diff <- admin_extract(diff,
  Geographies = Geographies,
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
change <- merge(change, base_areas[, list(iso3, admin0_name, admin1_name, admin2_name, total)], all.x = TRUE)
change[, value := round(100 * value / total, 1)][, total := NULL]

# Wrangle variable name. Layer name = future basename, e.g.
# "ssp126_ACCESS-CM2_2021-2040_PTOT-sum_mean". CR-093 FIX: strip the variable
# +ext-stat+stat suffix (`_PTOT-sum_mean`/`_PTOT_sum_mean`), then `_`-split into
# scenario_model_timeframe — GCMs use dashes so they stay one field (grammar:
# [[feedback-r2-filename-parsing-pitfalls]]). Year regex already anchored (4b28977).
var_names <- change$variable
var_names <- gsub("([0-9]{4})_([0-9]{4})", "\\1-\\2", var_names, perl = TRUE)
var_names <- sub("_PTOT[-_]sum(_(mean|sd|sum))?$", "", var_names)
var_names <- do.call("cbind", tstrsplit(var_names, "_"))
colnames(var_names) <- c("scenario", "model", "timeframe")

change <- cbind(change, var_names)[, variable := "PTOT"][, stat := "perc_change"]
diff <- cbind(diff, var_names)[, variable := "PTOT"][, stat := "diff"]


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

# list data files
files <- list.files(haz_time_risk_dir, "THI_max", full.names = TRUE)

# get severity thresholds
cat_thresholds <- haz_class[index_name == "THI_max" &
  description %in% c("Severe", "Extreme") &
  crop %in% c("cattle_highland", "cattle_tropical"), list(index_name, crop, description, threshold)][, code := paste0("THI_max_max-G", threshold)]

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
  Geographies = Geographies,
  FUN = "sum"
)

data <- admin_extract(data,
  Geographies = Geographies,
  FUN = "sum"
)

# Tabulate data
data <- merge_admin_extract(data)
base_areas <- merge_admin_extract(base_areas)
setnames(base_areas, "value", "total")

# Work out percentage change
data <- merge(data, base_areas[, list(iso3, admin0_name, admin1_name, admin2_name, total)], all.x = TRUE)
data[, value := round(100 * value / total, 1)][, total := NULL]

# Wrangle variable name
var_names <- data$variable
var_names <- gsub("sum.|_THI_max_max", "", var_names)
var_names <- gsub("([0-9]{4})_([0-9]{4})", "\\1-\\2", var_names, perl = TRUE)
var_names <- gsub(".G", "_", var_names)
var_names <- gsub("historical", "historical_historical_historical", var_names)
var_names <- do.call("cbind", tstrsplit(var_names, "_"))[, c(1:3, 5)]
colnames(var_names) <- c("scenario", "model", "timeframe", "severity")

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


choices <- expand.grid(haz = haz_choices, crop = crop_choices)

data <- rbindlist(lapply(seq_along(choices), FUN = function(j) {
  haz <- as.character(choices$haz[j])
  crop_focus <- as.character(choices$crop[j])

  # list data files
  files <- list.files(haz_time_risk_dir, haz, full.names = TRUE)
  files <- files[!grepl("ENSEMBLE", files)]


  # get severity thresholds
  cat_thresholds <- haz_class[index_name == haz &
    description %in% sev_classes &
    crop %in% crop_focus, list(index_name, crop, description, threshold)][, code := paste0(haz, "_mean-G", threshold)] # mean-G -> this needs to be generalized

  data <- terra::rast(lapply(seq_along(sev_classes), FUN = function(i) {
    files_ss <- grep(cat_thresholds[description == sev_classes[i], code], files, value = TRUE)
    data <- terra::rast(files_ss)
    names(data) <- paste0(gsub(".tif", "", basename(files_ss)), "_", tolower(sev_classes[i]))
    data
  }))

  data <- data * base_cellsize

  # Extract by admin area
  base_areas <- admin_extract(base_cellsize,
    Geographies = Geographies,
    FUN = "sum"
  )

  data <- admin_extract(data,
    Geographies = Geographies,
    FUN = "sum",
    max_cells_in_memory = 3 * 10^8
  )

  # Tabulate data
  data <- merge_admin_extract(data)
  setnames(data, "value", "area")
  base_areas <- merge_admin_extract(base_areas)
  setnames(base_areas, "value", "total_area")

  # Work out percentage change
  data <- merge(data, base_areas[, list(iso3, admin0_name, admin1_name, admin2_name, total_area)], all.x = TRUE)
  data[, perc := round(100 * area / total_area, 1)]

  # Wrangle variable name
  var_names <- data$variable
  var_names <- gsub(paste0("sum.|_", haz, "_mean"), "", var_names) # _mean needs to be generalized
  var_names <- gsub("([0-9]{4})_([0-9]{4})", "\\1-\\2", var_names, perl = TRUE)
  var_names <- gsub(".G", "_", var_names) # .G needs to be generalized
  var_names <- gsub("historical", "historical_historical_historical", var_names)
  var_names <- do.call("cbind", tstrsplit(var_names, "_"))[, c(1:3, 5)]
  colnames(var_names) <- c("scenario", "model", "timeframe", "severity")

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


data_ens[scenario == "historical", c("min", "max", "sd") := NA]

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

choices <- expand.grid(haz = haz_choices, crop = crop_choices)
extract_fun <- "mean"

data <- rbindlist(lapply(seq_along(choices), FUN = function(j) {
  haz <- as.character(choices$haz[j])
  crop_focus <- as.character(choices$crop[j])

  # list data files
  files <- list.files(haz_time_risk_dir, haz, full.names = TRUE)
  files <- files[!grepl("ENSEMBLE", files)]

  # get stat
  stat <- haz_meta[code == haz, `function`]

  # get severity thresholds
  cat_thresholds <- haz_class[index_name == haz &
    description %in% sev_classes &
    crop %in% crop_focus, list(index_name, crop, description, threshold, direction2)][, code := paste0(haz, "_", stat, "-", direction2, threshold)]

  data <- terra::rast(lapply(seq_along(sev_classes), FUN = function(i) {
    files_ss <- grep(cat_thresholds[description == sev_classes[i], code], files, value = TRUE)
    data <- terra::rast(files_ss)
    names(data) <- paste0(gsub(".tif", "", basename(files_ss)), "_", tolower(sev_classes[i]))
    data
  }))

  data <- admin_extract(data,
    Geographies = Geographies,
    FUN = extract_fun,
    max_cells_in_memory = 3 * 10^8
  )

  # Tabulate data
  data <- merge_admin_extract(data)

  # Wrangle variable name
  var_names <- data$variable
  var_names <- gsub(paste0(extract_fun, ".|_", haz, "_", stat), "", var_names)
  var_names <- gsub("([0-9]{4})_([0-9]{4})", "\\1-\\2", var_names, perl = TRUE)
  var_names <- gsub(paste0(".", cat_thresholds[1, direction2]), "_", var_names)
  var_names <- gsub("historical", "historical_historical_historical", var_names)
  var_names <- do.call("cbind", tstrsplit(var_names, "_"))[, c(1:3, 5)]
  colnames(var_names) <- c("scenario", "model", "timeframe", "severity")

  data <- cbind(data, var_names)[, hazard := haz][, crop := crop_focus][, variable := "frequency"]

  data
}))

years_hist <- terra::nlyr(terra::rast(list.files(haz_timeseries_dir, "hist", full.names = TRUE)[1]))
years_scen <- terra::nlyr(terra::rast(list.files(haz_timeseries_dir, "ssp245", full.names = TRUE)[1]))

data2 <- data.table::copy(data)
data2 <- data2[scenario != "historical", value := round(value * years_scen, 0)][scenario == "historical", value := round(value * years_hist, 0)][, variable := "frequency_n"]

data[, value := round(value, 2)]

data <- rbind(data, data2)

data[hazard == "NDWS", hazard_user := "drought"][hazard == "NDWL0", hazard_user := "wet"]

data_ens <- data[, list(
  mean = round(mean(value, na.rm = TRUE), 2),
  min = min(value, na.rm = TRUE),
  max = max(value, na.rm = TRUE),
  sd = round(sd(value, na.rm = TRUE), 1)
),
by = list(iso3, admin0_name, admin1_name, admin2_name, scenario, timeframe, hazard, hazard_user, severity, crop, variable)
]

data_ens[scenario == "historical", c("min", "max", "sd") := NA]

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
