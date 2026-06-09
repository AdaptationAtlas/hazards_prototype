# 2.1_create_monthly_haz_tables.R
# ================================
# Monthly hazard extraction + seasonal summarisation for the Atlas climateRationale
# notebook (Future Projections section).
#
# REQUIRES: source R/0_server_setup.R first (sets atlas_dirs, indices_dir, climdat_source).
# Use scripts/r21_rerun.sh for the canonical relaunch pattern.
#
# INPUTS:
#   indices_dir  — per-GCM hazard index rasters (CGlabs: atlas_nex-gddp_hazards/cmip6/indices/)
#   MapSPAM / GAUL boundaries (via atlas_dirs + boundaries_zonal)
#
# OUTPUTS (local, then publish via scripts/r21_publish_to_s3.R):
#   Data/hazard_timeseries_mean_month/
#     intermediate/        — per-GCM monthly parquets (sec 2)
#     haz_monthly_adm_mean_*.parquet    — combined monthly (sec 2.5)
#     haz_3months_adm_mean_*.parquet    — seasonal (sec 3.1)
#     *_anomaly-*_seasons.parquet       — per-model anomalies (sec 3.2)
#     *_anomaly-*_ensemble_seasons.parquet  — CANONICAL: ensemble_season_timeseries.parquet
#                                            (drives notebook Future Projections)
#     *_anomaly-*_ensemble.parquet      — period-aggregate ensemble (sec 3.3)
#     *_trends*.parquet                 — Theil-Sen + MK + TFPW trends (sec 3.4, ~9h/timeframe)
#
# SECTION RUN CONTROLS (env vars, all default to running):
#   SKIP_R2_1_SEC2=1   — skip extraction + combine (sec 2)
#   SKIP_R2_1_SEC3_1=1 — skip seasonal summarisation (sec 3.1)
#   SKIP_R2_1_SEC3_2=1 — skip anomaly calculation (sec 3.2)
#   SKIP_R2_1_SEC3_3=1 — skip ensemble statistics (sec 3.3; includes CR-060 quantiles)
#   SKIP_R2_1_SEC3_4=1 — skip trend computation (sec 3.4; includes CR-094 TFPW)
#   FORCE_OVERWRITE=1  — rewrite all existing output files
#
# TYPICAL RUNTIMES (CGlabs, 5-GCM NEX-GDDP subset):
#   Sec 2 (extraction):    ~1h  (parallel, worker_n1=5)
#   Sec 3.1 (seasonal):    ~30 min
#   Sec 3.2 (anomalies):   ~1-2h
#   Sec 3.3 (ensemble):    ~1-2h
#   Sec 3.4 (trends):      ~9h per timeframe (>10^6 linear models)

cat("Starting 2.1_create_monthly_haz_tables.R\n")

# 0) Load R functions & packages ####
packages <- c(
  "arrow",
  "geoarrow",
  "sf",
  "terra",
  "data.table",
  "dplyr",
  "furrr",
  "progressr",
  "parallel",
  "pbapply",
  "trend"
)

# Call the function to install and load packages
p_load(char = packages)
source(file.path(Sys.getenv("project_dir"), "R", "_helpers.R"))

# 1) Set up workspace ####
## 1.1) Set directories #####
output_dir <- atlas_dirs$data_dir$hazard_timeseries_mean_month
cat("output_dir =", output_dir, "\n")

output_int_dir <- file.path(output_dir, "intermediate")
if (!dir.exists(output_int_dir)) {
  dir.create(output_int_dir)
}

## 1.2) Set hazards to include in analysis #####
if (climdat_source == "atlas_delta") {
  hazards <- c("HSH_max", "TMAX", "TAVG", "NDWL0", "NDWS", "NTx35", "NTx40", "PTOT", "THI_max") # NDD is not being used as it cannot be projected to future scenarios
} else {
  # NTx40 temporarily exclude while pipeline completes ####
  hazards <- c("HSH_max", "TMAX", "TAVG", "NDWL0", "NDWS", "NTx35", "PTOT", "THI_max", "NDD")
}
cat("Working with hazards =", hazards, "\n")
file_name <- "all_hazards.parquet"

## 1.3) Set scenarios and time frames to analyse #####
Scenarios <- c("ssp126", "ssp245", "ssp370", "ssp585")
cat("Using scenarios =", Scenarios, "\n")

Times <- c("2021_2040", "2041_2060", "2061_2080", "2081_2100")
cat("Using time periods =", Times, "\n")

# Create combinations of scenarios and times
Scenarios <- rbind(data.table(Scenario = "historic", Time = "historic"), data.table(expand.grid(Scenario = Scenarios, Time = Times)))
Scenarios[, combined := paste0(Scenario, "-", Time)]

## 1.4) Load admin boundaries #####
# This is limited to admin1 (admin2 is possible but we generate huge files that go beyond the 2gb recommended size for a parquet file, so the data would need to split into chunks)
Geographies <- lapply(1:2, FUN = function(i) {
  # Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
  file <- geo_files_local[i]
  data <- arrow::open_dataset(file)
  data <- data |>
    sf::st_as_sf() |>
    terra::vect()
  data$zone_id <- ifelse(!is.na(data$gaul2_code), data$gaul2_code,
    ifelse(!is.na(data$gaul1_code), data$gaul1_code, data$gaul0_code)
  )
  data
})

# names(Geographies)<-names(geo_files_local)
names(Geographies) <- names(geo_files_local)[1:2]

base_rast <- terra::rast(base_rast_path) + 0

boundaries_zonal <- lapply(seq_along(Geographies), FUN = function(i) {
  file_path <- file.path(boundaries_int_dir, paste0(names(Geographies)[i], "_zonal.tif"))
  if (!file.exists(file_path)) {
    zones <- Geographies[[i]]
    zone_rast <- rasterize(
      x = zones,
      y = base_rast,
      field = "zone_id",
      background = NA, # cells not covered by any polygon become NA
      touches = TRUE # optional: count cells touched by polygon boundaries
    )
    terra::writeRaster(zone_rast, file_path, overwrite = TRUE)
  }
  file_path
})
names(boundaries_zonal) <- names(Geographies)

boundaries_index <- lapply(seq_along(Geographies), FUN = function(i) {
  data.frame(Geographies[[i]])[, c("iso3", "admin0_name", "admin1_name", "admin2_name", "zone_id", "gaul0_code", "gaul1_code", "gaul2_code")]
})

names(boundaries_index) <- names(Geographies)

## 1.5) Load hazard meta-data #####
haz_meta <- data.table::fread(haz_meta_url, showProgress = FALSE)

## 1.6) Controls ####

### Section run controls ####
# Each section can be skipped by setting the corresponding env var to "1".
# Setup variables (file lists, combos, baselines) always run — they are fast
# metadata operations needed by downstream sections regardless of which heavy
# processing is skipped.
#
# Usage examples:
#   Full rerun from scratch:        FORCE_OVERWRITE=1
#   Resume at sec 3.1 (skip sec 2): SKIP_R2_1_SEC2=1
#   Resume at sec 3.2 (skip 2+3.1): SKIP_R2_1_SEC2=1 SKIP_R2_1_SEC3_1=1
#   Skip trend computation only:     SKIP_R2_1_SEC3_4=1   (same as old SKIP_R2_1_3_4)
run_sec2   <- !nzchar(Sys.getenv("SKIP_R2_1_SEC2"))
run_sec3_1 <- !nzchar(Sys.getenv("SKIP_R2_1_SEC3_1"))
run_sec3_2 <- !nzchar(Sys.getenv("SKIP_R2_1_SEC3_2"))
run_sec3_3 <- !nzchar(Sys.getenv("SKIP_R2_1_SEC3_3"))
run_sec3_4 <- !nzchar(Sys.getenv("SKIP_R2_1_SEC3_4")) && !nzchar(Sys.getenv("SKIP_R2_1_3_4"))
cat(sprintf("Section controls: sec2=%s 3.1=%s 3.2=%s 3.3=%s 3.4=%s\n",
            run_sec2, run_sec3_1, run_sec3_2, run_sec3_3, run_sec3_4))

### Section 2 - Extraction of monthly hazards by admin areas ####
round1 <- 1
version1 <- 1
worker_n1 <- 5
overwrite1 <- nzchar(Sys.getenv("FORCE_OVERWRITE")) # set FORCE_OVERWRITE=1 to regenerate

# Data QC checks
max_rain <- 3000 # Max acceptable value for monthly rainfall
min_haz <- -10 # Min acceptable value for any hazards (some temperatures can be negative)
exclude_flagged <- FALSE # Exclude combinations of admin x timeframe x scenario x model x hazard that contain any bad values?

### Section 3 - Summarization of monthly hazards ####
worker_n2 <- 20
overwrite2 <- nzchar(Sys.getenv("FORCE_OVERWRITE"))
round3.1 <- 3
round3.3 <- 3
round3.4 <- 3

### Final data ####
round_final <- 1

# 2) Extract hazard folders by admin boundaries ####
## 2.1) List hazard folders ####
folders <- list.dirs(indices_dir, recursive = FALSE, full.names = TRUE)
folders <- folders[!grepl("ENSEMBLE|ipyn|gadm0|hazard_comb|indices_seasonal", folders)]
folders <- folders[grepl(paste0(Scenarios$Scenario, collapse = "|"), folders) & grepl(paste0(Scenarios$Time, collapse = "|"), folders)]

folders <- data.table(path = folders)
folders[, scenario := unlist(tstrsplit(basename(path), "_", keep = 1))][!grepl("historical", scenario), model := unlist(tstrsplit(basename(path), "_", keep = 2))][!grepl("historical", scenario), timeframe := paste0(unlist(tstrsplit(basename(path), "_", keep = 3:4)), collapse = "-"), by = path][grepl("historical", scenario), c("timeframe", "model") := scenario][, path_new := file.path(output_dir, paste0(scenario, "_", model, "_", timeframe)), by = .I][, path_new := gsub("historical", "historic", path_new)]


## Temporarily subset folders in nex-gddp ####
gcms <- c("MRI-ESM2-0", "ACCESS-ESM1-5", "MPI-ESM1-2-HR", "EC-Earth3", "INM-CM5-0")
folders <- folders[model %in% gcms]

folders <- data.frame(folders)

cat("Folders included =", paste0(unique(folders$path), collapse = "\n"), "\n")


## 2.2) Set parameters ####
levels <- c(admin0 = "adm0", admin1 = "adm1") # ,admin2="adm2")

id_vars <- c("iso3", "admin0_name", "admin1_name", "admin2_name", "gaul0_code", "gaul1_code", "gaul2_code")
split_delim <- "_"
split_colnames <- c("scenario", "timeframe", "model", "hazard", "year", "month")
extract_stat <- "mean"
order_by <- c("iso3", "admin0_name", "admin1_name", "admin2_name", "gaul0_code", "gaul1_code", "gaul2_code")
order_by2 <- c("admin0_name", "admin1_name", "season", "hazard", "scenario", "timeframe")

## 2.3) Define the extraction function ####
extract_hazard <- function(i, folders, hazards, output_dir, overwrite, round_dp, extract_stat,
                           boundaries_zonal, boundaries_index, id_vars, split_colnames,
                           order_by, haz_meta, version, extraction_rast, levels, base_rast_path) {
  folders_ss <- paste0(folders$path[i], "/", hazards)
  base_rast <- terra::rast(base_rast_path)

  invisible(purrr::map(hazards, function(hazard) {
    cat(dirname(folders_ss[1]), hazard, "\n")
    folders_ss_focus <- gsub("_max|_mean", "", paste0(folders$path[i], "/", hazard))
    h_var <- unlist(tail(tstrsplit(hazard, "_"), 1))

    filename <- paste0(basename(folders$path_new[i]), "_", gsub("_", "-", hazard), "_", extract_stat, ".parquet")
    save_file <- file.path(output_dir, filename)

    if (!file.exists(save_file) || overwrite) {
      files <- list.files(folders_ss_focus, ".tif$", full.names = TRUE)
      files <- files[!grepl("AVAIL", files)]
      if (h_var %in% c("mean", "max")) {
        files <- grep(paste0("_", h_var, "-"), files, value = TRUE)
      }

      if (length(files) != 0) {
        rast_data <- terra::rast(files)

        rast_data <- mask(rast_data, base_rast)

        if (hazard == "PTOT") rast_data[rast_data < 0] <- NA

        rast_names <- data.table(base_name = gsub(".tif", "", basename(files)))
        rast_names[, c("year", "month") := tstrsplit(base_name, "-", keep = 2:3)]
        rast_names[, hazard := gsub("_", "-", unlist(tstrsplit(base_name, "-", keep = 1)))]
        rast_names[, new_name := paste(folders$scenario[i], folders$timeframe[i], folders$model[i], hazard, year, month, sep = "_")]

        names(rast_data) <- rast_names$new_name
        if (anyDuplicated(names(rast_data)) > 0) stop("Duplicate layer names present")

        result <- rbindlist(purrr::map2(boundaries_zonal, boundaries_index, function(zonal_rast, idx) {
          zonal_r <- terra::rast(zonal_rast)
          dat <- zonal(rast_data, zonal_r, fun = extract_stat, na.rm = TRUE)
          dat <- merge(dat, idx, by = "zone_id", all.x = TRUE, sort = FALSE)
          dat$zone_id <- NULL
          dat
        }))

        result_long <- melt(result, id.vars = id_vars)
        if (!is.null(round_dp)) result_long[, value := round(value, round_dp)]
        result_long[, (split_colnames) := tstrsplit(variable, "_", fixed = TRUE)]
        result_long[, variable := NULL]
        result_long <- result_long[do.call(order, result_long[, ..order_by])]

        write_parquet_pushdown(
          result_long, save_file,
          sort_by         = c("iso3", "admin0_name", "admin1_name", "hazard", "scenario", "model", "timeframe", "year", "month"),
          verify_stats_on = c("admin0_name", "hazard")
        )
        write_json(list(
          source = list(input_raster = files, extraction_rast = extraction_rast),
          extraction_method = "zonal",
          geo_filters = id_vars,
          season_type = NA,
          filters = lapply(split_colnames, function(col) unique(result_long[[col]])),
          format = ".parquet",
          date_created = Sys.time(),
          version = version1,
          parent_script = "R/2.1_create_monthly_haz_tables.R - section 2",
          value_variable = unique(result_long$hazard),
          unit = haz_meta[haz_meta$variable.code == hazard, "base_unit"],
          extract_stat = extract_stat,
          notes = paste0("Monthly hazard values extracted by admin areas summarized using ", extract_stat, ".")
        ), paste0(save_file, ".json"), pretty = TRUE)

        rm(result, rast_data)
        gc()

        result_long
      }
    }
  }))
}

## 2.4) Run parallel extraction ####
if (run_sec2) {
# Set parallel plan
set_parallel_plan(n_cores = worker_n1, use_multisession = TRUE)

# Enable progress reporting
options(progressr.enable = TRUE)
progressr::handlers(global = TRUE)
progressr::handlers("progress")

# Progress-wrapped parallel execution
with_progress({
  prog <- progressor(steps = nrow(folders))
  results <- furrr::future_map(seq_len(nrow(folders)), function(i) {
    prog(sprintf("Processing folder %d of %d", i, nrow(folders)))
    extract_hazard(i,
      base_rast_path = base_rast_path,
      folders = folders,
      hazards = hazards,
      output_dir = output_int_dir,
      overwrite = overwrite1,
      round_dp = round1,
      extract_stat = extract_stat,
      boundaries_zonal = boundaries_zonal,
      boundaries_index = boundaries_index,
      id_vars = id_vars,
      split_colnames = split_colnames,
      order_by = order_by,
      haz_meta = data.frame(haz_meta),
      version = version1,
      extraction_rast = atlas_data$boundaries$name,
      levels = levels
    )
  }, .options = furrr::furrr_options(scheduling = Inf))
})
plan(sequential)
} # end if (run_sec2) — extraction

## 2.5) List and combine monthly hazard parquet files ####
# Always run setup (fast file listing — needed by all downstream sections
# even when sec 2 extraction is skipped).
files <- list.files(output_int_dir, ".parquet$", full.names = TRUE)
files <- data.table(file = files)[, c("scenario", "model", "timeframe", "hazard", "stat") := tstrsplit(basename(file), "_", keep = 1:5)][, stat := gsub(".parquet", "", stat)]

timeframes <- files[, unique(timeframe)]
baselines <- files[grep("historic", scenario), unique(scenario)]
# Names are assigned in the canonical order — truncated to however many
# baselines are actually present (CGlabs CMIP6 only has 1995-2014;
# AgERA5 1981-2022 may not be present on all setups).
all_baseline_names <- c("1995-2014", "AgERA5 1981-2022")
names(baselines) <- all_baseline_names[seq_along(baselines)]

futures <- files[!grepl("historic", timeframe), unique(timeframe)]

if (run_sec2) {
problem_data <- lapply(seq_along(timeframes), FUN = function(i) {
  timeframe_choice <- timeframes[i]
  save_path <- file.path(output_dir, paste0("haz_monthly_adm_mean_", timeframe_choice, ".parquet"))

  if (!file.exists(save_path) | overwrite1) {
    cat("2.4) Merging data for timeframe", timeframe_choice, "\n")

    files_ss <- files[timeframe == timeframe_choice, file]

    data <- rbindlist(pblapply(files_ss, arrow::read_parquet))

    # Check for any weird results
    check <- data[value > max_rain | value < (min_haz)] # Highest monthly recorded rainfall in africa is <3000mm
    if (nrow(check) > 0) {
      warning(
        nrow(check), " rows of data have values >", max_rain, " or <", min_haz, ". These hazards x admin areas x scenario x models have ",
        if (!exclude_flagged) {
          "not been"
        } else {
          "been"
        }, " excluded.\n"
      )
      problem_dat <- unique(check[, .(hazard, iso3, admin0_name, admin1_name, admin2_name, scenario, model)])
      cat("Problem data:\n")
      print(problem_dat)

      problem_dat[, suspect_value_flag := TRUE]
      data <- merge(data, problem_dat, all.x = TRUE, sort = FALSE)
      data[is.na(suspect_value_flag), suspect_value_flag := FALSE]
      if (exclude_flagged) {
        data <- data[suspect_value_flag != FALSE]
      }
    } else {
      data[, suspect_value_flag := FALSE]
    }

    if (!is.null(order_by)) {
      setorderv(data, order_by)
    }

    data[, year := as.integer(year)][, month := as.integer(month)]

    write_parquet_pushdown(
      data, save_path,
      sort_by         = c("iso3", "admin0_name", "admin1_name", "hazard", "scenario", "model", "timeframe", "year", "month"),
      verify_stats_on = c("admin0_name", "hazard", "scenario")
    )

    json_dat <- jsonlite::read_json(paste0(files_ss[1], ".json"), simplifyVector = TRUE)
    filters <- list(
      scenario = data[, unique(scenario)],
      model = data[, unique(model)],
      timeframe = data[, unique(timeframe)],
      year = data[, unique(year)],
      hazard = data[, unique(hazard)],
      month = data[, unique(month)]
    )

    jsonlite::write_json(
      list(
        source = list(input_raster = files_ss, extraction_rast = atlas_data$boundaries$name),
        extraction_method = "zonal",
        geo_filters = json_dat$geo_filters,
        season_type = NA,
        filters = filters,
        format = ".parquet",
        date_created = Sys.time(),
        version = json_dat$version,
        parent_script = "R/2.1_create_monthly_haz_tables.R - section 2.5",
        value_variable = "hazard value",
        unit = haz_meta[variable.code %in% data[, unique(hazard)], .(variable.code, base_unit)],
        extract_stat = json_dat$extract_stat,
        notes = paste0("Monthly hazard values extracted and summarized using ", extract_stat, "."),
        problem_data = check
      ), paste0(save_path, ".json"),
      pretty = TRUE
    )
  }

  if (nrow(check) > 0) {
    check
  } else {
    NULL
  }
})
} # end if (run_sec2) — combine

# Always compute monthly_files — fast path construction needed by all sections.
monthly_files <- file.path(output_dir, paste0("haz_monthly_adm_mean_", timeframes, ".parquet"))

# Check for missing values (coerce to data.table — arrow::read_parquet returns
# a tibble on some setups and data.table syntax requires a data.table).
data <- data.table(arrow::read_parquet(monthly_files[1]))
missing <- data[value == -Inf | is.infinite(value) | is.na(value) | is.null(value), .(hazard = paste(unique(hazard), collapse = ",")), by = .(admin0_name, admin1_name)]

if (nrow(missing) > 0) {
  warning("These hazards x admin areas are missing data")
  print(missing)
}


# 3) Summarize annually or 3 month windows ####
## 3.0) Create 3 month windows #####
# Define month abbreviations
month_abbr <- c("J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D")

# Generate 3-month periods in a year
three_month_periods <- lapply(1:12, function(start) {
  end <- start + 2
  if (end <= 12) {
    start:end
  } else {
    c(start:12, 1:(end - 12))
  }
})

# Name the list with month abbreviations
names(three_month_periods) <- sapply(1:12, function(start) {
  end <- start + 2
  if (end <= 12) {
    paste(month_abbr[start:end], collapse = "")
  } else {
    paste(c(month_abbr[start:12], month_abbr[1:(end - 12)]), collapse = "")
  }
})
three_month_periods$annual <- 1:12

## 3.1) Seasonal hazard calculation ####
cat("3.1) Seasonal hazard calculation \n")
if (run_sec3_1) {

# iso3 must be in id_vars so it propagates through sec 3.1 → 3.2 → 3.3
# where the sec 3.3 by-clause groups on it for the canonical schema.
id_vars <- c("iso3", "admin0_name", "admin1_name", "scenario", "model", "timeframe", "year", "hazard", "suspect_value_flag")

lapply(monthly_files, FUN = function(month_file) {
  save_file <- gsub("_monthly_", "_3months_", month_file)

  if (!file.exists(save_file) | overwrite2) {
    cat("3.1) Seasonal summarization: ", basename(month_file), "\n")
    data_ex_ss <- data.table(arrow::read_parquet(month_file))
    vars <- data_ex_ss[, unique(hazard)]

    data_ex_season <- lapply(seq_along(three_month_periods), function(j) {
      m_period    <- three_month_periods[[j]]
      season_name <- names(three_month_periods)[j]
      dt <- copy(data_ex_ss)[month %in% m_period]

      dt[, seq := find_consecutive_pattern(seq = month, pattern = m_period),
        by = .(admin0_name, admin1_name, model, scenario, timeframe, hazard)
      ]

      dt <- dt[!is.na(seq)]
      dt[, year := year[1], by = .(admin0_name, admin1_name, model, scenario, timeframe, hazard, seq)]
      dt[, seq := NULL]

      data_season <- rbindlist(lapply(vars, function(VAR) {
        func_name <- unique(haz_meta$`function`[haz_meta$variable.code == gsub("-", "_", VAR)])
        func <- match.fun(func_name)

        dt[hazard == VAR, .(
          value = round(func(value, na.rm = TRUE), round3.1),
          n_value = .N
        ), by = id_vars][, season := season_name]
      }), use.names = TRUE, fill = TRUE)

      cat("Completed: ", names(three_month_periods)[j], " ", j, "/", length(three_month_periods), "      \r")
      data_season
    })

    data_ex_season <- rbindlist(data_ex_season)

    if (!is.null(order_by2)) {
      setorderv(data_ex_season, order_by2)
    }

    write_parquet_pushdown(
      data_ex_season, save_file,
      sort_by         = c("iso3", "admin0_name", "admin1_name", "hazard", "scenario", "model", "timeframe", "season", "year"),
      verify_stats_on = c("admin0_name", "hazard", "scenario")
    )

    json_dat <- jsonlite::read_json(paste0(month_file, ".json"), simplifyVector = TRUE)
    filters <- list(
      scenario = data_ex_season[, unique(scenario)],
      model = data_ex_season[, unique(model)],
      timeframe = data_ex_season[, unique(timeframe)],
      year = data_ex_season[, unique(year)],
      hazard = data_ex_season[, unique(hazard)],
      season = data_ex_season[, unique(season)]
    )

    jsonlite::write_json(
      list(
        source = list(input_table = save_file, extraction_rast = atlas_data$boundaries$name),
        extraction_method = "zonal",
        geo_filters = grep("admin", colnames(data_ex_season), value = TRUE),
        season_type = NA,
        filters = filters,
        format = ".parquet",
        date_created = Sys.time(),
        version = json_dat$version,
        parent_script = "R/2.1_create_monthly_haz_tables.R - section 3.1",
        value_variable = "hazard value",
        unit = haz_meta[variable.code %in% data_ex_ss[, unique(hazard)], .(variable.code, base_unit)],
        extract_stat = json_dat$extract_stat,
        notes = paste0("Monthly hazard values extracted by admin areas and summarized using ", extract_stat, ". Values then combined across 3 or 12 month sequences using sum or mean depending on the hazard type."),
        problem_data = data_ex_season[suspect_value_flag == TRUE]
      ), paste0(save_file, ".json"),
      pretty = TRUE
    )
  }
})
} # end if (run_sec3_1)

# Always compute — fast string derivation, needed by sec 3.2+ even if 3.1 skipped.
monthly3_files <- gsub("_monthly_", "_3months_", monthly_files)

cat("3.1) Seasonal hazard calculation - Complete \n")

## 3.2) Add historical mean ####
cat("3.2) Adding historical means \n")

# Always compute baseline_timeframe_map — needed by file_combos (always-run)
# and by data_ex_hist inside the sec 3.2 guard.
# baselines contains scenario names (e.g. "historic") but monthly3_files
# use the timeframe column (e.g. "historical"). Build the lookup here.
baseline_timeframe_map <- setNames(
  files[, .(timeframe = unique(timeframe)[1]), by = scenario]$timeframe,
  files[, .(timeframe = unique(timeframe)[1]), by = scenario]$scenario
)

if (run_sec3_2) {

data_ex_hist <- lapply(baselines, FUN = function(baseline) {
  tf <- baseline_timeframe_map[baseline]
  data <- data.table(arrow::read_parquet(grep(paste0("_", tf, "[.]"), monthly3_files, value = TRUE)))
  # Include iso3 in the aggregation so it propagates through the sec 3.2 merge
  # and survives into the sec 3.3 ensemble by-clause.
  data <- data[, .(baseline_value = round(mean(value, na.rm = TRUE), round3.1)), by = c("iso3", "admin0_name", "admin1_name", "hazard", "season")]
  data[, baseline_name := baseline]
  data
})

names(data_ex_hist) <- baselines
} # end if (run_sec3_2) — data_ex_hist

# Always compute file_combos — fast path construction needed by sec 3.3 and 3.4
# even when sec 3.2 is skipped.
fut_monthly3_files <- monthly3_files[!grepl("historic", monthly3_files)]

file_combos <- data.table(rbind(
  expand.grid(data = fut_monthly3_files, baseline = baselines, stringsAsFactors = FALSE),
  rbindlist(lapply(baselines, FUN = function(baseline) {
    tf <- baseline_timeframe_map[baseline]
    data.frame(
      data = paste0(output_dir, "/haz_3months_adm_mean_", tf, ".parquet"),
      baseline = baseline
    )
  }))
))

file_combos[, save_file := gsub(".parquet", paste0("_anomaly-", baseline, "_seasons.parquet"), data), by = .I][, save_file2 := gsub(".parquet", paste0("_anomaly-", baseline, "_ensemble_seasons.parquet"), data), by = .I][, save_file3 := gsub(".parquet", paste0("_anomaly-", baseline, "_ensemble.parquet"), data), by = .I]

if (run_sec3_2) {
invisible(lapply(seq_len(nrow(file_combos)), FUN = function(i) {
  save_file <- file_combos$save_file[i]

  if (!file.exists(save_file) | overwrite2) {
    cat("3.2) Calculating anomalies for ", i, "/", nrow(file_combos), basename(save_file), "\n")

    baseline <- file_combos$baseline[i]
    baseline_name <- names(baselines)[baselines == baseline]
    data <- data.table(arrow::read_parquet(file_combos$data[i]))
    cat("  sec3.2 data cols:", paste(names(data), collapse=","), "\n")
    if (!"iso3" %in% names(data)) stop("CR-119 debug: iso3 missing from data BEFORE merge in sec 3.2")
    # Explicit by= so iso3 is a merge key.
    data <- merge(data, data_ex_hist[[baseline]],
                  by = c("iso3", "admin0_name", "admin1_name", "hazard", "season"),
                  all.x = TRUE)
    if (!"iso3" %in% names(data)) stop("CR-119 debug: iso3 missing from data AFTER merge in sec 3.2")
    data[, anomaly := value - baseline_value]
    data[, baseline_name := baseline_name]

    write_parquet_pushdown(
      data, save_file,
      sort_by         = c("iso3", "admin0_name", "admin1_name", "hazard", "scenario", "model", "timeframe", "season", "year"),
      verify_stats_on = c("iso3", "admin0_name", "hazard", "scenario")
    )
    # CR-119 debug: read back immediately to confirm iso3 survives round-trip
    .rb <- data.table(arrow::read_parquet(save_file))
    cat("  round-trip cols:", paste(names(.rb), collapse=","), "\n")
    if (!"iso3" %in% names(.rb)) stop("CR-119: iso3 LOST during write_parquet_pushdown round-trip")

    data_json <- jsonlite::read_json(file.path(output_dir, paste0(basename(file_combos$data[i]), ".json")), simplifyVector = TRUE)

    filters <- list(
      scenario = data[, unique(scenario)],
      timeframe = data[, unique(timeframe)],
      year = data[, unique(year)],
      hazard = data[, unique(hazard)],
      season = data[, unique(season)],
      model = data[, unique(model)]
    )

    field_descriptions <- list(
      admin0_name = "Name of the country (first-level administrative unit)",
      admin1_name = "Name of the subnational region (second-level administrative unit)",
      scenario = "Emissions scenario (e.g., SSP1-2.6, SSP3-7.0)",
      timeframe = "Future period being analyzed (e.g., 2030s, 2050s)",
      model = "General Circulation Model (GCM) identifier used to generate climate projections",
      year = "Calendar year of the data point",
      hazard = "Climate hazard variable (e.g., PTOT = precipitation total, TMAX = max temperature)",
      season = "3-month window or annual aggregation (e.g., DJF, MAM, annual)",
      value = paste0(
        "Monthly or seasonal hazard value summarized using ", extract_stat,
        " (e.g., average precipitation or max temperature)"
      ),
      baseline_value = "Historical mean value for the same location and season based on the selected baseline period",
      anomaly = "Difference between value and baseline_value, representing the climate anomaly",
      baseline_name = "Label for the baseline period used in anomaly calculations (e.g., 1995–2014)"
    )

    write_json(list(
      source = list(input_table = data_json$input_raster, extraction_rast = atlas_data$boundaries$name),
      extraction_method = "zonal",
      geo_filters = grep("admin", colnames(data), value = TRUE),
      season_type = "3-month windows or annual",
      filters = filters,
      format = ".parquet",
      date_created = Sys.time(),
      version = version1,
      parent_script = "R/2.1_create_monthly_haz_tables.R - section 3.3",
      value_variable = "value, baseline_value, anomaly",
      field_descriptions = field_descriptions,
      unit = unique(haz_meta[variable.code %in% data[, unique(hazard)], base_unit]),
      extract_stat = extract_stat,
      baseline = baseline_name,
      models = data[, paste0(sort(unique(model)), collapse = ",")],
      notes = paste0(
        "This file contains model-specific climate hazard data extracted for subnational administrative units (admin0_name, admin1_name), ",
        "organized by scenario, timeframe, hazard type, season, year, and GCM (model). ",
        "Monthly hazard values were first spatially summarized using the statistic '", extract_stat, "' (e.g., mean or sum), ",
        "then grouped into rolling 3-month or annual periods according to the 'season' column. ",
        "Anomaly values were calculated as the difference between each future value and the historical mean for the corresponding location and season, ",
        "based on the specified baseline period (baseline_name). ",
        "Each record retains the original GCM and year information, allowing temporal trend analysis and inter-model comparison. ",
        "This dataset has not been ensembled; all values reflect individual model behavior."
      )
    ), paste0(save_file, ".json"), pretty = TRUE)
  }
}))

cat("3.2) Adding historical means  - Complete \n")
} # end if (run_sec3_2)

## 3.3) Calculate ensembled statistics #####
cat("3.3) Calculating ensemble stats \n")
if (run_sec3_3) {
# CR-119: sec 3.3 runs SEQUENTIALLY. Parallel write collapsed save_file2
# when multiple file_combos resolved to the same output path (e.g.
# model dimension dropped in aggregation), producing TProtocolException
# on aggregate scans. Sequential is safe and single-digit minutes.
# Re-parallelise only after adding a stopifnot(unique(save_file2)) guard.
invisible(lapply(seq_len(nrow(file_combos)), FUN = function(i) {
  save_file <- file_combos$save_file[i]
  save_file2 <- file_combos$save_file2[i]
  save_file3 <- file_combos$save_file3[i]
  # Variables from sec 3.2's lapply closure don't persist here — recompute.
  baseline_name <- names(baselines)[baselines == file_combos$baseline[i]]
  data_json     <- jsonlite::read_json(
    file.path(output_dir, paste0(basename(file_combos$data[i]), ".json")),
    simplifyVector = TRUE)

  cat("3.3) Calculating ensemble stats for ", i, "/", nrow(file_combos), basename(save_file), "\n")

  if (!file.exists(save_file2) | overwrite2) {
    data_anomaly <- data.table(arrow::read_parquet(save_file))
    cat("  sec3.3 data_anomaly cols:", paste(names(data_anomaly), collapse=","), "\n")
    if (!"iso3" %in% names(data_anomaly)) stop(sprintf("CR-119: iso3 missing from data_anomaly in sec 3.3 (file: %s)", save_file))
    models <- data_anomaly[, paste0(sort(unique(model)), collapse = ",")]

    # Ensemble models by years.
    # CR-060: q5/q17/q50/q83/q95 added for IPCC AR6 calibrated-language
    # uncertainty bands. n_models tracks the per-year GCM count (some models
    # may drop out for specific years). Notebook's CR-061 swaps the ribbon
    # from sd_anomaly ± to q17_anomaly..q83_anomaly once this lands.
    data_anomaly_ens <- data_anomaly[, list(
      mean     = mean(value, na.rm = TRUE),
      max      = max(value, na.rm = TRUE),
      min      = min(value, na.rm = TRUE),
      sd       = sd(value, na.rm = TRUE),
      q5       = quantile(value, 0.05, na.rm = TRUE),
      q17      = quantile(value, 0.17, na.rm = TRUE),
      q50      = quantile(value, 0.50, na.rm = TRUE),
      q83      = quantile(value, 0.83, na.rm = TRUE),
      q95      = quantile(value, 0.95, na.rm = TRUE),
      n_models = sum(!is.na(value)),
      mean_anomaly = mean(anomaly, na.rm = TRUE),
      max_anomaly  = max(anomaly, na.rm = TRUE),
      min_anomaly  = min(anomaly, na.rm = TRUE),
      sd_anomaly   = sd(anomaly, na.rm = TRUE),
      q5_anomaly   = quantile(anomaly, 0.05, na.rm = TRUE),
      q17_anomaly  = quantile(anomaly, 0.17, na.rm = TRUE),
      q50_anomaly  = quantile(anomaly, 0.50, na.rm = TRUE),
      q83_anomaly  = quantile(anomaly, 0.83, na.rm = TRUE),
      q95_anomaly  = quantile(anomaly, 0.95, na.rm = TRUE)
    ),
    # CR-119 fix: iso3 must be in the by-clause or it is dropped from the schema.
    # (write_parquet_pushdown silently skips iso3 in sort_by when not present in tbl.)
    # Use character vector by= — bare name in list() evaluates in enclosing scope,
    # not the data.table frame, which fails if iso3 is defined elsewhere.
    by = c("iso3", "admin0_name", "admin1_name", "scenario", "timeframe", "year", "hazard", "season", "baseline_name")
    ]

    num_cols <- names(data_anomaly_ens)[sapply(data_anomaly_ens, is.numeric)]
    data_anomaly_ens[, (num_cols) := lapply(.SD, round, digits = round3.3), .SDcols = num_cols]

    data_anomaly_ens[, hazard := gsub("_mean|_max", "", hazard)]
    # CR-119 fix: do NOT replicate models as a per-row column (~250 bytes × millions of rows
    # = ~150-250 MB of bloat). Store in JSON sidecar / kv-metadata only.

    # Aggregate models over years then ensemble (iso3 must be in by-clause)
    data_ag <- data_anomaly[, list(
      mean = mean(value, na.rm = TRUE),
      mean_anomaly = mean(anomaly, na.rm = TRUE)
    ),
    by = c("iso3", "admin0_name", "admin1_name", "scenario", "timeframe", "model", "hazard", "season", "baseline_name")
    ]

    # CR-060: quantiles also on the period-aggregate ensemble (per-model
    # period means collapsed to ensemble distribution).
    data_ag_ens <- data_ag[, list(
      mean_mean    = mean(mean, na.rm = TRUE),
      min_mean     = min(mean, na.rm = TRUE),
      max_mean     = max(mean, na.rm = TRUE),
      median_mean  = median(mean, na.rm = TRUE),
      mean_anomaly = mean(mean_anomaly, na.rm = TRUE),
      max_anomaly  = max(mean_anomaly, na.rm = TRUE),
      min_anomaly  = min(mean_anomaly, na.rm = TRUE),
      sd_anomaly   = sd(mean_anomaly, na.rm = TRUE),
      q17_anomaly  = quantile(mean_anomaly, 0.17, na.rm = TRUE),
      q83_anomaly  = quantile(mean_anomaly, 0.83, na.rm = TRUE),
      n_models     = sum(!is.na(mean_anomaly))
    ),
    # CR-119 fix: iso3 in by-clause.
    by = c("iso3", "admin0_name", "admin1_name", "scenario", "timeframe", "hazard", "season", "baseline_name")
    ]
    # models stored in JSON sidecar only — not in data rows.

    num_cols <- names(data_ag_ens)[sapply(data_ag_ens, is.numeric)]
    data_ag_ens[, (num_cols) := lapply(.SD, round, digits = round_final), .SDcols = num_cols]

    if (!is.null(order_by2)) {
      setorderv(data_anomaly_ens, order_by2)
      setorderv(data_ag_ens, order_by2)
    }

    # CANONICAL — this file is published to S3 as
    # `variable=ensemble_season_timeseries.parquet` (after rename by the
    # AtlasDataManageR publisher) and drives the climateRationale
    # notebook's Future Projections section. Sort prefix matches the
    # 2026-05-27 dispatch's `[iso3, hazard, scenario, season, year,
    # admin1_name]` recommendation; local file uses admin0_name (iso3
    # is added downstream by the publisher) so the helper degrades
    # to admin0_name automatically via sort_cols_present intersect.
    write_parquet_pushdown(
      data_anomaly_ens, save_file2,
      sort_by         = c("iso3", "admin0_name", "hazard", "scenario", "season", "year", "timeframe", "admin1_name"),
      verify_stats_on = c("admin0_name", "hazard", "scenario", "season")
    )

    # filters must be re-defined here — sec 3.2's lapply closure doesn't
    # persist variables into sec 3.3's scope.
    filters <- list(
      scenario  = data_anomaly[, unique(scenario)],
      timeframe = data_anomaly[, unique(timeframe)],
      year      = data_anomaly[, unique(year)],
      hazard    = data_anomaly[, unique(hazard)],
      season    = data_anomaly[, unique(season)],
      model     = data_anomaly[, unique(model)]
    )
    filters$model <- NULL   # ensemble output — no per-model breakdown

    field_descriptions <- list(
      admin0_name = "Name of the country (first-level administrative unit)",
      admin1_name = "Name of the subnational region (second-level administrative unit)",
      scenario = "Emissions scenario (e.g., SSP1-2.6, SSP3-7.0)",
      timeframe = "Future period being analyzed (e.g., 2030s, 2050s)",
      year = "Calendar year of the data point",
      hazard = "Climate hazard variable (e.g., PTOT = precipitation total, TMAX = max temperature)",
      season = "3-month window or annual aggregation (e.g., DJF, MAM, annual)",
      baseline_name = "Label for the baseline period used in anomaly calculations (e.g., 1995–2014)",
      mean = paste0(
        "Mean of the hazard values across GCMs using ", extract_stat,
        " as the spatial summary method for each model"
      ),
      max = "Maximum hazard value across GCMs",
      min = "Minimum hazard value across GCMs",
      sd = "Standard deviation of hazard values across GCMs",
      mean_anomaly = "Mean anomaly across GCMs (difference from historical baseline)",
      max_anomaly = "Maximum anomaly across GCMs",
      min_anomaly = "Minimum anomaly across GCMs",
      sd_anomaly = "Standard deviation of anomalies across GCMs",
      models = "Comma-separated list of GCMs included in the ensemble"
    )

    write_json(list(
      source = list(input_table = data_json$input_raster, extraction_rast = atlas_data$boundaries$name),
      extraction_method = "zonal",
      geo_filters = grep("admin", colnames(data_anomaly_ens), value = TRUE),
      season_type = "3-month windows or annual",
      filters = filters,
      format = ".parquet",
      date_created = Sys.time(),
      version = version1,
      parent_script = "R/2.1_create_monthly_haz_tables.R - section 3.3",
      value_variable = "hazard mean, max, min, sd, mean_anomaly, max_anomaly, min_anomaly, sd_anomaly",
      field_descriptions = field_descriptions,
      unit = unique(haz_meta[variable.code %in% data_anomaly_ens[, unique(hazard)], base_unit]),
      extract_stat = extract_stat,
      baseline = baseline_name,
      models = models,
      notes = paste0(
        "This file contains ensembled summaries of monthly climate hazard values and their anomalies, ",
        "extracted for subnational administrative units (admin0_name, admin1_name) and grouped by scenario, timeframe, season, and hazard type. ",
        "Raw values were first summarized spatially using the statistic '", extract_stat, "' (e.g., mean or sum), ",
        "then aggregated into rolling 3-month or annual periods according to the 'season' column. ",
        "For each GCM (listed in the `models` column), anomaly values were computed relative to a specified baseline period. ",
        "The ensemble statistics include mean, min, max, and standard deviation (SD) across models, reported separately for both raw hazard values ",
        "and their anomalies. This provides a robust indication of central tendency and inter-model spread, which is critical for quantifying agreement ",
        "and uncertainty across climate projections."
      )
    ), paste0(save_file2, ".json"), pretty = TRUE)


    write_parquet_pushdown(
      data_ag_ens, save_file3,
      sort_by         = c("iso3", "admin0_name", "admin1_name", "hazard", "scenario", "timeframe", "season"),
      verify_stats_on = c("admin0_name", "hazard", "scenario")
    )

    filters$year <- NULL

    field_descriptions <- list(
      admin0_name     = "Name of the country (first-level administrative unit)",
      admin1_name     = "Name of the subnational region (second-level administrative unit)",
      scenario        = "Emissions scenario (e.g., SSP1-2.6, SSP3-7.0)",
      timeframe       = "Future period being analyzed (e.g., 2030s, 2050s)",
      hazard          = "Climate hazard variable (e.g., PTOT = precipitation total, TMAX = max temperature)",
      season          = "3-month window or annual aggregation (e.g., DJF, MAM, annual)",
      baseline_name   = "Label for the baseline period used in anomaly calculations (e.g., 1995–2014)",
      mean_mean       = paste0("Mean of yearly-averaged hazard values across all GCMs, where each model’s values were spatially summarized using ", extract_stat),
      min_mean        = "Minimum of the yearly-averaged hazard values across models",
      max_mean        = "Maximum of the yearly-averaged hazard values across models",
      median_mean     = "Median of the yearly-averaged hazard values across models",
      mean_anomaly    = "Mean of yearly-averaged anomalies (relative to baseline) across models",
      max_anomaly     = "Maximum of yearly-averaged anomalies across models",
      min_anomaly     = "Minimum of yearly-averaged anomalies across models",
      sd_anomaly      = "Standard deviation of yearly-averaged anomalies across models",
      models          = "Comma-separated list of GCMs included in the ensemble"
    )

    write_json(list(
      source = list(input_table = data_json$input_raster, extraction_rast = atlas_data$boundaries$name),
      extraction_method = "zonal",
      geo_filters = grep("admin", colnames(data_anomaly), value = TRUE),
      season_type = "3-month windows or annual",
      filters = filters,
      format = ".parquet",
      date_created = Sys.time(),
      version = version1,
      parent_script = "R/2.1_create_monthly_haz_tables.R - section 3.3",
      value_variable = "hazard mean, max, min, sd, mean_anomaly, max_anomaly, min_anomaly, sd_anomaly",
      field_descriptions = field_descriptions,
      unit = haz_meta[variable.code %in% data_anomaly[, unique(hazard)], base_unit],
      extract_stat = extract_stat,
      anomaly_baseline = baseline_name,
      models = models,
      notes = "This file presents ensemble summary statistics for climate hazard indicators and their anomalies, aggregated by subnational administrative units (admin0_name, admin1_name), scenario, timeframe, hazard, and season. Monthly hazard values were extracted using the selected spatial summary method (e.g., mean or sum) and grouped into rolling 3-month or annual periods based on the ‘season’ column. The resulting values and anomalies (relative to a historical baseline) were then averaged across all years within the specified timeframe for each GCM. These multi-year averages were used to calculate ensemble statistics across models (listed in the ‘models’ column), including the mean, min, max, and median for values, and mean, min, max, and standard deviation for anomalies. The file is designed to support high-level climate risk analysis, scenario comparison, and adaptation planning."
    ), paste0(save_file3, ".json"), pretty = TRUE)
  }
}))

cat("3.3) Calculating ensemble stats - Complete \n")
} # end if (run_sec3_3)

## 3.4) Calculate trends #####
# Controlled by run_sec3_4 (set via SKIP_R2_1_SEC3_4=1 or legacy SKIP_R2_1_3_4=1).
# CR-094 TFPW is in place (2026-06-01). ~9h per timeframe.
if (run_sec3_4) {

# CR-094: Yue et al. (2002) Trend-Free Pre-Whitening (TFPW).
# Applied per-GCM before Theil-Sen + Mann-Kendall so autocorrelated
# outputs (common in CMIP6 temperature series) don't inflate MK
# significance. Matches helpers/trend.ojs's corrected algorithm
# (Pete fixed a buggy formulation pre-commit; this R port was validated
# numerically against 05_trend-validation-reference.py — 4/4 PASS,
# zero diff on slope/AC/p for all four test series including the
# critical Series D case where the buggy version gives p=0.23 instead
# of the correct p=0.002).
#
# Algorithm (correct Yue 2002, NOT the buggy whitening-of-observed variant):
#   1. Theil-Sen slope + intercept on raw value
#   2. Detrend: detr = value - (slope * year + intercept)
#   3. Lag-1 AC of detrended residuals (biased estimator)
#   4. If |r| <= 0.1: skip (series is not autocorrelated enough)
#   5. Pre-whiten detrended residuals: wr[t] = detr[t] - r * detr[t-1]
#      (depends on detr[t-1], NOT wr[t-1], so fully vectorised in R)
#   6. Re-add deterministic trend: z = wr + slope*year + intercept
#   7. Theil-Sen + MK on z

yue_tfpw <- function(year, value, threshold = 0.1) {
  n <- length(value)
  if (n < 4L || !all(is.finite(value))) return(list(y = value, applied = FALSE, r = NA_real_))
  ts0 <- tryCatch(trend::sens.slope(value), error = function(e) NULL)
  if (is.null(ts0)) return(list(y = value, applied = FALSE, r = NA_real_))
  slope0     <- unname(ts0$estimates)
  intercept0 <- median(value - slope0 * year, na.rm = TRUE)
  detr       <- value - (slope0 * year + intercept0)
  d          <- detr - mean(detr, na.rm = TRUE)
  denom      <- sum(d * d, na.rm = TRUE)
  r          <- if (denom > 0) sum(d[-n] * d[-1L], na.rm = TRUE) / denom else 0.0
  # Speedup #2: return ts0 so caller can reuse it when TFPW not applied,
  # avoiding a second O(n²) sens.slope() call on the same series.
  if (abs(r) <= threshold) return(list(y = value, applied = FALSE, r = r, ts0 = ts0))
  wr <- c(detr[1L], detr[-1L] - r * detr[-n])
  list(y = wr + slope0 * year + intercept0, applied = TRUE, r = r, ts0 = NULL)
}

# Speedup #3 (Rcpp single-pass Theil–Sen + Mann–Kendall kernel).
# trend::sens.slope() (slope+CI) and trend::mk.test() (p) each do an independent
# O(n²) Kendall pass with heavy R-call/object overhead. mk_sen_cpp() does both in
# ONE pairwise pass — ~63× faster per group (230µs → 3.6µs at n=24), validated
# numerically IDENTICAL to trend 1.1.6 (R/probe_trend_kernel_identity.R: max diff
# 1e-16; R/probe_trend_kernel_yue_identity.R full TFPW path: max 2e-13, < round3.4).
# Compiled per process into .kernel_env; multisession workers each load from the
# shared Rcpp cache (populated once in main below). Falls back to the trend:: path
# if compilation fails or R21_DISABLE_TREND_KERNEL=1, so the section is never blocked.
kernel_cpp   <- file.path(Sys.getenv("project_dir"), "R", "trend_kernel.cpp")
kernel_cache <- file.path(Sys.getenv("project_dir"), "R", ".rcpp_cache")
.kernel_env  <- new.env(parent = baseenv())  # baseenv (not emptyenv): sourceCpp loader needs base fns
.ensure_kernel <- function() {
  if (is.null(.kernel_env$mk_sen_cpp)) {
    suppressMessages(Rcpp::sourceCpp(kernel_cpp, cacheDir = kernel_cache, env = .kernel_env))
  }
  invisible(NULL)
}

# Kernel equivalent of yue_tfpw + outer Sen/MK block. Returns the baseline-INVARIANT
# fit only (no intercept — that is recomputed per baseline downstream). Mirrors the
# trend:: math exactly: index-based Sen slope, year-based detrend intercept0, TFPW.
fit_value_kernel <- function(year, value) {
  n <- length(value)
  if (n < 4L || !all(is.finite(value)))
    return(list(slope = NA_real_, ci_low = NA_real_, ci_high = NA_real_,
                p_value = NA_real_, tfpw_applied = FALSE, lag1_ac = NA_real_))
  ts0        <- .kernel_env$mk_sen_cpp(value)
  slope0     <- ts0$slope
  intercept0 <- median(value - slope0 * year, na.rm = TRUE)
  detr       <- value - (slope0 * year + intercept0)
  r          <- .kernel_env$lag1_ac_cpp(detr)
  if (abs(r) <= 0.1)  # TFPW not applied — reuse ts0 (Speedup #2, now kernel-native)
    return(list(slope = ts0$slope, ci_low = ts0$ci_low, ci_high = ts0$ci_high,
                p_value = ts0$p_value, tfpw_applied = FALSE, lag1_ac = r))
  wr  <- c(detr[1L], detr[-1L] - r * detr[-n])
  z   <- wr + slope0 * year + intercept0
  tsz <- .kernel_env$mk_sen_cpp(z)
  list(slope = tsz$slope, ci_low = tsz$ci_low, ci_high = tsz$ci_high,
       p_value = tsz$p_value, tfpw_applied = TRUE, lag1_ac = r)
}

# Compile once in the main process so workers hit the Rcpp cache (no parallel-compile
# race). Compile into a THROWAWAY env, never the global .kernel_env: a populated
# .kernel_env holds a DLL external pointer that future would serialise to workers as a
# dead pointer. Keeping the global empty makes each worker .ensure_kernel() load fresh
# from the shared on-disk cache (cache hit → dyn.load only, no recompile).
USE_TREND_KERNEL <- FALSE
if (Sys.getenv("R21_DISABLE_TREND_KERNEL") != "1") {
  dir.create(kernel_cache, showWarnings = FALSE, recursive = TRUE)
  USE_TREND_KERNEL <- tryCatch({
    .probe_env <- new.env(parent = baseenv())
    suppressMessages(Rcpp::sourceCpp(kernel_cpp, cacheDir = kernel_cache, env = .probe_env))
    is.function(.probe_env$mk_sen_cpp)
  }, error = function(e) { message("§3.4 trend kernel compile failed — using trend:: fallback: ", conditionMessage(e)); FALSE })
}

# This involves running >10^6 linear models to look at trends, so the process is designed to run in parallel
cat(sprintf("3.4) Trend calculation (with Yue 2002 TFPW pre-whitening) — START %s UTC\n",
            format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC")))
cat(sprintf("3.4) trend kernel: %s\n",
            if (USE_TREND_KERNEL) "ENABLED (Rcpp single-pass, ~63x/fit)" else "DISABLED (trend:: fallback)"))
t_sec3_4_start <- Sys.time()
# sec 3.4 runs >10^6 linear models per file_combo — more memory-intensive
# than 3.3. mem_per_worker_gb=8 is conservative for the trend data.table ops.
#
# Speedup #1 (baseline-invariant fit dedup): the Theil–Sen + MK fit acts on
# `value`+`year`, which are IDENTICAL across baselines for the same source `data`
# file. Only the intercept (median(baseline_value - slope*year)) and the downstream
# anomaly_* stats are baseline-dependent. So we parallelise over the distinct source
# `data` file and loop its baselines INSIDE one worker, computing the expensive
# value-fit ONCE and reusing it for every baseline. Cuts §3.4 fit cost by ~#baselines.
#
# Parallel-write safety (CR-119 lesson): each worker owns one source, hence ALL of
# that source's per-baseline output paths and NO other worker's. Output paths are
# unique across the whole file_combos table (guarded below), so no two workers ever
# write the same file/object.
stopifnot(
  !anyDuplicated(file_combos$save_file),
  !anyDuplicated(file_combos$save_file2),
  !anyDuplicated(file_combos$save_file3)
)
source_groups <- split(seq_len(nrow(file_combos)), file_combos$data)

n_workers_3_4 <- safe_workers(worker_n2, n_tasks = length(source_groups), mem_per_worker_gb = 8)
set_parallel_plan(n_cores = n_workers_3_4, use_multisession = TRUE)
invisible(future.apply::future_lapply(seq_along(source_groups), FUN = function(gi) {
  combo_idx <- source_groups[[gi]]
  value_fit <- NULL  # baseline-invariant Theil–Sen/MK fit, computed once per source

  for (i in combo_idx) {
  data_file     <- file_combos$save_file[i]
  baseline_name <- names(baselines)[baselines == file_combos$baseline[i]]

  file_base <- gsub("_seasons", "", data_file)
  save_file <- gsub(".parquet", "_trends.parquet", file_base)
  save_file2 <- gsub(".parquet", "_trends_ensemble.parquet", file_base)
  save_file3 <- gsub(".parquet", "_trends_ensemble_minimal.parquet", file_base)

  cat(sprintf("3.4) [%s UTC] Processing combo %d/%d (source group %d/%d) %s\n",
              format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC"),
              i, nrow(file_combos), gi, length(source_groups), basename(data_file)))
  t_combo_start <- Sys.time()

  if (!file.exists(save_file) | overwrite2) {
    data_ex_trend <- data.table(arrow::read_parquet(data_file))

    # Filter out rows with NA/NaN/Inf in 'value' or 'year' before fitting the model
    data_ex_trend <- data_ex_trend[is.finite(value) & is.finite(year)][, n_value := NULL]

    fit_keys <- c("admin0_name", "admin1_name", "scenario", "timeframe", "model", "hazard", "season")

    ## 3.4.1) Calculate Theil–Sen estimator with TFPW ####
    # Speedup #1: value-fit (slope/ci/p/tfpw/lag1_ac) is baseline-invariant — compute
    # once for this source, reuse for every baseline. intercept is intentionally NOT
    # here (baseline-dependent) and the fit key intentionally excludes baseline_name.
    if (is.null(value_fit)) {
      if (USE_TREND_KERNEL) {
        # Speedup #3: single-pass Rcpp kernel (load into this worker from shared cache).
        .ensure_kernel()
        value_fit <- data_ex_trend[, fit_value_kernel(year, value), by = fit_keys]
      } else {
        value_fit <- data_ex_trend[
          ,
          {
            pw  <- yue_tfpw(year, value)
            yw  <- pw$y   # pre-whitened series (or raw if TFPW not applied)
            # Speedup #2: reuse ts0 computed inside yue_tfpw when TFPW not applied
            # — avoids a second O(n²) sens.slope() on the same raw series.
            ts  <- if (!pw$applied && !is.null(pw$ts0)) pw$ts0 else
                   tryCatch(sens.slope(yw), error = function(e) NULL)
            if (is.null(ts)) {
              list(
                slope = NA_real_,
                ci_low = NA_real_, ci_high = NA_real_, p_value = NA_real_,
                tfpw_applied = FALSE, lag1_ac = pw$r
              )
            } else {
              list(
                slope    = unname(ts$estimates),
                ci_low   = ts$conf.int[1],
                ci_high  = ts$conf.int[2],
                p_value  = tryCatch(mk.test(yw)$p.value, error = function(e) NA_real_),
                tfpw_applied = pw$applied,
                lag1_ac  = pw$r
              )
            }
          },
          by = fit_keys
        ]
      }
    }

    # Baseline-dependent intercept = median(baseline_value - slope*year) per group,
    # using THIS baseline's baseline_value and the cached slope. .EACHI evaluates j
    # over the data_ex_trend rows matching each value_fit group — identical row set
    # and identical median to the per-baseline fit, since value/year/keys are shared.
    trend_summary <- data_ex_trend[value_fit, on = fit_keys,
      .(
        slope        = i.slope,
        intercept    = median(baseline_value - i.slope * year),
        ci_low       = i.ci_low,
        ci_high      = i.ci_high,
        p_value      = i.p_value,
        tfpw_applied = i.tfpw_applied,
        lag1_ac      = i.lag1_ac
      ),
      by = .EACHI
    ]
    trend_summary[, baseline_name := baseline_name]

    data_ex_trend_m <- merge(data_ex_trend, trend_summary,
      by = c("admin0_name", "admin1_name", "scenario", "timeframe", "model", "hazard", "season", "baseline_name"),
      all.x = TRUE, sort = FALSE
    )

    ### 3.4.2) Calculate trend stats #####
    data_ex_trend_stats <- data_ex_trend_m[, .(
      value_slope = slope[1],
      value_start = min(year) * slope[1] + intercept[1],
      value_s5 = mean(value[1:5]),
      anomaly_s5 = mean(anomaly[1:5]),
      value_end = max(year) * slope[1] + intercept[1],
      value_e5 = mean(tail(value, 5)),
      anomaly_e5 = mean(tail(anomaly, 5)),
      value_decade = 10 * slope,
      value_pval = p_value[1]
    ),
    # CR-119: iso3 in by-clause so the trends canonical keeps it (else dropped here,
    # same silent-drop as §3.3). iso3 is 1:1 with admin0_name, carried from data_ex_trend.
    by = .(iso3, admin0_name, admin1_name, scenario, model, timeframe, hazard, season, baseline_name)
    ][, value_diff := value_e5 - value_s5][, anomaly_diff := anomaly_e5 - anomaly_s5]

    # Create dataset for ensembling, before any rounding occurs
    data_ex_trend_stats_ens <- melt(data_ex_trend_stats,
      id.vals = c("admin0_name", "admin1_name", "scenario", "model", "timeframe", "variable", "season", "baseline_name"),
      variable.name = "stat"
    )

    num_cols <- names(data_ex_trend_stats)[sapply(data_ex_trend_stats, is.numeric)]
    data_ex_trend_stats[, (num_cols) := lapply(.SD, round, digits = round3.4), .SDcols = num_cols]

    if (!is.null(order_by2)) {
      setorderv(data_ex_trend_stats, order_by2)
    }

    # Save result
    write_parquet_pushdown(
      data_ex_trend_stats, save_file,
      sort_by         = c("iso3", "admin0_name", "admin1_name", "hazard", "scenario", "model", "timeframe", "season"),
      verify_stats_on = c("admin0_name", "hazard", "scenario")
    )

    filters <- list(
      scenario = data_ex_trend_stats[, unique(scenario)],
      timeframe = data_ex_trend_stats[, unique(timeframe)],
      model = data_ex_trend_stats[, unique(model)],
      hazard = data_ex_trend_stats[, unique(hazard)],
      season = data_ex_trend_stats[, unique(season)]
    )

    field_descriptions <- list(
      admin0_name   = "Name of the country (first-level administrative unit)",
      admin1_name   = "Name of the subnational region (second-level administrative unit)",
      scenario      = "Emissions scenario (e.g., SSP1-2.6, SSP3-7.0)",
      model         = "Name of the General Circulation Model (GCM) used for climate projection",
      timeframe     = "Future period being analyzed (e.g., 2030s, 2050s)",
      hazard        = "Climate hazard variable (e.g., PTOT = precipitation total, TMAX = max temperature)",
      season        = "3-month rolling window or annual aggregation (e.g., DJF, MAM, annual)",
      baseline_name = "Label for the baseline period used to compute anomalies (e.g., 1995–2014)",
      value_slope   = "Sen's slope estimate of the linear trend in the `value` variable over time",
      value_start   = "Estimated `value` at the starting year of the time series using slope and intercept",
      value_s5      = "Mean of the first 5 `value` entries in the time series",
      anomaly_s5    = "Mean of the first 5 `anomaly` entries in the time series",
      value_end     = "Estimated `value` at the final year of the time series using slope and intercept",
      value_e5      = "Mean of the last 5 `value` entries in the time series",
      anomaly_e5    = "Mean of the last 5 `anomaly` entries in the time series",
      value_decade  = "Change in the `value` variable over a 10-year period (slope × 10)",
      value_pval    = "P-value from Mann-Kendall test assessing the significance of the trend in `value`",
      value_diff    = "Difference between the 5-year end and start means for `value`",
      anomaly_diff  = "Difference between the 5-year end and start means for `anomaly`"
    )

    write_json(list(
      source = list(input_raster = indices_dir, extraction_rast = atlas_data$boundaries$name),
      extraction_method = "zonal",
      geo_filters = grep("admin", colnames(data_ex_trend_stats), value = TRUE),
      season_type = "3-month windows or annual",
      filters = filters,
      value_var = field_descriptions,
      format = ".parquet",
      date_created = Sys.time(),
      version = version1,
      parent_script = "R/2.1_create_monthly_haz_tables.R - section 3.4",
      unit = unique(haz_meta[variable.code %in% data_ex_trend_stats[, unique(hazard)], .(variable.code, base_unit)]),
      extract_stat = extract_stat,
      anomaly_baseline = baseline_name,
      notes = paste0(
        "This file contains climate hazard summary statistics extracted from monthly raster data, ",
        "aggregated by subnational administrative units (admin0_name, admin1_name). Values were first ",
        "summarized using mean across spatial units, then grouped into rolling 3-month or 12-month periods ",
        "depending on the 'season' column. The summary metric (mean or sum) applied to the seasonal value depends on the hazard type ",
        "as defined in the hazard metadata. For each group of GCMs (models column), ensemble statistics (mean, min, max, SD) ",
        "were calculated for both the raw hazard value and its anomaly (deviation from a historical baseline period). ",
        "Temporal trends were assessed using Sen’s slope estimator, a non-parametric method robust to outliers and missing data, ",
        "with significance evaluated using the Mann–Kendall trend test (p-value column). The table supports climate trend analysis, ",
        "risk monitoring, and adaptation planning by season, region, and scenario."
      )
    ), paste0(save_file, ".json"), pretty = TRUE)


    # 3.7.1) Ensemble trend stats ######

    data_ex_trend_stats_ens <- melt(data_ex_trend_stats,
      id.vals = c("admin0_name", "admin1_name", "scenario", "model", "timeframe", "variable", "season"),
      variable.name = "stat"
    )

    data_ex_trend_stats_ens <- data_ex_trend_stats_ens[, list(
      mean = mean(value, na.rm = TRUE),
      max = max(value, na.rm = TRUE),
      min = min(value, na.rm = TRUE),
      sd = sd(value, na.rm = TRUE)
    ),
    # CR-119: iso3 in by-clause so the trends-ensemble canonical keeps it.
    by = list(iso3, admin0_name, admin1_name, scenario, timeframe, season, hazard, stat)
    ]

    data_ex_trend_stats_ens[, stat := as.character(stat)]

    if (!is.null(order_by2)) {
      setorderv(data_ex_trend_stats_ens, order_by2)
    }

    write_parquet_pushdown(
      data_ex_trend_stats_ens, save_file2,
      sort_by         = c("iso3", "admin0_name", "admin1_name", "hazard", "scenario", "timeframe", "season", "stat"),
      verify_stats_on = c("admin0_name", "hazard")
    )

    field_descriptions$model <- NULL

    write_json(list(
      source = list(
        input_raster = indices_dir,
        extraction_rast = atlas_data$boundaries$name
      ),
      extraction_method = "zonal",
      geo_filters = grep("admin", colnames(data_ex_trend_stats_ens), value = TRUE),
      season_type = "3-month windows or annual",
      filters = filters,
      value_var = field_descriptions,
      format = ".parquet",
      date_created = Sys.time(),
      version = version1,
      parent_script = "R/2.1_create_monthly_haz_tables.R - section 3.7.1",
      unit = unique(haz_meta[variable.code %in% data_ex_trend_stats_ens[, unique(hazard)], .(variable.code, base_unit)]),
      extract_stat = extract_stat,
      anomaly_baseline = baseline_name,
      notes = paste0(
        "This file contains ensemble-level summaries of trend statistics derived from seasonal hazard values, ",
        "aggregated by subnational administrative units. Each row corresponds to a single trend metric (e.g., Sen's slope, decadal change) ",
        "calculated across multiple GCM models for a given scenario, timeframe, season, and hazard type. ",
        "The 'stat' column indicates the specific trend metric summarized, while the 'mean', 'min', 'max', and 'sd' columns report ensemble ",
        "statistics across GCMs. Trend slopes were estimated using Sen's slope method, a robust non-parametric estimator. ",
        "The Mann–Kendall trend test was used to assess significance. These summaries support regional assessments of ",
        "climate hazard evolution and are suitable for visualizing uncertainty ranges across climate models."
      )
    ), paste0(save_file2, ".json"), pretty = TRUE)


    data_ex_trend_stats_ens_simple <- data_ex_trend_stats_ens[hazard %in% c("PTOT", "TAVG", "TMAX") & stat %in% c("value_diff", "value_decade", "anomaly_diff")]

    write_parquet_pushdown(
      data_ex_trend_stats_ens_simple, save_file3,
      sort_by         = c("iso3", "admin0_name", "admin1_name", "hazard", "scenario", "season", "stat"),
      verify_stats_on = c("admin0_name", "hazard")
    )

    filters <- list(
      scenario = data_ex_trend_stats_ens_simple[, unique(scenario)],
      timeframe = data_ex_trend_stats_ens_simple[, unique(timeframe)],
      hazard = data_ex_trend_stats_ens_simple[, unique(hazard)],
      season = data_ex_trend_stats_ens_simple[, unique(season)],
      stat = data_ex_trend_stats_ens_simple[, unique(stat)]
    )

    field_descriptions_simple <- list(
      admin0_name   = "Name of the country (first-level administrative unit)",
      admin1_name   = "Name of the subnational region (second-level administrative unit)",
      scenario      = "Emissions scenario (e.g., SSP1-2.6, SSP3-7.0)",
      timeframe     = "Future period being analyzed (e.g., 2030s, 2050s)",
      hazard        = "Climate hazard variable (e.g., PTOT = precipitation total, TMAX = max temperature)",
      season        = "3-month rolling window or annual aggregation (e.g., DJF, MAM, annual)",
      baseline_name = "Label for the baseline period used to compute anomalies (e.g., 1995–2014)",
      value_diff    = "Difference between end and start 5-year means for the raw hazard values (e.g., TAVG, PTOT, TMAX).",
      value_decade  = "Estimated change in the seasonal hazard value over a 10-year period, based on Sen’s slope.",
      anomaly_diff  = "Difference between end and start 5-year means for the seasonal anomalies relative to historical baseline."
    )

    write_json(list(
      source = list(input_raster = indices_dir, extraction_rast = atlas_data$boundaries$name),
      extraction_method = "zonal",
      geo_filters = grep("admin", colnames(data_ex_trend_stats_ens_simple), value = TRUE),
      season_type = "3-month windows or annual",
      filters = filters,
      value_var = field_descriptions_simple,
      format = ".parquet",
      date_created = Sys.time(),
      version = version1,
      parent_script = "R/2.1_create_monthly_haz_tables.R - section 3.7.1",
      unit = unique(haz_meta[variable.code %in% data_ex_trend_stats_ens_simple[, unique(hazard)], .(variable.code, base_unit)]),
      extract_stat = extract_stat,
      anomaly_baseline = baseline_name,
      notes = paste0(
        "This simplified file contains a filtered subset of ensemble-level climate trend summaries for key hazards ",
        "(precipitation total [PTOT], average temperature [TAVG], and maximum temperature [TMAX]). ",
        "It includes only three critical trend metrics—value_diff, value_decade, and anomaly_diff—sufficient for many use cases such as regional trend mapping, ",
        "climate impact summaries, and adaptation planning dashboards. These were computed across GCM ensembles and summarized using ",
        "mean, min, max, and standard deviation (SD) to express model spread. The values are provided for each season, scenario, and region, ",
        "enabling spatial and temporal comparison of hazard trends under different future climates."
      )
    ), paste0(save_file3, ".json"), pretty = TRUE)
  }
  cat(sprintf("3.4) [%s UTC] Done combo %d/%d in %.1f min %s\n",
              format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC"),
              i, nrow(file_combos),
              as.numeric(difftime(Sys.time(), t_combo_start, units = "mins")),
              basename(data_file)))
  }  # end for (i in combo_idx) — baselines of one source share the cached value_fit
}))

plan(sequential)
cat(sprintf("3.4) Trend calculations - Complete — END %s UTC (section took %.1f min)\n",
            format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC"),
            as.numeric(difftime(Sys.time(), t_sec3_4_start, units = "mins"))))
} # end if (run_sec3_4)

cat(sprintf("\n===== 2.1_create_monthly_haz_tables.R COMPLETE at %s UTC =====\n",
            format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
