# 0) Introduction ####
# This script is designed to summarize and process monthly climate hazard time series data. 
# It assumes that 0_server_setup.R has already been executed to configure the necessary R environment.
# 
# Key Purposes:
# - Identify, filter, and organize hazard data files (e.g., *.tif) generated from the hazards repository.
# - Prepare metadata and define calendar parameters (start and end of seasons) to support both annual 
#   and seasonal aggregation of hazards.
# - Calculate hazard statistics (e.g., mean, SD) across different scenarios, models, and time periods, 
#   including the creation of ensemble layers.
# 
# Key Processes:
# 1. Listing relevant hazard data files and organizing them into a data.table structure.
# 2. Filtering and grouping data by hazard types, scenarios, years, and months.
# 3. Using climate and crop calendars (from GGCMI, or user-defined season boundaries via an onset-of-rain
#    approach) to focus on meaningful time windows.
# 4. Employing parallel processing (future.apply) along with progress indicators (progressr) to efficiently
#    compute seasonal or yearly hazard metrics in bulk.
# 5. Aggregating individual model outputs into ensemble means and standard deviations.
# 
# Key Input Data:
# - Monthly *.tif hazard layers (e.g., TMAX, TMIN, THI, PTOT) stored under the 'working_dir' directory. 
#   These hazard layers are the output of the https://github.com/AdaptationAtlas/hazards pipeline.
# - A base raster for spatial alignment.
# - Crop calendar rasters (GGCMI or from an onset-of-rain dataset) for seasonal boundaries.
# 
# Key Output Data:
# - Hazard indicator files aggregated on annual and/or seasonal bases.
# - Ensemble statistics (e.g., mean, SD) by scenario, model, time, and hazard variable.
# - Summary data tables (e.g., hazard completeness checks) for downstream use or validation.
# 
# Once this script completes, you will have a set of processed hazard layers (organized under 'output_dir') 
# that can be used for further analysis, visualization, or integration into other workflows.
#
# Please run 0_server_setup.R before executing this script

cat("Started Script 1 - Time series extraction of hazards\n")

# 1) Setup #### 
  ## 1.1) Load R functions & packages ####
# Using pacman::p_load for convenient loading of multiple packages, including data.table and progressr.
pacman::p_load(
  terra,
  data.table,
  future,
  fs,
  future.apply,
  progressr
)

  ## 1.2) Set directories ####
# Directory where monthly timeseries data generated from 
# https://github.com/AdaptationAtlas/hazards/tree/main is stored.
# Note this is currently only available on cglabs, but we will be adding the ability 
# to download these data from the s3.
working_dir <- indices_dir

# Where hazard time series outputs will be saved
output_dir <- indices_dir2

  ## 1.3) Load base rast ####
base_rast<-terra::rast(base_rast_path)
# 2) Summarize existing data #####
if(F){
  folders <- list.dirs(working_dir, recursive = FALSE)
  
# Plan for parallelization. Adjust 'workers' to suit your machine
  set_parallel_plan(n_cores=10,use_multisession=T)
  
# Wrap the future_lapply call in a 'with_progress' block
existing_files_list <- with_progress({
  # Initialize progressor with the number of steps (length of 'folders')
  p <- progressr::progressor(steps = length(folders))
  
  future_lapply(seq_along(folders), function(i) {
    # Update progress bar for each folder
    p(sprintf("Processing folder %d of %d: %s", i, length(folders), folders[i]))
    
    # List .tif files in the current folder
    fs::dir_ls(path = folders[i], glob = "*tif", recurse = TRUE)
  })
})

  plan(sequential)

# Combine into a single vector
existing_files <- unlist(existing_files_list, use.names = FALSE)

# Filter out ipynb_checkpoints
existing_files <- existing_files[!grepl("ipynb_checkpoints", existing_files)]

# Remove unwanted patterns from the file list
existing_files <- existing_files[
  !grepl("PRCPTOT-PTOT|PTOT-New-PTOT|PTOT-Updated-PTOT|GSeason|THI/THI_MAX/|_AgERA5/|stats|daily|historical/HSM_NTx|AVAIL.tif|LongTermMean|/max_year|/mean_monthly|/mean_year|/median_monthly|/median_year",
         existing_files)
]

# Convert to a data.table for easier manipulation
existing_files <- data.table(file_path = existing_files)

# Create columns for the folder (variable), scenario, var_folder, and base_name
existing_files <- existing_files[, variable := dirname(file_path)
][, scenario := basename(dirname(variable[1])), by = variable
][, var_folder := basename(variable)
][, base_name := gsub(".tif", "", basename(file_path))]

# Quick example check for TAI in base_name
existing_files[grep("TAI", base_name)]

# (Optional diagnostic) If you need to verify unusual or problematic filenames:
if (FALSE) {
  base_names <- existing_files[, unique(base_name)]
  for (i in seq_along(base_names)) {
    cat('\r', strrep(' ', 150), '\r')
    cat("processing file", i, "/", length(base_names), base_names[i])
    flush.console()
    if (!grepl("TAI", base_names[i])) {
      tstrsplit(base_names[i], "-", keep = 3)
    }
  }
}

# Further split the base_name to get var_file, year, and month
existing_files[, var_file := unlist(tstrsplit(base_name[1], "-", keep = 1)), by = base_name
][, year := unlist(tstrsplit(base_name[1], "-", keep = 2)), by = base_name
][!grepl("TAI", base_name), month := unlist(tstrsplit(base_name[1], "-", keep = 3)), by = base_name]

# Summarize the number of files by scenario, var_folder, var_file
existing_files_summary <- existing_files[
  !grepl("AgERA5", var_folder),
  .(n_files = .N, years = length(unique(year))),
  by = .(scenario, var_folder, var_file)
]
# Add a combined variable code
existing_files_summary[, var_code := paste0(var_folder, "-", var_file)]

# Wide-format table of hazard file counts
hazard_completion <- dcast(
  data = existing_files_summary,
  formula = scenario ~ var_code,
  value.var = "n_files"
)

hazard_completion[,c("PRCPTOT-PTOT","PTOT-New-PTOT","PTOT-Updated-PTOT"):=NULL]

# Save hazard completeness info to CSV for reference
fwrite(
  hazard_completion,
  file.path(working_dir,paste0("indice_completion_",Sys.time(),".csv"))
)


hazard_completion[,c("scenario","model"):=tstrsplit(scenario,"_")]

dcast(hazard_completion[,.(model,scenario,`NDD-NDD`)],model~scenario)
dcast(hazard_completion[,.(model,scenario,`PTOT-PTOT`)],model~scenario)
dcast(hazard_completion[,.(model,scenario,`TAI-TAI`)],model~scenario)
dcast(hazard_completion[,.(model,scenario,`TMAX-TMAX`)],model~scenario)
dcast(hazard_completion[,.(model,scenario,`TMIN-TMIN`)],model~scenario)
dcast(hazard_completion[,.(model,scenario,`NDWL0-NDWL0`)],model~scenario)
dcast(hazard_completion[,.(model,scenario,`NDWS-NDWS`)],model~scenario)
}

  ## 2.1) Check file integrity ####
  # This section verifies that each tif file can be loaded using terra::rast.
  # It runs in parallel with a progress bar and uses tryCatch to record any files that 
  # cannot be loaded. Set 'delete_corrupt' to TRUE to automatically delete such files.
  if(F){
  delete_corrupt <- FALSE  # Change to TRUE to delete problematic files
  
  # Gather all files from historical and future directories
  files <- list.files(
    working_dir, 
    pattern = ".tif$", 
    recursive = TRUE, 
    full.names = TRUE
  )
  
  set_parallel_plan(n_cores=16,use_multisession=F)
  handlers("progress")
  
  with_progress({
    p <- progressor(along = files)
    
    results <- furrr::future_map_dfr(files, function(f) {
      res <- tryCatch({
        # Try to load the raster
        r <- terra::rast(f)
        tibble::tibble(file = f, success = TRUE, error_message = NA_character_)
      }, error = function(e) {
        tibble::tibble(file = f, success = FALSE, error_message = as.character(e))
      })
      p()
      res
    })
    
    # Report results
    failed_files <- results %>% dplyr::filter(success == FALSE)
    cat("Checked", nrow(results), "files.\n")
    cat(nrow(failed_files), "files could not be loaded.\n")
    
    if(nrow(failed_files) > 0) {
      print(failed_files)
      if(delete_corrupt) {
        cat("Deleting problematic files...\n")
        file.remove(failed_files$file)
      }
    }
  })
  
  plan(sequential)
  future:::ClusterRegistry("stop")
  }

# 3) Set up workspace ####

# List hazard folders (top-level)
folders <- list.dirs(working_dir, recursive = FALSE)
# Remove directories that are incomplete or unneeded for this analysis
folders <- folders[!grepl("ENSEMBLE|ipyn|gadm0|hazard_comb|indices_seasonal", folders)]
#folders <- folders[!grepl("ssp126|ssp370|2061_2080|2081_2100", folders)] 
folders <- basename(folders)

# Temporarily limit folders to 5 atlas gcms ####
gcms    <- c("MRI-ESM2-0", "ACCESS-ESM1-5", "MPI-ESM1-2-HR", "EC-Earth3", "INM-CM5-0")

folders<-grep(paste(gcms,collapse="|"),folders,value=T)

# Extract the model names (assuming each folder has a structure like scenario_model_yearrange)
model_names <- unique(unlist(tstrsplit(folders[!grepl("histor", folders)], "_", keep = 2)))

  ## 3.1) Load sos raster #####
# DEV NOTE: THE SOS DATA NEEDS REPLACING WITH SOMETHING BETTER
# This raster contains the start of season (S1, S2) and end of season (E1, E2) for two possible seasons.
# sos_rast <- terra::rast(file.path(sos_dir, "sos.tif"))
if(F){
  # Cropsuite is one option:
  # atlas_dirs$data_dir$sos_cropsuite
  # ~R/misc/sos_cropsuite.R
  # However there are issues with coverage of the second season for countries like Kenya
  sos_rast_julien<-rast(file.path(atlas_dirs$data_dir$sos_cropsuite,"seasons_median.tif"))
  
  # vectorised converter
  doy2month <- function(v, yr) {
    ok      <- !is.na(v)
    res     <- rep(NA_integer_, length(v))
    res[ok] <- as.integer(format(as.Date(v[ok] - 1,
                                         origin = sprintf("%d-01-01", yr)), "%m"))
    res
  }
  
  # apply layer by layer
  sos_rast<-rast(lapply(1:nlyr(sos_rast_julien),function(i){
    app(sos_rast_julien[[i]], fun = doy2month, yr =2010)
  }))
  names(sos_rast)<-c("S1","S2")
}

  ## 3.2) Load & process ggcmi crop calendar #####
  # The GGCMI crop calendar data (planting and maturity days).
  ggcmi_cc <- terra::rast(file.path(ggcmi_dir, "mai_rf_ggcmi_crop_calendar_phase3_v1.01.nc4"))
  # Crop the data to match base_rast extent, then resample to align cell size & origin
  ggcmi_cc <- terra::crop(ggcmi_cc, base_rast)
  ggcmi_cc <- terra::resample(ggcmi_cc, base_rast, method = "near")
  # Convert planting & maturity day-of-year to month-of-year
  ggcmi_cc$planting_month <- as.numeric(
    format(as.Date(ggcmi_cc$planting_day[], origin = as.Date("2024-01-01")), "%m")
  )
  ggcmi_cc$maturity_month <- as.numeric(
    format(as.Date(ggcmi_cc$maturity_day[], origin = as.Date("2024-01-01")), "%m")
  )
  # Keep just the planting_month and maturity_month layers
  ggcmi_cc <- ggcmi_cc[[c("planting_month", "maturity_month")]]

# 4) Choose hazards #####
# Load hazard metadata (e.g., for each variable.code, an associated function like "sum" or "mean")
haz_meta <- unique(data.table::fread(haz_meta_url)[, c("variable.code", "function")])

#  Core hazard variables
hazards_heat <- c("HSH","NTx35", "NTx40", "TAVG", "THI", "TMIN", "TMAX")
hazards_wet<- c("NDWL0", "NDWS", "PTOT", "TAI","NDD")
hazards<-c(hazards_wet,hazards_heat)

if(grepl("gddp",working_dir)){
  hazards<-hazards[!hazards %in% c("HSH","THI","TAVG")]
  hazards<-c(hazards,"NTx30")
  hazards2 <- paste0("NTx", c(20:29,31:34, 36:50))
  }else{
    hazards2 <- paste0("NTx", c(20:34, 36:39, 41:50)) 
    }

  # Add in more heat thresholds to meta-data
    haz_meta <- rbind(
      haz_meta, 
      data.table(variable.code = hazards2, `function` = "mean")
    )
    
    # Use these controls if we just want to process the additional NTx hazards or the NTx hazards only
    #hazards <- c(hazards, hazards2)
    hazards<-hazards2
    hazards<-"NTx40"
  
cat("Timeseries hazards = ",hazards,"\n")

# 5) Set analysis parameters ####
# Create ensembles?
do_ensemble<-F

# Should existing data be overwritten (T) or skipped (F)
overwrite<-F
overwrite_ensemble<-T

# Number of workers/cores to use with future.apply
worker_n <-16
# Set to F for multicore when working from the terminal in unix system
use_multisession<-F

# Logical toggles for whether or not to use a crop calendar approach,
# and whether or not to use the start-of-season (sos) approach, 
# which uses an onset-of-rain layer to define planting months.
use_crop_cal_choice <- c("no", "yes")
use_sos_cc_choice   <- c("no", "yes")

# Whether to use an end-of-season (eos) approach from the same layer.
# If use_sos_cc is "yes", then we can use eos or a fixed season_length.
use_eos_choice <- c(FALSE, TRUE)

# Potential season lengths (in months) if eos is not used. 
# This can be 3, 4, or 5 months, etc., as shown here.
season_lengths <- c(3, 4, 5)

# Combine possible configurations in a data.table
parameters  <- data.table(
  use_crop_cal   = c("no", "yes", "yes", "yes", "yes", "yes"),
  use_sos_cc     = c(NA,   "no", "yes", "yes", "yes", "yes"),
  use_eos        = c(NA,   NA,   TRUE,  FALSE, FALSE, FALSE),
  season_length  = c(NA,   NA,   NA,    3,     4,     5),
  subfolder_name = c("annual", "jagermeyr", "sos", "sos", "sos", "sos")
)

  # Subset to annual & jagermeyr (until viable sos dataset found) ####
# Remove use_eos option
parameters<-parameters[use_sos_cc!="yes"|is.na(use_sos_cc)]

cat("Timeseries parameters = \n")
print(parameters)

# Create combinations of folders and hazards for iteration
folders_x_hazards <- expand.grid(
    folders = folders,
    hazards = hazards,
    stringAsFactors = F
  )

folders_x_hazards$folder_path<-file.path(working_dir, folders_x_hazards$folders, folders_x_hazards$hazards)

# Check paths exist, exclude GCMS with missing folders
folders_x_hazards$folder_exists<-dir.exists(folders_x_hazards$folder_path)
folders_x_hazards$file_n<-sapply(folders_x_hazards$folder_path,function(i){length(list.files(i,".tif$"))})

# Check for inconsitent file_n
incomplete<-folders_x_hazards[!folders_x_hazards$file_n %in% c(20,240),]

if(nrow(incomplete)>0){
  stop("Check file completeness before continuing")
}


cat("There are",nrow(folders_x_hazards),"rows in the folder_x_hazards table.\n")

# 6) Run Analysis loop ####
# This loop runs over the different parameter configurations (whether or not to use crop calendars,
# start of season, end of season, etc.), and then processes each scenario, model, hazard combination.
for (ii in 1:nrow(parameters)) {
  # Extract the parameter configuration for this iteration
  use_crop_cal  <- parameters[ii, use_crop_cal]
  use_sos_cc    <- parameters[ii, use_sos_cc]
  use_eos       <- parameters[ii, use_eos]
  season_length <- parameters[ii, season_length]
  subfolder_name <- parameters[ii, subfolder_name]
  
  # If using crop calendars, we might have 1 or 2 seasons:
  # - if no start-of-season approach is used, we only do 1 loop
  # - if yes (start-of-season approach is used), we loop over 2 possible seasons
  if (use_crop_cal == "yes") {
    if (use_sos_cc == "yes") {
      n_seasons <- 2
    } else {
      n_seasons <- 1
    }
  } else {
    n_seasons <- 1
  }
  
  # Loop over the possible seasons (major/secondary) when using crop calendars
  for (season in 1:n_seasons) {
    
    cat("parameter set",ii,"/",nrow(parameters),"| season",season,"/",n_seasons,"\n")
    cat("use_crop_cal = ", use_crop_cal,
        " | use_sos_cc = ", use_sos_cc,
        " | use_eos = ",  use_eos,
        " | season_length = ", season_length, "\n")
    
    
    # Create the directory structure for each combination
    if (use_crop_cal == "yes") {
      if (use_sos_cc == "no") {
        # We'll just use the standard GGCMI planting and maturity months
        r_cal <- ggcmi_cc
        save_dir <- file.path(output_dir, subfolder_name)
      } else {
        # If using start-of-season from the sos_rast
        s1_name <- if (use_eos == TRUE) {
          "primary_eos"
        } else {
          paste0("primary_fixed_", season_length)
        }
        s2_name <- if (use_eos == TRUE) {
          "secondary_eos"
        } else {
          paste0("secondary_fixed_", season_length)
        }
        
        # Save path for each season
        save_dir <- file.path(
          output_dir, 
          paste0(
            subfolder_name, "_", 
            if (season == 1) { s1_name } else { s2_name }
          )
        )
      }
    } else {
      # No crop calendar usage => we do everything in one pass
      r_cal <- ggcmi_cc
      save_dir <- file.path(output_dir, subfolder_name)
      }

    if (!dir.exists(save_dir)) {
      dir.create(save_dir, recursive = TRUE)
    }
        
    cat("Save path = ",save_dir,"\n")
    
    # If we are using the SOS approach, then we assign planting and maturity months 
    # from the sos_rast. We either rely on E1, E2 for end-of-season or fix the 
    # season length in months.
    if (use_sos_cc == "yes" & use_crop_cal == "yes") {
      r_cal <- ggcmi_cc
      if (season == 1) {
        # Major season
        r_cal$planting_month <- sos_rast$S1
        if (use_eos) {
          r_cal$maturity_month <- sos_rast$E1
        } else {
          r_cal$maturity_month <- r_cal$planting_month + season_length - 1
          # Adjust for wrap-around if month goes beyond 12
          r_cal$maturity_month[r_cal$maturity_month[] > 12] <-
            r_cal$maturity_month[r_cal$maturity_month[] > 12] - 12
        }
      }
      if (season == 2) {
        # Minor season
        r_cal$planting_month <- sos_rast$S2
        if (use_eos) {
          r_cal$maturity_month <- sos_rast$E2
        } else {
          r_cal$maturity_month <- r_cal$planting_month + season_length - 1
          r_cal$maturity_month[r_cal$maturity_month[] > 12] <-
            r_cal$maturity_month[r_cal$maturity_month[] > 12] - 12
        }
      }
    }
    
    # Save the current r_cal (crop calendar) to a temp file for hazard_stacker
    r_cal_filepath <- file.path(output_dir, "rcal_temp.tif")
    terra::writeRaster(r_cal, r_cal_filepath, overwrite = TRUE)
    
    # -------------------------------------------------------------------------
    # Run hazard_stacker function (not shown in snippet) to compute seasonal 
    # or yearly hazard metrics, passing in the relevant parameters.
    # -------------------------------------------------------------------------
    if (worker_n > 1) {
      # Use parallel processing if worker_n > 1
      set_parallel_plan(n_cores=worker_n,use_multisession=T)
      future::plan()
      cat("Available cores: ", future::availableCores(),"\n")
      cat("Selected number of workers: ", future::nbrOfWorkers(),"\n")
      progressr::handlers(global = TRUE)
      progressr::handlers("progress")
      
      # Wrap the parallel code in a with_progress block so we can track progress
      p <- with_progress({
        # Define a progress bar
        progress <- progressr::progressor(along = 1:nrow(folders_x_hazards))
        
        future.apply::future_lapply(
          X = 1:nrow(folders_x_hazards),
          FUN = function(i) {
            # Update the progress bar for each iteration
            progress(sprintf(
              "Processing row %d/%d: %s, %s",
              i,
              nrow(folders_x_hazards),
              folders_x_hazards$folders[i],
              folders_x_hazards$hazards[i]
            ))
            
            hazard_stacker(
              i,
              folders_x_hazards  = folders_x_hazards,
              haz_meta          = haz_meta,
              model_names       = model_names,
              use_crop_cal      = use_crop_cal,
              r_cal_filepath    = r_cal_filepath,
              save_dir          = save_dir,
              overwrite=overwrite
            )
          }
        )
      })
      # After finishing, revert to sequential
      future::plan(sequential)
      future:::ClusterRegistry("stop")
      
      
    } else {
      # Single-core execution
      p <- lapply(
        X = 1:nrow(folders_x_hazards),
        FUN = function(i) {
          hazard_stacker(
            i,
            folders_x_hazards  = folders_x_hazards,
            haz_meta          = haz_meta,
            model_names       = model_names,
            use_crop_cal      = use_crop_cal,
            r_cal_filepath    = r_cal_filepath,
            save_dir          = save_dir,
            overwrite=overwrite
          )
        }
      )
    }
    
    
    if(do_ensemble){
    # -------------------------------------------------------------------------
    # 7) Create ensembles (mean, SD) across all models for each scenario/time 
    # combination. This looks for .tif outputs, groups them, and produces 
    # ensemble statistics.
    # -------------------------------------------------------------------------
    # List all output .tif files created by hazard_stacker
    files <- list.files(save_dir, ".tif")
    # Exclude historical or ensemble files
    files <- files[grepl("ssp", files)]
    files <- files[!grepl("ENSEMBLE", files)]
    
    # Extract model names from filenames
    models <- unique(paste0(unlist(tstrsplit(files, paste(hazards, collapse = "|"), keep = 1))))
    # Minor fix for naming
    models <- gsub("0-", "0", models)
    # Exclude any 'expos' references
    models <- models[!grepl("expos", models)]
    
    # Build scenario/time combos for iteration
    scenario <- unique(unlist(tstrsplit(models, "_", keep = 1)))
    time <- unique(paste0(
      unlist(tstrsplit(models, "_", keep = 3)), "_",
      unlist(tstrsplit(models, "_", keep = 4))
    ))
    scen_haz_time <- expand.grid(
      scenario = scenario,
      hazards  = hazards,
      time     = time,
      stringsAsFactors = FALSE
    )

    cat("Ensembling parameter set",ii,"scenarios x hazards x times = ",nrow(scen_haz_time),"| season",season,"/",n_seasons,"\n")
    
    # Use foreach parallel approach for ensemble creation
    doFuture::registerDoFuture()
    if (worker_n == 1) {
      future::plan("sequential")
    } else {
      set_parallel_plan(n_cores=worker_n,use_multisession=use_multisession)
    }
    
    p <- progressr::with_progress({
      # Progress bar for the ensemble step
      progress <- progressr::progressor(along = 1:nrow(scen_haz_time))
      
      n<-future_lapply(1:nrow(scen_haz_time), FUN=function(i){
        progress(sprintf(
          "Ensembling row %d/%d: %s, %s, %s",
          i,
          nrow(scen_haz_time),
          scen_haz_time$scenario[i],
          scen_haz_time$time[i],
          scen_haz_time$hazards[i]
        ))
        
        # Filter files for the relevant scenario, time, and hazard
        haz_files <- list.files(save_dir, scen_haz_time$hazards[i], full.names = TRUE)
        haz_files <- haz_files[!grepl("historic", haz_files)]
        haz_files <- grep(scen_haz_time$scenario[i], haz_files, value = TRUE)
        haz_files <- grep(scen_haz_time$time[i], haz_files, value = TRUE)
        
        # The part after "_hazard_" in the filename often indicates 
        # sub-variables (like different thresholds).
        var <- gsub(
          ".tif", 
          "", 
          unlist(tstrsplit(haz_files, paste0("_", scen_haz_time$hazards[i], "_"), keep = 2))
        )
        var_unique <- unique(var)
        
        # For each subvariable, generate ensemble mean & SD

          for (p in seq_along(var_unique)) {
          haz_files_ss <- haz_files[var == var_unique[p]]
          # Replace the model part of the name with "ENSEMBLE"
          savename_ensemble <- gsub(
            paste0(model_names, collapse = "|"), 
            "ENSEMBLE", 
            haz_files_ss[1]
          )
          # Create distinct filepaths for ensemble mean & SD
          savename_ensemble_mean <- gsub("_ENSEMBLE_", "_ENSEMBLEmean_", savename_ensemble)
          savename_ensemble_sd   <- gsub("_ENSEMBLE_", "_ENSEMBLEsd_",   savename_ensemble)
          
          # Check if the ensemble files already exist (skip if so)
          if (!file.exists(savename_ensemble_mean)|overwrite_ensemble) {
            # Read in all relevant model rasters
            rast_list <- lapply(haz_files_ss, function(X) {
              terra::rast(X)
            })
            
            # Calculate the per-layer mean
            haz_rast <- terra::rast(lapply(
              X = 1:terra::nlyr(rast_list[[1]]),
              FUN = function(j) {
                # For each layer j, stack all model rasters and take the mean
                terra::app(
                  terra::rast(lapply(rast_list, "[[", j)),
                  mean,
                  na.rm = TRUE
                )
              }
            ))
            # Name the layers to match the original
            names(haz_rast) <- names(rast_list[[1]])
            # Write ensemble mean to disk
            terra::writeRaster(
              haz_rast,
              savename_ensemble_mean,
              overwrite = TRUE,
              filetype = "COG",
              gdal = c("OVERVIEWS" = "NONE")
              )
            
            # Calculate the per-layer standard deviation
            haz_rast <- terra::rast(lapply(
              X = 1:terra::nlyr(rast_list[[1]]),
              FUN = function(j) {
                terra::app(
                  terra::rast(lapply(rast_list, "[[", j)),
                  sd,
                  na.rm = TRUE
                )
              }
            ))
            names(haz_rast) <- names(rast_list[[1]])
            # Write ensemble SD to disk
            terra::writeRaster(
              haz_rast,
              savename_ensemble_sd,
              overwrite = TRUE,
              filetype = "COG",
              gdal = c("OVERVIEWS" = "NONE")
              )
            
            # Housekeeping
            rm(haz_rast)
            gc()
          }
        }
        
        return(i)
      })
    })
    
    # Return to sequential plan after ensembles
    future::plan(sequential)
    future:::ClusterRegistry("stop")
    gc()
    cat("Ensembling of parameter set",ii,"complete\n")
  }
}
}
cat("Time series extraction of hazards completed.")

# 7) Check integrity of results ####
if(F){
result<-check_tif_integrity (dir_path,
                     recursive       = TRUE,
                     pattern         = "*.tif",
                     n_workers_files    = 10,
                     n_workers_folders = 1,
                     use_multisession = FALSE,
                     delete_corrupt  = FALSE)
}