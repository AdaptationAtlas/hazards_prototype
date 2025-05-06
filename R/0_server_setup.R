# 0) Introduction ####
# This script configures an R environment for the hazards_prototype workflow by:
#  1) Installing/Loading necessary packages (pacman, remotes, data.table, httr, s3fs, etc.).
#  2) Setting environment variables and verifying/creating local directories.
#  3) Dynamically downloading or updating input data (e.g., geoboundaries, MapSpam data, FAO data,
#     livestock/water data from ISIMIP, etc.) from remote sources such as S3 or GitHub.
#  4) Making sure the project structure is consistent across local/remote sessions (e.g., cglabs).
# 
# Once this script completes, your workspace is ready for more detailed hazards analysis. 
# The workflow is dependent on the outputs of the https://github.com/AdaptationAtlas/hazards pipeline

  # 0.1) Load packages and functions #####
  # Install and load pacman if not already installed
  if (!require("pacman", character.only = TRUE)) {
    install.packages("pacman")
    library(pacman)
  }
  
  # Use the isciences version of exactextractr, not the CRAN version
  if (!require("exactextractr")) {
    remotes::install_github("isciences/exactextractr")
  }
  
  # List of packages to be installed/loaded via pacman
  packages <- c("remotes", "data.table", "httr", "s3fs", "xml2", "paws.storage", "rvest", "glue","jsonlite")
  
  # Use pacman to install and load the packages
  pacman::p_load(char = packages)
  
  # Source additional functions used in this workflow from GitHub
  source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))
  
  # 0.2) Set timeframes #####
  # Possible timeframe calculations, e.g., "annual", "sos_primary_fixed_3", etc.
  timeframe_choices <- c(
    "annual",
    "jagermeyr",
    "sos_primary_eos",
    "sos_primary_fixed_3",
    "sos_primary_fixed_4",
    "sos_primary_fixed_5",
    "sos_secondary_eos",
    "sos_secondary_fixed_3",
    "sos_secondary_fixed_4",
    "sos_secondary_fixed_5"
  )
  
# 1) Setup workspace ####
# Increase download timeout (in seconds) to avoid timeouts during large data pulls
options(timeout = 600)

# Increase GDAL cache size for faster raster processing
terra::gdalCache(60000)

# Detect available CPU cores, check approximate free RAM (in MB)
parallel::detectCores()
terra::free_RAM() / 10^6

# Set how many workers to spawn in parallel processes (if used later in the script)
worker_n <- 20

  ## 1.1) Record R-project location #####
  # Function to add or update an environment variable in the .Renviron file
  set_env_variable <- function(var_name, var_value, renviron_file = "~/.Renviron") {
    # Read the .Renviron file if it exists
    if (file.exists(renviron_file)) {
      env_vars <- readLines(renviron_file)
    } else {
      env_vars <- character(0)
    }
    
    # Check if the variable already exists
    var_exists <- grepl(paste0("^", var_name, "="), env_vars)
    
    if (any(var_exists)) {
      # Update the existing variable
      env_vars[var_exists] <- paste0(var_name, "=", var_value)
    } else {
      # Add the new variable
      env_vars <- c(env_vars, paste0(var_name, "=", var_value))
    }
    
    # Write the updated .Renviron file
    writeLines(env_vars, renviron_file)
  }
  
  # Check if project_dir is already set in the environment
  if (!nzchar(Sys.getenv("project_dir"))) {
    project_dir <- getwd()
    Sys.setenv(project_dir = project_dir)
    
    # Add or update the project_dir variable in the .Renviron file
    set_env_variable("project_dir", project_dir)
    
    # Reload .Renviron so that the change takes effect in the current session
    readRenviron("~/.Renviron")
  }
  
  # Confirm project_dir was set
  (project_dir <- Sys.getenv("project_dir"))
  
  ## 1.2) Change working directory according to compute facility #####
  Cglabs <- FALSE
  if (project_dir == "/home/jovyan/atlas/hazards_prototype") {
    # cglabs environment
    working_dir <- "/home/jovyan/common_data/hazards_prototype"
    Cglabs <- TRUE
    timeframe_choices_local <- analysis_record[Location == "cglabs", Name]
  }
  
  # Local environment on Windows or Mac
  if (project_dir == "D:/rprojects/hazards_prototype") {
    working_dir <- "D:/common_data/hazards_prototype"
    timeframe_choices_local <- analysis_record[Location == "macbook", Name]
  }
  
  if (project_dir == "C:/rprojects/hazards_prototype") {
    working_dir <- "C:/rprojects/common_data/hazards_prototype"
    timeframe_choices_local <- analysis_record[Location == "macbook", Name]
  }
  
  if (project_dir == "/Users/pstewarda/Documents/rprojects/hazards_prototype") {
    working_dir <- "/Users/pstewarda/Documents/rprojects/common_data/hazards_prototype"
    timeframe_choices_local <- analysis_record[Location == "macbook", Name]
  }
  
  # Afrilabs environment
  Aflabs <- FALSE
  if (project_dir == "/home/psteward/rprojects/hazards_prototype") {
    Aflabs <- TRUE
    working_dir <- "/cluster01/workspace/atlas/hazards_prototype"
    timeframe_choices_local <- analysis_record[Location == "afrilab", Name]
  }
  
  # Create working_dir if needed
  if (!dir.exists(working_dir)) {
    dir.create(working_dir, recursive = TRUE)
  }
  
  # Set the working directory
  setwd(working_dir)
  
# 2) Create directory structures ####
  ## 2.1) Local directories #####
    atlas_data <- read_json(file.path(project_dir, "metadata/data.json"))

    ### 2.1.1) Outputs ######
    # Create a hierarchical list for top-level data directories
    atlas_dirs <- list()
    atlas_dirs$data_dir <- "Data"
    
    # Define subdirectories under 'data_dir'
    # List of all subdirectories to create under the root directory
    subdirs <- c(
      "hazard_timeseries",
      "hazard_timeseries_mean_month",
      "hazard_timeseries_class",
      "hazard_timeseries_risk",
      "hazard_risk",
      "hazard_timeseries_mean",
      "hazard_timeseries_sd",
      "hazard_timeseries_int",
      "hazard_risk_vop_usd",
      "hazard_risk_vop",
      "hazard_risk_ha",
      "hazard_risk_n",
      "hazard_risk_vop_reduced",
      "roi",
      "exposure",
      "isimip_timeseries",
      "isimip_timeseries_mean",
      "isimip_timeseries_sd",
      "cropsuite_class",
      "chirts_chirps_hist"
    )
    
    # Assign paths for each subdir key
    for (subdir in subdirs) {
      atlas_dirs$data_dir[[subdir]] <- file.path(atlas_dirs$data_dir[[1]], subdir)
    }
  
    # Subset of subdirs that need timeframe subfolders
    timeframe_subdirs <- c(
      "hazard_timeseries",
      "hazard_timeseries_class",
      "hazard_timeseries_risk",
      "hazard_risk",
      "hazard_timeseries_mean",
      "hazard_timeseries_sd",
      "hazard_timeseries_int",
      "hazard_risk_vop_usd",
      "hazard_risk_vop",
      "hazard_risk_ha",
      "hazard_risk_n",
      "hazard_risk_vop_reduced"
    )
    
    non_timeframe_subdirs<-subdirs[!subdirs %in% timeframe_subdirs]
    
    # Create non timeframe-based folders
    invisible(lapply(non_timeframe_subdirs, function(key) {
      dir_path <- atlas_dirs$data_dir[[key]]
      if (!dir.exists(dir_path)) dir.create(dir_path, recursive = TRUE)
    }))
    
    # Create objects like roi_dir, exposure_dir, etc.
    for (key in non_timeframe_subdirs) {
      var_name <- paste0(key, "_dir")
      assign(var_name, atlas_dirs$data_dir[[key]])
    }
    
    ### 2.1.2) Inputs #####
    # Indices directory (raw monthly hazard data)
    if (Cglabs) {
      # For cglabs users
      indices_dir <- "/home/jovyan/common_data/atlas_hazards/cmip6/indices"
      indices_dir2 <- "/home/jovyan/common_data/atlas_hazards/cmip6/indices_seasonal"
    } else {
      cat("Indice files are currently only available in CGlabs. Download functionality for the raw data is on the to-do list.\n",
          "See https://github.com/AdaptationAtlas/hazards if you need to replicate monthly hazard data creation.\n")
    }
    
    # Create new entries in atlas_dirs$data_dir for various directories beyond hazard outputs
    # atlas_dirs$data_dir$Boundaries         <- atlas_data$boundaries$alternate_paths$project # Created directly
    atlas_dirs$data_dir$GLPS               <- file.path(atlas_dirs$data_dir[[1]], "GLPS")
    atlas_dirs$data_dir$cattle_heatstress  <- file.path(atlas_dirs$data_dir[[1]], "cattle_heatstress")
    atlas_dirs$data_dir$adaptive_capacity  <- file.path(atlas_dirs$data_dir[[1]], "adaptive_capacity")
    atlas_dirs$data_dir$atlas_pop          <- file.path(atlas_dirs$data_dir[[1]], "atlas_pop")
   # atlas_dirs$data_dir$commodity_masks    <- file.path(atlas_dirs$data_dir[[1]], "commodity_masks")
    atlas_dirs$data_dir$GLW4               <- file.path(atlas_dirs$data_dir[[1]], "GLW4")
    atlas_dirs$data_dir$livestock_vop      <- file.path(atlas_dirs$data_dir[[1]], "livestock_vop")
    atlas_dirs$data_dir$afr_highlands      <- file.path(atlas_dirs$data_dir[[1]], "afr_highlands")
    atlas_dirs$data_dir$fao                <- file.path(atlas_dirs$data_dir[[1]], "fao")
    # atlas_dirs$data_dir$mapspam_2020v1r2   <- atlas_data$mapspam_2020v1r2$alternate_paths$project # folder object mapspam_dir created directly from atlas_data
    atlas_dirs$data_dir$sos                <- file.path(atlas_dirs$data_dir[[1]], "sos")
    atlas_dirs$data_dir$ggcmi              <- file.path(atlas_dirs$data_dir[[1]], "ggcmi")
    atlas_dirs$data_dir$hydrobasins        <- file.path(atlas_dirs$data_dir[[1]], "hydrobasins")
    atlas_dirs$data_dir$solution_tables    <- file.path(atlas_dirs$data_dir[[1]], "solution_tables")
    
    # Now create (if not present) each of these directories locally
    boundaries_dir <- sub("/$", "",atlas_data$boundaries$alternate_paths$project)
    if (!dir.exists(boundaries_dir)) {
      dir.create(boundaries_dir, recursive = TRUE)
    }
    
    glps_dir <- atlas_dirs$data_dir$GLPS
    if (!dir.exists(glps_dir)) {
      dir.create(glps_dir, recursive = TRUE)
    }
    
    cattle_heatstress_dir <- atlas_dirs$data_dir$cattle_heatstress
    if (!dir.exists(cattle_heatstress_dir)) {
      dir.create(cattle_heatstress_dir, recursive = TRUE)
    }
    
    ac_dir <- atlas_dirs$data_dir$adaptive_capacity
    if (!dir.exists(ac_dir)) {
      dir.create(ac_dir, recursive = TRUE)
    }
    
    hpop_dir <- atlas_dirs$data_dir$atlas_pop
    if (!dir.exists(hpop_dir)) {
      dir.create(hpop_dir, recursive = TRUE)
    }
    
    hpop_int_dir <- file.path(hpop_dir,"intermediate")
    if (!dir.exists(hpop_int_dir)) {
      dir.create(hpop_int_dir, recursive = TRUE)
    }
    
    hpop_pro_dir <- file.path(hpop_dir,"processed")
    if (!dir.exists(hpop_pro_dir)) {
      dir.create(hpop_pro_dir, recursive = TRUE)
    }
    
   # commodity_mask_dir <- atlas_dirs$data_dir$commodity_masks
  #  if (!dir.exists(commodity_mask_dir)) {
  #    dir.create(commodity_mask_dir, recursive = TRUE)
   # }
    
    glw_dir <- atlas_dirs$data_dir$GLW4
    if (!dir.exists(glw_dir)) {
      dir.create(glw_dir, recursive = TRUE)
    }
    
    glw_pro_dir <- file.path(glw_dir,"processed")
    if (!dir.exists(glw_pro_dir)) {
      dir.create(glw_pro_dir, recursive = TRUE)
    }
    
    glw_int_dir <- file.path(glw_dir,"intermediate")
    if (!dir.exists(glw_int_dir)) {
      dir.create(glw_int_dir, recursive = TRUE)
    }

    #ls_vop_dir <- atlas_dirs$data_dir$livestock_vop
    #if (!dir.exists(ls_vop_dir)) {
    #  dir.create(ls_vop_dir, recursive = TRUE)
    #}
    
    afr_highlands_dir <- atlas_dirs$data_dir$afr_highlands
    if (!dir.exists(afr_highlands_dir)) {
      dir.create(afr_highlands_dir, recursive = TRUE)
    }
    
    fao_dir <- atlas_dirs$data_dir$fao
    if (!dir.exists(fao_dir)) {
      dir.create(fao_dir, recursive = TRUE)
    }
    
    mapspam_dir <- sub("/$", "",atlas_data$mapspam_2020v1r2$alternate_paths$project)
    if (!dir.exists(mapspam_dir)) {
      dir.create(mapspam_dir, recursive = TRUE)
    }
    
    mapspam_pro_dir <- "Data/mapspam/2020V1r2_SSA/processed"
    if (!dir.exists(mapspam_pro_dir)) {
      dir.create(mapspam_pro_dir, recursive = TRUE)
    }
    
    sos_dir <- atlas_dirs$data_dir$sos
    if (!dir.exists(sos_dir)) {
      dir.create(sos_dir, recursive = TRUE)
    }
    
    ggcmi_dir <- atlas_dirs$data_dir$ggcmi
    if (!dir.exists(ggcmi_dir)) {
      dir.create(ggcmi_dir, recursive = TRUE)
    }
    
    hydrobasins_dir <- atlas_dirs$data_dir$hydrobasins
    if (!dir.exists(hydrobasins_dir)) {
      dir.create(hydrobasins_dir, recursive = TRUE)
    }
    
    solution_tables_dir <- atlas_dirs$data_dir$solution_tables
    if (!dir.exists(solution_tables_dir)) {
      dir.create(solution_tables_dir, recursive = TRUE)
    }
    
    # Special handling for Cglabs environment (common_data paths)
    if (Cglabs) {
      # Additional directory paths in cglabs environment
      sos_raw_dir <- "/home/jovyan/common_data/atlas_sos/seasonal_mean"
      isimip_raw_dir <- "/home/jovyan/common_data/isimip"
      chirts_raw_dir <- "/home/jovyan/common_data/chirts"
      chirps_raw_dir <- "/home/jovyan/common_data/chirps_wrld"
      
      cropsuite_raw_dir <- "/home/jovyan/common_data/atlas_cropSuite"
      if (!dir.exists(isimip_raw_dir)) {
        dir.create(isimip_raw_dir, recursive = TRUE)
      }
    }
    
  ## 2.2) Cloud directories (Atlas s3 bucket) #####
    ### 2.2.1) Set S3 directory structure ####
  bucket_name <- "http://digital-atlas.s3.amazonaws.com"
  bucket_name_s3 <- "s3://digital-atlas"
  
  atlas_dirs$s3_dir <- list(bucket_name_s3)
  
    # Define subdirectories under 's3_dir'
  subdirs <- c(
    "hazard_timeseries",
    "haz_time_risk_dir",
    "hazard_risk"
  )
  
  for (sub in subdirs) {
    atlas_dirs$s3_dir[[sub]] <- 
      file.path(atlas_dirs$s3_dir[[1]], sub)
  }

  hazard_timeseries_s3 <- atlas_dirs$s3_dir$hazard_timeseries
  
  ### 2.2.2) Create an S3FileSystem object for anonymous read access #### 
  s3 <- s3fs::S3FileSystem$new(anonymous = TRUE)
  
# 3) Download data ####
# Each subsection downloads data if it does not already exist locally.
# The 'update' flag can be toggled if you want to force a re-download.

  ## 3.1) Geoboundaries #####
  update <- FALSE

  admin_levels <- atlas_data$boundaries$params$level
  regions <- atlas_data$boundaries$params$region[[2]] # 1 = 'global', 2 = 'africa'

  geo_files_s3 <- file.path(
    bucket_name_s3,
      glue(
      atlas_data$boundaries$s3$path_pattern,
      region = regions,
      level = admin_levels)
  )
  
  geo_files_local <- file.path(boundaries_dir, basename(geo_files_s3))
  names(geo_files_local) <- c("admin0", "admin1", "admin2")
  
  lapply(seq_along(geo_files_local), FUN = function(i) {
    file <- geo_files_local[i]
    # Download each file from S3 if it doesn't exist locally or if update=TRUE
    if (!file.exists(file) | update == TRUE) {
      s3$file_download(geo_files_s3[i], file, overwrite = update)
    }
  })
  
  ## 3.2) Mapspam #####
  update <- FALSE
  ### 3.2.1) Processed data ####
  # Construct the S3 folder path
  folder_path <- "domain=exposure/type=crop/source=spam2020v1r2_ssa/region=ssa/processing=atlas-harmonized/"
  
  # List .csv files from the specified S3 bucket location
  files_s3 <- s3$dir_ls(file.path(bucket_name_s3, folder_path), recurse = TRUE)
  files_local <- gsub(file.path(bucket_name_s3, folder_path), paste0(mapspam_pro_dir, "/"), files_s3)
  
  # Download files if missing or if update=TRUE
  for (i in seq_along(files_local)) {
    cat("3.2.1) Downloading mapspam processed files",i,"/",length(files_local),"     \r")
    file <- files_local[i]
    save_dir<-dirname(file)
    if(!dir.exists(save_dir)){
      dir.create(save_dir,recursive=T)
    }
    if (!file.exists(file) | update == TRUE) {
      s3$file_download(files_s3[i], file, overwrite = TRUE)
    }
  }
  
  ### 3.2.2) Raw data ####
  # Construct the S3 folder path
  folder_path <- atlas_data$mapspam_2020v1r2$s3$path_pattern
  
  # List .csv files from the specified S3 bucket location
  files_s3 <- s3$dir_ls(file.path(bucket_name_s3, folder_path), recurse = TRUE)
  files_s3 <- files_s3[grepl(".csv", files_s3) & !grepl("index", files_s3)]
  files_local <- gsub(file.path(bucket_name_s3, folder_path), paste0(mapspam_dir, "/"), files_s3)
  
  # Download files if missing or if update=TRUE
  for (i in seq_along(files_local)) {
    cat("3.2.2) Downloading mapspam raw files",i,"/",length(files_local),"     \r")
    file <- files_local[i]
    if (!file.exists(file) | update == TRUE) {
      s3$file_download(files_s3[i], file, overwrite = TRUE)
    }
  }
  
  ## 3.3) Base Raster #####
  # Load a reference/base raster used for resampling or extent alignment
  base_rast_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/base_raster.tif"
  base_rast <- terra::rast(base_rast_url)
  base_rast_path <- file.path(project_dir, "metadata", "base_raster.tif")
  # Typically you might save this locally if you need repeated use.
  
  ## 3.4) GLW #####
  update <- FALSE
  # Download Global Livestock Density (GLW4) if missing
  glw_names <- c(
    poultry = "Ch", sheep = "Sh", pigs = "Pg", horses = "Ho",
    goats = "Gt", ducks = "Dk", buffalo = "Bf", cattle = "Ct"
  )
  glw_codes <- c(
    poultry = 6786792, sheep = 6769626, pigs = 6769654, horses = 6769681,
    goats = 6769696, ducks = 6769700, buffalo = 6770179, cattle = 6769711
  )
  glw_files <- file.path(glw_dir, paste0("5_", glw_names, "_2015_Da.tif"))
  
  for (i in seq_along(glw_files)) {
    glw_file <- glw_files[i]
    if (!file.exists(glw_file) | update == TRUE) {
      api_url <- paste0("https://dataverse.harvard.edu/api/access/datafile/", glw_codes[i])
      # Download directly from the Dataverse API
      response <- httr::GET(url = api_url, httr::write_disk(glw_file, overwrite = TRUE))
      if (httr::status_code(response) == 200) {
        print(paste0("File ", i, " downloaded successfully."))
      } else {
        print(paste("Failed to download file ", i, ". Status code:", httr::status_code(response)))
      }
    }
  }
  
  ## 3.5) Fao stat #####
    ### 3.5.1) Deflators ######
    update <- FALSE
    def_file <- paste0(fao_dir, "/Deflators_E_All_Data_(Normalized).csv")
    
    if (!file.exists(def_file) | update == TRUE) {
      url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Deflators_E_All_Data_(Normalized).zip"
      zip_file_path <- file.path(fao_dir, basename(url))
      
      download.file(url, zip_file_path, mode = "wb")
      unzip(zip_file_path, exdir = fao_dir)
      unlink(zip_file_path)
    }
    
    ### 3.5.2) Producer prices ######
    fao_econ_file <- file.path(fao_dir, "Prices_E_Africa_NOFLAG.csv")
    if (!file.exists(fao_econ_file)) {
      url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Prices_E_Africa.zip"
      zip_file_path <- file.path(fao_dir, "Prices_E_Africa.zip")
      
      download.file(url, zip_file_path, mode = "wb")
      unzip(zip_file_path, exdir = fao_dir)
      unlink(zip_file_path)
    }
    
    ### 3.5.3) Production ######
    prod_file <- file.path(fao_dir, "Production_Crops_Livestock_E_Africa_NOFLAG.csv")
    if (!file.exists(prod_file)) {
      url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Production_Crops_Livestock_E_Africa.zip"
      zip_file_path <- file.path(fao_dir, "Production_E_Africa.zip")
      
      download.file(url, zip_file_path, mode = "wb")
      unzip(zip_file_path, exdir = fao_dir)
      unlink(zip_file_path)
    }
    
    prod_file_world <- file.path(fao_dir, "Production_Crops_Livestock_E_All_Area_Groups.csv")
    if (!file.exists(prod_file_world)) {
      url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Production_Crops_Livestock_E_All_Area_Groups.zip"
      zip_file_path <- file.path(fao_dir, "Production_Crops_Livestock_E_All_Area_Groups.zip")
      
      download.file(url, zip_file_path, mode = "wb")
      unzip(zip_file_path, exdir = fao_dir)
      unlink(zip_file_path)
    }
    
    ### 3.5.4) Value of production #####
    vop_file <- file.path(fao_dir, "Value_of_Production_E_Africa.csv")
    if (!file.exists(vop_file)) {
      url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Value_of_Production_E_Africa.zip"
      zip_file_path <- file.path(fao_dir, "Value_of_Production_E_Africa.zip")
      
      download.file(url, zip_file_path, mode = "wb")
      unzip(zip_file_path, exdir = fao_dir)
      unlink(zip_file_path)
    }
    
    vop_file_world <- file.path(fao_dir, "Value_of_Production_E_All_Area_Groups.csv")
    if (!file.exists(vop_file_world)) {
      url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Value_of_Production_E_All_Area_Groups.zip"
      zip_file_path <- file.path(fao_dir, "Value_of_Production_E_All_Area_Groups.zip")
      
      download.file(url, zip_file_path, mode = "wb")
      unzip(zip_file_path, exdir = fao_dir)
      unlink(zip_file_path)
    }
    
  ## 3.6) Highlands map #####
  update <- FALSE
  afr_highlands_file <- file.path(afr_highlands_dir, "afr-highlands.asc")
  if (!file.exists(afr_highlands_file) | update == TRUE) {
    s3$file_download(file.path(bucket_name_s3, "afr_highlands/afr-highlands.asc"), afr_highlands_file, overwrite = TRUE)
  }
  
  ## 3.7) (Depreciated) Livestock vop #####
  # Now housed under processsing/intermediate in GLW
  if(F){
  update <- FALSE
  folder_path <- "livestock_vop/"
  files_s3 <- s3$dir_ls(file.path(bucket_name_s3, folder_path))
  files_s3 <- files_s3[grepl(".tif", files_s3)]
  files_local <- gsub(file.path(bucket_name_s3, folder_path), paste0(ls_vop_dir, "/"), files_s3)
  
  for (i in seq_along(files_local)) {
    file <- files_local[i]
    if (!file.exists(file) | update == TRUE) {
      s3$file_download(files_s3[i], file)
    }
  }
  }
  ## 3.8) Human population #####
  folder_path <- "population/worldpop_2020/"
  files_s3 <- s3$dir_ls(file.path(bucket_name_s3, folder_path))
  files_s3 <- files_s3[grepl("pop.tif", files_s3)]
  files_local <- gsub(file.path(bucket_name_s3, folder_path), paste0(hpop_dir, "/"), files_s3)
  
  for (i in seq_along(files_local)) {
    file <- files_local[i]
    if (!file.exists(file) | update == TRUE) {
      s3$file_download(files_s3[i], file)
    }
  }
  
  ## 3.9) GLPS #####
  local_dir <- glps_dir
  files_s3 <- s3$dir_ls(file.path(bucket_name_s3, basename(local_dir)))
  files_local <- file.path(local_dir, basename(files_s3))
  
  for (i in seq_along(files_local)) {
    file <- files_local[i]
    if (!file.exists(file) | update == TRUE) {
      s3$file_download(files_s3[i], file)
    }
  }
  
  ## 3.10) Cattle heatstress #####
  local_dir <- cattle_heatstress_dir
  files_s3 <- s3$dir_ls(file.path(bucket_name_s3, basename(local_dir)))
  files_local <- file.path(local_dir, basename(files_s3))
  
  for (i in seq_along(files_local)) {
    file <- files_local[i]
    if (!file.exists(file) | update == TRUE) {
      s3$file_download(files_s3[i], file)
    }
  }
  
  ## 3.11) SOS #####
  local_dir <- sos_dir
  files_s3 <- s3$dir_ls(file.path(bucket_name_s3, basename(local_dir)))
  files_local <- file.path(local_dir, basename(files_s3))
  
  for (i in seq_along(files_local)) {
    file <- files_local[i]
    if (!file.exists(file) | update == TRUE) {
      s3$file_download(files_s3[i], file)
    }
  }
  
  ## 3.12) GGCMI crop calendars #####
  update <- FALSE
  # If folder has fewer than ~40 files, re-check for updates
  if (length(list.files(ggcmi_dir)) != 40) {
    url <- "https://www.pik-potsdam.de/~jonasjae/GGCMI_Phase3_crop_calendar"
    webpage <- rvest::read_html(url)
    
    file_links <- webpage %>%
      rvest::html_nodes("a") %>%
      rvest::html_attr("href") %>%
      grep("\\.nc4$", ., value = TRUE)  # Only keep .nc4 files
    
    file_links <- file.path(url, file_links)
    
    for (i in seq_along(file_links)) {
      cat(sprintf("\rDownloading GGCMI file %d/%d", i, length(file_links)))
      file <- file.path(ggcmi_dir, basename(file_links[i]))
      if (!file.exists(file) | update == TRUE) {
        download.file(file_links[i], file)
      }
    }
  }
  
  ## 3.13) Hydrobasins #####
  # Download wmo basin boundaries in JSON format
  if (!file.exists(file.path(hydrobasins_dir, "wmobb_rivnets_Q00_01.json"))) {
    url <- "https://grdc.bafg.de/downloads/wmobb_json.zip"
    local_path <- file.path(hydrobasins_dir, basename(url))
    download.file(url, local_path)
    unzip(local_path, exdir = dirname(local_path))
    unlink(local_path)
  }
  
  ## 3.14) Solution tables #####
  update <- FALSE
  local_dir <- solution_tables_dir
  files_s3 <- s3$dir_ls(file.path(bucket_name_s3, basename(local_dir)))
  files_local <- file.path(local_dir, basename(files_s3))
  
  for (i in seq_along(files_local)) {
    file <- files_local[i]
    if (!file.exists(file) | update == TRUE) {
      s3$file_download(files_s3[i], file)
    }
  }
  
# 4) Set data URLs ####
  ## 4.1) hazard class #####
  haz_class_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_classes.csv"
  
  ## 4.2) hazard metadata #####
  haz_meta_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_metadata.csv"
  
  ## 4.3) mapspam codes #####
  ms_codes_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SpamCodes.csv"
  spam2fao_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SPAM2010_FAO_crops.csv"
  
  ## 4.4) ecocrop ####
  ecocrop_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/ecocrop.csv"
  
  ## 4.5) isimip metadata #####
  isimip_meta_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/isimip_water_var_metadata.csv"
  
  # ---------------------------------------------------------------------------------------------
  # End of script
  cat("0_server_setup.R has completed successfully.\n")
  cat("CGLabs = ",Cglabs,"\n")
# ---------------------------------------------------------------------------------------------

