# 0.1) Choose timeframe #####
# Choose season calculation method
timeframe_choices<-c("annual","jagermeyr","sos_primary_eos",
                     "sos_primary_fixed_3","sos_primary_fixed_4","sos_primary_fixed_5","sos_secondary_eos",
                     "sos_secondary_fixed_3","sos_secondary_fixed_4","sos_secondary_fixed_5")

timeframe_choice_index<-2
timeframe_choice <- timeframe_choices[timeframe_choice_index]
cat("You selected:", timeframe_choice, "\n")

# 0.2)Load packages and functions #####

# Install and load pacman if not already installed
if (!require("pacman", character.only = TRUE)) {
  install.packages("pacman")
  library(pacman)
}

# Use the isciences version and not the CRAN version of exactextractr
if(!require("exactextractr")){
  remotes::install_github("isciences/exactextractr")
}

# List of packages to be loaded
packages <- c("remotes","data.table","httr","s3fs","xml2","paws","rvest")

# Use pacman to install and load the packages
pacman::p_load(char=packages)

# Source functions used in this workflow
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

# 1) Setup workspace ####

# Increase download timeout
options(timeout = 600)

# Increase GDAL cache size
terra::gdalCache(60000)

# workers
parallel::detectCores()
terra::free_RAM()/10^6

worker_n<-20

  # 1.1) Record R-project location #####
# Function to add or update an environment variable in .Renviron file
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

# Check if the project_dir variable is already set
if (!nzchar(Sys.getenv("project_dir"))) {
  project_dir <- getwd()
  Sys.setenv(project_dir = project_dir)
  
  # Add or update the project_dir variable in the .Renviron file
  set_env_variable("project_dir", project_dir)
  
  # Optional: Reload the .Renviron file to make sure the environment variable is set
  readRenviron("~/.Renviron")
}

# Verify the environment variable is set
(project_dir<-Sys.getenv("project_dir"))

# 1.2) Change working directory according to compute facility #####
Cglabs<-F
if(project_dir=="/home/jovyan/atlas/hazards_prototype"){
  working_dir<-"/home/jovyan/common_data/hazards_prototype"
  Cglabs<-T
}

# Local
if(project_dir=="D:/rprojects/hazards_prototype"){
  working_dir<-"D:/common_data/hazards_prototype"
}

if(project_dir=="C:/rprojects/hazards_prototype"){
  working_dir<-"C:/rprojects/common_data/hazards_prototype"
}

if(project_dir=="/Users/pstewarda/Documents/rprojects/hazards_prototype"){
  working_dir<-"/Users/pstewarda/Documents/rprojects/common_data"
}

# Afrilabs
Aflabs<-F
if(project_dir=="/home/psteward/rprojects/hazards_prototype"){
  Aflabs<-T
  working_dir<-"/cluster01/workspace/atlas/hazards_prototype"
}

if(!dir.exists(working_dir)){
  dir.create(working_dir,recursive=T)
}

setwd(working_dir)

# 2) Create directory structures ####
  # 2.1) Local directories #####
    # 2.1.1) Outputs ######
    haz_timeseries_dir<-file.path("Data/hazard_timeseries",timeframe_choice)
    if(!dir.exists(haz_timeseries_dir)){dir.create(haz_timeseries_dir,recursive=T)}
    haz_timeseries_s3_dir<-paste0("s3://digital-atlas/risk_prototype/data/hazard_timeseries/",timeframe_choice)
    
    haz_timeseries_monthly_dir<-"Data/hazard_timeseries_mean_month"
    if(!dir.exists(haz_timeseries_monthly_dir)){dir.create(haz_timeseries_monthly_dir,recursive=T)}
    
    haz_time_class_dir<-paste0("Data/hazard_timeseries_class/",timeframe_choice)
    if(!dir.exists(haz_time_class_dir)){dir.create(haz_time_class_dir,recursive=T)}
    
    haz_time_risk_dir<-paste0("Data/hazard_timeseries_risk/",timeframe_choice)
    if(!dir.exists(haz_time_risk_dir)){dir.create(haz_time_risk_dir,recursive=T)}
    
    haz_risk_dir<-paste0("Data/hazard_risk/",timeframe_choice)
    if(!dir.exists(haz_risk_dir)){dir.create(haz_risk_dir,recursive = T)}
    
    haz_mean_dir<-paste0("Data/hazard_timeseries_mean/",timeframe_choice)
    if(!dir.exists(haz_mean_dir)){dir.create(haz_mean_dir,recursive=T)}
    
    haz_sd_dir<-paste0("Data/hazard_timeseries_sd/",timeframe_choice)
    if(!dir.exists(haz_sd_dir)){dir.create(haz_sd_dir,recursive=T)}
    
    haz_time_int_dir<-paste0("Data/hazard_timeseries_int/",timeframe_choice)
    if(!dir.exists(haz_time_int_dir)){dir.create(haz_time_int_dir,recursive=T)}
    
    haz_risk_vop17_dir<-file.path("Data/hazard_risk_vop17",timeframe_choice)
    if(!dir.exists(haz_risk_vop17_dir)){
      dir.create(haz_risk_vop17_dir,recursive = T)
    }
    
    haz_risk_vop_dir<-file.path("Data/hazard_risk_vop",timeframe_choice)
    if(!dir.exists(haz_risk_vop_dir)){
      dir.create(haz_risk_vop_dir,recursive = T)
    }
    
    haz_risk_ha_dir<-file.path("Data/hazard_risk_ha",timeframe_choice)
    if(!dir.exists(haz_risk_ha_dir)){
      dir.create(haz_risk_ha_dir,recursive = T)
    }
    
    haz_risk_n_dir<-file.path("Data/hazard_risk_n",timeframe_choice)
    if(!dir.exists(haz_risk_n_dir)){
      dir.create(haz_risk_n_dir,recursive = T)
    }
    
    haz_risk_vop_ac_dir<-paste0("Data/hazard_risk_vop_ac/",timeframe_choice)
    if(!dir.exists(haz_risk_vop_ac_dir)){
      dir.create(haz_risk_vop_ac_dir,recursive=T)
    }
    
    roi_dir<-"Data/roi"
    
    exposure_dir<-"Data/exposure"
    if(!dir.exists(exposure_dir)){
      dir.create(exposure_dir)
    }
    
    isimip_timeseries_dir<-"Data/isimip_timeseries"
    if(!dir.exists(isimip_timeseries_dir)){
      dir.create(isimip_timeseries_dir)
    }
    
    isimip_mean_dir<-"Data/isimip_timeseries_mean"
    if(!dir.exists(isimip_mean_dir)){
      dir.create(isimip_mean_dir)
    }
    
    isimip_sd_dir<-"Data/isimip_timeseries_sd"
    if(!dir.exists(isimip_sd_dir)){
      dir.create(isimip_sd_dir)
    }
    
    cropsuite_class_dir<-"Data/cropsuite_class"
    if(!dir.exists(cropsuite_class_dir)){
      dir.create(cropsuite_class_dir,recursive=T)
    }
    
    chirts_chirps_dir<-"Data/chirts_chirps_hist"
    if(!dir.exists(chirts_chirps_dir)){
      dir.create(chirts_chirps_dir,recursive=T)
    }
  
    # 2.1.2) Inputs #####
    
    # Where is the raw monthly hazards data stored?
    if(Cglabs){
      # Generated from https://github.com/AdaptationAtlas/hazards/tree/main is stored
      indices_dir<-"/home/jovyan/common_data/atlas_hazards/cmip6/indices"
      indices_dir2<-"/home/jovyan/common_data/atlas_hazards/cmip6/indices_seasonal"
      
      if(timeframe_choice!="annual"){
        indices_seasonal_dir<-paste0(indices_dir2,"/by_season/",timeframe_choice,"/hazard_timeseries")
      }else{
        indices_seasonal_dir<-paste0(indices_dir2,"/by_year/hazard_timeseries")
      }
    }else{
      cat("Indice files are currently only available in CGlabs, adding download functionality for raw data used in
        workflow is on the to-do list. You should also see the https://github.com/AdaptationAtlas/hazards workflow which 
        will enable you to replicate the creation of the foundational monthly hazard data used in this workflow.")
    }
    
    geo_dir<-"Data/boundaries"
    if(!dir.exists(geo_dir)){
      dir.create(geo_dir)
    }
  
    glps_dir<-file.path(working_dir,"Data","GLPS")
    if(!dir.exists(glps_dir)){
      dir.create(glps_dir)
    }
    
    cattle_heatstress_dir<-file.path(working_dir,"Data","cattle_heatstress")
    if(!dir.exists(cattle_heatstress_dir)){
      dir.create(cattle_heatstress_dir)
    }
    
    ac_dir<-"Data/adaptive_capacity"
    if(!dir.exists(ac_dir)){
      dir.create(ac_dir,recursive=T)
    }
    
    hpop_dir<-"Data/atlas_pop"
    if(!dir.exists(hpop_dir)){
      dir.create(hpop_dir)
    }
    
    commodity_mask_dir<-"Data/commodity_masks"
    if(!dir.exists(commodity_mask_dir)){
      dir.create(commodity_mask_dir)
    }
    
  
    glw_dir<-"Data/GLW4"
    if(!dir.exists(glw_dir)){
      dir.create(glw_dir)
    }
    
    ls_vop_dir<-"Data/livestock_vop"
    if(!dir.exists(ls_vop_dir)){
      dir.create(ls_vop_dir)
    }
    
    afr_highlands_dir<-"Data/afr_highlands"
    if(!dir.exists(afr_highlands_dir)){
      dir.create(afr_highlands_dir)
    }
    
    fao_dir<-"Data/fao"
    if(!dir.exists(fao_dir)){
      dir.create(fao_dir,recursive = T)
    }
    
    mapspam_dir<-"Data/mapspam/2020V1r2_SSA"
    if(!dir.exists(mapspam_dir)){
      dir.create(mapspam_dir,recursive=T)
    }
    
    sos_dir<-"Data/sos"
    if(!dir.exists(sos_dir)){
      dir.create(sos_dir,recursive=T)
    }
    
    ggcmi_dir<-"Data/ggcmi"
    if(!dir.exists(ggcmi_dir)){
      dir.create(ggcmi_dir,recursive=T)
    }
    
    hydrobasins_dir<-"Data/hydrobasins"
    if(!dir.exists(hydrobasins_dir)){
      dir.create(hydrobasins_dir,recursive=T)
    }
    
    
    if(Cglabs){
      sos_raw_dir<-"/home/jovyan/common_data/atlas_sos/seasonal_mean"
      isimip_raw_dir<-"/home/jovyan/common_data/isimip"
      chirts_raw_dir<-"/home/jovyan/common_data/chirts"
      chirps_raw_dir<-"/home/jovyan/common_data/chirps_wrld"
      
      cropsuite_raw_dir<-"/home/jovyan/common_data/atlas_cropSuite"
      if(!dir.exists(isimip_raw_dir)){
        dir.create(isimip_raw_dir,recursive=T)
      }
    }
    
    solution_tables_dir<-"Data/solution_tables"
    if(!dir.exists(solution_tables_dir)){
      dir.create(solution_tables_dir,recursive=T)
    }
    
  # 2.2) Cloud directories (Atlas s3 bucket) #####
  bucket_name <- "http://digital-atlas.s3.amazonaws.com"
  bucket_name_s3<-"s3://digital-atlas"
  s3<-s3fs::S3FileSystem$new(anonymous = T)
# 3) Download data ####
  # 3.1) Geoboundaries #####
  update<-F
  
  geo_files_s3<-c(
    file.path(bucket_name_s3,"boundaries/atlas-region_admin0_harmonized.parquet"),
    file.path(bucket_name_s3,"boundaries/atlas-region_admin1_harmonized.parquet"),
    file.path(bucket_name_s3,"boundaries/atlas-region_admin2_harmonized.parquet"))
  
  geo_files_local<-file.path(geo_dir,basename(geo_files_s3))
  names(geo_files_local)<-c("admin0","admin1","admin2")
  
  lapply(1:length(geo_files_local),FUN=function(i){
    file<-geo_files_local[i]
    if(!file.exists(file)|update==T){
      s3$file_download(geo_files_s3[i],file)
    }
  })
  
  # 3.2) Mapspam #####
  update<-F
  
  # Specify s3 prefix (folder path)
  folder_path <- file.path("MapSpam/raw",basename(mapspam_dir))

  # List files in the specified S3 bucket and prefix
  files_s3<-s3$dir_ls(file.path(bucket_name_s3,folder_path))

  files_s3<-files_s3[grepl(".csv",files_s3) & !grepl("index",files_s3)]
  files_local<-gsub(file.path(bucket_name_s3,folder_path),paste0(mapspam_dir,"/"),files_s3)
  
  # If mapspam data does not exist locally download from S3 bucket
  for(i in 1:length(files_local)){
    file<-files_local[i]
    if(!file.exists(file)|update==T){
      s3$file_download(files_s3[i],file,overwrite=T)
    }
  }
  
  # 3.3) Base Raster #####
  # Load base raster to which other datasets are resampled to
  base_rast_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/base_raster.tif"
  base_rast<-terra::rast(base_rast_url)
  base_rast_path<-file.path(project_dir,"metadata","base_raster.tif")
  
  # 3.4) GLW #####
  update<-F
  # If glw data does not exist locally download from S3 bucket
  # Current version is GLW 4
  glw_names<-c(poultry="Ch",sheep="Sh",pigs="Pg",horses="Ho",goats="Gt",ducks="Dk",buffalo="Bf",cattle="Ct")
  glw_codes<-c(poultry=6786792,sheep=6769626,pigs=6769654,horses=6769681,goats=6769696,ducks=6769700,buffalo=6770179,cattle=6769711)
  glw_files <- file.path(glw_dir,paste0("5_",glw_names,"_2015_Da.tif"))
  
  for(i in 1:length(glw_files)){
    glw_file<-glw_files[i]
    if(!file.exists(glw_file)|update==T){
      api_url <- paste0("https://dataverse.harvard.edu/api/access/datafile/",glw_codes[i])
      # Perform the API request and save the file
      response <- httr::GET(url = api_url, httr::write_disk(glw_file, overwrite = TRUE))
      
      # Check if the download was successful
      if (httr::status_code(response) == 200) {
        print(paste0("File ",i," downloaded successfully."))
      } else {
        print(paste("Failed to download file ",i,". Status code:", httr::status_code(response)))
      }
    }
  }
  
  # 3.5) Fao stat #####
    # 3.5.1) deflators ######
  update<-F
  # Download FAOstat deflators
  def_file<-paste0(fao_dir,"/Deflators_E_All_Data_(Normalized).csv")
  
  if(!file.exists(def_file)|update==T){
    # Define the URL and set the save path
    url<-"https://fenixservices.fao.org/faostat/static/bulkdownloads/Deflators_E_All_Data_(Normalized).zip"
    
    zip_file_path <- file.path(fao_dir,basename(url))
    
    # Download the file
    download.file(url, zip_file_path, mode = "wb")
    
    # Unzip the file
    unzip(zip_file_path, exdir = fao_dir)
    
    # Delete the ZIP file
    unlink(zip_file_path)
  }
  
    # 3.5.2) producer prices ######
  fao_econ_file<-file.path(fao_dir,"Prices_E_Africa_NOFLAG.csv")
  
  if(!file.exists(fao_econ_file)){
    # Define the URL and set the save path
    url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Prices_E_Africa.zip"
    zip_file_path <- file.path(fao_dir, "Prices_E_Africa.zip")
    
    # Download the file
    download.file(url, zip_file_path, mode = "wb")
    
    # Unzip the file
    unzip(zip_file_path, exdir = fao_dir)
    
    # Delete the ZIP file
    unlink(zip_file_path)
  }
  
    # 3.5.3) production ######
    prod_file<-file.path(fao_dir,"Production_Crops_Livestock_E_Africa_NOFLAG.csv")
    
    if(!file.exists(prod_file)){
      # Define the URL and set the save path
      url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Production_Crops_Livestock_E_Africa.zip"
      zip_file_path <- file.path(fao_dir, "Production_E_Africa.zip")
      
      # Download the file
      download.file(url, zip_file_path, mode = "wb")
      
      # Unzip the file
      unzip(zip_file_path, exdir = fao_dir)
      
      # Delete the ZIP file
      unlink(zip_file_path)
    }
    
    prod_file_world<-file.path(fao_dir,"Production_Crops_Livestock_E_All_Area_Groups.csv")
    if(!file.exists(prod_file_world)){
      # Define the URL and set the save path
      url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Production_Crops_Livestock_E_All_Area_Groups.zip"
      zip_file_path <- file.path(fao_dir, "Production_Crops_Livestock_E_All_Area_Groups.zip")
      
      # Download the file
      download.file(url, zip_file_path, mode = "wb")
      
      # Unzip the file
      unzip(zip_file_path, exdir = fao_dir)
      
      # Delete the ZIP file
      unlink(zip_file_path)
    }
    # 3.5.4) value of production #####
    vop_file<-file.path(fao_dir,"Value_of_Production_E_Africa.csv")
    if(!file.exists(vop_file)){
      # Define the URL and set the save path
      url<-"https://fenixservices.fao.org/faostat/static/bulkdownloads/Value_of_Production_E_Africa.zip"
      
      zip_file_path <- file.path(fao_dir, "Value_of_Production_E_Africa.zip")
      
      # Download the file
      download.file(url, zip_file_path, mode = "wb")
      
      # Unzip the file
      unzip(zip_file_path, exdir = fao_dir)
      
      # Delete the ZIP file
      unlink(zip_file_path)
    }
    
    vop_file_world<-file.path(fao_dir,"Value_of_Production_E_All_Area_Groups.csv")
    if(!file.exists(vop_file_world)){
      # Define the URL and set the save path
      url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Value_of_Production_E_All_Area_Groups.zip"
      zip_file_path <- file.path(fao_dir, "Value_of_Production_E_All_Area_Groups.zip")
      
      # Download the file
      download.file(url, zip_file_path, mode = "wb")
      
      # Unzip the file
      unzip(zip_file_path, exdir = fao_dir)
      
      # Delete the ZIP file
      unlink(zip_file_path)
    }
    
  # 3.6) Highlands map #####
  update<-F
  
  afr_highlands_file<-file.path(afr_highlands_dir,"afr-highlands.asc")
  if(!file.exists(afr_highlands_file)|update==T){
      s3$file_download(file.path(bucket_name_s3,"afr_highlands/afr-highlands.asc"),afr_highlands_file,overwrite = T)
  }
  # 3.7) Livestock vop #####
  update<-F
  
  # Specify s3 prefix (folder path)
  folder_path <- "livestock_vop/"
  
  # List files in the specified S3 bucket and prefix
  files_s3<-s3$dir_ls(file.path(bucket_name_s3,folder_path))
  files_s3<-files_s3[grepl(".tif",files_s3)]
  files_local<-gsub(file.path(bucket_name_s3,folder_path),paste0(ls_vop_dir,"/"),files_s3)
  
  # If data does not exist locally download from S3 bucket
  for(i in 1:length(files_local)){
    file<-files_local[i]
    if(!file.exists(file)|update==T){
      s3$file_download(files_s3[i],file)
    }
  }
  
  # 3.8) Human population #####
  # There is an issue with this folder we are trying fix,we cannot seem to change the policy to public-read
  if(F){
    # Specify s3 prefix (folder path)
    folder_path <- "population/worldpop_2020/"
  
    # List files in the specified S3 bucket and prefix
    files_s3<-s3$dir_ls(file.path(bucket_name_s3,folder_path))
    files_s3<-files_s3[grepl("pop.tif",files_s3)]
    files_local<-gsub(file.path(bucket_name_s3,folder_path),paste0(hpop_dir,"/"),files_s3)
    
    for(i in 1:length(files_local)){
      file<-files_local[i]
      if(!file.exists(file)|update==T){
        s3$file_download(files_s3[i],file)
      }
    }
  }  


  # 3.9) GLPS #####
  local_dir<-glps_dir
  # List files in the specified S3 bucket and prefix
  files_s3<-s3$dir_ls(file.path(bucket_name_s3, basename(local_dir)))
  files_local<-file.path(local_dir,basename(files_s3))

  # If data does not exist locally download from S3 bucket
  for(i in 1:length(files_local)){
    file<-files_local[i]
    if(!file.exists(file)|update==T){
      s3$file_download(files_s3[i],file)
    }
  }

  # 3.10) Cattle heatstress #####
  local_dir<-cattle_heatstress_dir
  # List files in the specified S3 bucket and prefix
  files_s3<-s3$dir_ls(file.path(bucket_name_s3, basename(local_dir)))
  files_local<-file.path(local_dir,basename(files_s3))
  
  # If data does not exist locally download from S3 bucket
  for(i in 1:length(files_local)){
    file<-files_local[i]
    if(!file.exists(file)|update==T){
      s3$file_download(files_s3[i],file)
    }
  }
  # 3.11) SOS #####
  local_dir<-sos_dir
  # List files in the specified S3 bucket and prefix
  files_s3<-s3$dir_ls(file.path(bucket_name_s3, basename(local_dir)))
  files_local<-file.path(local_dir,basename(files_s3))
  
  # If data does not exist locally download from S3 bucket
  for(i in 1:length(files_local)){
    file<-files_local[i]
    if(!file.exists(file)|update==T){
      s3$file_download(files_s3[i],file)
    }
  }
  
  # 3.12) GGCMI crop calendars #####
  update<-F
  if(length(list.files(ggcmi_dir))!=40){
  url <- "https://www.pik-potsdam.de/~jonasjae/GGCMI_Phase3_crop_calendar"
  webpage <- rvest::read_html(url)
  
  file_links <- webpage %>%
    rvest::html_nodes("a") %>%
    rvest::html_attr("href") %>%
    grep("\\.nc4$", ., value = TRUE) # Only keep .nc4 files
  
  file_links<-file.path(url,file_links)
  
  for(i in 1:length(file_links)){
    cat(sprintf("\rDownloading GGCMI file %d/%d", i, length(file_links)))
    file<-file.path(ggcmi_dir,basename(file_links[i]))
    if(!file.exists(file)|update==T){
      download.file(file_links[i],file)
    }
  }
  
  }
  
  # 3.13) Hydrobasins #####
  # https://grdc.bafg.de/GRDC/EN/02_srvcs/22_gslrs/223_WMO/wmo_regions_node.html
  if(!file.exists(file.path(hydrobasins_dir,"wmobb_rivnets_Q00_01.json"))){
    url<-"https://grdc.bafg.de/downloads/wmobb_json.zip"
    local_path<-file.path(hydrobasins_dir,basename(url))
    download.file(url,local_path)
    unzip(local_path,exdir=dirname(local_path))
    unlink(local_path)
  }
  # 3.14) Solution tables #####
  update<-F
  local_dir<-solution_tables_dir
  # List files in the specified S3 bucket and prefix
  files_s3<-s3$dir_ls(file.path(bucket_name_s3, basename(local_dir)))
  files_local<-file.path(local_dir,basename(files_s3))
  
  # If data does not exist locally download from S3 bucket
  for(i in 1:length(files_local)){
    file<-files_local[i]
    if(!file.exists(file)|update==T){
      s3$file_download(files_s3[i],file)
    }
  }
  
# 4) Set data urls ####
  # 4.1) hazard class #####
  haz_class_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_classes.csv"
  # 4.2) hazard metadata #####
  haz_meta_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_metadata.csv"
  # 4.3) mapspam codes #####
  ms_codes_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SpamCodes.csv"
  spam2fao_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SPAM2010_FAO_crops.csv"
  # 4.4) ecocrop ####
  ecocrop_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/ecocrop.csv"
  
  # 4.5) isimip metadata #####
  isimip_meta_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/isimip_water_var_metadata.csv"


  
  
  