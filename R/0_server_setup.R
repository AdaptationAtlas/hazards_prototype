# 0) Load packages and functions

# Increase download timeout
options(timeout = 600)

# Install and load pacman if not already installed
if (!require("pacman", character.only = TRUE)) {
  install.packages("pacman")
  library(pacman)
}

# List of packages to be loaded
packages <- c("remotes","data.table","httr","s3fs","xml2")

# Use pacman to install and load the packages
pacman::p_load(char=packages)

# Install package for exactextractr
if (!require("exactextractr", character.only = TRUE)) {
  remotes::install_github("isciences/exactextractr")
}

# Source functions used in this workflow
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

# 1) Setup server####
# Choose season calculation method
timeframe_choices<-c("annual","jagermeyr","sos_primary_eos",
                     "sos_primary_fixed_3","sos_primary_fixed_4","sos_primary_fixed_5",
                     "sos_secondary_fixed_3","sos_secondary_fixed_4","sos_secondary_fixed_5")

timeframe_choice<-timeframe_choices[4]

# Increase GDAL cache size
terra::gdalCache(60000)

# workers
parallel::detectCores()
terra::free_RAM()/10^6

worker_n<-20

# Record R-project location
if(!exists("package_dir")){
  package_dir<-getwd()
}

# 1.1) Where should workflow outputs be stored? #####

# Cglabs
working_dir<-"/home/jovyan/common_data/hazards_prototype"
# Local
working_dir<-"D:/common_data/hazards_prototype"
# Afrilabs
working_dir<-"/home/psteward/common_data"

if(!dir.exists(working_dir)){
  dir.create(working_dir,recursive=T)
}

setwd(working_dir)

# Where is the raw monthly hazards data stored?
# Generated from https://github.com/AdaptationAtlas/hazards/tree/main is stored
indices_dir<-"/home/jovyan/common_data/atlas_hazards/cmip6/indices"
indices_dir2<-"/home/jovyan/common_data/atlas_hazards/cmip6/indices_seasonal"

if(timeframe_choice!="annual"){
  indices_seasonal_dir<-paste0(indices_dir2,"/by_season/",timeframe_choice,"/hazard_timeseries")
}else{
  indices_seasonal_dir<-paste0(indices_dir2,"/by_year/hazard_timeseries")
}

# 2) Set directories ####
# 2.1) Local folders #####
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

geo_dir<-"Data/boundaries"

if(!dir.exists(geo_dir)){
  dir.create(geo_dir)
}

# Inputs
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

boundary_dir<-"Data/boundaries"
if(!dir.exists(boundary_dir)){
  dir.create(boundary_dir)
}

glw_dir<-"Data/GLW4"

ls_vop_dir<-"Data/livestock_vop"

afr_highlands_dir<-"Data/afr_highlands"

fao_dir<-"Data/fao"
if(!dir.exists(fao_dir)){
  dir.create(fao_dir,recursive = T)
}

mapspam_dir<-"Data/mapspam/2020V1r0_SSA"
if(!dir.exists(mapspam_dir)){
  dir.create(mapspam_dir)
}

# Set sos calendar directory
sos_dir<-"/home/jovyan/common_data/atlas_sos/seasonal_mean"


# 2.2) Atlas s3 bucket #####
bucket_name <- "http://digital-atlas.s3.amazonaws.com"
bucket_name_s3<-"s3://digital-atlas"
s3<-s3fs::S3FileSystem$new(anonymous = T)
# 3) Download key datasets ####
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
folder_path <- "MapSpam/raw/2020V1r0_SSA/"

# List files in the specified S3 bucket and prefix
files_s3 <- list_s3_bucket_contents(bucket_url=bucket_name, folder_path = folder_path)
files_s3<-files_s3[grepl(".csv",files_s3) & !grepl("index",files_s3)]
files_s3<-gsub(bucket_name,bucket_name_s3,files_s3)
files_local<-gsub(file.path(bucket_name_s3,folder_path),paste0(ls_vop_dir),files_s3)

# If mapspam data does not exist locally download from S3 bucket
lapply(1:length(files_local),FUN=function(i){
  file<-files_local[i]
  if(!file.exists(file)|update==T){
    s3$file_download(files_s3[i],file)
  }
})

# 3.3) Base Raster #####
# Load base raster to which other datasets are resampled to
base_raster<-"base_raster.tif"
if(!file.exists(base_raster)){
  url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/base_raster.tif"
  httr::GET(url, httr::write_disk(base_raster, overwrite = TRUE))
}

base_rast<-terra::rast(base_raster)
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

# 3.5) Fao stat deflators #####
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

# 3.6) Highlands map #####
update<-F

afr_highlands_file<-file.path(afr_highlands_dir,"afr-highlands.asc")

if(!file.exists(afr_highlands_file)|update==T){
  download.file(url="https://digital-atlas.s3.amazonaws.com/afr_highlands/afr-highlands.asc",
                destfile=afr_highlands_file)
}
# 3.7) Livestock vop #####
update<-F

# Specify s3 prefix (folder path)
folder_path <- "livestock_vop/"

# List files in the specified S3 bucket and prefix
files_s3 <- list_s3_bucket_contents(bucket_url=bucket_name, folder_path = folder_path)
files_s3<-files_s3[grepl(".tif",files_s3)]
files_s3<-gsub(bucket_name,bucket_name_s3,files_s3)
files_local<-gsub(file.path(bucket_name_s3,folder_path),paste0(ls_vop_dir),files_s3)

# If data does not exist locally download from S3 bucket
for(i in 1:length(files_local)){
  file<-files_local[i]
  if(!file.exists(file)|update==T){
    s3$file_download(files_s3[i],file)
  }
}

# 3.8) Human population #####
# Specify s3 prefix (folder path)
folder_path <- "population/"

# List files in the specified S3 bucket and prefix
files_s3 <- list_s3_bucket_contents(bucket_url=bucket_name, folder_path = folder_path)
files_s3<-files_s3[grepl(".tif",files_s3)]
files_s3<-gsub(bucket_name,bucket_name_s3,files_s3)
files_local<-gsub(file.path(bucket_name_s3,folder_path),paste0(hpop_dir),files_s3)

for(i in 1:length(files_local)){
  file<-files_local[i]
  if(!file.exists(file)|update==T){
    s3$file_download(files_s3[i],file)
  }
}

# 4) Set data paths ####
# 4.1) hazard class #####
haz_class_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_classes.csv"
# 4.2) hazard metadata #####
haz_meta_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_metadata.csv"
# 4.3) mapspam codes #####
ms_codes_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SpamCodes.csv"
# 4.4) ecocrop ####
ecocrop_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/ecocrop.csv"
