# Please run 0_server_setup.R before executing this script
# 0) Set-up workspace
  # 0.1) Install and load packages ####
  load_and_install_packages <- function(packages) {
    for (package in packages) {
      if (!require(package, character.only = TRUE)) {
        install.packages(package)
        library(package, character.only = TRUE)
      }
    }
  }
  
  # List of packages to be loaded
  packages <- c("s3fs",
                "aws.s3",
                "paws",
                "pbapply", 
                "future", 
                "future.apply", 
                "gdalUtilities", 
                "progressr",
                "terra")
  
  # Call the function to install and load packages
  pacman::p_load(char=packages)
  
  # 0.2) Set workers for parallel processing ####
  worker_n<-10
  
 # 1) General ####
  ## Upload - exposure ####
  s3_bucket <-"s3://digital-atlas/risk_prototype/data/exposure"
  folder<-"Data/exposure"
  
  s3_dir_ls(s3_bucket)
  
  # Prepare tif data by converting to COG format
  #ctc_wrapper(folder=folder,worker_n=1,delete=T,rename=T)
  
  files<-list.files(folder,full.names = T)
  files<-grep("exposure_adm_sum.parquet$",files,value=T)
  
  # Upload files
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
  
  # Processed livestock data parquets
  s3_bucket<-"s3://digital-atlas/exposure/livestock/processed"

  files<-list.files(folder,"livestock_vop",full.names = T)
  files<-grep("parquet$",files,value = T)
  s3_file_names<-gsub("_sum","",basename(files))
  
  upload_files_to_s3(files = files,
                     s3_file_names = s3_file_names,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
  
  # Processed livestock data tifs
  files<-list.files(folder,"livestock_vop",full.names = T)
  files<-grep("tif$",files,value = T)
  
  ctc_wrapper(files=files)
  
  s3_file_names<-gsub("_sum","",basename(files))

  upload_files_to_s3(files = files,
                     s3_file_names = s3_file_names,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F,
                     convert2cog = T,
                     mode="public-read")
  
  ## Upload - metadata ####
  # select a folder
  folder<-"metadata"
  # select a bucket
  s3_bucket <-"s3://digital-atlas/risk_prototype/data/metadata"
  
  ctc_wrapper(folder=folder,worker_n=1,delete=T,rename=T)
  
  # Upload files
  upload_files_to_s3(folder = folder,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F)
  
  # Upload - hazard_mean
  # Select a local folder
  folder<-paste0("Data/hazard_risk_vop/",timeframe_choice)
  
  # select a bucket
  selected_bucket <-paste0("s3://digital-atlas/risk_prototype/data/hazard_risk_vop/",timeframe_choice)
  
  # Create the new directory in the selected bucket
  if(!s3_dir_exists(selected_bucket)){
    s3_dir_create(selected_bucket)
  }
  
  # Select a folder to upload
  folder<-paste0("Data/hazard_risk_vop/",timeframe_choice)
  
  # Prepare tif data by converting to COG format
  # List tif files in the folder
  files_tif<-list.files(folder,".tif",full.names = T)
  # Remove any COGs from the tif list
  files_tif<-files_tif[!grepl("_COG.tif",files_tif)]
  
  # Set up parallel backend
  plan(multisession,workers=10)  # Change to multicore on Unix/Linux
  
  # Apply the function to each file
  future_sapply(files_tif, convert_to_cog,future.packages = c("gdalUtilities","terra"),delete=T,rename=T)
  
  plan(sequential)
  closeAllConnections()
  
  # Tifs 
  upload_files_to_s3(files = list.files(folder, pattern = "\\.tif$", full.names = TRUE),
                     selected_bucket=selected_bucket,
                     max_attempts = 3,
                     new_only=T)
  ## Upload - MapSPAM ####
  folder<-mapspam_dir
  s3_bucket <- file.path("s3://digital-atlas/MapSpam/raw",basename(mapspam_dir))
  
  files<-list.files(folder,full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F,
                     mode="public-read")
  
  ## Upload - livestock_vop ####
  folder<-ls_vop_dir
  s3_bucket <- "s3://digital-atlas/livestock_vop"
  
  s3_dir_ls(s3_bucket)
  
  # Prepare tif data by converting to COG format
  #ctc_wrapper(folder=folder,worker_n=1,delete=T,rename=T)
  files<-list.files(folder,".tif$",full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F,
                     mode="public-read")
  
  # You will need to run the function above again with overwrite=F to add the index
  index<-data.table(s3_path=s3_dir_ls(s3_bucket))[!grepl("index.csv",s3_path)]
  index[,s3_path:=gsub("s3://digital-atlas","https://digital-atlas.s3.amazonaws.com",s3_path)]
  fwrite(index,file.path(folder,"index.csv"))
  
  ## Upload - livestock afr-highlands ####
  folder<-afr_highlands_dir
  s3_bucket <- "s3://digital-atlas/afr_highlands"
  
  upload_files_to_s3(folder = folder,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
  # Geographies
  s3_bucket <- "s3://digital-atlas/boundaries"
  s3_dir_ls(s3_bucket)
  
  ## Upload - human population ####
  folder<-hpop_dir
  s3_bucket <- file.path(bucket_name_s3,"population/worldpop_2020")
  
  files<-list.files(folder,".tif$",full.names = T)
  
  upload_files_to_s3(files = files,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=F,
                   mode="public-read")

  ## Upload - glps #####
  folder<-glps_dir
  s3_bucket <- file.path(bucket_name_s3,basename(folder))
  
  files<-list.files(folder,".tif$",full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F,
                     mode="public-read")
  
  ## Upload - cattle heatstress data #####
  folder<-cattle_heatstress_dir
  s3_bucket <- file.path(bucket_name_s3,basename(folder))
  
  files<-list.files(folder,full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
  
  ## Upload - sos raster #####
  folder<-sos_dir
  s3_bucket <- file.path(bucket_name_s3,basename(folder))
  
  files<-list.files(folder,full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F,
                     mode="public-read")

  # Upload - solutions
  folder<-solution_tables_dir
  s3_bucket <- file.path(bucket_name_s3,basename(folder))
  
  files<-list.files(folder,".csv$",full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F,
                     mode="public-read")
  
  # Upload - boundaries
  folder<-geo_dir
  s3_bucket <- file.path(bucket_name_s3,"boundaries")
  
  files<-list.files(folder,"cgiar_countries",full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F,
                     workers=1,
                     mode="public-read")
  
# 2) Time sequence specific ####
  # Next task for enhanced generalization is to create folder vector and integrate into the loop (rather than have many sections)
  overwrite<-F
  worker_n<-15
  convert2cog<-F # Currently generates an error when run in parallel
  permission<-"public-read"
  #file_types<-"parquet"
  file_types<-"tif"
  file_types<-paste(paste0(file_types,"$"),collapse = "|")
  
  # Modify if you do not want to run all timeframes
  timeframe_choices_local<-timeframe_choices_local[1]
  
  # Timeframe loop
  for(timeframe_c in timeframe_choices_local){
    cat(timeframe_c,"\n")
  
  # 2.1) Upload - hazard timeseries ####
  cat(timeframe_c,"2.1 hazard timeseries \n")
  folder<-file.path("Data/hazard_timeseries",timeframe_c)
  s3_bucket <-file.path("s3://digital-atlas/risk_prototype/data/hazard_timeseries",timeframe_c)
  
  # Local files
  local_files<-list.files(folder,file_types,full.names = T)

  # Upload files
  upload_files_to_s3(files = local_files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=overwrite,
                     workers=worker_n,
                     convert2cog = convert2cog,
                     mode=permission)
  
  # 2.2) Upload - hazard classified ####
  cat(timeframe_c,"2.2 hazard classified \n")
  folder<-file.path("Data/hazard_timeseries_class",timeframe_c)
  s3_bucket <-file.path("s3://digital-atlas/risk_prototype/data/hazard_timeseries_class",timeframe_c)
  
  # Local files
  local_files<-list.files(folder,file_types,full.names = T)
  
  # Upload files
  upload_files_to_s3(files = local_files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=overwrite,
                     workers=worker_n,
                     convert2cog=convert2cog,
                     mode=permission)
  
  # 2.3) Upload - hazard timeseries mean ####
  cat(timeframe_c,"2.3 hazard timeseries mean \n")
  folder<-file.path("Data/hazard_timeseries_mean",timeframe_c)
  s3_bucket <-file.path("s3://digital-atlas/risk_prototype/data/hazard_timeseries_mean",timeframe_c)

  # Upload files
  local_files<-list.files(folder,file_types,full.names = T)
  
  upload_files_to_s3(files=local_files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     workers=worker_n,
                     mode=permission,
                     convert2cog = convert2cog,
                     overwrite=overwrite)
  
  # 2.4) Upload - hazard_timeseries_risk ####
  cat(timeframe_c,"2.4 hazard_timeseries_risk \n")
  folder<-file.path("Data/hazard_timeseries_risk",timeframe_c)
  s3_bucket <-file.path("s3://digital-atlas/risk_prototype/data/hazard_timeseries_risk",timeframe_c)

  # Local files
  local_files<-list.files(folder,file_types,full.names = T)
  
  upload_files_to_s3(files=local_files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     workers=worker_n,
                     convert2cog = convert_to_cog,
                     mode=permission,
                     overwrite=overwrite)
  
  # 2.5) Upload - hazard_timeseries_int ####
  cat(timeframe_c,"2.5 hazard_timeseries_int \n")
  folder<-file.path("Data/hazard_timeseries_int",timeframe_c)
  s3_bucket <-file.path("s3://digital-atlas/risk_prototype/data/hazard_timeseries_int",timeframe_c)
  
  # Upload files
  local_folders<-list.dirs(folder)
  local_folders<-local_folders[local_folders!=folder]
  for(i in 1:length(local_folders)){
    cat(i,"/",length(local_folders),"\n")
    FOLDER<-local_folders[i]
    S3_BUCKET<-gsub(folder,s3_bucket,FOLDER)
    
    LOCAL_FILES<-paste0(basename(FOLDER),"/",list.files(FOLDER,file_types))
    
    if(length(LOCAL_FILES)>0){
      upload_files_to_s3(files = LOCAL_FILES,
                         selected_bucket=S3_BUCKET,
                         max_attempts = 3,
                         convert2cog = convert2cog,
                         overwrite=overwrite,
                         mode=permission,
                         workers = worker_n)
    }
  }
  
  # 2.6) Upload - hazard_timeseries_risk sd ####
  cat(timeframe_c,"2.6 hazard_timeseries_risk sd \n")
  folder<-file.path("Data/hazard_timeseries_sd",timeframe_c)
  # select a bucket
  s3_bucket <-file.path("s3://digital-atlas/risk_prototype/data/hazard_timeseries_sd",timeframe_c)
  
  # Local files
  local_files<-list.files(folder,file_types,full.names = T)
  
  # Upload files
  if(length(local_files)>0){
  upload_files_to_s3(files = local_files,
                     selected_bucket=s3_bucket,
                     convert2cog = convert2cog,
                     overwrite=overwrite,
                     mode=permission,
                     workers = worker_n)
  }
  
  # 2.7) Upload - haz_risk ####
  cat(timeframe_c,"2.7 haz_risk \n")
  s3_bucket <-file.path("s3://digital-atlas/risk_prototype/data/hazard_risk",timeframe_c)
  folder<-file.path("Data/hazard_risk",timeframe_c)
  
  # Local files
  local_files<-list.files(folder,file_types,full.names = T)

  # Upload files
  upload_files_to_s3(files = local_files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     convert2cog = convert2cog,
                     overwrite=overwrite,
                     mode=permission,
                     workers = worker_n
                     )
  
  # 2.8) Upload - haz_vop_risk ####
  cat(timeframe_c,"2.8 haz_vop_risk \n")
  
  folder<-file.path("Data/hazard_risk_vop",timeframe_c)
  s3_bucket <-file.path("s3://digital-atlas/risk_prototype/data/hazard_risk_vop",timeframe_c)
  
  # Local files
  local_files<-list.files(folder,file_types,full.names = T)
  
  #s3_files<-s3fs::s3_dir_ls(s3_bucket)
  #s3_files<-grep(".tif",s3_files,value=T)
  #s3_file_delete(s3_files)
  
  upload_files_to_s3(files = local_files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     convert2cog = convert2cog,
                     overwrite=overwrite,
                     mode=permission,
                     workers = worker_n)
  
  # 2.9) Upload - haz_vop_usd_risk ####
  cat(timeframe_c,"2.9 haz_vop_usd_risk \n")
  
  folder<-file.path("Data/hazard_risk_vop_usd",timeframe_c)
  s3_bucket <-file.path("s3://digital-atlas/risk_prototype/data/hazard_risk_vop_usd",timeframe_c)
  
  # Local files
  local_files<-list.files(folder,file_types,full.names = T)

  upload_files_to_s3(files = local_files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     convert2cog = convert2cog,
                     overwrite=overwrite,
                     mode=permission,
                     workers = worker_n)
  
  # 2.10) Upload - haz_vop_risk_ac ####
  if(F){
    cat(timeframe_c,"2.10 haz_vop_risk_ac \n")
    
    
  s3_bucket <- paste0("s3://digital-atlas/risk_prototype/data/hazard_risk_vop_ac/",timeframe_c)
  folder<-paste0("Data/hazard_risk_vop_ac/",timeframe_c)
  
  local_files<-list.files(folder,full.names = T)
  
  if(upload_parquet_only){
    local_files<-grep("parquet",local_files,value=T)
  }

  # Upload files
  upload_files_to_s3(files=list.files(folder,".parquet",full.names = T),
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     convert2cog = convert2cog,
                     overwrite=overwrite,
                     mode=permission,
                     workers = worker_n)
  }

  }
  
# 3) ROI data ####
  s3_bucket <- paste0("s3://digital-atlas/risk_prototype/data/roi")
  folder<-roi_dir
  
  s3_dir_ls(s3_bucket)
  
  # Upload files
  upload_files_to_s3(files=list.files(folder,".parquet$",full.names = T),
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T)
  
# 4) Hazard_timeseries data (datasets from Julian's hazard workflow) ####
  # 4.1) Upload - hazard_timeseries
  ## 4.2) Upload - hazard timeseries mean monthly #####
  folder<-haz_timeseries_monthly_dir
  s3_bucket<-"s3://digital-atlas/hazards/hazard_timeseries_mean_month"
  
  files<-list.files(folder,"parquet",full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F,
                     mode="public-read",
                     workers=worker_n)
  
  s3_dir_ls(s3_bucket)
  
  ## 4.3) Upload - ptot change #####
  folder<-haz_mean_ptot_dir
  s3_bucket<-"s3://digital-atlas/hazards/hazard_timeseries_mean_month/ptot_change"
  
  files<-list.files(folder,"parquet$",full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
  
  s3_dir_ls(s3_bucket)
  
  ## 4.3) Upload - THI area vs severity #####
  folder<-haz_mean_thi_dir
  s3_bucket<-"s3://digital-atlas/hazards/hazard_timeseries_mean_month/thi_perc"
  
  files<-list.files(folder,"parquet$",full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
  
  
# 5) Isimip ####
  ## 5.1) Upload - isimip timeseries mean #####
  folder<-isimip_timeseries_mean_dir
  s3_bucket<-file.path(bucket_name_s3,"hazards",basename(folder),timeframe_choice)
  folder<-file.path(folder,timeframe_choice)

  files<-list.files(folder,".parquet",full.names=T)
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
  
  ## 5.2) Upload - extracted data #####
  folder<-isimip_mean_dir
  s3_bucket<-file.path(bucket_name_s3,"hazards",basename(folder),timeframe_choice)
  folder<-file.path(folder,timeframe_choice)
  
  files<-list.files(folder,"_adm_",full.names=T)

  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
# 6) CropSuite ####
  s3_bucket<-file.path(bucket_name_s3,"productivity","CropSuite","processed")
  
  folder<-cropsuite_class_dir
  
  files<-list.files(folder,"_adm_",full.names=T)
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
  s3$dir_ls(s3_bucket)
  
# 7) Chirps/chirts historical differences #####
  s3_bucket<-file.path(bucket_name_s3,"hazards","chirps_chirts")
  
  folder<-chirts_chirps_dir
  
  files<-list.files(folder,".parquet",full.names=T)
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
  
# 8) CHIRPS global CV (eia_climate_prioritization) ####
# pete macbook path
folder<-"/Users/pstewarda/Documents/rprojects/climate_prioritization/raw_data/chirps_cv"
s3_bucket <-file.path("s3://digital-atlas/hazards/chirps_cv_global")

# Local files
local_files<-list.files(folder,full.names = T)

upload_files_to_s3(files = local_files,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=T,
                   mode="public-read",
                   workers = worker_n)

# 9) eia_climate_prioritization ####
  ## 9.1) ERA5 NTx global (eia_climate_prioritization) ####
  # cg_labs path
  folder<-"/home/jovyan/common_data/EiA_pub"
  s3_bucket <-file.path("s3://digital-atlas/hazards/agera5_ntx_global")
  
  # Local files
  local_files<-list.files(folder,full.names = T)
  
  upload_files_to_s3(files = local_files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=overwrite,
                     mode="public-read",
                     workers = worker_n)
  
  ## 9.2) GDO drought indices (eia_climate_prioritization) ####
  # cg_labs path
  folder<-"/Users/pstewarda/Documents/rprojects/climate_prioritization/raw_data/drought_observatory"
  s3_bucket <-file.path("s3://digital-atlas/hazards/global_drought_observatory")
  
  # Local files
  local_files<-list.files(folder,full.names = T)
  
  upload_files_to_s3(files = local_files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read",
                     workers = worker_n)
  
  ## 9.3) GAEZ LGP (eia_climate_prioritization) ####
  # cg_labs path
  folder<-"/Users/pstewarda/Documents/rprojects/climate_prioritization/raw_data/gaez"
  s3_bucket <-"s3://digital-atlas/hazards/gaez_lgp"
  
  # Local files
  local_files<-list.files(folder,full.names = T)
  
  upload_files_to_s3(files = local_files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read",
                     workers = worker_n)
  
  ## 9.4) Spam (eia_climate_prioritization) #####
  # cg_labs path
  folder<-"/Users/pstewarda/Documents/rprojects/climate_prioritization/raw_data/SPAM"
  s3_bucket <-"s3://digital-atlas/exposure/mapspam/eia_climate_prioritization"
  
  # Local files
  local_files<-list.files(folder,"tif$",full.names = T)
  
  upload_files_to_s3(files = local_files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read",
                     workers = worker_n)
  
  ## 9.5) Countries (eia_climate_prioritization) #####
  # cg_labs path
  folder<-"/Users/pstewarda/Documents/rprojects/climate_prioritization/raw_data/boundaries"
  s3_bucket <-"s3://digital-atlas/boundaries/eia_climate_prioritization"
  
  s3_dir_ls(s3_bucket)
  
  # Local files
  local_files<-list.files(folder,full.names = T)
  
  upload_files_to_s3(files = local_files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read",
                     workers = worker_n)
  