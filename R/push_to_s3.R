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
  
  # 0.3) Create functions####
  # Update tifs to cog format
  convert_to_cog <- function(file) {
    
    is_cog<-grepl("LAYOUT=COG",gdalUtilities::gdalinfo(file))
    closeAllConnections()
    
    if(is_cog==F){
  
        data<-terra::rast(file)
        # Force into memory
        data<-data+0
        
       terra::writeRaster(data,filename = file,filetype = 'COG',gdal=c("COMPRESS=LZW",of="COG"),overwrite=T)
  
      }
    }
  
  ctc_wrapper<-function(folder=NULL,files=NULL,worker_n=1){
    
    if(is.null(files)){
      # List tif files in the folder
      files_tif<-list.files(folder,".tif",full.names = T)
    }else{
      files_tif<-files
    }
  
    if(worker_n>1){
      # Update tifs to cog format
      # Set up parallel backend
      future::plan(multisession,workers=worker_n)  # Change to multicore on Unix/Linux
      
      # Apply the function to each file
      future.apply::future_sapply(files_tif, convert_to_cog,future.packages = c("gdalUtilities","terra"),delete=delete,rename=rename)
      
      future::plan(sequential)
      closeAllConnections()
    }else{
      pbapply::pbsapply(files_tif,convert_to_cog)
    }
  }
  
# 1) General ####
  # Upload - exposure ####
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
                     mode="public-read")
  
  # Upload - metadata ####
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
  # Upload - MapSPAM ####
  folder<-mapspam_dir
  s3_bucket <- file.path("s3://digital-atlas/MapSpam/raw",basename(mapspam_dir))
  
  files<-list.files(folder,full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F,
                     mode="public-read")
  
  # Upload - livestock_vop ####
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
  
  # Upload - livestock afr-highlands ####
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
  
  # Upload - human population ####
  folder<-hpop_dir
  s3_bucket <- file.path(bucket_name_s3,"population/worldpop_2020")
  
  files<-list.files(folder,".tif$",full.names = T)
  
  upload_files_to_s3(files = files,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=F,
                   mode="public-read")

  # Upload - glps #####
  folder<-glps_dir
  s3_bucket <- file.path(bucket_name_s3,basename(folder))
  
  files<-list.files(folder,".tif$",full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F,
                     mode="public-read")
  
  # Upload - cattle heatstress data #####
  folder<-cattle_heatstress_dir
  s3_bucket <- file.path(bucket_name_s3,basename(folder))
  
  files<-list.files(folder,full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
  
  # Upload - sos raster #####
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
  
# 2) Time sequence specific ####
  # 2.1) Upload - hazard timeseries (parquets) ####
  folder<-paste0("Data/hazard_timeseries/",timeframe_choice)
  s3_bucket <-paste0("s3://digital-atlas/risk_prototype/data/hazard_timeseries/",timeframe_choice)
  
  # Local files
  local_files<-list.files(folder,full.names = T)
  
  # Upload files
  upload_files_to_s3(files = local_files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F,
                     workers=15,
                     mode="public-read")
  
  # 2.2) Upload - hazard classified ####
  folder<-paste0("Data/hazard_timeseries_class/",timeframe_choice)
  s3_bucket <-paste0("s3://digital-atlas/risk_prototype/data/hazard_timeseries_class/",timeframe_choice)
  
  # Local files
  local_files<-list.files(folder,full.names = T)
  
  if(F){
  # Prepare tif data by converting to COG format
  ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)
  }
  
  # Upload files
  upload_files_to_s3(files = local_files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F,
                     workers=4,
                     mode="public-read")
  
  # 2.3) Upload - hazard timeseries mean ####
  folder<-paste0("Data/hazard_timeseries_mean/",timeframe_choice)
  s3_bucket <-paste0("s3://digital-atlas/risk_prototype/data/hazard_timeseries_mean/",timeframe_choice)

  s3_dir_ls(s3_bucket)
  
    # Prepare tif data by converting to COG format
  if(F){
    ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)
  }
  # Upload files
  files<-list.files(folder,full.names = T)
  
  upload_files_to_s3(files=files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     workers=5,
                     mode="public-read",
                     overwrite=F)
  
  # 2.4) Upload - hazard_timeseries_risk ####
  folder<-paste0("Data/hazard_timeseries_risk/",timeframe_choice)
  s3_bucket <-paste0("s3://digital-atlas/risk_prototype/data/hazard_timeseries_risk/",timeframe_choice)

  # Prepare tif data by converting to COG format
  if(F){
  ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)
  }
  
  # Upload files
  files<-list.files(folder,full.names = T)
  
  upload_files_to_s3(files=files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     workers=5,
                     mode="public-read",
                     overwrite=F)
  
  # 2.5) Upload - hazard_timeseries_int ####
  folder<-paste0("Data/hazard_timeseries_int/",timeframe_choice)
  s3_bucket <-paste0("s3://digital-atlas/risk_prototype/data/hazard_timeseries_int/",timeframe_choice)

  # Prepare tif data by converting to COG format
  if(F){
  ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)
  }
  
  s3_dir_ls("digital-atlas",prefix="risk_prototype/data/hazard_timeseries_int/jagermeyr")
  s3_dir_delete("s3://digital-atlas/risk_prototype/data/hazard_timeseries_int/jagermeyr/NDWS-G15+NTx25-G7+NDWL0-G2")
  
  # Upload files
  local_folders<-list.dirs(folder)[-1]
  for(i in 1:length(local_folders)){
    cat(i,"/",length(local_folders),"\n")
    FOLDER<-local_folders[i]
    S3_BUCKET<-gsub(folder,s3_bucket,FOLDER)
    
    LOCAL_FILES<-paste0(basename(FOLDER),"/",list.files(FOLDER))
    LOCAL_FILES<-file.path(folder,LOCAL_FILES[!LOCAL_FILES %in% s3_files])
    
    if(length(LOCAL_FILES)>0){
      upload_files_to_s3(files = LOCAL_FILES,
                         selected_bucket=S3_BUCKET,
                         max_attempts = 3,
                         overwrite=T,
                         mode="public-read",
                         workers = 15)
    }
  }
  
  # 2.6) Upload - hazard_timeseries_risk sd ####
  folder<-paste0("Data/hazard_timeseries_sd/",timeframe_choice)
  # select a bucket
  s3_bucket <-paste0("s3://digital-atlas/risk_prototype/data/hazard_timeseries_sd/",timeframe_choice)
  
  # Prepare tif data by converting to COG format
  if(F){
  ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)
  }
  
  # Upload files
  upload_files_to_s3(folder = folder,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F)
  
  # 2.7) Upload - haz_risk ####
  s3_bucket <-paste0("s3://digital-atlas/risk_prototype/data/hazard_risk/",timeframe_choice)
  folder<-paste0("Data/hazard_risk/",timeframe_choice)
  
  s3fs::s3_dir_ls(s3_bucket)
  
  files<-list.files(folder,"parquet$",full.names = T,recursive=T)
  files<-grep("_adm_",files,value=T)
  
  # Upload files
  upload_files_to_s3(files =files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read"
                     )
  
  # 2.8) Upload - haz_vop_risk ####
  folder<-file.path("Data/hazard_risk_vop/",timeframe_choice)
  s3_bucket <-file.path("s3://digital-atlas/risk_prototype/data/hazard_risk_vop/",timeframe_choice)
  
  # Prepare tif data by converting to COG format
  if(F){
    ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)
  }
  
  if(F){
  s3_files<-s3fs::s3_dir_ls(s3_bucket)
  s3_files<-grep("[.]parquet",s3_files,value = T)
  s3_file_delete(s3_files)
  }
  
  files<-list.files(folder,full.names = T)
  files<-grep(".parquet",files,value=T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F,
                     mode="public-read",
                     workers=10)
  
  # 2.9) Upload - haz_vop17_risk ####
  folder<-file.path("Data/hazard_risk_vop17/",timeframe_choice)
  s3_bucket <-file.path("s3://digital-atlas/risk_prototype/data/hazard_risk_vop17/",timeframe_choice)

  files<-list.files(folder,full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F,
                     mode="public-read",
                     workers=15)
  
  
  # 2.10) Upload - haz_vop_risk_ac ####
  s3_bucket <- paste0("s3://digital-atlas/risk_prototype/data/hazard_risk_vop_ac/",timeframe_choice)
  folder<-paste0("Data/hazard_risk_vop_ac/",timeframe_choice)
  
  s3_dir_ls(s3_bucket)
  s3fs::s3_dir_delete(s3_bucket)

  # Upload files
  upload_files_to_s3(files=list.files(folder,".parquet",full.names = T),
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F)
  
  #file<-grep("reduced",s3_dir_ls(s3_bucket),value=T)
  #s3_file_download(file,new_path="haz_risk_vop_int_ac_reduced.parquet")
  
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
  # 4.1) Upload - hazard_timeseries #####
  s3_bucket <-haz_timeseries_s3_dir
  # make sure the folder is set to the atlas_hazards/cmip6/indices server folder
  folder<-indices_seasonal_dir
  
  s3_dir_ls(s3_bucket)
  
  # Updated hazards
  files<-list.files(folder,"tif$",full.names = T)
  # files<-grep("ENSEMBLEmean|ENSEMBLEsd|historic",files,value = T)
  haz<-paste0(paste(paste0("NTx",20:50,"_mean"),collapse="|"),"|NDWL0_mean|NDWS_mean|PTOT_sum|TAVG_mean|HSH_max_max|HSH_mean_mean|THI_mean_mean|THI_max_max|TAI_mean")
  files<-grep(haz,files,value=T)
  
  # Upload files
  upload_files_to_s3(files=files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F,
                     mode="public-read")
  

# 5) !!!***TO DO***!!! raw data by season
  # 4.2) Upload - hazard timeseries mean monthly #####
  folder<-haz_timeseries_monthly_dir
  s3_bucket<-"s3://digital-atlas/hazards/hazard_timeseries_mean_month"
  
  files<-list.files(folder,"parquet",full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
  
  s3_dir_ls(s3_bucket)
  
  # 4.3) Upload - ptot change #####
  folder<-haz_mean_ptot_dir
  s3_bucket<-"s3://digital-atlas/hazards/hazard_timeseries_mean_month/ptot_change"
  
  files<-list.files(folder,"parquet$",full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
  
  s3_dir_ls(s3_bucket)
  
  # 4.3) Upload - THI area vs severity #####
  folder<-haz_mean_thi_dir
  s3_bucket<-"s3://digital-atlas/hazards/hazard_timeseries_mean_month/thi_perc"
  
  files<-list.files(folder,"parquet$",full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
  
  
# 5) Isimip ####
  # 5.1) Upload - isimip timeseries mean #####
  folder<-isimip_timeseries_mean_dir
  s3_bucket<-file.path(bucket_name_s3,"hazards",basename(folder),timeframe_choice)
  folder<-file.path(folder,timeframe_choice)

  files<-list.files(folder,".parquet",full.names=T)
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
  
  # 5.2) Upload - extracted data #####
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
  