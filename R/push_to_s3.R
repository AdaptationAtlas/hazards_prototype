# Install and load packages ####
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
              "pbapply", 
              "future", 
              "future.apply", 
              "gdalUtilities", 
              "terra")

# Call the function to install and load packages
load_and_install_packages(packages)

# Set workers for parallel processing ####
worker_n<-10

# Create functions####
# Update tifs to cog format
convert_to_cog <- function(file,delete=T,rename=T) {
  
  is_cog<-grepl("LAYOUT=COG",gdalUtilities::gdalinfo(file))
  closeAllConnections()
  
  if(is_cog==F){

      data<-terra::rast(file)
      # Force into memory
      data<-data+0
      
      terra::writeRaster(data,filename = file,filetype = 'COG',gdal=c("COMPRESS=LZW",of="COG"),overwrite=T)

    }
  }

ctc_wrapper<-function(folder,worker_n=1,delete=T,rename=T){
  # List tif files in the folder
  files_tif<-list.files(folder,".tif",full.names = T)

  if(worker_n>1){
    # Update tifs to cog format
    # Set up parallel backend
    future::plan(multisession,workers=worker_n)  # Change to multicore on Unix/Linux
    
    # Apply the function to each file
    future.apply::future_sapply(files_tif, convert_to_cog,future.packages = c("gdalUtilities","terra"),delete=delete,rename=rename)
    
    future::plan(sequential)
    closeAllConnections()
  }else{
    pbapply::pbsapply(files_tif,convert_to_cog,delete=delete,rename=rename)
  }
}

# Upload files S3 bucket
upload_files_to_s3 <- function(files, folder=NULL, selected_bucket, new_only=F, max_attempts = 3, overwrite=F, check_identical=T) {
  
  # Create the s3 directory if it does not already exist
  if(!s3_dir_exists(selected_bucket)){
    s3_dir_create(selected_bucket)
  }
  
  # List files if a folder location is provided
  if(!is.null(folder)){
    files <- list.files(folder, full.names = T)
  }
  
  if(overwrite==F){
    # List files in the s3 bucket
    files_s3 <- basename(s3_dir_ls(selected_bucket))
    # Remove any files that already exist in the s3 bucket
    files <- files[!basename(files) %in% files_s3]
  }
  
  for (i in seq_along(files)) {
    cat('\r', paste("File:", i, "/", length(files))," | ",basename(files[i]),"                                                 ")
    flush.console()
    
    s3_file_path <- paste0(selected_bucket, "/", basename(files[i]))
    
    if(check_identical){
      # Assume s3_file_md5 and local_file_md5 are available
      if(s3_file_exists(s3_file_path)){
        if(s3_file_md5(s3_file_path) == local_file_md5(files[i])){
          cat("Skipping identical file:", files[i], "\n")
          next
        }
      }
    }
    
    tryCatch({
      attempt <- 1
      while(attempt <= max_attempts) {
        s3_file_upload(files[i], s3_file_path, overwrite = overwrite)
        # Check if upload successful
        file_check <- s3_file_exists(s3_file_path)
        if (file_check) break # Exit the loop if upload is successful
        
        if (attempt == max_attempts && !file_check) {
          stop("File did not upload successfully after ", max_attempts, " attempts.")
        }
        attempt <- attempt + 1
      }
    }, error = function(e) {
      cat("Error during file upload:", e$message, "\n")
    })
  }
}

# 1) General ####
  # Upload - exposure ####
  s3_bucket <-"s3://digital-atlas/risk_prototype/data/exposure"
  folder<-"Data/exposure"
  
  # Prepare tif data by converting to COG format
  ctc_wrapper(folder=folder,worker_n=1,delete=T,rename=T)
  
  # Upload files
  upload_files_to_s3(files = list.files(folder,".parquet$",full.names = T),
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F)
  
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
  folder<-paste0("Data/mapspam/")
  s3_bucket <- "s3://digital-atlas/MapSpam/raw/spam2017V2r3"
  s3_dir_ls(s3_bucket)
  
  upload_files_to_s3(folder = list.files(folder,"Vusd17",full.names = T),
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T)
  
  # Upload - livestock_vop ####
  folder<-paste0("Data/livestock_vop/")
  s3_bucket <- "s3://digital-atlas/livestock_vop"
  
  s3_dir_ls(s3_bucket)
  
  # Prepare tif data by converting to COG format
  ctc_wrapper(folder=folder,worker_n=1,delete=T,rename=T)
  
  upload_files_to_s3(folder = folder,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T)
  
  # Upload - livestock afr-highlands ####
  folder<-paste0("Data/afr_highlands/")
  s3_bucket <- "s3://digital-atlas/afr_highlands"
  
  upload_files_to_s3(folder = folder,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T)
  # Geographies
  s3_bucket <- "s3://digital-atlas/boundaries"
  
  s3_dir_ls(s3_bucket)
  
# 2) Time sequence specific ####
  # Upload - hazard timeseries (parquets) ####
  folder<-paste0("Data/hazard_timeseries/",timeframe_choice)
  s3_bucket <-paste0("s3://digital-atlas/risk_prototype/data/hazard_timeseries/",timeframe_choice)
  
  s3_files<-s3_dir_ls(s3_bucket)

  # Upload files
  upload_files_to_s3(files = list.files(folder,".parquet$",full.names = T),
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F)
  
  # Upload - hazard classified ####
  folder<-paste0("Data/hazard_timeseries_class/",timeframe_choice)
  s3_bucket <-paste0("s3://digital-atlas/risk_prototype/data/hazard_timeseries_class/",timeframe_choice)
  
  # Prepare tif data by converting to COG format
  ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)
  
  # Upload files
  upload_files_to_s3(folder = folder,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F)
  
  # Upload - hazard timeseries mean ####
  folder<-paste0("Data/hazard_timeseries_mean/",timeframe_choice)
  s3_bucket <-paste0("s3://digital-atlas/risk_prototype/data/hazard_timeseries_mean/",timeframe_choice)

  # Prepare tif data by converting to COG format
  if(F){
    ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)
  }
  # Upload files
  upload_files_to_s3(folder = folder,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F)
  
  # Upload - hazard_timeseries_risk ####
  folder<-paste0("Data/hazard_timeseries_risk/",timeframe_choice)
  s3_bucket <-paste0("s3://digital-atlas/risk_prototype/data/hazard_timeseries_risk/",timeframe_choice)
  #s3fs::s3_dir_ls(s3_bucket)
  
  # Prepare tif data by converting to COG format
  if(F){
  ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)
  }
  
  # Upload files
  upload_files_to_s3(folder = folder,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F)
  
  # Upload - hazard_timeseries_int ####
  folder<-paste0("Data/hazard_timeseries_int/",timeframe_choice)
  s3_bucket <-paste0("s3://digital-atlas/risk_prototype/data/hazard_timeseries_int/",timeframe_choice)
  #s3fs::s3_dir_ls(s3_bucket)
  
  # Prepare tif data by converting to COG format
  if(F){
  ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)
  }
  
  s3_files<-s3fs::s3_dir_ls(s3_bucket,recurse = T)
  s3_files<-unlist(tstrsplit(s3_files,paste0(timeframe_choice,"/"),keep=2))
  s3_files<-grep("/",s3_files,value=T)
  
  # Upload files
  local_folders<-list.dirs(folder)[-1]
  for(i in 2:length(local_folders)){
    print(paste0(i,"/",length(local_folders)))
    FOLDER<-local_folders[i]
    S3_BUCKET<-gsub(folder,s3_bucket,FOLDER)
    
    LOCAL_FILES<-paste0(basename(FOLDER),"/",list.files(FOLDER))
    LOCAL_FILES<-file.path(folder,LOCAL_FILES[!LOCAL_FILES %in% s3_files])
    
    if(length(LOCAL_FILES)>0){
      upload_files_to_s3(files = LOCAL_FILES,
                         selected_bucket=S3_BUCKET,
                         max_attempts = 3,
                         overwrite=F,
                         check_identical = F)
    }
  }
  
  # Upload - hazard_timeseries_risk sd ####
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
                     overwrite=F,
                     check_identical = F)
  
  # Upload - haz_risk ####
  s3_bucket <-paste0("s3://digital-atlas/risk_prototype/data/hazard_risk/",timeframe_choice)
  folder<-paste0("Data/hazard_risk/",timeframe_choice)
  
  s3fs::s3_dir_ls(s3_bucket)
  #s3fs::s3_file_delete(s3fs::s3_dir_ls(s3_bucket))
  #s3fs::s3_dir_ls(s3_bucket)
  
  # Prepare tif data by converting to COG format
  if(F){
    ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)
  }
  
  # Upload files
  upload_files_to_s3(files =list.files(folder,full.names = T),
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F)
  
  # Upload - haz_vop_risk ####
  s3_bucket <-paste0("s3://digital-atlas/risk_prototype/data/hazard_risk_vop/",timeframe_choice)
  folder<-paste0("Data/hazard_risk_vop/",timeframe_choice)
  
  # Prepare tif data by converting to COG format
  if(F){
    ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)
  }
  
  # Which files are missing
  files<-list.files(folder,full.names = T)

  # files<-grep("_adm_",files,value=T)
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F)
  
  # Upload - haz_vop_risk_ac ####
  s3_bucket <- paste0("s3://digital-atlas/risk_prototype/data/hazard_risk_vop_ac/",timeframe_choice)
  folder<-paste0("Data/hazard_risk_vop_ac/",timeframe_choice)
  
  s3_dir_ls(s3_bucket)
  #s3fs::s3_dir_delete(s3_bucket)

  # Upload files
  upload_files_to_s3(files=list.files(folder,".parquet",full.names = T),
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F)
  
# 3) ROI data ####
  s3_bucket <- paste0("s3://digital-atlas/risk_prototype/data/roi")
  folder<-paste0("Data/roi")
  
  s3_dir_ls(s3_bucket)
  
  # Upload files
  upload_files_to_s3(files=list.files(folder,".parquet$",full.names = T),
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T)
# 4) hazard_timeseries data ####
  s3_bucket <-paste0("s3://digital-atlas/risk_prototype/data/hazard_timeseries/",timeframe_choice)
  folder<-haz_timeseries_dir
  
  s3_dir_ls(s3_bucket)
  
  # Updated hazards
  files<-list.files(folder,"ENSEMBLEmean|ENSEMBLEsd|historic",full.names = T)
  haz<-"NTx35_mean|NTx40_mean|NDWL0_mean|NDWS_mean|PTOT_sum|TAVG_mean|HSH_max_max|HSH_mean_mean|THI_mean_mean|THI_max_max|TAI_mean"
  files<-grep(haz,files,value=T)
  
  # Upload files
  upload_files_to_s3(files=files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F)
  

# =========================####
# UPLOAD TO GOOGLEDRIVE ####
  # Ensure the necessary libraries are loaded
  library(googledrive)
  library(s3fs)
  library(mime)
  
  # Updated transfer function with support for s3fs
  transfer_s3_to_drive <- function(s3_bucket, prefix, drive_folder_id, overwrite = FALSE) {
    # Check for existing Google Drive authentication without forcing re-authentication
    if (!drive_has_token()) {
      drive_auth()
    } else {
      message("Using existing Google Drive authentication.")
    }
    
    # Check if the Google Drive folder exists
    if (length(drive_ls(drive_folder_id)) == 0) {
      stop("The specified Google Drive folder ID does not exist.")
    }
    
    
    # List files in the S3 bucket with the specified prefix
    s3_files <- s3fs::s3_dir_ls(s3_bucket)
    s3_files<-grep(prefix,s3_files,value = T)
    
    for (i in 1:length(s3_files)) {
      file_name <- basename(s3_files[i])
      
      # Check if the file exists in Google Drive and skip if overwrite is FALSE
      existing_files <- drive_ls(drive_folder_id)
      
      # Report progress
      message(sprintf("Processing file %d of %d: %s", i, length(s3_files), file_name))
      
      
      if (!(file_name %in% existing_files) |overwrite==T) {
        
        # Download file from S3 to a temporary location
        temp_file_path <- file.path(tempfile(),file_name)
        s3fs::s3_file_download(s3_files[i],new_path = temp_file_path)
        
        # Upload file to the specified Google Drive folder, overwriting if necessary
        drive_upload(media = temp_file_path,
                     path = paste0(as_id(drive_folder_id), "/", file_name))
        
        # Optionally, delete the temp file
        unlink(temp_file_path)
      }
      
    }
    
  }
  
  # Upload - haz_vop_risk
  
  s3_bucket <- paste0("s3://digital-atlas/risk_prototype/data/hazard_risk_vop_ac/",timeframe_choice)
  gdrive_folder<-paste0("atlas_",timeframe_choice)
  
  
  transfer_s3_to_drive(s3_bucket = s3_bucket,
                       prefix = "parquet",
                       drive_folder_id = gdrive_folder,
                       overwrite=F)
  