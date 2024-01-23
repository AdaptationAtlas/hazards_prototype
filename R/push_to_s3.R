library(s3fs)
library(pbapply)
library(future)
library(future.apply)
library(gdalUtilities)
library(terra)

worker_n<-10

# Update tifs to cog format
convert_to_cog <- function(file,delete=T,rename=T) {
  
  is_cog<-grepl("LAYOUT=COG",gdalUtilities::gdalinfo(file))
  closeAllConnections()
  
  if(is_cog==F){
    # Define the new file name
    file_out <- sub(".tif$", "_COG.tif", file)
    
    if(!file.exists(file_out)){
      data<-terra::rast(file)
      terra::writeRaster(data,filename = file_out,filetype = 'COG',gdal=c("COMPRESS=LZW",of="COG"),overwrite=T)
    }
    
    if(delete==T){
      unlink(file,recursive = T)
    }
    if(rename==T){
      if(!file.exists(file)){
        file.rename(file_out,file)
      }else{
        print("Unlink not working COG name retained")
      }
    }
    
  }
  
}

ctc_wrapper<-function(folder,worker_n=1,delete=T,rename=T){
  # List tif files in the folder
  files_tif<-list.files(folder,".tif",full.names = T)
  # Remove any COGs from the tif list
  files_tif<-files_tif[!grepl("_COG.tif",files_tif)]
  
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
upload_files_to_s3 <- function(files,folder=NULL,selected_bucket,new_only=F, max_attempts = 3,overwrite=F) {
  
  # Create the s3 directory if it does not already exist
  if(!s3_dir_exists(selected_bucket)){
    s3_dir_create(selected_bucket)
  }
  
  
  # List files if a folder location is provided
  if(!is.null(folder)){
    files<-list.files(folder,full.names = T)
  }
  
  if(overwrite==F){
    #List files in the s3 bucket
    files_s3<-basename(s3_dir_ls(selected_bucket))
    # Remove any files that already exist in the s3 bucket
    files<-files[!basename(files) %in% files_s3]
  }
  
  if (sum(grepl("_COG.tif", files)) > 0) {
    stop("COG names still exist in tif directory, indicating an issue.")
  } else {
    for (i in seq_along(files)) {
      cat('\r', paste("File:", i, "/", length(files)), "           ")
      flush.console()
      
      s3_file_path <- paste0(selected_bucket, "/", basename(files[i]))
      
      tryCatch({
        attempt <- 1
        while(attempt <= max_attempts) {
            s3_file_upload(files[i], s3_file_path)
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
}

# Check existing folders
files<-s3_dir_ls("s3://digital-atlas/risk_prototype/data")

# Upload - metadata ####
# select a folder
folder<-"metadata"
# select a bucket
s3_bucket <- "s3://digital-atlas/risk_prototype/data/metadata"

ctc_wrapper(folder=folder,worker_n=1,delete=T,rename=T)

# Upload files
upload_files_to_s3(folder = folder,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=F)

# Upload - hazard_mean
# Select a local folder
folder<-"Data/hazard_risk_vop/annual"

# select a bucket
selected_bucket <- "s3://digital-atlas/risk_prototype/data/hazard_risk_vop/annual"

# Create the new directory in the selected bucket
if(!s3_dir_exists(selected_bucket)){
  s3_dir_create(selected_bucket)
}

# Select a folder to upload
folder<-"Data/hazard_risk_vop/annual"

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
# Upload - hazard mean ####
folder<-"Data/hazard_mean/annual"
s3_bucket <- "s3://digital-atlas/risk_prototype/data/hazard_mean/annual"

# Prepare tif data by converting to COG format
ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)

# Upload files
upload_files_to_s3(folder = folder,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite = F)

# Upload - hazard timeseries mean ####
folder<-"Data/hazard_timeseries_mean/annual"
s3_bucket <- "s3://digital-atlas/risk_prototype/data/hazard_timeseries_mean/annual"

# Prepare tif data by converting to COG format
ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)

# Upload files
upload_files_to_s3(folder = folder,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=F)



# Upload - hazard_timeseries_risk ####
folder<-"Data/hazard_timeseries_risk/annual"
s3_bucket <- "s3://digital-atlas/risk_prototype/data/hazard_timeseries_risk/annual"

# Prepare tif data by converting to COG format
ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)

# Upload files
upload_files_to_s3(folder = folder,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=F)


# Upload - hazard_timeseries_risk sd ####
folder<-"Data/hazard_timeseries_sd/annual"
# select a bucket
s3_bucket <- "s3://digital-atlas/risk_prototype/data/hazard_timeseries_sd/annual"

# Prepare tif data by converting to COG format
ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)

# Upload files
upload_files_to_s3(folder = folder,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=F)




# Upload - haz_risk ####
s3_bucket <- "s3://digital-atlas/risk_prototype/data/hazard_risk/annual"
folder<-"Data/hazard_risk/annual"

# Prepare tif data by converting to COG format
ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)

# Upload files
upload_files_to_s3(folder = folder,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=F)



# Upload - haz_vop_risk ####
s3_bucket <- "s3://digital-atlas/risk_prototype/data/hazard_risk_vop/annual"
folder<-"Data/hazard_risk_vop/annual"

# Prepare tif data by converting to COG format
ctc_wrapper(folder=folder,worker_n=worker_n,delete=T,rename=T)

# Upload files
upload_files_to_s3(folder = folder,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=F)

# Upload - exposure ####
s3_bucket <- "s3://digital-atlas/risk_prototype/data/exposure"
folder<-"Data/exposure"

# Prepare tif data by converting to COG format
ctc_wrapper(folder=folder,worker_n=1,delete=T,rename=T)

# Upload files
upload_files_to_s3(folder = folder,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=F)
