library(s3fs)
library(pbapply)
library(future)
library(future.apply)
library(gdalUtilities)
library(terra)

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

#list buckets
(buckets<-s3_dir_ls())

# select a bucket
selected_bucket <- buckets[2]

# show directories
s3_dir_ls(selected_bucket)

# Add folder 1 ####
# Define the new directory name
new_directory_name <- "risk_prototype" # Replace with your desired directory name
# Create the new directory in the selected bucket
s3_dir_create(paste0(selected_bucket, "/", new_directory_name))
# show directories
s3_dir_ls(selected_bucket)

# Add folder 2 ####
# select a bucket
selected_bucket <- "s3://digital-atlas/risk_prototype"
new_directory_name<-"data"
# Create the new directory in the selected bucket
s3_dir_create(paste0(selected_bucket, "/", new_directory_name))
# show directories
s3_dir_ls(selected_bucket)

# Add folder 3 ####
# select a bucket
selected_bucket <- "s3://digital-atlas/risk_prototype/data"
new_directory_name<-"hazard_risk_vop"
# Create the new directory in the selected bucket
s3_dir_create(paste0(selected_bucket, "/", new_directory_name))
# show directories
s3_dir_ls(selected_bucket)

# Add folder 4 ####
# select a bucket
selected_bucket <- "s3://digital-atlas/risk_prototype/data/hazard_risk_vop"
new_directory_name<-"annual"
# Create the new directory in the selected bucket
s3_dir_create(paste0(selected_bucket, "/", new_directory_name))
# show directories
s3_dir_ls(selected_bucket)


# Upload Data - haz_vop_risk ####
# select a bucket
selected_bucket <- "s3://digital-atlas/risk_prototype/data/hazard_risk_vop/annual"

# Select a folder to upload
folder<-"Data/hazard_risk_vop/annual"

# Prepare tif data by converting to COG format ####

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

# Upload files S3 bucket ####
upload_files_to_s3 <- function(files, selected_bucket, max_attempts = 3,overwrite=F) {
  
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
          file_check <- s3_file_exists(s3_file_path)
          if ((!file_check)|overwrite==T) {
            s3_file_upload(files[i], s3_file_path)
            
            # Check if upload successful
            file_check <- s3_file_exists(s3_file_path)
            if (file_check) break # Exit the loop if upload is successful
          }
          
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

# Consider updating the above to allow parallel uploads
#plan(multisession, workers = 5)
#s3_file_upload_async(localpath, s3path)

# Tifs 
upload_files_to_s3(files = list.files(folder, pattern = "\\.tif$", full.names = TRUE),
             selected_bucket=selected_bucket,
             max_attempts = 3)

# Parquet
upload_files_to_s3(files = list.files(folder, pattern = "\\.parquet$", full.names = TRUE),
                   selected_bucket=selected_bucket,
                   max_attempts = 3)

# Feather
upload_files_to_s3(files = list.files(folder, pattern = "\\.feather$", full.names = TRUE),
                   selected_bucket=selected_bucket,
                   max_attempts = 3)



# Upload - metadata ####
# select a folder
folder<-"metadata"
# select a bucket
selected_bucket <- "s3://digital-atlas/risk_prototype/data"
new_directory_name<-"metadata"
# Create the new directory in the selected bucket
s3_dir_create(paste0(selected_bucket, "/", new_directory_name))
# show directories
s3_dir_exists(selected_bucket)

upload_files_to_s3(files = list.files(folder, full.names = TRUE),
                   selected_bucket=selected_bucket,
                   max_attempts = 3)
# Upload - hazard_timeseries_risk ####
folder<-"Data/hazard_timeseries_risk/annual"
# select a bucket
selected_bucket <- "s3://digital-atlas/risk_prototype/data/hazard_timeseries_risk/annual"
# Create the new directory in the selected bucket
s3_dir_create(selected_bucket)
# show directories
s3_dir_exists(selected_bucket)


# Prepare tif data by converting to COG format ####

# List tif files in the folder
files_tif<-list.files(folder,".tif",full.names = T)
# Remove any COGs from the tif list
files_tif<-files_tif[!grepl("_COG.tif",files_tif)]

# Update tifs to cog format
# Set up parallel backend
plan(multisession,workers=10)  # Change to multicore on Unix/Linux

# Apply the function to each file
future_sapply(files_tif, convert_to_cog,future.packages = c("gdalUtilities","terra"),delete=T,rename=T)

plan(sequential)
closeAllConnections()

# Upload cogs
files_tif<-list.files(folder,".tif",full.names = T)

upload_files_to_s3(files = files,
                   selected_bucket=selected_bucket,
                   max_attempts = 3)

# Upload - hazard_timeseries_risk sd ####
folder<-"Data/hazard_timeseries_sd/annual"
# select a bucket
selected_bucket <- "s3://digital-atlas/risk_prototype/data/hazard_timeseries_sd/annual"
# Create the new directory in the selected bucket
if(!s3_dir_exists(selected_bucket)){
  s3_dir_create(selected_bucket)
}

# Prepare tif data by converting to COG format ####

# List tif files in the folder
files_tif<-list.files(folder,".tif",full.names = T)
# Remove any COGs from the tif list
files_tif<-files_tif[!grepl("_COG.tif",files_tif)]

# Update tifs to cog format
# Set up parallel backend
plan(multisession,workers=10)  # Change to multicore on Unix/Linux

# Apply the function to each file
future_sapply(files_tif, convert_to_cog,future.packages = c("gdalUtilities","terra"),delete=T,rename=T)

plan(sequential)
closeAllConnections()

# Upload cogs
files_tif<-list.files(folder,".tif",full.names = T)

upload_files_to_s3(files = files,
                   selected_bucket=selected_bucket,
                   max_attempts = 3)


