# Please run 0_server_setup.R before executing this script
# 0) Load packages and create functions ####
packages<-c("terra","data.table","pbapply")
pacman::p_load(char=packages)

url_exists <- function(url) {
  response <- HEAD(url)
  return(status_code(response) == 200)
}


# 1) Create download paths #####
# This section of the code is responsible for constructing the download paths for the datasets based on various parameters.
# It uses a base URL and combines it with different model names, General Circulation Models (GCMs), scenarios, variables,
# and timeframes to create a complete URL for each dataset.

# Explanation of Scenarios:
# 1850soc: This represents a historical simulation using socio-economic conditions as they were in 1850.
#          This scenario is used to understand the impact of climate under pre-industrial socio-economic conditions.
# 2015soc: This represents a historical simulation using socio-economic conditions as they were in 2015.
#          This scenario helps to understand the impact of climate under contemporary socio-economic conditions.
# histsoc: This represents a historical simulation using actual socio-economic conditions over the historical period.
#          It is used to understand the impact of climate under the evolving socio-economic conditions over time.

# Base URL for the ISIMIP3b data repository
base_url <- "https://files.isimip.org/ISIMIP3b/OutputData"

# Define the topic, models, GCMs, scenarios, and variables of interest
topic <- "water_global"
model <- c("WaterGAP2-2e", "H08", "CWatM")
gcms <- c("gfdl-esm4", "ipsl-cm6a-lr", "mpi-esm1-2-hr", "mri-esm2-0", "ukesm1-0-ll")
scenarios <- c("historical_histsoc", "ssp126_2015soc", "ssp370_2015soc", "ssp585_2015soc")
variables <- c("evap", "qg", "qr", "qs", "qtot", "groundwstor", "dis")
variable_stat<-data.table(var=variables,stat=c("sum","sum","sum","sum","sum","mean","mean"))

# Create a data table with all combinations of the parameters
datasets <- data.table(expand.grid(topic = topic, model = model, gcms = gcms, scenarios = scenarios, variable = variables))

# Assign timeframes based on the scenario
datasets[, timeframe := "2015_2100"][scenarios == "historical_histsoc", timeframe := "1850_2014"]

# Adjust the variable name for evaporation based on the model
datasets[variable == "evap" & model != "H08", variable := "evap-total"]

# Modify GCM and scenario paths for non-historical scenarios
datasets[, gcms2 := gcms][scenarios != "historical_histsoc", gcms2 := paste0(gcms, "/future")]
datasets[, scenarios2 := paste0(unlist(tstrsplit(scenarios, "_", keep = 1)), "/")][scenarios2 != "historical/", scenarios2 := ""]

# Specific adjustment for CWatM model scenarios
datasets[model == "CWatM", scenarios := gsub("2015soc", "2015soc-from-histsoc", scenarios)]

# Construct the download paths for each dataset
datasets[, dl_path := paste0(base_url, "/",
                             topic, "/",
                             model, "/",
                             gcms2, "/",
                             scenarios2,
                             tolower(model), "_",
                             gcms, "_w5e5_",
                             scenarios, "_default_",
                             variable, "_global_monthly_",
                             timeframe, ".nc"
)]

# 1.1) Validate download paths #####
check<-lapply(1:nrow(datasets),FUN=function(i){
  a<-datasets$dl_path[i]
  exists<-url_exists(a)
  if(!exists){
    cat(exists,gsub("https://files.isimip.org/ISIMIP3b/OutputData/water_global/","",a),"\n")
  }
  exists
})

# 2) Download files #####
delete_original<-T

lapply(1:nrow(datasets),FUN=function(i){
  cat(i,"/",nrow(datasets),"\n")
  
  # Define URL and local file path
  url_path <- datasets$dl_path[i]
  local_path <- file.path(isimip_raw_dir, basename(url_path))
  local_path <- gsub("global_", "africa_", local_path)
  
  # Modify file path based on scenario
  if (datasets[i]$scenarios == "historical_histsoc") {
    local_path_hist <- gsub("1850_2014", "1995_2014", local_path)
    file_exists <- file.exists(local_path_hist)
  } else {
    local_path_2030 <- gsub("2015_2100", "2021_2040", local_path)
    local_path_2050 <- gsub("2015_2100", "2041_2060", local_path)
    file_exists <- all(file.exists(local_path_2030, local_path_2050))
  }
  
  # If the files do not exist, download and process them
  if (!file_exists) {
    # Retry logic for download with 3 attempts
    success <- FALSE
    retries <- 3
    while (retries > 0 && !success) {
      tryCatch({
        cat(sprintf("Downloading %s (Attempt %d)\n", url_path, 4 - retries))
        system.time(download.file(url = url_path, destfile = local_path))
        success <- TRUE
      }, error = function(e) {
        retries <- retries - 1
        cat(sprintf("Download failed for %s. Retries left: %d\n", url_path, retries))
        if (retries == 0) {
          stop(sprintf("Failed to download %s after 3 attempts", url_path))
        }
      })
    }
    
    # Load the downloaded raster data
    data <- terra::rast(local_path)
    time_info <- time(data)
    
    # Process historical data
    if (datasets[i]$scenarios == "historical_histsoc") {
      start_date <- as.Date("1995-01-01")
      end_date <- as.Date("2014-12-31")
      start_index <- which(time_info >= start_date)[1]
      end_index <- which(time_info <= end_date)[length(which(time_info <= end_date))]
      data_subset <- subset(data, start_index:end_index)
      data_subset <- terra::crop(data_subset, base_rast)
      terra::writeCDF(data_subset, local_path_hist,overwrite=T)
      
    } else {
      # Process future scenarios for 2020-2040
      start_date <- as.Date("2021-01-01")
      end_date <- as.Date("2040-12-31")
      start_index <- which(time_info >= start_date)[1]
      end_index <- which(time_info <= end_date)[length(which(time_info <= end_date))]
      data_subset <- subset(data, start_index:end_index)
      data_subset <- terra::crop(data_subset, base_rast)
      terra::writeCDF(data_subset, local_path_2030,overwrite=T)
      
      # Process future scenarios for 2040-2060
      start_date <- as.Date("2041-01-01")
      end_date <- as.Date("2060-12-31")
      start_index <- which(time_info >= start_date)[1]
      end_index <- which(time_info <= end_date)[length(which(time_info <= end_date))]
      data_subset <- subset(data, start_index:end_index)
      data_subset <- terra::crop(data_subset, base_rast)
      terra::writeCDF(data_subset, local_path_2050,overwrite=T)
    }
    
    # Optionally delete the original downloaded file
    if (delete_original) {
      unlink(local_path)
    }
  }
})


# 3) Create an index #####
file_index<-data.table(file_path=list.files(isimip_raw_dir,".nc$",full.names = T))[,basename:=basename(file_path)]
file_index[,model:=unlist(tstrsplit(basename,"_",keep=1))
      ][,gcm:=unlist(tstrsplit(basename,"_",keep=2))
        ][,scenario:=unlist(tstrsplit(basename,"_",keep=4))
          ][,scenario2:=unlist(tstrsplit(basename,"_",keep=5))
            ][,var:=unlist(tstrsplit(basename,"_",keep=7))
              ][,region:=unlist(tstrsplit(basename,"_",keep=8))
                ][,frequency:=unlist(tstrsplit(basename,"_",keep=9))
                  ][,timeframe:=gsub(".nc","",paste0(unlist(tstrsplit(basename[1],"_",keep=10:11)),collapse="-")),by=basename
                    ][,var:=gsub("-total","",var)]

file_index<-merge(file_index,variable_stat,all.x=T)

# Add aggregation function to file index
data.table::fwrite(file_index,file.path(isimip_raw_dir,"file_index.csv"))

