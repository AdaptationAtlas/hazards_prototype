packages<-c("rISIMIP")

options(timeout = 600)
remotes::install_github("https://github.com/RS-eco/rISIMIP")

# 1850soc: This represents a historical simulation using socio-economic conditions as they were in 1850. This scenario is used to understand the impact of climate under pre-industrial socio-economic conditions.
# 2015soc: This represents a historical simulation using socio-economic conditions as they were in 2015. This scenario helps to understand the impact of climate under contemporary socio-economic conditions.
# histsoc: This represents a historical simulation using actual socio-economic conditions over the historical period. It is used to understand the impact of climate under the evolving socio-economic conditions over time.

base_url<-"https://files.isimip.org/ISIMIP3b/OutputData/"
topic<-"water_global"
model<-c("watergap2-2e","h08","cwatm")
gcms<-c("gfdl-esm4","ipsl-cm6a-lr","mpi-esm1-2-hr","mri-esm2-0","kesm1-0-ll")
scenarios<-c("historical","ssp126","ssp370","ssp585")
variables<-c("evap","qg","qr","qs","qtot","groundwstor","dis")

datasets<-data.table(expand.grid(topic=topic,model=model,gcms=gcms,scenarios=scenarios,variables=variables))
datasets[,dl_path:=paste0(topic,"/",model,"/",gcms,"/",scenarios,"/",variables)]

# Load necessary libraries
library(httr)
library(jsonlite)

# Define the base URL and query parameters
base_url <- "https://data.isimip.org/api/v1/datasets/"
params <- list(
  topic = "water_global",
  model = "watergap2-2e",
  gcms = "gfdl-esm4",
  scenarios = "historical",
  variables = "evap",
  region = "africa",
  years = "1981-2014"
)

# Make the API request
response <- GET(base_url, query = params)

# Check if the request was successful
if (status_code(response) == 200) {
  data <- content(response, as = "parsed", type = "application/json")
  
  # Extract download URLs
  download_urls <- sapply(data, function(dataset) dataset$download_url)
  
  # Create a directory to save the downloaded files
  download_dir <- "./isimip_downloads/"
  if (!dir.exists(download_dir)) {
    dir.create(download_dir)
  }
  
  # Function to download each file
  download_file <- function(url, dir) {
    file_name <- basename(url)
    file_path <- file.path(dir, file_name)
    download.file(url, file_path, mode = "wb")
    message(paste("Downloaded:", file_name))
  }
  
  # Download each file
  for (url in download_urls) {
    download_file(url, download_dir)
  }
} else {
  message(paste("Failed to retrieve data:", status_code(response)))
}