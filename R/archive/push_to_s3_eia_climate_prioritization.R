# Archived from R/push_to_s3.R section 9.
# Run R/0_server_setup.R first to load dependencies (s3fs, upload_files_to_s3, worker_n, overwrite).

# 9) eia_climate_prioritization ####
## 9.1) ERA5 NTx global (eia_climate_prioritization) ####
# cg_labs path
folder <- "/home/jovyan/common_data/EiA_pub"
s3_bucket <- file.path("s3://digital-atlas/hazards/agera5_ntx_global")

# Local files
local_files <- list.files(folder, full.names = TRUE)

upload_files_to_s3(
  files = local_files,
  selected_bucket = s3_bucket,
  max_attempts = 3,
  overwrite = overwrite,
  mode = "public-read",
  workers = worker_n
)

## 9.2) GDO drought indices (eia_climate_prioritization) ####
# cg_labs path
folder <- "/Users/pstewarda/Documents/rprojects/climate_prioritization/raw_data/drought_observatory"
s3_bucket <- file.path("s3://digital-atlas/hazards/global_drought_observatory")

# Local files
local_files <- list.files(folder, full.names = TRUE)

upload_files_to_s3(
  files = local_files,
  selected_bucket = s3_bucket,
  max_attempts = 3,
  overwrite = TRUE,
  mode = "public-read",
  workers = worker_n
)

## 9.3) GAEZ LGP (eia_climate_prioritization) ####
# cg_labs path
folder <- "/Users/pstewarda/Documents/rprojects/climate_prioritization/raw_data/gaez"
s3_bucket <- "s3://digital-atlas/hazards/gaez_lgp"

# Local files
local_files <- list.files(folder, full.names = TRUE)

upload_files_to_s3(
  files = local_files,
  selected_bucket = s3_bucket,
  max_attempts = 3,
  overwrite = TRUE,
  mode = "public-read",
  workers = worker_n
)

## 9.4) Spam (eia_climate_prioritization) #####
# cg_labs path
folder <- "/Users/pstewarda/Documents/rprojects/climate_prioritization/raw_data/SPAM"
s3_bucket <- "s3://digital-atlas/exposure/mapspam/eia_climate_prioritization"

# Local files
local_files <- list.files(folder, "tif$", full.names = TRUE)

upload_files_to_s3(
  files = local_files,
  selected_bucket = s3_bucket,
  max_attempts = 3,
  overwrite = TRUE,
  mode = "public-read",
  workers = worker_n
)

## 9.5) Countries (eia_climate_prioritization) #####
# cg_labs path
folder <- "/Users/pstewarda/Documents/rprojects/climate_prioritization/raw_data/boundaries"
s3_bucket <- "s3://digital-atlas/boundaries/eia_climate_prioritization"

s3_dir_ls(s3_bucket)

# Local files
local_files <- list.files(folder, full.names = TRUE)

upload_files_to_s3(
  files = local_files,
  selected_bucket = s3_bucket,
  max_attempts = 3,
  overwrite = TRUE,
  mode = "public-read",
  workers = worker_n
)
