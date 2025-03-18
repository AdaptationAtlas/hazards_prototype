#############################################
# Script: Reclassify Temperature and Aridity Zones
# Project: Africa Agriculture Adaptation Atlas (AAAA)
# Author: Peter Steward (p.steward@cgiar.org, ORCID 0000-0003-3985-4911)
# Organization: Alliance of Bioversity International & CIAT
# Funder: Gates Foundation
# 
# Description:
# This script replicates the temperature and aridity zone classifications used in the 
# Concern Worldwide "Crop, Weather and Climate Vulnerability Profiles" (2017). 
# (Available at: https://www.concern.net/knowledge-hub/crop-weather-and-climate-vulnerability-profiles)
# 
# Specifically, it:
# 
# 1. Downloads Mean Annual Temperature (MAT) data from WorldClim and classifies it into 
#    three temperature regimes: Low (<15°C), Medium (15-25°C), High (>25°C).
# 
# 2. Reclassifies Agro-Ecological Zone (AEZ) data into four aridity classes: 
#    Arid, Semiarid, Subhumid, and Humid.
# 
# 3. Resamples and masks the AEZ data to match the MAT raster resolution and extent.
# 
# 4. Combines the temperature and aridity classifications into a single raster, 
#    matching the typology used in the Concern profiles.
# 
# 5. Outputs the combined raster for use in further climate risk and vulnerability assessments, 
#    supporting analysis of crop suitability, climate exposure, and adaptive capacity across 
#    Sub-Saharan Africa.
# 
# The final output can be used as a foundational layer in vulnerability profiles similar to 
# Concern’s approach, aiding policymakers and practitioners in targeting climate adaptation 
# interventions.
#
# IMPORTANT:
# Please run **0_server_setup.R** before executing this script to ensure all required 
# directories and configurations are initialized properly.
#############################################

# 0) Set-up workspace ####
  # 0.1) Load R functions & packages ####

# Load required packages
p_load(terra, geodata)

# 1) Download worldclim MAT ####
# Set directory to store WorldClim data
worldclim_dir <- "Data/worldclim"
if (!dir.exists(worldclim_dir)) {
  dir.create(worldclim_dir, recursive = TRUE)
}

# Define paths for MAT file (Mean Annual Temperature)
mat_file_old <- file.path(worldclim_dir, "climate/wc2.1_2.5m", "wc2.1_2.5m_bio_1.tif")
mat_file_new <- file.path(worldclim_dir, "wc2.1_2.5m_bio_1.tif")

# Download and unzip if MAT file is not already present
if (!file.exists(mat_file_new)) {
  # Download WorldClim bioclimatic variables at 2.5 arc-min resolution
  geodata::worldclim_global(var = "bio", res = 2.5, path = worldclim_dir)
  
  # Clean up unnecessary files, keeping only the MAT file
  files <- list.files(file.path(worldclim_dir, "climate/wc2.1_2.5m"), full.names = TRUE)
  unlink(files[files != mat_file_old])
  
  # Move MAT file to desired location
  file.rename(mat_file_old, mat_file_new)
  
  # Remove unneeded climate folder
  unlink(file.path(worldclim_dir, "climate"), recursive = TRUE)
}

# Load MAT raster
mat_rast <- terra::rast(mat_file_new)

# Crop MAT raster to Sub-Saharan Africa (SSA) extent
file <- file.path(geo_dir, "atlas-region_admin0_harmonized.parquet")
admin0 <- arrow::open_dataset(file) |> sf::st_as_sf() |> terra::vect()
mat_rast <- mask(crop(mat_rast, admin0), admin0)

  # 1.1) Classify MAT ####
# Define temperature classification matrix: Lower bound, Upper bound, New category value
temp_classes <- matrix(c(
  -Inf, 15, 1,  # Low temperature (<15°C)
  15, 25, 2,    # Medium temperature (15-25°C)
  25, Inf, 3    # High temperature (>25°C)
), ncol = 3, byrow = TRUE)

# Apply classification to MAT raster
mat_classified <- classify(mat_rast, temp_classes)

# Define category labels
temp_labels <- data.frame(value = 1:3, category = c("Low", "Medium", "High"))

# Assign labels to the classified raster
levels(mat_classified) <- list(temp_labels)

# Plot classified temperature raster
plot(mat_classified, main = "Temperature Regimes")

# 2) Load and reclassify AEZ data ####
# Locate AEZ raster file
aez_file <- list.files(aez_dir, pattern = "AEZ16.*\\.tif$", full.names = TRUE)
aez_rast <- terra::rast(aez_file)

# Define AEZ reclassification matrix: Old Value -> New Value
# Group AEZ zones into broader aridity classes
rcl_mat <- matrix(c(
  0, 1,  # Arid
  4, 1,
  7, 1,
  11, 1,
  1, 2,  # Semiarid
  5, 2,
  8, 2,
  12, 2,
  2, 3,  # Subhumid
  6, 3,
  9, 3,
  13, 3,
  3, 4,  # Humid
  10, 4,
  14, 4
), ncol = 2, byrow = TRUE)

# Apply AEZ reclassification
aridity <- classify(aez_rast, rcl_mat)

# Define aridity class labels
aridity_labels <- data.frame(value = 1:4, category = c("Arid", "Semiarid", "Subhumid", "Humid"))

# Assign labels to aridity raster
levels(aridity) <- list(aridity_labels)

# Plot reclassified aridity raster
plot(aridity, main = "Reclassified Aridity Classes")

# 2) Resample AEZ to match MAT resolution and extent ####
# Resample aridity raster to match MAT classified raster resolution
aridity_rs <- terra::resample(aridity, mat_classified)

# Mask aridity raster to SSA extent
aridity_rs <- mask(aridity_rs, admin0)

# 3) Combine AEZ and MAT classifications ####
# Combine aridity and MAT classifications:
# Multiply MAT class by 10, add aridity class to create unique combined code
combined_raster <- aridity_rs + mat_classified * 10

# Define combined class labels
class_map <- data.frame(
  value = c(11, 12, 13, 14, 21, 22, 23, 24, 31, 32, 33, 34),
  category = c(
    "Low Temp - Arid", "Low Temp - Semiarid", "Low Temp - Subhumid", "Low Temp - Humid",
    "Medium Temp - Arid", "Medium Temp - Semiarid", "Medium Temp - Subhumid", "Medium Temp - Humid",
    "High Temp - Arid", "High Temp - Semiarid", "High Temp - Subhumid", "High Temp - Humid"
  )
)

# Assign labels to combined raster
levels(combined_raster) <- list(class_map)

# Check unique combined class values
print(unique(combined_raster))

# Plot final combined classification
plot(combined_raster, main = "Combined Temperature & Humidity Classification")

# 4) Save result ####
# Save the combined raster to file
terra::writeRaster(combined_raster, file.path(aez_dir, "aez_temp.tif"))