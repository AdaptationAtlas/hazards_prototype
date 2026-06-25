#Create Livestock VoP mask at CHIRPS resolution, combining multiple species
#JRV, Dec. 2022

#load packages
library(terra)
library(tidyverse)

# Shared Stage-0 setup: data root, timestamped .log(), env run-controls.
local({
  cargs <- commandArgs(FALSE)
  fa <- grep("^--file=", cargs, value = TRUE)
  base <- if (length(fa)) dirname(normalizePath(sub("^--file=", "", fa[1]))) else getwd()
  cand <- c(file.path(base, "..", "00_setup.R"), file.path(base, "00_setup.R"),
            "../00_setup.R", "00_setup.R")
  hit <- cand[file.exists(cand)][1]
  if (is.na(hit)) stop("00_setup.R not found from ", base)
  source(normalizePath(hit), local = FALSE)
})

# rm(list=ls()) dropped — would wipe 00_setup.R helpers sourced above (see 00_setup.R)
gc(verbose=FALSE, full=TRUE, reset=TRUE)

#working directory
wd <- file.path(common_data_root(), "atlas_hazards/livestock_mask")
if (!file.exists(wd)) {dir.create(wd)}

#read Africa shapefile
r_msk <- terra::rast(file.path(common_data_root(), "atlas_hazards/roi/africa.tif"))

#first list all VoP individual species files
lstk_dir <- file.path(common_data_root(), "atlas_livestock/raw")
lstk_files <- list.files(lstk_dir, pattern="\\.tif")

#load them as raster
lstk_rs <- terra::rast(paste0(lstk_dir,"/",lstk_files)) %>%
  terra::crop(., r_msk)
lstk_rs <- terra::app(lstk_rs, fun=sum, na.rm=TRUE)
lstk_rs[lstk_rs[] == 0] <- NA

#resample resulting raster into CHIRPS resolution, use nn
chirps_rs <- terra::rast(file.path(common_data_root(), "chirps_wrld/chirps-v2.0.1995.01.01.tif")) %>%
  terra::crop(., r_msk)
chirps_rs[chirps_rs[]<0] <- NA
chirps_rs[!is.na(chirps_rs[])] <- 1
lstk_rs <- terra::resample(lstk_rs, chirps_rs, method="bilinear")

#make zeroes as NA
lstk_rs[!is.na(lstk_rs[])] <- 1

#write raster
terra::writeRaster(lstk_rs, paste0(wd, "/livestock_allspecies_mask.tif"), overwrite=TRUE)


