#Create SPAM mask at CHIRPS resolution, combining multiple crops
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

#clean-up environment
rm(list=ls())
gc(verbose=FALSE, full=TRUE, reset=TRUE)

#working directory
wd <- file.path(common_data_root(), "atlas_hazards/mapspam_mask")
if (!file.exists(wd)) {dir.create(wd)}

#read Africa shapefile
r_msk <- terra::rast(file.path(common_data_root(), "atlas_hazards/roi/africa.tif"))

#first list all MapSPAM files labeled as "All technologies" (\\_A.tif)
spam_dir <- file.path(common_data_root(), "mapspam_2017/raw")
spam_files <- list.files(spam_dir, pattern="\\_A.tif")

#filter by _H_ (harvested area)
spam_files <- spam_files[grep("_SSA_H_", spam_files)]

#load them as raster
spam_rs <- terra::rast(paste0(spam_dir,"/",spam_files)) %>%
  terra::crop(., r_msk)
spam_rs <- terra::app(spam_rs, fun=sum, na.rm=TRUE)

#resample resulting raster into CHIRPS resolution, use nn
chirps_rs <- terra::rast(file.path(common_data_root(), "chirps_wrld/chirps-v2.0.1995.01.01.tif")) %>%
  terra::crop(., r_msk)
chirps_rs[chirps_rs[]<0] <- NA
chirps_rs[!is.na(chirps_rs[])] <- 1
spam_rs <- terra::resample(spam_rs, chirps_rs, method="bilinear")

#make zeroes as NA
spam_rs[!is.na(spam_rs[])] <- 1

#write raster
terra::writeRaster(spam_rs, paste0(wd, "/spam2017V2r1_allcrop_mask.tif"), overwrite=TRUE)


