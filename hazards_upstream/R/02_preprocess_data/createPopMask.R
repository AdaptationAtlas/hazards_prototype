#Create population mask at CHIRPS resolution, combining urban+rural
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
wd <- file.path(common_data_root(), "atlas_hazards/population_mask")
if (!file.exists(wd)) {dir.create(wd)}

#read Africa shapefile
r_msk <- terra::rast(file.path(common_data_root(), "atlas_hazards/roi/africa.tif"))

#load population raster
pop_rs <- terra::rast(file.path(common_data_root(), "atlas_pop/raw/cell5m_afripop2020_urbanrural_ssa_popheadcount_total.tif")) %>%
  terra::crop(., r_msk)
pop_rs[pop_rs[] == 0] <- NA

#resample resulting raster into CHIRPS resolution, use nn
chirps_rs <- terra::rast(file.path(common_data_root(), "chirps_wrld/chirps-v2.0.1995.01.01.tif")) %>%
  terra::crop(., r_msk)
chirps_rs[chirps_rs[]<0] <- NA
chirps_rs[!is.na(chirps_rs[])] <- 1
pop_rs <- terra::resample(pop_rs, chirps_rs, method="bilinear")

#make zeroes as NA
pop_rs[!is.na(pop_rs[])] <- 1

#write raster
terra::writeRaster(pop_rs, paste0(wd, "/pop_mask.tif"), overwrite=TRUE)


