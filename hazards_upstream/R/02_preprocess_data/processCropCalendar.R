#Process crop calendar into an easily digestible raster
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
wd <- file.path(common_data_root(), "atlas_crop_calendar")

#output directory
out_dir <- paste0(wd, "/intermediate")
if (!file.exists(out_dir)) {dir.create(out_dir)}

#read Africa shapefile
r_msk <- terra::rast(file.path(common_data_root(), "atlas_hazards/roi/africa.tif"))

#read maize calendar of Jagermeyr
r_cal <- terra::rast(paste0(wd, "/raw_jagermeyr/mai_rf_ggcmi_crop_calendar_phase3_v1.01.nc4"))
r_cal <- r_cal[[1:3]]
r_cal <- r_cal %>%
  terra::crop(., r_msk) %>%
  terra::resample(., r_msk, method="near") %>%
  terra::mask(., r_msk)

#write raster
terra::writeRaster(r_cal, paste0(out_dir, "/mai_rf_ggcmi_crop_calendar_phase3_v1.01_Africa.tif"), overwrite=TRUE)



