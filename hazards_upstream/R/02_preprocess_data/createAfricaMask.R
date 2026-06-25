## Africa mask
## By: H. Achicanoy
## December, 2022

# R options
g <- gc(reset = T)   # rm(list=ls()) + warn=-1 dropped (see 00_setup.R)
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
suppressMessages(if(!require(pacman)){install.packages('pacman');library(pacman)} else {library(pacman)})
suppressMessages(pacman::p_load(tidyverse,raster,terra,sp,geodata,rnaturalearthdata,rnaturalearth))

# Root directory
root <- file.path(common_data_root(), "atlas_hazards")

## Shapefile
# Output file
out  <- paste0(root,'/roi/africa.gpkg')
dir.create(dirname(out), F, T)
if(!file.exists(out)){
  # World shapefile
  wrld <- rnaturalearth::ne_countries(returnclass = 'sf', scale = 50)
  # Africa shapefile
  afrc <- filter(wrld, region_un == 'Africa')
  # Save shapefile in .gpkg file
  afrc <- terra::vect(afrc)
  terra::writeVector(afrc, out)
}

## Raster
tif  <- paste0(root,'/roi/africa.tif')
if(!file.exists(tif)){
  # Africa shapefile
  afrc <- terra::vect(out)
  # CHIRPS template
  ref <- terra::rast(file.path(common_data_root(), "chirps_wrld/chirps-v2.0.1981.01.02.tif"))
  ref <- ref %>% terra::crop(terra::ext(afrc))
  ref <- terra::rasterize(afrc, ref)
  terra::writeRaster(ref, tif)
}

# Increase extent in the northern area
r <- terra::rast(tif)
r <- terra::extend(x = r, y = terra::ext(c(-25.3499976955354, 57.8000035434961,
                                            -46.9500014446676, 40)))
terra::writeRaster(r, tif, overwrite = T)
