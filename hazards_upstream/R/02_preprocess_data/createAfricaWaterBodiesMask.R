## Africa water bodies
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
suppressMessages(library(pacman))
suppressMessages(pacman::p_load(tidyverse, terra))

root <- '//catalogue/Workspace14/WFP_ClimateRiskPr' # CIAT route

# CHIRPS template
tmp <- terra::rast('./africa.tif')

# Water bodies to raster
fls <- list.files(paste0(root,'/1.Data/shps/GLWD'), pattern = '_fixed.shp$', full.names = T)
wbd <- fls %>%
  purrr::map(.f = function(fl){
    shp <- terra::vect(fl)
    wbd <- terra::rasterize(x = shp, y = tmp)
    return(wbd)
  })
wbd <- terra::rast(wbd)
wbd <- terra::app(x = wbd, fun = sum, na.rm = T)

terra::writeRaster(wbd, './africa_wbd.tif', overwrite = T) # Manually uploaded to EiA
