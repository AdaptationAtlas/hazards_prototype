## ------------------------------------------ ##
## Download daily global CHIRPS
## By: H. Achicanoy
## By: Harold Achicanoy
## WUR & ABC
## Feb 2025
## ------------------------------------------ ##

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
suppressMessages(pacman::p_load(tidyverse,terra,lubridate,R.utils))

# Time frame
ini <- as.Date('2024-10-01')
end <- as.Date('2025-01-31')
dts <- seq(from = ini, to = end, by = 'day'); rm(ini, end)

# Output directory
Out  <- '//catalogue/WFP_ClimateRiskPr1/1.Data/Chirps'
dir.create(Out,F,T)

# Main function
getChirps <- function(date = dts[1]){
  # CHIRPS base URL
  chrps <- 'https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p05'
  # Get day and year
  Day  <- date
  Year <- lubridate::year(Day)
  # Target file
  tfile <- paste0(chrps,'/',Year,'/chirps-v2.0.',gsub('-','.',Day,fixed=T),'.tif.gz')
  # Destination file
  dfile <- paste0(Out,'/',basename(tfile))
  # Raster file
  rfile <- gsub('.gz','',dfile,fixed = T)
  
  if(!file.exists(rfile)){
    
    # Downloading
    if(!file.exists(dfile)){
      tryCatch(expr = {
        utils::download.file(url = tfile, destfile = dfile)
      },
      error = function(e){
        cat(paste0(basename(tfile),' failed.\n'))
      })
    }
    
    # Unzip
    R.utils::gunzip(dfile)
    return(cat(paste0('Image ',basename(rfile),' processed correctly!!!\n')))
  } else {
    return(cat(paste0('Image ',basename(rfile),' already exists!\n')))
  }
  
}

# Loop through the dates
dts |> purrr::map(.f = getChirps)
