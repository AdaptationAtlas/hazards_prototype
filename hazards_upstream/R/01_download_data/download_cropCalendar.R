#Download crop calendar
#JRV, Dec. 2022

#load packages
library(terra)
library(tidyverse)
library(geodata)
library(devtools)
#install_github(c("inbo/inborutils"))
library(inborutils)

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

#clean-up environment (rm(list=ls()) dropped - see 00_setup.R)
gc(verbose=FALSE, full=TRUE, reset=TRUE)

#working directory
wd <- file.path(common_data_root(), "atlas_crop_calendar/raw_jagermeyr")
if (!file.exists(wd)) {dir.create(wd, recursive=TRUE)}

#set working directory
setwd(wd)

#download from zenodo, Jagermeyr et al. crop calendar (improved version of Sacks et al.)
download_zenodo(doi="10.5281/zenodo.5062513", path = ".", parallel = FALSE, quiet = FALSE)

####
#crop calendar from GAEZ+ 2015
wd <- file.path(common_data_root(), "atlas_crop_calendar/raw_gaez2015")
if (!file.exists(wd)) {dir.create(wd, recursive=TRUE)}

#set working directory
setwd(wd)

#url of interest
this_url <- "https://mygeohub.org/publications/60/serve/1?el=1"

#download the data, file name given per download
status <- geodata:::.downloadDirect(url=this_url,
                                    filename="bundle.zip", 
                                    unzip = TRUE, 
                                    quiet = FALSE, 
                                    mode = "wb", 
                                    cacheOK = FALSE)

#unzip file
try(utils::unzip("GAEZ2015_Monthly.zip", exdir = "."), silent = TRUE)

