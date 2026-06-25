# CDS dataset description
# https://cds.climate.copernicus.eu/cdsapp#!/dataset/sis-agrometeorological-indicators?tab=overview

# Has a number of dependencies, working on alternatives
# devtools::install_github("bluegreen-labs/ecmwfr")
# install.packages("ecmwfr")

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
suppressMessages(pacman::p_load(tidyverse,parallel,ecmwfr))
options(keyring_backend = "file")

# credentials — read from environment, NEVER hardcode. Set CDS_UID + CDS_KEY in
# your shell / ~/.Renviron. SECURITY: a key was previously hardcoded here and is
# LEAKED in git history (both AdaptationAtlas/hazards and, via the subtree vendor,
# hazards_prototype) — it MUST be rotated at https://cds.climate.copernicus.eu;
# de-hardcoding does NOT scrub it from history, only rotation invalidates it.
UID = Sys.getenv("CDS_UID")
key = Sys.getenv("CDS_KEY")
stopifnot(
  "CDS_UID not set — export it or add to ~/.Renviron" = nzchar(UID),
  "CDS_KEY not set — export it or add to ~/.Renviron" = nzchar(key)
)

# save key for CDS
ecmwfr::wf_set_key(user = UID,
                   key = key,
                   service = "cds")

getERA5 <- function(i, qq, year, month, datadir){
  q <- qq[i,]
  format <- "zip" # netcdf
  ofile <- paste0(paste(q$variable, q$statistics, year, month, sep = "-"), ".",format)
  
  if(!file.exists(file.path(datadir,ofile))){
    ndays <- lubridate::days_in_month(as.Date(paste0(year, "-" ,month, "-01")))
    ndays <- 1:ndays
    ndays <- sapply(ndays, function(x) ifelse(length(x) == 1, sprintf("%02d", x), x))
    ndays <- dput(as.character(ndays))
    
    cat("Downloading", q[!is.na(q)], "for", year, month, "\n"); flush.console();
    
    request <- list("dataset_short_name" = "sis-agrometeorological-indicators",
                    "variable" = q$variable,
                    "statistic" = q$statistics,
                    "year" = year,
                    "month" = month,
                    "day" = ndays,
                    "area" = "38/-26/-47/58", # Download Africa c(ymax,xmin,ymin,xmax)
                    "time" = q$time,
                    "format" = format,
                    "target" = ofile)
    
    request <- Filter(Negate(anyNA), request)
    
    file <- ecmwfr::wf_request(user     = UID,   # user ID (for authentification)
                               request  = request,  # the request
                               transfer = TRUE,     # download the file
                               path     = datadir)
  } else {
    cat("Already exists", q[!is.na(q)], "for", year, month, "\n"); flush.console();
  }
  return(NULL)
}

########################################################################################################
# Data directory
datadir <- file.path(common_data_root(), "ecmwf_agera5")
dir.create(datadir, F, T)

# Combinations to download
qq <- data.frame(variable = c("solar_radiation_flux",rep("2m_temperature",3),
                              "10m_wind_speed", "2m_relative_humidity"),
                 statistics = c(NA, "24_hour_maximum", "24_hour_mean", "24_hour_minimum",
                                "24_hour_mean", NA),
                 time = c(NA,NA,NA,NA,NA, "12_00"))
qq <- qq[qq$variable == 'solar_radiation_flux',]

# temporal range
years <- as.character(1995:2014)
months <- c(paste0('0', 1:9), 10:12)

# all download
for (i in 1:nrow(qq)){
  for (year in years){
    for (month in months){
      tryCatch(getERA5(i, qq, year, month, datadir), error = function(e) NULL)
    }
  }
}

# unzip
zz <- list.files(datadir, ".zip$", full.names = T)
vars <- c("solar_radiation_flux")

extractNC <- function(var, zz, datadir, ncores = 1){
  z <- grep(var, zz, value = TRUE)
  fdir <- file.path(datadir, var)
  dir.create(fdir, showWarnings = FALSE, recursive = TRUE)
  parallel::mclapply(z, function(x){unzip(x, exdir = fdir)}, mc.cores = ncores, mc.preschedule = FALSE)
  return(NULL)
}

for(var in vars){
  extractNC(var, zz, datadir, ncores = 1)
}
