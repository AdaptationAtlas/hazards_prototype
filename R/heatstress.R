## Heat stress generic crop (NTx40)
## By: H. Achicanoy & P.Steward
## April, 2024

# 1) Load packages and create functions ####
suppressMessages(library(pacman))
suppressMessages(pacman::p_load(tidyverse,terra,gtools,lubridate,future,furrr,purrr,dplyr,parallel,future.apply))

# Calculate NTx function
calc_ntx <- function(tx_pth,yr, mn, thr=40,out_dir){
  outfile <- paste0(out_dir,'/NTx',thr,'/NTx',thr,'-',yr,'-',mn,'.tif') 
  thr <- thr[!file.exists(outfile)]
  outfile <- outfile[!file.exists(outfile)]
  if(length(outfile) > 0){
    cat("...processing n=", length(outfile), "files for yr=", yr, "/ mn=", mn, "\n")
    # Create directories
    1:length(outfile) %>%
      purrr::map(.f = function(j){dir.create(dirname(outfile[j]),F,T)})
    # Last day of the month
    last_day <- lubridate::days_in_month(as.Date(paste0(yr,'-',mn,'-01')))
    # Sequence of dates
    dts <- seq(from = as.Date(paste0(yr,'-',mn,'-01')), to = as.Date(paste0(yr,'-',mn,'-',last_day)), by = 'day')
    # Files
    fls <- paste0(tx_pth,'/',yr,'/Tmax.',gsub(pattern='-', replacement='.', x=dts, fixed=T),'.tif')
    fls <- fls[file.exists(fls)]
    # Read maximum temperature data
    tmx <- terra::rast(fls)
    
    # This is a step that could be applied to the raw data to increase efficiency
    tmx <- classify(tmx, rcl = cbind(-9999, NA))
    
    # Calculate heat stress generic crop
    # It might be faster to classify into a 0/1 rast then sum
    
    for (j in 1:length(thr)) {
      cat("...processing threshold thr=", thr[j], "\n")
      ntxval <- sum(classify(tmx, matrix(c(-Inf, thr[j], 0, thr[j], Inf, 1), ncol = 3, byrow = TRUE)),na.rm = TRUE)
      terra::writeRaster(ntxval,filename = outfile[j])
    }
    
    # Clean up
    rm(tmx)
    gc(verbose=FALSE, full=TRUE, reset=TRUE)
  }
}

# Functions to crop and mask rasters in parallel into a new file structure
process_raster <- function(raster_file,old_path,new_path,mask) {
  save_file<-gsub(old_path,new_path,raster_file)
  cat("...processing - ",raster_file,"\n")
  flush.console()
  
  if(!file.exists(save_file)){
    raster <- terra::rast(raster_file)
    cropped <- terra::crop(raster, mask)
    masked <- terra::mask(cropped, mask)
    terra::writeRaster(masked,save_file)
  }
  return(save_file)
}
process_raster_wrap<-function(old_path,new_path,cores,mask){
  # Create new directory structure
  old_dirs<-list.dirs(old_path)[-1]
  new_dirs<-gsub(old_path,new_path,old_dirs)
  sapply(new_dirs,dir.create,recursive=T)
  
  # List files
  old_files<-list.files(old_dirs,".tif$",recursive=T,full.names = T)
  
  if (.Platform$OS.type == "windows") {
    plan(multisession, workers = cores)
  } else {
    plan(multicore, workers = cores)
  }
  
  results <- unlist(future_lapply(old_files,process_raster,old_path=old_path,new_path=new_path,mask=ref))
  plan(sequential)
  return(results)
}

# 2) R options ####
g <- gc(reset = T); rm(list = ls()) # Empty garbage collector
options(warn = -1, scipen = 999)    # Remove warning alerts and scientific notation

# 3) Set directories ####
root <- '/home/jovyan/common_data'

# 4) Load reference raster ####
ref <- terra::rast(paste0(root,'/atlas_hazards/roi/africa.tif'))

# 5) Set parameters ####

# NTx thresholds to calculate
thresholds<-20:29

# Set historic or future?
sce_climates <- c("historical","future")

# Future setup

# Set future gcms
gcms <- c('MRI-ESM2-0','ACCESS-ESM1-5','MPI-ESM1-2-HR','EC-Earth3','INM-CM5-0')

# Set future scenarios
#ssps<-c('ssp126', 'ssp245', 'ssp370', 'ssp585')
ssps<-c( 'ssp245','ssp585')

# Set future timeframes
#prds<-c('2021_2040', '2041_2060', '2061_2080', '2081_2100')
prds<-c('2021_2040', '2041_2060')

# Future setup
gcms <- c('MRI-ESM2-0','ACCESS-ESM1-5','MPI-ESM1-2-HR','EC-Earth3','INM-CM5-0')
thresholds<-20:29
sce_climates <- c("historical","future")

# Cores for parallel processing
cores<-floor(parallel::detectCores()*0.9)

# 6) prepare historical chirts data ####
tx_pth_hist <- paste0(root,'/chirts/Tmax')
tx_pth_hist_af<-paste0(root,'/chirts_africa/Tmax')

if(!dir.exists(tx_pth_hist_af)){
  process_raster_wrap(old_path=tx_pth_hist,
                      new_path=tx_pth_hist_af,
                      cores=cores,
                      mask=ref)
}


tx_pth_hist<-tx_pth_hist_af

# 7) perform calculation ####

for(sce_climate in sce_climates){
  if (sce_climate == "historical") {
    cat("...processing historical","\n")
    flush.console()
    
    # Historical setup
    yrs <- 1995:2014
    mns <- c(paste0('0',1:9),10:12)
    stp <- base::expand.grid(yrs, mns) %>% base::as.data.frame(); rm(yrs,mns)
    names(stp) <- c('yrs','mns')
    stp <- stp %>%
      dplyr::arrange(yrs, mns) %>%
      base::as.data.frame()
    tx_pth <-tx_pth_hist
    out_dir <- paste0(root,'/atlas_hazards/cmip6/indices/historical')
    1:nrow(stp) %>%
      purrr::map(.f = function(i){
        calc_ntx(yr = stp$yrs[i], mn = stp$mns[i], thr=thresholds,out_dir=out_dir)
        tmpfls <- list.files(tempdir(), full.names=TRUE)
        1:length(tmpfls) %>% purrr::map(.f = function(k) {system(paste0("rm -f ", tmpfls[k]))})
      })
  } else if (sce_climate == "future") {
    for (gcm in gcms) {
      for (ssp in ssps) {
        for (prd in prds) {
          cat("...processing gcm=", gcm, "/ ssp=", ssp, "/ period=", prd, "\n")
          flush.console()
          
          cmb <- paste0(ssp,'_',gcm,'_',prd)
          prd_num <- as.numeric(unlist(strsplit(x = prd, split = '_')))
          yrs <- prd_num[1]:prd_num[2]
          mns <- c(paste0('0',1:9),10:12)
          stp <- base::expand.grid(yrs, mns) %>% base::as.data.frame(); rm(yrs,mns)
          names(stp) <- c('yrs','mns')
          stp <- stp %>%
            dplyr::arrange(yrs, mns) %>%
            base::as.data.frame()
          tx_pth <- paste0(root,'/chirts_cmip6_africa/Tmax_',gcm,'_',ssp,'_',prd) # Daily maximum temperatures
          out_dir <- paste0(root,'/atlas_hazards/cmip6/indices/',cmb)
          
          if (.Platform$OS.type == "windows") {
            plan(multisession, workers = cores)
          } else {
            plan(multicore, workers = cores)
          }
          
          1:nrow(stp) %>%
            furrr::future_map(.f = function(i){
              calc_ntx(yr = stp$yrs[i], mn = stp$mns[i], thr=thresholds,out_dir=out_dir)
              tmpfls <- list.files(tempdir(), full.names=TRUE)
              1:length(tmpfls) %>% purrr::map(.f = function(k) {system(paste0("rm -f ", tmpfls[k]))})
            })
          
          plan(sequential)
          
          
          # Alternative approach using lapply
          if(F){
          indices <- 1:nrow(stp)  # Create a vector of row indices
          
          # Define the function to apply
          process_row <- function(i,thresholds,out_dir,tx_pth) {
            # Perform the main task
            calc_ntx(tx_pth=tx_pth,yr = stp$yrs[i], mn = stp$mns[i], thr = thresholds, out_dir = out_dir)
            
            # Clean up temporary files
            tmpfls <- list.files(tempdir(), full.names = TRUE)
            purrr::map(tmpfls, ~system(paste0("rm -f ", .x)))
          }
          
          # Use future_lapply to apply 'process_row' function over indices in parallel
           future_lapply(indices, process_row,thresholds,out_dir,tx_pth)
          }
          
          plan(sequential)
        }
      }
    }
  } else {
    cat("select one of historical or future for sce_climate \n")
    flush.console()
  }
}