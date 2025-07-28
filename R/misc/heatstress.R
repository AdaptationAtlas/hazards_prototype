# Please run 0_server_setup.R before executing this script
# Heat Stress (NTx) Calculation Script
# By: H. Achicanoy & P. Steward
# April 2024

cat("Running script R/misc/heatstress.R \n")
# 0) Load Packages and Create Functions ####
suppressMessages(library(pacman))
suppressMessages(pacman::p_load(tidyverse, terra, gtools, lubridate, future, furrr, purrr, dplyr, parallel, future.apply, progressr))

#' Calculate NTx Heat Stress Index
#'
#' This function calculates the NTx heat stress index for a given month/year by processing daily 
#' maximum temperature raster data. The function sums the number of days where the temperature exceeds 
#' a specified threshold.
#'
#' @param tx_pth Character. Path to the directory containing temperature rasters.
#' @param yr Numeric or character. Year for which to calculate the index.
#' @param mn Character. Month for which to calculate the index.
#' @param thr Numeric. A threshold or vector of thresholds (default is 40).
#' @param out_dir Character. Output directory where the resulting raster files will be saved.
#' @param verbose Logical. If TRUE, prints progress messages (default is FALSE).
#' @return No return value. Raster files are written to disk.
#' @import terra lubridate purrr
#' @export
calc_ntx <- function(tx_pth, 
                     yr, 
                     mn, 
                     thr = 40, 
                     out_dir, 
                     verbose = FALSE,
                     nexgddp=F,
                     overwrite=F,
                     crop_box=NULL) {
  thr <- as.numeric(thr)
  outfile <- paste0(out_dir, "/NTx", thr, "/NTx", thr, "-", yr, "-", mn, ".tif")
  if(overwrite==F){
    thr <- thr[!file.exists(outfile)]
    outfile <- outfile[!file.exists(outfile)]
  }
  
  if (length(outfile) > 0) {
    # Create required directories
    purrr::map(1:length(outfile), ~ dir.create(dirname(outfile[.x]), showWarnings = FALSE, recursive = TRUE))
    
    # Determine the last day of the month
    last_day <- lubridate::days_in_month(as.Date(paste0(yr, "-", mn, "-01")))
    dts <- seq(from = as.Date(paste0(yr, "-", mn, "-01")),
               to   = as.Date(paste0(yr, "-", mn, "-", last_day)),
               by   = "day")
    
    # Construct file paths for daily maximum temperature data
    if(nexgddp){
      fls <- paste0(tx_pth, "/tasmax_",dts,".tif")
    }else{
      fls <- paste0(tx_pth, "/", yr, "/Tmax.", gsub(pattern = "-", replacement = ".", x = dts, fixed = TRUE), ".tif")
    }
    
    f_exists<-file.exists(fls)
    if(sum(!f_exists)>0){
      warning(paste("Missing files for ",tx_pth,yr,as.character(mn)))
    }
    fls <- fls[f_exists]
    tmx <- terra::rast(fls)
    
    if(!is.null(crop_box)){
      crop_box<-terra::ext(crop_box)
      tmx <- crop(tmx, crop_box)
    }
    
    tmx <- classify(tmx, rcl = cbind(-9999, NA))
    
    # Loop through thresholds and compute NTx
    for (j in 1:length(thr)) {
      if (verbose) {
        cat("...processing n=", length(outfile), "files for yr=", yr, "/ mn=", mn, "/ thr=", thr[j], "\n")
      }
      ntxval <- sum(classify(tmx, matrix(c(-Inf, thr[j], 0, thr[j], Inf, 1), ncol = 3, byrow = TRUE)), na.rm = TRUE)
      terra::writeRaster(ntxval, filename = outfile[j], overwrite = TRUE,
                         filetype = 'COG',
                         gdal = c("COMPRESS=ZSTD", "of=COG"))
    }
    
    # Clean up memory
    rm(tmx)
    gc(verbose = FALSE, full = TRUE, reset = TRUE)
  }
}

#' Process a Raster File by Cropping and Masking
#'
#' This function reads a raster file, crops it using a provided mask, applies the mask, 
#' and then writes the processed raster to a new file.
#'
#' @param raster_file Character. Path to the input raster file.
#' @param old_path Character. The original base path to be replaced.
#' @param new_path Character. The new base path where the processed file will be saved.
#' @param mask SpatRaster. A raster object used for cropping and masking operations.
#' @return Character. The file path of the processed raster.
#' @import terra
#' @export
process_raster <- function(raster_file, old_path, new_path, mask) {
  save_file <- gsub(old_path, new_path, raster_file)
  cat("...processing - ", raster_file, "\n")
  flush.console()
  
  if (!file.exists(save_file)) {
    raster <- terra::rast(raster_file)
    cropped <- terra::crop(raster, mask)
    masked <- terra::mask(cropped, mask)
    terra::writeRaster(masked, save_file)
  }
  return(save_file)
}

#' Process Multiple Raster Files in Parallel
#'
#' This function wraps the \code{process_raster} function to process multiple raster files in parallel.
#' It creates a new directory structure based on the old path and applies cropping and masking to each file.
#'
#' @param old_path Character. Path to the original directory containing raster files.
#' @param new_path Character. New directory where processed rasters will be saved.
#' @param cores Numeric. Number of cores to use for parallel processing.
#' @param mask SpatRaster. A raster object used for cropping and masking operations.
#' @return A character vector containing the file paths of the processed raster files.
#' @import terra future.apply
#' @export
process_raster_wrap <- function(old_path, new_path, cores, mask) {
  # Create new directory structure based on old path
  old_dirs <- list.dirs(old_path)[-1]
  new_dirs <- gsub(old_path, new_path, old_dirs)
  sapply(new_dirs, dir.create, recursive = TRUE)
  
  # List all raster files (TIFF format)
  old_files <- list.files(old_dirs, "\\.tif$", recursive = TRUE, full.names = TRUE)
  
  # Setup parallel processing based on OS type
  if (.Platform$OS.type == "windows") {
    plan(multisession, workers = cores)
  } else {
    plan(multicore, workers = cores)
  }
  
  results <- unlist(future_lapply(old_files, process_raster, old_path = old_path, new_path = new_path, mask = mask))
  plan(sequential)
  return(results)
}


# 1) Directory Setup ####
root_dir            <- "/home/jovyan/common_data"  # Base directory for common data storage
ref_raster_path     <- file.path(root_dir, "atlas_hazards", "roi", "africa.tif")
chirts_path_hist    <- file.path(root_dir, "chirts", "Tmax")
chirts_path_africa  <- file.path(root_dir, "chirts_africa", "Tmax")
hist_index_dir      <- file.path(root_dir, "atlas_hazards", "cmip6", "indices", "historical")
future_chirts_base  <- file.path(root_dir, "chirts_cmip6_africa")
future_index_base   <- file.path(root_dir, "atlas_hazards", "cmip6", "indices")


# If nex-gddp
if(climdat_source=="nexgddp"){
  ref_raster_path     <- file.path(indices_dir,"ssp126_ACCESS-ESM1-5_2021_2040/NDD/NDD-2021-01.tif")
  future_chirts_base   <- file.path(root_dir, "nex-gddp-cmip6/tasmax")
  future_index_base   <- file.path(root_dir, "atlas_nex-gddp_hazards", "cmip6", "indices")
}

# 2) R Options and Environment Setup ####
options(warn = -1, scipen = 999)

# 3) Load Reference Raster and Set Parameters ####
# Load the reference raster for cropping and masking
ref <- terra::rast(ref_raster_path)

# Parameters for NTx calculation and scenarios
thresholds   <- 20:50                   # NTx thresholds to calculate
sce_climates <- c("historical", "future") # Scenario types: historical and future

ssps    <- c("ssp126", "ssp245", "ssp370", "ssp585")
prds    <- c("2021_2040", "2041_2060", "2061_2080", "2081_2100")
baseline_yrs <- 1995:2014                # Historical baseline period
gcms<- c("MRI-ESM2-0", "ACCESS-ESM1-5", "MPI-ESM1-2-HR", "EC-Earth3", "INM-CM5-0")

if(climdat_source=="nexgddp"){
  nexgddp<-T
  gcms_nexgddp<-basename(list.dirs(file.path(future_chirts_base,"ssp126"),recursive = F))
  # Temporaily exlcude 5 delta gcms (extent already fixed)
  gcms<-gcms_nexgddp[!gcms_nexgddp %in% gcms]
  do_historical<-F
  crop_box<- c(-180, 180, -50, 50)
}else{
  nexgddp<-F
  do_historical<-T
  crop_box<-NULL
}

cores<- ceiling(length(thresholds)/2) # Cores for parallel processing

## 3.1) Overwrite existing data? ####
overwrite<-T

# 4) Prepare Historical CHIRTS Data ####
if(do_historical){
  tx_pth_hist <- chirts_path_hist
  tx_pth_hist_af <- chirts_path_africa
  
  if (!dir.exists(tx_pth_hist_af)) {
    process_raster_wrap(old_path = tx_pth_hist,
                        new_path = tx_pth_hist_af,
                        cores = cores,
                        mask = ref)
  }
  
  tx_pth_hist <- tx_pth_hist_af
  sce_climates <- c("historical", "future") # Scenario types: historical and future
}else{
  sce_climates <- "future"
}

cat("Parameters:",
    "\n nexgddp =",nexgddp,
    "\n crop_box =",crop_box,
    "\n ref_raster_path =",ref_raster_path,
    "\n future_chirts_base =",future_chirts_base,
    "\n future_index_base =",future_index_base,
    "\n do_historical =",do_historical,
    "\n gcms = ",gcms,
    "\n ssps =",ssps,
    "\n prds =",prds,
    "\n cores =",cores,
    "\n sce_climates =",sce_climates,"\n")

# 5) Perform Calculations for Historical and Future Scenarios ####
for (sce_climate in sce_climates) {
  if (sce_climate == "historical") {
    cat("...processing historical\n")
    flush.console()
    
    # Historical scenario setup
    yrs <- baseline_yrs
    mns <- c(paste0("0", 1:9), 10:12)
    stp <- expand.grid(yrs, mns) %>% as.data.frame()
    names(stp) <- c("yrs", "mns")
    stp <- stp %>% arrange(yrs, mns) %>% as.data.frame()
    
    tx_pth <- tx_pth_hist
    out_dir <- hist_index_dir
    
    if (.Platform$OS.type == "windows") {
      plan(multisession, workers = cores)
    } else {
      plan(multicore, workers = cores)
    }
    
    lapply(1:nrow(stp), FUN = function(i) {
      calc_ntx(tx_pth = tx_pth, yr = stp$yrs[i], mn = stp$mns[i], thr = thresholds, out_dir = out_dir,crop_box=crop_box)
    })
    
    1:nrow(stp) %>% furrr::future_map(.f = function(i) {
      calc_ntx(tx_pth = tx_pth, yr = stp$yrs[i], mn = stp$mns[i], thr = thresholds, out_dir = out_dir,crop_box=crop_box)
      tmpfls <- list.files(tempdir(), full.names = TRUE)
      unlink(tmpfls, recursive = TRUE, force = TRUE)
    })
    
    plan(sequential)
    
  } else if (sce_climate == "future") {
    
    plan("multisession", workers = cores)
    
    for (gcm in gcms) {
      for (ssp in ssps) {
        for (prd in prds) {
          cmb <- paste0(ssp, "_", gcm, "_", prd)
          prd_num <- as.numeric(unlist(strsplit(x = prd, split = "_")))
          yrs <- prd_num[1]:prd_num[2]
          mns <- c(paste0("0", 1:9), 10:12)
          stp <- expand.grid(yrs, mns) %>% as.data.frame()
          names(stp) <- c("yrs", "mns")
          stp <- stp %>% arrange(yrs, mns) %>% as.data.frame()
          
          if(nexgddp){
            tx_pth <- file.path(future_chirts_base,ssp,gcm)
          }else{
            tx_pth <- file.path(future_chirts_base, paste0("Tmax_", gcm, "_", ssp, "_", prd))
          }
          out_dir <- file.path(future_index_base, cmb)
          
          # Check for existing output files
          expected_count <- nrow(stp) * length(thresholds)
          all_tifs <- list.files(out_dir, pattern = "\\.tif$", recursive = TRUE, full.names = TRUE)
          ntx_tifs <- grep("^NTx\\d+", basename(all_tifs), value = TRUE)
          actual_count <- length(ntx_tifs)
          
          if (actual_count >= expected_count & overwrite==F) {
            cat("...skipping combination (all files present) =", gcm, "/ ssp=", ssp, "/ period=", prd, "\n")
            flush.console()
          } else {
            cat("...processing combination =", gcm, "/ ssp=", ssp, "/ period=", prd, "\n")
            flush.console()
            
            handlers("progress")  # Set up progress handling
            
            with_progress({
              p <- progressor(steps = nrow(stp))
              
              1:nrow(stp) %>% furrr::future_map(.f = function(i) {
                p(message = paste("Processing row", i, "of", nrow(stp)))
                suppressMessages(calc_ntx(tx_pth = tx_pth, 
                                          yr = stp$yrs[i], 
                                          mn = stp$mns[i], 
                                          thr = thresholds, 
                                          out_dir = out_dir,
                                          nexgddp=nexgddp,
                                          crop_box=crop_box,
                                          overwrite=overwrite))
                tmpfls <- list.files(tempdir(), full.names = TRUE)
                unlink(tmpfls, recursive = TRUE, force = TRUE)
              })
            })
            gc()
          }
        }
      }
    }
    
    plan(sequential)
    future:::ClusterRegistry("stop")
    
  } else {
    cat("select one of historical or future for sce_climate \n")
    flush.console()
  }
}

# 6) Check NTx Files Integrity ####

# This section verifies that each NTx tif file can be loaded using terra::rast.
# It runs in parallel with a progress bar and uses tryCatch to record any files that 
# cannot be loaded. Set 'delete_corrupt' to TRUE to automatically delete such files.

delete_corrupt <- FALSE  # Change to TRUE to delete problematic files

# Gather all NTx files from historical and future directories
ntx_files_hist <- list.files(hist_index_dir, pattern = "^NTx\\d+.*\\.tif$", recursive = TRUE, full.names = TRUE)
ntx_files_future <- list.files(future_index_base, pattern = "^NTx\\d+.*\\.tif$", recursive = TRUE, full.names = TRUE)
ntx_files <- c(ntx_files_hist, ntx_files_future)

plan("multisession", workers = cores)
handlers("progress")

with_progress({
  p <- progressor(along = ntx_files)
  
  results <- furrr::future_map_dfr(ntx_files, function(f) {
    res <- tryCatch({
      # Try to load the raster
      r <- terra::rast(f)
      tibble::tibble(file = f, success = TRUE, error_message = NA_character_)
    }, error = function(e) {
      tibble::tibble(file = f, success = FALSE, error_message = as.character(e))
    })
    p()
    res
  })
  
  # Report results
  failed_files <- results %>% dplyr::filter(success == FALSE)
  cat("Checked", nrow(results), "files.\n")
  cat(nrow(failed_files), "files could not be loaded.\n")
  
  if(nrow(failed_files) > 0) {
    print(failed_files)
    if(delete_corrupt) {
      cat("Deleting problematic files...\n")
      file.remove(failed_files$file)
    }
  }
})

plan(sequential)
future:::ClusterRegistry("stop")