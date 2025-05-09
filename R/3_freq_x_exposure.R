#### Intersecting Climate Hazard Frequency with Agricultural Exposure
#
# Script: R/3_freq_x_exposure.R
# Author: Dr. Peter R. Steward (p.steward@cgiar.org)
# Organization: Alliance of Bioversity International and the International Center for Tropical Agriculture (CIAT)
# Project: Africa Agriculture Adaptation Atlas (AAAA)
#
# Description:
# This script intersects climate hazard frequency rasters with crop and livestock
# exposure data to quantify risk-weighted exposure across subnational regions
# in Africa. It is a core component of the Africa Agriculture Adaptation Atlas
# (AAAA) risk modeling pipeline and must be run after the following scripts:
#
#   - R/0_server_setup.R
#   - R/0.6_process_exposure.R
#   - R/1_make_timeseries.R
#   - R/2_calculate_haz_freq.R
#
# Main operations:
#   1. Load hazard frequency rasters, mean hazard values, exposure rasters, and
#      subnational boundary geometries (admin0–2).
#   2. Multiply hazard frequency by:
#        - Crop value of production (VoP, in both int’l and USD units)
#        - Crop harvested area
#        - Livestock numbers or VoP
#   3. Extract hazard statistics by administrative boundary using zonal operations.
#   4. Parallelize processing via `future.apply` with optional integrity checks.
#   5. Write Cloud-Optimized GeoTIFF (COG) rasters and `.parquet` tables for
#      downstream use in policy analysis and climate adaptation planning.
#
# Notes:
# - Skips raster files already processed unless `overwrite = TRUE`.
# - Progress bars and error catching are built in via `progressr` and `check_tif_integrity()`.
# - Designed for high-performance batch processing of multi-model, multi-scenario climate data.
#
cat("Starting 3_freq_x_exposure.R script/n")
# a) Install and load packages ####
packages <- c("terra", 
              "data.table", 
              "exactextractr",
              "s3fs",
              "sf",
              "dplyr",
              "geoarrow", 
              "arrow",
              "sfarrow", 
              "future",
              "future.apply",
              "progressr",
              "stringr", 
              "stringi",
              "httr")

# Call the function to install and load packages
pacman::p_load(char=packages)
# If you are experiencing issues with the admin_extract functions, delete the exactextractr package and use this version:  remotes::install_github("isciences/exactextractr")

# b) Load/create functions & wrappers ####
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

#' Multiply Hazard Risk Raster by Exposure and Save Output
#'
#' This function multiplies a hazard risk raster by an appropriate crop or livestock exposure raster
#' and writes the result to disk as a Cloud-Optimized GeoTIFF (COG). It is designed for batch use
#' with `future_lapply()` for parallel processing across many input files.
#'
#' @param file Character. Path to a single input hazard risk raster (GeoTIFF format). The crop or livestock
#'   name must be the prefix in the file name, separated by a hyphen (e.g., `"maize-risk.tif"`).
#' @param save_dir Character. Directory where the output raster will be saved.
#' @param variable Character. Output variable name (e.g., `"vop"`, `"risk"`, or `"loss"`) appended to the filename.
#' @param overwrite Logical. If `TRUE`, existing output files will be overwritten.
#' @param crop_exposure_path Character or `NULL`. Path to the multi-layer crop exposure raster. Each layer must be named by crop.
#' @param livestock_exposure_path Character or `NULL`. Path to the multi-layer livestock exposure raster. Each layer must be named by species.
#' @param crop_choices Character vector. List of crop names expected in the crop exposure stack.
#' @param verbose Logical. If `TRUE`, prints the input filename for debugging or progress reporting.
#'
#' @details
#' The function parses the crop name from the input filename. If the name matches one of `crop_choices`,
#' the corresponding layer from the crop exposure stack is used. If not, it checks the livestock exposure stack.
#' For `"generic"` entries, the sum of all crop exposure layers is used (excluding variable `"n"`).
#' If `variable == "n"` or a match cannot be found, the operation is skipped (returns `NA`).
#'
#' The result is saved as a Cloud Optimized GeoTIFF with compression. If the raster operation produces
#' a `SpatRaster`, it is named with `-{variable}` and written to `save_dir`. This function is designed
#' to be used inside a parallel loop (e.g., with `future.apply::future_lapply()`).
#'
#' @return No object is returned. Side effect is saving a raster file.
#'
#' @examples
#' \dontrun{
#' # Parallel application across a list of raster files
#' plan(multisession, workers = 4)
#' future_lapply(files, risk_x_exposure,
#'               save_dir = "output/",
#'               variable = "vop",
#'               overwrite = TRUE,
#'               crop_exposure_path = "crop_vop.tif",
#'               livestock_exposure_path = "livestock_vop.tif",
#'               crop_choices = c("maize", "sorghum", "wheat"))
#' }
#'
#' @seealso [future.apply::future_lapply()], [terra::rast()], [terra::writeRaster()]
#' @export
#'    
risk_x_exposure<-function(file,
                          save_dir,
                          variable,
                          overwrite,
                          crop_exposure_path=NULL,
                          livestock_exposure_path=NULL,
                          crop_choices,
                          verbose=F){
  if(verbose){cat(file,"\n")}
  
  data<-terra::rast(file)
  crop<-unlist(data.table::tstrsplit(basename(file),"-",keep=1))
  save_name<-file.path(save_dir,gsub(".tif",paste0("_",variable,".tif"),basename(file)))
  
  if(!file.exists(save_name)|overwrite==T){
    
    if(!is.null(crop_exposure_path)){
      crop_exposure<-terra::rast(crop_exposure_path)
    }
    
    if(!is.null(livestock_exposure_path)){
      livestock_exposure<-terra::rast(livestock_exposure_path)
    }
    
    # vop
    if(crop!="generic"){
      if(crop %in% crop_choices & variable!="n"){
        exposure<-crop_exposure[[crop]]
        data_ex<-data*exposure
      }else{
        if(variable !="ha" & !crop %in% crop_choices){
          exposure<-livestock_exposure[[crop]]
          data_ex<-data*exposure
        }else{
          data_ex<-NA
        }
      }
    }else{
      if(variable!="n"){
        exposure<-sum(crop_exposure)
        data_ex<-data*exposure
      }else{
        data_ex<-NA
      }
    }
    
    if(class(data_ex)=="SpatRaster"){
      names(data_ex)<-paste0(names(data_ex),"_",variable)
      terra::writeRaster(data_ex,
                         file=save_name,
                         overwrite=T,
                         filetype = 'COG',
                         gdal = c("COMPRESS=LZW", of = "COG"))
    }
  }
  
}

# c) (depreciated?) Set up workspace ####
haz_meta<-data.table::fread(haz_meta_url)

if(F){
  # Set scenarios and time frames to analyse
  Scenarios<-c("ssp245","ssp585")
  Times<-c("2021_2040","2041_2060")
  
  # admin levels
  levels<-c(admin0="adm0",admin1="adm1",admin2="adm2")
  
  # Create combinations of scenarios and times
  Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))
  Scenarios[,combined:=paste0(Scenario,"-",Time)]
  
  # Set hazards to include in analysis
  hazards<-c("NTx40","NTx35","HSH_max","HSH_mean","THI_max","THI_mean","NDWS","TAI","NDWL0","PTOT","TAVG")
  haz_2way<-c("PTOT","TAVG")
  hazards2<-c(hazards[!hazards %in% haz_2way],paste0(haz_2way,rep(c("_L","_H"),each=2)))
  
  haz_class<-data.table::fread(haz_class_url)
  haz_class<-haz_class[index_name %in% hazards,list(index_name,description,direction,crop,threshold)]
  haz_classes<-unique(haz_class$description)
  
  # List livestock names
  livestock_choices<-paste0(rep(c("cattle","sheep","goats","pigs","poultry"),each=2),c("_highland","_tropical"))
  
  # Pull out severity classes and associate impact scores
  severity_classes<-unique(fread(haz_class_url)[,list(description,value)])
  setnames(severity_classes,"description","class")
  
  # Create combinations of scenarios and hazards
  scenarios_x_hazards<-data.table(Scenarios,Hazard=rep(hazards,each=nrow(Scenarios)))[,Scenario:=as.character(Scenario)][,Time:=as.character(Time)]
  
  # Load crop names from mapspam metadata
  ms_codes<-data.table::fread(ms_codes_url)[,Code:=toupper(Code)]
  ms_codes<-ms_codes[compound=="no"]
  crop_choices<-unique(c(ms_codes[,sort(Fullname)],haz_class[,unique(crop)]))
}

#### Load datasets (non hazards)

# d) Load and prepare admin vectors and exposure rasters, extract exposure by admin ####
  ## d.1) Geographies #####
  Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
    file<-geo_files_local[i]
    data<-arrow::open_dataset(file)
    data <- data |> sf::st_as_sf() |> terra::vect()
    data$zone_id<-1:length(data)
    data
  })
  names(Geographies)<-names(geo_files_local)
  
  base_rast<-terra::rast(base_rast_path)+0
  
  boundaries_zonal<-lapply(1:length(Geographies),FUN=function(i){
    file_path<-file.path(boundaries_int_dir,paste0(names(Geographies)[i],"_zonal.tif"))
    if(!file.exists(file_path)){
      zones<-Geographies[[i]]
      zone_rast <- rasterize(
        x      = zones, 
        y      = base_rast, 
        field  = "zone_id", 
        background = NA,    # cells not covered by any polygon become NA
        touches    = TRUE   # optional: count cells touched by polygon boundaries
      )
      terra::writeRaster(zone_rast,file_path,overwrite=T)
    }
    file_path
  })
  names(boundaries_zonal)<-names(Geographies)
  
  boundaries_index<-lapply(1:length(Geographies),FUN=function(i){
    data.frame(Geographies[[i]])[,c("iso3","admin0_name","admin1_name","admin2_name","zone_id")]
  })
  
  names(boundaries_index)<-names(Geographies)
  
  ## d.2) Exposure variables ####
  overwrite<-F
    ### d.2.1) Crops (MapSPAM) #####
      #### d.2.1.1) Crop VoP (Value of production) ######
    # To generalize it might be better to just supply a filename for the mapspam
    files<-list.files(mapspam_pro_dir,".tif$",recursive=T,full.names=T)
    crop_vop_file<-grep("vop_intld15_all",files,value=T)
    cat("0.2.1.1) Using crop vop intd file:",basename(crop_vop_file),"\n")
    
    crop_vop_tot<-terra::rast(crop_vop_file)
    #crop_vop_tot_adm_sum<-arrow::read_parquet(file.path(exposure_dir,"crop_vop15_intd15_adm_sum.parquet"))
    
    crop_vop_usd_file<-grep("vop_usd2015_all",files,value=T)
    cat("0.2.1.1) Using crop vop usd file:",basename(crop_vop_usd_file),"\n")
    
    crop_vop_usd15_tot<-terra::rast(crop_vop_usd_file)
    #crop_vop_usd15_tot_adm_sum<-arrow::read_parquet(file.path(exposure_dir,"crop_vop15_cusd15_adm_sum.parquet"))
    
      #### d.2.1.2) Crop Harvested Area #####
    crop_ha_file<-grep("harv-area_ha_all",files,value=T)
    crop_ha_tot<-terra::rast(crop_ha_file)
    cat("0.2.1.2) Using crop harvested area file:",basename(crop_ha_file),"\n")
    #crop_ha_tot_adm_sum<-arrow::read_parquet(file.path(exposure_dir,"crop_ha_adm_sum.parquet"))
  ### d.2.2) Livestock #####
  # x) (Legacy) Livestock Mask #####
  # mask_ls_file<-paste0(glw_int_dir,"/livestock_masks.tif")
  #livestock_mask<-terra::rast(mask_ls_file)
  #livestock_mask_high<-livestock_mask[[grep("highland",names(livestock_mask))]]
  #livestock_mask_low<-livestock_mask[[!grepl("highland",names(livestock_mask))]]
  
    #### d.2.2.1) Livestock Numbers (GLW) ######
  livestock_no_file<-file.path(glw_pro_dir,"/livestock_number_number.tif")
  livestock_no<-terra::rast(livestock_no_file)
  #livestock_no_tot_adm<-arrow::read_parquet(file.path(exposure_dir,"livestock_no_adm_sum.parquet"))
  cat("0.2.2.1) Using livestock number file:",basename(livestock_no_file),"\n")
  
    #### d.2.2.2) Livestock VoP ######
  livestock_vop_file<-file.path(glw_pro_dir,"/livestock_vop_intld2015.tif")
  livestock_vop<-terra::rast(livestock_vop_file)
  #livestock_vop_tot_adm<-arrow::read_parquet(file.path(exposure_dir,"livestock_vop15_intd15_adm_sum.parquet"))
  cat("0.2.2.2) Using livestock vop intd file:",basename(livestock_vop_file),"\n")
  
  livestock_vop_usd_file<-file.path(glw_pro_dir,"livestock_vop_usd2015.tif")
  livestock_vop_usd<-terra::rast(livestock_vop_usd_file)
  #livestock_vop_usd_tot_adm<-arrow::read_parquet(file.path(exposure_dir,"livestock_vop15_cusd15_adm_sum.parquet"))
  cat("0.2.2.2) Using livestock vop usd file:",basename(livestock_vop_usd_file),"\n")
  
# e) Controls ####
  # e.1) Hazard frequency ####
run1<-T
overwrite1<-F
worker_n1<-5
multisession1<-T
round1<-3
version1<-2
  # e.2) Hazard means ####
run2<-F
overwrite2<-F
worker_n2<-5
multisession2<-T
round2<-2
version2<-2
  # e.3) (To Do!) Hazard timeseries ####
run3<-F
overwrite3<-F
worker_n3<-5
multisession3<-T
round3<-2
version3<-1
  # e.4) (In Progress!) Hazard x exposure ####
run4.1<-F
run4.2<-F
worker_n4<-20
worker_n4_check<-20
multisession4<-T
round4<-2
version4<-1

overwrite4<-F
do_vop<-F
do_vop_usd<-F
do_ha<-T
do_n<-F
check4.1<-T

# Start timeframe loop ####
for(tx in 1:length(timeframe_choices)){
  timeframe<-timeframe_choices[tx]
  
  cat("Processing", timeframe, tx, "/", length(timeframe_choices),
      "started at time:",format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
  
  haz_timeseries_dir<-file.path(indices_dir2,timeframe)
  cat("haz_timeseries_dir =",haz_timeseries_dir,"\n")
  
  # Create output folders
  haz_time_class_dir <- file.path(atlas_dirs$data_dir$hazard_timeseries_class, timeframe)
  haz_time_risk_dir <- file.path(atlas_dirs$data_dir$hazard_timeseries_risk, timeframe)
  haz_risk_dir <- file.path(atlas_dirs$data_dir$hazard_risk, timeframe)
  haz_mean_dir <- file.path(atlas_dirs$data_dir$hazard_timeseries_mean, timeframe)
  haz_time_int_dir <- file.path(atlas_dirs$data_dir$hazard_timeseries_int, timeframe)
  
  haz_risk_int_dir <- ensure_dir(haz_risk_dir, "intermediate")
  
  
  # 1) Extract hazard freq by admin ####
  if(run1){
    cat(timeframe,"1) Extract hazard freq by admin\n")
    cat("run1=",run1,
        "\noverwrite1=",overwrite1,
        "\nworker_n1=",worker_n1,
        "\nmultisession1=",multisession1,
        "\nround1=",round1,
        "\nversion1=",version1
    )
    
    files<-data.table(file=list.files(haz_risk_dir,".tif$",full.names = T))
    files[, c("model","severity") := tstrsplit(basename(file)[1], "_", keep=c(2,3),fixed = TRUE), by = file
    ][,severity:=gsub(".tif","",severity)
    ][,type:="solo"
    ][grepl("_int.tif",file),type:="int"
    ][,mod_x_type_x_sev:=paste0("haz-freq_",model,"_",type,"_adm_",severity)]
    
    mod_x_type_x_sev<-files[,unique(mod_x_type_x_sev)]
    
    cat(timeframe,"1) Combinations of model x type x severity = ",length(mod_x_type_x_sev),"\n")
    
    if(length(mod_x_type_x_sev)<worker_n1){
      worker_n1<-length(mod_x_type_x_sev)
    }
    
    files<-data.frame(files)
    
    id_vars<-c("iso3","admin0_name","admin1_name","admin2_name")
    split_delim<-"_"
    split_colnames<-c("scenario", "model", "timeframe", "hazard", "hazard_vars", "crop", "severity")
    extract_stat<-"mean"
    order_by<-c("iso3","admin0_name","admin1_name","admin2_name","crop")
    
    
    set_parallel_plan(n_cores=worker_n1,use_multisession=multisession1)
    
    # Enable progressr
    progressr::handlers(global = TRUE)
    progressr::handlers("progress")
    
    # Wrap the parallel processing in a with_progress call
    p<-with_progress({
      # Define the progress bar
      prog <- progressr::progressor(along = 1:length(mod_x_type_x_sev))
      
      invisible(
        future.apply::future_lapply(1:length(mod_x_type_x_sev),FUN=function(i){
          mts_choice<-mod_x_type_x_sev[i]
          prog(sprintf("Processing %s (%d of %d)", mts_choice, i, length(mod_x_type_x_sev)))
          
          #cat("Model x type x severity", i,"/", length(mod_x_type_x_sev),mts_choice,"        \r")
          
          save_file<-file.path(haz_risk_dir,paste0(mts_choice,".parquet"))
          
          if(!file.exists(save_file)|overwrite1){
            
            file_choices<-files$file[files$mod_x_type_x_sev==mts_choice]
            
            rast_data<-terra::rast(file_choices)
            
            if(any(table(names(rast_data))>1)){
              stop("duplicate layer names present")
            }
            
            result<-rbindlist(lapply(1:length(boundaries_zonal),FUN=function(k){
              cat("Model x type x severity", i,"/", length(mod_x_type_x_sev),mts_choice," - extracting boundary",k,"        \r")
              
              boundary_choice<-boundaries_zonal[[k]]
              zonal_rast<-terra::rast(boundary_choice)
              
              dat<- zonal(
                x   = rast_data, 
                z   = zonal_rast, 
                fun = extract_stat, 
                na.rm = TRUE
              ) 
              
              dat<-merge(dat,boundaries_index[[k]],by="zone_id",all.x=T,sort=F)
              dat$zone_id<-NULL
              return(dat)
              
            }))
            
            result_long<-data.table::melt(result, id.vars = id_vars)
            
            # Optional rounding
            if (!is.null(round1)) {
              result_long[, value := round(value, round1)]
            }
            
            # Clean and split variable column
            result_long[, c(split_colnames) := tstrsplit(variable[1], split_delim, fixed = TRUE), by = variable]
            result_long[, variable := NULL]
            
            # Optimize ordering
            if(!is.null(order)){
              result_long <- result_long %>% arrange(across(all_of(order_by)))
            }
            
            arrow::write_parquet(result_long,save_file)
            
            # Add attributes
            attr_file<-paste0(save_file,".json")
            
            attr_info <- list(
              source = list(input_raster=file_choices,extraction_rast=atlas_data$boundaries$name),
              extraction_method = "zonal",
              geo_filters = id_vars,
              season_type=timeframe,
              filters=list(
                timeframe = result_long[,unique(timeframe)],
                model=result_long[,unique(model)],
                scenario=result_long[,unique(scenario)],
                hazard=result_long[,unique(hazard)],
                hazard_vars=result_long[,unique(hazard_vars)],
                crop=result_long[,unique(crop)],
                severity=result_long[,unique(severity)]
              ),
              format=".parquet",
              date_created = Sys.time(),
              version = version1,
              parent_script = "3/3_freq_x_exposure.R - section 1",
              value_variable = "haz-freq",
              unit = "proportion",
              extract_stat=extract_stat,
              notes = paste0("A table of hazard occurence (a proportion 0-1 as the average of of a binary timeseries of 0/1 present/absent hazard) extracted by rasterized boundary data then summarized (fun = ",extract_stat,").")
            )
            
            write_json(attr_info, attr_file, pretty = TRUE)
            
            
            rm(rast_data,result,result_long)
            gc()
            
          }
          
        })
      )
    })
    
    plan(sequential)
    
    cat(timeframe,"1) Extract hazard freq by admin - Complete \n")
    
  }
  
  # 2) Extract hazard means and sd by admin ####
  if(run2){
    
    cat(timeframe,"2) Extract hazard means and sd by admin\n")
    cat("run2=",run2,
        "\noverwrite2=",overwrite2,
        "\nworker_n2=",worker_n2,
        "\nmultisession2=",multisession2,
        "\nround2=",round2,
        "\nversion2=",version2
    )
    
    files<-data.table(file=list.files(haz_mean_dir,".tif$",full.names = T))
    files[, c("model","base_stat") := tstrsplit(basename(file)[1], "_", keep=c(2,5),fixed = TRUE), by = file
    ][,base_stat:=gsub(".tif","",base_stat)][,mod_x_stat:=paste0(model,"_",base_stat)]
    
    ms_options<-files[,unique(mod_x_stat)]
    
    if(length(ms_options)<worker_n2){
      worker_n2<-length(ms_options)
    }
    
    files<-data.frame(files)
    
    id_vars<-c("iso3","admin0_name","admin1_name","admin2_name")
    split_delim<-"_"
    split_colnames<-c("scenario", "model", "timeframe", "hazard","stat")
    extract_stat<-"mean"
    order_by<-c("iso3","admin0_name","admin1_name","admin2_name","hazard")
    
    # Units for attr
    units<-unique(haz_meta[,.(variable.code,base_unit)])
    
    set_parallel_plan(n_cores=worker_n2,use_multisession=multisession2)
    
    # Enable progressr
    progressr::handlers(global = TRUE)
    progressr::handlers("progress")
    
    # Wrap the parallel processing in a with_progress call
    p<-with_progress({
      # Define the progress bar
      prog <- progressr::progressor(along = 1:length(ms_options))
      
      invisible(
        future.apply::future_lapply(1:length(ms_options),FUN=function(i){
          m_choice<-ms_options[i]
          prog(sprintf("Processing %s (%d of %d)", m_choice, i, length(ms_options)))
          
          save_file<-file.path(haz_mean_dir,paste0("haz-means_adm_",unlist(tstrsplit(m_choice,"_",keep=1)),".parquet"))
          
          if(!file.exists(save_file)|overwrite2){
            
            file_choices<-files$file[files$mod_x_stat==m_choice]
            
            rast_data<-terra::rast(file_choices)
            
            if(any(table(names(rast_data))>1)){
              stop("duplicate layer names present")
            }
            
            result<-rbindlist(lapply(1:length(boundaries_zonal),FUN=function(k){
              cat("Model x stat", i,"/", length(ms_options),m_choice," - extracting boundary",k,"raster layers = ",nlyr(rast_data),"        \r")
              
              boundary_choice<-boundaries_zonal[[k]]
              zonal_rast<-terra::rast(boundary_choice)
              
              dat<- zonal(
                x   = rast_data, 
                z   = zonal_rast, 
                fun = extract_stat, 
                na.rm = TRUE
              ) 
              
              dat<-merge(dat,boundaries_index[[k]],by="zone_id",all.x=T,sort=F)
              dat$zone_id<-NULL
              return(dat)
              
            }))
            
            result_long<-data.table::melt(result, id.vars = id_vars)
            
            # Optional rounding
            if (!is.null(round2)) {
              result_long[, value := round(value, round1)]
            }
            
            # Clean and split variable column
            result_long[, c(split_colnames) := tstrsplit(variable[1], split_delim, fixed = TRUE), by = variable]
            result_long[, variable := NULL]
            
            # Optimize ordering
            if(!is.null(order)){
              result_long <- result_long %>% arrange(across(all_of(order_by)))
            }
            
            arrow::write_parquet(result_long,save_file)
            
            # Add attributes
            attr_file<-paste0(save_file,".json")
            
            attr_info <- list(
              source = list(input_raster=file_choices,extraction_rast=atlas_data$boundaries$name),
              extraction_method = "zonal",
              geo_filters = id_vars,
              season_type=timeframe,
              filters=list(
                timeframe = result_long[,unique(timeframe)],
                model=result_long[,unique(model)],
                scenario=result_long[,unique(scenario)],
                hazard=result_long[,unique(hazard)],
                stat=result_long[,unique(stat)]
              ),
              format=".parquet",
              date_created = Sys.time(),
              version = version1,
              parent_script = "3/3_freq_x_exposure.R - section 2",
              value_variable = "haz-mean",
              hazard_units=units,
              extract_stat=extract_stat,
              notes = paste0("A table of mean or sd values for climate hazards or indices (calculated per pixel for all years or seasons within a timeframe) extracted by rasterized boundary data then summarized (fun = ",extract_stat,").")
            )
            
            write_json(attr_info, attr_file, pretty = TRUE)
            
            
            rm(rast_data,result,result_long)
            gc()
            
          }
          
        })
      )
    })
    
    plan(sequential)
    
    cat(timeframe,"2) Extract hazard means and sd by admin - Complete \n")
    
  }
  
  # 3) **To Do**Extract hazard timeseries by admin ####
  if(run3){
    
    # Check original format
    file_o<-file.choose()
    data_o<-read_parquet(file_o)
    
    haz_timeseries_files<-list.files(haz_timeseries_dir,".tif",full.names = T)
    haz_timeseries_files<-grep(paste(hazards,collapse = "|"),haz_timeseries_files,value=T)
    
    # Limit files to ensemble mean (very large files otherwise)
    # See 2.1_create_monthly_haz_tables.R for ideas on alternative methods for getting to this information
    # If needed per model suggest looping over models (or futher file splitting measures)
    #haz_timeseries_files_sd<-grep("ENSEMBLEsd",haz_timeseries_files,value = T)
    haz_timeseries_files<-grep("ENSEMBLEmean",haz_timeseries_files,value = T)
    
    # Load all timeseries data into a raster stack
    haz_timeseries<-terra::rast(haz_timeseries_files)
    #haz_timeseries_sd<-terra::rast(haz_timeseries_files)
    
    # Update names of raster stack to be filename/year
    layer_names<-unlist(lapply(1:length(haz_timeseries_files),FUN=function(i){
      file<-haz_timeseries_files[i]
      layers<-names(terra::rast(file))
      file<-gsub(".tif","",tail(unlist(strsplit(file,"/")),1),fixed=T)
      paste0(file,"_year",layers)
    }))
    names(haz_timeseries)<-layer_names
    
    #layer_names<-unlist(lapply(1:length(haz_timeseries_files_sd),FUN=function(i){
    #  file<-haz_timeseries_files_sd[i]
    #  layers<-names(terra::rast(file))
    #  file<-gsub(".tif","",tail(unlist(strsplit(file,"/")),1),fixed=T)
    #  paste0(file,"_year",layers)
    #}))
    #names(haz_timeseries_sd)<-layer_names
    
    # Extract hazard values by admin areas and average them
    # Extract by admin0
    haz_timeseries_adm<-admin_extract(haz_timeseries,Geographies["admin0"],FUN="mean")
    write_parquet(sf::st_as_sf(haz_timeseries_adm$admin0), paste0(haz_timeseries_dir,"/haz_timeseries_adm0.parquet"))
    
    # Extract by admin1
    haz_timeseries_adm<-admin_extract(haz_timeseries,Geographies["admin1"],FUN="mean")
    write_parquet(sf::st_as_sf(haz_timeseries_adm$admin1), paste0(haz_timeseries_dir,"/haz_timeseries_adm1.parquet"))
    
    # Extract by admin2
    haz_timeseries_adm<-admin_extract(haz_timeseries,Geographies["admin2"],FUN="mean")
    write_parquet(sf::st_as_sf(haz_timeseries_adm$admin2), paste0(haz_timeseries_dir,"/haz_timeseries_adm2.parquet"))
    
    # Restructure data into tabular form
    filename<-paste0(haz_timeseries_dir,"/haz_timeseries.parquet")
    # Extract data from vector files and restructure into tabular form
    haz_timeseries_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
      level<-levels[i]
      print(level)
      
      data<-data.table(data.frame(read_parquet(paste0(haz_timeseries_dir,"/haz_timeseries_",levels[i],".parquet"))))
      
      data<-data[,!c("admin_name","iso3","geometry")]
      
      admin<-"admin0_name"
      
      if(level %in% c("adm1","adm2")){
        admin<-c(admin,"admin1_name")
        data<-suppressWarnings(data[,!"a1_a0"])
      }
      
      if(level=="adm2"){
        admin<-c(admin,"admin2_name")
        data<-suppressWarnings(data[,!"a2_a1_a0"])
      }
      
      colnames(data)<-gsub("_nam$","_name",colnames(data))
      
      data<-melt(data,id.vars = admin)
      
      data[,variable:=gsub("historical","historic-NA-historic",variable[1]),by=variable
      ][,variable:=stringi::stri_replace_all_regex(variable[1],pattern=c("max_max","min_min","mean_mean"),replacement=c("max-max","min-min","mean-mean"),vectorise_all = F),by=variable
      ][,variable:=gsub(".","_",variable[1],fixed=T),by=variable
      ][,variable:=gsub("_year","-",variable[1],fixed=T),by=variable]
      
      data[,variable:=stringi::stri_replace_all_regex(variable[1],
                                                      pattern=Scenarios[Scenario!="historic",paste0("_",Time)],
                                                      replacement = Scenarios[Scenario!="historic",paste0("-",Time)],
                                                      vectorise_all = F),by=variable]
      
      data[,variable:=stringi::stri_replace_all_regex(variable[1],
                                                      pattern=Scenarios[,paste0(Time,"_")],
                                                      replacement = Scenarios[,paste0(Time,"-")],
                                                      vectorise_all = F),by=variable]
      
      data[,variable:=stringi::stri_replace_all_regex(variable[1],
                                                      pattern=paste0("_",haz_meta$code),
                                                      replacement =paste0("-",haz_meta$code),
                                                      vectorise_all = F),by=variable]
      
      data[,variable:=stringi::stri_replace_all_regex(variable[1],
                                                      pattern=paste0("_",Scenarios$Scenario),
                                                      replacement =paste0("-",Scenarios$Scenario),
                                                      vectorise_all = F),by=variable]
      
      data[,variable:=stringi::stri_replace_all_regex(variable[1],
                                                      pattern=paste0(Scenarios$Scenario,"_"),
                                                      replacement =paste0(Scenarios$Scenario,"-"),
                                                      vectorise_all = F),by=variable]
      
      variable<-cbind(data$variable,data.table(do.call("cbind",tstrsplit(data$variable,"-"))[,-1]))
      colnames(variable)<-c("variable","scenario","model","timeframe","hazard","hazard_stat","year")
      variable[is.na(hazard_stat),hazard:=gsub("_","-",hazard)
      ][is.na(hazard_stat),hazard_stat:=unlist(tstrsplit(hazard,"-",keep=2))
      ][,hazard:=unlist(tstrsplit(hazard,"-",keep=1))]
      
      data<-merge(data,unique(variable),all.x=T)[,variable:=NULL]
      
      return(data)
      
    }),fill=T)
    
    haz_timeseries_tab<-haz_timeseries_tab[,.(admin0_name,admin1_name,admin2_name,scenario,model,timeframe,hazard,hazard_stat,year,value)]
    
    # Save mean values as feather object
    arrow::write_parquet(haz_timeseries_tab,filename)
  }
  
  # 4) **To Do**Hazard risk x exposure ####
  if(run4.1|run4.2){
    
    cat(timeframe,"4) Hazard risk x exposure \n")
    
    cat("4) Controls:",
        "\nrun4=",run4.1,
        "\nrun4=",run4.2,
        "\noverwrite4=",overwrite4,
        "\nworker_n4=",worker_n4,
        "\nmultisession4=",multisession4,
        "\nround4=",round4,
        "\nversion4=",version4,
        "\ndo_vop=",do_vop,
        "\ndo_vop_usd=",do_vop_usd,
        "\ndo_ha=",do_ha,
        "\ndo_n=",do_n,
        "\ncheck4.1=",check4.1
    )
    
    # 4.0) Set-up ####
    
    ls_files<-list.files(glw_pro_dir,".tif$",recursive=T,full.names=T)
    spam_files<-files<-list.files(mapspam_pro_dir,".tif$",recursive=T,full.names=T)
    
    crop_vop_path<-crop_vop_file
    crop_vop_usd_path<-crop_vop_usd_file
    crop_ha_path<-crop_ha_file
    
    livestock_vop_path<-livestock_vop_file
    livestock_vop_usd_path<-livestock_vop_usd_file
    livestock_no_path<-livestock_no_file
    
    cat("4) Exposure filepaths",
        "\ncrop_vop_path=",crop_vop_path,
        "\ncrop_vop_usd_path=",crop_vop_usd_path,
        "\ncrop_ha_path=",crop_ha_path,
        "\nlivestock_vop_path=",livestock_vop_path,
        "\nlivestock_vop_usd_path=",livestock_vop_usd_path,
        "\nlivestock_no_path=",livestock_no_path)
    
    # Crop choices only 
    crop_choices<-crop_choices[!grepl("_tropical|_highland",crop_choices)]
    
    # List files
    files<-list.files(haz_risk_dir,".tif$",full.names = T)
    
    # 4.1) Multiply Hazard Freq by Exposure #####
    if(run4.1){
      cat(timeframe,"4.1) Intersecting hazard risk x exposure \n")
      
      freq_exp_dirs<-list()
      
      if(do_vop){
        cat(timeframe,"4.1.1) Intersecting hazard risk x exposure - vop-intdlr \n")
        haz_risk_vop_dir <- file.path(atlas_dirs$data_dir$haz_risk_vop, timeframe)
        freq_exp_dirs$haz_risk_vop_dir<-haz_risk_vop_dir
        
        set_parallel_plan(n_cores=worker_n4,use_multisession=multisession4)
        future.apply::future_lapply(files,
                                    risk_x_exposure,             
                                    save_dir=haz_risk_vop_dir,
                                    variable="vop",
                                    overwrite = overwrite4,
                                    crop_exposure_path = crop_vop_path,
                                    livestock_exposure_path=livestock_vop_path,
                                    crop_choices=crop_choices)
        future::plan(sequential)
        
        cat(timeframe,"4.1.1) Intersecting hazard risk x exposure - vop-intdlr - Complete \n")
      }
      
      if(do_vop_usd){
        cat(timeframe,"4.1.2) Intersecting hazard risk x exposure - vop-usd \n")
        
        haz_risk_vop_usd_dir <- file.path(atlas_dirs$data_dir$haz_risk_vop_usd, timeframe)
        freq_exp_dirs$haz_risk_vop_usd_dir<-haz_risk_vop_usd_dir
        
        set_parallel_plan(n_cores=worker_n4,use_multisession=multisession4)
        future.apply::future_lapply(files,
                                    risk_x_exposure,             
                                    save_dir=haz_risk_vop_usd_dir,
                                    variable="vop",
                                    overwrite = overwrite4,
                                    crop_exposure_path = crop_vop_usd_path,
                                    livestock_exposure_path=livestock_vop_usd_path,
                                    crop_choices=crop_choices)
        future::plan(sequential)
        cat(timeframe,"4.1.2) Intersecting hazard risk x exposure - vop-usd - Complete\n")
      }
      
      if(do_ha){
        cat(timeframe,"4.1.3) Intersecting hazard risk x exposure - harv-area \n")
        
        haz_risk_ha_dir <- file.path(atlas_dirs$data_dir$haz_risk_ha, timeframe)
        freq_exp_dirs$haz_risk_ha_dir<-haz_risk_ha_dir
        
        set_parallel_plan(n_cores=worker_n4,use_multisession=multisession4)
        future.apply::future_lapply(files,
                                    risk_x_exposure,             
                                    save_dir=haz_risk_ha_dir,
                                    variable="ha",
                                    overwrite = overwrite4,
                                    crop_exposure_path = crop_ha_path,
                                    crop_choices=crop_choices)
        future::plan(sequential)
        cat(timeframe,"4.1.4) Intersecting hazard risk x exposure - harv-area \n")
      }
      
      if(do_n){
        cat(timeframe,"4.1.4) Intersecting hazard risk x exposure - n \n")
        
        haz_risk_n_dir <- file.path(atlas_dirs$data_dir$haz_risk_n, timeframe)
        freq_exp_dirs$haz_risk_n_dir<-haz_risk_n_dir
        
        set_parallel_plan(n_cores=worker_n4,use_multisession=multisession4)
        future.apply::future_lapply(files,
                                    risk_x_exposure,             
                                    save_dir=haz_risk_n_dir,
                                    variable="n",
                                    overwrite = overwrite4,
                                    livestock_exposure=livestock_no_path,
                                    crop_choices=crop_choices)
        future::plan(sequential)
        
        cat(timeframe,"4.1.4) Intersecting hazard risk x exposure - harv-area - Complete\n")
      }
      # 4.1.1) Check results ######
      if(check4.1){
        
        for(i in 1:length(freq_exp_dirs)){
          check_folder<-freq_exp_dirs[[i]]
          cat(timeframe,"4.1) Checking file integrity",basename(check_folder),"\n")
          
          result<-check_tif_integrity (dir_path = check_folder,
                                       recursive = FALSE,
                                       pattern = "*.tif", # uses glob so make sure the * is present
                                       n_workers_files = worker_n4_check,
                                       n_workers_folders = 1,
                                       use_multisession = multisession4,
                                       delete_corrupt  = FALSE)
          result<-result[success==F]
          
          if(nrow(result)>0){
            cat("4.1)",basename(check_folder),"Error: some files could not be read:\n")
            print(result)
            
            error_dir<-file.path(dirname(check_folder),"errors")
            if(!dir.exists(error_dir)){
              dir.create(error_dir)
            }
            error_file<-file.path(error_dir,paste0(timeframe,"_read-errors.csv"))
            fwrite(result,error_file)
          }
        }
        
        cat(timeframe,"4.1) File integrity check complete\n")
      }
      
      cat(timeframe,"4.1) Intersecting hazard risk x exposure - Complete \n")
      
      # The code below can be used to interrogate output files (note the paths are old and require revision)
      if(F){
        crop_focus<-"potato"
        severity<-"severe"
        vop<-terra::rast(file.path(exposure_dir,"crop_vop.tif"))[[crop_focus]]
        
        (file<-list.files(haz_risk_vop_dir,paste0(crop_focus,"-",severity,"-int"),full.names = T)[1])
        risk_vop<-terra::rast(file)
        # Pick a scenario
        risk_vop<-risk_vop[[1]]
        
        # Values should not exceed 1
        plot(risk_vop/vop)
        
        # Check input hazard data
        (file<-list.files(haz_risk_dir,paste0(crop_focus,"-moderate-int"),full.names = T)[1])
        risk<-terra::rast(file)
        
        # Check max values
        max_values <- global(risk, fun = max, na.rm = TRUE)
        max_values<-data.table(variable=row.names(max_values),value=max_values)
        max_values[value.max>1]
        
        # Check that interacting hazards do not sum to greater than 1
        risk_names<-names(risk)
        risk_names<-risk_names[grepl("NDWS+NTxS+PTOT_G",risk_names,fixed = T) & grepl("ssp585-2041",risk_names)]
        i<-which(risk[[1]][]>0.5)
        (values<-data.table(risk[[risk_names]][i]))
        values_ss<-values[sample(1:nrow(values),10,replace = F)]
        t(values_ss)
        (issue<-which(apply(t(values)[2:8,],2,sum)>1.00001))
        
      }
    }
    
    # 4.2) Extract Freq x Exposure by Geography #####
    # Dev Note: This section can be revised to match zonal extraction in section 1 ####
    if(do4.2){
      
      for(INT in c(T,F)){
        if(do_vop){
          cat("Interaction =",INT,"variable = vop\n")
          
          haz_risk_exp_extract(severity_classes,
                               interactions=INT,
                               folder=haz_risk_vop_dir,
                               overwrite=overwrite,
                               Geographies=Geographies,
                               rm_crop=NULL,
                               rm_haz=NULL)
        }
        
        if(do_vop_usd){
          cat("Interaction =",INT,"variable = vop_usd\n")
          
          haz_risk_exp_extract(severity_classes,
                               interactions=INT,
                               folder=haz_risk_vop_usd_dir,
                               overwrite=overwrite,
                               Geographies=Geographies,
                               rm_crop=NULL,
                               rm_haz=NULL)
        }
        
        if(do_ha){
          cat("Interaction =",INT,"variable = ha\n")
          
          haz_risk_exp_extract(severity_classes,
                               interactions=INT,
                               folder=haz_risk_ha_dir,
                               overwrite=overwrite,
                               Geographies=Geographies,
                               rm_crop=NULL,
                               rm_haz=NULL)
        }
        
        if(do_n){
          cat("Interaction =",INT,"variable = n\n")
          
          haz_risk_exp_extract(severity_classes,
                               interactions=INT,
                               folder=haz_risk_n_dir,
                               overwrite=overwrite,
                               Geographies=Geographies,
                               rm_crop=NULL,
                               rm_haz=NULL)
        }
      }
      
      # 4.2.1) Check results #####
      if(F){
        admin0<-"Nigeria"
        crop_focus<-"potato"
        
        vop<-arrow::read_parquet(list.files("Data/exposure","crop_vop_adm_sum",full.names = T))
        (vop_val<-vop[admin0_name==admin0 & is.na(admin1_name) & crop==crop_focus,value])
        
        # Check resulting files
        (file<-list.files(haz_risk_vop_dir,"moderate_adm0_int",full.names = T))
        data<-sfarrow::st_read_parquet(file)
        dat_names<-c(names(data)[1:3],grep("ssp585.2021_2040.any.NDWS.NTxS.PTOT_G.potato.moderate",names(data),value=T))
        data<-data.table(data[dat_names])
        # This number should not exceed 1
        data[admin0_name==admin0,4]/vop_val
      }
      
    }
    # 4.3) (Legacy) Restructure Extracted Data ####
    levels<-c(admin0="adm0",admin1="adm1",admin2="adm2")
    
    for(SEV in tolower(severity_classes$class)){
      for(INT in c(T,F)){
        if(do_vop==T){
          cat(SEV,"- interaction =",INT,"variable = vop\n")
          recode_restructure_wrap(folder=haz_risk_vop_dir,
                                  file="adm",
                                  crops=crop_choices,
                                  livestock=livestock_choices,
                                  exposure_var="vop",
                                  severity=SEV,
                                  overwrite=overwrite,
                                  levels=levels,
                                  interaction=INT,
                                  hazards=haz_meta[,unique(type)])
        }
        
        if(do_vop_usd==T){
          cat(SEV,"- interaction =",INT,"variable = vop_usd\n")
          # Vop
          recode_restructure_wrap(folder=haz_risk_vop_usd_dir,
                                  file="adm",
                                  crops=crop_choices,
                                  livestock=livestock_choices,
                                  exposure_var="vop",
                                  severity=SEV,
                                  overwrite=overwrite,
                                  levels=levels,
                                  interaction=INT,
                                  hazards=haz_meta[,unique(type)])
        }
        
        # Harvested area
        if(do_ha==T){
          cat(SEV,"- interaction =",INT,"variable = do_ha\n")
          recode_restructure_wrap(folder=haz_risk_ha_dir,
                                  file="adm",
                                  crops=crop_choices,
                                  livestock=livestock_choices,
                                  exposure_var="ha",
                                  severity=SEV,
                                  overwrite=overwrite,
                                  levels=levels,
                                  interaction=INT,
                                  hazards=haz_meta[,unique(type)])
        }
        
        # Numbers
        if(do_n==T){
          cat(SEV,"- interaction =",INT,"variable = do_n\n")
          recode_restructure_wrap(folder=haz_risk_n_dir,
                                  file="adm",
                                  crops=crop_choices,
                                  livestock=livestock_choices,
                                  exposure_var="number",
                                  severity=SEV,
                                  overwrite=overwrite,
                                  levels=levels,
                                  interaction=INT,
                                  hazards=haz_meta[,unique(type)])
        }
      }
      
    }
    
    # 4.3.1) (Legacy) Check results #####
    if(F){
      # Check results
      (file<-list.files(haz_risk_vop_dir,"moderate_adm_int",full.names = T))
      
      vop<-arrow::read_parquet(list.files("Data/exposure","crop_vop_adm_sum",full.names = T))
      data<-arrow::read_parquet(file)
      
      admin0<-"Nigeria"
      crop_focus<-"potato"
      
      # The numbers should never exceed 1
      (vop_val<-vop[admin0_name==admin0 & is.na(admin1_name) & crop==crop_focus,value])
      data[admin0_name==admin0 & is.na(admin1_name) & crop==crop_focus & hazard=="any",value]/vop_val
      
      print(head(data))
      print(data[,unique(hazard)])
      print(data[,unique(hazard_vars)])
      print(data[,unique(crop)])
      
    }
    
    cat(timeframe,"4) Hazard risk x exposure - Complete \n")2
  }
  
}