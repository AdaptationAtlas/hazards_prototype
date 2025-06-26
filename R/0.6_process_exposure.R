# Please run 0_server_setup.R before executing this script
# To generate livestock vop you will need to run script 0.4
# To generate crop vop you will need to run script 0.45
# a) Load R functions & packages ####
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

# List of packages to be loaded
packages <- c("terra", 
              "data.table",
              "exactextractr",
              "arrow",
              "geoarrow",
              "pbapply")


pacman::p_load(packages,character.only=T)

# b) Load functions & wrappers ####
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))


# 0) Load and prepare admin vectors and exposure rasters, extract exposure by admin ####
  ## 0.0) Base rast ####
  base_rast <- terra::rast(base_rast_path)

## 0.1) Geographies #####
overwrite_boundary_zones<-T

Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
  file<-geo_files_local[i]
  data<-arrow::open_dataset(file)
  data <- data |> sf::st_as_sf() |> terra::vect()
  data$zone_id <- ifelse(!is.na(data$gaul2_code), data$gaul2_code,
                         ifelse(!is.na(data$gaul1_code), data$gaul1_code, data$gaul0_code))        
  data
})
names(Geographies)<-names(geo_files_local)

base_rast<-terra::rast(base_rast_path)+0

boundaries_zonal<-lapply(1:length(Geographies),FUN=function(i){
  file_path<-file.path(boundaries_int_dir,paste0(names(Geographies)[i],"_zonal.tif"))
  if(!file.exists(file_path)|overwrite_boundary_zones==T){
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
  data.frame(Geographies[[i]])[,c("iso3","admin0_name","admin1_name","admin2_name","zone_id", "gaul0_code", "gaul1_code", "gaul2_code")]
})

names(boundaries_index)<-names(Geographies)

# 1) Crop (MapSPAM) extraction by vector boundaries #####
overwrite_spam<-F
version_spam<-1
source_year_spam<-list(census=2015,values=2015)

files<-list.files(mapspam_pro_dir,".tif$",recursive=T,full.names=T)
files<-files[!grepl("yield",files)]
# Remove yield (one reason for this is that stat<-"mean" returns NA and needs debugging)
library(data.table)


field_descriptions <- data.table::data.table(
  field_name = c(
    "iso3", "admin0_name", "admin1_name", "admin2_name",
    "gaul0_code", "gaul1_code", "gaul2_code", "crop",
    "value", "stat", "exposure", "unit", "tech"
  ),
  type = c(
    "character", "character", "character", "character",
    "numeric", "numeric", "numeric", "factor",
    "numeric", "character", "character", "character", "character"
  ),
  description = c(
    "ISO 3166-1 alpha-3 country code (e.g., 'KEN' for Kenya).",
    "National-level administrative name (admin0), typically the country.",
    "Subnational administrative name (admin1, e.g., province or state); may be NA.",
    "Local administrative name (admin2, e.g., district); may be NA.",
    "GAUL (Global Administrative Unit Layer) code for admin0.",
    "GAUL code for admin1; may be NA.",
    "GAUL code for admin2; may be NA.",
    "Crop or system name (e.g., 'maize', 'rest of crops'); may include livestock categories.",
    "Extracted exposure value (e.g., harvested area, production); units defined in `unit`.",
    "Statistic used for zonal summary (e.g., 'sum', 'mean').",
    "Type of exposure indicator (e.g., 'harv-area', 'vop').",
    "Measurement unit of the value (e.g., 'ha' for hectares, 'usd15' for USD 2015).",
    "MapSPAM production system category (e.g., 'all', 'rainfed'); not applicable for livestock."
  )
)


spam_extracted<-rbindlist(lapply(1:length(files),FUN=function(i){
  cat("Extracting file",i,"/",length(files),basename(files[i]),"\n")
  file<-files[i]
  file_base<-gsub(".tif","",basename(file))
  var<-unlist(tstrsplit(basename(file),"_",keep=2))
  unit<-unlist(tstrsplit(basename(file),"_",keep=3))
  tech<-gsub(".tif","",unlist(tstrsplit(basename(file),"_",keep=4)))
  
  if(var=="yield"){
    stop("Use of stat == mean currently returns NA values. Remove yield from input data or debug error.")
      stat<-"mean"
  }else{
    stat<-"sum"
  }
  
  data<-terra::rast(file)
  
  result<-admin_extract_wrap(data=data,
                             save_dir=dirname(file),
                             filename =file_base,
                             FUN=stat,
                             append_vals=c(exposure=var,unit=unit,tech=tech),
                             var_name="crop",
                             round=1,
                             boundaries_zonal=boundaries_zonal,
                             boundaries_index=boundaries_index,
                             overwrite=overwrite_spam)
  
  attr_file<-file.path(dirname(file),paste0(file_base,"_adm_",stat,".parquet.json"))
  
  filter_colnames<-c("crop","stat","exposure","unit","tech")
  filters <- lapply(filter_colnames, function(split_col) {
    unique(result[[split_col]])
  })
  names(filters) <- filter_colnames
  
  
  if(file.exists(attr_file)){
    attr_dat<-jsonlite::read_json(attr_file)
    date_created<-unlist(attr_dat$date_created)
    version_attr<-unlist(attr_dat$version)
    if(overwrite_spam|version_attr!=version_spam){
      date_created<-Sys.time()
      update_attr_flag<-T
    }else{
      update_attr_flag<-F
    }
  }else{
    update_attr_flag<-T
    date_created<-Sys.time()
  }

  if(update_attr_flag){
  attr_info <- list(
    source = list(input_raster=atlas_data$mapspam_2020v1r2$name,extraction_vect=atlas_data$boundaries$name),
    source_year = list(input_raster=source_year_spam),
    date_created = date_created,
    field_descriptions = field_descriptions,
    filters = filters,
    version = version_spam,
    parent_script = "R/0.6_process_exposure.R",
    variable = var,
    unit = unit,
    technology = tech,
    stat=stat,
    notes = paste0("A table of mapspam crop values (",var,") extracted by boundary vectors then summarized (fun = ",FUN,").")
  )
  
  write_json(attr_info, attr_file, pretty = TRUE)
  }
  
  return(result)
  
}))

# 2) Livestock (GLW) extraction by vector boundaries #####
version_glw<-1
source_year_glw<-list(census=2020,values=2015)

  livestock_no_file<-paste0(glw_pro_dir,"/livestock_number_number.tif")
  if(!file.exists(livestock_no_file)){
    stop("Run script 0.4_create_livestock_exposure.R")
  }
  
  files<-list.files(glw_pro_dir,".tif$",recursive=T,full.names=T)
  
  glw_extracted<-rbindlist(lapply(1:length(files),FUN=function(i){
    cat("Extracting file",i,"/",length(files),basename(files[i]),"\n")
    file<-files[i]
    file_base<-gsub(".tif","",basename(file))
    var<-unlist(tstrsplit(basename(file),"_",keep=2))
    unit<-gsub(".tif","",unlist(tstrsplit(basename(file),"_",keep=3)))
    tech<-NA

    stat<-"sum"
    
    data<-terra::rast(file)
    
    result<-admin_extract_wrap(data=data,
                               save_dir=dirname(file),
                               filename =file_base,
                               FUN=stat,
                               append_vals=c(exposure=var,unit=unit,tech=tech),
                               var_name="crop",
                               round=1,
                               boundaries_zonal=boundaries_zonal,
                               boundaries_index=boundaries_index,
                               overwrite=overwrite_glw)
    
    attr_file<-file.path(dirname(file),paste0(file_base,"_adm_",stat,".parquet.json"))
    
    filter_colnames<-c("crop","stat","exposure","unit","tech")
    filters <- lapply(filter_colnames, function(split_col) {
      unique(result[[split_col]])
    })
    names(filters) <- filter_colnames
    
    
    if(file.exists(attr_file)){
      attr_dat<-jsonlite::read_json(attr_file)
      date_created<-unlist(attr_dat$date_created)
      version_attr<-unlist(attr_dat$version)
      if(overwrite_glw|version_attr!=version_glw){
        date_created<-Sys.time()
        update_attr_flag<-T
      }else{
        update_attr_flag<-F
      }
    }else{
      update_attr_flag<-T
      date_created<-Sys.time()
    }
    
    if(update_attr_flag){
      attr_info <- list(
        source = list(input_raster="GLW4",extraction_vect=atlas_data$boundaries$name),
        source_year = list(input_raster=source_year_glw),
        date_created = date_created,
        field_descriptions = field_descriptions,
        filters = filters,
        version = version_glw,
        parent_script = "R/0.6_process_exposure.R",
        variable = var,
        unit = unit,
        technology = tech,
        stat=stat,
        notes = paste0("A table of glw livestock values (",var,") extracted by boundary vectors then summarized (fun = ",stat,"). Note this analysis uses density adjusted (da) GLW values.")
      )
      
      write_json(attr_info, attr_file, pretty = TRUE)
    }
    
    return(result)
    
  }))
  
# 3) Combine exposure totals by admin areas ####
file<-paste0(exposure_dir,"/exposure_adm_sum.parquet")

if(!file.exists(file)|overwrite_glw|overwrite_spam){
  
  exposure_adm_sum_tab<-rbind(
    spam_extracted,
    glw_extracted
  )
  
  # Make values integer to save space
  exposure_adm_sum_tab[,value:=as.integer(value)]
  
  # Order to optimize parquet performance
  exposure_adm_sum_tab<-exposure_adm_sum_tab[order(iso3,admin0_name,admin1_name,admin2_name,exposure,unit,tech,crop)]

  filter_colnames<-c("crop","stat","exposure","unit","tech")
  filters <- lapply(filter_colnames, function(split_col) {
    unique(exposure_adm_sum_tab[[split_col]])
  })
  names(filters) <- filter_colnames
  
  
    attr_info <- list(
      source = list(input_raster1=atlas_data$mapspam_2020v1r2$name,
                    input_raster2="GLW4",
                    extraction_vect=atlas_data$boundaries$name),
      source_year = list(input_raster1=source_year_spam,input_raster2=source_year_glw),
      date_created = Sys.time(),
      field_descriptions = field_descriptions,
      filters = filters,
      version = list(input_version1=version_spam,input_version2=version_glw),
      parent_script = "R/0.6_process_exposure.R",
      variable = exposure_adm_sum_tab[,unique(exposure)],
      unit = exposure_adm_sum_tab[,unique(unit)],
      technology = exposure_adm_sum_tab[,unique(tech)],
      stat=stat,
      notes = paste0("A merged table of all mapspam crop x technology and glw livestock values extracted by boundary vectors then summarized.")
    )
    
    attr_file<-paste0(file,".json")
    
    write_json(attr_info, attr_file, pretty = TRUE)
    
    # Fix unit issue
    exposure_adm_sum_tab[unit==c("intld2015"),unit:="intld15"]
    exposure_adm_sum_tab[unit==c("usd2015"),unit:="usd15"]
    
    arrow::write_parquet(exposure_adm_sum_tab,file)
}

# 4) Population ######
overwrite_pop<-T
  ## 4.1) Harmonize to atlas base raster ####
hpop_file<-paste0(hpop_int_dir,"/hpop_atlas.tif")
if(!file.exists(hpop_file)|overwrite_pop==T){
  local_files<-list.files(hpop_dir,".tif",full.names = T)
  hpop<-terra::rast(local_files)
  hpop<-terra::crop(hpop,rast(boundaries_zonal[[1]]))
  
  # Convert hpop to density
  hpop<-hpop/cellSize(hpop,unit="ha")
  
  # Resample to base raster
  hpop<-terra::resample(hpop,rast(boundaries_zonal[[1]]))
  
  # Convert back to number per cell
  hpop<-hpop*cellSize(hpop,unit="ha")
  
  terra::writeRaster(hpop,filename =hpop_file,overwrite=T)
}

  ## 4.2) Extraction ####
version_hpop<-1
file<-paste0(exposure_dir,"/hpop_adm_sum.parquet")

if(!file.exists(file)|overwrite_pop==T){  
  
  data<-rast(hpop_file)

  cat("Extracting hpop \n")
  file_base<-gsub(".parquet","",basename(file))
  var<-gsub(".parquet","",unlist(tstrsplit(basename(file),"_",keep=1)))
  unit<-"number"

  stat<-"sum"
  
  hpop_extracted<-admin_extract_wrap(data=data,
                             save_dir=dirname(file),
                             filename =var,
                             FUN=stat,
                             append_vals=c(exposure=var,unit=unit),
                             var_name="type",
                             round=1,
                             boundaries_zonal=boundaries_zonal,
                             boundaries_index=boundaries_index,
                             overwrite=overwrite_pop)
  
  filter_colnames<-c("stat","exposure","unit","type")
  filters <- lapply(filter_colnames, function(split_col) {
    unique(hpop_extracted[[split_col]])
  })
  names(filters) <- filter_colnames
  
  
  field_descriptions2<-copy(field_descriptions)
  field_descriptions2[!field_name %in% c("tech","crop")]
  field_descriptions2<-rbind(field_descriptions2,data.table(field_name = "type",
                                       type = "character",
                                       description = "Type of population measure, rural, urban or total."))
  
  # Make values integer to save space
  hpop_extracted[,value:=as.integer(value)]
  
  # Order to optimize parquet performance
  hpop_extracted<-hpop_extracted[order(iso3,admin0_name,admin1_name,admin2_name)]
  
  arrow::write_parquet(hpop_extracted,file)
  
  attr_file<-paste0(file,".json")
  
  attr_info <- list(
    source = list(input_raster="Worldpop",
                  extraction_vect=atlas_data$boundaries$name),
    source_year = list(input_raster="2020"),
    date_created = Sys.Date(),
    field_descriptions = field_descriptions2,
    filters = filters,
    version = list(input_version=version_hpop),
    parent_script = "R/0.6_process_exposure.R",
    variable = exposure_adm_sum_tab[,unique(exposure)],
    unit = exposure_adm_sum_tab[,unique(unit)],
    type = exposure_adm_sum_tab[,unique(tech)],
    stat=stat,
    notes = paste0("Human population extracted by boundary vectors then summed")
  )
  
  write_json(attr_info, attr_file, pretty = TRUE)
}

