# Please run 0_server_setup.R before executing this script
# For generate livestock vop you will need to run script 0.4 first
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
Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
  file<-geo_files_local[i]
  data<-arrow::open_dataset(file)
  data <- data |> sf::st_as_sf() |> terra::vect()
  data
})
names(Geographies)<-names(geo_files_local)
# 1) Crop (MapSPAM) extraction by vector boundaries #####
overwrite_spam<-F
version_spam<-1
source_year_spam<-list(census=2015,values=2015)

files<-list.files(mapspam_pro_dir,".tif$",recursive=T,full.names=T)
files<-files[!grepl("yield",files)]
# Remove yield (one reason for this is that stat<-"mean" returns NA and needs debugging)

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
                             keep_int=F,
                             round=1,
                             Geographies=Geographies,
                             overwrite=overwrite_spam)
  
  attr_file<-file.path(dirname(file),paste0(file_base,"_adm_",stat,".parquet.json"))
  
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

# 2) Livestock (GLW) #####
overwrite_glw<-T
version_glw<-1
source_year_glw<-list(census=2020,values=2015)

  ## 2.1) Livestock Mask #####
  mask_ls_file<-paste0(glw_int_dir,"/livestock_masks.tif")
  
  if(!file.exists(mask_ls_file)|overwrite_glw==T){
    glw_files<-list.files(glw_dir,"_Da.tif$",full.names=T)
    glw<-terra::rast(glw_files)
    names(glw)<-names(glw_names)
    
    glw<-glw[[c("poultry","sheep","pigs","goats","cattle")]]
    
    lus<-c(glw$cattle*0.7,glw$poultry*0.01,glw$goats*0.1,glw$pigs*0.2,glw$sheep*0.1)
    lus<-c(lus,sum(lus,na.rm=T))
    names(lus)<-c("cattle","poultry","goats","pigs","sheep","total")
    lus<-terra::mask(terra::crop(lus,Geographies$admin0),Geographies$admin0)
    
    # resample to 0.05
    lu_density<-lus/terra::cellSize(lus,unit="ha")
    lu_density<-terra::resample(lu_density,base_rast)
    
    # Classify into binary mask
    livestock_mask <- terra::ifel(lu_density > 0, 1, 0)
    
    # Split mask by highland vs tropical areas
    
    # Load highland mask
    highlands<-terra::rast(afr_highlands_file)
    highlands<-terra::resample(highlands,base_rast,method="near")
    
    livestock_mask_high<-livestock_mask*highlands
    names(livestock_mask_high)<-paste0( names(livestock_mask_high),"_highland")
    
    lowlands<-classify(highlands,data.frame(from=c(0,1),to=c(1,0)))
    livestock_mask_low<-livestock_mask*lowlands
    names(livestock_mask_low)<-paste0( names(livestock_mask_low),"_tropical")
    
    livestock_mask<-c(livestock_mask_high,livestock_mask_low)
    
    terra::writeRaster(livestock_mask,filename=mask_ls_file,overwrite=T)
    
  }else{
    livestock_mask<-terra::rast(mask_ls_file)
    livestock_mask_high<-livestock_mask[[grep("highland",names(livestock_mask))]]
    livestock_mask_low<-livestock_mask[[!grepl("highland",names(livestock_mask))]]
  }
  ## 2.2) Livestock Numbers ######
  livestock_no_file<-paste0(glw_pro_dir,"/livestock_number_number.tif")
  shoat_prop_file<-paste0(glw_int_dir,"/shoat_prop.tif")
  
  if(!file.exists(livestock_no_file)|overwrite_glw==T){
    
    ls_files<-list.files(glw_dir,"_Da.tif",full.names = T)
    
    glw<-terra::rast(glw_files)
    names(glw)<-names(glw_names)
    
    glw<-glw[[c("poultry","sheep","pigs","goats","cattle")]]
    
    TLU<-sum(c(glw$cattle*0.7,glw$poultry*0.01,glw$goats*0.1,glw$pigs*0.2,glw$sheep*0.1))
    
    livestock_no<-c(glw$cattle,glw$poultry,glw$goats,glw$pigs,glw$sheep,TLU)
    names(livestock_no)[nlyr(livestock_no)]<-"total"
    livestock_no<-terra::mask(terra::crop(livestock_no,Geographies$admin0),Geographies$admin0)
    
    # resample to 0.05
    livestock_density<-livestock_no/terra::cellSize(livestock_no,unit="ha")
    livestock_density<-terra::resample(livestock_density,base_rast)
    livestock_no<-livestock_density*cellSize(livestock_density,unit="ha")
    
    # Pull out sheep and goat proportions for use in vop calculations before highland/tropical splitting
    sheep_prop<-livestock_no$sheep/(livestock_no$goats +livestock_no$sheep)
    goat_prop<-livestock_no$goats/(livestock_no$goats +livestock_no$sheep)
    
    terra::writeRaster(terra::rast(c(sheep_prop=sheep_prop,goat_prop=goat_prop)),filename = shoat_prop_file,overwrite=T)
    
    # Split livestock between highland and tropical
    livestock_no<-split_livestock(data=livestock_no,livestock_mask_high,livestock_mask_low)
    
    terra::writeRaster(livestock_no,filename = livestock_no_file,overwrite=T)
    
  }else{
    livestock_no<-terra::rast(livestock_no_file)
  }
  
  ## 2.3) Extraction by vector boundaries ####
  
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
                               keep_int=F,
                               round=1,
                               Geographies=Geographies,
                               overwrite=overwrite_glw)
    
    attr_file<-file.path(dirname(file),paste0(file_base,"_adm_",stat,".parquet.json"))
    
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
        version = version_glw,
        parent_script = "R/0.6_process_exposure.R",
        variable = var,
        unit = unit,
        technology = tech,
        stat=stat,
        notes = paste0("A table of glw livestock values (",var,") extracted by boundary vectors then summarized (fun = ",FUN,"). Note this analysis uses density adjusted (da) GLW values.")
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
  
  # Drop unneeded cols
  exposure_adm_sum_tab<-exposure_adm_sum_tab[,!c("un_subregion","un_a0_en","un_a0_fr","un_a0_es","currency_code",
                           "currency_name","agg_n","stat")]
  
  # Order to optimize parquet performance
  exposure_adm_sum_tab<-exposure_adm_sum_tab[order(iso3,admin0_name,admin1_name,admin2_name,crop)]

    attr_info <- list(
      source = list(input_raster1=atlas_data$mapspam_2020v1r2$name,
                    input_raster2="GLW4",
                    extraction_vect=atlas_data$boundaries$name),
      source_year = list(input_raster1=source_year_spam,input_raster2=source_year_glw),
      date_created = date_created,
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
    
    arrow::write_parquet(exposure_adm_sum_tab,file)
}

# 4) Population ######
overwrite_pop<-F
  ## 4.1) Harmonize to atlas base raster ####
file<-paste0(hpop_int_dir,"/hpop_atlas.tif")
if(!file.exists(file)|overwrite_pop){
  local_files<-list.files(hpop_dir,".tif",full.names = T)
  hpop<-terra::rast(local_files)
  hpop<-terra::crop(hpop,Geographies)
  
  # Convert hpop to density
  hpop<-hpop/cellSize(hpop,unit="ha")
  
  # Resample to base raster
  hpop<-terra::resample(hpop,base_rast)
  
  # Convert back to number per cell
  hpop<-hpop*cellSize(hpop,unit="ha")
  
  terra::writeRaster(hpop,filename =file,overwrite=T)
}

## 4.2) Extraction by vector boundaries ####
version_hpop<-1
file<-paste0(exposure_dir,"/hpop_adm_sum.parquet")
  
if(!file.exists(file)|overwrite==T){  
files<-list.files(hpop_dir,".tif$",recursive=F,full.names=T)
hpop_extracted<-rbindlist(lapply(1:length(files),FUN=function(i){
  cat("Extracting file",i,"/",length(files),basename(files[i]),"\n")
  file<-files[i]
  file_base<-gsub(".tif","",basename(file))
  var<-gsub(".tif","",unlist(tstrsplit(basename(file),"_",keep=2)))
  unit<-"number"
  type<-gsub(".tif","",unlist(tstrsplit(basename(file),"_",keep=1)))
  
  stat<-"sum"
  
  data<-terra::rast(file)
  
  result<-admin_extract_wrap(data=data,
                             save_dir=dirname(file),
                             filename =file_base,
                             FUN=stat,
                             append_vals=c(exposure=var,unit=unit,type=type),
                             var_name="stat",
                             keep_int=F,
                             round=1,
                             modify_colnames=F,
                             Geographies=Geographies,
                             overwrite=overwrite)
  
  
  attr_file<-file.path(dirname(file),paste0(file_base,"_adm_",stat,".parquet.json"))
  
  if(file.exists(attr_file)){
    attr_dat<-jsonlite::read_json(attr_file)
    date_created<-unlist(attr_dat$date_created)
    version_attr<-unlist(attr_dat$version)
    if(overwrite|version_attr!=version_hpop){
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
      source_year = list(input_raster=source_year),
      date_created = date_created,
      version = version_hpop,
      parent_script = "R/0.6_process_exposure.R",
      variable = var,
      unit = unit,
      type = type,
      stat=stat,
      notes = paste0("A table of glw livestock values (",var,") extracted by boundary vectors then summarized (fun = ",FUN,"). Note this analysis uses density adjusted (da) GLW values.")
    )
    
    write_json(attr_info, attr_file, pretty = TRUE)
  }
  
  return(result)
  
}))

# Make values integer to save space
hpop_extracted[,value:=as.integer(value)]

# Drop unneeded cols
hpop_extracted<-hpop_extracted[,!c("un_subregion","un_a0_en","un_a0_fr","un_a0_es","currency_code",
                                               "currency_name","agg_n","stat")]

# Order to optimize parquet performance
hpop_extracted<-hpop_extracted[order(iso3,admin0_name,admin1_name,admin2_name)]

arrow::write_parquet(hpop_extracted,file)

attr_file<-paste0(file,".json")

attr_info <- list(
  source = list(input_raster="Worldpop",
                extraction_vect=atlas_data$boundaries$name),
  source_year = list(input_raster="2020"),
  date_created = date_created,
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