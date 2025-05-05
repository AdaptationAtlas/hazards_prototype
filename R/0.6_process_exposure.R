# Please run 0_server_setup.R before executing this script
# For generate livestock vop you will need to run script 0.4 first
# a) Load R functions & packages ####
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

# List of packages to be loaded
packages <- c("terra", 
              "data.table",
              "exactextractr",
              "arrow",
              "geoarrow")


pacman::p_load(packages,character.only=T)

# b) Load functions & wrappers ####
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))


# 0) Load and prepare admin vectors and exposure rasters, extract exposure by admin ####
  # 0.0) Base rast ####
  base_rast <- terra::rast(base_rast_path)

# 0.1) Geographies #####
Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
  file<-geo_files_local[i]
  data<-arrow::open_dataset(file)
  data <- data |> sf::st_as_sf() |> terra::vect()
  data
})
names(Geographies)<-names(geo_files_local)
# 0.2) Exposure variables ####
overwrite<-T
# 0.2.1) Crops (MapSPAM) #####
# read in mapspam metadata
ms_codes<-data.table::fread(ms_codes_url, showProgress = FALSE)[,Code:=toupper(Code)]
ms_codes<-ms_codes[compound=="no"]
# 0.2.1.1) Crop VoP (Value of production) ######
  # 0.2.1.1.1) I$ #########
  # To generalize it might be better to just supply a filename for the mapspam
  # Dev Note: Why is this combination separate from the loop that follows?
  var<-"V"
  tech<-"TA"
  crop_vop_intd<-read_spam(variable="V",
                          technology="TA",
                          mapspam_dir=mapspam_dir,
                          save_dir=exposure_dir,
                          base_rast=base_rast,
                          filename="crop_vop15_intd15",
                          ms_codes=ms_codes,
                          overwrite=overwrite)

attr_info <- list(
  source = atlas_data$mapspam_2020v1r2$name,
  date_created = Sys.time(),
  notes = paste("A raster stack of crop value of production in 2015 international dollars created using the R/haz_functions/read_spam function and tabular vop data created by the R/0.45_create_crop_vop.R script.",
                "variable = ",var, " & technology =",tech)
)

write_json(attr_info, file.path(exposure_dir,"crop_vop15_intd15.tif.json"), pretty = TRUE)

 # Create other spam layers
 spam_combos<-data.table(expand.grid(variable=c("V-crop_vop15_intd15","H-crop_ha"),tech=c("TA-a","TI-i","TR-r")))
 spam_combos[,code1:=unlist(tstrsplit(variable,"-",keep=2))][,code2:=unlist(tstrsplit(tech,"-",keep=2))]
 spam_combos[,variable:=unlist(tstrsplit(variable,"-",keep=1))][,tech:=unlist(tstrsplit(tech,"-",keep=1))]
 spam_combos[,file_name:=paste0(code1,"_",code2)]
 spam_combos<-spam_combos[!(variable=="V" & tech=="TA")]
 
 for(i in 1:nrow(spam_combos)){
   cat("\r",i,"/",nrow(spam_combos))
   
   var<-spam_combos$variable[i]
   tech<-spam_combos$tech[i]
   spam_file<-spam_combos$file_name[i]
   
  read_spam(variable=var,
             technology=tech,
             mapspam_dir=mapspam_dir,
             save_dir=exposure_dir,
             base_rast=base_rast,
             filename=spam_file,
             ms_codes=ms_codes,
             overwrite=overwrite)
  
  if(var=="H"){
    var_desc<-"harvested area in ha per pixel"
  }
  
  if(var=="V"){
    var_desc<-"value of production in 2015 international dollars (calculated using the R/0.45_create_crop_vop.R script)"
  }
  
  attr_info <- list(
    source = atlas_data$mapspam_2020v1r2$name,
    date_created = Sys.time(),
    notes = paste0("A raster stack of ",var_desc," created using the R/haz_functions/read_spam function and tabular mapspam data.",
                  "variable = ",var, " & technology =",tech)
  )

  write_json(attr_info, file.path(exposure_dir,paste0(spam_file,".tif.json")), pretty = TRUE)
  
   }
  
  # Extract  values by admin areas
  crop_vop_intd_adm<-admin_extract_wrap(data=crop_vop_intd,
                                         save_dir=exposure_dir,
                                         filename = "crop_vop15_intd15",
                                         FUN="sum",
                                         varname="vop",
                                         Geographies=Geographies,
                                         overwrite=overwrite)
  # 0.2.1.1.2) US$ #########
    file <- file.path(mapspam_dir, "spam2020V1r2_SSA_Vusd15_TA.tif")
    if(length(file)!=1){
      warning("MapSPAM usd15 files do not exist - please redownload the mapspam folder and/or create these 0.45_create_crop_vop.R")
      crop_vop_usd<-NULL
      crop_vop_usd_adm<-NULL
      
    }else{
      crop_vop_usd<-terra::rast(file)
      writeRaster(crop_vop_usd, "crop_vop15_cusd15.tif")
      crop_vop_usd_adm<-admin_extract_wrap(data=crop_vop_usd,
                                                 save_dir=exposure_dir,
                                                 filename = "crop_vop15_cusd15",
                                                 FUN="sum",
                                                 varname="vop_cusd",
                                                 Geographies=Geographies,
                                                 overwrite=overwrite)
  }
  
# 0.2.1.2) Crop Harvested Area #####

crop_ha<-read_spam(variable="H",
                       technology="TA",
                       mapspam_dir=mapspam_dir,
                       save_dir=exposure_dir,
                       base_rast=base_rast,
                       filename="crop_ha",
                       ms_codes=ms_codes,
                       overwrite=overwrite)

# 2.1.2.1) Extraction of values by admin areas
crop_ha_adm<-admin_extract_wrap(data=crop_ha,
                                save_dir=exposure_dir,
                                filename = "crop_ha",
                                FUN="sum",
                                varname="ha",
                                Geographies=Geographies,
                                overwrite=overwrite)

# 0.2.1.3) Create Crop Masks ######
# Need to use mapspam physical area
mask_file<-paste0(commodity_mask_dir,"/crop_masks.tif")

if(!file.exists(mask_file)|overwrite==T){
  file<-list.files(mapspam_dir,"SSA_H_TA.csv",full.names = T)
  file<-file[!grepl("_gr_",file)]
  pa<-fread(file)
  crops<-tolower(ms_codes$Code)
  ms_fields<-c("x","y",grep(paste0(crops,collapse = "|"),colnames(pa),value=T))
  pa<-rast(pa[,..ms_fields],type="xyz",crs="EPSG:4326")
  names(pa)<-gsub("_a","",names(pa))
  names(pa)<-ms_codes[match(names(pa),tolower(ms_codes$Code)),Fullname]
  # convert to value/area 
  crop_pa<-pa/terra::cellSize(pa,unit="ha")
  # resample  data
  crop_pa<-terra::resample(crop_pa,base_rast)
  crop_pa_tot<-crop_pa*cellSize(crop_pa,unit="ha")
  
  # Areas with >0.01% harvested area = crop mask
  crop_pa_prop<-crop_pa_tot/cellSize(crop_pa_tot,unit="ha")
  crop_mask<-terra::classify(crop_pa_prop,  data.frame(from=c(0,0.001),to=c(0.001,2),becomes=c(0,1)))
  terra::writeRaster(crop_mask,filename=mask_file,overwrite=T)
}else{
  crop_mask<-terra::rast(mask_file)
}

# 0.2.2) Livestock #####
  # 0.2.2.1) Livestock Mask #####
  mask_ls_file<-paste0(commodity_mask_dir,"/livestock_masks.tif")
  
  if(!file.exists(mask_ls_file)|overwrite==T){
    
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
  # 0.2.2.2) Livestock Numbers (GLW) ######
  livestock_no_file<-paste0(exposure_dir,"/livestock_no.tif")
  shoat_prop_file<-paste0(glw_dir,"/shoat_prop.tif")
  
  if(!file.exists(livestock_no_file)|overwrite==T){
    
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
  
  # 2.2.1.1) Extraction of values by admin areas
  livestock_no_tot_adm<-admin_extract_wrap(data=livestock_no,
                                           save_dir=exposure_dir,
                                           filename = "livestock_no",
                                           FUN="sum",
                                           varname="number",
                                           Geographies=Geographies,
                                           overwrite=overwrite)
  
  # 0.2.2.3) Livestock VoP ######
    # You must have run 0.4_fao_producer_prices_livestock.R for the section to work
    # 0.2.2.3.1) I$ ######## 
    livestock_vop_intd_file<-file.path(exposure_dir,"livestock-vop15-intd15-processed.tif")
    
    if(!file.exists(livestock_vop_intd_file)|overwrite==T){
      
      livestock_vop_intd<-terra::rast(file.path(ls_vop_dir,"livestock-vop-2015-intd15.tif"))
    
      # Split vop by highland vs lowland
      livestock_vop_intd<-split_livestock(data=livestock_vop_intd,livestock_mask_high,livestock_mask_low)
      terra::writeRaster(livestock_vop_intd,filename = livestock_vop_intd_file,overwrite=T)
    }else{
      livestock_vop_intd<-terra::rast(livestock_vop_intd_file)
    }
    
    livestock_vop_intd_adm<-admin_extract_wrap(data=livestock_vop_intd,
                                          save_dir=exposure_dir,
                                          filename = "livestock_vop15_intd15",
                                          FUN="sum",
                                          varname="vop",
                                          Geographies=Geographies,
                                          overwrite=overwrite)
    
    # 0.2.2.3.2) US$ ########
    livestock_vop_usd_file<-paste0(exposure_dir,"/livestock-vop15-cusd15-processed.tif")
    
    if(!file.exists(livestock_vop_usd_file)){
      data<-terra::rast(paste0(ls_vop_dir,"/livestock-vop-2015-cusd15.tif"))
      
      livestock_vop_usd<-split_livestock(data=data,livestock_mask_high,livestock_mask_low)
      terra::writeRaster(livestock_vop_usd,filename = livestock_vop_usd_file,overwrite=T)
    }else{
      livestock_vop_usd<-terra::rast(livestock_vop_usd_file)
    }
    
    livestock_vop_usd_adm<-admin_extract_wrap(data=livestock_vop_usd,
                                              save_dir=exposure_dir,
                                              filename = "livestock_vop15_cusd15",
                                              FUN="sum",
                                              varname="vop_cusd",
                                              Geographies=Geographies,
                                              overwrite=overwrite)

# 0.2.3) Combine exposure totals by admin areas ####
file<-paste0(exposure_dir,"/exposure_adm_sum.parquet")
if(!file.exists(file)|overwrite==T){
  exposure_adm_sum_tab<-rbind(
    crop_vop_intd_adm,
    crop_vop_usd_adm,
    crop_ha_adm,
    livestock_vop_intd_adm,
    livestock_vop_usd_adm,
    livestock_no_tot_adm
  )
  arrow::write_parquet(exposure_adm_sum_tab,file)
}

# Create a table for total values 
file<-file.path(exposure_dir,"admin0_totals")

if(!file.exists(file)|overwrite==T){
  # Load raw mapspam
  file<-list.files(mapspam_dir,paste0("V", "_", "TA", ".csv"),full.names=T)
  file<-file[!grepl("_gr_",file)]
  data <- fread(file)
  
  crop_columns<-grep("_a$",colnames(data),value=T)
  
  # Calculating the sum for each crop within each iso3 group.
  vop_adm0_total <- data[, .(total_crops = sum(unlist(.SD))), by = .(iso3), .SDcols = crop_columns]
  
  vop_adm0_total<-merge(vop_adm0_total,Geographies$admin0[,c("iso3","admin0_name")])
  
  # Add livestock
  vop_adm0_total<-merge(vop_adm0_total,livestock_vop_intd_adm[is.na(admin1_name),list(total_livestock=sum(value)),by=admin0_name],by="admin0_name")
  
  # Grand total
  vop_adm0_total<-vop_adm0_total[,total:=total_crops+total_livestock][,exposure:="vop"]
  
  fwrite(vop_adm0_total,file.path(exposure_dir,"admin0_totals.csv"))
}

# 0.2.4) Population ######

file<-paste0(exposure_dir,"/hpop.tif")
if(!file.exists(file)){
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
}else{
  hpop<-terra::rast(file)
}

# 0.2.4.1) Extraction of hpop by admin areas ####
hpop_admin<-admin_extract_wrap(data=hpop,
                               save_dir=exposure_dir,
                               filename = "hpop",
                               FUN="sum",
                               varname="number",
                               Geographies=Geographies,
                               overwrite=overwrite)
