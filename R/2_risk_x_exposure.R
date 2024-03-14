# a) Install and load packages ####
load_and_install_packages <- function(packages) {
  for (package in packages) {
    if (!require(package, character.only = TRUE)) {
      install.packages(package)
      library(package, character.only = TRUE)
    }
  }
}

# List of packages to be loaded
packages <- c("terra", 
              "data.table", 
              "exactextractr",
              "s3fs",
              "sf", 
              "sfarrow", 
              "arrow",
              "doFuture",
              "stringr", 
              "stringi",
              "httr")

# Call the function to install and load packages
load_and_install_packages(packages)

# b) Load functions & wrappers ####
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

# c) Set up workspace ####

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

haz_meta<-data.table::fread("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_metadata.csv")
haz_class_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_classes.csv"
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
ms_codes<-data.table::fread("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SpamCodes.csv")[,Code:=toupper(Code)]
ms_codes<-ms_codes[compound=="no"]
crop_choices<-unique(c(ms_codes[,sort(Fullname)],haz_class[,unique(crop)]))

# Load base raster to resample to 
base_raster<-"base_raster.tif"
if(!file.exists(base_raster)){
url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/base_raster.tif"
httr::GET(url, write_disk(base_raster, overwrite = TRUE))
}

base_rast<-terra::rast(base_raster)

#### Load datasets (non hazards)
# d) Create directories ####
haz_risk_dir<-paste0("Data/hazard_risk/",timeframe_choice)
haz_mean_dir<-paste0("Data/hazard_timeseries_mean/",timeframe_choice)
haz_timeseries_dir<-paste0("Data/hazard_timeseries/",timeframe_choice)

haz_risk_vop17_dir<-paste0("Data/hazard_risk_vop17/",timeframe_choice)
if(!dir.exists(haz_risk_vop17_dir)){
  dir.create(haz_risk_vop17_dir,recursive = T)
}

haz_risk_vop_dir<-paste0("Data/hazard_risk_vop/",timeframe_choice)
if(!dir.exists(haz_risk_vop_dir)){
  dir.create(haz_risk_vop_dir,recursive = T)
}

haz_risk_ha_dir<-paste0("Data/hazard_risk_ha/",timeframe_choice)
if(!dir.exists(haz_risk_ha_dir)){
  dir.create(haz_risk_ha_dir,recursive = T)
}

haz_risk_n_dir<-paste0("Data/hazard_risk_n/",timeframe_choice)
if(!dir.exists(haz_risk_n_dir)){
  dir.create(haz_risk_n_dir,recursive = T)
}

exposure_dir<-"Data/exposure"
if(!dir.exists(exposure_dir)){
  dir.create(exposure_dir)
}

mapspam_local<-"Data/mapspam"
hpop_dir<-"Data/atlas_pop"

# 0) Load an prepare admin vectors and exposure rasters, extract exposure by admin ####
  # 0.1) Geographies #####
  # Load and combine geoboundaries
  overwrite<-F

  geoboundaries_s3<-"s3://digital-atlas/boundaries"
  geo_files_s3<-s3fs::s3_dir_ls(geoboundaries_s3)
  geo_files_s3<-grep("harmonized.gpkg",geo_files_s3,value=T)
  
  geo_files_local<-file.path("Data/boundaries",basename(geo_files_s3))
  names(geo_files_local)<-c("admin0","admin1","admin2")
  
  Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
    file<-geo_files_local[i]
    if(!file.exists(file)|overwrite==T){
      s3fs::s3_file_download(path=geo_files_s3[i],new_path=file,overwrite = T)
    }
    data<-terra::vect(file)
    names(data)<-gsub("_nam$","_name$",names(data))
    data
    
  })
  names(Geographies)<-names(geo_files_local)
  
  # 0.2) Exposure variables ####
    overwrite<-F
    # 0.2.1) Crops (MapSPAM) #####
      # 0.2.1.1) Download MapSPAM #####
      # If mapspam data does not exist locally download from S3 bucket
      
      if(!dir.exists(mapspam_local)|overwrite==T){
        dir.create(mapspam_local,recursive = T)
        s3_bucket <- "s3://digital-atlas/MapSpam/raw/spam2017V2r3"
        s3fs::s3_dir_download(s3_bucket,mapspam_local,overwrite=T)
      }
  
      # 0.2.1.2) Crop VoP (Value of production) ######
        # To generalize it might be better to just supply a filename for the mapspam
        crop_vop_tot<-read_spam(variable="V",
                                technology="TA",
                                mapspam_dir=mapspam_local,
                                save_dir=exposure_dir,
                                base_rast=base_rast,
                                filename="crop_vop",
                                ms_codes=ms_codes,
                                overwrite=overwrite)
  
    if(!file.exists("Data/mapspam/SSA_Vusd17_TA.csv")){
      print("MapSPAM files ned udpated please redownload the mapspam folder")
    }
        crop_vop17_tot<-read_spam(variable="Vusd17",
                                  technology="TA",
                                  mapspam_dir=mapspam_local,
                                  save_dir=exposure_dir,
                                  base_rast=base_rast,
                                  filename="crop_vop_usd17",
                                  ms_codes=ms_codes,
                                  overwrite=F)
  
    
    # 2.1.2.2) Extraction of values by admin areas
      crop_vop_tot_adm_sum<-admin_extract_wrap(data=crop_vop_tot,
                                               save_dir=exposure_dir,
                                               filename = "crop_vop",
                                               FUN="sum",
                                               varname="vop",
                                               Geographies=Geographies,
                                               overwrite=overwrite)
      
      crop_vop17_tot_adm_sum<-admin_extract_wrap(data=crop_vop17_tot,
                                                 save_dir=exposure_dir,
                                                 filename = "crop_vop_usd17",
                                                 FUN="sum",
                                                 varname="vop_usd17",
                                                 Geographies=Geographies,
                                                 overwrite=overwrite)
        
      # 0.2.1.3) Crop Harvested Area #####
  
      crop_ha_tot<-read_spam(variable="H",
                              technology="TA",
                              mapspam_dir=mapspam_local,
                              save_dir=exposure_dir,
                              base_rast=base_rast,
                              filename="crop_ha",
                              ms_codes=ms_codes,
                              overwrite=overwrite)
  
    # 2.1.2.1) Extraction of values by admin areas
    crop_ha_tot_adm_sum<-admin_extract_wrap(data=crop_ha_tot,
                                            save_dir=exposure_dir,
                                            filename = "crop_ha",
                                            FUN="sum",
                                            varname="ha",
                                            Geographies=Geographies,
                                            overwrite=overwrite)
  
      # 0.2.1.4) Create Crop Masks ######
    commodity_mask_dir<-"Data/commodity_masks"
    
    if(!dir.exists(commodity_mask_dir)){
      dir.create(commodity_mask_dir)
    }
    
    # Need to use mapspam physical area
    mask_file<-paste0(commodity_mask_dir,"/crop_masks.tif")
    
    if(!file.exists(mask_file)|overwrite==T){
      pa<-fread(paste0(mapspam_dir,"/SSA_A_TA.csv"))
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
      # 0.2.2.1) Download livestock data ####
      # If glw3 data does not exist locally download from S3 bucket
      glw3_dir<-"Data/GLW3"
      
      if(!dir.exists(glw3_dir)){
        dir.create(glw3_dir,recursive = T)
        s3_bucket <- "s3://digital-atlas/risk_prototype/data/GLW3"
        s3fs::s3_dir_download(s3_bucket,glw3_dir,overwrite = T)
      }
    
      # If livestock vop data does not exist locally download from S3 bucket
      ls_vop_dir<-"Data/livestock_vop"
      
      if(!dir.exists(ls_vop_dir)){
        dir.create(ls_vop_dir,recursive = T)
        s3_bucket <- "s3://digital-atlas/ls_vop_dir"
        s3fs::s3_dir_download(s3_bucket,ls_vop_dir,overwrite = T)
      }
      
      # If livestock highland vs tropical map does not exist locally download from S3 bucket
      afr_highlands_dir<-"Data/afr_highlands"
      
      if(!dir.exists(afr_highlands_dir)){
        dir.create(afr_highlands_dir,recursive = T)
        s3_bucket <- "s3://digital-atlas/afr_highlands"
        s3fs::s3_dir_download(s3_bucket,afr_highlands_dir,overwrite = T)
      }
  
      # 0.2.2.3) Livestock Mask #####
    mask_ls_file<-paste0(commodity_mask_dir,"/livestock_masks.tif")
    
    if(!file.exists(mask_ls_file)|overwrite==T){
      
      ls_files<-list.files(glw3_dir,"_Da.tif",full.names = T)
      
      Cattle<-terra::rast(grep("_Ct_2010_Da.tif",ls_files,value=T))
      Chicken<-terra::rast(grep("_Ch_2010_Da.tif",ls_files,value=T))
      Goat<-terra::rast(grep("_Gt_2010_Da.tif",ls_files,value=T))
      Pig<-terra::rast(grep("_Pg_2010_Da.tif",ls_files,value=T))
      Sheep<-terra::rast(grep("_Sh_2010_Da.tif",ls_files,value=T))
      
      TLU<-Cattle*0.7 + Sheep*0.1 + Goat*0.1 + 0.01*Chicken + 0.2*Pig
      
      lus<-c(Cattle*0.7,Chicken*0.01,Goat*0.1,Pig*0.2,Sheep*0.1,TLU)
      names(lus)<-c("cattle","poultry","goats","pigs","sheep","total")
      lus<-terra::mask(terra::crop(lus,Geographies$admin0),Geographies$admin0)
      
      # resample to 0.05
      lu_density<-lus/terra::cellSize(lus,unit="ha")
      lu_density<-terra::resample(lu_density,base_rast)
      
      # Classify requires 0.00001 livestock units per ha to be present
      livestock_mask<-terra::classify(lu_density, data.frame(from=c(0,0.00001),to=c(0.00001,Inf),becomes=c(0,1)))
      
      # Split mask by highland vs tropical areas
      
      # Load highland mask
      highlands<-terra::rast(paste0(afr_highlands_dir,"/afr-highlands.asc"))
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
      # 0.2.2.1) Livestock Numbers (GLW3) ######
    livestock_no_file<-paste0(exposure_dir,"/livestock_no.tif")
    shoat_prop_file<-paste0(glw3_dir,"/shoat_prop.tif")
      
    if(!file.exists(livestock_no_file)|overwrite==T){
      
      ls_files<-list.files(glw3_dir,"_Da.tif",full.names = T)
      
      Cattle<-terra::rast(grep("_Ct_2010_Da.tif",ls_files,value=T))
      Chicken<-terra::rast(grep("_Ch_2010_Da.tif",ls_files,value=T))
      Goat<-terra::rast(grep("_Gt_2010_Da.tif",ls_files,value=T))
      Pig<-terra::rast(grep("_Pg_2010_Da.tif",ls_files,value=T))
      Sheep<-terra::rast(grep("_Sh_2010_Da.tif",ls_files,value=T))
      
      TLU<-Cattle*0.7 + Sheep*0.1 + Goat*0.1 + 0.01*Chicken + 0.2*Pig
      
      livestock_no<-c(Cattle,Chicken,Goat,Pig,Sheep,TLU)
      names(livestock_no)<-c("cattle","poultry","goats","pigs","sheep","total_livestock_units")
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
  
      # 0.2.2.2) Livestock VoP ######
      # IUSD (old)
      livestock_vop_file<-paste0(exposure_dir,"/livestock_vop.tif")
      
      if(!file.exists(livestock_vop_file)|overwrite==T){
        
      # Note unit is IUSD 2005
        ls_vop_files<-list.files(ls_vop_dir,"_total.tif$",full.names = T)
        ls_vop_files<-grep("h7",ls_vop_files,value=T)
        livestock_vop<-terra::rast(ls_vop_files)
      
        names(livestock_vop)<-c("cattle","poultry","pigs","sheep_goat","total")
        
        # resample to 0.05
        livestock_density<-livestock_vop/terra::cellSize(livestock_vop,unit="ha")
        livestock_density<-terra::resample(livestock_density,base_rast)
        livestock_vop<-livestock_density*cellSize(livestock_density,unit="ha")
        rm(livestock_density)
        
        # Load prop files
        sheep_prop<-terra::rast(shoat_prop_file)$sheep_prop
        goat_prop<-terra::rast(shoat_prop_file)$goat_prop
        
        # Split sheep goat vop using their populations
        livestock_vop$sheep<-livestock_vop$sheep_goat*sheep_prop
        livestock_vop$goats<-livestock_vop$sheep_goat*goat_prop
        livestock_vop$sheep_goat<-NULL
      
      # Split vop by highland vs lowland
      
      livestock_vop<-split_livestock(data=livestock_vop,livestock_mask_high,livestock_mask_low)
      
      # Reorder cols to match mask
      order_n<-sapply(names(livestock_vop),FUN=function(X){grep(X,names(livestock_mask_high))})
      livestock_vop_high<-livestock_vop[[order_n]]
      livestock_vop_high<-livestock_vop_high*livestock_mask_high
      
      order_n<-sapply(names(livestock_vop),FUN=function(X){grep(X,names(livestock_mask_low))})
      livestock_vop_low<-livestock_vop[[order_n]]
      livestock_vop_low<-livestock_vop_low*livestock_mask_low
      
      
      names(livestock_vop_high)<-names(livestock_mask_high)
      names(livestock_vop_low)<-names(livestock_mask_low)
      
      livestock_vop<-c(livestock_vop_low,livestock_vop_high)
      
      terra::writeRaster(livestock_vop,filename = livestock_vop_file,overwrite=T)
      }else{
        livestock_vop<-terra::rast(livestock_vop_file)
      }
    
      # USD 2017 (see 0_fao_producer_prices_livestock.R)
      livestock_vop17_file<-paste0(exposure_dir,"/livestock_vop_usd17.tif")
    
      if(!file.exists(livestock_vop17_file)){
        data<-terra::rast(paste0(ls_vop_dir,"/vop_adj17.tif"))
        
        # Combine products for different species
        data<-terra::rast(c(cattle=data$cattle_meat+data$cattle_milk,
                goats=data$goat_meat+data$goat_milk,
                pigs=data$pig_meat,
                poultry=data$poultry_eggs+data$poultry_meat,
                sheep=data$sheep_meat+data$sheep_milk,
                total=sum(data,na.rm=T)))
        
        livestock_vop17<-split_livestock(data=data,livestock_mask_high,livestock_mask_low)
        terra::writeRaster(livestock_vop17,filename = livestock_vop17_file)
      }else{
        livestock_vop17<-terra::rast(livestock_vop17_file)
      }
    
      # 2.2.2.1) Extraction of values by admin areas
    livestock_vop_tot_adm<-admin_extract_wrap(data=livestock_vop,
                                              save_dir=exposure_dir,
                                              filename = "livestock_vop",
                                              FUN="sum",
                                              varname="vop",
                                              Geographies=Geographies,
                                              overwrite=overwrite)
    
    livestock_vop17_tot_adm<-admin_extract_wrap(data=livestock_vop17,
                                              save_dir=exposure_dir,
                                              filename = "livestock_vop_usd17",
                                              FUN="sum",
                                              varname="vop_usd17",
                                              Geographies=Geographies,
                                              overwrite=overwrite)
    
    # 0.2.3) Combine exposure totals by admin areas ####
      file<-paste0(exposure_dir,"/exposure_adm_sum.parquet")
      if(!file.exists(file)|overwrite==T){
        exposure_adm_sum_tab<-rbind(
          crop_vop_tot_adm_sum,
          crop_vop17_tot_adm_su,
          crop_ha_tot_adm_sum,
          livestock_vop_tot_adm,
          livestock_no_tot_adm
        )
            arrow::write_parquet(exposure_adm_sum_tab,file)
        }
  
    # 0.2.4) Population ######
    bucket_files<-paste0("s3://digital-atlas/population/",c("total_pop.tif","rural_pop.tif","urban_pop.tif"))
    local_files<-file.path(hpop_dir,basename(bucket_files))
    
    if(!dir.exists(hpop_dir)|overwrite==T){
      dir.create(hpop_dir)
      for (i in seq_along(bucket_files)) {
        s3fs::s3_file_download(bucket_files[i], local_files[i],overwrite=T)
      }
    }
    
      file<-paste0(exposure_dir,"/hpop.tif")
      if(!file.exists(file)){
            
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
    admin_extract_wrap(data=hpop,
                       save_dir=exposure_dir,
                       filename = "hpop",
                       FUN="sum",
                       varname="number",
                       Geographies=Geographies,
                       overwrite=overwrite)
      
# 1) Extract hazard risk by admin ####
  # 1.1) Solo and interactions combined into a single file (not any hazard) #####
overwrite<-F
files<-list.files(haz_risk_dir,".tif$",full.names = T)
files_solo<-files[!grepl("-int[.]tif$",files)]
files_int<-grep("-int[.]tif$",files,value = T)

admin_extract_wrap2(files=files_solo,
                    save_dir = haz_risk_dir,
                    filename="haz_risk_solo",
                    severity=unlist(severity_classes[,1]),
                    Geographies=Geographies,
                    overwrite=overwrite)

admin_extract_wrap2(files=files,
                    save_dir = haz_risk_dir,
                    filename="haz_risk_int",
                    severity=unlist(severity_classes[,1]),
                    Geographies=Geographies,
                    overwrite=overwrite)
  # 1.2) Restructure extracted data ####

  restructure_parquet(filename = "haz_risk_int",
                    save_dir = haz_risk_dir,
                    severity = unlist(severity_classes[,1]),
                    overwrite=overwrite,
                    crops = c("generic",crop_choices),
                    livestock=livestock_choices,
                    Scenarios=Scenarios,
                    hazards=haz_meta[,unique(type)])

restructure_parquet(filename = "haz_risk_solo",
                    save_dir = haz_risk_dir,
                    severity = unlist(severity_classes[,1]),
                    overwrite=overwrite,
                    crops = c("generic",crop_choices),
                    livestock=livestock_choices,
                    Scenarios=Scenarios,
                    hazards=haz_meta[,unique(type)])

if(F){
# Check resulting file
X<-arrow::read_parquet(paste0(haz_risk_dir,"/haz_risk_any_adm_moderate.parquet"))
grep("THI",names(X),value=T)
}

  # 1.3) Optional: Apply Crop Mask to Classified Hazard Risk ####

dirs<-list.dirs("Data/hazard_risk_class",recursive = F)

# join crop and livestock masks
commodity_masks<-c(crop_mask,livestock_mask)

# Remove total livestock units
commodity_masks<-commodity_masks[[!grepl("total_",names(commodity_masks))]]

for(k in 1:length(dirs)){
  
  haz_risk_class_dir<-paste0(dirs[k],"/",timeframe_choice)

  haz_risk_mask_dir<-gsub("hazard_risk_class/","hazard_risk_class_mask/",haz_risk_class_dir)
  if(!dir.exists(haz_risk_mask_dir)){
    dir.create(haz_risk_mask_dir,recursive = T)
  }
  
  risk_class_rast_files<-list.files(haz_risk_class_dir,".tif",full.names = T)
  
  file_crops<-gsub("_severe|_extreme|_int|[.]tif","",unlist(tail(tstrsplit(risk_class_rast_files,"/"),1)))
  
  for(i in 1:nlyr(commodity_masks)){
    crop<-names(commodity_masks)[i]
    mask<-commodity_masks[[i]]
    risk_files<-risk_class_rast_files[file_crops==crop]
    for(j in 1:length(risk_files)){
      
      # Display progress
      cat('\r                                                                                                                                                 ')
      cat('\r',paste("Crop:",i,"/",nlyr(commodity_masks)," | file:",j,"/",length(risk_files)))
      flush.console()
      
      file<-risk_files[j]
      
      save_name<-gsub(haz_risk_class_dir,haz_risk_mask_dir,file)
      if(!file.exists(save_name)){
        risk<-terra::rast(file)
        risk_masked<-risk*mask
        terra::writeRaster(risk_masked,filename =save_name)
      }
    }
  }

}

# 2) Extract hazard means and sd by admin ####
  # 2.1) Extract mean hazards ####
  folder<-haz_mean_dir
  
  files<-list.files(folder,".tif",full.names = T)
  # Note to look at change you will need to calculate change in mean values in 1_calculate_risks.R script and then subset to files containing change in the name
  files<-files[!grepl("change",files)]
  data<-terra::rast(files)
  names(data)<-gsub(".tif$","",basename(files))
  
  # If looking at change make sure update this field
  file<-"haz_means"
  file0<-file.path(folder,paste0(file,"_adm0.parquet"))
  file1<-gsub("adm0","adm1",file0)
  file2<-gsub("adm0","adm2",file0)
  
  
  if(!file.exists(file0)|overwrite==T){
    data_ex<-admin_extract(data=data,Geographies,FUN="mean")
    st_write_parquet(obj=sf::st_as_sf(data_ex$admin0), dsn=file0)
    st_write_parquet(obj=sf::st_as_sf(data_ex$admin1), dsn=file1)
    st_write_parquet(obj=sf::st_as_sf(data_ex$admin2), dsn=file2)
  }

  # 2.2) Compile mean hazards into tabular form ####
  filename<-gsub("adm0","adm",file0)
  
  if(!file.exists(filename)){
    # Extract data from vector files and restructure into tabular form
    data_ex<-rbindlist(lapply(1:length(levels),FUN=function(i){
      level<-levels[i]
      print(level)
      
      data<-data.table(data.frame(sfarrow::st_read_parquet(paste0(folder,"/",file,"_",levels[i],".parquet"))))
      
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
      
      data[,variable:=gsub("ENSEMBLEmean_","",variable)
           ][,variable:=gsub("historical","historic-historic",variable)
             ][,variable:=stringi::stri_replace_all_regex(variable,pattern=paste0(unique(Scenarios$Scenario),"_"),replacement=paste0(unique(Scenarios$Scenario),"-"),vectorise_all = F)
              ][,variable:=stringi::stri_replace_all_regex(variable,pattern=paste0(unique(Scenarios$Time),"_"),replacement=paste0(unique(Scenarios$Time),"-"),vectorise_all = F)
                ][,variable:=stringi::stri_replace_all_regex(variable,pattern=c("max_max","min_min","mean_mean"),replacement=c("max-max","min-min","mean-mean"),vectorise_all = F)
                  ][,variable:=gsub(".","-",variable,fixed=T)]
      
      variable<-cbind(data$variable,data.table(do.call("cbind",tstrsplit(data$variable,"-"))[,-1]))
      colnames(variable)<-c("variable","scenario","timeframe","hazard","hazard_stat")
      variable[is.na(hazard_stat),hazard:=gsub("_","-",hazard)
               ][is.na(hazard_stat),hazard_stat:=unlist(tstrsplit(hazard,"-",keep=2))
                 ][,hazard:=unlist(tstrsplit(hazard,"-",keep=1))]
    
      data<-merge(data,unique(variable),all.x=T)[,variable:=NULL]
      
     
      
    }),fill=T)
    data_ex<-data_ex[,c(1,7,8,3,4,5,6,2)]
    
    # Save mean values as feather object
    arrow::write_parquet(data_ex,filename)
  }
  
# 3) Extract hazard timeseries by admin ####
haz_timeseries_files<-list.files(haz_timeseries_dir,".tif",full.names = T)
haz_timeseries_files<-grep(paste(hazards,collapse = "|"),haz_timeseries_files,value=T)

haz_timeseries_files_sd<-grep("ENSEMBLEsd",haz_timeseries_files,value=T)
haz_timeseries_files<-haz_timeseries_files[!grepl("ENSEMBLEsd",haz_timeseries_files)]

# Load all timeseries data into a raster stack
haz_timeseries<-terra::rast(haz_timeseries_files)
haz_timeseries_sd<-terra::rast(haz_timeseries_files_sd)

# Update names of raster stack to be filename/year
layer_names<-unlist(lapply(1:length(haz_timeseries_files),FUN=function(i){
  file<-haz_timeseries_files[i]
  layers<-names(terra::rast(file))
  file<-gsub(".tif","",tail(unlist(strsplit(file,"/")),1),fixed=T)
  paste0(file,"_year",layers)
}))

names(haz_timeseries)<-layer_names

layer_names<-unlist(lapply(1:length(haz_timeseries_files_sd),FUN=function(i){
  file<-haz_timeseries_files_sd[i]
  layers<-names(terra::rast(file))
  file<-gsub(".tif","",tail(unlist(strsplit(file,"/")),1),fixed=T)
  paste0(file,"_year",layers)
}))

names(haz_timeseries_sd)<-layer_names

# Extract hazard values by admin areas and average them
# Extract by admin0
haz_timeseries_adm<-admin_extract(haz_timeseries,Geographies["admin0"],FUN="mean")
st_write_parquet(obj=sf::st_as_sf(haz_timeseries_adm$admin0), dsn=paste0(haz_timeseries_dir,"/haz_timeseries_adm0.parquet"))

# Extract by admin1
haz_timeseries_adm<-admin_extract(haz_timeseries,Geographies["admin1"],FUN="mean")
st_write_parquet(obj=sf::st_as_sf(haz_timeseries_adm$admin1), dsn=paste0(haz_timeseries_dir,"/haz_timeseries_adm1.parquet"))

# Extract by admin2
haz_timeseries_adm<-admin_extract(haz_timeseries,Geographies["admin2"],FUN="mean")
st_write_parquet(obj=sf::st_as_sf(haz_timeseries_adm$admin2), dsn=paste0(haz_timeseries_dir,"/haz_timeseries_adm2.parquet"))

# Restructure data into tabular form
filename<-paste0(haz_timeseries_dir,"/haz_timeseries.parquet")
# Extract data from vector files and restructure into tabular form
haz_timeseries_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
  level<-levels[i]
  print(level)
  
  haz_timeseries_tab<-data.table(data.frame(sfarrow::st_read_parquet(paste0(haz_timeseries_dir,"/haz_timeseries_",levels[i],".parquet"))))
  N<-colnames(haz_timeseries_tab)[-grep(c("admin0_nam|admin1_nam|admin2_nam|geometry"),colnames(haz_timeseries_tab))]
  haz_timeseries_tab<-haz_timeseries_tab[,..N]
  haz_timeseries_tab<-melt(haz_timeseries_tab,id.vars = c("admin_name","iso3"))
  
  haz_timeseries_tab[,variable:=gsub("mean.","",variable,fixed = T)
  ][,variable:=gsub("mean.","",variable,fixed = T)
  ][,variable:=gsub("_ENSEMBLEmean","",variable,fixed = T)
  ][,variable:=gsub("ssp245_","ssp245|",variable,fixed = T)
  ][,variable:=gsub("ssp585_","ssp585|",variable,fixed = T)
  ][,variable:=gsub("_year","|",variable,fixed = T)
  ][,variable:=gsub("historical_","historical|historical|",variable,fixed = T)
  ][,variable:=gsub("2060_","2060|",variable,fixed = T)
  ][,variable:=gsub("2040_","2040|",variable,fixed = T)
  ][,scenario:=unlist(tstrsplit(variable,"[|]",keep=1))
  ][,timeframe:=unlist(tstrsplit(variable,"[|]",keep=2))
  ][,hazard:=unlist(tstrsplit(variable,"[|]",keep=3))
  ][,year:=unlist(tstrsplit(variable,"[|]",keep=4))
  ][,stat:=tail(unlist(tstrsplit(hazard[1],"_")),1),by=hazard
  ][,hazard:=gsub("_mean_mean","mean",hazard,fixed = T)
  ][,hazard:=gsub("_max_max","max",hazard,fixed = T)
  ][,hazard:=gsub("_mean","",hazard,fixed = T)
  ][,hazard:=gsub("_sum","",hazard,fixed = T)
  ][,hazard:=gsub("_max","",hazard,fixed = T)
  ][,hazard:=gsub("mean","_mean",hazard,fixed = T)
  ][,hazard:=gsub("max","_max",hazard,fixed = T)
  ][,variable:=NULL
  ][,admin_level:=names(levels)[i]]
  
  
  haz_timeseries_tab
  
}))

# Save mean values as feather object
    arrow::write_parquet(haz_timeseries_tab,filename)


# Extract hazard values by admin areas and average them
# Extract by admin0
haz_timeseries_adm<-admin_extract(haz_timeseries_sd,Geographies["admin0"],FUN="mean")
st_write_parquet(obj=sf::st_as_sf(haz_timeseries_adm$admin0), dsn=paste0(haz_timeseries_dir,"/haz_timeseries_sd_adm0.parquet"))

# Extract by admin1
haz_timeseries_adm<-admin_extract(haz_timeseries_sd,Geographies["admin1"],FUN="mean")
st_write_parquet(obj=sf::st_as_sf(haz_timeseries_adm$admin1), dsn=paste0(haz_timeseries_dir,"/haz_timeseries_sd_adm1.parquet"))

# Extract by admin2
haz_timeseries_adm<-admin_extract(haz_timeseries_sd,Geographies["admin2"],FUN="mean")
st_write_parquet(obj=sf::st_as_sf(haz_timeseries_adm$admin2), dsn=paste0(haz_timeseries_dir,"/haz_timeseries_sd_adm2.parquet"))


# Restructure data into tabular form
filename<-paste0(haz_timeseries_dir,"/haz_timeseries_sd.parquet")
# Extract data from vector files and restructure into tabular form
haz_timeseries_sd_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
  level<-levels[i]
  print(level)
  
  haz_timeseries_tab<-data.table(data.frame(sfarrow::st_read_parquet(paste0(haz_timeseries_dir,"/haz_timeseries_sd_",levels[i],".parquet"))))
  N<-colnames(haz_timeseries_tab)[-grep(c("admin0_nam|admin1_nam|admin2_nam|geometry"),colnames(haz_timeseries_tab))]
  haz_timeseries_tab<-haz_timeseries_tab[,..N]
  haz_timeseries_tab<-melt(haz_timeseries_tab,id.vars = c("admin_name","iso3"))
  
  haz_timeseries_tab[,variable:=gsub("mean.","",variable,fixed = T)
  ][,variable:=gsub("mean.","",variable,fixed = T)
  ][,variable:=gsub("_ENSEMBLEsd","",variable,fixed = T)
  ][,variable:=gsub("ssp245_","ssp245|",variable,fixed = T)
  ][,variable:=gsub("ssp585_","ssp585|",variable,fixed = T)
  ][,variable:=gsub("_year","|",variable,fixed = T)
  ][,variable:=gsub("historical_","historical|historical|",variable,fixed = T)
  ][,variable:=gsub("2060_","2060|",variable,fixed = T)
  ][,variable:=gsub("2040_","2040|",variable,fixed = T)
  ][,scenario:=unlist(tstrsplit(variable,"[|]",keep=1))
  ][,timeframe:=unlist(tstrsplit(variable,"[|]",keep=2))
  ][,hazard:=unlist(tstrsplit(variable,"[|]",keep=3))
  ][,year:=unlist(tstrsplit(variable,"[|]",keep=4))
  ][,stat:=tail(unlist(tstrsplit(hazard[1],"_")),1),by=hazard
  ][,hazard:=gsub("_mean_mean","mean",hazard,fixed = T)
  ][,hazard:=gsub("_max_max","max",hazard,fixed = T)
  ][,hazard:=gsub("_mean","",hazard,fixed = T)
  ][,hazard:=gsub("_sum","",hazard,fixed = T)
  ][,hazard:=gsub("_max","",hazard,fixed = T)
  ][,hazard:=gsub("mean","_mean",hazard,fixed = T)
  ][,hazard:=gsub("max","_max",hazard,fixed = T)
  ][,variable:=NULL
  ][,admin_level:=names(levels)[i]]
  
  
  haz_timeseries_tab
  
}))

# Save mean values as feather object
    arrow::write_parquet(haz_timeseries_sd_tab,filename)

# 4) Extract hazard risk x exposure by admin ####

  # Crop choices only 
  crop_choices<-crop_choices[!grepl("_tropical|_highland",crop_choices)]

  # 4.0) Set-up ####
    do_vop17<-T
    do_ha<-F
    do_n<-F
    overwrite<-F
  
  # 4.1) Multiply Hazard Risk by Exposure ####

    files<-list.files(haz_risk_dir,".tif$",full.names = T)
    
    # The process below would benefit from parallization, but error in { : task  failed - "NULL value passed as symbol address!" needs debugging
    
    #registerDoFuture()
    #plan("multisession", workers = 2)
    
    #foreach(i =  1:length(files), .packages = c("terra")) %dopar% {
    for(i in 1:length(files)){
          
          file<-files[i]
          crop<-unlist(tstrsplit(basename(file),"-",keep=1))

          save_name_vop<-paste0(haz_risk_vop_dir,"/",gsub(".tif","-vop.tif",basename(file)))
          
          haz_risk<-terra::rast(file)
          
          # Display progress
          cat('\r                                                                                                                           ')
          cat('\r',paste("Risk x Exposure x VoP17 | file:",i,"/",length(files))," - ",file)
          flush.console()
          
          
          if(!file.exists(save_name_vop)|overwrite==T){
            # vop
            if(crop!="generic"){
              if(crop %in% crop_choices){
                haz_risk_vop<-haz_risk*crop_vop_tot[[crop]]
                }else{
                haz_risk_vop<-haz_risk*livestock_vop[[crop]]
              }
            }else{
              haz_risk_vop<-haz_risk*sum(crop_vop_tot)
            }
            
            names(haz_risk_vop)<-paste0(names(haz_risk_vop),"-vop")
            terra::writeRaster(haz_risk_vop,file=save_name_vop,overwrite=T)
            haz_risk_vop<-NULL
            gc()
          }
          
          # vop17
          if(do_vop17==T){
            save_name_vop17<-paste0(haz_risk_vop17_dir,"/",gsub(".tif","-vop.tif",basename(file)))
            if(!file.exists(save_name_vop17)|overwrite==T){
    
              if(crop!="generic"){
                if(crop %in% crop_choices){
                  haz_risk_vop17<-haz_risk*crop_vop17_tot[[crop]]
                }else{
                  haz_risk_vop17<-haz_risk*livestock_vop17[[crop]]
                }
              }else{
                haz_risk_vop17<-haz_risk*sum(crop_vop17_tot)
              }
              names(haz_risk_vop17)<-paste0(names(haz_risk_vop17),"-vop")
              terra::writeRaster(haz_risk_vop17,file=save_name_vop17,overwrite=T)
              haz_risk_vop17<-NULL
              gc()
            }
            
          }
          
          # ha
          if(do_ha==T){

            if(crop %in% c("generic",crop_choices)){
              # Display progress
              cat('\r                                                                                                                           ')
              cat('\r',paste("Risk x Exposure x ha | file:",i,"/",length(files))," - ",file)
              flush.console()
              
              save_name_ha<-paste0(haz_risk_ha_dir,"/",gsub(".tif$","-ha.tif$",basename(file)))
              
              if(!file.exists(save_name_ha)|overwrite==T){
                if(crop!="generic"){
                  haz_risk_ha<-haz_risk*crop_ha_tot[[crop]]
                }else{
                  haz_risk_ha<-haz_risk*sum(crop_ha_tot)
                }
                
                names(haz_risk_ha)<-paste0(names(haz_risk_ha),"-ha")
                terra::writeRaster(haz_risk_ha,file=save_name_ha,overwrite=T)
                haz_risk_ha<-NULL
                gc()
              }
            }
          }
          
          # numbers
          if(do_n==T){
            
             if(crop %in% c("generic",livestock_choices)){
               # Display progress
               cat('\r                                                                                                                           ')
               cat('\r',paste("Risk x Exposure x n | file:",i,"/",length(files))," - ",file)
               flush.console()
               
              save_name_n<-paste0(haz_risk_n_dir,"/",gsub(".tif$","-n.tif$",basename(file)))
              
              if(!file.exists(save_name_n)|overwrite==T){
                if(crop!="generic"){
                  haz_risk_n<-haz_risk*livestock_no[[crop]]
                }else{
                  haz_risk_n<-haz_risk*sum(livestock_no[[(c("total_tropical","total_highland"))]],na.rm=T)
                }
                
                names(haz_risk_n)<-paste0(names(haz_risk_n),"-n")
                terra::writeRaster(haz_risk_n,file=save_name_n,overwrite=T)
                haz_risk_n<-NULL
                gc()
              }
            }
          }
          
    }
    
    #plan(sequential)
    
  # 4.2) Extract Risk x Exposure by Geography  ####

    for(INT in c(T,F)){
    print(paste0("Interactions = ",INT))
    haz_risk_exp_extract(severity_classes,
                         interactions=INT,
                         folder=haz_risk_vop_dir,
                         overwrite=overwrite,
                         rm_crop=NULL,
                         rm_haz=NULL)
    
    if(do_vop17){
      haz_risk_exp_extract(severity_classes,
                           interactions=INT,
                           folder=haz_risk_vop17_dir,
                           overwrite=overwrite,
                           rm_crop=NULL,
                           rm_haz=NULL)
    }
    
    if(do_ha){
      haz_risk_exp_extract(severity_classes,
                           interactions=INT,
                           folder=haz_risk_vop_dir,
                           overwrite=overwrite,
                           rm_crop=NULL,
                           rm_haz=NULL)
      }
    
    if(do_n){
        haz_risk_exp_extract(severity_classes,
                             interactions=INT,
                             folder=haz_risk_vop_dir,
                             overwrite=overwrite,
                             rm_crop=NULL,
                             rm_haz=NULL)
      }
  }
  
  if(F){
    # Check resulting files
    (file<-list.files(haz_risk_vop_dir,"parquet",full.names = T))
    data<-sfarrow::st_read_parquet(file[4])
    names(data)
  }
        
  # 4.3) Restructure Extracted Data ####

  for(SEV in tolower(severity_classes$class)){
    # Restructure Extracted Data ####
    for(INT in c(T,F)){
      print(paste0(SEV," - interaction = ",INT))
      # Vop
      recode_restructure_wrap(folder=haz_risk_vop_dir,
                              file="adm",
                              crops=c("generic",crop_choices),
                              livestock=livestock_choices,
                              exposure_var="vop",
                              severity=SEV,
                              overwrite=overwrite,
                              levels=levels,
                              interaction=INT,
                              hazards=haz_meta[,unique(type)])
      
      # Harvested area
      if(do_ha==T){
        recode_restructure_wrap(folder=haz_risk_ha_dir,
                                file="adm",
                                crops=c("generic",crop_choices),
                                livestock=livestock_choices,
                                exposure_var="ha",
                                Severity=SEV,
                                overwrite=overwrite,
                                levels=levels,
                                interaction=INT,
                                hazards=haz_meta[,unique(type)])
      }
      
      # Numbers
      if(do_n==T){
        recode_restructure_wrap(folder=haz_risk_n_dir,
                                file="adm",
                                crops=c("generic",crop_choices),
                                livestock=livestock_choices,
                                exposure_var="number",
                                Severity=SEV,
                                overwrite=overwrite,
                                levels=levels,
                                interaction=INT,
                                hazards=haz_meta[,unique(type)])
      }
    }
    
  }
  
  if(F){
     # Check results
    (files<-list.files(haz_risk_vop_dir,"_adm_",full.names = T))
    for(i in 1:length(files)){
      file<-files[i]
      print(file)
      data<-arrow::read_parquet(file)
      print(head(data))
      print(data[,unique(hazard)])
      print(data[,unique(crop)])
  }
  }
  