require(terra)
require(data.table)
require(exactextractr)
require(sf)
require(sfarrow)
require(arrow)
require(feather)
require(doFuture)

# Set scenarios and time frames to analyse
Scenarios<-c("ssp245","ssp585")
Times<-c("2021_2040","2041_2060")

# Create combinations of scenarios and times
Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))
Scenarios[,combined:=paste0(Scenario,"-",Time)]

# Set hazards to include in analysis
hazards<-c("NDD","NTx40","NTx35","HSH_max","HSH_mean","THI_max","THI_mean","NDWS","TAI","NDWL0","PTOT","TAVG")
haz_meta<-data.table::fread("./Data/metadata/haz_metadata.csv")
haz_class<-fread("./Data/metadata/haz_classes.csv")[,list(index_name,description,direction,crop,threshold)]
haz_classes<-unique(haz_class$description)

# Pull out severity classes and associate impact scores
severity_classes<-unique(fread("./Data/metadata/haz_classes.csv")[,list(description,value)])
setnames(severity_classes,"description","class")

# Create combinations of scenarios and hazards
scenarios_x_hazards<-data.table(Scenarios,Hazard=rep(hazards,each=nrow(Scenarios)))[,Scenario:=as.character(Scenario)][,Time:=as.character(Time)]

timeframe_choice<-"annual"
#timeframe_choice<-"jagermeyr"

# Load crop names from mapspam metadata
ms_codes<-data.table::fread("./Data/metadata/SpamCodes.csv")[,Code:=toupper(Code)]
ms_codes<-ms_codes[compound=="no"]
crop_choices<-ms_codes[,sort(Fullname)]

# Create extraction function
admin_extract<-function(data,Geographies,FUN="mean",max_cells_in_memory=3*10^7){
  output<-list()
  if("admin0" %in% names(Geographies)){
    data0<-exactextractr::exact_extract(data,sf::st_as_sf(Geographies$admin0),fun=FUN,append_cols=c("admin_name","admin0_nam","iso3"),max_cells_in_memory=max_cells_in_memory)
    data0<-terra::merge(Geographies$admin0,data0)
    output$admin0<-data0
  }
  
  if("admin1" %in% names(Geographies)){
    data1<-exactextractr::exact_extract(data,sf::st_as_sf(Geographies$admin1),fun=FUN,append_cols=c("admin_name","admin0_nam","admin1_nam","iso3"),max_cells_in_memory=max_cells_in_memory)
    data1<-terra::merge(Geographies$admin1,data1)
    output$admin1<-data1
  }
  
  if("admin2" %in% names(Geographies)){
    data2<-exactextractr::exact_extract(data,sf::st_as_sf(Geographies$admin2),fun=FUN,append_cols=c("admin_name","admin0_nam","admin1_nam","admin2_nam","iso3"),max_cells_in_memory=max_cells_in_memory)
    data2<-terra::merge(Geographies$admin2,data2)
    output$admin2<-data2
  }
  
  return(output)
}

#### Load datasets
# 1) Geographies #####
# Load and combine geoboundaries
Geographies<-list(
  admin2=terra::vect("Data/geoboundaries/admin2_processed.shp"),
  admin1=terra::vect("Data/geoboundaries/admin1_processed.shp"),
  admin0=terra::vect("Data/geoboundaries/admin0_processed.shp")
)

# 2) Exposure variables ####

# create a "base raster" for resampling 
haz_risk_dir<-paste0("Data/hazard_risk/",timeframe_choice)
haz_risk_files<-list.files(haz_risk_dir,full.names = T)
haz_risk<-terra::rast(haz_risk_files[1])

exposure_dir<-"Data/exposure"
if(!dir.exists(exposure_dir)){
  dir.create(exposure_dir)
}
  # 2.1) Crops (MapSPAM) #####
  
  mapspam_dir<-"Data/mapspam"
  
    # 2.1.1) Crop Value of production ######
  vop<-fread(paste0(mapspam_dir,"/spam2017V2r3_SSA_V_TA.csv"))
  crops<-tolower(ms_codes$Code)
  ms_fields<-c("x","y",grep(paste0(crops,collapse = "|"),colnames(vop),value=T))
  vop<-rast(vop[,..ms_fields],type="xyz",crs="EPSG:4326")
  names(vop)<-gsub("_a","",names(vop))
  names(vop)<-ms_codes[match(names(vop),tolower(ms_codes$Code)),Fullname]
  # convert to value/area 
  crop_vop<-vop/terra::cellSize(vop,unit="ha")
  # resample data
  crop_vop<-terra::resample(crop_vop,haz_risk)
  crop_vop_tot<-crop_vop*cellSize(crop_vop,unit="ha")
  
  terra::writeRaster(crop_vop_tot,filename = paste0(exposure_dir,"/crop_vop.tif"),overwrite=T)
  
    # 2.1.2) Crop Harvested Area #####
  ha<-fread(paste0(mapspam_dir,"/spam2017V2r3_SSA_H_TA.csv"))
  crops<-tolower(ms_codes$Code)
  ms_fields<-c("x","y",grep(paste0(crops,collapse = "|"),colnames(ha),value=T))
  ha<-rast(ha[,..ms_fields],type="xyz",crs="EPSG:4326")
  names(ha)<-gsub("_a","",names(ha))
  names(ha)<-ms_codes[match(names(ha),tolower(ms_codes$Code)),Fullname]
  # convert to value/area 
  crop_ha<-ha/terra::cellSize(ha,unit="ha")
  # resample  data
  crop_ha<-terra::resample(crop_ha,haz_risk)
  crop_ha_tot<-crop_ha*cellSize(crop_ha,unit="ha")
  
  terra::writeRaster(crop_ha_tot,filename = paste0(exposure_dir,"/crop_ha.tif"),overwrite=T)
  
    # 2.1.3) Create Crop Masks ######
  commodity_mask_dir<-"Data/commodity_masks"
  if(!dir.exists(commodity_mask_dir)){
    dir.create(commodity_mask_dir)
  }
  
  # Need pto use mapspam physical area
  pa<-fread(paste0(mapspam_dir,"/spam2017V2r3_SSA_A_TA.csv"))
  crops<-tolower(ms_codes$Code)
  ms_fields<-c("x","y",grep(paste0(crops,collapse = "|"),colnames(pa),value=T))
  pa<-rast(pa[,..ms_fields],type="xyz",crs="EPSG:4326")
  names(pa)<-gsub("_a","",names(pa))
  names(pa)<-ms_codes[match(names(pa),tolower(ms_codes$Code)),Fullname]
  # convert to value/area 
  crop_pa<-pa/terra::cellSize(pa,unit="ha")
  # resample  data
  crop_pa<-terra::resample(crop_pa,haz_risk)
  crop_pa_tot<-crop_pa*cellSize(crop_pa,unit="ha")
  
  # Areas with >0.01% harvested area = crop mask
  crop_pa_prop<-crop_pa_tot/cellSize(crop_pa_tot,unit="ha")
  crop_mask<-terra::classify(crop_pa_prop,  data.frame(from=c(0,0.001),to=c(0.001,2),becomes=c(0,1)))
  terra::writeRaster(crop_mask,filename=paste0(commodity_mask_dir,"/crop_masks.tif"),overwrite=T)
  
  # 2.2) Livestock #####
    # 2.2.1) Livestock Numbers (GLW3) ######
    Cattle<-terra::rast("Data/GLW3/5_Ct_2010_Da.tif")
    Chicken<-terra::rast("Data/GLW3/5_Ch_2010_Da.tif")
    Goat<-terra::rast("Data/GLW3/5_Gt_2010_Da.tif")
    Pig<-terra::rast("Data/GLW3/5_Pg_2010_Da.tif")
    Sheep<-terra::rast("Data/GLW3/5_Sh_2010_Da.tif")
    
    TLU<-Cattle*0.7 + Sheep*0.1 + Goat*0.1 + 0.01*Chicken + 0.2*Pig
    
    livestock_no<-c(Cattle,Chicken,Goat,Pig,Sheep,TLU)
    names(livestock_no)<-c("cattle","poultry","goats","pigs","sheep","total_livestock_units")
    livestock_no<-terra::mask(terra::crop(livestock_no,Geographies$admin0),Geographies$admin0)
    
    # resample to 0.05
    livestock_density<-livestock_no/terra::cellSize(livestock_no,unit="ha")
    livestock_density<-terra::resample(livestock_density,haz_risk)
    livestock_no<-livestock_density*cellSize(livestock_density,unit="ha")
    
    terra::writeRaster(livestock_no,filename = paste0(exposure_dir,"/livestock_no.tif"),overwrite=T)
    
    
    # 2.2.2) Livestock VoP ######
    # Note unit is IUSD 2005
    livestock_vop<-terra::rast(list.files("Data/livestock_vop",".tif",full.names = T))
    names(livestock_vop)<-c("cattle","poultry","pigs","sheep_goat","total")
    
    # resample to 0.05
    livestock_density<-livestock_vop/terra::cellSize(livestock_vop,unit="ha")
    livestock_density<-terra::resample(livestock_density,haz_risk)
    livestock_vop<-livestock_density*cellSize(livestock_density,unit="ha")
    rm(livestock_density)
    
    # Split sheep goat vop using their populations
    sheep_prop<-livestock_no$sheep/(livestock_no$goats +livestock_no$sheep)
    goat_prop<-livestock_no$goats/(livestock_no$goats +livestock_no$sheep)
    
    livestock_vop$sheep<-livestock_vop$sheep_goat*sheep_prop
    livestock_vop$goats<-livestock_vop$sheep_goat*goat_prop
    
    livestock_vop$sheep_goat<-NULL
    
    terra::writeRaster(livestock_vop,filename = paste0(exposure_dir,"/livestock_vop.tif"),overwrite=T)
    
    # 2.2.3) Livestock Mask #####
    Cattle<-terra::rast("Data/GLW3/5_Ct_2010_Da.tif")
    Chicken<-terra::rast("Data/GLW3/5_Ch_2010_Da.tif")
    Goat<-terra::rast("Data/GLW3/5_Gt_2010_Da.tif")
    Pig<-terra::rast("Data/GLW3/5_Pg_2010_Da.tif")
    Sheep<-terra::rast("Data/GLW3/5_Sh_2010_Da.tif")
    
    TLU<-Cattle*0.7 + Sheep*0.1 + Goat*0.1 + 0.01*Chicken + 0.2*Pig
    
    lus<-c(Cattle*0.7,Chicken*0.01,Goat*0.1,Pig*0.2,Sheep*0.1,TLU)
    names(lus)<-c("cattle","poultry","goats","pigs","sheep","total")
    lus<-terra::mask(terra::crop(lus,Geographies$admin0),Geographies$admin0)
    
    # resample to 0.05
    lu_density<-lus/terra::cellSize(lus,unit="ha")
    lu_density<-terra::resample(lu_density,haz_risk)
    
    # Classify requires 0.00001 livestock units per ha to be present
    livestock_mask<-terra::classify(lu_density, data.frame(from=c(0,0.00001),to=c(0.00001,Inf),becomes=c(0,1)))
    terra::writeRaster(livestock_mask,filename=paste0(commodity_mask_dir,"/livestock_masks.tif"),overwrite=T)
    
# 2.3) Population ######
base_rast<-terra::rast(list.files("Data/hazard_classified/annual",".tif",full.names = T))[[1]]

hpop<-terra::rast(list.files("Data/atlas_pop",".tif",full.names=T))
hpop<-terra::crop(hpop,Geographies)
hpop<-terra::resample(hpop,base_rast)

names(hpop)<-unlist(tail(tstrsplit(names(hpop),"_"),1))

terra::writeRaster(hpop,filename = paste0(exposure_dir,"/hpop.tif"),overwrite=T)


#### Intersect Risk and Exposure ####
# 1) Hazard Risk ####
haz_risk_dir<-paste0("Data/hazard_risk/",timeframe_choice)
haz_risk_files<-list.files(haz_risk_dir,".tif",full.names = T)

overwrite<-F
terra::gdalCache(60000)
max_cells_in_memory<-15*10^9 # I think the largest files require 14.3 billion cells not sure how much memory a cell uses, 1 billion bytes = 1 GB

# Note I have tried to introduce foreach parallization into the loop below,but it returned an error

for(SEV in tolower(severity_classes$class[2])){
  print(SEV)
  if((!file.exists(paste0(haz_risk_dir,"/haz_risk_adm0_",SEV,".parquet")))|overwrite==T){
    haz_risk<-terra::rast(haz_risk_files[grepl(SEV,haz_risk_files)])
    
        # Display progress
        cat('\r                                                                                                                                                 ')
        cat('\r',paste("Severity Class:",SEV,"| Chunk:",i,"/",cores))
        flush.console()
        
        haz_risk_adm<-admin_extract(haz_risk,Geographies["admin0"],FUN="mean",max_cells_in_memory=max_cells_in_memory)
        st_write_parquet(obj=sf::st_as_sf(haz_risk_adm$admin0), dsn=paste0(haz_risk_dir,"/haz_risk_adm0_",SEV,".parquet"))
        gc()
        haz_risk_adm<-admin_extract(haz_risk,Geographies["admin1"],FUN="mean",max_cells_in_memory=max_cells_in_memory)
        st_write_parquet(obj=sf::st_as_sf(haz_risk_adm$admin1), dsn=paste0(haz_risk_dir,"/haz_risk_adm1_",SEV,".parquet"))
        gc()
        haz_risk_adm<-admin_extract(haz_risk,Geographies["admin2"],FUN="mean",max_cells_in_memory=max_cells_in_memory)
        st_write_parquet(obj=sf::st_as_sf(haz_risk_adm$admin2), dsn=paste0(haz_risk_dir,"/haz_risk_adm2_",SEV,".parquet"))
        gc()
    }

}

# 2) Commodity Specific Hazard Risk x Crop or Livestock VoP & Crop Harvested Area ####
haz_risk_vop_dir<-paste0("Data/hazard_risk_vop/",timeframe_choice)
if(!dir.exists(haz_risk_vop_dir)){
  dir.create(haz_risk_vop_dir,recursive = T)
}

haz_risk_vop_dir_tot<-paste0(haz_risk_vop_dir,"/tot")
if(!dir.exists(haz_risk_vop_dir_tot)){
  dir.create(haz_risk_vop_dir_tot,recursive = T)
}

haz_risk_ha_dir<-paste0("Data/hazard_risk_ha/",timeframe_choice)
if(!dir.exists(haz_risk_ha_dir)){
  dir.create(haz_risk_ha_dir,recursive = T)
}

haz_risk_ha_dir_tot<-paste0(haz_risk_ha_dir,"/tot")
  if(!dir.exists(haz_risk_ha_dir_tot)){
    dir.create(haz_risk_ha_dir_tot,recursive = T)
  }

haz_risk_n_dir<-paste0("Data/hazard_risk_n/",timeframe_choice)
if(!dir.exists(haz_risk_n_dir)){
  dir.create(haz_risk_n_dir,recursive = T)
}

haz_risk_n_dir_tot<-paste0(haz_risk_n_dir,"/tot")
if(!dir.exists(haz_risk_n_dir_tot)){
  dir.create(haz_risk_n_dir_tot,recursive = T)
}

# List livestock names
livestock_choices<-list.files(haz_risk_dir,".tif")
livestock_choices<-grep("highland|tropical",livestock_choices,value = T)
livestock_choices<-unique(unlist(tstrsplit(livestock_choices,"_extr|_seve",keep=1)))

haz_risk_files<-list.files(haz_risk_dir,".tif",full.names = T)

calculate_totals<-F

for(SEV in tolower(severity_classes$class[2])){
  
  #### Multiply Hazard Risk by Exposure ####
  
  haz_risk_files2<-haz_risk_files[grepl(SEV,haz_risk_files) & !grepl("_int",haz_risk_files)]
  
  #registerDoFuture()
  #plan("multisession", workers = 10)
  
  #  loop starts here
  #foreach(i = 1:length(haz_risk_files2)) %dopar% {
    
    for(i in 1:length(haz_risk_files2)){
    
    crop<-tail(unlist(tstrsplit(unlist(tstrsplit(haz_risk_files2[i],"_",keep=2)),"/")),1)
    if(crop %in% unlist(tstrsplit(livestock_choices,"_",keep=1))){
      crop1<-crop
      if(grepl("highland",haz_risk_files2[i])){
        crop<-paste0(crop,"_highland")
      }else{
        crop<-paste0(crop,"_tropical")
      }
    }
    
    # Display progress
    cat('\r                                                                                                                           ')
    cat('\r',paste("Risk x Exposure | crop:",i,"/",length(haz_risk_files2)," ",crop,"| severity:",SEV))
    flush.console()
    
    save_name_vop<-paste0(haz_risk_vop_dir,"/",crop,"_",SEV,"_vop.tif")
    
    files<-haz_risk_files2[i]
    files<-c(files,gsub(".tif","_int.tif",files))
    
    if(!file.exists(save_name_vop)){
      haz_risk<-terra::rast(files)
      
      # vop
      if(crop!="generic"){
        if(crop %in% crop_choices){
         haz_risk_vop<-haz_risk*crop_vop_tot[[crop]]
        }else{
          haz_risk_vop<-haz_risk*livestock_vop[[crop1]]
        }
        }else{
        haz_risk_vop<-haz_risk*sum(crop_vop_tot)
      }
      
      names(haz_risk_vop)<-paste0(names(haz_risk_vop),"-vop")
      writeRaster(haz_risk_vop,file=save_name_vop)
    
    # ha
      if(crop %in% c("generic",crop_choices)){
      save_name_ha<-paste0(haz_risk_ha_dir,"/",crop,"_",SEV,"_ha.tif")

      if(crop!="generic"){
       haz_risk_ha<-haz_risk*crop_ha_tot[[crop]]
      }else{
        haz_risk_ha<-haz_risk*sum(crop_ha_tot)
      }
      
      names(haz_risk_ha)<-paste0(names(haz_risk_ha),"-ha")
      writeRaster(haz_risk_ha,file=save_name_ha)
      }
      
      
      # numbers
      if(crop %in% c("generic",livestock_choices)){
        save_name_n<-paste0(haz_risk_n_dir,"/",crop,"_",SEV,"_n.tif")
        
        if(crop!="generic"){
          haz_risk_n<-haz_risk*livestock_no[[crop1]]
        }else{
          haz_risk_n<-haz_risk*livestock_no$total_livestock_units
        }
        
        names(haz_risk_n)<-paste0(names(haz_risk_n),"-n")
        writeRaster(haz_risk_n,file=save_name_n)
      }
    }
    
  }
  
  if(calculate_totals){
    # Combine crops to give total exposure by hazard
    # vop
    save_name<-paste0(save_name_vop,"/total_vop.tif")
    if(!file.exists(save_name)){
      haz_risk_vop_files<-list.files(haz_risk_vop_dir,SEV,full.names = T)
      haz_risk_vop_files<-haz_risk_vop_files[grepl(paste0(crop_choices,collapse = "|"),haz_risk_vop_files)]
      
      haz_risk_vop<-terra::rast(haz_risk_vop_files)
      haz_risk_vop_names<-names(haz_risk_vop)
      hazards<-unique(unlist(tstrsplit(haz_risk_vop_names,"-",keep=3)))
      
      haz_risk_vop_tot<-terra::rast(lapply(1:nrow(scenarios_x_hazards),FUN=function(j){
        names_1<-haz_risk_vop_names[grepl(scenarios_x_hazards[j,combined],haz_risk_vop_names)]
        x<-terra::rast(lapply(1:length(hazards),FUN=function(k){
          # Display progress
          cat('\r                                                                                                                     ')
          cat('\r',paste("Risk x Exposure (vop)| creating totals:",j,scenarios_x_hazards[j,combined],"| hazard:",hazards[k],k))
          flush.console()
          
          lyr_name<-paste0(scenarios_x_hazards[j,combined],"-",hazards[k],"-total-",SEV,"-vop")
          file_name<-paste0(haz_risk_vop_dir_tot,"/",lyr_name,".tif")
          if(!file.exists(file_name)){
          names_2<-names_1[grepl(hazards[k],names_1)]
          data<-haz_risk_vop[[names_2]]
          data<-app(data,sum,na.rm=T)
          names(data)<-lyr_name
          write(data,file=file_name)
          data
          }else{
            terra::rast(file_name)
          }
        }))
        x
      }))
      
      writeRaster(haz_risk_vop_tot,file=save_name)
      
    }
  
    # harvested area
    save_name<-paste0(save_name_ha,"/total_ha.tif")
    if(!file.exists(save_name)){
      haz_risk_ha_files<-list.files(haz_risk_ha_dir,SEV,full.names = T)
      haz_risk_ha_files<-haz_risk_ha_files[grepl(paste0(crop_choices,collapse = "|"),haz_risk_ha_files)]
      
      haz_risk_ha<-terra::rast(haz_risk_ha_files)
      haz_risk_ha_names<-names(haz_risk_ha)
      hazards<-unique(unlist(tstrsplit(haz_risk_ha_names,"-",keep=3)))
      
      haz_risk_ha_tot<-terra::rast(lapply(1:nrow(scenarios_x_hazards),FUN=function(j){
        names_1<-haz_risk_ha_names[grepl(scenarios_x_hazards[j,combined],haz_risk_ha_names)]
        data<-terra::rast(lapply(1:length(hazards),FUN=function(k){
          # Display progress
          cat('\r                                                                                                                     ')
          cat('\r',paste("Risk x Exposure (ha) | creating totals:",j,scenarios_x_hazards[j,combined],"| hazard:",hazards[k],k))
          flush.console()
          
          lyr_name<-paste0(scenarios_x_hazards[j,combined],"-",hazards[k],"-total-",SEV,"-ha")
          file_name<-paste0(haz_risk_ha_dir_tot,"/",lyr_name,".tif")
          if(!file.exists(file_name)){
            names_2<-names_1[grepl(hazards[k],names_1)]
            data<-haz_risk_ha[[names_2]]
            data<-sum(data)
            names(data)<-lyr_name
            write(data,file=file_name)
            data
          }else{
            terra::rast(file_name)
          }
        }))
      }))
      
      writeRaster(haz_risk_ha_tot,file=save_name)
      
    }
    
    # number
    save_name<-paste0(save_name_n,"/total_n.tif")
    if(!file.exists(save_name)){
      haz_risk_n_files<-list.files(haz_risk_n_dir,SEV,full.names = T)
      haz_risk_n_files<-haz_risk_n_files[grepl(paste0(crop_choices,collapse = "|"),haz_risk_n_files)]
      
      haz_risk_n<-terra::rast(haz_risk_n_files)
      haz_risk_n_names<-names(haz_risk_n)
      hazards<-unique(unlist(tstrsplit(haz_risk_n_names,"-",keep=3)))
      
      haz_risk_n_tot<-terra::rast(lapply(1:nrow(scenarios_x_hazards),FUN=function(j){
        names_1<-haz_risk_n_names[grepl(scenarios_x_hazards[j,combined],haz_risk_n_names)]
        data<-terra::rast(lapply(1:length(hazards),FUN=function(k){
          # Display progress
          cat('\r                                                                                                                     ')
          cat('\r',paste("Risk x Exposure (numbers) | creating totals:",j,scenarios_x_hazards[j,combined],"| hazard:",hazards[k],k))
          flush.console()
          
          lyr_name<-paste0(scenarios_x_hazards[j,combined],"-",hazards[k],"-total-",SEV,"-ha")
          file_name<-paste0(haz_risk_n_dir_tot,"/",lyr_name,".tif")
          if(!file.exists(file_name)){
            names_2<-names_1[grepl(hazards[k],names_1)]
            data<-haz_risk_n[[names_2]]
            data<-sum(data)
            names(data)<-lyr_name
            write(data,file=file_name)
            data
          }else{
            terra::rast(file_name)
          }
        }))
      }))
      
      writeRaster(haz_risk_n_tot,file=save_name)
      
    
    }
  }
  
  #### Extract Risk x Exposure by Geography  ####
  
  # Display progress
  cat('\r                                                                                                                     ')
  cat('\r',paste("Risk x Exposure - VoP admin extraction| severity:",SEV))
  flush.console()
  
  haz_risk_vop_files<-list.files(haz_risk_vop_dir,".tif",full.names = T)
  haz_risk_vop_files<-haz_risk_vop_files[grepl(SEV,haz_risk_vop_files)]
  
  haz_risk_vop<-terra::rast(haz_risk_vop_files)
  
  haz_risk_vop_adm<-admin_extract(haz_risk_vop,Geographies["admin0"],FUN="sum")
  sfarrow::st_write_parquet(obj=sf::st_as_sf(haz_risk_vop_adm$admin0), dsn=paste0(haz_risk_vop_dir,"/haz_risk_vop_adm0.parquet"))
  gc()
  haz_risk_vop_adm<-admin_extract(haz_risk_vop,Geographies["admin1"],FUN="sum")
  sfarrow::st_write_parquet(obj=sf::st_as_sf(haz_risk_vop_adm$admin1), dsn=paste0(haz_risk_vop_dir,"/haz_risk_vop_adm1.parquet"))
  gc()
  haz_risk_vop_adm<-admin_extract(haz_risk_vop,Geographies["admin2"],FUN="sum")
  sfarrow::st_write_parquet(obj=sf::st_as_sf(haz_risk_vop_adm$admin2), dsn=paste0(haz_risk_vop_dir,"/haz_risk_vop_adm2.parquet"))
  gc()
  
  # Display progress
  cat('\r                                                                                                                     ')
  cat('\r',paste("Risk x Exposure - Harvested Area admin extraction| severity:",SEV))
  flush.console()
  
  haz_risk_ha_files<-list.files(haz_risk_ha_dir,".tif",full.names = T)
  haz_risk_ha_files<-haz_risk_ha_files[grepl(SEV,haz_risk_ha_files)]
  
  haz_risk_ha<-terra::rast(haz_risk_ha_files)
  
  haz_risk_ha_adm<-admin_extract(haz_risk_ha,Geographies["admin0"],FUN="sum")
  sfarrow::st_write_parquet(obj=sf::st_as_sf(haz_risk_ha_adm$admin0), dsn=paste0(haz_risk_ha_dir,"/haz_risk_ha_adm0.parquet"))
  gc()
  haz_risk_ha_adm<-admin_extract(haz_risk_ha,Geographies["admin1"],FUN="sum")
  sfarrow::st_write_parquet(obj=sf::st_as_sf(haz_risk_ha_adm$admin1), dsn=paste0(haz_risk_ha_dir,"/haz_risk_ha_adm1.parquet"))
  gc()
  haz_risk_ha_adm<-admin_extract(haz_risk_ha,Geographies["admin2"],FUN="sum")
  sfarrow::st_write_parquet(obj=sf::st_as_sf(haz_risk_ha_adm$admin2), dsn=paste0(haz_risk_ha_dir,"/haz_risk_ha_adm2.parquet"))
  gc()
  
  # Display progress
  cat('\r                                                                                                                     ')
  cat('\r',paste("Risk x Exposure - Numbers admin extraction| severity:",SEV))
  flush.console()
  
  haz_risk_n_files<-list.files(haz_risk_n_dir,".tif",full.names = T)
  haz_risk_n_files<-haz_risk_n_files[grepl(SEV,haz_risk_n_files)]
  
  haz_risk_n<-terra::rast(haz_risk_n_files)

  haz_risk_n_adm<-admin_extract(haz_risk_n,Geographies["admin0"],FUN="sum")
  sfarrow::st_write_parquet(obj=sf::st_as_sf(haz_risk_n_adm$admin0), dsn=paste0(haz_risk_n_dir,"/haz_risk_n_adm0.parquet"))
  gc()
  haz_risk_n_adm<-admin_extract(haz_risk_n,Geographies["admin1"],FUN="sum")
  sfarrow::st_write_parquet(obj=sf::st_as_sf(haz_risk_n_adm$admin1), dsn=paste0(haz_risk_n_dir,"/haz_risk_n_adm1.parquet"))
  gc()
  haz_risk_n_adm<-admin_extract(haz_risk_n,Geographies["admin2"],FUN="sum")
  sfarrow::st_write_parquet(obj=sf::st_as_sf(haz_risk_n_adm$admin2), dsn=paste0(haz_risk_n_dir,"/haz_risk_n_adm2.parquet"))
  gc()

  #### Restructure Extracted Data ####
  levels<-c(admin0="adm0",admin1="adm1",admin2="adm2")
  
  recode_restructure<-function(data,crop_choices,livestock_choices,Scenarios,exposure,Severity,admin_level){
    
    # Remove "." in crop names
    for(crop in crop_choices){
      data[,variable:=gsub(gsub(" ",".",crop,fixed=T),crop,variable)]
    }
    
    # Remove "." in hazard names
    variables<-data[,unique(variable)]
    variables<-gsub("sum.","",variables)
    variables<-gsub(paste0(Scenarios$Scenario,collapse = "|"),"",variables)
    variables<-gsub(paste0(Scenarios$Time,collapse = "|"),"",variables)
    variables<-gsub(paste0(crop_choices,collapse = "|"),"",variables)
    variables<-gsub(paste0(livestock_choices,collapse = "|"),"",variables)
    variables<-unique(gsub("Moderate|Severe|Extreme","",variables))
    # Remove trailing exposure name (as this can be something like "n" it needs a different approach)
    variables<-sapply(1:length(variables),FUN=function(g){
      substring(variables[g],1,nchar(variables[g])-nchar(paste0(".",exposure)))
    })
    variables<-grep("[.]",unique(gsub("[.][.]|[.][.][.]","",variables)),value=T)
    variables1<-unique(gsub("[.]","+",variables))
    
    for(n in 1:length(variables)){
      data[,variable:=gsub(variables[n],variables1[n],variable,fixed=T)]
    }
    
    # Split variable string into columns
    data[,scenario:=unlist(tstrsplit(variable[1],"[.]",keep=2)),by=variable
    ][,timeframe:=unlist(tstrsplit(variable[1],"[.]",keep=3)),by=variable
    ][,hazard:=unlist(tstrsplit(variable[1],"[.]",keep=4)),by=variable
    ][,crop:=unlist(tstrsplit(variable[1],"[.]",keep=5)),by=variable
    ][,severity:=Severity
    ][,exposure:=unlist(tstrsplit(variable[1],"[.]",keep=7)),by=variable
    ][,variable:=NULL
    ][,admin_level:=admin_level] 

    return(data)
  }
  
  haz_risk_vop_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
    level<-levels[i]
    # Vop
    haz_risk_vop_tab<-data.table(data.frame(sfarrow::st_read_parquet(paste0(haz_risk_vop_dir,"/haz_risk_vop_",levels[i],".parquet"))))
    N<-colnames(haz_risk_vop_tab)[-grep(c("admin0_nam|admin1_nam|admin2_nam|geometry"),colnames(haz_risk_vop_tab))]
    haz_risk_vop_tab<-haz_risk_vop_tab[,..N]
    haz_risk_vop_tab<-melt(haz_risk_vop_tab,id.vars = c("admin_name","iso3"))

    haz_risk_vop_tab<-recode_restructure(data=haz_risk_vop_tab,
                                         crop_choices = c("generic",crop_choices),
                                         livestock_choices = livestock_choices,
                                         Scenarios = Scenarios,
                                         exposure="vop",
                                         Severity=SEV,
                                         admin_level=names(levels)[i])
    

    haz_risk_vop_tab
  }))
  
  # Save as feather object
  filename<-paste0(haz_risk_vop_dir,"/haz_risk_vop_",SEV,".feather")
  feather::write_feather(haz_risk_vop_tab,filename)
  
  # Harvested area
  haz_risk_ha_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
    level<-levels[i]
    
    # Harvested area
    haz_risk_ha_tab<-data.table(data.frame(sfarrow::st_read_parquet(paste0(haz_risk_ha_dir,"/haz_risk_ha_",levels[i],".parquet"))))
    N<-colnames(haz_risk_ha_tab)[-grep(c("admin0_nam|admin1_nam|admin2_nam|geometry"),colnames(haz_risk_ha_tab))]
    haz_risk_ha_tab<-haz_risk_ha_tab[,..N]
    haz_risk_ha_tab<-melt(haz_risk_ha_tab,id.vars = c("admin_name","iso3"))
    
    haz_risk_ha_tab<-recode_restructure(data=haz_risk_ha_tab,
                                         crop_choices = c("generic",crop_choices),
                                         livestock_choices = livestock_choices,
                                         Scenarios = Scenarios,
                                         exposure="ha",
                                         Severity=SEV,
                                         admin_level=names(levels)[i])
    haz_risk_ha_tab
    
  }))
  filename<-paste0(haz_risk_ha_dir,"/haz_risk_ha_",SEV,".feather")
  feather::write_feather(haz_risk_ha_tab,filename)
  
  # Numbers
  haz_risk_n_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
    level<-levels[i]
    
    haz_risk_n_tab<-data.table(data.frame(sfarrow::st_read_parquet(paste0(haz_risk_n_dir,"/haz_risk_n_",levels[i],".parquet"))))
    N<-colnames(haz_risk_n_tab)[-grep(c("admin0_nam|admin1_nam|admin2_nam|geometry"),colnames(haz_risk_n_tab))]
    haz_risk_n_tab<-haz_risk_n_tab[,..N]
    haz_risk_n_tab<-melt(haz_risk_n_tab,id.vars = c("admin_name","iso3"))
    
    haz_risk_n_tab<-recode_restructure(data=haz_risk_n_tab,
                                        crop_choices = c("generic",crop_choices),
                                        livestock_choices = livestock_choices,
                                        Scenarios = Scenarios,
                                        exposure="n",
                                        Severity=SEV,
                                        admin_level=names(levels)[i])
    haz_risk_n_tab
    
  }))
  filename<-paste0(haz_risk_n_dir,"/haz_risk_n_",SEV,".feather")
  feather::write_feather(haz_risk_n_tab,filename)
  
}



# 3) Generic Risk x Human Population #####
haz_risk_hpop_dir<-"Data/hazard_risk_hpop"
if(!dir.exists(haz_risk_hpop_dir)){
  dir.create(haz_risk_hpop_dir)
}

overwrite<-T

for(SEV in tolower(severity_classes$class[2])){
  haz_risk_files2<-haz_risk_files[grepl(SEV,haz_risk_files)]
  # Intersect human population only with generic hazards
  haz_risk_files2<-haz_risk_files2[grepl("generic",haz_risk_files2)]

    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure - Human | severity:",SEV))
    flush.console()
    
    save_name_hpop<-paste0(haz_risk_hpop_dir,"/hpop_",SEV,".tif")
    
    if((!file.exists(save_name_hpop))|overwrite==T){
      haz_risk<-terra::rast(haz_risk_files2)
      
      haz_risk_hpop_r<-haz_risk*hpop$rural
      names(haz_risk_hpop_r)<-paste0(names(haz_risk_hpop_r),"_hpop-rural")
      haz_risk_hpop_t<-haz_risk*hpop$total
      names(haz_risk_hpop_t)<-paste0(names(haz_risk_hpop_t),"_hpop-total")
      haz_risk_hpop_u<-haz_risk*hpop$urban
      names(haz_risk_hpop_u)<-paste0(names(haz_risk_hpop_u),"_hpop-urban")
      
      
      haz_risk_hpop<-c(haz_risk_hpop_r,haz_risk_hpop_t,haz_risk_hpop_u)
   
      writeRaster(haz_risk_hpop,file=save_name_hpop,overwrite=T)
      
      haz_risk_hpop_adm<-admin_extract(haz_risk_hpop,Geographies["admin0"],FUN="sum")
      st_write_parquet(obj=sf::st_as_sf(haz_risk_hpop_adm$admin0), dsn=paste0(haz_risk_hpop_dir,"/haz_risk_hpop_adm0.parquet"))
      haz_risk_hpop_adm<-admin_extract(haz_risk_hpop,Geographies["admin1"],FUN="sum")
      st_write_parquet(obj=sf::st_as_sf(haz_risk_hpop_adm$admin1), dsn=paste0(haz_risk_hpop_dir,"/haz_risk_hpop_adm1.parquet"))
      haz_risk_hpop_adm<-admin_extract(haz_risk_hpop,Geographies["admin2"],FUN="sum")
      st_write_parquet(obj=sf::st_as_sf(haz_risk_hpop_adm$admin2), dsn=paste0(haz_risk_hpop_dir,"/haz_risk_hpop_adm2.parquet"))
    }
    
  }

# 3) Generic Risk x Total Harvested Area
crop_ha_tot_sum<-sum(crop_ha_tot)

haz_risk_croparea_dir<-"Data/hazard_risk_croparea"
if(!dir.exists(haz_risk_croparea_dir)){
  dir.create(haz_risk_croparea_dir)
}

overwrite<-T

for(SEV in tolower(severity_classes$class[2])){
    haz_risk_files2<-haz_risk_files[grepl(SEV,haz_risk_files)]
    # Intersect human population only with generic hazards
    haz_risk_files2<-haz_risk_files2[grepl("generic",haz_risk_files2)]
    
    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure - Crop Area | severity:",SEV))
    flush.console()
    
    save_name_croparea<-paste0(haz_risk_croparea_dir,"/croparea_",SEV,".tif")
    
    if((!file.exists(save_name_croparea))|overwrite==T){
      haz_risk<-terra::rast(haz_risk_files2)
      
      haz_risk_croparea<-haz_risk*crop_ha_tot_sum
      names(haz_risk_croparea)<-paste0(names(haz_risk_croparea),"_croparea")
      
      writeRaster(haz_risk_croparea,file=save_name_croparea,overwrite=T)
      
      haz_risk_croparea_adm<-admin_extract(haz_risk_croparea,Geographies["admin0"],FUN="sum")
      st_write_parquet(obj=sf::st_as_sf(haz_risk_croparea_adm$admin0), dsn=paste0(haz_risk_croparea_dir,"/haz_risk_croparea_adm0.parquet"))
      haz_risk_croparea_adm<-admin_extract(haz_risk_croparea,Geographies["admin1"],FUN="sum")
      st_write_parquet(obj=sf::st_as_sf(haz_risk_croparea_adm$admin1), dsn=paste0(haz_risk_croparea_dir,"/haz_risk_croparea_adm1.parquet"))
      haz_risk_croparea_adm<-admin_extract(haz_risk_croparea,Geographies["admin2"],FUN="sum")
      st_write_parquet(obj=sf::st_as_sf(haz_risk_croparea_adm$admin2), dsn=paste0(haz_risk_croparea_dir,"/haz_risk_croparea_adm2.parquet"))
    }
    
  }

