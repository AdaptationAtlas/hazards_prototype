require(terra)
require(data.table)
require(exactextractr)
require(sf)
require(sfarrow)
require(arrow)
require(feather)
require(countrycode)

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
admin_extract<-function(data,Geographies,FUN="mean"){
  output<-list()
  if("admin0" %in% names(Geographies)){
    data0<-exactextractr::exact_extract(data,sf::st_as_sf(Geographies$admin0),fun=FUN,append_cols=c("admin_name","admin0_nam","iso3"))
    data0<-terra::merge(Geographies$admin0,data0)
    output$admin0<-data0
  }
  
  if("admin1" %in% names(Geographies)){
    data1<-exactextractr::exact_extract(data,sf::st_as_sf(Geographies$admin1),fun=FUN,append_cols=c("admin_name","admin0_nam","admin1_nam","iso3"))
    data1<-terra::merge(Geographies$admin1,data1)
    output$admin1<-data1
  }
  
  if("admin2" %in% names(Geographies)){
    data2<-exactextractr::exact_extract(data,sf::st_as_sf(Geographies$admin2),fun=FUN,append_cols=c("admin_name","admin0_nam","admin1_nam","admin2_nam","iso3"))
    data2<-terra::merge(Geographies$admin2,data2)
    output$admin2<-data2
  }
  
  return(output)
}

#### Load datasets
# 1) Geographies #####
# Load and combine geoboundaries ####
Geographies<-list(
  admin2=terra::vect("Data/geoboundaries/admin2_processed.shp"),
  admin1=terra::vect("Data/geoboundaries/admin1_processed.shp"),
  admin0=terra::vect("Data/geoboundaries/admin0_processed.shp")
)

# 1) Hazard Risk
haz_risk_dir<-paste0("Data/hazard_risk/",timeframe_choice)
haz_risk_files<-list.files(haz_risk_dir,full.names = T)

for(SEV in tolower(severity_classes$class[2:3])){
  
  haz_risk<-terra::rast(haz_risk_files[grepl(SEV,haz_risk_files)])
    
  haz_risk_adm<-admin_extract(haz_risk,Geographies,FUN="mean")
  
  st_write_parquet(obj=sf::st_as_sf(haz_risk_adm$admin0), dsn=paste0(haz_risk_dir,"/haz_risk_adm0.parquet"))
  st_write_parquet(obj=sf::st_as_sf(haz_risk_adm$admin1), dsn=paste0(haz_risk_dir,"/haz_risk_adm1.parquet"))
  st_write_parquet(obj=sf::st_as_sf(haz_risk_adm$admin2), dsn=paste0(haz_risk_dir,"/haz_risk_adm2.parquet"))
}

# 2) Exposure variables ####
# 2.1) Crops (MapSPAM) #####

mapspam_dir<-"Data/mapspam"

# 2.1.1) Value of production
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

### 2.1.2) Harvested Area
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

# 2.2) Livestock #####
# 2.2.1) Numbers
# 2.2.2) VoP
# 2.3) Population ######
base_rast<-terra::rast(list.files("Data/hazard_classified/annual",".tif",full.names = T))[[1]]

hpop<-terra::rast(list.files("Data/atlas_pop",".tif",full.names=T))
hpop<-terra::crop(hpop,Geographies)
hpop<-terra::resample(hpop,base_rast)

names(hpop)<-unlist(tail(tstrsplit(names(hpop),"_"),1))

#### Intersect Risk and Exposure ####
# 1) Crop Risk x Crop VoP/Harvested Area #####
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

calculate_totals<-T

for(SEV in tolower(severity_classes$class[2])){
  
  #### Multiply Hazard Risk by Exposure ####
  
  haz_risk_files2<-haz_risk_files[grepl(SEV,haz_risk_files)]
  
  for(i in 1:length(haz_risk_files2)){
    
    crop<-tail(unlist(tstrsplit(unlist(tstrsplit(haz_risk_files2[i],"_",keep=2)),"/")),1)
    
    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure | crop:",i,crop,"| severity:",SEV))
    flush.console()
    
    save_name_vop<-paste0(haz_risk_vop_dir,"/",crop,"_",SEV,"_vop.tif")
    
    if(!file.exists(save_name_vop)){
      haz_risk<-terra::rast(haz_risk_files2[i])
      
      # vop
      if(crop!="generic"){
        haz_risk_vop<-haz_risk*crop_vop_tot[[crop]]
      }else{
        haz_risk_vop<-haz_risk*sum(crop_vop_tot)
      }
      names(haz_risk_vop)<-paste0(names(haz_risk_vop),"-vop")
      writeRaster(haz_risk_vop,file=save_name_vop)
    
    # ha
      save_name_ha<-paste0(haz_risk_ha_dir,"/",crop,"_",SEV,"_ha.tif")

      if(crop!="generic"){
       haz_risk_ha<-haz_risk*crop_ha_tot[[crop]]
      }else{
        haz_risk_ha<-haz_risk*sum(crop_ha_tot)
      }
      
      names(haz_risk_ha)<-paste0(names(haz_risk_ha),"-ha")
      writeRaster(haz_risk_ha,file=save_name_ha)
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
  
  }
  
  #### Extract Risk x Exposure by Geography  ####
  
  # Display progress
  cat('\r                                                                                                                     ')
  cat('\r',paste("Risk x Exposure - VoP admin extraction| severity:",SEV))
  flush.console()
  
  haz_risk_vop_files<-list.files(haz_risk_vop_dir,".tif",full.names = T)
  haz_risk_vop_files<-haz_risk_vop_files[grepl(SEV,haz_risk_vop_files)]
  
  haz_risk_vop<-terra::rast(haz_risk_vop_files)
  haz_risk_vop_adm<-admin_extract(haz_risk_vop,Geographies,FUN="sum")
  
  sfarrow::st_write_parquet(obj=sf::st_as_sf(haz_risk_vop_adm$admin0), dsn=paste0(haz_risk_vop_dir,"/haz_risk_vop_adm0.parquet"))
  sfarrow::st_write_parquet(obj=sf::st_as_sf(haz_risk_vop_adm$admin1), dsn=paste0(haz_risk_vop_dir,"/haz_risk_vop_adm1.parquet"))
  sfarrow::st_write_parquet(obj=sf::st_as_sf(haz_risk_vop_adm$admin2), dsn=paste0(haz_risk_vop_dir,"/haz_risk_vop_adm2.parquet"))
 
  # Display progress
  cat('\r                                                                                                                     ')
  cat('\r',paste("Risk x Exposure - Harvested Area admin extraction| severity:",SEV))
  flush.console()
  
  haz_risk_ha_files<-list.files(haz_risk_ha_dir,".tif",full.names = T)
  haz_risk_ha_files<-haz_risk_ha_files[grepl(SEV,haz_risk_ha_files)]
  
  haz_risk_ha<-terra::rast(haz_risk_ha_files)
  haz_risk_ha_adm<-admin_extract(haz_risk_ha,Geographies,FUN="sum")
  
  sfarrow::st_write_parquet(obj=sf::st_as_sf(haz_risk_ha_adm$admin0), dsn=paste0(haz_risk_ha_dir,"/haz_risk_ha_adm0.parquet"))
  sfarrow::st_write_parquet(obj=sf::st_as_sf(haz_risk_ha_adm$admin1), dsn=paste0(haz_risk_ha_dir,"/haz_risk_ha_adm1.parquet"))
  sfarrow::st_write_parquet(obj=sf::st_as_sf(haz_risk_ha_adm$admin2), dsn=paste0(haz_risk_ha_dir,"/haz_risk_ha_adm2.parquet"))

  #### Restructure Extracted Data ####
  levels<-c("admin0","admin1","admin2")
  
  haz_risk_vop_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
    level<-levels[i]
    # Vop
    haz_risk_vop_tab<-data.table(data.frame(haz_risk_vop_adm[[level]]))
    N<-colnames(haz_risk_vop_tab)[-grep(c("admin0_nam|admin1_nam|admin2_nam"),colnames(haz_risk_vop_tab))]
    haz_risk_vop_tab<-haz_risk_vop_tab[,..N]
    haz_risk_vop_tab<-melt(haz_risk_vop_tab,id.vars = "admin_name")
    
    for(crop in crop_choices){
      haz_risk_vop_tab[,variable:=gsub(gsub(" ",".",crop,fixed=T),crop,variable)]
    }
    
    haz_risk_vop_tab[,scenario:=unlist(tstrsplit(variable[1],"[.]",keep=2)),by=variable
    ][,timeframe:=unlist(tstrsplit(variable[1],"[.]",keep=3)),by=variable
    ][,hazard:=unlist(tstrsplit(variable[1],"[.]",keep=4)),by=variable
    ][,crop:=unlist(tstrsplit(variable[1],"[.]",keep=5)),by=variable
    ][,severity:=unlist(tstrsplit(variable[1],"[.]",keep=6)),by=variable
    ][,exposure:=unlist(tstrsplit(variable[1],"[.]",keep=7)),by=variable
    ][,variable:=NULL]
    
    haz_risk_vop_tab$admin_level<-level
    haz_risk_vop_tab$severity<-SEV
    haz_risk_vop_tab
  }))
  
  haz_risk_ha_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
    level<-levels[i]
    
    # Harvested area
    haz_risk_ha_tab<-data.table(data.frame(haz_risk_ha_adm[[level]]))
    N<-colnames(haz_risk_ha_tab)[-grep(c("admin0_nam|admin1_nam|admin2_nam"),colnames(haz_risk_ha_tab))]
    haz_risk_ha_tab<-haz_risk_ha_tab[,..N]
    haz_risk_ha_tab<-melt(haz_risk_ha_tab,id.vars = "admin_name")
    
    for(crop in crop_choices){
      haz_risk_ha_tab[,variable:=gsub(gsub(" ",".",crop,fixed=T),crop,variable)]
    }
    
    haz_risk_ha_tab[,scenario:=unlist(tstrsplit(variable[1],"[.]",keep=2)),by=variable
    ][,timeframe:=unlist(tstrsplit(variable[1],"[.]",keep=3)),by=variable
    ][,hazard:=unlist(tstrsplit(variable[1],"[.]",keep=4)),by=variable
    ][,crop:=unlist(tstrsplit(variable[1],"[.]",keep=5)),by=variable
    ][,severity:=unlist(tstrsplit(variable[1],"[.]",keep=6)),by=variable
    ][,exposure:=unlist(tstrsplit(variable[1],"[.]",keep=7)),by=variable
    ][,variable:=NULL]
    
    haz_risk_ha_tab$admin_level<-level
    haz_risk_ha_tab$severity<-SEV
    haz_risk_ha_tab
    
  }))
  
  # Save as feather objects
  filename<-paste0(haz_risk_vop_dir,"/haz_risk_vop_",SEV,".feather")
  feather::write_feather(haz_risk_vop_tab,filename)
  
  filename<-paste0(haz_risk_ha_dir,"/haz_risk_ha_",SEV,".feather")
  feather::write_feather(haz_risk_ha_tab,filename)
  
}

haz_risk_vop_tab[,unique(hazard)]
haz_risk_vop_tab[,unique(admin_level)]

# 2) Generic Risk x Human Population #####
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
      
      haz_risk_hpop_adm<-admin_extract(haz_risk_hpop,Geographies,FUN="sum")
      
      st_write_parquet(obj=sf::st_as_sf(haz_risk_hpop_adm$admin0), dsn=paste0(haz_risk_hpop_dir,"/haz_risk_hpop_adm0.parquet"))
      st_write_parquet(obj=sf::st_as_sf(haz_risk_hpop_adm$admin1), dsn=paste0(haz_risk_hpop_dir,"/haz_risk_hpop_adm1.parquet"))
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
      
      haz_risk_croparea_adm<-admin_extract(haz_risk_croparea,Geographies,FUN="sum")
      
      st_write_parquet(obj=sf::st_as_sf(haz_risk_croparea_adm$admin0), dsn=paste0(haz_risk_croparea_dir,"/haz_risk_croparea_adm0.parquet"))
      st_write_parquet(obj=sf::st_as_sf(haz_risk_croparea_adm$admin1), dsn=paste0(haz_risk_croparea_dir,"/haz_risk_croparea_adm1.parquet"))
      st_write_parquet(obj=sf::st_as_sf(haz_risk_croparea_adm$admin2), dsn=paste0(haz_risk_croparea_dir,"/haz_risk_croparea_adm2.parquet"))
    }
    
  }

# 3) Livestock Risk x Livestock VoP/Numbers ####