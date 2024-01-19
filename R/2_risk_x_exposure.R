#Sys.setenv(PATH = paste("C:/OSGeo4W/bin", Sys.getenv("PATH"), sep = ";"))
#Sys.setenv(PROJ_LIB = "C:/OSGeo4W64/share/proj")
#Sys.setenv(GDAL_DATA = "C:/OSGeo4W/apps/gdal/share")

#install.packages("sf", type = "source")
#install.packages("terra", type = "source")

#sf::sf_extSoftVersion()["GDAL"]
#terra::gdal()

#Sys.getenv("GDAL_DATA")
#Sys.getenv("PATH")

require(terra)
require(data.table)
require(exactextractr)
require(sf)
require(sfarrow)
require(arrow)
require(feather)
require(doFuture)
require(stringr)

# Set up workspace ####
# Increase GDAL cache size
terra::gdalCache(60000)

# Set scenarios and time frames to analyse
Scenarios<-c("ssp245","ssp585")
Times<-c("2021_2040","2041_2060")

# admin levels
levels<-c(admin0="adm0",admin1="adm1",admin2="adm2")

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
crop_choices<-unique(c(ms_codes[,sort(Fullname)],haz_class[,unique(crop)]))

# Load base raster to resample to 
base_rast<-terra::rast(list.files("Data/hazard_timeseries_class/annual",".tif",full.names = T))[[1]]

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

#### Load datasets (non hazards)
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
  
  # 2.1.1.2) Extraction of values by admin areas
  file<-paste0(exposure_dir,"/crop_vop_adm_sum.feather")
  if(!file.exists(file)){
    crop_vop_tot_adm<-admin_extract(crop_vop_tot,Geographies,FUN="sum")
    
    crop_vop_tot_adm_sum<-rbindlist(lapply(1:length(levels),FUN=function(i){
      level<-levels[i]
      print(level)
      
      
      data<-data.table(data.frame(crop_vop_tot_adm[[names(level)]]))
      N<-colnames(data)[-grep(c("admin0_nam|admin1_nam|admin2_nam|geometry"),colnames(data))]
      data<-data[,..N]
      data<-melt(data,id.vars = c("admin_name","iso3"))
      
      data[,crop:=gsub("sum.","",variable,fixed=T)][,exposure:="vop"][,admin_level:=names(levels)[i]][,variable:=NULL]
      
      data
      
    }))
    crop_vop_tot_adm_sum[,crop:=gsub("."," ",crop,fixed=T)]
    
    feather::write_feather(crop_vop_tot_adm_sum,file)
  }
  
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
  
  # 2.1.2.1) Extraction of values by admin areas
  file<-paste0(exposure_dir,"/crop_ha_adm_sum.feather")
  if(!file.exists(file)){
    crop_ha_tot_adm<-admin_extract(crop_ha_tot,Geographies,FUN="sum")
    
    crop_ha_tot_adm_sum<-rbindlist(lapply(1:length(levels),FUN=function(i){
      level<-levels[i]
      print(level)
      
      data<-data.table(data.frame(crop_ha_tot_adm[[names(level)]]))
      N<-colnames(data)[-grep(c("admin0_nam|admin1_nam|admin2_nam|geometry"),colnames(data))]
      data<-data[,..N]
      data<-melt(data,id.vars = c("admin_name","iso3"))
      
      data[,crop:=gsub("sum.","",variable,fixed=T)][,exposure:="ha"][,admin_level:=names(levels)[i]][,variable:=NULL]
      
      data
      
    }))
    
    crop_ha_tot_adm_sum[,crop:=gsub("."," ",crop,fixed=T)]
    
    feather::write_feather(crop_ha_tot_adm_sum,file)
  }
    # 2.1.3) Create Crop Masks ######
  commodity_mask_dir<-"Data/commodity_masks"
  if(!dir.exists(commodity_mask_dir)){
    dir.create(commodity_mask_dir)
  }
  
  # Need to use mapspam physical area
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
    
    # Split mask by highland vs tropical areas
    
    # Load highland mask
    highlands<-terra::rast("Data/afr_highlands/afr-highlands.asc")
    highlands<-terra::resample(highlands,base_rast,method="near")

    
    livestock_mask_high<-livestock_mask*highlands
    names(livestock_mask_high)<-paste0( names(livestock_mask_high),"_highland")
    
    lowlands<-classify(highlands,data.frame(from=c(0,1),to=c(1,0)))
    livestock_mask_low<-livestock_mask*lowlands
    names(livestock_mask_low)<-paste0( names(livestock_mask_low),"_tropical")
    
    livestock_mask<-c(livestock_mask_high,livestock_mask_low)
    
    terra::writeRaster(livestock_mask,filename=paste0(commodity_mask_dir,"/livestock_masks.tif"),overwrite=T)
    
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
    
    # Pull out sheep and goat proportions for use in vop calculations before highland/tropical splitting
    sheep_prop<-livestock_no$sheep/(livestock_no$goats +livestock_no$sheep)
    goat_prop<-livestock_no$goats/(livestock_no$goats +livestock_no$sheep)
    
    # Split livestock between highland and tropical
    # Reorder cols to match mask
    order_n<-sapply(names(livestock_no),FUN=function(X){grep(X,names(livestock_mask_high))})
    livestock_no_high<-livestock_no[[order_n]]
    livestock_no_high<-livestock_no_high*livestock_mask_high
    
    order_n<-sapply(names(livestock_no),FUN=function(X){grep(X,names(livestock_mask_low))})
    livestock_no_low<-livestock_no[[order_n]]
    livestock_no_low<-livestock_no*livestock_mask_low
    
    
    names(livestock_no_high)<-names(livestock_mask_high)
    names(livestock_no_low)<-names(livestock_mask_low)
    
    livestock_no<-c(livestock_no_low,livestock_no_high)
    
    terra::writeRaster(livestock_no,filename = paste0(exposure_dir,"/livestock_no.tif"),overwrite=T)
  
    # 2.2.1.1) Extraction of values by admin areas
    file<-paste0(exposure_dir,"/livestock_no_adm_sum.feather")
    if(!file.exists(file)){
      livestock_no_tot_adm<-admin_extract(livestock_no,Geographies,FUN="sum")
      
      livestock_no_tot_adm<-rbindlist(lapply(1:length(levels),FUN=function(i){
        level<-levels[i]
        print(level)
        
        data<-data.table(data.frame(livestock_no_tot_adm[[names(level)]]))
        N<-colnames(data)[-grep(c("admin0_nam|admin1_nam|admin2_nam|geometry"),colnames(data))]
        data<-data[,..N]
        data<-melt(data,id.vars = c("admin_name","iso3"))
        
        data[,crop:=gsub("sum.","",variable,fixed=T)][,exposure:="number"][,admin_level:=names(levels)[i]][,variable:=NULL]
        
        data
        
      }))
      
      feather::write_feather(livestock_no_tot_adm,file)
    }
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
    livestock_vop$sheep<-livestock_vop$sheep_goat*sheep_prop
    livestock_vop$goats<-livestock_vop$sheep_goat*goat_prop
    livestock_vop$sheep_goat<-NULL
    
    # Split vop by highland vs lowland
    
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
    
    terra::writeRaster(livestock_vop,filename = paste0(exposure_dir,"/livestock_vop.tif"),overwrite=T)
    
    # 2.2.2.1) Extraction of values by admin areas
    file<-paste0(exposure_dir,"/livestock_vop_adm_sum.feather")
    if(!file.exists(file)){
    livestock_vop_tot_adm<-admin_extract(livestock_vop,Geographies,FUN="sum")
    
    livestock_vop_tot_adm<-rbindlist(lapply(1:length(levels),FUN=function(i){
      level<-levels[i]
      print(level)
      
      data<-data.table(data.frame(livestock_vop_tot_adm[[names(level)]]))
      N<-colnames(data)[-grep(c("admin0_nam|admin1_nam|admin2_nam|geometry"),colnames(data))]
      data<-data[,..N]
      data<-melt(data,id.vars = c("admin_name","iso3"))
      
      data[,crop:=gsub("sum.","",variable,fixed=T)][,exposure:="vop"][,admin_level:=names(levels)[i]][,variable:=NULL]
      
      data
      
    }))
    
    feather::write_feather(livestock_vop_tot_adm,file)
    }
    
  # 2.3) Combine exposure totals by admin areas ####
    file<-paste0(exposure_dir,"/exposure_adm_sum.feather")
    if(!file.exists(file)){
      exposure_adm_sum_tab<-rbind(
        crop_vop_tot_adm_sum,
        crop_ha_tot_adm_sum,
        livestock_vop_tot_adm,
        livestock_no_tot_adm
      )
      feather::write_feather(exposure_adm_sum_tab,file)
      }

  # 2.4) Population ######
hpop<-terra::rast(list.files("Data/atlas_pop",".tif",full.names=T))
hpop<-terra::crop(hpop,Geographies)

# Convert hpop to density
hpop<-hpop/cellSize(hpop,unit="ha")

# Resample to base raster
hpop<-terra::resample(hpop,base_rast)

# Convert back to number per cell
hpop<-hpop*cellSize(hpop,unit="ha")

names(hpop)<-unlist(tail(tstrsplit(names(hpop),"_"),1))

terra::writeRaster(hpop,filename = paste0(exposure_dir,"/hpop.tif"),overwrite=T)

# 2.3.1) Extraction of values by admin areas
hpop_tot_adm<-admin_extract(hpop,Geographies,FUN="sum")

hpop_tot_adm<-rbindlist(lapply(1:length(levels),FUN=function(i){
  level<-levels[i]
  print(level)
  
  data<-data.table(data.frame(hpop_tot_adm[[names(level)]]))
  N<-colnames(data)[-grep(c("admin0_nam|admin1_nam|admin2_nam|geometry"),colnames(data))]
  data<-data[,..N]
  data<-melt(data,id.vars = c("admin_name","iso3"))
  
  data[,variable:=gsub("sum.","",variable,fixed=T)][,exposure:="number"][,admin_level:=names(levels)[i]]
  
  data
  
}))

feather::write_feather(hpop_tot_adm,paste0(exposure_dir,"/hpop_adm_sum.feather"))


terra::writeVector(hpop_tot_adm$admin0, filename =paste0(exposure_dir,"/hpop_adm_sum_adm0.parquet"),filetype="Parquet")




#### Intersect Risk and Exposure ####
# 1) Hazard Risk ####
haz_risk_dir<-paste0("Data/hazard_risk/",timeframe_choice)
haz_risk_files<-list.files(haz_risk_dir,".tif",full.names = T)
haz_risk_files<-haz_risk_files[!grepl("_any.tif",haz_risk_files)]

overwrite<-F

# max_cells_in_memory<-14452723000 # It should be faster preloading the entire working to memory, this can be done by increase this argument and adding the argument max_cells_in_memory=max_cells_in_memory to the admin_extract function

for(SEV in tolower(severity_classes$class[2])){
  print(SEV)
  if((!file.exists(paste0(haz_risk_dir,"/haz_risk_adm0_",SEV,".parquet")))|overwrite==T){
    haz_risk<-terra::rast(haz_risk_files[grepl(SEV,haz_risk_files)])
    
        # Display progress
        cat('\r                                                                                                                                                 ')
        cat('\r',paste("Adm0 - Severity Class:",SEV))
        flush.console()
        
        haz_risk_adm<-admin_extract(haz_risk,Geographies["admin0"],FUN="mean")
        st_write_parquet(obj=sf::st_as_sf(haz_risk_adm$admin0), dsn=paste0(haz_risk_dir,"/haz_risk_adm0_",SEV,".parquet"))
        # terra::writeVector(haz_risk_adm$admin0, filename =paste0(haz_risk_dir,"/haz_risk_adm0_",SEV,".parquet"),filetype="Parquet")
        gc()
        
        # Display progress
        cat('\r                                                                                                                                                 ')
        cat('\r',paste("Adm1 - Severity Class:",SEV))
        flush.console()
        
        haz_risk_adm<-admin_extract(haz_risk,Geographies["admin1"],FUN="mean")
        st_write_parquet(obj=sf::st_as_sf(haz_risk_adm$admin1), dsn=paste0(haz_risk_dir,"/haz_risk_adm1_",SEV,".parquet"))
        #terra::writeVector(haz_risk_adm$admin1, filename=paste0(haz_risk_dir,"/haz_risk_adm1_",SEV,".parquet"),filetype="Parquet")
        
        gc()
        
        # Display progress
        cat('\r                                                                                                                                                 ')
        cat('\r',paste("Adm2 - Severity Class:",SEV))
        flush.console()
        
        haz_risk_adm<-admin_extract(haz_risk,Geographies["admin2"],FUN="mean")
        st_write_parquet(obj=sf::st_as_sf(haz_risk_adm$admin2), dsn=paste0(haz_risk_dir,"/haz_risk_adm2_",SEV,".parquet"))
        #terra::writeVector(haz_risk_adm$admin2, filename=paste0(haz_risk_dir,"/haz_risk_adm2_",SEV,".parquet"),filetype="Parquet")
        gc()
    }

}

# 1.1) Hazard Total Risk ####

haz_risk_files_any<-list.files(haz_risk_dir,"_any.tif",full.names = T)

for(SEV in severity_classes$class[2]){
  haz_risk_files_any2<-grep(paste0("_",SEV,"_"),haz_risk_files_any,value=T,ignore.case = T)
  data<-terra::rast(haz_risk_files_any2)
  
  file0<-paste0(haz_risk_dir,"/haz_risk_adm0_",SEV,"_any.parquet")
  file1<-paste0(haz_risk_dir,"/haz_risk_adm1_",SEV,"_any.parquet")
  file2<-paste0(haz_risk_dir,"/haz_risk_adm2_",SEV,"_any.parquet")
  
  if(!file.exists(file0)){
    # Display progress
    cat('\r                                                                                                                                                 ')
    cat('\r',paste("Adm0 - Severity Class:",SEV))
    flush.console()
    
    data_ex<-admin_extract(data,Geographies["admin0"],FUN="mean")
    st_write_parquet(obj=sf::st_as_sf(data_ex$admin0), dsn=file0)
    # terra::writeVector(haz_risk_adm$admin0, filename =file0,filetype="Parquet")
    gc()
  }
  
  if(!file.exists(file1)){
    # Display progress
    cat('\r                                                                                                                                                 ')
    cat('\r',paste("Adm1 - Severity Class:",SEV))
    flush.console()
    
    data_ex<-admin_extract(data,Geographies["admin1"],FUN="mean")
    st_write_parquet(obj=sf::st_as_sf(data_ex$admin1), dsn=file1)
    #terra::writeVector(data_ex$admin1, filename=file1,filetype="Parquet")
    
    gc()
  }
  
  if(!file.exists(file2)){
    # Display progress
    cat('\r                                                                                                                                                 ')
    cat('\r',paste("Adm2 - Severity Class:",SEV))
    flush.console()
    
    data_ex<-admin_extract(data,Geographies["admin2"],FUN="mean")
    st_write_parquet(obj=sf::st_as_sf(data_ex$admin2), dsn=file2)
    #terra::writeVector(data_ex$admin2, filename=file2,filetype="Parquet")
    gc()
  }
  
}


# Check resulting file
X<-st_read_parquet(paste0(haz_risk_dir,"/haz_risk_adm0_",SEV,"_any.parquet"))
grep("THI",names(X),value=T)

# 2) Apply Crop Mask to Classified Hazard Risk ####

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

# 3) Hazard Mean ####

save_dir_means<-paste0("Data/hazard_mean/",timeframe_choice)
files<-list.files(save_dir_means,".tif",full.names = T)

haz_means<-terra::rast(files[!grepl("change",files)])
haz_means_change<-terra::rast(files[grepl("change",files)])

# extract mean hazards
haz_means_adm<-admin_extract(haz_means,Geographies)
st_write_parquet(obj=sf::st_as_sf(haz_means_adm$admin0), dsn=paste0(save_dir_means,"/haz_means_adm0.parquet"))
st_write_parquet(obj=sf::st_as_sf(haz_means_adm$admin1), dsn=paste0(save_dir_means,"/haz_means_adm1.parquet"))
st_write_parquet(obj=sf::st_as_sf(haz_means_adm$admin2), dsn=paste0(save_dir_means,"/haz_means_adm2.parquet"))

filename<-paste0(save_dir_means,"/haz_means.feather")

# Extract data from vector files and restructure into tabular form
haz_means_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
  level<-levels[i]
  print(level)
  
  haz_means_tab<-data.table(data.frame(sfarrow::st_read_parquet(paste0(save_dir_means,"/haz_means_",levels[i],".parquet"))))
  N<-colnames(haz_means_tab)[-grep(c("admin0_nam|admin1_nam|admin2_nam|geometry"),colnames(haz_means_tab))]
  haz_means_tab<-haz_means_tab[,..N]
  haz_means_tab<-melt(haz_means_tab,id.vars = c("admin_name","iso3"))
  
  haz_means_tab[,variable:=gsub("5.2","5|2",variable,fixed = T)
  ][,variable:=gsub("mean.","",variable,fixed = T)
  ][,variable:=gsub("ric.his","ric|his",variable,fixed = T)
  ][,variable:=gsub("c_","c|",variable,fixed = T)
  ][,variable:=gsub("_mean_mean","mean_mean",variable,fixed = T)
  ][,variable:=gsub("_max_mean","max_mean",variable,fixed = T)
  ][,variable:=gsub("_mean","",variable,fixed = T)
  ][,variable:=gsub("0_","0|",variable,fixed = T)
  ][,variable:=gsub("mean","_mean",variable,fixed = T)
  ][,variable:=gsub("max","_max",variable,fixed = T)
  ][,scenario:=unlist(tstrsplit(variable,"[|]",keep=1))
  ][,timeframe:=unlist(tstrsplit(variable,"[|]",keep=2))
  ][,hazard:=unlist(tstrsplit(variable,"[|]",keep=3))
  ][,variable:=NULL
  ][,admin_level:=names(levels)[i]]
  
  
  
  haz_means_tab
}))

# Save mean values as feather object
feather::write_feather(haz_means_tab,filename)

# extract change in mean hazards
haz_means_change_adm<-admin_extract(haz_means_change,Geographies)

st_write_parquet(obj=sf::st_as_sf(haz_means_adm$admin0), dsn=paste0(save_dir_means,"/haz_means_change_adm0.parquet"))
st_write_parquet(obj=sf::st_as_sf(haz_means_adm$admin1), dsn=paste0(save_dir_means,"/haz_means_change_adm1.parquet"))
st_write_parquet(obj=sf::st_as_sf(haz_means_adm$admin2), dsn=paste0(save_dir_means,"/haz_means_change_adm2.parquet"))

# 4) Hazard Trend ####
haz_timeseries_dir<-paste0("Data/hazard_timeseries/",timeframe_choice)
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
filename<-paste0(haz_timeseries_dir,"/haz_timeseries.feather")
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
feather::write_feather(haz_timeseries_tab,filename)


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
filename<-paste0(haz_timeseries_dir,"/haz_timeseries_sd.feather")
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
feather::write_feather(haz_timeseries_sd_tab,filename)

# 5) Commodity Specific Hazard Risk x Crop or Livestock VoP & Crop Harvested Area ####
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

# List livestock names
livestock_choices<-list.files(haz_risk_dir,".tif")
livestock_choices<-grep("highland|tropical",livestock_choices,value = T)
livestock_choices<-unique(unlist(tstrsplit(livestock_choices,"_extr|_seve",keep=1)))

# Crop choices only 
crop_choices<-crop_choices[!grepl("_tropical|_highland",crop_choices)]

# Solo and interactions combined into a single file (not any hazard) ####
haz_risk_files<-list.files(haz_risk_dir,".tif",full.names = T)
haz_risk_files<-haz_risk_files[!grepl("_any.tif",haz_risk_files)]
overwrite<-F
do_ha<-T
do_n<-T

for(SEV in tolower(severity_classes$class[2])){
  #### Multiply Hazard Risk by Exposure ####
  
  haz_risk_files2<-haz_risk_files[grepl(SEV,haz_risk_files) & !grepl("_int",haz_risk_files)]

  #registerDoFuture()
  #plan("multisession", workers = 10)
  #foreach(i = 1:length(haz_risk_files2)) %dopar% {
    
    for(i in 1:length(haz_risk_files2)){
    
    crop<-gsub(paste0("_",SEV,".tif"),"",tail(tstrsplit(haz_risk_files2[i],"/"),1))
    
    # Display progress
    cat('\r                                                                                                                           ')
    cat('\r',paste("Risk x Exposure | crop:",i,"/",length(haz_risk_files2)," ",crop,"| severity:",SEV))
    flush.console()
    
    save_name_vop<-paste0(haz_risk_vop_dir,"/",crop,"_",SEV,"_vop.tif")
    
    files<-haz_risk_files2[i]
    files<-c(files,gsub(".tif","_int.tif",files))
    haz_risk<-terra::rast(files)
    
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
      writeRaster(haz_risk_vop,file=save_name_vop,overwrite=T)
    }
    
    # ha
    if(do_ha==T){
      if(crop %in% c("generic",crop_choices)){
      save_name_ha<-paste0(haz_risk_ha_dir,"/",crop,"_",SEV,"_ha.tif")

      if(!file.exists(save_name_ha)|overwrite==T){
        if(crop!="generic"){
         haz_risk_ha<-haz_risk*crop_ha_tot[[crop]]
        }else{
          haz_risk_ha<-haz_risk*sum(crop_ha_tot)
        }
        
        names(haz_risk_ha)<-paste0(names(haz_risk_ha),"-ha")
        writeRaster(haz_risk_ha,file=save_name_ha,overwrite=T)
        }
      }
    }
      
    # numbers
    if(do_n==T){
      if(crop %in% c("generic",livestock_choices)){
        save_name_n<-paste0(haz_risk_n_dir,"/",crop,"_",SEV,"_n.tif")
        
        if(!file.exists(save_name_n)|overwrite==T){
          if(crop!="generic"){
            haz_risk_n<-haz_risk*livestock_no[[crop]]
          }else{
            haz_risk_n<-haz_risk*sum(livestock_no[[(c("total_tropical","total_highland"))]],na.rm=T)
          }
          
          names(haz_risk_n)<-paste0(names(haz_risk_n),"-n")
          writeRaster(haz_risk_n,file=save_name_n,overwrite=T)
      }
      }
    }
    
  }
  
 # plan(sequential)
  
  #### Extract Risk x Exposure by Geography  ####
  
  # VoP #####
  haz_risk_vop_files<-list.files(haz_risk_vop_dir,".tif",full.names = T)
  haz_risk_vop_files<-haz_risk_vop_files[!grepl("_any.tif",haz_risk_vop_files)]
  haz_risk_vop_files<-haz_risk_vop_files[grepl(SEV,haz_risk_vop_files)]
  data<-terra::rast(haz_risk_vop_files)
  
  file0<-paste0(haz_risk_vop_dir,"/haz_risk_vop_adm0_",SEV,".parquet")
  file1<-gsub("_adm0_","_adm1_",file0)
  file2<-gsub("_adm0_","_adm2_",file0)
  
  if(!file.exists(file0)|overwrite==T){
    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure - VoP admin extraction - adm0| severity:",SEV))
    flush.console()
    
    data_ex<-admin_extract(data,Geographies["admin0"],FUN="sum")
    sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin0), dsn=file0)
    rm(data_ex)
    gc()
  }
  if(!file.exists(file1)|overwrite==T){
    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure - VoP admin extraction - adm1| severity:",SEV))
    flush.console()
    
    data_ex<-admin_extract(data,Geographies["admin1"],FUN="sum")
    sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin1), dsn=file1)
    rm(data_ex)
    gc()
  }
  if(!file.exists(file2)|overwrite==T){
    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure - VoP admin extraction - adm2| severity:",SEV))
    flush.console()
    
    data_ex<-admin_extract(data,Geographies["admin2"],FUN="sum")
    sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin2), dsn=file2)
    rm(data_ex)
    gc()
  }
  
  # Harvested Area #####
  if(do_ha==T){
    haz_risk_ha_files<-list.files(haz_risk_ha_dir,".tif",full.names = T)
    haz_risk_ha_files<-haz_risk_ha_files[!grepl("_any.tif",haz_risk_ha_files)]
    haz_risk_ha_files<-haz_risk_ha_files[grepl(SEV,haz_risk_ha_files)]
    data<-terra::rast(haz_risk_ha_files)
    
    file0<-paste0(haz_risk_ha_dir,"/haz_risk_ha_adm0_",SEV,".parquet")
    file1<-gsub("_adm0_","_adm1_",file0)
    file2<-gsub("_adm0_","_adm2_",file0)
    
    if(!file.exists(file0)|overwrite==T){
      # Display progress
      cat('\r                                                                                                                     ')
      cat('\r',paste("Risk x Exposure - HA admin extraction - adm0| severity:",SEV))
      flush.console()
      
      data_ex<-admin_extract(data,Geographies["admin0"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin0), dsn=file0)
      rm(data_ex)
      gc()
    }
    if(!file.exists(file1)|overwrite==T){
      # Display progress
      cat('\r                                                                                                                     ')
      cat('\r',paste("Risk x Exposure - HA admin extraction - adm1| severity:",SEV))
      flush.console()
      
      data_ex<-admin_extract(data,Geographies["admin1"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin1), dsn=file1)
      rm(data_ex)
      gc()
    }
    if(!file.exists(file2)|overwrite==T){
      # Display progress
      cat('\r                                                                                                                     ')
      cat('\r',paste("Risk x Exposure - HA admin extraction - adm2| severity:",SEV))
      flush.console()
      
      data_ex<-admin_extract(data,Geographies["admin2"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin2), dsn=file2)
      rm(data_ex)
      gc()
    }
  }
  
  # Numbers #####
  if(do_n==T){
    haz_risk_n_files<-list.files(haz_risk_n_dir,".tif",full.names = T)
    haz_risk_n_files<-haz_risk_n_files[!grepl("_any.tif",haz_risk_n_files)]
    haz_risk_n_files<-haz_risk_n_files[grepl(SEV,haz_risk_n_files)]
    data<-terra::rast(haz_risk_n_files)
    
    file0<-paste0(haz_risk_n_dir,"/haz_risk_n_adm0_",SEV,".parquet")
    file1<-gsub("_adm0_","_adm1_",file0)
    file2<-gsub("_adm0_","_adm2_",file0)
    
    if(!file.exists(file0)|overwrite==T){
      # Display progress
      cat('\r                                                                                                                     ')
      cat('\r',paste("Risk x Exposure - N admin extraction - adm0| severity:",SEV))
      flush.console()
      
      data_ex<-admin_extract(data,Geographies["admin0"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin0), dsn=file0)
      rm(data_ex)
      gc()
    }
    if(!file.exists(file1)|overwrite==T){
      # Display progress
      cat('\r                                                                                                                     ')
      cat('\r',paste("Risk x Exposure - N admin extraction - adm1| severity:",SEV))
      flush.console()
      
      data_ex<-admin_extract(data,Geographies["admin1"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin1), dsn=file1)
      rm(data_ex)
      gc()
    }
    if(!file.exists(file2)|overwrite==T){
      # Display progress
      cat('\r                                                                                                                     ')
      cat('\r',paste("Risk x Exposure - N admin extraction - adm2| severity:",SEV))
      flush.console()
      
      data_ex<-admin_extract(data,Geographies["admin2"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin2), dsn=file2)
      rm(data_ex)
      gc()
    }
  }
  
  #### Restructure Extracted Data ####
  
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
  
  # Vop
  filename<-paste0(haz_risk_vop_dir,"/haz_risk_vop_",SEV,".feather")
  
  if(!file.exists(filename)){
    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure - VoP restructuring data| severity: ",SEV))
    flush.console()
    
    haz_risk_vop_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
      level<-levels[i]
      print(level)
      # Vop
      haz_risk_vop_tab<-data.table(data.frame(sfarrow::st_read_parquet(paste0(haz_risk_vop_dir,"/haz_risk_vop_",levels[i],"_",SEV,".parquet"))))
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
    feather::write_feather(haz_risk_vop_tab,filename)
  }
  
  # Harvested area
  if(do_ha==T){
    filename<-paste0(haz_risk_ha_dir,"/haz_risk_ha_",SEV,".feather")
    
    if(!file.exists(filename)){
      # Display progress
      cat('\r                                                                                                                     ')
      cat('\r',paste("Risk x Exposure - Harvested area restructuring data| severity:",SEV))
      flush.console()
      
      haz_risk_ha_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
      level<-levels[i]
      
      # Harvested area
      haz_risk_ha_tab<-data.table(data.frame(sfarrow::st_read_parquet(paste0(haz_risk_ha_dir,"/haz_risk_ha_",levels[i],"_",SEV,".parquet"))))
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
      feather::write_feather(haz_risk_ha_tab,filename)
    }
  }
  
  # Numbers
  if(do_n==T){
    filename<-paste0(haz_risk_n_dir,"/haz_risk_n_",SEV,".feather")
    
    if(!file.exists(filename)){
      haz_risk_n_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
        # Display progress
        cat('\r                                                                                                                     ')
        cat('\r',paste("Risk x Exposure - Number restructuring data| severity:",SEV))
        flush.console()
        
        level<-levels[i]
        
        haz_risk_n_tab<-data.table(data.frame(sfarrow::st_read_parquet(paste0(haz_risk_n_dir,"/haz_risk_n_",levels[i],"_",SEV,".parquet"))))
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
      feather::write_feather(haz_risk_n_tab,filename)
    }
  }
  
}

# Any hazard only ####
haz_risk_files<-list.files(haz_risk_dir,"_any.tif",full.names = T)
overwrite<-F
do_ha<-T
do_n<-T

for(SEV in tolower(severity_classes$class[2])){
  
  #### Multiply Hazard Risk by Exposure ####
  
  haz_risk_files2<-haz_risk_files[grepl(SEV,haz_risk_files)]
  
  #registerDoFuture()
  #plan("multisession", workers = 10)
  
  #  loop starts here
  #foreach(i = 1:length(haz_risk_files2)) %dopar% {
  
  for(i in 1:length(haz_risk_files2)){
    
    
    crop<-gsub(paste0("_",SEV,"_any.tif"),"",tail(tstrsplit(haz_risk_files2[i],"/"),1))
    
    # Display progress
    cat('\r                                                                                                                           ')
    cat('\r',paste("Risk x Exposure | crop:",i,"/",length(haz_risk_files2)," ",crop,"| severity:",SEV))
    flush.console()
    
    # Load crop hazard raster
    haz_risk<-terra::rast(haz_risk_files2[i])
    
    # multiply risk by vop
    save_name_vop<-paste0(haz_risk_vop_dir,"/",crop,"_",SEV,"_vop_any.tif")
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
      writeRaster(haz_risk_vop,file=save_name_vop,overwrite=T)
    }
    
    # multiply risk by ha
    if(do_ha==T){
      save_name_ha<-paste0(haz_risk_ha_dir,"/",crop,"_",SEV,"_ha_any.tif")
      if(crop %in% c("generic",crop_choices)){
        
        if(!file.exists(save_name_ha)|overwrite==T){
          if(crop!="generic"){
            haz_risk_ha<-haz_risk*crop_ha_tot[[crop]]
          }else{
            haz_risk_ha<-haz_risk*sum(crop_ha_tot)
          }
          
          names(haz_risk_ha)<-paste0(names(haz_risk_ha),"-ha")
          writeRaster(haz_risk_ha,file=save_name_ha,overwrite=T)
        }
      }
    }
    # multiply risk by numbers
    if(do_n==T){
      save_name_n<-paste0(haz_risk_n_dir,"/",crop,"_",SEV,"_n_any.tif")
      if(crop %in% c("generic",livestock_choices)){
      
      if(!file.exists(save_name_n)|overwrite==T){
        if(crop!="generic"){
          haz_risk_n<-haz_risk*livestock_no[[crop]]
        }else{
          haz_risk_n<-haz_risk*sum(livestock_no[[(c("total_tropical","total_highland"))]],na.rm=T)
        }
        
        names(haz_risk_n)<-paste0(names(haz_risk_n),"-n")
        writeRaster(haz_risk_n,file=save_name_n,overwrite=T)
      }
    }
    }
    
  }
  
  #### Extract Risk x Exposure by Geography  ####
  
  # VoP
  haz_risk_vop_files<-list.files(haz_risk_vop_dir,"_any.tif",full.names = T)
  haz_risk_vop_files<-haz_risk_vop_files[grepl(SEV,haz_risk_vop_files)]
  data<-terra::rast(haz_risk_vop_files)
  
  file0<-paste0(haz_risk_vop_dir,"/haz_risk_vop_any_adm0_",SEV,".parquet")
  file1<-gsub("_adm0_","_adm1_",file0)
  file2<-gsub("_adm0_","_adm2_",file0)
  
  if(!file.exists(file0)|overwrite==T){
    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure - VoP admin extraction - adm0| severity:",SEV))
    flush.console()
    
    data_ex<-admin_extract(data,Geographies["admin0"],FUN="sum")
    sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin0), dsn=file0)
    rm(data_ex)
    gc()
  }
  if(!file.exists(file1)|overwrite==T){
    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure - VoP admin extraction - adm1| severity:",SEV))
    flush.console()
    
    data_ex<-admin_extract(data,Geographies["admin1"],FUN="sum")
    sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin1), dsn=file1)
    rm(data_ex)
    gc()
  }
  if(!file.exists(file2)|overwrite==T){
    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure - VoP admin extraction - adm2| severity:",SEV))
    flush.console()
    
    data_ex<-admin_extract(data,Geographies["admin2"],FUN="sum")
    sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin2), dsn=file2)
    rm(data_ex)
    gc()
  }
  
  # Harvested Area
  if(do_ha==T){
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure - Harvested Area admin extraction| severity:",SEV))
    flush.console()
    
    # Display progress
    file0<-paste0(haz_risk_ha_dir,"/haz_risk_ha_any_adm0_",SEV,".parquet")
    file1<-gsub("_adm0_","_adm1_",file0)
    file2<-gsub("_adm0_","_adm2_",file0)
    
    haz_risk_ha_files<-list.files(haz_risk_ha_dir,".tif",full.names = T)
    haz_risk_ha_files<-haz_risk_ha_files[grepl(SEV,haz_risk_ha_files)]
    data<-terra::rast(haz_risk_ha_files)
    
    if(!file.exists(file0)|overwrite==T){
      data_ex<-admin_extract(data,Geographies["admin0"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin0), dsn=file0)
  
      data_ex<-admin_extract(data,Geographies["admin1"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin1), dsn=file1)
  
      data_ex<-admin_extract(data,Geographies["admin2"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin2), dsn=file2)
      rm(data_ex)
      gc()
    }
  }
  # Display progress
  if(do_n==T){
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure - Numbers admin extraction| severity:",SEV))
    flush.console()
    
    haz_risk_n_files<-list.files(haz_risk_n_dir,".tif",full.names = T)
    haz_risk_n_files<-haz_risk_n_files[grepl(SEV,haz_risk_n_files)]
    data<-terra::rast(haz_risk_n_files)
    
    file0<-paste0(haz_risk_n_dir,"/haz_risk_n_any_adm0_",SEV,".parquet")
    file1<-gsub("_adm0_","_adm1_",file0)
    file2<-gsub("_adm0_","_adm2_",file0)
    
    if(!file.exists(file0)|overwrite==T){
      data_ex<-admin_extract(data,Geographies["admin0"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin0), dsn=file0)
      
      data_ex<-admin_extract(data,Geographies["admin1"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin1), dsn=file1)
      
      data_ex<-admin_extract(data,Geographies["admin2"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin2), dsn=file2)
      rm(data_ex)
      gc()
    }
  }
  
  #### Restructure Extracted Data ####
  
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
  
  # Vop
  filename<-paste0(haz_risk_vop_dir,"/haz_risk_vop_any_",SEV,".feather")
  
  if(!file.exists(filename)){
    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure - VoP restructuring data| severity:",SEV))
    flush.console()
    
    haz_risk_vop_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
      level<-levels[i]
      print(level)
      # Vop
      haz_risk_vop_tab<-data.table(data.frame(sfarrow::st_read_parquet(paste0(haz_risk_vop_dir,"/haz_risk_vop_any_",levels[i],"_",SEV,".parquet"))))
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
    feather::write_feather(haz_risk_vop_tab,filename)
  }
  
  # Harvested area
  if(do_ha==T){
    filename<-paste0(haz_risk_ha_dir,"/haz_risk_ha_any_",SEV,".feather")
    
    if(!file.exists(filename)){
      # Display progress
      cat('\r                                                                                                                     ')
      cat('\r',paste("Risk x Exposure - Harvested area restructuring data| severity:",SEV))
      flush.console()
      
      haz_risk_ha_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
        level<-levels[i]
        
        # Harvested area
        haz_risk_ha_tab<-data.table(data.frame(sfarrow::st_read_parquet(paste0(haz_risk_ha_dir,"/haz_risk_ha_any_",levels[i],"_",SEV,".parquet"))))
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
      feather::write_feather(haz_risk_ha_tab,filename)
    }
  }
  # Numbers
  if(do_n==T){
    filename<-paste0(haz_risk_n_dir,"/haz_risk_n_any_",SEV,".feather")
    if(!file.exists(filename)){
      haz_risk_n_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
        # Display progress
        cat('\r                                                                                                                     ')
        cat('\r',paste("Risk x Exposure - Number restructuring data| severity:",SEV))
        flush.console()
        
        level<-levels[i]
        
        haz_risk_n_tab<-data.table(data.frame(sfarrow::st_read_parquet(paste0(haz_risk_n_dir,"/haz_risk_n_any_",levels[i],"_",SEV,".parquet"))))
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
      feather::write_feather(haz_risk_n_tab,filename)
    }
  }
}

# 6) Generic Risk x Human Population #####
# This is only calculated for the risk of a "generic" hazard
haz_risk_hpop_dir<-paste0("Data/hazard_risk_hpop/",timeframe_choice)
if(!dir.exists(haz_risk_hpop_dir)){
  dir.create(haz_risk_hpop_dir)
}

overwrite<-T
haz_risk_files<-list.files(haz_risk_dir,".tif",full.names = T)
haz_risk_files<-haz_risk_files[grepl("generic_",haz_risk_files)]

for(SEV in tolower(severity_classes$class[2])){
  haz_risk_files2<- haz_risk_files[grepl(SEV,haz_risk_files)]
  
  # Intersect human population only with generic hazards
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

# 7) Generic Risk x Total Harvested Area ####
crop_ha_tot_sum<-sum(crop_ha_tot)

haz_risk_croparea_dir<-paste0("Data/",timeframe_choice,"/hazard_risk_croparea")
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

