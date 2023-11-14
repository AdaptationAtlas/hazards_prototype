source("R/haz_functions.R")
require(data.table)
require(terra)
require(doFuture)
require(exactextractr)
require(sf)
require(sfarrow)
require(stringr)

# Load metadata for countries to consider in the atlas - exclude islands for which we do not have climate data
countries_metadata<-fread("Data/metadata/countries.csv")[excluded_island==FALSE]

# Load and combine geoboundaries ####
Geographies<-list(
  admin2=terra::vect("Data/geoboundaries/admin2_processed.shp"),
  admin1=terra::vect("Data/geoboundaries/admin1_processed.shp"),
  admin0=terra::vect("Data/geoboundaries/admin0_processed.shp")
)

# Set scenarios and time frames to analyse
Scenarios<-c("ssp245","ssp585")
Times<-c("2021_2040","2041_2060")

# Create combinations of scenarios and times
Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))
Scenarios[,combined:=paste0(Scenario,"-",Time)]

# read in mapspam ####
mapspam_dir<-"Data/mapspam"
# metadata
ms_codes<-data.table::fread("./Data/metadata/SpamCodes.csv")[,Code:=toupper(Code)]
ms_codes<-ms_codes[compound=="no"]

# mapspam vop 
vop<-fread(paste0(mapspam_dir,"/spam2017V2r3_SSA_V_TA.csv"))
crops<-tolower(ms_codes$Code)
ms_fields<-c("x","y",grep(paste0(crops,collapse = "|"),colnames(vop),value=T))
vop<-rast(vop[,..ms_fields],type="xyz",crs="EPSG:4326")
names(vop)<-gsub("_a","",names(vop))
names(vop)<-ms_codes[match(names(vop),tolower(ms_codes$Code)),Fullname]

# mapspam harvested area 
ha<-fread(paste0(mapspam_dir,"/spam2017V2r3_SSA_H_TA.csv"))
crops<-tolower(ms_codes$Code)
ms_fields<-c("x","y",grep(paste0(crops,collapse = "|"),colnames(ha),value=T))
ha<-rast(ha[,..ms_fields],type="xyz",crs="EPSG:4326")
names(ha)<-gsub("_a","",names(ha))
names(ha)<-ms_codes[match(names(ha),tolower(ms_codes$Code)),Fullname]

# read in mean hazard values ####
timeframe_choice<-"annual"

save_dir_means<-paste0("Data/hazard_mean/",timeframe_choice)
files<-list.files(save_dir_means,".tif",full.names = T)

haz_means<-terra::rast(files[!grepl("change",files)])
haz_means_change<-terra::rast(files[grepl("change",files)])

# read in hazard indices ####
save_dir_hi<-paste0("Data/hazard_index/",timeframe_choice)
files<-list.files(save_dir_hi,".tif",full.names = T)
files<-files[!grepl(paste0(Scenarios$Scenario,collapse = "|"),files)]

hi<-terra::rast(files[!grepl("change",files)])
hi_change<-terra::rast(files[grepl("change",files)])

# run extractions ####
admin_extract<-function(data,Geographies,FUN="mean"){
  output<-list()
  if("admin0" %in% names(Geographies)){
  data0<-exactextractr::exact_extract(data,sf::st_as_sf(Geographies$admin0),fun=FUN,append_cols=c("shapeGroup","shapeID","admin_name"))
  colnames(data0)<-gsub(paste0(FUN,"."),"",colnames(data0),fixed = T)
  colnames(data0)<-gsub("-","_",colnames(data0),fixed = T)
  colnames(data0)<-gsub("1_2","1.2",colnames(data0),fixed = T)
  data0<-terra::merge(Geographies$admin0,data0)
  output$admin0<-data0
  }
  
  if("admin1" %in% names(Geographies)){
  data1<-exactextractr::exact_extract(data,sf::st_as_sf(Geographies$admin1),fun=FUN,append_cols=c("shapeGroup","shapeID","admin_name"))
  colnames(data1)<-gsub(paste0(FUN,"."),"",colnames(data1),fixed = T)
  colnames(data1)<-gsub("-","_",colnames(data1),fixed = T)
  colnames(data1)<-gsub("1_2","1.2",colnames(data1),fixed = T)
  data1<-terra::merge(Geographies$admin1,data1)
  output$admin1<-data1
  }
  
  if("admin2" %in% names(Geographies)){
    data2<-exactextractr::exact_extract(data,sf::st_as_sf(Geographies$admin2),fun=FUN,append_cols=c("shapeGroup","shapeID","admin_name"))
    colnames(data2)<-gsub(paste0(FUN,"."),"",colnames(data2),fixed = T)
    colnames(data2)<-gsub("-","_",colnames(data2),fixed = T)
    colnames(data2)<-gsub("1_2","1.2",colnames(data2),fixed = T)
    data2<-terra::merge(Geographies$admin2,data2)
    output$admin2<-data2
  }
  
  return(output)
}

# mean hazards  ####
haz_means_adm<-admin_extract(haz_means,Geographies)

st_write_parquet(obj=sf::st_as_sf(haz_means_adm$admin0), dsn=paste0(save_dir_means,"/haz_means_adm0.parquet"))
st_write_parquet(obj=sf::st_as_sf(haz_means_adm$admin1), dsn=paste0(save_dir_means,"/haz_means_adm1.parquet"))
st_write_parquet(obj=sf::st_as_sf(haz_means_adm$admin2), dsn=paste0(save_dir_means,"/haz_means_adm2.parquet"))

# change in mean hazards  ####
haz_means_change_adm<-admin_extract(haz_means_change,Geographies)

st_write_parquet(obj=sf::st_as_sf(haz_means_adm$admin0), dsn=paste0(save_dir_means,"/haz_means_change_adm0.parquet"))
st_write_parquet(obj=sf::st_as_sf(haz_means_adm$admin1), dsn=paste0(save_dir_means,"/haz_means_change_adm1.parquet"))
st_write_parquet(obj=sf::st_as_sf(haz_means_adm$admin2), dsn=paste0(save_dir_means,"/haz_means_change_adm2.parquet"))

# hazard indices  ####
hi_adm<-admin_extract(hi,Geographies)

st_write_parquet(obj=sf::st_as_sf(hi_adm$admin0), dsn=paste0(save_dir_hi,"/hi_adm0.parquet"))
st_write_parquet(obj=sf::st_as_sf(hi_adm$admin1), dsn=paste0(save_dir_hi,"/hi_adhttp://127.0.0.1:15977/graphics/39d473f1-bfb2-46ec-b40b-a779211737a5.pngm1.parquet"))
st_write_parquet(obj=sf::st_as_sf(hi_adm$admin2), dsn=paste0(save_dir_hi,"/hi_adm2.parquet"))

# hazard indices change  ####
hi_change_adm<-admin_extract(hi_change,Geographies)

st_write_parquet(obj=sf::st_as_sf(hi_change_adm$admin0), dsn=paste0(save_dir_hi,"/hi_change_adm0.parquet"))
st_write_parquet(obj=sf::st_as_sf(hi_change_adm$admin1), dsn=paste0(save_dir_hi,"/hi_change_adm1.parquet"))
st_write_parquet(obj=sf::st_as_sf(hi_change_adm$admin2), dsn=paste0(save_dir_hi,"/hi_change_adm2.parquet"))

# Hazard timeseries #####
save_dir_ts<-"Data/hazard_timeseries/"

files<-list.files(paste0(save_dir_ts,timeframe_choice),".tif",full.names = T)

data<-terra::rast(lapply(1:length(files),FUN=function(i){
  file<-files[i]
  file_name<-gsub(".tif","",tstrsplit(file,"/",keep=stringr::str_count(file,"/")+1))
  
  data<-terra::rast(file)
  names(data)<-paste0(file_name,"_",names(data))
  data
}))

haz_timeseries_adm<-admin_extract(data,Geographies)

st_write_parquet(obj=sf::st_as_sf(haz_timeseries_adm$admin0), dsn=paste0(save_dir_ts,"/haz_timeseries_adm0.parquet"))
st_write_parquet(obj=sf::st_as_sf(haz_timeseries_adm$admin1), dsn=paste0(save_dir_ts,"/haz_timeseries_adm1.parquet"))
st_write_parquet(obj=sf::st_as_sf(haz_timeseries_adm$admin2), dsn=paste0(save_dir_ts,"/haz_timeseries_adm2.parquet"))

# MapSpam  ####
# convert to value/area 
vop<-vop/terra::cellSize(vop,unit="ha")
ha<-ha/terra::cellSize(ha,unit="ha")

# resample vop data
vop<-terra::resample(vop,haz_means)
ha<-terra::resample(ha,haz_means)

vop_tot<-vop*cellSize(vop,unit="ha")
ha_tot<-ha*cellSize(ha,unit="ha")

terra::writeRaster(vop_tot,paste0(mapspam_dir,"/spam2017V2r3_SSA_V_TA-vop_tot.tif"), overwrite=T)
terra::writeRaster(ha_tot,paste0(mapspam_dir,"/spam2017V2r3_SSA_H_TA-ha_tot.tif"), overwrite=T)
terra::writeRaster(vop,paste0(mapspam_dir,"/spam2017V2r3_SSA_V_TA-vop_ha.tif"), overwrite=T)
terra::writeRaster(ha,paste0(mapspam_dir,"/spam2017V2r3_SSA_H_TA-ha_ha.tif"), overwrite=T)

# Extract total vop per admin area
vop_tot_adm<-admin_extract(vop_tot,Geographies,FUN="sum")

st_write_parquet(obj=sf::st_as_sf(vop_tot_adm$admin0), dsn=paste0(mapspam_dir,"/spam2017V2r3_SSA_V_TA-vop_adm0.parquet"))
st_write_parquet(obj=sf::st_as_sf(vop_tot_adm$admin1), dsn=paste0(mapspam_dir,"/spam2017V2r3_SSA_V_TA-vop_adm1.parquet"))
st_write_parquet(obj=sf::st_as_sf(vop_tot_adm$admin2), dsn=paste0(mapspam_dir,"/spam2017V2r3_SSA_V_TA-vop_adm2.parquet"))

ha_tot_adm<-admin_extract(ha_tot,Geographies,FUN="sum")

st_write_parquet(obj=sf::st_as_sf(ha_tot_adm$admin0), dsn=paste0(mapspam_dir,"/spam2017V2r3_SSA_H_TA-ha_adm0.parquet"))
st_write_parquet(obj=sf::st_as_sf(ha_tot_adm$admin1), dsn=paste0(mapspam_dir,"/spam2017V2r3_SSA_H_TA-ha_adm1.parquet"))
st_write_parquet(obj=sf::st_as_sf(ha_tot_adm$admin2), dsn=paste0(mapspam_dir,"/spam2017V2r3_SSA_H_TA-ha_adm2.parquet"))
