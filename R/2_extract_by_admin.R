source("R/haz_functions.R")
require(data.table)
require(terra)
require(doFuture)
require(exactextractr)
require(sf)

# Load metadata for countries to consider in the atlas - exclude islands for which we do not have climate data
countries_metadata<-fread("Data/metadata/countries.csv")[excluded_island==FALSE]

# Load and combine geoboundaries ####
Geographies<-list(
  admin2=terra::vect("Data/geoboundaries/admin2.shp"),
  admin1=terra::vect("Data/geoboundaries/admin1.shp"),
  admin0=terra::vect("Data/geoboundaries/admin0.shp")
)

# Create standard name field for each admin vector
Geographies$admin2$admin_name<-Geographies$admin2$shapeName
Geographies$admin1$admin_name<-Geographies$admin1$shapeName
Geographies$admin0$admin_name<-Geographies$admin0$shapeName

# read in mapspam ####
# metadata
ms_codes<-data.table::fread("./Data/metadata/SpamCodes.csv")[,Code:=toupper(Code)]
ms_codes<-ms_codes[compound=="no"]

# mapspam vop 
vop<-fread("Data/mapspam/SPAM2017V2R2_GADM_SSA_GR_V_TA.csv")
crops<-tolower(ms_codes$Code)
ms_fields<-c("x","y",grep(paste0(crops,collapse = "|"),colnames(vop),value=T))
vop<-rast(vop[,..ms_fields],type="xyz",crs="EPSG:4326")


# read in mean hazard values ####
timeframe_choice<-"annual"

save_dir_means<-paste0("Data/hazard_means/",timeframe_choice)
files<-list.files(save_dir_means,".tif",full.names = T)

haz_means<-terra::rast(files[!grepl("change",files)])
haz_means_change<-terra::rast(files[grepl("change",files)])

# read in hazard indices ####
save_dir_hi<-paste0("Data/hazard_indices/",timeframe_choice)
files<-list.files(save_dir_hi,"combined",full.names = T)
files<-grep("tif",files,value = T)

hi<-terra::rast(files[!grepl("change",files)])
hi_change<-terra::rast(files[grepl("change",files)])

# run extractions ####

admin_extract<-function(data,Geographies,FUN="mean"){
  data0<-exactextractr::exact_extract(data,sf::st_as_sf(Geographies$admin0),fun=FUN,append_cols=c("shapeGroup","shapeID","admin_name"))
  data1<-exactextractr::exact_extract(data,sf::st_as_sf(Geographies$admin1),fun=FUN,append_cols=c("shapeGroup","shapeID","admin_name"))
  data2<-exactextractr::exact_extract(data,sf::st_as_sf(Geographies$admin2),fun=FUN,append_cols=c("shapeGroup","shapeID","admin_name"))
  
  colnames(data0)<-gsub(paste0(FUN,"."),"",colnames(data0),fixed = T)
  colnames(data1)<-gsub(paste0(FUN,"."),"",colnames(data1),fixed = T)
  colnames(data2)<-gsub(paste0(FUN,"."),"",colnames(data2),fixed = T)
  
  colnames(data0)<-gsub("-","_",colnames(data0),fixed = T)
  colnames(data1)<-gsub("-","_",colnames(data1),fixed = T)
  colnames(data2)<-gsub("-","_",colnames(data2),fixed = T)
  
  colnames(data0)<-gsub("1_2","1.2",colnames(data0),fixed = T)
  colnames(data1)<-gsub("1_2","1.2",colnames(data1),fixed = T)
  colnames(data2)<-gsub("1_2","1.2",colnames(data2),fixed = T)
  
  data0<-terra::merge(Geographies$admin0,data0)
  data1<-terra::merge(Geographies$admin1,data1)
  data2<-terra::merge(Geographies$admin2,data2)
  
  return(list(admin0=data0,admin1=data1,admin2=data2))
}

# mean hazards  ####
haz_means_adm<-admin_extract(haz_means,Geographies)

terra::writeVector(haz_means_adm$admin0,filename=paste0(save_dir_means,"/haz_means_adm0.geojson"),overwrite=T, filetype = "geojson")
terra::writeVector(haz_means_adm$admin1,filename=paste0(save_dir_means,"/haz_means_adm1.geojson"),overwrite=T, filetype = "geojson")
terra::writeVector(haz_means_adm$admin2,filename=paste0(save_dir_means,"/haz_means_adm2.geojson"),overwrite=T, filetype = "geojson")

# change in mean hazards  ####
haz_means_change_adm<-admin_extract(haz_means_change,Geographies)

terra::writeVector(haz_means_change_adm$admin0,filename=paste0(save_dir_means,"/haz_means_change_adm0.geojson"),overwrite=T, filetype = "geojson")
terra::writeVector(haz_means_change_adm$admin1,filename=paste0(save_dir_means,"/haz_means_change_adm1.geojson"),overwrite=T, filetype = "geojson")
terra::writeVector(haz_means_change_adm$admin2,filename=paste0(save_dir_means,"/haz_means_change_adm2.geojson"),overwrite=T, filetype = "geojson")

# hazard indices  ####
hi_adm<-admin_extract(hi,Geographies)

A<-terra::vect(paste0(save_dir_hi,"/hi_adm0.geojson"))

terra::writeVector(hi_adm$admin0,filename=paste0(save_dir_hi,"/hi_adm0.geojson"),overwrite=T, filetype = "geojson")
terra::writeVector(hi_adm$admin1,filename=paste0(save_dir_hi,"/hi_adm1.geojson"),overwrite=T, filetype = "geojson")
terra::writeVector(hi_adm$admin2,filename=paste0(save_dir_hi,"/hi_adm2.geojson"),overwrite=T, filetype = "geojson")

# hazard indices change  ####
hi_change_adm<-admin_extract(hi,Geographies)

terra::writeVector(hi_change_adm$admin0,filename=paste0(save_dir_hi,"/hi_change_adm0.geojson"), filetype = "geojson")
terra::writeVector(hi_change_adm$admin1,filename=paste0(save_dir_hi,"/hi_change_adm1.geojson"), filetype = "geojson")
terra::writeVector(hi_change_adm$admin2,filename=paste0(save_dir_hi,"/hi_change_adm2.geojson"), filetype = "geojson")

# MapSpam  ####
# convert value of production to value/area 
vop<-vop/terra::cellSize(vop,unit="ha")
names(vop)<-gsub("_a","",names(vop))
# resample vop data
vop<-terra::resample(vop,haz_means)
vop_tot<-vop*cellSize(vop,unit="ha")

# Extract total vop per admin area
vop_tot_adm<-admin_extract(vop_tot,Geographies,FUN="sum")

terra::writeVector(vop_tot_adm$admin0,filename="Data/mapspam/vop_adm0.geojson", filetype = "geojson",overwrite=T)
terra::writeVector(vop_tot_adm$admin1,filename="Data/mapspam/vop_adm1.geojson", filetype = "geojson",overwrite=T)
terra::writeVector(vop_tot_adm$admin2,filename="Data/mapspam/vop_adm2.geojson", filetype = "geojson",overwrite=T)