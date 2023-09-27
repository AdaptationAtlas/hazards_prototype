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

# mapspam vop 2017
vop<-fread("Data/mapspam/SPAM2017V2R2_GADM_SSA_GR_V_TA.csv")
ms_fields<-c("x","y",grep(tolower(paste0(ms_codes$Code,collapse = "|")),colnames(vop),value=T))
vop<-rast(vop[,..ms_fields],type="xyz",crs="EPSG:4326")
# convert value of production to value/area 
vop<-vop/terra::cellSize(vop,unit="ha")
names(vop)<-gsub("_a","",names(vop))

# read in mean hazard values ####
timeframe_choice<-"annual"

save_dir_means<-paste0("Data/hazard_means/",timeframe_choice)
files<-list.files(save_dir_means,full.names = T)

haz_means<-terra::rast(files[!grepl("change",files)])
haz_means_change<-terra::rast(files[grepl("change",files)])

# resample vop data ####
vop<-terra::resample(vop,haz_means)

# read in hazard indices ####
save_dir_hi<-paste0("Data/hazard_indices/",timeframe_choice)
files<-list.files(save_dir_hi,"combined",full.names = T)

hi<-terra::rast(files[!grepl("change",files)])
hi_change<-terra::rast(files[grepl("change",files)])

# run extractions ####

# mean hazards
haz_means_adm0<-rbindlist(exactextractr::exact_extract(haz_means,sf::st_as_sf(Geographies$admin0),include_cols="admin_name"))
haz_means_adm1<-rbindlist(exactextractr::exact_extract(haz_means,sf::st_as_sf(Geographies$admin1),include_cols="admin_name"))
haz_means_adm2<-rbindlist(exactextractr::exact_extract(haz_means,sf::st_as_sf(Geographies$admin2),include_cols="admin_name"))

# change in mean hazards
haz_means_change_adm0<-rbindlist(exactextractr::exact_extract(haz_means_change,sf::st_as_sf(Geographies$admin0),include_cols="admin_name"))
haz_means_change_adm1<-rbindlist(exactextractr::exact_extract(haz_means_change,sf::st_as_sf(Geographies$admin1),include_cols="admin_name"))
haz_means_change_adm2<-rbindlist(exactextractr::exact_extract(haz_means_change,sf::st_as_sf(Geographies$admin2),include_cols="admin_name"))

# hazard indices
hi_adm0<-rbindlist(exactextractr::exact_extract(hi,sf::st_as_sf(Geographies$admin0),include_cols="admin_name"))
hi_adm1<-rbindlist(exactextractr::exact_extract(hi,sf::st_as_sf(Geographies$admin1),include_cols="admin_name"))
hi_adm2<-rbindlist(exactextractr::exact_extract(hi,sf::st_as_sf(Geographies$admin2),include_cols="admin_name"))

# hazard indices change
hi_change_adm0<-rbindlist(exactextractr::exact_extract(hi_change,sf::st_as_sf(Geographies$admin0),include_cols="admin_name"))
hi_change_adm1<-rbindlist(exactextractr::exact_extract(hi_change,sf::st_as_sf(Geographies$admin1),include_cols="admin_name"))
hi_change_adm2<-rbindlist(exactextractr::exact_extract(hi_change,sf::st_as_sf(Geographies$admin2),include_cols="admin_name"))