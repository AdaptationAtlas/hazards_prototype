# Install and load packages ####
load_and_install_packages <- function(packages) {
  for (package in packages) {
    if (!require(package, character.only = TRUE)) {
      install.packages(package)
      library(package, character.only = TRUE)
    }
  }
}

# List of packages to be loaded
packages <- c("data.table", 
              "countrycode",
              "terra",
              "pbapply",
              "wbstats")

# Call the function to install and load packages
load_and_install_packages(packages)

# Load functions & wrappers ####
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

# Setup workspace ####
# Load base raster to resample to 
base_raster<-"base_raster.tif"
if(!file.exists(base_raster)){
  url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/base_raster.tif"
  httr::GET(url, write_disk(base_raster, overwrite = TRUE))
}

base_rast<-terra::mask(terra::rast(base_raster),geoboundaries)

# Load Livestock production data ####

ms_codes<-data.table::fread("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SpamCodes.csv")[,Code:=toupper(Code)]
crops<-tolower(ms_codes[compound=="no",Code])
colnames(prod)<-gsub("_a$","",colnames(prod))

# Location of livestock production datasets
lps_dir<-"Data/lps_herrero/raw"

# Where to save processed lps data
save_dir<-"Data/lps_herrero"
if(!dir.exists(save_dir)){
  dir.create(save_dir)
}

# Load geoboundaries ####
# Load and combine geoboundaries
geo_files_s3<-"https://digital-atlas.s3.amazonaws.com/boundaries/atlas-region_admin0_harmonized.gpkg"
geo_files_local<-file.path("Data/boundaries",basename(geo_files_s3))

if(!file.exists(geo_files_local)){
  download.file(url=geo_files_s3,destfile=geo_files_local)
}

geoboundaries<-terra::vect(geo_files_local)
  
# LPS 2005 production ####
prod05_file<-paste0(save_dir,"/prod_05.tif")

if(!file.exists(prod05_file)){
  files<-list.files(lps_dir,"_kg.zip$",full.names = T)
  
  # Unzip files
  sapply(files,unzip,exdir=lps_dir)
  
  files<-list.files(lps_dir,".tif$",full.names = T)
  
  names<-unlist(tstrsplit(basename(files),"_",keep=1))
  keep<-c(cattle_meat="bvmeat",
          cattle_milk="bvmilk",
          pig_meat="pimeat",
          poultry_eggs="poeggs",
          poultry_meat="pomeat",
          shoat_meat="sgmeat",
          shoat_milk="sgmilk")
  
  files<-files[names %in% keep]
  names<-names[names %in% keep]
  names(files)<-names(keep)[match(names,keep)]
  
  # Unit is kg/km2
  prod_05<-terra::rast(lapply(1:length(files),FUN=function(i){
    data<-terra::rast(files[i])
    data<-resample(data,base_rast)
    # Convert to tons per pixel
    data<-data*terra::cellSize(data,unit="km")/1000
    data
  }))
  names(prod_05)<-names(files)
  
  # mask to atlas area
  prod_05<-terra::mask(prod_05,geoboundaries)
  
  # Divide shoats according to sheep_goat ratios in glw3
  prod_05$sheep_meat<-prod_05$shoat_meat*sheep_prop
  prod_05$sheep_milk<-prod_05$shoat_milk*sheep_prop
  prod_05$goat_meat<-prod_05$shoat_meat*goat_prop
  prod_05$goat_milk<-prod_05$shoat_milk*goat_prop
  
  prod_05$shoat_meat<-NULL
  prod_05$shoat_milk<-NULL
  
  terra::writeRaster(prod_05,filename = prod05_file,overwrite=T)
  
  files<-list.files(save_dir,full.names = T)
  files<-files[!files %in% c(prod05_file)]
  unlink(files)
}else{
  prod_05<-terra::rast(prod05_file)
}

# Download producer price data from FAO ####
fao_dir<-"Data/fao"

if(!dir.exists(fao_dir)){
  dir.create(fao_dir,recursive = T)
}

econ_file<-file.path(fao_dir,"Prices_E_Africa_NOFLAG.csv")

if(!file.exists(econ_file)){
  # Define the URL and set the save path
  url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Prices_E_Africa.zip"
  zip_file_path <- file.path(fao_dir, "Prices_E_Africa.zip")
  
  # Download the file
  download.file(url, zip_file_path, mode = "wb")
  
  # Unzip the file
  unzip(zip_file_path, exdir = fao_dir)
  
  # Delete the ZIP file
  unlink(zip_file_path)
}


# Download production data from FAO ####
prod_file<-file.path(fao_dir,"Production_Crops_Livestock_E_Africa_NOFLAG.csv")

if(!file.exists(prod_file)){
  # Define the URL and set the save path
  url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Production_Crops_Livestock_E_Africa.zip"
  zip_file_path <- file.path(fao_dir, "Production_E_Africa.zip")
  
  # Download the file
  download.file(url, zip_file_path, mode = "wb")
  
  # Unzip the file
  unzip(zip_file_path, exdir = fao_dir)
  
  # Delete the ZIP file
  unlink(zip_file_path)
}

# Download production value data from FAO ####
vop_file<-paste0(fao_dir,"/Value_of_Production_E_Africa.csv")

if(!file.exists(vop_file)){
  # Define the URL and set the save path
  url<-"https://fenixservices.fao.org/faostat/static/bulkdownloads/Value_of_Production_E_Africa.zip"

  zip_file_path <- file.path(fao_dir, "Value_of_Production_E_Africa.zip")
  
  # Download the file
  download.file(url, zip_file_path, mode = "wb")
  
  # Unzip the file
  unzip(zip_file_path, exdir = fao_dir)
  
  # Delete the ZIP file
  unlink(zip_file_path)
}

# Download deflators from FAO ####

def_file<-file.path(fao_dir,"Deflators_E_All_Data_(Normalized).csv")


if(!file.exists(def_file)){
  # Define the URL and set the save path
  url<-"https://fenixservices.fao.org/faostat/static/bulkdownloads/Deflators_E_All_Data_(Normalized).zip"
  
  zip_file_path <- file.path(fao_dir,basename(url))
  
  # Download the file
  download.file(url, zip_file_path, mode = "wb")
  
  # Unzip the file
  unzip(zip_file_path, exdir = fao_dir)
  
  # Delete the ZIP file
  unlink(zip_file_path)
}

# Load and prepare fao data ####
remove_countries<- c("Ethiopia PDR","Sudan (former)","Cabo Verde","Comoros","Mauritius","R\xe9union","Seychelles")
atlas_iso3<-geoboundaries$iso3

prod<-prepare_fao_data(file=prod_file,
                       lps2fao,
                       elements="Production",
                       units="t",
                       remove_countries = remove_countries,
                       keep_years=c(1998:2002,2015:2019),
                       atlas_iso3=atlas_iso3)

prod_price<-prepare_fao_data(file=econ_file,
                             lps2fao,
                             elements="Producer Price (USD/tonne)",
                             remove_countries = remove_countries,
                             keep_years=2015:2019,
                             atlas_iso3=atlas_iso3)

# Note we have checked indigenous Items, they are always less than or equal to their parent category
fread(vop_file)[,unique(Element)]

# Note current thousand US$ has only 35 values whereas constant 12-16 has 157
target_year<-2017

prod_value<-prepare_fao_data(file=vop_file,
                             lps2fao,
                             elements="Gross Production Value (constant 2014-2016 thousand US$)",
                             remove_countries = remove_countries,
                             keep_years=target_year,
                             atlas_iso3=atlas_iso3)

setnames(prod_value,paste0("Y",target_year),"value")

# Deflate constant USD to current USD ####
# Prepare deflators data 
deflators<-fread(def_file)
deflators[,M49:=as.numeric(gsub("[']","",`Area Code (M49)`))]
deflators[,iso3:=countrycode(sourcevar=M49,origin="un",destination = "iso3c")]
deflators<-deflators[iso3 %in% atlas_iso3 & 
                       Year %in% c(2015,target_year) & 
                       Element == "Value US$, 2015 prices" &
                       Item == "Value Added Deflator (Agriculture, forestry and fishery)"
                     ][,deflator:=Value]

deflators<-deflators[,list(iso3,Year,deflator)][,Year:=paste0("D",Year)]
deflators<-dcast(deflators,iso3~Year)
setnames(deflators,paste0("D",c(2015,target_year)),c("def_past","def_target"))
deflators[,def:=def_target/def_past][,c("def_past","def_target"):=NULL]

# Add deflator to production value 2015
prod_value<-merge(prod_value,deflators,all.x=T)
# Any non matches?
prod_value[!is.na(value) & is.na(def)]

# Divide 2017 value in constant usd by deflator to get 2017 current usd
prod_value[,value_def:=value/def]

# Check results
prod_value[!is.na(value_def)]

# Average prices over 5-year period by country ####
prod_price[,mean:=mean(c(Y2019,Y2018,Y2017,Y2016,Y2015),na.rm=T),by=list(iso3,atlas_name)]

# Determine change in production 2000 to 2017 ####
prod[,mean17:=mean(c(Y2019,Y2018,Y2017,Y2016,Y2015),na.rm=T),by=list(iso3,atlas_name)
     ][,mean00:=mean(c(Y1998,Y1999,Y2000,Y2001,Y2002),na.rm=T),by=list(iso3,atlas_name)
       ][,ratio:=mean17/mean00]
  
# Substitute data from nearby areas ####

# Price
prod_price<-add_nearby(data=prod_price,value_field = "mean",neighbors=african_neighbors,regions)

# Production
prod<-add_nearby(data=prod,value_field = "ratio",neighbors=african_neighbors,regions)

# Save processed faostat data ####
fwrite(prod_price,paste0(fao_dir,"/production_value_processed.csv"))
fwrite(prod,paste0(fao_dir,"/production_processed.csv"))
  
# Convert faostat data into a raster ####
  overwrite<-F
  # Prices
  prod_price_cast<-dcast(prod_price[,list(iso3,mean_final,atlas_name)],iso3~atlas_name,value.var = "mean_final")
  
  # Use goat milk as proxy for sheep milk
  prod_price_cast$sheep_milk<-prod_price_cast$goat_milk
  
  file<-file.path(fao_dir,"production_value.tif")
  
  if(!file.exists(file)|overwrite){
    data<-merge(geoboundaries,prod_price_cast)
    
    cols<-names(data)
    cols<-cols[!cols %in% c("iso3","admin_name","admin0_name")]    
    
    price_rast<-terra::rast(pblapply(cols,FUN=function(COL){terra::rasterize(x=data,y=base_rast,field=COL)}))
    
    terra::writeRaster(price_rast,file,overwrite=T)
  }else{
    price_rast<-terra::rast(file)
  }
  
  
  # Production
  prod_ratio_cast<-dcast(prod[,list(iso3,ratio_final,atlas_name)],iso3~atlas_name,value.var = "ratio_final")
  
  file<-file.path(fao_dir,"fao_production_ratio.tif")
  
  if(!file.exists(file)|overwrite){

    geoboundaries<-merge(geoboundaries,prod_ratio_cast)
    
    cols<-names(geoboundaries)
    cols<-cols[!cols %in% c("iso3","admin_name","admin0_name")]
    
    prod_rast<-terra::rast(pblapply(cols,FUN=function(COL){terra::rasterize(x=geoboundaries,y=base_rast,field=COL)}))
  
    terra::writeRaster(prod_rast,file,overwrite=T)
  }else{
    prod_rast<-terra::rast(file)
  }

# Recreate raster ####
items<-data_ex[,sort(unique(item))]

prod_adj_rast<-terra::rast(lapply(1:length(items),FUN = function(i){
  print(paste(i,"/",length(items)))
  data_ss<-data_ex[!is.na(value) & item==items[i]]
  XX<-base_rast
  XX[match(data_ss$pix_val,pix_vals[])]<-data_ss$value
  XX

}))

names(prod_adj_rast)<-items
plot(prod_adj_rast)


# Multiply production by production value ####
# USD
  # Check names match
  names(prod_adj_rast) %in% names(price_rast)
  prod_vop<-prod_adj_rast*price_rast
  
  filename<-file.path(save_dir,"vop_adj17.tif")
  terra::writeRaster(prod_vop,filename)
  
# Calibrate/check against FAOstat data
  prod_value_cast<-dcast(prod_value,iso3~atlas_name,value.var = "value_def")
  prod_value_vect<-merge(geoboundaries,prod_value_cast)
  prod_names<-names(prod_value_vect)[4:ncol(prod_value_vect)]
  
  prod_value_rast<-terra::rast(lapply(prod_names,FUN=function(prod_name){
    x<-terra::rasterize(prod_value_vect,base_rast,field=prod_name)
    names(x)<-prod_name
    x
  }))
  
  prod_value_rast<-prod_value_rast*1000
  
  
  prod_vop_ex<-data.table(terra::extract(prod_vop,geoboundaries,fun=sum,na.rm=T))
  prod_vop_ex[,iso3:=geoboundaries$iso3][,ID:=NULL]
  prod_vop_vect<-merge(geoboundaries,prod_vop_ex)
  
  prod_vop_rast<-terra::rast(lapply(prod_names,FUN=function(prod_name){
    x<-terra::rasterize(prod_vop_vect,base_rast,field=prod_name)
    names(x)<-prod_name
    x
  }))
  
  plot(prod_value_rast/prod_vop_rast)
  

  
  livestock_vop17_tot_adm[admin0_name=="Ethiopia" & is.na(admin1_name)]
  
# IUSD - to do
