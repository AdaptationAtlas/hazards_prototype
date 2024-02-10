# !!!***This script assumes you have run the level 0_fao scripts first***!!! ####

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
packages <- c("countrycode", 
              "data.table", 
              "pbapply",
              "terra")

# Call the function to install and load packages
load_and_install_packages(packages)

# Create functions ####
transform_fao<-function(data,element,exclude_units,lps2fao,spam2fao,remove_countries,keep_years,add_missing=T){
  
  data<-data[((Item %in% lps2fao)|(`Item Code` %in% spam2fao$code_fao)) & Element==element  & !Unit %in% exclude_units]
  data[,crop:=names(lps2fao)[match(Item,lps2fao)]]
  data[is.na(crop),crop:=spam2fao$long_spam2010[match(data[is.na(crop)]$`Item Code`,spam2fao$code_fao)]]
  
  # Add iso3 codes and remove non-atlas countries ####
  data[,M49:=as.numeric(gsub("[']","",`Area Code (M49)`))]
  data[,iso3:=countrycode(sourcevar=M49,origin="un",destination = "iso3c")]
  data<-data[iso3 %in% atlas_iso3]
  
  # Remove Ethiopia PDR & Sudan (Former) & non-atlas countries
  data<-data[!Area %in% remove_countries]
  
  keep_cols<-c("iso3","crop",keep_years)
  data<-data[,..keep_cols]
  
  # Add missing crops x countries to fao ####
  
  if(add_missing){
    # Production
    fao_countries<-unique(data[,list(iso3)])
    data1<-data
    crops<-data[,unique(crop)]
    
    missing<-rbindlist(lapply(1:nrow(fao_countries),FUN=function(i){
      country<-fao_countries[i,iso3]
      missing_crops<-crops[!crops %in% data1[iso3==country,crop]]
      if(length(missing_crops)>0){
        data1<-data.table(iso3=country,
                          crop=missing_crops)
        data1[,(keep_years):=NA]
        data1
      }else{
        NULL
      }
    }))
    data<-rbind(data,missing)
    
    # Add missing countries to fao ####
    #Production
    missing_countries<-atlas_iso3[!atlas_iso3 %in% data$iso3]
    
    if(length(missing_countries)>0){
      
      missing<-rbindlist(lapply( 1:length(missing_countries),FUN=function(i){
        data1<-data.table(iso3=missing_countries[i],
                          crop=data[,unique(crop)])
        data1[,(keep_years):=NA]
        data1
      }))
      
      data<-rbind(data,missing)
    }
  }
  
  # Melt data to long form ####
  data<-melt(data,id.vars = c("iso3","crop"),variable.name = "year")
  data[,year:=as.numeric(gsub("Y","",as.character(year)))]
  
  return(data)
}
fill_gaps<-function(data){
  # Create list of neighboring countries ####
  neighbors <- list(
    DZA = c("TUN", "LBY", "NER", "ESH", "MRT", "MLI", "MAR"),
    AGO = c("COG", "COD", "ZMB", "NAM"),
    BEN = c("BFA", "NER", "NGA", "TGO"),
    BWA = c("ZMB", "ZWE", "NAM", "ZAF"),
    BFA = c("MLI", "NER", "BEN", "TGO", "GHA", "CIV"),
    BDI = c("COD", "RWA", "TZA"),
    CPV = c(),
    CMR = c("NGA", "TCD", "CAF", "COG", "GAB", "GNQ"),
    CAF = c("TCD", "SDN", "COD", "COG", "CMR"),
    TCD = c("LBY", "SDN", "CAF", "CMR", "NGA", "NER"),
    COM = c(),
    COG = c("GAB", "CMR", "CAF", "COD", "AGO"),
    COD = c("CAF", "SSD", "UGA", "RWA", "BDI", "TZA", "ZMB", "AGO", "COG"),
    CIV = c("LBR", "GIN", "MLI", "BFA", "GHA"),
    DJI = c("ERI", "ETH", "SOM"),
    EGY = c("LBY", "SDN", "ISR", "PSE"),
    GNQ = c("CMR", "GAB"),
    ERI = c("ETH", "SDN", "DJI"),
    SWZ = c("MOZ", "ZAF"),
    ETH = c("ERI", "DJI", "SOM", "KEN", "SSD", "SDN"),
    GAB = c("CMR", "GNQ", "COG"),
    GMB = c("SEN"),
    GHA = c("CIV", "BFA", "TGO"),
    GIN = c("LBR", "SLE", "CIV", "MLI", "SEN"),
    GNB = c("SEN", "GIN"),
    KEN = c("ETH", "SOM", "SSD", "UGA", "TZA"),
    LSO = c("ZAF"),
    LBR = c("GIN", "CIV", "SLE"),
    LBY = c("TUN", "DZA", "NER", "TCD", "SDN", "EGY"),
    MDG = c(),
    MWI = c("MOZ", "TZA", "ZMB"),
    MLI = c("DZA", "NER", "BFA", "CIV", "GIN", "SEN", "MRT"),
    MRT = c("DZA", "ESH", "SEN", "MLI"),
    MAR = c("DZA", "ESH", "ESP"),
    MOZ = c("ZAF", "SWZ", "ZWE", "ZMB", "MWI", "TZA"),
    NAM = c("AGO", "BWA", "ZAF", "ZMB"),
    NER = c("DZA", "LBY", "TCD", "NGA", "BEN", "BFA", "MLI"),
    NGA = c("BEN", "CMR", "TCD", "NER"),
    RWA = c("BDI", "COD", "TZA", "UGA"),
    STP = c(),
    SEN = c("GMB", "GIN", "GNB", "MLI", "MRT"),
    SYC = c(),
    SLE = c("GIN", "LBR"),
    SOM = c("ETH", "DJI", "KEN"),
    ZAF = c("NAM", "BWA", "ZWE", "MOZ", "SWZ", "LSO"),
    SSD = c("CAF", "COD", "ETH", "KEN", "UGA"),
    SDN = c("EGY", "ERI", "ETH", "SSD", "CAF", "TCD", "LBY"),
    TZA = c("KEN", "UGA", "RWA", "BDI", "COD", "ZMB", "MWI", "MOZ"),
    TGO = c("BEN", "BFA", "GHA"),
    TUN = c("DZA", "LBY"),
    UGA = c("KEN", "SSD", "COD", "RWA", "TZA"),
    ZMB = c("AGO", "COD", "MWI", "MOZ", "NAM", "TZA", "ZWE"),
    ZWE = c("BWA", "MOZ", "ZAF", "ZMB"),
    BRN = c("MYS"), 
    KHM = c("THA", "LAO", "VNM"), 
    IDN = c("MYS", "PNG", "TLS"), 
    LAO = c("MMR", "KHM", "THA", "VNM", "CHN"), 
    MYS = c("BRN", "IDN", "THA"), 
    MMR = c("BGD", "IND", "LAO", "THA", "CHN"), 
    PHL = c(),
    SGP = c(), 
    THA = c("MMR", "LAO", "KHM", "MYS"), 
    TLS = c("IDN"), 
    VNM = c("CHN", "LAO", "KHM") 
  )
  
  # Create list of regions ####
  regions <- list(
    East_Africa = c("BDI", "COM", "DJI", "ERI", "ETH", "KEN", "MDG", "MUS", "MWI", "RWA", "SYC", "SOM", "SSD", "TZA", "UGA"),
    Southern_Africa = c("BWA", "LSO", "NAM", "SWZ", "ZAF", "ZMB", "ZWE","MOZ"),
    West_Africa = c("BEN", "BFA", "CPV", "CIV", "GMB", "GHA", "GIN", "GNB", "LBR", "MLI", "MRT", "NER", "NGA", "SEN", "SLE", "TGO"),
    Central_Africa = c("AGO", "CMR", "CAF", "TCD", "COD", "COG", "GNQ", "GAB", "STP"),
    North_Africa = c("DZA", "EGY", "LBY", "MAR", "SDN", "TUN"),
    SE_Asia= c("BRN","KHM","IDN","LAO","MYS","MMR","PHL","SGP","THA","TLS","VNM")
  )
  
  # Fill in gaps with mean of neighbors ####
  avg_neighbors<-function(data,crop_focus,iso3_focus,neighbors){
    neighbors_focal<-neighbors[[iso3_focus]]
    result<-data[iso3 %in% neighbors_focal & crop==crop_focus,mean(value,na.rm=T)]
    return(result)
  }
  
  data_copy<-copy(data)
  data[,mean_neighbors:=avg_neighbors(data=data_copy,crop_focus=crop[1],iso3_focus=iso3[1],neighbors=neighbors),by=list(iso3,crop)]
  
  # Fill in gaps with mean of regions ####
  avg_regions<-function(data,iso3_focus,crop_focus,regions){
    region_focal<-names(regions)[sapply(regions,FUN=function(X){iso3_focus %in% X})]
    neighbors_focal<-regions[[region_focal]]
    result<-data[crop==crop_focus & iso3 %in% neighbors_focal,mean(value,na.rm=T)]
    return(result)
  }
  
  data[,mean_region:=avg_regions(data=data_copy,iso3_focus=iso3[1],crop_focus=crop[1],regions=regions),by=list(iso3,crop)]
  
  
  # Fill in gap with continental average ####
  data[,mean_continent:=mean(value,na.rm=T),by=crop]
  
  return(data)
  
}

# Setup workspace ####
# Load geoboundaries
overwrite<-F
geoboundaries_s3<-"s3://digital-atlas/boundaries"
geo_files_s3<-s3fs::s3_dir_ls(geoboundaries_s3)
geo_file_s3<-grep("admin0_harmonized.gpkg",geo_files_s3,value=T)

file_local<-file.path("Data/boundaries",basename(geo_file_s3))

if(!file.exists(file_local)|overwrite==T){
  s3fs::s3_file_download(path=geo_files_s3[i],new_path=file_local,overwrite = T)
}

geoboundaries<-terra::vect(file_local)

atlas_iso3<-geoboundaries$iso3

# Load base raster for resampling
base_raster<-"base_raster.tif"
if(!file.exists(base_raster)){
  url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/base_raster.tif"
  httr::GET(url, write_disk(base_raster, overwrite = TRUE))
}

base_rast<-terra::mask(terra::rast(base_raster),geoboundaries)


# Load SPAM codes ####
ms_codes<-data.table::fread("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SpamCodes.csv")[,Code:=toupper(Code)]
crops<-tolower(ms_codes[compound=="no",Code])

# Load file for translation of spam to fao stat names/codes ####
spam2fao<-fread("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SPAM2010_FAO_crops.csv")
spam2fao<-spam2fao[short_spam2010 %in% crops]

# Are all crops represented in fao name conversion sheet? 
if(!length(crops[!crops %in% spam2fao$short_spam2010])==0){
  stop("MAPspam crop missing from fao name conversion table SPAM2010_FAO_crops.csv")
}
# Map LPS names to FAOstat ####
lps2fao<-c(cattle_meat="Meat of cattle with the bone, fresh or chilled",
           cattle_milk="Raw milk of cattle",
           pig_meat="Meat of pig with the bone, fresh or chilled",
           poultry_eggs="Hen eggs in shell, fresh",
           poultry_meat="Meat of chickens, fresh or chilled",
           sheep_meat="Meat of sheep, fresh or chilled",
           sheep_milk="Raw milk of sheep",
           goat_meat="Meat of goat, fresh or chilled",
           goat_milk="Raw milk of goats")

# Load and Prepare fao data ####
fao_dir<-"Data/fao"

# Load data  ####
data_file<-file.path(fao_dir,"Prices_E_Africa_NOFLAG.csv")

# Set countries to remove and years to keep
remove_countries<- c("Ethiopia PDR","Sudan (former)","Cabo Verde","Comoros","Mauritius","R\xe9union","Seychelles")
keep_years<-paste0("Y",2010:2022)

data<-transform_fao(data=fread(data_file),
                     element="Producer Price (USD/tonne)",
                     exclude_units=NULL,
                     lps2fao=lps2fao,
                     spam2fao=spam2fao,
                     remove_countries=remove_countries,
                     keep_years=keep_years,
                     add_missing=T)

# Set zero values to NAs 
data[value==0,value:=NA]

# Gap filling ####
years<-data[,unique(year)]
data<-rbindlist(pblapply(years,FUN=function(y){
  fill_gaps(data[year==y])
}))

data[,value_filled:=value
     ][is.na(value_filled),value_filled:=mean_neighbors
       ][is.na(value_filled),value_filled:=mean_region
         ][is.na(value_filled),value_filled:=mean_continent]

fwrite(data,file=file.path(fao_dir,"faostat_price_2010_2022.csv"))

