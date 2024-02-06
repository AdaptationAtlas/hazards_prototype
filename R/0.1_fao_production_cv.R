# This script assumes you have run the level 0_fao scripts first ####

# Load required libraries ####
require(data.table)
require(countrycode)
require(terra)
require(pbapply)

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

# Load base raster to resample to 
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
prod_file<-file.path(fao_dir,"Production_Crops_Livestock_E_Africa_NOFLAG.csv")

prod<-fread(prod_file)
prod<-prod[((Item %in% lps2fao)|(`Item Code` %in% spam2fao$code_fao)) & grepl("Yield",Element)  & Unit != "No/An"]
prod[,atlas_name:=names(lps2fao)[match(Item,lps2fao)]]
prod[is.na(atlas_name),atlas_name:=spam2fao$long_spam2010[match(prod[is.na(atlas_name)]$`Item Code`,spam2fao$code_fao)]]

# Add iso3 codes and remove non-atlas countries ####
prod[,M49:=as.numeric(gsub("[']","",`Area Code (M49)`))]
prod[,iso3:=countrycode(sourcevar=M49,origin="un",destination = "iso3c")]
prod<-prod[iso3 %in% atlas_iso3]

# Remove Ethiopia PDR & Sudan (Former) & non-atlas countries
remove_countries<- c("Ethiopia PDR","Sudan (former)","Cabo Verde","Comoros","Mauritius","R\xe9union","Seychelles")
prod<-prod[!Area %in% remove_countries]

keep_years<-paste0("Y",1961:2022)
keep_cols<-c("iso3","atlas_name",keep_years)
prod<-prod[,..keep_cols]

# Add missing crops x countries to fao ####

# Production
fao_countries<-unique(prod[,list(iso3)])
data<-prod

missing<-rbindlist(lapply(1:nrow(fao_countries),FUN=function(i){
  country<-fao_countries[i,iso3]
  missing_crops<-crops[!crops %in% data[iso3==country,atlas_name]]
  if(length(missing_crops)>0){
    data<-data.table(iso3=country,
                     atlas_name=missing_crops)
    data[,(keep_years):=NA]
    data
  }else{
    NULL
  }
}))
prod<-rbind(prod,missing)

# Add missing countries to fao ####
#Production
missing_countries<-atlas_iso3[!atlas_iso3 %in% prod$iso3]

if(length(missing_countries)>0){
  
  missing<-rbindlist(lapply( 1:length(missing_countries),FUN=function(i){
    data<-data.table(iso3=missing_countries[i],
                     atlas_name=prod[,unique(atlas_name)])
    data[,(keep_years):=NA]
    data
  }))
  
  prod<-rbind(prod,missing)
}


# Melt data to long form ####
prod<-melt(prod,id.vars = c("iso3","atlas_name"),variable.name = "year")
prod[,year:=as.numeric(gsub("Y","",year))]

# Function to detrend data ####
calc_cv<-function(data,detrend=T,rm.na=T,min_data=10){
  
  if(sum(is.na(data))>0 & rm.na==F){
    return(as.numeric(NA))
  }else{
    data<-data[!is.na(data)]
    
    if(length(data)<min_data){
      return(as.numeric(NA))
    }else{
        
      if(detrend==T){
        years <- seq_along(data)  # Assuming each yield corresponds to a consecutive year
        
        # Fit a linear model to the data
        model <- lm(data ~ years)
        
        X<-summary(model)
        pr_years<-data.frame(X$coefficients)[2,4]
        r2_adj<-data.frame(X$adj.r.squared)
        
        if(pr_years<0.05 & r2_adj>0.1){
        # Get the residuals, which represent the detrended data
        detrended_data <- residuals(model) + mean(production)
        
        # Then calculate the coefficient of variation on the adjusted detrended production
        cv <- sd(detrended_data) / mean(detrended_data)
        }else{
          cv<-as.numeric(NA)
        }
      }else{
        cv <- sd(data) / mean(data)
      }
      
    return(cv)
    }
  }
}

(data<-prod[iso3=="KEN" & atlas_name=="cattle_milk",value])

calc_cv(data=data,detrend = T)


# Apply CV function ####
cv<-prod[,list(cv=calc_cv(data=value,detrend = F),cv_detrended=calc_cv(data=value,detrend = T)),by=list(iso3,atlas_name)]
cv[iso3=="KEN"]

