# You will need run these scripts before running this one: ####
# https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/fao_producer_prices.R
# https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/fao_producer_prices_livestock.R

# List of packages to be loaded
pacman::p_load(countrycode, 
              data.table, 
              pbapply,
              terra)

# Call the function to install and load packages
load_and_install_packages(packages)

# Create functions ####
transform_fao<-function(data,element,exclude_units,lps2fao,spam2fao,remove_countries,keep_years,add_missing=T){
  
  data<-data[((Item %in% lps2fao)|(`Item Code` %in% spam2fao$code_fao)) & Element==element  & !Unit %in% exclude_units]
  data[,atlas_name:=names(lps2fao)[match(Item,lps2fao)]]
  data[is.na(atlas_name),atlas_name:=spam2fao$long_spam2010[match(data[is.na(atlas_name)]$`Item Code`,spam2fao$code_fao)]]
  
  # Add iso3 codes and remove non-atlas countries ####
  data[,M49:=as.numeric(gsub("[']","",`Area Code (M49)`))]
  data[,iso3:=countrycode(sourcevar=M49,origin="un",destination = "iso3c")]
  data<-data[iso3 %in% atlas_iso3]
  
  # Remove Ethiopia PDR & Sudan (Former) & non-atlas countries
  data<-data[!Area %in% remove_countries]
  
  keep_cols<-c("iso3","atlas_name",keep_years)
  data<-data[,..keep_cols]
  
  # Add missing crops x countries to fao ####
  
  if(add_missing){
    # Production
    fao_countries<-unique(data[,list(iso3)])
    data1<-data
    crops<-data[,unique(atlas_name)]
    
    missing<-rbindlist(lapply(1:nrow(fao_countries),FUN=function(i){
      country<-fao_countries[i,iso3]
      missing_crops<-crops[!crops %in% data1[iso3==country,atlas_name]]
      if(length(missing_crops)>0){
        data1<-data.table(iso3=country,
                          atlas_name=missing_crops)
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
                          atlas_name=data[,unique(atlas_name)])
        data1[,(keep_years):=NA]
        data1
      }))
      
      data<-rbind(data,missing)
    }
  }
  
  # Melt data to long form ####
  data<-melt(data,id.vars = c("iso3","atlas_name"),variable.name = "year")
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
    ZWE = c("BWA", "MOZ", "ZAF", "ZMB")
  )
  
  # Create list of regions ####
  regions <- list(
    East_Africa = c("BDI", "COM", "DJI", "ERI", "ETH", "KEN", "MDG", "MUS", "MWI", "RWA", "SYC", "SOM", "SSD", "TZA", "UGA"),
    Southern_Africa = c("BWA", "LSO", "NAM", "SWZ", "ZAF", "ZMB", "ZWE","MOZ"),
    West_Africa = c("BEN", "BFA", "CPV", "CIV", "GMB", "GHA", "GIN", "GNB", "LBR", "MLI", "MRT", "NER", "NGA", "SEN", "SLE", "TGO"),
    Central_Africa = c("AGO", "CMR", "CAF", "TCD", "COD", "COG", "GNQ", "GAB", "STP"),
    North_Africa = c("DZA", "EGY", "LBY", "MAR", "SDN", "TUN")
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
calc_cv<-function(data,detrend=T,rm.na=T,min_data=10,prob_req=0.05,rsq_req=0.1){
  
  if(sum(is.na(data))>0 & rm.na==F){
    return(as.numeric(NA))
  }else{
    data<-data[!is.na(data)]
    
    if(length(data)<min_data|length(unique(data))==1){
      return(as.numeric(NA))
    }else{
      
      if(detrend==T){
        years <- seq_along(data)  # Assuming each yield corresponds to a consecutive year
        
        # Fit a linear model to the data
        model <- lm(data ~ years)
        
        X<-summary(model)
        pr_years<-data.frame(X$coefficients)[2,4]
        r2_adj<-data.frame(X$adj.r.squared)
        
        if(pr_years<prob_req & r2_adj>rsq_req){
          # Get the residuals, which represent the detrended data
          detrended_data <- residuals(model) + mean(data)
          
          # Then calculate the coefficient of variation on the adjusted detrended datauction
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

# Setup workspace ####
# Load geoboundaries
geoboundaries<-terra::vect(geo_files_local[1])
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
data_file<-file.path(fao_dir,"Production_Crops_Livestock_E_Africa_NOFLAG.csv")

# Set countries to remove and years to keep
remove_countries<- c("Ethiopia PDR","Sudan (former)","Cabo Verde","Comoros","Mauritius","R\xe9union","Seychelles")
keep_years<-paste0("Y",2000:2022)

yield<-transform_fao(data=fread(data_file),
                     element="Yield",
                     exclude_units="No/An",
                     lps2fao=lps2fao,
                     spam2fao=spam2fao,
                     remove_countries=remove_countries,
                     keep_years=keep_years,
                     add_missing=T)[,var:="yield"]

prod<-transform_fao(data=fread(data_file),
                     element="Production",
                     exclude_units="1000 No",
                     lps2fao=lps2fao,
                     spam2fao=spam2fao,
                     remove_countries=remove_countries,
                     keep_years=keep_years,
                     add_missing=T)[,var:="prod"]

data<-rbind(yield,prod)

# Set zero values to NAs 
data[value==0,value:=NA]


# Look for issues in the data ####
if(F){
  X<-split(data,by=c("iso3","atlas_name","var"))
  
  for(i in 1:length(X)){
    print(i)
    values<-X[[i]]$value
    calc_cv(data=values,detrend = T)
    calc_cv(data=values,detrend = F)
  }
}


# Apply CV function ####
min_data<-10
prob_req<-0.05
rsq_req<-0.1

cv<-data[,list(cv=calc_cv(data=value,detrend = F,min_data=min_data,prob_req=prob_req,rsq_req=rsq_req),
               cv_detrended=calc_cv(data=value,detrend = T,min_data=min_data,prob_req=prob_req,rsq_req=rsq_req),
               n_years=sum(!is.na(value))),
         by=list(iso3,atlas_name,var)]

cv<-dcast(cv,iso3+atlas_name~var,value.var = c("cv","cv_detrended","n_years"))

# Explore differences in variables and effect of detrending
if(F){
  cv[,cv_prod_diff:=cv_prod-cv_detrended_prod
   ][,cv_yield_diff:=cv_yield-cv_detrended_yield
     ][,cv_diff:=cv_prod-cv_yield
        ][,cv_detrended_diff:=cv_detrended_prod-cv_detrended_yield]
  
  cv[iso3=="NGA" & !is.na(cv_prod)]
}

# Choose which values to use ####
data<-cv[,value:=cv_detrended_prod][,list(iso3,atlas_name,value)]
setnames(data,"atlas_name","crop")

# Gap filling ####
data<-fill_gaps(data)

data[,value_filled:=value
     ][is.na(value_filled),value_filled:=mean_neighbors
       ][is.na(value_filled),value_filled:=mean_region
         ][is.na(value_filled),value_filled:=mean_continent]


fwrite(data,file=file.path(fao_dir,"faostat_prod_cv.csv"))

