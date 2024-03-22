require(data.table)
require(countrycode)
require(terra)
require(pbapply)

# Setup workspace ####
# Load base raster to resample to 
base_raster<-"base_raster.tif"
if(!file.exists(base_raster)){
  url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/base_raster.tif"
  httr::GET(url, write_disk(base_raster, overwrite = TRUE))
}

base_rast<-terra::mask(terra::rast(base_raster),geoboundaries)

# Load Livestock production data ####

prod<-fread("Data/mapspam/SSA_P_TA.csv")

ms_codes<-data.table::fread("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SpamCodes.csv")[,Code:=toupper(Code)]
crops<-tolower(ms_codes[compound=="no",Code])
colnames(prod)<-gsub("_a$","",colnames(prod))

# Location of livestock production datasets
lps_dir<-"D:/datasets/Livestock Production Systems (Herrero)"

# Where to save processed lps data
save_dir<-"Data/lps_herrero"
if(dir.exists(save_dir)){
  dir.create(save_dir)
}

# Load geoboundaries ####
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

# LPS 2005 production ####
prod05_file<-paste0(save_dir,"/prod_05.tif")

if(!file.exists(prod05_file)){
  files<-list.files(lps_dir,"_kg.zip$",full.names = T)
  
  # Unzip files
  sapply(files,unzip,exdir=save_dir)
  
  files<-list.files(save_dir,".tif$",full.names = T)
  
  names<-unlist(tstrsplit(basename(files),"_",keep=1))
  keep<-c(cattle_meat="bvmeat",cattle_milk="bvmilk",pig_meat="pimeat",poultry_eggs="poeggs",poultry_meat="pomeat",shoat_meat="sgmeat",shoat_milk="sgmilk")
  
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

# Download production value data from FAO ####
fao_dir<-"Data/fao"

if(!dir.exists(fao_dir)){
  dir.create(fao_dir,recursive = T)
}

econ_file<-paste0(fao_dir,"/Prices_E_Africa_NOFLAG.csv")

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
prod_file<-paste0(fao_dir,"/Production_Crops_Livestock_E_Africa_NOFLAG.csv")

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

# Prepare fao data ####

# Load datasets
prod<-fread(prod_file)
prod<-prod[Item %in% lps2fao & Element=="Production" & Unit == "t"]

prod_price<-fread(econ_file)
prod_price<-prod_price[Item %in% lps2fao & Element=="Producer Price (USD/tonne)"][,atlas_name:=names(lps2fao)[match(Item,lps2fao)]]

prod_price_IUSD<-fread(econ_file)
prod_price_IUSD<-prod_price[Item %in% lps2fao & Element=="Producer Price (IUSD/tonne)"][,atlas_name:=names(lps2fao)[match(Item,lps2fao)]]

# Covert fao item name to atlas name
prod[,atlas_name:=names(lps2fao)[match(Item,lps2fao)]]
prod_price[,atlas_name:=names(lps2fao)[match(Item,lps2fao)]]
prod_price_IUSD[,atlas_name:=names(lps2fao)[match(Item,lps2fao)]]

# Add iso3 codes and remove non-atlas countries
prod[,M49:=as.numeric(gsub("[']","",`Area Code (M49)`))]
prod[,iso3:=countrycode(sourcevar=M49,origin="un",destination = "iso3c")]
prod<-prod[iso3 %in% atlas_iso3]

prod_price[,M49:=as.numeric(gsub("[']","",`Area Code (M49)`))]
prod_price[,iso3:=countrycode(sourcevar=M49,origin="un",destination = "iso3c")]
prod_price<-prod_price[iso3 %in% atlas_iso3]

prod_price_IUSD[,M49:=as.numeric(gsub("[']","",`Area Code (M49)`))]
prod_price_IUSD[,iso3:=countrycode(sourcevar=M49,origin="un",destination = "iso3c")]
prod_price_IUSD<-prod_price_IUSD[iso3 %in% atlas_iso3]

# Remove Ethiopia PDR & Sudan (Former) & non-atlas countries
remove_countries<- c("Ethiopia PDR","Sudan (former)","Cabo Verde","Comoros","Mauritius","R\xe9union","Seychelles")

prod<-prod[!Area %in% remove_countries]
prod_price<-prod_price[!Area %in% remove_countries]
prod_price_IUSD<-prod_price_IUSD[!Area %in% remove_countries]

keep_years_prod<-paste0("Y",c(1998:2002,2015:2019))
keep_cols<-c("iso3","atlas_name",keep_years_prod)
prod<-prod[,..keep_cols]

keep_years_price<-paste0("Y",2015:2019)
keep_cols<-c("iso3","atlas_name",keep_years_price)

prod_price<-prod_price[,..keep_cols]
prod_price_IUSD<-prod_price_IUSD[,..keep_cols]

  # Add missing crops x countries to fao ####
  # Price
  fao_countries<-unique(prod_price[,list(iso3)])
  crops<-prod_price[,unique(atlas_name)]
  
  data<-prod_price
  keep_years<-keep_years_price
  
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
  prod_price<-rbind(prod_price,missing)
  
  data<-prod_price_IUSD
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
  prod_price_IUSD<-rbind(prod_price_IUSD,missing)
  
  # Production
  fao_countries<-unique(prod[,list(iso3)])
  data<-prod
  keep_years<-keep_years_prod
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
    # Price
    missing_countries<-atlas_iso3[!atlas_iso3 %in% prod_price$iso3]
    keep_years<-keep_years_price

    missing<-rbindlist(lapply( 1:length(missing_countries),FUN=function(i){
      data<-data.table(iso3=missing_countries[i],
                       atlas_name=prod_price[,unique(atlas_name)])
      data[,(keep_years):=NA]
      data
    }))
    
    prod_price<-rbind(prod_price,missing)
    
    # Price IUSD
    missing<-rbindlist(lapply( 1:length(missing_countries),FUN=function(i){
      data<-data.table(iso3=missing_countries[i],
                       atlas_name=prod_price_IUSD[,unique(atlas_name)])
      data[,(keep_years):=NA]
      data
    }))
    
    prod_price_IUSD<-rbind(prod_price_IUSD,missing)
    
    #Production
    missing_countries<-atlas_iso3[!atlas_iso3 %in% prod$iso3]
    
    if(length(missing_countries)>0){
    keep_years<-keep_years_prod
    
    missing<-rbindlist(lapply( 1:length(missing_countries),FUN=function(i){
      data<-data.table(iso3=missing_countries[i],
                       atlas_name=prod[,unique(atlas_name)])
      data[,(keep_years):=NA]
      data
    }))
    
    prod<-rbind(prod,missing)
    }
    
  # Average prices over 5-year period by country####
    prod_price[,mean:=mean(c(Y2019,Y2018,Y2017,Y2016,Y2015),na.rm=T),by=list(iso3,atlas_name)]
    prod_price_IUSD[,mean:=mean(c(Y2019,Y2018,Y2017,Y2016,Y2015),na.rm=T),by=list(iso3,atlas_name)]
  
  # Determine change in production 2000 to 2017 ####
  prod[,mean17:=mean(c(Y2019,Y2018,Y2017,Y2016,Y2015),na.rm=T),by=list(iso3,atlas_name)
       ][,mean00:=mean(c(Y1998,Y1999,Y2000,Y2001,Y2002),na.rm=T),by=list(iso3,atlas_name)
         ][,ratio:=mean17/mean00]
  
  # Create list of neighbouring countries ####
  african_neighbors <- list(
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
  
  # Add mean of neighbours ####
  # Price
  avg_neighbours<-function(iso3,crop,neighbours,data){
    neighbours<-african_neighbors[[iso3]]
    N<-data[atlas_name==crop & iso3 %in% neighbours,mean(mean,na.rm=T)]
    return(N)
  }
  
  prod_price[,mean_neighbours:=avg_neighbours(iso3=iso3,
                                              crop=atlas_name,
                                              neighbours=african_neighbors,
                                              data=copy(prod_price)),
             by=list(iso3,atlas_name)]
  
  
  # Production
  avg_neighbours<-function(iso3,crop,neighbours,data){
    neighbours<-african_neighbors[[iso3]]
    N<-data[atlas_name==crop & iso3 %in% neighbours,mean(ratio,na.rm=T)]
    return(N)
  }
  
  prod[,ratio_neighbours:=avg_neighbours(iso3=iso3,
                                         crop=atlas_name,
                                         neighbours=african_neighbors,
                                         data=copy(prod)),
       by=list(iso3,atlas_name)]
  
  # Create list of regions ####
  regions <- list(
    East_Africa = c("BDI", "COM", "DJI", "ERI", "ETH", "KEN", "MDG", "MUS", "MWI", "RWA", "SYC", "SOM", "SSD", "TZA", "UGA"),
    Southern_Africa = c("BWA", "LSO", "NAM", "SWZ", "ZAF", "ZMB", "ZWE","MOZ"),
    West_Africa = c("BEN", "BFA", "CPV", "CIV", "GMB", "GHA", "GIN", "GNB", "LBR", "MLI", "MRT", "NER", "NGA", "SEN", "SLE", "TGO"),
    Central_Africa = c("AGO", "CMR", "CAF", "TCD", "COD", "COG", "GNQ", "GAB", "STP"),
    North_Africa = c("DZA", "EGY", "LBY", "MAR", "SDN", "TUN")
  )

  # Add mean of regions ####
  # Price
  avg_regions<-function(iso3,crop,regions,data){
    region_focal<-names(regions)[sapply(regions,FUN=function(X){iso3 %in% X})]
    neighbours<-regions[[region_focal]]
    N<-data[atlas_name==crop & iso3 %in% neighbours,mean(mean,na.rm=T)]
    return(N)
  }
  
  
  prod_price[,mean_region:=avg_regions(iso3=iso3,
                                       crop=atlas_name,
                                       regions=regions,
                                       data=copy(prod_price)),
             by=list(iso3,atlas_name)]
  
  # Production
  avg_regions<-function(iso3,crop,regions,data){
    region_focal<-names(regions)[sapply(regions,FUN=function(X){iso3 %in% X})]
    neighbours<-regions[[region_focal]]
    N<-data[atlas_name==crop & iso3 %in% neighbours,mean(ratio,na.rm=T)]
    return(N)
  }
  
  prod[,ratio_region:=avg_regions(iso3=iso3,
                                 crop=atlas_name,
                                 regions=regions,
                                 data=copy(prod)),
             by=list(iso3,atlas_name)]
  
  # Add continental average ####
  prod_price[,mean_continent:=mean(mean,na.rm=T),by=atlas_name]
  prod[,ratio_continent:=mean(ratio,na.rm=T),by=atlas_name]
  
  # Add composite value ####
  prod_price[,mean_final:=mean
             ][is.na(mean_final),mean_final:=mean_neighbours
               ][is.na(mean_final),mean_final:=mean_region
                 ][is.na(mean_final),mean_final:=mean_continent]
  
  prod[,ratio_final:=ratio
       ][is.na(ratio_final),ratio_final:=ratio_neighbours
         ][is.na(ratio_final),ratio_final:=ratio_region
           ][is.na(ratio_final),ratio_final:=ratio_continent]
  
  # Save processed faostat data ####
  fwrite(prod_price,paste0(fao_dir,"/production_value_processed.csv"))
  fwrite(prod,paste0(fao_dir,"/production_processed.csv"))
  
# Convert faostat data into a raster ####
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

# Inflate 2000 to 2017 ####
  
# Reorder layers to match
layer_order<-names(prod_05)
prod_rast<-prod_rast[[layer_order]]

# extract pixel values
pix_vals<-base_rast
pix_vals[]<-1:ncell(pix_vals)

pix_vals_ext<-data.table(extract(pix_vals,geoboundaries))
pix_vals_ext$ID<-geoboundaries$iso3[pix_vals_ext$ID]
pix_vals_ext$N<-1:nrow(pix_vals_ext)
colnames(pix_vals_ext)[2]<-"pix_val"

# extract production
data<-data.table(extract(prod_05,geoboundaries))
data$ID<-geoboundaries$iso3[data$ID]
data$N<-1:nrow(data)
data$pix_val<-pix_vals_ext$pix_val

# Subset faostat production data for national checks
prod_ss<-prod[,list(iso3,atlas_name,mean00,mean17,Y2000,Y2017)]
setnames(prod_ss,c("iso3","atlas_name"),c("country","item"))

# Save simplified production dataset
fwrite(prod_ss,file = paste0(fao_dir,"/fao_summary.csv"))

item_country<-prod[,expand.grid(item=unique(atlas_name),country=unique(iso3),stringsAsFactors =F)]

# Adjust production values per item x country
verbose<-F
data_ex<-rbindlist(lapply(1:nrow(item_country),FUN=function(i){
  
  country<-item_country$country[i]
  item<-item_country$item[i]
  
  # Display progress
  message<-paste0("Row:",i," | ",country,"/",item)
  
  if(verbose==F){
  cat('\r                                                                                                                                                 ')
  cat('\r',message)
  flush.console()
  }else{
    print(message)
  }
  
  
  cols<-c("N","pix_val",item)
  Y<-data[ID==country,..cols]
  colnames(Y)[3]<-"value"
  
  # Sort values in increasing order
  Y<-Y[order(value)]
  
  
  # ratio by which we need to inflate production by
    # NOTE: we could simply use Y_sum (lps production in 2000) and fao production in 2017 instead of a ratio derived from
    # fao stat to cut out the fao validation steps below. However this would not work for countries where we do have production
    # data and we cannot infer this from neighbouring countries.
  ratio<-round(unlist(prod_ratio_cast[iso3==country,..item]),3)
  
  # Production in 2000
  Y_sum<-Y[,sum(value,na.rm=T)]
  
  # calculate amount  we need to add to get to 2017
  Y_add<-Y_sum*ratio-Y_sum
  
  # Adjust total to match 2017 fao stat data where FAO production data exist (note lps in 2000 vs faostat does not always match well)
  fao17<-prod_ss[item==item_country$item[i] & country==item_country$country[i],Y2017]
  
  if(!is.na(fao17)){
    Y_add<-fao17-Y_sum
  }
  
  if(is.na(fao17)){
    fao17<-prod_ss[item==item_country$item[i] & country==item_country$country[i],mean17]
    if(!is.na(fao17)){
      Y_add<-fao17-Y_sum
    }
  }
  
  # Add cumulative value and cumulative proportion
  Y[,value_cum:=cumsum(value)][,value_cum_prop:=value_cum/sum(value,na.rm=T)]
  
  # Limit the values we are adding to those with cumulative proportion above 5% (we exclude the low tail of the values)
  Z<-Y[value_cum_prop>0.05,list(N,value)]
  
  if(nrow(Z)>0){
  
    # If value is positive distribute increase biased towards intermediate production
  if(Y_add>0){
    # Work out the range of the remaining data
    Z_range<-Z[,range(value)]
    
    # Create a normal distribution based on the centre of the range 
    Z_rdist<-rnorm(n=nrow(Z),mean=mean(Z_range),sd=mean(Z_range)*0.33)
    # Values cannot be negative,set to zero
    Z_rdist[Z_rdist<0]<-0
    
    # Truncate high values
    max<-tail(quantile(Z_rdist,probs=seq(0,1,0.1)),2)[1]
    
    N<-Z_rdist[Z_rdist>max]
    Z_rdist[Z_rdist>max]<-rnorm(n=length(N),mean=max,sd=max/100)
    
    # Rescale to amount that needs to be added
    Z_rdist<-Z_rdist*(Y_add/sum(Z_rdist))
    
    # Order so that tails are low values and mid point is high values
    Z_rdist<-sort(Z_rdist)
    Z_rdist_odd<-Z_rdist[seq(1,length(Z_rdist),2)]
    Z_rdist_even<-Z_rdist[seq(2,length(Z_rdist),2)]
    Z_to_add<-c(Z_rdist_odd,rev(Z_rdist_even))
    
    # Add adjustment values back to original table and the combine with the starting values
    Y[,to_add:=0
      ][value_cum_prop>0.05,to_add:=Z_to_add
        ][,value_adj:=value+to_add]
    
  }else{
    # If value is negative reduce production evenly across production
    red_ratio<-(Y_add+Y_sum)/Y_sum
    Y[,to_add:=value*red_ratio
      ][,value_adj:=value+to_add]
  }
    
    Y[,item:=item][,country:=country]
    

  if(verbose==T){
    print(Y[,c(country=country[1],item=item[1],ratio=ratio,intial_value=round(sum(value,na.rm=T),0),to_add_calc=round(as.numeric(Y_add),0),to_add=round(sum(to_add,na.rm=T),0))])
  }
  
  Y
  }else{
    NULL
  }

}))

# Check national totals against FAOstat
data_sum<-data_ex[,list(prod00=sum(value,na.rm=T),prod_adj17=sum(value_adj,na.rm=T)),by=list(item,country)]
data_sum<-merge(data_sum,prod_ss,all.x=T,by=c("item","country"))

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
  
# IUSD - to do
