# ==============================================================================
# Script Title: Estimate and Allocate Crop Value of Production Across Africa
# Author(s): Peter Steward, African Agricultural Adaptation Atlas
# Affiliation: Alliance of Bioversity International and CIAT
# Date Created: 2024-05-30
# Last Updated: 2025-07-14
# ==============================================================================

# Description:
# This script estimates the national Value of Production (VoP) for SPAM crops in 
# Sub-Saharan Africa using FAOSTAT production, price, and value data. It then 
# allocates these national VoP values spatially by combining them with SPAM 
# gridded production rasters, disaggregated by technology (subsistence, low, 
# high, irrigated). Price gaps are filled using a fallback hierarchy that draws 
# from national time series, neighboring countries, regional and global medians.

# Key Steps:
# 1. Load base raster and admin boundaries used across the Atlas.
# 2. Load and clean SPAM crop codes and FAOSTAT name mappings.
# 3. Read FAO economic data (production value, prices, production volume) 
#    for Africa and World totals, in both constant USD and I$.
# 4. Merge economic data by crop and country; clean anomalies manually (e.g., tea).
# 5. Estimate nominal USD price per tonne for key time windows (e.g., 2015, 2021) 
#    using national FAOSTAT values, fallback substitution, and global benchmarks.
# 6. Apply final price surfaces to SPAM production rasters to create crop-level 
#    VoP rasters by farming system (4 SPAM technologies × ~30 crops).
# 7. Output national and gridded VoP rasters in nominal USD per pixel.

# Input Datasets:
# - FAOSTAT CSVs: Production, Value of Production, Prices (Africa + World)
# - SPAM 2010 crop production rasters (by crop and technology)
# - Crop name mappings: SPAM codes to FAOSTAT names
# - Atlas base raster
# - Sub-Saharan admin boundaries (GeoArrow/Parquet)

# Output Files:
# - `crop_price_nominal-usd-2015-t.tif` — Estimated 2015 prices per crop (USD/tonne)
# - `variable=vop_nominal-usd-2015/spam_vop_nominal-usd-2015_<tech>.tif` — Gridded VoP rasters
# - `crop_price_nominal-usd-2021-t.tif` — Estimated 2021 prices per crop
# - `spam_vop_nominal-usd-2021_<tech>.tif` — VoP per pixel (2021 prices × SPAM production)
# - Intermediate tables for FAO–SPAM merging, price inference, and QA

# Notes:
# - VoP is calculated in nominal USD using FAOSTAT production × inferred price.
# - Regional and global price medians are used where country-level data are missing.
# - Coffee and millet prices are split manually into arabica/robusta and pearl/small millet.
# - Output rasters are aligned to the Atlas base raster for downstream harmonization.

# ==============================================================================

## 0 - functions and libraries
pacman::p_load(terra,geoarrow,arrow,countrycode,data.table)
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

## 1 - Read and subset initial data ####

  ### 1.1 Base Raster ####
  
  # The base_rast_path value is set in script 0, however we have not yet implemented this workflow globally so we have hardcoded in 
  # the Atlas Africa base raster
  # base_rast<-terra::rast(base_rast_path)

  base_rast<-terra::rast("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/base_raster.tif")

  ### 1.2 Admin Boundaries #####
file<-geo_files_local[1]
geoboundaries<-read_parquet(file)
geoboundaries <- geoboundaries |> sf::st_as_sf() |> terra::vect()
geoboundaries <- aggregate(geoboundaries, "iso3")  
  
  ### 1.3 Processing constants #####
  remove_countries <- c("Ethiopia PDR", "Sudan (former)", "Cabo Verde", "Comoros", "Mauritius", "R\xe9union", "Seychelles")
  atlas_iso3<-geoboundaries$iso3
  target_year<-c(2009:2023)

  ### 1.4 Load SPAM codes #####
  path_spamCode <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SpamCodes.csv"
  ms_codes <- data.table::fread(path_spamCode)[, Code := toupper(Code)][!is.na(Code)][,code_low:=tolower(Code)]
  crops <- tolower(ms_codes[compound == "no", Code])
  
  ### 1.5 Load file for translation of spam to FAO stat names/codes #####
  spam2fao <- fread("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SPAM2010_FAO_crops.csv")[
    short_spam2010 %in% crops & name_fao != "Mustard seed" &
      !(short_spam2010 %in% c("rcof", "smil", "pmil", "acof"))
  ]
  
  spam2fao[short_spam2010 == "rape", name_fao_val := "Rape or colza seed"]
  
  spam2fao_formatted <- c(
    setNames(spam2fao$name_fao_val, spam2fao$short_spam2010),
    setNames(c("Coffee, green", "Millet"), c("coff", "mill"))
  )
   
  ### 1.6 Load Value of Production data from FAO #####
    #### 1.6.1) cusd15 ####
      #### 1.6.1.1) Africa ####
      element<-"Gross Production Value (constant 2014-2016 thousand US$)"
      value_name<-"value_cusd15"
      path_vop_africa_fao <- file.path(fao_dir, "Value_of_Production_E_Africa.csv")
      
      prod_value_africa <- fread(path_vop_africa_fao)
      
      prod_value_usd_africa_fao <- prepare_fao_data(
        file = path_vop_africa_fao,
        spam2fao_formatted,
        elements = element,
        remove_countries = remove_countries,
        keep_years = target_year,
        atlas_iso3 = atlas_iso3
      )
      
      prod_value_usd_africa_fao<-melt(prod_value_usd_africa_fao,id.vars=c("iso3","atlas_name"),value.name = value_name,variable.name = "year")
      
      #### 1.6.1.2) World ####
      path_vop_world_fao <- file.path(fao_dir, "Value_of_Production_E_All_Area_Groups.csv")
      value_usd_world <- fread(path_vop_world_fao)
      value_usd_world <- value_usd_world[Area == "World" & Element == element & Item %in% spam2fao_formatted, c("Item",paste0("Y",target_year)),with=F]
      prod_value_usd_world_fao <- merge(value_usd_world, data.table(Item = spam2fao_formatted, atlas_name = names(spam2fao_formatted)), all.x = T)
      prod_value_usd_world_fao<-melt(prod_value_usd_world_fao[,!"Item"],id.vars=c("atlas_name"),value.name = value_name,variable.name = "year")
      
    #### 1.6.2) intd15 ####
      #### 1.6.2.1) Africa ####
      element<-"Gross Production Value (constant 2014-2016 thousand I$)"
      value_name<-"value_intd15"
      
      prod_value_intd_africa_fao <- prepare_fao_data(
        file = path_vop_africa_fao,
        spam2fao_formatted,
        elements = element,
        remove_countries = remove_countries,
        keep_years = target_year,
        atlas_iso3 = atlas_iso3
      )
      
      prod_value_intd_africa_fao<-melt(prod_value_intd_africa_fao,id.vars=c("iso3","atlas_name"),value.name = value_name,variable.name = "year")
      
      #### 1.6.2.2) World ####
      path_vop_world_fao <- file.path(fao_dir, "Value_of_Production_E_All_Area_Groups.csv")
      value_intd_world <- fread(path_vop_world_fao)
      value_intd_world <- value_intd_world[Area == "World" & Element == element & Item %in% spam2fao_formatted, c("Item",paste0("Y",target_year)),with=F]
      prod_value_intd_world_fao <- merge(value_intd_world, data.table(Item = spam2fao_formatted, atlas_name = names(spam2fao_formatted)), all.x = T)
      prod_value_intd_world_fao<-melt(prod_value_intd_world_fao[,!"Item"],id.vars=c("atlas_name"),value.name = value_name,variable.name = "year")
      
  ### 1.7 Load Producer Prices #####
    #### 1.7.1) Africa ####
  
  path_prices_fao <- file.path(fao_dir, "Prices_E_Africa_NOFLAG.csv")
  
  prod_price_africa_fao <- prepare_fao_data(
    file = path_prices_fao,
    spam2fao_formatted,
    elements = "Producer Price (USD/tonne)",
    remove_countries = remove_countries,
    keep_years = target_year, 
    atlas_iso3 = atlas_iso3
  )
  
  prod_price_africa_fao<-melt(prod_price_africa_fao,id.vars=c("iso3","atlas_name"),value.name = "price_usd",variable.name = "year")
  
  # 1.7.1.1) Filter out weird values ####
  prod_price_africa_fao[atlas_name=="sugb" & iso3=="NER",price_usd:=NA]
  prod_price_africa_fao[atlas_name=="teas" & iso3 %in% c("RWA","BDI"),price_usd:=NA]
  prod_price_africa_fao[atlas_name=="toba" & iso3=="SLE",price_usd:=NA]
  prod_price_africa_fao[atlas_name=="cowp" & iso3 %in% c("GNB"),price_usd:=NA]
  prod_price_africa_fao[atlas_name=="coco" & iso3 %in% c("GIN"),price_usd:=NA]
  prod_price_africa_fao[atlas_name=="oilp" & iso3 %in% c("BDI"),price_usd:=NA]
  
      #### 1.7.2) World ####
  
  prod_price_world<-fread(fao_econ_file_world)
  prod_price_world<-prod_price_world[Element=="Producer Price (USD/tonne)"]
  prod_price_world[, M49 := as.numeric(gsub("[']", "", `Area Code (M49)`))]
  prod_price_world[, iso3 := countrycode(sourcevar = M49, origin = "un", destination = "iso3c")]
  prod_price_world <- merge(prod_price_world, data.table(Item = spam2fao_formatted, atlas_name = names(spam2fao_formatted)),by="Item", all.x = T)
  
  prod_price_world<-prod_price_world[!is.na(atlas_name),.(iso3,atlas_name,Year,Value)]
  setnames(prod_price_world,c("Value","Year"),c("price_usd","year"))
  
  ### 1.8 Load FAO production estimates #####
    #### 1.8.1 Africa #####
  
  path_prod_africa_fao <- file.path(fao_dir, "Production_Crops_Livestock_E_Africa_NOFLAG.csv")
  
  prod_ton_africa_fao <- prepare_fao_data(
    file = path_prod_africa_fao,
    spam2fao_formatted,
    elements = "Production",
    units = "t",
    remove_countries = remove_countries,
    keep_years = target_year,
    atlas_iso3 = atlas_iso3
  )
  
  prod_ton_africa_fao<-melt(prod_ton_africa_fao,id.vars=c("iso3","atlas_name"),value.name = "production_t",variable.name = "year")

    #### 1.8.2 World #####
  prod_prod_world_fao <- file.path(fao_dir, "Production_Crops_Livestock_E_All_Area_Groups.csv")
  
  prod_world <- fread(prod_file_world)
  prod_world <- prod_world[Area == "World" & Element == "Production" & Item %in% spam2fao_formatted & Unit == "t", c("Item",paste0("Y",target_year)),with=F]
  prod_ton_world_fao <- merge(prod_world, data.table(Item = spam2fao_formatted, atlas_name = names(spam2fao_formatted)), all.x = T)
  
  prod_ton_world_fao<-melt(prod_ton_world_fao[,!"Item"],id.vars=c("atlas_name"),value.name = "production_t",variable.name = "year")
  
  ### 1.9) Merge datasets ####
  prod_merge<-merge(prod_value_usd_africa_fao,prod_value_intd_africa_fao,all.x=T)
  prod_merge<-merge(prod_merge,prod_price_africa_fao,all.x=T)
  prod_merge<-merge(prod_merge,prod_ton_africa_fao,all.x=T)
  prod_merge[,year:=as.integer(gsub("Y","",year))]
  
## 2) Load spam Production data ######
spam_files<-data.table(path=list.files(file.path(mapspam_pro_dir,"variable=prod_t"),full.names = T))
spam_files<-spam_files[grepl("tif$",path)]
spam_files[,variable:=tstrsplit(basename(path),"_",keep=2)
           ][,tech:=gsub(".tif","",tstrsplit(basename(path),"_",keep=4)),by=.I]

prod_rast<-lapply(spam_files$path,rast)
names(prod_rast)<-spam_files$tech

## 3) Infer missing prices ####
# Ok so now we want to estimate a sensible nominal value in usd from the most recent data available
# We will use production amount and price to get this as conversion of iusd15 to nominal usd seems to give unrealistically low values
year_sets<-list(y2021=2019:2023,y2015=2014:2016,y2020=2019:2020)

price_usd_list<-lapply(1:length(year_sets),function(i){
  ymin<-min(year_sets[[i]])
  ymax<-max(year_sets[[i]])
  # Median values for the year set
  prod_merge_recent<-prod_merge[year %in% year_sets[[i]],.(price_usd=median(price_usd,na.rm=T),
                                                           production_t=median(production_t,na.rm=T)),
                                .(atlas_name,iso3)]
  
  # Values are often missing, is there a value from a longer time series?
  price_any<-prod_merge[year %in% (ymin-5):ymax,.(price_median=median(price_usd,na.rm=T),
                                              price_min=min(price_usd,na.rm=T),
                                              price_max=max(price_usd,na.rm=T),
                                              price_tail=tail(price_usd[!is.na(price_usd)],1),
                                              price_n=sum(!is.na(price_usd))),
                        .(atlas_name,iso3)]
  
  # Merge median price from longer time-series
  prod_merge_recent<-merge(prod_merge_recent,price_any[,.(price_median,iso3,atlas_name)],all.x=T)
  
  prod_merge_recent<-add_nearby(data=prod_merge_recent,group_field="atlas_name",value_field = "price_usd",neighbors=african_neighbors,regions=regions)
  
  prod_price_global_recent<-prod_price_world[year %in% year_sets[[i]],.(price_usd_global=median(price_usd,na.rm=T)),.(atlas_name)]
  
  prod_merge_recent<-merge(prod_merge_recent,prod_price_global_recent,by="atlas_name",all.x=T)
  
  unique(prod_merge_recent[,.(atlas_name,price_usd_continent,price_usd_global)])
  
  prod_merge_recent[,price_usd_final:=price_usd
  ][is.na(price_usd_final),price_usd_final:=price_median
  ][is.na(price_usd_final),price_usd_final:=price_usd_neighbors
  ][is.na(price_usd_final),price_usd_final:=price_usd_region
  ][is.na(price_usd_final),price_usd_final:=price_usd_continent
  ][is.na(price_usd_final),price_usd_final:=price_usd_global
  ][,vop_usd_nominal:=production_t*price_usd_global
  ][,year:=names(year_sets)[i]]
  
  # Hack tea
  if(ymax==2023){
    tea_median<- prod_merge_recent[atlas_name=="teas",mean(price_median,na.rm=T)]
    prod_merge_recent[atlas_name=="teas" & price_usd_final<600,price_usd_final:=tea_median]
  }
  
  prod_merge_recent
})

names(price_usd_list)<-paste0("nominal-usd-",gsub("y","",names(year_sets)))

## 4) Multiply mapspam production by price ####

for(i in 1:length(price_usd_list)){
  cat("Running period i =",i,"/",length(price_usd_list),"           \n")
  
  # Unit is t x usd/t = usd or 1000 intdlr x 1000 there should be no need for any unit conversions (e.g. x 1000)
  final_price<-copy(price_usd_list[[i]])
  setnames(final_price,"price_usd_final","value",skip_absent=T)
  
  # Duplicate coffee prices for arabica and robusta (they are lumped in faostat)
  final_price<-final_price[,list(iso3,atlas_name,value)]
  robusta<-final_price[atlas_name=="coff"][,atlas_name:="rcof"]
  final_price[atlas_name=="coff",atlas_name:="acof"]
  final_price<-rbind(final_price,robusta)
  
  # Duplicate millet prices for pearl millet and small millet (they are lumped in faostat)
  final_price<-final_price[,list(iso3,atlas_name,value)]
  pearl<-final_price[atlas_name=="mill"][,atlas_name:="pmil"]
  final_price[atlas_name=="mill",atlas_name:="smil"]
  final_price<-rbind(final_price,pearl)
  
  final_price<-merge(final_price,ms_codes[,.(code_low,Fullname)],by.x="atlas_name",by.y="code_low",all.x=T)
  
  no_match<-final_price[is.na(Fullname)]
  if(nrow(no_match)>0){
    stop("Some crop names are not matching to ms_codes: " ,no_match[,paste0(unique(atlas_name),collapse=", ")])
  }
  
  final_price_cast<-dcast(final_price,iso3~Fullname)
  
  # Convert value to vector then raster
  final_price_vect<-geoboundaries
  final_price_vect<-merge(final_price_vect,final_price_cast,all.x=T)
  
  crop_names<-sort(names(final_price_cast)[-1])
  
  final_price_rast<-terra::rast(lapply(crop_names,FUN=function(NAME){
    terra::rasterize(final_price_vect,base_rast,field=NAME)
  }))
  names(final_price_rast)<-crop_names
  
  price_save_file<-file.path(mapspam_pro_dir,"fao_prices",paste0("crop_price_",names(price_usd_list)[i],"-t.tif"))
  ensure_dir(dirname(price_save_file))
  terra::writeRaster(final_price_rast,price_save_file,overwrite=T)
      
  # Multiply national VoP by glw cell proportion
  for(j in 1:length(prod_rast)){
    cat("Running period i =",i,"/",length(price_usd_list),"| spam system j =",j,"/",length(prod_rast),"           \n")
    prod_rast_focus<-prod_rast[[j]]
    spam_names<-names(prod_rast_focus)
    name_check<-!crop_names %in% spam_names
    if(sum(name_check)>0){
    stop("Non match in crop names between fao and spam:",crop_names[!name_check])
    }
    
    prod_rast_focus<-prod_rast_focus[[crop_names]]
  
    if(sum(names(prod_rast_focus) != names(final_price_rast))>0){
      stop("Check order of rasters in price x production multiplication")
    }
    
    prod_vop<-prod_rast_focus*final_price_rast
    prod_vop<-round(prod_vop,0)
    
    save_file<-file.path(mapspam_pro_dir,
                         paste0("variable=vop_",names(price_usd_list)[i]),
                         paste0("spam_vop_",names(price_usd_list)[i],"_",names(prod_rast)[j],".tif"))
    ensure_dir(dirname(save_file))
    terra::writeRaster(prod_vop,save_file,overwrite=T)
  }
}
