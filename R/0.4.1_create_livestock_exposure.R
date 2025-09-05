# ==============================================================================
# Script Title: Estimate and Allocate Livestock VoP Across Sub-Saharan Africa
# Author(s): Peter Steward, African Agricultural Adaptation Atlas
# Affiliation: Alliance of Bioversity International and CIAT
# Date Created: 2023-09-11
# Last Updated: 2025-07-08
# ==============================================================================

# Description:
# This script estimates the Value of Production (VoP) of key livestock systems 
# (cattle, goats, sheep, pigs, poultry) across Sub-Saharan Africa at subnational 
# resolution. It combines gridded livestock population data (GLW4) with FAOSTAT 
# production, price, and value data, as well as World Bank exchange rate and PPP 
# statistics. It distributes national-level VoP to raster surfaces using spatial 
# disaggregation and then splits values between highland and tropical zones.

# Key Steps:
# 1. Load base raster, national boundaries, and GLW4 livestock population data (2015).
# 2. Resample and mask GLW layers to a standard resolution and target geography.
# 3. Aggregate livestock counts by country and map to ERA-Atlas naming conventions.
# 4. Load and clean FAOSTAT data on production, prices, and gross production value 
#    in both constant USD and international dollars (I$), filling gaps via:
#       - Neighboring country substitution
#       - Regional and global averages
#       - Hybrid VoP estimation strategies
# 5. Merge with PPP/XRAT data to enable unit conversion between currencies.
# 6. Allocate VoP (in USD and I$) to raster surfaces based on proportional livestock 
#    density per pixel.
# 7. Generate masks to split VoP into highland and tropical components.
# 8. Output final VoP rasters and livestock number rasters per animal class and zone.
# 9. QA/QC: Compare gridded results to national FAOSTAT values.

# Input Datasets:
# - GLW4 livestock distribution (5 species, 2015 or 2020; FAO)
# - FAOSTAT Production, Prices, Value of Production CSVs (various)
# - World Bank PPP & XRAT indicators (wbstats)
# - Atlas standard raster template
# - National boundaries (Parquet or GeoArrow format)
# - Optional: highland mask raster (binary)

# Output Files:
# - `livestock_vop_usd2015.tif` — VoP per pixel in constant USD 2014–16, split by zone
# - `livestock_vop_intld2015.tif` — VoP per pixel in constant I$ 2014–16, split by zone
# - `livestock_number_number.tif` — Livestock counts per pixel (5 classes + TLU)
# - `shoat_prop.tif` — Proportion of sheep vs goats per pixel
# - Multiple intermediate and QA raster products for comparison to FAO totals

# Notes:
# - FAOSTAT data must be pre-downloaded in CSV format and saved in the `fao_dir`.
# - Only countries with available GLW4 and Atlas-compatible boundaries are processed.
# - Output rasters are aligned to the `base_raster` for interoperability with other Atlas data.

# ==============================================================================

# a) Load R functions & packages ####
pacman::p_load(terra,data.table,httr,countrycode,wbstats,arrow,geoarrow,ggplot2,dplyr,tidyr,pbapply)

# Load functions & wrappers
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))
options(scipen=999)

# b) Load base raster ####
base_rast<-terra::rast(atlas_data$base_rast$atlas_delta$local_path)

# 1) Load geographies ####
file<-geo_files_local[1]
geoboundaries<-read_parquet(file)
geoboundaries <- geoboundaries |> sf::st_as_sf() |> terra::vect()
geoboundaries <- aggregate(geoboundaries, "iso3")  

# 2) Load GLW4 data ####
glw_codes<-c(poultry="Ch",sheep="Sh",pigs="Pg",horses="Ho",goats="Gt",ducks="Dk",buffalo="Bf",cattle="Ct")

  ## 2.0.1) Run - 2020 #### 
  glw_dir<-atlas_dirs$data_dir$GLW4_2020
  glw_int_dir<-glw2020_int_dir
  glw_pro_dir<-glw2020_pro_dir
  dataset_name<-"glw4-2020"
  
  glw_files<-list.files(glw_dir,".tif$",full.names = T)
  glw<-terra::rast(glw_files)
  glw_names<-unlist(tstrsplit(names(glw),"_",keep=1))
  glw_names<-names(glw_codes)[match(glw_names,glw_codes)]
  names(glw)<-glw_names
  glw<-glw[[c("poultry","sheep","pigs","goats","cattle")]]
  
  ## 2.0.2) Not Run - 2015 #### 
  if(F){
  # Note that GLW4 data is for the year 2015
  # file suffix _da =  dysymmetric, unit = total animals per pixel
  
  glw_codes<-c(poultry=6786792,sheep=6769626,pigs=6769654,horses=6769681,goats=6769696,ducks=6769700,buffalo=6770179,cattle=6769711)
  
  glw_files <- file.path(atlas_dirs$data_dir$GLW4,paste0("5_",glw_names,"_2015_Da.tif"))
  glw<-terra::rast(glw_files)
  names(glw)<-names(glw_names)
  glw<-glw[[c("poultry","sheep","pigs","goats","cattle")]]
  }
  
  ## 2.1) Resample & mask to atlas area #####
#### units are absolute number of animals per pixel
# convert to density
glw<-glw/terra::cellSize(glw,unit="ha")
# resample to the atlas base raster
glw<-terra::resample(glw,base_rast)
# convert back to numbers per pixel
glw<-glw*terra::cellSize(glw,unit="ha")
# mask to focal countries
glw<-terra::mask(glw,geoboundaries)

  ## 2.2) Extract by admin0 #####
  glw_admin0<-terra::extract(glw,geoboundaries,fun="sum",na.rm=T)
  glw_admin0$iso3<-geoboundaries$iso3
  glw_admin0$ID<-NULL
  glw_admin0<-melt(data.table(glw_admin0),id.vars="iso3",variable.name = "glw3_name",value.name="glw3_no")
  
  ## 2.3) map GLW values to atlas #####
  glw2atlas<-list(poultry=grep("poultry",names(lps2fao ),value=T),
                  sheep=grep("sheep",names(lps2fao ),value=T),
                  pigs=grep("pig",names(lps2fao ),value=T),
                  goats=grep("goat",names(lps2fao ),value=T),
                  cattle=grep("cattle",names(lps2fao ),value=T))
  
  glw2atlas <- data.table(
    glw3_name = rep(names(glw2atlas), sapply(glw2atlas, length)),
    atlas_name = unlist(glw2atlas, use.names = FALSE)
  )

  # 2.4) Load high/low mask ####
  mask_ls_file<-paste0(glw_int_dir,"/livestock_masks.tif")
  overwrite_glw<-F
  if(!file.exists(mask_ls_file)|overwrite_glw==T){
    lus<-c(glw$cattle*0.7,glw$poultry*0.01,glw$goats*0.1,glw$pigs*0.2,glw$sheep*0.1)
    lus<-c(lus,sum(lus,na.rm=T))
    names(lus)<-c("cattle","poultry","goats","pigs","sheep","total")
    lus<-terra::mask(terra::crop(lus,geoboundaries),geoboundaries)
    
    # resample to 0.05
    lu_density<-lus/terra::cellSize(lus,unit="ha")
    lu_density<-terra::resample(lu_density,base_rast)
    
    # Classify into binary mask
    livestock_mask <- terra::ifel(lu_density > 0, 1, 0)
    
    # Split mask by highland vs tropical areas
    
    # Load highland mask
    highlands<-terra::rast(afr_highlands_file)
    highlands<-terra::resample(highlands,base_rast,method="near")
    
    livestock_mask_high<-livestock_mask*highlands
    names(livestock_mask_high)<-paste0( names(livestock_mask_high),"_highland")
    
    lowlands<-classify(highlands,data.frame(from=c(0,1),to=c(1,0)))
    livestock_mask_low<-livestock_mask*lowlands
    names(livestock_mask_low)<-paste0( names(livestock_mask_low),"_tropical")
    
    livestock_mask<-c(livestock_mask_high,livestock_mask_low)
    
    terra::writeRaster(livestock_mask,filename=mask_ls_file,overwrite=T)
    
  }else{
    livestock_mask<-terra::rast(mask_ls_file)
    livestock_mask_high<-livestock_mask[[grep("highland",names(livestock_mask))]]
    livestock_mask_low<-livestock_mask[[!grepl("highland",names(livestock_mask))]]
  }
  
  ## 2.5) Convert glw3 pixels to proportion of national total ####
  glw_admin0_cast<-dcast(glw_admin0,iso3~glw3_name,value.var="glw3_no")
  glw_vect<-merge(geoboundaries,glw_admin0_cast,all.x=T,by="iso3")
  
  glw_rast<-terra::rast(lapply(unique(glw2atlas$glw3_name),FUN=function(NAME){
    terra::rasterize(glw_vect,base_rast,field=NAME)
  }))
  
  names(glw_rast)<-unique(glw2atlas$glw3_name)
  
  glw_prop<-glw/glw_rast
  
# 3) Load FAOstat data ####
remove_countries<- c("Ethiopia PDR","Sudan (former)","Cabo Verde","Comoros","Mauritius","R\xe9union","Seychelles")
atlas_iso3<-geoboundaries$iso3
target_year<-c(2014:2023)

  ## 3.1) VoP #####
  vop_file<-file.path(fao_dir,"Value_of_Production_E_Africa.csv")
  vop_file_world<-file.path(fao_dir,"Value_of_Production_E_All_Area_Groups.csv")
  
  # use indigenous meat production values
  lps2fao_ind<-lps2fao
  lps2fao_ind[grep("Meat",lps2fao_ind)]<-paste0(lps2fao_ind[grep("Meat",lps2fao_ind)]," (indigenous)")

    ### 3.1.1) Production value - constant 2014-16 $US#####
    prod_value<-fread(vop_file)
    prod_value[grep(paste(lps2fao,collapse="|"),Item),unique(Item)]

    # Choose element
    prod_value[,unique(Element)]
    element<-"Gross Production Value (constant 2014-2016 thousand US$)"
    # Note current thousand US$ has only 35 values whereas constant 12-16 has 157
    
    prod_value_usd<-unique(prepare_fao_data(file=vop_file,
                                 lps2fao_ind,
                                 elements=element,
                                 remove_countries = remove_countries,
                                 keep_years=target_year,
                                 atlas_iso3=atlas_iso3))
    
    prod_value_usd[,atlas_name:=gsub(" (indigenous)","",atlas_name)]
    
    prod_value_usd<-melt(prod_value_usd,id.vars=c("iso3","atlas_name"),value.name = "value_cusd15",variable.name = "year")
    
    
    prod_value_usd_world<-fread(vop_file_world)
    prod_value_usd_world<-prod_value_usd_world[Area=="World" & Element == element & Item %in% lps2fao_ind,c("Item",paste0("Y",target_year)),with=F]
    prod_value_usd_world<-merge(prod_value_usd_world,data.table(Item=lps2fao_ind,atlas_name=names(lps2fao_ind)),all.x=T)
   
    prod_value_usd_world<-melt(prod_value_usd_world[,!"Item"],id.vars=c("atlas_name"),value.name = "value_cusd15",variable.name = "year")
    
    ### 3.1.2) VoP - constant 2014-16 $I #####
    prod_value_i<-fread(vop_file)
    prod_value_i[grep(paste(lps2fao,collapse="|"),Item),unique(Item)]
    
    # Choose element
    prod_value_i[,unique(Element)]
    element<-"Gross Production Value (constant 2014-2016 thousand I$)"
    # Note current thousand US$ has only 35 values whereas constant 12-16 has 157
    
    prod_value_i<-unique(prepare_fao_data(file=vop_file,
                                 lps2fao_ind,
                                 elements=element,
                                 remove_countries = remove_countries,
                                 keep_years=target_year,
                                 atlas_iso3=atlas_iso3))
    
    prod_value_i[,atlas_name:=gsub(" (indigenous)","",atlas_name)]
    
    prod_value_i<-melt(prod_value_i,id.vars=c("iso3","atlas_name"),value.name = "value_intd15",variable.name = "year")
    
    prod_value_i_world<-fread(vop_file_world)
    prod_value_i_world<-prod_value_i_world[Area=="World" & Element == element & Item %in% lps2fao_ind,c("Item",paste0("Y",target_year)),with=F]
    prod_value_i_world<-merge(prod_value_i_world,data.table(Item=lps2fao_ind,atlas_name=names(lps2fao_ind)),all.x=T)
    prod_value_i_world<-melt(prod_value_i_world[,!"Item"],id.vars=c("atlas_name"),value.name = "value_intd15",variable.name = "year")
    
  ## 3.2) Price #####
    econ_file<-file.path(fao_dir,"Prices_E_Africa_NOFLAG.csv")
    prod_price<-fread(econ_file)
    prod_price[grep("Meat of goat",Item),unique(Item)]
    prod_price[grep(paste(lps2fao,collapse="|"),Item),unique(Item)]
    
    prod_price[,unique(Element)]
    element<-"Producer Price (USD/tonne)"
    
    prod_price<-unique(prepare_fao_data(file=econ_file,
                                 lps2fao,
                                 elements=element,
                                 remove_countries = remove_countries,
                                 keep_years=target_year,
                                 atlas_iso3=atlas_iso3))
    
    prod_price<-melt(prod_price,id.vars=c("iso3","atlas_name"),value.name = "price_usd",variable.name = "year")
    
    prod_price_global<-fread(fao_econ_file_world)
    prod_price_global<-prod_price_global[Element==element]
    prod_price_global[, M49 := as.numeric(gsub("[']", "", `Area Code (M49)`))]
    prod_price_global[, iso3 := countrycode(sourcevar = M49, origin = "un", destination = "iso3c")]
    prod_price_global <- prod_price_global[Item %in% lps2fao][, atlas_name := names(lps2fao)[match(Item, lps2fao)]]
    
    prod_price_global<-prod_price_global[,.(iso3,atlas_name,Year,Value)]
    setnames(prod_price_global,c("Value","Year"),c("price_usd","year"))
    
  ## 3.3) Production #####
    prod_file<-file.path(fao_dir,"Production_Crops_Livestock_E_Africa_NOFLAG.csv")
    prod_file_world<-file.path(fao_dir,"Production_Crops_Livestock_E_All_Area_Groups.csv")
    
    prod<-fread(prod_file, encoding = "Latin-1")
    prod[grep(paste(lps2fao,collapse="|"),Item),unique(Item)]
    
    element<-"Production"
    prod<-unique(prepare_fao_data(file=prod_file,
                           lps2fao,
                           elements=element,
                           units="t",
                           remove_countries = remove_countries,
                           keep_years=target_year,
                           atlas_iso3=atlas_iso3))
    
    prod<-melt(prod,id.vars=c("iso3","atlas_name"),value.name = "production_t",variable.name = "year")

  ## 3.4) Not Run: Deflators ####
    if(F){
      deflators<-fread(def_file, encoding = "Latin-1")
      deflators<-deflators[Item=="Value Added Deflator (Agriculture, forestry and fishery)" & Unit=="USD"]
      
      # Convert Area Code (M49) to ISO3 codes and filter by atlas_iso3 countries
      deflators[, M49 := as.numeric(gsub("[']", "", `Area Code (M49)`))]
      deflators[, iso3 := countrycode(sourcevar = M49, origin = "un", destination = "iso3c")]
      deflators <- deflators[iso3 %in% atlas_iso3]
      
      # Remove specified countries
      deflators <- deflators[!Area %in% remove_countries,.(iso3,Year,Value)]
      setnames(deflators,c("Value","Year"),c("deflator_usd","year"))
    }

  ## 3.5) Not Run: PPP ####
    if(F){
    indicators <- data.table(wb_search(pattern = "PPP"))
    print(indicators[grepl("conversion",indicator) & grepl("PP",indicator_id),indicator])
    
    # LCU per I$
    ppp<- data.table(wbstats::wb_data("PA.NUS.PPP", country="countries_only",start_date = min(target_year),end_date=max(target_year)))
    setnames(ppp,c("iso3c","date","PA.NUS.PPP"),c("iso3","year","ppp_lcu"))
    ppp<-ppp[iso3 %in% atlas_iso3,list(iso3,year,ppp_lcu)]

    
    # Retrieve exchange rate data (LCU per USD) for 2015
    xrat <- data.table(wb_data(
      indicator = "PA.NUS.FCRF",
      country="countries_only",
      start_date = min(target_year),
      end_date = max(target_year)
    ))
    setnames(xrat,c("iso3c","date","PA.NUS.FCRF"),c("iso3","year","xrat_lcu_per_usd"))
    
    xrat<-xrat[iso3 %in% atlas_iso3,list(iso3,year,xrat_lcu_per_usd)]
    
    ppp_xrat<-merge(ppp,xrat,all.x=T)
    
    ppp_xrat[iso3=="ZWE",xrat:=1]
    }
    
  ## 3.6) Merge datasets ####
    prod_merge<-merge(prod_value_usd,prod_value_i,all.x=T)
    prod_merge<-merge(prod_merge,prod_price,all.x=T)
    prod_merge<-merge(prod_merge,prod,all.x=T)
    prod_merge[,year:=as.integer(gsub("Y","",year))]
    
    # Not Run
    # prod_merge<-merge(prod_merge,deflators,by=c("iso3","year"),all.x=T)
    # prod_merge<-merge(prod_merge,ppp_xrat,by=c("iso3","year"),all.x=T)

# 4) Infer missing value from production and price data ####
   ## 4.1) Nominal usd ####
   # Ok so now we want to estimate a sensible nominal value in usd from the most recent data available
   # We will use production amount and price to get this as conversion of iusd15 to nominal usd seems to give unrealistically low values
   year_sets<-list(y2021=2019:2023,y2015=2014:2016,y2020=2019:2020)
   
   nominal_usd<-lapply(1:length(year_sets),function(i){
     
     ymin<-min(year_sets[[i]])
     ymax<-max(year_sets[[i]])
     
     prod_merge_recent<-prod_merge[year %in% year_sets[[i]],.(price_usd=median(price_usd,na.rm=T),
                                                            production_t=median(production_t,na.rm=T)),
                                   .(atlas_name,iso3)]
     
     prod_merge_recent<-add_nearby(data=prod_merge_recent,group_field="atlas_name",value_field = "price_usd",neighbors=african_neighbors,regions=regions)
     
     # Values are often missing, is there a value from a longer time series?
     price_any<-prod_merge[year %in% (ymin-5):ymax,.(price_median=median(price_usd,na.rm=T),
                                                     price_min=min(price_usd,na.rm=T),
                                                     price_max=max(price_usd,na.rm=T),
                                                     price_tail=tail(price_usd[!is.na(price_usd)],1),
                                                     price_n=sum(!is.na(price_usd))),
                           .(atlas_name,iso3)]
     
     # Merge median price from longer time-series
     prod_merge_recent<-merge(prod_merge_recent,price_any[,.(price_median,iso3,atlas_name)],all.x=T)
     
     prod_price_global_recent<-prod_price_global[year %in% year_sets[[i]],.(price_usd_global=median(price_usd,na.rm=T)),.(atlas_name)]
     
     prod_merge_recent<-merge(prod_merge_recent,prod_price_global_recent,by="atlas_name",all.x=T)
     
    
     unique(prod_merge_recent[,.(atlas_name,price_usd_continent,price_usd_global)])
     
     prod_merge_recent[,price_usd_final:=price_usd
                       ][is.na(price_usd_final),price_usd_final:=price_median
                       ][is.na(price_usd_final),price_usd_final:=price_usd_neighbors
                       ][is.na(price_usd_final),price_usd_final:=price_usd_region
                         # Goat and sheep milk are very low value in FAOstat for Africa, we will use the global median for these instead
                         ][atlas_name %in% c("goat_milk","sheep_milk"),price_usd_final:=price_usd_global
                           ][is.na(price_usd_final),price_usd_final:=price_usd_continent
                           ][is.na(price_usd_final),price_usd_final:=price_usd_global
                             ][,vop_usd_nominal:=production_t*price_usd_global
                               ][,year:=names(year_sets)[i]]
     
     prod_merge_recent
   })
   
   names(nominal_usd)<-paste0(dataset_name,"_vop_nominal-usd-",gsub("y","",names(year_sets)))
   
   ## 4.2) iusd2015 ####
   intd_2015<-lapply(1:length(year_sets),function(i){
     
     prod_merge_recent<-prod_merge[year %in% year_sets[[i]],.(vop_intd15=median(value_intd15,na.rm=T)*1000),
                                 .(atlas_name,iso3)]
     
     prod_merge_recent<-merge(prod_merge_recent,nominal_usd[[i]][,.(atlas_name,iso3,vop_usd_nominal)],all.x=T)
     prod_merge_recent[,year:=names(year_sets)[i]]
     
      prod_merge_recent
   })
   
   names(intd_2015)<-paste0(dataset_name,"_vop_intld15-",gsub("y","",names(year_sets)))
   
   ## 4.3) Not Run: Converting iusd2015 to nominal usd (not used - unrealistic) ####
   # We have investigated this approach and it yields very low production values compared to 
   # national statistics in Kenya (for example, <50% of the reported value)
   if(F){
   prod_merge[, ppp_cf := ppp_lcu / xrat_lcu_per_usd]   # I$ ➔ USD (both at 2015 PP)
   
   prod_merge[
     !is.na(value_intd15) & !is.na(ppp_cf) & !is.na(deflator_usd),
     `:=`(
       value_intd15_to_cusd15  = value_intd15 * ppp_cf,                 # constant-2015 USD
       value_intd15_to_usd_nom  = value_intd15 * ppp_cf * (deflator_usd / 100)  # nominal-year USD
     )
   ]
   
   prod_merge[,ratio:=value_intd15_to_usd_nom/value_intd15]
   
   prod_merge[iso3 == "TZA" &
     year %in% c(2015, 2020, 2023) &
       !is.na(value_intd15_to_usd_nom),
     .(iso3, atlas_name, year, value_intd15, value_intd15_to_cusd15,ratio,deflator_usd, value_intd15_to_usd_nom,ppp_lcu,xrat_lcu_per_usd)
   ][order(iso3, atlas_name, year)]
   
   prod_merge[iso3=="KEN" & year==2023,.(iso3,atlas_name,year,production_t,price_usd,VoP,value_intd15_to_usd_nom)]
   }
   
# 5) Distribute vop to GLW4 livestock ####

   # Combine lists
   vop_list<-c(nominal_usd,intd_2015)
   
for(i in 1:length(vop_list)){
  
  # Unit is t x usd/t = usd or 1000 intdlr x 1000 there should be no need for any unit conversions (e.g. x 1000)
  final_vop<-copy(vop_list[[i]])
  setnames(final_vop,"vop_usd_nominal","value")
    
  final_vop<-final_vop[,list(iso3,atlas_name,value)]
  final_vop<-merge(final_vop,glw2atlas,all.x=T,by="atlas_name")
  
  # Where there is missing data explore how many animals are in these areas
  final_vop_merge<-merge(final_vop,glw_admin0,all.x=T,by=c("iso3","glw3_name"))
  final_vop_merge[is.na(value) & !grepl("goat_milk|sheep_milk",atlas_name)]
  final_vop_merge[,variable:=paste0("vop_",names(vop_list)[i])]
  
  vop_save_file<-file.path(glw_pro_dir,"fao_vop",paste0(names(vop_list)[i],"-prices-used.csv"))
  ensure_dir(dirname(vop_save_file))
  fwrite(final_vop_merge,vop_save_file)
  
  # Sum values for glw classes
  final_vop <- final_vop[, .(
    value = if (all(is.na(value))) as.numeric(NA) else sum(value, na.rm = TRUE)
  ), by = .(glw3_name, iso3)]  
  
  final_vop_cast<-dcast(final_vop,iso3~glw3_name)
  
  # Convert value to vector then raster
  final_vop_vect<-geoboundaries
  final_vop_vect<-merge(final_vop_vect,final_vop_cast,all.x=T)
  
  final_vop_rast<-terra::rast(lapply(unique(glw2atlas$glw3_name),FUN=function(NAME){
    terra::rasterize(final_vop_vect,base_rast,field=NAME)
  }))
  names(final_vop_rast)<-unique(glw2atlas$glw3_name)
  
  # Multiply national VoP by glw cell proportion
  glw_vop<-glw_prop*final_vop_rast
  
  # Split between highland and tropical zones
  glw_vop_usd_split<-split_livestock(data=glw_vop,livestock_mask_high,livestock_mask_low)
  unit<-gsub(paste0(dataset_name,"_"),"",names(vop_list)[i])
  save_file<-file.path(glw_pro_dir,paste0("variable=",unit),paste0(names(vop_list)[i],".tif"))
  ensure_dir(dirname(save_file))
  terra::writeRaster(round(glw_vop_usd_split,0),save_file,overwrite=T)
}
  
# 6) Livestock Numbers ######
  livestock_no_file<-file.path(glw_pro_dir,"variable=number",paste0(dataset_name,"_number_number.tif"))
  shoat_prop_file<-file.path(glw_int_dir,"shoat_prop.tif")
  overwrite_glw<-F
  
  if(!file.exists(livestock_no_file)|overwrite_glw==T){
    
    glw<-terra::rast(glw_files)
    names(glw)<-unlist(tstrsplit(names(glw),"_",keep=1))
    glw_names<-unlist(tstrsplit(names(glw),"_",keep=1))
    glw_names<-names(glw_codes)[match(glw_names,glw_codes)]
    names(glw)<-glw_names
    glw<-glw[[c("poultry","sheep","pigs","goats","cattle")]]
    
    livestock_no<-glw[[c("poultry","sheep","pigs","goats","cattle")]]
    
    TLU<-sum(c(glw$cattle*0.7,glw$poultry*0.01,glw$goats*0.1,glw$pigs*0.2,glw$sheep*0.1))
    
    livestock_no$total<-TLU
    livestock_no<-terra::mask(terra::crop(livestock_no,geoboundaries),geoboundaries)
    
    # resample to base_rast
    livestock_density<-livestock_no/terra::cellSize(livestock_no,unit="ha")
    livestock_density<-terra::resample(livestock_density,base_rast)
    livestock_no<-livestock_density*cellSize(livestock_density,unit="ha")
    
    # Pull out sheep and goat proportions for use in vop calculations before highland/tropical splitting
    sheep_prop<-livestock_no$sheep/(livestock_no$goats +livestock_no$sheep)
    goat_prop<-livestock_no$goats/(livestock_no$goats +livestock_no$sheep)
    
    terra::writeRaster(terra::rast(c(sheep_prop=sheep_prop,goat_prop=goat_prop)),
                       filename = shoat_prop_file,overwrite=T)
    
    # Split livestock between highland and tropical
    livestock_no<-split_livestock(data=livestock_no,livestock_mask_high,livestock_mask_low)
    
    terra::writeRaster(livestock_no,filename = livestock_no_file,overwrite=T)
  }
  
  
# 7) QAQC: Extract by admin0 and compare back to FAOstat ####
  if(F){
  # This section needs updating  
    
  # GLW distributed data
  data<-glw_vop
  
  qa_rasterizer<-function(data,base_rast,geoboundaries,glw2atlas){
    # constant usd 2014-2016
    data_ex<-terra::extract(data,geoboundaries,fun="sum",na.rm=T)
    data_ex$ID<-NULL
    names(data_ex)<-paste0(names(data_ex))
    data_ex$iso3<-geoboundaries$iso3
    
    
    data_ex_vect<-geoboundaries
    data_ex_vect<-merge(data_ex_vect,data_ex,all.x=T,by="iso3")
    
    data_ex_rast<-terra::rast(lapply(unique(glw2atlas$glw3_name),FUN=function(NAME){
      terra::rasterize(data_ex_vect,base_rast,field=NAME)
    }))
    
    return(list(table=data_ex,vect=data_ex_vect,rast=data_ex_rast))
  }
  
  glw_vop_cusd15_adm0<-qa_rasterizer(data=glw_vop,base_rast,geoboundaries,glw2atlas)$rast
  glw_vop_int15_adm0<-qa_rasterizer(data=glw_vop_i,base_rast,geoboundaries,glw2atlas)$rast

  # FAOstat data
  fao_vop_cusd15<-prod_value_usd[,list(Y2015,iso3,atlas_name)]
  fao_vop_cusd15<-merge(fao_vop_cusd15,glw2atlas,all.x=T,by="atlas_name")
  fao_vop_cusd15<-fao_vop_cusd15[,list(value=sum(Y2015,na.rm=T),N=sum(is.na(Y2015))),by=list(glw3_name,iso3)]
  fao_vop_cusd15<-fao_vop_cusd15[N==0]
  
  fao_vop_cusd15<-dcast(fao_vop_cusd15,iso3~glw3_name)
  fao_vop_cusd15_vect<-geoboundaries
  fao_vop_cusd15_vect<-merge(fao_vop_cusd15_vect,fao_vop_cusd15,all.x=T,by="iso3")
  
  fao_vop_cusd15_rast<-terra::rast(lapply(unique(glw2atlas$glw3_name),FUN=function(NAME){
    terra::rasterize(fao_vop_cusd15_vect,base_rast,field=NAME)
  }))
  
  # Constant USD 2015
  
  # use indigenous meat production values
  lps2fao_ind<-lps2fao
  lps2fao_ind[grep("Meat",lps2fao_ind)]<-paste0(lps2fao_ind[grep("Meat",lps2fao_ind)]," (indigenous)")

  element<-"Gross Production Value (current thousand US$)" 

  fao_vop_usd15<-prepare_fao_data(file=vop_file,
                               lps2fao_ind,
                               elements=element,
                               remove_countries = remove_countries,
                               keep_years=target_year,
                               atlas_iso3=atlas_iso3)
  
  fao_vop_usd15[,atlas_name:=gsub(" (indigenous)","",atlas_name)]

  fao_vop_usd15<-fao_vop_usd15[,list(Y2015,iso3,atlas_name)]
  fao_vop_usd15<-merge(fao_vop_usd15,glw2atlas,all.x=T,by="atlas_name")
  fao_vop_usd15<-fao_vop_usd15[,list(value=sum(Y2015,na.rm=T),N=sum(is.na(Y2015))),by=list(glw3_name,iso3)]
  fao_vop_usd15<-fao_vop_usd15[N==0]
  
  fao_vop_usd15<-dcast(fao_vop_usd15,iso3~glw3_name)
  fao_vop_usd15_vect<-geoboundaries
  fao_vop_usd15_vect<-merge(fao_vop_usd15_vect,fao_vop_usd15,all.x=T,by="iso3")
  
  NAMES<-unique(glw2atlas$glw3_name)
  NAMES<-NAMES[NAMES %in% names(fao_vop_usd15_vect)]
  fao_vop_usd15_rast<-terra::rast(lapply(NAMES,FUN=function(NAME){
    terra::rasterize(fao_vop_usd15_vect,base_rast,field=NAME)
  }))
  
  # $I
  element<-"Gross Production Value (constant 2014-2016 thousand I$)"
  
  fao_vop_id15<-prepare_fao_data(file=vop_file,
                                  lps2fao_ind,
                                  elements=element,
                                  remove_countries = remove_countries,
                                  keep_years=target_year,
                                  atlas_iso3=atlas_iso3)
  
  fao_vop_id15[,atlas_name:=gsub(" (indigenous)","",atlas_name)]
  
  fao_vop_id15<-fao_vop_id15[!is.na(Y2015),list(Y2015,iso3,atlas_name)]
  fao_vop_id15<-merge(fao_vop_id15,glw2atlas,all.x=T,by="atlas_name")
  fao_vop_id15<-fao_vop_id15[,list(value=sum(Y2015,na.rm=T),N=sum(is.na(Y2015))),by=list(glw3_name,iso3)]
  fao_vop_id15<-fao_vop_id15[N==0]
  
  fao_vop_id15<-dcast(fao_vop_id15,iso3~glw3_name)
  fao_vop_id15_vect<-geoboundaries
  fao_vop_id15_vect<-merge(fao_vop_id15_vect,fao_vop_id15,all.x=T,by="iso3")
  
  NAMES<-unique(glw2atlas$glw3_name)
  NAMES<-NAMES[NAMES %in% names(fao_vop_id15_vect)]
  
  # Exclude countries
  exclude<-"ERI"
  fao_vop_id15_rast<-terra::rast(lapply(NAMES,FUN=function(NAME){
    terra::rasterize(fao_vop_id15_vect[!fao_vop_id15_vect$iso3 %in% exclude,],base_rast,field=NAME)
  }))
  
  # Comparisons
  plot(glw_vop_cusd15_adm0/fao_vop_cusd15_rast)
  plot(glw_vop_int15_adm0/fao_vop_id15_rast)
  }
  