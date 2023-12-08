if(!require("pacman", character.only = TRUE)){install.packages("pacman",dependencies = T)}

required.packages <- c("terra",
                       "data.table",
                       "DT",
                       "countrycode",
                       "exactextractr",
                       "feather")

pacman::p_load(char=required.packages,install = T,character.only = T)

base_rast<-terra::rast(list.files("Data/hazard_classified/annual",".tif",full.names = T))[[1]]

# Load and combine geoboundaries ####
Geographies<-list(
  admin2=terra::vect("Data/geoboundaries/admin2_processed.shp"),
  admin1=terra::vect("Data/geoboundaries/admin1_processed.shp"),
  admin0=terra::vect("Data/geoboundaries/admin0_processed.shp")
)

# Countries
Countries<-sort(unique(Geographies$admin0$admin0_nam))

# Crops
ms_codes<-data.table::fread("./Data/metadata/SpamCodes.csv")[,Code:=toupper(Code)]
ms_codes<-ms_codes[compound=="no"]
crop_choices<-sort(ms_codes[,sort(Fullname)])

# Livestock
livestock_choices<-sort(c("all livestock","cattle","goats","sheep","pigs","poultry"))

# Exposure
exposure_choices<-c("value of production","harvested area","rural pop")

# Hazards crops
haz_heat_choices<-sort(c("NTx35","TAVG_G"))

# Hazards livestock
haz_heat_ls_choices<-sort(c("THI"))

# Hazards both
haz_dry_choices<-sort(c("NDD","NDWS","PTOT_L"))
haz_wet_choices<-sort(c("NDWL0","PTOT_G"))

haz_meta<-data.table::fread("./Data/metadata/haz_metadata.csv")

scenario_choices<-c("ssp245","ssp585")
timeframe_choices<-c("historic","2021_2040","2041_2060")

# CHOICES ####
country_choice<-"Tanzania"
AdminLevel<-"admin1"
adm1_selection<-"All"
admin1<-sort(unique(Geographies$admin1$admin1_nam[Geographies$admin1$admin0_nam %in% country_choice]))
adm2_selection<-"All"
admin2<-sort(unique(Geographies$admin2$admin2_nam[Geographies$admin2$admin0_nam %in% country_choice & Geographies$admin2$admin1_nam %in% admin1]))
season_choice<-"annual"
timeframe<-timeframe_choices[1]
future_scenario<- scenario_choices[1]
future_timeframe<-timeframe_choices[2]
scenario_choice<-if(timeframe=="historic"){"historic"}else{future_scenario}
crops<-crop_choices[c(10,12)]
livestock<-livestock_choices[2]
haz_dry<-haz_dry_choices[1]
haz_heat<-haz_heat_choices[2]
haz_wet<-haz_wet_choices[2]
severity<-"severe"
exposure<-exposure_choices[1]

# Subset geographies ####
if(AdminLevel=="admin0"){
  geographies_selected<-Geographies$admin0[Geographies$admin0$admin0_nam %in% country_choice]
}

if(AdminLevel=="admin1"){
  geographies_selected<-Geographies$admin1[Geographies$admin1$admin0_nam %in% country_choice & 
                                             Geographies$admin1$admin1_nam %in% admin1]
}

if(AdminLevel=="admin2"){
  geographies_selected<-Geographies$admin2[Geographies$admin2$admin0_nam %in% country_choice & 
                                             Geographies$admin2$admin1_nam %in% admin1 & 
                                             Geographies$admin2$admin2_nam %in% admin2]
}

# Load Risk x Exposure datasets ####
# RENAME HAZARD FIELDS TO GENERIC GROUPS
haz_risk_vop_tab<-data.table(feather::read_feather(paste0(paste0("Data/hazard_risk_vop/",season_choice),"/haz_risk_vop_",severity,".feather")))
haz_risk_ha_tab<-data.table(feather::read_feather(paste0(paste0("Data/hazard_risk_ha/",season_choice),"/haz_risk_ha_",severity,".feather")))
haz_risk_tab_join<-rbind(haz_risk_vop_tab,haz_risk_ha_tab)

# Consolidate Admin Selection ####
admin_choice<-if(AdminLevel=="admin0"){
  country_choice
  }else{
    if(AdminLevel=="admin1"){
      admin1
      }else{
        if(AdminLevel=="admin2"){
          admin2
        }}}


# Merge hazards ####
heat<-haz_heat
wet<-haz_wet
dry<-haz_dry

haz_choice<-c(heat=heat,
                wet=wet,
                dry=dry,
                heat_wet=paste0(heat,"+",wet),
                heat_dry=paste0(heat,"+",dry),
                wet_dry=paste0(wet,"+",dry),
                heat_wet_dry=paste0(heat,"+",wet,"+",dry))

# Exposure headline stats ####
# Table - Risk Exposure Headline ####
# Total value of crops exposed to hazards ####
haz_risk_tab_join_sum<-haz_risk_tab_join[admin_level==AdminLevel & 
                           admin_name %in% admin_choice & 
                           crop %in% crops & 
                           hazard %in% haz_choice,
                         list(value=sum(value,na.rm = T)),
                         by=list(exposure,severity,scenario,timeframe,hazard)
                         ][,hazard_rename:=names(haz_choice)[match(hazard,haz_choice)]]

dplyr::mutate_if(dplyr::mutate_if(
  dcast(haz_risk_tab_join_sum,exposure+severity+scenario+timeframe~hazard_rename,value.var = "value"),
  is.numeric,~round(.,0)),is.character,~as.factor(.))

# Exposure by admin area x crop x hazard ####
# Subset Risk x Exposure datasets ####
haz_risk_vop_tab2<- haz_risk_vop_tab[admin_level==AdminLevel & 
                       admin_name %in% admin_choice &
                       crop %in% crops &
                       timeframe==timeframe &
                       scenario==scenario_choice & 
                       hazard %in% haz_choice
  ][,hazard_rename:=names(haz_choice)[match(hazard,haz_choice)]]

haz_risk_ha_tab2<-haz_risk_ha_tab[admin_level==AdminLevel & 
                      admin_name %in% admin_choice & 
                      crop %in% crops &
                      timeframe==timeframe &
                      scenario==scenario_choice & 
                      hazard %in% haz_choice
  ][,hazard_rename:=names(haz_choice)[match(hazard,haz_choice)]]


if(exposure=="value of production"){
  dplyr::mutate_if(dplyr::mutate_if(
    dcast(haz_risk_vop_tab2,admin_name+crop~hazard_rename,value.var = "value"),
    is.numeric,~round(.,0)),is.character,~as.factor(.))
}else{
  if(exposure=="harvested area"){
    dplyr::mutate_if(dplyr::mutate_if(
      dcast(haz_risk_ha_tab2,admin_name+crop~hazard_rename,value.var = "value"),
      is.numeric,~round(.,0)),is.character,~as.factor(.))
  }else{
    NULL
  }
}


# Combined Risk For Human Pop ####

# TO DO -> RENAME HAZARDS TO HEAT,WET,DRY

risk_threshold<-0.5

# Grab risk rasters for selection - these should be pre-masked to crop production areas
risk_rast_files<-list.files(paste0("Data/hazard_risk_class_mask/t",risk_threshold,"/",season_choice),".tif",full.names = T)
risk_rast_full<-terra::rast(risk_rast_files)

risk_rast<-risk_rast_full[[grep(severity,names(risk_rast_full),value = T,ignore.case = T)]]
risk_rast<-risk_rast[[grep(paste0(crops,collapse = "|"),names(risk_rast),value = T,ignore.case = T)]]
risk_rast<-risk_rast[[unlist(tstrsplit(names(risk_rast),"-",keep=3)) %in% haz_choice]]

# Crop to geography
risk_rast<-terra::mask(terra::crop(risk_rast,geographies_selected),geographies_selected)

risk_rast_past<-risk_rast[[grep("historic",names(risk_rast),value = T,ignore.case = T)]]
risk_rast_fut<-risk_rast[[grep(future_scenario,names(risk_rast),value = T,ignore.case = T)]]
risk_rast_fut<-risk_rast_fut[[grep(future_timeframe,names(risk_rast_fut),value = T,ignore.case = T)]]

# Very slow - this needs to be pre-baked
risk_rast_diff<-risk_rast_fut-risk_rast_past


# Merge risk areas (crops x hazards) by hazard
rr_names<-unique(unlist(tstrsplit(names(risk_rast_past),"-",keep=3)))

risk_rast_past_max<-terra::rast(lapply(rr_names,FUN=function(NAME){
  NAME<-paste0("-",NAME,"-")
  Layers<-grep(NAME,names(risk_rast_past),value=T,fixed = T)
  X<-risk_rast_past[[Layers]]
  terra::app(X,fun="max",na.rm=T)
}))

names(risk_rast_past_max)<-rr_names

risk_rast_past_max$ANY<- terra::app(risk_rast_past,fun="max",na.rm=T)
n_haz<-str_count(names(risk_rast_past_max),"[+]")+1
risk_rast_past_max$ONE<- terra::app(risk_rast_past[[n_haz==1]],fun="max",na.rm=T)
risk_rast_past_max$TWO<- terra::app(risk_rast_past[[n_haz==2]],fun="max",na.rm=T)

# Extract and sum exposure per hazard risk area
# Load human population
hpop<-terra::rast("Data/exposure/hpop.tif")

hpop_crop<-terra::mask(terra::crop(hpop,geographies_selected),geographies_selected)

hpop_risk<-risk_rast_past_max*hpop_crop$rural
plot(hpop_risk)

hpop_risk_tab<-exactextractr::exact_extract(hpop_risk,sf::st_as_sf(geographies_selected),fun="sum",append_cols=c("admin_name","iso3"))
hpop_risk_tab_tot<-colSums(hpop_risk_tab[,-(1:2)])

# Extract and sum exposure for all risk area
