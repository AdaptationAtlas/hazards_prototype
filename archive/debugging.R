if(!require("pacman", character.only = TRUE)){install.packages("pacman",dependencies = T)}

required.packages <- c("terra",
                       "data.table",
                       "DT",
                       "countrycode",
                       "exactextractr",
                       "feather",
                       "stringr",
                       "ggplot2",
                       "sfarrow")

pacman::p_load(char=required.packages,install = T,character.only = T)

PalFun<-function(PalName,N,Names) {
  Viridis<-data.table(Source="viridis",Palette=c("magma","inferno","plasma","viridis","cividis","rocket","mako","turbo"))
  Met<-data.table(Source="MetBrewer",Palette=names(MetBrewer::MetPalettes))
  Wes<-data.table(Source="Wes",Palette=names(wesanderson::wes_palettes))
  Palettes<-rbind(Viridis,Met,Wes)
  
  if(Palettes[Palette==PalName,Source]=="viridis"){
    PAL<-viridis::viridis(N,option=PalName)
  }
  
  if(Palettes[Palette==PalName,Source]=="MetBrewer"){
    PAL<-MetBrewer::met.brewer(name=PalName, n=N, type="continuous")
  }
  
  if(Palettes[Palette==PalName,Source]=="Wes"){
    if(N>length(wes_palettes[[PalName]])){
      PAL<-wesanderson::wes_palette(name=PalName, n=length(wes_palettes[[PalName]]), type="continuous")
      PAL<-colorRampPalette(PAL)(N)
    }else{
      PAL<-wesanderson::wes_palette(name=PalName, n=N, type="continuous")
    }
  }
  names(PAL)<-Names
  
  return(PAL)
}

TextSize<-1

ggplot_theme<-{
  X<-theme_minimal()
  X<-X+theme(axis.text = element_text(size = 10*TextSize),
             axis.title = element_text(size = 11*TextSize),
             plot.title = element_text(size = 12*TextSize),
             strip.text = element_text(size = 11*TextSize),
             legend.text = element_text(size = 10*TextSize),
             legend.title = element_text(size = 11*TextSize)) 
  X
}

base_rast<-terra::rast(list.files("Data/hazard_classified/annual",".tif",full.names = T))[[1]]

# Load and combine geoboundaries ####
Geographies<-list(
  admin2=terra::vect("Data/geoboundaries/admin2_processed.shp"),
  admin1=terra::vect("Data/geoboundaries/admin1_processed.shp"),
  admin0=terra::vect("Data/geoboundaries/admin0_processed.shp")
)

# Create Selection Values ####
# Countries
Countries<-sort(unique(Geographies$admin0$admin0_nam))

# Crops
ms_codes<-data.table::fread("./Data/metadata/SpamCodes.csv")[,Code:=toupper(Code)]
ms_codes<-ms_codes[compound=="no"]
crop_choices<-sort(ms_codes[,sort(Fullname)])

# Livestock
livestock_choices<-expand.grid(c("cattle","goats","sheep","pigs","poultry"),c("_highland","_tropical"))
livestock_choices<-paste0(livestock_choices$Var1,livestock_choices$Var2)

# Exposure
exposure_choices<-c("value of production","harvested area","rural pop")

# Hazards crops
haz_heat_choices<-sort(c("NTx35","TAVG_G"))

# Hazards livestock
haz_heat_ls<-"THI_max"

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
crops<-crop_choices[c(4,13)]
livestock<-livestock_choices[2]
haz_dry<-haz_dry_choices[3]
haz_heat<-haz_heat_choices[2]
haz_wet<-haz_wet_choices[2]
severity<-"severe"
exposure<-exposure_choices[1]

# Controls to add or issues to deal with ####
 # 1) Seasonality - which dataset
 # 2) Seasonality - which season
  # 2.1) How to combine two seasons? - we do not have mapspam by season
 # 3) Livestock hazards vs crop hazards 
 # 4) What THI is being used for livestock THI_max? THI_max & THI_min still in data tables

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

# Load total exposure values per crop per admin area 
exposure_dir<-"Data/exposure"
exposure_sum_tab<-data.table(feather::read_feather(paste0(exposure_dir,"/exposure_adm_sum.feather")))
setnames(exposure_sum_tab,c("value"),c("total_value"))

# Load exposed vop per crop per admin area 
haz_risk_vop_tab<-data.table(feather::read_feather(paste0(paste0("Data/hazard_risk_vop/",season_choice),"/haz_risk_vop_",severity,".feather")))
haz_risk_vop_tab<-merge(haz_risk_vop_tab,exposure_sum_tab,all.x=T)

# Load exposed harvested area per crop per admin area 
haz_risk_ha_tab<-data.table(feather::read_feather(paste0(paste0("Data/hazard_risk_ha/",season_choice),"/haz_risk_ha_",severity,".feather")))
haz_risk_ha_tab<-merge(haz_risk_ha_tab,exposure_sum_tab,all.x=T)

# Load exposed harvested area per crop per admin area 
haz_risk_n_tab<-data.table(feather::read_feather(paste0(paste0("Data/hazard_risk_n/",season_choice),"/haz_risk_n_",severity,".feather")))[,exposure:="number"]
haz_risk_n_tab<-merge(haz_risk_n_tab,exposure_sum_tab,all.x=T)

# Join exposed vop and ha tables
haz_risk_tab_join<-rbind(haz_risk_vop_tab,haz_risk_ha_tab,haz_risk_n_tab)

# Add totals to haz_risk_tab_join tab
haz_risk_tab_join[,value:=round(value,0)][,total_value:=round(total_value,0)]
haz_risk_tab_join[total_value<value,total_value:=value]

# Load total human population values per per admin area 
hpop_sum_tab<-data.table(feather::read_feather(paste0(exposure_dir,"/hpop_adm_sum.feather")))
# Subset to rural popoulation and remove unneeded columns
hpop_sum_tab<-hpop_sum_tab[variable=="rural",!c("variable","exposure")]
setnames(hpop_sum_tab,"value","total_value")

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

if(length(crops)>0 & !is.null(crops) & !is.na(crops[1])){
haz_choice<-c(heat=heat,
                wet=wet,
                dry=dry,
                heat_wet=paste0(heat,"+",wet),
                heat_dry=paste0(heat,"+",dry),
                wet_dry=paste0(wet,"+",dry),
                heat_wet_dry=paste0(heat,"+",wet,"+",dry))
}

if(length(livestock)>0 & !is.null(livestock) & !is.na(livestock[1])){
  haz_choice_ls<-c(heat=haz_heat_ls,
                   wet=wet,
                   dry=dry,
                   heat_wet=paste0(haz_heat_ls,"+",wet),
                   heat_dry=paste0(haz_heat_ls,"+",dry),
                   wet_dry=paste0(wet,"+",dry),
                   heat_wet_dry=paste0(haz_heat_ls,"+",wet,"+",dry))
}

# Exposure headline stats ####

# Exposure by admin area x crop x hazard ####
# Note that risk is present as risk of any dry, wet or heat hazard alone or in combination with other hazards. So the sum of wet, dry and heat exposure can count the
# exposure variable (e.g. human population or vop) multiple times

# Subset Risk x Exposure datasets ####
if(length(crops)>0 & !is.null(crops) & !is.na(crops[1])){
  haz_risk_tab2_crop<- haz_risk_tab_join[admin_level==AdminLevel & 
                         admin_name %in% admin_choice &
                         crop %in% crops &
                         hazard %in% haz_choice
    ][,hazard_rename:=names(haz_choice)[match(hazard,haz_choice)]]
  }else{
    haz_risk_tab2_crop<-NULL
  }

  if(length(livestock)>0 & !is.null(livestock) & !is.na(livestock[1])){
haz_risk_tab2_ls<- haz_risk_tab_join[admin_level==AdminLevel & 
                                       admin_name %in% admin_choice &
                                       crop %in% livestock &
                                       hazard %in% haz_choice_ls
                                       ][,hazard_rename:=names(haz_choice_ls)[match(hazard,haz_choice_ls)]]
  }else{
    haz_risk_tab2_ls<-NULL
  }

haz_risk_tab2<-rbind(dcast(rbind(haz_risk_tab2_crop,haz_risk_tab2_ls),admin_name+crop+scenario+timeframe+exposure+total_value~hazard_rename,value.var = "value"))

haz_risk_tab2[exposure=="vop",exposure:="value of production"
              ][exposure=="ha",exposure:="harvested area"]


sdcols<-colnames(haz_risk_tab2)[!colnames(haz_risk_tab2) %in% c("admin_name","iso3","crop","scenario","timeframe","exposure")]

# Sum across admin areas and crops
risk_tab_tot_sum<-haz_risk_tab2[,lapply(.SD,sum),by=list(timeframe,scenario,exposure),.SDcols=sdcols]
# Sum across admin areas and crops
risk_tab_crop_sum<-haz_risk_tab2[,lapply(.SD,sum),by=list(timeframe,scenario,crop,exposure),.SDcols=sdcols]

combine_hazards<-function(data){
  data[,one_hazard_only:=(dry+heat+wet)-2*heat_dry-2*heat_wet-2*wet_dry+3*heat_wet_dry
  ][,two_hazards_only:=heat_dry+heat_wet+wet_dry-3*heat_wet_dry
  ][,any_hazards:=one_hazard_only+two_hazards_only+heat_wet_dry
  ][,any_hazard_perc:=round(100*any_hazards/total_value,2)]
}

# Add in combined hazards
haz_risk_tab2<-combine_hazards(data=haz_risk_tab2)
risk_tab_crop_sum<-combine_hazards(data=risk_tab_crop_sum)
risk_tab_tot_sum<-combine_hazards(data=risk_tab_tot_sum)

# NEED TO CREATE A RASTER LAYER FOR TOTAL VOP EXPOSURE FOR USE IN EXPOSURE X VULNERABILITY BIPLOT
risk_vop_rast_files<-list.files(paste0("Data/hazard_risk_vop/",season_choice),".tif",full.names = T)

# Subset to selected crops and severity
risk_vop_rast_files_ss<-grep(severity,risk_vop_rast_files,value = T,ignore.case = T)
risk_vop_rast_files_ss<-grep(paste0(crops,collapse = "|"),risk_vop_rast_files_ss,value = T,ignore.case = T)

# Load rasters
risk_vop_rast<-terra::rast(risk_vop_rast_files_ss)

# Subset raster layers to selected hazards
risk_vop_rast<-risk_vop_rast[[unlist(tstrsplit(names(risk_vop_rast),"-",keep=3)) %in% haz_choice]]

# Rename layers


# Crop and mask raster to geography selected
risk_vop_rast<-terra::mask(terra::crop(risk_vop_rast,geographies_selected),geographies_selected)

# Subset raster layers to historic baseline
risk_vop_rast_past<-risk_vop_rast[[grep("historic",names(risk_vop_rast),value = T,ignore.case = T)]]

# Subset raster layers to future scenario
risk_vop_rast_fut<-risk_vop_rast[[grep(future_scenario,names(risk_vop_rast),value = T,ignore.case = T)]]
risk_vop_rast_fut<-risk_vop_rast_fut[[grep(future_timeframe,names(risk_vop_rast_fut),value = T,ignore.case = T)]]

# WE NEED TO PRECOOK A LAYER FOR TOTAL VOP EXPOSED TO HAZARDS

# Combined Risk For Human Pop ####
risk_threshold<-0.5

# Grab risk rasters for selection - these should be pre-masked to crop production areas
risk_rast_files<-list.files(paste0("Data/hazard_risk_class_mask/t",risk_threshold,"/",season_choice),".tif",full.names = T)

# QC check to look for NA values in raster layer names
lyr_name_check<-sapply(risk_rast_files,FUN=function(FILE){
  data<-terra::rast(FILE)
  any(is.na(names(data)))|any(names(data)=="NA")
},USE.NAMES = T)

lyr_name_check[lyr_name_check==T]
names(rast(names(lyr_name_check[lyr_name_check==T])))

# NOTE THERE IS AN ISSUES WITH THE NAMES FOR:
#[1] "Data/hazard_risk_class_mask/t0.5/annual/barley_extreme_int.tif"    "Data/hazard_risk_class_mask/t0.5/annual/bean_extreme_int.tif"     
#[3] "Data/hazard_risk_class_mask/t0.5/annual/cocoa_extreme_int.tif"     "Data/hazard_risk_class_mask/t0.5/annual/cowpea_extreme_int.tif"   
#[5] "Data/hazard_risk_class_mask/t0.5/annual/pigeonpea_extreme_int.tif" "Data/hazard_risk_class_mask/t0.5/annual/rapeseed_extreme_int.tif" 
#[7] "Data/hazard_risk_class_mask/t0.5/annual/rice_extreme_int.tif" 


# Subset to severity
risk_rast_files_ss<-grep(severity,risk_rast_files,value = T,ignore.case = T)
# Subset to crops
risk_rast_files_ss<-grep(paste0(crops,collapse = "|"),risk_rast_files_ss,value = T,ignore.case = T)

# Load rasters
risk_rast<-terra::rast(risk_rast_files_ss)

# Subset raster layers to selected hazards
risk_rast<-risk_rast[[unlist(tstrsplit(names(risk_rast),"-",keep=3)) %in% haz_choice]]

# Crop and mask raster to geography selected
risk_rast<-terra::mask(terra::crop(risk_rast,geographies_selected),geographies_selected)

# Subset raster layers to historic baseline
risk_rast_past<-risk_rast[[grep("historic",names(risk_rast),value = T,ignore.case = T)]]

# Subset raster layers to future scenario
risk_rast_fut<-risk_rast[[grep(future_scenario,names(risk_rast),value = T,ignore.case = T)]]
risk_rast_fut<-risk_rast_fut[[grep(future_timeframe,names(risk_rast_fut),value = T,ignore.case = T)]]

# Merge risk areas (crops x hazards) by hazard
merge_risk_areas<-function(data){
  rr_names<-unique(unlist(tstrsplit(names(data),"-",keep=3)))
  
  data_max<-terra::rast(sapply(rr_names,FUN=function(NAME){
    NAME<-paste0("-",NAME,"-")
    Layers<-grep(NAME,names(data),value=T,fixed = T)
    X<-data[[Layers]]
    terra::app(X,fun="max",na.rm=T)
  }))
  
  names(data_max)<-rr_names
  
  n_haz<-str_count(names(data),"[+]")+1
  data_max$one_hazard_only<- terra::app(data[[n_haz==1]],fun="max",na.rm=T)
  data_max$two_hazards_only<- terra::app(data[[n_haz==2]],fun="max",na.rm=T)
  data_max$one_hazard_only<- data_max$one_hazard_only-data_max$two_hazards_only
  data_max$any_hazards<- terra::app(data,fun="max",na.rm=T)
  
  # Rename specific hazards to generic names
  names(data_max)<-gsub(haz_choice["heat"],"heat",names(data_max))
  names(data_max)<-gsub(haz_choice["wet"],"wet",names(data_max))
  names(data_max)<-gsub(haz_choice["dry"],"dry",names(data_max))
  
  return(data_max)
}

risk_rast_past_max<-merge_risk_areas(data=risk_rast_past)
risk_rast_fut_max<-merge_risk_areas(data=risk_rast_fut)




# Extract and sum exposure per hazard risk area
# Load human population
hpop<-terra::rast("Data/exposure/hpop.tif")

hpop_crop<-terra::mask(terra::crop(hpop,geographies_selected),geographies_selected)

# Multiply hazard risk by rural human population
hpop_risk_past<-risk_rast_past_max*hpop_crop$rural
hpop_risk_fut<-risk_rast_fut_max*hpop_crop$rural

hpop_risk_past_tab<-exactextractr::exact_extract(hpop_risk_past,sf::st_as_sf(geographies_selected),fun="sum",append_cols=c("admin_name","iso3"))
hpop_risk_fut_tab<-exactextractr::exact_extract(hpop_risk_fut,sf::st_as_sf(geographies_selected),fun="sum",append_cols=c("admin_name","iso3"))

hpop_risk_past_tab$timeframe<-"historic"
hpop_risk_fut_tab$timeframe<-future_timeframe

hpop_risk_tab_tot<-data.table(rbind(hpop_risk_past_tab,hpop_risk_fut_tab))

colnames(hpop_risk_tab_tot)<-gsub("sum.","",colnames(hpop_risk_tab_tot),fixed=T)

# Add total population to table
hpop_risk_tab_tot[,admin_level:=AdminLevel]
hpop_risk_tab_tot<-merge(hpop_risk_tab_tot,hpop_sum_tab)
hpop_risk_tab_tot[,admin_level:=NULL]

# Extract and sum exposure for all risk area
sdcols<-colnames(hpop_risk_tab_tot)[!colnames(hpop_risk_tab_tot) %in% c("admin_name","iso3","timeframe")]

hpop_risk_tab_tot_sum<-hpop_risk_tab_tot[,lapply(.SD,sum),by=timeframe,.SDcols=sdcols]

# Time Series Trends ####
# 1) Trends in mean values #####
haz_timeseries_dir<-paste0("Data/hazard_timeseries/",season_choice)

haz_timeseries_tab<-data.table(feather::read_feather(paste0(haz_timeseries_dir,"/haz_timeseries.feather")))

haz_choice_joined<-gsub("_G|_L","",c(dry=haz_dry,heat=haz_heat,wet=haz_wet))

# Clunky code that concatenates the generic names of the hazards in the specific haz choice vector (e.g. PTOT can be for dry and wet hazards)
X<-paste0(names(haz_choice_joined)[haz_choice_joined==names(table(haz_choice_joined)[table(haz_choice_joined)>1])],collapse="_or_")
Y<-unique(haz_choice_joined[table(haz_choice_joined)>1])
names(haz_choice_joined)[haz_choice_joined==Y]<-rep(X,length(haz_choice_joined[haz_choice_joined==Y]))
# Remove duplicates
haz_choice_joined<-haz_choice_joined[!duplicated(haz_choice_joined)]

dt<-haz_timeseries_tab[admin_level==AdminLevel & 
                admin_name %in% admin_choice & 
                timeframe %in% c("historical",future_timeframe) & 
                scenario %in% c("historical",future_scenario) & 
                hazard %in% haz_choice_joined
                ][admin_name %in% unique(admin_name)[1:3]
                  ][,year:=as.numeric(year)
                    ][,scen_time:=paste0(unique(c(scenario[1],timeframe[1])),collapse=" "),by=list(scenario,timeframe)]

palette_choice<-"magma"

haz_pal<-PalFun(PalName=palette_choice,
                N=length(haz_choice_joined),
                Names=haz_choice_joined)



#shading_table<-haz_class[index_name %in% haz_choices,list(index_name,lower_lim,upper_lim,class,description)
#][,xmin:=min(dt$year)
#][,xmax:=max(dt$year)
#][,description:=factor(description,levels = haz_class[index_name %in% haz_choices][order(class,decreasing = T),description])]

# Create the line plot
g<-ggplot()+
  geom_line(data=dt, aes(x = year, y = value, color = admin_name,lty=admin_name)) +
  geom_smooth(data=dt, aes(x = year, y = value, color = admin_name,lty=admin_name),method=lm,se=F)+
  #geom_rect(data = shading_table,
  #          aes(xmin =xmin, xmax = xmax, ymin = lower_lim, ymax = upper_lim,fill=description),
  #          alpha = 0.2) +
  labs(x = "Year", y = "Value", color = "Location",fill="Location",lty="Location") +
  ggplot_theme+
  #scale_fill_manual(values=haz_pal)+
  facet_grid(hazard~scen_time,scales = "free")

# 1a) Choose crop to show thresholds

# 1b) Plot by admin area or overall (area weighted average across areas) for wet, dry, heat

# 1c) Trends in area exposed?

# This would require classification of annual rasters then to give 1/0, multiplication by cell area and then summation by admin polygons = % area affected per year.
# Would only be interesting if large geographies are selected

# 2) Trends in exposure values #####
haz_risk_dir<-paste0("Data/hazard_risk_class/t",risk_threshold,"/",season_choice)
files<-list.files(haz_risk_dir,full.names = T)

X<-terra::rast(files[1])


haz_class_dir<-paste0("Data/hazard_classified/",season_choice)
files<-list.files(haz_class_dir,full.names = T)

X<-terra::rast(files[1])

# Hazard Mean Maps ####

# Hazard Mean Polygons ####
haz_means_dir<-paste0("Data/hazard_mean/",season_choice)
haz_means_tab<-data.table(feather::read_feather(paste0(haz_means_dir,"/haz_means.feather")))

haz_means<-list(
  admin0=terra::vect(sfarrow::st_read_parquet(paste0(haz_means_dir,"/haz_means_adm0.parquet"))),
  admin1=terra::vect(sfarrow::st_read_parquet(paste0(haz_means_dir,"/haz_means_adm1.parquet"))),
  admin2=terra::vect(sfarrow::st_read_parquet(paste0(haz_means_dir,"/haz_means_adm2.parquet")))
)

haz_means<-sapply(haz_means,FUN=function(data){
  names(data)<-gsub("mean.","",names(data),fixed = T)
  names(data)<-gsub("_mean","",names(data),fixed = T)
  data
}, USE.NAMES = T)

# Plot results
fut_strings<-paste(paste0(future_scenario,".",future_timeframe,"_",haz_choice_joined),collapse = "|")
past_strings<-paste(paste0("historic.historic_",haz_choice_joined),collapse = "|")

X<-haz_means[[AdminLevel]]
fut_cols<-grep(fut_strings,names(X))
past_cols<-grep(past_strings,names(X))
Y<-X[X$admin_name %in% admin_choice,fut_cols]
N<-match(unlist(tail(tstrsplit(names(Y),"_"),1)),haz_choice_joined)
names(Y)<-paste0(haz_choice_joined[N],"_(",names(haz_choice_joined)[N],")")

terra::plot(x=Y,y=names(Y),type="continuous")

# Prioritization ####
# Which commodities x hazards are the most important in this geography?
haz_risk_tab2[variable == exposure]

# This could be used to select default commodities and hazards for the geography.