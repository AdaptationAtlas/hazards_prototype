# Setup if working from Atlas server####
timeframe_choice<-"jagermeyr"

# Project location
package_dir<-getwd()

# Set working directory
working_dir<-"/home/jovyan/common_data/hazards_prototype"
if(!dir.exists(working_dir)){
  dir.create(working_dir,recursive=T)
}

setwd(working_dir)

# Set output directories
# Working on cglabs server
if(timeframe_choice!="annual"){
  haz_timeseries_dir<-paste0("/home/jovyan/common_data/atlas_hazards/cmip6/indices_seasonal/by_season/",timeframe_choice,"/hazard_timeseries")
}


# Set data directories
#mapspam_dir<-"/home/jovyan/common_data/atlas_mapspam/raw"
#glw3_dir<-"/home/jovyan/common_data/atlas_glw"
#ls_vop_dir<-"/home/jovyan/common_data/atlas_livestock/intermediate/vop_total"

# Setup for Pete working locally ####
timeframe_choice<-"annual"

# Set data directories
#mapspam_dir<-"Data/mapspam"
#glw3_dir<-"Data/GLW3"
#ls_vop_dir<-"Data/livestock_vop"

# Set output directories
haz_timeseries_dir<-paste0("Data/hazard_timeseries/",timeframe_choice)