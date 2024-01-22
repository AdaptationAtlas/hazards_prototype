# Setup if working from Atlas server####
# Project location
package_dir<-getwd()

# Set working directory
working_dir<-"/home/jovyan/common_data/hazards_prototype"
if(!dir.exists(working_dir)){
  dir.create(working_dir,recursive=T)
}

setwd(working_dir)

geoboundaries_location<-"/home/jovyan/common_data/atlas_boundaries/processed/geoboundaries_6.0.0"
mapspam_dir<-"/home/jovyan/common_data/atlas_mapspam/raw"
glw3_dir<-"/home/jovyan/common_data/atlas_glw"
afr_highlands_dir<-"/home/jovyan/common_data/atlas_afr_highlands"
ls_vop_dir<-"/home/jovyan/common_data/atlas_livestock/intermediate/vop_total"
hpop_dir<-"/home/jovyan/common_data/atlas_pop/raw"

# Setup for Pete working locally ####

geoboundaries_location<-"Data/geoboundaries"
mapspam_dir<-"Data/mapspam"
glw3_dir<-"Data/GLW3"
afr_highlands_dir<-"Data/afr_highlands"
ls_vop_dir<-"Data/livestock_vop"
hpop_dir<-"Data/atlas_pop"