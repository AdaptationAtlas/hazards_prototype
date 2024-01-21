# Setup if working from Atlas server####
# Package location
package_dir<-getwd()

# Set working directory
working_dir<-"/home/jovyan/common_data/hazards_prototype"
if(!dir.exists(working_dir)){
  dir.create(working_dir,recursive=T)
}

setwd(working_dir)

# Geoboundaries location
geoboundaries_location<-"/home/jovyan/common_data/atlas_boundaries/processed/geoboundaries_6.0.0"
mapspam_dir<-"/home/jovyan/common_data/atlas_mapspam/raw"


# Setup for Pete working locally ####

# MapSPAM
geoboundaries_location<-"Data/geoboundaries"
mapspam_dir<-"Data/mapspam"