# Package location
package_dir<-getwd()

# Set working directory
working_dir<-"/home/jovyan/common_data/hazards_prototype"
if(!dir.exists(working_dir)){
  dir.create(working_dir,recursive=T)
}

setwd(working_dir)
