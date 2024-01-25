# Install and load packages ####
load_and_install_packages <- function(packages) {
  for (package in packages) {
    if (!require(package, character.only = TRUE)) {
      install.packages(package)
      library(package, character.only = TRUE)
    }
  }
}

# List of packages to be loaded
packages <- c("terra", 
              "data.table", 
              "arrow",
              "stringr",
              "s3fs")

# Call the function to install and load packages
load_and_install_packages(packages)

# Set up workspace ####
haz_class_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_classes.csv"
severity_classes<-unique(fread(haz_class_url)[,list(description,value)])



# Adaptive Capacity ####
ac_dir<-"Data/adaptive_capacity"
if(!dir.exists(ac_dir)){
  dir.create(ac_dir,recursive=T)
}

# Download data from s3
file<-"vulnerability_adm_long.parquet"
local_file<-file.path(ac_dir,file)
if(!file.exists(local_file)){
  s3_file <- paste0("s3://digital-atlas/vulnerability/",file)
  s3fs::s3_file_download(path=s3_file,new_path=local_file,overwrite = T)
}

adaptive_capacity<-data.table(arrow::read_parquet(local_file))
# Cast dataset
adaptive_capacity_cast<-dcast(adaptive_capacity,admin_code+admin_name+iso3+admin_level~vulnerability,value.var="value_binary")

# Hazard Risk x VoP #####
haz_risk_vop_dir<-paste0("Data/hazard_risk_vop/",timeframe_choice)
files<-paste0("haz_risk_vop_",tolower(severity_classes$description),".parquet")
local_files<-file.path(haz_risk_vop_dir,files)

haz_risk_vop<-rbindlist(lapply(1:length(local_files),FUN=function(i){
  if(file.exists(local_files[i])){
  arrow::read_parquet(local_files[i])
  }else{
    warning(paste0("File does not exist: ",local_files[i]))
    NULL
  }
  
}))

# TEMP: Deal with naming issue of duplicate admin2 labels within countries ####
# This is a hack that just removes the duplicate rows, eventually the hazards are going to be updated to remove this issue

haz_risk_vop[,code:=paste0(admin_name,iso3,admin_level)]
adaptive_capacity_cast[,code:=paste0(admin_name,iso3,admin_level)]

# Find duplicate names
dups<-adaptive_capacity_cast[,names(table(code)[table(code)>1])]
adaptive_capacity_cast[code %in% dups]
N<-adaptive_capacity_cast[,which(code %in% dups)]
adaptive_capacity_cast[N,code]

# Remove duplicates
adaptive_capacity_cast<-adaptive_capacity_cast[!code %in% dups][,code:=NULL]
haz_risk_vop<-haz_risk_vop[!code %in% dups][,code:=NULL]

# Append adaptive capacity data to haz_risk_vop ####
haz_risk_vop_ac_dir<-paste0("Data/hazard_risk_vop_ac/",timeframe_choice)
if(!dir.exists(haz_risk_vop_ac_dir)){
  dir.create(haz_risk_vop_ac_dir,recursive=T)
}
file<-file.path(haz_risk_vop_ac_dir,"haz_risk_vop_ac.parquet")

haz_risk_vop_ac<-merge(haz_risk_vop,adaptive_capacity_cast,all.x=T)

arrow::write_parquet(haz_risk_vop_ac,file)
