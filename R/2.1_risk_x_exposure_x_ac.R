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
overwrite<-F
if(!file.exists(local_file)|overwrite==T){
  s3_file <- paste0("s3://digital-atlas/vulnerability/",file)
  s3fs::s3_file_download(path=s3_file,new_path=local_file,overwrite = T)
}

adaptive_capacity<-data.table(arrow::read_parquet(local_file))

# Cast dataset
adaptive_capacity_cast<-dcast(adaptive_capacity,admin0_name+admin1_name+admin2_name+iso3+total_pop+rural_pop~vulnerability,value.var="value_binary")

# Hazard Risk x VoP #####
# Data is found in "s3://digital-atlas/risk_prototype/data/hazard_risk_vop/annual" for example

interaction<-F
haz_risk_vop_dir<-paste0("Data/hazard_risk_vop/",timeframe_choice)
if(interaction==T){
  files<-list.files(haz_risk_vop_dir,"_adm_int",full.names = T)
}else{
  files<-list.files(haz_risk_vop_dir,"_adm_solo",full.names = T)
}


haz_risk_vop<-rbindlist(lapply(1:length(files),FUN=function(i){
  print(files[i])
  if(file.exists(files[i])){
  arrow::read_parquet(files[i])
  }else{
    warning(paste0("File does not exist: ",files[i]))
    NULL
  }
}))

haz_risk_vop<-haz_risk_vop[,list(admin0_name,admin1_name,admin2_name,scenario,timeframe,crop,severity,hazard_vars,hazard,value)]

# Check admin names all match ####
if(F){
    # Create admin_code to match with adaptive capacity layer
  haz_risk_vop[,admin_code:=admin0_name
               ][!is.na(admin1_name) & is.na(admin2_name),admin_code:=paste0(admin1_name[1],"_",admin0_name[1]),by=list(admin1_name,admin0_name)
                 ][!is.na(admin2_name),admin_code:=paste0(admin2_name[1],"_",admin1_name[1],"_",admin0_name[1]),by=list(admin2_name,admin1_name,admin0_name)
                   ][,admin_code:=tolower(admin_code[1]),by=admin_code]
  
  adaptive_capacity_cast[,admin_code:=admin0_name
                         ][!is.na(admin1_name) & is.na(admin2_name),admin_code:=paste0(admin1_name[1],"_",admin0_name[1]),by=list(admin1_name,admin0_name)
                           ][!is.na(admin2_name),admin_code:=paste0(admin2_name[1],"_",admin1_name[1],"_",admin0_name[1]),by=list(admin2_name,admin1_name,admin0_name)
                             ][,admin_code:=tolower(admin_code[1]),by=admin_code]
  
  # Check for non matches
  no_match<-haz_risk_vop[,unique(admin_code)] %in% adaptive_capacity_cast[,unique(admin_code)]
  (no_match_haz<-haz_risk_vop[,unique(admin_code)][!no_match])
  
  no_match<-adaptive_capacity_cast[,unique(admin_code)] %in% haz_risk_vop[,unique(admin_code)]
  (no_match_ac<-adaptive_capacity_cast[,unique(admin_code)][!no_match])
  
  haz_risk_vop[,admin_code:=NULL]
  adaptive_capacity_cast[,admin_code:=NULL]
}

# Append adaptive capacity data to haz_risk_vop ####
haz_risk_vop_ac_dir<-paste0("Data/hazard_risk_vop_ac/",timeframe_choice)
if(!dir.exists(haz_risk_vop_ac_dir)){
  dir.create(haz_risk_vop_ac_dir,recursive=T)
}

if(interaction==T){
  file<-file.path(haz_risk_vop_ac_dir,"haz_risk_vop_int_ac.parquet")
}else{
  file<-file.path(haz_risk_vop_ac_dir,"haz_risk_vop_solo_ac.parquet")
}

# Merge ac and risk exposure datasets
haz_risk_vop_ac<-merge(haz_risk_vop,adaptive_capacity_cast[,-c("iso3")],all.x=T)

# Save merged dataset
arrow::write_parquet(haz_risk_vop_ac,file)

# Reduce file size of merged data ####

# Read in the data
if(is.null(haz_risk_vop_ac)){
  if(interaction==T){
    file<-file.path(haz_risk_vop_ac_dir,"haz_risk_vop_int_ac.parquet")
  }else{
    file<-file.path(haz_risk_vop_ac_dir,"haz_risk_vop_solo_ac.parquet")
  }
  haz_risk_vop_ac<-arrow::read_parquet(file)
}


# Subset data to a specific hazard combination to reduce file size ####
# Set crop hazard combination
crop_haz<-c(dry="NDWS",heat="NTx35",wet="NDWL0")

# Set animal hazard combination
ani_haz<-c(dry="NDWS",heat="THI_max",wet="NDWL0")

# Join crop and animal hazard combinations
if(interaction==T){
  haz<-c(paste0(crop_haz,collapse = "+"),paste0(ani_haz,collapse = "+"))
}else{
  haz<-unique(c(crop_haz,ani_haz))
}
# Subset data

data_ss<-haz_risk_vop_ac[hazard_vars %in% haz]


# Set population fields to be integer to reduce file size
data_ss[,total_pop:=as.integer(total_pop)][,rural_pop:=as.integer(rural_pop)]
data_ss[,value:=round(value,0)]

# Remove crops we don't need
rm_crops<-c("rapeseed","sugarbeet")
data_ss<-data_ss[!crop %in% rm_crops]

# Remove hazard_vars column 
data_ss[,hazard_vars:=NULL]

# Save results
file_r<-gsub("ac.parquet","ac_reduced.parquet",file)
arrow::write_parquet(data_ss,file_r)


