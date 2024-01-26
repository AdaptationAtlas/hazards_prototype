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
adaptive_capacity_cast<-dcast(adaptive_capacity,admin_code+admin_name+iso3+admin_level+total_pop+rural_pop~vulnerability,value.var="value_binary")

# Hazard Risk x VoP #####
haz_risk_vop_dir<-paste0("Data/hazard_risk_vop/",timeframe_choice)
files<-paste0("haz_risk_vop_",tolower(severity_classes$description),".parquet")
local_files<-file.path(haz_risk_vop_dir,files)

haz_risk_vop<-rbindlist(lapply(1:length(local_files),FUN=function(i){
  print(local_files[i])
  if(file.exists(local_files[i])){
  arrow::read_parquet(local_files[i])
  }else{
    warning(paste0("File does not exist: ",local_files[i]))
    NULL
  }
}))

# Create admin_code to match with adaptive capacity layer

haz_risk_vop[,admin_code:=admin0_nam
             ][!is.na(admin1_nam) & is.na(admin2_nam),admin_code:=paste0(admin1_nam[1],"_",admin0_nam[1]),by=list(admin1_nam,admin0_nam)
               ][!is.na(admin2_nam),admin_code:=paste0(admin2_nam[1],"_",admin1_nam[1],"_",admin0_nam[1]),by=list(admin2_nam,admin1_nam,admin0_nam)]


no_match<-haz_risk_vop[,unique(admin_code)] %in% adaptive_capacity[,unique(admin_code)]
haz_risk_vop[,unique(admin_code)][!no_match]

adaptive_capacity_cast[grep("SNNP",admin_code),unique(admin_code)]
haz_risk_vop[,unique(admin_code)[grep("SNNP",unique(admin_code))]]


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

# Reduce file size - haz_vop_risk_ac ####
# These are the interactions of climate hazards we have in the data
crop_heat<-c("NTx35","TAVG_G")
crop_wet<-c("NDWL0","PTOT_G")
crop_dry<-c("PTOT_L","NDWS")

animal_heat<-c("THI_max") # THI_mean or THI_max can be used here (or both)
animal_wet<-c("NDWL0","PTOT_G")
animal_dry<-c("NDD","PTOT_L","NDWS")

# We will using  crops = heat NTx35, wet NDWL0, dry NDWS & animals =  heat THI, wet NDWL0, dry NDWS
crop_haz<-sort(c(heat="NTx35",dry="NDWL0",wet="NDWS"))
crop_comb<-apply(combn(crop_haz,m=2),2,paste,collapse="+")
crop_comb<-c(crop_haz,crop_comb,paste(crop_haz,collapse = "+"))

# Read in the data
file<-file.path(folder,"haz_risk_vop_ac.parquet")
data<-arrow::read_parquet(file)

# Subset hazard_risk_vop_ac to a specific hazard to reduce file size
data_ss<-data[hazard %in% crop_comb]

# Set population fields to be integer to reduce file size
data_ss[,total_pop:=as.integer(total_pop)][,rural_pop:=as.integer(rural_pop)]
data_ss[,value:=round(value,0)]

# Remove crops we don't need
rm_crops<-c("rapeseed","sugarbeet")
data_ss<-data_ss[!crop %in% rm_crops]

# Remove country from admin_code
countries<-data_ss[,unique(unlist(tail(tstrsplit(admin_code[1],"_"),1))),by=admin_code][,unique(V1[!is.na(V1)])]
data_ss[admin_code %in% countries,admin_code:=NA]
countries<-paste0("_",countries)
data_ss[,admin_code:=gsub(paste(countries,collapse = "|"),"",admin_code[1]),by=admin_code]

arrow::write_parquet(data_ss,file.path(folder,"haz_risk_vop_ac_reduced.parquet"))


