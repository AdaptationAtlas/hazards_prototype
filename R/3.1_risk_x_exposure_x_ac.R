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
              "s3fs",
              "ggplot2")

# Call the function to install and load packages
load_and_install_packages(packages)

# Set up workspace ####
haz_class_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_classes.csv"
severity_classes<-unique(fread(haz_class_url)[,list(description,value)])

# Look at solo or interaction hazard risk data?
interaction<-T # set to F if you want to look at solo data

# Load adaptive capacity data ####

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
adaptive_capacity_cast<-data.table(dcast(adaptive_capacity,admin0_name+admin1_name+admin2_name+iso3+total_pop+rural_pop~vulnerability,value.var="value_binary"))

# Load hazard risk x VoP data #####
# Data is found in "s3://digital-atlas/risk_prototype/data/hazard_risk_vop/annual" for example

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

# Load total VoP data ####
# Data is found in "s3://digital-atlas/risk_prototype/data/exposure/annual" for example
exposure_dir<-"Data/exposure/"
exposure<-data.table(arrow::read_parquet(file.path(exposure_dir,"exposure_adm_sum.parquet")))
# Subset exposure to VoP
exposure<-exposure[exposure=="vop"][,exposure:=NULL]
setnames(exposure,"value","total_value")


# merge data
haz_risk_vop<-merge(haz_risk_vop,exposure,all.x=T)

# Work out total exposure 
haz_risk_vop_any<-haz_risk_vop[hazard=="any"]
haz_risk_vop_any[,value_non_exposed:=round(total_value-value,0)]
haz_risk_vop_any[value_non_exposed<(-1000),]
haz_risk_vop_any[,hazard:="no hazard"][,value:=value_non_exposed][,value_non_exposed:=NULL]

# combine back
haz_risk_vop<-rbind(haz_risk_vop,haz_risk_vop_any)

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
if(interaction==T){
  file<-file.path(haz_risk_vop_ac_dir,"haz_risk_vop_int_ac.parquet")
}else{
  file<-file.path(haz_risk_vop_ac_dir,"haz_risk_vop_solo_ac.parquet")
}

# Merge ac and risk exposure datasets
haz_risk_vop_ac<-merge(haz_risk_vop,adaptive_capacity_cast[,-c("iso3")],all.x=T)

# Save merged dataset ####
arrow::write_parquet(haz_risk_vop_ac,file)
# ==================================== #####
# Reduce file size of merged data (interactions only) ####

# Read in the data
if(!exists("haz_risk_vop_ac")){
  if(interaction==T){
    file<-file.path(haz_risk_vop_ac_dir,"haz_risk_vop_int_ac.parquet")
  }else{
    file<-file.path(haz_risk_vop_ac_dir,"haz_risk_vop_solo_ac.parquet")
  }
  haz_risk_vop_ac<-arrow::read_parquet(file)
}

# Subset data to a specific hazard combination to reduce file size ####
# Set population fields to be integer to reduce file size

haz_risk_vop_ac[,total_pop:=as.integer(total_pop)
                ][,rural_pop:=as.integer(rural_pop)
                  ][,value:=round(value,0)]

haz_risk_vop_ac[,scenario_x_time:=unique(paste0(scenario[1],"-",timeframe[1])),by=list(scenario,timeframe)]

# Create function that subsets parquet table
reduce_parquet<-function(dry,heat_crop,heat_ani,wet,interaction,data,rm_crops,filename,folder,severities,scenarios,admin_level,rm_st){
  # Set crop hazard combination
  crop_haz<-c(dry=dry,heat=heat_crop,wet=wet)
  
  # Set animal hazard combination
  ani_haz<-c(dry=dry,heat=heat_ani,wet=wet)
  
  # Join crop and animal hazard combinations
  if(interaction==T){
    haz<-c(paste0(crop_haz,collapse = "+"),paste0(ani_haz,collapse = "+"))
  }else{
    haz<-unique(c(crop_haz,ani_haz))
  }
  
  if(admin_level!="all"){
    # Subset data to admin_level
    if(admin_level=="admin2"){
      data<-data[!is.na(admin2_name)]
    }
    
    if(admin_level=="admin1"){
      data<-data[is.na(admin2_name) & !is.na(admin1_name)]
    }
    
    if(admin_level=="admin0"){
      data<-data[is.na(admin1_name)]
    }
  }
  # Subset data to hazards
  data_ss<-data[hazard_vars %in% haz & severity %in% severities & scenario_x_time %in% scenarios ]
  
  # Remove crops we don't need
  data_ss<-data_ss[!crop %in% rm_crops]
  
  # Remove hazard_vars column 
  data_ss[,hazard_vars:=NULL]
  
  if(rm_st){
    data_ss[,scenario_x_time:=NULL]
  }
  
  # Save results
  if(!is.null(filename)){
    file_r<-file.path(folder,paste0(filename,".parquet"))
    arrow::write_parquet(data_ss,file_r)
  }
  
  return(data_ss)
}

scenarios<-haz_risk_vop_ac[,unique(scenario_x_time)]

reduce_parquet(data=haz_risk_vop_ac,
               dry="NDWS",
               heat_crop="NTx35",
               heat_ani="THI_max",
               wet="NDWL0",
               severities=c("moderate","severe","extreme"),
               admin_level<-"all", # subsets to admin0, admin1, admin2
               scenarios=scenarios,
               interaction=T,
               rm_crops=c("rapeseed","sugarbeet"),
               filename = "haz_risk_vop_int_ac_reduced",
               folder=haz_risk_vop_ac_dir,
               rm_st=T)

if(F){
# Check resulting file
check<-arrow::read_parquet(file.path(haz_risk_vop_ac_dir,"haz_risk_vop_int_ac_reduced.parquet"))
check<-arrow::read_parquet("haz_risk_vop_int_ac_reduced.parquet")

head(check)
dim(check)
check[,unique(hazard)]

admin0<-"Malawi"
bar_var<-"%"

vop_data_ordered<-check[hazard!="any"
                       ][crop!="generic"
                         ][admin0_name %in% admin0 #& crop %in% input$crop
                           ][,list(admin0_name,scenario,timeframe,crop,severity,hazard,value)
                             ][,list(value=sum(value,na.rm=T)),by=list(crop,hazard,scenario,timeframe)
                               ][,total_value:=sum(value,na.rm=T),by=list(crop,scenario,timeframe)
                                 ][,value_p:=100*(value/total_value)]




  vop_data_ordered$hazard <- factor(vop_data_ordered$hazard, levels = rev(c("dry", "heat", "wet", "dry+heat", "dry+wet", "heat+wet","dry+heat+wet","no hazard")))
  custom_colors <- c("dry" = "#F28E2B", "heat" = "#E15759", "wet" = "#76B7B2", "dry+heat" = "#59A14F", "dry+wet" = "#EDC948", "heat+wet" = "#B07AA1","dry+heat+wet" = "black", "no hazard" = "grey90")
  
  if(bar_var=="IntDollar"){
    vop_data_ordered[,plot_value:=value]
    y_lab<-"Total Value (IntDollar)"
  }else{
    vop_data_ordered[,plot_value:=value_p]
    y_lab<-"Total Value (%)"
  }
  
  livestock<-vop_data_ordered[grep("tropical|highland",crop),sort(unique(crop))]
  crops<-vop_data_ordered[!grepl("tropical|highland",crop),sort(unique(crop))]
  
  vop_data_ordered[,crop:=factor(crop,levels=rev(c(crops,livestock)))]
  
  ggplot(vop_data_ordered[!total_value==0 & scenario=="historic"], aes(x = crop, y = plot_value, fill = hazard)) +
    geom_bar(stat = "identity", position = "stack") +
    labs(x = "Crop", y = y_lab, fill = "Hazard") +
    coord_flip() +  # Flipping coordinates to have crops on the y-axis
    theme_minimal() +
    scale_fill_manual(values = custom_colors) + # Using custom color palette
    theme(
      axis.text = element_text(size=14),      
      axis.title = element_text(size = 16), # Adjusting axis title size
      legend.title = element_text(size = 14), # Adjusting legend title size
      legend.text = element_text(size = 14), # Adjusting legend text size
      panel.spacing.y = unit(0.5, "lines") # Reducing space between bars
    )
}

# Subset loop ####

# Ideally folder structure of
# 1) admin0, admin1 and admin2
# 2) files named by hazard, scenario, severity
haz<-haz_risk_vop_ac[,unique(hazard_vars)]
haz<-haz[!grepl("THI",haz)]
haz<-strsplit(haz,"[+]")

folder<-paste0(haz_risk_vop_ac_dir,"/subsets")
if(!dir.exists(folder)){
  dir.create(folder,recursive = T)
}

admin_level<-"admin0"

for(i in 1:length(haz)){
  for(SEV in c("moderate","severe","extreme")){
    for(j in 1:length(scenarios)){
      filename<-paste0(admin_level,"-",paste0(haz[[i]],collapse = "+"),"-",SEV,"-",scenarios[j])
      print(filename)
      X<-reduce_parquet(data=haz_risk_vop_ac,
                     dry=haz[[i]][1],
                     heat_crop=haz[[i]][2],
                     heat_ani="THI_max",
                     wet=haz[[i]][3],
                     severities=SEV,
                     admin_levels<-admin_level,
                     scenarios=scenarios[j],
                     interaction=T,
                     rm_crops=c("rapeseed","sugarbeet"),
                     filename = filename,
                     folder=folder,
                     rm_st=F)
    }
}
}





