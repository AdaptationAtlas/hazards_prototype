# Please run 0_server_setup.R before executing this script

# Note - this entire script can be generalized.
  # We would need to pull in the stat from haz_meta to automate the "_mean-G" text creation (e.g. 2.2)
  # Would also need to automate the G/L coding
  # Livestock and crops could combined, but the highland/tropical split for livestock would need to be incorporated
  # The entire workflow here could be generalized to generate area merge with total area. The resulting table could then
  # Be wrangled to give % and % change from baseline.

# First run server_setup script
# 0) Load R functions & packages ####
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

load_and_install_packages <- function(packages) {
  for (package in packages) {
    if (!require(package, character.only = TRUE)) {
      install.packages(package)
      library(package, character.only = TRUE)
    }
  }
}

# List of packages to be loaded
packages <- c("arrow",
              "geoarrow",
              "sf",
              "terra", 
              "data.table", 
              "doFuture",
              "future.apply",
              "exactextractr",
              "parallel",
              "pbapply")

# Call the function to install and load packages
load_and_install_packages(packages)

merge_admin_extract<-function(data_ex) {
  
  # Define a mapping of administrative level names to short codes.
  levels <- c(admin0 = "adm0", admin1 = "adm1", admin2 = "adm2")

  # Process the extracted data to format it for analysis or further processing.
  data_ex <- rbindlist(lapply(1:length(levels), FUN = function(i) {
    level <- levels[i]
    
    # Convert the data to a data.table and remove specific columns.
    data <- data.table(data.frame(data_ex[[names(level)]]))
    data <- data[, !c("admin_name", "iso3")]
    
    # Determine the administrative level being processed and adjust the data accordingly.
    admin <- "admin0_name"
    if (level %in% c("adm1", "adm2")) {
      admin <- c(admin, "admin1_name")
      data <- suppressWarnings(data[, !"a1_a0"])
    }
    
    if (level == "adm2") {
      admin <- c(admin, "admin2_name")
      data <- suppressWarnings(data[, !"a2_a1_a0"])
    }
    
    # Adjust column names and reshape the data.
    colnames(data) <- gsub("_nam$", "_name", colnames(data))
    data <- data.table(melt(data, id.vars = admin))
    
    data
  }), fill = T)
  
  
  # Return the processed or read data.
  return(data_ex)
}

  # 0.1) Set up workspace #####
haz_class<-fread(haz_class_url)
haz_class[,direction2:="G"][direction=="<",direction2:="L"]
haz_meta<-fread(haz_meta_url)
# Make cell size raster
base_cellsize<-terra::cellSize(base_rast,unit="km")

# 0.2) Load admin boundaries #####
Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
  file<-geo_files_local[i]
  data<-arrow::open_dataset(file)
  data <- data |> sf::st_as_sf() |> terra::vect()
  data
})
names(Geographies)<-names(geo_files_local)

# 1) % area of precipitation increase or decrease by admin vect ####
# Create save folder
haz_mean_ptot_dir<-file.path(haz_mean_dir,"ptot_perc")
if(!dir.exists(haz_mean_ptot_dir)){
  dir.create(haz_mean_ptot_dir)
}

# Load annual precip rasters

files<-list.files(haz_mean_dir,".tif",full.names = T)
files<-grep("PTOT",files,value=T)
files<-files[!grepl("change",files)]
files_hist<-grep("historic",files,value = T)
files_fut<-files[!files %in% files_hist]

# Temporarily exclude problem folders until issues with input data are resolved ####
exclude_dirs<-file.path(haz_mean_dir,"ssp245_EC-Earth3_2021_2040_PTOT_sum.tif")
files_fut<-files_fut[!files_fut %in% exclude_dirs]

save_file<-file.path(haz_mean_ptot_dir,"ptot_perc_change.tif")
do_save<-F
overwrite<-T

if(!file.exists(save_file)|overwrite==T){
  

    var<-gsub("historical_","",tail(tstrsplit(file_hist,"/"),1))
    files_fut_ss<-grep(var,files_fut,value=T)
    future<-terra::rast(files_fut_ss)
    past<-terra::rast(file_hist)
    
    diff<-future-past
    change<-round(100*(diff)/past,1)
    
    names(change)<-gsub(".tif","",basename(files_fut_ss))
    names(diff)<-gsub(".tif","",basename(files_fut_ss))
    
  if(do_save){
   terra::writeRaster(change,filename=save_file,filetype = "COG",gdal = c("OVERVIEWS"="NONE"))
   terra::writeRaster(diff,filename=gsub("_change","_diff",save_file), filetype = "COG",gdal = c("OVERVIEWS"="NONE"))
  }
  
}else{
  change<-terra::rast(save_file)
  diff<-terra::rast(gsub("_change","_diff",save_file))
}


# Increasing area
change_inc<-terra::classify(change,rcl=data.frame(from=c(-999999999,5),to=c(5,99999999999),becomes=c(0,1)))
change_inc<-change_inc*base_cellsize
# Decreasing area
change_dec<-terra::classify(change,rcl=data.frame(from=c(-999999999,-5),to=c(-5,99999999999),becomes=c(1,0)))
change_dec<-change_dec*base_cellsize

# Sum areas by admin vectors
base_areas<-admin_extract(base_cellsize,
              Geographies = Geographies,
              FUN = "sum")

change_inc<-admin_extract(change_inc,
                          Geographies = Geographies,
                          FUN = "sum")

change_dec<-admin_extract(change_dec,
                          Geographies = Geographies,
                          FUN = "sum")

diff<-admin_extract(diff,
                    Geographies = Geographies,
                    FUN = "mean")

# Tabulate data
change_inc<-merge_admin_extract(change_inc)[,direction:="increase_5"]
change_dec<-merge_admin_extract(change_dec)[,direction:="decrease_5"]

base_areas<-merge_admin_extract(base_areas)[,direction:="total"]
setnames(base_areas,"value","total")

diff<-merge_admin_extract(diff)

# Work out percentage change
change<-rbind(change_inc,change_dec)
change<-merge(change,base_areas[,list(admin0_name,admin1_name,admin2_name,total)],all.x=T)
change[,value:=round(100*value/total,1)][,total:=NULL]

# Wrangle variable name
var_names<-change$variable
var_names<-gsub("sum.|_PTOT_sum","",var_names)
var_names<-gsub("1_2","1-2",var_names)
var_names<-do.call("cbind",tstrsplit(var_names,"_"))
colnames(var_names)<-c("scenario","model","timeframe")

change<-cbind(change,var_names)[,variable:="PTOT"][,stat:="perc_change"]
diff<-cbind(diff,var_names)[,variable:="PTOT"][,stat:="diff"]


# Generate ensemble data from models
change_ens<-change[!grepl("ENSEMBLE",model)]
change_ens<-change_ens[,list(mean=mean(value,na.rm=T),min=min(value,na.rm=T),max=max(value,na.rm=T),sd=sd(value,na.rm=T)),
                       by=list(admin0_name,admin1_name,admin2_name,scenario,timeframe,direction,variable,stat)]

diff_ens<-diff[!grepl("ENSEMBLE",model)]
diff_ens<-diff_ens[,list(mean=mean(value,na.rm=T),min=min(value,na.rm=T),max=max(value,na.rm=T),sd=sd(value,na.rm=T)),
                       by=list(admin0_name,admin1_name,admin2_name,scenario,timeframe,variable,stat)]

# save results
arrow::write_parquet(change,file.path(haz_mean_ptot_dir,"ptot_change_by_model.parquet"))
arrow::write_parquet(change_ens,file.path(haz_mean_ptot_dir,"ptot_change_ensemble.parquet"))

arrow::write_parquet(diff,file.path(haz_mean_ptot_dir,"ptot_diff_by_model.parquet"))
arrow::write_parquet(diff_ens,file.path(haz_mean_ptot_dir,"ptot_diff_ensemble.parquet"))

# 2) % area of severe or extreme crop or livestock heat stress ####
  # 2.1) Livestock #####
  # set save location
  haz_mean_thi_dir<-file.path(haz_mean_dir,"thi_perc")
  if(!dir.exists(haz_mean_thi_dir)){
    dir.create(haz_mean_thi_dir)
  }
  
  # list data files
  files<-list.files(haz_time_risk_dir,"THI_max",full.names =T)
  
  # get severity thresholds
  cat_thresholds<-haz_class[index_name=="THI_max" & 
                              description %in% c("Severe","Extreme") & 
                              crop  %in% c("cattle_highland","cattle_tropical"),list(index_name,crop,description,threshold)
                            ][,code:=paste0("THI_max_max-G",threshold)]
  
  # get highland/lowland mask
  highlands<-terra::rast(afr_highlands_file)
  highlands<-terra::resample(highlands,base_rast,method="near")
  tropical<-classify(highlands,data.frame(from=c(0,1),to=c(1,0)))
  
  # subset data files to 
  data<-pblapply(1:nrow(cat_thresholds),FUN=function(i){
    files_ss<-grep(cat_thresholds[i,code],files,value=T)
    data<-terra::rast(files_ss)
    
    # Apply highland/lowland mask
    if(cat_thresholds[i,grepl("tropical",crop)]){
      data<-data*tropical
    }else{
      data<-data*highlands
    }
    
    names(data)<-paste0(gsub(".tif","",basename(files_ss)),"_",cat_thresholds[i,tolower(description)])
    
    data
  })
  
  data_sev<-data[grep("Severe",cat_thresholds$description)]
  data_sev<-data_sev[[1]]+data_sev[[2]]
  
  data_ext<-data[grep("Extreme",cat_thresholds$description)]
  data_ext<-data_ext[[1]]+data_ext[[2]]
  
  data<-c(data_sev,data_ext)
  data<-data*base_cellsize
  
  # Extract by admin area
  base_areas<-admin_extract(base_cellsize,
                            Geographies = Geographies,
                            FUN = "sum")
  
  data<-admin_extract(data,
                      Geographies = Geographies,
                      FUN = "sum")
  
  # Tabulate data
  data<-merge_admin_extract(data)
  base_areas<-merge_admin_extract(base_areas)
  setnames(base_areas,"value","total")
  
  # Work out percentage change
  data<-merge(data,base_areas[,list(admin0_name,admin1_name,admin2_name,total)],all.x=T)
  data[,value:=round(100*value/total,1)][,total:=NULL]
  
  # Wrangle variable name
  var_names<-data$variable
  var_names<-gsub("sum.|_THI_max_max","",var_names)
  var_names<-gsub("1_2","1-2",var_names)
  var_names<-gsub(".G","_",var_names)
  var_names<-gsub("historical","historical_historical_historical",var_names)
  var_names<-do.call("cbind",tstrsplit(var_names,"_"))[,c(1:3,5)]
  colnames(var_names)<-c("scenario","model","timeframe","severity")
  
  data<-cbind(data,var_names)[,hazard:="THI"][,variable:="perc_area"][,crop:="cattle"]
  
  # Generate ensemble data from models
  data_ens<-data[,list(mean=mean(value,na.rm=T),
                       min=min(value,na.rm=T),
                       max=max(value,na.rm=T),
                       sd=round(sd(value,na.rm=T),1)),
                         by=list(admin0_name,admin1_name,admin2_name,scenario,timeframe,variable,severity,variable,crop)]
  
  arrow::write_parquet(data,file.path(haz_mean_thi_dir,"thi_perc_area_by_model.parquet"))
  arrow::write_parquet(data_ens,file.path(haz_mean_thi_dir,"thi_perc_area_ensemble.parquet"))

  # 2.2) Crops #####

haz_mean_ntx_dir<-file.path(haz_mean_dir,"ntx_perc")
if(!dir.exists(haz_mean_ntx_dir)){
  dir.create(haz_mean_ntx_dir)
}

# choose hazards
haz_choices<-c("NTx35","NTx40")
# choose crops
crop_choices<-"generic"
# choose severity classes
sev_classes<-c("Severe","Extreme")


choices<-expand.grid(haz=haz_choices,crop=crop_choices)

data<-rbindlist(lapply(1:length(choices),FUN=function(j){
  haz<-as.character(choices$haz[j])
  crop_focus<-as.character(choices$crop[j])

  # list data files
  files<-list.files(haz_time_risk_dir,haz,full.names =T)
  files<-files[!grepl("ENSEMBLE",files)]

  
  # get severity thresholds
  cat_thresholds<-haz_class[index_name==haz & 
                              description %in% sev_classes & 
                              crop  %in% crop_focus,list(index_name,crop,description,threshold)
  ][,code:=paste0(haz,"_mean-G",threshold)] # mean-G -> this needs to be generalized
  
  data<-terra::rast(lapply(1:length(sev_classes),FUN=function(i){
    files_ss<-grep(cat_thresholds[description==sev_classes[i],code],files,value=T)
    data<-terra::rast(files_ss)
    names(data)<-paste0(gsub(".tif","",basename(files_ss)),"_",tolower(sev_classes[i]))
    data
  }))
  
  data<-data*base_cellsize
  
  # Extract by admin area
  base_areas<-admin_extract(base_cellsize,
                            Geographies = Geographies,
                            FUN = "sum")
  
  data<-admin_extract(data,
                      Geographies = Geographies,
                      FUN = "sum",
                      max_cells_in_memory = 3*10^8)
  
  # Tabulate data
  data<-merge_admin_extract(data)
  setnames(data,"value","area")
  base_areas<-merge_admin_extract(base_areas)
  setnames(base_areas,"value","total_area")
  
  # Work out percentage change
  data<-merge(data,base_areas[,list(admin0_name,admin1_name,admin2_name,total_area)],all.x=T)
  data[,perc:=round(100*area/total_area,1)]
  
  # Wrangle variable name
  var_names<-data$variable
  var_names<-gsub(paste0("sum.|_",haz,"_mean"),"",var_names) # _mean needs to be generalized
  var_names<-gsub("1_2","1-2",var_names)
  var_names<-gsub(".G","_",var_names) # .G needs to be generalized
  var_names<-gsub("historical","historical_historical_historical",var_names)
  var_names<-do.call("cbind",tstrsplit(var_names,"_"))[,c(1:3,5)]
  colnames(var_names)<-c("scenario","model","timeframe","severity")
  
  data<-cbind(data,var_names)[,hazard:=haz][,crop:=crop_focus]
  
  data
  
}))

  # 2.3) Generate ensemble data from models ####
  data_ens<-data
  setnames(data_ens,"perc","value")

  data_ens<-data[,list(mean=mean(value,na.rm=T),
                       min=min(value,na.rm=T),
                       max=max(value,na.rm=T),
                       sd=round(sd(value,na.rm=T),1)),
                 by=list(admin0_name,admin1_name,admin2_name,scenario,timeframe,hazard,severity,crop)
                 ][,variable:="perc_area"]


  data_ens[scenario=="historical",c("min","max","sd"):=NA]
  
  arrow::write_parquet(data,file.path(haz_mean_ntx_dir,"ntx_perc_area_by_model.parquet"))
  arrow::write_parquet(data_ens,file.path(haz_mean_ntx_dir,"ntx_perc_area_ensemble.parquet"))
  
# 3) Extreme drought or wet spells ####
  # choose hazards
  haz_choices<-c("NDWS","NDWL0")
  # choose crops
  crop_choices<-"generic"
  # choose severity classes
  sev_classes<-c("Severe","Extreme")
  
  choices<-expand.grid(haz=haz_choices,crop=crop_choices)
  extract_fun<-"mean"
  
  data<-rbindlist(lapply(1:length(choices),FUN=function(j){
    haz<-as.character(choices$haz[j])
    crop_focus<-as.character(choices$crop[j])
    
    # list data files
    files<-list.files(haz_time_risk_dir,haz,full.names =T)
    files<-files[!grepl("ENSEMBLE",files)]
    
    # get stat
    stat<-haz_meta[code==haz,`function`]
    
    # get severity thresholds
    cat_thresholds<-haz_class[index_name==haz & 
                                description %in% sev_classes & 
                                crop  %in% crop_focus,list(index_name,crop,description,threshold,direction2)
    ][,code:=paste0(haz,"_",stat,"-",direction2,threshold)]
    
    data<-terra::rast(lapply(1:length(sev_classes),FUN=function(i){
      files_ss<-grep(cat_thresholds[description==sev_classes[i],code],files,value=T)
      data<-terra::rast(files_ss)
      names(data)<-paste0(gsub(".tif","",basename(files_ss)),"_",tolower(sev_classes[i]))
      data
    }))
    
    data<-admin_extract(data,
                        Geographies = Geographies,
                        FUN = extract_fun,
                        max_cells_in_memory = 3*10^8)
    
    # Tabulate data
    data<-merge_admin_extract(data)

    # Wrangle variable name
    var_names<-data$variable
    var_names<-gsub(paste0(extract_fun,".|_",haz,"_",stat),"",var_names)
    var_names<-gsub("1_2","1-2",var_names)
    var_names<-gsub(paste0(".",cat_thresholds[1,direction2]),"_",var_names)
    var_names<-gsub("historical","historical_historical_historical",var_names)
    var_names<-do.call("cbind",tstrsplit(var_names,"_"))[,c(1:3,5)]
    colnames(var_names)<-c("scenario","model","timeframe","severity")
    
    data<-cbind(data,var_names)[,hazard:=haz][,crop:=crop_focus][,variable:="frequency"]
    
    data
    
  }))
  
  years_hist<-terra::nlyr(terra::rast(list.files(haz_timeseries_dir,"hist",full.names =T)[1]))
  years_scen<-terra::nlyr(terra::rast(list.files(haz_timeseries_dir,"ssp245",full.names =T)[1]))
  
  data2<-data.table::copy(data)
  data2<-data2[scenario!="historical",value:=round(value*years_scen,0)
               ][scenario=="historical",value:=round(value*years_hist,0)
                 ][,variable:="frequency_n"]
  
  data[,value:=round(value,2)]
  
  data<-rbind(data,data2)
  
  data[hazard=="NDWS",hazard_user:="drought"
       ][hazard=="NDWL0",hazard_user:="wet"]
  
  data_ens<-data[,list(mean=round(mean(value,na.rm=T),2),
                       min=min(value,na.rm=T),
                       max=max(value,na.rm=T),
                       sd=round(sd(value,na.rm=T),1)),
                 by=list(admin0_name,admin1_name,admin2_name,scenario,timeframe,hazard,hazard_user,severity,crop,variable)]
  
  data_ens[scenario=="historical",c("min","max","sd"):=NA]
  
  haz_time_risk_stats_dir<-file.path(haz_time_risk_dir,"stats")
  if(!dir.exists(haz_time_risk_stats_dir)){
    dir.create(haz_time_risk_stats_dir)
  }
  
  arrow::write_parquet(data,file.path(haz_time_risk_stats_dir,"haz_freq.parquet"))
  arrow::write_parquet(data_ens,file.path(haz_time_risk_stats_dir,"haz_freq_ensemble.parquet"))
  
  
    
  
  