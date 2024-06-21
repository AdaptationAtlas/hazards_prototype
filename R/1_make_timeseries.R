# Please run 0_server_setup.R before executing this script
# 1) Load R functions & packages ####
packages <- c("terra","data.table")
p_load(char=packages)

# 2) Set directories  ####
# Directory where monthly timeseries data generated from https://github.com/AdaptationAtlas/hazards/tree/main is stored
# Note this is currently only available on cglabs, but we will be adding the ability to downlaod these data from the s3
working_dir<-indices_dir
setwd(working_dir)

# Where will hazard time series be saved?
output_dir<-indices_dir2

# 3) Set up workspace ####

# List hazard folders
folders<-list.dirs(recursive=F)
folders<-folders[!grepl("ENSEMBLE|ipyn|gadm0|hazard_comb|indices_seasonal",folders)]
folders<-folders[!grepl("ssp126|ssp370|2061_2080|2081_2100",folders)] # these scenarios have incomplete information
folders<-unlist(tstrsplit(folders,"/",keep=2))

model_names<-unique(unlist(tstrsplit(folders[!grepl("histor",folders)],"_",keep=2)))

  # 3.1) Load sos raster #####
  sos_rast<-terra::rast(file.path(sos_dir,"sos.tif"))
  
  # 3.2) Load & process ggcmi crop calendar #####
  ggcmi_cc<-terra::rast(file.path(ggcmi_dir,"mai_rf_ggcmi_crop_calendar_phase3_v1.01.nc4"))
  ggcmi_cc<-terra::crop(ggcmi_cc,base_rast)
  ggcmi_cc <- terra::resample(ggcmi_cc, base_rast, method = "near")
  ggcmi_cc$planting_month<-as.numeric(format(as.Date(ggcmi_cc$planting_day[],origin = as.Date("2024-01-01")),"%m"))
  ggcmi_cc$maturity_month<-as.numeric(format(as.Date(ggcmi_cc$maturity_day[],origin = as.Date("2024-01-01")),"%m"))
  ggcmi_cc<-ggcmi_cc[[c("planting_month","maturity_month")]]

# 4) Choose hazards #####
# Read in climate variable information
haz_meta<-unique(data.table::fread(haz_meta_url)[,c("variable.code","function")])

# Choose hazards
hazards<-c("HSH","NDWL0", "NDWS","NTx35","NTx40", "PTOT" , "TAI" ,  "TAVG" , "THI","TMIN","TMAX") 

# Add in more heat thresholds
if(F){
  hazards2<-paste0("NTx",c(20:34,36:39,41:50))
  haz_meta<-rbind(haz_meta,data.table(variable.code=hazards2,`function`="mean"))
  hazards<-c(hazards,hazards2)
}

if(T){
  hazards2<-paste0("NTx",c(30:50))
  haz_meta<-rbind(haz_meta,data.table(variable.code=hazards2,`function`="mean"))
  hazards<-c(hazards,hazards2)
}

  # 4.1) Check hazards are complete #####
if(F){
check_folders<-list.dirs(working_dir)
check_folders<-check_folders[!grepl("ipynb",check_folders)]

exists<-rbindlist(pbapply::pblapply(hazards,FUN=function(H){
  folders<-grep(paste0("/",H,"$"),check_folders,value=T)
  result<-sapply(folders,FUN=function(x){length(list.files(x))},USE.NAMES=T)
  exists<-data.table(do.call("cbind",tail(tstrsplit(grep(paste0("/",H,"$"),folders,value=T),"/"),2)))
  names(exists)<-c("scenario","hazard")
  exists[,nfiles:=result]
  exists
}))
(exists<-dcast(exists,scenario~hazard,value.var = "nfiles"))
}

# 5) Set analysis parameters ####
doParallel<-F

use_crop_cal_choice<- c("no","yes") # if set to no then values are calculated for the year (using the jagermeyr cc as the starting month for each year)
use_sos_cc_choice<-c("no","yes") # Use onset of rain layer to set starting month of season

# Use end of season layer?
use_eos_choice<-c(F,T) # If use_sos_cc is "yes" use eos as estimated using Aridity Index? If set to "no" then season_length argument will be used to fix the season length
season_lengths<-c(3,4,5) # This can be varied to create season lengths for different crops (2,3,4,5,6,7,8 months) - gets messy when two season are present?

# Create table of possible combinations
parameters<-data.table(use_crop_cal=c("no","yes","yes","yes","yes","yes"),
           use_sos_cc=c(NA,"no","yes","yes","yes","yes"),
           use_eos=c(NA,NA,T,F,F,F),
           season_length=c(NA,NA,NA,3,4,5))

# 6) Run Analysis loop   ####
for(ii in 1:nrow(parameters)){
  use_crop_cal<-parameters[ii,use_crop_cal]
  use_sos_cc<-parameters[ii,use_sos_cc]
  use_eos<-parameters[ii,use_eos]
  season_length<-parameters[ii,season_length]
  
  cat("use_crop_cal = ",use_crop_cal," | use_sos_cc = ",use_sos_cc," | use_eos = ", use_eos," |season_length = ",season_length,"\n")
  
  # Set directory for output files
  if(use_crop_cal=="yes"){
    save_dir1<-paste0(output_dir,"/by_season")
  }else{
    save_dir1<-paste0(output_dir,"/by_year/hazard_timeseries")
  }
  
  if(!dir.exists(save_dir1)){
    dir.create(save_dir1,recursive=T)
  }
  
  if(use_crop_cal=="yes"){
    if(use_sos_cc=="yes"){
      n_seasons<-2
    }else{
      n_seasons<-1
    }
  }else{
    n_seasons<-1
  }
  
  for(season in 1:n_seasons){

    # Update save directory to create structures for different crop calendars within seasonal folder
    if(use_crop_cal=="yes"){
      
      if(use_sos_cc=="no"){
        r_cal<-ggcmi_cc
        
        save_dir<-paste0(save_dir1,"/jagermeyr/hazard_timeseries")
        if(!dir.exists(save_dir)){
          dir.create(save_dir,recursive=T)
        }
        
        r_cal_file<-paste0(save_dir,"/crop_cal.tif")
        if(!file.exists(r_cal_file)){
          terra::writeRaster(r_cal,r_cal_file)
        }
        
      }else{
        s1_name<-if(use_eos==T){"primary_eos/hazard_timeseries"}else{paste0("primary_fixed_",season_length,"/hazard_timeseries")}
        s2_name<-if(use_eos==T){"secondary_eos/hazard_timeseries"}else{paste0("secondary_fixed_",season_length,"/hazard_timeseries")}
        
        save_dir<-paste0(save_dir1,"/sos_",if(season==1){s1_name}else{s2_name})
        if(!dir.exists(save_dir)){
          dir.create(save_dir,recursive=T)
        }
        
        sos_rast_file<-paste0(save_dir,"/crop_cal.tif")
        
        if(!file.exists(sos_rast_file)){
          terra::writeRaster(sos_rast,sos_rast_file)
        }
      }
      
    }else{
      r_cal<-ggcmi_cc
      save_dir<-save_dir1
    }
    
    if(use_sos_cc=="yes" & use_crop_cal=="yes"){
      r_cal<-ggcmi_cc
      # Major season
      if(season==1){
        r_cal$planting_month<-sos_rast$S1
        if(use_eos){
          r_cal$maturity_month<-sos_rast$E1
        }else{
          r_cal$maturity_month <-r_cal$planting_month+season_length-1
          r_cal$maturity_month [r_cal$maturity_month []>12]<-r_cal$maturity_month [r_cal$maturity_month[]>12]-12
        }
      }
      
      # Minor season
      if(season==2){
        r_cal$planting_month<-sos_rast$S2
        if(use_eos){
          r_cal$maturity_month<-sos_rast$E2
        }else{
          r_cal$maturity_month <-r_cal$planting_month+season_length-1
          r_cal$maturity_month [r_cal$maturity_month []>12]<-r_cal$maturity_month [r_cal$maturity_month[]>12]-12
        }
      }
    }
    
    folders_x_hazards<-data.table(expand.grid(folders=folders,hazards=hazards))
    
    lapply(1:nrow(folders_x_hazards),
           FUN=hazard_stacker,
           folders_x_hazards=folders_x_hazards,
           model_names=model_names,
           use_crop_cal=use_crop_cal,
           r_cal=r_cal,
           save_dir=save_dir)
    
    # Create ensembles
    files<-list.files(save_dir,".tif")
    files<-files[grepl("ssp",files)]
    files<-files[!grepl("ENSEMBLE",files)]
    
    models<-unique(paste0(unlist(tstrsplit(files,paste(hazards,collapse="|"),keep=1))))
    models<-gsub("0-","0",models)
    models<-models[!grepl("expos",models)]
    
    scenario<-unique(unlist(tstrsplit(models,"_",keep=1)))
    time<-unique(paste0(unlist(tstrsplit(models,"_",keep=3)),"_",unlist(tstrsplit(models,"_",keep=4))))
    
    scen_haz_time<-expand.grid(scenario=scenario,hazards=hazards,time=time,stringsAsFactors =F)
    
    
    for(i in 1:nrow(scen_haz_time)){
      # Display progress
      cat("Ensembling: cc = ",use_crop_cal," | fixed = ",!use_eos," | season = ",season," | ",scen_haz_time$scenario[i]," | ",scen_haz_time$hazards[i]," | ",scen_haz_time$time[i]," - ",i,"\n")

      haz_files<-list.files(save_dir,scen_haz_time$hazards[i],full.names = T)
      haz_files<-haz_files[!grepl("historic",haz_files)]
      haz_files<-grep(scen_haz_time$scenario[i],haz_files,value = T)
      haz_files<-grep(scen_haz_time$time[i],haz_files,value = T)
      
      var<-gsub(".tif","",unlist(tstrsplit(haz_files,paste0("_",scen_haz_time$hazards[i],"_"),keep=2)))
      var_unique<-unique(var)
      
      for(p in 1:length(var_unique)){
        haz_files_ss<-haz_files[var==var_unique[p]]
        savename_ensemble<-gsub(paste0(model_names,collapse="|"),"ENSEMBLE",haz_files_ss[1])
        savename_ensemble_mean<-gsub("_ENSEMBLE_","_ENSEMBLEmean_",savename_ensemble)
        savename_ensemble_sd<-gsub("_ENSEMBLE_","_ENSEMBLEsd_",savename_ensemble)
        
        if(!file.exists(savename_ensemble_mean)){
          rast_list<-lapply(haz_files_ss,FUN=function(X){
            terra::rast(X)
          })
          
          # mean
          haz_rast<-terra::rast(lapply(1:terra::nlyr(rast_list[[1]]),FUN=function(j){
            terra::app(terra::rast(lapply(rast_list,"[[",j)),mean,na.rm=T)
          }))
          names(haz_rast)<-names(rast_list[[1]])
          
          terra::writeRaster(haz_rast,savename_ensemble_mean,overwrite=T)
          
          # sd
          haz_rast<-terra::rast(lapply(1:terra::nlyr(rast_list[[1]]),FUN=function(j){
            terra::app(terra::rast(lapply(rast_list,"[[",j)),sd,na.rm=T)
          }))
          names(haz_rast)<-names(rast_list[[1]])
          
          terra::writeRaster(haz_rast,savename_ensemble_sd,overwrite=T)
          
          rm(haz_rast)
          gc()
        }
      }
    }
  }
}

