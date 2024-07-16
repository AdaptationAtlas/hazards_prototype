# Please run 0_server_setup.R before executing this script
# 1) Load R functions & packages ####
pacman::p_load(terra,data.table,future,fs,future.apply,progressr)

# 2) Set directories  ####
# Directory where monthly timeseries data generated from https://github.com/AdaptationAtlas/hazards/tree/main is stored
# Note this is currently only available on cglabs, but we will be adding the ability to downlaod these data from the s3
working_dir<-indices_dir
#setwd(working_dir)

# Where will hazard time series be saved?
output_dir<-indices_dir2

# 2.1) Summarize existing data #####
list_files_fs <- function(path, pattern, recursive = TRUE) {
  files <- fs::dir_ls(path, glob = paste0("*", pattern), recurse = recursive)
  return(files)
}

# There are a very large number of files to be listed, this seems to create instability when running the function.
# Loop over each folder to try and overcome this issue
folders<-list.dirs(working_dir,recursive=F)
folders<-folders[!grepl("ipynb_checkpoints",folders)]

existing_files<-unlist(lapply(1:length(folders),FUN=function(i){
  cat('\r', strrep(' ', 150), '\r')
  cat("folder",i,"/",length(folders))
  flush.console()
  files<-list_files_fs(folders[i],"tif$",recursive=T)
  return(files)
}))

existing_files<-existing_files[!grepl("GSeason|THI/THI_MAX/|/THI_AgERA5/|stats|daily|historical/HSM_NTx|AVAIL.tif|LongTermMean|/max_year|/mean_monthly|/mean_year|/median_monthly|/median_year",
                                      existing_files)]

existing_files<-data.table(file_path=existing_files)

existing_files<-existing_files[,variable:=dirname(file_path)
                               ][,scenario:=basename(dirname(variable[1])),by=variable
                                 ][,var_folder:=basename(variable)
                                   ][,base_name:=gsub(".tif","",basename(file_path))]

existing_files[grep("TAI",base_name)]

# Function to check problem file names
if(F){
  base_names<-existing_files[,unique(base_name)]
  
  for(i in 1:length(base_names)){
    # Display progress
    cat('\r', strrep(' ', 150), '\r')
    cat("processing crop", i, "/", length(base_names), base_names[i])
    flush.console()  # Ensure console output is updated
    
    if(!grepl("TAI",base_names[i])){
    tstrsplit(base_names[i],"-",keep=3)
    }
  }
}

existing_files[,var_file:=unlist(tstrsplit(base_name[1],"-",keep=1)),by=base_name
                       ][,year:=unlist(tstrsplit(base_name[1],"-",keep=2)),by=base_name
                         ][!grepl("TAI",base_name),month:=unlist(tstrsplit(base_name[1],"-",keep=3)),by=base_name]

# Ignoring AgERA5 files what files are present in the historical scenario not present in others
existing_files_summary<-existing_files[!grepl("AgERA5",var_folder),.(n_files=.N,years=length(unique(year))),by=.(scenario,var_folder,var_file)]
existing_files_summary[,var_code:=paste0(var_folder,"-",var_file)]

hazard_completion<-dcast(existing_files_summary,scenario~var_code,value.var="n_files")

fwrite(hazard_completion,file.path(working_dir,"indice_completion.csv"))

# 3) Set up workspace ####

# List hazard folders
folders<-list.dirs(working_dir,recursive=F)
folders<-folders[!grepl("ENSEMBLE|ipyn|gadm0|hazard_comb|indices_seasonal",folders)]
folders<-folders[!grepl("ssp126|ssp370|2061_2080|2081_2100",folders)] # these scenarios have incomplete information
folders<-basename(folders)

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
worker_n<-1

use_crop_cal_choice<- c("no","yes") # if set to no then values are calculated for the year (using the jagermeyr cc as the starting month for each year)
use_sos_cc_choice<-c("no","yes") # Use onset of rain layer to set starting month of season

# Use end of season layer?
use_eos_choice<-c(F,T) # If use_sos_cc is "yes" use eos as estimated using Aridity Index? If set to "no" then season_length argument will be used to fix the season length
season_lengths<-c(3,4,5) # This can be varied to create season lengths for different crops (2,3,4,5,6,7,8 months) - gets messy when two season are present?

# Create table of possible combinations
parameters<-data.table(use_crop_cal=c("no","yes","yes","yes","yes","yes"),
                       use_sos_cc=c(NA,"no","yes","yes","yes","yes"),
                       use_eos=c(NA,NA,T,F,F,F),
                       season_length=c(NA,NA,NA,3,4,5),
                       folder_name=c("by_year",rep("by_season",5)),
                       subfolder_name=c(NA,"jagermeyr","sos","sos","sos","sos"))

# Create folder index of scenarios and hazards
folders_x_hazards<-data.table(expand.grid(folders=folders,hazards=hazards))[,folder_path:=file.path(working_dir,folders,hazards)]

# 6) Run Analysis loop   ####
for(ii in 1:nrow(parameters)){
  use_crop_cal<-parameters[ii,use_crop_cal]
  use_sos_cc<-parameters[ii,use_sos_cc]
  use_eos<-parameters[ii,use_eos]
  season_length<-parameters[ii,season_length]
  folder_name<-parameters[ii,folder_name]
  subfolder_name<-parameters[ii,subfolder_name]
  
  cat("use_crop_cal = ",use_crop_cal," | use_sos_cc = ",use_sos_cc," | use_eos = ", use_eos," |season_length = ",season_length,"\n")
  
  # Set directory for output files
  save_dir1<-file.path(output_dir,folder_name)

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
        
        save_dir<-file.path(save_dir1,subfolder_name)
        if(!dir.exists(save_dir)){
          dir.create(save_dir,recursive=T)
        }
        
      }else{
        s1_name<-if(use_eos==T){"primary_eos"}else{paste0("primary_fixed_",season_length)}
        s2_name<-if(use_eos==T){"secondary_eos"}else{paste0("secondary_fixed_",season_length)}
        
        save_dir<-file.path(save_dir1,paste0(subfolder_name,"_",if(season==1){s1_name}else{s2_name}))
        if(!dir.exists(save_dir)){
          dir.create(save_dir,recursive=T)
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
    
    # Save r_cal in a temporary location for reading into hazard_stacker function (needed for parallel processing)
    r_cal_filepath<-file.path(output_dir,"rcal_temp.tif")
    terra::writeRaster(r_cal,r_cal_filepath,overwrite=T)
    
    # Run hazard_stacker function
    if(worker_n>1){
    future::plan("multisession", workers = worker_n)
    
    # Enable progressr
    progressr::handlers(global = TRUE)
    progressr::handlers("progress")
    
    # Wrap the parallel processing in a with_progress call
    p <- with_progress({
      # Define the progress bar
      progress <- progressr::progressor(along = 1:nrow(folders_x_hazards))
      
      future.apply::future_lapply(
        1:nrow(folders_x_hazards),
        FUN = function(i) {
          progress(sprintf("Processing row %d/%d: %s, %s", 
                           i, 
                           nrow(folders_x_hazards), 
                           folders_x_hazards$folders[i], 
                           folders_x_hazards$hazards[i]))
          hazard_stacker(
            i,
            folders_x_hazards = folders_x_hazards,
            haz_meta=haz_meta,
            model_names = model_names,
            use_crop_cal = use_crop_cal,
            r_cal_filepath = r_cal_filepath,
            save_dir = save_dir
          )
        }
      )
    })
    future::plan(sequential)
    }else{
      p<-lapply(
        1:nrow(folders_x_hazards),
        FUN = function(i) {
          hazard_stacker(
            i,
            folders_x_hazards = folders_x_hazards,
            haz_meta=haz_meta,
            model_names = model_names,
            use_crop_cal = use_crop_cal,
            r_cal_filepath = r_cal_filepath,
            save_dir = save_dir
          )
        }
      )
    }
    
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
    
    
    doFuture::registerDoFuture()
    if (worker_n == 1) {
      future::plan("sequential")
    } else {
      future::plan("multisession", workers = worker_n)
    }
    
    p<-progressr::with_progress({
      # Define the progress bar
      progress <- progressr::progressor(along = 1:nrow(scen_haz_time))
    
      foreach(i = 1:nrow(scen_haz_time)) %dopar% {
      # Display progress
      # cat("Ensembling: cc = ",use_crop_cal," | fixed = ",!use_eos," | season = ",season," | ",scen_haz_time$scenario[i]," | ",scen_haz_time$hazards[i]," | ",scen_haz_time$time[i]," - ",i,"\n")

      progress(sprintf("Ensembling row %d/%d: %s, %s, %s", 
                       i, 
                       nrow(scen_haz_time),
                       scen_haz_time$scenario[i],
                       scen_haz_time$time[i],
                       scen_haz_time$hazards[i]))
        
        
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
    
    })
    
    future::plan(sequential)
  }
}

