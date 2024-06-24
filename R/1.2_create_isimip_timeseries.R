# Please run 0_server_setup.R before executing this script
# Please run 0.1_get_isimip.R before executing this script

# 1) Load R functions & packages ####
packages <- c("terra","data.table","future","doFuture","sf","geoarrow","arrow")
p_load(char=packages)

# 2) Set directories  ####
# Directory where isimip timeseries data are stored
working_dir<-isimip_raw_dir

# Where will processed data be saved?
output_dir<-isimip_timeseries_dir

# 3) Set up workspace ####
file_index<-data.table::fread(file.path(working_dir,"file_index.csv"))
# Add group for ensembling
gcm_names<-file_index[,unique(gcm)]
file_index[,group:=paste0(model,"_",scenario,"_",timeframe,"_",var)
           ][,ensemble_name:=gsub(paste0(gcm_names,collapse="|"),"gcm-ensemble",basename)]

  # 3.1) Load sos raster #####
  sos_rast<-terra::rast(file.path(sos_dir,"sos.tif"))
  sos_rast<-terra::resample(sos_rast,terra::rast(file_index$file_path[i]),method="near")
  
  # 3.2) Load & process ggcmi crop calendar #####
  ggcmi_cc<-terra::rast(file.path(ggcmi_dir,"mai_rf_ggcmi_crop_calendar_phase3_v1.01.nc4"))
  ggcmi_cc<-terra::crop(ggcmi_cc,base_rast)
  ggcmi_cc$planting_month<-as.numeric(format(as.Date(ggcmi_cc$planting_day[],origin = as.Date("2024-01-01")),"%m"))
  ggcmi_cc$maturity_month<-as.numeric(format(as.Date(ggcmi_cc$maturity_day[],origin = as.Date("2024-01-01")),"%m"))
  ggcmi_cc<-ggcmi_cc[[c("planting_month","maturity_month")]]
  ggcmi_cc<-terra::resample(ggcmi_cc,terra::rast(file_index$file_path[i]),method="near")
  
  # 3.3) Load admin boundaries #####
  Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
    file<-geo_files_local[i]
    data<-arrow::open_dataset(file)
    data <- data |> sf::st_as_sf() |> terra::vect()
    data
  })
  names(Geographies)<-names(geo_files_local)
  # 3.4) Load isimip metadata
  isimip_meta<-data.table::fread(isimip_meta_url)
# 4) Set analysis parameters ####
use_crop_cal_choice<- c("no","yes") # if set to no then values are calculated for the year (using the jagermeyr cc as the starting month for each year)
use_sos_cc_choice<-c("no","yes") # Use onset of rain layer to set starting month of season

# Use end of season layer?
use_eos_choice<-c(F,T) # If use_sos_cc is "yes" use eos as estimated using Aridity Index? If set to "no" then season_length argument will be used to fix the season length
season_lengths<-c(3,4,5) # This can be varied to create season lengths for different crops (2,3,4,5,6,7,8 months) - gets messy when two season are present?

# Create table of possible combinations
parameters<-data.table(use_crop_cal=c(F,T,T,T,T,T),
                       use_sos_cc=c(NA,F,T,T,T,T),
                       use_eos=c(NA,NA,T,F,F,F),
                       season_length=c(NA,NA,NA,3,4,5))

# overwrite existing files?
overwrite<-F

# 5) Run analysis loop   ####
for(ii in 1:nrow(parameters)){
  use_crop_cal<-parameters[ii,use_crop_cal]
  use_sos_cc<-parameters[ii,use_sos_cc]
  use_eos<-parameters[ii,use_eos]
  season_length<-parameters[ii,season_length]
  
  cat("use_crop_cal = ",use_crop_cal," | use_sos_cc = ",use_sos_cc," | use_eos = ", use_eos," |season_length = ",season_length,"\n")
  
  # Set directory for output files
  if(use_crop_cal){
    save_dir1<-paste0(output_dir,"/by_season")
  }else{
    save_dir1<-paste0(output_dir,"/by_year")
  }
  
  if(!dir.exists(save_dir1)){
    dir.create(save_dir1,recursive=T)
  }
  
  if(use_crop_cal){
    if(use_sos_cc){
      n_seasons<-2
    }else{
      n_seasons<-1
    }
  }else{
    n_seasons<-1
  }
  
  for(season in 1:n_seasons){
    
    # Update save directory to create structures for different crop calendars within seasonal folder
    if(use_crop_cal){
      
      if(!use_sos_cc){
        r_cal<-ggcmi_cc
        
        save_dir<-file.path(save_dir1,"jagermeyr")
        if(!dir.exists(save_dir)){
          dir.create(save_dir,recursive=T)
        }
        
        r_cal_file<-file.path(save_dir,"crop_cal.tif")
        if(!file.exists(r_cal_file)){
          terra::writeRaster(r_cal,r_cal_file)
        }
        
      }else{
        s1_name<-if(use_eos==T){"primary_eos"}else{paste0("primary_fixed_",season_length,"")}
        s2_name<-if(use_eos==T){"secondary_eos"}else{paste0("secondary_fixed_",season_length,"")}
        
        save_dir<-file.path(save_dir1,paste0("/sos_",if(season==1){s1_name}else{s2_name}))
        if(!dir.exists(save_dir)){
          dir.create(save_dir,recursive=T)
        }
        
      }
      
    }else{
      r_cal<-ggcmi_cc
      save_dir<-save_dir1
    }
    
    if(use_sos_cc & use_crop_cal){
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


    for (i in 1:nrow(file_index)) {
      # Display progress
      cat('\r', strrep(' ', 150), '\r')
      cat("Extracting file", i, "/", nrow(file_index))
      flush.console()
      
      process_isimip_files(file_path=file_index[i]$file_path,
                    stat=file_index[i]$stat, 
                    save_dir=save_dir, 
                    r_cal=r_cal, 
                    overwrite = overwrite, 
                    use_crop_cal = use_crop_cal)
    }

  
    
    
    # Create ensembles
    groups<-unique(file_index[,list(group,ensemble_name)])

    
    for(i in 1:nrow(groups)){
      # Display progress
      cat('\r', strrep(' ', 150), '\r')
      cat("Ensembling group", i, "/", nrow(groups))
      flush.console()
      
      save_file_mean<-groups[i,file.path(save_dir,gsub("ensemble_","ensemble-mean_",ensemble_name))]
      save_file_sd<-groups[i,file.path(save_dir,gsub("ensemble_","ensemble-sd_",ensemble_name))]
        
      files<-file_index[group==groups[i]$group,file.path(save_dir,basename)]
      
        if(!file.exists(save_file_mean)|overwrite){
          
          rast_list<-lapply(files,terra::rast)
          
          # mean
          ens_mean_rast<-terra::rast(lapply(1:terra::nlyr(rast_list[[1]]),FUN=function(j){
            terra::app(terra::rast(lapply(rast_list,"[[",j)),mean,na.rm=T)
          }))
          names(ens_mean_rast)<-format(time(rast_list[[1]]),"%Y")
          varnames(ens_mean_rast)<-gsub(".nc","",basename(save_file_mean))
          time(ens_mean_rast)<-time(rast_list[[1]])
          
          terra::writeCDF(ens_mean_rast,save_file_mean,overwrite=T)
          
          # sd
          ens_sd_rast<-terra::rast(lapply(1:terra::nlyr(rast_list[[1]]),FUN=function(j){
            terra::app(terra::rast(lapply(rast_list,"[[",j)),sd,na.rm=T)
          }))
          names(ens_sd_rast)<-format(time(rast_list[[1]]),"%Y")
          varnames(ens_sd_rast)<-gsub(".nc","",basename(save_file_mean))
          time(ens_sd_rast)<-time(rast_list[[1]])
          
          terra::writeCDF(ens_sd_rast,save_file_sd,overwrite=T)
         }
      }
    }
}

# 5.1) Restructure folder #####
old_dir_name<-list_bottom_directories(isimip_timeseries_dir)

new_dir_name<-gsub("by_year","annual",old_dir_name)
new_dir_name<-gsub("by_season/","",new_dir_name)
file.rename(old_dir_name, new_dir_name)

unlink(file.path(isimip_timeseries_dir,"by_season"),recursive = T)

# 6) Calculate timeseries mean and sd ####

# List timese
files<-data.table(timeseries_path=list.files(output_dir,".nc",full.names = T,recursive = T))
# Remove annual sd estimates
files<-files[!grepl("ensemble-sd",timeseries_path)]
files[,mean_path:=gsub(output_dir,isimip_mean_dir,timeseries_path)
      ][,sd_path:=gsub(output_dir,isimip_sd_dir,timeseries_path)]

files_mean<-files$mean_path
files_sd<-files$sd_path
timeseries_paths<-files$timeseries_path

overwrite<-F

doFuture::registerDoFuture()
future::plan("multisession", workers = 10)

# Enable progressr
progressr::handlers(global = TRUE)
progressr::handlers("progress")

# Wrap the parallel processing in a with_progress call
p <- progressr::with_progress({
  # Define the progress bar
  progress <- progressr::progressor(along = files_mean)
  
  foreach(i = 1:length(files_mean)) %dopar% {
    file_mean <- files_mean[i]
    file_sd <- files_sd[i]
    
    if (!dir.exists(dirname(file_mean))) {
      dir.create(dirname(file_mean), recursive = TRUE)
    }
    
    if (!dir.exists(dirname(file_sd))) {
      dir.create(dirname(file_sd), recursive = TRUE)
    }
    
    if (!file.exists(file_mean) || overwrite) {
      data <- terra::rast(timeseries_paths[i])
      data_agg <- terra::app(data, fun = "mean", na.rm = TRUE)
      varnames(data_agg) <- varnames(data)
      terra::writeCDF(data_agg, filename = file_mean, overwrite = TRUE)
      
      data_agg <- terra::app(data, fun = "sd", na.rm = TRUE)
      varnames(data_agg) <- varnames(data)
      terra::writeCDF(data_agg, filename = file_sd, overwrite = TRUE)
    }
    
    # Display progress
    progress(sprintf("File %d/%d", i, length(files_mean)))
  }
})

future::plan(sequential)


# 7) Extract by admin area ####
focal_dir<-isimip_mean_dir
leaf_dirs<-list_bottom_directories(focal_dir)
filename<-basename(focal_dir)
overwrite=T

for(i in 1:length(leaf_dirs)){
  cat("Processing files in dir",i,"/",length(leaf_dirs),":",leaf_dirs[i],"\n")
  
  files<-list.files(leaf_dirs[i],".nc",full.names = T)
  data<-terra::rast(files)

  #names(data)<-basename(gsub(".nc","",files))
  save_file<-file.path(leaf_dirs[i],paste0(filename,"_adm_mean.parquet"))
  
  if(!file.exists(save_file)|overwrite){
  data_ex<-admin_extract_wrap(data=data, 
                              save_dir=leaf_dirs[i], 
                              filename=filename, 
                              FUN = "mean", 
                              varname:=NA, 
                              Geographies, 
                              overwrite = F,
                              modify_colnames = F)
  
  data_ex<-data_ex[,variable:=gsub("mean[.]","",variable[1]),by=variable
          ][,model:=unlist(tstrsplit(variable[1],"_",keep=1)),by=variable
            ][,gcm:=gsub(".w5e5","",unlist(tstrsplit(variable[1],"_",keep=2))),by=variable
              ][,scenario:=unlist(tstrsplit(variable[1],"_",keep=4)),by=variable
                ][,var:=unlist(tstrsplit(variable[1],"_",keep=7)),by=variable
                  ][,timeframe:=unlist(tstrsplit(variable[1],"_",keep=10)),by=variable
                    ][,variable:=NULL
                      ][,var:=gsub("evap.total","evap",var)]
  
  arrow::write_parquet(data_ex,save_file)
  
  # Prepare for notebook
  data_ex_ens<-merge(data_ex,isimip_meta[,list(var,var_long)],all.x=T)[gcm!="gcm.ensemble.mean"][,]
  data_ex_ens[,var:=NULL]
  setnames(data_ex_ens,"var_long","variable")
  data_ex_ens[timeframe %in% c("1981","1995"),timeframe:="historical"
              ][timeframe %in% c("2040","2041"),timeframe:="2041_2060"
                ][timeframe %in% c("2020","2021"),timeframe:="2021_2040"]
  
  hist_dat<-data_ex_ens[scenario=="historical",!c("scenario","timeframe")]
  setnames(hist_dat,"value","value_hist")
  
  data_ex_ens<-merge(data_ex_ens[scenario!="historical"],hist_dat,all.x=T)
  data_ex_ens[,prop_change:=value/value_hist]
  
  # Ensemble over gcms by model
  data_ex_ens_mod<-data_ex_ens[,list(mean=mean(prop_change,na.rm=T),
                                 max=max(prop_change,na.rm=T),
                                 min=min(prop_change,na.rm=T),
                                 sd=sd(prop_change,na.rm=T)),
                           by=list(admin0_name,admin1_name,admin2_name,model,scenario,timeframe,variable)
                           ][,unit:="proportional change"]
  
  # Ensemble over models and gcms
  data_ex_ens_all<-data_ex_ens[,list(mean=mean(prop_change,na.rm=T),
                                 max=max(prop_change,na.rm=T),
                                 min=min(prop_change,na.rm=T),
                                 sd=sd(prop_change,na.rm=T)),
                                by=list(admin0_name,admin1_name,admin2_name,scenario,timeframe,variable)
                               ][,unit:="proportional change"]
  
  focal_cols<-c("mean","min","max","sd")
  
  data_ex_ens_all[, (focal_cols) := lapply(.SD, round,2), .SDcols = focal_cols]
  save_file<-file.path(leaf_dirs[i],paste0(filename,"_adm_mean_ens_all.parquet"))
  arrow::write_parquet(data_ex_ens_all,save_file)
  
  data_ex_ens_mod[, (focal_cols) := lapply(.SD, round,2), .SDcols = focal_cols]
  save_file<-file.path(leaf_dirs[i],paste0(filename,"_adm_mean_ens_mod.parquet"))
  arrow::write_parquet(data_ex_ens_mod,save_file)
  }
}
