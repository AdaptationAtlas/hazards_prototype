source("R/haz_functions.R")
require(data.table)
require(terra)
require(doFuture)

# NEED TO FIX NEGATIVE VALUES IN PRECIP

# Set scenarios and time frames to analyse
Scenarios<-c("ssp245","ssp585")
Times<-c("2021_2040","2041_2060")

# Create combinations of scenarios and times
Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))
Scenarios[,combined:=paste0(Scenario,"-",Time)]

# Set hazards to include in analysis
hazards<-c("NDD","NTx40","NTx35","HSH_max","HSH_mean","THI_max","THI_mean","NDWS","TAI","NDWL0","PTOT","TAVG")
haz_meta<-data.table::fread("./Data/metadata/haz_metadata.csv")
haz_class<-fread("./Data/metadata/haz_classes.csv")[,list(index_name,description,direction,crop,threshold)]
haz_classes<-unique(haz_class$description)

# Pull out severity classes and associate impact scores
severity_classes<-unique(fread("./Data/metadata/haz_classes.csv")[,list(description,value)])
setnames(severity_classes,"description","class")

# Create combinations of scenarios and hazards
scenarios_x_hazards<-data.table(Scenarios,Hazard=rep(hazards,each=nrow(Scenarios)))[,Scenario:=as.character(Scenario)][,Time:=as.character(Time)]

# read in mapspam metadata
ms_codes<-data.table::fread("./Data/metadata/SpamCodes.csv")[,Code:=toupper(Code)]
ms_codes<-ms_codes[compound=="no"]

# read in ecocrop
ecocrop<-miceadds::load.Rdata2(file="Data/ecocrop.RData")[,list(species,Life.span,temp_opt_min,Temp_Opt_Max,Temp_Abs_Min,Temp_Abs_Max,Rain_Opt_Min,Rain_Opt_Max,Rain_Abs_Min,
                                                                Rain_Abs_Max,cycle_min,cycle_max)]

# Using the mapspam species transpose the ecocrop data into mod, severe and extreme hazards (match the format of the haz_class data.table)
description<-c("Moderate","Severe","Extreme")
ec_haz<-rbindlist(lapply(1:nrow(ms_codes),FUN=function(i){
  crop<-ms_codes[i,sci_name]
  crop_common<-ms_codes[i,Fullname]
  
  crops<-unlist(strsplit(crop,";"))
  
  ec_haz<-rbindlist(lapply(1:length(crops),FUN=function(j){
    ecrop<-ecocrop[species==crops[j]]
    
    if(nrow(ecrop)>0){
      print(paste0(i,"-",j," | ",crop_common,"/",crops[j]))
      
      # PTOT low
      ptot_low<-data.table(index_name="PTOT",
                           description=description,
                           direction="<",
                           crop=crop_common,
                           threshold=c(
                             unlist(ecrop$Rain_Opt_Min), # Moderate
                             (unlist(ecrop$Rain_Abs_Min)+unlist(ecrop$Rain_Opt_Min))/2, # Severe
                             unlist(ecrop$Rain_Abs_Min))) # Extreme
      
      # PTOT high
      ptot_high<-data.table(index_name="PTOT",
                            description=description,
                            direction=">",
                            crop=crop_common,
                            threshold=c(
                              unlist(ecrop$Rain_Opt_Max), # Moderate
                              (unlist(ecrop$Rain_Opt_Max)+unlist(ecrop$Rain_Abs_Max))/2, # Severe
                              unlist(ecrop$Rain_Abs_Max))) # Extreme
      
      # TAVG low
      tavg_low<-data.table(index_name="TAVG",
                           description=description,
                           direction="<",
                           crop=crop_common,
                           threshold=c(
                             unlist(ecrop$temp_opt_min), # Moderate
                             (unlist(ecrop$temp_opt_min)+unlist(ecrop$Temp_Abs_Min))/2, # Severe
                             unlist(ecrop$Temp_Abs_Min))) # Extreme
      
      # TAVG high
      tavg_high<-data.table(index_name="TAVG",
                            description=description,
                            direction=">",
                            crop=crop_common,
                            threshold=c(
                              unlist(ecrop$Temp_Opt_Max), # Moderate
                              (unlist(ecrop$Temp_Opt_Max)+unlist(ecrop$Temp_Abs_Max))/2, # Severe
                              unlist(ecrop$Temp_Abs_Max))) # Extreme
      
      rbind(ptot_low,ptot_high,tavg_low,tavg_high)
    }else{
      print(paste0(i,"-",j," | ",crop, " - ERROR NO MATCH"))
      NULL
    }
  }))
  
  ec_haz<-ec_haz[,list(threshold=mean(threshold,na.rm=T)),by=list(index_name,description,direction,crop)]
  ec_haz
}))

# Replicate generic hazards that are not TAVG or PTOT for each crop
haz_class2<-rbindlist(lapply(1:nrow(ms_codes),FUN=function(i){
  Y<-ec_haz[crop==ms_codes[i,Fullname]]
  X<-haz_class[!index_name %in% ec_haz[,unique(index_name)]]
  X$crop<-ms_codes[i,Fullname]
  rbind(Y,X)
}))

haz_class<-rbind(haz_class,haz_class2)

# Set analysis parameters
PropThreshold<-0.5
PropTDir=">"

#  Country loop starts here
timeframe_choice<-"annual"

# Where are climate data stacks saved?
clim_dir<-paste0("Data/hazard_timeseries/",timeframe_choice)

# where to save classified climate hazards?
haz_class_dir<-paste0("Data/hazards_classified/",timeframe_choice)
if(!dir.exists(haz_class_dir)){
  dir.create(haz_class_dir,recursive = T)
}

# Global hazard index savedir
haz_index_dir<-paste0("Data/hazard_index/",timeframe_choice)
if(!dir.exists(haz_index_dir)){
  dir.create(haz_index_dir,recursive=T)
}

crop_choices<-c("generic",ms_codes[,sort(Fullname)])

# Create classify data for timeperiod x scenario (for the selected timeframe annual/jagermeyer) ####

registerDoFuture()
plan("multisession", workers = 10)

#  Crop loop starts here
  foreach(j = 1:length(crop_choices)) %dopar% { # Can hit error when parallel due to different workers trying to write the same file
    # To solve issue loop over unique threshold x variable combinations rather than crops
    # This will require putting the foreach loop into the hazard wrapper function
  #for(j in 1:length(crop_choices)){ 
    
    # Display progress
    print(paste0("Crop ",crop_choices[j]))
    #cat('\r                                                                                                                     ')
    #cat('\r',paste0("Crop ",crop_choices[j]))
    #flush.console()
    
    crop_choice<-crop_choices[j]
    save_name<-paste0(haz_index_dir,"/hi_",crop_choice,"-",PropThreshold,".tif")
    
    if(!file.exists(save_name)){
      
      # Create Thresholds Table for crop
      Thresholds<-haz_class[description!="No significant stress" & crop==crop_choice]
      setnames(Thresholds,c("index_name","description","direction"),c("Variable","Severity_class","Direction"))
      
      Thresholds[,Code:=paste0(Direction,threshold)
      ][,Code:=gsub("<","L",Code)
      ][,Code:=gsub(">","G",Code)
      ][,Code:=paste0(Variable,"_",Code)]
      
      Hazards<-HazardWrapper(Thresholds,
                            SaveDir=haz_class_dir,
                            PropThreshold=PropThreshold,
                            PropTDir=PropTDir,
                            hazard_dir = clim_dir,
                            Scenarios=Scenarios,
                            verbose=T)
      
        # Hazard Index = severity x recurrence
        haz_index<-hazard_index(Data=Hazards,
                                hazards=hazards,
                                verbose = T,
                                SaveDir=haz_index_dir,
                                crop_choice = crop_choice,
                                severity_classes=severity_classes,
                                PropThreshold=PropThreshold)
        
        hi_names<-unlist(lapply(1:length(haz_index),FUN=function(k){
          paste0(names(haz_index)[k],"_",crop_choice,"_",names(haz_index[[k]]))
        }))
        
        haz_index<-terra::rast(haz_index)
        names(haz_index)<-hi_names
        
        writeRaster(haz_index,filename=save_name)
      }
    }
    

# Calculate change for hazard index ####

files_hi<-paste0("hi_",crop_choices,"-",PropThreshold,".tif")


data<-lapply(1:length(crop_choices),FUN=function(j){
  crop<-crop_choices[j]
  # create change stack
  data<-terra::rast(paste0(haz_index_dir,"/hi_",crop,"-",PropThreshold,".tif"))
  data_hist<-data[[grep("historic",names(data),value=T)]]

  change<-terra::rast(lapply(Scenarios[Scenario!="historic",combined],FUN=function(SCENARIO){
        # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste0("Crop: ",crop," | Scenario:",SCENARIO))
    flush.console()
    
    
    data_fut<-data[[grep(SCENARIO,names(data))]]
    data_fut<-data_fut-data_hist
    data_fut
  }))
  
  names(change)<-paste0(names(change),"_change")
  
  terra::writeRaster(change,filename = paste0(haz_index_dir,"/hi_",crop,"-",PropThreshold,"_change.tif"),overwrite=T)
  
})


# Calculate change for classified values ####
files<-list.files(haz_class_dir,".tif",full.names = T)
files_hist<-grep("historic",files,value = T)

for(i in 1:length(files_hist)){
  
  data_hist<-terra::rast(files_hist[i])
  
  files_fut<-grep(tstrsplit(files_hist[i],"historic-historic-",keep=2),files,value=T)
  files_fut<-files_fut[!grepl("historic",files_fut)]
  
  for(j in 1:length(files_fut)){
    data_fut<-terra::rast(files_fut[j])
    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste0("Threshold: ",i," | Scenario: ",j))
    flush.console()
    
    change<-data_fut-data_hist
    names(change)<-paste0(names(change),"_change")
    
    save_name_change<-gsub(".tif","_change.tif",files_fut[j])  
  
    terra::writeRaster(change,filename = save_name_change,overwrite=T)
    
  }
 
 }


files<-list.files(clim_dir,full.names = T)
files<-grep("PTOT",files,value=T)

PTOT_hist<-rast(files[1])
PTOT_fut<-rast(files[2])
