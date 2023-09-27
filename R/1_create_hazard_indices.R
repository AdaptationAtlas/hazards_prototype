source("R/haz_functions.R")
require(data.table)
require(terra)
require(doFuture)

# Load metadata for countries to consider in the atlas - exclude islands for which we do not have climate data
countries_metadata<-fread("Data/metadata/countries.csv")[excluded_island==FALSE]

# What country level data is downloaded locally?
country_zips<-data.table(filepath=list.files("./Data/country_data_zips",".zip",full.names = T))
country_zips[,iso3c:=unlist(tstrsplit(tail(tstrsplit(filepath,"/"),1),"-",keep=1)),by=filepath
][,timeframe:=gsub(".zip","",unlist(tstrsplit(tail(tstrsplit(filepath,"/"),1),"-",keep=2)),"-"),by=filepath
][timeframe=="seasonal",timeframe:="seasonal_jagermeyer_cc"]
country_zips[,folder:=gsub(".zip","",unlist(tail(tstrsplit(filepath,"/"),1)))]
country_zips<-merge(country_zips,countries_metadata[,list(country,iso3)],by.x="iso3c",by.y="iso3")

# Check all countries are represented in the data
iso3_available<-country_zips[,unique(iso3c)]
iso3_required<-countries_metadata[,unique(iso3)]

countries_metadata[iso3 %in% iso3_required[!iso3_required %in% iso3_available]]

# Set scenarios and time frames to analyse
Scenarios<-c("ssp245","ssp585")
Times<-c("2021_2040","2041_2060")

# Create combinations of scenarios and times
Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))

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
country_choice<-"Burundi"
timeframe_choice<-"annual"

countries<-countries_metadata$country

# Global hazard index savedir
save_dir_all<-paste0("Data/hazard_indices/",timeframe_choice)
if(!dir.exists(save_dir_all)){
  dir.create(save_dir_all)
}

crop_choices<-c("generic",ms_codes[,sort(Fullname)])

# Create hazard indices for each country x crop x timeperiod x scenario (for the selected timeframe annual/jagermeyer) ####
# THERE IS AN ISSUE WITH TAI that needs to be debugged - Zambia, Zimbabwe, South Africa are missing
registerDoFuture()
plan("multisession", workers = 12)

foreach(i = 1:length(countries)) %dopar% {
#for(i in 1:length(countries)){
  country_choice<-countries[i]
  country_iso3<-countries_metadata[country==country_choice,iso3]
  country_dir<-paste0("./Data/country_data/", country_zips[country==country_choice & timeframe==timeframe_choice,folder])
  
  if(!dir.exists(country_dir)){
    dir.create(country_dir)
    unzip(zipfile=country_zips[country==country_choice & timeframe==timeframe_choice,filepath],exdir=country_dir,junkpaths=T)
  }
  
  SaveDir<- paste0(country_dir,"/Analysis")
  
  if(!dir.exists(SaveDir)){
    dir.create(SaveDir)
  }
  
  #  Crop loop starts here
  #foreach(j = 1:length(crop_choices)) %dopar% { # Hits errors when parallel due to different workers trying to write the same file
  for(j in 1:length(crop_choices)){ 
    
    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste0("Country ", country_choice," | Crop ",crop_choices[j]))
    flush.console()
    
    crop_choice<-crop_choices[j]
    
    save_name<-paste0(save_dir_all,"/",country_iso3,"_",crop_choice,"_hi.tif")
    
    if(!file.exists(save_name)){
      # Create Thresholds Table for crop
      Thresholds<-haz_class[description!="No significant stress" & crop==crop_choice]
      setnames(Thresholds,c("index_name","description","direction"),c("Variable","Severity_class","Direction"))
      
      Thresholds[,Code:=paste0(Direction,threshold)
      ][,Code:=gsub("<","L",Code)
      ][,Code:=gsub(">","G",Code)
      ][,Code:=paste0(Variable,"_",Code)]
      
      Hazards<-HazardWrapper(Thresholds,
                            SaveDir=SaveDir,
                            PropThreshold=PropThreshold,
                            PropTDir=PropTDir,
                            hazard_dir = country_dir,
                            Scenarios=Scenarios,
                            verbose=F)
      
      # Hazard Index = severity x recurrence
      haz_index<-hazard_index(Hazards,
                              verbose = T,
                              SaveDir=SaveDir,
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
}

# Merge mean hazards across countries ####
# ISSUE WITH NA values for some variables especially TAI

# Global mean savedir
save_dir_means<-paste0("Data/hazard_means/",timeframe_choice)
if(!dir.exists(save_dir_means)){
  dir.create(save_dir_means,recursive=T)
}

Scenarios[,combined:=paste0(Scenario,"-",Time)]

data<-lapply(1:length(countries),FUN=function(j){
  country_choice<-countries[j]
  country_dir<-paste0("./Data/country_data/", country_zips[country==country_choice & timeframe==timeframe_choice,folder])
  SaveDir<- paste0(country_dir,"/Analysis")
  
  means<-terra::rast(lapply(1:nrow(Scenarios),FUN=function(i){
    scenario<-Scenarios[i,combined]
    files<-list.files(SaveDir,scenario,full.names = T)
    files<-files[!grepl("hi_",files)]
    
    means<-terra::rast(lapply(hazards,FUN=function(hazard){
      file<-grep(hazard,files,value=T)[1]
      data<-terra::rast(file)[[1]]
      data
    }))
    
    names(means)<-paste0(scenario,"_",names(means))
    names(means)<-gsub("_mean_mean","-mean_mean",names(means))
    names(means)<-gsub("_max_mean","-max_mean",names(means))
    means
   }))
  means

})
data<-terra::sprc(data)
data<-terra::mosaic(data)

terra::writeRaster(data,filename = paste0(save_dir_means,"/haz_means.tif"))

# create change stack
data_hist<-data[[grep("historic-historic",names(data))]]

change<-terra::rast(lapply(Scenarios[Scenario!="historic",combined],FUN=function(SCENARIO){
  data_fut<-data[[grep(SCENARIO,names(data))]]
  data_fut<-data_fut-data_hist
  data_fut
}))

terra::writeRaster(change,filename = paste0(save_dir_means,"/haz_means_change.tif"))


# Merge hazard indices across countries ####
filenames<-list.files(save_dir_all,full.names = T)
for(i in 1:length(crop_choices)){
  crop<-crop_choices[i]
  save_name<-paste0(save_dir_all,"/combined_",crop,"_hi.tif")
  save_name<-paste0(save_dir_all,"/combined_",crop,"_hi_change.tif")
  
  # Display progress
  cat('\r                                                                                                                     ')
  cat('\r',paste0("Crop: ",crop))
  flush.console()
  
  if(!file.exists(save_name)){
    files<-grep(crop,filenames,value = T)
    
    data<-terra::sprc(lapply(files,terra::rast))
    data<-terra::mosaic(data)
    
    terra::writeRaster(data,filename = save_name)
  }
  
}

# calculate change
for(i in 1:length(crop_choices)){
  crop<-crop_choices[i]
  
  cat('\r                                                                                                                     ')
  cat('\r',paste0("Crop: ",crop))
  flush.console()
  
  save_name<-paste0(save_dir_all,"/combined_",crop,"_hi.tif")
  save_name_change<-paste0(save_dir_all,"/combined_",crop,"_hi_change.tif")
  
  data<-terra::rast(save_name)
  data_hist<-data[[grep("historic-historic",names(data))]]
  
  change<-terra::rast(lapply(Scenarios[Scenario!="historic",combined],FUN=function(SCENARIO){
    data_fut<-data[[grep(SCENARIO,names(data))]]
    data_fut-data_hist
  }))
  
  terra::writeRaster(change,filename = save_name_change)
  
}


# Combined files
filenames<-list.files(save_dir_all,"combined_",full.names = T)

data<-terra::rast(filenames[2])
plot(data)
