
# Load R functions & packages ####
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

require(data.table)
require(terra)
require(doFuture)
require(stringi)

# Set location of climate data stacks ####

#timeframe_choice<-"annual"
timeframe_choice<-"jagermeyr"

# Working locally
#haz_timeseries_dir<-paste0("Data/hazard_timeseries/",timeframe_choice

# Working on cglabs server
if(timeframe_choice!="annual"){
  haz_timeseries_dir<-paste0("/home/jovyan/common_data/atlas_hazards/cmip6/indices_seasonal/by_season/",timeframe_choice,"/hazard_timeseries")
}

# Set up workspace ####
# Set scenarios and time frames to analyse
Scenarios<-c("ssp245","ssp585")
Times<-c("2021_2040","2041_2060")

# Create combinations of scenarios and times
Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))
Scenarios[,combined:=paste0(Scenario,"-",Time)]

# Set hazards to include in analysis
hazards<-c("NDD","NTx40","NTx35","HSH_max","HSH_mean","THI_max","THI_mean","NDWS","TAI","NDWL0","PTOT","TAVG")
haz_meta<-data.table::fread("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_metadata.csv")
haz_class_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_classes.csv"
haz_class<-data.table::fread(haz_class_url)
haz_class<-haz_class[index_name %in% hazards,list(index_name,description,direction,crop,threshold)]
haz_classes<-unique(haz_class$description)

# duplicate generic non-heat stress variables for livestock
livestock<-haz_class[crop!="generic",unique(crop)]
non_heat<-c("NDD","NTx40","NTx35","NDWS","TAI","NDWL0","PTOT")

haz_class<-rbind(haz_class[crop=="generic"],
  rbindlist(lapply(1:length(livestock),FUN=function(i){
    rbind(haz_class[crop=="generic" & index_name %in% non_heat][,crop:=livestock[i]],haz_class[crop==livestock[i]])
  }))
)

# Pull out severity classes and associate impact scores
severity_classes<-unique(fread(haz_class_url)[,list(description,value)])
setnames(severity_classes,"description","class")

# Create combinations of scenarios and hazards
scenarios_x_hazards<-data.table(Scenarios,Hazard=rep(hazards,each=nrow(Scenarios)))[,Scenario:=as.character(Scenario)][,Time:=as.character(Time)]

# read in mapspam metadata
ms_codes<-data.table::fread("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SpamCodes.csv")[,Code:=toupper(Code)]
ms_codes<-ms_codes[compound=="no"]

# read in ecocrop
ecocrop<-fread("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/ecocrop.csv")
ecocrop[,Temp_Abs_Min:=as.numeric(Temp_Abs_Min)
        ][,Temp_Abs_Max:=as.numeric(Temp_Abs_Max)
          ][,Rain_Abs_Min:=as.numeric(Rain_Abs_Min)
            ][,Rain_Abs_Max:=as.numeric(Rain_Abs_Max)]

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
  # Remove THI & HSH this is not for crops
  X<-X[!grepl("THI|HSH",index_name)]
  X$crop<-ms_codes[i,Fullname]
  rbind(Y,X)
}))

haz_class<-rbind(haz_class,haz_class2)

haz_class[,direction2:="G"
          ][direction=="<",direction2:="L"
            ][,index_name2:=index_name
              ][index_name %in% c("TAVG","PTOT"),index_name2:=paste0(index_name,"_",direction2)]

haz_class<-unique(haz_class)

# Set analysis parameters
PropThreshold<-0.5
PropTDir=">"

crop_choices<-c(fread(haz_class_url)[,unique(crop)],ms_codes[,sort(Fullname)])

# Classify time series climate variables based on hazard thresholds ####

# Set save directory for classified hazard stacks
haz_time_class_dir<-paste0("Data/hazard_timeseries_class/",timeframe_choice)
if(!dir.exists(haz_time_class_dir)){dir.create(haz_time_class_dir,recursive=T)}

# Create a table of unique thresholds
Thresholds_U<-unique(haz_class[description!="No significant stress",list(index_name,direction,threshold)])
Thresholds_U[,code:=paste0(direction,threshold)
][,code:=gsub("<","L",code)
][,code:=gsub(">","G",code)]

files<-list.files(haz_timeseries_dir,".tif",full.names = T)
files<-files[!grepl("ENSEMBLEsd",files)]
overwrite<-F
n<-0

rast_class<-function(data,direction,threshold,minval=-9999,maxval=9999){
  from=c(minval,threshold)
  to=c(threshold,maxval)
  
  if(direction %in% c("G","g",">")){
    becomes<-c(0,1)
  }
  
  if(direction %in% c("L","l","<")){
    becomes<-c(1,0)
  }
  
  data<-terra::classify(data,data.frame(from=from,to=to,becomes=becomes))
  return(data)
}

parallel::detectCores()

registerDoFuture()
plan("multisession", workers = 20) # change to multicore for linux execution

foreach(i = 1:nrow(Thresholds_U)) %dopar% {

#for(i in 1:nrow(Thresholds_U)){
  files_ss<-grep(Thresholds_U[i,index_name],files,value=T)
  
  for(j in 1:length(files_ss)){
    #n<-n+1
    file<-gsub(".tif",paste0("-",Thresholds_U[i,code],".tif"),paste0(haz_time_class_dir,"/",tail(tstrsplit(files_ss[j],"/"),1)),fixed = T)
    
    # Display progress
    #cat('\r   ')
    #cat('\r',paste0(n,"/",nrow(Thresholds_U)*length(files_ss)," | Threshold ",i,"/",nrow(Thresholds_U), " - ",Thresholds_U[i,paste0(c(index_name,direction,threshold),collapse = " ")]," | scenario ",j,"/",length(files_ss)))
    #flush.console()
    
    if((!file.exists(file))|overwrite){
      data<-terra::rast(files_ss[j])
      data_class<-rast_class(data=data,
                             direction = Thresholds_U[i,direction],
                             threshold = Thresholds_U[i,threshold],
                             minval=-9999,
                             maxval=9999)
      terra::writeRaster(data_class,filename = file)
      }
  }
  
}

plan(sequential)

# Calculate risk across classified time series ####
haz_time_risk_dir<-paste0("Data/hazard_timeseries_risk/",timeframe_choice)
if(!dir.exists(haz_time_risk_dir)){dir.create(haz_time_risk_dir,recursive=T)}

files<-list.files(haz_time_class_dir,full.names = T)
files2<-list.files(haz_time_class_dir)
overwrite<-F

registerDoFuture()
plan("multisession", workers = 12)

foreach(i = 1:length(files)) %dopar% {
  
  file<-paste0(haz_time_risk_dir,"/",files2[i])
  
  if((!file.exists(file))|overwrite){
    data<-terra::rast(files[i])
    data<-terra::app(data,fun="mean",na.rm=T)
    terra::writeRaster(data,filename = file)
  }
}

plan(sequential)

# Calculate mean and sd across time series ####
haz_mean_dir<-paste0("Data/hazard_timeseries_mean/",timeframe_choice)
if(!dir.exists(haz_mean_dir)){dir.create(haz_mean_dir,recursive=T)}

haz_sd_dir<-paste0("Data/hazard_timeseries_sd/",timeframe_choice)
if(!dir.exists(haz_sd_dir)){dir.create(haz_sd_dir,recursive=T)}

files<-list.files(haz_timeseries_dir,".tif",full.names = T)
files2<-list.files(haz_timeseries_dir,".tif")
overwrite<-F

registerDoFuture()
plan("multisession", workers = 12)

foreach(i = 1:length(files)) %dopar% {
  
  file<-paste0(haz_mean_dir,"/",files2[i])
  file2<-paste0(haz_sd_dir,"/",files2[i])
  
  if((!file.exists(file))|overwrite){
    data<-terra::rast(files[i])
    data<-terra::app(data,fun="mean",na.rm=T)
    terra::writeRaster(data,filename = file)
    
    data<-terra::app(data,fun="sd",na.rm=T)
    terra::writeRaster(data,filename = file2,overwrite=T)
  }
}

plan(sequential)

# Calculate change in mean values ####
files<-list.files(haz_mean_dir,".tif",full.names = T)
files<-files[!grepl("change",files)]
files_hist<-grep("historic",files,value = T)
files_fut<-files[!files %in% files_hist]

data_hist<-terra::rast(files_hist[1])

for(j in 1:length(files_fut)){
  data_fut<-terra::rast(files_fut[j])
  # Display progress
  cat('\r                                                                                                                     ')
  cat('\r',paste0("Scenario: ",j))
  flush.console()
  
  change<-data_fut-data_hist
  names(change)<-paste0(names(change),"_change")
  
  save_name_change<-gsub(".tif","_change.tif",files_fut[j])  
  
  terra::writeRaster(change,filename = save_name_change,overwrite=T)
  
}

# Create Crop Risk Stacks####
haz_risk_dir<-paste0("Data/hazard_risk/",timeframe_choice)
if(!dir.exists(haz_risk_dir)){
  dir.create(haz_risk_dir,recursive = T)
}

names(rast(list.files(haz_risk_dir,".tif",full.names = T)))


# Create stacks of hazard x crop/animal x scenario x timeframe
haz_class_files<-list.files(haz_time_class_dir)

# Subset to severe
severity_classes<-severity_classes[value %in% c(2,3)]

overwrite=F

registerDoFuture()
plan("multisession", workers = 12)

# Note there are only 2-3 severity values here it would be better to divide the next step between more workers
foreach(i = 1:haz_class[,length(unique(crop))]) %dopar%{
  
  for(j in 1:nrow(severity_classes)){
  
    crop_focus<-haz_class[,unique(crop)][i]
    severity_class<-severity_classes[j,class]

    save_name<-paste0(haz_risk_dir,"/",crop_focus,"_",tolower(severity_class),".tif")
    
    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste("Crop:",i,crop_focus,"| severity:",j,severity_class))
    flush.console()
    
    if(!file.exists(save_name)|overwrite==T){
      
      haz_class_crop<-haz_class[crop==crop_focus & description == severity_class]
      grep_vals<-haz_class_crop[,paste0(paste0(index_name,"-",direction2,threshold),collapse = "|")]
      haz_class_files2<-gsub("_max_max","max",haz_class_files)
      haz_class_files2<-gsub("_mean_mean","mean",haz_class_files2)
      haz_class_files2<-gsub("_max","",haz_class_files2)
      haz_class_files2<-gsub("_mean","",haz_class_files2)
      haz_class_files2<-gsub("_sum","",haz_class_files2)
      haz_class_files2<-gsub("max","_max",haz_class_files2)
      haz_class_files2<-gsub("mean","_mean",haz_class_files2)
      
      renames<-haz_class_files2[grepl(grep_vals,haz_class_files2)]
      renames<-gsub("historical_","historic-historic-",renames)
      renames<-gsub("ssp245_ENSEMBLE_mean_","ssp245-",renames)
      renames<-gsub("ssp585_ENSEMBLE_mean_","ssp585-",renames)
      renames<-gsub("2021_2040_","2021_2040-",renames)
      renames<-gsub("2041_2060_","2041_2060-",renames)
      renames<-tstrsplit(renames,"-",keep=1:3)
      renames<-paste0(renames[[1]],"-",renames[[2]],"-",renames[[3]],"-",crop_focus,"-",severity_class)
      
      haz_class_files2<-haz_class_files[grepl(grep_vals,haz_class_files2)]
      
      data<-terra::rast(paste0(haz_time_risk_dir,"/",haz_class_files2))
      names(data)<-renames
      
      terra::writeRaster(data,file=save_name,overwrite=T)
    }
    
  }
  
}

# Interactions ####
haz_time_int_dir<-paste0("Data/hazard_timeseries_int/",timeframe_choice)
if(!dir.exists(haz_time_int_dir)){dir.create(haz_time_int_dir,recursive=T)}

# Interactions - Crops
# Set variables that can be interacted for heat wet and dry
crop_heat<-c("NTx35","TAVG_G")
crop_wet<-c("NDWL0","PTOT_G")
crop_dry<-c("NDD","PTOT_L","NDWS")

crop_choices2<-crop_choices[!grepl("_tropical|_highland",crop_choices)]

# Create a unique list of all the 3-way combinations required for the crops and severity classes selected
combinations_c<-unique(rbindlist(lapply(1:length(crop_choices2),FUN=function(i){
  rbindlist(lapply(1:length(severity_classes$class),FUN=function(j){
    X<-data.table(expand.grid(heat=crop_heat,wet=crop_wet,dry=crop_dry,stringsAsFactors=F))
    haz_rename<-haz_class[crop==crop_choices2[i] & description == severity_classes$class[j],list(old=index_name2,new=paste0(index_name,"-",direction2,threshold))]
    X[,heat:=stri_replace_all_regex(heat,pattern=haz_rename$old,replacement = haz_rename$new,vectorize_all = F)
      ][,dry:=stri_replace_all_regex(dry,pattern=haz_rename$old,replacement = haz_rename$new,vectorize_all = F)
        ][,wet:=stri_replace_all_regex(wet,pattern=haz_rename$old,replacement = haz_rename$new,vectorize_all = F)
          ][,severity_class:=severity_classes$class[j]
            ][,crop:=crop_choices2[i]]
    X
  }))
})))

# Interactions - Animals
# Set variables that can be interacted for heat wet and dry
animal_heat<-c("THI_max") # THI_mean or THI_max can be used here (or both)
animal_wet<-c("NDWL0","PTOT_G")
animal_dry<-c("NDD","PTOT_L","NDWS")

crop_choicesX<-crop_choices[grepl("_tropical|_highland",crop_choices)]

# Create a unique list of all the 3-way combinations required for the crops and severity classes selected
combinations_a<-unique(rbindlist(lapply(1:length(crop_choicesX),FUN=function(i){
  rbindlist(lapply(1:length(severity_classes$class),FUN=function(j){
    X<-data.table(expand.grid(heat=animal_heat,wet=animal_wet,dry=animal_dry,stringsAsFactors=F))
    haz_rename<-haz_class[crop==crop_choicesX[i] & description == severity_classes$class[j],list(old=index_name2,new=paste0(index_name,"-",direction2,threshold))]
    X[,heat:=stri_replace_all_regex(heat,pattern=haz_rename$old,replacement = haz_rename$new,vectorize_all = F)
    ][,dry:=stri_replace_all_regex(dry,pattern=haz_rename$old,replacement = haz_rename$new,vectorize_all = F)
    ][,wet:=stri_replace_all_regex(wet,pattern=haz_rename$old,replacement = haz_rename$new,vectorize_all = F)
    ][,severity_class:=severity_classes$class[j]
      ][,crop:=crop_choicesX[i]]
    X
  }))
})))

# Restructure names of classified hazard files so they can be easily searched for scenario x timeframe x hazard x threshold
haz_class_files<-list.files(haz_time_class_dir,full.names = T)
haz_class_files2<-list.files(haz_time_class_dir)
haz_class_files2<-gsub("_max_max","max",haz_class_files2)
haz_class_files2<-gsub("_mean_mean","mean",haz_class_files2)
haz_class_files2<-gsub("_max","",haz_class_files2)
haz_class_files2<-gsub("_mean","",haz_class_files2)
haz_class_files2<-gsub("_sum","",haz_class_files2)
haz_class_files2<-gsub("max","_max",haz_class_files2)
haz_class_files2<-gsub("mean","_mean",haz_class_files2)
haz_class_files2<-gsub("historical_","historic-historic-",haz_class_files2)
haz_class_files2<-gsub("ssp245_ENSEMBLE_mean_","ssp245-",haz_class_files2)
haz_class_files2<-gsub("ssp585_ENSEMBLE_mean_","ssp585-",haz_class_files2)
haz_class_files2<-gsub("2021_2040_","2021_2040-",haz_class_files2)
haz_class_files2<-gsub("2041_2060_","2041_2060-",haz_class_files2)

# Join livestock and crop combinations
combinations<-unique(rbind(combinations_c,combinations_a)[,crop:=NULL])
# Limit to "Severe" class
combinations<-combinations[severity_class=="Severe"]
overwrite<-F

registerDoFuture()
plan("multisession", workers = 10)

foreach(i =  sample(1:nrow(combinations))) %dopar% {
#for(i in 1:nrow(combinations)){

        combos<-combinations[i,1:3]
        combo_haz<-sort(unlist(combinations[i,1:3]))
        grep_vals<-paste0(paste0(combo_haz,".tif"),collapse = "|")
        combo_names<-c(paste0(combo_haz,collapse="+"),apply(combn(1:3,2),2,FUN=function(X){paste0(combo_haz[X],collapse = "+")}))
        combo_binary<-data.table(combo_name=c(combo_names,combo_haz),value=0)[grep(combo_haz[1],combo_name),value:=1
                                                         ][grep(combo_haz[2],combo_name),value:=value+10
                                                           ][grep(combo_haz[3],combo_name),value:=value+100]
        
       for(l in 1:nrow(Scenarios)){
         
         # Display progress
         cat('\r                                                                                                                     ')
         cat('\r',paste("Combination:",i,"/",nrow(combinations),"| Scenario:",l,"/",nrow(Scenarios)))
         flush.console()
         
         
         lyr_names<-paste0(Scenarios[l,combined],"-",combo_names)
         save_names<-paste0(haz_time_int_dir,"/",lyr_names,".tif")
         save_name_any<-gsub(".tif","_any.tif",save_names[1])

         
         if((!file.exists(save_names[1])|!file.exists(save_name_any))|overwrite==T){
          
          files<-haz_class_files[grepl(grep_vals,haz_class_files2) & grepl(Scenarios[l,combined],haz_class_files2)]
          haz<-lapply(files,rast)
          names(haz)<-combo_haz
          
          
          haz[[2]]<-haz[[2]]*10
          haz[[3]]<-haz[[3]]*100
          
          haz_sum<-terra::rast(lapply(1:nlyr(haz[[1]]),FUN=function(m){
            sum(terra::rast(lapply(haz,"[[",m)),na.rm=T)
          }))
          names(haz_sum)<-names(haz[[1]])
  
          # Any haz
          if(!file.exists(save_name_any)|overwrite==T){
            data<-terra::mask(haz_sum,haz_sum,maskvalues=1:111,updatevalue=1)
            data<-terra::app(data,fun="mean",na.rm=T)
            names(data)<-paste0(lyr_names[1],"_any")
            terra::writeRaster(data,filename =  save_name_any,overwrite==T)
          }
     
          
          # 3-way interaction
          int_risk<-function(data,interaction_n,lyr_names){
            data<-terra::mask(data,data,maskvalues=combo_binary$value[-interaction_n],updatevalue=0)
            data<-terra::classify(data,data.table(from=1,to=999999,becomes=1))
            data<-terra::app(data,fun="mean",na.rm=T)
            names(data)<-lyr_names[interaction_n]
            return(data)
          }
          
          if(!file.exists(save_names[1])|overwrite==T){
            data1<-int_risk(data=haz_sum,interaction_n = 1,lyr_names)
            terra::writeRaster(data1,filename = save_names[1],overwrite==T)
          }
   
       
          if(!file.exists(save_names[2])|overwrite==T){
            data2<-int_risk(data=haz_sum,interaction_n = 2,lyr_names)
            terra::writeRaster(data2,filename = save_names[2],overwrite==T)
            rm(data2)
          }
          
          if(!file.exists(save_names[3])|overwrite==T){
            data3<-int_risk(data=haz_sum,interaction_n = 3,lyr_names)
            terra::writeRaster(data3,filename = save_names[3],overwrite==T)
            rm(data3)
          }
          
          if(!file.exists(save_names[4])|overwrite==T){
            data4<-int_risk(data=haz_sum,interaction_n = 4,lyr_names)
            terra::writeRaster(data4,filename = save_names[4],overwrite==T)
            rm(data4)
          }
          
          rm(data1,haz,haz_sum)
          gc()
          }
          }
        }
  
plan(sequential)

# Interactions: For each crop combine hazards into a single file and add to hazard_risk dir #####
combinations_ca<-rbind(combinations_c,combinations_a)
sev_class<-"Severe"

haz_int_files<-list.files(haz_time_int_dir,".tif",full.names = T)
overwrite<-T

registerDoFuture()
plan("multisession", workers = 10)

foreach(i =  1:length(crop_choices)) %dopar% {
#for(i in 1:length(crop_choices)){
  for(j in 1:length(sev_class)){
    
    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste("crop_choices:",i,"/",length(crop_choices),"| Severity Class:",j,"/",length(sev_class)))
    flush.console()
    
    crop_focus<-crop_choices[i]

    save_name2<-paste0(haz_risk_dir,"/",crop_focus,"_",tolower(sev_class[j]),"_int.tif")
    save_name_any<-paste0(haz_risk_dir,"/",crop_focus,"_",tolower(sev_class[j]),"_any.tif")
    
    crop_combos<-combinations_ca[crop==crop_focus & severity_class==sev_class[j]]
  
    grep_names<-unique(unlist(lapply(1:nrow(crop_combos),FUN=function(k){
      combo_haz<-sort(unlist(crop_combos[k,1:3]))
      combo_names<-paste0("-",c(paste0(combo_haz,collapse="+"),apply(combn(1:3,2),2,FUN=function(X){paste0(combo_haz[X],collapse = "+")})),".tif")   
      combo_names
    })))
      
      
    if((!file.exists(save_name_any))|overwrite==T){
      grep_names_any<-grep_names[str_count(grep_names,"[+]")==2]
      grep_names_any<-gsub(".tif","_any.tif",grep_names_any)
  
      grep_names_any<-paste0(grep_names_any,collapse = "|")
      
      grep_names_any<-gsub(".","[.]",grep_names_any,fixed = T)
      grep_names_any<-gsub("+","[+]",grep_names_any,fixed = T)
      
      
      int_files_any<-grep(paste0(grep_names_any,collapse = "|"),haz_int_files,value = T)
      
      
      data<-terra::rast(int_files_any)
      N<-names(data)
      N<-gsub("PTOT-L","PTOT_L-",N)
      N<-gsub("PTOT-G","PTOT_G-",N)
      N<-gsub("TAVG-L","TAVG_L-",N)
      N<-gsub("TAVG-G","TAVG_G-",N)
      N1<-tstrsplit(N,"[+]")
      N2<-tstrsplit(N1[[1]],"-")
      
      N<-data.table(scenario=N2[[1]],
                    timeframe=N2[[2]],
                    haz1=unlist(tstrsplit(N2[[3]],"-",keep=1)),
                    haz2=unlist(tstrsplit(N1[[2]],"-",keep=1)),
                    haz3=unlist(tstrsplit(N1[[3]],"-",keep=1)))
      N[,layer_name:=paste0(scenario,"-",timeframe,"-",haz1,"+",haz2,if(!is.na(haz3[1])){paste0("+",haz3[1])}else{""},"-",crop_focus,"-",sev_class[j]),by=haz3]
      
      names(data)<-N$layer_name
      
      terra::writeRaster(data,filename = save_name_any,overwrite=overwrite)
      
      }
      
      if((!file.exists(save_name2))|overwrite==T){
        grep_names<-paste0(grep_names,collapse = "|")
        # Escape special characters, you cannot use fixed and the "|" character as it treats the bar as a fixed too
        grep_names<-gsub(".","[.]",grep_names,fixed = T)
        grep_names<-gsub("+","[+]",grep_names,fixed = T)
        
        int_files<-grep(paste0(grep_names,collapse = "|"),haz_int_files,value = T)
        
        data<-terra::rast(int_files)
        N<-names(data)
        N<-gsub("PTOT-L","PTOT_L-",N)
        N<-gsub("PTOT-G","PTOT_G-",N)
        N<-gsub("TAVG-L","TAVG_L-",N)
        N<-gsub("TAVG-G","TAVG_G-",N)
        N1<-tstrsplit(N,"[+]")
        N2<-tstrsplit(N1[[1]],"-")
        N<-data.table(scenario=N2[[1]],
                      timeframe=N2[[2]],
                      haz1=unlist(tstrsplit(N2[[3]],"-",keep=1)),
                      haz2=unlist(tstrsplit(N1[[2]],"-",keep=1)),
                      haz3=unlist(tstrsplit(N1[[3]],"-",keep=1)))
        N[,layer_name:=paste0(scenario,"-",timeframe,"-",haz1,"+",haz2,if(!is.na(haz3[1])){paste0("+",haz3[1])}else{""},"-",crop_focus,"-",sev_class[j]),by=haz3]
       
        names(data)<-N$layer_name
        
        terra::writeRaster(data,filename = save_name2,overwrite=overwrite)
      }
    
      
  }}

plan(sequential)

# Check results 
file<-list.files(haz_risk_dir,"_int",full.names = T)
names(rast(file[1]))
plot(rast(file[1]))


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
    cat('\r',paste0("File: ",i," | Scenario: ",j))
    flush.console()
    
    save_name_change<-gsub(".tif","_change.tif",files_fut[j])  
    
    if(!file.exists(save_name_change)){
    change<-data_fut-data_hist
    names(change)<-paste0(names(change),"_change")
    
  
    terra::writeRaster(change,filename = save_name_change,overwrite=T)
    }
    
  }
 
}


# Create Classified Risk Stacks ####
risk_threshold<-0.5
haz_risk_dir_class<-paste0("Data/hazard_risk_class/t",risk_threshold,"/",timeframe_choice)
if(!dir.exists(haz_risk_dir_class)){
  dir.create(haz_risk_dir_class,recursive = T)
}

files<-list.files(haz_risk_dir,".tif",full.names = T)
files<-files[!grepl("change",files)]
overwrite<-F

registerDoFuture()
plan("multisession", workers = 10)

#  Crop loop starts here
foreach(i = 1:length(files)) %dopar% {
#for(i in 1:length(files)){
  # Display progress
  cat('\r                                                                                                                     ')
  cat('\r',paste0("File: ",i,"/",length(files)," - ", files[i]))
  flush.console()
  
  save_name<-paste0(haz_risk_dir_class,"/",tail(unlist(tstrsplit(files[i],"/")),1))
  
  if((!file.exists(save_name))|overwrite==T){
    data<-terra::rast(files[i])
    N<-values(data)
    N[N<risk_threshold & !is.na(N)]<-0
    N[N>=risk_threshold & !is.na(N)]<-1
    data[]<-N
    
    terra::writeRaster(data,filename = save_name,overwrite=T)
  }
}
