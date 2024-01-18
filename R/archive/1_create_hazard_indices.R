source("R/haz_functions.R")
require(data.table)
require(terra)
require(doFuture)

# Set scenarios and time frames to analyse
Scenarios<-c("ssp245","ssp585")
Times<-c("2021_2040","2041_2060")

# Create combinations of scenarios and times
Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))
Scenarios[,combined:=paste0(Scenario,"-",Time)]

# Set hazards to include in analysis
hazards<-c("NDD","NTx40","NTx35","HSH_max","HSH_mean","THI_max","THI_mean","NDWS","TAI","NDWL0","PTOT","TAVG")
haz_meta<-data.table::fread("./Data/metadata/haz_metadata.csv")
haz_class<-fread("./Data/metadata/haz_classes.csv")[index_name %in% hazards,list(index_name,description,direction,crop,threshold)]
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
  # Remove THI & HSH this is not for crops
  X<-X[!grepl("THI|HSH",index_name)]
  X$crop<-ms_codes[i,Fullname]
  rbind(Y,X)
}))

haz_class<-rbind(haz_class,haz_class2)

# Set analysis parameters
PropThreshold<-0.5
PropTDir=">"

timeframe_choice<-"annual"
#timeframe_choice<-"jagermeyr"

# Where are climate data stacks saved?
clim_dir<-paste0("Data/hazard_timeseries/",timeframe_choice)

# where to save classified climate hazards?
haz_class_dir<-paste0("Data/hazard_classified/",timeframe_choice)
if(!dir.exists(haz_class_dir)){
  dir.create(haz_class_dir,recursive = T)
}

# Global hazard index savedir
haz_index_dir<-paste0("Data/hazard_index/",timeframe_choice)
if(!dir.exists(haz_index_dir)){
  dir.create(haz_index_dir,recursive=T)
}

crop_choices<-c(fread("./Data/metadata/haz_classes.csv")[,unique(crop)],ms_codes[,sort(Fullname)])

# Calculate hazard risk and index for time period x scenario (for the selected timeframe annual/jagermeyer) ####
# Calculate hazard indices?
calc_hi<-F

# TO DO:
# HAZARD WRAPPER NEEDS AMENDING
# We need to convert each time series stack to hazard 1/0 per threshold number and save these
# These stacks can then be combined to look at the intersection of hazards properly

#registerDoFuture()
#plan("multisession", workers = 10)

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
      
      if(calc_hi==T){
        # Hazard Index = severity x recurrence
        haz_index<-hazard_index(Data=Hazards,
                                hazards=unique(Thresholds$Variable),
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
    }
    

# Calculate change for hazard index ####
calc_hi<-F
if(calc_hi){
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
}

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

# Pull out mean values for each timeseries ####
# where to save mean values
haz_mean_dir<-paste0("Data/hazard_mean/",timeframe_choice)
if(!dir.exists(haz_mean_dir)){
  dir.create(haz_mean_dir,recursive = T)
}


files<-list.files(haz_class_dir,".tif",full.names = T)
files<-files[!grepl("_change",files)]

scens<-unique(scenarios_x_hazards$combined)

for(j in 1:length(scens)){
  filename<-paste0(haz_mean_dir,"/",scens[j],".tif")
  data<-terra::rast(lapply(1:length(hazards),FUN=function(i){
      # Display progress
      cat('\r                                                                                                                     ')
      cat('\r',paste0("Scenario: ",j," | Hazard: ",i))
      flush.console()
    
      file<-grep(paste0(scens[j],"-",hazards[i]),files,value = T)[1]
      data<-terra::rast(file)[[1]]
      names(data)<-paste0(scens[j],"_",names(data))
      data
  }))
  terra::writeRaster(data,file=filename,overwrite=T)
  
}


# Calculate change mean values ####
files<-list.files(haz_mean_dir,".tif",full.names = T)
files<-files[!grepl("change",files)]
files_hist<-grep("historic",files,value = T)
files_fut<-files[files!=files_hist]

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

# Create Risk Stacks ####
haz_risk_dir<-paste0("Data/hazard_risk/",timeframe_choice)
if(!dir.exists(haz_risk_dir)){
  dir.create(haz_risk_dir,recursive = T)
}

haz_class[,direction2:="G"
          ][direction=="<",direction2:="L"]
# Create stacks of hazard x crop/animal x scenario x timeframe
haz_class_files<-list.files(haz_class_dir)

# Subset to severe
severity_classes<-severity_classes[value %in% c(2,3)]

overwrite=F

registerDoFuture()
plan("multisession", workers = 10)

# Note there are only 2-3 severity values here it would be better to divide the next step between more workers
foreach(j = 1:length(severity_classes)) %dopar%{

#for(j in 1:nrow(severity_classes)){

  lapply(1:length(crop_choices),FUN=function(i){
    crop_focus<-crop_choices[i]
      severity_class<-severity_classes[j,class]
     haz_class_crop<-haz_class[crop==crop_focus & description == severity_class]
     grep_vals<-haz_class_crop[,paste0(paste0(index_name,"-",direction2,threshold,"-"),collapse = "|")]
     haz_class_files2<-haz_class_files[grepl(grep_vals,haz_class_files) & !grepl("change",haz_class_files)]
   
     save_name<-paste0(haz_risk_dir,"/",crop_focus,"_",tolower(severity_class),".tif")
     
     # Display progress
     cat('\r                                                                                                                     ')
     cat('\r',paste("Crop:",i,crop_focus,"| severity:",j,severity_class))
     flush.console()
     
     if(!file.exists(save_name)|overwrite==T){
     
     data<-terra::rast(lapply(1:length(haz_class_files2),FUN=function(k){
       # Display progress
       cat('\r                                                                                                                     ')
       cat('\r',paste("Combining layers - crop:",i,crop_focus,"| severity:",j,severity_class,"| file:",k))
       flush.console()
       
       file<-haz_class_files2[k]
       data<-terra::rast(paste0(haz_class_dir,"/",file))[[3]]
       if(grepl("TAVG|PTOT",file)){
         lyr_name<-c(unlist(tstrsplit(file,"-",keep=c(2,3,4,5))),crop_focus,severity_class)
         lyr_name[4]<-substr(lyr_name[4],1,1)
         lyr_name[3]<-paste0(lyr_name[3],"_",lyr_name[4])
         lyr_name<-lyr_name[-4]
       }else{
        lyr_name<-c(unlist(tstrsplit(file,"-",keep=c(2,3,4))),crop_focus,severity_class)
       }
       lyr_name<-paste0(lyr_name,collapse = "-")
       names(data)<-lyr_name
       data
     }))
     
     terra::writeRaster(data,file=save_name,overwrite=T)
     }
   
   })
  
  
  }

# Interaction of Risks ####
# Crops  ###
crop_heat<-c("NTx35","TAVG_G")
crop_wet<-c("NDWL0","PTOT_G")
crop_dry<-c("NDD","PTOT_L","NDWS")

combinations<-rbind(
  expand.grid(heat=crop_heat,wet=crop_wet,dry=crop_dry),
  expand.grid(heat=crop_heat,wet=crop_wet,dry=NA),
  expand.grid(heat=crop_heat,wet=NA,dry=crop_dry),
  expand.grid(heat=NA,wet=crop_wet,dry=crop_dry)
)

files<-list.files(haz_risk_dir,full.names = T)
files<-grep(paste(c("generic",ms_codes$Fullname),collapse = "|"),files,value = T)
files<-files[!grepl("_int",files)]

overwrite<-F

registerDoFuture()
plan("multisession", workers = 10)

#  loop starts here
#foreach(j = 1:length(files)) %dopar% {
for(j in 1:length(files)){
  file<-files[j]
  save_name<-gsub(".tif","_int.tif",file)
  data<-terra::rast(file)
  data_names<-names(data)
  
  if(!file.exists(save_name)|overwrite==T){
    interactions<-terra::rast(lapply(1:nrow(combinations),FUN=function(i){
      
      # Add filename & check to see if it exists    
      haz<-combinations[i,]
      haz<-haz[!is.na(haz)]
      
      # Display progress
      cat('\r                                                                                                                                                 ')
      cat('\r',paste("Multiplying Risks - file:",j,"/",length(files)," - ",file,"| hazard:",i,"/",nrow(combinations)," - ",paste0(haz,collapse = "+")))
      flush.console()
      
      data_comb<-lapply(haz,FUN=function(HAZ){
        data[[grep(HAZ,data_names,value = T)]]
      })
      
      X<-data_comb[[1]]
      for(k in 2:length(data_comb)){
        X<-X*data_comb[[k]]
      }
      
      names(X)<-gsub(haz[1],paste0(haz,collapse = "+"),names(X))
      X
    }))
    terra::writeRaster(interactions,filename = save_name,overwrite=T)
  }
  
}

#### Livestock
livestock<-fread("./Data/metadata/haz_classes.csv")[crop!="generic",unique(crop)]

animal_heat<-c("THI_max") # THI_mean or THI_max can be used here (or both)
animal_wet<-c("NDWL0","PTOT_G")
animal_dry<-c("NDD","PTOT_L","NDWS")

combinations<-rbind(
  expand.grid(heat=animal_heat,wet=animal_wet,dry=animal_dry),
  expand.grid(heat=animal_heat,wet=animal_wet,dry=NA),
  expand.grid(heat=animal_heat,wet=NA,dry=animal_dry),
  expand.grid(heat=NA,wet=animal_wet,dry=animal_dry)
)

files<-list.files(haz_risk_dir,full.names = T)
files<-grep(paste(livestock,collapse = "|"),files,value = T)
files<-files[!grepl("_int",files)]

registerDoFuture()
plan("multisession", workers = 10)


#  Livestock loop starts here
foreach(j = 1:length(files)) %dopar% {
  
 #for(j in 1:length(files)){
  file<-files[j]
  save_name<-gsub(".tif","_int.tif",file)
  data<-terra::rast(file)
  data_names<-names(data)
  
  if(!file.exists(save_name)){
    interactions<-terra::rast(lapply(1:nrow(combinations),FUN=function(i){
      
      haz<-combinations[i,]
      haz<-haz[!is.na(haz)]
      
      # Display progress
      cat('\r                                                                                                                                                 ')
      cat('\r',paste("Multiplying Risks - file:",j,"/",length(files)," - ",file,"| hazard:",i,"/",nrow(combinations)," - ",paste0(haz,collapse = "+")))
      flush.console()
      
      data_comb<-lapply(haz,FUN=function(HAZ){
        data[[grep(HAZ,data_names,value = T)]]
      })
      
      X<-data_comb[[1]]
      for(k in 2:length(data_comb)){
        X<-X*data_comb[[k]]
      }
      
      names(X)<-gsub(haz[1],paste0(haz,collapse = "+"),names(X))
      X
    }))
    terra::writeRaster(interactions,filename = save_name)
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
