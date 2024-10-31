# Please run 0_server_setup.R before executing this script
# 0.1) Load R functions & packages ####
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

# List of packages to be loaded
packages <- c("terra", 
              "data.table", 
              "future",
              "future.apply",
              "progressr",
              "parallel",
              "doFuture",
              "httr",
              "s3fs",
              "stringr", 
              "stringi",
              "httr",
              "xml2")

# This function will call packages first from the user library and second the system library
# This can help overcome issues with the Afrilab server where the system library has outdated packages that 
# require an contacting admin user to update
load_packages_prefer_user <- function(packages) {
  user_lib <- Sys.getenv("R_LIBS_USER")
  current_libs <- .libPaths()
  
  # Set user library as the first in the search path
  .libPaths(c(user_lib, current_libs))
  
  # Load pacman package
  library(pacman)
  
  # Install and load packages using pacman
  pacman::p_load(char = packages)
  
  # Restore original library paths
  .libPaths(current_libs)
}

load_packages_prefer_user(packages)

# 0.2) Set up workspace ####
# Set number of workers
worker_n<-parallel::detectCores()-1

# Set scenarios and time frames to analyse
Scenarios<-c("ssp245","ssp585")
Times<-c("2021_2040","2041_2060")

# Create combinations of scenarios and times
Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))
Scenarios[,combined:=paste0(Scenario,"-",Time)]

# Set hazards to include in analysis
hazards<-c("NTx40","NTx35","HSH_max","HSH_mean","THI_max","THI_mean","NDWS","TAI","NDWL0","PTOT","TAVG") # NDD is not being used as it cannot be projected to future scenarios

haz_meta<-data.table::fread(haz_meta_url, showProgress = FALSE)
haz_meta[variable.code %in% hazards]
haz_meta[,code2:=paste0(haz_meta$code,"_",haz_meta$`function`)]

haz_class<-data.table::fread(haz_class_url, showProgress = FALSE)
haz_class<-haz_class[index_name %in% hazards,list(index_name,description,direction,crop,threshold)]
haz_classes<-unique(haz_class$description)

# duplicate generic non-heat stress variables for livestock
livestock<-livestock<-haz_class[grepl("cattle|goats|poultry|pigs|sheep",crop),unique(crop)]
non_heat<-c("NTx40","NTx35","NDWS","TAI","NDWL0","PTOT") # NDD is not being used as it cannot be projected to future scenarios

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
ms_codes<-data.table::fread(ms_codes_url, showProgress = FALSE)[,Code:=toupper(Code)]
ms_codes<-ms_codes[compound=="no"]

# read in ecocrop
ecocrop<-fread(ecocrop_url, showProgress = FALSE)
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
                              ceiling((unlist(ecrop$Rain_Opt_Max)+unlist(ecrop$Rain_Abs_Max))/2), # Severe
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
      
      # NTxCrop - moderate  (>optimum)
      ntxcrop_m<-data.table(index_name=paste0("NTxM",ecrop$Temp_Opt_Max),
                           description=description,
                           direction=">",
                           crop=crop_common,
                           threshold=c(7, # Moderate
                                       14, # Severe
                                       21)) # Extreme
      
      # NTxCrop - severe  
      ntxcrop_s<-data.table(index_name=paste0("NTxS",ceiling((unlist(ecrop$Temp_Opt_Max)+unlist(ecrop$Temp_Abs_Max))/2)),
                            description=description,
                            direction=">",
                            crop=crop_common,
                            threshold=c(7, # Moderate
                                        14, # Severe
                                        21)) # Extreme
      
      
      # NTxCrop extreme (>absolute)
      ntxcrop_e<-data.table(index_name=paste0("NTxE",ecrop$Temp_Abs_Max),
                          description=description,
                          direction=">",
                          crop=crop_common,
                          threshold=c(1, # Moderate
                                      5, # Severe
                                      10)) # Extreme
      
      rbind(ptot_low,ptot_high,tavg_low,tavg_high,ntxcrop_m,ntxcrop_s,ntxcrop_e)
    }else{
      print(paste0(i,"-",j," | ",crop, " - ERROR NO MATCH"))
      NULL
    }
  }))
  
  # Average NTxM/S/E thresholds
  ec_haz<-unique(ec_haz[grep("NTxS",index_name),index_name:=paste0("NTxS",ceiling(mean(as.numeric(substr(index_name,5,6)))))
                        ][grep("NTxM",index_name),index_name:=paste0("NTxM",ceiling(mean(as.numeric(substr(index_name,5,6)))))
                          ][grep("NTxE",index_name),index_name:=paste0("NTxE",ceiling(mean(as.numeric(substr(index_name,5,6)))))])
  
  # Average threholds where multiple crops exist for a mapspam commodity
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
              ][index_name %in% c("TAVG","PTOT"),index_name2:=paste0(index_name,"_",direction2)
                ][,index_name:=gsub("NTxE|NTxM|NTxS","NTx",index_name)
                  ][grep("NTxE",index_name2),index_name2:="NTxE"
                    ][grep("NTxM",index_name2),index_name2:="NTxM"
                      ][grep("NTxS",index_name2),index_name2:="NTxS"]

haz_class<-unique(haz_class)

# Add summary function description to haz_class
haz_class<-merge(haz_class,unique(haz_meta[,c("variable.code","function")]),by.x="index_name",by.y="variable.code",all.x=T)
haz_class[,code2:=paste0(index_name,"_",`function`)][,code2:=gsub("_G_|_L_","_",code2)]

# Add hazard type to haz_class
haz_class[,filename:=paste0(index_name,"-",direction2,threshold,".tif")]
haz_class[,match_field:=code2][grepl("PTOT|TAVG",code2),match_field:=paste0(index_name2[1],"_",`function`[1]),by=code2]
haz_class[,match_field:=unlist(match_field)]
haz_class<-merge(haz_class,unique(haz_meta[,list(code2,type)]),by.y="code2",by.x="match_field",all.x=T)
haz_class[,match_field:=NULL]
haz_class[,haz_filename:=paste0(type,"-",index_name2,"-",crop,"-",description)]

# Set analysis parameters
PropThreshold<-0.5
PropTDir=">"

# Set crops and livestock included in the analysis
crop_choices<-c(fread(haz_class_url, showProgress = FALSE)[,unique(crop)],ms_codes[,sort(Fullname)])

# 0.3) Download hazard timeseries from s3 bucket ####
overwrite<-F
workers_dl<-10
# Specify the bucket name and the prefix (folder path)
s3_folder_path <- file.path("risk_prototype/data/hazard_timeseries",timeframe_choice,"")

# List files in the specified S3 bucket and prefix
file_list<-s3$dir_ls(file.path(bucket_name_s3,s3_folder_path))
file_list<-data.table(file_list=grep(".tif",file_list,value=T))
file_list<-file_list[,new_files:=gsub(file.path(bucket_name_s3,s3_folder_path),paste0(haz_timeseries_dir,"/"),file_list)]

# Here you can check if there are issues with file downloads by comparing the local size to the s3 size
if(F){
  file_list[,local_size:=round(file.info(new_files)$size/10^6,1)]
  file_list[,s3_size:=s3$file_size(file_list)][,s3_size:=round(as.numeric(s3_size),1)]
  file_list[s3_size!=local_size,new_files]
}

if(!overwrite){
  file_list<-file_list[!file.exists(new_files)]
}
  if(nrow(file_list)>0){
    
    convert_to_bytes <- function(size) {
      # Convert to character
      size <- as.character(size)
      # Remove any spaces
      size <- gsub(" ", "", size)
      
      # Extract the units (K, M, G) and the numeric value
      units <- tolower(substr(size, nchar(size), nchar(size)))
      value <- as.numeric(substr(size, 1, nchar(size) - 1))
      
      # Convert based on the unit
      if (units == "k") {
        return(value * 1024)
      } else if (units == "m") {
        return(value * 1024^2)
      } else if (units == "g") {
        return(value * 1024^3)
      } else {
        return(value)  # If no units, assume it's already in bytes
      }
    }
    
    calculate_file_hash <- function(file_path) {
      return(digest::digest(file = file_path, algo = "md5"))
    }
    
  # Set up the future plan for parallel processing
    if (worker_n == 1) {
      future::plan("sequential")
    } else {
      future::plan("multisession", workers = worker_n)
    }
    
    
    # Enable progressr
    progressr::handlers(global = TRUE)
    progressr::handlers("progress")
    
    # Define the maximum number of retry attempts
    max_retries <- 3
    # Difference in file size allowed
    tolerance<-0.05 # % difference 
    
    
    # Wrap the parallel processing in a with_progress call
    p <- progressr::with_progress({
      # Define the progress bar
      progress <- progressr::progressor(along = 1:nrow(file_list))
      
      # Initialize a list to store problematic files
      problem_files <- list()
      
      # Download files in parallel
      results <- future.apply::future_lapply(1:nrow(file_list), function(i) {
        #progress(sprintf("File %d/%d", i, nrow(file_list)))
        progress()
        
        # Initialize retry count
        retries <- 0
        
        repeat {
          tryCatch({
            # Check if the file needs to be downloaded
            if ((!file.exists(file_list$new_files[i])) || overwrite == TRUE) {
              # Get the size of the S3 file
              s3_file_info<- s3$file_info(file_list$file_list[i])
              s3_file_hash<-s3_file_info$etag
              s3_file_hash<-gsub("\"", "", s3_file_hash)
              s3_file_size <- convert_to_bytes(as.character(s3_file_info$size))
              # Download the file
              s3$file_download(file_list$file_list[i], file_list$new_files[i], overwrite = TRUE)
              
              # Get the size of the downloaded file
              downloaded_file_size <- file.info(file_list$new_files[i])$size
              downloaded_file_hash <- calculate_file_hash(file_list$new_files[i])
              
              diff<-100*(((1-(s3_file_size/downloaded_file_size))^2)^0.5)
              
              # Check if the file sizes match
              if (diff>tolerance|s3_file_hash!=downloaded_file_hash) {
                stop(sprintf("File size mismatch for %s: S3 size = %d, Downloaded size = %d",
                             file_list$file_list[i], s3_file_size, downloaded_file_size))
              }
              
              # This should not return an error, if it does it will trigger the tryCatch
              tail(rast(file_list$new_files[i]))
            }
            
            # If download and size check succeed, break the loop
            break
          }, error = function(e) {
            retries <- retries + 1
            if (retries >= max_retries) {
              # Record the problematic file and the error message
              problem_files <<- rbind(problem_files, data.frame(
                file_name = file_list$file_list[i],
                error_message = e$message,
                stringsAsFactors = FALSE
              ))
              break
            }
          })
        }
      })
      
      # Return the list of problematic files
      problem_files
    })
    
    print(p)
  
  future::plan(sequential)
  }

# Check if files can load 
files<-list.files(haz_timeseries_dir,".tif",full.names = T)

# Function to check if a file can be loaded by terra::rast
load_rast <- function(file) {
  tryCatch({
  suppressWarnings(tail(rast(file)) ) # Suppress warnings while loading the file
    TRUE                          # If successful, return TRUE
  }, error = function(e) {
    FALSE                         # If there's an error, return FALSE
  })
}

# Apply the load_rast function to each file
# Set up progress handling and parallel plan
handlers("progress")
plan(multisession,workers=worker_n)  # Adjust to your system: multisession, multicore, or cluster

# Define the file list
files <- list.files(path = "your_directory", pattern = "*.tif", full.names = TRUE)

# Use progressr and future.apply to process in parallel with a progress bar
with_progress({
  p <- progressor(along = files)  # Initialize progress for each file
  
  # Load each raster file in parallel
  results <- future_sapply(files, function(file) {
    p()  # Update progress bar
    terra::rast(file)  # Load raster using terra package
  })
})

(bad_files<-files[!load_results])

# If you finding files will not open delete them then run the download process again
if(length(bad_files)>0){
  unlink(bad_files)
  stop("Bad downloads, these have been deleted please run through the download section again")
}

# 0.4) Summarize data availability #####
file_summary<-basename(files)
file_summary<-gsub("historical","historic_historic_1995_2014",file_summary)
file_summary<-gsub("max_max","max-max",file_summary)
file_summary<-gsub("mean_max","mean-max",file_summary)
file_summary<-gsub("max_mean","max-mean",file_summary)
file_summary<-gsub("mean_mean","mean-mean",file_summary)

file_summary<-as.data.table(t(as.data.table(strsplit(file_summary,"_"))))
colnames(file_summary)<-c("scenario","timeframe","y1","y2","variable","stat")
file_summary[,timeframe:=paste0(y1,"-",y2)][,c("y1","y2"):=NULL][,stat:=gsub(".tif","",stat)]

dcast(file_summary,formula = scenario+timeframe~variable,fun.aggregate = length)

# 1) Classify time series climate variables based on hazard thresholds ####

# Create a table of unique thresholds
Thresholds_U<-unique(haz_class[description!="No significant stress",list(index_name,code2,direction,threshold)][,index_name:=gsub("NTxM|NTxS|NTxE","NTx",index_name)])
Thresholds_U[,code:=paste0(direction,threshold)
][,code:=gsub("<","L",code)
][,code:=gsub(">","G",code)]

files<-list.files(haz_timeseries_dir,".tif",full.names = T)
# Limite to ensemble or historical data (for individual models we would need to customize the layer name functions of some sections)
# files<-grep("ENSEMBLEmean|historical",files,value=T)

overwrite<-F
n<-0

registerDoFuture()
if (worker_n == 1) {
  future::plan("sequential")
} else {
  future::plan("multisession", workers = worker_n)
}

# Enable progressr
progressr::handlers(global = TRUE)
progressr::handlers("progress")

# Wrap the parallel processing in a with_progress call
p<-with_progress({
  # Define the progress bar
  progress <- progressr::progressor(along = 1:nrow(Thresholds_U))
  
  foreach(i = 1:nrow(Thresholds_U), .packages = c("terra", "progressr")) %dopar% {
    
  #for(i in 1:nrow(Thresholds_U)){
    index_name<-Thresholds_U[i,code2]
    files_ss<-grep(index_name,files,value=T)
    progress(sprintf("Threshold %d/%d", i, nrow(Thresholds_U)))
    
    for(j in 1:length(files_ss)){
    # cat(i,"-",j,"\n")
  
      file<-gsub(".tif",paste0("-",Thresholds_U[i,code],".tif"),file.path(haz_time_class_dir,"/",tail(tstrsplit(files_ss[j],"/"),1)),fixed = T)
      
      if((!file.exists(file))|overwrite){
        data<-terra::rast(files_ss[j])
        data_class<-rast_class(data=data,
                               direction = Thresholds_U[i,direction],
                               threshold = Thresholds_U[i,threshold],
                               minval=-999999,
                               maxval=999999)
        terra::writeRaster(data_class,filename = file,overwrite=T)
      }
      
      # Display progress
    }

    
  }
})

plan(sequential)

# 2) Calculate risk across classified time series ####

files<-list.files(haz_time_class_dir,full.names = T)

# Limit to ensemble or historical data
#files<-grep("ENSEMBLE|historical",files,value=T)

overwrite<-F

registerDoFuture()
if (worker_n == 1) {
  future::plan("sequential")
} else {
  future::plan("multisession", workers = worker_n)
}

# Enable progressr
progressr::handlers(global = TRUE)
progressr::handlers("progress")

# Wrap the parallel processing in a with_progress call
p<-with_progress({
  # Define the progress bar
  progress <- progressr::progressor(along = 1:length(files))
    
  foreach(i = 1:length(files)) %dopar% {
    #for(i in 1:length(files)){
    file<-file.path(haz_time_risk_dir,basename(files[i]))
    
    if((!file.exists(file))|overwrite){
      data<-terra::rast(files[i])
      data<-terra::app(data,fun="mean",na.rm=T)
      terra::writeRaster(data,filename = file,overwrite=T)
    }
    
    # Display progress
    progress(sprintf("File %d/%d", i, length(files)))
  }

})
plan(sequential)

# 3) Create crop risk stacks####

# Create stacks of hazard x crop/animal x scenario x timeframe
haz_class_files<-list.files(haz_time_class_dir,".tif$")

# Subset to ensemble or historical
# Note if you want to use models with this process then we will have to adjust the layer naming to accommodate a model name, suggest joining with the scenario with a non - or _ delimiter.
haz_class_files<-grep("ENSEMBLE|historical",haz_class_files,value=T)
haz_class_files2<-gsub("_max_max","max",haz_class_files)
haz_class_files2<-gsub("_mean_mean","mean",haz_class_files2)
haz_class_files2<-gsub("_max","",haz_class_files2)
haz_class_files2<-gsub("_mean","",haz_class_files2)
haz_class_files2<-gsub("_sum","",haz_class_files2)
haz_class_files2<-gsub("max","_max",haz_class_files2)
haz_class_files2<-gsub("mean","_mean",haz_class_files2)

haz_class_scenarios<-gsub("historical_","historic-historic-",haz_class_files2)
haz_class_scenarios<-gsub("ssp245_ENSEMBLE_mean_","ssp245-",haz_class_scenarios)
haz_class_scenarios<-gsub("ssp585_ENSEMBLE_mean_","ssp585-",haz_class_scenarios)
haz_class_scenarios<-gsub("2021_2040_","2021_2040-",haz_class_scenarios)
haz_class_scenarios<-gsub("2041_2060_","2041_2060-",haz_class_scenarios)

# Step 1: Split the elements
split_elements <- strsplit(haz_class_scenarios, "-")

# Step 2: Extract the first and second parts
list1 <- sapply(split_elements, `[`, 1)
list2 <- sapply(split_elements, `[`, 2)

# Step 3: Paste corresponding elements
haz_class_scenarios <- mapply(paste, list1, list2, MoreArgs = list(sep = "-"))

# Check names are unique
sum(table(haz_class_files2)>1)

overwrite<-T
crops<-haz_class[,unique(crop)]

registerDoFuture()
if (worker_n == 1) {
  future::plan("sequential")
} else {
  future::plan("multisession", workers = worker_n)
}

# Enable progressr
progressr::handlers(global = TRUE)
progressr::handlers("progress")

# Wrap the parallel processing in a with_progress call
p<-with_progress({
  # Define the progress bar
  progress <- progressr::progressor(along = 1:length(crops))
  
  foreach(i = 1:length(crops), .packages = c("terra", "data.table")) %dopar% {
    # for(i in 1:length(crops)){
    crop_focus<-crops[i]
    
    for(j in 1:nrow(severity_classes)){
      
      severity_class<-severity_classes[j,class]
      
      save_name<-file.path(haz_risk_dir,paste0(crop_focus,"-",tolower(severity_class),".tif"))
      
      # Display progress
      cat('\r                                                                                                                     ')
      cat('\r',"Crop:",i,crop_focus,"| severity:",j,severity_class)
      flush.console()
      
      if(!file.exists(save_name)|overwrite==T){
        
        haz_class_crop<-haz_class[crop==crop_focus & description == severity_class]
        
        files<-lapply(haz_class_crop$filename,FUN=function(file){
          grep(file,haz_class_files2)
          })
        
        layer_names<-unlist(lapply(1:length(files),FUN=function(k){
          scen_name<-haz_class_scenarios[files[[k]]]
          h_name<-haz_class_crop[k,haz_filename]
          paste0(scen_name,"-",h_name)
        }))
        
        files<-haz_class_files[unlist(files)]
        
        if(any(table(layer_names))>1){
          stop("Non-unique layer names are present!")
        }

        data<-terra::rast(file.path(haz_time_risk_dir,files))
        names(data)<-layer_names
        
        terra::writeRaster(data,file=save_name,overwrite=T)
      }
      
    }
    
    # Display progress
    progress(sprintf("Crop %d/%d", i, length(crops)))
    
  }

})

plan(sequential)

# 4) Calculate mean and sd across time series ####

# List timeseries hazard files
files<-list.files(haz_timeseries_dir,".tif",full.names = T)

# Remove annual sd estimates
files<-files[!grepl("ENSEMBLEsd",files)]

# Limit to ensemble or historical data
#files<-grep("ENSEMBLE|historical",files,value=T)

overwrite<-F

registerDoFuture()
if (worker_n == 1) {
  future::plan("sequential")
} else {
  future::plan("multisession", workers = worker_n)
}


# Enable progressr
progressr::handlers(global = TRUE)
progressr::handlers("progress")

# Wrap the parallel processing in a with_progress call
p<-with_progress({
  # Define the progress bar
  progress <- progressr::progressor(along = 1:length(files))
    
  foreach(i = 1:length(files)) %dopar% {
    
    file<-paste0(haz_mean_dir,"/",basename(files[i]))
    file2<-paste0(haz_sd_dir,"/",basename(files[i]))
    
    if((!file.exists(file))|overwrite){
      data<-terra::rast(files[i])
      data<-terra::app(data,fun="mean",na.rm=T)
      terra::writeRaster(data,filename = file,overwrite=T,filetype = 'COG',gdal=c("COMPRESS=LZW",of="COG"))
      
      data<-terra::app(data,fun="sd",na.rm=T)
      terra::writeRaster(data,filename = file2,overwrite=T,filetype = 'COG',gdal=c("COMPRESS=LZW",of="COG"))
    }
    # Display progress
    progress(sprintf("File %d/%d", i, length(files)))
  }

})

plan(sequential)

  # 4.1) Calculate change in mean values #####
if(F){
  files<-list.files(haz_mean_dir,".tif",full.names = T)
  files<-files[!grepl("change",files)]
  files_hist<-grep("historic",files,value = T)
  files_fut<-files[!files %in% files_hist]
  
  change_file<-file.path(haz_mean_dir,"change.tif")
  
  if(!file.exists(change_file)|overwrite==T){
    
     change<-pblapply(1:length(files_hist),FUN=function(i){
     # Display progress
      sprintf("File %d/%d", i, length(file_hist))
      
      file_hist<-files_hist[i]
      var<-gsub("historical_","",tail(tstrsplit(file_hist,"/"),1))
      files_fut_ss<-grep(var,files_fut,value=T)
      future<-terra::rast(files_fut_ss)
      past<-terra::rast(file_hist)
      
      change<-future-past
      names(change)<-gsub(".tif","",basename(files_fut_ss))
      return(change)
      })
   
   change<-terra::rast(change)
   
   terra::writeRaster(change,filename=change_file)
   
  }
}


# 5) Interactions ####
  # 5.1) Set Interaction Variables ####
  # Crops
  # Set variables that can be interacted for heat wet and dry
  crop_heat<-c("NTx35","TAVG_G","NTxS")
  crop_wet<-c("NDWL0","PTOT_G")
  crop_dry<-c("PTOT_L","NDWS")
  
  crop_choices2<-crop_choices[!grepl("_tropical|_highland|generic",crop_choices)]
  
  # Create a unique list of all the 3-way combinations required for the crops and severity classes selected
  # Function to replace exact matches
  replace_exact_matches <- function(strings, old_values, new_values) {
    replacement_map <- setNames(new_values, old_values)
    return(replacement_map[strings])
  }
  
  combinations_c<-unique(rbindlist(lapply(1:length(crop_choices2),FUN=function(i){
    crop_focus<-crop_choices2[i]
    rbindlist(lapply(1:length(severity_classes$class),FUN=function(j){
      severity_focus<-severity_classes$class[j]
      X<-data.table(expand.grid(heat_simple=crop_heat,wet_simple=crop_wet,dry_simple=crop_dry,stringsAsFactors=F))
      haz_rename<-haz_class[crop==crop_focus & description==severity_focus,
                            list(old=index_name2,new=gsub(".tif","",filename))]

      replace_exact_matches(X$heat,haz_rename$old, haz_rename$new)

      X[,heat:=replace_exact_matches(heat_simple,old_values=haz_rename$old,new_values = haz_rename$new)
        ][,dry:=replace_exact_matches(dry_simple,old_values=haz_rename$old,new_values = haz_rename$new)
          ][,wet:=replace_exact_matches(wet_simple,old_values=haz_rename$old,new_values = haz_rename$new)
            ][,severity_class:=severity_focus
              ][,crop:=crop_focus]
      X
    }))
  })))
  
  # Interactions - Animals
  # Set variables that can be interacted for heat wet and dry
  animal_heat<-c("THI_max") # THI_mean or THI_max can be used here (or both)
  animal_wet<-c("NDWL0","PTOT_G")
  animal_dry<-c("PTOT_L","NDWS")
  
  livestock_choices<-crop_choices[grepl("_tropical|_highland",crop_choices)]
  
  # Create a unique list of all the 3-way combinations required for the crops and severity classes selected
  combinations_a<-unique(rbindlist(lapply(1:length(livestock_choices),FUN=function(i){
    crop_focus<-livestock_choices[i]
    rbindlist(lapply(1:length(severity_classes$class),FUN=function(j){
      severity_focus<-severity_classes$class[j]
      X<-data.table(expand.grid(heat_simple=animal_heat,wet_simple=animal_wet,dry_simple=animal_dry,stringsAsFactors=F))
      haz_rename<-haz_class[crop==crop_focus & description==severity_focus,
                            list(old=index_name2,new=gsub(".tif","",filename))]
      
      X[,heat:=replace_exact_matches(heat_simple,old_values=haz_rename$old,new_values = haz_rename$new)
      ][,dry:=replace_exact_matches(dry_simple,old_values=haz_rename$old,new_values = haz_rename$new)
      ][,wet:=replace_exact_matches(wet_simple,old_values=haz_rename$old,new_values = haz_rename$new)
      ][,severity_class:=severity_focus
      ][,crop:=crop_focus]
      X
    }))
  })))
  
  # Join livestock and crop combinations
  combinations<-unique(rbind(combinations_c,combinations_a)[,crop:=NULL])
  
  # Restructure names of classified hazard files so they can be easily searched for scenario x timeframe x hazard x threshold
  haz_class_files<-list.files(haz_time_class_dir,full.names = T)
  haz_class_files<-grep("ENSEMBLE|historical",haz_class_files,value=T)
  
  haz_class_files2<-basename(haz_class_files)
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
  
  # 5.2) Calculate interactions ####
  combinations[,code:=paste(c(dry,heat,wet),collapse="+"),by=list(dry,heat,wet)]
  combinations<-combinations[order(code)]
  
  overwrite<-F
  
  registerDoFuture()
  # Estimate the RAM available therefore the number of workers
  workers_int<- floor(free_RAM()*0.6/10^6/5)
  if(workers_int>worker_n){workers_int<-worker_n}
  if (workers_int == 1) {
    future::plan("sequential")
  } else {
    future::plan("multisession", workers = workers_int)
  }
  
  # Enable progressr
  progressr::handlers(global = TRUE)
  progressr::handlers("progress")
  
  # Wrap the parallel processing in a with_progress call
  p<-with_progress({
    # Define the progress bar
    progress <- progressr::progressor(along = 1:nrow(combinations))
  
    foreach(i = 1:nrow(combinations), .packages = c("terra", "data.table", "progressr")) %dopar% {
   # for(i in 1:nrow(combinations)){
    
    # Display progress
    progress(sprintf("Combination %d/%d", i, nrow(combinations)))
  
          combos<-unlist(combinations[i,list(dry,heat,wet)])
          grep_vals<-paste0(paste0(combos,".tif"),collapse = "|")
          
          combo_names<-c(names(combos),
                         apply(combn(names(combos),2),2,paste,collapse="+"),
                         paste(names(combos),collapse="+"))
          
          combo_binary<-data.table(combo_name=combo_names,value=0)[grep(names(combos)[1],combo_name),value:=1
                                                                     ][grep(names(combos)[2],combo_name),value:=value+10
                                                                       ][grep(names(combos)[3],combo_name),value:=value+100]
            
          folder<-paste0(haz_time_int_dir,"/",paste0(combos,collapse = "+"))
          
          if(!dir.exists(folder)){
            dir.create(folder)
          }
  
         for(l in 1:nrow(Scenarios)){
           
           # Display progress
           cat('\r                                                                                                                     ')
           cat('\r',paste("Combination:",i,"/",nrow(combinations),"| Scenario:",l,"/",nrow(Scenarios)))
           flush.console()
           
           
           combo_binary[,lyr_names:=paste0(Scenarios[l,combined],"-",combo_name)
                        ][,save_names:=file.path(folder,paste0(lyr_names,".tif"))]
           save_names<-combo_binary$save_names
           save_name_any<-file.path(folder,paste0(Scenarios[l,combined],"-any.tif"))
  
           if(!all(file.exists(save_names))|!file.exists(save_name_any)|overwrite==T){
            
            files<-sapply(combos,FUN=function(x){
              haz_class_files[grepl(paste0(x,".tif"),haz_class_files2) & 
                                grepl(Scenarios[l,combined],haz_class_files2)]
              })
            
            if(length(unlist(files))!=3){
              stop("Issue with classified hazard files, 3 files not found.")
            }
            
            haz<-lapply(files,rast)
            names(haz)<-names(files)
            
            # Multiply risk probability to create a binary value when summed
            haz[["heat"]]<-haz[["heat"]]*10
            haz[["wet"]]<-haz[["wet"]]*100
            
            haz_sum<-terra::rast(lapply(1:nlyr(haz[[1]]),FUN=function(m){
              sum(terra::rast(lapply(haz,"[[",m)),na.rm=T)
            }))
            names(haz_sum)<-names(haz[[1]])

            # Any haz
            if(!file.exists(save_name_any)|overwrite==T){
              data<-terra::mask(haz_sum,haz_sum,maskvalues=1:111,updatevalue=1)
              data<-terra::app(data,fun="mean",na.rm=T)
              names(data)<-paste0(Scenarios[l,combined],"-any")
              terra::writeRaster(data,filename =  save_name_any,overwrite=T)
            }
            
            # Interactions
            for(a in 1:nrow(combo_binary)){
              if(combo_binary[a,!file.exists(save_names)]|overwrite==T){
                data<-int_risk(data=haz_sum,interaction_mask_vals = combo_binary[-a,value],lyr_name = combo_binary[a,lyr_names])
                terra::writeRaster(data,filename = combo_binary[a,save_names],overwrite=T)
              }
            }
           
            rm(data,haz,haz_sum)
            gc()
            }
            }
  }
  })
    
  plan(sequential)
  
 n_combos<-combinations[,code:=paste(c(dry,heat,wet),collapse="+"),by=list(heat,wet,dry)][,unique(code)]
 n_missing<-n_combos[!n_combos %in% list.dirs(haz_time_int_dir,recursive = F,full.names=F)]
  
  if(length(n_missing)>0){
    stop("Analysis of interactions incomplete")
    print(n_missing)
  }
 
 n_length<-sapply(list.dirs(haz_time_int_dir,recursive = F,full.names=T),FUN=function(DIR){length(list.files(DIR))},USE.NAMES = T)
 
 if(length(unique(n_length))>1|unique(n_length)!=40){
   stop("Missing files")
   print(n_length[n_length!=40])
 }
 
  # 5.3) Per crop combine hazards into a single file #####
 overwrite<-T
  combinations_ca<-rbind(combinations_c,combinations_a)[,combo_name:=paste0(c(dry,heat,wet),collapse="+"),by=list(dry,heat,wet,crop,severity_class)
                                                        ][,folder:=paste0(haz_time_int_dir,"/",combo_name)
                                                          ][,severity_class:=tolower(severity_class)]
  
  combinations_ca[,heat1:=stringi::stri_replace_all_regex(heat,pattern=haz_meta[,gsub("_","-",code)],replacement=haz_meta[,paste0(code,"-")],vectorise_all = F)][,heat1:=unlist(tstrsplit(heat1,"-",keep=1))]
  combinations_ca[,dry1:=stringi::stri_replace_all_regex(dry,pattern=haz_meta[,gsub("_","-",code)],replacement=haz_meta[,paste0(code,"-")],vectorise_all = F)][,dry1:=unlist(tstrsplit(dry1,"-",keep=1))]
  combinations_ca[,wet1:=stringi::stri_replace_all_regex(wet,pattern=haz_meta[,gsub("_","-",code)],replacement=haz_meta[,paste0(code,"-")],vectorise_all = F)][,wet1:=unlist(tstrsplit(wet1,"-",keep=1))]
  combinations_ca[,combo_name1:=paste0(c(dry1[1],heat1[1],wet1[1]),collapse="+"),by=list(dry1,heat1,wet1)]
  combinations_ca[,combo_name_simple:=paste0(c(dry_simple[1],heat_simple[1],wet_simple[1]),collapse="+"),by=list(dry_simple,heat_simple,wet_simple)]
  
  combinations_crops<-combinations_ca[,unique(crop)]
  
  doFuture::registerDoFuture()
  if (worker_n == 1) {
    future::plan("sequential")
  } else {
    future::plan("multisession", workers = worker_n)
  }
  
  
  # Enable progressr
  progressr::handlers(global = TRUE)
  progressr::handlers("progress")
  
  # Wrap the parallel processing in a with_progress call
  p<-with_progress({
    # Define the progress bar
    progress <- progressr::progressor(along = 1:length(combinations_crops))
    
    foreach(i = 1:length(combinations_crops), .packages = c("terra", "data.table", "progressr")) %dopar% {
    #  for(i in 1:length(combinations_crops)){
      
      for(j in 1:nrow(severity_classes)){
      
      # Display progress
      #cat('\r                                                                                                                                                           ')
      #cat('\r',paste("crop_choices:",i,"/",length(combinations_crops),"| Severity Class:",j,"/",nrow(severity_classes)))
      #flush.console()
      
      save_file<-paste0(haz_risk_dir,"/",combinations_crops[i],"-",tolower(severity_classes$class[j]),"-int.tif")

      if(!file.exists(save_file)|overwrite==T){
        subset<-combinations_ca[crop==combinations_crops[i] & severity_class==tolower(severity_classes$class[j])]
        
        data<-terra::rast(lapply(1:nrow(subset),FUN=function(k){
          files<-list.files(subset[k,folder],full.names = T)
          data<-terra::rast(files)
          names(data)<-paste0(names(data),"-",subset[k,combo_name_simple],"-",combinations_crops[i],"-",subset[k,severity_class])
          data
        }))
        
        x<-table(names(data))
        x<-x[x>1]
        
        if(length(x)>0){
        stop(paste("i = ",i,"| j = ",j,"Duplicate layers present."))
        }
        
        terra::writeRaster(data,filename = save_file,overwrite=T)
      }
      }
      
      progress(sprintf("Crop %d/%d", i, length(combinations_crops)))
      
    }
    
  })

  plan(sequential)

  