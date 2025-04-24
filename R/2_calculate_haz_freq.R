# Please run 0_server_setup.R before executing this script
cat("Starting 2_calculate_haz_freq.R script/n")
# 0) Set-up workspace ####
  ## 0.1) Load R functions & packages ####
  
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
                "xml2",
                "Rcpp")
  
  pacman::p_load(packages,character.only=T)
  
  # Source functions from github
  source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))
  
  # Create C++ function for calculating sd and compile it
  Rcpp::cppFunction('
double fast_sd(NumericVector x) {
  int n = x.size();
  int n_valid = 0;
  double sum = 0, sumsq = 0;

  for (int i = 0; i < n; ++i) {
    if (!NumericVector::is_na(x[i])) {
      n_valid++;
      sum += x[i];
      sumsq += x[i] * x[i];
    }
  }

  if (n_valid <= 1) return NA_REAL;

  double mean = sum / n_valid;
  return sqrt((sumsq - n_valid * mean * mean) / (n_valid - 1));
}
')
  
  ## 0.2) Set up workspace #####
    ### 0.2.1) Set number of workers ######
    worker_n<-parallel::detectCores()-1
    cat("System workers -1 = ",worker_n,"\n")
    worker_n<-15
    cat("worker_n = ",worker_n,"\n")
    
    # For section 5 - interactions this can be RAM heavy so reduce number of workers
    workers_int<- worker_n/2 
    cat("worker_n (interactions) = ",worker_n,"\n")
    
    ### 0.2.2) Set parameters ######
     #### 0.2.2.1) Set scenarios,time frames & crops/livestock ####
      Scenarios<-c("ssp126","ssp245","ssp370","ssp585")
      Times<-c("2021_2040","2041_2060","2061_2080","2081_2100")
      
      # Create combinations of scenarios and times
      Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))
      Scenarios[,combined:=paste0(Scenario,"-",Time)]
      
      cat("Scenarios x timeframes:",Scenarios$combined,"\n")
      
      # Load spam metadata
      ms_codes<-data.table::fread(ms_codes_url, showProgress = FALSE)[,Code:=toupper(Code)]
      ms_codes<-ms_codes[compound=="no" & !is.na(Code)]
      
      # Set crops and livestock included in the analysis (default is all the commodities in the spam metadata file)
      crop_choices<-c(fread(haz_class_url, showProgress = FALSE)[,unique(crop)],ms_codes[,sort(Fullname)])
      
     #### 0.2.2.2) Set master hazards ####
  
      hazards<-c("NTx40","NTx35","HSH_max","HSH_mean","THI_max","THI_mean","NDWS","TAI","NDWL0","PTOT","TAVG") # NDD is not being used as it cannot be projected to future scenarios with delta method
      
      cat("Full hazards =",hazards,"\n")
      
      haz_meta<-data.table::fread(file.path(project_dir,"metadata","haz_metadata.csv"))
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
      
     #### 0.2.2.3) Create combinations of scenarios and hazards ####
      scenarios_x_hazards<-data.table(Scenarios,Hazard=rep(hazards,each=nrow(Scenarios)))[,Scenario:=as.character(Scenario)][,Time:=as.character(Time)]
      
     #### 0.2.2.4) Set hazard thresholds from ecocrop ######
  
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
        cat(i,"-",j," | ",crop_common,"/",crops[j],"                \r")
        
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
  
     #### 0.2.2.5) !!TO DO!! Set hazard thresholds using CCW crop climate profiles ####
  
     #### 0.2.2.6) Set hazard interactions ####
       ##### 0.2.2.6.1)  Set interactions ####
  # Crop interactions (each row is a combination of heat, wet and dry variables)
  crop_interactions<-data.table(heat_simple=c("NTx35","NTxS"),wet_simple=c("NDWL0","PTOT_G"),dry_simple=c("NDWS","PTOT_L"))
  
  # Animal interactions (each row is a combination of heat, wet and dry variables)
  animal_interactions<-data.table(heat_simple=c("THI_max","THI_max"),wet_simple=c("NDWL0","PTOT_G"),dry_simple=c("NDWS","PTOT_L"))
  
  interaction_haz<-unique(c(unlist(crop_interactions),unlist(animal_interactions)))
  
  cat("Crop interaction variables\n")
  print(crop_interactions)
  cat("Animal interaction variables\n")
  print(animal_interactions)
  
       ##### 0.2.2.6.2) Create crop/livestock specific hazard table for interactions ####
  # Crops
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
      X<-crop_interactions
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
  livestock_choices<-crop_choices[grepl("_tropical|_highland",crop_choices)]
  
  # Create a unique list of all the 3-way combinations required for the crops and severity classes selected
  combinations_a<-unique(rbindlist(lapply(1:length(livestock_choices),FUN=function(i){
    crop_focus<-livestock_choices[i]
    rbindlist(lapply(1:length(severity_classes$class),FUN=function(j){
      severity_focus<-severity_classes$class[j]
      X<-animal_interactions
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
  
  # Add code to combinations and order
  combinations[,code:=paste(c(dry,heat,wet),collapse="+"),by=list(dry,heat,wet)]
  combinations<-combinations[order(code)]
  
  
    ### 0.2.3) Set flow controls and overwrite parameters ####
    run1<-T
    overwrite1<-F
    
    run2<-T
    overwrite2<-F
    
    run3<-F
    overwrite3<-T
    
    run4<-F
    run4.1<-F
    overwrite4<-F
    
    run5.2<-F
    run5.3<-F
    overwrite5<-T
    
    cat("Control and overwrite settings:")
    cat("run1 =",run1,"overwrite =",overwrite1,
        "\nrun2 =",run2,"overwrite =",overwrite2,
        "\nrun3 = ",run3,"overwrite =",overwrite3,
        "\nrun4 =",run4,"overwrite =",overwrite4,
        "\nrun5.2 =",run5.2,"overwrite =",overwrite5,
        "\nrun5.3 = ",run5.3,"overwrite =",overwrite5)
    
    timeframes<-timeframe_choices
      

  # 0.3) Download hazard timeseries from s3 bucket (if required) ####
   # Dev Note: needs to be within timeframe loop? ####
   # Dev Note: subset to required hazards only ####
  if(!Cglabs){
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
      
      set_parallel_plan(n_cores=worker_n,use_multisession=F)
      
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
  
  
    # 0.3.1) Check if downloaded files can load ######
  files<-list.files(haz_timeseries_dir,".tif",full.names = T)
  
  result<-check_tif_integrity (dir_path=haz_timeseries_dir,
                               recursive       = TRUE,
                               pattern         = "*.tif",
                               n_workers_files    = worker_n,
                               n_workers_folders = 1,
                               use_multisession = FALSE,
                               delete_corrupt  = FALSE)
  
  
 # If you finding files will not open delete them then run the download process again
  if(sum(result$success==F)>0){
    stop("Bad downloads were present run through the download section again")
  }
  }
    
# Start timeframe loop ####
for(timeframe in timeframes){
  cat("Processing ",timeframe,"of",timeframes,"\n")
  
  haz_timeseries_dir<-file.path(indices_dir2,timeframe)
  cat("haz_timeseries_dir =",haz_timeseries_dir,"\n")
# 1) Classify time series climate variables based on hazard thresholds ####
  # Create output folder
  haz_time_class_dir <- file.path(atlas_dirs$data_dir$hazard_timeseries_class, timeframe)
  
  # Create a table of unique thresholds
  Thresholds_U<-unique(haz_class[description!="No significant stress",list(index_name,code2,direction,threshold)
                                 ][,index_name:=gsub("NTxM|NTxS|NTxE","NTx",index_name)])
  
  Thresholds_U[,direction2:=direction][,direction2:=gsub("<","L",direction2)][,direction2:=gsub(">","G",direction2)]
  
  Thresholds_U[,code:=paste0(direction2,threshold),by=.I]
  
  Thresholds_U[,index_name2:=index_name][index_name2 %in% c("PTOT","TAVG"),index_name2:=paste0(index_name2,"_",direction2)]
  
  # Subset to interaction hazards
  Thresholds_U<-Thresholds_U[grepl(paste(if(any(grepl("NTx",interaction_haz))){c("NTx",interaction_haz)}else{interaction_haz},collapse = "|"),index_name2)]
  
if(run1){
  cat(timeframe,"1) Classify time series climate variables based on hazard thresholds\n")
  
  if (!dir.exists(haz_time_class_dir)){dir.create(haz_time_class_dir, recursive = TRUE)}
  
  files<-list.files(haz_timeseries_dir,".tif",full.names = T)
  
  set_parallel_plan(n_cores=worker_n,use_multisession=F)
  
  # Enable progressr
  progressr::handlers(global = TRUE)
  progressr::handlers("progress")
  
  # Wrap the parallel processing in a with_progress call
  p<-with_progress({
    # Define the progress bar
    prog <- progressr::progressor(along = 1:nrow(Thresholds_U))
    
      invisible(future.apply::future_lapply(1:nrow(Thresholds_U),FUN=function(i){
        #invisible(lapply(1:nrow(Thresholds_U),FUN=function(i){
      index_name<-Thresholds_U[i,code2]
      files_ss<-grep(index_name,files,value=T)
      files_ss<-files_ss[!grepl("ENSEMBLEsd",files_ss)]
      prog(sprintf("Threshold %d/%d", i, nrow(Thresholds_U)))
      
      for(j in 1:length(files_ss)){
       #cat(i,"-",j,"\n")
    
        file_name<-gsub(".tif",paste0("-",Thresholds_U[i,code],".tif"),file.path(haz_time_class_dir,basename(files_ss[i])),fixed = T)
        
        # Fix issues with file naming formulation
        file_name<-gsub("_max","-max",file_name)
        file_name<-gsub("_mean","-mean",file_name)
        file_name<-gsub("_sum","-sum",file_name)

        file_name<-gsub("historical_","historic_historic_historic_",file_name)
        file_name<-gsub("2021_2040_","2021-2040_",file_name)
        file_name<-gsub("2041_2060_","2041-2060_",file_name)
        file_name<-gsub("2061_2080_","2061-2080_",file_name)
        file_name<-gsub("2081_2100_","2081-2100_",file_name)
        file_name<-gsub("ENSEMBLE_mean_","ENSEMBLEmean-",file_name)

        if((!file.exists(file_name))|overwrite1){
          data<-terra::rast(files_ss[j])
          data_class<-rast_class(data=data,
                                 direction = Thresholds_U[i,direction],
                                 threshold = Thresholds_U[i,threshold],
                                 minval=-999999,
                                 maxval=999999)
          terra::writeRaster(data_class,filename = file_name,overwrite=T, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
        }
      }
    }))
  })
  
  plan(sequential)

cat(timeframe,"1) Classify time series climate variables based on hazard thresholds - Completed\n")

  # 1.1) Check results ######
  files<-list.files(haz_time_class_dir,"tif$",full.names = T,recursive=T)
  (bad_files<-check_and_delete_bad_files(files,delete_bad=T,worker_n=worker_n))
  # If you finding files will not open delete them then run the download process again
  if(length(bad_files)>0){
    stop("Bad files were present, run through this section again")
  }


}

# 2) Calculate risk across classified time series ####
  # Create output folder
  haz_time_risk_dir <- file.path(atlas_dirs$data_dir$hazard_timeseries_risk, timeframe)
  
if(run2){
cat(timeframe,"2) Calculate risk across classified time series","\n")
  
  if (!dir.exists(haz_time_risk_dir)) dir.create(haz_time_risk_dir, recursive = TRUE)
  
files<-list.files(haz_time_class_dir,full.names = T)

set_parallel_plan(n_cores=worker_n,use_multisession=F)

# Enable progressr
progressr::handlers(global = TRUE)
progressr::handlers("progress")

# Wrap the parallel processing in a with_progress call
p<-with_progress({
  # Define the progress bar
  progress <- progressr::progressor(along = 1:length(files))
    
  foreach(i = 1:length(files)) %dopar% {
    #for(i in 1:length(files)){
    # Display progress
    progress(sprintf("File %d/%d", i, length(files)))
    
    file<-file.path(haz_time_risk_dir,basename(files[i]))
    
    if((!file.exists(file))|overwrite2){
      data<-terra::rast(files[i])
      data <- terra::mean(data, na.rm = TRUE)    
      terra::writeRaster(data,filename = file,overwrite=T, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
    }
  }

})
plan(sequential)

cat(timeframe,"2) Calculate risk across classified time series - Complete\n")

}
# 3) Create crop risk stacks####
  # Create output folder
  haz_risk_dir <- file.path(atlas_dirs$data_dir$hazard_risk, timeframe)
  
  if(run3){
    
cat(timeframe,"3) Create crop risk stacks\n")
    
if (!dir.exists(haz_risk_dir)){dir.create(haz_risk_dir, recursive = TRUE)}
    

# Create stacks of hazard x crop/animal x scenario x timeframe
haz_class_files<-list.files(haz_time_class_dir,".tif$",full.names = T)

# SEE NOTE BELOW!!!! ####
  # Note if you want to use models with this process then we will have to adjust the layer naming to accommodate a model name, suggest joining with the scenario with a non - or _ delimiter.
  # Alternatively we can make files per model or ensemble
haz_class_files_ss<-haz_class_files[!grepl("ENSEMBLEsd",haz_class_files)]

haz_class_files2<-gsub("_max_max","max",basename(haz_class_files_ss))
haz_class_files2<-gsub("_mean_mean","mean",haz_class_files2)
haz_class_files2<-gsub("_max","",haz_class_files2)
haz_class_files2<-gsub("_mean","",haz_class_files2)
haz_class_files2<-gsub("_sum","",haz_class_files2)
haz_class_files2<-gsub("max","-max",haz_class_files2)
haz_class_files2<-gsub("mean","-mean",haz_class_files2)

# Check names are unique
sum(table(haz_class_files2)>1)

haz_class_scenarios<-gsub("historical_","historic_historic_historic_",haz_class_files2)
haz_class_scenarios<-gsub("2021_2040_","2021-2040_",haz_class_scenarios)
haz_class_scenarios<-gsub("2041_2060_","2041-2060_",haz_class_scenarios)
haz_class_scenarios<-gsub("2061_2080_","2061-2080_",haz_class_scenarios)
haz_class_scenarios<-gsub("2081_2100_","2081-2100_",haz_class_scenarios)
haz_class_scenarios<-gsub("ENSEMBLE_mean_","ENSEMBLE-",haz_class_scenarios)

# Split the file elements
split_elements <- strsplit(haz_class_scenarios, "_")
haz_class_file_tab <- rbindlist(lapply(split_elements, as.list))
colnames(haz_class_file_tab)<-c("scenario","model","timeframe","hazard")
haz_class_file_tab[,hazard:=gsub("[.]tif","",hazard)][,file:=haz_class_files_ss]

models<-haz_class_file_tab[,unique(model)]

# Step 2: Extract the first and second parts
#list1 <- sapply(split_elements, `[`, 1)
#list2 <- sapply(split_elements, `[`, 2)

# Step 3: Paste corresponding elements
#haz_class_scenarios <- mapply(paste, list1, list2, MoreArgs = list(sep = "-"))

crops<-haz_class[,unique(crop)]

set_parallel_plan(n_cores=worker_n,use_multisession=F)

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
      
      for(k in 1:length(models)){
      model_k<-models[k]
      
      save_name<-file.path(haz_risk_dir,paste0(gsub("_| ","-",crop_focus),"_",model_k,"_",tolower(severity_class),".tif"))
      
      # Display progress
      cat("Crop:",i,"/",length(crops),crop_focus,"| severity:",j,"/",length(severity_classes$class),severity_class,"| model:",k,"/",length(models),model_k,"               \r")

      if(!file.exists(save_name)|overwrite3==T){
        
        haz_crop_classes<-haz_class[crop==crop_focus & description == severity_class,gsub("[.]tif","",filename)]
        files<-haz_class_file_tab[model==model_k & hazard %in% haz_crop_classes,file]
        
        if(length(files)<length(haz_crop_classes)){
          stop("Classified hazard files are missing in step 3.")
        }
        
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
        
        terra::writeRaster(data,file=save_name,overwrite=T, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
        
        }
      }
    }
    
    # Display progress
    progress(sprintf("Crop %d/%d", i, length(crops)))
    
  }

})

plan(sequential)

cat(timeframe,"3) Create crop risk stacks - Complete\n")
}
# 4) Calculate mean and sd across time series ####
if(run4){
  cat(timeframe,"4) Calculate mean and sd across time series\n")
  
  # Create output folder
  haz_mean_dir <- file.path(atlas_dirs$data_dir$haz_mean, timeframe)
  if (!dir.exists(haz_mean_dir)) dir.create(haz_mean_dir, recursive = TRUE)
  
  haz_sd_dir <- file.path(atlas_dirs$data_dir$haz_sd, timeframe)
  if (!dir.exists(haz_sd_dir)) dir.create(haz_sd_dir, recursive = TRUE)
  
# List timeseries hazard files
files<-list.files(haz_timeseries_dir,".tif",full.names = T)

# Remove annual sd estimates
files<-files[!grepl("ENSEMBLEsd",files)]

# Limit to ensemble or historical data
#files<-grep("ENSEMBLE|historical",files,value=T)

set_parallel_plan(n_cores=worker_n,use_multisession=F)

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
    
    if((!file.exists(file))|overwrite4){
      data<-terra::rast(files[i])
      data<-terra::mean(data,na.rm=T)
      terra::writeRaster(data,filename = file,overwrite=T,filetype = 'COG',gdal=c("COMPRESS=LZW",of="COG", "OVERVIEWS"="NONE"))
      
      data<-terra::app(data,fun=fast_sd,na.rm=T)
      terra::writeRaster(data,filename = file2,overwrite=T,filetype = 'COG',gdal=c("COMPRESS=LZW",of="COG", "OVERVIEWS"="NONE"))
    }
    # Display progress
    progress(sprintf("File %d/%d", i, length(files)))
  }

})

plan(sequential)

cat(timeframe,"4) Calculate mean and sd across time series - Complete\n")

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
   
   terra::writeRaster(change,filename=change_file, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
   
  }
}

}
# 5) Interactions ####
  # 5.1) List and rename classified hazard rasters ####
 
  # Restructure names of classified hazard files so they can be easily searched for scenario x timeframe x hazard x threshold
  haz_class_files<-list.files(haz_time_class_dir,full.names = T)

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

  # Dev Note: At the moment this section handles moderate, severe and extreme hazards separately,
  #           Can we upgrade this? so instead of 1/0 classification we can merge the hazard severity class to have 3/2/1/0

  # Create output folder
  haz_time_int_dir <- file.path(atlas_dirs$data_dir$hazard_timeseries_int, timeframe)
  if (!dir.exists(haz_time_int_dir)) dir.create(haz_time_int_dir, recursive = TRUE)
  
  
  if(run5.2){
    cat("5.2) Calculate interactions\n")
    
  # Estimate the RAM available therefore the number of workers
  set_parallel_plan(n_cores=workers_int,use_multisession=F)
  
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
           cat(paste("Combination:",i,"/",nrow(combinations),"| Scenario:",l,"/",nrow(Scenarios)),"                   \r")

           
           combo_binary[,lyr_names:=paste0(Scenarios[l,combined],"-",combo_name)
                        ][,save_names:=file.path(folder,paste0(lyr_names,".tif"))]
           save_names<-combo_binary$save_names
           save_name_any<-file.path(folder,paste0(Scenarios[l,combined],"-any.tif"))
  
           if(!all(file.exists(save_names))|!file.exists(save_name_any)|overwrite5==T){
            
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
            if(!file.exists(save_name_any)|overwrite5==T){
              data<-terra::mask(haz_sum,haz_sum,maskvalues=1:111,updatevalue=1)
              data<-terra::mean(data,na.rm=T)
              names(data)<-paste0(Scenarios[l,combined],"-any")
              terra::writeRaster(data,filename =  save_name_any,overwrite=T, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
            }
            
            # Interactions
            for(a in 1:nrow(combo_binary)){
              if(combo_binary[a,!file.exists(save_names)]|overwrite==T){
                data<-int_risk(data=haz_sum,interaction_mask_vals = combo_binary[-a,value],lyr_name = combo_binary[a,lyr_names])
                terra::writeRaster(data,filename = combo_binary[a,save_names],overwrite=T, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
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
 
 cat("5.2) Calculate interactions - Complete\n")
 
    # 5.2.1) Check results ######
 files<-list.files(haz_time_int_dir,"tif$",full.names = T,recursive=T)
 (bad_files<-check_and_delete_bad_files(files,delete_bad=F,worker_n=worker_n))
 # If you finding files will not open delete them then run the download process again
 if(length(bad_files)>0){
   (bad_dirs<-unique(dirname(bad_files)))
   unlink(bad_dirs,recursive=T)
   stop("Bad files were present, run through this section again")
 }
 
  }
  # 5.3) Per crop combine hazards into a single file #####
if(run5.3){
  cat("5.3) Per crop combine hazards into a single file\n")
  
  combinations_ca<-rbind(combinations_c,combinations_a)[,combo_name:=paste0(c(dry,heat,wet),collapse="+"),by=list(dry,heat,wet,crop,severity_class)
                                                        ][,folder:=paste0(haz_time_int_dir,"/",combo_name)
                                                          ][,severity_class:=tolower(severity_class)]
  
  combinations_ca[,heat1:=stringi::stri_replace_all_regex(heat,pattern=haz_meta[,gsub("_","-",code)],replacement=haz_meta[,paste0(code,"-")],vectorise_all = F)][,heat1:=unlist(tstrsplit(heat1,"-",keep=1))]
  combinations_ca[,dry1:=stringi::stri_replace_all_regex(dry,pattern=haz_meta[,gsub("_","-",code)],replacement=haz_meta[,paste0(code,"-")],vectorise_all = F)][,dry1:=unlist(tstrsplit(dry1,"-",keep=1))]
  combinations_ca[,wet1:=stringi::stri_replace_all_regex(wet,pattern=haz_meta[,gsub("_","-",code)],replacement=haz_meta[,paste0(code,"-")],vectorise_all = F)][,wet1:=unlist(tstrsplit(wet1,"-",keep=1))]
  combinations_ca[,combo_name1:=paste0(c(dry1[1],heat1[1],wet1[1]),collapse="+"),by=list(dry1,heat1,wet1)]
  combinations_ca[,combo_name_simple:=paste0(c(dry_simple[1],heat_simple[1],wet_simple[1]),collapse="+"),by=list(dry_simple,heat_simple,wet_simple)]
  
  combinations_crops<-combinations_ca[,unique(crop)]
  
  set_parallel_plan(n_cores=worker_n,use_multisession=F)
  
  # Enable progressr
  progressr::handlers(global = TRUE)
  progressr::handlers("progress")
  
  # Wrap the parallel processing in a with_progress call
  p<-with_progress({
    # Define the progress bar
    progress <- progressr::progressor(along = 1:length(combinations_crops))
    
    foreach(i = 1:length(combinations_crops), .packages = c("terra", "data.table", "progressr")) %dopar% {
      progress(sprintf("Crop %d/%d", i, length(combinations_crops)))
    #  for(i in 1:length(combinations_crops)){
      
      for(j in 1:nrow(severity_classes)){
      
      # Display progress
      #cat("crop_choices:",i,"/",length(combinations_crops),"| Severity Class:",j,"/",nrow(severity_classes),"              \r")

      save_file<-paste0(haz_risk_dir,"/",combinations_crops[i],"-",tolower(severity_classes$class[j]),"-int.tif")

      if(!file.exists(save_file)|overwrite5==T){
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
        
        terra::writeRaster(data,filename = save_file,overwrite=T, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
      }
      }
    }
  })

  plan(sequential)
  
  cat("5.3) Per crop combine hazards into a single file - Complete\n")
  
   # 5.3.1) Check results ######
  files<-list.files(haz_risk_dir,"tif$",full.names = T)
  (bad_files<-check_and_delete_bad_files(files,delete_bad=T,worker_n=worker_n))
  
  # If you finding files will not open delete them then run the download process again
  if(length(bad_files)>0){
    stop("Corrupt files were present, run through this section again")
  }
  
  
  
}
}
  