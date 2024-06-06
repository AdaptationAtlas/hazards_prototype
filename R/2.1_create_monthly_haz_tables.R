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

# 1) Set up workspace ####
  # 1.0) Set cores for parallel #####
  cores<-parallel::detectCores()-2
  # 1.1) Set directories #####
  output_dir<-haz_timeseries_monthly_dir

  # 1.2) Set hazards to include in analysis #####
  hazards<-c("HSH_max","TMAX","TAVG","NDWL0","NDWS","NTx35","NTx40","PTOT","THI_max") # NDD is not being used as it cannot be projected to future scenarios
  file_name<-"all_hazards.parquet"
  
  hazards<-c("TMAX","TAVG","PTOT") # NDD is not being used as it cannot be projected to future scenarios
  file_name<-"tmax_tavg_ptot_data.parquet"
  
  # 1.2) Set scenarios and time frames to analyse #####
  Scenarios<-c("ssp126","ssp245","ssp370","ssp585")
  
  if(file_name=="all_data.parquet"){
    Scenarios<-c("ssp245","ssp585")
  }
  
  Times<-c("2021_2040","2041_2060")
  
  # Create combinations of scenarios and times
  Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))
  Scenarios[,combined:=paste0(Scenario,"-",Time)]
  
    # 1.3) List hazard folders ####
  folders<-list.dirs(indices_dir,recursive=F,full.names = T)
  folders<-folders[!grepl("ENSEMBLE|ipyn|gadm0|hazard_comb|indices_seasonal",folders)]
  
  folders<-data.table(path=folders[grepl(paste0(Scenarios$Scenario,collapse="|"),folders) & grepl(paste0(Scenarios$Time,collapse="|"),folders)])
  folders[,scenario:=unlist(tstrsplit(basename(path),"_",keep=1))
          ][scenario!="historical",model:=unlist(tstrsplit(basename(path),"_",keep=2))
            ][scenario!="historical",timeframe:=paste0(unlist(tstrsplit(basename(path),"_",keep=3:4)),collapse="-"),by=path
              ][scenario=="historical",timeframe:="historical"]

  # 1.4) Load admin boundaries #####
  Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
    file<-geo_files_local[i]
    data<-arrow::open_dataset(file)
    data <- data |> sf::st_as_sf() |> terra::vect()
    data
  })
  names(Geographies)<-names(geo_files_local)
  
  # 1.5) Load hazard meta-data #####
  haz_meta<-data.table::fread(haz_meta_url, showProgress = FALSE)
  
# 2) Extract by admin ####
levels<-c(admin0="adm0",admin1="adm1",admin2="adm2")
FUN<-"mean"
overwrite<-F
update<-T

file_all<-file.path(output_dir,file_name)

if(!file.exists(file_all)|update==T){
  data_ex<-rbindlist(lapply(1:nrow(folders),FUN=function(i){
    folders_ss<-paste0(folders$path[i],"/",hazards)
    data<-lapply(1:length(folders_ss),FUN=function(j){
      
      folders_ss_focus<-folders_ss[j]
      hazard<-basename(folders_ss_focus)
      
      h_var<-unlist(tail(tstrsplit(folders_ss_focus,"_"),1))
      if(h_var %in% c("mean","max")){
        folders_ss_focus<-gsub("_max|_mean","",folders_ss_focus)
      }
      
      filename<-paste0(basename(folders$path[i]),"_",basename(folders_ss[j]),".parquet")
      save_file<-file.path(output_dir,filename)
      
      # Progress
      cat("folder =", i,"/",nrow(folders),basename(folders$path[i])," | hazard = ",j,"/",length(folders_ss),hazard,"\n")
  
      if(!file.exists(save_file)|overwrite==T){
  
      files<-list.files(folders_ss_focus,".tif$",full.names = T)
      files<-files[!grepl("AVAIL",files)]
      
      if(h_var %in% c("mean","max")){
        files<-grep(paste0("_",h_var,"-"),files,value=T)
      }
      
      rast_stack<-terra::rast(files)
      
      if(hazard=="PTOT"){
        # Reclassify -9999 to NA
        rast_stack[rast_stack <0] <- NA
      }
      
      names(rast_stack)<-gsub(".tif","",basename(files))
     
      data_ex <- admin_extract(data=rast_stack, Geographies, FUN = "mean", max_cells_in_memory = 1*10^9)
      
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
        
        data[, variable := sub(paste0(FUN, "\\."), "", variable[1]), by = variable]
        
        data
      }),use.names=T,fill=T)
      
      # Add and modify columns to include crop type and exposure information.
      
      data_ex<-data_ex[,scenario:=folders$scenario[i]
                       ][,model :=folders$model [i]
                         ][,timeframe:=folders$timeframe[i]
                           ][,year:=as.integer(unlist(tstrsplit(variable,"[.]",keep=2)))
                             ][,month:=as.integer(unlist(tstrsplit(variable,"[.]",keep=3)))
                              ][,variable:=unlist(tstrsplit(variable,"[.]",keep=1))
                                ][,list(admin0_name,admin1_name,admin2_name,scenario,timeframe,model,year,month,variable,value)
                                  ][,value:=round(value,1)]
      
      arrow::write_parquet(data_ex,sink=save_file)
      }else{
        data_ex<-arrow::read_parquet(save_file)
      }
      
      data_ex
      
    })
    data<-rbindlist(data,use.names=T)
 }),use.names=T)
  
  data_ex <- data_ex[order(admin0_name, admin1_name, admin2_name, variable, scenario, timeframe, model)]
  
  # This step is redundant and can be removed once the data is replaced (i.e. update = T, overwrite= T)
  data_ex<-data_ex[,value:=round(value,1)
                   ][!variable %in% c("AVAIL")]
  
  arrow::write_parquet(data_ex,file_all)

}else{
  data_ex<-arrow::read_parquet(file_all)
}

# 3) Summarize annually or 3 month windows ####
data_ex_ens<-data_ex[is.na(admin2_name)
                     ][,admin2_name:=NULL
                       ][,month:=as.integer(month)
                         ][,year:=as.integer(year)
                           ][!variable %in% c("AVAIL","HSH_mean","THI_mean")] 

vars<-data_ex_ens[,unique(variable)]

# Define month abbreviations
month_abbr <- c("J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D")

# Generate 3-month periods in a year
three_month_periods <- lapply(1:12, function(start) {
  end <- start + 2
  if (end <= 12) {
    return(start:end)
  } else {
    return(c(start:12, 1:(end - 12)))
  }
})

# Name the list with month abbreviations
names(three_month_periods) <- sapply(1:12, function(start) {
  end <- start + 2
  if (end <= 12) {
    return(paste(month_abbr[start:end], collapse = ""))
  } else {
    return(paste(c(month_abbr[start:12], month_abbr[1:(end - 12)]), collapse = ""))
  }
})

three_month_periods$annual<-1:12

find_consecutive_pattern <- function(seq, pattern) {
  pattern_length <- length(pattern_split)
  
  # Initialize the result vector with NA
  result <- rep(NA, length(seq))
  
  # Check for the pattern in the sequence using a sliding window approach
  for (i in 1:(length(seq) - pattern_length + 1)) {
    if (all(seq[i:(i + pattern_length - 1)] == pattern)) {
      result[i:(i + pattern_length - 1)] <- 1
    }
  }
  
  # Replace NA with a unique marker (e.g., 0)
  result[!is.na(result)]<-rep(1:(length(result[!is.na(result)])/pattern_length),each=pattern_length)

  return(result)
}

data_ex_ens <- rbindlist(pblapply(1:length(three_month_periods),FUN=function(i){
  m_period<-three_month_periods[[i]]
  data<-data_ex_ens[month %in% m_period]

  data[,seq:=find_consecutive_pattern(seq=month,pattern=m_period),by=list(admin0_name,admin1_name,model,scenario,timeframe,variable)]
  data<-data[!is.na(seq)
             ][,year:=year[1],by=list(admin0_name,admin1_name,model,scenario,timeframe,variable,seq)
               ][,seq:=NULL]
  
  data<-rbindlist(lapply(vars, function(VAR) {
    # Get the corresponding function for the variable
    func_name <- unique(haz_meta$`function`[haz_meta$variable.code == VAR])
    # Ensure the function exists and is valid
    func <- get(func_name, mode = "function", envir = parent.frame())
    # Summarize the data for the variable using the specified function
    
    data<-data[variable == VAR, .(value = func(value, na.rm = TRUE)),
               by = .(admin0_name, admin1_name, scenario, model, timeframe, year, variable)
    ][,season:=names(three_month_periods)[i]]
    
    data
    }))
  }))

# Add historical mean
data_ex_ens[,baseline_mean:=round(mean(value[scenario=="historical" & timeframe=="historical"]),1),
            by=list(admin0_name,admin1_name,variable,season)
            ][,anomaly:=value-baseline_mean]



# Check -Inf values (Annobón is a tiny island missed by CHIRTS)
unique(data_ex_ens[value==-Inf|is.infinite(value)|is.na(value)|is.null(value),list(admin0_name,admin1_name,variable)])

# Calculate ensembled statistics
data_ex_ens<-data_ex_ens[,list(mean=mean(value,na.rm=T),
                               max=max(value,na.rm=T),
                               min=min(value,na.rm=T),
                               sd=sd(value,na.rm=T),
                               mean_anomaly=mean(anomaly,na.rm=T),
                               max_anomaly=max(anomaly,na.rm=T),
                               min_anomaly=min(anomaly,na.rm=T),
                               sd_anomaly=sd(anomaly,na.rm=T)),
                         by=list(admin0_name,admin1_name,scenario,timeframe,year,variable,season)]

data_ex_ens[scenario=="historical",c("max","min","max_anomaly","min_anomaly","sd_anomaly"):=NA]

data_ex_ens[,variable:=gsub("_mean|_max","",variable)]

numeric_cols <- c("mean","max","min","sd","max_anomaly","mean_anomaly","min_anomaly","sd_anomaly")
data_ex_ens[, (numeric_cols) := lapply(.SD, round,1), .SDcols = numeric_cols]


data_ex_ens<- data_ex_ens[order(admin0_name, admin1_name, season,variable, scenario, timeframe)]

unique(data_ex_ens[mean<0 & admin1_name!="Annobón",list(admin0_name,admin1_name,scenario,timeframe,variable,mean)])
data_ex_ens[is.infinite(mean) & admin1_name!="Annobón"]

arrow::write_parquet(data_ex_ens[,!c("sd","sd_anomaly")],file.path(output_dir,gsub(".parquet","_ensembled.parquet",file_name)))



