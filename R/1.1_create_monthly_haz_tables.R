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
packages <- c("terra", 
              "data.table", 
              "doFuture",
              "future.apply",
              "exactextractr",
              "parallel")

# Call the function to install and load packages
load_and_install_packages(packages)

# 1) Set up workspace ####
# 1.0) Set cores for parallel ####
cores<-parallel::detectCores()-2
# 1.1) Set directories
# Directory where monthly timeseries data generated from https://github.com/AdaptationAtlas/hazards/tree/main is stored
hazards_dir<-"/home/jovyan/common_data/atlas_hazards/cmip6/indices"

output_dir<-"/home/jovyan/common_data/hazards_prototype/Data/hazard_timeseries_mean_month"
if(!dir.exists(output_dir)){
  dir.create(output_dir)
}

# 1.2) Set scenarios and time frames to analyse
Scenarios<-c("ssp245","ssp585")
Times<-c("2021_2040","2041_2060")

# Create combinations of scenarios and times
Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))
Scenarios[,combined:=paste0(Scenario,"-",Time)]

# Set hazards to include in analysis
hazards<-c("NTx40","NTx35","HSH_max","HSH_mean","THI_max","THI_mean","NDWS","NDWL0","PTOT","TAVG","TMAX") # NDD is not being used as it cannot be projected to future scenarios


# 1.3) List hazard folders ####
folders<-list.dirs(hazards_dir,recursive=F,full.names = T)
folders<-folders[!grepl("ENSEMBLE|ipyn|gadm0|hazard_comb|indices_seasonal",folders)]

folders<-data.table(path=folders[grepl(paste0(Scenarios$Scenario,collapse="|"),folders) & grepl(paste0(Scenarios$Time,collapse="|"),folders)])
folders[,scenario:=unlist(tstrsplit(basename(path),"_",keep=1))
        ][scenario!="historical",model:=unlist(tstrsplit(basename(path),"_",keep=2))
          ][scenario!="historical",timeframe:=paste0(unlist(tstrsplit(basename(path),"_",keep=3:4)),collapse="-"),by=path
            ][scenario=="historical",timeframe:="historical"]

# 1.4) Load admin boundaries #####
# Load and combine geoboundaries
overwrite<-F

geo_files_s3<-c(
  "https://digital-atlas.s3.amazonaws.com/boundaries/atlas-region_admin0_harmonized.gpkg",
  "https://digital-atlas.s3.amazonaws.com/boundaries/atlas-region_admin1_harmonized.gpkg",
  "https://digital-atlas.s3.amazonaws.com/boundaries/atlas-region_admin2_harmonized.gpkg")


geo_files_local<-file.path("Data/boundaries",basename(geo_files_s3))
names(geo_files_local)<-c("admin0","admin1","admin2")

Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
  file<-geo_files_local[i]
  if(!file.exists(file)|overwrite==T){
    download.file(url=geo_files_s3[i],destfile=file)
  }
  
  data<-terra::vect(file)
  names(data)<-gsub("_nam$","_name$",names(data))
  data
  
})
names(Geographies)<-names(geo_files_local)

# 2) Extract by admin ####

FUN<-"mean"
overwrite<-F

# Parallel processing did not appear to speed up process

data_ex<-rbindlist(lapply(1:nrow(folders),FUN=function(i){
  folders_ss<-paste0(folders$path[i],"/",hazards)
  
  rbindlist(lapply(1:length(folders_ss),FUN=function(j){
    
    folders_ss_focus<-folders_ss[j]
    
    h_var<-unlist(tail(tstrsplit(folders_ss_focus,"_"),1))
    if(h_var %in% c("mean","max")){
      folders_ss_focus<-gsub("_max|_mean","",folders_ss_focus)
    }
    
    filename<-paste0(basename(folders$path[i]),"_",basename(folders_ss_focus),".parquet")
    save_file<-file.path(output_dir,filename)
    
    # Progress
    cat("folder =", i,"/",nrow(folders),basename(folders$path[i])," | hazard = ",j,"/",length(folders_ss),basename(folders_ss_focus),"\n")

    if(!file.exists(save_file)|overwrite==T){

    files<-list.files(folders_ss_focus,".tif$",full.names = T)
    
    if(h_var %in% c("mean","max")){
      files<-grep(paste0("_",h_var,"-"),files,value=T)
    }
    
    rast_stack<-terra::rast(files)
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
      
      # Add and modify columns to include crop type and exposure information.
      data[, variable := gsub(paste0(FUN, "."), "", variable[1], fixed = T), by = variable]
      
      data
    }), fill = T)
    
    data_ex[,scenario:=folders$scenario[i]
            ][,model :=folders$model [i]
              ][,timeframe:=folders$timeframe[i]
                ][,year:=unlist(tstrsplit(variable,"[.]",keep=2))
                  ][,month:=unlist(tstrsplit(variable,"[.]",keep=3))
                    ][,variable:=unlist(tstrsplit(variable,"[.]",keep=1))
                      ][,list(admin0_name,admin1_name,admin2_name,scenario,timeframe,model,year,month,variable,value)]
    
    
    arrow::write_parquet(data_ex,sink=save_file)
    }else{
      data_ex<-arrow::read_parquet(save_file)
    }
    
    data_ex
    
  }))
 }))


