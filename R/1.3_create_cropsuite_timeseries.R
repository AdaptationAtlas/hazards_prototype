# Please run 0_server_setup.R before executing this script
# Note this only runs from CGlabs server as cropSuite is not publicly available yet

# 1) Load R functions & packages ####
packages <- c("terra","data.table","future","doFuture","sf","geoarrow","arrow")
p_load(char=packages)

# 2) Set-up workspace ####
# 2.1) Load admin boundaries #####
Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
  file<-geo_files_local[i]
  data<-arrow::open_dataset(file)
  data <- data |> sf::st_as_sf() |> terra::vect()
  data
})
names(Geographies)<-names(geo_files_local)

# 2.2) Create file index #####
cropsuite_dirs<-list_bottom_directories(cropsuite_raw_dir)
cropsuite_dirs<-cropsuite_dirs[!grepl("ipynb_checkpoints",cropsuite_dirs)]

file_index<-data.table(file_path=list.files(cropsuite_raw_dir,".tif$",full.names=T,recursive = T))
file_index[,file_name:=basename(file_path)
           ][,scenario:=unlist(tstrsplit(file_name,"_",keep=1))
             ][,timeframe:=unlist(tstrsplit(file_name,"_",keep=2))
               ][,crop:=unlist(tstrsplit(file_name,"_",keep=3))
                 ][,variable:=gsub(".tif","",unlist(tstrsplit(file_name,"_",keep=4)))]

fwrite(file_index,file.path(cropsuite_raw_dir,"file_index.csv"))

# map cropsuite to MapSPAM names
spamcodes<-fread("ms_codes_url")

# 2.3) Extract suitability data
thresholds<-data.table(from=c(0,25,50,75),to=c(25,50,75,100),becomes=c(0,1,2,3),name=c("unsuitable","low","med","high"))
cats<-thresholds[,list(becomes,name)]
setnames(cats,"becomes","value")

thresholds_change<-data.table(expand.grid(historical=thresholds$becomes,future=thresholds$becomes*10))
cats2<-thresholds[,list(becomes,name)]
setnames(cats2,c("becomes","name"),c("historical","name_hist"))
thresholds_change<-merge(thresholds_change,cats2,all.x=T)

cats2<-thresholds[,list(becomes,name)][,becomes:=becomes*10]
setnames(cats2,c("becomes","name"),c("future","name_fut"))
thresholds_change<-merge(thresholds_change,cats2,by="future")
thresholds_change[,name:=paste0(name_hist,"->",name_fut)][,value:=future+historical]

diff_levels<-thresholds_change[,list(value,name)]


for(var in file_index[grep("suitability",variable),unique(variable)]){
  files<-file_index[variable==var,file_path]
  crops<-file_index[variable==var,unique(crop)]
  
  save_file_diff_class<-file.path(cropsuite_class_dir,paste0(var,"_diff_class.tif"))
  save_file_diff<-file.path(cropsuite_class_dir,paste0(var,"_diff.tif"))
  
  if(!file.exists(save_file_diff)|overwrite){

  data_diff<-terra::rast(lapply(1:length(crops),FUN=function(i){
    # Display progress
    cat('\r', strrep(' ', 150), '\r')
    cat("processing crop", i, "/", length(crops),crops[i])
    flush.console() 
    
    files_hist<-file_index[variable==var & crop==crops[i] & scenario=="historical",file_path]
    files_fut<-file_index[variable==var & crop==crops[i] & scenario!="historical",file_path]

      data_hist<-terra::rast(files_hist)
      data_fut<-terra::rast(files_fut)
      
      data_diff<-data_hist-data_fut
      names(data_diff)<-gsub(".tif","",basename(files_fut))
      data_diff
    }))
  
  terra::writeRaster(data_diff,save_file_diff,overwrite=T)
  terra::writeRaster(data_diff_class,save_file_diff_class,overwrite=T)
  
  data_diff_ex<-admin_extract_wrap(data=data_diff, 
                                    save_dir=cropsuite_class_dir, 
                                    filename=paste0(var,"_diff"), 
                                    FUN = "mean", 
                                    varname:=NA, 
                                    Geographies, 
                                    overwrite = F,
                                    modify_colnames = F)
  
  data_diff_ex<-data_diff_ex[,variable:=gsub("mean.","",variable)
               ][,scenario:=unlist(tstrsplit(variable[1],"_",keep=1)),by=variable
                 ][,timeframe:=unlist(tstrsplit(variable[1],"_",keep=2)),by=variable
                   ][,crop:=unlist(tstrsplit(variable[1],"_",keep=3)),by=variable
                     ][,variable:=gsub(".tif","",unlist(tstrsplit(variable[1],"_",keep=4))),by=variable
                       ][,value:=round(value,1)]
  
  save_file<-file.path(cropsuite_class_dir,paste0(var,"_diff","_adm_mean.parquet"))
  arrow::write_parquet(data_diff_ex,save_file)
  
  
  # Classify differences
  data_diff_class<-terra::rast(lapply(1:length(crops),FUN=function(i){
    # Display progress
    cat('\r', strrep(' ', 150), '\r')
    cat("processing crop", i, "/", length(crops),crops[i])
    flush.console() 
    
    files_hist<-file_index[variable==var & crop==crops[i] & scenario=="historical",file_path]
    files_fut<-file_index[variable==var & crop==crops[i] & scenario!="historical",file_path]
    
    data_hist<-terra::rast(files_hist)
    data_fut<-terra::rast(files_fut)
    
    data_hist_class<-classify(data_hist,thresholds[,list(from,to,becomes)])
    data_fur_class<-classify(data_fut,thresholds[,list(from,to,becomes)][,becomes:=becomes*10])
    data_change<-data_hist_class+data_fur_class
    
    names(data_diff)<-gsub(".tif","",basename(files_fut))
    data_diff
  }))
  
  # Assign category names to each layer of the classified raster
  levels(data_diff_class)<- rep(list(diff_levels), nlyr(data_diff_class))
  
  names(data_diff_class)<-gsub(".tif","",names(data_diff))
  arrow::write_parquet(data_diff_class,save_file_diff_class)
  
  
  base_cellsize<-terra::cellSize(data_diff_class,unit="km")
  
  # Historical suitable area
  suit_past_vals<-thresholds_change[name_hist %in% c("med","high"),value]
  # Future suitable area
  suit_fut_vals<-thresholds_change[name_fut %in% c("med","high"),value]
  
  
  }
  
}


filename<-""
save_file<-file.path(cropsuite_class_dir,paste0(basename(cropsuite_dirs[i]),"_suit_mean_class.parquet"))

# Calculate change #####
# Reduced suitability pixels
# Increase suitability