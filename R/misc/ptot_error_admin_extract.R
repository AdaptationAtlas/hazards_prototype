# d) Load and prepare admin vectors and exposure rasters, extract exposure by admin ####
## d.1) Geographies #####
Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
  file<-geo_files_local[i]
  data<-arrow::open_dataset(file)
  data <- data |> sf::st_as_sf() |> terra::vect()
  data$zone_id<-1:length(data)
  data
})
names(Geographies)<-names(geo_files_local)

base_rast<-terra::rast(base_rast_path)+0

boundaries_zonal<-lapply(1:length(Geographies),FUN=function(i){
  file_path<-file.path(boundaries_int_dir,paste0(names(Geographies)[i],"_zonal.tif"))
  if(!file.exists(file_path)){
    zones<-Geographies[[i]]
    zone_rast <- rasterize(
      x      = zones, 
      y      = base_rast, 
      field  = "zone_id", 
      background = NA,    # cells not covered by any polygon become NA
      touches    = TRUE   # optional: count cells touched by polygon boundaries
    )
    terra::writeRaster(zone_rast,file_path,overwrite=T)
  }
  file_path
})
names(boundaries_zonal)<-names(Geographies)

boundaries_index<-lapply(1:length(Geographies),FUN=function(i){
  data.frame(Geographies[[i]])[,c("iso3","admin0_name","admin1_name","admin2_name","zone_id")]
})

names(boundaries_index)<-names(Geographies)


folders<-paste0("~/common_data/affected_geographies/increase_",c(15,30,15))

for(j in 1:length(folders)){
  folder<-folders[j]
  cat(folder,"\n")
  save_file<-file.path(folder,"admin_extract.json")
    
  if(!file.exists(save_file)){
  files<-list.files(folder,full.names = T)
  
  result<-lapply(1:length(boundaries_zonal),FUN=function(i){
    extract_by<-rast(boundaries_zonal[[i]])
    result<-rbindlist(pblapply(files,FUN=function(file){
      dat<-rast(file)
      result<-zonal(dat,extract_by,fun="sum",na.rm=T)
      result$file<-gsub(".tif","",basename(file))
      result
    }))
    result
    })
  
  result<-lapply(1:length(boundaries_zonal),FUN=function(i){
    data<-result[[i]]
    ids<-boundaries_index[[i]]
    data<-merge(data,ids,by="zone_id",all.x=T)
    data$zone_id<-NULL
    data[,scenario:=tstrsplit(file,"_",keep=1)][,model:=tstrsplit(file,"_",keep=2)]
    data[,timeframe:=paste0(unlist(tstrsplit(file,"_",keep=3:4)),collapse="-"),by=.I]
    data[,file:=NULL]
    data
  })
  
  result<-lapply(1:length(boundaries_zonal),FUN=function(i){
    data<-result[[i]]
    data[, row_sum := rowSums(.SD), .SDcols = as.character(sprintf("%02d", 1:12))]
    data[row_sum>0]
  })
  
  
  names(result)<-names(Geographies)
  
  jsonlite::write_json(result,save_file,simplifyVector=T)
  }

}

# Rain
folder<-"~/common_data/hazards_prototype/Data/hazard_timeseries_mean_month/intermediate"

files<-list.files(folder,"PTOT.*parquet$",full.names=T)
files<-files[!grepl("historic",files)]
file<-files[1]
data<-rbindlist(pblapply(files,read_parquet))






