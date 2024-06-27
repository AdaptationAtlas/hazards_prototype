# 0) Load R functions & packages ####
packages <- c("arrow",
              "geoarrow",
              "sf",
              "terra", 
              "data.table", 
              "future",
              "future.apply",
              "exactextractr",
              "pbapply")

# Call the function to install and load packages
p_load(char=packages)

# 1) Set directories ####
data_dir<-chirts_raw_dir
monthly_dir<-file.path(data_dir,"africa_tavg_monthly_mean")
if(!dir.exists(monthly_dir)){
  dir.create(monthly_dir)
}

# 2) Set parameters ####
years_buff<-c(1995-2,2014+6)
years<-c(1995,2014)

files_tmax<-data.table(file_path=list.files(file.path(data_dir,"Tmax",years_buff[1]:years_buff[2]),full.names = T),variable="Tmax")
files_tmin<-data.table(file_path=list.files(file.path(data_dir,"Tmin",years_buff[1]:years_buff[2]),full.names = T),variable="Tmin")

file_paths<-rbind(files_tmax,files_tmin)

file_paths[,year:=as.integer(unlist(tstrsplit(basename(file_path),"[.]",keep=2)))
           ][,month:=as.integer(unlist(tstrsplit(basename(file_path),"[.]",keep=3)))
             ][,day:=as.integer(unlist(tstrsplit(basename(file_path),"[.]",keep=4)))]



times<-unique(file_paths[,.(year,month)])[,file_name:=file.path(monthly_dir,paste0("tavg-",year,"-",month,".tif"))]

# Set up parallel processing plan
n_workers<-20
future::plan(multisession, workers = n_workers)


# Function to process each time step
process_time_step <- function(i, times, file_paths, base_rast_path) {
  save_file <- times$file_name[i]
  
  if (!file.exists(save_file)) {
    base_rast<-terra::rast(base_rast_path)
    files_max <- file_paths[year == times[i]$year & month == times[i]$month & variable == "Tmax", file_path]
    files_min <- file_paths[year == times[i]$year & month == times[i]$month & variable == "Tmin", file_path]
    
    rast_tmax <- terra::rast(files_max)
    rast_tmin <- terra::rast(files_min)
    
    rast_tmax <- terra::crop(rast_tmax, base_rast)
    rast_tmin <- terra::crop(rast_tmin, base_rast)
    
    rast_tavg <- (rast_tmax + rast_tmin) / 2
    rast_tavg <- terra::mean(rast_tavg)
    rast_tavg <- terra::classify(rast_tavg, data.frame(from = -9999, to = NA))
    
    terra::writeRaster(rast_tavg, save_file)
  }
}

# Apply the function in parallel
tavg_month <- future.apply::future_lapply(1:nrow(times), FUN = function(i) {
  process_time_step(i, times, file_paths,base_rast_path=base_rast_path)
})
  
future::plan(sequential)

tavg_month<-pblapply(1:nrow(times),FUN=function(i){
  files_max<-file_paths[year==times[i]$year & month==times[i]$month & variable=="Tmax",file_path]
  files_min<-file_paths[year==times[i]$year & month==times[i]$month & variable=="Tmin",file_path]
  
  rast_tmax<-terra::rast(files_max)
  rast_tmin<-terra::rast(files_min)
  
  rast_tmax<-terra::crop(rast_tmax,base_rast)
  rast_tmin<-terra::crop(rast_tmin,base_rast)
  
  rast_tavg<-(rast_tmax+rast_tmin)/2
  
  rast_tavg<-mean(rast_tavg)
  rast_tavg<-terra::classify(rast_tavg,data.frame(from=-9999,to=NA))
  
  return(rast_tmax)
})

tavg_month

