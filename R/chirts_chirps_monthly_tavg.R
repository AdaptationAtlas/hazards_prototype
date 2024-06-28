# Please run 0_server_setup.R before executing this script
# Note this desinged to work from the CGlab server where chirts data has already been downloaded.
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

  # 0.1) Load admin boundaries #####
    # This is limited to admin1 (admin2 is possible but we generate huge files that go beyond the 2gb recommended size for a parquet file, so the data would need to split into chunks)
    Geographies<-lapply(1:2,FUN=function(i){
      file<-geo_files_local[i]
      data<-arrow::open_dataset(file)
      data <- data |> sf::st_as_sf() |> terra::vect()
      data
    })
    names(Geographies)<-names(geo_files_local)[1:2]

# 1) Set directories ####
data_dir_chirts<-chirts_raw_dir
monthly_dir_chirts<-file.path(data_dir_chirts,"africa_tavg_monthly_mean")
if(!dir.exists(monthly_dir_chirts)){
  dir.create(monthly_dir_chirts)
}

data_dir_chirps<-chirps_raw_dir
monthly_dir_chirps<-file.path(data_dir_chirps,"africa_monthly_mean")
if(!dir.exists(monthly_dir_chirps)){
  dir.create(monthly_dir_chirps)
}

save_dir<-chirts_chirps_dir

# 2) Set-up workspace ####
# 2.1) Set parameters #####
round_by<-2
year_range<-(1995-2):(2014+2)
levels<-c(admin0="adm0",admin1="adm1") #,admin2="adm2")

# 2.2) List files #####
files_tmax<-data.table(file_path=list.files(file.path(data_dir_chirts,"Tmax",years_buff[1]:years_buff[2]),full.names = T),variable="tmax")
files_tmin<-data.table(file_path=list.files(file.path(data_dir_chirts,"Tmin",years_buff[1]:years_buff[2]),full.names = T),variable="tmin")
files_prec<-data.table(file_path=list.files(data_dir_chirps,"tif$",full.names = T),variable="prec")

file_paths<-rbind(files_tmax,files_tmin)

file_paths<-rbind(
  file_paths[,year:=as.integer(unlist(tstrsplit(basename(file_path),"[.]",keep=2)))
             ][,month:=as.integer(unlist(tstrsplit(basename(file_path),"[.]",keep=3)))
               ][,day:=as.integer(unlist(tstrsplit(basename(file_path),"[.]",keep=4)))],
  files_prec[,year:=as.integer(unlist(tstrsplit(basename(file_path),"[.]",keep=3)))
             ][,month:=as.integer(unlist(tstrsplit(basename(file_path),"[.]",keep=4)))
               ][,day:=as.integer(unlist(tstrsplit(basename(file_path),"[.]",keep=5)))]
  )


times<-unique(file_paths[,.(year,month)])[year %in% year_range]
times<-rbind(copy(times)[,file_name:=file.path(monthly_dir_chirts,paste0("tavg-",year,"-",month,".tif"))][,var:="tavg"],
             copy(times)[,file_name:=file.path(monthly_dir_chirps,paste0("prec-",year,"-",month,".tif"))][,var:="prec"])

# 3) Process daily data to monthly #####

# Function to process each time step
process_time_step <- function(i, times, file_paths, base_rast_path) {
  save_file <- times[i]$file_name
  var<-times[i]$var
  
  if (!file.exists(save_file)) {
    base_rast<-terra::rast(base_rast_path)
    if(var=="tavg"){
      files_max <- file_paths[year == times[i]$year & month == times[i]$month & variable == "tmax", file_path]
      files_min <- file_paths[year == times[i]$year & month == times[i]$month & variable == "tmin", file_path]
      
      rast_tmax <- terra::rast(files_max)
      rast_tmin <- terra::rast(files_min)
      
      rast_tmax <- terra::crop(rast_tmax, base_rast)
      rast_tmin <- terra::crop(rast_tmin, base_rast)
      
      rast_var <- (rast_tmax + rast_tmin) / 2
      rast_var <- terra::mean(rast_var)
      rast_var <- terra::classify(rast_var, data.frame(from = -9999, to = NA))
      
    }else{
      files <- file_paths[year == times[i]$year & month == times[i]$month & variable == "prec", file_path]
      rast_var <- terra::rast(files)
      rast_var <- terra::crop(rast_var, base_rast)
      rast_var <- sum(rast_var)
      rast_var <- terra::classify(rast_var, data.frame(from = -99999999, to = -0.0000001,becomes=NA))
    }
    
    terra::writeRaster(rast_var, save_file)
  }
}

# Set up parallel processing plan
n_workers<-20
future::plan(multisession, workers = n_workers)

# Apply the function in parallel
future.apply::future_lapply(1:nrow(times), FUN = function(i) {
  process_time_step(i, times, file_paths,base_rast_path=base_rast_path)
})
  
future::plan(sequential)

# 4) Extract by admin ####
overwrite<-F # overwrite folder level extractions
save_file<-file.path(save_dir,"chirts_chirps_monthly_1993-2016.parquet")

if(!file.exists(save_file)|overwrite){
  files<-c(list.files(monthly_dir_chirts,"tif$",full.names = T),list.files(monthly_dir_chirps,"tif$",full.names = T))
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
    data
  }),use.names=T,fill=T)
  
  data_ex<-data_ex[,variable:=gsub("mean.","",variable)
                   ][,year:=as.integer(unlist(tstrsplit(variable,"[.]",keep=2)))
                     ][,month:=as.integer(unlist(tstrsplit(variable,"[.]",keep=3)))
                       ][,variable:=unlist(tstrsplit(variable,"[.]",keep=1))
                         ][,list(admin0_name,admin1_name,year,month,variable,value)
                           ][,value:=round(value,1)
                             ][order(admin0_name,admin1_name,year,month)]
  
  arrow::write_parquet(data_ex,sink=save_file)
  }else{
    data_ex<-arrow::read_parquet(save_file)
    }

# 5) Summarize annually or 3 month windows ####
# 5.1) Create 3 month windows #####
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

# 5.2) Summarize data by season #####
data_ex_season <- rbindlist(pblapply(1:length(three_month_periods),FUN=function(i){
  m_period<-three_month_periods[[i]]
  data<-data_ex[month %in% m_period]
  
  data[,seq:=find_consecutive_pattern(seq=month,pattern=m_period),by=list(admin0_name,admin1_name,variable)]
  data<-data[!is.na(seq)|seq==0
             ][,year:=year[1],by=list(admin0_name,admin1_name,variable,seq)
               ][,seq:=NULL]
  
  # Summarize the data for the variable
  data<-data[,.(value = if(variable=="tavg"){mean(value, na.rm = TRUE)}else{sum(value,na.rm=T)}),by = .(admin0_name, admin1_name,year, variable)
             ][,season:=names(three_month_periods)[i]
               ][,value:=round(value,round_by)]
    
  data

}))

# Add historical mean
data_ex_season[,baseline_mean:=round(mean(value),round_by),
               by=list(admin0_name,admin1_name,variable,season)
][,anomaly:=round(value-baseline_mean,round_by)]

# Check -Inf values (Annobón is a tiny island missed by CHIRTS)
unique(data_ex_season[value==-Inf|is.infinite(value)|is.na(value)|is.null(value),list(admin0_name,admin1_name,variable)])

# Remove non-finite values
data_ex_season<-data_ex_season[is.finite(value)]

# 5.3) Calculate differences #####
data_ex_season_stats <- data_ex_season[,.(value_s5=round(mean(value[year %in% 1993:1997]),round_by),
                                          anomaly_s5=round(mean(anomaly[year %in% 1993:1997]),round_by),
                                          value_e5=round(mean(value[year %in% 2012:2016]),round_by),
                                          anomaly_e5=round(mean(anomaly[year %in% 2012:2016]),round_by)),
                                            by=.(admin0_name,admin1_name,variable,season)
                                            ][,value_diff_tot:=value_e5-value_s5
                                              ][,value_diff_year:=round(value_diff_tot/19,2)]

data_ex_season_stats_simple<-data_ex_season_stats[,list(admin0_name,admin1_name,season,variable,value_diff_tot,value_diff_year)]

# 5.4) Save results #####
save_file<-file.path(save_dir,"chirts_chirps_seasonal-stats_1995-2014.parquet")
arrow::write_parquet(data_ex_season_stats,sink=save_file)

save_file<-file.path(save_dir,"chirts_chirps_seasonal-stats_1995-2014_simple.parquet")
arrow::write_parquet(data_ex_season_stats_simple,sink=save_file)

# 6) Area trends ####
# 6.1) Set parameters #####
thresholds<-c(5,10,20)
start_years<-1993:1997
end_years<-2012:2016
overwrite<-F # overwrite folder level extractions
# 6.2) Name save_file #####
save_file<-file.path(save_dir,paste0("chirts_chirps_monthly_",median(start_years),"-",median(end_years),"_area-class.parquet"))
# 6.3) List month files #####
files<-rbind(
  data.table(file_path=list.files(monthly_dir_chirps,"tif$",full.names = T),var="prec"),
  data.table(file_path=list.files(monthly_dir_chirts,"tif$",full.names = T),var="tavg")
  )

files[,year:=unlist(tstrsplit(basename(file_path),"-",keep=2))
      ][,month:=gsub(".tif","",unlist(tstrsplit(basename(file_path),"-",keep=3)))]
# Pull out variable options
var_choices<-files[,unique(var)]

# 6.4) Make cell area raster #####
rast_area<-terra::cellSize(terra::rast(files$file_path[1]),unit="km")

# 6.5) Calculate change and classify ####
# Uses years and thresholds set in 6.1

combinations<-data.table(expand.grid(var=var_choices,m_period=three_month_periods),m_period_name=names(three_month_periods))

rast_area_class<-terra::rast(lapply(1:length(three_month_periods),FUN=function(i){
    var_choice<-combinations[i]$var
    m_period<-unlist(combinations[i]$m_period)
    m_period_name<-combinations[i]$m_period_name
    files_hist<-files[var==var_choice & year %in% start_years & month %in% m_period
                      ][,seq:=find_consecutive_pattern(seq=month,pattern=m_period)]
    files_fut<-files[var==var_choice & year %in% end_years & month %in% m_period
                     ][,seq:=find_consecutive_pattern(seq=month,pattern=m_period)]
    
     rast_hist<-lapply(files_hist[,unique(seq)],FUN=function(i){
       sum(terra::rast(files_hist[seq==i,file_path]))
     })
     
     rast_hist<-terra::mean(terra::rast(rast_hist))
     
     rast_fut<-lapply(files_fut[,unique(seq)],FUN=function(i){
       sum(terra::rast(files_fut[seq==i,file_path]))
     })
     
     rast_fut<-terra::mean(terra::rast(rast_fut))
    
     rast_diff<- rast_fut-rast_hist
     
     rast_class<-terra::rast(lapply(1:length(thresholds),FUN=function(j){
       cat('\r', strrep(' ', 150), '\r')
       cat("Processing combination",i,"/",nrow(combinations),"| threshold",j,"/",length(thresholds))
       flush.console()
       
       threshold<-thresholds[j]
       increase<-classify(rast_diff,data.frame(from=c(-99999,threshold),to=c(threshold,999999),becomes=c(0,1)))*rast_area
       decrease<-classify(rast_diff,data.frame(from=c(-99999,-threshold),to=c(-threshold,999999),becomes=c(1,0)))*rast_area
       change<-c(increase,decrease)
       names(change)<-paste0(var_choice,"_",m_period_name,"_",median(start_years),"v",median(end_years),"_area-",c("increase","decrease"),"_",threshold,"perc-change")
       return(change)
     }))
     # Next extract and sum area by admin
     rast_class
     }))

# 6.6) Extract data by admin areas #####
  data_ex <- admin_extract(data=rast_area_class, Geographies, FUN = "sum", max_cells_in_memory = 1*10^9)
  
  # Process the extracted data to format it for analysis or further processing.
  rast_area_class_ex<-rbindlist(lapply(1:length(levels), FUN = function(i) {
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
    data
  }),use.names=T,fill=T)
  
  rast_area_class_ex<-rast_area_class_ex[,variable:=gsub("sum.","",variable)
  ][,season:=unlist(tstrsplit(variable,"_",keep=2))
  ][,comparison:=unlist(tstrsplit(variable,"_",keep=3))
  ][,stat:=unlist(tstrsplit(variable,"_",keep=4))
  ][,stat_threshold:=unlist(tstrsplit(variable,"_",keep=5))
  ][,variable:=unlist(tstrsplit(variable,"_",keep=1))
  ][,list(admin0_name,admin1_name,variable,season,comparison,stat,stat_threshold,value)
  ][order(admin0_name,admin1_name,variable,season)]
  setnames(rast_area_class_ex,"value","area_km")
  
  # Calculate admin total areas
  data_ex <- admin_extract(data=rast_area, Geographies, FUN = "sum", max_cells_in_memory = 1*10^9)
  
  # Process the extracted data to format it for analysis or further processing.
  admin_area_ex <- rbindlist(lapply(1:length(levels), FUN = function(i) {
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
    data
  }),use.names=T,fill=T)[,variable:=NULL]
  setnames(admin_area_ex,"value","total_area_km")
  rast_area_class_ex<-merge(rast_area_class_ex,admin_area_ex,all.x=T)
  
  rast_area_class_ex[,perc:=round(100*(area_km/total_area_km),1)][,area_km:=as.integer(area_km)][,total_area_km:=as.integer(total_area_km)]
  
  arrow::write_parquet(rast_area_class_ex,sink=save_file)


