# Please run 0_server_setup.R before executing this script
# Note this script will only work on CGlabs server or if a folder containing the hazard_indices calculated in the https://github.com/AdaptationAtlas/hazards workflow are present

cat("Starting 2.1_create_monthly_haz_tables.R/n")

# 0) Load R functions & packages ####
packages <- c("arrow",
              "geoarrow",
              "sf",
              "terra", 
              "data.table", 
              "dplyr",
              "furrr",
              "progressr",
              "parallel",
              "pbapply")

# Call the function to install and load packages
p_load(char=packages)

# 1) Set up workspace ####
  ## 1.1) Set directories #####
  output_dir<-atlas_dirs$data_dir$hazard_timeseries_mean_month
  cat("output_dir =",output_dir,"\n")
  
  output_int_dir<-file.path(output_dir,"intermediate")
  if(!dir.exists(output_int_dir)){
    dir.create(output_int_dir)
  }
  
  
  ## 1.2) Set hazards to include in analysis #####
  hazards<-c("HSH_max","TMAX","TAVG","NDWL0","NDWS","NTx35","NTx40","PTOT","THI_max") # NDD is not being used as it cannot be projected to future scenarios
  cat("Working with hazards =",hazards,"\n")
  file_name<-"all_hazards.parquet"
  
  ## 1.3) Set scenarios and time frames to analyse #####
  Scenarios<-c("ssp126","ssp245","ssp370","ssp585")
  cat("Using scenarios =",Scenarios,"\n")
  
  Times<-c("2021_2040","2041_2060","2061_2080","2081_2100")
  cat("Using time periods =",Times,"\n")
  
  # Create combinations of scenarios and times
  Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))
  Scenarios[,combined:=paste0(Scenario,"-",Time)]
  
  ## 1.4) Load admin boundaries #####
  # This is limited to admin1 (admin2 is possible but we generate huge files that go beyond the 2gb recommended size for a parquet file, so the data would need to split into chunks)
  Geographies<-lapply(1:2,FUN=function(i){
    #Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
    file<-geo_files_local[i]
    data<-arrow::open_dataset(file)
    data <- data |> sf::st_as_sf() |> terra::vect()
    data$zone_id<-1:length(data)
    data
  })
  
  #names(Geographies)<-names(geo_files_local)
  names(Geographies)<-names(geo_files_local)[1:2]
  
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
  
  ## 1.5) Load hazard meta-data #####
  haz_meta<-data.table::fread(haz_meta_url, showProgress = FALSE)
  
# 1.6) Controls ####
  
  ## Section 2 - Extraction of monthly hazards by admin areas ####
  round1<-1
  version1<-1
  worker_n1<-5
  overwrite1<-F # overwrite folder level extractions

  # Data QC checks
  max_rain<-3000 # Max acceptable value for monthly rainfall 
  min_haz<--10 # Min acceptable value for any hazards (some temperatures can be negative)
  exclude_flagged<-F # Exclude combinations of admin x timeframe x scenario x model x hazard that contain any bad values?
  
  ## Section 3 - Summarization of monthly hazards ####
  worker_n2<-20
  overwrite2<-F
  round3.1<-3
  round3.3<-3
  
  ## Final data ####
  round_final<-1
  
# 2) Extract hazard folders by admin boundaries ####
  ## 2.1) List hazard folders ####
  folders<-list.dirs(indices_dir,recursive=F,full.names = T)
  folders<-folders[!grepl("ENSEMBLE|ipyn|gadm0|hazard_comb|indices_seasonal",folders)]
  folders<-folders[grepl(paste0(Scenarios$Scenario,collapse="|"),folders) & grepl(paste0(Scenarios$Time,collapse="|"),folders)]
  
  folders<-data.table(path=folders)
  folders[,scenario:=unlist(tstrsplit(basename(path),"_",keep=1))
  ][!grepl("historical",scenario),model:=unlist(tstrsplit(basename(path),"_",keep=2))
  ][!grepl("historical",scenario),timeframe:=paste0(unlist(tstrsplit(basename(path),"_",keep=3:4)),collapse="-"),by=path
  ][grepl("historical",scenario),c("timeframe","model"):=scenario
  ][,path_new:=file.path(output_dir,paste0(scenario,"_",model,"_",timeframe)),by=.I
  ][,path_new:=gsub("historical","historic",path_new)]
  
  folders<-data.frame(folders)
  
  cat("Folders included =",folders$path,"\n")
  
  ## 2.2) Set parameters ####  
  levels<-c(admin0="adm0",admin1="adm1") #,admin2="adm2")
  
  id_vars<-c("iso3","admin0_name","admin1_name","admin2_name")
  split_delim<-"_"
  split_colnames<-c("scenario","timeframe","model","hazard", "year", "month")
  extract_stat<-"mean"
  order_by<-c("iso3","admin0_name","admin1_name","admin2_name")
  order_by2<-c("admin0_name", "admin1_name", "season","hazard", "scenario", "timeframe")
  
  ## 2.3) Define the extraction function ####
  extract_hazard <- function(i, folders, hazards, output_dir, overwrite, round_dp, extract_stat,
                             boundaries_zonal, boundaries_index, id_vars, split_colnames,
                             order_by, haz_meta, version, extraction_rast, levels) {
    
    folders_ss <- paste0(folders$path[i], "/", hazards)
    
     invisible(purrr::map(hazards, function(hazard) {
      folders_ss_focus <- gsub("_max|_mean", "", paste0(folders$path[i], "/", hazard))
      h_var <- unlist(tail(tstrsplit(hazard, "_"), 1))
      
      filename <- paste0(basename(folders$path_new[i]), "_", gsub("_", "-", hazard), "_", extract_stat, ".parquet")
      save_file <- file.path(output_dir, filename)
      
      if (!file.exists(save_file) || overwrite) {
        files <- list.files(folders_ss_focus, ".tif$", full.names = TRUE)
        files <- files[!grepl("AVAIL", files)]
        if (h_var %in% c("mean", "max")) {
          files <- grep(paste0("_", h_var, "-"), files, value = TRUE)
        }
        
        if (length(files) != 0) {
          rast_data <- terra::rast(files)
          if (hazard == "PTOT") rast_data[rast_data < 0] <- NA
          
          rast_names <- data.table(base_name = gsub(".tif", "", basename(files)))
          rast_names[, c("year", "month") := tstrsplit(base_name, "-", keep = 2:3)]
          rast_names[, hazard := gsub("_", "-", unlist(tstrsplit(base_name, "-", keep = 1)))]
          rast_names[, new_name := paste(folders$scenario[i], folders$timeframe[i], folders$model[i], hazard, year, month, sep = "_")]
          
          names(rast_data) <- rast_names$new_name
          if (anyDuplicated(names(rast_data)) > 0) stop("Duplicate layer names present")
          
          result <- rbindlist(purrr::map2(boundaries_zonal, boundaries_index, function(zonal_rast, idx) {
            zonal_r <- terra::rast(zonal_rast)
            dat <- zonal(rast_data, zonal_r, fun = extract_stat, na.rm = TRUE)
            dat <- merge(dat, idx, by = "zone_id", all.x = TRUE, sort = FALSE)[, zone_id := NULL]
            dat
          }))
          
          result_long <- melt(result, id.vars = id_vars)
          if (!is.null(round_dp)) result_long[, value := round(value, round_dp)]
          result_long[, (split_colnames) := tstrsplit(variable, "_", fixed = TRUE)]
          result_long[, variable := NULL]
          if (!is.null(order_by)) result_long <- result_long[order(do.call(order, .SD)), .SDcols = order_by]
          
          arrow::write_parquet(result_long, save_file)
          write_json(list(
            source = list(input_raster = files, extraction_rast = extraction_rast),
            extraction_method = "zonal",
            geo_filters = id_vars,
            season_type = NA,
            filters = lapply(split_colnames, function(col) unique(result_long[[col]])),
            format = ".parquet",
            date_created = Sys.time(),
            version = version1,
            parent_script = "R/2.1_create_monthly_haz_tables.R - section 2",
            value_variable = unique(result_long$hazard),
            unit = haz_meta[variable.code == hazard, base_unit],
            extract_stat = extract_stat,
            notes = paste0("Monthly hazard values extracted by admin areas summarized using ", extract_stat, ".")
          ), paste0(save_file, ".json"), pretty = TRUE)
          
          rm(result,rast_stack)
          gc()
          
          return(result_long)
        }
      } 
    }))
    
  }
  
  
  ## 2.4) Run parallel extraction ####
  # Set parallel plan
  set_parallel_plan(n_cores = worker_n1, use_multisession = TRUE)
  
  # Enable progress reporting
  options(progressr.enable = TRUE)
  progressr::handlers(global = TRUE)
  progressr::handlers("progress")
  
  # Progress-wrapped parallel execution
  with_progress({
    prog <- progressor(steps = nrow(folders))
    results <- furrr::future_map(1:nrow(folders), function(i) {
      prog(sprintf("Processing folder %d of %d", i, nrow(folders)))
      extract_hazard(i, 
                     folders = folders, 
                     hazards = hazards,
                     output_dir = output_int_dir, 
                     overwrite = overwrite1, 
                     round_dp = round1,
                     extract_stat = extract_stat,
                     boundaries_zonal = boundaries_zonal, 
                     boundaries_index = boundaries_index, 
                     id_vars = id_vars, 
                     split_colnames =split_colnames,
                     order_by = order_by, 
                     haz_meta =data.frame(haz_meta),
                     version = version, 
                     extraction_rast = atlas_data$boundaries$name,
                     levels = levels)
    }, .options = furrr::furrr_options(scheduling = Inf))
  })
  plan(sequential)
  
  ## 2.5) List and combine monthly hazard parquet files ####
  files<-list.files(output_int_dir,".parquet$",full.names = T)
  files<-data.table(file=files)[,c("scenario","model","timeframe","hazard","stat"):=tstrsplit(basename(file),"_",keep=1:5)
                                ][,stat:=gsub(".parquet","",stat)]
  
  timeframes<-files[,unique(timeframe)]
  baselines<-files[grep("historic",scenario),unique(scenario)]
  futures<-files[!grepl("historic",timeframe),unique(timeframe)]
  
  problem_data<-lapply(1:length(timeframes),FUN=function(i){
    timeframe_choice<-timeframes[i]
    save_path<-file.path(output_dir,paste0("haz_monthly_adm_mean_",timeframe_choice,".parquet"))
    
    if(!file.exists(save_path)|overwrite1){
    cat("2.4) Merging data for timeframe",timeframe_choice,"\n")
      
    files_ss<-files[timeframe==timeframe_choice,file]
    
    data<-rbindlist(pblapply(files_ss,arrow::read_parquet))
    
    # Check for any weird results
    check<-data[value>max_rain | value<(min_haz)] # Highest monthly recorded rainfall in africa is <3000mm
    if(nrow(check)>0){
      warning(nrow(check)," rows of data have values >",max_rain," or <",min_haz,". These hazards x admin areas x scenario x models have ",
              if(!exclude_flagged){"not been"}else{"been"}," excluded.\n")
      problem_dat<-unique(check[,.(hazard,iso3,admin0_name,admin1_name,admin2_name,scenario,model)])
      cat("Problem data:\n")
      print(problem_dat)
      
      problem_dat[,suspect_value_flag:=T]
      data<-merge(data,problem_dat,all.x=T,sort=F)
      data[is.na(suspect_value_flag),suspect_value_flag:=F]
      if(exclude_flagged){
        data<-data[suspect_value_flag!=F]
      }
      
      }else{
        data[,suspect_value_flag:=F]
        }
    
    if (!is.null(order_by)){ 
      setorderv(data, order_by)
    }
    
    data[,year:=as.integer(year)][,month:=as.integer(month)]
    
    arrow::write_parquet(data,save_path)
    
    json_dat<-jsonlite::read_json(paste0(files_ss[1],".json"),simplifyVector=T)
    filters<-list(scenario=data_ex_season[,unique(scenario)],
                  model=data_ex_season[,unique(model)],
                  timeframe=data_ex_season[,unique(timeframe)],
                  year=data_ex_season[,unique(year)],
                  hazard=data_ex_season[,unique(hazard)],
                  month=data_ex_season[,unique(month)])
    
    jsonlite::write_json(
      list(
      source = list(input_raster = files_ss, extraction_rast = atlas_data$boundaries$name),
      extraction_method = "zonal",
      geo_filters = json_dat$geo_filters,
      season_type = NA,
      filters =  filters,
      format = ".parquet",
      date_created = Sys.time(),
      version = json_dat$version,
      parent_script = "R/2.1_create_monthly_haz_tables.R - section 2.5",
      value_variable = "hazard value",
      unit = haz_meta[variable.code %in% data[,unique(hazard)], .(variable.code,base_unit)],
      extract_stat = json_dat$extract_stat,
      notes = paste0("Monthly hazard values extracted and summarized using ", extract_stat, "."),
      problem_data = check
    ), paste0(save_path, ".json"), pretty = TRUE)
    }
    
    if(nrow(check)>0){
      return(check)
    }else{
      return(NULL)
    }
    
  })
  
  monthly_files<-file.path(output_dir,paste0("haz_monthly_adm_mean_",timeframes,".parquet"))
  
  # Check for missing values
  data<-arrow::read_parquet(monthly_files[1])
  missing<-data[value==-Inf|is.infinite(value)|is.na(value)|is.null(value),.(hazard=paste(unique(hazard),collapse=",")),by=.(admin0_name,admin1_name)]
  
  if(nrow(missing)>0){
    warning("These hazards x admin areas are missing data")
    print(missing)
  }
  
  
# 3) Summarize annually or 3 month windows ####
## 3.0) Create 3 month windows #####
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


## 3.1) Seasonal hazard calculation ####
cat("3.1) Seasonal hazard calculation \n")

id_vars <- c("admin0_name", "admin1_name", "scenario", "model", "timeframe", "year", "hazard","suspect_value_flag")

lapply(monthly_files,FUN=function(month_file){
  save_file<-gsub("_monthly_","_3months_",month_file)
  
  if(!file.exists(save_file)|overwrite2){
  cat("3.1) Seasonal summarization: ",basename(month_file), "\n")
  data_ex_ss<-arrow::read_parquet(month_file)
  vars<-data_ex_ss[,unique(vars)]

  data_ex_season <- lapply(1:length(three_month_periods), function(j) {
      
      m_period <- three_month_periods[[j]]
      dt <- copy(data_ex_ss)[month %in% m_period]
      
      dt[, seq := find_consecutive_pattern(seq = month, pattern = m_period),
         by = .(admin0_name, admin1_name, model, scenario, timeframe, hazard)]
      
      dt <- dt[!is.na(seq)]
      dt[, year := year[1], by = .(admin0_name, admin1_name, model, scenario, timeframe, hazard, seq)]
      dt[, seq := NULL]
      
      data_season <- rbindlist(lapply(vars, function(VAR) {
        func_name <- unique(haz_meta$`function`[haz_meta$variable.code == gsub("-","_",VAR)])
        func <- get(func_name, mode = "function", envir = parent.frame())
        
        dt[hazard == VAR, .(
          value = round(func(value, na.rm = TRUE), round3.1),
          n_value = .N
        ), by = id_vars][, season := season_name]
      }), use.names = TRUE, fill = TRUE)
      
      cat("Completed: ", names(three_month_periods)[j]," ",j,"/",length(three_month_periods),"      \r")
      data_season
    })

  data_ex_season<-rbindlist(data_ex_season)
  
  if (!is.null(order_by2)){ 
    setorderv(data, order_by2)
  }

  arrow::write_parquet(data_ex_season,save_file)
  
  json_dat<-jsonlite::read_json(paste0(month_file,".json"),simplifyVector=T)
  filters<-list(scenario=data_ex_season[,unique(scenario)],
                model=data_ex_season[,unique(model)],
                timeframe=data_ex_season[,unique(timeframe)],
                year=data_ex_season[,unique(year)],
                hazard=data_ex_season[,unique(hazard)],
                season=data_ex_season[,unique(season)])
  
  jsonlite::write_json(
    list(
      source = list(input_table = save_file, extraction_rast = atlas_data$boundaries$name),
      extraction_method = "zonal",
      geo_filters = grep("admin",colnames(data_ex_season),value=T),
      season_type = NA,
      filters =  filters,
      format = ".parquet",
      date_created = Sys.time(),
      version = json_dat$version,
      parent_script = "R/2.1_create_monthly_haz_tables.R - section 3.1",
      value_variable = "hazard value",
      unit = haz_meta[variable.code %in% data[,unique(hazard)], .(variable.code,base_unit)],
      extract_stat = json_dat$extract_stat,
      notes = paste0("Monthly hazard values extracted by admin areas and summarized using ", extract_stat, ". Values then combined across 3 or 12 month sequences using sum or mean depending on the hazard type."),
      problem_data = data_ex_season[suspect_value_flag==T]
    ), paste0(save_file, ".json"), pretty = TRUE)
  
  }
})

monthly3_files<-gsub("_monthly_","_3months_",monthly_files)

cat("3.1) Seasonal hazard calculation - Complete \n")

## 3.2) Add historical mean ####
cat("3.2) Adding historical means \n")

data_ex_season_hist<-rbindlist(lapply(baselines,FUN=function(baseline){
    data<-data.table(arrow::read_parquet(grep(paste0("_",baseline,"[.]"),monthly3_files,value=T)))
    data<-data[,.(value=round(mean(value,na.rm=T),round3.1)),by=.(admin0_name,admin1_name,hazard,season)]
    data[,baseline:=baseline]
    data
}))

data_ex_season_hist<-dcast(data_ex_season_hist,admin0_name+admin1_name+hazard+season~baseline)
colnames(data_ex_season_hist)<-gsub("historic","baseline",colnames(data_ex_season_hist))
colnames(data_ex_season_hist)[grepl("baseline",colnames(data_ex_season_hist))]<-paste0(colnames(data_ex_season_hist)[grepl("baseline",colnames(data_ex_season_hist))],"_mean")
baseline_cols<-colnames(data_ex_season_hist)[grepl("baseline",colnames(data_ex_season_hist))]
anomaly_cols<-gsub("baseline","anomaly",baseline_cols)

fut_monthly3_files<-monthly3_files[!grepl("historic",monthly3_files)]

data_ex_season<-pblapply(fut_monthly3_files,FUN=function(file){

  data<-data.table(arrow::read_parquet(file))
  data<-merge(data,data_ex_season_hist,all.x=T)
    
  data[, (anomaly_cols) := lapply(.SD, function(x) value-x), .SDcols = baseline_cols]
  data
})

names(data_ex_season)<-basename(fut_monthly3_files)

cat("3.2) Adding historical means  - Complete \n")

# Remove non-finite values
# data_ex_season<-data_ex_season[is.finite(value)]

## 3.3) Calculate ensembled statistics #####
### Dev note: In future iterations the baseline name creation should be automated ####
baselines
baseline_names<-c("1995-2014","AgERA5 1981-2022")

data_ex_season_ens<-pblapply(1:length(data_ex_season),FUN=function(i){
  data<-copy(data_ex_season[[i]])
  save_file<-file.path(out_dir,names(data_ex_season)[i])
  models<-data[,paste0(sort(unique(model)),collapse=",")]
  data_vals<-data[,list(mean=mean(value,na.rm=T),
                        max=max(value,na.rm=T),
                        min=min(value,na.rm=T),
                        sd=sd(value,na.rm=T)),
                  by=list(admin0_name,admin1_name,scenario,timeframe,year,hazard,season)]
  
  num_cols<-c("mean","min","max","sd")
  data_vals[, (num_cols) := lapply(.SD, round, digits = round_final), .SDcols = num_cols]
  data_vals[,hazard:=gsub("_mean|_max","",hazard)]
  
  data_anomaly<-lapply(1:length(anomaly_cols),FUN=function(j){
    anomaly_col<-anomaly_cols[j]
    save_file2<-gsub(".parquet",paste0("_",gsub("_mean","",anomaly_col),"_ensemble_seasons.parquet"),save_file)
    save_file3<-gsub(".parquet",paste0("_",gsub("_mean","",anomaly_col),"_ensemble.parquet"),save_file)
    
    if(!file.exists(save_file2)|overwrite2){
    setnames(data,anomaly_col,"target")
    data_anomaly<-data[,list(mean_anomaly=mean(target,na.rm=T),
                       max_anomaly=max(target,na.rm=T),
                       min_anomaly=min(target,na.rm=T),
                       sd_anomaly=sd(target,na.rm=T)),
                 by=list(admin0_name,admin1_name,scenario,timeframe,year,hazard,season)]
    
    data_anomaly<-data_anomaly[,!c("admin0_name","admin1_name","scenario","timeframe","year","hazard","season")]
    data_anomaly<-round(data_anomaly,round_final)
    
    data_anomaly<-cbind(data_vals,data_anomaly)
    data_anomaly[,models:=models]

    data_ag<-data[,list(mean=mean(value,na.rm=T),
                        mean_anomaly=mean(target,na.rm=T)),
                  by=list(admin0_name,admin1_name,scenario,timeframe,model,hazard,season)]
    
    data_ag_ens<-data_ag[,list(mean_mean=mean(mean,na.rm=T),
                                        min_mean=min(mean,na.rm=T),
                                        max_mean=max(mean,na.rm=T),
                                        median_mean=median(mean,na.rm=T),
                                        mean_anomaly=mean(mean_anomaly,na.rm=T),
                                        max_anomaly=max(mean_anomaly,na.rm=T),
                                        min_anomaly=min(mean_anomaly,na.rm=T),
                                        sd_anomaly=sd(mean_anomaly,na.rm=T)),
                                  by=list(admin0_name,admin1_name,scenario,timeframe,hazard,season)]
    data_ag_ens[,models:=models]
    
    num_cols <- names(data_ag_ens)[sapply(data_ag_ens, is.numeric)]
    data_ag_ens[, (num_cols) := lapply(.SD, round, digits = round_final), .SDcols = num_cols]
    

    if (!is.null(order_by2)){ 
      setorderv(data_anomaly, order_by2)
      setorderv(data_ag_ens, order_by2)
    }
    
    arrow::write_parquet(data_anomaly,save_file2)
    
    data_json<-jsonlite::read_json(file.path(output_dir,paste0(save_file,".json")),simplifyVector=T)
    
    files<-data_json
    
    filters<-list(scenario=data_anomaly[,unique(scenario)],
                  timeframe=data_anomaly[,unique(timeframe)],
                  year=data_anomaly[,unique(year)],
                  hazard=data_anomaly[,unique(hazard)],
                  season=data_anomaly[,unique(season)])
    
    write_json(list(
      source = list(input_table = data_json$input_table, extraction_rast = atlas_data$boundaries$name),
      extraction_method = "zonal",
      geo_filters =  grep("admin",colnames(data_anomaly),value=T),
      season_type = "3-month windows or annual",
      filters = filters,
      format = ".parquet",
      date_created = Sys.time(),
      version = version1,
      parent_script = "R/2.1_create_monthly_haz_tables.R - section 3.3",
      value_variable = "hazard mean, max, min, sd, mean_anomaly, max_anomaly, min_anomaly, sd_anomaly",
      unit = haz_meta[variable.code %in% data_anomaly[,unique(hazard)], base_unit],
      extract_stat = extract_stat,
      baseline=baseline_names[j],
      models = models,
      notes =  paste0("Monthly hazard values extracted by admin areas and summarized using ", extract_stat, 
                      ". Values then combined across 3 or 12 month sequences (season col) using sum or mean depending on the hazard type. ",
                      " Descriptive statistics from an ensemble (GCMs are listed in the models col) are presented for both the timeframe x scenario value and it's difference from baseline (anomaly).")
    ), paste0(save_file2, ".json"), pretty = TRUE)
    
    
    arrow::write_parquet(data_ag_ens,save_file3)
    
    filters$year<-NULL
    write_json(list(
      source = list(input_table = data_json$input_table, extraction_rast = atlas_data$boundaries$name),
      extraction_method = "zonal",
      geo_filters =  grep("admin",colnames(data_anomaly),value=T),
      season_type = "3-month windows or annual",
      filters = filters,
      format = ".parquet",
      date_created = Sys.time(),
      version = version1,
      parent_script = "R/2.1_create_monthly_haz_tables.R - section 3.3",
      value_variable = "hazard mean, max, min, sd, mean_anomaly, max_anomaly, min_anomaly, sd_anomaly",
      unit = haz_meta[variable.code %in% data_anomaly[,unique(hazard)], base_unit],
      extract_stat = extract_stat,
      baseline=baseline_names[j],
      models = models,
      notes =  paste0("Monthly hazard values extracted by admin areas and summarized using ", extract_stat, 
                      ". Values then combined across 3 or 12 month sequences (season col) using sum or mean depending on the hazard type. ",
                      ". Next values and anomalies are averaged across the years in the time series. ",
                      " Finally descriptive statistics from an ensemble (GCMs are listed in the models col) are calculated for both the timeframe x scenario value and it's difference from baseline (anomaly).")
    ), paste0(save_file3, ".json"), pretty = TRUE)
    
    data[,target:=NULL]
    
    }
  })

})

# 3.4) TO DO: Calculate trends #####
# This involves running >10^6 linear models to look at trends, so the process is designed to run in parallel

# Filter out rows with NA/NaN/Inf in 'value' or 'year' before fitting the model
data_ex_trend <- data_ex_season[is.finite(value) & is.finite(year)]

# Add a grouping variable id
data_ex_trend[, ID := .GRP, by = list(admin0_name, admin1_name, scenario, timeframe, model, variable, season)]

# Create an object with the minimal data required
dt<-data_ex_trend[,list(ID,year,value)]

# Define a function to fit a linear model
fit_lm <- function(df) {
  df[, {
    model <- tryCatch(lm(value ~ year), error = function(e) NULL)
    if (is.null(model)) {
      list(intercept = NA, slope = NA, p_value = NA)
    } else {
      coef <- coef(model)
      p_value <- summary(model)$coefficients["year", "Pr(>|t|)"]
      list(intercept = coef["(Intercept)"], slope = coef["year"], p_value = p_value)
    }
  }, by = ID]
}

# Split in n chunks
unique_ids <- unique(dt$ID)
chunk_size <- ceiling(length(unique_ids) / n_workers)
id_chunks <- split(unique_ids, ceiling(seq_along(unique_ids) / chunk_size))
dt_chunks <- lapply(id_chunks, function(ids) dt[ID %in% ids])

# Set up parallel processing plan
n_workers<-20
future::plan(multisession, workers = n_workers)

results_list <- future.apply::future_lapply(dt_chunks, fit_lm)

# Clean up the future plan
future::plan(sequential)

# Combine results into a data.table
results <- rbindlist(results_list)

# Merge results back with original data
data_ex_trend_m<-merge(data_ex_trend,results,all.x=T,by="ID")

# 3.7) Calculate trend stats using the lms  #####
data_ex_trend_stats <- data_ex_trend_m[,.(value_slope=round(slope[1],3),
                                          value_start=round(min(year)*slope[1]+intercept[1],round_by),
                                          value_s5=round(mean(value[1:5]),round_by),
                                          anomaly_s5=round(mean(anomaly[1:5]),round_by),
                                          value_end=round(max(year)*slope[1]+intercept[1],round_by),
                                          value_e5=round(mean(tail(value,5)),round_by),
                                          anomaly_e5=round(mean(tail(anomaly)),round_by),
                                          value_decade=round(10*slope,round_by),
                                          value_pval=round(p_value[1],3)),
                                       by=.(admin0_name,admin1_name,scenario,model,timeframe,variable,season)
][,value_diff:=value_e5-value_s5
][,anomaly_diff:=anomaly_e5-anomaly_s5]


# 3.7.1) Ensemble trend stats ######
data_ex_trend_stats_ens<-melt(data_ex_trend_stats,
                              id.vals=c("admin0_name","admin1_name","scenario","model","timeframe","variable","season"),
                              variable.name="stat")

data_ex_trend_stats_ens<-data_ex_trend_stats_ens[,list(mean=mean(value,na.rm=T),
                                                       max=max(value,na.rm=T),
                                                       min=min(value,na.rm=T),
                                                       sd=round(sd(value,na.rm=T),2)),
                                                 by=list(admin0_name,admin1_name,scenario,timeframe,season,variable,stat)]

data_ex_trend_stats_ens_simple<-data_ex_trend_stats_ens[variable %in% c("PTOT","TAVG") & stat %in% c("value_diff","value_decade","anomaly_diff")]

save_file_trends<-file.path(haz_timeseries_monthly_dir,gsub(".parquet","_trends.parquet",file_name))
save_file_trends_simple<-file.path(haz_timeseries_monthly_dir,gsub(".parquet","_trends_simple.parquet",file_name))


# Define the schema with metadata
schema <- schema(
  admin0_name = utf8(),
  admin1_name = utf8(),
  scenario = utf8(),
  model = utf8(),
  timeframe = utf8(),
  variable = utf8(),
  season = utf8(),
  stat = utf8(),
  mean = float64(),
  max = float64(),
  min = float64(),
  sd = float64()
)

# Add metadata to the schema
metadata <- list(
  description = "Ensemble trend statistics for various scenarios and models",
  value_slope = "The slope of the linear regression model for the `value` variable.",
  value_start = "The estimated value of the `value` variable at the start year.",
  value_s5 = "The mean of the first 5 values in the time series.",
  anomaly_s5 = "The mean of the first 5 anomaly values in the time series.",
  value_end = "The estimated value of the `value` variable at the end year.",
  value_e5 = "The mean of the last 5 values in the time series.",
  anomaly_e5 = "The mean of the last 5 anomaly values in the time series.",
  value_decade = "The change in `value` over a decade.",
  value_pval = "The p-value of the slope coefficient in the linear regression model.",
  value_diff = "The difference between the ending and starting 5-year mean values.",
  anomaly_diff = "The difference between the ending and starting 5-year mean anomalies."
)

# Convert metadata to JSON and save it
arrow::write_parquet(data_ex_trend_stats_ens, save_file_trends)
jsonlite::write_json(metadata, gsub(".parquet",".json",save_file_trends))

arrow::write_parquet(data_ex_trend_stats_ens_simple, save_file_trends_simple)
jsonlite::write_json(metadata, gsub(".parquet",".json",save_file_trends_simple))

