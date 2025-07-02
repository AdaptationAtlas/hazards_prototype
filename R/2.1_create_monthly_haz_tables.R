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
              "pbapply",
              "trend")

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
    data$zone_id <- ifelse(!is.na(data$gaul2_code), data$gaul2_code,
      ifelse(!is.na(data$gaul1_code), data$gaul1_code, data$gaul0_code))    
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
    data.frame(Geographies[[i]])[,c("iso3","admin0_name","admin1_name","admin2_name","zone_id","gaul0_code","gaul1_code","gaul2_code")]
  })
  
  names(boundaries_index)<-names(Geographies)
  
  ## 1.5) Load hazard meta-data #####
  haz_meta<-data.table::fread(haz_meta_url, showProgress = FALSE)
  
  ## 1.6) Controls ####
    
    ### Section 2 - Extraction of monthly hazards by admin areas ####
    round1<-1
    version1<-1
    worker_n1<-5
    overwrite1<-F # overwrite folder level extractions
    overwrite1_haz_monthly <- T #overwrite the monthly files
  
    # Data QC checks
    max_rain<-3000 # Max acceptable value for monthly rainfall 
    min_haz<--10 # Min acceptable value for any hazards (some temperatures can be negative)
    exclude_flagged<-F # Exclude combinations of admin x timeframe x scenario x model x hazard that contain any bad values?
    
    ### Section 3 - Summarization of monthly hazards ####
    worker_n2<-20
    overwrite2<-T
    round3.1<-3
    round3.3<-3
    round3.4<-3
    
    ### Final data ####
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
  
  id_vars<-c("iso3","admin0_name","admin1_name","admin2_name", "gaul0_code", "gaul1_code", "gaul2_code")
  split_delim<-"_"
  split_colnames<-c("scenario","timeframe","model","hazard", "year", "month")
  extract_stat<-"mean"
  order_by<-c("iso3","admin0_name","admin1_name","admin2_name", "gaul0_code", "gaul1_code", "gaul2_code")
  order_by2<-c("admin0_name", "admin1_name","gaul0_code","season","hazard", "scenario", "timeframe")
  
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
            dat <- merge(dat, idx, by = "zone_id", all.x = TRUE, sort = FALSE)
            dat$zone_id <- NULL
            return(dat)
          }))
          
          result_long <- melt(result, id.vars = id_vars)
          if (!is.null(round_dp)) result_long[, value := round(value, round_dp)]
          result_long[, (split_colnames) := tstrsplit(variable, "_", fixed = TRUE)]
          result_long[, variable := NULL]
          if (!is.null(order_by)) {
            result_long <- result_long[order(do.call(order, result_long[, ..order_by]))]
          }
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
            unit = haz_meta[variable.code == hazard, ][["base_unit"]],
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
  names(baselines)<-c("1995-2014","AgERA5 1981-2022")
  
  futures<-files[!grepl("historic",timeframe),unique(timeframe)]
  
  problem_data<-lapply(1:length(timeframes),FUN=function(i){
    timeframe_choice<-timeframes[i]
    save_path<-file.path(output_dir,paste0("haz_monthly_adm_mean_",timeframe_choice,".parquet"))
    
    if(!file.exists(save_path)|overwrite1_haz_monthly){
    cat("2.4) Merging data for timeframe",timeframe_choice,"\n")
      
    files_ss<-files[timeframe==timeframe_choice,file]
    
    data<-rbindlist(pblapply(files_ss,arrow::read_parquet))
    
    # Check for any weird results
    check<-data[value>max_rain | value<(min_haz)] # Highest monthly recorded rainfall in africa is <3000mm
    if(nrow(check)>0){
      warning(nrow(check)," rows of data have values >",max_rain," or <",min_haz,". These hazards x admin areas x scenario x models have ",
              if(!exclude_flagged){"not been"}else{"been"}," excluded.\n")
      problem_dat<-unique(check[,.(hazard,iso3,admin0_name,admin1_name,gaul0_code,admin2_name,scenario,model)])
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
    filters<-list(scenario=data[,unique(scenario)],
                  model=data[,unique(model)],
                  timeframe=data[,unique(timeframe)],
                  year=data[,unique(year)],
                  hazard=data[,unique(hazard)],
                  month=data[,unique(month)])
    
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
    
    if(nrow(check)>0){
      return(check)
    }else{
      return(NULL)
    }
  }
  })
  
  monthly_files<-file.path(output_dir,paste0("haz_monthly_adm_mean_",timeframes,".parquet"))
  
  # Check for missing values
  data<-as.data.table(arrow::read_parquet(monthly_files[1]))
  missing<-data[value==-Inf|is.infinite(value)|is.na(value)|is.null(value),.(hazard=paste(unique(hazard),collapse=",")),by=.(admin0_name,admin1_name, gaul0_code)]
  
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
  
  id_vars <- c("admin0_name", "admin1_name", "gaul0_code", "scenario", "model", "timeframe", "year", "hazard","suspect_value_flag")
  
  lapply(monthly_files,FUN=function(month_file){
    save_file<-gsub("_monthly_","_3months_",month_file)
    
    if(!file.exists(save_file)|overwrite2){
    cat("3.1) Seasonal summarization: ",basename(month_file), "\n")
    data_ex_ss<-as.data.table(arrow::read_parquet(month_file))
    vars<-data_ex_ss[,unique(hazard)]
  
   data_ex_season <- lapply(1:length(three_month_periods), function(j) {
        
        m_period <- three_month_periods[[j]]
        season_name <- names(three_month_periods)[j]
        dt <- copy(data_ex_ss)[month %in% m_period]
        setorder(dt, year, month)
      
      dt[, seq := find_consecutive_pattern(seq = month, pattern = m_period),
           by = .(admin0_name, admin1_name, gaul0_code, model, scenario, timeframe, hazard)]
        
        dt <- dt[!is.na(seq)]
        dt[, year := year[1], by = .(admin0_name, admin1_name, gaul0_code, model, scenario, timeframe, hazard, seq)]
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
      setorderv(data_ex_season, order_by2)
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
  
  # baseline averages
  data_ex_hist<-lapply(baselines,FUN=function(baseline){
      data<-as.data.table(arrow::read_parquet(grep(paste0("_",baseline,"[.]"),monthly3_files,value=T)))
      data<-data[,.(baseline_value=round(mean(value,na.rm=T),round3.1)),by=.(admin0_name,admin1_name,gaul0_code,hazard,season)]
      data[,baseline_name:=baseline]
      data
  })

  names(data_ex_hist)<-baselines
  
  # Combinations
  fut_monthly3_files<-monthly3_files[!grepl("historic",monthly3_files)]

  file_combos<-data.table(rbind(
    expand.grid(data=fut_monthly3_files,baseline=baselines,stringsAsFactors = F),
    rbindlist(lapply(baselines,FUN=function(baseline){
      data.frame(data = paste0(output_dir,"/haz_3months_adm_mean_",baseline,".parquet"),
               baseline=baseline)
    }))
  ))
  
  file_combos[,save_file:=gsub(".parquet",paste0("_anomaly-",baseline,"_seasons.parquet"),data),by=.I
              ][,save_file2:=gsub(".parquet",paste0("_anomaly-",baseline,"_ensemble_seasons.parquet"),data),by=.I
                ][,save_file3:=gsub(".parquet",paste0("_anomaly-",baseline,"_ensemble.parquet"),data),by=.I]
  
  invisible(lapply(1:nrow(file_combos),FUN=function(i){
    save_file<-file_combos$save_file[i]
    
    if(!file.exists(save_file)|overwrite2){
      
    cat("3.2) Calculating anomalies for ",i,"/",nrow(file_combos),basename(save_file),"\n")
      
    baseline<-file_combos$baseline[i]
    baseline_name<-names(baselines)[baselines==baseline]
    data<-as.data.table(arrow::read_parquet(file_combos$data[i]))
    data<-merge(data,data_ex_hist[[baseline]],all.x=T)
    data[, anomaly:=value-baseline_value]
    data[,baseline_name:=baseline_name]
    
    arrow::write_parquet(data,save_file)
    
    data_json<-jsonlite::read_json(file.path(output_dir,paste0(basename(file_combos$data[i]),".json")),simplifyVector=T)
    
    filters<-list(scenario=data[,unique(scenario)],
                  timeframe=data[,unique(timeframe)],
                  year=data[,unique(year)],
                  hazard=data[,unique(hazard)],
                  season=data[,unique(season)],
                  model=data[,unique(model)])

    field_descriptions = list(
      admin0_name     = "Name of the country (first-level administrative unit)",
      admin1_name     = "Name of the subnational region (second-level administrative unit)",
      scenario        = "Emissions scenario (e.g., SSP1-2.6, SSP3-7.0)",
      timeframe       = "Future period being analyzed (e.g., 2030s, 2050s)",
      model           = "General Circulation Model (GCM) identifier used to generate climate projections",
      year            = "Calendar year of the data point",
      hazard          = "Climate hazard variable (e.g., PTOT = precipitation total, TMAX = max temperature)",
      season          = "3-month window or annual aggregation (e.g., DJF, MAM, annual)",
      value           = paste0("Monthly or seasonal hazard value summarized using ", extract_stat, 
                               " (e.g., average precipitation or max temperature)"),
      baseline_value  = "Historical mean value for the same location and season based on the selected baseline period",
      anomaly         = "Difference between value and baseline_value, representing the climate anomaly",
      baseline_name   = "Label for the baseline period used in anomaly calculations (e.g., 1995–2014)"
    )
    
    write_json(list(
      source = list(input_table = data_json$input_raster, extraction_rast = atlas_data$boundaries$name),
      extraction_method = "zonal",
      geo_filters =  grep("admin",colnames(data),value=T),
      season_type = "3-month windows or annual",
      filters = filters,
      format = ".parquet",
      date_created = Sys.time(),
      version = version1,
      parent_script = "R/2.1_create_monthly_haz_tables.R - section 3.3",
      value_variable = "value, baseline_value, anomaly",
      field_descriptions = field_descriptions,
      unit = unique(haz_meta[variable.code %in% data[,unique(hazard)], base_unit]),
      extract_stat = extract_stat,
      baseline=baseline_name,
      models = data[,paste0(sort(unique(model)),collapse=",")],
      notes =  paste0(
        "This file contains model-specific climate hazard data extracted for subnational administrative units (admin0_name, admin1_name), ",
        "organized by scenario, timeframe, hazard type, season, year, and GCM (model). ",
        "Monthly hazard values were first spatially summarized using the statistic '", extract_stat, "' (e.g., mean or sum), ",
        "then grouped into rolling 3-month or annual periods according to the 'season' column. ",
        "Anomaly values were calculated as the difference between each future value and the historical mean for the corresponding location and season, ",
        "based on the specified baseline period (baseline_name). ",
        "Each record retains the original GCM and year information, allowing temporal trend analysis and inter-model comparison. ",
        "This dataset has not been ensembled; all values reflect individual model behavior."
      )
    ), paste0(save_file, ".json"), pretty = TRUE)
    
    }
    
    
  }))

  cat("3.2) Adding historical means  - Complete \n")
  
  # Remove non-finite values
  # data_ex_season<-data_ex_season[is.finite(value)]
  
  ## 3.3) Calculate ensembled statistics #####
  cat("3.3) Calculating ensemble stats \n")
  
  # Each file create is a combination of futures x baselines, apart from baselines which are compared to themselves
  invisible(lapply(1:nrow(file_combos),FUN=function(i){
    
    save_file<-file_combos$save_file[i]
    save_file2<-file_combos$save_file2[i]
    save_file3<-file_combos$save_file3[i]
    
    cat("3.3) Calculating ensemble stats for ",i,"/",nrow(file_combos),basename(save_file),"\n")
    
    if(!file.exists(save_file2)|overwrite2){
      
    data_anomaly<-as.data.table(arrow::read_parquet(save_file))
    models<- data_anomaly[,paste0(sort(unique(model)),collapse=",")]
    
    # Ensemble models by years
    data_anomaly_ens<-data_anomaly[,list(mean=mean(value,na.rm=T),
                          max=max(value,na.rm=T),
                          min=min(value,na.rm=T),
                          sd=sd(value,na.rm=T),
                          mean_anomaly=mean(anomaly,na.rm=T),
                          max_anomaly=max(anomaly,na.rm=T),
                          min_anomaly=min(anomaly,na.rm=T),
                          sd_anomaly=sd(anomaly,na.rm=T)),
                    by=list(admin0_name,admin1_name,gaul0_code,scenario,timeframe,year,hazard,season,baseline_name)]
    print(paste("Years: ",unique(data_anomaly_ens$year)))
    
    num_cols <- names(data_anomaly_ens)[sapply(data_anomaly_ens, is.numeric)]
    num_cols <- num_cols[num_cols != "gaul0_code"]
    data_anomaly_ens[, (num_cols) := lapply(.SD, round, digits = round3.3), .SDcols = num_cols]
    
    data_anomaly_ens[,hazard:=gsub("_mean|_max","",hazard)]
    data_anomaly_ens[,models:=models]
  
    # Aggregate models over years then ensemble
    data_ag<-data_anomaly[,list(mean=mean(value,na.rm=T),
                                mean_anomaly=mean(anomaly,na.rm=T)),
                    by=list(admin0_name,admin1_name,gaul0_code,scenario,timeframe,model,hazard,season,baseline_name)]
      
    data_ag_ens<-data_ag[,list(mean_mean=mean(mean,na.rm=T),
                                        min_mean=min(mean,na.rm=T),
                                        max_mean=max(mean,na.rm=T),
                                        median_mean=median(mean,na.rm=T),
                                        mean_anomaly=mean(mean_anomaly,na.rm=T),
                                        max_anomaly=max(mean_anomaly,na.rm=T),
                                        min_anomaly=min(mean_anomaly,na.rm=T),
                                        sd_anomaly=sd(mean_anomaly,na.rm=T)),
                                  by=list(admin0_name,admin1_name,gaul0_code,scenario,timeframe,hazard,season,baseline_name)]
    
      data_ag_ens[,models:=models]
      
      num_cols <- names(data_ag_ens)[sapply(data_ag_ens, is.numeric)]
      num_cols <- num_cols[num_cols != "gaul0_code"]
      data_ag_ens[, (num_cols) := lapply(.SD, round, digits = round_final), .SDcols = num_cols]
      if (!is.null(order_by2)){ 
        setorderv(data_anomaly_ens, order_by2)
        setorderv(data_ag_ens, order_by2)
      }
      
      arrow::write_parquet(data_anomaly_ens,save_file2)

          filters<-list(scenario=data_anomaly_ens[,unique(scenario)],
              timeframe=data_anomaly_ens[,unique(timeframe)],
              year=data_anomaly_ens[,unique(year)],
              hazard=data_anomaly_ens[,unique(hazard)],
              season=data_anomaly_ens[,unique(season)])

       data_json<-jsonlite::read_json(file.path(output_dir,paste0(basename(file_combos$data[i]),".json")),simplifyVector=T)
       baseline<-file_combos$baseline[i]
      filters$model<-NULL
      baseline_name <- data_ag_ens[,unique(baseline_name)]
      
      field_descriptions = list(
        admin0_name     = "Name of the country (first-level administrative unit)",
        admin1_name     = "Name of the subnational region (second-level administrative unit)",
        scenario        = "Emissions scenario (e.g., SSP1-2.6, SSP3-7.0)",
        timeframe       = "Future period being analyzed (e.g., 2030s, 2050s)",
        year            = "Calendar year of the data point",
        hazard          = "Climate hazard variable (e.g., PTOT = precipitation total, TMAX = max temperature)",
        season          = "3-month window or annual aggregation (e.g., DJF, MAM, annual)",
        baseline_name   = "Label for the baseline period used in anomaly calculations (e.g., 1995–2014)",
        mean            = paste0("Mean of the hazard values across GCMs using ", extract_stat, 
                                 " as the spatial summary method for each model"),
        max             = "Maximum hazard value across GCMs",
        min             = "Minimum hazard value across GCMs",
        sd              = "Standard deviation of hazard values across GCMs",
        mean_anomaly    = "Mean anomaly across GCMs (difference from historical baseline)",
        max_anomaly     = "Maximum anomaly across GCMs",
        min_anomaly     = "Minimum anomaly across GCMs",
        sd_anomaly      = "Standard deviation of anomalies across GCMs",
        models          = "Comma-separated list of GCMs included in the ensemble"
      )
      
      write_json(list(
        source = list(input_table = data_json$input_raster, extraction_rast = atlas_data$boundaries$name),
        extraction_method = "zonal",
        geo_filters =  grep("admin",colnames(data_anomaly_ens),value=T),
        season_type = "3-month windows or annual",
        filters = filters,
        format = ".parquet",
        date_created = Sys.time(),
        version = version1,
        parent_script = "R/2.1_create_monthly_haz_tables.R - section 3.3",
        value_variable = "hazard mean, max, min, sd, mean_anomaly, max_anomaly, min_anomaly, sd_anomaly",
        field_descriptions = field_descriptions,
        unit = unique(haz_meta[variable.code %in% data_anomaly_ens[,unique(hazard)], base_unit]),
        extract_stat = extract_stat,
        baseline=baseline_name,
        models = models,
        notes =  paste0(  "This file contains ensembled summaries of monthly climate hazard values and their anomalies, ",
                          "extracted for subnational administrative units (admin0_name, admin1_name) and grouped by scenario, timeframe, season, and hazard type. ",
                          "Raw values were first summarized spatially using the statistic '", extract_stat, "' (e.g., mean or sum), ",
                          "then aggregated into rolling 3-month or annual periods according to the 'season' column. ",
                          "For each GCM (listed in the `models` column), anomaly values were computed relative to a specified baseline period. ",
                          "The ensemble statistics include mean, min, max, and standard deviation (SD) across models, reported separately for both raw hazard values ",
                          "and their anomalies. This provides a robust indication of central tendency and inter-model spread, which is critical for quantifying agreement ",
                          "and uncertainty across climate projections.")
      ), paste0(save_file2, ".json"), pretty = TRUE)
      
      
      arrow::write_parquet(data_ag_ens,save_file3)
      
      filters$year<-NULL
      
      field_descriptions = list(
        admin0_name     = "Name of the country (first-level administrative unit)",
        admin1_name     = "Name of the subnational region (second-level administrative unit)",
        scenario        = "Emissions scenario (e.g., SSP1-2.6, SSP3-7.0)",
        timeframe       = "Future period being analyzed (e.g., 2030s, 2050s)",
        hazard          = "Climate hazard variable (e.g., PTOT = precipitation total, TMAX = max temperature)",
        season          = "3-month window or annual aggregation (e.g., DJF, MAM, annual)",
        baseline_name   = "Label for the baseline period used in anomaly calculations (e.g., 1995–2014)",
        mean_mean       = paste0("Mean of yearly-averaged hazard values across all GCMs, where each model’s values were spatially summarized using ", extract_stat),
        min_mean        = "Minimum of the yearly-averaged hazard values across models",
        max_mean        = "Maximum of the yearly-averaged hazard values across models",
        median_mean     = "Median of the yearly-averaged hazard values across models",
        mean_anomaly    = "Mean of yearly-averaged anomalies (relative to baseline) across models",
        max_anomaly     = "Maximum of yearly-averaged anomalies across models",
        min_anomaly     = "Minimum of yearly-averaged anomalies across models",
        sd_anomaly      = "Standard deviation of yearly-averaged anomalies across models",
        models          = "Comma-separated list of GCMs included in the ensemble"
      )    
      
      write_json(list(
        source = list(input_table = data_json$input_raster, extraction_rast = atlas_data$boundaries$name),
        extraction_method = "zonal",
        geo_filters =  grep("admin",colnames(data_anomaly),value=T),
        season_type = "3-month windows or annual",
        filters = filters,
        format = ".parquet",
        date_created = Sys.time(),
        version = version1,
        parent_script = "R/2.1_create_monthly_haz_tables.R - section 3.3",
        value_variable = "hazard mean, max, min, sd, mean_anomaly, max_anomaly, min_anomaly, sd_anomaly",
        field_descriptions = field_descriptions,
        unit = haz_meta[variable.code %in% data_anomaly[,unique(hazard)], base_unit],
        extract_stat = extract_stat,
        anomaly_baseline=baseline_name,
        models = models,
        notes = "This file presents ensemble summary statistics for climate hazard indicators and their anomalies, aggregated by subnational administrative units (admin0_name, admin1_name), scenario, timeframe, hazard, and season. Monthly hazard values were extracted using the selected spatial summary method (e.g., mean or sum) and grouped into rolling 3-month or annual periods based on the ‘season’ column. The resulting values and anomalies (relative to a historical baseline) were then averaged across all years within the specified timeframe for each GCM. These multi-year averages were used to calculate ensemble statistics across models (listed in the ‘models’ column), including the mean, min, max, and median for values, and mean, min, max, and standard deviation for anomalies. The file is designed to support high-level climate risk analysis, scenario comparison, and adaptation planning."
      ), paste0(save_file3, ".json"), pretty = TRUE)
      
      }
    }))
  
  cat("3.3) Calculating ensemble stats - Complete \n")

  ## 3.4) Calculate trends #####
# This involves running >10^6 linear models to look at trends, so the process is designed to run in parallel
cat("3.4) Trend calculation\n")

invisible(lapply(1:nrow(file_combos),FUN=function(i){
  data_file<-file_combos$save_file[i]

  file_base<-gsub("_seasons","",data_file)
  save_file<-gsub(".parquet","_trends.parquet",file_base)
  save_file2<-gsub(".parquet","_trends_ensemble.parquet",file_base)
  save_file3<-gsub(".parquet","_trends_ensemble_minimal.parquet",file_base)
  
  cat("3.4) Trends - Processing",i,"/",nrow(file_combos),basename(data_file),"\n")
  
  if(!file.exists(save_file)|overwrite2){
  data_ex_trend<-as.data.table(arrow::read_parquet(data_file))
  
  # Filter out rows with NA/NaN/Inf in 'value' or 'year' before fitting the model
  data_ex_trend <- data_ex_trend[is.finite(value) & is.finite(year)][,n_value:=NULL]
  
  ## 3.4.1) Calculate Theil–Sen estimator ####
  trend_summary <- data_ex_trend[
    , {
      ts <- tryCatch(sens.slope(value), error = function(e) NULL)
      if (is.null(ts)) {
        list(slope = NA_real_, intercept = NA_real_,
             ci_low = NA_real_, ci_high = NA_real_, p_value = NA_real_)
      } else {
        m <- unname(ts$estimates)
        intercept <- median(baseline_value - m * year)
        list(
          slope = m,
          intercept = intercept,
          ci_low = ts$conf.int[1],
          ci_high = ts$conf.int[2],
          p_value = tryCatch(mk.test(value)$p.value, error = function(e) NA_real_)
        )
      }
    },
    by = .(admin0_name, admin1_name, gaul0_code, scenario, timeframe, model, hazard, season,baseline_name)
  ]

  data_ex_trend_m<-merge(data_ex_trend,trend_summary,
                         by=c("admin0_name", "admin1_name", "gaul0_code", "scenario", "timeframe", "model", "hazard", "season","baseline_name"),
                         all.x=T,sort=F)
  
  ### 3.4.2) Calculate trend stats #####
  data_ex_trend_stats <- data_ex_trend_m[,.(value_slope=slope[1],
                                            value_start=min(year)*slope[1]+intercept[1],
                                            value_s5=mean(value[1:5]),
                                            anomaly_s5=mean(anomaly[1:5]),
                                            value_end=max(year)*slope[1]+intercept[1],
                                            value_e5=mean(tail(value,5)),
                                            anomaly_e5=mean(tail(anomaly,5)),
                                            value_decade=10*slope,
                                            value_pval=p_value[1]),
                                         by=.(admin0_name,admin1_name,gaul0_code,scenario,model,timeframe,hazard,season,baseline_name)
  ][,value_diff:=value_e5-value_s5
  ][,anomaly_diff:=anomaly_e5-anomaly_s5]
  
  # Create dataset for ensembling, before any rounding occurs
  data_ex_trend_stats_ens<-melt(data_ex_trend_stats,
                                id.vals=c("admin0_name","admin1_name","gaul0_code","scenario","model","timeframe","variable","season","baseline_name"),
                                variable.name="stat")
  
  num_cols <- names(data_ex_trend_stats)[sapply(data_ex_trend_stats, is.numeric)]
  num_cols <- num_cols[num_cols != "gaul0_code"]
  data_ex_trend_stats[, (num_cols) := lapply(.SD, round, digits = round3.4), .SDcols = num_cols]
  
  if (!is.null(order_by2)){ 
    setorderv(data_ex_trend_stats, order_by2)
  }
  
  # Save result
  arrow::write_parquet(data_ex_trend_stats,save_file)
  
  filters<-list(scenario=data_ex_trend_stats[,unique(scenario)],
                timeframe=data_ex_trend_stats[,unique(timeframe)],
                model=data_ex_trend_stats[,unique(model)],
                hazard=data_ex_trend_stats[,unique(hazard)],
                season=data_ex_trend_stats[,unique(season)])
  
  field_descriptions <- list(
    admin0_name   = "Name of the country (first-level administrative unit)",
    admin1_name   = "Name of the subnational region (second-level administrative unit)",
    scenario      = "Emissions scenario (e.g., SSP1-2.6, SSP3-7.0)",
    model         = "Name of the General Circulation Model (GCM) used for climate projection",
    timeframe     = "Future period being analyzed (e.g., 2030s, 2050s)",
    hazard        = "Climate hazard variable (e.g., PTOT = precipitation total, TMAX = max temperature)",
    season        = "3-month rolling window or annual aggregation (e.g., DJF, MAM, annual)",
    baseline_name = "Label for the baseline period used to compute anomalies (e.g., 1995–2014)",
    
    value_slope   = "Sen's slope estimate of the linear trend in the `value` variable over time",
    value_start   = "Estimated `value` at the starting year of the time series using slope and intercept",
    value_s5      = "Mean of the first 5 `value` entries in the time series",
    anomaly_s5    = "Mean of the first 5 `anomaly` entries in the time series",
    value_end     = "Estimated `value` at the final year of the time series using slope and intercept",
    value_e5      = "Mean of the last 5 `value` entries in the time series",
    anomaly_e5    = "Mean of the last 5 `anomaly` entries in the time series",
    value_decade  = "Change in the `value` variable over a 10-year period (slope × 10)",
    value_pval    = "P-value from Mann-Kendall test assessing the significance of the trend in `value`",
    value_diff    = "Difference between the 5-year end and start means for `value`",
    anomaly_diff  = "Difference between the 5-year end and start means for `anomaly`"
  )
  
  write_json(list(
    source = list(input_raster = indices_dir, extraction_rast = atlas_data$boundaries$name),
    extraction_method = "zonal",
    geo_filters =  grep("admin",colnames(data_ex_trend_stats),value=T),
    season_type = "3-month windows or annual",
    filters = filters,
    value_var = field_descriptions,
    format = ".parquet",
    date_created = Sys.time(),
    version = version1,
    parent_script = "R/2.1_create_monthly_haz_tables.R - section 3.4",
    unit = unique(haz_meta[variable.code %in% data_ex_trend_stats[,unique(hazard)], .(variable.code,base_unit)]),
    extract_stat = extract_stat,
    # anomaly_baseline=baseline_names[j],
    notes =  paste0(
      "This file contains climate hazard summary statistics extracted from monthly raster data, ",
      "aggregated by subnational administrative units (admin0_name, admin1_name). Values were first ",
      "summarized using mean across spatial units, then grouped into rolling 3-month or 12-month periods ",
      "depending on the 'season' column. The summary metric (mean or sum) applied to the seasonal value depends on the hazard type ",
      "as defined in the hazard metadata. For each group of GCMs (models column), ensemble statistics (mean, min, max, SD) ",
      "were calculated for both the raw hazard value and its anomaly (deviation from a historical baseline period). ",
      "Temporal trends were assessed using Sen’s slope estimator, a non-parametric method robust to outliers and missing data, ",
      "with significance evaluated using the Mann–Kendall trend test (p-value column). The table supports climate trend analysis, ",
      "risk monitoring, and adaptation planning by season, region, and scenario."
    )
  ), paste0(save_file, ".json"), pretty = TRUE)
  
  
  # 3.7.1) Ensemble trend stats ######
  
  data_ex_trend_stats_ens<-melt(data_ex_trend_stats,
                                id.vals=c("admin0_name","admin1_name","gaul0_code","scenario","model","timeframe","variable","season"),
                                variable.name="stat")
  
  data_ex_trend_stats_ens<-data_ex_trend_stats_ens[,list(mean=mean(value,na.rm=T),
                                                         max=max(value,na.rm=T),
                                                         min=min(value,na.rm=T),
                                                         sd=sd(value,na.rm=T)),
                                                   by=list(admin0_name,admin1_name,gaul0_code,scenario,timeframe,season,hazard,stat)]
  
  data_ex_trend_stats_ens[,stat:=as.character(stat)]
  
  if (!is.null(order_by2)){ 
    setorderv(data_ex_trend_stats_ens, order_by2)
  }
  
  arrow::write_parquet(data_ex_trend_stats_ens, save_file2)
  
  field_descriptions$model<-NULL
  
  write_json(list(
    source = list(
      input_raster = indices_dir,
      extraction_rast = atlas_data$boundaries$name
    ),
    extraction_method = "zonal",
    geo_filters = grep("admin", colnames(data_ex_trend_stats_ens), value = TRUE),
    season_type = "3-month windows or annual",
    filters = filters,
    value_var = field_descriptions,
    format = ".parquet",
    date_created = Sys.time(),
    version = version1,
    parent_script = "R/2.1_create_monthly_haz_tables.R - section 3.7.1",
    unit = unique(haz_meta[variable.code %in% data_ex_trend_stats_ens[, unique(hazard)], .(variable.code, base_unit)]),
    extract_stat = extract_stat,
    # anomaly_baseline = baseline_names[j],
    notes = paste0(
      "This file contains ensemble-level summaries of trend statistics derived from seasonal hazard values, ",
      "aggregated by subnational administrative units. Each row corresponds to a single trend metric (e.g., Sen's slope, decadal change) ",
      "calculated across multiple GCM models for a given scenario, timeframe, season, and hazard type. ",
      "The 'stat' column indicates the specific trend metric summarized, while the 'mean', 'min', 'max', and 'sd' columns report ensemble ",
      "statistics across GCMs. Trend slopes were estimated using Sen's slope method, a robust non-parametric estimator. ",
      "The Mann–Kendall trend test was used to assess significance. These summaries support regional assessments of ",
      "climate hazard evolution and are suitable for visualizing uncertainty ranges across climate models."
    )
  ), paste0(save_file2, ".json"), pretty = TRUE)
  
  
  data_ex_trend_stats_ens_simple<-data_ex_trend_stats_ens[hazard %in% c("PTOT","TAVG","TMAX") & stat %in% c("value_diff","value_decade","anomaly_diff")]
  
  arrow::write_parquet(data_ex_trend_stats_ens_simple, save_file3)
  
  filters<-list(scenario=data_ex_trend_stats_ens_simple[,unique(scenario)],
                timeframe=data_ex_trend_stats_ens_simple[,unique(timeframe)],
                hazard=data_ex_trend_stats_ens_simple[,unique(hazard)],
                season=data_ex_trend_stats_ens_simple[,unique(season)],
                stat=data_ex_trend_stats_ens_simple[,unique(stat)])
  
  field_descriptions_simple <- list(
    admin0_name   = "Name of the country (first-level administrative unit)",
    admin1_name   = "Name of the subnational region (second-level administrative unit)",
    scenario      = "Emissions scenario (e.g., SSP1-2.6, SSP3-7.0)",
    timeframe     = "Future period being analyzed (e.g., 2030s, 2050s)",
    hazard        = "Climate hazard variable (e.g., PTOT = precipitation total, TMAX = max temperature)",
    season        = "3-month rolling window or annual aggregation (e.g., DJF, MAM, annual)",
    baseline_name = "Label for the baseline period used to compute anomalies (e.g., 1995–2014)",
    
    value_diff    = "Difference between end and start 5-year means for the raw hazard values (e.g., TAVG, PTOT, TMAX).",
    value_decade  = "Estimated change in the seasonal hazard value over a 10-year period, based on Sen’s slope.",
    anomaly_diff  = "Difference between end and start 5-year means for the seasonal anomalies relative to historical baseline."
  )  
  
  write_json(list(
    source = list(input_raster = indices_dir,extraction_rast = atlas_data$boundaries$name),
    extraction_method = "zonal",
    geo_filters = grep("admin", colnames(data_ex_trend_stats_ens_simple), value = TRUE),
    season_type = "3-month windows or annual",
    filters = filters,
    value_var = field_descriptions_simple,
    format = ".parquet",
    date_created = Sys.time(),
    version = version1,
    parent_script = "R/2.1_create_monthly_haz_tables.R - section 3.7.1",
    unit = unique(haz_meta[variable.code %in% data_ex_trend_stats_ens_simple[, unique(hazard)], .(variable.code, base_unit)]),
    extract_stat = extract_stat,
    # anomaly_baseline = baseline_names[j],
    notes = paste0(
      "This simplified file contains a filtered subset of ensemble-level climate trend summaries for key hazards ",
      "(precipitation total [PTOT], average temperature [TAVG], and maximum temperature [TMAX]). ",
      "It includes only three critical trend metrics—value_diff, value_decade, and anomaly_diff—sufficient for many use cases such as regional trend mapping, ",
      "climate impact summaries, and adaptation planning dashboards. These were computed across GCM ensembles and summarized using ",
      "mean, min, max, and standard deviation (SD) to express model spread. The values are provided for each season, scenario, and region, ",
      "enabling spatial and temporal comparison of hazard trends under different future climates."
    )
  ), paste0(save_file3, ".json"), pretty = TRUE)
    }

  }))

cat("3.4) Trend calculations - Complete\n")
