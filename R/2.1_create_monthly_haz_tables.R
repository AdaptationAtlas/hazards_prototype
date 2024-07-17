# Please run 0_server_setup.R before executing this script

# 0) Load R functions & packages ####
packages <- c("arrow",
              "geoarrow",
              "sf",
              "terra", 
              "data.table", 
              "future",
              "future.apply",
              "exactextractr",
              "parallel",
              "pbapply")

# Call the function to install and load packages
p_load(char=packages)

# 1) Set up workspace ####
  # 1.0) Set cores for parallel #####
  n_workers<-parallel::detectCores()-2
  # 1.1) Set directories #####
  output_dir<-haz_timeseries_monthly_dir

  # 1.2) Set hazards to include in analysis #####
  hazards<-c("HSH_max","TMAX","TAVG","NDWL0","NDWS","NTx35","NTx40","PTOT","THI_max") # NDD is not being used as it cannot be projected to future scenarios
  file_name<-"all_hazards.parquet"
  
  #hazards<-c("TMAX","TAVG","PTOT") # NDD is not being used as it cannot be projected to future scenarios
  #file_name<-"tmax_tavg_ptot_data.parquet"
  
  # 1.2) Set scenarios and time frames to analyse #####
  Scenarios<-c("ssp126","ssp245","ssp370","ssp585")
  
  #if(file_name=="all_data.parquet"){
  #  Scenarios<-c("ssp245","ssp585")
  #}
  
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
  # This is limited to admin1 (admin2 is possible but we generate huge files that go beyond the 2gb recommended size for a parquet file, so the data would need to split into chunks)
  Geographies<-lapply(1:2,FUN=function(i){
    file<-geo_files_local[i]
    data<-arrow::open_dataset(file)
    data <- data |> sf::st_as_sf() |> terra::vect()
    data
  })
  names(Geographies)<-names(geo_files_local)[1:2]
  
  # 1.5) Load hazard meta-data #####
  haz_meta<-data.table::fread(haz_meta_url, showProgress = FALSE)
  
# 2) Extract by admin ####
levels<-c(admin0="adm0",admin1="adm1") #,admin2="adm2")
FUN<-"mean"
overwrite<-F # overwrite folder level extractions

# Temporarily exclude problem folders until issues with input data are resolved ####
exclude_dirs<-file.path(indices_dir,"ssp245_EC-Earth3_2021_2040","PTOT")

data_ex<-rbindlist(lapply(1:nrow(folders),FUN=function(i){
      folders_ss<-paste0(folders$path[i],"/",hazards)
      
      # Exclude any folders with problem data
      folders_ss<-folders_ss[!folders_ss %in% exclude_dirs]
      
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
        
        if(length(files)!=0){
        
        rast_stack<-terra::rast(files)
        
        if(hazard=="PTOT"){
          # Reclassify -9999 to NA
          terra::classify(rast_stack,rcl=data.frame(from=-99999999,to=-0.000000001,NA))
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
                                  ][,list(admin0_name,admin1_name,scenario,timeframe,model,year,month,variable,value)
                                    ][,value:=round(value,1)]
        
        data_ex<-data_ex[,list(admin0_name,admin1_name,variable,scenario,timeframe,model,year,month,value)]
        
        data_ex<-data_ex[order(admin0_name,admin1_name,variable,scenario,timeframe,model,year,month)]
        
        arrow::write_parquet(data_ex,sink=save_file)
        }else{
          data_ex<-NULL
        }
        }else{
          data_ex<-arrow::read_parquet(save_file)
          
          if(!"adm2" %in% levels){
            if("admin2_name" %in% colnames(data_ex)){
            data_ex<-data_ex[is.na(admin2_name)][,admin2_name:=NULL]
            }
          }
        }
        
        data_ex
      })
      data<-rbindlist(data,use.names=T)
   }),use.names=T)

data_ex[value>10000]

# 3) Summarize annually or 3 month windows ####
  # 3.1) Subset data #####
  data_ex_ss<-data_ex[,month:=as.integer(month)
                       ][,year:=as.integer(year)
                         ][!variable %in% c("AVAIL","HSH_mean","THI_mean")] 
  
  vars<-data_ex_ss[,unique(variable)]
  
  # 3.2) Create 3 month windows #####
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
  
  # 3.3) Summarize data by season #####
  round_by<-2
  data_ex_season <- rbindlist(pblapply(1:length(three_month_periods),FUN=function(i){
    m_period<-three_month_periods[[i]]
    data<-data_ex_ss[month %in% m_period]
  
    data[,seq:=find_consecutive_pattern(seq=month,pattern=m_period),by=list(admin0_name,admin1_name,model,scenario,timeframe,variable)]
    data<-data[!is.na(seq)
               ][,year:=year[1],by=list(admin0_name,admin1_name,model,scenario,timeframe,variable,seq)
                 ][,seq:=NULL]
    
    data_season<-rbindlist(lapply(vars, function(VAR) {
      # Get the corresponding function for the variable
      func_name <- unique(haz_meta$`function`[haz_meta$variable.code == VAR])
      # Ensure the function exists and is valid
      func <- get(func_name, mode = "function", envir = parent.frame())
      # Summarize the data for the variable using the specified function
      
      data_ss<-data[variable == VAR, .(value = func(value, na.rm = TRUE),n_value=.N),
                 by = .(admin0_name, admin1_name, scenario, model, timeframe, year, variable)
      ][,season:=names(three_month_periods)[i]
        ][,value:=round(value,round_by)]
      
      data_ss
      }))
    return(data_season)
    }))
  
  # Add historical mean
  data_ex_season_hist<-data_ex_season[scenario=="historical" & timeframe=="historical"
                                      ][,.(baseline_mean=mean(value,na.rm=T)),by=.(admin0_name,admin1_name,variable,season)]
  
  data_ex_season[,baseline_mean:=round(mean(value[scenario=="historical" & timeframe=="historical"]),round_by),
              by=list(admin0_name,admin1_name,variable,season)
              ][,anomaly:=round(value-baseline_mean,round_by)]
  
  # Check -Inf values (Annobón is a tiny island missed by CHIRTS)
  unique(data_ex_season[value==-Inf|is.infinite(value)|is.na(value)|is.null(value),list(admin0_name,admin1_name,variable)])
  
  # Remove non-finite values
  data_ex_season<-data_ex_season[is.finite(value)]
  
  # 3.4) Calculate ensembled statistics #####
  data_ex_season_ens<-data_ex_season[,list(mean=mean(value,na.rm=T),
                                 max=max(value,na.rm=T),
                                 min=min(value,na.rm=T),
                                 sd=sd(value,na.rm=T),
                                 mean_anomaly=mean(anomaly,na.rm=T),
                                 max_anomaly=max(anomaly,na.rm=T),
                                 min_anomaly=min(anomaly,na.rm=T),
                                 sd_anomaly=sd(anomaly,na.rm=T),
                                 n_models=length(unique(model))),
                           by=list(admin0_name,admin1_name,scenario,timeframe,year,variable,season)]
  
  
  data_ex_season_ens<-data_ex_season_ens[!n_models %in% c(2:4)][!(n_models==1 & timeframe!="historical")]
  
  data_ex_season_ens[scenario=="historical",c("max","min","max_anomaly","min_anomaly","sd_anomaly","n_models"):=NA]
  
  data_ex_season_ens[,variable:=gsub("_mean|_max","",variable)]
  
  numeric_cols <- c("mean","max","min","sd","max_anomaly","mean_anomaly","min_anomaly","sd_anomaly")
  data_ex_season_ens[, (numeric_cols) := lapply(.SD, round,1), .SDcols = numeric_cols]
  
  data_ex_season_ens<- data_ex_season_ens[order(admin0_name, admin1_name, season,variable, scenario, timeframe)]
  
  # 3.4.1) Calculate differences between baseline and future
  data_ex_season_ag<-data_ex_season[,list(mean=mean(value,na.rm=T),
                                          mean_anomaly=mean(anomaly,na.rm=T)),
                                     by=list(admin0_name,admin1_name,scenario,timeframe,model,variable,season)]
  
  # Add ensemble
  data_ex_season_ag_ens<-data_ex_season_ag[,list(mean_mean=mean(mean,na.rm=T),
                                                 min_mean=min(mean,na.rm=T),
                                                 max_mean=max(mean,na.rm=T),
                                                 median_mean=median(mean,na.rm=T),
                                              mean_anomaly=mean(mean_anomaly,na.rm=T),
                                              max_anomaly=max(mean_anomaly,na.rm=T),
                                              min_anomaly=min(mean_anomaly,na.rm=T),
                                              sd_anomaly=sd(mean_anomaly,na.rm=T),
                                              n_models=length(unique(model))),
                                    by=list(admin0_name,admin1_name,scenario,timeframe,variable,season)]
  
  
  # 3.5) Save output #####
  arrow::write_parquet(data_ex_season_ens[,!c("sd","sd_anomaly")],file.path(output_dir,gsub(".parquet","_ensembled.parquet",file_name)))
  
  data_ex_season_ag[, c("mean","mean_anomaly") := lapply(.SD, round, 1), .SDcols = c("mean","mean_anomaly")]
  arrow::write_parquet(data_ex_season_ag,file.path(output_dir,gsub(".parquet","_season-agg.parquet",file_name)))
  
  numeric_cols <- names(data_ex_season_ag_ens)[sapply(data_ex_season_ag_ens, is.numeric)]
  numeric_cols<-numeric_cols[numeric_cols!="n_models"]
  data_ex_season_ag_ens[, (numeric_cols) := lapply(.SD, round, 1), .SDcols = numeric_cols]
  arrow::write_parquet(data_ex_season_ag_ens,file.path(output_dir,gsub(".parquet","_season-agg_ens.parquet",file_name)))
  
  # 3.6) Calculate trends #####
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

