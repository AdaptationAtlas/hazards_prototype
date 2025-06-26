
pacman::p_load(terra,duckdb,geoarrow,arrow,sf)


# Set choice/meta-data vectors ####

# mapping of spam codes to full crop names
ms_codes_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SpamCodes.csv"
ms_codes<-data.table::fread(ms_codes_url)[compound=="no"][,Code:=tolower(Code)][,Fullname:=gsub(" ","-",Fullname)][!is.na(Code)]

# Set commodity groups
crops<-ms_codes$Fullname
cereals<-ms_codes[cereal==T,Fullname]
legumes<-ms_codes[legume==T,Fullname]
root_tuber<-ms_codes[root_tuber==T,Fullname]
livestock<-c("cattle-highland","cattle-tropical","goats-highland","goats-tropical","pigs-highland","pigs-tropical", 
             "poultry-highland","poultry-tropical","sheep-highland","sheep-tropical")


int_hazards<-c("any","dry","wet","heat","dry+heat","dry+wet","heat+wet","dry+heat+wet")
timeframes<-c("2021-2040","2041-2060","2061-2080","2081-2100")
scenarios<-c("ssp126","ssp245","ssp370","ssp585","ssp")
models<-c("historic","ENSEMBLE")
severities<-c("moderate","extreme","severe")
variables<-c("vop_intld15","vop_usd15")
periods<-c("annual","jagermeyr")
interactions<-c("NDWS+NTx35+NDWL0","NDWS+THI-max+NDWL0")

# Simulated user selections ####
user_selections<-list(severity="severe",
                      unit = "usd15",
                      exposure = "vop",
                      period="annual",
                      interaction="NDWS+NTx35+NDWL0",
                      comparison_scen1=c(scenario=tstrsplit("historic_historic","_",keep=1),
                                         timeframe=tstrsplit("historic_historic","_",keep=2)),
                      comparison_scen2=c(scenario=tstrsplit("ssp585_2041-2060","_",keep=1),
                                         timeframe=tstrsplit("ssp585_2041-2060","_",keep=2)),
                      admin0 = "Kenya",
                      admin1 = NULL,
                      admin2 = NULL,
                      crop = crops)

user_selections$variable<-paste0(user_selections$exposure,"_",user_selections$unit)

# Duckdb connection to admin vector geoparquet table ####

# Load admin vector
admin_0_file<-"s3://digital-atlas/domain=boundaries/type=admin/source=gaul2024/region=africa/processing=simplified/level=adm0/atlas_gaul24_a0_africa_simple-midres.parquet"
admin_1_file<-"s3://digital-atlas/domain=boundaries/type=admin/source=gaul2024/region=africa/processing=simplified/level=adm0/atlas_gaul24_a1_africa_simple-midres.parquet"
admin_2_file<-"s3://digital-atlas/domain=boundaries/type=admin/source=gaul2024/region=africa/processing=simplified/level=adm0/atlas_gaul24_a2_africa_simple-midres.parquet"

# Connect and load extensions
con <- dbConnect(duckdb())
dbExecute(con, "INSTALL httpfs; LOAD httpfs;")
dbExecute(con, "INSTALL spatial; LOAD spatial;")  # Required for geometry columns

# Extract only WKT version of geometry
query <- paste0("
  SELECT admin0_name, ST_AsText(geometry) AS wkt_geom
  FROM '", admin_0_file, "'
  WHERE admin0_name = '",user_selection$admin0,"'
")

admin_df <- dbGetQuery(con, query)

# Convert to sf using WKT
geo_selection <- vect(st_as_sf(admin_df, wkt = "wkt_geom", crs = 4326))
plot(geo_selection)

# Define area of interest in raster CRS (e.g. WGS84 lon/lat) 
aoi <- ext(geo_selection) 

 
# Access total exposure ####
# This will be merged with the hazard exposure data so that we can calculate relative exposure
exp_file<-"s3://digital-atlas/domain=exposure/type=combined/source=glw4+spam2020v1r2_ssa/region=ssa/gaul24_adm0-1-2_exposure.parquet"

# Construct SQL filters
# No crop filter (download all for the selected geographies so user crop selections do not trigger new downloads)
sql_admin0 <- paste(sprintf("'%s'", user_selections$admin0), collapse = ", ")
select_cols<-c("admin0_name","admin1_name","admin2_name","crop","value")
sql_select_cols <- paste(select_cols, collapse = ", ")  

# NULL admin1/admin2 means `IS NULL` in SQL
sql_admin1 <- if (is.null(user_selections$admin1)) {
  "admin1_name IS NULL"
} else {
  sprintf("admin1_name IN (%s)", paste(sprintf("'%s'", user_selections$admin1), collapse = ", "))
}

sql_admin2 <- if (is.null(user_selections$admin2)) {
  "admin2_name IS NULL"
} else {
  sprintf("admin2_name IN (%s)", paste(sprintf("'%s'", user_selections$admin2), collapse = ", "))
}

tech<-"all"

con <- DBI::dbConnect(duckdb::duckdb(), config = list("parquet_enable_dictionary" = FALSE))
## one-time install (downloads the extension into ~/.local/share/R/duckdb/extensions)
dbExecute(con, "INSTALL httpfs")      # need internet access
dbExecute(con, "LOAD httpfs")         # activate it for this session

query <- glue("
  SELECT {sql_select_cols}
  FROM read_parquet('{exp_file}')
  WHERE admin0_name IN ({sql_admin0})
    AND {sql_admin1 |> 
         sub('admin1_name IS NULL', \"COALESCE(admin1_name,'') = ''\", x = _)}
    AND {sql_admin2 |>
         sub('admin2_name IS NULL', \"COALESCE(admin2_name,'') = ''\", x = _)}
    AND unit = '{user_selections$unit}'
    AND (tech = 'all' OR tech IS NULL)
")

exp_tot <- data.table(DBI::dbGetQuery(con, query))
dbDisconnect(con, shutdown = TRUE)

# Sum across non-value cols 
agg_cols<-select_cols[select_cols!="value"]
exp_tot<-exp_tot[,.(value=sum(value,na.rm=T)),by=agg_cols]
setnames(exp_tot,"value","value_tot")

# Use this if you want to download the whole datasets without using duckdb
#full_dat<-read_parquet(exp_file)
#full_dat[,unique(unit)]



# Access Hazard exposure ####
s3_parquet_paths<-data.table(expand.grid(model=models,
                                         severity=severities,
                                         variable=variables,
                                         period=periods,
                                         interaction=interactions))

s3_parquet_paths[,filepath:=paste0("s3://digital-atlas/domain=hazard_exposure/source=atlas_cmip6/region=ssa/processing=hazard-risk-exposure/variable=",
                                 variable,
                               "/period=",period,
                               "/model=",model,
                               "/severity=",severity,
                               "/interaction.parquet")]

haz_exp_hist_path<-s3_parquet_paths[severity == user_selections$severity & 
                                 period == user_selections$period &
                                 variable == user_selections$variable &
                                 interaction == user_selections$interaction &
                                 model == "historic",filepath]

haz_exp_ens_path<-s3_parquet_paths[severity == user_selections$severity & 
                                period == user_selections$period &
                                variable==user_selections$variable &
                                interaction == user_selections$interaction &
                                model == "ENSEMBLE",filepath]


# Connect to an in-memory DuckDB instance

haz_s3q<-function(path,crops,admin0,admin1,admin2,timeframe,scenario,hazard_vars,select_cols){
  con <- dbConnect(duckdb::duckdb())
   on.exit(dbDisconnect(con, shutdown = TRUE))

  if(timeframe=="historic"){
    select_cols<-select_cols[select_cols!="value_sd"]
  }
  
  # Construct SQL filters
  sql_crop <- paste(sprintf("'%s'", crops), collapse = ", ")
  sql_admin0 <- paste(sprintf("'%s'", admin0), collapse = ", ")
  sql_select_cols <- paste(select_cols, collapse = ", ")  
  
  # NULL admin1/admin2 means `IS NULL` in SQL
  sql_admin1 <- if (is.null(admin1)) {
    "admin1_name IS NULL"
  } else {
    sprintf("admin1_name IN (%s)", paste(sprintf("'%s'", admin1), collapse = ", "))
  }
  
  sql_admin2 <- if (is.null(admin2)) {
    "admin2_name IS NULL"
  } else {
    sprintf("admin2_name IN (%s)", paste(sprintf("'%s'", admin2), collapse = ", "))
  }

  
  # Build the query
  query <- sprintf("
    SELECT %s
    FROM read_parquet('%s')
    WHERE crop IN (%s)
      AND admin0_name IN (%s)
      AND %s
      AND %s
      AND timeframe = '%s'
      AND scenario = '%s'
      AND hazard_vars = '%s'
  ", 
                   sql_select_cols,
                   path,
                   sql_crop,
                   sql_admin0,
                   sql_admin1,
                   sql_admin2,
                   timeframe,
                   scenario,
                   hazard_vars
  )
  
  # Execute the query
  result <- data.table(dbGetQuery(con, query))
  
  # In contested areas sometimes mean admin0/admin1 units are split, sum results over split areas to merge them with parent country
  agg_cols <- setdiff(select_cols, c("value", "value_sd"))

    if(timeframe=="historic"){
   result<-result[,.(value=sum(value,na.rm=T)),by=agg_cols]
   result[,value_sd:=NA_real_]
  }else{
   result<-result[,.(value=sum(value,na.rm=T),value_sd=sum(value_sd,na.rm=T)),by=agg_cols]
  }
  
  do.call(setorder, c(list(result), select_cols))
  
  return(result)
}

select_cols <- c("severity","scenario","timeframe","admin0_name", "admin1_name", "admin2_name", "crop", "hazard","value", "value_sd")

haz_s1<-haz_s3q(path = if(user_selections$comparison_scen1$scenario=="historic"){haz_exp_hist_path}else{haz_exp_ens_path},
                      crops = user_selections$crop,
                      admin0 = user_selections$admin0,
                      admin1 = user_selections$admin1,
                      admin2 = user_selections$admin2,
                      timeframe =  user_selections$comparison_scen1$timeframe,
                      scenario =  user_selections$comparison_scen1$scenario,
                      hazard_vars = user_selections$interaction,
                      select_cols = select_cols)  

haz_s2<-haz_s3q(path = if(user_selections$comparison_scen2$scenario=="historic"){haz_exp_hist_path}else{haz_exp_ens_path},
                    crops = user_selections$crop,
                    admin0 = user_selections$admin0,
                    admin1 = user_selections$admin1,
                    admin2 = user_selections$admin2,
                    timeframe =  user_selections$comparison_scen2$timeframe,
                    scenario =  user_selections$comparison_scen2$scenario,
                    hazard_vars = user_selections$interaction,
                    select_cols = select_cols)

if(nrow(haz_s1)!=nrow(haz_s2)){
  warning("Number of rows in scenario 1",nrow(haz_s1),"does not match scenario 2",nrow(haz_s2))
}
  
  # Merge scenarios into a single table
  setnames(haz_s1,c("value","value_sd"),c("value1","value_sd1"),skip_absent=T)
  setnames(haz_s2,c("value","value_sd"),c("value2","value_sd2"),skip_absent=T)
  haz_s1[,scenario1:=paste(unique(c(scenario,timeframe)),collapse="-")][,c("scenario","timeframe"):=NULL]
  haz_s2[,scenario2:=paste(unique(c(scenario,timeframe)),collapse="-")][,c("scenario","timeframe"):=NULL]

  haz_merge<-cbind(haz_s1,haz_s2[,.(scenario2,value2,value_sd2)])
  
  # Add 1 only, 2 only, 3 only hazard categories
  agg_cols <- c("severity","scenario1","scenario2","admin0_name", "admin1_name", "admin2_name", "crop", "hazard")
  
  num_cols <- names(haz_merge)[
    sapply(haz_merge, is.numeric) & !names(haz_merge) %in% agg_cols
  ]
  
  haz1<-haz_merge[!is.na(hazard) & !grepl("\\+", hazard) & hazard!="any"]
  haz1[,hazard:="1 hazard"]
  haz2<-haz_merge[!is.na(hazard) & lengths(regmatches(hazard, gregexpr("\\+", hazard))) == 1]
  haz2[,hazard:="2 hazards"]
  haz3<-haz_merge[!is.na(hazard) & lengths(regmatches(hazard, gregexpr("\\+", hazard))) >= 2]
  haz3[,hazard:="3 hazards"]
  
  haz123<-rbind(haz1,haz2,haz3)
  
  # Sum values for the new categories
  haz123 <- haz123[, lapply(.SD, sum, na.rm = TRUE), by = agg_cols, .SDcols = num_cols]
  
  # Bind to the main dataset
  haz_merge<-rbind(haz_merge,haz123)
  
  # Add any heat, any wet, and any dry ####
  heat<-haz_merge[grep("heat",hazard)]
  heat[,hazard:="heat (any)"]
  
  wet<-haz_merge[grep("wet",hazard)]
  wet[,hazard:="wet (any)"]
  
  dry<-haz_merge[grep("dry",hazard)]
  dry[,hazard:="dry (any)"]
  
  # Sum values for the new categories
  dhw<-rbind(dry,heat,wet)
  
  # Sum values for the new categories
  dhw <- dhw[, lapply(.SD, sum, na.rm = TRUE), by = agg_cols, .SDcols = num_cols]
  
  # Bind to the main dataset
  haz_merge<-rbind(haz_merge,dhw)
  
  # Merge in total exposure values (i.e. the total value of production or harvested areas)
  haz_merge<-merge(haz_merge,exp_tot,by=c("admin0_name","admin1_name","admin2_name","crop"),all.x=T,sort=F)
  
  # Calculate no hazard
  no_haz<-haz_merge[hazard=="any"]
  no_haz[,value1:=value_tot-value1
         ][,value_sd1:=((value_tot-value1)/value1)*value_sd1
           ][,value2:=value_tot-value2
             ][,value_sd2:=((value_tot-value2)/value2)*value_sd2
               ][,hazard:="no hazard"]

  haz_merge<-rbind(haz_merge,no_haz)
  
  # Add totals
  # Grand total across all geos and crops
  agg_cols <- c("severity","scenario1","scenario2","admin0_name", "admin1_name", "admin2_name", "crop", "hazard")
  
  num_cols <- names(haz_merge)[
    sapply(haz_merge, is.numeric) & !names(haz_merge) %in% agg_cols
  ]
  
  agg_cols <- c("severity","scenario1","scenario2", "hazard")
  haz_tot <- haz_merge[, lapply(.SD, sum, na.rm = TRUE), by = agg_cols, .SDcols = num_cols]
  haz_tot[,c("admin0_name","admin1_name","admin2_name","crop"):="all"]
  
  # Grand total across all crops
  agg_cols <- c("severity","scenario1","scenario2","admin0_name", "admin1_name", "admin2_name", "hazard")
  haz_tot_geo <- haz_merge[, lapply(.SD, sum, na.rm = TRUE), by = agg_cols, .SDcols = num_cols]
  haz_tot_geo<-haz_tot_geo[,crop:="all"]
  
  # Bind back grouped data
  haz_merge<-rbind(haz_merge,haz_tot,haz_tot_geo)
  
  # Calculate difference
  haz_merge[,diff:=value2-value1] 
  
  # Calulate % exposure to hazards
  ci_95 <- function(mean, sd, n) {
    error <- qt(0.975, df = n - 1) * sd / sqrt(n)
    lower <- mean - error
    upper <- mean + error
    return(data.table(lower = lower, upper = upper))
  }
  
    haz_merge[,perc1:=round(100*value1/value_tot,2)
              ][,perc_sd1:=round(100*value_sd1/value_tot,2)
                ][,cv1:=round(100*value_sd1/value1,2)
                  ][,perc2:=round(100*value2/value_tot,2)
                    ][,perc_sd2:=round(100*value_sd2/value_tot,2)
                      ][,cv2:=round(100*value_sd2/value2,2)
                        ][,perc_diff:=round(100*diff/value_tot,2)]
  
  haz_merge[,c("value1_low","value1_high"):=ci_95(value1,value_sd1,5),by=.I]
  haz_merge[,c("perc1_low","perc1_high"):=round(ci_95(perc1,perc_sd1,5),2),by=.I]
  haz_merge[,c("value2_low","value2_high"):=ci_95(value2,value_sd2,5),by=.I]
  haz_merge[,c("perc2_low","perc2_high"):=round(ci_95(perc2,perc_sd2,5),2),by=.I]
  
  # Rename heat, dry, wet to heat_only etc.
  haz_merge[hazard=="heat",hazard:="heat (only)"]
  haz_merge[hazard=="wet",hazard:="wet (only)"]
  haz_merge[hazard=="dry",hazard:="dry (only)"]
  
  # Q1: Upset plot
  
  # Q2: Crops>Hazards
  
  # Q3: Hazards>Crops
  
  # Q4: Geographies
  
  # Q5: Variability   
  
  

# COG tif plotting ####
  s3_crop_tif_paths<-data.table(expand.grid(model=c("historic","ENSEMBLEmean","ENSEMBLEsd"),
                                            severity=c("moderate","extreme","severe"),
                                            variable=c("vop_intld15","vop_usd15"),
                                            period=c("annual","jagermeyr"),
                                            interaction=c("NDWS+NTx35+NDWL0","NDWS+THI-max+NDWL0"),
                                            crop=crops))
  
  s3_crop_tif_paths[,file_path:=paste0("s3://digital-atlas/domain=hazard_exposure/source=atlas_cmip6/region=ssa/processing=hazard-risk-exposure/variable=",
                                       variable,"/period=",period,"/model=",model,"/severity=",severity,"/interaction=",interaction,"/",crop,".tif")]
  
  user_selections_crop_tifs<-list(crop="maize",timeframe="2021-2040",scenario="ssp585",int_hazard="any")
  
  s3_crop_tif<-s3_crop_tif_paths[severity==user_selections$severity & 
                                   variable==user_selections$variable & 
                                   period==user_selections$period & 
                                   interaction==user_selections$interaction &
                                   crop == user_selections_crop_tifs$crop,file_path]
  
  # If we need relative values we will need to get total exposure for the selected crop
  if(user_selections_crop_tifs$crop %in% crops){
    tot_file<-paste0(
      "s3://digital-atlas/domain=exposure/type=crop/source=spam2020v1r2_ssa/region=ssa/processing=analysis-ready/variable=", 
      user_selections$variable,"/spam_",user_selections$variable,"_all.tif")
    user_selection_crop_code<-ms_codes[Fullname==user_selections_crop_tifs$crop,Code]
    r_tot<-rast(tot_file)[[user_selection_crop_code]]
  }else{
    tot_file<-paste0(
      "s3://digital-atlas/domain=exposure/type=livestock/source=glw4/region=ssa/time=2015/processing=atlas-harmonized/variable=",
      user_selections$variable,"/glw4_",user_selections$variable,".tif")
    
  }
  
  r_tot <- mask(crop(r_tot, aoi),geo_selection)
  
  
  
  # Connect to remote COG – this does NOT download the full file
  r_fut <- rast(s3_crop_tif[grepl("ENSEMBLEmean",s3_crop_tif)])
  r_fut<-r_fut[[grep(paste0(user_selections_crop_tifs$scenario,".*",user_selections_crop_tifs$timeframe),names(r_fut),value=T)]]
  r_hist <- rast(s3_crop_tif[grepl("historic",s3_crop_tif)])
  
  names(r_hist)<-unlist(tstrsplit(names(r_hist),"_",keep=4))
  names(r_fut)<-unlist(tstrsplit(names(r_fut),"_",keep=4))
  
  r_hist<-r_hist[[user_selections_crop_tifs$int_hazard]]
  r_fut<-r_fut[[user_selections_crop_tifs$int_hazard]]
  
  r<-c(r_hist,r_fut)
  names(r)<-paste(paste(user_selections_crop_tifs$int_hazard,"hazard: "),c("historical",paste(user_selections_crop_tifs$scenario,user_selections_crop_tifs$timeframe)))
  
  # Crop – this triggers a HTTP range request to fetch only needed tiles
  r_crop <- mask(crop(r, aoi),geo_selection)
  r_crop_diff<-r_crop[[2]] - r_crop[[1]]
  # Plot cropped data
  plot(r_crop)
  plot(r_crop_diff)
  
  
  
