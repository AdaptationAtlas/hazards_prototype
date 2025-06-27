
pacman::p_load(terra,duckdb,geoarrow,arrow,sf,ggupset,ggplot2)
options(scipen = 999)

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
  # Rename fields
  setnames(haz_s1,c("value","value_sd"),c("value1","value_sd1"),skip_absent=T)
  setnames(haz_s2,c("value","value_sd"),c("value2","value_sd2"),skip_absent=T)
  haz_s1[,scenario1:=paste(unique(c(scenario,timeframe)),collapse="-")][,c("scenario","timeframe"):=NULL]
  haz_s2[,scenario2:=paste(unique(c(scenario,timeframe)),collapse="-")][,c("scenario","timeframe"):=NULL]

  # Merge scenarios into a single table ####
  haz_merge<-cbind(haz_s1,haz_s2[,.(scenario2,value2,value_sd2)])
  
  # Add 1 only, 2 only, 3 only hazard categories ####
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
  
  # Sum values for the new categories ####
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
  
  # Sum values for the new categories ####
  dhw <- dhw[, lapply(.SD, sum, na.rm = TRUE), by = agg_cols, .SDcols = num_cols]
  
  # Bind to the main dataset
  haz_merge<-rbind(haz_merge,dhw)
  
  # Merge in total exposure values (i.e. the total value of production or harvested areas) ####
  haz_merge<-merge(haz_merge,exp_tot,by=c("admin0_name","admin1_name","admin2_name","crop"),all.x=T,sort=F)
  
  # Calculate no hazard ####
  no_haz<-haz_merge[hazard=="any"]
  no_haz[,value1:=value_tot-value1
         ][,value_sd1:=((value_tot-value1)/value1)*value_sd1
           ][,value2:=value_tot-value2
             ][,value_sd2:=((value_tot-value2)/value2)*value_sd2
               ][,hazard:="no hazard"]

  haz_merge<-rbind(haz_merge,no_haz)
  
  # Add totals ####
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
  
  # Grand total across all geo
  agg_cols <- c("severity","scenario1","scenario2","crop","hazard")
  haz_tot_crop <- haz_merge[, lapply(.SD, sum, na.rm = TRUE), by = agg_cols, .SDcols = num_cols]
  haz_tot_crop[,c("admin0_name","admin1_name","admin2_name"):="all"]
  
  # Bind back grouped data
  haz_merge<-rbindlist(list(haz_merge,haz_tot,haz_tot_geo,haz_tot_crop),use.names=T)
  
  # Calculate difference ####
  haz_merge[,diff:=value2-value1]
  
  # Calculate relative exposure & errors ####
  ci_95 <- function(mean, sd, n) {
    error <- qt(0.975, df = n - 1) * sd / sqrt(n)
    lower <- mean - error
    upper <- mean + error
    return(data.table(lower = lower, upper = upper))
  }
  
  # enforce sd being all NA if scenario is historic
  haz_merge[scenario1=="historic",value_sd1:=NA]
  haz_merge[scenario2=="historic",value_sd2:=NA]
  
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
  
  # Rename hazard labels and create hazard sets ####
  haz_merge[hazard=="heat",hazard:="heat (only)"]
  haz_merge[hazard=="wet",hazard:="wet (only)"]
  haz_merge[hazard=="dry",hazard:="dry (only)"]
  haz_merge[hazard=="any",hazard:="any hazard"]
  
  # Define haz sets
  compound_set_full<-c("no hazard","dry (only)","heat (only)","wet (only)","dry+heat","dry+wet","heat+wet","dry+heat+wet")
  compound_set_simple<-c("no hazard","1 hazard","2 hazards","3 hazards")
  solo_set<-c("no hazard","dry (any)","heat (any)","wet (any)","any")
  
  # Create palettes
  # Build a pallete 
  mix_hex <- function(col1, col2, p = 0.5) {
    stopifnot(is.numeric(p), p >= 0, p <= 1)
    rgb <- grDevices::colorRamp(c(col1, col2), space = "Lab")(p)   # 0–255
    grDevices::rgb(rgb[1], rgb[2], rgb[3], maxColorValue = 255)    # back to hex
  }
  
  #Pick three base hues for the single hazards
  palette<-"Set 2"
  base_hues <- qualitative_hcl(3, palette = palette)   # any palette is fine
  names(base_hues) <- c("dry", "heat", "wet")
  
  # Build a named vector of final fill colours
  cols_fill <- c(
    ## 0-hazard (grey)
    "no hazard"      = "grey85",
    
    ## 1-hazard (very light versions of the bases)
    "dry (only)"     = as.character(lighten(base_hues["dry"],  .5)),
    "heat (only)"    = as.character(lighten(base_hues["heat"], .5)),
    "wet (only)"     = as.character(lighten(base_hues["wet"],  .5)),
    
    ## 2-hazard (medium lightness – mix two bases, then lighten a bit)
    "dry+heat"       = lighten(mix_hex(base_hues["dry"],  base_hues["heat"], .50), -0.1),
    "dry+wet"        = lighten(mix_hex(base_hues["dry"],  base_hues["wet"],  .50), -0.1),
    "heat+wet"       = lighten(mix_hex(base_hues["heat"], base_hues["wet"],  .50), -0.1),
    
    ## 3-hazard (darkest – mix the three bases 1/3-1/3-1/3)
    "dry+heat+wet"   = lighten(mix_hex(
      mix_hex(base_hues["dry"], base_hues["heat"], .50),
      base_hues["wet"], .67),-0.5)
  )
  
  cols_fill_simple <- c(
    "no hazard"      = "grey85",
    "1 hazard"     = as.character(lighten(base_hues["dry"],  .7)),
    "2 hazards"     = as.character(lighten(base_hues["dry"],  .35)),
    "3 hazards"     = as.character(base_hues["dry"])
  )
  
  cols_fill_solo <- c(
    "no hazard"      = "grey85",
    "heat (any)"     = as.character(base_hues["heat"]),
    "dry (any)"     = as.character(base_hues["dry"]),
    "wet (any)"     = as.character(base_hues["wet"]),
    "any"           = lighten(mix_hex(
      mix_hex(base_hues["dry"], base_hues["heat"], .50),
      base_hues["wet"], .67),-0.5)
  )
  
  # Plots ####
  # Q1: Upset plot ####
  
  # Bonus points if error_bars can be added
  q1_dat<-data[admin0_name=="all" & hazard %in% compound_set_full]

  
  up_plot<-function(data,val_col,exp_lab,plot_title,val_alt=NULL){
  
  q1_dat<-data
  
  q1_dat[, dry  := grepl("\\bdry\\b",  hazard)]
  q1_dat[, heat := grepl("\\bheat\\b", hazard)]
  q1_dat[, wet  := grepl("\\bwet\\b",  hazard)]

  # Convert logicals to hazard list column
  q1_dat[, hazards := apply(.SD, 1, function(row) names(row)[as.logical(row)]), .SDcols = c("dry", "heat", "wet")]

  setnames(q1_dat,val_col,"value")
  
  if(!is.null(val_alt)){
    q1_dat[,valx:=value]
  }else{
    q1_dat[,valx:=val_alt,with=F]
  }
  
  if(q1_dat[,max(valx)>10^9]){
    q1_dat[,value:=value/10^9]
    lab<-paste0("Exposure B ",exp_lab)
    }else{
  if(q1_dat[,max(valx)>10^6]){
    q1_dat[,value:=value/10^6]
    lab<-paste0("Exposure M ",exp_lab)
  }else{
    if(q1_dat[,max(valx)>10^3]){
      q1_dat[,value:=value/10^3]
      lab<-paste0("Exposure k ",exp_lab)
    }else{
      lab<-paste0("Exposure ",exp_lab)
    }}}
  
  # Use ggupset to make upset ploy
  p_up<-ggplot(q1_dat, aes(x = hazards, y = value)) +
    geom_col(fill = "grey40") +
    geom_text(aes(label = scales::comma(value)), vjust = -0.3, size = 3) +
    scale_x_upset(order_by = NULL) +
    labs(
      title = plot_title,
      x = NULL,
      y = lab
    ) +
    theme_minimal()+
    theme(
      axis.text.x        = element_blank(),
      axis.ticks.x       = element_blank(),
      panel.grid = element_blank(),
      plot.margin        = margin(t = 0, r = 0, b = 0, l = 0)  
    )
  
  # Make bars for total hazard exposure
  haz_cols <- c("dry","heat","wet")
  
  single_totals <- data.table(
    set      = haz_cols,
    exposure = sapply(haz_cols, \(s) q1_dat[get(s) == TRUE, sum(value, na.rm = TRUE)])
  )[order(set)]  
  
  # Need to ensure order of bars is the same as the upset plot, it works in this example but needs further testing
  
    p_set<-ggplot(single_totals,
                   aes(x = exposure,
                       y = factor(set, levels = rev(single_totals$set)))) +
    geom_col(width = .55, fill = "grey40") +
    geom_text(aes(label = comma(exposure)),
              hjust = 1.05, colour = "white", size = 3) +
    scale_x_reverse(position = "top",expand = expansion(mult = c(0, 0))) +  # ← reverse!
    coord_cartesian(clip = "off") +
    theme(
      axis.text.y        = element_blank(),
      axis.ticks.y       = element_blank(),
      axis.title = element_blank(),
      panel.grid = element_blank(),
      panel.background = element_blank(),
      plot.margin        = margin(t = 0, r = 0, b = 0, l = 0)  
    )
    
    # Assemble plots using patchwork
    design <- "
A
B
"  
    #  upset panel 
    plot_right <-wrap_plots(
        A =p_up,   # empty top-left cell
        B = patchwork::plot_spacer(), 
        design  = design,
        heights = c(0.9999, 0.0001)      
      )
    
    # total bars   
    plot_left <-wrap_plots(
      A =patchwork::plot_spacer(),  
      B = p_set, 
      design  = design,
      heights = c(0.93, 0.07)      
    )

    # merged plot
    final_plot <-
      (plot_left | plot_right) +         
      plot_layout(widths = c(3, 7))  
    
    return(final_plot)
  }
  s1_lab<-haz_merge$scenario1[1]
  s2_lab<-haz_merge$scenario2[1]
  diff_lab<-paste0("Δ ",haz_merge$scenario2[1]," minus ",plot_title=haz_merge$scenario1[1])
  
  up_s1<-up_plot(data=q1_dat,val_col = "value1",exp_lab=user_selections$unit,plot_title=s1_lab) 
  up_s2<-up_plot(data=q1_dat,val_col = "value2",exp_lab=user_selections$unit,plot_title=s2_lab)
  # An issue with difference is that the unit changes from B to M, we might want to set an override where the unit selection can be derived from
  # a different column
  up_diff<-up_plot(data=q1_dat,val_col = "diff",exp_lab=user_selections$unit,plot_title=diff_lab)
  
  up_s1p<-up_plot(data=q1_dat,val_col = "perc1",exp_lab="%",plot_title=s1_lab) 
  up_s2p<-up_plot(data=q1_dat,val_col = "perc2",exp_lab="%",plot_title=s2_lab) 
  up_diffp<-up_plot(data=q1_dat,val_col = "perc_diff",exp_lab="%",plot_title=diff_lab) 
  
      
  # Q2: Crops>Hazards ####

  q2_fun<-function(plot_dat,
                   exp_lab,
                   n_y.groups=NULL,
                   y_axis = "crop",
                   val_col,
                   haz_levels,
                   cols_fill,
                   plot_title,
                   is_compound=T,
                   plotly=T){

  plot_dat[,value:=plot_dat[[val_col]]]
  plot_dat[,y_axis:=plot_dat[[y_axis]]]

  if(grepl("diff",val_col)){
    y_order <- plot_dat[hazard!="no hazard" & value>0, .(tot = sum(value)), by = y_axis][
    order(-tot), y_axis]
  }else{
    y_order <- plot_dat[hazard!="no hazard" , .(tot = sum(value)), by = y_axis][
      order(-tot), y_axis]
  }
  
  if(!is.null(n_y.groups)){
    y_order<-y_order[1:n_y.groups]
    plot_dat<-plot_dat[y_axis %in% y_order]
  }
  
  plot_dat[ , y_axis := factor(y_axis, levels = rev(yorder))]
  plot_dat[ , hazardf := factor(hazard, levels = haz_levels)]
  
  result<-ggplot(plot_dat,
                 aes(x = value,
                     y = y_axis,
                     fill = hazardf, 
                     text = paste0("Y-group: ", y_axis, "<br>",
                                   "Hazard: ", hazard, "<br>",
                                   "Exposure: ", value))) +             
    scale_x_continuous(labels = label_number(scale_cut = cut_short_scale())) +
    scale_fill_manual(values = cols_fill,
                      breaks  = haz_levels,     # keep the legend in that order
                      drop    = FALSE) +  
    labs(x = lab,
         y = NULL,
         title = plot_title,
         fill = "Hazard") +                 # legend title
    theme_minimal(base_size = 12) +
    theme(panel.grid.major.y = element_blank())
  
  if(is_compound==T){
    result<-result+geom_bar(stat="identity",colour="grey20",size=0.2)
  }else{
    result<-result+geom_col(position = position_dodge(width = 0.9)) 
  }
  
  if(plotly){
    result<-ggplotly(result,tooltip = "text")
  }
  return(result)
  
  }
  
  # n_crops to show
  n_crops<-15
  if(n_crops>length(user_selections$crop)){
    n_crops<-length(user_selections$crop)
  }
  
  q2_s1<-q2_fun(plot_dat=haz_merge[admin0_name=="all" & crop!="all" & hazard %in% compound_set_full],
         exp_lab = user_selections$unit,
         n_y.groups=n_crops,
         val_col="value1",
         haz_levels = compound_set_full,
         cols_fill = cols_fill,
         plot_title=haz_merge$scenario1[1])
  
  q2_s2<-q2_fun(plot_dat=haz_merge[admin0_name=="all" & crop!="all" & hazard %in% compound_set_full],
                exp_lab = user_selections$unit,
                n_y.groups=n_crops,
                val_col="value2",
                haz_levels = compound_set_full,
                cols_fill = cols_fill,
                plot_title=haz_merge$scenario2[1])
  
  q2_diff<-q2_fun(plot_dat=haz_merge[admin0_name=="all" & crop!="all" & hazard %in% compound_set_full],
                exp_lab = user_selections$unit,
                n_y.groups=n_crops,
                val_col="diff",
                haz_levels = compound_set_full,
                cols_fill = cols_fill,
                plot_title=haz_merge$scenario2[1])
  
  q2_s1p<-q2_fun(plot_dat=haz_merge[admin0_name=="all" & crop!="all" & hazard %in% compound_set_full],
                palette="Set 1",
                exp_lab = "%",
                n_crops=n_crops,
                val_col="perc1",
                haz_levels = compound_set_full,
                cols_fill = cols_fill,
                plot_title=haz_merge$scenario1[1])
  
  q2_s2p<-q2_fun(plot_dat=haz_merge[admin0_name=="all" & crop!="all" & hazard %in% compound_set_full],
                exp_lab = user_selections$unit,
                n_y.groups=n_crops,
                val_col="perc2",
                haz_levels = compound_set_full,
                cols_fill = cols_fill,
                plot_title=haz_merge$scenario2[1])
  
  q2_diffp<-q2_fun(plot_dat=haz_merge[admin0_name=="all" & crop!="all" & hazard %in% compound_set_full],
                  exp_lab = user_selections$unit,
                  n_y.groups=n_crops,
                  val_col="perc_diff",
                  haz_levels = compound_set_full,
                  cols_fill = cols_fill,
                  plot_title=haz_merge$scenario2[1])
  
  # Simple versions ofcompound plot
  q2_s1<-q2_fun(plot_dat=haz_merge[admin0_name=="all" & crop!="all" & hazard %in% compound_set_simple],
                exp_lab = user_selections$unit,
                n_y.groups=n_crops,
                val_col="value1",
                haz_levels = compound_set_simple,
                cols_fill = cols_fill_simple,
                plot_title=haz_merge$scenario1[1])
  
  # Non compound plot
  q2_s1nc<-q2_fun(plot_dat=haz_merge[admin0_name=="all" & crop!="all" & hazard %in% solo_set],
                exp_lab = user_selections$unit,
                n_y.groups=n_crops,
                val_col="value1",
                is_compound=F,
                haz_levels = solo_set,
                cols_fill = cols_fill_solo,
                plot_title=haz_merge$scenario1[1])
  
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
  
  
  
