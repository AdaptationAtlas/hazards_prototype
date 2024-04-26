# If working on the CGlabs server please run https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/server_setup.R
# 0) Set-up workspace ####
# 0.1) Load packages #####
if (!require("pacman")) {
  install.packages("pacman")
  require(pacman)
}

# Use p_load to install if not present and load the packages
p_load(arrow, data.table,terra)

# 0.2) Set directories #####
exposure_dir<-"Data/exposure"
if(!dir.exists(exposure_dir)){
  dir.create(exposure_dir)
}

boundary_dir<-"Data/boundaries"
if(!dir.exists(boundary_dir)){
  dir.create(boundary_dir)
}

haz_mean_month_dir<-"Data/hazard_timeseries_mean_month"
if(!dir.exists(haz_mean_month_dir)){
  dir.create(haz_mean_month_dir)
}

haz_risk_dir<-paste0("Data/hazard_risk")
if(!dir.exists(haz_risk_dir)){dir.create(haz_risk_dir,recursive = T)}

# 1) Download data ####
  # 1.1) Geoboundaries #####
# Load and combine geoboundaries
overwrite<-F

geo_files_s3<-c(
  "https://digital-atlas.s3.amazonaws.com/boundaries/atlas-region_admin0_harmonized.gpkg",
  "https://digital-atlas.s3.amazonaws.com/boundaries/atlas-region_admin1_harmonized.gpkg",
  "https://digital-atlas.s3.amazonaws.com/boundaries/atlas-region_admin2_harmonized.gpkg")


geo_files_local<-file.path(boundary_dir,basename(geo_files_s3))
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

  # 1.2) Exposure data #####
# Created in section 0.2 https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/2_risk_x_exposure.R

s3_link<-"https://digital-atlas.s3.amazonaws.com/risk_prototype/data/exposure/exposure_adm_sum.parquet"
ex_file<-file.path(exposure_dir,basename(s3_link))

if(!file.exists(ex_file)){
  download.file(url=geo_files_s3[i],destfile=ex_file)
}

exposure<-arrow::read_parquet(ex_file)

# Show data structure
head(exposure)

# Explanation of fields: 
  # admin0_name = name of country
  # admin1_name = name of admin area level 1
  # admin2_name = name of admin area level 2
  # exposure = name of exposure variable (vop = value of production int$ 2005, vop_usd17 = value of production in current USD 2017, ha = harvested area in hectares, number = number of livestock)
  # crop = name of crop or animal species
  # value = sum across cells in admin extractions

# Available crops
exposure[,unique(crop)]

  # 1.3) Monthly hazard data ####
  # Create here: https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/1.1_create_monthly_haz_tables.R
  s3_link<-"https://digital-atlas.s3.amazonaws.com/risk_prototype/data/hazard/hazard_timeseries_mean_month/all_data.parquet"
  haz_file<-file.path(haz_mean_month_dir,basename(s3_link))
  
  if(!file.exists(haz_file)){
    download.file(url=geo_files_s3[i],destfile=haz_file)
  }
  
  # File is abour 700mb on disk
  haz_timeseries_mean_month<-arrow::read_parquet(haz_file)
  
  # Show data structure
  head(haz_timeseries_mean_month)
  
  # Explanation of fields: 
  # admin0_name = name of country
  # admin1_name = name of admin area level 1
  # admin2_name = name of admin area level 2
  # variable = climate variable see https://github.com/AdaptationAtlas/hazards/wiki/Hazards-definitions.
  # value = mean across cells in admin extractions
  # scenario = ssp (historical,ssp245,ssp585)
  # model = GCM models (ACCESS-ESM1-5, EC-Earth3, INM-CM5-0, MPI-ESM1-2-HR, MRI-ESM2-0)
  # timeframe = historical (1995-2014), near-term (2021-2040), mid-term (2041-2060)
  # year = year of observation
  # month = month of observation
  
# 1.4) Hazard risk data ####
  # See this link for methods: https://github.com/AdaptationAtlas/hazards_prototype/tree/main?tab=readme-ov-file#process-1-calculate-hazard-risk-across-a-timeseries-of-years-or-seasons
  # Created by: https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/1_calculate_risks.R
  
  # Note there is no model in the outputs these values are calculated using an ensemble
  
  # Select cropping calendar for which risk is calculated, in this case it is calculated using the GGCMI Phase 3 crop calendar
  # for maize https://zenodo.org/records/5062513
  cropping_calendar<-"jagermeyr" # The other option is annual, but these are yet to be extracted by admin and uploaded to the s3
  
  haz_risk_dir2<-paste0("Data/hazard_risk/",cropping_calendar)
  if(!dir.exists(haz_risk_dir2)){dir.create(haz_risk_dir2,recursive = T)}
  
  risk_files<-data.table(file=c("haz_risk_int_adm_extreme.parquet",
                                "haz_risk_int_adm_moderate.parquet" ,
                                "haz_risk_int_adm_severe.parquet"   ,
                                "haz_risk_solo_adm_extreme.parquet" ,
                                "haz_risk_solo_adm_moderate.parquet",
                                "haz_risk_solo_adm_severe.parquet"))
  
  risk_files[,s3_path:=paste0("https://digital-atlas.s3.amazonaws.com/risk_prototype/data/hazard_risk/",cropping_calendar,"/",file)
             ][,local_path:=file.path(haz_risk_dir2,file)]
  
  for(i in 1:nrow(risk_files)){
    local_file<-risk_files$local_path[i]
    if(!file.exists(local_file)){
        download.file(url=risk_files[i,s3_path],destfile=local_file)
      }
  }
  
  # Explore data (each file is about 200mb on disk)
  
  # Risk calculated for interaction of 3 hazards
  risk_int<-rbindlist(lapply(risk_files[grep("_int_",local_path),local_path],arrow::read_parquet))
  
  # Risk calculated for each hazard alone, not considering any interactions
  risk_solo<-rbindlist(lapply(risk_files[grep("_solo_",local_path),local_path],arrow::read_parquet))
  

  head(risk_int)
  
  # Explanation of fields: 
  # admin0_name = name of country
  # admin1_name = name of admin area level 1
  # admin2_name = name of admin area level 2
  # value = risk frequency for given hazard_var (0-1)
  # scenario = ssp (historical,ssp245,ssp585)
  # timeframe = historical (1995-2014), near-term (2021-2040), mid-term (2041-2060)
  # year = year of observation  # Explanation of fields: 
  # admin0_name = name of country
  # admin1_name = name of admin area level 1
  # admin2_name = name of admin area level 2
  # variable = climate variable see https://github.com/AdaptationAtlas/hazards/wiki/Hazards-definitions.
  # value = mean across cells in admin extractions
  # scenario = ssp
  # model = GCM models (ACCESS-ESM1-5, EC-Earth3, INM-CM5-0, MPI-ESM1-2-HR, MRI-ESM2-0)
  # timeframe = historical (1995-2014), near-term (2021-2040), mid-term (2041-2060)
  # hazard_vars = hazard code, co-occurrence of 2-3 hazards is indicated by a +. For definitions see https://github.com/AdaptationAtlas/hazards/wiki/Hazards-definitions
  # hazard = simplified name for hazard_vars (any,heat,wet,dry). Any refers to the risk of any hazard or hazard combination occurring.
  # severity = severity class (moderate, severe or extreme)
  
  # These are the generic thresholds used:
  haz_meta<-data.table::fread("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_metadata.csv")
  
  # Crop specific thresholds are derived using ecocrop in this script https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/1_calculate_risks.R
  
  # There is a bug with sweet potato, this will be fixed shortly
  unique(risk_int[grepl("sweet",hazard_vars),list(crop,hazard_vars)]) # crop should be sweet potato
  