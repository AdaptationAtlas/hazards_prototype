# Please run 0_server_setup.R before executing this script
# If you are experiencing issues with the admin_extract functions, delete the exactextractr package and use this version:  remotes::install_github("isciences/exactextractr")

# a) Install and load packages ####
packages <- c("terra", 
              "data.table",
              "arrow",
              "geoarrow",
              "countrycode")

# Call the function to install and load packages
pacman::p_load(char=packages)


# 0.1) Load glps data #####
glps_file<-list.files(glps_dir,".tif$",full.names = T)

glps_systems<-terra::rast(glps_file)
glps_systems<-terra::classify(glps_systems,matrix(c(1:15,c(0,0,1,2,3,3,4,5,3,3,4,5,6,7,NA)),ncol = 2))

glps_vals<-data.frame(Code=0:7,
                      Short=c("LGA", "LGH", "LGT", "MXA", "MXH", "MXT", "Urban", "Other"),
                      LPS=c("Rangelands Arid","Rangelands Humid","Rangelands Temperate","Mixed Arid","Mixed Humid","Mixed Temperate","Urban","Other"))

levels(glps_systems)<-glps_vals[,c("Code","LPS")]

# 0.2) Geographies #####
Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
  file<-geo_files_local[i]
  data<-arrow::open_dataset(file)
  data <- data |> sf::st_as_sf() |> terra::vect()
  data
})
names(Geographies)<-names(geo_files_local)
for(i in 1:length(Geographies)){
  crs(Geographies[[i]])<-crs(glps_systems)
}

# 0.3) Set path for cattle number data #####
cattle_no_file<-file.path(glw_dir,"/5_Ct_2015_Da.tif")

# 0.4) Deflators #####
# Prepare deflators data 
target_year<-c(2005,2020)

def_file<-file.path(fao_dir,"Deflators_E_All_Data_(Normalized).csv")

deflators<-fread(def_file)
deflators[,M49:=as.numeric(gsub("[']","",`Area Code (M49)`))]
deflators[,iso3:=countrycode::countrycode(sourcevar=M49,origin="un",destination = "iso3c")]
deflators<-deflators[iso3 %in% Geographies$admin0$iso3 & 
                       Year %in% target_year & 
                       Element == "Value US$, 2015 prices" &
                       Item == "Value Added Deflator (Agriculture, forestry and fishery)"
][,deflator:=Value]

deflators<-deflators[,list(iso3,Year,deflator)][,Year:=paste0("D",Year)]
deflators<-dcast(deflators,iso3~Year)
setnames(deflators,paste0("D",target_year),c("def_past","def_target"))
deflators[,def:=def_target/def_past][,c("def_past","def_target"):=NULL]

# 0.5) Read in thornton et al. 2022 economic loss data #####

# Phil T: (what is dmi?) It’s a unitless DMI modification factor to account for cold 
# (so more than the baseline value, i.e. the animal eats more) and heat (less than the baseline value, the animal 
# eats less).  The baseline value depends very much on AEZ etc – so up to 1.1 in some colder places, down to 0.8 or 
# so in other hot/humid places.

econ_loss<-data.table::fread(file.path(cattle_heatstress_dir,"dmi_and_econ_loss.csv"))
econ_loss<-econ_loss[ISO %in% Geographies$admin0$iso3]
econ_loss<-econ_loss[,`WTD-AV`:=NULL][,Region:=NULL]
setnames(econ_loss,"ISO","iso3")
econ_loss<-melt(econ_loss,id.vars = c("Country","iso3","Variable","Scenario"),variable.name = "glps_short")
econ_loss[,value:=gsub(",","",value)][,value:=as.numeric(value)]
econ_loss<-dcast(econ_loss,Country+Scenario+glps_short+iso3~Variable)
econ_loss[, glps_short := gsub("URB", "Urban", glps_short)]
econ_loss[, glps_short := gsub("OTHER", "Other", glps_short)]

# Add fao deflator ratios to inflate from 2005 to 2020
econ_loss<-merge(econ_loss,deflators,all.x=T,by="iso3")
econ_loss[,meat_loss :=meat_loss *def][,milk_loss:=milk_loss*def][,def:=NULL]

dmi_econ_loss<-dcast(econ_loss,glps_short+iso3~Scenario,value.var = "dmi")
colnames(dmi_econ_loss)[-(1:2)]<-paste0("dmi_",colnames(dmi_econ_loss)[-(1:2)])

dmi_fut_cols<-grep("5_",colnames(dmi_econ_loss),value = T)
dmi_diff<-dmi_econ_loss[,..dmi_fut_cols]-dmi_econ_loss[,dmi_historical]
dmi_diff_perc<-round(100*dmi_diff/dmi_econ_loss[,dmi_historical],2)
colnames(dmi_diff)<-gsub("dmi_","dmi_diff_",colnames(dmi_diff))
colnames(dmi_diff_perc)<-gsub("dmi_","dmi_diff_perc_",colnames(dmi_diff_perc))
dmi_econ_loss<-cbind(dmi_econ_loss,dmi_diff,dmi_diff_perc)

meat_econ_loss<-dcast(econ_loss,glps_short+iso3~Scenario,value.var = "meat_loss")
colnames(meat_econ_loss)[-(1:2)]<-paste0("meat_",colnames(meat_econ_loss)[-(1:2)])
dmi_econ_loss<-cbind(dmi_econ_loss,meat_econ_loss[,3:6])

milk_econ_loss<-dcast(econ_loss,glps_short+iso3~Scenario,value.var = "milk_loss")
colnames(milk_econ_loss)[-(1:2)]<-paste0("milk_",colnames(milk_econ_loss)[-(1:2)])
dmi_econ_loss<-cbind(dmi_econ_loss,milk_econ_loss[,3:6])

# 1) ####
# 1.1) Calculate livestock per glps per country #####
countries<-Geographies$admin0
# Give each countries a unique code
countries$Code<-1:length(countries)*100

glps_systems_masked<-terra::mask(terra::crop(glps_systems,countries),countries)

# Rasterize the admin areas
countries_rast<-terra::rasterize(countries,glps_systems_masked,field="Code")

# Combine glps number with region code
glps_x_admin_rast<-glps_systems_masked+countries_rast
names(glps_x_admin_rast)<-"glps"

# Load and resample cattle data
cattle<-terra::rast(cattle_no_file)
cattle<-terra::mask(terra::crop(cattle,countries),countries)
cattle<-cattle/cellSize(cattle,unit="ha")
cattle<-terra::resample(cattle,glps_systems_masked)
cattle<-cattle*cellSize(cattle,unit="ha")

# Calculate zonal stats and convert codes to text
X<-data.table(zonal(cattle,glps_x_admin_rast,fun=sum,na.rm=T))
X[,admin:=floor(glps/100)*100]
X[,glps:=glps-admin]
X[,iso3:=terra::values(countries)[match(X$admin,countries$Code),"iso3"]]
X[,admin:=terra::values(countries)[match(X$admin,countries$Code),"admin_name"]]
X[,glps_short:=glps_vals[match(X$glps,glps_vals$Code),"Short"]]
X[,glps:=glps_vals[match(X$glps,glps_vals$Code),"LPS"]]
names(X)[2]<-"cattle_prod_tot"
cattle_glps_country<-X

# 1.1) Extract and process livestock number and cost per glps and admin area #####

# Choose admin level
admin_levels<-names(Geographies)

heat_impacts<-rbindlist(lapply(1:length(admin_levels),FUN=function(i){
  admin_choice<-admin_levels[i]
  
  cat("Running ",admin_choice,"\n")
  
  focal_admin<-Geographies[[admin_choice]]
  
  if(admin_choice=="admin2"){
    focal_admin$admin_name<-paste0(focal_admin$admin2_name,"_",focal_admin$admin1_name,"_",focal_admin$admin0_name)
  }
  
  if(admin_choice=="admin1"){
    focal_admin$admin_name<-paste0(focal_admin$admin1_name,"_",focal_admin$admin0_name)
  }
  
  # Mask glps to admin area
  glps_systems_masked<-terra::mask(terra::crop(glps_systems,focal_admin),focal_admin)
  
  # Give each region a unique code
  focal_admin$Code<-1:length(focal_admin)*100
  
  # Rasterize the admin areas
  focal_admin_rast<-terra::rasterize(focal_admin,glps_systems_masked,field="Code")
  
  # Combine glps number with region code
  glps_x_admin_rast<-glps_systems_masked+focal_admin_rast
  names(glps_x_admin_rast)<-"glps"
  
  # a) get area of glps per admin ####
  glps_area<-cellSize(glps_systems_masked,unit="ha")
  
  # Calculate zonal stats and convert codes to text
  X<-data.table(zonal(glps_area,glps_x_admin_rast,fun=sum,na.rm=T))
  X[,admin:=floor(glps/100)*100]
  X[,glps:=glps-admin]
  X[,iso3:=terra::values(focal_admin)[match(X$admin,focal_admin$Code),"iso3"]]
  X[,admin:=terra::values(focal_admin)[match(X$admin,focal_admin$Code),"admin_name"]]
  X[,glps_short:=glps_vals[match(X$glps,glps_vals$Code),"Short"]]
  X[,glps:=glps_vals[match(X$glps,glps_vals$Code),"LPS"]]
  
  # Add area of admins
  admin_sizes<-data.table(zonal(glps_area,focal_admin_rast,fun=sum,na.rm=T))
  admin_sizes[,admin:=terra::values(focal_admin)[match(admin_sizes$Code,focal_admin$Code),"admin_name"]][,Code:=NULL]
  setnames(admin_sizes,"area","admin_unit_ha")
  
  X<-merge(X,admin_sizes,all.x=T,by="admin")
  # Work out % of glps per admin area
  X[,perc:=round(100*area/admin_unit_ha,2)][,area:=round(area,2)]
  
  heat_impacts<-X
  
  # b) Extract cattle numbers per admin per glps #####
  
  # Load and resample cattle data
  cattle<-terra::rast(cattle_no_file)
  cattle<-terra::mask(terra::crop(cattle,focal_admin),focal_admin)
  cattle<-cattle/cellSize(cattle,unit="ha")
  cattle<-terra::resample(cattle,glps_systems_masked)
  cattle<-cattle*cellSize(cattle,unit="ha")
  
  # Calculate zonal stats and convert codes to text
  X<-data.table(zonal(cattle,glps_x_admin_rast,fun=sum,na.rm=T))
  X[,admin:=floor(glps/100)*100]
  X[,glps:=glps-admin]
  X[,iso3:=terra::values(focal_admin)[match(X$admin,focal_admin$Code),"iso3"]]
  X[,admin:=terra::values(focal_admin)[match(X$admin,focal_admin$Code),"admin_name"]]
  X[,glps_short:=glps_vals[match(X$glps,glps_vals$Code),"Short"]]
  X[,glps:=glps_vals[match(X$glps,glps_vals$Code),"LPS"]]
  names(X)[2]<-"cattle_prod"
  
  # Merge total cattle numbers per glps per country
  X<-merge(X,cattle_glps_country[,list(glps_short,iso3,cattle_prod_tot)],all.x=T,by=c("glps_short","iso3"))
  X[,cattle_glps_prop:=round(cattle_prod/cattle_prod_tot,3)]
  cattle_admin_lps<-X
  
  # c) Merge area and cattle production results #####
  heat_impacts<-merge(heat_impacts,cattle_admin_lps[,list(admin,iso3,glps_short,cattle_glps_prop)],all.x=T,by=c("admin","iso3","glps_short"))
  
  # d) Merge dmi data with GPLS system data #####
  heat_impacts<-merge(heat_impacts,dmi_econ_loss,by=c("glps_short","iso3"),all.x=T)
  
  # e) Multiply losses by proportion of cattle in that glps x admin combo ####3
  target_cols<-grep("meat|milk",colnames(heat_impacts),value=T)
  heat_impacts[, (target_cols) := lapply(.SD, function(x) round(1000*x * cattle_glps_prop)), .SDcols = target_cols]
  
  # f) Split admin names #####
  if(admin_choice=="admin2"){
    heat_impacts[,admin2_name:=unlist(tstrsplit(admin,"_",keep=1))
                 ][,admin1_name:=unlist(tstrsplit(admin,"_",keep=2))
                   ][,admin0_name:=unlist(tstrsplit(admin,"_",keep=3))
                     ][,admin:=NULL]
  }
  
  if(admin_choice=="admin1"){
    heat_impacts[,admin2_name:=as.character(NA)
                 ][,admin1_name:=unlist(tstrsplit(admin,"_",keep=1))
                   ][,admin0_name:=unlist(tstrsplit(admin,"_",keep=2))
                     ][,admin:=NULL]
  }
  
  if(admin_choice=="admin0"){
    heat_impacts[,admin2_name:=as.character(NA)
                 ][,admin1_name:=as.character(NA)
                   ][,admin0_name:=admin
                     ][,admin:=NULL]
  }
  
  setnames(heat_impacts,"area","glps_area")
  
  return(heat_impacts)
}))

filename<-file.path(cattle_heatstress_dir,"heat_impact_full_results.parquet")
arrow::write_parquet(heat_impacts,filename)

# Melt data
heat_impacts_m<-melt(heat_impacts[,!c("glps_short","cattle_glps_prop","glps_area","admin_unit_ha","perc")],id.vars = c("admin0_name","admin1_name","admin2_name","glps","iso3"))
heat_impacts_m<-merge(heat_impacts_m,cattle_glps_country[,list(iso3,glps,cattle_prod_prop)],all.x=T,by=c("iso3","glps"))

# Calculate importance of glps to cattle production within countries for use as weighting
cattle_glps_country[,admin_tot:=sum(cattle_prod_tot),by=iso3][,cattle_prod_prop:=round(cattle_prod_tot/admin_tot,3)]

# Summarize across glps
heat_impacts_dmi<-heat_impacts_m[grepl("dmi_diff_perc",variable),list(value=weighted.mean(value,cattle_prod_prop,na.rm=T)),by=list(admin0_name,admin1_name,admin2_name,variable)]
heat_impacts_loss<-heat_impacts_m[grepl("meat|milk",variable),list(value=sum(value,na.rm=T)),by=list(admin0_name,admin1_name,admin2_name,variable)]

heat_impacts_m<-rbind(heat_impacts_dmi,heat_impacts_loss)
heat_impacts_m[,variable:=gsub("dmi_diff_perc","dry matter intake",variable)][,timeframe:=unlist(tstrsplit(variable,"_",keep=2))
                                                           ][,scenario:=unlist(tstrsplit(variable,"_",keep=3))
                                                             ][,variable:=unlist(tstrsplit(variable,"_",keep=1))
                                                               ][,value:=round(value,1)
                                                                 ][,scenario:=tolower(scenario)
                                                                   ][variable=="dry matter intake",unit:="percent change"
                                                                     ][variable!="dry matter intake",unit:="USD (2020)"]

heat_impacts_m[is.na(admin1_name) & variable %in% c("meat","milk"),list(value=sum(value)/10^6),by=list(timeframe,scenario)]
heat_impacts_m[is.na(admin1_name) & variable %in% c("dry matter intake"),hist(value)]

filename<-file.path(cattle_heatstress_dir,"heat_impact_simple.parquet")
arrow::write_parquet(heat_impacts_m,filename)



