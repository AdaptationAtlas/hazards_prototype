# a) Install and load packages ####
packages <- c("terra", 
              "data.table",
              "arrow",
              "geoarrow",
              "countrycode")

# Call the function to install and load packages
pacman::p_load(char=packages)


# 0.1) Load GLPS data #####
glps_file<-list.files(glps_dir,".tif$")

GLPS_thornton<-terra::rast(glps_file)
GLPS_thornton<-terra::classify(GLPS_thornton,matrix(c(1:15,c(0,0,1,2,3,3,4,5,3,3,4,5,6,7,NA)),ncol = 2))

GLPS_vals<-data.frame(Code=0:7,
                      Short=c("LGA", "LGH", "LGT", "MXA", "MXH", "MXT", "Urban", "Other"),
                      LPS=c("Rangelands Arid","Rangelands Humid","Rangelands Temperate","Mixed Arid","Mixed Humid","Mixed Temperate","Urban","Other"))

levels(GLPS_thornton)<-GLPS_vals[,c("Code","LPS")]

# 0.2) Geographies #####
Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
  file<-geo_files_local[i]
  data<-arrow::open_dataset(file)
  data <- data |> sf::st_as_sf() |> terra::vect()
  data
})
names(Geographies)<-names(geo_files_local)
for(i in 1:length(Geographies)){
  crs(Geographies[[i]])<-crs(GLPS_thornton)
}

# 0.3) Set path for cattle number data #####
cattle_no_file<-file.path(glw_dir,"/5_Ct_2015_Da.tif")


# 0.4) Deflators
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

# 0.5) Read in thornton et al. 2022 data #####
econ_loss<-data.table::fread(file.path(cattle_heatstress_dir,"dmi_and_econ_loss.csv"))
econ_loss<-econ_loss[ISO %in% Regions$iso3]
econ_loss<-econ_loss[,`WTD-AV`:=NULL][,Region:=NULL]
setnames(econ_loss,"ISO","iso3")
econ_loss<-melt(econ_loss,id.vars = c("Country","iso3","Variable","Scenario"),variable.name = "GLPS_short")
econ_loss[,value:=gsub(",","",value)][,value:=as.numeric(value)]
econ_loss<-dcast(econ_loss,Country+Scenario+GLPS_short+iso3~Variable)
econ_loss[, GLPS_short := gsub("URB", "Urban", GLPS_short)]
econ_loss[, GLPS_short := gsub("OTHER", "Other", GLPS_short)]

# Add fao deflator ratios to inflate from 2005 to 2020
econ_loss<-merge(econ_loss,deflators,all.x=T,by="iso3")
econ_loss[,meat_loss :=meat_loss *def][,milk_loss:=milk_loss*def][,def:=NULL]

dmi_econ_loss<-dcast(econ_loss,GLPS_short+iso3~Scenario,value.var = "dmi")
colnames(dmi_econ_loss)[-(1:2)]<-paste0("dmi_",colnames(dmi_econ_loss)[-(1:2)])

dmi_fut_cols<-grep("5_",colnames(dmi_econ_loss),value = T)
dmi_diff<-dmi_econ_loss[,..dmi_fut_cols]-dmi_econ_loss[,dmi_historical]
dmi_diff_perc<-round(100*dmi_diff/dmi_econ_loss[,dmi_historical],2)
colnames(dmi_diff)<-gsub("dmi_","dmi_diff_",colnames(dmi_diff))
colnames(dmi_diff_perc)<-gsub("dmi_","dmi_diff_perc_",colnames(dmi_diff_perc))
dmi_econ_loss<-cbind(dmi_econ_loss,dmi_diff,dmi_diff_perc)

meat_econ_loss<-dcast(econ_loss,GLPS_short+iso3~Scenario,value.var = "meat_loss")
colnames(meat_econ_loss)[-(1:2)]<-paste0("meat_",colnames(meat_econ_loss)[-(1:2)])
dmi_econ_loss<-cbind(dmi_econ_loss,meat_econ_loss[,3:7])

milk_econ_loss<-dcast(econ_loss,GLPS_short+iso3~Scenario,value.var = "milk_loss")
colnames(milk_econ_loss)[-(1:2)]<-paste0("milk_",colnames(milk_econ_loss)[-(1:2)])
dmi_econ_loss<-cbind(dmi_econ_loss,milk_econ_loss[,3:7])

# 1) ####
# 1.1) Extract glps by admin area #####

countries<-Geographies$admin0
# Give each countries a unique code
countries$Code<-1:length(countries)*100

GLPS_thornton_masked<-terra::mask(terra::crop(GLPS_thornton,countries),countries)

# Rasterize the admin areas
countries_rast<-terra::rasterize(countries,GLPS_thornton_masked,field="Code")

# Combine glps number with region code
HAZ<-GLPS_thornton_masked+countries_rast
names(HAZ)<-"GLPS"

# Load and resample cattle data
cattle<-terra::rast(livestock_no_file)
cattle<-terra::mask(terra::crop(cattle,countries),countries)
cattle<-cattle/cellSize(cattle,unit="ha")
cattle<-terra::resample(cattle,GLPS_thornton_masked)
cattle<-cattle*cellSize(cattle,unit="ha")

# Calculate zonal stats and convert codes to text
X<-data.table(zonal(cattle,HAZ,fun=sum,na.rm=T))
X[,admin:=floor(GLPS/100)*100]
X[,GLPS:=GLPS-admin]
X[,iso3:=terra::values(countries)[match(X$admin,countries$Code),"iso3"]]
X[,admin:=terra::values(countries)[match(X$admin,countries$Code),"admin_name"]]
X[,GLPS_short:=GLPS_vals[match(X$GLPS,GLPS_vals$Code),"Short"]]
X[,GLPS:=GLPS_vals[match(X$GLPS,GLPS_vals$Code),"LPS"]]
names(X)[2]<-"cattle_prod_tot"
cattle_glps_country<-X

# Choose admin level
admin_choice<-"admin2"

Regions<-Geographies[[admin_choice]]

if(admin_choice=="admin2"){
  Regions$admin_name<-paste0(Regions$admin2_name,"_",Regions$admin1_name,"_",Regions$admin0_name)
}

if(admin_choice=="admin1"){
  Regions$admin_name<-paste0(Regions$admin1_name,"_",Regions$admin1_name)
}

# Mask GLPS to admin area
GLPS_thornton_masked<-terra::mask(terra::crop(GLPS_thornton,Regions),Regions)

# Give each region a unique code
Regions$Code<-1:length(Regions)*100

# Rasterize the admin areas
REG<-terra::rasterize(Regions,GLPS_thornton_masked,field="Code")

# Combine glps number with region code
HAZ<-GLPS_thornton_masked+REG
names(HAZ)<-"GLPS"

# Get area of glps per admin
GLPS_area<-cellSize(GLPS_thornton_masked,unit="ha")

# Calculate zonal stats and convert codes to text
X<-data.table(zonal(GLPS_area,HAZ,fun=sum,na.rm=T))
X[,admin:=floor(GLPS/100)*100]
X[,GLPS:=GLPS-admin]
X[,iso3:=terra::values(Regions)[match(X$admin,Regions$Code),"iso3"]]
X[,admin:=terra::values(Regions)[match(X$admin,Regions$Code),"admin_name"]]
X[,GLPS_short:=GLPS_vals[match(X$GLPS,GLPS_vals$Code),"Short"]]
X[,GLPS:=GLPS_vals[match(X$GLPS,GLPS_vals$Code),"LPS"]]

# Add area of admins
admin_sizes<-data.table(zonal(GLPS_area,REG,fun=sum,na.rm=T))
admin_sizes[,admin:=terra::values(Regions)[match(admin_sizes$Code,Regions$Code),"admin_name"]][,Code:=NULL]
setnames(admin_sizes,"area","admin_unit_ha")

X<-merge(X,admin_sizes,all.x=T,by="admin")
# Work out % of glps per admin area
X[,perc:=round(100*area/admin_unit_ha,2)][,area:=round(area,2)]

GLPS_table<-X

# 1.2) Extract cattle numbers per admin per GLPS #####

# Load and resample cattle data
cattle<-terra::rast(livestock_no_file)
cattle<-terra::mask(terra::crop(cattle,Regions),Regions)
cattle<-cattle/cellSize(cattle,unit="ha")
cattle<-terra::resample(cattle,GLPS_thornton_masked)
cattle<-cattle*cellSize(cattle,unit="ha")

# Calculate zonal stats and convert codes to text
X<-data.table(zonal(cattle,HAZ,fun=sum,na.rm=T))
X[,admin:=floor(GLPS/100)*100]
X[,GLPS:=GLPS-admin]
X[,iso3:=terra::values(Regions)[match(X$admin,Regions$Code),"iso3"]]
X[,admin:=terra::values(Regions)[match(X$admin,Regions$Code),"admin_name"]]
X[,GLPS_short:=GLPS_vals[match(X$GLPS,GLPS_vals$Code),"Short"]]
X[,GLPS:=GLPS_vals[match(X$GLPS,GLPS_vals$Code),"LPS"]]
names(X)[2]<-"cattle_prod"

# Merge total cattle numbers per glps per country
X<-merge(X,cattle_glps_country[,list(GLPS_short,iso3,cattle_prod_tot)],all.x=T,by=c("GLPS_short","iso3"))
X[,cattle_glps_prop:=round(cattle_prod/cattle_prod_tot,3)]
cattle_admin_lps<-X

# 1.3) Merge area and cattle production results #####
GLPS_table<-merge(GLPS_table,cattle_admin_lps[,list(admin,iso3,GLPS_short,cattle_glps_prop)],all.x=T,by=c("admin","iso3","GLPS_short"))

# 1.4) Merge dmi data with GPLS system data #####
GLPS_table<-merge(GLPS_table,dmi_econ_loss,by=c("GLPS_short","iso3"),all.x=T)

