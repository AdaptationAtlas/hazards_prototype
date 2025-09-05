# a) Load R functions & packages ####
pacman::p_load(terra,data.table,httr,countrycode,wbstats,arrow,geoarrow,dplyr,tidyr,pbapply)

# Load functions & wrappers
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))
options(scipen=999)

# b) Set up workspace ####
spam_dir<-list.files(mapspam_pro_dir,"prod_t",full.names = T)
geo_dir<-boundaries_dir

# Mapspam code tables
ms_codes_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SpamCodes.csv"
spam2fao_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SPAM2010_FAO_crops.csv"

# 1) Load geographies ####
file<-geo_files_local[1]
geoboundaries<-read_parquet(file)
geoboundaries <- geoboundaries |> sf::st_as_sf() |> terra::vect()
geoboundaries <- aggregate(geoboundaries, "iso3")  

# 2) Load mapspam data ####

### 2.1) Stack files & rename
# Set base raster. Note the use of the atlas_delta base raster is deliberate.
base_rast<-rast(atlas_data$base_rast$atlas_delta$local_path)

# Rasterize geoboundaries
admin_rast<-terra::rasterize(geoboundaries, base_rast, field = "iso3")
admin_rast<-crop(admin_rast,base_rast)
ms_codes<-data.table::fread(ms_codes_url)[,Code:=toupper(Code)][,Code_ifpri_2020:=toupper(Code_ifpri_2020)]
ms_codes<-ms_codes[compound=="no" & !is.na(Code_ifpri_2020) & !is.na(Code)]

# List spam files
files_raw<-list.files(spam_dir,".tif",full.names = T)

spam_dat<-pblapply(1:length(files_raw),function(i){
  file<-files_raw[i]
  dat<-rast(file)
  # fao stat combines robusta and arabica coffee so create a merged production value for the two
  dat$coffee<-dat$`arabica coffee`+dat$`robusta coffee`
  return(dat)
  })

tech<-gsub(".tif","",unlist(tstrsplit(basename(files_raw),"_",keep=4)))
names(spam_dat)<-tech

## 2.4) Extract spam totals by geoboundaries #####
# Use terra::zonal to sum production values by administrative unit
iso3_levels<-levels(admin_rast)[[1]]
spam_prod_admin0_ex<-pblapply(1:length(spam_dat),function(i){
  dat<-spam_dat[[i]]
  ex_dat<-data.table(terra::zonal(dat, admin_rast, fun = "sum", na.rm = TRUE))
  ex_dat<-melt(data.table(ex_dat),id.vars="iso3",variable.name = "Code",value.name="prod")
  ex_dat<-merge(ex_dat,iso3_levels,by="iso3",all.x=T)
  ex_dat[,tech:=names(spam_dat)[i]]
  return(ex_dat)
})
names(spam_prod_admin0_ex)<-tech

## 2.5) map spam codes values to atlas #####
spam2fao<-fread(spam2fao_url)[,short_spam2010:=toupper(short_spam2010)][short_spam2010 %in% c(ms_codes$Code,"COFF")]

## 2.6) Create proportions ####

# Create a raster stack directly from spam_prod_admin0_ex
spam_prop<-lapply(1:length(spam_prod_admin0_ex),function(i){
  cat("Spam tech",i,"/",length(spam_prod_admin0_ex),"\n")
  dat<-spam_prod_admin0_ex[[i]]
  raw_dat<-spam_dat[[i]]
  crops<-names(raw_dat)
  spam_tot <- terra::rast(pblapply(1:length(crops), function(j) {
    crop<-crops[j]
    
    # Extract the column for the current variable and its corresponding ISO3 codes
    temp_data <- dat[Code==crop, .(ID,prod)]
  
    # Create a reclassification matrix directly
    rcl <- as.matrix(temp_data)  # Columns: iso3, value
    # Reclassify admin_rast based on the rcl matrix
    raster_layer <- terra::classify(admin_rast, rcl = rcl, include.lowest = TRUE)
    
    return(raster_layer)
  }))
  
  # Create proportions (divide pixel production by country total)
  spam_prop<-raw_dat/spam_tot
  # Replace infinite values with NA
  spam_prop[is.infinite(spam_prop)] <- NA
  
  # Assign meaningful names to the layers
  names(spam_prop) <- crops
  
  return(spam_prop)
})

names(spam_prop)<-names(spam_prod_admin0_ex)

# 3) Download and process FAOstat data ####

## 3.1) Download data #####  
vop_file_world<-file.path(fao_dir,"Value_of_Production_E_All_Data.csv")
if(!file.exists(vop_file_world)){
  # Define the URL and set the save path
  url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Value_of_Production_E_All_Data.zip"
  zip_file_path <- file.path(fao_dir, basename(url))
  
  # Download the file
  download.file(url, zip_file_path, mode = "wb")
  
  # Unzip the file
  unzip(zip_file_path, exdir = fao_dir)
  
  # Delete the ZIP file
  unlink(zip_file_path)
}  

remove_countries<- c("Ethiopia PDR","Sudan (former)")
atlas_iso3<-geoboundaries$ADM0_A3
target_year<-2019:2023

## 3.2) Load data #####
prod_value_i<-fread(vop_file_world,encoding = "Latin-1")

# Choose element
prod_value_i[,unique(Element)]
element<-"Gross Production Value (constant 2014-2016 thousand I$)"
# Note current thousand US$ has only 35 values whereas constant 12-16 has 157

cols<-c("Item","Element","Area","Area Code (M49)",paste0("Y",target_year))
prod_value_i<-prod_value_i[Element %in% element,..cols]

# Convert Area Code (M49) to ISO3 codes and filter by atlas_iso3 countries
prod_value_i[, M49 := as.numeric(gsub("[']", "", `Area Code (M49)`))]
prod_value_i[, iso3 := countrycode(sourcevar = M49, origin = "un", destination = "iso3c")]
prod_value_i<-prod_value_i[!is.na(iso3)]

# Any countries missing?
unique(spam_prod_admin0$iso3[!spam_prod_admin0$iso3 %in% prod_value_i$iso3 ])

# Combine similar products
prod_value_i[grep("Maize",Item),Item:="Maize (corn)"]

# Define columns to sum
y_cols <- grep("^Y\\d{4}$", names(prod_value_i), value = TRUE) # Select columns starting with "Y"

# Group by iso3 and atlas_name and sum the Y columns
prod_value_i <- prod_value_i[, lapply(.SD, sum, na.rm = TRUE), by = .(iso3, Item), .SDcols = y_cols]


val_cols<-paste0("Y",target_year)
prod_value_i[,value:=rowMeans(.SD,na.rm=T),.SDcols = val_cols[2:4]
][is.na(value),value:=rowMeans(.SD,na.rm=T),.SDcols = val_cols]

### 3.1.3) Update names ####
prod_value_i<-merge(prod_value_i,spam2fao[,.(short_spam2010,name_fao_val)],by.x="Item",by.y="name_fao_val",all.x=T)
prod_value_i<-prod_value_i[!is.na(short_spam2010)]
setnames(prod_value_i,"short_spam2010","Code")

# 4) Distribute fao vop to spam production ####
final_vop_i<-prod_value_i[,list(iso3,Code,value)]

final_vop_i<-merge(final_vop_i,iso3_levels,by="iso3",all.x=T)
final_vop_i<-final_vop_i[!is.na(ID)]

crops<-final_vop_i[,unique(Code)]

# Create a raster stack directly from final_vop_i_vect
final_vop_i_rast <- terra::rast(pblapply(1:length(crops), function(i) {
  crop<-crops[i]
  # Extract the column for the current variable and its corresponding ISO3 codes
  temp_data <- final_vop_i[Code==crop, .(ID,value)]
  
  # Remove rows with NA in the column being processed
  #temp_data <- temp_data[!is.na(temp_data[[colname]])]
  
  # Create a reclassification matrix directly
  rcl <- as.matrix(temp_data)  # Columns: iso3, value
  # Reclassify admin_rast based on the rcl matrix
  raster_layer <- terra::classify(admin_rast, rcl = rcl, include.lowest = TRUE)
  
  return(raster_layer)
}))

names(final_vop_i_rast)<-crops

# Multiply national VoP by cell proportion
names(final_vop_i_rast)<-spam2fao[match(names(final_vop_i_rast),spam2fao$short_spam2010),long_spam2010]

spam_prop_all<-spam_prop$all
spam_prop_all<-spam_prop_all[[names(spam_prop_all) %in% names(final_vop_i_rast)]]
spam_prop_all<-spam_prop_all[[sort(names(spam_prop_all))]]
final_vop_i_rast<-final_vop_i_rast[[sort(names(final_vop_i_rast))]]

names(spam_prop_all) == names(final_vop_i_rast)

spam_vop_intd<-spam_prop_all*final_vop_i_rast

# Split COFF in ACOF and RCOF
coff<-spam_vop_intd$coffee
acof<-spam_dat$all$`arabica coffee`
rcof<-spam_dat$all$`robusta coffee`
arcof<-acof+rcof
acof<-coff * acof/arcof
names(acof)<-"arabica coffee"
rcof<-coff * rcof/arcof
names(rcof)<-"robusta coffee"

spam_vop_intd$`robusta coffee`<-rcof
spam_vop_intd$`arabica coffee`<-acof
spam_vop_intd<-spam_vop_intd[[order(names(spam_vop_intd))]]
spam_vop_intd$coffee<-NULL

spam_vop_intd_save<-round(spam_vop_intd*1000,1)
save_file<-file.path(mapspam_pro_dir,"variable=vop_intld15-2021","spam_vop_intld15-2021_all.tif")
terra::writeRaster(spam_vop_intd_save,save_file,overwrite=T)

# 5) Split between rainfed and irrigated ####
spam_prod_i<-spam_dat$irr[[order(names(spam_dat$irr))]]
spam_prod_a<-spam_dat$all[[order(names(spam_dat$all))]]

spam_prod_i_p<-spam_prod_i/spam_prod_a


spam_prod_i_p<-spam_prod_i_p[[names(spam_prod_i_p) %in% names(spam_vop_intd)]]
names(spam_vop_intd) == names(spam_prod_i_p)
length(spam_vop_intd) == length(spam_prod_i_p)

# Irrigated = irrigated_prod/total_prod * vop
spam_vop_intd_i<-spam_prod_i_p*spam_vop_intd
sub_dat<-spam_vop_intd_i
sub_dat[is.na(sub_dat)]<-0
spam_vop_intd_r<-spam_vop_intd-sub_dat

spam_vop_intd_i_save<-round(spam_vop_intd_i*1000,1)
spam_vop_intd_r_save<-round(spam_vop_intd_r*1000,1)
terra::writeRaster(spam_vop_intd_i_save,file.path(mapspam_pro_dir,"variable=vop_intld15-2021","spam_vop_intld15-2021_irr.tif"),overwrite=T)
terra::writeRaster(spam_vop_intd_r_save,file.path(mapspam_pro_dir,"variable=vop_intld15-2021","spam_vop_intld15-2021_rf-all.tif"),overwrite=T)

# Check data
a<-spam_vop_intd$soybean*1000
i<-spam_vop_intd_i$soybean*1000
r<-spam_vop_intd_r$soybean*1000
plot(c(a,i,r))
# i + r should be virtually the same as a
diff_plot<-a-sum(c(r,i),na.rm=T)
plot(diff_plot)
