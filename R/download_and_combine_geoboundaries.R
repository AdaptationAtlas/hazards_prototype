require(data.table)
require(terra)

source("https://github.com/AdaptationAtlas/hazards_prototype/raw/main/R/dl_geoboundaries.R")
source("https://github.com/AdaptationAtlas/hazards_prototype/raw/main/R/try_download.R")

# South East Asia
countries_metadata <- data.frame(iso3= c(
  "Brunei Darussalam" = "BRN",
  "Cambodia" = "KHM",
  "Indonesia" = "IDN",
  "Lao PDR" = "LAO",
  "Malaysia" = "MYS",
  "Myanmar" = "MMR",
  "Philippines" = "PHL",
  "Singapore" = "SGP",
  "Thailand" = "THA",
  "Timor-Leste" = "TLS",
  "Viet Nam" = "VNM"
))

folder<-"Data/geoboundaries_SA"

# Sub-saharan Africa
countries_metadata<-fread("Data/metadata/countries.csv")[excluded_island==FALSE]
folder<-"Data/geoboundaries_SSA"


# download data
dl_geoboundaries(savedir=paste0(folder,"/gbHumanitarian"),
                 iso3=countries_metadata$iso3,
                 adm_levels=c("ADM0","ADM1","ADM2"),
                 release="gbHumanitarian",
                 attempts=3)

dl_geoboundaries(savedir=paste0(folder,"/gbOpen"),
                 iso3=countries_metadata$iso3,
                 adm_levels=c("ADM0","ADM1","ADM2"),
                 release="gbOpen",
                 attempts=3)


# Process data into a single file for the selected countries

# gbHumanitarian
# unzip data
files<-list.files(paste0(folder,"/gbHumanitarian"),".zip",full.names = T)
if(length(files)>0){
  for(file in files){unzip(zipfile=file,exdir = paste0(folder,"/gbHumanitarian"))}
  # unlink(files) # deletes zip files after opening
}

# List shapefiles
files_hum<-list.files(paste0(folder,"/gbHumanitarian"),".shp",full.names = T)
# do not use simplified boundaries
files_hum<-files_hum[!grepl("simplified",files_hum)]
# subset to countries
files_hum<-files_hum[grepl(paste0(countries_metadata$iso3,collapse="|"),files_hum)]

# gbOpen
# unzip data
files<-list.files(paste0(folder,"/gbOpen"),".zip",full.names = T)
if(length(files)>0){
  for(file in files){unzip(zipfile=file,exdir = paste0(folder,"/gbOpen"))}
  # unlink(files) # deletes zip files after opening
}

# List shapefiles
files_open<-list.files(paste0(folder,"/gbOpen"),".shp",full.names = T)
# do not use simplified boundaries
files_open<-files_open[!grepl("simplified",files_open)]

# which countries are missing in gbHumanitarian?
missing<-sapply(countries_metadata$iso3,FUN=function(iso3){any(grepl(iso3,files_hum))})
missing<-names(missing)[!missing]

files_open<-files_open[grepl(paste0(missing,collapse="|"),files_open)]

# Combine list of shapefiles
files_both<-c(files_hum,files_open)

files_adm0<-files_both[grepl("ADM0",files_both)]
admin0<-lapply(files_adm0,FUN=function(file){terra::vect(file)})
admin0<-do.call("rbind",admin0)
terra::writeVector(admin0,paste0(folder,"/admin0.shp"),overwrite=T)

files_adm1<-files_both[grepl("ADM1",files_both)]
admin1<-lapply(files_adm1,FUN=function(file){terra::vect(file)})
admin1<-do.call("rbind",admin1)
terra::writeVector(admin1,paste0(folder,"/admin1.shp"),overwrite=T)

files_adm2<-files_both[grepl("ADM2",files_both)]
admin2<-lapply(files_adm2,FUN=function(file){terra::vect(file)})
admin2<-do.call("rbind",admin2)
terra::writeVector(admin2,paste0(folder,"/admin2.shp"),overwrite=T)

# Delete downloaded data?
# unlink("Data/geoboundaries/gbHumanitarian",recursive=T)
# unlink("Data/geoboundaries/gbOpen",recursive=T)
