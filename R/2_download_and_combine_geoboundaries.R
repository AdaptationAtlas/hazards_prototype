require(data.table)
require(terra)

# Load metadata for countries to consider in the atlas - exclude islands for which we do not have climate data
countries_metadata<-fread("Data/metadata/countries.csv")[excluded_island==FALSE]

# download data
dl_geoboundaries(savedir="Data/geoboundaries/gbHumanitarian",
                 iso3=countries_metadata$iso3,
                 adm_levels=c("ADM0","ADM1","ADM2"),
                 release="gbHumanitarian",
                 attempts=3)

dl_geoboundaries(savedir="Data/geoboundaries/gbOpen",
                 iso3=countries_metadata$iso3,
                 adm_levels=c("ADM0","ADM1","ADM2"),
                 release="gbOpen",
                 attempts=3)


# gbHumanitarian
# unzip data
files<-list.files("Data/geoboundaries/gbHumanitarian",".zip",full.names = T)
if(length(files)>0){
  for(file in files){unzip(zipfile=file,exdir = "Data/geoboundaries/gbHumanitarian")}
  # unlink(files) # deletes zip files after opening
}

# List shapefiles
files_hum<-list.files("Data/geoboundaries/gbHumanitarian",".shp",full.names = T)
# do not use simplified boundaries
files_hum<-files_hum[!grepl("simplified",files_hum)]
# subset to atlas countries
files_hum<-files_hum[grepl(paste0(iso3_required,collapse="|"),files_hum)]

# gbOpen
# unzip data
files<-list.files("Data/geoboundaries/gbOpen",".zip",full.names = T)
if(length(files)>0){
  for(file in files){unzip(zipfile=file,exdir = "Data/geoboundaries/gbOpen")}
  # unlink(files) # deletes zip files after opening
}

# List shapefiles
files_open<-list.files("Data/geoboundaries/gbOpen",".shp",full.names = T)
# do not use simplified boundaries
files_open<-files_open[!grepl("simplified",files_open)]

# which countries are missing in gbHumanitarian?
missing<-sapply(iso3_required,FUN=function(iso3){any(grepl(iso3,files_hum))})
missing<-names(missing)[!missing]

files_open<-files_open[grepl(paste0(missing,collapse="|"),files_open)]

# Combine list of shapefiles
files_both<-c(files_hum,files_open)

files_adm0<-files_both[grepl("ADM0",files_both)]
admin0<-lapply(files_adm0,FUN=function(file){terra::vect(file)})
admin0<-do.call("rbind",admin0)
terra::writeVector(admin0,"Data/geoboundaries/admin0.shp",overwrite=T)

files_adm1<-files_both[grepl("ADM1",files_both)]
admin1<-lapply(files_adm1,FUN=function(file){terra::vect(file)})
admin1<-do.call("rbind",admin1)
terra::writeVector(admin1,"Data/geoboundaries/admin1.shp",overwrite=T)

files_adm2<-files_both[grepl("ADM2",files_both)]
admin2<-lapply(files_adm2,FUN=function(file){terra::vect(file)})
admin2<-do.call("rbind",admin2)
terra::writeVector(admin2,"Data/geoboundaries/admin2.shp",overwrite=T)