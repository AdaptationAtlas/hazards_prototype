
p_load(terra,geoarrow,arrow)

admin0<-read_parquet(geo_files_local["admin0"])
admin0 <- admin0 |> sf::st_as_sf() |> terra::vect()

# Load mapspam metadata ####
spam_meta<-data.table::fread(file.path(project_dir,"metadata","SpamCodes.csv"))
short_crops<-spam_meta[long_season==F & !is.na(cropsuite_name),cropsuite_name]
short_crops<-gsub(" ","-",short_crops)


files<-list.files(cropsuite_raw_dir,"historical.*sowing-date.*tif",recursive=T,full.names=T)
files<-grep(paste(paste0("_",short_crops,"_"),collapse="|"),files,value=T)

files<-data.table(file=files)
files[,c("timeframe","years","crop","variable"):=tstrsplit(basename(file),"_")
      ][,variable:=gsub(".tif","",variable)]

files_s1<-files[grepl("first",variable),file]
files_s2<-files[grepl("second",variable),file]
files_s0<-files[!grepl("first|second",variable),file]

s1<-rast(files_s1)
s2<-rast(files_s2)
s0<-rast(files_s0)


# Remove corrupt files
s2$`historical_1991-2010_barley_optimal-sowing-date-mc-second`<-NULL
s2$`historical_1991-2010_millet_optimal-sowing-date-mc-second`<-NULL
# Remove from s1 for consistence
s1$`historical_1991-2010_barley_optimal-sowing-date-mc-second`<-NULL
s1$`historical_1991-2010_millet_optimal-sowing-date-mc-second`<-NULL


s0_median<-app(s0,fun=median,na.rm=T)
s0_mean<-app(s0,fun="mean",na.rm=T)
s1_median<-app(s1,fun=median,na.rm=T)
s1_mean<-app(s1,fun="mean",na.rm=T)
s2_median<-app(s2,fun=median,na.rm=T)
s2_mean<-app(s2,fun="mean",na.rm=T)
plant_dates<-c(s0_median,s0_mean,s1_median,s1_mean,s2_median,s2_mean)
names(plant_dates)<-c("s0_median","s0_mean","s1_median","s1_mean","s2_median","s2_mean")
plot(plant_dates)

s1s0_median<-mask(s0_median,s1_median,inverse=T)
s1s0_median<-sum(c(s1s0_median,s1_median),na.rm=T)
medians<-c(s1s0_median,s2_median)
names(medians)<-c("S1_median","S2_median")

s1s0_mean<-mask(s0_mean,s1_mean,inverse=T)
s1s0_mean<-sum(c(s1s0_mean,s1_mean),na.rm=T)
means<-c(s1s0_mean,s2_mean)
names(means)<-c("S1_mean","S2_mean")

plot(c(medians,means))

KE_median<-mask(crop(medians,admin0[admin0$iso3=="KEN",]),admin0[admin0$iso3=="KEN",])
KE_mean<-mask(crop(means,admin0[admin0$iso3=="KEN",]),admin0[admin0$iso3=="KEN",])
plot(c(KE_median,KE_mean))

writeRaster(medians,file.path(atlas_dirs$data_dir$sos_cropsuite,"seasons_median.tif"),overwrite=T)
writeRaster(means,file.path(atlas_dirs$data_dir$sos_cropsuite,"seasons_mean.tif"),overwrite=T)

