cat("Running patch to fix wet ndws issue\n")

patch_dirs<-c(atlas_dirs$data_dir$hazard_risk,
              atlas_dirs$data_dir$hazard_risk_vop,
              atlas_dirs$data_dir$hazard_risk_vop_usd)

for(i in length(patch_dirs)){
  cat("Patching ",patch_dirs[j])
  
  dir1<-file.path(patch_dirs[j],timeframe_choices)
  
  tifs<-list.files(dir1,"tif",full.names=T)
  tifs<-tifs[!grepl("[+]",tifs)]
  tifs<-tifs[grepl("highland|tropical|generic",tifs)]
  
  parquets<-list.files(dir1,"solo.*parquet",full.names=T)
  parquets<-parquets[!grepl("json",parquets)]
  
  for(j in 1:length(tifs)){
    cat(j,"/",length(tids),"       \r")
    file<-tifs[j]
    rdat<-rast(file)
    if(any(grepl("wet_NDWS",names(rdat)))){
    names(rdat)<-gsub("wet_NDWS","dry_NDWS",names(rdat))
    rdat<-rdat+0
    terra::writeRaster(rdat,
                       file=file,
                       overwrite=T,
                       filetype = 'COG',
                       gdal = c("COMPRESS=ZSTD"))     
    }
  }
  
  for(j in 1:length(parquets)){
    cat(j,"/",length(parquets),"       \r")
    file<-parquets[j]
    dat<-read_parquet(file)
    dat<-data.table(dat)
    if(dat[hazard=="wet" & hazard_vars=="NDWS",.N>0]){
      dat[hazard=="wet" & hazard_vars=="NDWS",hazard:="dry"]
      write_parquet(dat,file)     
    }
  }
  
  cat("Patching complete to fix wet ndws issue")
  }
