cat("Running patch to fix wet ndws issue\n")

patch_dirs<-c(atlas_dirs$data_dir$hazard_risk,
              atlas_dirs$data_dir$hazard_risk_vop,
              atlas_dirs$data_dir$hazard_risk_vop_usd)

for(i in length(patch_dirs)){
  cat("Patching ",patch_dirs[i],"\n")
  
  dir1<-file.path(patch_dirs[i],timeframe_choices)
  
  tifs<-list.files(dir1,"tif",full.names=T)
  tifs<-tifs[!grepl("[+]",tifs)]
  tifs<-tifs[grepl("highland|tropical|generic",tifs)]
  
  parquets<-list.files(dir1,"solo.*parquet",full.names=T)
  parquets<-parquets[!grepl("json",parquets)]
  
  cat("Patching ",patch_dirs[i],"- tifs \n")
  
  set_parallel_plan(n_cores=10,use_multisession=T)
  
  # Enable progressr
  progressr::handlers(global = TRUE)
  progressr::handlers("progress")
  
  # Wrap the parallel processing in a with_progress call
  p<-with_progress({
    # Define the progress bar
    prog <- progressr::progressor(along = 1:length(tifs))
    
    invisible(
      future.apply::future_lapply(1:length(tifs),FUN=function(j){
        #    cat(j,"/",length(tids),"       \r")
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
   })
   )
  
  })
  
  plan(sequential)
  
  cat("Patching ",patch_dirs[j],"- parquets \n")
  set_parallel_plan(n_cores=10,use_multisession=T)
  
  # Enable progressr
  progressr::handlers(global = TRUE)
  progressr::handlers("progress")
  
  # Wrap the parallel processing in a with_progress call
  p<-with_progress({
    # Define the progress bar
    prog <- progressr::progressor(along = 1:length(parquets))
    
    invisible(
      future.apply::future_lapply(1:length(parquets),FUN=function(j){
      #  cat(j,"/",length(parquets),"       \r")
        file<-parquets[j]
        dat<-read_parquet(file)
        dat<-data.table(dat)
        if(dat[hazard=="wet" & hazard_vars=="NDWS",.N>0]){
          dat[hazard=="wet" & hazard_vars=="NDWS",hazard:="dry"]
          write_parquet(dat,file)     
        }
      })
    )
  })
  
  plan(sequential)
  
  cat("Patching",patch_dirs[i]," complete\n")
}

cat("All patching complete\n")
