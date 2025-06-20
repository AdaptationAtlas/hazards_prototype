
p_load(arrow,geoarrow,pbapply,terra)

# Load mapspam metadata ####
spam_meta<-data.table::fread(file.path(project_dir,"metadata","SpamCodes.csv"))
short_crops<-spam_meta[long_season==F,Fullname]
short_crops<-gsub(" ","-",short_crops)

# Load boundaries ####
Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
  file<-geo_files_local[i]
  data<-arrow::open_dataset(file)
  data <- data |> sf::st_as_sf() |> terra::vect()
  data$zone_id<-1:length(data)
  data
})
names(Geographies)<-names(geo_files_local)

base_rast<-terra::rast(base_rast_path)+0

boundaries_zonal<-lapply(1:length(Geographies),FUN=function(i){
  file_path<-file.path(boundaries_int_dir,paste0(names(Geographies)[i],"_zonal.tif"))
  if(!file.exists(file_path)){
    zones<-Geographies[[i]]
    zone_rast <- rasterize(
      x      = zones, 
      y      = base_rast, 
      field  = "zone_id", 
      background = NA,    # cells not covered by any polygon become NA
      touches    = TRUE   # optional: count cells touched by polygon boundaries
    )
    terra::writeRaster(zone_rast,file_path,overwrite=T)
  }
  file_path
})
names(boundaries_zonal)<-names(Geographies)

boundaries_index<-lapply(1:length(Geographies),FUN=function(i){
  data.frame(Geographies[[i]])[,c("iso3","admin0_name","admin1_name","admin2_name","zone_id")]
})

names(boundaries_index)<-names(Geographies)


# Set season pairs ####
season_pairs<-data.table(s1=c("sos_primary_fixed_3","sos_primary_fixed_4","sos_primary_fixed_5","sos_primary_eos"),
                         s2=c("sos_secondary_fixed_4","sos_secondary_fixed_5","sos_secondary_fixed_5","sos_secondary_eos"),
                         name=c("sos_fixed_3","sos_fixed_4","sos_fixed_5","sos_eos"))


# Exposure folders
exposure_dirs<-data.table(dirs=c(atlas_dirs$data_dir$hazard_risk_vop,
                                 atlas_dirs$data_dir$hazard_risk_vop_usd),
                          exp_var=c("vop_intld15_all","vop_usd2015_all")
                          )

for(e in 1:nrow(exposure_dirs)){
  exp_dir<-exposure_dirs$dirs[e]
  exp_var<-exposure_dirs$exp_var[e]
  cat("Processing exposure var",e,"/",nrow(exposure_dirs),exp_var,"\n")
  # Load SPAM Data and intersect with multicropping areas ####
  file<-list.files(mapspam_pro_dir,paste0(exp_var,".*tif"),recursive=T,full.names=T)
  if(length(file)>1){
    stop("More than one exposure file selected")
  }

  cat("Using exposure file",file,"\n")

  spam_dat<-rast(file)
  
  sos_rast <- terra::rast(file.path(sos_dir, "sos.tif"))
  s1<-sos_rast$S1
  s2<-sos_rast$S2
  
  spam_dats1<-mask(spam_dat,s1)
  spam_dats2<-mask(spam_dat,s2)
  
  spam_ex_s1<-data.table(zonal(spam_dats1,rast(boundaries_zonal$admin2),fun="sum",na.rm=T))
  spam_ex_s2<-data.table(zonal(spam_dats2,rast(boundaries_zonal$admin2),fun="sum",na.rm=T))
  
  spam_ex_s1<-melt(spam_ex_s1,id.vars="zone_id")
  spam_ex_s2<-melt(spam_ex_s2,id.vars="zone_id")
  setnames(spam_ex_s2,"value","value2")
  
  spam_ex_s12<-merge(spam_ex_s1,spam_ex_s2,all.x=T)
  setnames(spam_ex_s12,c("value","value2"),c("total1","total2"))
  
  spam_ex_s12<-merge(spam_ex_s12,boundaries_index$admin2,all.x=T,by="zone_id")
  spam_ex_s12[,zone_id:=NULL]
  
  setnames(spam_ex_s2,"variable","crop")

  for(s in 1:nrow(season_pairs)){
    s<-1
    season_name<-season_pairs$name[s]
    save_dir<-ensure_dir(file.path(exp_dir,season_name))
    
    cat("Processing season",s,"/",nrow(season_pairs),"| exposure_var",e,"/",nrow(exposure_dirs),exp_var,"\n")
    
    tx1<-season_pairs$s1[s]
    tx2<-season_pairs$s2[s]
    
    # Load PTOT hazard exposure ####
    cat("Creating PTOT weightings from historical data\n")
    haz_risk_dir1 <- file.path(atlas_dirs$data_dir$hazard_risk, tx1)
    haz_risk_dir2 <- file.path(atlas_dirs$data_dir$hazard_risk, tx2)
    
    files1<-list.files(haz_risk_dir1,"histor.*severe.tif",full.names = T)
    files2<-list.files(haz_risk_dir2,"histor.*severe.tif",full.names = T)
    
    ptot_haz<-pblapply(1:length(files1),function(i){
      #cat(i,"       \r")
      dat1<-rast(files1[i])
      names(dat1)<-gsub("wet_NDWS","dry_NDWS",names(dat1))
      layer_sel<-grep("dry_",names(dat1),value=T)
      if(length(layer_sel)>1){
        layer_sel<-grep("NDWS",layer_sel,value=T)
      }
      dat1<-dat1[layer_sel]
      
      crop<-unlist(tstrsplit(names(dat1),"_",keep=6))
      hazard<-unlist(tstrsplit(names(dat1),"_",keep=5))
      
      dat2<-rast(files2[i])
      names(dat2)<-gsub("wet_NDWS","dry_NDWS",names(dat2))
      dat2<-dat2[layer_sel]
      
      dat1m<-mask(dat1,dat2)
      
      dat1m2<-c(dat1m,dat2)
      names(dat1m2)<-paste0(hazard,"_",crop,c("_primary","_secondary"))
      dat1m2
    })
    
    ptot_haz<-rast(ptot_haz)
    names(ptot_haz)
    
    ptot_haz_ex<-data.table(zonal(ptot_haz,rast(boundaries_zonal$admin2),fun="mean",na.rm=T))
    ptot_haz_ex<-melt(ptot_haz_ex,id.vars="zone_id")
    ptot_haz_ex[,c("hazard","crop","season"):=tstrsplit(variable,"_")][,variable:=NULL]
    ptot_haz_ex<-dcast(ptot_haz_ex,zone_id+crop~season)
    
    ptot_haz_ex<-merge(ptot_haz_ex,boundaries_index$admin2,by="zone_id",all.x=T)
    ptot_haz_ex$zone_id<-NULL
    
    # Invert hazards (higher is now better)
    ptot_haz_ex[,primary:=1-primary][,secondary:=1-secondary]
    
    setnames(ptot_haz_ex,
             c("primary","secondary"),
             c("weight1","weight2"))
    
    # Load hazard exposure data ####
    
    h_files1<-data.table(file=list.files(file.path(atlas_dirs$data_dir$hazard_risk_vop,tx1),"parquet$",full.names = T))
    h_files1[,c("data_type","variable","unit","model","compound","adm","severity"):=tstrsplit(basename(file),"_")
            ][,severity:=gsub(".parquet","",severity)
              ][,c("data_type","adm"):=NULL
                ][,season:=tstrsplit(file,"/",keep = 3)]
    
    h_files2<-data.table(file=list.files(file.path(atlas_dirs$data_dir$hazard_risk_vop,tx1),"parquet$",full.names = T))
    h_files2[,c("data_type","variable","unit","model","compound","adm","severity"):=tstrsplit(basename(file),"_")
    ][,severity:=gsub(".parquet","",severity)
    ][,c("data_type","adm"):=NULL
    ][,season:=tstrsplit(file,"/",keep = 3)]
    
    if(nrow(h_files1)!=nrow(h_files2)){
      stop("Uneven number of hazard exposure files between seasons")
    }
    
    for(i in 1:length(h_files1)){
  
    file1<-h_files1$file[i]
    file2<-h_files2$file[i]
    
    save_name<-file.path(save_dir,basename(file1))
    
    cat("Processing file",i,"/",length(h_files1),basename(file1), "\nseason",s,"/",nrow(season_pairs),"\nexposure_var",e,"/",nrow(exposure_dirs),exp_var,"\n")
    
    if(basename(file1)!=nrow(file2)){
      stop("Hazard exposure file names do not match between seasons")
    }
    
    dat1<-read_parquet(file1)
    dat2<-read_parquet(file2)
    setnames(dat2,"value","value2")
    
    dat12<-merge(dat1,dat2,all.x=T)
    rm(dat1,dat2)
    gc()
    # Subset to short_season crops only
    dat12<-dat12[crop %in% short_crops]
    
    # Subset to admin2 only
    dat12<-dat12[!is.na(admin2_name)]
    
    # Add total exposure
    dat12<-merge(dat12,spam_ex_s12,all.x=T,by=c("iso3","admin0_name","admin1_name","admin2_name","crop"))
    
    # Add PTOT weightings
    dat12<-merge(dat12,ptot_haz_ex,all.x=T,by=c("iso3","admin0_name","admin1_name","admin2_name","crop"))
    
    dat12[,share2 := fifelse(
      is.na(weight1) | is.na(weight2) | (weight1 + weight2) == 0,
        0.5,                                # fallback
        weight2 / (weight1 + weight2)       
      )
    ]
    
    dat12[
      , `:=`(
        # adjusted production (you can rename or overwrite as you prefer)
        total2_adj = total2 * share2,
        total1_adj = total1 - total2 * share2,
        
        # adjusted hazard-exposed VoP (if needed)
        value2_adj = value2 * share2,
        # haz_freq x non-shared amount + shared amount x s1 weighting
        value1_adj = (value/total1) * ((total1-total2) + (total2*(1-share2)))
      )
    ]
    
    if(dat12[value2_adj<0|value1_adj<0,.N>0]){
      stop("Negative values present in seasonally adjusted numbers")
    }
    
    }

  }

}



#### XXXX #####
# Legacy - Load cropsuite data ####
cropsuite_dir<-"~/common_data/atlas_cropSuite"

cropsuite_files<-data.table(file=list.files(cropsuite_dir,".tif$",full.names = T,recursive=T))
cropsuite_files[,c("scenario","timeframe","crop","variable"):=tstrsplit(basename(file),"_")
][,variable:=gsub(".tif","",variable)
][,multiple_seasons:=any(grepl("-mc-",file)),by=crop]

# Which crops have multiple cropping seasons?
cropsuite_files<-cropsuite_files[multiple_seasons==T]

crops<-cropsuite_files[,unique(crop)]
crops3<-cropsuite_files[grepl("third",file),unique(crop)]

cs_dat<-rbindlist(lapply(1:length(crops),function(i){
  crop_choice<-crops[i]
  cat("Processing crop",i,"/",length(crops),"\n")
  # More likely it is a sum
  file_mc<-cropsuite_files[crop==crop_choice & variable=="climate-suitability-mc" & timeframe=="1991-2010",file]
  file<-cropsuite_files[crop==crop_choice & variable=="climate-suitability" & timeframe=="1991-2010",file]
  
  
  rdat_mc<-terra::rast(file_mc)
  rdat<-terra::rast(file)
  rdat_diff<-rdat_mc-rdat
  
  
  if(crop_choice %in% crops3){
    file3<-cropsuite_files[crop==crop_choice & grepl("third",variable) & timeframe=="1991-2010",file]
    rdat_3<-terra::rast(file3)  
    # Divide three season suitability by 2 
    rdat_diff[rdat_3>0]<-rdat_diff[rdat_3>0]/2
    season3<-T
  }else{
    season3<-F
  }
  
  rdat_diff[rdat_diff==0]<-NA
  rdat[rdat==0]<-NA
  rdat_mc[rdat_mc==0]<-NA
  
  # Set suitability to NA where there is no second season
  rdat_mask<-mask(rdat,rdat_diff)
  rdat_mc_mask<-mask(rdat_mc,rdat_diff)
  
  rdat<-c(rdat,rdat_mc,rdat_mask,rdat_mc_mask,rdat_diff)
  names(rdat)<-c("s","s_mc","s_masked","s_mc_masked","s-s_mc")
  
  rdat<-resample(rdat,base_rast)
  
  ex_dat<-rbindlist(lapply(1:length(boundaries_zonal),function(i){
    ex_dat<-zonal(rdat,rast(boundaries_zonal[[i]]),fun="mean",na.rm=T)
    ex_dat<-merge(ex_dat,boundaries_index[[i]],all.x=T)
    ex_dat$zone_id<-NULL
    ex_dat
  }))
  
  ex_dat[,crop:=crop_choice][,season3:=season3]
  ex_dat
}))

