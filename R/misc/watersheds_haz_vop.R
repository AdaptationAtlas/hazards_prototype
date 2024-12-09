watersheds_file<-file.choose()
watersheds<-read_parquet(watersheds_file)
watersheds<-sf::st_as_sf(watersheds)
watersheds<-vect(watersheds)

names(watersheds)<-c("watershed","iso3","admin0_name")
watersheds$admin_name<-watersheds$admin0_name
watersheds<-list(admin0=watersheds)

# 4.2) Extract Freq x Exposure by Geography #####

for(INT in c(T,F)){
  if(do_vop_usd){
    cat("Interaction =",INT,"variable = vop_usd\n")
    
    haz_risk_exp_extract(severity_classes,
                         interactions=INT,
                         folder=haz_risk_vop_usd_dir,
                         overwrite=overwrite,
                         Geographies=watersheds,
                         level="watershed",
                         rm_crop=NULL,
                         rm_haz=NULL)
  }
  
}

# 4.2.1) Check results #####
if(F){
  admin0<-"Nigeria"
  crop_focus<-"potato"
  
  vop<-arrow::read_parquet(list.files("Data/exposure","crop_vop_adm_sum",full.names = T))
  (vop_val<-vop[admin0_name==admin0 & is.na(admin1_name) & crop==crop_focus,value])
  
  # Check resulting files
  (file<-list.files(haz_risk_vop_dir,"moderate_adm0_int",full.names = T))
  data<-sfarrow::st_read_parquet(file)
  dat_names<-c(names(data)[1:3],grep("ssp585.2021_2040.any.NDWS.NTxS.PTOT_G.potato.moderate",names(data),value=T))
  data<-data.table(data[dat_names])
  # This number should not exceed 1
  data[admin0_name==admin0,4]/vop_val
}

# 4.3) Restructure Extracted Data ####
levels<-c(watershed0="adm0")

for(SEV in tolower(severity_classes$class)){
  for(INT in c(T,F)){

      cat(SEV,"- interaction =",INT,"variable = vop_usd\n")
      # Vop
      recode_restructure_wrap(folder=haz_risk_vop_usd_dir,
                              file="watersheds",
                              crops=crop_choices,
                              livestock=livestock_choices,
                              exposure_var="vop",
                              severity=SEV,
                              overwrite=overwrite,
                              levels=levels,
                              interaction=INT,
                              hazards=haz_meta[,unique(type)])
    
  }
}

# 4.3.1) Check results #####
if(F){
  # Check results
  (file<-list.files(haz_risk_vop_dir,"moderate_adm_int",full.names = T))
  
  vop<-arrow::read_parquet(list.files("Data/exposure","crop_vop_adm_sum",full.names = T))
  data<-arrow::read_parquet(file)
  
  admin0<-"Nigeria"
  crop_focus<-"potato"
  
  # The numbers should never exceed 1
  (vop_val<-vop[admin0_name==admin0 & is.na(admin1_name) & crop==crop_focus,value])
  data[admin0_name==admin0 & is.na(admin1_name) & crop==crop_focus & hazard=="any",value]/vop_val
  
  print(head(data))
  print(data[,unique(hazard)])
  print(data[,unique(hazard_vars)])
  print(data[,unique(crop)])
  
}

