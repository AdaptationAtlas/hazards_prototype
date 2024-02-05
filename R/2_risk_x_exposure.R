# Install and load packages ####
load_and_install_packages <- function(packages) {
  for (package in packages) {
    if (!require(package, character.only = TRUE)) {
      install.packages(package)
      library(package, character.only = TRUE)
    }
  }
}

# List of packages to be loaded
packages <- c("terra", 
              "data.table", 
              "exactextractr",
              "s3fs",
              "sf", 
              "sfarrow", 
              "arrow",
              "feather",
              "doFuture",
              "stringr", 
              "stringi",
              "httr")

# Call the function to install and load packages
load_and_install_packages(packages)

# Create functions ####
# Function to convert tabular mapspam data into a raster
read_spam<-function(variable,technology,mapspam_dir,save_dir,base_rast,filename,ms_codes,overwrite){
  ms_file<-paste0(save_dir,"/",filename,".tif")
  
  if(!file.exists(ms_file)|overwrite==T){
    data<-fread(paste0(mapspam_dir,"/SSA_",variable,"_",technology,".csv"))
    crops<-tolower(ms_codes$Code)
    ms_fields<-c("x","y",grep(paste0(crops,collapse = "|"),colnames(data),value=T))
    data<-terra::rast(data[,..ms_fields],type="xyz",crs="EPSG:4326")
    names(data)<-gsub("_a$|_h$|_i$|_l$|_r$|_s$","",names(data))
    names(data)<-ms_codes[match(names(data),tolower(ms_codes$Code)),Fullname]
    # convert to value/area 
    data<-data/terra::cellSize(data,unit="ha")
    # resample data
    data<-terra::resample(data,base_rast)
    data<-data*cellSize(data,unit="ha")
    terra::writeRaster(data,filename =ms_file,overwrite=T)
  }else{
    data<-terra::rast(ms_file)
  }
  return(data)
}

# Create extraction by geography function
admin_extract<-function(data,Geographies,FUN="mean",max_cells_in_memory=3*10^7){
  output<-list()
  if("admin0" %in% names(Geographies)){
    data0<-exactextractr::exact_extract(data,sf::st_as_sf(Geographies$admin0),fun=FUN,append_cols=c("admin_name","admin0_name","iso3"),max_cells_in_memory=max_cells_in_memory)
    data0<-terra::merge(Geographies$admin0,data0)
    output$admin0<-data0
  }
  
  if("admin1" %in% names(Geographies)){
    data1<-exactextractr::exact_extract(data,sf::st_as_sf(Geographies$admin1),fun=FUN,append_cols=c("admin_name","admin0_name","admin1_name","iso3"),max_cells_in_memory=max_cells_in_memory)
    data1<-terra::merge(Geographies$admin1,data1)
    output$admin1<-data1
  }
  
  if("admin2" %in% names(Geographies)){
    data2<-exactextractr::exact_extract(data,sf::st_as_sf(Geographies$admin2),fun=FUN,append_cols=c("admin_name","admin0_name","admin1_name","admin2_name","iso3"),max_cells_in_memory=max_cells_in_memory)
    data2<-terra::merge(Geographies$admin2,data2)
    output$admin2<-data2
  }
  
  return(output)
}

# Create extraction function wrapper for exposure data
admin_extract_wrap<-function(data,save_dir,filename,FUN="sum",varname,Geographies,overwrite=F){
  
  levels<-c(admin0="adm0",admin1="adm1",admin2="adm2")
  
  file<-paste0(save_dir,"/",filename,"_adm_",FUN,".parquet")
  file0<-gsub("_adm_","_adm0_",file)
  file1<-gsub("_adm_","_adm1_",file)
  file2<-gsub("_adm_","_adm2_",file)
  
  if(!file.exists(file)|!file.exists(file1)|overwrite==T){
    data_ex<-admin_extract(data,Geographies,FUN=FUN)
    
    st_write_parquet(obj=sf::st_as_sf(data_ex$admin0), dsn=file0)
    st_write_parquet(obj=sf::st_as_sf(data_ex$admin1), dsn=file1)
    st_write_parquet(obj=sf::st_as_sf(data_ex$admin2), dsn=file2)
    
    data_ex<-rbindlist(lapply(1:length(levels),FUN=function(i){
      level<-levels[i]
      print(level)
      
      
      data<-data.table(data.frame(data_ex[[names(level)]]))
      data<-data[,!c("admin_name","iso3")]
      
      admin<-"admin0_name"
      
      if(level %in% c("adm1","adm2")){
        admin<-c(admin,"admin1_name")
        data<-suppressWarnings(data[,!"a1_a0"])
      }
      
      if(level=="adm2"){
        admin<-c(admin,"admin2_name")
        data<-suppressWarnings(data[,!"a2_a1_a0"])
      }
      
      colnames(data)<-gsub("_nam$","_name",colnames(data))
      
      data<-melt(data,id.vars = admin)
      
      data[,crop:=gsub(paste0(FUN,"."),"",variable[1],fixed=T),by=variable][,exposure:=varname][,variable:=NULL]
      
      data
      
    }),fill=T)
    data_ex[,crop:=gsub("."," ",crop,fixed=T)]
    
    
    arrow::write_parquet(data_ex,file)
  }else{
    data_ex<- arrow::read_parquet(file)
  }
  
  return(data_ex)
}

# Create extraction function wrapper for risk data
admin_extract_wrap2<-function(files,save_dir,filename,severity,overwrite=F,FUN="mean",Geographies){
  
  for(SEV in tolower(severity)){
    
    file0<-paste0(save_dir,"/",filename,"_adm0_",SEV,".parquet")
    file1<-gsub("_adm0_","_adm1_",file0)
    file2<-gsub("_adm0_","_adm2_",file0)
    
    data<-terra::rast(files[grepl(SEV,files)])
    
    if((!file.exists(file0))|overwrite==T){
      # Display progress
      cat('\r                                                                                                                                                 ')
      cat('\r',paste("Adm0 - Severity Class:",SEV))
      flush.console()
      
      data_ex<-admin_extract(data,Geographies["admin0"],FUN=FUN)
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin0), dsn=file0)
    }
    
    if((!file.exists(file1))|overwrite==T){
      # Display progress
      cat('\r                                                                                                                                                 ')
      cat('\r',paste("Adm1 - Severity Class:",SEV))
      flush.console()
      
      data_ex<-admin_extract(data,Geographies["admin1"],FUN=FUN)
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin1), dsn=file1)
    }
    
    if((!file.exists(file2))|overwrite==T){
      # Display progress
      cat('\r                                                                                                                                                 ')
      cat('\r',paste("Adm2 - Severity Class:",SEV))
      flush.console()
      
      data_ex<-admin_extract(data,Geographies["admin2"],FUN=FUN)
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin2), dsn=file2)
    }
    
  }
}

# Function that restructures hazard risk data into a long format table
restructure_parquet<-function(filename,save_dir,severity,overwrite=F,crops,livestock,Scenarios){
  severity<-tolower(severity)
  for(SEV in severity){
    file<-paste0(save_dir,"/",filename,"_adm_",SEV,".parquet")
    
    if(!file.exists(file)|overwrite==T){
      file0<-paste0(save_dir,"/",filename,"_adm0_",SEV,".parquet")
      file1<-gsub("_adm0_","_adm1_",file0)
      file2<-gsub("_adm0_","_adm2_",file0)
      
      files<-list(adm0=file0,adm1=file1,adm2=file2)
      
      # Read in geoparquet vectors, extract tabular data and join together
      data<-rbindlist(lapply(1:length(files),FUN=function(i){
        file<-files[i]
        level<-names(files[i])
        
        print(paste0(SEV,"-",level))
        
        admins<-"admin0_name"
        
        if(level %in% c("adm1","adm2")){
          admins<-c(admins,"admin1_name")
        }
        
        if(level=="adm2"){
          admins<-c(admins,"admin2_name")
        }
        
        data<-data.table(data.frame(terra::vect(sfarrow::st_read_parquet(files[[i]]))))
        data<-suppressWarnings(data[,!c("admin_name","iso3","a2_a1_a0","a1_a0")])
        
        colnames(data)<-gsub("_nam$","_name",colnames(data))
        
        melt(data,id.vars = admins)
      }),fill=T)
      
      # Renaming of variable to allow splitting
      new<-paste0(Scenarios$combined,"-")
      old<-paste0(Scenarios[,paste0(Scenario,".",Time)],".")
      
      # Replace space in the crop names with a . to match the parquet column names
      new<-c(new,paste0("-",crops,"-"))
      old<-c(old,paste0(".",gsub(" ",".",crops,fixed = T),"."))
      
      new<-c(new,paste0("-",livestock,"-"))
      old<-c(old,paste0(".",livestock,"."))

      variable_old<-data[,unique(variable)]
      variable_new<-data.table(variable=stringi::stri_replace_all_regex(variable_old,pattern=old,replacement=new,vectorise_all = F))
      
      # Note this method of merging a list back to the original table is much faster than the method employed in the hazards x exposure section
      split<-variable_new[,list(var_split=list(tstrsplit(variable[1],"-"))),by=variable]
      split_tab<-rbindlist(split$var_split)
      colnames(split_tab)<-c("scenario","timeframe","hazard","crop","severity")
      split_tab$variable<-variable_old
      split_tab[,hazard:=gsub(".","+",hazard[1],fixed=T),by=hazard
      ][,scenario:=unlist(tstrsplit(scenario[1],".",keep=2,fixed=T)),by=scenario
      ][,severity:=tolower(severity)]
      
      data<-merge(data,split_tab,all.x=T)
      data[,variable:=NULL]
      
      arrow::write_parquet(data,file)
    }
    
  }
  
}

# Make functions to restructure hazard risk x exposure geoparquet in a long tabular form
recode_restructure<-function(data,crop_choices,livestock_choices,Scenarios,exposure,Severity){
  
  # Renaming of variable to allow splitting
  new<-paste0(Scenarios$combined,"-")
  old<-paste0(Scenarios[,paste0(Scenario,".",Time)],".")
  
  # Replace space in the crop names with a . to match the parquet column names
  new<-c(new,paste0("-",crop_choices,"-"))
  old<-c(old,paste0(".",gsub(" ",".",crop_choices,fixed = T),"."))
  
  new<-c(new,paste0("-",livestock_choices,"-"))
  old<-c(old,paste0(".",livestock_choices,"."))
  
  new<-c(new,paste0("-",exposure))
  old<-c(old,paste0(".",exposure))

  variable_old<-data[,unique(variable)]
  
  variable_new<-data.table(variable=stringi::stri_replace_all_regex(variable_old,pattern=old,replacement=new,vectorise_all = F))
  
  split<-variable_new[,list(var_split=list(tstrsplit(variable[1],"-"))),by=variable]
  split_tab<-rbindlist(split$var_split)
  colnames(split_tab)<-c("scenario","timeframe","hazard","crop","severity","exposure")
  split_tab$variable<-variable_old
  split_tab[,hazard:=gsub(".","+",hazard[1],fixed=T),by=hazard
  ][,scenario:=unlist(tstrsplit(scenario[1],".",keep=2,fixed=T)),by=scenario
  ][,severity:=tolower(severity)]
  
  data<-merge(data,split_tab,all.x=T)
  data[,variable:=NULL]

  return(data)
}
recode_restructure_wrap<-function(folder,file,crops,livestock,exposure_var,Severity,overwrite,any=F,levels){
  
  if(any==F){
    filename<-paste0(folder,"/",file,"_",Severity,".parquet")
  }else{
    filename<-paste0(folder,"/",file,"_any_",Severity,".parquet")
  }
  
  if((!file.exists(filename))|overwrite==T){
    data<-rbindlist(lapply(1:length(levels),FUN=function(i){
      level<-levels[i]
      
      # Display progress
      cat('\r                                                                                                                     ')
      cat('\r',paste("Risk x Exposure - ",exposure_var," restructuring data | severity: ",Severity, " | admin level:",level))
      flush.console()
      
      # Vop
      if(any==F){
        data<-data.table(data.frame(sfarrow::st_read_parquet(paste0(folder,"/",file,"_",levels[i],"_",Severity,".parquet"))))
      }else{
        data<-data.table(data.frame(sfarrow::st_read_parquet(paste0(folder,"/",file,"_any_",levels[i],"_",Severity,".parquet"))))
      }
      data<-data[,!c("geometry","iso3","admin_name")]
      
      colnames(data)<-gsub("_nam$","_name",colnames(data))
      
      admins<-"admin0_name"
      if(level %in% c("adm1","adm2")){
        admins<-c(admins,"admin1_name")
      }
      
      if(level=="adm2"){
        admins<-c(admins,"admin2_name")
      }
      
      data<-melt(data,id.vars = admins)
      
      data<-recode_restructure(data=data,
                               crop_choices = crops,
                               livestock_choices = livestock,
                               Scenarios = Scenarios,
                               exposure=exposure_var,
                               Severity=Severity)
      
      
      data
    }),fill=T)
    # Save as feather object
    arrow::write_parquet(data,filename)
  }
}

# Function to split livestock between highland and tropical
split_livestock<-function(data,livestock_mask_high,livestock_mask_low){
  
  # Reorder cols to match mask
  order_n<-sapply(names(data),FUN=function(X){grep(X,names(livestock_mask_high))})
  data_high<-data[[order_n]]
  data_high<-data_high*livestock_mask_high
  
  order_n<-sapply(names(data),FUN=function(X){grep(X,names(livestock_mask_low))})
  data_low<-data[[order_n]]
  data_low<-data_low*livestock_mask_low
  
  
  names(data_high)<-names(livestock_mask_high)
  names(data_low)<-names(livestock_mask_low)
  
  data_joined<-c(data_low,data_high)
  
  return(data_joined)
}

# Set up workspace ####
# Increase GDAL cache size
terra::gdalCache(60000)

# workers
worker_n<-10

# Set scenarios and time frames to analyse
Scenarios<-c("ssp245","ssp585")
Times<-c("2021_2040","2041_2060")

# admin levels
levels<-c(admin0="adm0",admin1="adm1",admin2="adm2")

# Create combinations of scenarios and times
Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))
Scenarios[,combined:=paste0(Scenario,"-",Time)]

# Set hazards to include in analysis
hazards<-c("NDD","NTx40","NTx35","HSH_max","HSH_mean","THI_max","THI_mean","NDWS","TAI","NDWL0","PTOT","TAVG")
haz_2way<-c("PTOT","TAVG")
hazards2<-c(hazards[!hazards %in% haz_2way],paste0(haz_2way,rep(c("_L","_H"),each=2)))


haz_class_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_classes.csv"
haz_class<-data.table::fread(haz_class_url)
haz_class<-haz_class[index_name %in% hazards,list(index_name,description,direction,crop,threshold)]
haz_classes<-unique(haz_class$description)

# List livestock names
livestock_choices<-paste0(rep(c("cattle","sheep","goats","pigs","poultry"),each=2),c("_highland","_tropical"))

# Pull out severity classes and associate impact scores
severity_classes<-unique(fread(haz_class_url)[,list(description,value)])
setnames(severity_classes,"description","class")

# Create combinations of scenarios and hazards
scenarios_x_hazards<-data.table(Scenarios,Hazard=rep(hazards,each=nrow(Scenarios)))[,Scenario:=as.character(Scenario)][,Time:=as.character(Time)]

# Load crop names from mapspam metadata
ms_codes<-data.table::fread("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SpamCodes.csv")[,Code:=toupper(Code)]
ms_codes<-ms_codes[compound=="no"]
crop_choices<-unique(c(ms_codes[,sort(Fullname)],haz_class[,unique(crop)]))

# Load base raster to resample to 
base_raster<-"base_raster.tif"
if(!file.exists(base_raster)){
url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/base_raster.tif"
httr::GET(url, write_disk(base_raster, overwrite = TRUE))
}

base_rast<-terra::rast(base_raster)

#### Load datasets (non hazards)
# 1) Geographies #####
# Load and combine geoboundaries
overwrite<-F
geoboundaries_s3<-"s3://digital-atlas/boundaries"
geo_files_s3<-s3fs::s3_dir_ls(geoboundaries_s3)
geo_files_s3<-grep("harmonized.gpkg",geo_files_s3,value=T)

geo_files_local<-file.path("Data/boundaries",basename(geo_files_s3))
names(geo_files_local)<-c("admin0","admin1","admin2")


Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
  file<-geo_files_local[i]
  if(!file.exists(file)|overwrite==T){
    s3fs::s3_file_download(path=geo_files_s3[i],new_path=file,overwrite = T)
  }
  data<-terra::vect(file)
  names(data)<-gsub("_nam$","_name$",names(data))
  data
  
})
names(Geographies)<-names(geo_files_local)

# 2) Exposure variables ####

exposure_dir<-"Data/exposure"
if(!dir.exists(exposure_dir)){
  dir.create(exposure_dir)
}

overwrite<-F

  # 2.1) Crops (MapSPAM) #####
    # 2.1.1) Download MapSPAM #####
    # If mapspam data does not exist locally download from S3 bucket
    mapspam_local<-"Data/mapspam"
    
    if(!dir.exists(mapspam_local)){
      dir.create(mapspam_local,recursive = T)
      s3_bucket <- "s3://digital-atlas/risk_prototype/data/mapspam"
      s3fs::s3_dir_download(s3_bucket,mapspam_local)
    }

    # 2.1.2) Crop VoP (Value of production) ######
      # To generalize it might be better to just supply a filename for the mapspam
      crop_vop_tot<-read_spam(variable="V",
                              technology="TA",
                              mapspam_dir=mapspam_local,
                              save_dir=exposure_dir,
                              base_rast=base_rast,
                              filename="crop_vop",
                              ms_codes=ms_codes,
                              overwrite=overwrite)
      
      crop_vop17_tot<-read_spam(variable="Vusd17",
                                technology="TA",
                                mapspam_dir=mapspam_local,
                                save_dir=exposure_dir,
                                base_rast=base_rast,
                                filename="crop_vop_usd17",
                                ms_codes=ms_codes,
                                overwrite=overwrite)

  
  # 2.1.2.2) Extraction of values by admin areas
    crop_vop_tot_adm_sum<-admin_extract_wrap(data=crop_vop_tot,
                                             save_dir=exposure_dir,
                                             filename = "crop_vop",
                                             FUN="sum",
                                             varname="vop",
                                             Geographies=Geographies,
                                             overwrite=overwrite)
    
    crop_vop17_tot_adm_sum<-admin_extract_wrap(data=crop_vop17_tot,
                                               save_dir=exposure_dir,
                                               filename = "crop_vop_usd17",
                                               FUN="sum",
                                               varname="vop_usd17",
                                               Geographies=Geographies,
                                               overwrite=overwrite)
      
    # 2.1.3) Crop Harvested Area #####

    crop_ha_tot<-read_spam(variable="H",
                            technology="TA",
                            mapspam_dir=mapspam_local,
                            save_dir=exposure_dir,
                            base_rast=base_rast,
                            filename="crop_ha",
                            ms_codes=ms_codes,
                            overwrite=overwrite)

  # 2.1.2.1) Extraction of values by admin areas
  crop_ha_tot_adm_sum<-admin_extract_wrap(data=crop_ha_tot,
                                          save_dir=exposure_dir,
                                          filename = "crop_ha",
                                          FUN="sum",
                                          varname="ha",
                                          Geographies=Geographies,
                                          overwrite=overwrite)

    # 2.1.4) Create Crop Masks ######
  commodity_mask_dir<-"Data/commodity_masks"
  
  if(!dir.exists(commodity_mask_dir)){
    dir.create(commodity_mask_dir)
  }
  
  # Need to use mapspam physical area
  mask_file<-paste0(commodity_mask_dir,"/crop_masks.tif")
  
  if(!file.exists(mask_file)|overwrite==T){
    pa<-fread(paste0(mapspam_dir,"/SSA_A_TA.csv"))
    crops<-tolower(ms_codes$Code)
    ms_fields<-c("x","y",grep(paste0(crops,collapse = "|"),colnames(pa),value=T))
    pa<-rast(pa[,..ms_fields],type="xyz",crs="EPSG:4326")
    names(pa)<-gsub("_a","",names(pa))
    names(pa)<-ms_codes[match(names(pa),tolower(ms_codes$Code)),Fullname]
    # convert to value/area 
    crop_pa<-pa/terra::cellSize(pa,unit="ha")
    # resample  data
    crop_pa<-terra::resample(crop_pa,base_rast)
    crop_pa_tot<-crop_pa*cellSize(crop_pa,unit="ha")
    
    # Areas with >0.01% harvested area = crop mask
    crop_pa_prop<-crop_pa_tot/cellSize(crop_pa_tot,unit="ha")
    crop_mask<-terra::classify(crop_pa_prop,  data.frame(from=c(0,0.001),to=c(0.001,2),becomes=c(0,1)))
    terra::writeRaster(crop_mask,filename=mask_file,overwrite=T)
  }else{
    crop_mask<-terra::rast(mask_file)
  }
  
  # 2.2) Livestock #####
    # 2.2.1) Download livestock data ####
    # If glw3 data does not exist locally download from S3 bucket
    glw3_dir<-"Data/GLW3"
    
    if(!dir.exists(glw3_dir)){
      dir.create(glw3_dir,recursive = T)
      s3_bucket <- "s3://digital-atlas/risk_prototype/data/GLW3"
      s3fs::s3_dir_download(s3_bucket,glw3_dir,overwrite = T)
    }
  
    # If livestock vop data does not exist locally download from S3 bucket
    ls_vop_dir<-"Data/livestock_vop"
    
    if(!dir.exists(ls_vop_dir)){
      dir.create(ls_vop_dir,recursive = T)
      s3_bucket <- "s3://digital-atlas/ls_vop_dir"
      s3fs::s3_dir_download(s3_bucket,ls_vop_dir,overwrite = T)
    }
    
    # If livestock highland vs tropical map does not exist locally download from S3 bucket
    afr_highlands_dir<-"Data/afr_highlands"
    
    if(!dir.exists(afr_highlands_dir)){
      dir.create(afr_highlands_dir,recursive = T)
      s3_bucket <- "s3://digital-atlas/afr_highlands"
      s3fs::s3_dir_download(s3_bucket,afr_highlands_dir,overwrite = T)
    }
    
    # 2.2.3) Livestock Mask #####
  mask_ls_file<-paste0(commodity_mask_dir,"/livestock_masks.tif")
  
  if(!file.exists(mask_ls_file)|overwrite==T){
    
    ls_files<-list.files(glw3_dir,"_Da.tif",full.names = T)
    
    Cattle<-terra::rast(grep("_Ct_2010_Da.tif",ls_files,value=T))
    Chicken<-terra::rast(grep("_Ch_2010_Da.tif",ls_files,value=T))
    Goat<-terra::rast(grep("_Gt_2010_Da.tif",ls_files,value=T))
    Pig<-terra::rast(grep("_Pg_2010_Da.tif",ls_files,value=T))
    Sheep<-terra::rast(grep("_Sh_2010_Da.tif",ls_files,value=T))
    
    TLU<-Cattle*0.7 + Sheep*0.1 + Goat*0.1 + 0.01*Chicken + 0.2*Pig
    
    lus<-c(Cattle*0.7,Chicken*0.01,Goat*0.1,Pig*0.2,Sheep*0.1,TLU)
    names(lus)<-c("cattle","poultry","goats","pigs","sheep","total")
    lus<-terra::mask(terra::crop(lus,Geographies$admin0),Geographies$admin0)
    
    # resample to 0.05
    lu_density<-lus/terra::cellSize(lus,unit="ha")
    lu_density<-terra::resample(lu_density,base_rast)
    
    # Classify requires 0.00001 livestock units per ha to be present
    livestock_mask<-terra::classify(lu_density, data.frame(from=c(0,0.00001),to=c(0.00001,Inf),becomes=c(0,1)))
    
    # Split mask by highland vs tropical areas
    
    # Load highland mask
    highlands<-terra::rast(paste0(afr_highlands_dir,"/afr-highlands.asc"))
    highlands<-terra::resample(highlands,base_rast,method="near")

    
    livestock_mask_high<-livestock_mask*highlands
    names(livestock_mask_high)<-paste0( names(livestock_mask_high),"_highland")
    
    lowlands<-classify(highlands,data.frame(from=c(0,1),to=c(1,0)))
    livestock_mask_low<-livestock_mask*lowlands
    names(livestock_mask_low)<-paste0( names(livestock_mask_low),"_tropical")
    
    livestock_mask<-c(livestock_mask_high,livestock_mask_low)
    
    terra::writeRaster(livestock_mask,filename=mask_ls_file,overwrite=T)
    
  }else{
    livestock_mask<-terra::rast(mask_ls_file)
    livestock_mask_high<-livestock_mask[[grep("highland",names(livestock_mask))]]
    livestock_mask_low<-livestock_mask[[!grepl("highland",names(livestock_mask))]]
  }
    # 2.2.1) Livestock Numbers (GLW3) ######
  livestock_no_file<-paste0(exposure_dir,"/livestock_no.tif")
  shoat_prop_file<-paste0(glw3_dir,"/shoat_prop.tif")
    
  if(!file.exists(livestock_no_file)|overwrite==T){
    
    ls_files<-list.files(glw3_dir,"_Da.tif",full.names = T)
    
    Cattle<-terra::rast(grep("_Ct_2010_Da.tif",ls_files,value=T))
    Chicken<-terra::rast(grep("_Ch_2010_Da.tif",ls_files,value=T))
    Goat<-terra::rast(grep("_Gt_2010_Da.tif",ls_files,value=T))
    Pig<-terra::rast(grep("_Pg_2010_Da.tif",ls_files,value=T))
    Sheep<-terra::rast(grep("_Sh_2010_Da.tif",ls_files,value=T))
    
    TLU<-Cattle*0.7 + Sheep*0.1 + Goat*0.1 + 0.01*Chicken + 0.2*Pig
    
    livestock_no<-c(Cattle,Chicken,Goat,Pig,Sheep,TLU)
    names(livestock_no)<-c("cattle","poultry","goats","pigs","sheep","total_livestock_units")
    livestock_no<-terra::mask(terra::crop(livestock_no,Geographies$admin0),Geographies$admin0)
    
    # resample to 0.05
    livestock_density<-livestock_no/terra::cellSize(livestock_no,unit="ha")
    livestock_density<-terra::resample(livestock_density,base_rast)
    livestock_no<-livestock_density*cellSize(livestock_density,unit="ha")
    
    # Pull out sheep and goat proportions for use in vop calculations before highland/tropical splitting
    sheep_prop<-livestock_no$sheep/(livestock_no$goats +livestock_no$sheep)
    goat_prop<-livestock_no$goats/(livestock_no$goats +livestock_no$sheep)
    
    terra::writeRaster(terra::rast(c(sheep_prop=sheep_prop,goat_prop=goat_prop)),filename = shoat_prop_file,overwrite=T)
    
    # Split livestock between highland and tropical
    livestock_no<-split_livestock(data=livestock_no,livestock_mask_high,livestock_mask_low)

    terra::writeRaster(livestock_no,filename = livestock_no_file,overwrite=T)
    
  }else{
    livestock_no<-terra::rast(livestock_no_file)
  }
  
    # 2.2.1.1) Extraction of values by admin areas
  livestock_no_tot_adm<-admin_extract_wrap(data=livestock_no,
                                           save_dir=exposure_dir,
                                           filename = "livestock_no",
                                           FUN="sum",
                                           varname="number",
                                           Geographies=Geographies,
                                           overwrite=overwrite)

    # 2.2.2) Livestock VoP ######
    # IUSD (old)
    livestock_vop_file<-paste0(exposure_dir,"/livestock_vop.tif")
    
    if(!file.exists(livestock_vop_file)|overwrite==T){
      
    # Note unit is IUSD 2005
      ls_vop_files<-list.files(ls_vop_dir,"_total.tif$",full.names = T)
      ls_vop_files<-grep("h7",ls_vop_files,value=T)
      livestock_vop<-terra::rast(ls_vop_files)
    
      names(livestock_vop)<-c("cattle","poultry","pigs","sheep_goat","total")
      
      # resample to 0.05
      livestock_density<-livestock_vop/terra::cellSize(livestock_vop,unit="ha")
      livestock_density<-terra::resample(livestock_density,base_rast)
      livestock_vop<-livestock_density*cellSize(livestock_density,unit="ha")
      rm(livestock_density)
      
      # Load prop files
      sheep_prop<-terra::rast(shoat_prop_file)$sheep_prop
      goat_prop<-terra::rast(shoat_prop_file)$goat_prop
      
      # Split sheep goat vop using their populations
      livestock_vop$sheep<-livestock_vop$sheep_goat*sheep_prop
      livestock_vop$goats<-livestock_vop$sheep_goat*goat_prop
      livestock_vop$sheep_goat<-NULL
    
    # Split vop by highland vs lowland
    
    livestock_vop<-split_livestock(data=livestock_vop,livestock_mask_high,livestock_mask_low)
    
    # Reorder cols to match mask
    order_n<-sapply(names(livestock_vop),FUN=function(X){grep(X,names(livestock_mask_high))})
    livestock_vop_high<-livestock_vop[[order_n]]
    livestock_vop_high<-livestock_vop_high*livestock_mask_high
    
    order_n<-sapply(names(livestock_vop),FUN=function(X){grep(X,names(livestock_mask_low))})
    livestock_vop_low<-livestock_vop[[order_n]]
    livestock_vop_low<-livestock_vop_low*livestock_mask_low
    
    
    names(livestock_vop_high)<-names(livestock_mask_high)
    names(livestock_vop_low)<-names(livestock_mask_low)
    
    livestock_vop<-c(livestock_vop_low,livestock_vop_high)
    
    terra::writeRaster(livestock_vop,filename = livestock_vop_file,overwrite=T)
    }else{
      livestock_vop<-terra::rast(livestock_vop_file)
    }
  
    # USD 2017 (see 0_fao_producer_prices_livestock.R)
    livestock_vop17_file<-paste0(exposure_dir,"/livestock_vop_usd17.tif")
  
    if(!file.exists(livestock_vop17_file)){
      data<-terra::rast(paste0(ls_vop_dir,"/vop_adj17.tif"))
      
      # Combine products for different species
      data<-terra::rast(c(cattle=data$cattle_meat+data$cattle_milk,
              goats=data$goat_meat+data$goat_milk,
              pigs=data$pig_meat,
              poultry=data$poultry_eggs+data$poultry_meat,
              sheep=data$sheep_meat+data$sheep_milk,
              total=sum(data,na.rm=T)))
      
      livestock_vop17<-split_livestock(data=data,livestock_mask_high,livestock_mask_low)
      terra::writeRaster(livestock_vop17,filename = livestock_vop17_file)
    }else{
      livestock_vop17<-terra::rast(livestock_vop17_file)
    }
  
    # 2.2.2.1) Extraction of values by admin areas
  livestock_vop_tot_adm<-admin_extract_wrap(data=livestock_vop,
                                            save_dir=exposure_dir,
                                            filename = "livestock_vop",
                                            FUN="sum",
                                            varname="vop",
                                            Geographies=Geographies,
                                            overwrite=overwrite)
  
  livestock_vop17_tot_adm<-admin_extract_wrap(data=livestock_vop17,
                                            save_dir=exposure_dir,
                                            filename = "livestock_vop_usd17",
                                            FUN="sum",
                                            varname="vop_usd17",
                                            Geographies=Geographies,
                                            overwrite=overwrite)
  
  # 2.3) Combine exposure totals by admin areas ####
    file<-paste0(exposure_dir,"/exposure_adm_sum.parquet")
    if(!file.exists(file)|overwrite==T){
      exposure_adm_sum_tab<-rbind(
        crop_vop_tot_adm_sum,
        crop_vop17_tot_adm_su,
        crop_ha_tot_adm_sum,
        livestock_vop_tot_adm,
        livestock_no_tot_adm
      )
          arrow::write_parquet(exposure_adm_sum_tab,file)
      }

  # 2.4) Population ######
  hpop_dir<-"Data/atlas_pop"
  bucket_files<-paste0("s3://digital-atlas/population/",c("total_pop.tif","rural_pop.tif","urban_pop.tif"))
  local_files<-file.path(hpop_dir,basename(bucket_files))
  
  if(!dir.exists(hpop_dir)|overwrite==T){
    dir.create(hpop_dir)
    for (i in seq_along(bucket_files)) {
      s3fs::s3_file_download(bucket_files[i], local_files[i],overwrite=T)
    }
  }
  
    file<-paste0(exposure_dir,"/hpop.tif")
    if(!file.exists(file)){
          
      hpop<-terra::rast(local_files)
      hpop<-terra::crop(hpop,Geographies)
      
      # Convert hpop to density
      hpop<-hpop/cellSize(hpop,unit="ha")
      
      # Resample to base raster
      hpop<-terra::resample(hpop,base_rast)
      
      # Convert back to number per cell
      hpop<-hpop*cellSize(hpop,unit="ha")
      
      terra::writeRaster(hpop,filename =file,overwrite=T)
    }else{
      hpop<-terra::rast(file)
      }

    # 2.4.1) Extraction of hpop by admin areas ####
  admin_extract_wrap(data=hpop,
                     save_dir=exposure_dir,
                     filename = "hpop",
                     FUN="sum",
                     varname="number",
                     Geographies=Geographies,
                     overwrite=overwrite)
    
#### Intersect Risk and Exposure ####
# 1) Hazard Risk ####
haz_risk_dir<-paste0("Data/hazard_risk/",timeframe_choice)

 # 1.1) Solo and interactions combined into a single file (not any hazard) ####
files<-list.files(haz_risk_dir,".tif$",full.names = T)
files<-files[!grepl("_any.tif",files)]

admin_extract_wrap2(files=files,
                    save_dir = haz_risk_dir,
                    filename="haz_risk",
                    severity=severity_classes$class,
                    Geographies=Geographies,
                    overwrite=F)

restructure_parquet(filename = "haz_risk",
                    save_dir = haz_risk_dir,
                    severity = severity_classes$class,
                    overwrite=F,
                    crops = c("generic",crop_choices),
                    livestock=livestock_choices,
                    Scenarios)

  # 1.2) Any hazard only ####

files<-list.files(haz_risk_dir,"_any.tif$",full.names = T)

admin_extract_wrap2(files=files,
                    save_dir = haz_risk_dir,
                    filename="haz_risk_any",
                    severity=severity_classes$class,
                    Geographies=Geographies,
                    overwrite=F)

restructure_parquet(filename = "haz_risk_any",
                    save_dir = haz_risk_dir,
                    severity = severity_classes$class,
                    overwrite=F,
                    crops = c("generic",crop_choices),
                    livestock=livestock_choices,
                    Scenarios)

# Check resulting file
X<-arrow::read_parquet(paste0(haz_risk_dir,"/haz_risk_any_adm_",SEV,".parquet"))
grep("THI",names(X),value=T)

# 2) Apply Crop Mask to Classified Hazard Risk ####

dirs<-list.dirs("Data/hazard_risk_class",recursive = F)

# join crop and livestock masks
commodity_masks<-c(crop_mask,livestock_mask)

# Remove total livestock units
commodity_masks<-commodity_masks[[!grepl("total_",names(commodity_masks))]]

for(k in 1:length(dirs)){
  
  haz_risk_class_dir<-paste0(dirs[k],"/",timeframe_choice)

  haz_risk_mask_dir<-gsub("hazard_risk_class/","hazard_risk_class_mask/",haz_risk_class_dir)
  if(!dir.exists(haz_risk_mask_dir)){
    dir.create(haz_risk_mask_dir,recursive = T)
  }
  
  risk_class_rast_files<-list.files(haz_risk_class_dir,".tif",full.names = T)
  
  file_crops<-gsub("_severe|_extreme|_int|[.]tif","",unlist(tail(tstrsplit(risk_class_rast_files,"/"),1)))
  
  for(i in 1:nlyr(commodity_masks)){
    crop<-names(commodity_masks)[i]
    mask<-commodity_masks[[i]]
    risk_files<-risk_class_rast_files[file_crops==crop]
    for(j in 1:length(risk_files)){
      
      # Display progress
      cat('\r                                                                                                                                                 ')
      cat('\r',paste("Crop:",i,"/",nlyr(commodity_masks)," | file:",j,"/",length(risk_files)))
      flush.console()
      
      file<-risk_files[j]
      
      save_name<-gsub(haz_risk_class_dir,haz_risk_mask_dir,file)
      if(!file.exists(save_name)){
        risk<-terra::rast(file)
        risk_masked<-risk*mask
        terra::writeRaster(risk_masked,filename =save_name)
      }
    }
  }

}

# 3) Hazard Means ####
haz_mean_dir<-paste0("Data/hazard_timeseries_mean/",timeframe_choice)

  # 3.2.1) Extract mean hazards ####
  folder<-haz_mean_dir
  
  files<-list.files(folder,".tif",full.names = T)
  # Note to look at change you will need to calculate change in mean values in 1_calculate_risks.R script and then subset to files containing change in the name
  files<-files[!grepl("change",files)]
  data<-terra::rast(files)
  names(data)<-gsub(".tif$","",basename(files))
  
  # If looking at change make sure update this field
  file<-"haz_means"
  file0<-file.path(folder,paste0(file,"_adm0.parquet"))
  file1<-gsub("adm0","adm1",file0)
  file2<-gsub("adm0","adm2",file0)
  
  
  if(!file.exists(file0)|overwrite==T){
    data_ex<-admin_extract(data=data,Geographies,FUN="mean")
    st_write_parquet(obj=sf::st_as_sf(data_ex$admin0), dsn=file0)
    st_write_parquet(obj=sf::st_as_sf(data_ex$admin1), dsn=file1)
    st_write_parquet(obj=sf::st_as_sf(data_ex$admin2), dsn=file2)
  }

  # 3.2.2) Compile mean hazards into tabular form ####
  filename<-gsub("adm0","adm",file0)
  
  if(!file.exists(filename)){
    # Extract data from vector files and restructure into tabular form
    data_ex<-rbindlist(lapply(1:length(levels),FUN=function(i){
      level<-levels[i]
      print(level)
      
      data<-data.table(data.frame(sfarrow::st_read_parquet(paste0(folder,"/",file,"_",levels[i],".parquet"))))
      
      data<-data[,!c("admin_name","iso3","geometry")]
      
      admin<-"admin0_name"
      
      if(level %in% c("adm1","adm2")){
        admin<-c(admin,"admin1_name")
        data<-suppressWarnings(data[,!"a1_a0"])
      }
      
      if(level=="adm2"){
        admin<-c(admin,"admin2_name")
        data<-suppressWarnings(data[,!"a2_a1_a0"])
      }
      
      colnames(data)<-gsub("_nam$","_name",colnames(data))
      
      data<-melt(data,id.vars = admin)
      
      data[,variable:=gsub("ENSEMBLEmean_","",variable)
           ][,variable:=gsub("historical","historic-historic",variable)
             ][,variable:=stringi::stri_replace_all_regex(variable,pattern=paste0(unique(Scenarios$Scenario),"_"),replacement=paste0(unique(Scenarios$Scenario),"-"),vectorise_all = F)
              ][,variable:=stringi::stri_replace_all_regex(variable,pattern=paste0(unique(Scenarios$Time),"_"),replacement=paste0(unique(Scenarios$Time),"-"),vectorise_all = F)
                ][,variable:=stringi::stri_replace_all_regex(variable,pattern=c("max_max","min_min","mean_mean"),replacement=c("max-max","min-min","mean-mean"),vectorise_all = F)
                  ][,variable:=gsub(".","-",variable,fixed=T)]
      
      variable<-cbind(data$variable,data.table(do.call("cbind",tstrsplit(data$variable,"-"))[,-1]))
      colnames(variable)<-c("variable","scenario","timeframe","hazard","hazard_stat")
      variable[is.na(hazard_stat),hazard:=gsub("_","-",hazard)
               ][is.na(hazard_stat),hazard_stat:=unlist(tstrsplit(hazard,"-",keep=2))
                 ][,hazard:=unlist(tstrsplit(hazard,"-",keep=1))]
    
      data<-merge(data,unique(variable),all.x=T)[,variable:=NULL]
      
     
      
    }),fill=T)
    data_ex<-data_ex[,c(1,7,8,3,4,5,6,2)]
    
    # Save mean values as feather object
    arrow::write_parquet(data_ex,filename)
  }
  
# 4) Hazard Trend ####
haz_timeseries_dir<-paste0("Data/hazard_timeseries/",timeframe_choice)
haz_timeseries_files<-list.files(haz_timeseries_dir,".tif",full.names = T)
haz_timeseries_files<-grep(paste(hazards,collapse = "|"),haz_timeseries_files,value=T)

haz_timeseries_files_sd<-grep("ENSEMBLEsd",haz_timeseries_files,value=T)
haz_timeseries_files<-haz_timeseries_files[!grepl("ENSEMBLEsd",haz_timeseries_files)]

# Load all timeseries data into a raster stack
haz_timeseries<-terra::rast(haz_timeseries_files)
haz_timeseries_sd<-terra::rast(haz_timeseries_files_sd)

# Update names of raster stack to be filename/year
layer_names<-unlist(lapply(1:length(haz_timeseries_files),FUN=function(i){
  file<-haz_timeseries_files[i]
  layers<-names(terra::rast(file))
  file<-gsub(".tif","",tail(unlist(strsplit(file,"/")),1),fixed=T)
  paste0(file,"_year",layers)
}))

names(haz_timeseries)<-layer_names

layer_names<-unlist(lapply(1:length(haz_timeseries_files_sd),FUN=function(i){
  file<-haz_timeseries_files_sd[i]
  layers<-names(terra::rast(file))
  file<-gsub(".tif","",tail(unlist(strsplit(file,"/")),1),fixed=T)
  paste0(file,"_year",layers)
}))

names(haz_timeseries_sd)<-layer_names

# Extract hazard values by admin areas and average them
# Extract by admin0
haz_timeseries_adm<-admin_extract(haz_timeseries,Geographies["admin0"],FUN="mean")
st_write_parquet(obj=sf::st_as_sf(haz_timeseries_adm$admin0), dsn=paste0(haz_timeseries_dir,"/haz_timeseries_adm0.parquet"))

# Extract by admin1
haz_timeseries_adm<-admin_extract(haz_timeseries,Geographies["admin1"],FUN="mean")
st_write_parquet(obj=sf::st_as_sf(haz_timeseries_adm$admin1), dsn=paste0(haz_timeseries_dir,"/haz_timeseries_adm1.parquet"))

# Extract by admin2
haz_timeseries_adm<-admin_extract(haz_timeseries,Geographies["admin2"],FUN="mean")
st_write_parquet(obj=sf::st_as_sf(haz_timeseries_adm$admin2), dsn=paste0(haz_timeseries_dir,"/haz_timeseries_adm2.parquet"))

# Restructure data into tabular form
filename<-paste0(haz_timeseries_dir,"/haz_timeseries.parquet")
# Extract data from vector files and restructure into tabular form
haz_timeseries_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
  level<-levels[i]
  print(level)
  
  haz_timeseries_tab<-data.table(data.frame(sfarrow::st_read_parquet(paste0(haz_timeseries_dir,"/haz_timeseries_",levels[i],".parquet"))))
  N<-colnames(haz_timeseries_tab)[-grep(c("admin0_nam|admin1_nam|admin2_nam|geometry"),colnames(haz_timeseries_tab))]
  haz_timeseries_tab<-haz_timeseries_tab[,..N]
  haz_timeseries_tab<-melt(haz_timeseries_tab,id.vars = c("admin_name","iso3"))
  
  haz_timeseries_tab[,variable:=gsub("mean.","",variable,fixed = T)
  ][,variable:=gsub("mean.","",variable,fixed = T)
  ][,variable:=gsub("_ENSEMBLEmean","",variable,fixed = T)
  ][,variable:=gsub("ssp245_","ssp245|",variable,fixed = T)
  ][,variable:=gsub("ssp585_","ssp585|",variable,fixed = T)
  ][,variable:=gsub("_year","|",variable,fixed = T)
  ][,variable:=gsub("historical_","historical|historical|",variable,fixed = T)
  ][,variable:=gsub("2060_","2060|",variable,fixed = T)
  ][,variable:=gsub("2040_","2040|",variable,fixed = T)
  ][,scenario:=unlist(tstrsplit(variable,"[|]",keep=1))
  ][,timeframe:=unlist(tstrsplit(variable,"[|]",keep=2))
  ][,hazard:=unlist(tstrsplit(variable,"[|]",keep=3))
  ][,year:=unlist(tstrsplit(variable,"[|]",keep=4))
  ][,stat:=tail(unlist(tstrsplit(hazard[1],"_")),1),by=hazard
  ][,hazard:=gsub("_mean_mean","mean",hazard,fixed = T)
  ][,hazard:=gsub("_max_max","max",hazard,fixed = T)
  ][,hazard:=gsub("_mean","",hazard,fixed = T)
  ][,hazard:=gsub("_sum","",hazard,fixed = T)
  ][,hazard:=gsub("_max","",hazard,fixed = T)
  ][,hazard:=gsub("mean","_mean",hazard,fixed = T)
  ][,hazard:=gsub("max","_max",hazard,fixed = T)
  ][,variable:=NULL
  ][,admin_level:=names(levels)[i]]
  
  
  haz_timeseries_tab
  
}))

# Save mean values as feather object
    arrow::write_parquet(haz_timeseries_tab,filename)


# Extract hazard values by admin areas and average them
# Extract by admin0
haz_timeseries_adm<-admin_extract(haz_timeseries_sd,Geographies["admin0"],FUN="mean")
st_write_parquet(obj=sf::st_as_sf(haz_timeseries_adm$admin0), dsn=paste0(haz_timeseries_dir,"/haz_timeseries_sd_adm0.parquet"))

# Extract by admin1
haz_timeseries_adm<-admin_extract(haz_timeseries_sd,Geographies["admin1"],FUN="mean")
st_write_parquet(obj=sf::st_as_sf(haz_timeseries_adm$admin1), dsn=paste0(haz_timeseries_dir,"/haz_timeseries_sd_adm1.parquet"))

# Extract by admin2
haz_timeseries_adm<-admin_extract(haz_timeseries_sd,Geographies["admin2"],FUN="mean")
st_write_parquet(obj=sf::st_as_sf(haz_timeseries_adm$admin2), dsn=paste0(haz_timeseries_dir,"/haz_timeseries_sd_adm2.parquet"))


# Restructure data into tabular form
filename<-paste0(haz_timeseries_dir,"/haz_timeseries_sd.parquet")
# Extract data from vector files and restructure into tabular form
haz_timeseries_sd_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
  level<-levels[i]
  print(level)
  
  haz_timeseries_tab<-data.table(data.frame(sfarrow::st_read_parquet(paste0(haz_timeseries_dir,"/haz_timeseries_sd_",levels[i],".parquet"))))
  N<-colnames(haz_timeseries_tab)[-grep(c("admin0_nam|admin1_nam|admin2_nam|geometry"),colnames(haz_timeseries_tab))]
  haz_timeseries_tab<-haz_timeseries_tab[,..N]
  haz_timeseries_tab<-melt(haz_timeseries_tab,id.vars = c("admin_name","iso3"))
  
  haz_timeseries_tab[,variable:=gsub("mean.","",variable,fixed = T)
  ][,variable:=gsub("mean.","",variable,fixed = T)
  ][,variable:=gsub("_ENSEMBLEsd","",variable,fixed = T)
  ][,variable:=gsub("ssp245_","ssp245|",variable,fixed = T)
  ][,variable:=gsub("ssp585_","ssp585|",variable,fixed = T)
  ][,variable:=gsub("_year","|",variable,fixed = T)
  ][,variable:=gsub("historical_","historical|historical|",variable,fixed = T)
  ][,variable:=gsub("2060_","2060|",variable,fixed = T)
  ][,variable:=gsub("2040_","2040|",variable,fixed = T)
  ][,scenario:=unlist(tstrsplit(variable,"[|]",keep=1))
  ][,timeframe:=unlist(tstrsplit(variable,"[|]",keep=2))
  ][,hazard:=unlist(tstrsplit(variable,"[|]",keep=3))
  ][,year:=unlist(tstrsplit(variable,"[|]",keep=4))
  ][,stat:=tail(unlist(tstrsplit(hazard[1],"_")),1),by=hazard
  ][,hazard:=gsub("_mean_mean","mean",hazard,fixed = T)
  ][,hazard:=gsub("_max_max","max",hazard,fixed = T)
  ][,hazard:=gsub("_mean","",hazard,fixed = T)
  ][,hazard:=gsub("_sum","",hazard,fixed = T)
  ][,hazard:=gsub("_max","",hazard,fixed = T)
  ][,hazard:=gsub("mean","_mean",hazard,fixed = T)
  ][,hazard:=gsub("max","_max",hazard,fixed = T)
  ][,variable:=NULL
  ][,admin_level:=names(levels)[i]]
  
  
  haz_timeseries_tab
  
}))

# Save mean values as feather object
    arrow::write_parquet(haz_timeseries_sd_tab,filename)

# 5) Commodity Specific Hazard Risk x Crop or Livestock VoP & Crop Harvested Area ####
haz_risk_vop_dir<-paste0("Data/hazard_risk_vop/",timeframe_choice)
if(!dir.exists(haz_risk_vop_dir)){
  dir.create(haz_risk_vop_dir,recursive = T)
}

haz_risk_ha_dir<-paste0("Data/hazard_risk_ha/",timeframe_choice)
if(!dir.exists(haz_risk_ha_dir)){
  dir.create(haz_risk_ha_dir,recursive = T)
}

haz_risk_n_dir<-paste0("Data/hazard_risk_n/",timeframe_choice)
if(!dir.exists(haz_risk_n_dir)){
  dir.create(haz_risk_n_dir,recursive = T)
}

# Crop choices only 
crop_choices<-crop_choices[!grepl("_tropical|_highland",crop_choices)]

  # 5.1) Solo and interactions combined into a single file (not any hazard) ####
  haz_risk_files<-list.files(haz_risk_dir,".tif$",full.names = T)
  haz_risk_files<-haz_risk_files[!grepl("_any.tif$",haz_risk_files)]
  overwrite<-F
  do_ha<-F
  do_n<-F
  
  for(SEV in tolower(severity_classes$class)){
    # Multiply Hazard Risk by Exposure ####
    
    haz_risk_files2<-haz_risk_files[grepl(SEV,haz_risk_files) & !grepl("_int",haz_risk_files)]
  
    #registerDoFuture()
    #plan("multisession", workers = worker_n)
    #foreach(i = 1:length(haz_risk_files2)) %dopar% {
      
     for(i in 1:length(haz_risk_files2)){
      
      crop<-gsub(paste0("_",SEV,".tif"),"",tail(tstrsplit(haz_risk_files2[i],"/"),1))
      
      # Display progress
      cat('\r                                                                                                                           ')
      cat('\r',paste("Risk x Exposure | crop:",i,"/",length(haz_risk_files2)," ",crop,"| severity:",SEV))
      flush.console()
      
      save_name_vop<-paste0(haz_risk_vop_dir,"/",crop,"_",SEV,"_vop.tif")
      
      files<-haz_risk_files2[i]
      files<-c(files,gsub(".tif","_int.tif",files))
      haz_risk<-terra::rast(files)
      
      if(!file.exists(save_name_vop)|overwrite==T){
        # vop
        if(crop!="generic"){
          if(crop %in% crop_choices){
           haz_risk_vop<-haz_risk*crop_vop_tot[[crop]]
          }else{
            haz_risk_vop<-haz_risk*livestock_vop[[crop]]
          }
          }else{
          haz_risk_vop<-haz_risk*sum(crop_vop_tot)
        }
        
        names(haz_risk_vop)<-paste0(names(haz_risk_vop),"-vop")
        writeRaster(haz_risk_vop,file=save_name_vop,overwrite=T)
      }
      
      # ha
      if(do_ha==T){
        if(crop %in% c("generic",crop_choices)){
        save_name_ha<-paste0(haz_risk_ha_dir,"/",crop,"_",SEV,"_ha.tif")
  
        if(!file.exists(save_name_ha)|overwrite==T){
          if(crop!="generic"){
           haz_risk_ha<-haz_risk*crop_ha_tot[[crop]]
          }else{
            haz_risk_ha<-haz_risk*sum(crop_ha_tot)
          }
          
          names(haz_risk_ha)<-paste0(names(haz_risk_ha),"-ha")
          writeRaster(haz_risk_ha,file=save_name_ha,overwrite=T)
          }
        }
      }
        
      # numbers
      if(do_n==T){
        if(crop %in% c("generic",livestock_choices)){
          save_name_n<-paste0(haz_risk_n_dir,"/",crop,"_",SEV,"_n.tif")
          
          if(!file.exists(save_name_n)|overwrite==T){
            if(crop!="generic"){
              haz_risk_n<-haz_risk*livestock_no[[crop]]
            }else{
              haz_risk_n<-haz_risk*sum(livestock_no[[(c("total_tropical","total_highland"))]],na.rm=T)
            }
            
            names(haz_risk_n)<-paste0(names(haz_risk_n),"-n")
            writeRaster(haz_risk_n,file=save_name_n,overwrite=T)
        }
        }
      }
      
    }
    
   # plan(sequential)
    
    # Extract Risk x Exposure by Geography  ####
      # VoP #####
      haz_risk_vop_files<-list.files(haz_risk_vop_dir,".tif$",full.names = T)
      haz_risk_vop_files<-haz_risk_vop_files[!grepl("_any.tif$",haz_risk_vop_files)]
      haz_risk_vop_files<-haz_risk_vop_files[grepl(SEV,haz_risk_vop_files)]
      data<-terra::rast(haz_risk_vop_files)
      
      file0<-paste0(haz_risk_vop_dir,"/haz_risk_vop_adm0_",SEV,".parquet")
      file1<-gsub("_adm0_","_adm1_",file0)
      file2<-gsub("_adm0_","_adm2_",file0)
      
      if(!file.exists(file0)|overwrite==T){
        # Display progress
        cat('\r                                                                                                                     ')
        cat('\r',paste("Risk x Exposure - VoP admin extraction - adm0| severity:",SEV))
        flush.console()
        
        data_ex<-admin_extract(data,Geographies["admin0"],FUN="sum")
        sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin0), dsn=file0)
        rm(data_ex)
        gc()
      }
      if(!file.exists(file1)|overwrite==T){
        # Display progress
        cat('\r                                                                                                                     ')
        cat('\r',paste("Risk x Exposure - VoP admin extraction - adm1| severity:",SEV))
        flush.console()
        
        data_ex<-admin_extract(data,Geographies["admin1"],FUN="sum")
        sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin1), dsn=file1)
        rm(data_ex)
        gc()
      }
      if(!file.exists(file2)|overwrite==T){
        # Display progress
        cat('\r                                                                                                                     ')
        cat('\r',paste("Risk x Exposure - VoP admin extraction - adm2| severity:",SEV))
        flush.console()
        
        data_ex<-admin_extract(data,Geographies["admin2"],FUN="sum")
        sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin2), dsn=file2)
        rm(data_ex)
        gc()
      }
      
      # Harvested Area #####
      if(do_ha==T){
        haz_risk_ha_files<-list.files(haz_risk_ha_dir,".tif$",full.names = T)
        haz_risk_ha_files<-haz_risk_ha_files[!grepl("_any.tif$",haz_risk_ha_files)]
        haz_risk_ha_files<-haz_risk_ha_files[grepl(SEV,haz_risk_ha_files)]
        data<-terra::rast(haz_risk_ha_files)
        
        file0<-paste0(haz_risk_ha_dir,"/haz_risk_ha_adm0_",SEV,".parquet")
        file1<-gsub("_adm0_","_adm1_",file0)
        file2<-gsub("_adm0_","_adm2_",file0)
        
        if(!file.exists(file0)|overwrite==T){
          # Display progress
          cat('\r                                                                                                                     ')
          cat('\r',paste("Risk x Exposure - HA admin extraction - adm0| severity:",SEV))
          flush.console()
          
          data_ex<-admin_extract(data,Geographies["admin0"],FUN="sum")
          sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin0), dsn=file0)
          rm(data_ex)
          gc()
        }
        if(!file.exists(file1)|overwrite==T){
          # Display progress
          cat('\r                                                                                                                     ')
          cat('\r',paste("Risk x Exposure - HA admin extraction - adm1| severity:",SEV))
          flush.console()
          
          data_ex<-admin_extract(data,Geographies["admin1"],FUN="sum")
          sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin1), dsn=file1)
          rm(data_ex)
          gc()
        }
        if(!file.exists(file2)|overwrite==T){
          # Display progress
          cat('\r                                                                                                                     ')
          cat('\r',paste("Risk x Exposure - HA admin extraction - adm2| severity:",SEV))
          flush.console()
          
          data_ex<-admin_extract(data,Geographies["admin2"],FUN="sum")
          sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin2), dsn=file2)
          rm(data_ex)
          gc()
        }
      }
      
      # Numbers #####
      if(do_n==T){
        haz_risk_n_files<-list.files(haz_risk_n_dir,".tif$",full.names = T)
        haz_risk_n_files<-haz_risk_n_files[!grepl("_any.tif$",haz_risk_n_files)]
        haz_risk_n_files<-haz_risk_n_files[grepl(SEV,haz_risk_n_files)]
        data<-terra::rast(haz_risk_n_files)
        
        file0<-paste0(haz_risk_n_dir,"/haz_risk_n_adm0_",SEV,".parquet")
        file1<-gsub("_adm0_","_adm1_",file0)
        file2<-gsub("_adm0_","_adm2_",file0)
        
        if(!file.exists(file0)|overwrite==T){
          # Display progress
          cat('\r                                                                                                                     ')
          cat('\r',paste("Risk x Exposure - N admin extraction - adm0| severity:",SEV))
          flush.console()
          
          data_ex<-admin_extract(data,Geographies["admin0"],FUN="sum")
          sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin0), dsn=file0)
          rm(data_ex)
          gc()
        }
        if(!file.exists(file1)|overwrite==T){
          # Display progress
          cat('\r                                                                                                                     ')
          cat('\r',paste("Risk x Exposure - N admin extraction - adm1| severity:",SEV))
          flush.console()
          
          data_ex<-admin_extract(data,Geographies["admin1"],FUN="sum")
          sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin1), dsn=file1)
          rm(data_ex)
          gc()
        }
        if(!file.exists(file2)|overwrite==T){
          # Display progress
          cat('\r                                                                                                                     ')
          cat('\r',paste("Risk x Exposure - N admin extraction - adm2| severity:",SEV))
          flush.console()
          
          data_ex<-admin_extract(data,Geographies["admin2"],FUN="sum")
          sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin2), dsn=file2)
          rm(data_ex)
          gc()
        }
      }
      
    # Restructure Extracted Data ####
    
    # Vop
    recode_restructure_wrap(folder=haz_risk_vop_dir,
                            file="haz_risk_vop",
                            crops=c("generic",crop_choices),
                            livestock=livestock_choices,
                            exposure_var="vop",
                            Severity=SEV,
                            overwrite=overwrite,
                            levels=levels)
    
    # Harvested area
    if(do_ha==T){
      recode_restructure_wrap(folder=haz_risk_ha_dir,
                              file="haz_risk_ha",
                              crops=c("generic",crop_choices),
                              livestock=livestock_choices,
                              exposure_var="ha",
                              Severity=SEV,
                              overwrite=overwrite,
                              levels=levels)
    }
    
    # Numbers
    if(do_n==T){
      recode_restructure_wrap(folder=haz_risk_n_dir,
                              file="haz_risk_n",
                              crops=c("generic",crop_choices),
                              livestock=livestock_choices,
                              exposure_var="number",
                              Severity=SEV,
                              overwrite=overwrite,
                              levels=levels)
    }
    
  }
  
  # 5.2) Any hazard only ####
  haz_risk_files<-list.files(haz_risk_dir,"_any.tif",full.names = T)
  overwrite<-F
  do_ha<-F
  do_n<-F
  
  for(SEV in tolower(severity_classes$class)){
  
  #### Multiply Hazard Risk by Exposure ####
  
  haz_risk_files2<-haz_risk_files[grepl(SEV,haz_risk_files)]
  
  #registerDoFuture()
  #plan("multisession", workers = 10)
  
  #  loop starts here
  #foreach(i = 1:length(haz_risk_files2)) %dopar% {
  
  for(i in 1:length(haz_risk_files2)){
    
    
    crop<-gsub(paste0("_",SEV,"_any.tif"),"",tail(tstrsplit(haz_risk_files2[i],"/"),1))
    
    # Display progress
    cat('\r                                                                                                                           ')
    cat('\r',paste("Risk x Exposure | crop:",i,"/",length(haz_risk_files2)," ",crop,"| severity:",SEV))
    flush.console()
    
    # Load crop hazard raster
    haz_risk<-terra::rast(haz_risk_files2[i])
    
    # multiply risk by vop
    save_name_vop<-paste0(haz_risk_vop_dir,"/",crop,"_",SEV,"_vop_any.tif")
    if(!file.exists(save_name_vop)|overwrite==T){
      # vop
      if(crop!="generic"){
        if(crop %in% crop_choices){
          haz_risk_vop<-haz_risk*crop_vop_tot[[crop]]
        }else{
          haz_risk_vop<-haz_risk*livestock_vop[[crop]]
        }
      }else{
        haz_risk_vop<-haz_risk*sum(crop_vop_tot)
      }
      
      names(haz_risk_vop)<-paste0(names(haz_risk_vop),"-vop")
      writeRaster(haz_risk_vop,file=save_name_vop,overwrite=T)
    }
    
    # multiply risk by ha
    if(do_ha==T){
      save_name_ha<-paste0(haz_risk_ha_dir,"/",crop,"_",SEV,"_ha_any.tif")
      if(crop %in% c("generic",crop_choices)){
        
        if(!file.exists(save_name_ha)|overwrite==T){
          if(crop!="generic"){
            haz_risk_ha<-haz_risk*crop_ha_tot[[crop]]
          }else{
            haz_risk_ha<-haz_risk*sum(crop_ha_tot)
          }
          
          names(haz_risk_ha)<-paste0(names(haz_risk_ha),"-ha")
          writeRaster(haz_risk_ha,file=save_name_ha,overwrite=T)
        }
      }
    }
    # multiply risk by numbers
    if(do_n==T){
      save_name_n<-paste0(haz_risk_n_dir,"/",crop,"_",SEV,"_n_any.tif")
      if(crop %in% c("generic",livestock_choices)){
      
      if(!file.exists(save_name_n)|overwrite==T){
        if(crop!="generic"){
          haz_risk_n<-haz_risk*livestock_no[[crop]]
        }else{
          haz_risk_n<-haz_risk*sum(livestock_no[[(c("total_tropical","total_highland"))]],na.rm=T)
        }
        
        names(haz_risk_n)<-paste0(names(haz_risk_n),"-n")
        writeRaster(haz_risk_n,file=save_name_n,overwrite=T)
      }
    }
    }
    
  }
  
  #### Extract Risk x Exposure by Geography  ####
  
  # VoP
  haz_risk_vop_files<-list.files(haz_risk_vop_dir,"_any.tif",full.names = T)
  haz_risk_vop_files<-haz_risk_vop_files[grepl(SEV,haz_risk_vop_files)]
  data<-terra::rast(haz_risk_vop_files)
  
  file0<-paste0(haz_risk_vop_dir,"/haz_risk_vop_any_adm0_",SEV,".parquet")
  file1<-gsub("_adm0_","_adm1_",file0)
  file2<-gsub("_adm0_","_adm2_",file0)
  
  if(!file.exists(file0)|overwrite==T){
    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure - VoP admin extraction - adm0| severity:",SEV))
    flush.console()
    
    data_ex<-admin_extract(data,Geographies["admin0"],FUN="sum")
    sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin0), dsn=file0)
    rm(data_ex)
    gc()
  }
  if(!file.exists(file1)|overwrite==T){
    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure - VoP admin extraction - adm1| severity:",SEV))
    flush.console()
    
    data_ex<-admin_extract(data,Geographies["admin1"],FUN="sum")
    sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin1), dsn=file1)
    rm(data_ex)
    gc()
  }
  if(!file.exists(file2)|overwrite==T){
    # Display progress
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure - VoP admin extraction - adm2| severity:",SEV))
    flush.console()
    
    data_ex<-admin_extract(data,Geographies["admin2"],FUN="sum")
    sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin2), dsn=file2)
    rm(data_ex)
    gc()
  }
  
  # Harvested Area
  if(do_ha==T){
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure - Harvested Area admin extraction| severity:",SEV))
    flush.console()
    
    # Display progress
    file0<-paste0(haz_risk_ha_dir,"/haz_risk_ha_any_adm0_",SEV,".parquet")
    file1<-gsub("_adm0_","_adm1_",file0)
    file2<-gsub("_adm0_","_adm2_",file0)
    
    haz_risk_ha_files<-list.files(haz_risk_ha_dir,".tif",full.names = T)
    haz_risk_ha_files<-haz_risk_ha_files[grepl(SEV,haz_risk_ha_files)]
    data<-terra::rast(haz_risk_ha_files)
    
    if(!file.exists(file0)|overwrite==T){
      data_ex<-admin_extract(data,Geographies["admin0"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin0), dsn=file0)
  
      data_ex<-admin_extract(data,Geographies["admin1"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin1), dsn=file1)
  
      data_ex<-admin_extract(data,Geographies["admin2"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin2), dsn=file2)
      rm(data_ex)
      gc()
    }
  }
  # Display progress
  if(do_n==T){
    cat('\r                                                                                                                     ')
    cat('\r',paste("Risk x Exposure - Numbers admin extraction| severity:",SEV))
    flush.console()
    
    haz_risk_n_files<-list.files(haz_risk_n_dir,".tif",full.names = T)
    haz_risk_n_files<-haz_risk_n_files[grepl(SEV,haz_risk_n_files)]
    data<-terra::rast(haz_risk_n_files)
    
    file0<-paste0(haz_risk_n_dir,"/haz_risk_n_any_adm0_",SEV,".parquet")
    file1<-gsub("_adm0_","_adm1_",file0)
    file2<-gsub("_adm0_","_adm2_",file0)
    
    if(!file.exists(file0)|overwrite==T){
      data_ex<-admin_extract(data,Geographies["admin0"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin0), dsn=file0)
      
      data_ex<-admin_extract(data,Geographies["admin1"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin1), dsn=file1)
      
      data_ex<-admin_extract(data,Geographies["admin2"],FUN="sum")
      sfarrow::st_write_parquet(obj=sf::st_as_sf(data_ex$admin2), dsn=file2)
      rm(data_ex)
      gc()
    }
  }
  
  #### Restructure Extracted Data ####
  
  # Vop
  recode_restructure_wrap(folder=haz_risk_vop_dir,
                          file="haz_risk_vop",
                          crops=c("generic",crop_choices),
                          livestock=livestock_choices,
                          exposure_var="vop",
                          Severity=SEV,
                          overwrite=overwrite,
                          any=T,
                          levels=levels)
  # Harvested area
  if(do_ha==T){
    recode_restructure_wrap(folder=haz_risk_ha_dir,
                            file="haz_risk_ha",
                            crops=c("generic",crop_choices),
                            livestock=livestock_choices,
                            exposure_var="ha",
                            Severity=SEV,
                            overwrite=overwrite,
                            any=T,
                            levels=levels)
  }
  # Numbers
  if(do_n==T){
    recode_restructure_wrap(folder=haz_risk_n_dir,
                            file="haz_risk_n",
                            crops=c("generic",crop_choices),
                            livestock=livestock_choices,
                            exposure_var="n",
                            Severity=SEV,
                            overwrite=overwrite,
                            any=T,
                            levels=levels)
  }
}

# 6) Generic Risk x Human Population #####
# This is only calculated for the risk of a "generic" hazard
haz_risk_hpop_dir<-paste0("Data/hazard_risk_hpop/",timeframe_choice)
if(!dir.exists(haz_risk_hpop_dir)){
  dir.create(haz_risk_hpop_dir)
}

overwrite<-T
haz_risk_files<-list.files(haz_risk_dir,".tif",full.names = T)
haz_risk_files<-haz_risk_files[grepl("generic_",haz_risk_files)]

for(SEV in tolower(severity_classes$class[2])){
  haz_risk_files2<- haz_risk_files[grepl(SEV,haz_risk_files)]
  
  # Intersect human population only with generic hazards
  # Display progress
  cat('\r                                                                                                                     ')
  cat('\r',paste("Risk x Exposure - Human | severity:",SEV))
  flush.console()
  
  save_name_hpop<-paste0(haz_risk_hpop_dir,"/hpop_",SEV,".tif")
  
  if((!file.exists(save_name_hpop))|overwrite==T){
    haz_risk<-terra::rast(haz_risk_files2)
    
    haz_risk_hpop_r<-haz_risk*hpop$rural
    names(haz_risk_hpop_r)<-paste0(names(haz_risk_hpop_r),"_hpop-rural")
    haz_risk_hpop_t<-haz_risk*hpop$total
    names(haz_risk_hpop_t)<-paste0(names(haz_risk_hpop_t),"_hpop-total")
    haz_risk_hpop_u<-haz_risk*hpop$urban
    names(haz_risk_hpop_u)<-paste0(names(haz_risk_hpop_u),"_hpop-urban")
    
    
    haz_risk_hpop<-c(haz_risk_hpop_r,haz_risk_hpop_t,haz_risk_hpop_u)
    
    writeRaster(haz_risk_hpop,file=save_name_hpop,overwrite=T)
    
    haz_risk_hpop_adm<-admin_extract(haz_risk_hpop,Geographies["admin0"],FUN="sum")
    st_write_parquet(obj=sf::st_as_sf(haz_risk_hpop_adm$admin0), dsn=paste0(haz_risk_hpop_dir,"/haz_risk_hpop_adm0.parquet"))
    haz_risk_hpop_adm<-admin_extract(haz_risk_hpop,Geographies["admin1"],FUN="sum")
    st_write_parquet(obj=sf::st_as_sf(haz_risk_hpop_adm$admin1), dsn=paste0(haz_risk_hpop_dir,"/haz_risk_hpop_adm1.parquet"))
    haz_risk_hpop_adm<-admin_extract(haz_risk_hpop,Geographies["admin2"],FUN="sum")
    st_write_parquet(obj=sf::st_as_sf(haz_risk_hpop_adm$admin2), dsn=paste0(haz_risk_hpop_dir,"/haz_risk_hpop_adm2.parquet"))
  }
  
}

# 7) Generic Risk x Total Harvested Area ####
crop_ha_tot_sum<-sum(crop_ha_tot)

haz_risk_croparea_dir<-paste0("Data/",timeframe_choice,"/hazard_risk_croparea")
if(!dir.exists(haz_risk_croparea_dir)){
  dir.create(haz_risk_croparea_dir)
}

overwrite<-T

for(SEV in tolower(severity_classes$class[2])){
  haz_risk_files2<-haz_risk_files[grepl(SEV,haz_risk_files)]
  # Intersect human population only with generic hazards
  haz_risk_files2<-haz_risk_files2[grepl("generic",haz_risk_files2)]
  
  # Display progress
  cat('\r                                                                                                                     ')
  cat('\r',paste("Risk x Exposure - Crop Area | severity:",SEV))
  flush.console()
  
  save_name_croparea<-paste0(haz_risk_croparea_dir,"/croparea_",SEV,".tif")
  
  if((!file.exists(save_name_croparea))|overwrite==T){
    haz_risk<-terra::rast(haz_risk_files2)
    
    haz_risk_croparea<-haz_risk*crop_ha_tot_sum
    names(haz_risk_croparea)<-paste0(names(haz_risk_croparea),"_croparea")
    
    writeRaster(haz_risk_croparea,file=save_name_croparea,overwrite=T)
    
    haz_risk_croparea_adm<-admin_extract(haz_risk_croparea,Geographies["admin0"],FUN="sum")
    st_write_parquet(obj=sf::st_as_sf(haz_risk_croparea_adm$admin0), dsn=paste0(haz_risk_croparea_dir,"/haz_risk_croparea_adm0.parquet"))
    haz_risk_croparea_adm<-admin_extract(haz_risk_croparea,Geographies["admin1"],FUN="sum")
    st_write_parquet(obj=sf::st_as_sf(haz_risk_croparea_adm$admin1), dsn=paste0(haz_risk_croparea_dir,"/haz_risk_croparea_adm1.parquet"))
    haz_risk_croparea_adm<-admin_extract(haz_risk_croparea,Geographies["admin2"],FUN="sum")
    st_write_parquet(obj=sf::st_as_sf(haz_risk_croparea_adm$admin2), dsn=paste0(haz_risk_croparea_dir,"/haz_risk_croparea_adm2.parquet"))
  }
  
}



