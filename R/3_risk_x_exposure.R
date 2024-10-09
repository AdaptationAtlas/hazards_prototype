# Please run 0_server_setup.R before executing this script
# Please run 0.0.1_process_exposure.R before executing this script
# Please run 0.3_fao_producer_prices.R to 0.4_fao_producer_prices_livestock.R before running this script
# Please run 1_make_timeseries.R if time series have not been calculated or are not available on the server
# Please run 2_calculate_risks.R before executing this script
# a) Install and load packages ####
packages <- c("terra", 
              "data.table", 
              "exactextractr",
              "s3fs",
              "sf",
              "geoarrow", 
              "arrow",
              "sfarrow", 
              "doFuture",
              "stringr", 
              "stringi",
              "httr",
              "wbstats")

# Call the function to install and load packages
pacman::p_load(char=packages)
# If you are experiencing issues with the admin_extract functions, delete the exactextractr package and use this version:  remotes::install_github("isciences/exactextractr")

# b) Load functions & wrappers ####
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

# c) Set up workspace ####

# Set scenarios and time frames to analyse
Scenarios<-c("ssp245","ssp585")
Times<-c("2021_2040","2041_2060")

# admin levels
levels<-c(admin0="adm0",admin1="adm1",admin2="adm2")

# Create combinations of scenarios and times
Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))
Scenarios[,combined:=paste0(Scenario,"-",Time)]

# Set hazards to include in analysis
hazards<-c("NTx40","NTx35","HSH_max","HSH_mean","THI_max","THI_mean","NDWS","TAI","NDWL0","PTOT","TAVG")
haz_2way<-c("PTOT","TAVG")
hazards2<-c(hazards[!hazards %in% haz_2way],paste0(haz_2way,rep(c("_L","_H"),each=2)))

haz_meta<-data.table::fread(haz_meta_url)
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
ms_codes<-data.table::fread(ms_codes_url)[,Code:=toupper(Code)]
ms_codes<-ms_codes[compound=="no"]
crop_choices<-unique(c(ms_codes[,sort(Fullname)],haz_class[,unique(crop)]))

#### Load datasets (non hazards)

# 0) Load and prepare admin vectors and exposure rasters, extract exposure by admin ####
# Note this sections has been moved to 0.0.1_process_exposure.R so can be simplied to loading the datasets ####
  # 0.1) Geographies #####
  Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
    file<-geo_files_local[i]
    data<-arrow::open_dataset(file)
    data <- data |> sf::st_as_sf() |> terra::vect()
    data
  })
  names(Geographies)<-names(geo_files_local)
  # 0.2) Exposure variables ####
    overwrite<-F
    # 0.2.1) Crops (MapSPAM) #####
      # 0.2.1.1) Crop VoP (Value of production) ######
        # To generalize it might be better to just supply a filename for the mapspam
        crop_vop_tot<-read_spam(variable="V",
                                technology="TA",
                                mapspam_dir=mapspam_dir,
                                save_dir=exposure_dir,
                                base_rast=base_rast,
                                filename="crop_vop",
                                ms_codes=ms_codes,
                                overwrite=overwrite)
  
    file_present<-any(grepl("SSA_Vusd17_TA.csv",list.files(mapspam_dir)))
    if(!file_present){
      print("MapSPAM usd17 files do not exist - please redownload the mapspam folder and/or create these")
      # https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/fao_producer_prices.R
    }else{
        crop_vop17_tot<-read_spam(variable="Vusd17",
                                  technology="TA",
                                  mapspam_dir=mapspam_dir,
                                  save_dir=exposure_dir,
                                  base_rast=base_rast,
                                  filename="crop_vop_usd17",
                                  ms_codes=ms_codes,
                                  overwrite=overwrite)
        }
    
    # 2.1.2.2) Extraction of values by admin areas
      crop_vop_tot_adm_sum<-admin_extract_wrap(data=crop_vop_tot,
                                               save_dir=exposure_dir,
                                               filename = "crop_vop",
                                               FUN="sum",
                                               varname="vop",
                                               Geographies=Geographies,
                                               overwrite=overwrite)
      
      if(file_present){
      crop_vop17_tot_adm_sum<-admin_extract_wrap(data=crop_vop17_tot,
                                                 save_dir=exposure_dir,
                                                 filename = "crop_vop_usd17",
                                                 FUN="sum",
                                                 varname="vop_usd17",
                                                 Geographies=Geographies,
                                                 overwrite=overwrite)
      }
        
      # 0.2.1.2) Crop Harvested Area #####
  
      crop_ha_tot<-read_spam(variable="H",
                              technology="TA",
                              mapspam_dir=mapspam_dir,
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
  
      # 0.2.1.3) Create Crop Masks ######
    # Need to use mapspam physical area
    mask_file<-paste0(commodity_mask_dir,"/crop_masks.tif")
    
    if(!file.exists(mask_file)|overwrite==T){
      file<-list.files(mapspam_dir,"SSA_H_TA.csv",full.names = T)
      file<-file[!grepl("_gr_",file)]
      pa<-fread(file)
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
    
    # 0.2.2) Livestock #####
      # 0.2.2.1) Livestock Mask #####
    mask_ls_file<-paste0(commodity_mask_dir,"/livestock_masks.tif")
    
    if(!file.exists(mask_ls_file)|overwrite==T){
      
      glw<-terra::rast(glw_files)
      names(glw)<-names(glw_names)
      
      glw<-glw[[c("poultry","sheep","pigs","goats","cattle")]]
      
      lus<-c(glw$cattle*0.7,glw$poultry*0.01,glw$goats*0.1,glw$pigs*0.2,glw$sheep*0.1)
      lus<-c(lus,sum(lus,na.rm=T))
      names(lus)<-c("cattle","poultry","goats","pigs","sheep","total")
      lus<-terra::mask(terra::crop(lus,Geographies$admin0),Geographies$admin0)
      
      # resample to 0.05
      lu_density<-lus/terra::cellSize(lus,unit="ha")
      lu_density<-terra::resample(lu_density,base_rast)
      
      # Classify into binary mask
      livestock_mask <- terra::ifel(lu_density > 0, 1, 0)
      
      # Split mask by highland vs tropical areas
      
      # Load highland mask
      highlands<-terra::rast(afr_highlands_file)
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
      # 0.2.2.2) Livestock Numbers (GLW) ######
    livestock_no_file<-paste0(exposure_dir,"/livestock_no.tif")
    shoat_prop_file<-paste0(glw_dir,"/shoat_prop.tif")
      
    if(!file.exists(livestock_no_file)|overwrite==T){
      
      ls_files<-list.files(glw_dir,"_Da.tif",full.names = T)
      
      glw<-terra::rast(glw_files)
      names(glw)<-names(glw_names)
      
      glw<-glw[[c("poultry","sheep","pigs","goats","cattle")]]
      
      TLU<-sum(c(glw$cattle*0.7,glw$poultry*0.01,glw$goats*0.1,glw$pigs*0.2,glw$sheep*0.1))
      
      livestock_no<-c(glw$cattle,glw$poultry,glw$goats,glw$pigs,glw$sheep,TLU)
      names(livestock_no)[nlyr(livestock_no)]<-"total"
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
  
      # 0.2.2.3) Livestock VoP ######
      # IUSD (old)
      livestock_vop_file<-file.path(exposure_dir,"livestock_vop.tif")
      
      if(!file.exists(livestock_vop_file)|overwrite==T){
        
      # Note unit is IUSD 2005
        ls_vop_files<-list.files(ls_vop_dir,"_total.tif$",full.names = T)
        ls_vop_files<-grep("h7",ls_vop_files,value=T)
        livestock_vop<-terra::rast(ls_vop_files)
      
        names(livestock_vop)<-unlist(tstrsplit(names(livestock_vop),"-",keep=3))
        
        # Remove totals
        livestock_vop<-livestock_vop[[!grepl("total",names(livestock_vop))]]
        
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
      terra::writeRaster(livestock_vop,filename = livestock_vop_file,overwrite=T)
      }else{
        livestock_vop<-terra::rast(livestock_vop_file)
      }
      
      # USD 2017 (see 0_fao_producer_prices_livestock.R)
      livestock_vop17_file<-paste0(exposure_dir,"/livestock_vop_usd17.tif")
    
      if(!file.exists(livestock_vop17_file)){
        data<-terra::rast(paste0(ls_vop_dir,"/livestock-vop-2017-usd17.tif"))
        
        livestock_vop17<-split_livestock(data=data,livestock_mask_high,livestock_mask_low)
        terra::writeRaster(livestock_vop17,filename = livestock_vop17_file,overwrite=T)
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
    
    # 0.2.3) Combine exposure totals by admin areas ####
      file<-paste0(exposure_dir,"/exposure_adm_sum.parquet")
      if(!file.exists(file)|overwrite==T){
        exposure_adm_sum_tab<-rbind(
          crop_vop_tot_adm_sum,
          crop_vop17_tot_adm_sum,
          crop_ha_tot_adm_sum,
          livestock_vop_tot_adm,
          livestock_vop17_tot_adm,
          livestock_no_tot_adm
        )
            arrow::write_parquet(exposure_adm_sum_tab,file)
      }
    
    # Create a table for total values 
    file<-file.path(exposure_dir,"admin0_totals")
      
    if(!file.exists(file)|overwrite==T){
      # Load raw mapspam
      file<-list.files(mapspam_dir,paste0("V", "_", "TA", ".csv"),full.names=T)
      file<-file[!grepl("_gr_",file)]
      data <- fread(file)
      
      crop_columns<-grep("_a$",colnames(data),value=T)
  
      # Calculating the sum for each crop within each iso3 group.
      vop_adm0_total <- data[, .(total_crops = sum(unlist(.SD))), by = .(iso3), .SDcols = crop_columns]
      
      vop_adm0_total<-merge(vop_adm0_total,Geographies$admin0[,c("iso3","admin0_name")])
      
      # Add livestock
      vop_adm0_total<-merge(vop_adm0_total,livestock_vop_tot_adm[is.na(admin1_name),list(total_livestock=sum(value)),by=admin0_name],by="admin0_name")
      
      # Grand total
      vop_adm0_total<-vop_adm0_total[,total:=total_crops+total_livestock][,exposure:="vop"]
      
      fwrite(vop_adm0_total,file.path(exposure_dir,"admin0_totals.csv"))
    }
  
    # 0.2.4) Population ######
    
      file<-paste0(exposure_dir,"/hpop.tif")
      if(!file.exists(file)){
        local_files<-list.files(hpop_dir,".tif",full.names = T)
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
  
      # 0.2.4.1) Extraction of hpop by admin areas ####
    hpop_admin<-admin_extract_wrap(data=hpop,
                       save_dir=exposure_dir,
                       filename = "hpop",
                       FUN="sum",
                       varname="number",
                       Geographies=Geographies,
                       overwrite=overwrite)
      
# 1) Extract hazard risk by admin ####
  # 1.1) Solo and interactions combined into a single file (not any hazard) #####
overwrite<-T
files<-list.files(haz_risk_dir,".tif$",full.names = T)
files_solo<-files[!grepl("-int[.]tif$",files)]
files_int<-grep("-int[.]tif$",files,value = T)

admin_extract_wrap2(files=files_solo,
                    save_dir = haz_risk_dir,
                    filename="haz_risk_solo",
                    severity=unlist(severity_classes[,1]),
                    Geographies=Geographies,
                    overwrite=overwrite)

admin_extract_wrap2(files=files_int,
                    save_dir = haz_risk_dir,
                    filename="haz_risk_int",
                    severity=unlist(severity_classes[,1]),
                    Geographies=Geographies,
                    overwrite=overwrite)
  # 1.2) Restructure extracted data ####

  restructure_parquet(filename = "haz_risk_int",
                    save_dir = haz_risk_dir,
                    severity = unlist(severity_classes[,1]),
                    overwrite=overwrite,
                    crops = c("generic",crop_choices),
                    livestock=livestock_choices,
                    Scenarios=Scenarios,
                    hazards=haz_meta[,unique(type)])

restructure_parquet(filename = "haz_risk_solo",
                    save_dir = haz_risk_dir,
                    severity = unlist(severity_classes[,1]),
                    overwrite=overwrite,
                    crops = c("generic",crop_choices),
                    livestock=livestock_choices,
                    Scenarios=Scenarios,
                    hazards=haz_meta[,unique(type)])

  # 1.3) Optional: Apply Crop Mask to Classified Hazard Risk ####

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

# 2) Extract hazard means and sd by admin ####
  overwrite<-T
  # 2.1) Extract mean hazards ####
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
    write_parquet(sf::st_as_sf(data_ex$admin0), file0)
    write_parquet(sf::st_as_sf(data_ex$admin1), file1)
    write_parquet(sf::st_as_sf(data_ex$admin2), file2)
  }

  # 2.2) Compile mean hazards into tabular form ####
  filename<-gsub("adm0","adm",file0)
  
  if(!file.exists(filename)|overwrite==T){
    # Extract data from vector files and restructure into tabular form
    data_ex<-rbindlist(lapply(1:length(levels),FUN=function(i){
      level<-levels[i]
      print(level)
      
      data<-data.table(data.frame(arrow::read_parquet(paste0(folder,"/",file,"_",levels[i],".parquet"))))
      
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
      
      data[,variable:=gsub("historical","historic-NA-historic",variable[1]),by=variable
           ][,variable:=stringi::stri_replace_all_regex(variable[1],pattern=c("max_max","min_min","mean_mean"),replacement=c("max-max","min-min","mean-mean"),vectorise_all = F),by=variable
                  ][,variable:=gsub(".","_",variable[1],fixed=T),by=variable]
      
      data[,variable:=stringi::stri_replace_all_regex(variable[1],
                                                      pattern=Scenarios[Scenario!="historic",paste0("_",Time)],
                                                      replacement = Scenarios[Scenario!="historic",paste0("-",Time)],
                                                      vectorise_all = F),by=variable]
      
      data[,variable:=stringi::stri_replace_all_regex(variable[1],
                                                      pattern=paste0("_",haz_meta$code),
                                                      replacement =paste0("-",haz_meta$code),
                                                      vectorise_all = F),by=variable]
      
      data[,variable:=stringi::stri_replace_all_regex(variable[1],
                                                      pattern=paste0("_",Scenarios$Scenario),
                                                      replacement =paste0("-",Scenarios$Scenario),
                                                      vectorise_all = F),by=variable]
      
      data[,variable:=stringi::stri_replace_all_regex(variable[1],
                                                      pattern=paste0(Scenarios$Scenario,"_"),
                                                      replacement =paste0(Scenarios$Scenario,"-"),
                                                      vectorise_all = F),by=variable]
      
      variable<-cbind(data$variable,data.table(do.call("cbind",tstrsplit(data$variable,"-"))[,-1]))
      colnames(variable)<-c("variable","scenario","model","timeframe","hazard","hazard_stat")
      variable[is.na(hazard_stat),hazard:=gsub("_","-",hazard)
               ][is.na(hazard_stat),hazard_stat:=unlist(tstrsplit(hazard,"-",keep=2))
                 ][,hazard:=unlist(tstrsplit(hazard,"-",keep=1))]
    
      data<-merge(data,unique(variable),all.x=T)[,variable:=NULL]
      
     
      
    }),fill=T)
    data_ex<-data_ex[,c(1,8,9,3,5,4,6,7,2)]
    
    # Save mean values as feather object
    arrow::write_parquet(data_ex,filename)
  }
  
# 3) Extract hazard timeseries by admin ####
haz_timeseries_files<-list.files(haz_timeseries_dir,".tif",full.names = T)
haz_timeseries_files<-grep(paste(hazards,collapse = "|"),haz_timeseries_files,value=T)

# Limit files to ensemble mean (very large files otherwise)
  # See 2.1_create_monthly_haz_tables.R for ideas on alternative methods for getting to this information
  # If needed per model suggest looping over models (or futher file splitting measures)
haz_timeseries_files<-grep("ENSEMBLEmean",haz_timeseries_files,value = T)

# Load all timeseries data into a raster stack
haz_timeseries<-terra::rast(haz_timeseries_files)

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
write_parquet(sf::st_as_sf(haz_timeseries_adm$admin0), paste0(haz_timeseries_dir,"/haz_timeseries_adm0.parquet"))

# Extract by admin1
haz_timeseries_adm<-admin_extract(haz_timeseries,Geographies["admin1"],FUN="mean")
write_parquet(sf::st_as_sf(haz_timeseries_adm$admin1), paste0(haz_timeseries_dir,"/haz_timeseries_adm1.parquet"))

# Extract by admin2
haz_timeseries_adm<-admin_extract(haz_timeseries,Geographies["admin2"],FUN="mean")
write_parquet(sf::st_as_sf(haz_timeseries_adm$admin2), paste0(haz_timeseries_dir,"/haz_timeseries_adm2.parquet"))

# Restructure data into tabular form
filename<-paste0(haz_timeseries_dir,"/haz_timeseries.parquet")
# Extract data from vector files and restructure into tabular form
haz_timeseries_tab<-rbindlist(lapply(1:length(levels),FUN=function(i){
  level<-levels[i]
  print(level)
  
  data<-data.table(data.frame(read_parquet(paste0(haz_timeseries_dir,"/haz_timeseries_",levels[i],".parquet"))))
  
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
  
  data[,variable:=gsub("historical","historic-NA-historic",variable[1]),by=variable
  ][,variable:=stringi::stri_replace_all_regex(variable[1],pattern=c("max_max","min_min","mean_mean"),replacement=c("max-max","min-min","mean-mean"),vectorise_all = F),by=variable
  ][,variable:=gsub(".","_",variable[1],fixed=T),by=variable
  ][,variable:=gsub("_year","-",variable[1],fixed=T),by=variable]
  
  data[,variable:=stringi::stri_replace_all_regex(variable[1],
                                                  pattern=Scenarios[Scenario!="historic",paste0("_",Time)],
                                                  replacement = Scenarios[Scenario!="historic",paste0("-",Time)],
                                                  vectorise_all = F),by=variable]
  
  data[,variable:=stringi::stri_replace_all_regex(variable[1],
                                                  pattern=Scenarios[,paste0(Time,"_")],
                                                  replacement = Scenarios[,paste0(Time,"-")],
                                                  vectorise_all = F),by=variable]
  
  data[,variable:=stringi::stri_replace_all_regex(variable[1],
                                                  pattern=paste0("_",haz_meta$code),
                                                  replacement =paste0("-",haz_meta$code),
                                                  vectorise_all = F),by=variable]
  
  data[,variable:=stringi::stri_replace_all_regex(variable[1],
                                                  pattern=paste0("_",Scenarios$Scenario),
                                                  replacement =paste0("-",Scenarios$Scenario),
                                                  vectorise_all = F),by=variable]
  
  data[,variable:=stringi::stri_replace_all_regex(variable[1],
                                                  pattern=paste0(Scenarios$Scenario,"_"),
                                                  replacement =paste0(Scenarios$Scenario,"-"),
                                                  vectorise_all = F),by=variable]
  
  variable<-cbind(data$variable,data.table(do.call("cbind",tstrsplit(data$variable,"-"))[,-1]))
  colnames(variable)<-c("variable","scenario","model","timeframe","hazard","hazard_stat","year")
  variable[is.na(hazard_stat),hazard:=gsub("_","-",hazard)
  ][is.na(hazard_stat),hazard_stat:=unlist(tstrsplit(hazard,"-",keep=2))
  ][,hazard:=unlist(tstrsplit(hazard,"-",keep=1))]
  
  data<-merge(data,unique(variable),all.x=T)[,variable:=NULL]
  
  return(data)
  
}),fill=T)

haz_timeseries_tab<-haz_timeseries_tab[,.(admin0_name,admin1_name,admin2_name,scenario,model,timeframe,hazard,hazard_stat,year,value)]

# Save mean values as feather object
arrow::write_parquet(haz_timeseries_tab,filename)

# 4) Hazard risk x exposure ####
  # 4.0) Set-up ####
    do_vop<-T
    do_vop17<-T
    do_ha<-T
    do_n<-T
    overwrite<-T
    crop_vop_path<-file.path(exposure_dir,"crop_vop.tif")
    crop_vop_usd17_path<-file.path(exposure_dir,"crop_vop_usd17.tif")
    crop_ha_path<-file.path(exposure_dir,"crop_ha.tif")
    
    livestock_vop_path<-file.path(exposure_dir,"livestock_vop.tif")
    livestock_vop_usd17_path<-file.path(exposure_dir,"livestock_vop_usd17.tif")
    livestock_no_path<-file.path(exposure_dir,"livestock_no.tif")
    
    worker_n<-10
    
    # Crop choices only 
    crop_choices<-crop_choices[!grepl("_tropical|_highland",crop_choices)]
    
    # Download pre-baked hazard_risk tifs from the s3?
    if(F){
      # Specify s3 prefix (folder path)
      folder_path <- haz_risk_dir
      s3_bucket <-paste0("s3://digital-atlas/risk_prototype/data/hazard_risk/",timeframe_choice)
      
      
      # List files in the specified S3 bucket and prefix
      files_s3<-grep(".tif",s3$dir_ls(s3_bucket),value=T)
      
      files_local<-file.path(folder_path,basename(files_s3))
      
      # If data does not exist locally download from S3 bucket
      for(i in 1:length(files_local)){
        file<-files_local[i]
        if(!file.exists(file)|update==T){
          cat("downloading file",i,"/",length(files_local),"\n")
          s3$file_download(files_s3[i],file,overwrite=T)
        }
      }
    }
    
    # List files
    files<-list.files(haz_risk_dir,".tif$",full.names = T)
    
  # 4.1) Multiply Hazard Risk by Exposure #####
    # Note if you are finding the parallel processing is not working reload /reimport packages and functions.
    
     risk_x_exposure<-function(file,
                               save_dir,
                               variable,
                               overwrite,
                               crop_exposure_path=NULL,
                               livestock_exposure_path=NULL,
                               crop_choices,
                               verbose=F){
      if(verbose){print(file)}
       
      data<-terra::rast(file)
      crop<-unlist(data.table::tstrsplit(basename(file),"-",keep=1))
      save_name<-file.path(save_dir,gsub(".tif",paste0("-",variable,".tif"),basename(file)))

      if(!file.exists(save_name)|overwrite==T){
        
        if(!is.null(crop_exposure_path)){
          crop_exposure<-terra::rast(crop_exposure_path)
        }
        
        if(!is.null(livestock_exposure_path)){
          livestock_exposure<-terra::rast(livestock_exposure_path)
        }
        
        # vop
        if(crop!="generic"){
          if(crop %in% crop_choices & variable!="n"){
            exposure<-crop_exposure[[crop]]
            data_ex<-data*exposure
          }else{
            if(variable !="ha" & !crop %in% crop_choices){
            exposure<-livestock_exposure[[crop]]
            data_ex<-data*exposure
            }else{
              data_ex<-NA
            }
          }
        }else{
          if(variable!="n"){
            exposure<-sum(crop_exposure)
            data_ex<-data*exposure
          }else{
            data_ex<-NA
          }
        }
        
        if(class(data_ex)=="SpatRaster"){
          names(data_ex)<-paste0(names(data_ex),"-",variable)
          terra::writeRaster(data_ex,file=save_name,overwrite=T)
        }
      }
      
     }

      if(do_vop){
        future::plan("multisession", workers = worker_n)
        future.apply::future_lapply(files,
                                    risk_x_exposure,             
                                    save_dir=haz_risk_vop_dir,
                                    variable="vop",
                                    overwrite = overwrite,
                                    crop_exposure = crop_vop_usd17_path,
                                    livestock_exposure=livestock_vop_usd17_path,
                                    crop_choices=crop_choices)
        future::plan(sequential)
      }
      
      if(do_vop17){
        future::plan("multisession", workers = worker_n)
        future.apply::future_lapply(files,
                                    risk_x_exposure,             
                                    save_dir=haz_risk_vop17_dir,
                                    variable="vop",
                                    overwrite = overwrite,
                                    crop_exposure = crop_vop_path,
                                    livestock_exposure=livestock_vop_path,
                                    crop_choices=crop_choices)
        future::plan(sequential)
      }
      
      if(do_ha){
        future::plan("multisession", workers = worker_n)
        future.apply::future_lapply(files,
                                    risk_x_exposure,             
                                    save_dir=haz_risk_ha_dir,
                                    variable="ha",
                                    overwrite = overwrite,
                                    crop_exposure = crop_ha_path,
                                    crop_choices=crop_choices)
        future::plan(sequential)
      }
      
      if(do_n){
        future::plan("multisession", workers = worker_n)
        future.apply::future_lapply(files,
                                    risk_x_exposure,             
                                    save_dir=haz_risk_n_dir,
                                    variable="n",
                                    overwrite = overwrite,
                                    livestock_exposure=livestock_no_path,
                                    crop_choices=crop_choices)
        future::plan(sequential)
      }
  # 4.2) Extract Risk x Exposure by Geography #####

    for(INT in c(T,F)){
      if(do_vop){
        cat(SEV,"- interaction =",INT,"variable = vop\n")
        
      haz_risk_exp_extract(severity_classes,
                           interactions=INT,
                           folder=haz_risk_vop_dir,
                           overwrite=overwrite,
                           Geographies=Geographies,
                           rm_crop=NULL,
                           rm_haz=NULL)
      }
    
    if(do_vop17){
      cat(SEV,"- interaction =",INT,"variable = vop17\n")
      
      haz_risk_exp_extract(severity_classes,
                           interactions=INT,
                           folder=haz_risk_vop17_dir,
                           overwrite=overwrite,
                           Geographies=Geographies,
                           rm_crop=NULL,
                           rm_haz=NULL)
    }
    
    if(do_ha){
      cat(SEV,"- interaction =",INT,"variable = ha\n")
      
      haz_risk_exp_extract(severity_classes,
                           interactions=INT,
                           folder=haz_risk_vop_dir,
                           overwrite=overwrite,
                           Geographies=Geographies,
                           rm_crop=NULL,
                           rm_haz=NULL)
      }
    
    if(do_n){
      cat(SEV,"- interaction =",INT,"variable = n\n")
      
        haz_risk_exp_extract(severity_classes,
                             interactions=INT,
                             folder=haz_risk_vop_dir,
                             overwrite=overwrite,
                             Geographies=Geographies,
                             rm_crop=NULL,
                             rm_haz=NULL)
      }
  }
  
  if(F){
    # Check resulting files
    (file<-list.files(haz_risk_vop_dir,"parquet",full.names = T))
    data<-sfarrow::st_read_parquet(file[4])
    names(data)
  }
        
  # 4.3) Restructure Extracted Data ####

  for(SEV in tolower(severity_classes$class)){
    for(INT in c(T,F)){
      if(do_vop==T){
        cat(SEV,"- interaction =",INT,"variable = vop\n")
        recode_restructure_wrap(folder=haz_risk_vop_dir,
                                file="adm",
                                crops=crop_choices,
                                livestock=livestock_choices,
                                exposure_var="vop",
                                severity=SEV,
                                overwrite=overwrite,
                                levels=levels,
                                interaction=INT,
                                hazards=haz_meta[,unique(type)])
      }
      
      if(do_vop17==T){
        cat(SEV,"- interaction =",INT,"variable = vop17\n")
        # Vop
        recode_restructure_wrap(folder=haz_risk_vop17_dir,
                                file="adm",
                                crops=crop_choices,
                                livestock=livestock_choices,
                                exposure_var="vop",
                                severity=SEV,
                                overwrite=overwrite,
                                levels=levels,
                                interaction=INT,
                                hazards=haz_meta[,unique(type)])
      }
      
      # Harvested area
      if(do_ha==T){
        cat(SEV,"- interaction =",INT,"variable = do_ha\n")
        recode_restructure_wrap(folder=haz_risk_ha_dir,
                                file="adm",
                                crops=crop_choices,
                                livestock=livestock_choices,
                                exposure_var="ha",
                                Severity=SEV,
                                overwrite=overwrite,
                                levels=levels,
                                interaction=INT,
                                hazards=haz_meta[,unique(type)])
      }
      
      # Numbers
      if(do_n==T){
        cat(SEV,"- interaction =",INT,"variable = do_n\n")
        recode_restructure_wrap(folder=haz_risk_n_dir,
                                file="adm",
                                crops=crop_choices,
                                livestock=livestock_choices,
                                exposure_var="number",
                                Severity=SEV,
                                overwrite=overwrite,
                                levels=levels,
                                interaction=INT,
                                hazards=haz_meta[,unique(type)])
      }
    }
    
  }
  
  if(F){
     # Check results
    (files<-list.files(haz_risk_vop17_dir,"_adm_",full.names = T))
    for(i in 1:length(files)){
      file<-files[i]
      print(file)
      data<-arrow::read_parquet(file)
      print(head(data))
      print(data[,unique(hazard)])
      print(data[,unique(hazard_vars)])
      print(data[,unique(crop)])
  }
  }
    
  