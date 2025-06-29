#### Climate Hazard Frequency Classification and Interaction Stacking
#
# Script: R/2_calculate_haz_freq.R
# Author: Dr. Peter R. Steward (p.steward@cgiar.org)
# Organization: Alliance of Bioversity International and the International Center for Tropical Agriculture (CIAT)
# Project: Africa Agriculture Adaptation Atlas (AAAA)
#
# Description:
# This script classifies and processes time series climate hazard rasters using
# crop- and livestock-specific thresholds, then computes hazard frequency (0–1)
# and interaction layers across scenarios, models, and timeframes.
#
# Key functions include:
#   1. Classify each time series layer into binary exceedance rasters based on 
#      crop/livestock-specific thresholds from Ecocrop and hazard metadata.
#   2. Calculate per-pixel hazard frequency (mean of exceedance) for each
#      classified raster.
#   3. Ensemble hazard frequencies across GCMs.
#   4. Stack hazard layers into crop- or livestock-specific frequency bundles.
#   5. Create 3-way hazard interaction layers (heat × wet × dry) for all 
#      relevant combinations of crops, severity classes, and models.
#   6. Merge per-hazard interaction stacks into crop-specific outputs.
#
# Inputs:
#   - Hazard time series rasters (historical and projected)
#   - Ecocrop and MapSPAM crop metadata
#   - Hazard classification and interaction definitions
#
# Outputs:
#   - Classified hazard rasters by threshold
#   - Per-pixel hazard frequencies and ensemble summaries
#   - Multi-layer risk stacks per crop and model
#   - Raster layers for 3-way interactions across severity classes
#
# Usage:
#   This script must be run **after**:
#     - R/0_server_setup.R
#     - R/1_make_timeseries.R
#   It is recommended to run only selected steps (e.g., classification, frequency)
#   based on control flags (`run1`, `run2`, etc.) and system resources.
#
# Notes:
# - Rasters are written as Cloud Optimized GeoTIFFs (COGs)
# - Parallel processing and progress bars are enabled using `future`, `progressr`
# - Intermediate raster integrity is validated before proceeding to next stages
################################################################################
cat("Starting 2_calculate_haz_freq.R script/n")
# 0) Set-up workspace ####
  ## 0.1) Load R functions & packages ####
  
  # List of packages to be loaded
  packages <- c("terra", 
                "data.table", 
                "future",
                "future.apply",
                "progressr",
                "parallel",
                "doFuture",
                "httr",
                "s3fs",
                "stringr", 
                "stringi",
                "httr",
                "xml2")
  
  pacman::p_load(packages,character.only=T)
  
  # Source functions from github
  source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))
  
  ## 0.2) Set up workspace #####
    ### 0.2.1) Set scenarios,time frames & crops/livestock ####
    Scenarios<-c("ssp126","ssp245","ssp370","ssp585")
    Times<-c("2021_2040","2041_2060","2061_2080","2081_2100")
    
    # Create combinations of scenarios and times
    Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))
    Scenarios[,combined:=paste0(Scenario,"-",Time)]
    
    cat("Scenarios x timeframes:",Scenarios$combined,"\n")
    
    # Load spam metadata
    ms_codes<-data.table::fread(ms_codes_url, showProgress = FALSE)[,Code:=toupper(Code)]
    ms_codes<-ms_codes[compound=="no" & !is.na(Code)]
    
    ### 0.2.2) Set master hazards & create haz_class table ####
    # haz_class table is the source of truth for interactions & crop specific information
    hazards<-c("NTx40","NTx35","HSH_max","HSH_mean","THI_max","THI_mean","NDWS","TAI","NDWL0","PTOT","TAVG") # NDD is not being used as it cannot be projected to future scenarios with delta method
    
    cat("Full hazards =",hazards,"\n")
    
    haz_meta<-data.table::fread(file.path(project_dir,"metadata","haz_metadata.csv"))
    haz_meta[variable.code %in% hazards]
    haz_meta[,code2:=paste0(haz_meta$code,"_",haz_meta$`function`)]
    
    haz_class<-data.table::fread(haz_class_url, showProgress = FALSE)
    haz_class<-haz_class[index_name %in% hazards,list(index_name,description,direction,crop,threshold)]
    haz_classes<-unique(haz_class$description)
    
    # duplicate generic non-heat stress variables for livestock
    livestock<-livestock<-haz_class[grepl("cattle|goats|poultry|pigs|sheep",crop),unique(crop)]
    non_heat<-c("NTx40","NTx35","NDWS","TAI","NDWL0","PTOT") # NDD is not being used as it cannot be projected to future scenarios
    
    haz_class<-rbind(haz_class[crop=="generic"],
                     rbindlist(lapply(1:length(livestock),FUN=function(i){
                       rbind(haz_class[crop=="generic" & index_name %in% non_heat][,crop:=livestock[i]],haz_class[crop==livestock[i]])
                     }))
    )
    
    # Pull out severity classes and associate impact scores
    severity_classes<-unique(fread(haz_class_url)[,list(description,value)])
    setnames(severity_classes,"description","class")
    
      #### 0.2.2.1) Generate hazard thresholds from ecocrop ######
      
      # read in ecocrop
      ecocrop<-fread(ecocrop_url, showProgress = FALSE)
      ecocrop[,Temp_Abs_Min:=as.numeric(Temp_Abs_Min)
      ][,Temp_Abs_Max:=as.numeric(Temp_Abs_Max)
      ][,Rain_Abs_Min:=as.numeric(Rain_Abs_Min)
      ][,Rain_Abs_Max:=as.numeric(Rain_Abs_Max)]
      
      # Using the mapspam species transpose the ecocrop data into mod, severe and extreme hazards (match the format of the haz_class data.table)
      description<-c("Moderate","Severe","Extreme")
      ec_haz<-rbindlist(lapply(1:nrow(ms_codes),FUN=function(i){
        crop<-ms_codes[i,sci_name]
        crop_common<-ms_codes[i,Fullname]
        
        crops<-unlist(strsplit(crop,";"))
        
        ec_haz<-rbindlist(lapply(1:length(crops),FUN=function(j){
          ecrop<-ecocrop[species==crops[j]]
          
          if(nrow(ecrop)>0){
            cat(i,"-",j," | ",crop_common,"/",crops[j],"                \r")
            
            # PTOT low
            ptot_low<-data.table(index_name="PTOT",
                                 description=description,
                                 direction="<",
                                 crop=crop_common,
                                 threshold=c(
                                   unlist(ecrop$Rain_Opt_Min), # Moderate
                                   (unlist(ecrop$Rain_Abs_Min)+unlist(ecrop$Rain_Opt_Min))/2, # Severe
                                   unlist(ecrop$Rain_Abs_Min))) # Extreme
            
            # PTOT high
            ptot_high<-data.table(index_name="PTOT",
                                  description=description,
                                  direction=">",
                                  crop=crop_common,
                                  threshold=c(
                                    unlist(ecrop$Rain_Opt_Max), # Moderate
                                    ceiling((unlist(ecrop$Rain_Opt_Max)+unlist(ecrop$Rain_Abs_Max))/2), # Severe
                                    unlist(ecrop$Rain_Abs_Max))) # Extreme
            
            # TAVG low
            tavg_low<-data.table(index_name="TAVG",
                                 description=description,
                                 direction="<",
                                 crop=crop_common,
                                 threshold=c(
                                   unlist(ecrop$temp_opt_min), # Moderate
                                   (unlist(ecrop$temp_opt_min)+unlist(ecrop$Temp_Abs_Min))/2, # Severe
                                   unlist(ecrop$Temp_Abs_Min))) # Extreme
            
            # TAVG high
            tavg_high<-data.table(index_name="TAVG",
                                  description=description,
                                  direction=">",
                                  crop=crop_common,
                                  threshold=c(
                                    unlist(ecrop$Temp_Opt_Max), # Moderate
                                    (unlist(ecrop$Temp_Opt_Max)+unlist(ecrop$Temp_Abs_Max))/2, # Severe
                                    unlist(ecrop$Temp_Abs_Max))) # Extreme
            
            # NTxCrop - moderate  (>optimum)
            ntxcrop_m<-data.table(index_name=paste0("NTxM",ecrop$Temp_Opt_Max),
                                  description=description,
                                  direction=">",
                                  crop=crop_common,
                                  threshold=c(7, # Moderate
                                              14, # Severe
                                              21)) # Extreme
            
            # NTxCrop - severe  
            ntxcrop_s<-data.table(index_name=paste0("NTxS",ceiling((unlist(ecrop$Temp_Opt_Max)+unlist(ecrop$Temp_Abs_Max))/2)),
                                  description=description,
                                  direction=">",
                                  crop=crop_common,
                                  threshold=c(7, # Moderate
                                              14, # Severe
                                              21)) # Extreme
            
            
            # NTxCrop extreme (>absolute)
            ntxcrop_e<-data.table(index_name=paste0("NTxE",ecrop$Temp_Abs_Max),
                                  description=description,
                                  direction=">",
                                  crop=crop_common,
                                  threshold=c(1, # Moderate
                                              5, # Severe
                                              10)) # Extreme
            
            rbind(ptot_low,ptot_high,tavg_low,tavg_high,ntxcrop_m,ntxcrop_s,ntxcrop_e)
          }else{
            print(paste0(i,"-",j," | ",crop, " - ERROR NO MATCH"))
            NULL
          }
        }))
        
        # Average NTxM/S/E thresholds
        ec_haz<-unique(ec_haz[grep("NTxS",index_name),index_name:=paste0("NTxS",ceiling(mean(as.numeric(substr(index_name,5,6)))))
        ][grep("NTxM",index_name),index_name:=paste0("NTxM",ceiling(mean(as.numeric(substr(index_name,5,6)))))
        ][grep("NTxE",index_name),index_name:=paste0("NTxE",ceiling(mean(as.numeric(substr(index_name,5,6)))))])
        
        # Average threholds where multiple crops exist for a mapspam commodity
        ec_haz<-ec_haz[,list(threshold=mean(threshold,na.rm=T)),by=list(index_name,description,direction,crop)]
        
        ec_haz
      }))
      
      
      #### 0.2.2.2) !!TO DO!! Generate hazard thresholds using CCW crop climate profiles ####
      #### 0.2.2.3) Replicate generic hazards that are not TAVG or PTOT for each crop ####
      # Dev Note: Testing removal of generic hazards when creating crop stacks
      replicate_generic_hazards<-F
      cat("2 - ","Replication of generic hazard per crop is set to",replicate_generic_hazards)
      if(replicate_generic_hazards){
        haz_class2<-rbindlist(lapply(1:nrow(ms_codes),FUN=function(i){
          Y<-ec_haz[crop==ms_codes[i,Fullname]]
          X<-haz_class[!index_name %in% ec_haz[,unique(index_name)]]
          # Remove THI & HSH this is not for crops
          X<-X[!grepl("THI|HSH",index_name)]
          X$crop<-ms_codes[i,Fullname]
          rbind(Y,X)
        }))
      }else{
        haz_class2<-copy(ec_haz)
      }
      haz_class<-rbind(haz_class,haz_class2)
      
      #### 0.2.2.4) Update fields in haz_class table ####
      
      haz_class[crop=="generic",crop:="generic_crop"
      ][,crop:=gsub(" |_","-",crop)]
      
      haz_class[,direction2:="G"
      ][direction=="<",direction2:="L"
      ][,index_name2:=index_name]
      
      haz_class[index_name %in% c("TAVG","PTOT"),index_name2:=paste0(index_name,"_",direction2)]
      
      haz_class[grepl("NTxE",index_name),index_name2:="NTxE"]
      haz_class[grepl("NTxM",index_name),index_name2:="NTxM"]
      haz_class[grepl("NTxS",index_name),index_name2:="NTxS"]
      
      haz_class[,index_name:=gsub("NTxE|NTxM|NTxS","NTx",index_name)]
      haz_class<-unique(haz_class)
      
      # Add summary function description to haz_class
      haz_class<-merge(haz_class,unique(haz_meta[,c("variable.code","function")]),by.x="index_name",by.y="variable.code",all.x=T)
      haz_class[,code2:=paste0(index_name,"_",`function`)][,code2:=gsub("_G_|_L_","_",code2)]
      
      # Add hazard type to haz_class
      haz_class[,filename:=paste0(index_name,"-",direction2,threshold,".tif")]
      haz_class[,match_field:=code2
      ][grepl("PTOT|TAVG",code2),match_field:=paste0(index_name2,"_",`function`),by=.I]
      
      haz_class<-merge(haz_class,unique(haz_meta[,list(code2,type)]),by.y="code2",by.x="match_field",all.x=T)
      haz_class[,match_field:=NULL]
      haz_class[,haz_filename:=paste0(type,"-",index_name2,"-",crop,"-",description)]
      
      #### 0.2.2.5) Get crops for seasonal calculations (annual crops) ####
      spam_meta<-data.table::fread(file.path(project_dir,"metadata","SpamCodes.csv"))
      short_crops<-spam_meta[long_season==F,Fullname]
      short_crops<-gsub(" ","-",short_crops)
    ### 0.2.3) Create combinations of scenarios and hazards ####
    scenarios_x_hazards<-data.table(Scenarios,Hazard=rep(hazards,each=nrow(Scenarios)))[,Scenario:=as.character(Scenario)][,Time:=as.character(Time)]
    ### 0.2.4) Set hazard interactions ####
      #### 0.2.4.1) Set interactions ####
      
      # Dev note: This section is somewhat hacky and needs a rethink in future, it has been created adhoc not 
      # strategically. The a possible issue is that we cannot have hazards that are both fixed (i.e. generic crop) and
      # crop specific. Overall the conceptualization of how we manage fixed vs variable hazards needs some 
      # simplification. A stop has been introduced to prevent any combinations that cannot be handled.
      
      # In previous iterations fixed hazards were calculated for each crop which ended up repeating the same hazard
      # combinations for each crop, obviously a waste of time and storage. The process was revised so only
      # "generic-crop" is calculated for the fixed hazards. At later stages the generic crop hazard interaction freq is intersected with crop
      # specific exposure (script 3).
      
      # Crop interactions (each row is a combination of heat, wet and dry variables)
      crop_interactions<-data.table(heat_simple=c("NTx35","NTxS"),
                                    wet_simple=c("NDWL0","PTOT_G"),
                                    dry_simple=c("NDWS","PTOT_L"),
                                    heat_fixed=c(T,F),
                                    wet_fixed=c(T,F),
                                    dry_fixed=c(T,F),
                                    type="crop")
      
      # Animal interactions (each row is a combination of heat, wet and dry variables)
      animal_interactions<-data.table(heat_simple=c("THI_max","THI_max"),
                                      wet_simple=c("NDWL0","PTOT_G"),
                                      dry_simple=c("NDWS","PTOT_L"),
                                      heat_fixed=c(F,F),
                                      wet_fixed=c(T,F),
                                      dry_fixed=c(T,F),
                                      type="animal")
      
      interaction_haz<-unique(c(unlist(crop_interactions[,.(heat_simple,wet_simple,dry_simple)]),
                                unlist(animal_interactions[,.(heat_simple,wet_simple,dry_simple)])))
      
      interaction_haz_fixed<-unique(c(crop_interactions[heat_fixed==T,heat_simple],
                                      crop_interactions[wet_fixed==T,wet_simple],
                                      crop_interactions[dry_fixed==T,dry_simple],
                                      animal_interactions[heat_fixed==T,heat_simple],
                                      animal_interactions[wet_fixed==T,wet_simple],
                                      animal_interactions[dry_fixed==T,dry_simple]))
      
      interaction_haz_free<-unique(c(crop_interactions[heat_fixed==F,heat_simple],
                                     crop_interactions[wet_fixed==F,wet_simple],
                                     crop_interactions[dry_fixed==F,dry_simple],
                                     animal_interactions[heat_fixed==F,heat_simple],
                                     animal_interactions[wet_fixed==F,wet_simple],
                                     animal_interactions[dry_fixed==F,dry_simple]))
      
      
      if(any(interaction_haz_fixed %in% interaction_haz_free)){
        stop("The system has not been tested with the same hazards used in both fixed (does not vary by crop, fixed =T) vs crop specific (fixed=F) combinations. This may lead to issues when assembling crop risk stacks for solo hazards (simplified hazard names could end up being duplicated for different thresholds in fixed vs crop specific scenarios). ")
      }
      
      cat("Crop interaction variables\n")
      print(crop_interactions)
      cat("Animal interaction variables\n")
      print(animal_interactions)
      
      #### 0.2.4.2) Create crop/livestock specific hazard table for interactions ####
      # Set crops and livestock included in the analysis (default is all the commodities in the spam metadata file)
      crop_choices<-haz_class[,unique(crop)]
      
      # Crops
      crop_choices2<-crop_choices[!grepl("-tropical|-highland",crop_choices)]
      
      # Create a unique list of all the 3-way combinations required for the crops and severity classes selected
      # Function to replace exact matches
      replace_exact_matches <- function(strings, old_values, new_values) {
        replacement_map <- setNames(new_values, old_values)
        return(replacement_map[strings])
      }
      
      combinations_c<-unique(rbindlist(lapply(1:length(crop_choices2),FUN=function(i){
        crop_focus<-crop_choices2[i]
        rbindlist(lapply(1:length(severity_classes$class),FUN=function(j){
          severity_focus<-severity_classes$class[j]
          X<-copy(crop_interactions)
          haz_rename<-haz_class[crop==crop_focus & description==severity_focus,
                                list(old=index_name2,new=gsub("_","-",gsub(".tif","",filename)))]
          
          X[,heat:=replace_exact_matches(heat_simple,old_values=haz_rename$old,new_values = haz_rename$new)]
          X[,dry:=replace_exact_matches(dry_simple,old_values=haz_rename$old,new_values = haz_rename$new)]
          X[,wet:=replace_exact_matches(wet_simple,old_values=haz_rename$old,new_values = haz_rename$new)]
          X[,severity_class:=severity_focus]
          X[,crop:=crop_focus]
          X
        }))
      })))
      combinations_c<-combinations_c[!is.na(heat) & !is.na(dry) & !is.na(wet)]
      
      # Interactions - Animals
      livestock_choices<-crop_choices[grepl("-tropical|-highland",crop_choices)]
      
      # Create a unique list of all the 3-way combinations required for the crops and severity classes selected
      combinations_a<-unique(rbindlist(lapply(1:length(livestock_choices),FUN=function(i){
        crop_focus<-livestock_choices[i]
        result<-rbindlist(lapply(1:length(severity_classes$class),FUN=function(j){
          severity_focus<-severity_classes$class[j]
          X<-copy(animal_interactions)
          haz_rename<-haz_class[crop==crop_focus & description==severity_focus,
                                list(old=index_name2,new=gsub("_","-",gsub(".tif","",filename)))]
          
          X<-X[,heat:=replace_exact_matches(heat_simple,old_values=haz_rename$old,new_values = haz_rename$new)]
          X[,dry:=replace_exact_matches(dry_simple,old_values=haz_rename$old,new_values = haz_rename$new)]
          X[,wet:=replace_exact_matches(wet_simple,old_values=haz_rename$old,new_values = haz_rename$new)]
          X[,severity_class:=severity_focus]
          X[,crop:=crop_focus]
          return(X)
        }))
        return(result)
      })))
      combinations_a<-combinations_a[!is.na(heat) & !is.na(dry) & !is.na(wet)]
      
      # Join livestock and crop combinations
      combinations<-unique(rbind(combinations_c,combinations_a)[,crop:=NULL])
      combinations_ss<-unique(combinations_c[crop %in% short_crops][,crop:=NULL])
      
      # Add code to combinations and order
      combinations[,code:=paste(c(dry,heat,wet),collapse="+"),by=list(dry,heat,wet)]
      combinations<-unique(combinations[order(code)])
      
      combinations_ss[,code:=paste(c(dry,heat,wet),collapse="+"),by=list(dry,heat,wet)]
      combinations_ss<-unique(combinations_ss[order(code)])
      
      
      #### 0.2.4.3) Merge crop and animal combinations ####
      
      combinations_ca<-rbind(combinations_c,combinations_a)[,combo_name:=paste0(c(dry,heat,wet),collapse="+"),by=list(dry,heat,wet,crop,severity_class)
      ][,severity_class:=tolower(severity_class)]
      
      combinations_ca[,heat1:=stringi::stri_replace_all_regex(heat,pattern=haz_meta[,gsub("_","-",code)],replacement=haz_meta[,paste0(code,"-")],vectorise_all = F)][,heat1:=unlist(tstrsplit(heat1,"-",keep=1))]
      combinations_ca[,dry1:=stringi::stri_replace_all_regex(dry,pattern=haz_meta[,gsub("_","-",code)],replacement=haz_meta[,paste0(code,"-")],vectorise_all = F)][,dry1:=unlist(tstrsplit(dry1,"-",keep=1))]
      combinations_ca[,wet1:=stringi::stri_replace_all_regex(wet,pattern=haz_meta[,gsub("_","-",code)],replacement=haz_meta[,paste0(code,"-")],vectorise_all = F)][,wet1:=unlist(tstrsplit(wet1,"-",keep=1))]
      
      combinations_ca[,combo_name1:=paste0(c(dry1[1],heat1[1],wet1[1]),collapse="+"),by=list(dry1,heat1,wet1)]
      combinations_ca[,combo_name_simple2:=paste0(c(gsub("_","-",dry_simple[1]),
                                                    gsub("_","-",heat_simple[1]),
                                                    gsub("_","-",wet_simple[1])),collapse="+"),by=list(dry_simple,heat_simple,wet_simple)]
      
      combinations_ca[,combo_name_simple1:=paste0(c(dry_simple[1],heat_simple[1],wet_simple[1]),collapse="+"),by=list(dry_simple,heat_simple,wet_simple)]
      
      combinations_crops<-combinations_ca[,unique(crop)]
      
      combinations_ca<-data.frame(combinations_ca)
      
      # For short season crops only
      combinations_ca_ss<-combinations_c[crop %in% short_crops][,combo_name:=paste0(c(dry,heat,wet),collapse="+"),by=list(dry,heat,wet,crop,severity_class)
      ][,severity_class:=tolower(severity_class)]
      
      combinations_ca_ss[,heat1:=stringi::stri_replace_all_regex(heat,pattern=haz_meta[,gsub("_","-",code)],replacement=haz_meta[,paste0(code,"-")],vectorise_all = F)][,heat1:=unlist(tstrsplit(heat1,"-",keep=1))]
      combinations_ca_ss[,dry1:=stringi::stri_replace_all_regex(dry,pattern=haz_meta[,gsub("_","-",code)],replacement=haz_meta[,paste0(code,"-")],vectorise_all = F)][,dry1:=unlist(tstrsplit(dry1,"-",keep=1))]
      combinations_ca_ss[,wet1:=stringi::stri_replace_all_regex(wet,pattern=haz_meta[,gsub("_","-",code)],replacement=haz_meta[,paste0(code,"-")],vectorise_all = F)][,wet1:=unlist(tstrsplit(wet1,"-",keep=1))]
      
      combinations_ca_ss[,combo_name1:=paste0(c(dry1[1],heat1[1],wet1[1]),collapse="+"),by=list(dry1,heat1,wet1)]
      combinations_ca_ss[,combo_name_simple2:=paste0(c(gsub("_","-",dry_simple[1]),
                                                       gsub("_","-",heat_simple[1]),
                                                       gsub("_","-",wet_simple[1])),collapse="+"),by=list(dry_simple,heat_simple,wet_simple)]
      
      combinations_ca_ss[,combo_name_simple1:=paste0(c(dry_simple[1],heat_simple[1],wet_simple[1]),collapse="+"),by=list(dry_simple,heat_simple,wet_simple)]
      
      combinations_crops_ss<-combinations_ca_ss[,unique(crop)]
      
      combinations_ca_ss<-data.frame(combinations_ca_ss)
      
    ### 0.2.5) Create a table of unique hazard thresholds ####
    Thresholds_U<-unique(haz_class[description!="No significant stress",list(index_name,code2,direction,threshold)
    ][,index_name:=gsub("NTxM|NTxS|NTxE","NTx",index_name)])
    
    Thresholds_U[,direction2:=direction
    ][,direction2:=gsub("<","L",direction2)
    ][,direction2:=gsub(">","G",direction2)
    ][,code:=paste0(direction2,threshold),by=.I
    ][,index_name2:=index_name][index_name2 %in% c("PTOT","TAVG"),index_name2:=paste0(index_name2,"_",direction2)]
    
    # Subset to interaction hazards
    Thresholds_U<-Thresholds_U[grepl(paste(if(any(grepl("NTx",interaction_haz))){c("NTx",interaction_haz)}else{interaction_haz},collapse = "|"),index_name2)]
    
    # Do the same for annual crops only
    Thresholds_U_ss<-unique(haz_class[crop %in% short_crops & description!="No significant stress",list(index_name,code2,direction,threshold)
    ][,index_name:=gsub("NTxM|NTxS|NTxE","NTx",index_name)])
    
    Thresholds_U_ss[,direction2:=direction
    ][,direction2:=gsub("<","L",direction2)
    ][,direction2:=gsub(">","G",direction2)
    ][,code:=paste0(direction2,threshold),by=.I
    ][,index_name2:=index_name][index_name2 %in% c("PTOT","TAVG"),index_name2:=paste0(index_name2,"_",direction2)]
    
    # Subset to interaction hazards
    Thresholds_U_ss<-Thresholds_U_ss[grepl(paste(if(any(grepl("NTx",interaction_haz))){c("NTx",interaction_haz)}else{interaction_haz},collapse = "|"),index_name2)]
    
  ## 0.3) Set flow controls and overwrite parameters ####
    ### 0.3.1) Classify hazards ####
    run1<-F
    overwrite1<-F
    worker_n1<-20
    multisession1<-T
    annual_season_subset<-T # When seasonal data is being analysed (other than GCCMI crop calendar) should only annual crops be run?
    
    ### 0.3.2) Calculate hazard risk freq ####
    run2<-T
    run2_main<-F # Set to F if you only want to run the ensemble step only (setting do_ensemble2 to T)
    check2<-T
    round2<-NULL # set to integer if you wish to round results
    worker_n2<-20
    overwrite2<-T
    multisession2<-T
    do_ensemble2<-T
    
    ### 0.3.3) Make crop stacks for risk freq ####
    run3<-F
    check3<-T
    overwrite3<-F
    worker_n3<-20
    multisession3<-T
    
    ### 0.3.4) Calculate hazard time series mean and sd ####
    run4<-F
    check4<-F
    run4.1<-F # Difference (currently not updated, keep as F)
    round4<-3 # set to integer if you wish to round results
    overwrite4<-F
    worker_n4<-15
    do_ensemble4<-T
    multisession4<-T
    
    ### 0.3.5) Calculate interactions ####
    
    # Interaction Tifs
    run5.2<-F
    do5.2_main<-F # Set to F if you only want to run the ensembling step
    check5.2<-T
    round5.2<-3
    overwrite5.2<-F
    do_ensemble5.2<-T
    worker_n5.2<-20
    multisession5.2<-T
    
    # Interaction crop stacks
    run5.3<-F
    worker_n5.3<-15
    overwrite5.3<-F
    upload5<-F
    multisession5.3<-T
    check5.3<-T
    round5.3<-NULL
    
    ### 0.3.6) Set workers & permission for uploads ####
    worker_n_upload<-20
    permission<-"public-read"
    
    ### 0.3.7) Choose timeframes to loop through ####
    if(exists("indices_dir2")){
      timeframes<-basename(list.dirs(indices_dir2,recursive=F))
      timeframes<-timeframes[timeframes %in% timeframe_choices]
    }else{
      timeframes<-basename(list.dirs(path=atlas_dirs$data_dir$hazard_timeseries_class,recursive=F))
    }
    
    cat("Control and overwrite settings:\n")
    cat("timeframes = ", timeframes,
        "\n\nrun1 =",run1,"overwrite1 =",overwrite1,"workers1 =",worker_n1,"multisession1 =",multisession1,
        "\n\nrun2 =",run2,"round2 =",round2,"check2 =",check2,"overwrite =",overwrite2,"workers2 =",worker_n2,"multisession2= ",multisession2,"do_ensemble2=",do_ensemble2,
        "\n\nrun4 =",run4,"round4 =",round4,"check 4 =",check4,"overwrite4 =",overwrite4,"workers4 =",worker_n4,"multisession4 =",multisession4,"do_ensemble4 =",do_ensemble4,
        "\n\nrun5.2 =",run5.2,"check5.2 =",check5.2,"round5.2 =",round5.2,"overwrite5.2 =",overwrite5.2,"workers5.2 =",worker_n5.2,"multisession5.2 =",multisession5.2,"do_ensemble5.2 =",do_ensemble5.2,
        "\n\nrun5.3 =",run5.3,"check5.3 =",check5.3,"round5.3 =",round5.3,"overwrite5.3 =",overwrite5.3,"workers5.3 =",worker_n5.3,"multisession5.3 =",multisession5.3,
        "\n\nupload workers n =",worker_n_upload," upload permission =",permission,"\n\n")
    
# ***Start timeframe loop*** ####
for(tx in 1:length(timeframes)){
  timeframe<-timeframes[tx]
  
  start_time<-Sys.time()
  cat("Processing", timeframe, tx, "/", length(timeframes),
      ". Started at time:",format(start_time, "%Y-%m-%d %H:%M:%S"), "\n")
  
  # Create output folders
  haz_time_class_dir <- file.path(atlas_dirs$data_dir$hazard_timeseries_class, timeframe)
  haz_time_risk_dir <- file.path(atlas_dirs$data_dir$hazard_timeseries_risk, timeframe)
  haz_risk_dir <- file.path(atlas_dirs$data_dir$hazard_risk, timeframe)
  haz_mean_dir <- file.path(atlas_dirs$data_dir$hazard_timeseries_mean, timeframe)
  haz_time_int_dir <- file.path(atlas_dirs$data_dir$hazard_timeseries_int, timeframe)
  
  # 1) Classify time series climate variables based on hazard thresholds ####
  if(run1){
    cat(timeframe,"1) Classify time series climate variables based on hazard thresholds\n")
    
    haz_timeseries_dir<-file.path(indices_dir2,timeframe)
    cat("haz_timeseries_dir =",haz_timeseries_dir,"\n")
    
    if (!dir.exists(haz_time_class_dir)){dir.create(haz_time_class_dir, recursive = TRUE)}
    
    files<-list.files(haz_timeseries_dir,".tif",full.names = T)
    
    if(annual_season_subset==T & grepl("sos",timeframe)){
      thresholds<-copy(Thresholds_U)
    }else{
      thresholds<-copy(Thresholds_U_ss)
    }
    
    set_parallel_plan(n_cores=worker_n1,use_multisession=multisession1)
    
    # Enable progressr
    progressr::handlers(global = TRUE)
    progressr::handlers("progress")
    
    # Wrap the parallel processing in a with_progress call
    p<-with_progress({
      # Define the progress bar
      prog <- progressr::progressor(along = 1:nrow(thresholds))
      
      invisible(future.apply::future_lapply(1:nrow(thresholds),FUN=function(i){
        
        #invisible(lapply(1:nrow(thresholds),FUN=function(i){
        index_name<-thresholds[[i,"code2"]]
        files_ss<-grep(index_name,files,value=T)
        files_ss<-files_ss[!grepl("ENSEMBLE",files_ss)]
        prog(sprintf("Threshold %d/%d", i, nrow(thresholds)))
        
        for(j in 1:length(files_ss)){
          #cat(i,"-",j,"\n")
          
          file_name<-gsub(".tif",paste0("-",thresholds[[i,"code"]],".tif"),file.path(haz_time_class_dir,basename(files_ss[j])),fixed = T)
          
          # Fix issues with file naming formulation
          file_name<-gsub("_max","-max",file_name)
          file_name<-gsub("_mean","-mean",file_name)
          file_name<-gsub("_sum","-sum",file_name)
          
          file_name<-gsub("historical_","historic_historic_historic_",file_name)
          file_name<-gsub("2021_2040_","2021-2040_",file_name)
          file_name<-gsub("2041_2060_","2041-2060_",file_name)
          file_name<-gsub("2061_2080_","2061-2080_",file_name)
          file_name<-gsub("2081_2100_","2081-2100_",file_name)
          file_name<-gsub("ENSEMBLE_mean_","ENSEMBLEmean-",file_name)
          
          if((!file.exists(file_name))|overwrite1){
            data<-terra::rast(files_ss[j])
            data_class<-rast_class(data=data,
                                   direction = thresholds[[i,"direction"]],
                                   threshold = thresholds[[i,"threshold"]],
                                   minval=-999999,
                                   maxval=999999)
            terra::writeRaster(data_class,filename = file_name,overwrite=T, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
            rm(data,data_class)
            gc()
          }
        }
      }))
    })
    
    plan(sequential)
    
    cat(timeframe,"1) Classify time series climate variables based on hazard thresholds - Completed\n")
    
    # 1.1) Check results ######
    files<-list.files(haz_time_class_dir,"tif$",full.names = T,recursive=T)
    (bad_files<-check_and_delete_bad_files(files,delete_bad=T,worker_n=worker_n1))
    # If you finding files will not open delete them then run the download process again
    if(length(bad_files)>0){
      stop("Bad files were present, run through this section again")
    }
    
    
  }
  
  # 2) Calculate hazard frequency across classified time series ####
  
  if(run2){
    cat(timeframe,"2) Calculate hazard frequency (0-1) across classified time series","\n")
    
    if (!dir.exists(haz_time_risk_dir)) dir.create(haz_time_risk_dir, recursive = TRUE)
    
    files<-list.files(haz_time_class_dir,full.names = T)
    file<-files[!grepl("ENSEMBLE",files)]
    
    if(run2_main){
      set_parallel_plan(n_cores=worker_n2,use_multisession=multisession2)
      
      # Enable progressr
      progressr::handlers(global = TRUE)
      progressr::handlers("progress")
      
      # Wrap the parallel processing in a with_progress call
      p<-with_progress({
        # Define the progress bar
        progress <- progressr::progressor(along = 1:length(files))
        
        invisible(future.apply::future_lapply(1:length(files),FUN=function(i) {
          #for(i in 1:length(files)){
          # Display progress
          progress(sprintf("File %d/%d", i, length(files)))
          
          file<-file.path(haz_time_risk_dir,basename(files[i]))
          
          if((!file.exists(file))|overwrite2){
            data<-terra::rast(files[i])
            data <- terra::mean(data, na.rm = TRUE)    
            if(!is.null(round2)){
              data<-round(data,round2)
            }
            terra::writeRaster(data,filename = file,overwrite=T, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
            rm(data)
            gc()
          }
        }))
        
      })
      plan(sequential)
      cat(timeframe,"2) Calculate hazard frequency across classified time series - Complete\n")
    }
    
    # 2.1) Ensemble models ####
    if(do_ensemble2){
      cat(timeframe,"2.1) Ensembling hazard frequency\n")
      
      # Create stacks of hazard x crop/animal x scenario x timeframe
      haz_freq_files<-list.files(haz_time_risk_dir,".tif$",full.names = T)
      haz_freq_files<-haz_freq_files[!grepl("ENSEMBLE",haz_freq_files)]
      
      # Split the file elements
      split_elements <- strsplit(basename(haz_freq_files), "_")
      haz_freq_file_tab <- rbindlist(lapply(split_elements, as.list))
      colnames(haz_freq_file_tab)<-c("scenario","model","timeframe","hazard")
      haz_freq_file_tab[,hazard:=gsub("[.]tif","",hazard)]
      haz_freq_file_tab[,file:=haz_freq_files]
      haz_freq_file_tab[,hazard2:=unlist(tstrsplit(hazard,"-",keep=1))]
      haz_freq_file_tab[str_count(hazard,"-")>2,stat:=unlist(tstrsplit(hazard,"-",keep=2))]
      haz_freq_file_tab[!is.na(stat),hazard2:=paste0(hazard2,"-",stat)]
      haz_freq_file_tab[,hazard2:=paste0(hazard2[1],"-",tail(unlist(strsplit(hazard[1],"-")),1)),by=.(hazard,hazard2)]
      haz_freq_file_tab[,layer_name:=paste(c(scenario[1],timeframe[1],hazard2[1]),collapse="_"),by=.(scenario,timeframe,hazard2)]
      
      haz_freq_file_tab<-haz_freq_file_tab[model!="historic"]
      
      model_options<-haz_freq_file_tab[,unique(model)]
      
      haz_scen_mod<-data.frame(unique(haz_freq_file_tab[,.(hazard,scenario,timeframe)]))
      haz_freq_file_tab<-data.frame(haz_freq_file_tab)
      
      set_parallel_plan(n_cores=worker_n2,use_multisession=multisession2)
      
      # Enable progressr
      progressr::handlers(global = TRUE)
      progressr::handlers("progress")
      
      # Wrap the parallel processing in a with_progress call
      p<-with_progress({
        # Define the progress bar
        progress <- progressr::progressor(along = 1:nrow(haz_scen_mod))
        
        invisible(future.apply::future_lapply(1:nrow(haz_scen_mod),FUN=function(i){
          
          progress(sprintf("Haz x Scenario x Model %d/%d", i, nrow(haz_scen_mod)))
          
          haz_choice<-haz_scen_mod$hazard[i]
          scenario_choice<-haz_scen_mod$scenario[i]
          time_choice<-haz_scen_mod$timeframe[i]
          
          ensemble_files<-haz_freq_file_tab[haz_freq_file_tab$hazard==haz_choice & 
                                              haz_freq_file_tab$scenario==scenario_choice & 
                                              haz_freq_file_tab$timeframe==time_choice,"file"]
          
          save_file_mean<-file.path(haz_time_risk_dir,paste0(scenario_choice,"_ENSEMBLEmean_",time_choice,"_",haz_choice,".tif"))
          save_file_sd<-file.path(haz_time_risk_dir,paste0(scenario_choice,"_ENSEMBLEsd_",time_choice,"_",haz_choice,".tif"))
          
          if(!file.exists(save_file_mean)|overwrite2){
            ensemble_stack<-terra::rast(ensemble_files)
            ensemble_mean<-mean(ensemble_stack)
            ensemble_sd<-app(ensemble_stack,fun=sd)
            
            if(!is.null(round2)){
              ensemble_mean<-round(ensemble_mean,round2)
              ensemble_sd<-round(ensemble_sd,round2)
            }
            
            ensemble_names<-gsub(".tif","",basename(ensemble_files[1]))
            ensemble_names_mean<-gsub(paste0(model_options,collapse="|"),"ENSEMBLEmean",ensemble_names)
            ensemble_names_sd<-gsub(paste0(model_options,collapse="|"),"ENSEMBLEsd",ensemble_names)
            
            names(ensemble_mean)<-ensemble_names_mean
            names(ensemble_sd)<-ensemble_names_sd
            
            terra::writeRaster(ensemble_mean,filename =  save_file_mean,overwrite=T, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
            terra::writeRaster(ensemble_sd,filename =  save_file_sd,overwrite=T, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
            
            rm(ensemble_stack,ensemble_mean,ensemble_sd)
            gc()
          }
        }))
      })
      plan(sequential)
      
      cat(timeframe,"2.1) Ensembling hazard frequency - Complete\n")
    }
    
    ## 2.2) Check file integrity #### 
    if(check2){
      cat(timeframe,"2) Checking file integrity\n")
      
      result<-check_tif_integrity (dir_path = haz_time_risk_dir,
                                   recursive = FALSE,
                                   pattern = "*.tif", # uses glob so make sure the * is present
                                   n_workers_files = worker_n2,
                                   n_workers_folders = 1,
                                   use_multisession = multisession2,
                                   delete_corrupt  = FALSE)
      result<-result[success==F]
      
      if(nrow(result)>0){
        cat("Error: some files could not be read:\n")
        print(result)
        
        error_dir<-file.path(atlas_dirs$data_dir$hazard_timeseries_risk,"errors")
        if(!dir.exists(error_dir)){
          dir.create(error_dir)
        }
        error_file<-file.path(error_dir,paste0(timeframe,"_read-errors.csv"))
        fwrite(result,error_file)
      }
      
      cat(timeframe,"2)  File integrity check complete\n")
    }
  }
  
  # 3) Create crop hazard frequency stacks####
  
  if(run3){
    
    cat(timeframe,"3) Create crop risk stacks\n")
    
    if (!dir.exists(haz_risk_dir)){dir.create(haz_risk_dir, recursive = TRUE)}
    
    # Create stacks of hazard x crop/animal x scenario x timeframe
    haz_freq_files<-list.files(haz_time_risk_dir,".tif$",full.names = T)
    #haz_freq_files<-haz_freq_files[!grepl("ENSEMBLE",haz_freq_files)]
    
    # Split the file elements
    split_elements <- strsplit(basename(haz_freq_files), "_")
    haz_freq_file_tab <- rbindlist(lapply(split_elements, as.list))
    colnames(haz_freq_file_tab)<-c("scenario","model","timeframe","hazard")
    haz_freq_file_tab[,hazard:=gsub("[.]tif","",hazard)]
    haz_freq_file_tab[,file:=haz_freq_files]
    haz_freq_file_tab[,hazard2:=unlist(tstrsplit(hazard,"-",keep=1))]
    haz_freq_file_tab[str_count(hazard,"-")>2,stat:=unlist(tstrsplit(hazard,"-",keep=2))]
    haz_freq_file_tab[!is.na(stat),hazard2:=paste0(hazard2,"-",stat)]
    haz_freq_file_tab[,hazard2:=paste0(hazard2[1],"-",tail(unlist(strsplit(hazard[1],"-")),1)),by=.(hazard2,hazard)]
    haz_freq_file_tab[,layer_name:=paste(c(scenario[1],timeframe[1],hazard2[1]),collapse="_"),by=.(scenario,timeframe,hazard2)]
    
    models<-haz_freq_file_tab[,unique(model)]
    
    # List crops (inc. livestock)
    crops<-haz_class[,unique(crop)]
    
    if(annual_season_subset==T & grepl("sos",timeframe)){
      crops<-crops[crops %in% short_crops]
    }
    
    # combinations_ca
    dat<-combinations_ca[,c("heat_simple","dry_simple","wet_simple","crop","severity_class")]
    
    haz_class_df<-data.frame(haz_class)
    
    haz_freq_file_tab<-data.frame(haz_freq_file_tab)
    
    set_parallel_plan(n_cores=worker_n3,use_multisession=multisession3)
    
    # Enable progressr
    progressr::handlers(global = TRUE)
    progressr::handlers("progress")
    
    # Wrap the parallel processing in a with_progress call
    p<-with_progress({
      # Define the progress bar
      progress <- progressr::progressor(along = 1:length(crops))
      
      invisible(future.apply::future_lapply(1:length(crops),FUN=function(i){
        # Display progress
        progress(sprintf("Crop %d/%d", i, length(crops)))
        
        # for(i in 1:length(crops)){
        crop_focus<-crops[i]
        
        for(j in 1:nrow(severity_classes)){
          sev_choice<-severity_classes[[j,"class"]]
          
          for(k in 1:length(models)){
            model_k<-models[k]
            
            save_name<-file.path(haz_risk_dir,paste0(gsub("_| ","-",crop_focus),"_",model_k,"_",tolower(sev_choice),".tif"))
            
            # Display progress
            cat("Crop:",i,"/",length(crops),crop_focus,"| severity:",j,"/",length(severity_classes$class),sev_choice,"| model:",k,"/",length(models),model_k,"               \r")
            
            if(!file.exists(save_name)|overwrite3==T){
              
              haz_class_df_subset<-haz_class_df[haz_class_df$crop==crop_focus & 
                                                  haz_class_df$description == sev_choice & 
                                                  haz_class_df$index_name2 %in% if(crop_focus!="generic-crop"){interaction_haz_fixed}else{interaction_haz_free},c("type","filename","index_name2")]
              haz_crop_classes<-unique(gsub("_","-",gsub("[.]tif","",haz_class_df_subset$filename)))
              
              files<-haz_freq_file_tab[haz_freq_file_tab$model==model_k & 
                                         haz_freq_file_tab$hazard2 %in% haz_crop_classes,]
              
              if(length(files$file)<length(haz_crop_classes)){
                stop("Classified hazard files are missing in step 3.\n","Crop: ",i,"/",length(crops)," ",crop_focus," | severity: ",j,"/",length(severity_classes$class)," ",sev_choice,
                     " | model: ",k,"/",length(models)," ",model_k)
              }
              
              data<-terra::rast(files$file)
              
              haz_class_df_subset$hazard2<-gsub("_","-",gsub(".tif","",haz_class_df_subset$filename))
              haz_class_df_subset$filename<-NULL
              haz_class_df_subset$index_name2<-gsub("_","-",haz_class_df_subset$index_name2)
              
              files<-data.table(files)
              files<-merge(files,haz_class_df_subset,by="hazard2",all.x=T,sort=F)
              
              files[, layer_name :=
                      paste(scenario, model, timeframe, type,
                            index_name2, crop_focus, sev_choice,
                            sep = "_")]
              
              if(any(table(files$layer_name))>1){
                stop("Non-unique layer names are present!")
              }
              
              names(data)<-files$layer_name
              
              terra::writeRaster(data,
                                 file=save_name,
                                 overwrite=T, 
                                 filetype = "COG", 
                                 gdal = c("OVERVIEWS"="NONE",COMPRESS= "ZSTD"))
              rm(data)
              gc()
            }
          }
        }
        
      }))
      
      if(check3){
        cat(timeframe,"3) Checking file integrity\n")
        
        result<-check_tif_integrity (dir_path = haz_risk_dir,
                                     recursive = FALSE,
                                     pattern = "*.tif", # uses glob so make sure the * is present
                                     n_workers_files = worker_n3,
                                     n_workers_folders = 1,
                                     use_multisession = multisession3,
                                     delete_corrupt  = FALSE)
        result<-result[success==F]
        
        if(nrow(result)>0){
          cat("3) Error: some files could not be read:\n")
          print(result)
          
          error_dir<-file.path(atlas_dirs$data_dir$haz_risk_dir,"errors")
          if(!dir.exists(error_dir)){
            dir.create(error_dir)
          }
          error_file<-file.path(error_dir,paste0(timeframe,"_read-errors.csv"))
          fwrite(result,error_file)
        }
      }
      cat(timeframe,"3) File integrity check complete\n")
    })
    
    plan(sequential)
    
    cat(timeframe,"3) Create crop risk stacks - Complete\n")
  }
  
  # 4) Calculate mean and sd across time series ####
  
  if(run4){
    # Create output folder
    if (!dir.exists(haz_mean_dir)) dir.create(haz_mean_dir, recursive = TRUE)
    
    # List timeseries hazard files
    files<-list.files(haz_timeseries_dir,".tif",full.names = T)
    
    # Remove ensemble 
    files<-files[!grepl("ENSEMBLE",files)]
    
    # Rename files from first part of hazard pipeline to use consistent delimiters
    files_new<-gsub("1_2","1-2",basename(files))
    files_new<-gsub("_mean","-mean",files_new)
    files_new<-gsub("_max","-max",files_new)
    files_new<-gsub("_min","-min",files_new)
    files_new<-gsub("_sum","-sum",files_new)
    files_new<-gsub("historical_","historic_historic_historic_",files_new)
    
    cat(timeframe,"4) Calculate mean and sd across time series.\nProcessing n =",length(files),"files\n")
    
    set_parallel_plan(n_cores=worker_n4,use_multisession=F)
    
    # Enable progressr
    progressr::handlers(global = TRUE)
    progressr::handlers("progress")
    
    # Wrap the parallel processing in a with_progress call
    p<-with_progress({
      # Define the progress bar
      progress <- progressr::progressor(along = 1:length(files))
      
      invisible(future.apply::future_lapply(1:length(files),FUN=function(i){
        
        # Display progress
        progress(sprintf("File %d/%d", i, length(files)))
        
        file_mean<-paste0(haz_mean_dir,"/",gsub("[.]tif","_mean.tif",files_new[i]))
        file_sd<-paste0(haz_mean_dir,"/",gsub("[.]tif","_sd.tif",files_new[i]))
        
        if((!file.exists(file_mean))|overwrite4){
          data<-terra::rast(files[i])
          data_mean<-terra::mean(data,na.rm=T)
          data_sd<-terra::app(data,fun=sd,na.rm=T)
          
          names(data_mean)<-gsub(".tif","",basename(file_mean))
          names(data_sd)<-gsub(".tif","",basename(file_sd))
          
          if(!is.null(round4)){
            data_mean<-round(data_mean,round4)
            data_sd<-round(data_sd,round4)
          }
          
          terra::writeRaster(data_mean,filename = file_mean,overwrite=T,filetype = 'COG',gdal=c("COMPRESS=LZW",of="COG", "OVERVIEWS"="NONE"))
          terra::writeRaster(data_sd,filename = file_sd,overwrite=T,filetype = 'COG',gdal=c("COMPRESS=LZW",of="COG", "OVERVIEWS"="NONE"))
          
          rm(data_mean,data_sd,data)
          gc()
        }
        
      }))
      
    })
    
    plan(sequential)
    cat(timeframe,"4) Calculate mean and sd across time series - Complete\n")
    
    ## 4.1) Ensemble models ####
    if(do_ensemble4){
      cat(timeframe,"4.1) Ensembling timeseries mean\n")
      
      # Create stacks of hazard x crop/animal x scenario x timeframe
      files<-list.files(haz_mean_dir,".tif$",full.names = T)
      files<-files[!grepl("historic|ENSEMBLE|_sd.tif",files)]
      
      # Split the file elements
      split_elements <- strsplit(basename(files), "_")
      file_tab <- rbindlist(lapply(split_elements, as.list))
      colnames(file_tab)<-c("scenario","model","timeframe","hazard","stat")
      file_tab[,stat:=gsub("[.]tif","",stat)
      ][,file:=files]
      
      model_options<-file_tab[,unique(model)]
      
      haz_scen_mod<-data.frame(unique(file_tab[,.(hazard,scenario,timeframe)]))
      file_tab<-data.frame(file_tab)
      
      set_parallel_plan(n_cores=worker_n4,use_multisession=multisession4)
      
      # Enable progressr
      progressr::handlers(global = TRUE)
      progressr::handlers("progress")
      
      # Wrap the parallel processing in a with_progress call
      p<-with_progress({
        # Define the progress bar
        progress <- progressr::progressor(along = 1:nrow(haz_scen_mod))
        
        invisible(future.apply::future_lapply(1:nrow(haz_scen_mod),FUN=function(i){
          
          progress(sprintf("Haz x Scenario x Model %d/%d", i, nrow(haz_scen_mod)))
          
          haz_choice<-haz_scen_mod$hazard[i]
          scenario_choice<-haz_scen_mod$scenario[i]
          time_choice<-haz_scen_mod$timeframe[i]
          
          ensemble_files<-file_tab[file_tab$hazard==haz_choice & 
                                     file_tab$scenario==scenario_choice & 
                                     file_tab$timeframe==time_choice,"file"]
          
          if(length(ensemble_files)!=length(model_options)){
            stop("4.1) Error - number of GCMS (",length(ensemble_files),") !=","number of models (",length(model_options),
                 ") ",paste(haz_scen_mod[i,],collapse=" / "))
          }
          
          save_file_mean<-file.path(haz_mean_dir,paste0(scenario_choice,"_ENSEMBLEmean_",time_choice,"_",haz_choice,"_mean.tif"))
          save_file_sd<-file.path(haz_mean_dir,paste0(scenario_choice,"_ENSEMBLEmean_",time_choice,"_",haz_choice,"_sd.tif"))
          
          if(!file.exists(save_file_mean)|overwrite2){
            
            ensemble_stack <- terra::rast(ensemble_files)
            
            ensemble_mean<-mean(ensemble_stack)
            ensemble_sd<-terra::app(ensemble_stack, fun = sd)
            
            if(!is.null(round4)){
              ensemble_mean<-round(ensemble_mean,round4)
              ensemble_sd<-round(ensemble_sd,round4)
            }
            
            names(ensemble_mean)<-gsub(".tif","",basename(save_file_mean))
            names(ensemble_sd)<-gsub(".tif","",basename(save_file_sd))
            
            terra::writeRaster(ensemble_mean,filename =  save_file_mean,overwrite=T, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
            terra::writeRaster(ensemble_sd,filename =  save_file_sd,overwrite=T, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
            
            rm(ensemble_stack,ensemble_mean,ensemble_sd)
            gc()
          }
        }))
      })
      plan(sequential)
      
      cat(timeframe,"4.1) Ensembling timeseries mean - Complete\n")
    }
    
    ## 4.2) Check integrity ####
    if(check4){
      cat(timeframe,"4.2) Checking file integrity\n")
      
      result<-check_tif_integrity (dir_path = haz_mean_dir,
                                   recursive = FALSE,
                                   pattern = "*.tif", # uses glob so make sure the * is present
                                   n_workers_files = worker_n3,
                                   n_workers_folders = 1,
                                   use_multisession = multisession3,
                                   delete_corrupt  = FALSE)
      result<-result[success==F]
      
      if(nrow(result)>0){
        cat("4) Error: some files could not be read:\n")
        print(result)
        
        error_dir<-file.path(atlas_dirs$data_dir$haz_risk_dir,"errors")
        if(!dir.exists(error_dir)){
          dir.create(error_dir)
        }
        error_file<-file.path(error_dir,paste0(timeframe,"_read-errors.csv"))
        fwrite(result,error_file)
      }
      
      cat(timeframe,"4.2) File integrity check complete\n")
    }
    
    # 4.1) (Disabled) Calculate change in mean values #####
    if(F){
      files<-list.files(haz_mean_dir,".tif",full.names = T)
      files<-files[!grepl("change",files)]
      files_hist<-grep("historic",files,value = T)
      files_fut<-files[!files %in% files_hist]
      
      change_file<-file.path(haz_mean_dir,"change.tif")
      
      if(!file.exists(change_file)|overwrite==T){
        
        change<-pblapply(1:length(files_hist),FUN=function(i){
          # Display progress
          sprintf("File %d/%d", i, length(file_hist))
          
          file_hist<-files_hist[i]
          var<-gsub("historical_","",tail(tstrsplit(file_hist,"/"),1))
          files_fut_ss<-grep(var,files_fut,value=T)
          future<-terra::rast(files_fut_ss)
          past<-terra::rast(file_hist)
          
          change<-future-past
          names(change)<-gsub(".tif","",basename(files_fut_ss))
          return(change)
        })
        
        change<-terra::rast(change)
        
        terra::writeRaster(change,filename=change_file, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
        
      }
    }
    
  }
  # 5) Interactions ####
  ## 5.1) List classfied raster stacks ####
  
  # Restructure names of classified hazard files so they can be easily searched for scenario x timeframe x hazard x threshold
  haz_class_files<-list.files(haz_time_class_dir,full.names = T)
  haz_class_files<-haz_class_files[!grepl("ENSEMBLE",haz_class_files)]
  haz_class_files2<-basename(haz_class_files)
  
  # Split the file elements
  split_elements <- strsplit(basename(haz_class_files), "_")
  haz_class_file_tab <- rbindlist(lapply(split_elements, as.list))
  colnames(haz_class_file_tab)<-c("scenario","model","timeframe","hazard")
  haz_class_file_tab[,hazard:=gsub("[.]tif","",hazard)
  ][,file:=haz_class_files
  ][,hazard2:=unlist(tstrsplit(hazard,"-",keep=1))
  ][str_count(hazard,"-")>2,stat:=unlist(tstrsplit(hazard,"-",keep=2))
  ][!is.na(stat),hazard2:=paste0(hazard2,"-",stat)
  ][,hazard2:=paste0(hazard2,"-",tail(unlist(strsplit(hazard,"-")),1)),by=.I
  ][,layer_name:=paste(c(scenario,timeframe,hazard2),collapse="_"),by=.I]
  
  scenarios_x_models<-unique(haz_class_file_tab[,.(scenario,timeframe,model)
  ][,scen_x_time:=paste0(scenario,"_",timeframe)
  ][,scen_mod_time:=paste0(scenario,"_",model,"_",timeframe)])
  
  model_options<-scenarios_x_models[,unique(model)]
  scenarios_x_models<-data.frame(scenarios_x_models)
  
  
  #cat("5.2) scenarios_x_models available:\n")
  #print(scenarios_x_models)
  
  haz_class_file_tab<-data.frame(haz_class_file_tab)
  ## 5.2) Calculate interactions ####
  
  # Dev Note: At the moment this section handles moderate, severe and extreme hazards separately,
  #           Can we upgrade this? so instead of 1/0 classification we can merge the hazard severity class to have 3/2/1/0
  
  # To Do:
  # 1) Stack hazards instead of making folders
  # 2) Incorporate model into stack name "model_haz1+haz2+haz3.tif"
  
  
  # Create output folder
  if (!dir.exists(haz_time_int_dir)) dir.create(haz_time_int_dir, recursive = TRUE)
  
  if(run5.2){
    cat("5.2) Calculate interactions\n")
    
    if(annual_season_subset==T & grepl("sos",timeframe)){
      combinations_choice<-combinations_ss
    }else{
      combinations_choice<-combinations
    }
    
    # Estimate the RAM available therefore the number of workers
    set_parallel_plan(n_cores=floor(worker_n5.2),use_multisession=multisession5.2)
    
    # Enable progressr
    progressr::handlers(global = TRUE)
    progressr::handlers("progress")
    
    # Wrap the parallel processing in a with_progress call
    p<-with_progress({
      # Define the progress bar
      progress <- progressr::progressor(along = 1:nrow(combinations_choice))
      
      invisible(future.apply::future_lapply(1:nrow(combinations_choice), FUN=function(i){
        
        # Display progress
        progress(sprintf("Combination %d/%d", i, nrow(combinations_choice)))
        
        combos<-gsub("_","-",unlist(combinations_choice[i,list(dry,heat,wet)]))
        grep_vals<-paste0(paste0(combos,".tif"),collapse = "|")
        
        combo_names<-c(names(combos),
                       apply(combn(names(combos),2),2,paste,collapse="+"),
                       paste(names(combos),collapse="+"))
        
        combo_binary<-data.table(combo_name=combo_names,value=0)[grep(names(combos)[1],combo_name),value:=1
        ][grep(names(combos)[2],combo_name),value:=value+10
        ][grep(names(combos)[3],combo_name),value:=value+100]
        
        if(do5.2_main){
          for(l in 1:nrow(scenarios_x_models)){
            
            scenario_choice<-scenarios_x_models$scenario[l]
            time_choice<-scenarios_x_models$timeframe[l]
            model_choice<-scenarios_x_models$model[l]
            scen_mod_time_choice<-scenarios_x_models$scen_mod_time[l]
            
            # Display progress
            cat("Combination:",i,"/",nrow(combinations_choice),
                "| Scenario x Time x model:",l,"/",nrow(scenarios_x_models),"                   \r")
            
            combo_binary[,lyr_names:=paste0(scen_mod_time_choice,"_",combo_name)]
            save_file<-file.path(haz_time_int_dir,paste0(scen_mod_time_choice,"_",paste0(combos,collapse = "+"),".tif"))
            
            if(!file.exists(save_file)|overwrite5.2==T){
              
              files<-haz_class_file_tab[haz_class_file_tab$timeframe==time_choice &       
                                          haz_class_file_tab$scenario==scenario_choice &
                                          haz_class_file_tab$model == model_choice,]
              files<-files[match(combos,files$hazard2),"file"]
              
              if(length(files)!=3){
                stop("Issue with classified hazard files, 3 files not found.")
              }
              
              haz<-lapply(files,rast)
              names(haz)<-names(combos)
              
              # Multiply risk probability to create a binary value when summed
              haz[["heat"]]<-haz[["heat"]]*10
              haz[["wet"]]<-haz[["wet"]]*100
              
              haz_sum<-terra::rast(lapply(1:nlyr(haz[[1]]),FUN=function(m){
                sum(terra::rast(lapply(haz,"[[",m)))
              }))
              
              names(haz_sum)<-names(haz[[1]])
              
              # Any haz
              any_haz <- terra::ifel(haz_sum >= 1 & haz_sum <= 999999, 1, 0)
              any_haz_mean<-terra::mean(any_haz,na.rm=T)
              names(any_haz_mean)<-paste0(scen_mod_time_choice,"_any")
              
              # Interactions
              int<-terra::rast(lapply(1:nrow(combo_binary),FUN=function(a){
                data<-int_risk(data=haz_sum,interaction_mask_vals = combo_binary[-a,value],lyr_name = combo_binary[a,lyr_names])
              }))
              
              int_any<-c(any_haz_mean,int)
              
              if(!is.null(round5.2)){
                int_any<-round(int_any,round5.2)
              }
              
              terra::writeRaster(int_any,filename =  save_file,overwrite=T, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
              
              rm(haz,haz_sum,int,any_haz,any_haz_mean,int_any)
              gc()
            }
          }
        }
        
        scenarios<-scenarios_x_models
        scenarios$model<-NULL
        scenarios$scen_mod_time<-NULL
        scenarios<-unique(scenarios)
        
        for(l in 1:nrow(scenarios)){
          cat("Calculating Ensemble:",i,"/",nrow(combinations_choice),
              "| Scenario:",l,"/",length(unique(scenarios$scen_x_time)),"                    \r")
          
          scenario_choice<-scenarios$scenario[l]
          
          # Ensemble models
          if(scenario_choice!="historic" & do_ensemble5.2){
            
            time_choice<-scenarios$timeframe[l]
            scen_time_choice<-scenarios$scen_x_time[l]
            scen_mod_time_choice<-scenarios_x_models[scenarios_x_models$scen_x_time==scen_time_choice,"scen_mod_time"]
            
            ensemble_files<-file.path(haz_time_int_dir,paste0(scen_mod_time_choice,"_",paste0(combos,collapse = "+"),".tif"))
            save_file_mean<-file.path(haz_time_int_dir,paste0(scenario_choice,"_ENSEMBLEmean_",time_choice,"_",paste0(combos,collapse = "+"),".tif"))
            save_file_sd<-file.path(haz_time_int_dir,paste0(scenario_choice,"_ENSEMBLEsd_",time_choice,"_",paste0(combos,collapse = "+"),".tif"))
            
            if(!file.exists(save_file_mean)|overwrite5.2){
              ensemble_stack <- lapply(ensemble_files,terra::rast) 
              
              ensemble_mean<-terra::rast(lapply(1:nlyr(ensemble_stack[[1]]),FUN=function(j){
                ensemble_dat<-terra::rast(lapply(ensemble_stack,"[[",j)) 
                mean(ensemble_dat)
              }))
              
              ensemble_sd<-terra::rast(lapply(1:nlyr(ensemble_stack[[1]]),FUN=function(j){
                ensemble_dat<-terra::rast(lapply(ensemble_stack,"[[",j)) 
                terra::app(ensemble_dat, fun = sd)
              }))
              
              if(!is.null(round5.2)){
                ensemble_mean<-round(ensemble_mean,round5.2)
                ensemble_sd<-round(ensemble_sd,round5.2)
              }
              
              ensemble_names<-names(ensemble_stack[[1]])
              ensemble_names_mean<-gsub(paste0(model_options,collapse="|"),"ENSEMBLEmean",ensemble_names)
              ensemble_names_sd<-gsub(paste0(model_options,collapse="|"),"ENSEMBLEsd",ensemble_names)
              
              names(ensemble_mean)<-ensemble_names_mean
              names(ensemble_sd)<-ensemble_names_sd
              
              terra::writeRaster(ensemble_mean,filename =  save_file_mean,overwrite=T, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
              terra::writeRaster(ensemble_sd,filename =  save_file_sd,overwrite=T, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
              
              rm(ensemble_stack,ensemble_mean,ensemble_sd)
              gc()
            }
          }
        }
        
      }))
    })
    
    plan(sequential)
    
    cat("5.2) Calculate interactions - Complete\n")
    
    # 5.2.1) Check results ######
    if(check5.2){
      
      result<-check_tif_integrity (dir_path = haz_time_int_dir,
                                   recursive = FALSE,
                                   pattern = "*.tif", # uses glob so make sure the * is present
                                   n_workers_files = worker_n5.2,
                                   n_workers_folders = 1,
                                   use_multisession = multisession5.2,
                                   delete_corrupt  = FALSE)
      result<-result[success==F]
      
      if(nrow(result)>0){
        cat("Error: some files could not be read:\n")
        print(result)
        
        error_dir<-file.path(atlas_dirs$data_dir$hazard_timeseries_int,"errors")
        if(!dir.exists(error_dir)){
          dir.create(error_dir)
        }
        error_file<-file.path(error_dir,paste0(timeframe,"_read-errors.csv"))
        fwrite(result,error_file)
      }
    }
  }
  
  ## 5.3) Per crop combine hazards into a single file #####
  if(run5.3){
    cat("5.3) Per crop, combine hazards into a single file\n")
    
    # Make a table of interaction stack files
    haz_int_files<-list.files(haz_time_int_dir,full.names = T)
    
    # Split the file elements
    split_elements <- strsplit(basename(haz_int_files), "_")
    haz_int_file_tab <- rbindlist(lapply(split_elements, as.list))
    colnames(haz_int_file_tab)<-c("scenario","model","timeframe","hazard")
    haz_int_file_tab[,hazard:=gsub("[.]tif","",hazard)
    ][,file:=haz_int_files]
    #][,layer_name:=paste(c(scenario,timeframe,hazard2),collapse="_"),by=.I]
    
    model_options<-haz_int_file_tab[,unique(model)]
    timeframe_options<-haz_int_file_tab[timeframe!="historic",unique(timeframe)]
    
    haz_int_file_tab<-data.frame(haz_int_file_tab)
    
    if(annual_season_subset==T & grepl("sos",timeframe)){
      combinations_crops_choice<-combinations_crops_ss
      combinations_ca_choice<-combinations_ca_ss
    }else{
      combinations_crops_choice<-combinations_crops
      combinations_ca_choice<-combinations_ca
    }
    
    set_parallel_plan(n_cores=worker_n5.3,use_multisession=multisession5.3)
    
    # Enable progressr
    progressr::handlers(global = TRUE)
    progressr::handlers("progress")
    
    # Wrap the parallel processing in a with_progress call
    p<-with_progress({
      # Define the progress bar
      progress <- progressr::progressor(along = 1:length(combinations_crops_choice))
      
      future.apply::future_lapply(1:length(combinations_crops_choice),FUN=function(i){
        progress(sprintf("Crop %d/%d", i, length(combinations_crops_choice)))
        #lapply(1:length(combinations_crops),FUN=function(i){
        crop_choice<-combinations_crops_choice[i]
        for(j in 1:nrow(severity_classes)){
          sev_choice<-tolower(severity_classes$class[j])
          
          for(m in 1:length(model_options)){
            model_choice<-model_options[m]
            
            subset<-combinations_ca[combinations_ca_choice$crop==crop_choice & 
                                      combinations_ca_choice$severity_class==sev_choice,]
            
            invisible(lapply(1:nrow(subset),FUN=function(k){
              combo_name<-subset$combo_name[k]
              combo_simple2<-subset$combo_name_simple2[k]
              
              # Display progress
              cat("crop_choice (i):",i,"/",length(combinations_crops),
                  "| sev_choice (j):",j,"/",nrow(severity_classes),
                  "| model_choice (m):",m,"/",length(model_options),
                  "| subset (k):",k,"/",nrow(subset),"              \r")
              
              save_file<-file.path(haz_risk_dir,paste0(gsub("_","-",crop_choice),"_",model_choice,"_",sev_choice,"_",combo_simple2,"_int.tif"))
              
              if(!file.exists(save_file)|overwrite5.3==T){
                
                files<-haz_int_file_tab[haz_int_file_tab$hazard == combo_name &
                                          haz_int_file_tab$model == model_choice,"file"]
                
                if(model_choice=="historic"){
                  if(length(files)!=1){
                    stop("5.3) there should be one interaction stack for historic timeframe"
                         ," | i = ",i,"/",crop_choice," | j  = ",j,"/",sev_choice," | m = ",m,"/",model_choice,
                         ", but n = ",length(files))
                  }
                }else{
                  if(length(files)<length(timeframe_options)){
                    stop("5.3) there should be ",length(timeframe_options)," interaction stacks"
                         ," | i = ",i,"/",crop_choice," | j  = ",j,"/",sev_choice," | m = ",m,"/",model_choice,
                         ", but n = ",length(files))
                  }
                }
                
                data<-terra::rast(files)
                
                if(!is.null(round5.3)){
                  data<-round(data,round5.3)
                }
                
                # Add combination, crop and severity to layer name
                names(data)<-paste0(names(data),"_",combo_simple2,"_",crop_choice,"_",sev_choice)
                
                # Check for any duplicate layers
                x<-table(names(data))
                
                if(any(x>1)){
                  stop("5.3) Duplicate layers present in result"," | i = ",i,"/",crop_choice," | j  = ",j,"/",sev_choice," | m = ",m,"/",model_choice)
                }
                
                terra::writeRaster(data,filename = save_file,overwrite=T, filetype = "COG", gdal = c("OVERVIEWS"="NONE"))
                
              }
              
              
              
            }))
            
          }
        }
      })
    })
    
    plan(sequential)
    
    cat("5.3) Per crop combine hazards into a single file - Complete\n")
    
    # 5.3.1) Check results ######
    
    cat(timeframe,"5.3) Checking file integrity\n")
    
    result<-check_tif_integrity (dir_path = haz_risk_dir,
                                 recursive = FALSE,
                                 pattern = "*.tif", # uses glob so make sure the * is present
                                 n_workers_files = worker_n5.3,
                                 n_workers_folders = 1,
                                 use_multisession = multisession5.3,
                                 delete_corrupt  = FALSE)
    result<-result[success==F]
    
    if(nrow(result)>0){
      cat("5.3) Error: some files could not be read:\n")
      print(result)
      
      error_dir<-file.path(atlas_dirs$data_dir$haz_risk_dir,"errors")
      if(!dir.exists(error_dir)){
        dir.create(error_dir)
      }
      error_file<-file.path(error_dir,paste0(timeframe,"_read-errors.csv"))
      fwrite(result,error_file)
    }
    
    cat(timeframe,"5.3) File integrity check complete\n")
    
  }
  
  end_time<-Sys.time()
  cat("Processing of", timeframe, tx, "/", length(timeframes),
      ". Completed at time:",format(end_time, "%Y-%m-%d %H:%M:%S"), 
      ".\n Time for completion =",end_time-start_time,"seconds \n")
  
}

cat("Script 2 -  timeframe loop completed.\n")  

# Not Run (Brayden has handled this elsewhere?) ####
if(F){
  # 6) Upload outputs ####
  # 6.1) (not implemented) Classified hazards ####
  # 6.2) Hazard Freq ####
  if(upload2){
    cat("Uploading hazard frequency tifs\n")
    cat("upload_overwrite2=",upload_overwrite2,"upload_delete2=",upload_delete2,"permission=",permission,"\n")
    haz_time_risk_dir <- file.path(atlas_dirs$data_dir$hazard_timeseries_risk, timeframe)
    
    for(tx in 1:length(timeframes)){
      
      timeframe<-timeframes(tx)
      cat(timeframe,tx,"/",length(timeframes),"\n")
      
      s3_bucket<-file.path(atlas_dirs$s3_dir$haz_time_risk_dir,timeframe)
      cat("uploading to:",s3_bucket,"\n")
      
      if (upload_delete2) {
        cat("Deleting existing .tif files using AWS CLI (fast method)\n")
        
        # Construct AWS CLI command
        cmd <- sprintf("aws s3 rm %s --recursive --exclude '*' --include '*.tif'", s3_bucket)
        
        # Run it
        system(cmd, intern = TRUE)
      }
      
      # Local files
      local_files<-list.files(haz_time_risk_dir,"[.]tif$",full.names = T)
      
      cat("uploading",length(local_files),"tif files\n")
      
      upload_files_to_s3(files=local_files,
                         selected_bucket=s3_bucket,
                         max_attempts = 3,
                         workers=worker_n_upload,
                         convert2cog = F,
                         mode=permission,
                         overwrite=upload_overwrite2)
    }
  }
  
  # 6.3) Crop hazard frequency stacks ####
  if(upload3){
    cat("Uploading hazard frequency crop tifs\n")
    cat("upload_overwrite3=",upload_overwrite3,"upload_delete3=",upload_delete3,"permission=",permission,"\n")
    haz_risk_dir <- file.path(atlas_dirs$data_dir$hazard_risk, timeframe)
    
    for(tx in 1:length(timeframes)){
      
      timeframe<-timeframes(tx)
      cat(timeframe,tx,"/",length(timeframes),"\n")
      
      s3_bucket<-file.path(atlas_dirs$s3_dir$hazard_risk,timeframe)
      cat("uploading to:",s3_bucket,"\n")
      
      if (upload_delete3) {
        cat("Deleting existing .tif files using AWS CLI (fast method)\n")
        
        # Construct AWS CLI command
        cmd <- sprintf("aws s3 rm %s --recursive --exclude '*' --include '*.tif'", s3_bucket)
        
        # Run it
        system(cmd, intern = TRUE)
      }
      
      # Local files
      local_files<-list.files(haz_risk_dir,"[.]tif$",full.names = T)
      
      cat("uploading",length(local_files),"tif files\n")
      
      upload_files_to_s3(files=local_files,
                         selected_bucket=s3_bucket,
                         max_attempts = 3,
                         workers=worker_n_upload,
                         convert2cog = F,
                         mode=permission,
                         overwrite=upload_overwrite3)
    }
  }
}