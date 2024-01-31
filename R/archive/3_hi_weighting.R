source("R/haz_functions.R")
require(data.table)
require(terra)
require(tmap)
require(sfarrow)

# temporary function to harmonize names
fix_names<-function(data){
  data<-gsub("-","_",data,fixed = T)
  data<-gsub("1_2","1.2",data,fixed = T)
  data
}


# Use weighted hi or not?
weighted<-T

# Choose crop weighting method (the hazard index for a crop is multiplied by the weighting for that crop):
  # 1) sum - crop proportion of the total VoP for all crops (equivalent of the weighted mean, hazard index does not exceed it's maximum)
  # 2) max - for experimental purposes only, may be of use if we need an index that reflects the exposure of multiple important crops in an area. The crop proportion of the maximum VoP out of all of the crops (hazard index can now exceed 3, if 4 crops at 25% each and each 3 exposure then the hi would be 12)
weight_method<-"sum"

# Use VoP (vop) or Harvested Area (ha) to create weightings?
weighting_var<-"vop"

# Statistic that combines hi across crops or hazard indices (options are sum, max and mean)
  
stat<-"sum"

# Load metadata for countries to consider in the atlas - exclude islands for which we do not have climate data
countries_metadata<-fread("Data/metadata/countries.csv")[excluded_island==FALSE]

# Load and combine geoboundaries ####
Geographies<-list(
  admin2=terra::vect("Data/geoboundaries/admin2.shp"),
  admin1=terra::vect("Data/geoboundaries/admin1.shp"),
  admin0=terra::vect("Data/geoboundaries/admin0.shp")
)

# Create standard name field for each admin vector
Geographies$admin2$admin_name<-Geographies$admin2$shapeName
Geographies$admin1$admin_name<-Geographies$admin1$shapeName
Geographies$admin0$admin_name<-Geographies$admin0$shapeName

# read in mean hazard values ####
timeframe_choice<-"annual"

save_dir_means<-paste0("Data/hazard_mean/",timeframe_choice)
files<-list.files(save_dir_means,".tif",full.names = T)

haz_means<-terra::rast(files[!grepl("change",files)])
haz_means_change<-terra::rast(files[grepl("change",files)])

haz_means_adm0<-terra::vect(sfarrow::st_read_parquet(paste0(save_dir_means,"/haz_means_adm0.parquet")))
haz_means_adm1<-terra::vect(sfarrow::st_read_parquet(paste0(save_dir_means,"/haz_means_adm1.parquet")))
haz_means_adm2<-terra::vect(sfarrow::st_read_parquet(paste0(save_dir_means,"/haz_means_adm2.parquet")))

haz_means_change_adm0<-terra::vect(sfarrow::st_read_parquet(paste0(save_dir_means,"/haz_means_change_adm0.parquet")))
haz_means_change_adm1<-terra::vect(sfarrow::st_read_parquet(paste0(save_dir_means,"/haz_means_change_adm1.parquet")))
haz_means_change_adm2<-terra::vect(sfarrow::st_read_parquet(paste0(save_dir_means,"/haz_means_change_adm2.parquet")))

# read in hazard indices ####
save_dir_hi<-paste0("Data/hazard_index/",timeframe_choice)

hi_adm0<-terra::vect(sfarrow::st_read_parquet(paste0(save_dir_hi,"/hi_adm0.parquet")))
hi_adm1<-terra::vect(sfarrow::st_read_parquet(paste0(save_dir_hi,"/hi_adm1.parquet")))
hi_adm2<-terra::vect(sfarrow::st_read_parquet(paste0(save_dir_hi,"/hi_adm2.parquet")))

names(hi_adm0)<-fix_names(names(hi_adm0))
names(hi_adm1)<-fix_names(names(hi_adm1))
names(hi_adm2)<-fix_names(names(hi_adm2))

hi_adm0<-hi_adm0[,!grepl("generic",names(hi_adm0))]
hi_adm1<-hi_adm1[,!grepl("generic",names(hi_adm1))]
hi_adm2<-hi_adm2[,!grepl("generic",names(hi_adm2))]

# Weighting variable ####
# load metadata
ms_codes<-data.table::fread("./Data/metadata/SpamCodes.csv")[,Code:=toupper(Code)]
ms_codes<-ms_codes[compound=="no"]

# load chosen weighting variable 
if(weighting_var=="vop"){
  ms_tot_adm0<-terra::vect(sfarrow::st_read_parquet(paste0("Data/mapspam/spam2017V2r3_SSA_V_TA-vop_adm0.parquet")))
  ms_tot_adm1<-terra::vect(sfarrow::st_read_parquet(paste0("Data/mapspam/spam2017V2r3_SSA_V_TA-vop_adm1.parquet")))
  ms_tot_adm2<-terra::vect(sfarrow::st_read_parquet(paste0("Data/mapspam/spam2017V2r3_SSA_V_TA-vop_adm2.parquet")))
}

if(weighting_var=="ha"){
  ms_tot_adm0<-terra::vect(sfarrow::st_read_parquet(paste0("Data/mapspam/spam2017V2r3_SSA_H_TA-ha_adm0.parquet")))
  ms_tot_adm1<-terra::vect(sfarrow::st_read_parquet(paste0("Data/mapspam/spam2017V2r3_SSA_H_TA-ha_adm1.parquet")))
  ms_tot_adm2<-terra::vect(sfarrow::st_read_parquet(paste0("Data/mapspam/spam2017V2r3_SSA_H_TA-ha_adm2.parquet")))
}

# Create vop weights
if(weight_method=="sum"){
  ms_tot_adm0_weights<-cbind(ms_tot_adm0[,1:6],data.frame(ms_tot_adm0[,-(1:6)])/rowSums(data.frame(ms_tot_adm0[,-(1:6)])))
  ms_tot_adm1_weights<-cbind(ms_tot_adm1[,1:6],data.frame(ms_tot_adm1[,-(1:6)])/rowSums(data.frame(ms_tot_adm1[,-(1:6)])))
  ms_tot_adm2_weights<-cbind(ms_tot_adm2[,1:6],data.frame(ms_tot_adm2[,-(1:6)])/rowSums(data.frame(ms_tot_adm2[,-(1:6)])))
}

if(weight_method=="max"){
  ms_tot_adm0_weights<-cbind(ms_tot_adm0[,1:6],data.frame(ms_tot_adm0[,-(1:6)])/apply(data.frame(ms_tot_adm0[,-(1:6)]),1,max,na.rm=T))
  ms_tot_adm1_weights<-cbind(ms_tot_adm1[,1:6],data.frame(ms_tot_adm1[,-(1:6)])/apply(data.frame(ms_tot_adm0[,-(1:6)]),1,max,na.rm=T))
  ms_tot_adm2_weights<-cbind(ms_tot_adm2[,1:6],data.frame(ms_tot_adm2[,-(1:6)])/apply(data.frame(ms_tot_adm0[,-(1:6)]),1,max,na.rm=T))
}

vop<-list(admin0=ms_tot_adm0,
          admin1=ms_tot_adm1,
          admin2=ms_tot_adm2,
          admin0_weights=ms_tot_adm0_weights,
          admin1_weights=ms_tot_adm1_weights,
          admin2_weights=ms_tot_adm2_weights)

# Weight hi data according to vop weights ####

data<-data.frame(hi_adm0)[,-(1:6)]
data_names<-unlist(tstrsplit(names(data),"_",keep=3))
hi_adm0_weighted<-data*data.frame(ms_tot_adm0_weights[,data_names])
names(hi_adm0_weighted)<-paste0(names(hi_adm0_weighted),"_weighted")
hi_adm0_weighted<-cbind(hi_adm0[,1:6],hi_adm0_weighted)

data<-data.frame(hi_adm1)[,-(1:6)]
data_names<-unlist(tstrsplit(names(data),"_",keep=3))
hi_adm1_weighted<-data*data.frame(ms_tot_adm1_weights[,data_names])
names(hi_adm1_weighted)<-paste0(names(hi_adm1_weighted),"_weighted")
hi_adm1_weighted<-cbind(hi_adm1[,1:6],hi_adm1_weighted)

data<-data.frame(hi_adm2)[,-(1:6)]
data_names<-unlist(tstrsplit(names(data),"_",keep=3))
hi_adm2_weighted<-data*data.frame(ms_tot_adm2_weights[,data_names])
names(hi_adm2_weighted)<-paste0(names(hi_adm2_weighted),"_weighted")
hi_adm2_weighted<-cbind(hi_adm2[,1:6],hi_adm2_weighted)

hi<-list(admin0=hi_adm0,
         admin1=hi_adm1,
         admin2=hi_adm2,
         admin0_weighted=hi_adm0_weighted,
         admin1_weighted=hi_adm1_weighted,
         admin2_weighted=hi_adm2_weighted)

# metadata
ms_codes<-data.table::fread("./Data/metadata/SpamCodes.csv")[,Code:=toupper(Code)]
ms_codes<-ms_codes[compound=="no"]

# list crops
crops<-ms_codes$Fullname

# list hazards
hazards<-c("NDD","NTx40","NTx35","HSHmax","HSHmean","THImax","THImean","NDWS","TAI","NDWL0","PTOT_L","PTOT_H","TAVG_H")

# Create combinations of scenarios and times
Scenarios<-c("ssp245","ssp585")
Times<-c("2021.2040","2041.2060")
Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))
Scenarios[,combined:=paste0(Scenario,"_",Time)]

# Worked example where user chooses crops and scenario x time period, hazards are preconfigured
crop_choice<-c("maize","wheat","yams","cassava","rice")
crop_choice_codes<-ms_codes[Fullname %in% crop_choice,Code]

scenario_choice<-Scenarios$combined[2]
admin_level<-"admin0"
admin_choice<-hi$admin0$admin_name[1:10]

hazards<-c("TAVG_H","PTOT_L","NDD")

geog_sub<-Geographies[[admin_level]]
geog_sub<-geog_sub[geog_sub$admin_name %in% admin_choice,]

# Vector approach to subsetting, weighting and combining hazard index data ####

# Subset hazard index
subset_fun1<-function(data,crop_choice,scenario_choice,admin_level,admin_choice,hazards,keep_name=F,weighted=T){
  if(weighted){
    admin_level<-paste0(admin_level,"_weighted")
  }
  X<-data[[admin_level]]
  X<-X[X$admin_name %in% admin_choice]
  Y<-X[,(grepl(paste0(crop_choice,collapse = "|"),names(X))*grepl(scenario_choice,names(X))*grepl(paste0(hazards,collapse = "|"),names(X)))==1]
  if(keep_name){
    Y<-cbind(X[,1:6],Y)
  }
  return(Y)
}

# For each crop take sum,mean, or max, and weighted by vop results across select columns

combine_fun<-function(data,values,stat,name){
  data_comb<-do.call("cbind",lapply(values,FUN=function(VAL){
    hi_cols<-unlist(sapply(VAL,grep,names(data),value=T))
    vals<-data.frame(data)[,hi_cols]
    
    if(stat=="mean"){
      results<-data.table(rowMeans(vals,na.rm=T))
    }
    
    if(stat=="max"){
      results<-data.table(apply(vals,1,max,na.rm=T))
      
    }
    
    if(stat=="sum"){
      results<-data.table(rowSums(vals,na.rm=T))
    }
    
    names(results)<-VAL
    results
  }))
  
  # Sum weighted results across crops
  data_comb[,max:=apply(data_comb,1,FUN=function(X){
    X<-which(X==max(X))
    paste(sort(names(data_comb)[X]),collapse = ".")
  })]
  
  data_comb$combined<-rowSums(data_comb[,!"max"])
  
  setnames(data_comb,"max",paste0(name,"_max"))
  
  return(data_comb)
}


# Subset data to chosen admin level, admin areas, crops and hazards. Also subset to weighted or unweighted data
hi_adm_ss<-subset_fun1(data=hi,
                       crop_choice=crop_choice,
                       scenario_choice=scenario_choice,
                       admin_level=admin_level,
                       admin_choice=admin_choice,
                       hazards=hazards,
                       keep_name=T,
                       weighted=weighted)

# Combine data across hazards or 
combine_hi_vect<-function(data=hi_adm_ss,crop_choice,hazards,stat){

  # Summarize hazard indices by crop
  hi_adm_ss_crop_combined<-combine_fun(data=data,values=crop_choice,stat=stat,name="crop")
  

  # Summarize hazard indices by hazard
  hi_adm_ss_haz_combined<-combine_fun(data=data,values=hazards,stat=stat,name="haz")

  # Combine data back with vector admin map
  hi_adm_ss_combined<-cbind(data[,1:6],hi_adm_ss_crop_combined,hi_adm_ss_haz_combined[,!"combined"])
  return(hi_adm_ss_combined)

}

hi_adm_ss_combined<-combine_hi_vect(data=hi_adm_ss,
                                   crop_choice=crop_choice,
                                   hazards=hazards,
                                   stat=stat)

# Plots to test 
plot_names<-c("combined",crop_choice)

plot(hi_adm_ss_combined,plot_names,type="continuous",col=RColorBrewer::brewer.pal(9,"YlOrRd"))

plot_names<-c("combined",hazards)
plot(hi_adm_ss_combined,plot_names,type="continuous",col=RColorBrewer::brewer.pal(9,"YlOrRd"))


# Raster approach to sub-setting, weighting and combining hazard index data ####
files<-list.files(save_dir_hi,".tif",full.names = T)
files<-files[!grepl(paste0(Scenarios$Scenario,collapse = "|"),files)]


# Precook weighted hi raster data ####
hi_rast<-terra::rast(files[!grepl("change",files)])
names(hi_rast)<-fix_names(names(hi_rast))
hi_rast<-hi_rast[[!grepl("generic",names(hi_rast))]]

# weight mapspam data
ms_rast<-terra::rast("Data/mapspam/spam2017V2r3_SSA_V_TA-vop_tot.tif")

data<-ms_rast/terra::app(ms_rast,sum,na.rm=T)
terra::writeRaster(data,"Data/mapspam/spam2017V2r3_SSA_V_TA-vop_tot_weightsum.tif",overwrite=T)
data<-ms_rast/terra::app(ms_rast,max,na.rm=T)
terra::writeRaster(data,"Data/mapspam/spam2017V2r3_SSA_V_TA-vop_tot_weightmax.tif",overwrite=T)


ms_rast<-terra::rast("Data/mapspam/spam2017V2r3_SSA_H_TA-ha_tot.tif")

data<-ms_rast/terra::app(ms_rast,sum,na.rm=T)
terra::writeRaster(data,"Data/mapspam/spam2017V2r3_SSA_H_TA-ha_tot_weightsum.tif",overwrite=T)
data<-ms_rast/terra::app(ms_rast,max,na.rm=T)
terra::writeRaster(data,"Data/mapspam/spam2017V2r3_SSA_H_TA-ha_tot_weightmax.tif",overwrite=T)


# This is RAM heavy, need at least 128gb
for(var in c("vop","ha")){
  for(method in c("sum","max")){
    file<-paste0(save_dir_hi,"/hi_weighted_",var,"_",method,".tif")
    print(file)
    if(!file.exists(file)){
      
      if(var=="vop"){
        if(method=="sum"){
          ms_rast_weights<-terra::rast("Data/mapspam/spam2017V2r3_SSA_V_TA-vop_tot_weightsum.tif")
        }
        if(method=="max"){
          ms_rast_weights<-terra::rast("Data/mapspam/spam2017V2r3_SSA_V_TA-vop_tot_weightmax.tif")
        }
        }else{
          if(method=="sum"){
            ms_rast_weights<-terra::rast("Data/mapspam/spam2017V2r3_SSA_H_TA-ha_tot_weightsum.tif")
          }
          if(method=="max"){
            ms_rast_weights<-terra::rast("Data/mapspam/spam2017V2r3_SSA_H_TA-ha_tot_weightmax.tif")
          }   
          }
      
      crops<-ms_codes[,Fullname]
      
      hi_rast_weighted<-terra::rast(lapply(crops,FUN=function(crop){
        print(crop)
        ms_focal<-ms_rast_weights[[grep(crop,names(ms_rast_weights),value=T)]]
        hi_rast_focal<-hi_rast[[grep(crop,names(hi_rast),value=T)]]
        hi_rast_focal*ms_focal
      }))
      
      terra::writeRaster(hi_rast_weighted,filename=file)
      
      rm("hi_rast_weighted")
      gc()
    }
  }
}

if(weighted){
  file<-paste0(save_dir_hi,"/hi_weighted_",var,"_",method,".tif")
  hi_rast<-terra::rast(file)
}

subset_rast_fun<-function(data,crop_choice,scenario_choice,hazards){
  X<-data
  X<-X[[names(X)[(grepl(paste0(crop_choice,collapse = "|"),names(X))*grepl(scenario_choice,names(X))*grepl(paste0(hazards,collapse = "|"),names(X)))==1]]]
  return(X)
}

hi_rast_ss<-subset_rast_fun(data=hi_rast,crop_choice,scenario_choice,hazards)

combine_fun_rast<-function(data,values,name,stat){
  
  data_comb<-terra::rast(lapply(values,FUN=function(VAL){
    print(VAL)
    layers<-grep(VAL,names(data),value = T)
    data_ss<-data[[layers]]
    
    if(stat=="mean"){
      data_ss<-terra::app(data_ss,mean,na.rm=T)
    }
    
    if(stat=="max"){
      data_ss<-terra::app(data_ss,max,na.rm=T)
      
    }
    
    if(stat=="sum"){
      data_ss<-sum(data_ss,na.rm = T)
    }
    
    data_ss
  }))
  
  names(data_comb)<-values

  data_comb_max<-data_comb==data_comb_max
  for(i in 2:nlyr(data_comb_max)){
    data_comb_max[[i]]<-data_comb_max[[i]]*10^(i-1)
  }
  data_comb_max<-sum(data_comb_max,na.rm = T)
  
  # Need to add in value table
  
  data_comb[[paste0(name,"_max")]]<-data_comb_max
  data_comb[[paste0(name,"_combined")]]<-sum(data_comb,na.rm=T)

  return(data_comb)
}

combine_hi_rast<-function(data,crop_choice,hazards,stat){
  
  
  # Summarize hazard indices by crop
  hi_adm_ss_crop_combined<-combine_fun_rast(data=data,values=crop_choice,name="crop",stat=stat)
  
  
  # Summarize hazard indices by hazard
  hi_adm_ss_haz_combined<-combine_fun_rast(data=data,values=hazards,name="haz",stat=stat)
  
  # Combine data back with vector admin map
  hi_adm_ss_combined<-c(hi_adm_ss_crop_combined,hi_adm_ss_haz_combined)
  return(hi_adm_ss_combined)
  
}

hi_rast_ss_combined<-combine_hi_rast(data=hi_rast_ss,crop_choice,hazards,stat=stat)

# 






