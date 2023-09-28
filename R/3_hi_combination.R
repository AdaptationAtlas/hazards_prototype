source("R/haz_functions.R")
require(data.table)
require(terra)

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

save_dir_means<-paste0("Data/hazard_means/",timeframe_choice)
files<-list.files(save_dir_means,".tif",full.names = T)

haz_means<-terra::rast(files[!grepl("change",files)])
haz_means_change<-terra::rast(files[grepl("change",files)])

haz_means_adm0<-terra::vect(paste0(save_dir_means,"/haz_means_adm0.gpkg"))
haz_means_adm1<-terra::vect(paste0(save_dir_means,"/haz_means_adm1.gpkg"))
haz_means_adm2<-terra::vect(paste0(save_dir_means,"/haz_means_adm2.gpkg"))

haz_means_change_adm0<-terra::vect(paste0(save_dir_means,"/haz_means_change_adm0.gpkg"))
haz_means_change_adm1<-terra::vect(paste0(save_dir_means,"/haz_means_change_adm1.gpkg"))
haz_means_change_adm2<-terra::vect(paste0(save_dir_means,"/haz_means_change_adm2.gpkg"))

# read in hazard indices ####
save_dir_hi<-paste0("Data/hazard_indices/",timeframe_choice)
files<-list.files(save_dir_hi,"combined",full.names = T)
files<-grep("tif",files,value = T)

# temporary function to harmonize names
fix_names<-function(data){
  data<-gsub("-","_",data,fixed = T)
  data<-gsub("1_2","1.2",data,fixed = T)
  data
}

hi<-terra::rast(files[!grepl("change",files)])
names(hi)<-fix_names(names(hi))
hi_change<-terra::rast(files[grepl("change",files)])
names(hi_change)<-fix_names(names(hi_change))

hi_adm0<-cbind(terra::vect(paste0(save_dir_hi,"/hi_adm0_1.gpkg")),terra::vect(paste0(save_dir_hi,"/hi_adm0_2.gpkg"))[,-(1:6)])
hi_adm1<-cbind(terra::vect(paste0(save_dir_hi,"/hi_adm1_1.gpkg")),terra::vect(paste0(save_dir_hi,"/hi_adm1_2.gpkg"))[,-(1:6)])
hi_adm2<-cbind(terra::vect(paste0(save_dir_hi,"/hi_adm2_1.gpkg")),terra::vect(paste0(save_dir_hi,"/hi_adm2_2.gpkg"))[,-(1:6)])
names(hi_adm0)<-fix_names(names(hi_adm0))
names(hi_adm1)<-fix_names(names(hi_adm1))
names(hi_adm2)<-fix_names(names(hi_adm2))

hi<-list(admin0=hi_adm0,admin1=hi_adm1,admin2=hi_adm2)

hi_change_adm0<-cbind(terra::vect(paste0(save_dir_hi,"/hi_change_adm0_1.gpkg")),terra::vect(paste0(save_dir_hi,"/hi_change_adm0_2.gpkg"))[,-(1:6)])
hi_change_adm1<-cbind(terra::vect(paste0(save_dir_hi,"/hi_change_adm1_1.gpkg")),terra::vect(paste0(save_dir_hi,"/hi_change_adm1_2.gpkg"))[,-(1:6)])
hi_change_adm2<-cbind(terra::vect(paste0(save_dir_hi,"/hi_change_adm2_1.gpkg")),terra::vect(paste0(save_dir_hi,"/hi_change_adm2_2.gpkg"))[,-(1:6)])

names(hi_change_adm0)<-fix_names(names(hi_change_adm0))
names(hi_change_adm1)<-fix_names(names(hi_change_adm1))
names(hi_change_adm2)<-fix_names(names(hi_change_adm2))

# read in mapspam ####
# metadata
ms_codes<-data.table::fread("./Data/metadata/SpamCodes.csv")[,Code:=toupper(Code)]
ms_codes<-ms_codes[compound=="no"]

# mapspam vop 
vop<-terra::rast("Data/mapspam/vop_tot.tif")

vop_tot_adm0<-terra::vect("Data/mapspam/vop_adm0.gpkg")
vop_tot_adm1<-terra::vect("Data/mapspam/vop_adm1.gpkg")
vop_tot_adm2<-terra::vect("Data/mapspam/vop_adm2.gpkg")

vop<-list(admin0=vop_tot_adm0,admin1=vop_tot_adm1,admin2=vop_tot_adm2)

# metadata
ms_codes<-data.table::fread("./Data/metadata/SpamCodes.csv")[,Code:=toupper(Code)]
ms_codes<-ms_codes[compound=="no"]

# list crops
crops<-ms_codes$Fullname

# list hazards
hazards<-c("NDD","NTx40","NTx35","HSHmax","HSHmean","THImax","THImean","NDWS","TAI","NDWL0","PTOT","TAVG")

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
admin_choice<-hi$admin0$admin_name[1:5]

hazards<-c("TAVG","PTOT","NDD")

subset_fun<-function(data,crop_choice,scenario_choice,admin_level,admin_choice,hazards){
  X<-data[[admin_level]]
  X<-X[X$admin_name %in% admin_choice]
  X<-X[,(grepl(paste0(crop_choice,collapse = "|"),names(X))*grepl(scenario_choice,names(X))*grepl(paste0(hazards,collapse = "|"),names(X)))==1]
  return(X)
}

# Subset hazard index
hi_adm_ss<-subset_fun(data=hi,crop_choice,scenario_choice,admin_level,admin_choice,hazards)

# Subset vop
vop_adm_ss<-subset_fun(data=vop,crop_choice,scenario_choice="",admin_level,admin_choice,hazards="")
vop_adm_ss_weights<-data.frame(vop_adm_ss)/rowSums(data.frame(vop_adm_ss))

# Apply weighting



haz_means
haz_means_change
haz_means_adm0


# Combine hazard indice for selected variables
haz_comb<-terra::rast(lapply(1:length(haz_index),FUN=function(i){
  terra::app(haz_index[[i]][[paste0(Analysis_Vars,"_hazard_index")]],sum,na.rm=T)
}))
names(haz_comb)<-names(haz_index)
plot(haz_comb[[2:5]]-haz_comb[[1]])

# Combined classified severity by hazard

hazard_severity<-function(Hazards,verbose=T,SaveDir,crop_choice,severity_classes,PropThreshold){
  
  severity_classes2<-rbind(data.table(class="None",value=0),severity_classes)
  
  scenario_names<-names(Hazards)
  
  data<-lapply(1:length(Hazards),FUN = function(j){
    
    filename<-paste0(SaveDir,"/hs_",crop_choice,"_",scenario_names[j],"-",PropThreshold,".tif")
    
    if(!file.exists(filename)){
      
      # Subtract severe and extreme from moderate, and severe from extreme
      data<-terra::rast(lapply(1:length(hazards),FUN=function(i){
        
        if(verbose){
          # Display progress
          cat('\r                                                                                                                     ')
          cat('\r',paste0("Scenario ", scenario_names[j]," | Hazard ",hazards[i]))
          flush.console()
        }
        
        N<-paste0(hazards[i],"_propclass_",severity_classes$class)
        sev<-Hazards[[j]][[N]]
        sev<-terra::rast(lapply(1:nlyr(sev),FUN=function(k){
          sev[[k]]*severity_classes[k,value]
        }))
        sev<-terra::app(sev,max,na.rm=T)
        
        sev_vals<-unique(values(sev))
        sev_vals<-sev_vals[!is.na(sev_vals)]
        
        levels(sev)<-severity_classes2[value %in% sev_vals,list(value,class)]
        names(sev)<-paste(hazards[i],"_propclass_merged")
        
        sev
      }))
      
      terra::writeRaster(data,file=filename)
      
      data
      
    }else{
      data<-terra::rast(filename)
    }
    
    data
    
  })
  names(data)<-scenario_names
  
  return(data)
}

haz_sev<-hazard_severity(Hazards,verbose=T,SaveDir,crop_choice,severity_classes,PropThreshold)

severity_classes2<-rbind(data.table(class="None",value=0),severity_classes)
severity_classes2$col<-PalFun(Palette,nrow(severity_classes2),invert=F,alpha=0.5)

# Use the above to harmonize colours for severity classes between maps
haz_sev_plot<-lapply(1:length(haz_sev),FUN=function(i){
  data<-haz_sev[[i]]
  data<-terra::rast(lapply(1:nlyr(data),FUN=function(j){
    map<-data[[j]]
    coltab(map)<-severity_classes2[value %in% levels(map)[[1]]$value,list(value,col)]
    map
  }))
  data
})

names(haz_sev_plot)<-names(haz_sev)

plot(haz_sev_plot$`historic-historic`)

# Combined plot
haz_sev_comb_plot<-terra::rast(lapply(1:length(haz_sev),FUN=function(i){
  data<-haz_sev[[i]]
  data<-terra::app(data,max,na.rm=T)
  data_vals<-unique(values(data))
  data_vals<-data_vals[!is.nan(data_vals)]
  levels(data)<-severity_classes2[value %in% data_vals,list(value,class)]
  coltab(data)<-severity_classes2[value %in% levels(data)[[1]]$value,list(value,col)]
  names(data)<-names(haz_sev)[i]
  data
}))

# Intersection with exposure
Hazards$`historic-historic`$
  
  # **** COMBINE CROPS ***** 
  # use a weighting for crop value
  
  # Combined Hazards - This section needs updating to show the most severe hazard(s) for a pixel
  HazComb<-HazCombWrapper(Hazards=Hazards,
                          SaveDir=SaveDir,
                          Scenarios=Scenarios,
                          FileName=FileName2,
                          SelectedHaz = Analysis_Vars)


HazPalCombMean<-PalFun(PalName=Palette,
                       N=nrow(HazComb[["MeanHaz"]][[Future]][["Classes"]]),
                       Names=HazComb[["MeanHaz"]][[Future]][["Classes"]][["Hazard"]])

HazPalCombProp<-PalFun(PalName=Palette,
                       N=nrow(HazComb[["PropHaz"]][[Future]][["Classes"]]),
                       Names=HazComb[["PropHaz"]][[Future]][["Classes"]][["Hazard"]])



addGeog1<-function(){terra::plot(terra::aggregate(SubGeog,by="NAME_1"),add=T,border="black",lwd=borderwidth)}
addGeog2<-function(){terra::plot(SubGeog,add=T,border="black",lwd=borderwidth)}

Plot_Vars<-Analysis_Vars


#SR_plot1_mean

PlotHazards_mean<-PlotHazards[[grep("mean_Moderate",names(PlotHazards))]]
names(PlotHazards_mean)<-gsub("_Moderate","",names(PlotHazards_mean))

PlotHazards_future_mean<-PlotHazards_future[[grep("mean_Moderate",names(PlotHazards))]]
names(PlotHazards_future_mean)<-gsub("_Moderate","",names(PlotHazards_future_mean))

PlotHazards_diff_mean<-PlotHazards_future_mean- PlotHazards_mean

SR_plot1_mean<-lapply(1:length(Plot_Vars),FUN=function(i){
  historic<-PlotHazards_mean[[paste0(Plot_Vars[i],"_mean")]]
  future<-PlotHazards_future_mean[[paste0(Plot_Vars[i],"_mean")]]
  names(historic)<-paste0("historic-",names(historic))
  names(future)<-paste0(Future,"-",names(future))
  c(historic,future)
})
names(SR_plot1_mean)<-Plot_Vars


terra::plot(SR_plot1_mean[[1]],
            fun=if(AdminLevel=="Admin2"){addGeog2}else{addGeog1},
            plg=list(x=LegPos,cex = TextSize,ncol=LegCols),
            pax=list(cex.axis = TextSize),
            cex.main=TextSize*1.2,
            range=range_fun(SR_plot1_mean[[1]]),
            col=PalFun(PalName=Palette,
                       N=50)
)

# THIS NEEDS TO MOVED OUTSIDE OF THIS SECTION
Thresholds$Renamed<-Thresholds$Variable