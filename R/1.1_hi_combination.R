source("R/haz_functions.R")
require(data.table)
require(terra)





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