source("R/haz_functions.R")

country_choice<-"Burundi"
timeframe_choice<-"annual"

country_zips<-data.table(filepath=list.files("./Data/country_data_zips",".zip",full.names = T))
country_zips[,iso3c:=unlist(tstrsplit(tail(tstrsplit(filepath,"/"),1),"-",keep=1)),by=filepath
][,timeframe:=gsub(".zip","",unlist(tstrsplit(tail(tstrsplit(filepath,"/"),1),"-",keep=2)),"-"),by=filepath
][timeframe=="seasonal",timeframe:="seasonal_jagermeyer_cc"]
country_zips[,folder:=gsub(".zip","",unlist(tail(tstrsplit(filepath,"/"),1)))]
country_zips[,Country:=countrycode::countrycode(iso3c, origin = 'iso3c', destination = 'country.name')]

country_dir<-paste0("./Data/country_data/", country_zips[Country==country_choice & timeframe==timeframe_choice,folder])
ocha_dir<-paste0("Data/ocha_boundaries/", country_zips[Country==country_choice & timeframe==timeframe_choice,iso3c])

if(!dir.exists(country_dir)){
  dir.create(country_dir)
  unzip(zipfile=country_zips[Country==country_choice & timeframe==timeframe_choice,filepath],exdir=country_dir,junkpaths=T)
}


SaveDir<- paste0(country_dir,"/Analysis")

if(!dir.exists(SaveDir)){
  dir.create(SaveDir)
}

hazards<-c("NDD","NTx40","NTx35","HSH_max","HSH_mean","THI_max","THI_mean","NDWS","TAI","NDWL0","PTOT","TAVG")
haz_meta<-data.table::fread("./Data/metadata/haz_metadata.csv")
haz_class<-fread("./Data/metadata/haz_classes.csv")[,list(index_name,description,direction,crop,threshold)]
haz_classes<-unique(haz_class$description)

Scenarios<-c("ssp245","ssp585")
Times<-c("2021_2040","2041_2060")
Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))

scenarios_x_hazards<-data.table(Scenarios,Hazard=rep(hazards,each=nrow(Scenarios)))[,Scenario:=as.character(Scenario)][,Time:=as.character(Time)]


haz_names<-data.table(Variable=hazards,
                      Renamed=hazards)


# read in mapspam
ms_codes<-data.table::fread("./Data/metadata/SpamCodes.csv")[,Code:=toupper(Code)]
ms_codes<-ms_codes[compound=="no"]
crop_choices<-ms_codes[,sort(Fullname)]

# read in ecocrop
ecocrop<-miceadds::load.Rdata2(file="Data/ecocrop.RData")[,list(species,Life.span,temp_opt_min,Temp_Opt_Max,Temp_Abs_Min,Temp_Abs_Max,Rain_Opt_Min,Rain_Opt_Max,Rain_Abs_Min,
                                                                Rain_Abs_Max,cycle_min,cycle_max)]

# add ecocrop thresholds to mapspam - PTOT, TAVG
description<-c("Moderate","Severe","Extreme")
for(i in 1:nrow(ms_codes)){
  crop<-ms_codes[i,sci_name]
  ecrop<-ecocrop[grep(crop,species)]
  ecrop[,list(Rain_Abs_Min,Rain_Opt_Min,Rain_Opt_Max,Rain_Abs_Max)]
  
  # PTOT low
  ptot_low<-data.table(index_name="PTOT",
                       description=description,
                       direction="<",
                       crop=ms_codes[i,Fullname],
                       threshold=c(
                         unlist(ecrop$Rain_Opt_Min), # Moderate
                         (unlist(ecrop$Rain_Abs_Min)+unlist(ecrop$Rain_Opt_Min))/2, # Severe
                         unlist(ecrop$Rain_Abs_Min))) # Extreme
  
  # PTOT high
  ptot_high<-data.table(index_name="PTOT",
                       description=description,
                       direction=">",
                       crop=ms_codes[i,Fullname],
                       threshold=c(
                         unlist(ecrop$Rain_Opt_Max), # Moderate
                         (unlist(ecrop$Rain_Opt_Max)+unlist(ecrop$Rain_Abs_Max))/2, # Severe
                         unlist(ecrop$Rain_Abs_Max))) # Extreme
  
  # TAVG low
    tavg_low<-data.table(index_name="TAVG",
                       description=description,
                       direction="<",
                       crop=ms_codes[i,Fullname],
                       threshold=c(
                         unlist(ecrop$temp_opt_min), # Moderate
                         (unlist(ecrop$temp_opt_min)+unlist(ecrop$Temp_Abs_Min))/2, # Severe
                         unlist(ecrop$Temp_Abs_Min))) # Extreme
  
  # TAVG high
  tavg_high<-data.table(index_name="TAVG",
                       description=description,
                       direction=">",
                       crop=ms_codes[i,Fullname],
                       threshold=c(
                         unlist(ecrop$Temp_Opt_Max), # Moderate
                         (unlist(ecrop$Temp_Opt_Max)+unlist(ecrop$Temp_Abs_Max))/2, # Severe
                         unlist(ecrop$Temp_Abs_Max))) # Extreme
  
  rbind(ptot_low,ptot_high,tavg_low,tavg_high)

  

}


crop_choice<-"generic"
Thresholds<-haz_class[description!="No significant stress" & crop_choice=="generic",list(index_name,description,direction,threshold)]
setnames(Thresholds,c("index_name","description","direction"),c("Variable","Severity_class","Direction"))

Thresholds<-merge(Thresholds,haz_names,by="Variable",all.x=T)

Thresholds<-haz_class[description!="No significant stress",list(index_name,description,direction,threshold)]
setnames(Thresholds,c("index_name","description","direction"),c("Variable","Severity_class","Direction"))
Thresholds$Renamed<-Thresholds$Variable

Thresholds[,Code:=paste0(Direction,threshold)
           ][,Code:=gsub("<","L",Code)
             ][,Code:=gsub(">","G",Code)
               ][,Code:=paste0(Variable,"_",Code)]
    
hazard_dir<-country_dir
PropThreshold<-0.5
PropTDir=">"

Hazards<-HazardWrapper(Thresholds,
                      SaveDir=SaveDir,
                      PropThreshold=PropThreshold,
                      PropTDir=PropTDir,
                      hazard_dir = country_dir,
                      Scenarios=Scenarios,
                      verbose=F)

# Add severity classes to hazards
Thresholds_unique<-unique(Thresholds[,list(Variable,Renamed,Severity_class)])
A<-nchar(unlist(tstrsplit(names(Hazards[[1]]),paste0(hazards,collapse="|"),keep=2)))
B<-substr(names(Hazards[[1]]),1,nchar(names(Hazards[[1]]))-A)

for(i in 1:length(Hazards)){
  names(Hazards[[i]])<-paste0(names(Hazards[[i]]),"_",rep(Thresholds_unique$Severity_class,rep(rle(B)$lengths,each=3)/3))
}


if(dir.exists(ocha_dir)){
  unlink(list.files(ocha_dir,".xml",full.names = T))
  # OCHA format
  Geographies<-list(
    admin2=terra::aggregate(terra::vect(grep("_adm2_",list.files(ocha_dir,".shp",full.names = T),value = T)),by="ADM2_EN"),
    admin1=terra::aggregate(terra::vect(grep("_adm1_",list.files(ocha_dir,".shp",full.names = T),value = T)),by="ADM1_EN"),
    admin0=terra::vect(grep("_adm0_",list.files(ocha_dir,".shp",full.names = T),value = T))
  )
  
  # Create standard name field for each admin vector
  Geographies$admin2$admin_name<-Geographies$admin2$ADM2_EN
  Geographies$admin1$admin_name<-Geographies$admin1$ADM1_EN
  Geographies$admin0$admin_name<-Geographies$admin0$ADM0_EN
  
}else{ 
  # GADM format
  Geographies<-list(
    admin2=terra::aggregate(terra::vect(grep("_2.shp",list.files(country_dir,full.names = T),value = T)),by="NAME_2"),
    admin1=terra::aggregate(terra::vect(grep("_1.shp",list.files(country_dir,full.names = T),value = T)),by="NAME_1"),
    admin0=terra::vect(grep("_0.shp",list.files(country_dir,full.names = T),value = T))
  )
  
  # Create standard name field for each admin vector
  Geographies$admin2$admin_name<-Geographies$admin2$NAME_2
  Geographies$admin1$admin_name<-Geographies$admin1$NAME_1
  Geographies$admin0$admin_name<-Geographies$admin0$COUNTRY
}


AdminLevel<-"Admin1"
Admin1<-Geographies$admin1$admin_name
Admin2<-Geographies$admin2$admin_name

Future<-"ssp245-2041_2060"
PropThreshold<-0.5
Palette<-"turbo"
borderwidth<-1
LegCols<-1
TextSize<-1.2
LegPos<-"bottomleft"

Analysis_Vars<-haz_names[c(3,5,9),Renamed]

SubGeog<-if(AdminLevel=="Admin2"){
  Geographies$admin2[Geographies$admin2$admin_name %in% Admin2,]
}else{
  Geographies$admin1[Geographies$admin1$admin_name %in% Admin1,]
}
  

PlotHazards<-terra::mask(terra::crop(Hazards[["historic-historic"]],SubGeog),SubGeog)
PlotHazards_future<-terra::mask(terra::crop(Hazards[[Future]],SubGeog),SubGeog)
PlotHazards_diff<-PlotHazards_future- PlotHazards


severity_classes<-data.table(class=c("Moderate","Severe","Extreme"),value=c(1,2,3))

# Hazard Index = severity x recurrence
haz_index<-hazard_index(Hazards,verbose = T,SaveDir=SaveDir,crop_choice = crop_choice,severity_classes=severity_classes,PropThreshold=PropThreshold)

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

