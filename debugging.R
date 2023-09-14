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
haz_class<-fread("./Data/metadata/haz_classes.csv")
haz_classes<-unique(haz_class$description)

Scenarios<-c("ssp245","ssp585")
Times<-c("2021_2040","2041_2060")
Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))

scenarios_x_hazards<-data.table(Scenarios,Hazard=rep(hazards,each=nrow(Scenarios)))[,Scenario:=as.character(Scenario)][,Time:=as.character(Time)]


haz_names<-data.table(Variable=hazards,
                      Renamed=hazards)

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

Analysis_Vars<-haz_names[c(3,5,9),Renamed]

SubGeog<-if(AdminLevel=="Admin2"){
  Geographies$admin2[Geographies$admin2$admin_name %in% Admin2,]
}else{
  Geographies$admin1[Geographies$admin1$admin_name %in% Admin1,]
}
  

PlotHazards<-terra::mask(terra::crop(Hazards[["historic-historic"]],SubGeog),SubGeog)
PlotHazards_future<-terra::mask(terra::crop(Hazards[[Future]],SubGeog),SubGeog)
PlotHazards_diff<-PlotHazards_future- PlotHazards

# Hazard Index = severity x recurrence
haz_index<-hazard_index(Hazards,verbose = T,SaveDir=SaveDir,crop_choice = crop_choice)

# Combine hazard indice for selected variables
haz_comb<-lapply(1:length(haz_index),FUN=function(i){
  terra::app(haz_index[[i]][[paste0(Analysis_Vars,"_hazard_index")]],sum,na.rm=T)
})
names(haz_comb)<-names(haz_index)

# Combined classified severity by hazard


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
