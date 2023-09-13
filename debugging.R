country_choice<-"Burundi"
timeframe_choice<-"annual"

if(!dir.exists(country_dir)){
  dir.create(country_dir)
  unzip(zipfile=country_zips[Country==country_choice & timeframe==timeframe_choice,filepath],exdir=country_dir,junkpaths=T)
}

SaveDir<- paste0(country_dir,"/Analysis")

if(!dir.exists(SaveDir)){
  dir.create(SaveDir)
}

country_zips<-data.table(filepath=list.files("./Data/country_data_zips",".zip",full.names = T))
country_zips[,iso3c:=unlist(tstrsplit(tail(tstrsplit(filepath,"/"),1),"-",keep=1)),by=filepath
][,timeframe:=gsub(".zip","",unlist(tstrsplit(tail(tstrsplit(filepath,"/"),1),"-",keep=2)),"-"),by=filepath
][timeframe=="seasonal",timeframe:="seasonal_jagermeyer_cc"]
country_zips[,folder:=gsub(".zip","",unlist(tail(tstrsplit(filepath,"/"),1)))]
country_zips[,Country:=countrycode::countrycode(iso3c, origin = 'iso3c', destination = 'country.name')]

country_dir<-paste0("./Data/country_data/", country_zips[Country==country_choice & timeframe==timeframe_choice,folder])
ocha_dir<-paste0("Data/ocha_boundaries/", country_zips[Country==country_choice & timeframe==timeframe_choice,iso3c])


hazards<-c("NDD","NTx40","NTx35","HSH_max","HSH_mean","THI_max","THI_mean","NDWS","TAI","NDWL0","PTOT")
haz_meta<-data.table::fread("./Data/metadata/haz_metadata.csv")
haz_class<-fread("./Data/metadata/haz_classes.csv")
haz_classes<-unique(haz_class$description)

Scenarios<-c("ssp245","ssp585")
Times<-c("2021_2040","2041_2060")
Scenarios<-rbind(data.table(Scenario="historic",Time="historic"),data.table(expand.grid(Scenario=Scenarios,Time=Times)))

scenarios_x_hazards<-data.table(Scenarios,Hazard=rep(hazards,each=nrow(Scenarios)))[,Scenario:=as.character(Scenario)][,Time:=as.character(Time)]


haz_names<-data.table(Variable=hazards,
                      Renamed=hazards)

Thresholds<-haz_class[description!="No significant stress",list(index_name,description,lower_lim,upper_lim,direction)]
setnames(Thresholds,c("index_name","description","direction"),c("Variable","Severity_class","Direction"))

Thresholds<-merge(Thresholds,haz_names,by="Variable",all.x=T)

Thresholds<-haz_class[description!="No significant stress",list(index_name,description,lower_lim,upper_lim,direction)]
setnames(Thresholds,c("index_name","description","direction"),c("Variable","Severity_class","Direction"))
Thresholds$Renamed<-Thresholds$Variable
  
hazard_dir<-country_dir
PropThreshold<-0.5
PropTDir=">"


Hazards<-HazardWrapper(Thresholds,
              SaveDir,
              PropThreshold,
              PropTDir,
              hazard_dir = country_dir,
              Scenarios,
              verbose=F)

A<-nchar(unlist(tstrsplit(names(Hazards[[i]]),paste0(hazards,collapse="|"),keep=2)))
B<-substr(names(Hazards[[i]]),1,nchar(names(Hazards[[i]]))-A)

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
FileName2<- Thresholds[Renamed %in% Analysis_Vars,paste(unique(Code),collapse = "")]


SubGeog<-if(AdminLevel=="Admin2"){
  Geographies$admin2[Geographies$admin2$admin_name %in% Admin2,]
}else{
  Geographies$admin1[Geographies$admin1$admin_name %in% Admin1,]
}
  

PlotHazards<-terra::mask(terra::crop(Hazards[["historic-historic"]],SubGeog),SubGeog)
PlotHazards_future<-terra::mask(terra::crop(Hazards[[Future]],SubGeog),SubGeog)
PlotHazards_diff<-PlotHazards_future- PlotHazards

# Hazard Index
scenario_names<-paste0(Scenarios$Scenario,"-",Scenarios$Time)
data<-Hazards[[scenario_names[[1]]]]

# Index = severity x recurrence
data

recurrence<-data[[grep("_prop_",names(data),value=T)]]

# Subtract severe and extreme from moderate, and severe from extreme
for(i in 1:length(hazards)){
  N<-paste0(hazards[i],"_prop_",severity_classes$class)
  N1<-which(names(recurrence)==N[1])
  N2<-which(names(recurrence)==N[2])
  X<-recurrence[[N[1]]]-recurrence[[N[2]]]-recurrence[[N[3]]]
  X[][X[]<0 & !is.na(X[])]<-0
  recurrence[[N1]]<-X
  Y<-recurrence[[N2]]-recurrence[[N[3]]]
  Y[][Y[]<0 & !is.na(Y[])]<-0
  recurrence[[N2]]<-Y
}


severity<-data[[grep("_propclass",names(data),value=T)]]

severity_classes<-data.table(class=c("Moderate","Severe","Extreme"),value=c(1,2,3))

for(i in 1:nrow(severity_classes)){
  N<-which(grepl(severity_classes$class[i],names(severity)))
  severity[[N]]<-severity[[N]]*severity_classes$value[i]
}

haz_index<-recurrence*severity

haz_index<-terra::rast(lapply(1:length(hazards),FUN=function(i){
  N<-paste0(hazards[i],"_prop_",severity_classes$class)
  X<-terra::app(haz_index[[N]],sum,na.rm=T)
  names(X)<-paste0(hazards[i],"_hazard_index")
  X
}))


haz_comb<-terra::app(haz_index[[paste0(Analysis_Vars,"_hazard_index")]],sum,na.rm=T)


# Combined Hazards
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
