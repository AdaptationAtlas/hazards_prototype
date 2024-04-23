# a) Load R functions & packages ####
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
              "httr",
              "countrycode",
              "wbstats")

# Call the function to install and load packages
load_and_install_packages(packages)

# Load functions & wrappers
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

# b) Set up workspace ####
# Load base raster to resample to 
base_raster<-"base_raster.tif"
if(!file.exists(base_raster)){
url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/base_raster.tif"
httr::GET(url, write_disk(base_raster, overwrite = TRUE))
}

base_rast<-terra::rast(base_raster)

# c) Set directories ####
glw_dir<-"Data/GLW4"
if(!dir.exists(glw_dir)){
  dir.create(glw_dir)
}

fao_dir<-"Data/fao"
if(!dir.exists(fao_dir)){
  dir.create(fao_dir,recursive = T)
}

geo_dir<-"Data/boundaries"
if(!dir.exists(geo_dir)){
  dir.create(geo_dir,recursive = T)
}

ls_vop_dir<-"Data/livestock_vop"
if(!dir.exists(ls_vop_dir)){
  dir.create(ls_vop_dir,recursive = T)
}

# 1) Load geographies ####
# Load and combine geoboundaries
overwrite<-F

geo_files_s3<-"https://digital-atlas.s3.amazonaws.com/boundaries/atlas-region_admin0_harmonized.gpkg"

file<-file.path(geo_dir,basename(geo_files_s3))

if(!file.exists(file)|overwrite==T){
  download.file(url=geo_files_s3[i],destfile=file)
}
  
geoboundaries<-terra::vect(file)

# 2) Load glw4 data ####
glw_names<-c(poultry="Ch",sheep="Sh",pigs="Pg",horses="Ho",goats="Gt",ducks="Dk",buffalo="Bf",cattle="Ct")
glw_codes<-c(poultry=6786792,sheep=6769626,pigs=6769654,horses=6769681,goats=6769696,ducks=6769700,buffalo=6770179,cattle=6769711)

glw_files <- file.path(glw_dir,paste0("5_",glw_names,"_2015_Da.tif"))
overwrite<-F

for(i in 1:length(glw_files)){
  glw_file<-glw_files[i]
  if(!file.exists(glw_file)|overwrite==T){
    api_url <- paste0("https://dataverse.harvard.edu/api/access/datafile/",glw_codes[i])
    # Perform the API request and save the file
    response <- GET(url = api_url, write_disk(glw_file, overwrite = TRUE))
    
    # Check if the download was successful
    if (status_code(response) == 200) {
      print(paste0("File ",i," downloaded successfully."))
    } else {
      print(paste("Failed to download file ",i,". Status code:", status_code(response)))
    }
  }
}

glw<-terra::rast(glw_files)
names(glw)<-names(glw_names)

glw<-glw[[c("poultry","sheep","pigs","goats","cattle")]]

# 2.1) Resample & mask to atlas area #####
#### units are absolute number of animals per pixel
# convert to density
glw<-glw/terra::cellSize(glw,unit="ha")
# resample to the atlas base raster
glw<-terra::resample(glw,base_rast)
# convert back to numbers per pixel
glw<-glw*terra::cellSize(glw,unit="ha")
# mask to focal countries
glw<-terra::mask(glw,geoboundaries)

# 3) Load FAOstat data ####
remove_countries<- c("Ethiopia PDR","Sudan (former)","Cabo Verde","Comoros","Mauritius","R\xe9union","Seychelles")
atlas_iso3<-geoboundaries$iso3
target_year<-c(2015,2017)

  # 3.1) VoP #####
    # 3.1.1) Download ######
    vop_file<-file.path(fao_dir,"Value_of_Production_E_Africa.csv")
    update<-F
    if(!file.exists(vop_file)|update){
      # Define the URL and set the save path
      url<-"https://fenixservices.fao.org/faostat/static/bulkdownloads/Value_of_Production_E_Africa.zip"
      
      zip_file_path <- file.path(fao_dir, "Value_of_Production_E_Africa.zip")
      
      # Download the file
      download.file(url, zip_file_path, mode = "wb")
      
      # Unzip the file
      unzip(zip_file_path, exdir = fao_dir)
      
      # Delete the ZIP file
      unlink(zip_file_path)
    }
    
    vop_file_world<-file.path(fao_dir,"Value_of_Production_E_All_Area_Groups.csv")
    if(!file.exists(vop_file_world)){
      # Define the URL and set the save path
      url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Value_of_Production_E_All_Area_Groups.zip"
      zip_file_path <- file.path(fao_dir, "Value_of_Production_E_All_Area_Groups.zip")
      
      # Download the file
      download.file(url, zip_file_path, mode = "wb")
      
      # Unzip the file
      unzip(zip_file_path, exdir = fao_dir)
      
      # Delete the ZIP file
      unlink(zip_file_path)
    }
    
    # 3.1.2) Prepare ######
    
    prod_value<-fread(vop_file)
    prod_value[grep(paste(lps2fao,collapse="|"),Item),unique(Item)]

    # use indigenous meat production values
    lps2fao_ind<-lps2fao
    lps2fao_ind[grep("Meat",lps2fao_ind)]<-paste0(lps2fao_ind[grep("Meat",lps2fao_ind)]," (indigenous)")
    lps2fao_ind %in% prod_value[Item %in% lps2fao_ind,unique(Item)]
    
    # Choose element
    prod_value[,unique(Element)]
    element<-"Gross Production Value (constant 2014-2016 thousand US$)"
    #element<-"Gross Production Value (constant 2014-2016 thousand I$)"
    # Note current thousand US$ has only 35 values whereas constant 12-16 has 157
    
    prod_value<-prepare_fao_data(file=vop_file,
                                 lps2fao_ind,
                                 elements=element,
                                 remove_countries = remove_countries,
                                 keep_years=target_year,
                                 atlas_iso3=atlas_iso3)
    
    prod_value[,atlas_name:=gsub(" (indigenous)","",atlas_name)]
    
    prod_value[!is.na(Y2017),.N]
    prod_value[!is.na(Y2015),.N]
    
    prod_value_world<-fread(vop_file_world)
    prod_value_world<-prod_value_world[Area=="World" & Element == element & Item %in% lps2fao_ind,list(Item,Y2016,Y2017,Y2018)]
    prod_value_world<-merge(prod_value_world,data.table(Item=lps2fao_ind,atlas_name=names(lps2fao_ind)),all.x=T)

  # 3.2) Price #####
    # 3.2.1) Download ######
    econ_file<-file.path(fao_dir,"Prices_E_Africa_NOFLAG.csv")
    
    if(!file.exists(econ_file)){
      # Define the URL and set the save path
      url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Prices_E_Africa.zip"
      zip_file_path <- file.path(fao_dir, "Prices_E_Africa.zip")
      
      # Download the file
      download.file(url, zip_file_path, mode = "wb")
      
      # Unzip the file
      unzip(zip_file_path, exdir = fao_dir)
      
      # Delete the ZIP file
      unlink(zip_file_path)
    }

    # 3.2.2) Prepare ######
    prod_price<-fread(econ_file)
    prod_price[grep("Meat of goat",Item),unique(Item)]
    prod_price[grep(paste(lps2fao,collapse="|"),Item),unique(Item)]
    
    prod_price[,unique(Element)]
    element<-"Producer Price (USD/tonne)"
    
    prod_price<-prepare_fao_data(file=econ_file,
                                 lps2fao,
                                 elements=element,
                                 remove_countries = remove_countries,
                                 keep_years=2016:2018,
                                 atlas_iso3=atlas_iso3)
    
    prod_price[,mean:=mean(c(Y2018,Y2017,Y2016),na.rm=T),by=list(iso3,atlas_name)]
    
    prod_price[!is.na(mean),.N]
    
    prod_price<-add_nearby(data=prod_price,value_field = "mean",neighbors=african_neighbors,regions)
    
  # 3.3) Production #####
    # 3.3.1) Download ######
    prod_file<-file.path(fao_dir,"Production_Crops_Livestock_E_Africa_NOFLAG.csv")
    
    if(!file.exists(prod_file)){
      # Define the URL and set the save path
      url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Production_Crops_Livestock_E_Africa.zip"
      zip_file_path <- file.path(fao_dir, "Production_E_Africa.zip")
      
      # Download the file
      download.file(url, zip_file_path, mode = "wb")
      
      # Unzip the file
      unzip(zip_file_path, exdir = fao_dir)
      
      # Delete the ZIP file
      unlink(zip_file_path)
    }
    
    prod_file_world<-file.path(fao_dir,"Production_Crops_Livestock_E_All_Area_Groups.csv")
    if(!file.exists(prod_file_world)){
      # Define the URL and set the save path
      url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Production_Crops_Livestock_E_All_Area_Groups.zip"
      zip_file_path <- file.path(fao_dir, "Production_Crops_Livestock_E_All_Area_Groups.zip")
      
      # Download the file
      download.file(url, zip_file_path, mode = "wb")
      
      # Unzip the file
      unzip(zip_file_path, exdir = fao_dir)
      
      # Delete the ZIP file
      unlink(zip_file_path)
    }

    # 3.3.2) Prepare ######
    prod<-fread(prod_file, encoding = "Latin-1")
    prod[grep(paste(lps2fao,collapse="|"),Item),unique(Item)]
    
    element<-"Production"
    prod<-prepare_fao_data(file=prod_file,
                           lps2fao,
                           elements=element,
                           units="t",
                           remove_countries = remove_countries,
                           keep_years=c(2016:2018),
                           atlas_iso3=atlas_iso3)
    
    prod[,prod_mean:=mean(c(Y2018,Y2017,Y2016),na.rm=T),by=list(iso3,atlas_name)]
    

    prod_world<-fread(prod_file_world)
    prod_world<-prod_world[Area=="World" & Element == element & Item %in% lps2fao & Unit=="t",list(Item,Y2016,Y2017,Y2018)]
    prod_world<-merge(prod_world,data.table(Item=lps2fao,atlas_name=names(lps2fao)),all.x=T)
    
  # 3.4) Deflators #####
    # 3.4.1) Download ######
    def_file<-file.path(fao_dir,"Deflators_E_All_Data_(Normalized).csv")
    
    if(!file.exists(def_file)){
      # Define the URL and set the save path
      url<-"https://fenixservices.fao.org/faostat/static/bulkdownloads/Deflators_E_All_Data_(Normalized).zip"
      
      zip_file_path <- file.path(fao_dir,basename(url))
      
      # Download the file
      download.file(url, zip_file_path, mode = "wb")
      
      # Unzip the file
      unzip(zip_file_path, exdir = fao_dir)
      
      # Delete the ZIP file
      unlink(zip_file_path)
    }
    # 3.4.2) Prepare ######
    # Prepare deflators data 
    deflators<-fread(def_file)
    deflators[,M49:=as.numeric(gsub("[']","",`Area Code (M49)`))]
    deflators[,iso3:=countrycode(sourcevar=M49,origin="un",destination = "iso3c")]
    deflators<-deflators[iso3 %in% atlas_iso3 & 
                           Year %in% target_year & 
                           Element == "Value US$, 2015 prices" &
                           Item == "Value Added Deflator (Agriculture, forestry and fishery)"
    ][,deflator:=Value]
    
    deflators<-deflators[,list(iso3,Year,deflator)][,Year:=paste0("D",Year)]
    deflators<-dcast(deflators,iso3~Year)
    setnames(deflators,paste0("D",target_year),c("def_past","def_target"))
    deflators[,def:=def_target/def_past][,c("def_past","def_target"):=NULL]
    
  # 4) Infer missing value from production and price data
  # 3.5) PPP ####
    ppp<- data.table(wbstats::wb_data("PA.NUS.PPP", country="countries_only"))
    setnames(ppp,"iso3c","iso3")
    
    ppp17<-ppp[date==2017 & iso3 %in% atlas_iso3,list(iso3,PA.NUS.PPP)]
    
    # SSD has no data for 2017, nearest is 2005
    ppp17[iso3=="SSD",PA.NUS.PPP:= ppp[iso3 == "SSD" & date==2015,PA.NUS.PPP]]
    
    ppp15<-ppp[date==2015 & iso3 %in% atlas_iso3,list(iso3,PA.NUS.PPP)]
    
  # 4.1) Merge production and national or nearby price ####
  prod_price2<-prod_price[,list(iso3,atlas_name,mean,mean_neighbors,mean_region,mean_continent,mean_final)]
  
  setnames(prod_price2,"mean_final","price_mean")
  
  prod<-prod[,list(iso3,atlas_name,prod_mean)]
  
  prod<-merge(prod,prod_price2,all.x=T)
  prod[,VoP:=prod_mean*price_mean]
  
  # Deflate USD to 2015
  prod<-merge(prod,deflators,all.x=T)
  prod[,VoP15:=VoP/def][,VoP_est:=round(VoP15/1000,0)]
  
  # Merge VoP estimate from price and production with faostat vop
  prod_value<-merge(prod_value,prod[,list(iso3,atlas_name,def,VoP_est)],all.x=T)
  
  # 4.2) Using World VoP - merge  production and price ####
  
  # Unit is tonnes
  prod_world<-prod_world[,list(atlas_name,Y2017)]
  setnames(prod_world,"Y2017","production")
  
  # Unit is $1000
  prod_value_world<-prod_value_world[,list(atlas_name,Y2017)]
  setnames(prod_value_world,"Y2017","value")
  
  price_world<-merge(prod_world,prod_value_world,all.x=T)
  
  # Unit is $1000 per tonne (constant USD 2014-2016)
  price_world[,price_world:=value/production]
  
  # Add production to value table
  prod_value<-merge(prod_value,prod[,list(iso3,atlas_name,prod_mean)],all.x=T,by=c("iso3","atlas_name"))
  
  # Add global vop to value table
  prod_value<-merge(prod_value,price_world[,list(atlas_name,price_world)],all.x=T,by=c("atlas_name"))
  
  # Estimate value 
  prod_value[,VoP_world:=prod_mean*price_world]
  
  # 4.3) Hybrid ####
  prod_price2<-prod_price[,list(iso3,atlas_name,mean,mean_neighbors,mean_region,mean_continent)
                          ][,mean_final:=mean][is.na(mean_final),mean_final:=mean_neighbors]
  
  setnames(prod_price2,"mean_final","price_mean_n")
  
  prod_value<-merge(prod_value,prod_price2,all.x=T,by=c("iso3","atlas_name"))
  prod_value[,VoP_hybrid:=(price_mean_n*prod_mean)/def/1000][is.na(VoP_hybrid),VoP_hybrid:=VoP_world]

  # 4.4) Which estimation approach is better? ####
  
  max_y<-prod_value[,max(max(VoP_est,na.rm=T),max(VoP_world,na.rm=T),max(VoP_hybrid,na.rm=T))]

  #VoP_est
  model<-lm(Y2017~VoP_est,prod_value)
  plot(prod_value$Y2017, prod_value$VoP_est, main = "VoP_est",xlab = "FAO", ylab = "Estimated", pch = 19, col = "red",ylim=c(0,max_y))
  abline(model, col = "blue")
  
  # VoP_world
  model<-lm(Y2017~VoP_world,prod_value)
  plot(prod_value$Y2017, prod_value$VoP_world, main = "VoP_world",xlab = "FAO", ylab = "Estimated", pch = 19, col = "red",ylim=c(0,max_y))
  abline(model, col = "blue")
  
  # VoP_hybrid
  model<-lm(Y2017~VoP_hybrid,prod_value)
  plot(prod_value$Y2017, prod_value$VoP_hybrid, main = "VoP_hybrid",xlab = "FAO", ylab = "Estimated", pch = 19, col = "red",ylim=c(0,max_y))
  abline(model, col = "blue")

  rbind(
  prod_value[,list(source="VoP_est",
                   mean_prop=mean(Y2017/VoP_est,na.rm=T),
                   mean_sd=sd(Y2017/VoP_est,na.rm=T),
                   lm.adj.r.squared=summary(lm(Y2017~VoP_est,prod_value))$adj.r.squared)
             ][,cv:=mean_sd/mean_prop],
  
  prod_value[,list(source="VoP_world",
                   mean_prop=mean(Y2017/VoP_world,na.rm=T),
                   mean_sd=sd(Y2017/VoP_world,na.rm=T),
                   lm.adj.r.squared=summary(lm(Y2017~VoP_world,prod_value))$adj.r.squared)
             ][,cv:=mean_sd/mean_prop],
  
  prod_value[,list(source="VoP_hybrid",
                   mean_prop=mean(Y2017/VoP_hybrid,na.rm=T),
                   mean_sd=sd(Y2017/VoP_hybrid,na.rm=T),
                   lm.adj.r.squared=summary(lm(Y2017~VoP_hybrid,prod_value))$adj.r.squared)  
             ][,cv:=mean_sd/mean_prop]
  )
  
# 5) Distribute vop to GLW4 livestock ####
  
  glw_admin0<-extract(glw,geoboundaries,fun="sum",na.rm=T)
  glw_admin0$iso3<-geoboundaries$iso3
  glw_admin0$ID<-NULL
  glw_admin0<-melt(glw_admin0,id.vars="iso3",variable.name = "glw3_name",value.name="glw3_no")
  
  glw2atlas<-list(poultry=grep("poultry",names(lps2fao ),value=T),
                  sheep=grep("sheep",names(lps2fao ),value=T),
                  pigs=grep("pig",names(lps2fao ),value=T),
                  goats=grep("goat",names(lps2fao ),value=T),
                  cattle=grep("cattle",names(lps2fao ),value=T))
  
  glw2atlas <- data.table(
    glw3_name = rep(names(glw2atlas), sapply(glw2atlas, length)),
    atlas_name = unlist(glw2atlas, use.names = FALSE)
  )
  
  final_vop<-prod_value[,list(iso3,atlas_name,Y2017,VoP_hybrid)][is.na(Y2017),Y2017:=VoP_hybrid][,VoP_hybrid:=NULL]
  final_vop<-merge(final_vop,glw2atlas,all.x=T,by="atlas_name")
  
  # Where there is missing data explore how many animals are in these areas
  final_vop_merge<-merge(final_vop,glw_admin0,all.x=T,by=c("iso3","glw3_name"))
  final_vop_merge[is.na(Y2017) & !grepl("goat_milk|sheep_milk",atlas_name)]
  
  # Sum values for glw classes
  final_vop<-final_vop[,list(value=sum(Y2017,na.rm=T)),by=list(glw3_name,iso3)]
  final_vop<-dcast(final_vop,iso3~glw3_name)
  
  # Convert value to vector then raster
  final_vop_vect<-geoboundaries
  final_vop_vect<-merge(final_vop_vect,final_vop,all.x=T)
  
  final_vop_rast<-terra::rast(lapply(unique(glw2atlas$glw3_name),FUN=function(NAME){
    terra::rasterize(final_vop_vect,base_rast,field=NAME)
  }))
  names(final_vop_rast)<-unique(glw2atlas$glw3_name)
  
  # Convert glw3 pixels to proportion of national total
  glw_admin0<-dcast(glw_admin0,iso3~glw3_name,value.var="glw3_no")
  glw_vect<-geoboundaries
  glw_vect<-merge(glw_vect,glw_admin0,all.x=T,by="iso3")
  
  glw_rast<-terra::rast(lapply(unique(glw2atlas$glw3_name),FUN=function(NAME){
    terra::rasterize(glw_vect,base_rast,field=NAME)
  }))
  
  names(glw_rast)<-unique(glw2atlas$glw3_name)
  
  glw_prop<-glw/glw_rast
  
  # Multiply national VoP by cell proportion
  glw_vop<-glw_prop*final_vop_rast
  
  terra::writeRaster(round(glw_vop*1000,0),file.path(ls_vop_dir,"livestock-vop-2017-cusd15.tif"),overwrite=T)
  
  # 5.1) Convert to IUSD and USD 2017####
  # vectorise ppp & deflators
  deflators_vect<-geoboundaries
  deflators_vect<-merge(deflators_vect,deflators,all.x=T,by="iso3")
  deflators_rast<-terra::rasterize(deflators_vect,base_rast,field="def")  
  
  ppp17_vect<-geoboundaries
  ppp17_vect<-merge(ppp17_vect,ppp17,all.x=T,by="iso3")
  ppp17_rast<-terra::rasterize(ppp17_vect,base_rast,field="PA.NUS.PPP")  
  
  ppp15_vect<-geoboundaries
  ppp15_vect<-merge(ppp15_vect,ppp15,all.x=T,by="iso3")
  ppp15_rast<-terra::rasterize(ppp15_vect,base_rast,field="PA.NUS.PPP")  
  
  # vop constant usd 14-16 x ppp2015
  glw_vop_intd15<-glw_vop*ppp15_rast
  terra::writeRaster(round(glw_vop_intd15*1000,0),file.path(ls_vop_dir,"livestock-vop-2017-intd15.tif"),overwrite=T)
  
  # current usd 2017
  glw_vop_usd17<-glw_vop*deflators_rast
  terra::writeRaster(round(glw_vop_usd17*1000,0),file.path(ls_vop_dir,"livestock-vop-2017-usd17.tif"),overwrite=T)
  
  # 6) QAQC: Extract by admin0 and compare back to FAOstat ####
  
  # GLW3 distributed data
  data<-glw_vop
  
  qa_rasterizer<-function(data,base_rast,geoboundaries,glw2atlas){
    # constant usd 2014-2016
    data_ex<-extract(data,geoboundaries,fun="sum",na.rm=T)
    data_ex$ID<-NULL
    names(data_ex)<-paste0(names(data_ex))
    data_ex$iso3<-geoboundaries$iso3
    
    
    data_ex_vect<-geoboundaries
    data_ex_vect<-merge(data_ex_vect,data_ex,all.x=T,by="iso3")
    
    data_ex_rast<-terra::rast(lapply(unique(glw2atlas$glw3_name),FUN=function(NAME){
      terra::rasterize(data_ex_vect,base_rast,field=NAME)
    }))
    
    return(list(table=data_ex,vect=data_ex_vect,rast=data_ex_rast))
  }
  
  glw_vop_cusd15_adm0<-qa_rasterizer(data=glw_vop,base_rast,geoboundaries,glw2atlas)$rast
  glw_vop_int15_adm0<-qa_rasterizer(data=glw_vop_intd15,base_rast,geoboundaries,glw2atlas)$rast
  glw_vop_usd17_adm0<-qa_rasterizer(data=glw_vop_usd17,base_rast,geoboundaries,glw2atlas)$rast
  
  # FAOstat data
  fao_vop_cusd15<-prod_value[,list(Y2017,iso3,atlas_name)]
  fao_vop_cusd15<-merge(fao_vop_cusd15,glw2atlas,all.x=T,by="atlas_name")
  fao_vop_cusd15<-fao_vop_cusd15[,list(value=sum(Y2017,na.rm=T),N=sum(is.na(Y2017))),by=list(glw3_name,iso3)]
  fao_vop_cusd15<-fao_vop_cusd15[N==0]
  
  fao_vop_cusd15<-dcast(fao_vop_cusd15,iso3~glw3_name)
  fao_vop_cusd15_vect<-geoboundaries
  fao_vop_cusd15_vect<-merge(fao_vop_cusd15_vect,fao_vop_cusd15,all.x=T,by="iso3")
  
  fao_vop_cusd15_rast<-terra::rast(lapply(unique(glw2atlas$glw3_name),FUN=function(NAME){
    terra::rasterize(fao_vop_cusd15_vect,base_rast,field=NAME)
  }))
  
  # Constant USD 2017
  
  # use indigenous meat production values
  lps2fao_ind<-lps2fao
  lps2fao_ind[grep("Meat",lps2fao_ind)]<-paste0(lps2fao_ind[grep("Meat",lps2fao_ind)]," (indigenous)")

  element<-"Gross Production Value (current thousand US$)" 
  # Note current thousand US$ has only 35 values whereas constant 12-16 has 157
  
  fao_vop_usd17<-prepare_fao_data(file=vop_file,
                               lps2fao_ind,
                               elements=element,
                               remove_countries = remove_countries,
                               keep_years=target_year,
                               atlas_iso3=atlas_iso3)
  
  fao_vop_usd17[,atlas_name:=gsub(" (indigenous)","",atlas_name)]

  fao_vop_usd17<-fao_vop_usd17[,list(Y2017,iso3,atlas_name)]
  fao_vop_usd17<-merge(fao_vop_usd17,glw2atlas,all.x=T,by="atlas_name")
  fao_vop_usd17<-fao_vop_usd17[,list(value=sum(Y2017,na.rm=T),N=sum(is.na(Y2017))),by=list(glw3_name,iso3)]
  fao_vop_usd17<-fao_vop_usd17[N==0]
  
  fao_vop_usd17<-dcast(fao_vop_usd17,iso3~glw3_name)
  fao_vop_usd17_vect<-geoboundaries
  fao_vop_usd17_vect<-merge(fao_vop_usd17_vect,fao_vop_usd17,all.x=T,by="iso3")
  
  NAMES<-unique(glw2atlas$glw3_name)
  NAMES<-NAMES[NAMES %in% names(fao_vop_usd17_vect)]
  fao_vop_usd17_rast<-terra::rast(lapply(NAMES,FUN=function(NAME){
    terra::rasterize(fao_vop_usd17_vect,base_rast,field=NAME)
  }))
  
  # Comparisons
  plot(glw_vop_cusd15_adm0/fao_vop_cusd15_rast)
  
  # Differences are due to where only one of meat or milk is present.

  glw_vop_cusd15_tab<-qa_rasterizer(data=glw_vop,base_rast,geoboundaries,glw2atlas)$table
  glw_vop_cusd15_tab<-melt(glw_vop_cusd15_tab,id.vars=c("iso3"),variable.name="glw3_name",value.name="VoP_ext")
  
  data<-prod_value[,list(iso3,atlas_name,Y2017,VoP_world,VoP_hybrid)]
  
  setnames(data,c("Y2017","VoP_world","VoP_hybrid"),c("FAOstat","VoP_est_world","VoP_est_hybrid"))
  data[,Unit:="Gross Production Value (constant 2014-2016 thousand US$)"][,Year:=2017]
  data<-merge(data,glw2atlas,all.x=T,by="atlas_name")
  
  fwrite(data,file.path(ls_vop_dir,"QAQC_2017_cusd15_full.csv"))
  
  data2<-data[,list(FAOstat=sum(FAOstat,na.rm=T),n_na=sum(is.na(FAOstat)),VoP_est_world=sum(VoP_est_world ,na.rm=T),VoP_est_hybrid=sum(VoP_est_hybrid,na.rm=T)),by=list(iso3,glw3_name,Unit,Year)]
  data2<-merge(data2,glw_vop_cusd15_tab,all.x=T)
  
  fwrite(data2,file.path(ls_vop_dir,"QAQC_2017_cusd15_glw.csv"))
  