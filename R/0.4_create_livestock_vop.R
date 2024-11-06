# a) Load R functions & packages ####
pacman::p_load(terra,data.table,httr,countrycode,wbstats,arrow,geoarrow,ggplot2,dplyr,tidyr)

# Load functions & wrappers
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))
options(scipen=999)

# b) Set up workspace ####
# Load base raster to resample to 
base_raster<-"base_raster.tif"
if(!file.exists(base_raster)){
url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/base_raster.tif"
httr::GET(url, write_disk(base_raster, overwrite = TRUE))
}

base_rast<-terra::rast(base_raster)

# 1) Load geographies ####
file<-geo_files_local[1]
geoboundaries<-arrow::open_dataset(file)
geoboundaries <- geoboundaries |> sf::st_as_sf() |> terra::vect()

# 2) Load GLW4 data ####
# Note that GLW4 data is for the year 2015
# file suffix _da =  dysymmetric, unit = total animals per pixel
glw_names<-c(poultry="Ch",sheep="Sh",pigs="Pg",horses="Ho",goats="Gt",ducks="Dk",buffalo="Bf",cattle="Ct")
glw_codes<-c(poultry=6786792,sheep=6769626,pigs=6769654,horses=6769681,goats=6769696,ducks=6769700,buffalo=6770179,cattle=6769711)

glw_files <- file.path(glw_dir,paste0("5_",glw_names,"_2015_Da.tif"))
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

  # 2.2) Extract by admin0 #####
  glw_admin0<-terra::extract(glw,geoboundaries,fun="sum",na.rm=T)
  glw_admin0$iso3<-geoboundaries$iso3
  glw_admin0$ID<-NULL
  glw_admin0<-melt(glw_admin0,id.vars="iso3",variable.name = "glw3_name",value.name="glw3_no")
  
  # 2.3) map GLW values to atlas #####
  glw2atlas<-list(poultry=grep("poultry",names(lps2fao ),value=T),
                  sheep=grep("sheep",names(lps2fao ),value=T),
                  pigs=grep("pig",names(lps2fao ),value=T),
                  goats=grep("goat",names(lps2fao ),value=T),
                  cattle=grep("cattle",names(lps2fao ),value=T))
  
  glw2atlas <- data.table(
    glw3_name = rep(names(glw2atlas), sapply(glw2atlas, length)),
    atlas_name = unlist(glw2atlas, use.names = FALSE)
  )

# 3) Load FAOstat data ####
remove_countries<- c("Ethiopia PDR","Sudan (former)","Cabo Verde","Comoros","Mauritius","R\xe9union","Seychelles")
atlas_iso3<-geoboundaries$iso3
target_year<-c(2015,2017)

  # 3.1) VoP #####
  vop_file<-file.path(fao_dir,"Value_of_Production_E_Africa.csv")
  vop_file_world<-file.path(fao_dir,"Value_of_Production_E_All_Area_Groups.csv")
  
  # use indigenous meat production values
  lps2fao_ind<-lps2fao
  lps2fao_ind[grep("Meat",lps2fao_ind)]<-paste0(lps2fao_ind[grep("Meat",lps2fao_ind)]," (indigenous)")

    # 3.1.1) VoP - constant 2014-16 $US#####
    prod_value<-fread(vop_file)
    prod_value[grep(paste(lps2fao,collapse="|"),Item),unique(Item)]

    # Choose element
    prod_value[,unique(Element)]
    element<-"Gross Production Value (constant 2014-2016 thousand US$)"
    # Note current thousand US$ has only 35 values whereas constant 12-16 has 157
    
    prod_value_usd<-prepare_fao_data(file=vop_file,
                                 lps2fao_ind,
                                 elements=element,
                                 remove_countries = remove_countries,
                                 keep_years=target_year,
                                 atlas_iso3=atlas_iso3)
    
    prod_value_usd[,atlas_name:=gsub(" (indigenous)","",atlas_name)]
    
    prod_value_usd[!is.na(Y2017),.N]
    prod_value_usd[!is.na(Y2015),.N]
    
    prod_value_usd_world<-fread(vop_file_world)
    prod_value_usd_world<-prod_value_usd_world[Area=="World" & Element == element & Item %in% lps2fao_ind,.(Item,Y2014,Y2015,Y2016)]
    prod_value_usd_world<-merge(prod_value_usd_world,data.table(Item=lps2fao_ind,atlas_name=names(lps2fao_ind)),all.x=T)
   
    # 3.1.2) VoP - constant 2014-16 $I #####
    prod_value_i<-fread(vop_file)
    prod_value_i[grep(paste(lps2fao,collapse="|"),Item),unique(Item)]
    
    # Choose element
    prod_value_i[,unique(Element)]
    element<-"Gross Production Value (constant 2014-2016 thousand I$)"
    # Note current thousand US$ has only 35 values whereas constant 12-16 has 157
    
    prod_value_i<-prepare_fao_data(file=vop_file,
                                 lps2fao_ind,
                                 elements=element,
                                 remove_countries = remove_countries,
                                 keep_years=target_year,
                                 atlas_iso3=atlas_iso3)
    
    prod_value_i[,atlas_name:=gsub(" (indigenous)","",atlas_name)]
    
    prod_value_i[!is.na(Y2017),.N]
    prod_value_i[!is.na(Y2015),.N]
    
    prod_value_i_world<-fread(vop_file_world)
    prod_value_i_world<-prod_value_i_world[Area=="World" & Element == element & Item %in% lps2fao_ind,list(Item,Y2014,Y2015,Y2016)]
    prod_value_i_world<-merge(prod_value_i_world,data.table(Item=lps2fao_ind,atlas_name=names(lps2fao_ind)),all.x=T)
    
  # 3.2) Price #####
    econ_file<-file.path(fao_dir,"Prices_E_Africa_NOFLAG.csv")
    prod_price<-fread(econ_file)
    prod_price[grep("Meat of goat",Item),unique(Item)]
    prod_price[grep(paste(lps2fao,collapse="|"),Item),unique(Item)]
    
    prod_price[,unique(Element)]
    element<-"Producer Price (USD/tonne)"
    
    prod_price<-prepare_fao_data(file=econ_file,
                                 lps2fao,
                                 elements=element,
                                 remove_countries = remove_countries,
                                 keep_years=2014:2016,
                                 atlas_iso3=atlas_iso3)
    
    prod_price[,mean:=mean(c(Y2016,Y2015,Y2014),na.rm=T),by=list(iso3,atlas_name)]
    
    prod_price[!is.na(mean),.N]
    
    prod_price<-add_nearby(data=prod_price,group_field="atlas_name",value_field = "mean",neighbors=african_neighbors,regions)
    
  # 3.3) Production #####
    prod_file<-file.path(fao_dir,"Production_Crops_Livestock_E_Africa_NOFLAG.csv")
    prod_file_world<-file.path(fao_dir,"Production_Crops_Livestock_E_All_Area_Groups.csv")
    
    prod<-fread(prod_file, encoding = "Latin-1")
    prod[grep(paste(lps2fao,collapse="|"),Item),unique(Item)]
    
    element<-"Production"
    prod<-prepare_fao_data(file=prod_file,
                           lps2fao,
                           elements=element,
                           units="t",
                           remove_countries = remove_countries,
                           keep_years=c(2014:2016),
                           atlas_iso3=atlas_iso3)
    
    prod[,prod_mean:=mean(c(Y2014,Y2015,Y2016),na.rm=T),by=list(iso3,atlas_name)]
    

    prod_world<-fread(prod_file_world)
    prod_world<-prod_world[Area=="World" & Element == element & Item %in% lps2fao & Unit=="t",.(Item,Y2014,Y2015,Y2016)]
    prod_world<-merge(prod_world,data.table(Item=lps2fao,atlas_name=names(lps2fao)),all.x=T)
    
  # 3.4) PPP ####
    indicators <- data.table(wb_search(pattern = "PPP"))
    print(indicators[grepl("conversion",indicator) & grepl("PP",indicator_id),indicator])
    
    # LCU per I$
    ppp<- data.table(wbstats::wb_data("PA.NUS.PPP", country="countries_only",start_date = 2015,end_date=2015))
    setnames(ppp,"iso3c","iso3")
    
    ppp<-ppp[iso3 %in% atlas_iso3,list(iso3,PA.NUS.PPP)]
    colnames(ppp)[2]<-"ppp"
    
    # Retrieve exchange rate data (LCU per USD) for 2015
    xrat <- data.table(wb_data(
      indicator = "PA.NUS.FCRF",
      country="countries_only",
      start_date = 2015,
      end_date = 2015
    ))
    setnames(xrat,"iso3c","iso3")
    
    xrat<-xrat[iso3 %in% atlas_iso3,list(iso3,PA.NUS.FCRF)]
    colnames(xrat)[2]<-"xrat"
    
    ppp_xrat<-merge(ppp,xrat,all.x=T)
    ppp_xrat[iso3=="ZWE",xrat:=1]
    
    # To get from usd to iusd will will have to multiply usd by xrat and divide by ppp
    ppp_xrat[,ppp_cf:=xrat/ppp]
    
    # Liberia (LBR) has a weird ppp value, substitute Sierra Leone
    val<-as.numeric(ppp_xrat[iso3=="SLE",ppp_cf])
    ppp_xrat[iso3=="LBR",ppp_cf:=val]
    
  # 4) Infer missing value from production and price data ####
    # 4.1) Merge production and national or nearby price #####
  prod_price2<-prod_price[,list(iso3,atlas_name,mean,mean_neighbors,mean_region,mean_continent,mean_final)]
  colnames(prod_price2)<-gsub("mean","price_mean",colnames(prod_price2))

  prod<-prod[,list(iso3,atlas_name,prod_mean)]
  
  prod<-merge(prod,prod_price2,all.x=T)
  prod[,VoP:=round(prod_mean*price_mean/1000,0)
       ][,VoP_neighbors:=round(prod_mean*price_mean_neighbors/1000,0)
         ][,VoP_region:=round(prod_mean*price_mean_region/1000,0)
           ][,VoP_continent:=round(prod_mean*price_mean_continent/1000,0)]
  
  if(F){
  # Deflate USD to 2015 (only needed if we are not looking at 2015)
  prod<-merge(prod,deflators,all.x=T)
  prod[,VoP15:=VoP/def][,VoP_est:=round(VoP15/1000,0)]
  }
  
  # Merge VoP estimate from price and production with faostat vop
  prod_value_usd<-merge(prod_value_usd,
                        prod[,list(iso3,atlas_name,VoP,VoP_neighbors,VoP_region,VoP_continent)],
                        by=c("iso3","atlas_name"),
                        all.x=T)

    # 4.2) Using World VoP - merge  production and price ####
    # Unit is tonnes
    prod_world<-prod_world[,list(atlas_name,Y2015)]
    setnames(prod_world,"Y2015","production")
    
    # Unit is $1000
    prod_value_world_usd<-prod_value_usd_world[,list(atlas_name,Y2015)]
    setnames(prod_value_world_usd,"Y2015","value")
    
    price_world<-merge(prod_world,prod_value_world_usd,all.x=T)
    
    # Unit is $1000 per tonne (constant USD 2014-2016)
    price_world[,price_world:=value/production]
    
    # Add production to value table
    prod_value_usd<-merge(prod_value_usd,prod[,list(iso3,atlas_name,prod_mean)],all.x=T,by=c("iso3","atlas_name"))
    
    # Add global vop to value table
    prod_value_usd<-merge(prod_value_usd,price_world[,list(atlas_name,price_world)],all.x=T,by=c("atlas_name"))
    
    # Estimate value 
    prod_value_usd<-prod_value_usd[,VoP_world:=prod_mean*price_world][,!c("prod_mean","price_world")]
    
    # 4.3) Hybrid ####
    prod_value_usd[,VoP_hybrid:=VoP
                   ][is.na(VoP_hybrid),VoP_hybrid:=VoP_neighbors
                     ][is.na(VoP_hybrid),VoP_hybrid:=VoP_region
                       ][is.na(VoP_hybrid),VoP_hybrid:=VoP_continent]
    
    prod_value_usd[,VoP_hybrid2:=VoP
    ][is.na(VoP_hybrid2),VoP_hybrid2:=VoP_world]
    
    prod_value_usd[,VoP_hybrid3:=VoP
    ][is.na(VoP_hybrid3),VoP_hybrid3:=VoP_continent]
    
  
    # 4.4) Which $US estimation approach is better? ####
    
    max_y<-prod_value_usd[,max(max(VoP,na.rm=T),max(VoP_world,na.rm=T),max(VoP_hybrid,na.rm=T))]
  
    par(mfcol = c(2, 3))  # 3 rows, 1 column
    
    #VoP
    model<-lm(Y2015~VoP,prod_value_usd)
    plot(prod_value_usd$Y2015, prod_value_usd$VoP, main = "VoP",xlab = "FAO", ylab = "Estimated", pch = 19, col = "red",ylim=c(0,max_y))
    abline(a = 0, b = 1,, col = "blue")
    
    # VoP_neighbors
    model<-lm(Y2015~VoP_neighbors,prod_value_usd)
    plot(prod_value_usd$Y2015, prod_value_usd$VoP_neighbors, main = "VoP_neighbors",xlab = "FAO", ylab = "Estimated", pch = 19, col = "red",ylim=c(0,max_y))
    abline(a = 0, b = 1,, col = "blue")
    
    # VoP_region
    model<-lm(Y2015~VoP_region,prod_value_usd)
    plot(prod_value_usd$Y2015, prod_value_usd$VoP_region, main = "VoP_region",xlab = "FAO", ylab = "Estimated", pch = 19, col = "red",ylim=c(0,max_y))
    abline(a = 0, b = 1,, col = "blue")
    
    # VoP_continent
    model<-lm(Y2015~VoP_continent,prod_value_usd)
    plot(prod_value_usd$Y2015, prod_value_usd$VoP_continent, main = "VoP_continent",xlab = "FAO", ylab = "Estimated", pch = 19, col = "red",ylim=c(0,max_y))
    abline(a = 0, b = 1,, col = "blue")
    
    # VoP_world
    model<-lm(Y2015~VoP_world,prod_value_usd)
    plot(prod_value_usd$Y2015, prod_value_usd$VoP_world, main = "VoP_world",xlab = "FAO", ylab = "Estimated", pch = 19, col = "red",ylim=c(0,max_y))
    abline(a = 0, b = 1,, col = "blue")
    
    # VoP_hybrid
    model<-lm(Y2015~VoP_hybrid,prod_value_usd)
    plot(prod_value_usd$Y2015, prod_value_usd$VoP_hybrid, main = "VoP_hybrid",xlab = "FAO", ylab = "Estimated", pch = 19, col = "red",ylim=c(0,max_y))
    abline(a = 0, b = 1,, col = "blue")
    
    rbind(
      prod_value_usd[!is.na(Y2015) & !is.na(VoP), .(
        source = "VoP",
        n = length(na.omit(Y2015 / VoP)),
        mean_prop = mean(Y2015 / VoP, na.rm = TRUE),
        mean_sd = sd(Y2015 / VoP, na.rm = TRUE),
        lm.adj.r.squared = summary(lm(Y2015 ~ VoP, prod_value_usd))$adj.r.squared,
        rmse = sqrt(mean((Y2015 - predict(lm(Y2015 ~ VoP, prod_value_usd)))^2, na.rm = TRUE)),  # RMSE calculation
        mae = mean(abs(Y2015 - predict(lm(Y2015 ~ VoP))))  # Adding RMSE calculation
      )][, cv := mean_sd / mean_prop],
      
      prod_value_usd[!is.na(Y2015) & !is.na(VoP_neighbors),list(source="VoP_neighbors",
                           n=length(na.omit(Y2015/VoP_neighbors)),
                           mean_prop=mean(Y2015/VoP_neighbors,na.rm=T),
                           mean_sd=sd(Y2015/VoP_neighbors,na.rm=T),
                           lm.adj.r.squared=summary(lm(Y2015~VoP_neighbors,prod_value_usd))$adj.r.squared,
                           rmse = sqrt(mean((Y2015 - predict(lm(Y2015 ~ VoP_neighbors, prod_value_usd)))^2, na.rm = TRUE)),  # RMSE calculation
                           mae = mean(abs(Y2015 - predict(lm(Y2015 ~ VoP_neighbors)))))
      ][,cv:=mean_sd/mean_prop],
      
      prod_value_usd[!is.na(Y2015) & !is.na(VoP_region),list(source="VoP_region",
                           n=length(na.omit(Y2015/VoP_region)),
                           mean_prop=mean(Y2015/VoP_region,na.rm=T),
                           mean_sd=sd(Y2015/VoP_region,na.rm=T),
                           lm.adj.r.squared=summary(lm(Y2015~VoP_region,prod_value_usd))$adj.r.squared,
                           rmse = sqrt(mean((Y2015 - predict(lm(Y2015 ~ VoP_region, prod_value_usd)))^2, na.rm = TRUE)),  # RMSE calculation
                           mae = mean(abs(Y2015 - predict(lm(Y2015 ~ VoP_region)))))
      ][,cv:=mean_sd/mean_prop],
      
      prod_value_usd[!is.na(Y2015) & !is.na(VoP_continent),list(source="VoP_continent",
                           n=length(na.omit(Y2015/VoP_continent)),
                           mean_prop=mean(Y2015/VoP_continent,na.rm=T),
                           mean_sd=sd(Y2015/VoP_continent,na.rm=T),
                           lm.adj.r.squared=summary(lm(Y2015~VoP_continent,prod_value_usd))$adj.r.squared,
                           rmse = sqrt(mean((Y2015 - predict(lm(Y2015 ~ VoP_continent, prod_value_usd)))^2, na.rm = TRUE)),  # RMSE calculation
                           mae = mean(abs(Y2015 - predict(lm(Y2015 ~ VoP_continent)))))
      ][,cv:=mean_sd/mean_prop],
    
      prod_value_usd[!is.na(Y2015) & !is.na(VoP_world),list(source="VoP_world",
                     n=length(na.omit(Y2015/VoP_world)),
                     mean_prop=mean(Y2015/VoP_world,na.rm=T),
                     mean_sd=sd(Y2015/VoP_world,na.rm=T),
                     lm.adj.r.squared=summary(lm(Y2015~VoP_world,prod_value_usd))$adj.r.squared,
                     rmse = sqrt(mean((Y2015 - predict(lm(Y2015 ~ VoP_world, prod_value_usd)))^2, na.rm = TRUE)),  # RMSE calculation
                                 mae = mean(abs(Y2015 - predict(lm(Y2015 ~ VoP_world)))))
               ][,cv:=mean_sd/mean_prop],
    
      prod_value_usd[!is.na(Y2015) & !is.na(VoP_hybrid),list(source="VoP_hybrid",
                     n=length(na.omit(Y2015/VoP_hybrid)),
                     mean_prop=mean(Y2015/VoP_hybrid,na.rm=T),
                     mean_sd=sd(Y2015/VoP_hybrid,na.rm=T),
                     lm.adj.r.squared=summary(lm(Y2015~VoP_hybrid,prod_value_usd))$adj.r.squared,
                     rmse = sqrt(mean((Y2015 - predict(lm(Y2015 ~ VoP_hybrid, prod_value_usd)))^2, na.rm = TRUE)),  # RMSE calculation
                     mae = mean(abs(Y2015 - predict(lm(Y2015 ~ VoP_hybrid)))))  
               ][,cv:=mean_sd/mean_prop],
      
      prod_value_usd[!is.na(Y2015) & !is.na(VoP_hybrid2),list(source="VoP_hybrid2",
                                                             n=length(na.omit(Y2015/VoP_hybrid2)),
                                                             mean_prop=mean(Y2015/VoP_hybrid2,na.rm=T),
                                                             mean_sd=sd(Y2015/VoP_hybrid2,na.rm=T),
                                                             lm.adj.r.squared=summary(lm(Y2015~VoP_hybrid2,prod_value_usd))$adj.r.squared,
                                                             rmse = sqrt(mean((Y2015 - predict(lm(Y2015 ~ VoP_hybrid2, prod_value_usd)))^2, na.rm = TRUE)),  # RMSE calculation
                                                             mae = mean(abs(Y2015 - predict(lm(Y2015 ~ VoP_hybrid2)))))  
      ][,cv:=mean_sd/mean_prop],
      
      prod_value_usd[!is.na(Y2015) & !is.na(VoP_hybrid3),list(source="VoP_hybrid3",
                                                             n=length(na.omit(Y2015/VoP_hybrid3)),
                                                             mean_prop=mean(Y2015/VoP_hybrid3,na.rm=T),
                                                             mean_sd=sd(Y2015/VoP_hybrid3,na.rm=T),
                                                             lm.adj.r.squared=summary(lm(Y2015~VoP_hybrid3,prod_value_usd))$adj.r.squared,
                                                             rmse = sqrt(mean((Y2015 - predict(lm(Y2015 ~ VoP_hybrid3, prod_value_usd)))^2, na.rm = TRUE)),  # RMSE calculation
                                                             mae = mean(abs(Y2015 - predict(lm(Y2015 ~ VoP_hybrid3)))))  
      ][,cv:=mean_sd/mean_prop]
    )
    
    # 4.5) Explore per animal values (does not work well!) #####
      # 4.5.1) $I per animal ######
      # add glw names to faostat table
      prod_value_i_glw3<-merge(prod_value_i,glw2atlas,all.x=T)
      prod_value_i_glw3<-prod_value_i_glw3[,.(id_tot=1000*sum(Y2015,na.rm=T)),by=.(iso3,glw3_name)]
      glw_admin0_val<-data.table(merge(glw_admin0,prod_value_i_glw3,all.x=T))
      glw_admin0_val[,id_n:=id_tot/glw3_no][id_n==0,id_n:=NA]
      
      # 4.5.2) $US per animal ######
      # add glw names to faostat table
      prod_value_usd_glw3<-merge(prod_value_usd[,.(iso3,atlas_name,Y2015)],glw2atlas,by="atlas_name",all.x=T)
      prod_value_usd_glw3<-prod_value_usd_glw3[,.(cusd_tot=1000*sum(Y2015,na.rm=T)),by=.(iso3,glw3_name)]
      glw_admin0_val<-data.table(merge(glw_admin0_val,prod_value_usd_glw3,by=c("iso3","glw3_name"),all.x=T))
      glw_admin0_val[,cusd_n:=cusd_tot/glw3_no][cusd_n==0,cusd_n:=NA]
      
      # 4.5.3) Compare $I and $US data availability ######
      glw_admin0_val[,.(id=length(na.omit(id_n)),cusd=length(na.omit(cusd_n)))]
      
      ggplot(glw_admin0_val, aes(x = id_n)) +
        geom_histogram(binwidth = 10, fill = "steelblue", color = "black", alpha = 0.7) +
        facet_wrap(~ glw3_name, scales = "free") +
        labs(x = "id_n", y = "Count", title = "Faceted Histogram of id_n by Animal Type") +
        theme_minimal()
      
      ggplot(glw_admin0_val, aes(x = cusd_n)) +
        geom_histogram(binwidth = 10, fill = "steelblue", color = "black", alpha = 0.7) +
        facet_wrap(~ glw3_name, scales = "free") +
        labs(x = "id_n", y = "Count", title = "Faceted Histogram of id_n by Animal Type") +
        theme_minimal()
      
      # whilst $I is more available $US is perhaps better to use when gap filling.
      glw_admin0_cusd<-add_nearby(data=copy(glw_admin0_val[,!c("id_n","id_tot")]),
                                 value_field = "cusd_n",
                                 group_field="glw3_name",
                                 neighbors=african_neighbors,regions)
      glw_admin0_cusd<-melt(glw_admin0_cusd,id.vars=c("iso3","glw3_name","glw3_no","cusd_tot","cusd_n"))
      
      
      glw_admin0_id<-add_nearby(data=copy(glw_admin0_val[,!c("cusd_n","cusd_tot")]),
                                  value_field = "id_n",
                                  group_field="glw3_name",
                                  neighbors=african_neighbors,regions)
      
      glw_admin0_id<-melt(glw_admin0_id,id.vars=c("iso3","glw3_name","glw3_no","id_tot","id_n"))
      
      # Neigbouring values very bad at predicting
      # No evidence that this approach is a good idea
      rbind(
       glw_admin0_id[variable!="id_n_final",.(unit="$I",
                             mean_prop=mean(id_n/value,na.rm=T),
                             mean_sd=sd(id_n/value,na.rm=T),
                             lm.adj.r.squared=summary(lm(id_n~value))$adj.r.squared),
                      by=.(glw3_name,variable)][,cv:=mean_sd/mean_prop],
       glw_admin0_cusd[variable!="cusd_n_final",.(unit="$US",
                                                  mean_prop=mean(cusd_n/value,na.rm=T),
                                                  mean_sd=sd(cusd_n/value,na.rm=T),
                       lm.adj.r.squared=summary(lm(cusd_n~value))$adj.r.squared),
                             by=.(glw3_name,variable)][,cv:=mean_sd/mean_prop]
      )
       
          
# 5) Distribute vop to GLW4 livestock ####
    # 5.1) US$ ####
    # We will use reported value or substitute using world prices (vop_hybrid2)
  focal_year<-"Y2015"
  final_vop<-copy(prod_value_usd)
  setnames(final_vop,focal_year,"value")
    
  final_vop<-final_vop[,list(iso3,atlas_name,value,VoP_hybrid2)]
  final_vop<-merge(final_vop,glw2atlas,all.x=T,by="atlas_name")
  setnames(final_vop,"VoP_hybrid2","VoP")
  
  # use value if present otherwise substitute the estimate numbers
  final_vop[is.na(value),value:=VoP][,VoP:=NULL]
  
  # Where there is missing data explore how many animals are in these areas
  final_vop_merge<-merge(final_vop,glw_admin0,all.x=T,by=c("iso3","glw3_name"))
  final_vop_merge[is.na(value) & !grepl("goat_milk|sheep_milk",atlas_name)]
  
  # Sum values for glw classes
  # Sum values for glw classes
  final_vop <- final_vop[, .(
    value = if (all(is.na(value))) as.numeric(NA) else sum(value, na.rm = TRUE)
  ), by = .(glw3_name, iso3)]  
  
  final_vop_cast<-dcast(final_vop,iso3~glw3_name)
  
  # Convert value to vector then raster
  final_vop_vect<-geoboundaries
  final_vop_vect<-merge(final_vop_vect,final_vop_cast,all.x=T)
  
  final_vop_rast<-terra::rast(lapply(unique(glw2atlas$glw3_name),FUN=function(NAME){
    terra::rasterize(final_vop_vect,base_rast,field=NAME)
  }))
  names(final_vop_rast)<-unique(glw2atlas$glw3_name)
  
  # Convert glw3 pixels to proportion of national total
  glw_admin0_cast<-dcast(glw_admin0,iso3~glw3_name,value.var="glw3_no")
  glw_vect<-geoboundaries
  glw_vect<-merge(glw_vect,glw_admin0_cast,all.x=T,by="iso3")
  
  glw_rast<-terra::rast(lapply(unique(glw2atlas$glw3_name),FUN=function(NAME){
    terra::rasterize(glw_vect,base_rast,field=NAME)
  }))
  
  names(glw_rast)<-unique(glw2atlas$glw3_name)
  
  glw_prop<-glw/glw_rast
  
  # Multiply national VoP by cell proportion
  glw_vop<-glw_prop*final_vop_rast
  
  terra::writeRaster(round(glw_vop*1000,0),file.path(ls_vop_dir,"livestock-vop-2015-cusd15.tif"),overwrite=T)
  
    # 5.2) I$ ####
  final_vop_i<-copy(prod_value_i)
  setnames(final_vop_i,focal_year,"value")
  
  final_vop_i<-final_vop_i[,list(iso3,atlas_name,value)]
  final_vop_i<-merge(final_vop_i,glw2atlas,all.x=T,by="atlas_name")
  
  final_vop_i[is.na(value)]
  
  # Sum values for glw classes
  final_vop_i<-final_vop_i[, .(NAs=sum(is.na(value)),value = sum(value,na.rm=T)), by = .(glw3_name, iso3)]
  
  # These values have missing data when summing (ignoring goat and sheep milk) and will be set to NA
  final_vop_i[NAs>0 & !glw3_name %in% c("goats","sheep")]
  final_vop_i[NAs>0 & !glw3_name %in% c("goats","sheep"),value:=NA]
  
      # 5.2.1) Explore if we can convert cusd values from faostat or estimated from production x price data to get missing $I data ######
  # Merge in missing values from cusd table
  setnames(final_vop,"value","cusd")
  final_vop_i<-merge(final_vop_i,final_vop,all.x=T)
  
  # Add xrat & ppp ratio to convert cusd to isud
  final_vop_i<-merge(final_vop_i,ppp_xrat,all.x=T,by="iso3")
  
  # Our calculaute for I$ does not give expected results, they are much higher than the faostat int dollar rates
  final_vop_i[,intd:=cusd*xrat/ppp]
  
  # We will have to work out implied rates without understanding what the FAO methodology is
  final_vop_i<-final_vop_i[,.(iso3,glw3_name,value)]

  # Prepare cusd values  
  final_vop_cusd<-copy(prod_value_usd)
  setnames(final_vop_cusd,focal_year,"cusd15")
  
  final_vop_cusd<-final_vop_cusd[,list(iso3,atlas_name,cusd15)]
  final_vop_cusd<-merge(final_vop_cusd,glw2atlas,all.x=T,by="atlas_name")
  
  # Prepare iusd values
  final_vop_intd<-copy(prod_value_i)
  setnames(final_vop_intd,focal_year,"value")
  
  final_vop_intd<-final_vop_intd[,list(iso3,atlas_name,value)]
  final_vop_intd<-merge(final_vop_intd,glw2atlas,all.x=T,by="atlas_name")
  
  # Merge I$ and US$
  final_vop_merge<-merge(final_vop_intd,final_vop_cusd,by=c("iso3","atlas_name","glw3_name"),all.x=T)
  
  final_vop_merge[,implied_cf:=cusd15/value]
  
  # It appears the FAOstat PPP values are commodity specific, for example they vary between cattle meat and milk in burundi.
  final_vop_merge[!is.na(implied_cf)]
  
  # Data is missing for some countries x commodities:
  final_vop_merge[is.na(value) & !grepl("milk",atlas_name)]
  
  # Most of these countries are data deficient in 2015, only eritre has some information on what the implied PPP might be
  ppp_missing<-final_vop_merge[is.na(value) & !grepl("milk",atlas_name),unique(iso3)]
  final_vop_merge[iso3 %in% ppp_missing]
  
      # !!!TO DO!!! see if the missing ppp cf values can be inputed from other years #####
      # 5.2.2) Distribute $I data we do have to GLW4 ######
  final_vop_i_cast<-dcast(final_vop_i,iso3~glw3_name)
  
  # Convert value to vector then raster
  final_vop_i_vect<-geoboundaries
  final_vop_i_vect<-merge(final_vop_i_vect,final_vop_i_cast,all.x=T)
  
  final_vop_i_rast<-terra::rast(lapply(unique(glw2atlas$glw3_name),FUN=function(NAME){
    terra::rasterize(final_vop_i_vect,base_rast,field=NAME)
  }))
  names(final_vop_i_rast)<-unique(glw2atlas$glw3_name)
  
  # Multiply national VoP by cell proportion
  glw_vop_i<-glw_prop*final_vop_i_rast
  
  terra::writeRaster(round(glw_vop_i*1000,0),file.path(ls_vop_dir,"livestock-vop-2015-intd15.tif"),overwrite=T)
  
 

  # 6) QAQC: Extract by admin0 and compare back to FAOstat ####
  
  # GLW3 distributed data
  data<-glw_vop
  
  qa_rasterizer<-function(data,base_rast,geoboundaries,glw2atlas){
    # constant usd 2014-2016
    data_ex<-terra::extract(data,geoboundaries,fun="sum",na.rm=T)
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
  glw_vop_int15_adm0<-qa_rasterizer(data=glw_vop_i,base_rast,geoboundaries,glw2atlas)$rast

  # FAOstat data
  fao_vop_cusd15<-prod_value_usd[,list(Y2015,iso3,atlas_name)]
  fao_vop_cusd15<-merge(fao_vop_cusd15,glw2atlas,all.x=T,by="atlas_name")
  fao_vop_cusd15<-fao_vop_cusd15[,list(value=sum(Y2015,na.rm=T),N=sum(is.na(Y2015))),by=list(glw3_name,iso3)]
  fao_vop_cusd15<-fao_vop_cusd15[N==0]
  
  fao_vop_cusd15<-dcast(fao_vop_cusd15,iso3~glw3_name)
  fao_vop_cusd15_vect<-geoboundaries
  fao_vop_cusd15_vect<-merge(fao_vop_cusd15_vect,fao_vop_cusd15,all.x=T,by="iso3")
  
  fao_vop_cusd15_rast<-terra::rast(lapply(unique(glw2atlas$glw3_name),FUN=function(NAME){
    terra::rasterize(fao_vop_cusd15_vect,base_rast,field=NAME)
  }))
  
  # Constant USD 2015
  
  # use indigenous meat production values
  lps2fao_ind<-lps2fao
  lps2fao_ind[grep("Meat",lps2fao_ind)]<-paste0(lps2fao_ind[grep("Meat",lps2fao_ind)]," (indigenous)")

  element<-"Gross Production Value (current thousand US$)" 

  fao_vop_usd15<-prepare_fao_data(file=vop_file,
                               lps2fao_ind,
                               elements=element,
                               remove_countries = remove_countries,
                               keep_years=target_year,
                               atlas_iso3=atlas_iso3)
  
  fao_vop_usd15[,atlas_name:=gsub(" (indigenous)","",atlas_name)]

  fao_vop_usd15<-fao_vop_usd15[,list(Y2015,iso3,atlas_name)]
  fao_vop_usd15<-merge(fao_vop_usd15,glw2atlas,all.x=T,by="atlas_name")
  fao_vop_usd15<-fao_vop_usd15[,list(value=sum(Y2015,na.rm=T),N=sum(is.na(Y2015))),by=list(glw3_name,iso3)]
  fao_vop_usd15<-fao_vop_usd15[N==0]
  
  fao_vop_usd15<-dcast(fao_vop_usd15,iso3~glw3_name)
  fao_vop_usd15_vect<-geoboundaries
  fao_vop_usd15_vect<-merge(fao_vop_usd15_vect,fao_vop_usd15,all.x=T,by="iso3")
  
  NAMES<-unique(glw2atlas$glw3_name)
  NAMES<-NAMES[NAMES %in% names(fao_vop_usd15_vect)]
  fao_vop_usd15_rast<-terra::rast(lapply(NAMES,FUN=function(NAME){
    terra::rasterize(fao_vop_usd15_vect,base_rast,field=NAME)
  }))
  
  # $I
  element<-"Gross Production Value (constant 2014-2016 thousand I$)"
  
  fao_vop_id15<-prepare_fao_data(file=vop_file,
                                  lps2fao_ind,
                                  elements=element,
                                  remove_countries = remove_countries,
                                  keep_years=target_year,
                                  atlas_iso3=atlas_iso3)
  
  fao_vop_id15[,atlas_name:=gsub(" (indigenous)","",atlas_name)]
  
  fao_vop_id15<-fao_vop_id15[!is.na(Y2015),list(Y2015,iso3,atlas_name)]
  fao_vop_id15<-merge(fao_vop_id15,glw2atlas,all.x=T,by="atlas_name")
  fao_vop_id15<-fao_vop_id15[,list(value=sum(Y2015,na.rm=T),N=sum(is.na(Y2015))),by=list(glw3_name,iso3)]
  fao_vop_id15<-fao_vop_id15[N==0]
  
  fao_vop_id15<-dcast(fao_vop_id15,iso3~glw3_name)
  fao_vop_id15_vect<-geoboundaries
  fao_vop_id15_vect<-merge(fao_vop_id15_vect,fao_vop_id15,all.x=T,by="iso3")
  
  NAMES<-unique(glw2atlas$glw3_name)
  NAMES<-NAMES[NAMES %in% names(fao_vop_id15_vect)]
  
  # Exclude countries
  exclude<-"ERI"
  fao_vop_id15_rast<-terra::rast(lapply(NAMES,FUN=function(NAME){
    terra::rasterize(fao_vop_id15_vect[!fao_vop_id15_vect$iso3 %in% exclude,],base_rast,field=NAME)
  }))
  
  # Comparisons
  plot(glw_vop_cusd15_adm0/fao_vop_cusd15_rast)
  plot(glw_vop_int15_adm0/fao_vop_id15_rast)
  
  