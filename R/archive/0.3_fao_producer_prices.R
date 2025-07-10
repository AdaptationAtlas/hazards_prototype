# Is this script redundant?
# fao_producer_prices_2017.csv not used 0.5,4,4.1?

# Please run 0_server_setup.R before executing this script
# 0) Install and load packages ####
pacman::p_load(data.table,countrycode,terra)
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

# Load SPAM production data ####
file<-list.files(mapspam_dir,"SSA_P_TA.csv",full.names = T)
file<-file[!grepl("_gr_",mapspam_dir)]
prod<-fread(file)
  
  ms_codes<-data.table::fread(ms_codes_url)[,Code:=toupper(Code)]
  crops<-tolower(ms_codes[compound=="no",Code])
  colnames(prod)<-gsub("_a$","",colnames(prod))
  
  # Read in production value data from FAO ####
  prod_price<-fread(fao_econ_file)
  
  # Load file for translation of spam to fao stat names/codes ####
  spam2fao<-fread(spam2fao_url)
  
  # Are all crops represented in fao name conversion sheet? 
  if(!length(crops[!crops %in% spam2fao$short_spam2010])==0){
    stop("MAPspam crop missing from fao name conversion table SPAM2010_FAO_crops.csv")
  }
  
  # Subset fao to crops being considered ####
  spam2fao<-spam2fao[short_spam2010 %in% crops]
  
  # There two entries for "rape" remove one
  spam2fao<-spam2fao[name_fao != "Mustard seed"]
  
  # Note pearl/small millets and robusta/arabica share the same fao_code so choose only one of these when merging datasets
  
  # Subset fao data (this step can be combined in the merge below)
  prod_price<-prod_price[`Item Code` %in% spam2fao$code_fao]
  
  # Add spam name column
  prod_price<-merge(prod_price,spam2fao[!short_spam2010 %in% c("smil","rcof"),list(code_fao,short_spam2010)],by.x="Item Code",by.y="code_fao")
  
  # Check fao & spam country names match ####
  prod_price[,M49:=as.numeric(gsub("[']","",`Area Code (M49)`))]
  prod_price[,iso3:=countrycode(sourcevar=M49,origin="un",destination = "iso3c")]
  
  # Non matches
  unique(prod_price[M49 %in% c(230,736),list(Area,M49,iso3)])
  
  # Check countries represented in FAOstat
  spam_countries<-unique(prod$iso3) 
  unique(prod[!iso3 %in% prod_price$iso3,list(name_cntr,iso3)])
  prod_price[,unique(Area)]
  
  # Remove Ethiopia PDR & Sudan (Former) & non-atlas countries
  prod_price<-prod_price[!Area %in% c("Ethiopia PDR","Sudan (former)","Cabo Verde","Comoros","Mauritius","R\xe9union","Seychelles")]
  
  # Some countries are missing
  missing_countries<-c("SSD","DJI","SOM","LBR","COD","UGA","GAB","SWZ")
  names(missing_countries)<-prod[match(missing_countries,iso3),name_cntr]
  
  
  # Subset fao to USD/tonne ####
  prod_price<-prod_price[Element == "Producer Price (USD/tonne)"]
  
  # Subset fao  columns ####
  keep_years<-c("Y2019","Y2018","Y2017","Y2016","Y2015")
  keep_cols<-c("Area","iso3","short_spam2010",keep_years)
  prod_price<-prod_price[,..keep_cols]
  
  # Add missing crops x countries to fao ####
  fao_countries<-unique(prod_price[,list(iso3,Area)])
  crops<-prod_price[,unique(short_spam2010)]
  
  missing<-rbindlist(lapply(1:nrow(fao_countries),FUN=function(i){
    country<-fao_countries[i,iso3]
    country_name<-fao_countries[i,Area]
    missing_crops<-crops[!crops %in% prod_price[iso3==country,short_spam2010]]
    if(length(missing_crops)>0){
    data<-data.table(Area=country_name,
                     iso3=country,
                     short_spam2010=missing_crops)
    data[,(keep_years):=NA]
    data
    }else{
      NULL
    }
  }))
  
  prod_price<-rbind(prod_price,missing)
  
  # Add missing countries to fao ####
   missing<-rbindlist(lapply( 1:length(missing_countries),FUN=function(i){
    data<-data.table(Area=names(missing_countries[i]),
                     iso3=missing_countries[i],
                     short_spam2010=prod_price[,unique(short_spam2010)])
    data[,(keep_years):=NA]
    data
  }))
   
   prod_price<-rbind(prod_price,missing)
  
  # Fix suspect values ####
  # Remove suspect tobacco values for Sierra Leone
  prod_price[iso3=="SLE" & short_spam2010=="toba",Y2015:=NA]
  
  # 2016 and 2015 prices for maize and sesame in SLE are extremely high remove, coconut prices are volatile too, s
  prod_price[iso3 %in% c("SLE") & short_spam2010 %in% c("maiz","cnut","sesa"),c("Y2016","Y2015"):=NA]
  
  # Many prices in GIN seem unreasonably low
  prod_price[iso3 %in% c("GIN") & short_spam2010 %in% c("swpo","yams","cass","grou","cott","sorg","pmil","bana"),c("Y2019","Y2018","Y2017","Y2016","Y2015"):=NA]

  # 2018 and 2017 prices for groundnut in EGY are extremely high remove
  prod_price[iso3 %in% c("EGY") & short_spam2010=="grou",c("Y2018","Y2017"):=NA]
  
  # Remove oilpalm prices for Burundi
  prod_price[iso3 %in% c("BDI") & short_spam2010 %in% c("oilp"),c("Y2019","Y2018","Y2017","Y2016","Y2015"):=NA]
  
  # Remove very low sugar cane price from Senegal
  prod_price[iso3 %in% c("SEN") & short_spam2010 %in% c("sugc"),c("Y2019","Y2018","Y2017","Y2016","Y2015"):=NA]
  
  # Average prices over 5-year period by country####
  prod_price[,mean:=mean(c(Y2019,Y2018,Y2017,Y2016,Y2015),na.rm=T),by=list(Area,iso3,short_spam2010)]
  
  # Check for missing 2017 values
  prod_price[is.na(mean)]

  
  # Fill in gaps with mean of neighbours ####
  avg_neighbours<-function(iso3,crop,neighbours,prod_price){
    neighbours<-african_neighbors[[iso3]]
    N<-prod_price[short_spam2010==crop & iso3 %in% neighbours,mean(mean,na.rm=T)]
    return(N)
  }
  
  prod_price[is.na(mean),mean_neighbours:=avg_neighbours(iso3=iso3,
                                                         crop=short_spam2010,
                                                         neighbours=african_neighbors,
                                                         prod_price=copy(prod_price)),
             by=list(iso3,short_spam2010  ,Area)]
  
  # Check for remaining gaps
  prod_price[is.na(mean) & is.na(mean_neighbours)]
  
  # Check if any countries not represented in regions
  if(F){
    X<-prod_price[,unique(iso3)]
    
    for(iso3 in X){
      print(paste0(iso3,"-",names(regions)[sapply(regions,FUN=function(X){iso3 %in% X})]))
    }
  }
  
  # Fill in gaps with mean of regions ####
  avg_regions<-function(iso3,crop,regions,prod_price){
    region_focal<-names(regions)[sapply(regions,FUN=function(X){iso3 %in% X})]
    neighbours<-regions[[region_focal]]
    N<-prod_price[short_spam2010==crop & iso3 %in% neighbours,mean(mean,na.rm=T)]
    return(N)
    }
  
  
  prod_price[is.na(mean) & is.na(mean_neighbours),mean_region:=avg_regions(iso3=iso3,
                                                      crop=short_spam2010,
                                                      regions=regions,
                                                      prod_price=copy(prod_price)),
             by=list(iso3,short_spam2010  ,Area)]
  
  # Check for remaining gaps
  prod_price[is.na(mean) & is.na(mean_neighbours & is.na(mean_region))]
  
  # Fill in gap with continental average ####
  prod_price[,mean_continent:=mean(mean,na.rm=T),by=short_spam2010][!is.na(mean) | !is.na(mean_neighbours) | !is.na(mean_region),mean_continent:=NA]
  
  # Check for remaining gaps
  prod_price[is.na(mean) & is.na(mean_neighbours) & is.na(mean_region) & is.na(mean_continent)]
  
  prod_price[,mean_final:=mean
             ][is.na(mean_final),mean_final:=mean_neighbours
               ][is.na(mean_final),mean_final:=mean_region
                 ][is.na(mean_final),mean_final:=mean_continent]
  
  # Check prices seem reasonable ####
    # View(prod_price[order(mean_final,decreasing = T)])
    # Price of tobacco in Sierra Leone seems erroneous at $12994/ton this will be removed.
  
  # Multiply mapspam production by producer price ####
  
  files<-list.files(mapspam_dir,"SSA_P_",full.names = T)
  prod_price_file<-paste0(fao_dir,"/fao_producer_prices_2017.csv")
  # List ms countries
  countries<-prod[,unique(iso3)]
  
  overwrite<-T
  
  for(i in 1:length(files)){
    file<-files[i]
    print(paste(i,"-",file))
    save_name<-gsub("SSA_P_","SSA_Vusd17_",file)
    
    # Load SPAM production data (bring back the two crops we removed)
    prod<-fread(file)
    Crops<-tolower(ms_codes[compound=="no",Code])
    
    colnames(prod)<-gsub("_a$|_h$|_i$|_l$|_r$|_s$","",colnames(prod))
    
    ms_fields<-c("x","y","iso3",sort(Crops))
    prod<-prod[,..ms_fields]
    
    if(!file.exists(prod_price_file)|overwrite){
      # Restructure fao data
      prod_price_cast<-dcast(prod_price[,list(iso3,mean_final,short_spam2010)],iso3~short_spam2010,value.var = "mean_final")
      
      # Check columns align
      Crops[!Crops %in% colnames(prod_price_cast)]
      
      # Add back missing crops
      prod_price_cast[,rcof:=acof][,smil:=pmil]
      prod_price_cast[,country:=countrycode(sourcevar=iso3,origin="iso3c",destination = "country.name")]
      
      fwrite(prod_price_cast,prod_price_file,bom=T)
    }else{
      prod_price_cast<-fread(prod_price_file) 
    }

    
    # Multiply production by price per ton for each country
    vop<-rbindlist(lapply(1:length(countries),FUN=function(i){
      data<-prod[iso3 == countries[i]]
      vop<-cbind(data[,list(x,y)],data[,..Crops] * prod_price_cast[iso3==countries[i],..Crops][rep(1,nrow(data))])
    }))
    
    vop[,tota:=apply(vop[,!c("x","y")],1,sum,na.rm=T)]
    
    fwrite(vop,file = save_name)
  
  }
