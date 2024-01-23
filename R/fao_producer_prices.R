require(data.table)

  # Load SPAM production data ####
  prod<-fread(paste0(mapspam_dir,"/spam2017V2r3_SSA_P_TA.csv"))
  crops<-tolower(ms_codes$Code)
  colnames(prod)<-gsub("_a$","",colnames(prod))
  
  # Read in production value data from FAO ####
  econ_file<-paste0(fao_dir,"/Prices_E_Africa_NOFLAG.csv")
  if(!dir.exists(fao_dir)){
    dir.create(fao_dir,recursive = T)
  }
  
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
  
  # Load fao producer price data
  prod_price<-fread(econ_file)
  
  # Load file for translation of spam to fao stat names/codes ####
  spam2fao<-fread("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SPAM2010_FAO_crops.csv")
  
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
  
  # Remove Ethiopia PDR & Sudan (Former)
  prod_price<-prod_price[!Area %in% c("Ethiopia PDR","Sudan (former)")]
  
  # Some countries are missing
  missing_countries<-c("SSD","DJI","SOM","LBR","COD","UGA","GAB","STP","SWZ")
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
  
  # Remove suspect tobacco values for Sierra Leone
  prod_price[iso3=="SLE" & short_spam2010=="toba",Y2015:=NA]
  

  # Average prices over 5-year period by country####
  prod_price[,mean:=mean(c(Y2019,Y2018,Y2017,Y2016,Y2015),na.rm=T),by=list(Area,iso3,short_spam2010)]
  
  # Check for missing 2017 values
  prod_price[is.na(mean)]
  
  # Create list of neighbouring countries ####
  african_neighbors <- list(
    DZA = c("TUN", "LBY", "NER", "ESH", "MRT", "MLI", "MAR"),
    AGO = c("COG", "COD", "ZMB", "NAM"),
    BEN = c("BFA", "NER", "NGA", "TGO"),
    BWA = c("ZMB", "ZWE", "NAM", "ZAF"),
    BFA = c("MLI", "NER", "BEN", "TGO", "GHA", "CIV"),
    BDI = c("COD", "RWA", "TZA"),
    CPV = c(),
    CMR = c("NGA", "TCD", "CAF", "COG", "GAB", "GNQ"),
    CAF = c("TCD", "SDN", "COD", "COG", "CMR"),
    TCD = c("LBY", "SDN", "CAF", "CMR", "NGA", "NER"),
    COM = c(),
    COG = c("GAB", "CMR", "CAF", "COD", "AGO"),
    COD = c("CAF", "SSD", "UGA", "RWA", "BDI", "TZA", "ZMB", "AGO", "COG"),
    CIV = c("LBR", "GIN", "MLI", "BFA", "GHA"),
    DJI = c("ERI", "ETH", "SOM"),
    EGY = c("LBY", "SDN", "ISR", "PSE"),
    GNQ = c("CMR", "GAB"),
    ERI = c("ETH", "SDN", "DJI"),
    SWZ = c("MOZ", "ZAF"),
    ETH = c("ERI", "DJI", "SOM", "KEN", "SSD", "SDN"),
    GAB = c("CMR", "GNQ", "COG"),
    GMB = c("SEN"),
    GHA = c("CIV", "BFA", "TGO"),
    GIN = c("LBR", "SLE", "CIV", "MLI", "SEN"),
    GNB = c("SEN", "GIN"),
    KEN = c("ETH", "SOM", "SSD", "UGA", "TZA"),
    LSO = c("ZAF"),
    LBR = c("GIN", "CIV", "SLE"),
    LBY = c("TUN", "DZA", "NER", "TCD", "SDN", "EGY"),
    MDG = c(),
    MWI = c("MOZ", "TZA", "ZMB"),
    MLI = c("DZA", "NER", "BFA", "CIV", "GIN", "SEN", "MRT"),
    MRT = c("DZA", "ESH", "SEN", "MLI"),
    MAR = c("DZA", "ESH", "ESP"),
    MOZ = c("ZAF", "SWZ", "ZWE", "ZMB", "MWI", "TZA"),
    NAM = c("AGO", "BWA", "ZAF", "ZMB"),
    NER = c("DZA", "LBY", "TCD", "NGA", "BEN", "BFA", "MLI"),
    NGA = c("BEN", "CMR", "TCD", "NER"),
    RWA = c("BDI", "COD", "TZA", "UGA"),
    STP = c(),
    SEN = c("GMB", "GIN", "GNB", "MLI", "MRT"),
    SYC = c(),
    SLE = c("GIN", "LBR"),
    SOM = c("ETH", "DJI", "KEN"),
    ZAF = c("NAM", "BWA", "ZWE", "MOZ", "SWZ", "LSO"),
    SSD = c("CAF", "COD", "ETH", "KEN", "UGA"),
    SDN = c("EGY", "ERI", "ETH", "SSD", "CAF", "TCD", "LBY"),
    TZA = c("KEN", "UGA", "RWA", "BDI", "COD", "ZMB", "MWI", "MOZ"),
    TGO = c("BEN", "BFA", "GHA"),
    TUN = c("DZA", "LBY"),
    UGA = c("KEN", "SSD", "COD", "RWA", "TZA"),
    ZMB = c("AGO", "COD", "MWI", "MOZ", "NAM", "TZA", "ZWE"),
    ZWE = c("BWA", "MOZ", "ZAF", "ZMB")
  )
  
  # Fill in gaps with mean of neighbours ####
  N<-prod_price[,which(is.na(mean))]
  
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
  
  # Create list of regions ####
  regions <- list(
    East_Africa = c("BDI", "COM", "DJI", "ERI", "ETH", "KEN", "MDG", "MUS", "MWI", "RWA", "SYC", "SOM", "SSD", "TZA", "UGA"),
    Southern_Africa = c("BWA", "LSO", "NAM", "SWZ", "ZAF", "ZMB", "ZWE","MOZ"),
    West_Africa = c("BEN", "BFA", "CPV", "CIV", "GMB", "GHA", "GIN", "GNB", "LBR", "MLI", "MRT", "NER", "NGA", "SEN", "SLE", "TGO"),
    Central_Africa = c("AGO", "CMR", "CAF", "TCD", "COD", "COG", "GNQ", "GAB", "STP"),
    North_Africa = c("DZA", "EGY", "LBY", "MAR", "SDN", "TUN")
  )
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
    neighbours<-neighbours[neighbours!="iso3"]
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
  
  # Load SPAM production data (bring back the two crops we removed)
  prod<-fread(paste0(mapspam_dir,"/spam2017V2r3_SSA_P_TA.csv"))
  crops<-tolower(ms_codes$Code)
  colnames(prod)<-gsub("_a$","",colnames(prod))
  
  ms_fields<-c("x","y","iso3",sort(crops))
  prod<-prod[,..ms_fields]
  
  # Restructure fao data
  prod_price_cast<-dcast(prod_price[,list(iso3,mean_final,short_spam2010)],iso3~short_spam2010,value.var = "mean_final")
  
  # Check columns align
  crops[!crops %in% colnames(prod_price_cast)]
  
  # Add back missing crops
  prod_price_cast[,rcof:=acof][,smil:=pmil]
  
  write.table(prod_price_cast,"clipboard",sep="\t",row.names = F)
  
  # List ms countries
  countries<-prod[,unique(iso3)]
  
  # Mulitple production by price per ton for each country
  vop<-rbindlist(lapply(1:length(countries),FUN=function(i){
    data<-prod[iso3 == countries[i]]
    vop<-cbind(data[,list(x,y)],data[,..crops] * prod_price_cast[iso3==countries[i],..crops][rep(1,nrow(data))])
  }))
  
  vop<-terra::rast(vop,type="xyz",crs="EPSG:4326")
  

