# Install and load packages ####
load_and_install_packages <- function(packages) {
  for (package in packages) {
    if (!require(package, character.only = TRUE)) {
      install.packages(package)
      library(package, character.only = TRUE)
    }
  }
}

# List of packages to be loaded
packages <- c("data.table", 
              "countrycode",
              "s3fs",
              "arrow")

# Call the function to install and load packages
load_and_install_packages(packages)

# Create functions ####
# Function to calculate avoided loss
avloss<-function(mean,sd,change,fixed,reps=100000){
  #Calculate co-efficient of variation
  
  # Calculate new standard deviation based on changed CV
  if(fixed){
    sdcis<-((sd/mean)-change)*mean # Change value is substracted from CV
  }else{
    sdcis<-((sd/mean)*(1-change))*mean # Change value is applied as a proportional reduction in CV
  }
  
  if(!sdcis<=0){
    
    # Create normal distribution of values
    x<-rnorm(n=reps,mean=mean,sd=sd)
    # Calculate probabilities
    pnorm<-pnorm(x,mean=mean,sd=sd)
    pnormCIS<-pnorm(x,mean=mean,sd=sdcis)
    
    # Calculate differences in probabilities
    pnormDiff<-pnormCIS-pnorm
    
    # Sum negative differences and divide by total probability for normal CV
    avloss<-sum(pnormDiff[pnormDiff<0])/sum(pnorm)
  }else{
    avloss<-NA
  }
  
  return(avloss)
}

# 1) Load data ####
  # 1.1) Crops - MapSPAM ####

    # This file can be used if yields are required
    # file<-"C:/Users/Peter Steward/OneDrive - CGIAR/Projects/EiA/Regional Prioritization/PAiCE/SPAM/mapspam_complete_extraction_adm1.csv"
    #exposure<-fread(file)
    #exposure<-exposure[technology=="all"][,list(exposure,admin0_name,admin1_name,crop,value)]

    file<-"Data/exposure/crop_vop_usd17_adm_sum.parquet"
    
    if(!file.exists(file)){
      if(!dir.exists(dirname(file))){
        dir.create(dirname(file),recursive = T)
      }
      
      s3_file <-"s3://digital-atlas/risk_prototype/data/exposure/crop_vop_usd17_adm_sum.parquet"
      s3fs::s3_file_download(path=s3_file,new_path = file,overwrite = T)
    }
    
    exposure<-data.table(arrow::read_parquet(file))
    exposure<-exposure[is.na(admin2_name)][,admin2_name:=NULL]
    
  # 1.2) Livestock ####
    file<-"Data/exposure/livestock_vop_usd17_adm_sum.parquet"
    
    if(!file.exists(file)){
      if(!dir.exists(dirname(file))){
        dir.create(dirname(file),recursive = T)
      }
      
      s3_file <-"s3://digital-atlas/risk_prototype/data/exposure/livestock_vop_usd17_adm_sum.parquet"
      s3fs::s3_file_download(path=s3_file,new_path = file,overwrite = T)
    }
    
  livestock_vop<-data.table(arrow::read_parquet(file)  )
  livestock_vop<-livestock_vop[is.na(admin2_name)][,admin2_name:=NULL]
  
  # Combine highland and lowland cattle
  livestock_vop<-livestock_vop[,crop:=gsub("_tropical|_highland","",crop)][,list(value=sum(value,na.rm=T)),by=list(admin0_name,admin1_name,crop,exposure)]
    
  # 1.3) Join livestock and crop exposures ####
  exposure<-rbind(exposure,livestock_vop)[crop!="total"]
  rm(livestock_vop)
  
  # 1.4) FAO CV in production ####
  file<-"Data/fao/faostat_prod_cv.csv"
  
  if(!file.exists(file)){
    if(!dir.exists(dirname(file))){
      dir.create(dirname(file),recursive = T)
    }
    
    s3_file <-  "s3://digital-atlas/risk_prototype/data/fao/faostat_prod_cv.csv"
    s3fs::s3_file_download(path=s3_file,new_path = file,overwrite = T)
  }
 
  cv<-fread(file)
  
  # Add in admin0_name
  cv<-cv[,admin0_name:=countrycode::countrycode(sourcevar=iso3,origin="iso3c",destination = "country.name")
         ][,list(crop,admin0_name,value_filled)]
  
  setnames(cv,"value_filled","cv")
  
  # Remove milk and egg yields, keep meat
  cv<-cv[!grepl("milk|eggs",crop)
         ][,crop:=gsub("_meat","",crop)
           ][,crop:=stringi::stri_replace_all_regex(crop,c("goat$","pig$"),c("goats","pigs"),vectorize_all = F)
             ][crop!="total"]
  
  
  cv[,unique(crop)]
  
  # Do all countries match?
  exposure$admin0_name[!exposure$admin0_name %in% cv$admin0_name]
  cv$admin0_name[!cv$admin0_name %in% exposure$admin0_name]
  
# 2) Set parameters and save directory ####
years<-16
adoption<-c(0.005,0.01,0.02)
prod_impact<-c(0.1,0.2,0.3,0.4,0.5)
cis_impact<-c(0,0.01,0.05,0.1)
bcrs<-1.62
# Combinations
nrow(expand.grid(1:years,adoption,prod_impact,bcrs))

save_dir<-"Data/roi"
if(!dir.exists(save_dir)){
  dir.create(save_dir)
}
combinations<-data.table(expand.grid(adoption=adoption,prod_impact=prod_impact))

N1<-rep(1:nrow(exposure),each=nrow(combinations))
N2<-rep(1:nrow(combinations),nrow(exposure))

# 3) Work out marginal change ####
data<-cbind(exposure[N1],combinations[N2])

y_cols<-paste0("y",1:years)

for(i in 1:years){
  if(i==1){
  # For year one the amount of the value adopted is simply the value at year 0 x adoption
  data[,x:=round(value*adoption,1)]
  }else{
    # Years >1 the cumulative amount under adoption is:
    # ((value at year 0 - cumulative adoption value from previous year)*adoption_rate) + cumulative adoption value from previous year
    cname<-paste0("y",i-1)
    data[,(cname):=x]
    data[,x:=round((value-x)*adoption,1)+x]
  }
  
  if(i==years){
    cname<-paste0("y",i)
    setnames(data,"x",cname)
    col_order<-c(setdiff(colnames(data), cname), cname)
    data <- data[,..col_order ]
  }
}

# Melt data into long form
data<-melt(data,
             id.vars = c("admin0_name","value","crop","exposure", "admin1_name","adoption","prod_impact"),
             variable.name = "year_char",
             value.name = "result")

# Convert year from character to numeric
data[,year:=as.numeric(gsub("y","",year_char[1])),by=year_char][,year_char:=NULL]

# Reorder dataset
data<-data[order(admin0_name,admin1_name,exposure,value,crop,adoption,prod_impact)]

# Round value to integer to reduced size and calculation demands
data[,value:=round(value,0)]

  # 3.1) Add marginal benefit to production ####
    # a) Multiply the cumulative adoption amount (value of production) by production benefit of adoption
    # b) Substract the cumulative adoption amount to get the difference in production over non-adoption
  data<-data[exposure %in% c("vop_usd17","vop","production"),result_w_impact:=round(result*(1+prod_impact),1)
               ][,marginal_impact:=round(result_w_impact-result,1)]
  
  # 3.2) Add cv and marginal benefit of cis to dataset ####
  # Do all crops match?
  unique(data$crop[!data$crop %in% cv$crop])
  unique(cv$crop[!cv$crop %in% data$crop])
  
  
  # Duplicate dataset for cis_impact values
  N1<-rep(1:nrow(data),each=length(cis_impact))
  N2<-rep(1:length(cis_impact),nrow(data))
  data<-data.table(
    data[N1],
    cis_impact=cis_impact[N2]
  )
  
  
  # Merge cv data
  data<-merge(data,cv,all.x=T)
  data[,cv:=round(cv,4)]
  
  # Calculate % avoided loss
  # Note it appears the mean is not actually required in the avloss function as it returns a proportion that relates to
  # cv, the magnitude of the mean does not affect the outcome.
  data[value!=0,avloss:=abs(round(avloss(mean=1,
                                 sd=1*cv[1],
                                 change=cis_impact[1],
                                 fixed=F,
                                 reps=10000),4)),by=list(cv,cis_impact)]
  
  data[,marginal_impact_w_cis:=round(marginal_impact*(1+avloss),1)
       ][is.na(marginal_impact_w_cis),marginal_impact_w_cis:=0]
  
# 4) Calculate adoption benefits using VoP approach ####
  data_benefit<-data[exposure == "vop_usd17"][,c("exposure"):=NULL]

  # Duplicate dataset for BCR values
  N1<-rep(1:nrow(data_benefit),each=length(bcrs))
  N2<-rep(1:length(bcrs),nrow(data_benefit))
  data_benefit<-data.table(
    data_benefit[N1],
    bcr=bcrs[N2]
  )

  #data_benefit[,cost:=(value+marginal_impact)/bcr
  #          ][,nr:=(value+marginal_impact)-cost]
  
  data_benefit[,project_benefit:=marginal_impact_w_cis-marginal_impact_w_cis/bcr]
  
  data_benefit<-data_benefit[,!c("result_w_impact","marginal_impact","value","result","cv","avloss","marginal_impact_w_cis")]
  
  # Set benefits to be integer to reduce file size
  data_benefit[,project_benefit:=as.integer(project_benefit)]
  
# 4.1) Save dataset ####
  arrow::write_parquet(data_benefit,sink=paste0(save_dir,"/roi_data.parquet"))