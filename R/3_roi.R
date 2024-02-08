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
years<-15
adoption<-c(0.005,0.01,0.02)
prod_impact<-c(0.05,0.1,0.2,0.33,0.5,1)
cis_impact<-c(0,0.05,0.01)
discount_rates<-c(4,8,12)
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
  
  
  
  data_benefit<-data_benefit[admin0_name=="Kenya" & is.na(admin1_name) & crop=="maize"]
  
  
  
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
  arrow::write_parquet(data_benefit,sink=paste0(save_dir,"/roi_data.parquet"))
  
# 5) See script 5.1 for economic indicators ####
#---------------------------------------------------####
  # X) Old Approach ####

  # X.1) Work out benefits ####
  data_rs<-data[exposure %in% c("harvested_area","vop_USD17")]
  data_rs[,BCR:=1.62]
  data_rs_impact<-dcast(data_rs,admin0_name+admin1_name+crop+prod_impact+adoption+year+BCR~exposure,value.var = "result")
  
  # Add base harvested area
  data_rs_impact<-merge(data_rs_impact,data_rs[exposure=="harvested_area",!c("result","exposure")])
  setnames(data_rs_impact,"value","ha_base")
  
  # Add base price
  data_rs_impact<-merge(data_rs_impact,data_rs[exposure=="vop_USD17",!c("result","exposure")])
  setnames(data_rs_impact,"value","vop_base")
  
  # Add base production
  by_fields<-c("admin0_name","crop","admin1_name","adoption","prod_impact","year")
    data_rs<-data[exposure %in% c("production")]
  data_rs_impact<-merge(data_rs_impact,data_rs[,!c("result","exposure")],by=by_fields)
  setnames(data_rs_impact,"value","prod_base")
  
  # Add base yield
  data_rs<-data[exposure %in% c("yield")]
  data_rs_impact<-merge(data_rs_impact,data_rs[exposure=="yield",!c("result","exposure")],by=by_fields)
  setnames(data_rs_impact,"value","yield")
  data_rs_impact[,yield:=yield/1000]
  
  # Add Price
  data_rs_impact[,Price:=vop_base/prod_base]
  setnames(data_rs_impact,c("harvested_area","yield"),c("A_adopt","Y_non_adopt"))
  data_rs_impact[,A_non_adopt:=ha_base-A_adopt]
  data_rs_impact[,Y_adopt:=Y_non_adopt*(1+prod_impact)]
  
  # Calculate VOP
  data_rs_impact[,VOP_non_adopt:=A_non_adopt*Y_non_adopt*Price
                 ][,VOP_adopt:=A_adopt*Y_adopt*Price]
  
  # Estimate Cost
  data_rs_impact[,Cost_adopt:=Price*Y_adopt/BCR
                 ][,Cost_non_adopt:=Price*Y_non_adopt/BCR]
  
  # Estimate Net Return
  data_rs_impact[,NR_adopt:=A_adopt*(Y_adopt*Price-Cost_adopt)
                 ][,NR_non_adopt:=A_non_adopt*(Y_non_adopt*Price-Cost_non_adopt)]
  
  # Estimate project benefit
  data_rs_impact[,project_benefit:=NR_non_adopt+NR_adopt
                 ][,project_benefit:=project_benefit-(ha_base*((Y_non_adopt*Price)-Cost_non_adopt)),by=list(admin0_name,admin1_name,crop,adoption,prod_impact)]
  

  # X.2) Economic indicators ####
  discount_rate_choice<-5
  project_year_choice<-5
  project_cost<-10^6
  
  
  # sum results
  data_rs_sum<-data_rs_impact[,list(project_benefit=sum(project_benefit,na.rm=T)),by=list(adoption,prod_impact,year,BCR )]
  
  # Re-sort dataset so that years are in order, this is required for irr calculations
  data_rs_sum<-data_rs_sum[order(adoption,prod_impact,BCR,year)]
  
  # Calculate IRR
  data_rs_sum[,irr:=as.numeric(irr_wrap(project_cost,years=year,project_benefits=project_benefit)),by=list(adoption,prod_impact,BCR)]
  
  # Add discount rates
  data_rs_sum<-data.table(
    data_rs_sum[rep(1:nrow(data_rs_sum),each=length(discount_rates))],
    discount_rate=discount_rates[rep(1:length(discount_rates),nrow(data_rs_sum))]
  )
  
  
  # Calculate
  data_rs_sum[,npv:=(project_benefit/(1+discount_rate/100)^year)-project_cost
  ][,bcr:=npv/project_cost]
  
  # Under what conditions is NPV positive?
  unique(data_rs_sum[npv>0,list(adoption,prod_impact,discount_rate,year,npv)][order(npv,decreasing=T)])
  
  

  



  
