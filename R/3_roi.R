# You will need run these scripts before running this one: ####
# https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/fao_producer_prices.R
# https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/fao_producer_prices_livestock.R
# https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/fao_production_cv.R

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
avloss <- function(cv, change, fixed = FALSE, reps = 10^6) {
  # Calculate co-efficient of variation
  x <- 1
  
  # Calculate new standard deviations
  if(fixed){
    # Ensure fixed change does not exceed cv
    change <- min(change, cv)
    sd_with<-(cv - change) * x
  }else{
    sd_with<-(cv * (1 - change)) * x
    }
  
  sd_without <- cv * x
  
  # Avoid computation if the standard deviation would be negative
  if (sd_with < 0) {
    return(NA)
  }
  
  # Generate normal distributions
  with <- rnorm(n = reps, mean = x, sd = sd_with)
  without <- rnorm(n = reps, mean = x, sd = sd_without)
  
  # Direct calculation of sum of lower half without sorting
  with_lh <- sum(with[with <= median(with)])
  without_lh <- sum(without[without <= median(without)])
  
  # Calculate average loss reduction and express as proportion of total without innovation
  avloss <- (with_lh - without_lh) / sum(without)
  
  avloss[avloss<0]<-0
  
  return(avloss)
}

# Test avloss function
if(F){
  library(ggplot2)
  library(reshape2)
  
  # Create the input ranges for cv and change
  cv <- seq(0, 1, by = 0.025)
  change <- seq(0, 0.9, by = 0.01)
  
  # Create an empty data frame to store the results
  results <- expand.grid(cv=cv, change=change)
  results$avloss <- NA
  
  # Calculate avloss for each combination of cv and change
  for (i in 1:nrow(results)) {
    results$avloss[i] <- avloss(cv = results$cv[i], change = results$change[i], reps = 10^4)  # Reduced reps for speed
  }
  
  # Reshape for plotting
  results_long <- melt(results, id.vars = c("cv", "change"), variable.name = "variable", value.name = "avloss")
  
  # Generate the plot
  ggplot(results_long, aes(x = change, y = cv, fill = avloss)) +
    geom_tile() +
    scale_fill_gradient2(low = "grey90", mid="yellow2",high = "darkgreen") +
    labs(x = "Proportional reduction in CV", y = "CV", fill = "Avoided Loss Benefit") +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 90, hjust = 1))

}

# 1) Load data ####
  # 1.1) Crops - MapSPAM ####
    # 1.1.1) Vop 2017usd ####
    # This file can be used if yields are required
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
    
  # x) Human pop ####
  file<-"Data/exposure/hpop_adm_sum.parquet"
  
  if(!file.exists(file)){
    if(!dir.exists(dirname(file))){
      dir.create(dirname(file),recursive = T)
    }
    
    s3_file <-"s3://digital-atlas/risk_prototype/data/exposure/hpop_adm_sum.parquet"
    s3fs::s3_file_download(path=s3_file,new_path = file,overwrite = T)
  }
  
  hpop<-data.table(arrow::read_parquet(file))
  hpop<-hpop[is.na(admin2_name)][,admin2_name:=NULL][crop=="rural"]

  
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
  
  # Note robusta coffee and small millet do not exist in FAOstat data so substitute values for arabica and pearl millet
  
  cv<-rbind(cv,
            cv[crop=="arabica coffee"][,crop:="robusta coffee"],
            cv[crop=="pearl millet"][,crop:="small millet"]
  )
  
  # Do all countries match?
  exposure$admin0_name[!exposure$admin0_name %in% cv$admin0_name]
  cv$admin0_name[!cv$admin0_name %in% exposure$admin0_name]
  
  
  # 1.5) Combine exposure and impact tables ####
  combine_tables <- function(table1, table2) {
    # Get the number of rows for each table
    n1 <- nrow(table1)
    n2 <- nrow(table2)
    
    # Create an index for each table
    idx1 <- rep(1:n1, each=n2)
    idx2 <- rep(1:n2, times=n1)
    
    # Expand the tables
    expanded_table1 <- table1[idx1, ]
    expanded_table2 <- table2[idx2, ]
    
    # Bind the columns of both expanded tables side by side
    combined_table <- cbind(expanded_table1, expanded_table2)
    
    # Return the combined table
    return(combined_table)
  }
  
  data_hpop<-combine_tables(hpop,combinations[,"adoption"])[,prod_impact:=NA]
  data<-combine_tables(exposure,combinations)
  
  data<-rbind(data,data_hpop)
  
# 2) Set parameters and save directory ####
years<-16
adoption<-c(0.005,0.01,0.02)
prod_impact<-c(0.1,0.2,0.3,0.4,0.5)
cis_impact<-c(0,0.1,0.25,0.5)
bcrs<-1.62
# Combinations
nrow(expand.grid(1:years,adoption,prod_impact,bcrs))

save_dir<-"Data/roi"
if(!dir.exists(save_dir)){
  dir.create(save_dir)
}
combinations<-data.table(expand.grid(adoption=adoption,prod_impact=prod_impact))



# 3) Work out marginal change ####

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
data<-data.table(melt(data,
             id.vars = c("admin0_name","value","crop","exposure", "admin1_name","adoption","prod_impact"),
             variable.name = "year_char",
             value.name = "result"))

# Convert year from character to numeric
data[,year:=as.numeric(gsub("y","",year_char[1])),by=year_char][,year_char:=NULL]

# Reorder dataset
data<-data[order(admin0_name,admin1_name,exposure,value,crop,adoption,prod_impact)]

# Round value to integer to reduced size and calculation demands
data[,value:=round(value,0)]

# 3.1) Separate population and production/vop ####
data_hpop<-data[crop=="rural"][,prod_impact:=NULL]
data_hpop[,crop:="rural_pop"]

setnames(data_hpop,c("crop","exposure","result"),c("variable","unit","adopters"))
data_hpop[,non_adopters:=value-adopters][,adoption_perc:=round(100*adopters/value,2)]

data_hpop<-unique(data_hpop)

arrow::write_parquet(data_hpop,sink="Data/roi/adoption_rates.parquet")

# Remove population data, this is because we cannot easily assign human population to crops
# As we have removed population data we can also remove the geographic data

data_hpop<-unique(data_hpop[,list(adoption_perc=mean(adoption_perc)),
                                    by=list(adoption,year)
                            ][,adoption_perc:=round(adoption_perc,1)])

arrow::write_parquet(data_hpop,sink="Data/roi/adoption_rates_perc.parquet")


data<-data[crop!="rural"]

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
  data[value!=0,avloss:=abs(round(avloss(cv=1*cv[1],
                                         change=cis_impact[1],
                                         fixed=F,
                                         reps=10^6),4)),by=list(cv,cis_impact)]
  
  # Multiply production of adopters by cis benefit, subtract adopter product from adopter production + cis to get marginal benefit of cis
  data[,result_w_impact_cis:=round(result_w_impact*(1+avloss),1)
       ][,marginal_cis:=result_w_impact_cis-result_w_impact
         ][is.na(marginal_cis),marginal_cis:=0
           ][is.na(result_w_impact_cis),result_w_impact_cis:=0
             ][,marginal_impact_cis:=marginal_cis+marginal_impact]
  
# 4) Calculate adoption benefits using VoP approach ####
  data_benefit<-data[exposure == "vop_usd17"][,c("exposure"):=NULL]

  # Duplicate dataset for BCR values
  N1<-rep(1:nrow(data_benefit),each=length(bcrs))
  N2<-rep(1:length(bcrs),nrow(data_benefit))
  data_benefit<-data.table(
    data_benefit[N1],
    bcr=bcrs[N2]
  )
  

  #data_benefit[,cost:=(value+marginal_impact_cis)/bcr
  #          ][,nr:=(value+marginal_impact_cis)-cost]
  
  data_benefit[,project_benefit:=marginal_impact_cis-marginal_impact_cis/bcr]
  
  # You can use this section to manually validate the project benefit formula
  if(F){
    income_with<-303137.2
    income_without<-275551.7
    marginal_impact_cis<-27585.5
    bcr<-1.62
    cost_with<-income_with/bcr
    cost_without<-income_without/bcr
    cost_diff<-cost_with-cost_without
    (project_benefit<-marginal_impact_cis-cost_diff)
  }
  
  data_benefit<-data_benefit[,!c("result_w_impact","marginal_impact","value","result","cv","avloss","marginal_impact_cis","result_w_impact_cis","marginal_cis")]
  
  # Set benefits to be integer to reduce file size
  data_benefit[,project_benefit:=as.integer(project_benefit)]
  
# 4.1) Save dataset ####
  arrow::write_parquet(data_benefit,sink=paste0(save_dir,"/roi_data.parquet"))
  