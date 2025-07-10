# ======================================================================
# Script: Calculate Adoption Benefits and ROI for Agriculture
# Project: Africa Agriculture Adaptation Atlas (AAAA)
# Lead: Pete Steward (p.steward@cgiar.org, ORCID: 0000-0003-3985-4911)
# Organization: Alliance of Bioversity International & CIAT
# Funder: Gates Foundation
# ----------------------------------------------------------------------
# Description:
# This script calculates the marginal production and climate information
# system (CIS) benefits of agricultural interventions based on varying
# adoption rates, impacts, and benefit-cost ratios (BCRs). It estimates
# avoided losses using a provided coefficient of variation (CV) dataset
# and computes final economic benefits in terms of value of production.
#
# Key Inputs:
# - exposure_adm_sum.parquet: Admin-level exposure data from MapSPAM
# - faostat_prod_cv.csv: FAOSTAT CV data for crop/livestock production
# - hpop_adm_sum.parquet: Rural population data (currently unused)
#
# Key Outputs:
# - roi_data_v2.parquet: Final dataset containing ROI benefit estimates
# - adoption_rates.parquet: Detailed adoption statistics by admin unit
# - adoption_rates_perc.parquet: Aggregated adoption rates (% adoption)
#
# Dependencies:
# Run these scripts before executing this one:
# 1. 0.6_process_exposure.R
#   Available at: https://github.com/AdaptationAtlas/hazards_prototype
#
# ======================================================================

options(scipen=999)

# List of packages to be loaded
pacman::p_load(data.table, 
              countrycode,
              s3fs,
              arrow,
              progressr,
              future,
              future.apply)

# Load functions & wrappers
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

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
  ## 1.1) Crops - MapSPAM ####
    file<-file.path(atlas_dirs$data_dir$exposure,"exposure_adm_sum.parquet")

    exposure<-data.table(arrow::read_parquet(file))
    exposure<-exposure[,exposure:=paste0(exposure,"_",unit)
                       ][is.na(admin2_name) & (tech=="all"|is.na(tech))
                         ][,c("admin2_name","unit","tech"):=NULL
                          ][exposure %in% c("vop_usd15","vop_intld15","prod_t")]
    
  # Combine highland and lowland cattle
    exposure<-exposure[,crop:=gsub("-tropical|-highland","",crop)][,list(value=sum(value,na.rm=T)),by=list(admin0_name,admin1_name,crop,exposure)]
    
    exposure<-exposure[!grepl("other-|rest-of|temperate-fruit|tropical-fruit|vegetables",crop)]
    
  ## x) (Not used) Human pop ####
  file<-file.path(atlas_dirs$data_dir$exposure,"hpop_adm_sum.parquet")
  hpop<-data.table(arrow::read_parquet(file))
  hpop<-hpop[is.na(admin2_name) & type == "rural"][,c("admin2_name","unit"):=NULL]
  
  ## 1.2) FAO CV in production ####
  file<-file.path(atlas_dirs$data_dir$fao,"faostat_prod_cv.csv")
  cv<-fread(file)
  
  # Add in admin0_name
  cv<-cv[,list(crop,admin0_name,value_filled)]
  
  setnames(cv,"value_filled","cv")
  
  # Remove milk and egg yields, keep meat
  cv<-cv[!grepl("milk|eggs",crop)
         ][,crop:=gsub("_meat","",crop)
           ][,crop:=stringi::stri_replace_all_regex(crop,c("goat$","pig$"),c("goats","pigs"),vectorize_all = F)
             ][crop!="total"]
  
  cv[,crop:=gsub(" ","-",crop)]
  
  # Note robusta coffee and small millet do not exist in FAOstat data so substitute values for arabica and pearl millet
  
  cv<-rbind(cv,
            cv[crop=="arabica-coffee"][,crop:="robusta-coffee"],
            cv[crop=="pearl-millet"][,crop:="small-millet"]
  )
  
# 2) Set parameters and save directory ####
years<-16
adoption<-c(0.005,0.01,0.02)
prod_impact<-c(0.1,0.2,0.3,0.4,0.5)
cis_impact<-c(0,0.1,0.25,0.5)
bcrs<-1.62
  # 2.1) Create combinations #####
  nrow(expand.grid(1:years,adoption,prod_impact,bcrs))
  
  save_dir<-roi_dir
  if(!dir.exists(save_dir)){
    dir.create(save_dir)
  }
  combinations<-data.table(expand.grid(adoption=adoption,prod_impact=prod_impact))
  
  # Combine exposure and impact tables
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
  
  data<-rbind(data,data_hpop[,!c("gaul0_code","gaul1_code","gaul2_code","type","stat","iso3")][,crop:="rural-pop"])

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

# Round value to reduced size and calculation demands
data[,value:=round(value,0)]

# 3.1) Separate population and production/vop ####
data_hpop<-data[crop=="rural-pop"][,prod_impact:=NULL]
setnames(data_hpop,c("crop","exposure","result"),c("variable","unit","adopters"))
data_hpop[,non_adopters:=value-adopters][,adoption_perc:=round(100*adopters/value,2)]

data_hpop<-unique(data_hpop)

adoption_file<-file.path(atlas_dirs$data_dir$roi,"adoption_rates.parquet")
arrow::write_parquet(data_hpop,adoption_file)

data_hpop<-unique(data_hpop[,list(adoption_perc=mean(adoption_perc,na.rm=T)),
                                    by=list(adoption,year)
                            ][,adoption_perc:=round(adoption_perc,1)])

adoption_file2<-file.path(atlas_dirs$data_dir$roi,"adoption_rates_perc.parquet")
arrow::write_parquet(data_hpop,adoption_file2)

# Remove population data, this is because we cannot easily assign human population to crops
# As we have removed population data we can also remove the geographic data
data<-data[crop!="rural-pop"]

  # 3.1) Add marginal benefit to production ####
    # a) Multiply the cumulative adoption amount (value of production) by production benefit of adoption
    # b) Substract the cumulative adoption amount to get the difference in production over non-adoption
  data<-data[,result_w_impact:=round(result*(1+prod_impact),1)
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
  
  # None matches?
  unique(data[is.na(cv),.(admin0_name,crop)])
  
  # Calculate % avoided loss
  # Note it appears the mean is not actually required in the avloss function as it returns a proportion that relates to
  # cv, the magnitude of the mean does not affect the outcome.
  if(F){
  data[value!=0,avloss:=abs(round(avloss(cv=1*cv[1],
                                         change=cis_impact[1],
                                         fixed=F,
                                         reps=10^6),4)),by=.(cv,cis_impact)]
  }
  
  # Setup parallel plan and progress handler
  set_parallel_plan(n_cores=20,use_multisession=T)
  handlers(global = TRUE)
  handlers("progress")
  
  
  # Extract only needed columns to a lighter object
  data_filtered <- unique(data[value != 0, .(cv, cis_impact)])
  
  with_progress({
    p <- progressor(along = seq_len(nrow(data_filtered)))
    
    # Only pass row indices, not the full table
    results <- future_lapply(seq_len(nrow(data_filtered)), function(i) {
      row_cv <- data_filtered$cv[i]
      row_impact <- data_filtered$cis_impact[i]
      av <- abs(round(avloss(cv = row_cv, change = row_impact, fixed = FALSE, reps = 1e6), 4))
      p()
      list(cv = row_cv, cis_impact = row_impact, avloss = av)
    })
  })
  
  plan(sequential)
  
  results_dt <- rbindlist(results)
  data <- merge(data, results_dt, by = c("cv", "cis_impact"), all.x = TRUE)
  
  # Multiply production of adopters by cis benefit, subtract adopter product from adopter production + cis to get marginal benefit of cis
  data[,result_w_impact_cis:=round(result_w_impact*(1+avloss),1)
       ][,marginal_cis:=result_w_impact_cis-result_w_impact
         ][is.na(marginal_cis),marginal_cis:=0
           ][is.na(result_w_impact_cis),result_w_impact_cis:=0
             ][,marginal_impact_cis:=marginal_cis+marginal_impact]
  
# 4) Calculate adoption benefits using VoP approach ####
  data_benefit<-data[exposure == "vop_usd15"][,c("exposure"):=NULL]

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
  data_benefit[,project_benefit:=round(project_benefit,0)]
  
# 4.1) Save dataset ####
  arrow::write_parquet(data_benefit,sink=file.path(atlas_dirs$data_dir$roi,"roi_data_v2.parquet"))
  