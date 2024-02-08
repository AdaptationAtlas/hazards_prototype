# !!!***This script assumes you have run the level 0.1_fao_production_cv.R script***!!! ####
# TO DO: Harvested Areas Also Need to be adjusted to near present? #####


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
              "s3fs",
              "arrow",
              "FinCal",
              "ggplot2")

# Call the function to install and load packages
load_and_install_packages(packages)

# Create functions ####
# Create a wrapper for the fincal function
irr_wrap <- function(project_cost, years, project_benefits) {
  sapply(years, FUN = function(YEAR) {
    if (YEAR %in% 0) {
      return(as.numeric(NA))
    } else {
      cashflows <- c(-project_cost, project_benefits[1:YEAR])
      # Use tryCatch to handle errors
      result <- tryCatch({
        suppressWarnings(jrvFinance::irr(cashflows))
      }, error = function(e) {
        # In case of an error, return NA
        as.numeric(NA)
      })
      return(result)
    }
  })
}

# 1) Load data ####

# THIS FILE NEEDS UPLOADING TO THE BUCKET #####
file<-"C:/Users/Peter Steward/OneDrive - CGIAR/Projects/EiA/Regional Prioritization/PAiCE/SPAM/mapspam_complete_extraction_adm1.csv"

if(!file.exists(file)){
  if(!dir.exists(dirname(file))){
    dir.create(dirname(file),recursive = T)
  }
  
  #s3_file <-"s3://digital-atlas/risk_prototype/data/exposure/exposure_adm_sum.parquet"
  #s3fs::s3_file_download(path=s3_file,new_path = file,overwrite = T)
}

#exposure<-data.table(arrow::read_parquet(file))
# Subset to admin1
#exposure<-exposure[is.na(admin2_name)][,admin2_name:=NULL]

exposure<-fread(file)
exposure<-exposure[technology=="all"][,list(exposure,admin0_name,admin1_name,crop,value)]

# CV in production
cv<-fread("Data/fao/faostat_prod_cv.csv")

# 2) Set parameters and save directory ####
years<-15
adoption<-c(0.005,0.01,0.02)
prod_impact<-c(0.05,0.1,0.2,0.33,0.5,1)
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
data<-cbind(exposure[N1],combinations[N2])

# 3) Work out marginal change ####

y_cols<-paste0("y",1:years)

for(i in 1:years){
  if(i==1){
  # For year one the amount of the value adopted is simply the value at year 0 x adoption
  data[,x:=round(value*adoption,3)]
  }else{
    # Years >1 the cumulative amount under adoption is:
    # ((value at year 0 - cumulative adoption value from previous year)*adoption_rate) + cumulative adoption value from previous year
    cname<-paste0("y",i-1)
    data[,(cname):=x]
    data[,x:=round((value-x)*adoption,3)+x]
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

# Add marginal benefit to production
  # 1) Multiply the cumulative adoption amount (value of production) by production benefit of adoption
  # 2) Substract the cumulative adoption amount to get the difference in production over non-adoption
data[exposure %in% c("vop_USD17","vop","production"),result_w_impact:=result*(1+prod_impact)
             ][,marginal_impact:=result_w_impact-result]


# TEMP subset for development
#data<-data[admin1_name=="" & admin0_name=="Kenya" & crop=="bean"]

# 4) VoP only approach ####
  data_benefit<-data[exposure == "vop_USD17"][,c("exposure"):=NULL]
  
  # Duplicate dataset for BCR values
  N1<-rep(1:nrow(data_benefit),each=length(bcrs))
  N2<-rep(1:length(bcrs),nrow(data_benefit))
  data_benefit<-data.table(
    data_benefit[N1],
    bcr=bcrs[N2]
  )

  #data_benefit[,cost:=(value+marginal_impact)/bcr
  #          ][,nr:=(value+marginal_impact)-cost]
  
  data_benefit[,project_benefit:=marginal_impact-marginal_impact/bcr]
  
  
  arrow::write_parquet(data_benefit[,!c("result_w_impact","marginal_impact","value","result")],sink=paste0(save_dir,"/roi_data.parquet"))
  

# 5) Select crops and geography ####
admin0_choice<-"Kenya"
admin1_choice<-data_benefit[admin0_name==admin0_choice,sample(unique(admin1_name),4,replace=F)]

priority_crops<-data[exposure=="harvested_area" & admin0_name %in% admin0_choice & admin1_name %in% admin1_choice
][,list(value=mean(value)),by=crop
][order(value,decreasing = T)
][value>100]

crops_choice<-priority_crops$crop[1:4]

  # 5.1) Subset data ####
data_merge_ss<-data_benefit[admin0_name %in% admin0_choice & admin1_name %in% admin1_choice & crop %in% crops_choice]

# sum results
data_sum<-data_merge_ss[,list(project_benefit=sum(project_benefit,na.rm=T)),by=list(adoption,prod_impact,year,bcr )]

# Re-sort dataset so that years are in order, this is required for irr calculations
data_sum<-data_sum[order(adoption,prod_impact,bcr,year)]

# 6) Economic indicators ####
discount_rate_choice<-5
project_year_choice<-5
project_cost<-10^6

# Calculate IRR
data_sum[,irr:=as.numeric(irr_wrap(project_cost,years=year,project_benefits=project_benefit)),by=list(adoption,prod_impact,bcr)]

# Add discount rates
data_sum<-data.table(
  data_sum[rep(1:nrow(data_sum),each=length(discount_rates))],
  discount_rate=discount_rates[rep(1:length(discount_rates),nrow(data_sum))]
)


# Calculate
A[,npv:=(project_benefit/(1+discount_rate/100)^year)-project_cost
         ][,bcr:=npv/project_cost]

# Under what conditions is NPV positive?
unique(data_sum[npv>0,list(adoption,prod_impact,discount_rate,year,npv)][order(npv,decreasing=T)])

# 7) Adoption rate % table ####
ad_rate<-data.table(value=1,adoption=seq(0.0025,0.03,0.0025))

for(i in 1:years){
  if(i==1){
    # For year one the amount of the value adopted is simply the value at year 0 x adoption
    ad_rate[,x:=value*adoption]
  }else{
    # Years >1 the cumulative amount under adoption is:
    # ((value at year 0 - cumulative adoption value from previous year)*adoption_rate) + cumulative adoption value from previous year
    cname<-paste0("y",i-1)
    ad_rate[,(cname):=x]
    ad_rate[,x:=((value-x)*adoption)+x]
  }
  
  if(i==years){
    cname<-paste0("y",i)
    setnames(ad_rate,"x",cname)
    col_order<-c(setdiff(colnames(ad_rate), cname), cname)
    ad_rate <- ad_rate[,..col_order ]
  }
}

ad_rate<-melt(ad_rate[,value:=NULL],id.vars=c("adoption"),variable.name = "year")
ad_rate[,year:=as.numeric(gsub("y","",year))][,adoption:=as.factor(adoption)]

# Create the plot
label_positions <- ad_rate[, .(value = max(value)), by = .(adoption, year)]
label_positions <- label_positions[year == max(year), ]
label_positions[,adoption:=as.numeric(as.character(adoption))][,adoption:=paste0(100*adoption,"%")]

ggplot(ad_rate, aes(x = year, y = value, group = adoption, color = adoption, linetype = adoption)) +
  geom_line() +
  geom_point() +
  scale_x_continuous(name = "Year", breaks = unique(ad_rate$year)) +  # Show each year on the x-axis
  scale_y_continuous(name = "Proportion Adopted") +
  labs(title = "Adoption Rate Over Time", subtitle = "Different rates of adoption across years") +
  theme_minimal() +
  theme(axis.line.x = element_line(color = "black", linewidth = 0.5),  # Add x-axis line
        axis.line.y = element_line(color = "black", linewidth = 0.5), # Add y-axis line
        plot.margin = margin(0,1.5,0,0,"cm"),
        legend.position = "none") + 
  geom_text(data = label_positions, aes(label = adoption, y = value ), hjust = -0.2, vjust = -0, color="black", show.legend = FALSE) # Adjust label positions

# Assuming ad_rate is your data.frame or data.table
ad_rate[,adoption_perc:=100*as.numeric(as.character(adoption))]
ggplot(ad_rate, aes(x = factor(year), y = adoption_perc, fill = value)) +
  geom_tile(color = "white") +  # Add white borders for better tile distinction
  scale_fill_gradient(low = "white", high = "black", name = "Adoption\nProportion") +  # Light to dark gradient
  labs(x = "Years", y = "Adoption Rate (%)", title = "Heatmap of Adoption Proportion over Time") +
  scale_y_continuous(breaks = unique(ad_rate$adoption_perc)) +  # Show each year on the x-axis
  theme_minimal() +
  theme(axis.title.x = element_text(size = 14, face = "bold"),   # Bold and larger x-axis title
        axis.title.y = element_text(size = 14, face = "bold"),   # Bold and larger y-axis title
        axis.text.x = element_text(size = 12, face = "bold"),    # Bold and larger x-axis text
        axis.text.y = element_text(size = 12, face = "bold"))   # Bold and larger y-axis text
#geom_shadowtext(data = ad_rate, aes(label = sprintf("%.2f", value), x = factor(year), y = adoption),
#               color = "black", bg.color = "white", size = 4, position = position_nudge(y = 0), lineheight = 0.5)



# 8) Old Approach ####

  # 8.1) Work out benefits ####
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
  

  # 8.2) Economic indicators ####
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
  
  

  


