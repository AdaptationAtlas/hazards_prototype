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
              "FinCal")

# Call the function to install and load packages
load_and_install_packages(packages)

# Create a wrapper for the fincal function
irr_wrap <- function(project_cost, years, project_benefits) {
  sapply(years, FUN = function(YEAR) {
    if (YEAR %in% 1) {
      return(as.numeric(NA))
    } else {
      cashflows <- c(-project_cost, project_benefits[1:YEAR])
      # Use tryCatch to handle errors
      result <- tryCatch({
        jrvFinance::irr(cashflows)
      }, error = function(e) {
        # In case of an error, return NA
        as.numeric(NA)
      })
      return(result)
    }
  })
}

# 1) Load data ####
file<-"Data/exposure/exposure_adm_sum.parquet"

if(!file.exists(file)){
  if(!dir.exists(dirname(file))){
    dir.create(dirname(file),recursive = T)
  }
  
  s3_file <-"s3://digital-atlas/risk_prototype/data/exposure/exposure_adm_sum.parquet"
  s3fs::s3_file_download(path=s3_file,new_path = file,overwrite = T)
}

exposure<-data.table(arrow::read_parquet(file))

# 2) Create controls ###
adoption<-c(0.005,0.01,0.02)
prod_impact<-seq(0.1,1,0.1)
discount_rates<-0:15

combinations<-data.table(expand.grid(adoption=adoption,prod_impact=prod_impact))

N1<-rep(1:nrow(exposure),each=nrow(combinations))
N2<-rep(1:nrow(combinations),nrow(exposure))
data<-cbind(exposure[N1],combinations[N2])

# Subset to admin1 and value of production in USD2017
data<-data[is.na(admin2_name)][,admin2_name:=NULL]

# 3) Work out marginal change ####

# Set number of years to assess
years<-10
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
data<-data[order(admin0_name,admin1_name,exposure,crop,adoption,prod_impact)]

# 3.1) Work out benefits ####
data_benefit<-data[exposure %in% c("vop","vop_usd17") & !grepl("highland|tropical",crop)]

# To work out the marginal gain in VoP: 
  # 1) Multiply the cumulative adoption amount (value of production) by production benefit of adoption
  # 2) Substract the cumulative adoption amount to get the difference in production over non-adoption
data_benefit[, result := (result * (1+prod_impact))-result]
setnames(data_benefit,"result","marginal_gain")

# Without the project gross income is the total vop x the number of years
# "ith the project gross income is gross income without the project plus the marginal gain in income
data_benefit[,gross_inc_without:=value*year]
data_benefit[,gross_inc_with:=gross_inc_without+marginal_gain]

# 3.2) Subset areas ####
data_ha<-data[exposure=="ha" & crop %in% data_benefit$crop]

# 4) Select crops and geography ####
admin0_choice<-"Kenya"
admin1_choice<-data[admin0_name==admin0_choice,sample(unique(admin1_name),4,replace=F)]

priority_crops<-data_ha[admin0_name %in% admin0_choice & admin1_name %in% admin1_choice
][,list(value=mean(value)),by=crop
][order(value,decreasing = T)
][value>100]

crops_choice<-priority_crops$crop[1:4]

# 4.1) Subset data ####
data_cost<-data_ha[admin0_name %in% admin0_choice & admin1_name %in% admin1_choice & crop %in% crops_choice]
data_benefit_ss<-data_benefit[admin0_name %in% admin0_choice & admin1_name %in% admin1_choice & crop %in% crops_choice]

# 5) Enter Costs ####

# Costs in USD/ha
cost_adopter<-200
cost_non_adopter<-150

# Difference in cost between adopters and non-adopters
cost_diff<-cost_adopter-cost_non_adopter

# Marginal cost is the cumulative area under adoption (ha) x the difference in cost between adopter and non-adopter
# Cost without the project is the total area x the number of years x the cost for non-adoption
# Cost with the project is the cost without plus the marginal change in cost
data_cost<-data_cost[,marginal_cost:=result*cost_diff
                      ][,cost_without:=year*value*cost_non_adopter
                        ][,cost_with:=cost_without+marginal_cost
                          ][,result:=NULL]

# 6) Merge benefits and costs ####

data_merge<-merge(data_benefit_ss[,!"exposure"],data_cost[,!c("value","exposure")])
data_merge[,project_benefit:=marginal_gain-marginal_cost]

# sum results
cols<-c("marginal_gain","gross_inc_without","gross_inc_with","marginal_cost","cost_without","cost_with","project_benefit")
data_sum<-data_merge[,lapply(.SD, sum,na.rm=T), .SDcols = cols,by=list(adoption,prod_impact,year)]

# 7) Economic indicators ###
discount_rate_choice<-5
project_year_choice<-5
project_cost<-10^6

# Calculate IRR
data_sum[,irr:=irr_wrap(project_cost,years=years,project_benefits=project_benefit),by=list(adoption,prod_impact)]

# Add discount rates
data_sum<-data.table(
  data_sum[rep(1:nrow(data_sum),each=length(discount_rates))],
  discount_rate=discount_rates[rep(1:length(discount_rates),nrow(data_sum))]
)

# Resort dataset so that years are in order, this is required for irr calculations
data_sum<-data_sum[order(adoption,prod_impact,year,discount_rate)]

# Calculate
data_sum[,npv:=(project_benefit/(1+discount_rate/100)^year)-project_cost
         ][,bcr:=npv/project_cost]


# Under what conditions is NPV positive?
unique(data_sum[npv>0,list(adoption,prod_impact,discount_rate,year,npv)][order(npv,decreasing=T)])


project_benefits<-rnorm(10,90000,sd=10000)

irr_wrap(project_cost,years=1:10,project_benefits=rnorm(10,200000,sd=100))

?jrvFinance::irr

# 6) Set-up Project & Choose Discount Rate ####

cols<-colnames(data_cost)[-(1:7)]









