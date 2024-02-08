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

# 1) Load precooked roi data ####
file<-"Data/roi/roi_data.parquet"

if(!file.exists(file)){
  if(!dir.exists(dirname(file))){
    dir.create(dirname(file),recursive = T)
  }
  
  s3_file <-"s3://digital-atlas/risk_prototype/data/roi/roi_data.parquet"
  s3fs::s3_file_download(path=s3_file,new_path = file,overwrite = T)
}

data_benefit<-arrow::read_parquet(file)

  # 1.1) Set number of discount rate the user can choose from ####
discount_rates<-c(4,8,12)

# 2) User selects crops and geography ####

# This code is just to select some geographies and crops that are interesting, it is not intended for the UI
admin0_choice<-"Kenya"
admin1_choice<-data_benefit[admin0_name==admin0_choice,sample(unique(admin1_name),4,replace=F)]

priority_crops<-data_benefit[admin0_name %in% admin0_choice & admin1_name %in% admin1_choice
                             ][,list(value=mean(project_benefit)),by=crop
                               ][order(value,decreasing = T)
                                 ][value>100]

crops_choice<-priority_crops$crop[1:4]

# 3) Subset data ####
data_merge_ss<-data_benefit[admin0_name %in% admin0_choice & admin1_name %in% admin1_choice & crop %in% crops_choice]

# sum results
data_sum<-data_merge_ss[,list(project_benefit=sum(project_benefit,na.rm=T)),by=list(adoption,prod_impact,cis_impact,bcr,year)]

# Re-sort dataset so that years are in order, this is required for irr calculations
data_sum<-data_sum[order(adoption,prod_impact,cis_impact,bcr,year)]

# 4) Calculate economic indicators ####
  # 4.1) User sets a project cost ####
  project_cost<-10^6
  
  # 4.2) Calculate IRR ####
  data_sum[,irr:=as.numeric(irr_wrap(project_cost,years=year,project_benefits=project_benefit)),by=list(adoption,prod_impact,cis_impact,bcr)]
  
  # Add discount rates
  data_sum<-data.table(
    data_sum[rep(1:nrow(data_sum),each=length(discount_rates))],
    discount_rate=discount_rates[rep(1:length(discount_rates),nrow(data_sum))]
  )
  
  # Change bcr name
  setnames(data_sum,"bcr","farm_bcr")
  
  # 4.3) Calculate npv and bcr ####
  
  data_sum[,npv:=(project_benefit/(1+discount_rate/100)^year)-project_cost
           ][,project_bcr:=npv/project_cost]
  
  # This is the final table the user can interrogate by adoption, production impact, cis_impact, bcr ratio for farming system used to estimate cost,
  # year and discount rate
  
  # Reorder cols
  data_sum<-data_sum[,list(adoption,prod_impact,cis_impact,farm_bcr,year,project_benefit,irr,discount_rate,npv,project_bcr)]
  
  
  # Under what conditions is NPV positive?
  unique(data_sum[npv>0,list(adoption,prod_impact,discount_rate,year,npv)][order(npv,decreasing=T)])
  
# Misc - Example of Adoption rate % table ####
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


