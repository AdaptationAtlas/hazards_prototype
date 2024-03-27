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
              "ggplot2",
              "treemap")

# Call the function to install and load packages
load_and_install_packages(packages)

# Create functions ####


# Generate a test dataset to validate functions
#cashflows<-data_sum[adoption==0.02 & prod_impact==1 & cis_impact==0.05 & discount_rate==4,project_benefit-project_cost]


# Create a function to calculate internal rate of return

irr_wrap <- function(cashflow) {
  
  sapply(1:length(cashflow), FUN = function(i) {
    
    cashflow_ss <- cashflow[1:i]
      # Use tryCatch to handle errors
      result <- tryCatch({
        suppressWarnings(jrvFinance::irr(cashflow_ss))
      }, error = function(e) {
        # In case of an error, return NA
        as.numeric(NA)
      })
      return(result)
    
  })
}

npv_wrap <- function(cashflows,discount_rate) {
  sapply(1:length(cashflows), FUN = function(i) {
    
    cashflows <- cashflows[1:i]
    # Use tryCatch to handle errors
    result <- tryCatch({
      suppressWarnings(jrvFinance::npv(cf=cashflows,rate=discount_rate))
    }, error = function(e) {
      # In case of an error, return NA
      as.numeric(NA)
    })
    return(result)
    
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

  # 1.1) Load value of production data #####

# Crops
file<-"Data/exposure/crop_vop_usd17_adm_sum.parquet"

if(!file.exists(file)){
  if(!dir.exists(dirname(file))){
    dir.create(dirname(file),recursive = T)
  }
  
  s3_file <-"s3://digital-atlas/risk_prototype/data/exposure/crop_vop_usd17_adm_sum.parquet"
  s3fs::s3_file_download(path=s3_file,new_path = file,overwrite = T)
}

data_vop_usd17<-arrow::read_parquet(file)

# Livestock
file<-"Data/exposure/livestock_vop_usd17_adm_sum.parquet"

if(!file.exists(file)){
  if(!dir.exists(dirname(file))){
    dir.create(dirname(file),recursive = T)
  }
  
  s3_file <-"s3://digital-atlas/risk_prototype/data/exposure/livestock_vop_usd17_adm_sum.parquet"
  s3fs::s3_file_download(path=s3_file,new_path = file,overwrite = T)
}

data_vop_usd17_ls<-arrow::read_parquet(file)
data_vop_usd17_ls<-data_vop_usd17_ls[!grepl("total",crop)]

# Combine crops and livestock
data_vop_usd17<-rbind(data_vop_usd17,data_vop_usd17_ls)

  # 1.2) Load adoption data ####
  file<-"Data/roi/adoption_rates_perc.parquet"
  
  if(!file.exists(file)){
    if(!dir.exists(dirname(file))){
      dir.create(dirname(file),recursive = T)
    }
    
    s3_file <-"s3://digital-atlas/risk_prototype/data/roi/adoption_rates_perc.parquet"
    s3fs::s3_file_download(path=s3_file,new_path = file,overwrite = T)
  }
  
  data_adoption<-arrow::read_parquet(file)
  
# 2) User sets up project  ####
  # 2.1) Geography  #####
  # This code is just to select some geographies and crops that are interesting, it is not intended for the UI
  admin0_choice<-c("Kenya")
  
  # 2.2) Crops  #####
  
    # 2.2.1) Value of crops ####
    crop_values<-data_vop_usd17[admin0_name %in% admin0_choice & is.na(admin1_name)
                       ][,value_Musd:=round(value/10^6,0)
                         ][order(value,decreasing=T)
                           ][,list(admin0_name,crop,value_Musd)
                             ][,crop:=gsub("_"," ",crop)
                               ][,crop:=paste0(crop," $",value_Musd,"M")]
    

    # Assuming 'data' is your data frame and it looks like the one in the image
    # Replace 'data' with the actual name of your data frame
    
    # Create a scaled version of the value_Musd column to range from 0 to 1
    scaled_values <- scale(crop_values$value_Musd, 
                           center = min(crop_values$value_Musd), 
                           scale = max(crop_values$value_Musd) - min(crop_values$value_Musd))
    
    # Use the scaled values to get a gradient color from the YlGn palette
    get_color <- colorRampPalette(RColorBrewer::brewer.pal(9, "YlGn"))(100)
    crop_values$color <- get_color[as.numeric(cut(scaled_values, breaks = 100))]
    
    # Create the treemap with the manually created color gradient
    treemap(crop_values,
            index = c("crop"),     # The column that defines the grouping
            vSize = "value_Musd",  # The column that defines the size of the rectangles
            vColor = "color",      # The column that defines the manually created color gradient
            title = "Treemap of Crop Values",
            type="color",
            fontsize.title = 18)
    
        
        # 2.2.2) Choose crops ####
    crops_choice<-c("maize")
    
    data_vop_usd17[admin0_name %in% admin0_choice & is.na(admin1_name) & crop %in% crops_choice,paste0("total crop value ($M) = ",round(sum(value)/10^6,1))]
    
 
    
  # 2.3) Cost #####
  project_cost<-50*10^6
  
  # 2.4) Duration #####
  project_years<-8
  
  # 2.5) Impact ####
  prod_impact_choice<-0.3
  cis_impact_choice<-0.25
  adoption_rate_choice<-0.01
  
  # 2.5.1) Show adoption ####
   data_adoption[adoption==adoption_rate_choice,list(year,adoption_perc)
                 ][year<=project_years]

  # 2.6) Discount rate ####
  discount_rate<-0.08
  
  # 2.5) User sets return start year (min = 1)
  return_start<-1
  
# 3) Prepare data ####
  # 3.1) Subset data #####
  data_ss<-data_benefit[admin0_name %in% admin0_choice & 
                          is.na(admin1_name) &
                          crop %in% crops_choice &
                          prod_impact == prod_impact_choice &
                          adoption == adoption_rate_choice &
                          cis_impact == cis_impact_choice &
                          year %in% 1:project_years]
  
  # 3.2) Sum benefits #####
  data_ss<-data_ss[,list(project_benefit=sum(project_benefit,na.rm=T)),by=list(adoption,prod_impact,cis_impact,bcr,year)]
  
  # 3.3) Re-sort dataset so that years are in order, this is required for irr calculations #####
  data_ss<-data_ss[order(adoption,prod_impact,cis_impact,bcr,year)]
  
  # 3.4) Add year0 where project benefit is zero #####
  data_ss<-data_ss[c(1,1:nrow(data_ss))]
  data_ss[1,c("year","project_benefit"):=list(0,0)]
  
  # 3.5) Add cost data #####
  # Here we are assuming an even disbursement schedule, however we may want to give the user the option to specify this manually
  disbursement_schedule<-project_cost/project_years
  data_ss[,cost:=0][1:project_years,cost:=disbursement_schedule][,cost_cum:=cumsum(cost)]
  
  # 3.6) Adjust data_sum according to return year ####
  # results start in year 1, if results > year 1 then we need to shift benefit start dates
  if(return_start>1){
    benefit<-data_ss$project_benefit
    benefit<-c(rep(0,return_start),benefit[2:(length(benefit)-return_start+1)])
    data_ss[,project_benefit:=benefit]
  }
  
  # 3.7) Add cashflow ####
  data_ss[,cashflow:=project_benefit-cost]
  
# 4) Calculate economic indicators ####

  # 4.1) Calculate IRR ####
  data_ss[,irr:=as.numeric(irr_wrap(cashflow = cashflow))]
  
  # 4.3) Calculate NPV ####
  data_ss[,npv:=npv_wrap(cashflow = cashflow,discount_rate = discount_rate)] 

  # 4.3) Calculate BCR ####
  data_ss[,project_bcr:=npv/cost_cum]

  print(data_ss)
  
  c(
  data_ss[nrow(data_ss),paste0("IRR = ",round(irr*100,2),"%")],
  data_ss[nrow(data_ss),paste0("NPV = $",round(npv/10^6,2),"M")],
  data_ss[nrow(data_ss),paste0("BCR = ",round(project_bcr,2))]
  )
  
  write.table(data_ss,"clipboard",sep="/t",row.names = F)
  

  
# 5) Misc - Example of Adoption rate % table ####
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


