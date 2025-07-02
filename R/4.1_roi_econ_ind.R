# List of packages to be loaded
pacman::p_load(data.table, 
              s3fs,
              arrow,
              jrvFinance,
              ggplot2,
              treemap)


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

# Continuous compounding npv function (not used)
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

# Single NPV calculation (Discrete compounding)
npv_discrete <- function(cashflows, discount_rate) {
  periods <- seq_along(cashflows) - 1  # Year 0 is not discounted
  sum(cashflows / (1 + discount_rate)^periods)
}

# Cumulative NPV (year-by-year) calculation
npv_discrete_cum <- function(cashflows, discount_rate) {
  sapply(seq_along(cashflows), function(i) {
    npv_discrete(cashflows[1:i], discount_rate)
  })
}

# Mirr function
mirr <- function(cashflows, finance_rate, reinvest_rate) {
  n <- length(cashflows) - 1
  inflows <- ifelse(cashflows > 0, cashflows, 0)
  outflows <- ifelse(cashflows < 0, cashflows, 0)
  
  # Future value of inflows (compounded to end)
  FV_inflows <- sum(inflows * (1 + reinvest_rate)^(n - seq_along(cashflows) + 1))
  
  # Present value of outflows (discounted to start)
  PV_outflows <- sum(outflows / (1 + finance_rate)^(seq_along(cashflows) - 1))
  
  if (PV_outflows == 0 || FV_inflows <= 0) return(NA_real_)
  
  return((FV_inflows / abs(PV_outflows))^(1 / n) - 1)
}

mirr_wrap <- function(cashflows, finance_rate = 0.08, reinvest_rate = 0.08) {
  sapply(seq_along(cashflows), function(i) {
    tryCatch({
      mirr(cashflows[1:i], finance_rate, reinvest_rate)
    }, error = function(e) NA_real_)
  })
}

# Cumulative adoption function
add_cumulative_adoption <- function(dt, annual_rate = 0.01, colname = "cum_adoption") {
  stopifnot("year" %in% names(dt))
  stopifnot(all(dt$year == sort(dt$year)))  # assumes ordered years
  
  dt <- copy(dt)  # avoid modifying original
  
  # Get unique years
  years <- sort(unique(dt$year))
  n_years <- length(years)
  
  # Calculate cumulative adoption over time (recursive)
  cum <- numeric(n_years)
  for (i in seq_len(n_years)) {
    if (i == 1) {
      cum[i] <- annual_rate
    } else {
      cum[i] <- cum[i - 1] + (1 - cum[i - 1]) * annual_rate
    }
  }
  
  # Add to table by year
  dt[, (colname) := cum[match(year, years)]]
  
  return(dt)
}

# 1) Load precooked roi data ####
file<-file.path(atlas_dirs$data_dir$roi,"roi_data_v2.parquet")
data_benefit<-arrow::read_parquet(file)

  # 1.1) Load value of production data #####

  # Crops
  file<-file.path(atlas_dirs$data_dir$exposure,"exposure_adm_sum.parquet")
  
  data_vop_usd17<-data.table(arrow::read_parquet(file))
  data_vop_usd17<-data_vop_usd17[,exposure:=paste0(exposure,"_",unit)
    ][is.na(admin2_name) & (tech=="all"|is.na(tech))
    ][,c("admin2_name","unit","tech"):=NULL
    ][exposure %in% c("vop_usd15")]
    
    # Combine highland and lowland cattle
  data_vop_usd17<-data_vop_usd17[,crop:=gsub("-tropical|-highland","",crop)][,list(value=sum(value,na.rm=T)),by=list(admin0_name,admin1_name,crop,exposure)]
    
  data_vop_usd17<-data_vop_usd17[!grepl("other-|rest-of|temperate-fruit|tropical-fruit|vegetables",crop)]
  
  # 1.2) Load adoption data ####
  file<-file.path(atlas_dirs$data_dir$roi,"adoption_rates_perc.parquet")
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
   data_adoption[adoption==adoption_rate_choice & year<=project_years]

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
  
  # 3.2) Sum benefits across commodities #####
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
  
  # Discounted IRR (MIRR) ####
  data_ss[, mirr := mirr_wrap(cashflow, finance_rate = discount_rate, reinvest_rate = discount_rate)]
  
  # 4.3) Calculate NPV ####
  data_ss[,npv:=npv_discrete_cum(cashflows = cashflow,discount_rate = discount_rate)]

  # 4.3) Calculate BCR ####
  data_ss[, discounted_cost_cum := npv_discrete_cum(cost, discount_rate)]
  data_ss[, discounted_benefit_cum := npv_discrete_cum(project_benefit, discount_rate)]
  data_ss[, discounted_bcr := discounted_benefit_cum / discounted_cost_cum]
  
  # 4.4) Add cumulative adoption ####
  data_ss<-add_cumulative_adoption(data_ss,annual_rate=adoption_rate_choice)
  
  print(data_ss)
  
  cat(
  "End of project ( year",project_years,"):\n",
  "cost =",project_cost,"\n",
  "discount rate = ",discount_rate,"\n",
  data_ss[nrow(data_ss),paste0("IRR = ",round(irr*100,2),"%")],"\n",
  data_ss[nrow(data_ss),paste0("MIRR = ",round(mirr*100,2),"%")],"\n",
  data_ss[nrow(data_ss),paste0("NPV = $",round(npv/10^6,2),"M")],"\n",
  data_ss[nrow(data_ss),paste0("BCR = ",round(discounted_bcr,2))]
  )
  
  data_ss
  
  # Plot results
  # Base plot style
  theme_lineplot <- theme_minimal(base_size = 14) +
    theme(panel.grid.minor = element_blank(),
          legend.position = "bottom")
  
 
  
  # IRR and MIRR over time
  ggplot(data_ss) +
    geom_line(aes(x = year, y = irr, color = "IRR"), size = 1.1) +
    geom_line(aes(x = year, y = mirr, color = "MIRR"), size = 1.1) +
    scale_color_manual(values = c("IRR" = "purple", "MIRR" = "orange")) +
    labs(title = "Internal Rates of Return (IRR & MIRR)",
         x = "Year", y = "Rate", color = "Metric") +
    theme_lineplot
  
  
  # BCR
  
  # Define scaling constants manually
  bcr_range <- range(data_ss$discounted_bcr, na.rm = TRUE)
  benefit_range <- range(data_ss$project_benefit_cum, na.rm = TRUE)
  
  # Calculate rescale factor
  scale_factor <- diff(benefit_range) / diff(bcr_range)
  
  # Shift bcr so it visually aligns
  data_ss[, bcr_scaled := discounted_bcr * scale_factor]
  data_ss[, adoption_scaled := cum_adoption * scale_factor]
  
  ggplot(data_ss, aes(x = year)) +
    # Bars for cumulative adoption
    geom_col(aes(y = adoption_scaled, fill = "Cumulative Adoption"), alpha = 0.3, width = 0.8) +
    
    # Lines for benefits and BCR
    geom_line(aes(y = project_benefit_cum, color = "Cumulative Benefit"), size = 1.1) +
    geom_line(aes(y = discounted_benefit_cum, color = "Discounted Benefit"), size = 1.1) +
    geom_line(aes(y = discounted_cost_cum, color = "Discounted Cost"), size = 1.1) +
    geom_line(aes(y = bcr_scaled, color = "Discounted BCR"), size = 1.1, linetype = "dashed") +
    
    # Horizontal reference lines
    geom_hline(yintercept = project_cost, linetype = "dotted", color = "black", size = 0.8) +
    geom_hline(yintercept = 1 * scale_factor, linetype = "dashed", color = "gray40", size = 0.8) +
    
    # Labels for horizontal lines
    annotate("text", 
             x = 0, 
             y = project_cost, 
             label = "Total Project Cost", 
             vjust = -0.8, hjust = 0, size = 4.2, fontface = "italic") +
    annotate("text", 
             x = max(data_ss$year), 
             y = 1 * scale_factor, 
             label = "BCR = 1 (Break-even)", 
             vjust = -0.8, hjust = 1, size = 4.2, fontface = "italic", color = "gray40") +
    
    # Axes and scales
    scale_y_continuous(
      name = "Cost & Benefit (USD)",
      sec.axis = sec_axis(~ . / scale_factor,
                          name = "Benefit-Cost Ratio / Adoption")
    ) +
    scale_color_manual(
      name = "Line Metrics",
      values = c("Cumulative Benefit" = "darkgreen",
                 "Discounted Benefit" = "purple",
                 "Discounted Cost" = "red",
                 "Discounted BCR" = "steelblue")
    ) +
    scale_fill_manual(
      name = "Bar Metric",
      values = c("Cumulative Adoption" = "gray60")
    ) +
    
    # Labels and themes
    labs(title = "Project Returns and Adoption Over Time",
         x = "Year") +
    theme_minimal(base_size = 14) +
    theme(
      legend.position = "bottom",
      legend.box = "vertical",
      legend.title = element_text(size = 11),
      legend.text = element_text(size = 10)
    ) +
    guides(
      color = guide_legend(order = 1),
      fill = guide_legend(order = 2)
    )
    
  
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

ggplot(ad_rate, aes(x = year, y = 100*value, group = adoption, color = adoption, linetype = adoption)) +
  geom_line() +
  geom_point() +
  scale_x_continuous(name = "Year", breaks = unique(ad_rate$year)) +  # Show each year on the x-axis
  scale_y_continuous(name = "% Adopted") +
  labs(title = "Adoption Rate Over Time", subtitle = "Different rates of adoption across years") +
  theme_minimal() +
  theme(axis.line.x = element_line(color = "black", linewidth = 0.5),  # Add x-axis line
        axis.line.y = element_line(color = "black", linewidth = 0.5), # Add y-axis line
        plot.margin = margin(0,1.5,0,0,"cm"),
        legend.position = "none") + 
  geom_text(data = label_positions, aes(label = adoption, y = 100*value ), hjust = -0.2, vjust = -0, color="black", show.legend = FALSE) # Adjust label positions

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


