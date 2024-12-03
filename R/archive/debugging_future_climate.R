data<-data_ex

files<-list.files(output_dir,"EC-Earth3",full.names=T)
files<-grep("TAVG",files,value=T)

data<-rbindlist(lapply(files,read_parquet),use.names = T)
data<-data[is.na(admin2_name)]
data<-data[admin0_name=="Togo" & admin1_name=="Kara"]

data1<-data[,.(value=mean(value)),by=.(admin0_name,admin1_name,variable,scenario,timeframe,model,year)]

data2<-data1[,.(value=round(mean(value),1),by=.(admin0_name,admin1_name,variable,scenario,timeframe,year)]
data2[,value:=round(value,1)]
library(ggplot2)

# Assuming your data is stored in a data frame called data2
ggplot(data2, aes(x = year, y = value, color = scenario, group = scenario)) +
  geom_line(size = 1) +
  labs(
    title = "Climate Scenario Trends Over Years",
    x = "Year",
    y = "Value",
    color = "Scenario"
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(hjust = 0.5),
    legend.position = "right"
  )
data2[,max(value)]