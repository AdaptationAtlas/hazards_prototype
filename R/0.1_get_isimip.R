packages<-c("rISIMIP")
remotes::install_github('https://github.com/RS-eco/rISIMIP')

listISIMIP(path="I:/", version="ISIMIP3b", type="landuse", 
           scenario="histsoc", var="5crops", startyear=1861, endyear=2005)


# 1850soc: This represents a historical simulation using socio-economic conditions as they were in 1850. This scenario is used to understand the impact of climate under pre-industrial socio-economic conditions.
# 2015soc: This represents a historical simulation using socio-economic conditions as they were in 2015. This scenario helps to understand the impact of climate under contemporary socio-economic conditions.
# histsoc: This represents a historical simulation using actual socio-economic conditions over the historical period. It is used to understand the impact of climate under the evolving socio-economic conditions over time.