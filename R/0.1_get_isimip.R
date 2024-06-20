packages<-c("rISIMIP")

options(timeout = 600)
remotes::install_github("https://github.com/RS-eco/rISIMIP")

# 1850soc: This represents a historical simulation using socio-economic conditions as they were in 1850. This scenario is used to understand the impact of climate under pre-industrial socio-economic conditions.
# 2015soc: This represents a historical simulation using socio-economic conditions as they were in 2015. This scenario helps to understand the impact of climate under contemporary socio-economic conditions.
# histsoc: This represents a historical simulation using actual socio-economic conditions over the historical period. It is used to understand the impact of climate under the evolving socio-economic conditions over time.

base_url<-"https://files.isimip.org/ISIMIP3b/OutputData"
topic<-"water_global"
model<-c("WaterGAP2-2e","H08","CWatM")
gcms<-c("gfdl-esm4","ipsl-cm6a-lr","mpi-esm1-2-hr","mri-esm2-0","ukesm1-0-ll")
scenarios<-c("historical_histsoc","ssp126_2015soc","ssp370_2015soc","ssp585_2015soc")
variables<-c("evap","qg","qr","qs","qtot","groundwstor","dis")

datasets<-data.table(expand.grid(topic=topic,model=model,gcms=gcms,scenarios=scenarios,variable=variables))
datasets[,timeframe:="2015_2100"][scenarios=="historical_histsoc",timeframe:="1850_2014"]
datasets[variable=="evap" & model!="H08",variable:="evap-total"]
datasets[,gcms2:=gcms][scenarios!="historical_histsoc",gcms2:=paste0(gcms,"/future")]
datasets[,scenarios2:= paste0(unlist(tstrsplit(scenarios,"_",keep=1)),"/")][scenarios2!="historical/",scenarios2:=""]
datasets[model=="CWatM",scenarios:=gsub("2015soc","2015soc-from-histsoc",scenarios)]

datasets[,dl_path:=paste0(base_url,"/",
                          topic,"/",
                          model,"/",
                          gcms2,"/",
                          scenarios2,
                          tolower(model),"_",
                          gcms,
                          "_w5e5_",
                          scenarios,
                          "_default_",
                          variable,
                          "_global_monthly_",
                          timeframe,
                          ".nc"
                          )]

datasets$dl_path[2]

# website
CWatM/gfdl-esm4/future/cwatm_gfdl-esm4_w5e5_ssp126_2015soc-from-histsoc_default_evap-total_global_monthly_2015_2100.nc# local
CWatM/gfdl-esm4/future/cwatm_gfdl-esm4_w5e5_ssp126_2015soc_default_evap-total_global_monthly_2015_2100.nc 

url_exists <- function(url) {
  response <- HEAD(url)
  return(status_code(response) == 200)
}

check<-lapply(200:nrow(datasets),FUN=function(i){
  a<-datasets$dl_path[i]
  exists<-url_exists(a)
  cat(exists,gsub("https://files.isimip.org/ISIMIP3b/OutputData/water_global/","",a),"\n")
  exists
})
