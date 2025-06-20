season_choice<-"annual"
files<-list.files(file.path(atlas_dirs$data_dir$hazard_risk_vop_usd,season_choice),"parquet",full.name=T)

severity_choice<-"severe"
ntx_var<-paste0("NTx",toupper(substr(severity_choice,1,1)))
hazard_vars_fixed<-c("NDWS+NTx35+NDWL0","NDWS+THI-max+NDWL0")
hazard_vars_variable<-c(paste0("PTOT-L+",ntx_var,"+PTOT-G"),"PTOT-L+THI-max+PTOT-G") # Note that NTx will reflect severity (NTxM, NTxS, or NTxE)

ensemble_mean_file<-grep("ENSEMBLEmean_int.*severe.*parquet$",files,value=T)
cat(ensemble_mean_file)

ensemble_sd_file<-grep("ENSEMBLEsd_int.*severe.*parquet$",files,value=T)
cat(ensemble_sd_file)

mean_dat<-data.table(read_parquet(ensemble_mean_file))
sd_dat<-data.table(read_parquet(ensemble_sd_file))

admin0_sel<-c("Kenya","Uganda")
admin1_sel<-NULL
admin2_sel<-NULL

mean_dat_ss<-mean_dat[admin0_name %in% admin0_sel & hazard_vars %in% hazard_vars_variable]
sd_dat_ss<-sd_dat[admin0_name %in% admin0_sel & hazard_vars %in% hazard_vars_variable]
setnames(sd_dat_ss,"value","value_sd")

data_ss<-merge(mean_dat_ss,sd_dat_ss,all.x=T)
data_ss[value>0]

comm_sel<-c("maize","common-bean","barley")



files<-list.files(file.path(atlas_dirs$data_dir$hazard_risk,season_choice),"sd.*tif",full.name=T)
x<-rast(files[1])

files<-list.files(file.path(atlas_dirs$data_dir$hazard_risk_vop,season_choice),"ENSEMBLEsd.*tif",full.name=T)
x<-rast(files[1])

files<-list.files(file.path(atlas_dirs$data_dir$hazard_timeseries_mean,season_choice),"ENSEMBLE.*tif$",full.name=T)
x<-rast(files[1])
plot(x)

haz_time_risk_dir