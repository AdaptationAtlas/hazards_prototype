
# Update geoboundary extraction with file containing admin_code
# Update mapspam to USD 2017
# Update NTx35 and NTx40 to NTxCROP with a crop specific threshold

# 1) NA values in hazard_risk_vop for hazard_vars
haz_risk_vop_dir<-paste0("Data/hazard_risk_vop/",timeframe_choice)

files<-list.files(haz_risk_vop_dir,"_adm_int",full.names = T)
haz_risk_vop<-rbindlist(lapply(1:length(files),FUN=function(i){
  print(files[i])
  if(file.exists(files[i])){
    arrow::read_parquet(files[i])
  }else{
    warning(paste0("File does not exist: ",files[i]))
    NULL
  }
}))

# Are NA values present in all sections?
  haz_risk_vop[is.na(admin2_name) & is.na(admin1_name),table(hazard_vars,useNA = "ifany")]
  haz_risk_vop[is.na(admin2_name) & !is.na(admin1_name)  == SEV,table(hazard_vars,useNA = "ifany")]
  haz_risk_vop[!is.na(admin2_name) & !is.na(admin1_name) ,table(hazard_vars,useNA = "ifany")]
  
  # Note that all sections apart from admin name and value seem to be NA, even severity, perhaps a merge issue?
  head(haz_risk_vop[is.na(hazard_vars)])

# Yes, debug 2 section 5.3


# 2) check dry/wet hazards co-occurence
haz_risk_vop_dir<-paste0("Data/hazard_risk_vop/",timeframe_choice)

files<-list.files(haz_risk_vop_dir,"_adm_int",full.names = T)
haz_risk_vop<-rbindlist(lapply(1:length(files),FUN=function(i){
  print(files[i])
  if(file.exists(files[i])){
    arrow::read_parquet(files[i])
  }else{
    warning(paste0("File does not exist: ",files[i]))
    NULL
  }
}))

haz_risk_vop[,unique(hazard_vars)]

haz_risk_vop[is.na(hazard_vars)]
hazards<-c("PTOT_L+NTx35+PTOT_G","NDWS+NTx35+NDWL0" ) 

options(scipen=999)
data<-haz_risk_vop[hazard_vars %in% hazards & admin0_name == "Kenya" & is.na(admin1_name) & scenario == "historic" & timeframe == "historic"]
data[crop=="maize" & severity=="severe"][order(hazard_vars,hazard)]
