require(terra)
require(data.table)

# Create functions ####
hazard_stacker<-function(i,folders_x_hazards,model_names,use_crop_cal,r_cal,save_dir){
  variable<-folders_x_hazards$hazards[i]
  scenario<-folders_x_hazards$folders[i]
  
  haz_files<-list.files(paste0(scenario,"/",variable),".tif",recursive=F,full.names = T)
  haz_files<-haz_files[!grepl("AVAIL.tif",haz_files)]
  
  n<-0
  
  # While loop to investigate strange and unrepeatable error (i.e. if you specify i & j and run from outside the loop there is no issue)
  # Possibly relates to NTx40 or PTOT
  while(is.na(haz_files[1]) & n<10){
    print(paste0("cc = ",use_crop_cal," | ",scenario,"-",variable))
    print(paste0("i=",i," | haz_files is returning NA | attempt = ",n))
    haz_files<-list.files(paste0(scenario,"/",variable),".tif",recursive=F,full.names = T)
    haz_files<-haz_files[!grepl("AVAIL.tif",haz_files)]
    n<-n+1
  }
  
  years<-unique(as.numeric(gsub(".tif","",unlist(tstrsplit(unlist(tail(tstrsplit(haz_files,"/"),1)),"-",keep=2)))))
  
  if(any(grepl("mean",haz_files))){
    haz_files_mean<-grep("mean",haz_files,value=T)
    haz_files_max<-grep("max",haz_files,value=T)
    haz_files<-list(mean=haz_files_mean,max=haz_files_mean)
  }else{
    haz_files<-list(mean=haz_files)
  }
  
  X<-""
  for(k in 1:length(haz_files)){
    
    #print(paste0("i=",i," j=",j," k=",k))
    variable2<-if(length(haz_files)>1){
      paste0(variable,"_",names(haz_files)[k])
    }else{
      variable2<-variable
    }
    
    stat<-unlist(haz_meta[variable.code==variable2,"function"])
    
    haz_files1<-haz_files[[k]]
    
    savename<-paste0(save_dir,"/",scenario,"_",variable2,"_",stat,".tif")
    
    if(!file.exists(savename)){
      if(folders_x_hazards$hazards[i]=="TAI"){
        # Display progress
        cat('\r                                                                                                                                          ')
        cat('\r',paste0("cc = ",use_crop_cal," | fixed = ",!use_eos," | ",scenario,"-",variable2,"-",stat))
        flush.console()
        
        haz_rast_years<-terra::rast(haz_files1)
        names(haz_rast_years)<-years
        
        # Remove last year of data (to be compatible with monthly derived hazards)
        haz_rast_years<-haz_rast_years[[1:(nlyr(haz_rast_years)-1)]]
        
        haz_rast_years
      }else{
        
        # Load all months of hazard data
        haz_rast<-terra::rast(haz_files1)
        
        # Force into memory (more efficient, less read operations)
        haz_rast<-haz_rast+0
        
        # Remove problematic -9999 values from precipitation data
        if(variable=="PTOT"){
          haz_rast<-terra::classify(haz_rast, cbind(-Inf, 0, NA), right=FALSE)
        }
        
        # Copy planting and harvest months
        plant<-r_cal$planting_month
        
        if(use_crop_cal=="yes"){
          harvest<-r_cal$maturity_month
          # Where plant>harvest (e.g. plant = 11 harvest = 3) add 12 to harvest (e.g. plant = 11 harvest = 15)
          harvest[plant[]>harvest[]]<-harvest[plant[]>harvest[]]+12
        }else{
          # Calculate harvest for the year following planting
          harvest<-plant+11
        }
        
        plant_min<-min(plant[],na.rm=T)
        harvest_max<-max(harvest[],na.rm=T)
        
        # Loop through years, note the final year is removed in case the harvest date extends beyond the end of the dataset
        haz_rast_years<-terra::rast(lapply(1:(length(years)-1),FUN=function(m){
          # Display progress
          cat('\r                                                                                                                                          ')
          cat('\r',paste0("cc = ",use_crop_cal," | fixed = ",!use_eos," | season = ",season," | ",scenario,"-",variable2,"-",stat,"-",years[m]," | i = ",i))
          flush.console()
          
          #plant1<-plant+12*(m-1)
          #harvest1<-harvest+12*(m-1)
          
          # Subset haz_rast to increase efficiency
          x = haz_rast[[(plant_min+12*(m-1)):(harvest_max+12*(m-1))]]
          
          haz_rast1<-terra::rapp(x,
                                 first=plant,
                                 last=harvest,
                                 fun=if(stat=="sum"){sum}else{
                                   if(stat=="max"){max}else{
                                     if(stat=="mean"){mean}else{if(stat=="min"){min}else{stop("invalid stat function supplied")}}}},
                                 na.rm=T)
          
          #haz_rastX<-terra::rapp(haz_rast,
          #                       first=plant1,
          #                       last=harvest1,
          #                       fun=if(stat=="sum"){sum}else{
          #                         if(stat=="max"){max}else{
          #                           if(stat=="mean"){mean}else{if(stat=="min"){min}else{stop("invalid stat function supplied")}}}},
          #                       na.rm=T)

          names(haz_rast1)<-years[m]
          haz_rast1
        }))
      }
      terra::writeRaster(haz_rast_years,savename,overwrite=T)
      rm(haz_rast_years)
      gc()
    }
    X[k]<-savename
  }
  
  X
  
}

# Set directories  ####
working_dir<-"/home/jovyan/common_data/atlas_hazards/cmip6/indices"
setwd(working_dir)

# Where will time series be saved?
output_dir<-"/home/jovyan/common_data/atlas_hazards/cmip6/indices_seasonal"
#output_dir<-"indices_seasonal"

# Load a base raster
base_rast<-terra::rast(list.files("historical/HSH",".tif",full.names = T)[1])

# Set sos calendar directory
sos_dir<-"/home/jovyan/common_data/atlas_sos/seasonal_mean"

# combine sos crop calendars
files<-list.files(sos_dir,".RData")

# List hazard folders
folders<-list.dirs(recursive=F)
folders<-folders[!grepl("ENSEMBLE|ipyn|gadm0|hazard_comb|indices_seasonal",folders)]
folders<-folders[!grepl("ssp126|ssp370|2061_2080|2081_2100",folders)] # these scenarios have incomplete information
folders<-unlist(tstrsplit(folders,"/",keep=2))

model_names<-unique(unlist(tstrsplit(folders[!grepl("histor",folders)],"_",keep=2)))

# Load sos data estimated from historical data, here SOS is calculated for each season and then averaged
sos_data<-rbindlist(lapply(files,FUN=function(file){
  X<-miceadds::load.Rdata2(file=file,path=sos_dir)
  X$admin0<-gsub(".RData","",file)
  X
}),use.names=T)[,list(admin0,x,y,S1,S2,E1,E2,LGP1,LGP2,Tot.Rain1,Tot.Rain2)]

# Fix seasonal allocation for specific countries
N<-sos_data[,which(S1>30 & ((is.na(S1)+is.na(S2))==1) & admin0=="Angola")]
sos_data[N,E2:=E1]
sos_data[N,E1:=NA]
sos_data[N,LGP2:=LGP1]
sos_data[N,LGP1:=NA]
sos_data[N,Tot.Rain2:=Tot.Rain1]
sos_data[N,Tot.Rain1:=NA]
sos_data[N,S2:=S1]
sos_data[N,S1:=NA]
sos_data[,admin0:=NULL]

# Update dekads to months
sos_data[,S1:=as.numeric(format(as.Date((S1*10),origin = as.Date("2024-01-01")),"%m"))]
sos_data[,S2:=as.numeric(format(as.Date((S2*10),origin = as.Date("2024-01-01")),"%m"))]
sos_data[,E1:=as.numeric(format(as.Date((E1*10),origin = as.Date("2024-01-01")),"%m"))]
sos_data[,E2:=as.numeric(format(as.Date((E2*10),origin = as.Date("2024-01-01")),"%m"))]
sos_data[,LGP1:=ceiling(LGP1/3)]
sos_data[,LGP2:=ceiling(LGP2/3)]

# If start and end month are the same then add a month to the end month
sos_data[S1==E1,E1:=E1+1]
sos_data[S2==E2,E2:=E2+1]
sos_data[E1==13,E1:=1]
sos_data[E2==13,E2:=1]

N<-sos_data[,which(is.na(S1))]

sos_data[is.na(S1),c("S1","S2"):=list(S2,S1)]
sos_data[is.na(E1),c("E1","E2"):=list(E2,E1)]
sos_data[is.na(LGP1),c("LGP1","LGP2"):=list(LGP2,LGP1)]

# covert to raster
sos_rast<-terra::rast(as.data.frame(sos_data)[,c("x","y","S1","S2","E1","E2","LGP1","LGP2")], type="xyz", crs="+proj=longlat +datum=WGS84 +no_defs", digits=3, extent=NULL)
sos_rast<-terra::resample(sos_rast,base_rast)

# Choose hazards #####
# Read in climate variable information
haz_meta<-unique(data.table::fread("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_metadata.csv")[,c("variable.code","function")])

# Also available are NTx30:NTx50
hazards<-c("HSH","NDWL0", "NDWS","NTx35","NTx40", "PTOT" , "TAI" ,  "TAVG" , "THI","TMIN","TMAX") 

hazards<-c("NDWL0", "NDWS","NTx35","NTx40") 

# Add in more heat thresholds
if(F){
  hazards2<-c("NTx36","NTx37","NTx38","NTx39","NTx41","NTx42","NTx43","NTx44","NTx45","NTx30","NTx31","NTx32","NTx33","NTx34","NTx46","NTx47","NTx48","NTx49","NTx50")
  haz_meta<-rbind(haz_meta,data.table(variable.code=hazards2,`function`="mean"))
  hazards<-c(hazards,hazards2)
}

# Run analysis loop   ####
doParallel<-F
use_crop_cal_choice<- c("no","yes") # if set to no then values are calculated for the year (using the jagermeyr cc as the starting month for each year)
use_sos_cc_choice<-c("no","yes") # Use onset of rain layer to set starting month of season

# Use end of season layer?
use_eos_choice<-c(F,T) # If use_sos_cc is "yes" use eos as estimated using Aridity Index? If set to "no" then season_length argument will be used to fix the season length
season_length<-4 # This can be varied to create season lengths for different crops (2,3,4,5,6,7,8 months) - gets messy when two season are present?

for(use_crop_cal in use_crop_cal_choice){
  for(use_sos_cc in use_sos_cc_choice){
    for(use_eos in use_eos_choice){
      print(paste0("use_crop_cal = ",use_crop_cal," | use_sos_cc = ",use_sos_cc," | use_eos = ", use_eos))
      
      # Set directory for output files
      if(use_crop_cal=="yes"){
        save_dir1<-paste0(output_dir,"/by_season")
      }else{
        save_dir1<-paste0(output_dir,"/by_year/hazard_timeseries")
      }
      
      if(!dir.exists(save_dir1)){
        dir.create(save_dir1,recursive=T)
      }
      
      if(use_crop_cal=="yes"){
        if(use_sos_cc=="yes"){
          n_seasons<-2
        }else{
          n_seasons<-1
        }
      }else{
        n_seasons<-1
      }
      
      for(season in 1:n_seasons){
        
        # Load crop calendar - jagermeyer   ####
        jagermeyer_cc<-terra::rast("/home/jovyan/common_data/atlas_crop_calendar/intermediate/mai_rf_ggcmi_crop_calendar_phase3_v1.01_Africa.tif")
        jagermeyer_cc$planting_month<-as.numeric(format(as.Date(jagermeyer_cc$planting_day[],origin = as.Date("2024-01-01")),"%m"))
        jagermeyer_cc$maturity_month<-as.numeric(format(as.Date(jagermeyer_cc$maturity_day[],origin = as.Date("2024-01-01")),"%m"))
        jagermeyer_cc<-jagermeyer_cc[[c("planting_month","maturity_month")]]
        jagermeyer_cc<-terra::resample(jagermeyer_cc,base_rast)
        
        # Update save directory to create structures for different crop calendars within seasonal folder
        if(use_crop_cal=="yes"){
          
          if(use_sos_cc=="no"){
            r_cal<-jagermeyer_cc
            
            save_dir<-paste0(save_dir1,"/jagermeyr/hazard_timeseries")
            if(!dir.exists(save_dir)){
              dir.create(save_dir,recursive=T)
            }
            
            r_cal_file<-paste0(save_dir,"/crop_cal.tif")
            if(!file.exists(r_cal_file)){
              terra::writeRaster(r_cal,r_cal_file)
            }
            
          }else{
            s1_name<-if(use_eos==T){"primary_eos/hazard_timeseries"}else{paste0("primary_fixed_",season_length,"/hazard_timeseries")}
            s2_name<-if(use_eos==T){"secondary_eos/hazard_timeseries"}else{paste0("secondary_fixed_",season_length,"/hazard_timeseries")}
            
            save_dir<-paste0(save_dir1,"/sos_",if(season==1){s1_name}else{s2_name})
            if(!dir.exists(save_dir)){
              dir.create(save_dir,recursive=T)
            }
            
            sos_rast_file<-paste0(save_dir,"/crop_cal.tif")
            
            if(!file.exists(sos_rast_file)){
              terra::writeRaster(sos_rast,sos_rast_file)
            }
          }
          
        }else{
          r_cal<-jagermeyer_cc
          save_dir<-save_dir1
        }
        
        if(use_sos_cc=="yes" & use_crop_cal=="yes"){
          r_cal<-jagermeyer_cc
          # Major season
          if(season==1){
            r_cal$planting_month<-sos_rast$S1
            if(use_eos){
              r_cal$maturity_month<-sos_rast$E1
            }else{
              r_cal$maturity_month <-r_cal$planting_month+season_length-1
              r_cal$maturity_month [r_cal$maturity_month []>12]<-r_cal$maturity_month [r_cal$maturity_month[]>12]-12
            }
          }
          
          # Minor season
          if(season==2){
            r_cal$planting_month<-sos_rast$S2
            if(use_eos){
              r_cal$maturity_month<-sos_rast$E2
            }else{
              r_cal$maturity_month <-r_cal$planting_month+season_length-1
              r_cal$maturity_month [r_cal$maturity_month []>12]<-r_cal$maturity_month [r_cal$maturity_month[]>12]-12
            }
          }
        }
        
        folders_x_hazards<-data.table(expand.grid(folders=folders,hazards=hazards))
        
        lapply(1:nrow(folders_x_hazards),
               FUN=hazard_stacker,
               folders_x_hazards=folders_x_hazards,
               model_names=model_names,
               use_crop_cal=use_crop_cal,
               r_cal=r_cal,
               save_dir=save_dir)
        
        
        # Create ensembles
        files<-list.files(save_dir,".tif")
        files<-files[grepl("ssp",files)]
        files<-files[!grepl("ENSEMBLE",files)]
        
        models<-unique(paste0(unlist(tstrsplit(files,paste(hazards,collapse="|"),keep=1))))
        models<-gsub("0-","0",models)
        models<-models[!grepl("expos",models)]
        
        scenario<-unique(unlist(tstrsplit(models,"_",keep=1)))
        time<-unique(paste0(unlist(tstrsplit(models,"_",keep=3)),"_",unlist(tstrsplit(models,"_",keep=4))))
        
        scen_haz_time<-expand.grid(scenario=scenario,hazards=hazards,time=time,stringsAsFactors =F)
        
        
        for(i in 1:nrow(scen_haz_time)){
          # Display progress
          cat('\r                                                                                                                                          ')
          cat('\r',paste0("Ensembling: cc = ",use_crop_cal," | fixed = ",!use_eos," | season = ",season," | ",scen_haz_time$scenario[i]," | ",scen_haz_time$hazards[i]," | ",scen_haz_time$time[i]," - ",i))
          flush.console()
          
          haz_files<-list.files(save_dir,scen_haz_time$hazards[i],full.names = T)
          haz_files<-haz_files[!grepl("historic",haz_files)]
          haz_files<-grep(scen_haz_time$scenario[i],haz_files,value = T)
          haz_files<-grep(scen_haz_time$time[i],haz_files,value = T)
          
          var<-gsub(".tif","",unlist(tstrsplit(haz_files,paste0("_",scen_haz_time$hazards[i],"_"),keep=2)))
          var_unique<-unique(var)
          
          for(p in 1:length(var_unique)){
            haz_files_ss<-haz_files[var==var_unique[p]]
            savename_ensemble<-gsub(paste0(model_names,collapse="|"),"ENSEMBLE",haz_files_ss[1])
            savename_ensemble_mean<-gsub("_ENSEMBLE_","_ENSEMBLEmean_",savename_ensemble)
            savename_ensemble_sd<-gsub("_ENSEMBLE_","_ENSEMBLEsd_",savename_ensemble)
            
            if(!file.exists(savename_ensemble_mean)){
              rast_list<-lapply(haz_files_ss,FUN=function(X){
                terra::rast(X)
              })
              
              # mean
              haz_rast<-terra::rast(lapply(1:terra::nlyr(rast_list[[1]]),FUN=function(j){
                terra::app(terra::rast(lapply(rast_list,"[[",j)),mean,na.rm=T)
              }))
              names(haz_rast)<-names(rast_list[[1]])
              
              terra::writeRaster(haz_rast,savename_ensemble_mean,overwrite=T)
              
              # sd
              haz_rast<-terra::rast(lapply(1:terra::nlyr(rast_list[[1]]),FUN=function(j){
                terra::app(terra::rast(lapply(rast_list,"[[",j)),sd,na.rm=T)
              }))
              names(haz_rast)<-names(rast_list[[1]])
              
              terra::writeRaster(haz_rast,savename_ensemble_sd,overwrite=T)
              
              rm(haz_rast)
              gc()
            }
            
            
          }
          
        }
        
      }
    }
  } 
}
