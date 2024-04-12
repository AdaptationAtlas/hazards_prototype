#' Threshold Function
#'
#' This function calculates the fraction of elements in \code{Data} that are 
#' either greater or less than a specified threshold value.
#'
#' @param Data A numeric vector.
#' @param Threshold A numeric value representing the threshold value.
#' @param Direction A character string indicating the direction of the comparison. 
#'        It can be either '>' to indicate greater than, or '<' to indicate less than.
#'
#' @return A numeric value representing the fraction of elements in \code{Data} 
#'         that satisfy the specified condition.
#' 
#' @examples
#' ThreshFun(c(1, 2, 3, 4, 5), 3, '>')
#' ThreshFun(c(1, 2, 3, 4, 5), 3, '<')
#'
#' @export
ThreshFun<-function(Data,Threshold,Direction){
  if(Direction==">"){
    sum(Data>Threshold)/length(Data)
  }else{
    sum(Data<Threshold)/length(Data)
  }
}

#' Classify data based on threshold
#'
#' This function classifies data based on a given threshold and direction
#'
#' @param Data Numeric vector of data to be classified
#' @param Threshold Numeric value representing the threshold for classification
#' @param Direction Character vector specifying the direction of classification. 
#'   Available options are ">" (greater than) and "<" (less than)
#'
#' @return Integer vector indicating the classification result. 
#'   Returns 1 if the condition is met and 0 otherwise.
#'   
#' @examples
#' ClassSimple(c(4, 6, 3, 7), 5, ">")
#' ClassSimple(c(2, 9, 4, 7), 5, "<")
#'
#' @export
ClassSimple<-function(Data,Threshold,Direction){
  if(Direction==">"){
    as.integer(Data>Threshold)
  }else{
    as.integer(Data<Threshold)
  }
}

#' Exceedance
#'
#' This function calculates the exceedance value of a given data set based on a threshold.
#'
#' @param Data A numeric vector representing the data set.
#' @param Threshold A numeric value indicating the threshold to compare the data with.
#' @param Direction A string indicating the comparison direction. Options are '>' (greater than) and '<' (less than).
#' @param Function A string indicating the function to calculate the exceedance value. Options are 'mean' and 'max'.
#'
#' @return Returns the exceedance value according to the specified function. If there are no values exceeding the threshold, returns 0. If Data contains only NA, returns NA.
#'
#' @examples
#' # Calculate mean exceedance value for data greater than threshold
#' Data <- c(1, 2, 3, 4, 5)
#' Threshold <- 3
#' Exceedance(Data, Threshold, '>', 'mean')
#'
#' # Calculate maximum exceedance value for data less than threshold
#' Data <- c(5, 4, 3, 2, 1)
#' Threshold <- 3
#' Exceedance(Data, Threshold, '<', 'max')
#' @export
Exceedance<-function(Data,Threshold,Direction,Function){
  if(!all(is.na(Data))){
    if(Direction==">"){
      Data<-Data[Data>Threshold]
    }else{
      Data<-Data[Data<Threshold]
    }
    
    if(length(Data)>0){
      Data<-abs(Data-Threshold)
      
      if(Function=="mean"){
        Data<-mean(Data,na.rm=T)
      }
      
      if(Function=="max"){
        Data<-max(Data,na.rm=T)
      }
    }else{
      Data<-0
    }
    
  }else{
    Data<-NA
  }
  return(Data)
  
}

#' Replace Infinite Values with NA
#'
#' This function replaces infinite or -9999 values in a vector or matrix with NA.
#'
#' @param Data A vector or matrix containing numeric values.
#' @return The input vector or matrix with all infinite values replaced by NA.
#' @examples
#' x <- c(1, 2, 3, Inf, 5)
#' Infinite2NA(x)
#' # Output: 1  2  3 NA  5
#'
#' y <- matrix(c(1, Inf, 3, 4, 5, Inf), nrow = 2)
#' Infinite2NA(y)
#' # Output:      [,1] [,2]
#' #           [1,]    1   NA
#' #           [2,]    3    4
#' @export
Infinite2NA<-function(Data){
  Data[is.infinite(Data)]<-NA
  return(Data)
}

#' ClassifyFun
#'
#' Classify input data based on threshold values and directions.
#'
#' @param Data the input data
#' @param VAR the variable name
#' @param Threshold the classification threshold
#' @param Direction the classification direction
#' @param PropThreshold the proportion threshold
#' @param PropTDir the proportion classification direction
#'
#' @return a vector containing the mean, mean class, proportion, proportion class, exceedance mean, and exceedance max values
#' 
#' @export
ClassifyFun<-function(Data,VAR,Threshold,Direction,PropThreshold,PropTDir){
  Variable<-terra::app(Data,Infinite2NA)
  VarMean<- terra::app(Variable,mean,na.rm=T)
  VarMin<- terra::app(Variable,min,na.rm=T)
  VarMax<- terra::app(Variable,mean,na.rm=T)
  
  VarMClass<-terra::rast(lapply(1:length(Threshold),FUN=function(i){
    VarMClass<-VarMean
    VarMClass[]<-as.integer(ClassSimple(Data=VarMean[],Threshold[i],Direction[i]))
    VarMClass
  }))
  
  VarProp<-terra::rast(lapply(1:length(Threshold),FUN=function(i){
    terra::app(Variable,ThreshFun,Threshold[i],Direction[i])
  }))
  
  VarExceedMean<-terra::rast(lapply(1:length(Threshold),FUN=function(i){
    X<-terra::app(Variable,Exceedance,Threshold[i],Direction[i],Function="mean")
    if(Direction[i]=="<"){
      X<--X
    }
    X
  }))
  names(VarExceedMean)<-paste0(VAR,"_exceedmean_",Direction)
  
  VarExceedMax<-terra::rast(lapply(1:length(Threshold),FUN=function(i){
    X<-terra::app(Variable,Exceedance,Threshold[i],Direction[i],Function="max")
    if(Direction[i]=="<"){
      X<--X
    }
    X
  }))
  
  names(VarExceedMax)<-paste0(VAR,"_exceedmax_",Direction)
  
  if(length(Threshold)>1){
    VarMClass<-terra::app(VarMClass,max)
    VarProp<-terra::app(VarProp,sum)
  }
  
  VarPClass<-VarProp
  VarPClass[]<-as.integer(ClassSimple(Data=VarPClass[],PropThreshold,PropTDir))
  
  Var<-c(VarMean,VarMClass,VarProp,VarPClass)
  names(Var)<-paste0(VAR,c("_mean","_meanclass","_prop","_propclass"))
  Var<-c(Var,VarExceedMax,VarExceedMean)
  return(Var)
}

#' Descriptives Fun
#'
#' Return mean,min,max and deviation for variable stacks
#'
#' @param Data the input data
#' @param VAR the variable name
#' @return a vector containing the mean, min, max and sd of each pixel in the stack
#' 
#' @export
DescriptivesFun<-function(Data,VAR){
  Variable<-terra::app(Data,Infinite2NA)
  VarMean<- terra::app(Variable,mean,na.rm=T)
  VarMin<- terra::app(Variable,min,na.rm=T)
  VarMax<- terra::app(Variable,mean,na.rm=T)
  VarSD<- terra::app(Variable,sd,na.rm=T)
  
  Var<-c(VarMean,VarMin,VarMax,VarSD)
  names(Var)<-paste0(VAR,c("_mean","_min","_max","_sd"))
  return(Var)
}

#' Classify Hazards
#'
#' This function generates binary classifications of hazards based on a raster dataset.
#' 
#' @param Data A raster dataset.
#' 
#' @return A list with two components:
#' \itemize{
#'   \item \code{RastReclass}: A reclassified raster dataset with hazard labels.
#'   \item \code{Classes}: A data table containing the hazard code, hazard label, and recoded hazard value.
#' }
#' 
#' @importFrom terra rast nlyr
#' @importFrom data.table data.table
#' @importFrom stringi stri_reverse
#' @importFrom stringr str_count
#' @importFrom stringr str_locate_all
#' @importFrom stringr str_pad
#' 
#' @examples
#' # Example usage of the ClassifyHazards function
#' Data <- ...
#' ClassifyHazards(Data)
#' @export
ClassifyHazards<-function(Data){
  
  # Generate binary classification of hazards
  X<-sum(terra::rast(lapply(1:terra::nlyr(Data),FUN=function(i){
    X<-Data[[i]]
    X[which(X[]==1)]<-10^(i-1)
    X
  })))
  
  # Translate binary sequence to label
  labels<-names(Data)
  values<-unique(values(X))
  values<-values[!(is.na(values)|is.nan(values))]
  values<-str_pad(values, length(labels), pad = "0")
  
  
  labels_new<-unlist(lapply(values,FUN=function(val){
    val<-stringi::stri_reverse(val)
    pos<-str_locate_all(val,"1")[[1]][,1]
    paste(labels[pos],collapse="+")
  }))
  
  labels<-data.table(values=as.numeric(values),labels=labels_new)[,Nhaz:=0
  ][labels!="",Nhaz:=1+stringr::str_count(labels,"[+]")
  ][order(Nhaz,labels)][,N:=0:(.N-1)]
  
  labels[Nhaz==0,labels:="None"][,labels:=gsub("_meanclass|_propclass","",labels)]
  
  # Recode raster to integer values and add labels
  X<-subst(X,labels[,values],labels[,N])
  
  levels(X)<-labels[,list(N,labels)]
  
  Classes<-labels[,list(values,labels,N)]
  colnames(Classes)<-c("Code","Hazard","Recode")
  
  names(X)<-""
  
  X<-list(RastReclass=X,Classes=Classes)
  
  return(X)
}

#' Hazard Wrapper
#'
#' This function takes in various parameters and creates hazard maps based on given thresholds and scenarios.
#'
#' @param Thresholds A data frame specifying the threshold values for classifying the hazard.
#' @param FileName Name of the file.
#' @param SaveDir Directory to save the hazard maps.
#' @param PropThreshold Proportion threshold.
#' @param PropTDir Proportion threshold direction.
#' @param hazard_dir Directory containing the hazard files.
#' @param Scenarios Data frame specifying the scenarios for which hazard maps are to be created.
#' @param verbose If TRUE, display progress.
#' 
#' @return A list of hazard maps.
#'
#' @export
HazardWrapper<-function(Thresholds,SaveDir,PropThreshold,PropTDir,hazard_dir,Scenarios,verbose=F){
  Files<-list.files(hazard_dir,".tif",full.names = T)
  Files<-Files[grepl("historical|ENSEMBLE",Files) & !grepl("tif.aux.xml",Files)]
  
  Hazards<-lapply(1:nrow(Scenarios),FUN=function(j){
    for(i in 1:nrow(Thresholds)){
      if(verbose){
        # Display progress
        cat('\r                                                                                                                     ')
        cat('\r',paste0("Scenario ", Scenarios$Scenario[j],"-",j," | Hazard ",Thresholds$Variable[i],"-",i))
        flush.console()
      }
      
      Threshold_focal<-Thresholds[i]
      Threshold_focal[,Code:=paste0(Direction,threshold)
                      ][,Code:=gsub("<","L",Code)
                        ][,Code:=gsub(">","G",Code)]
      TCode<-Threshold_focal[,Code]
      
      
      save_file<-paste0(SaveDir,"/Haz-",Scenarios$Scenario[j],"-",Scenarios$Time[j],"-",Threshold_focal$Variable,"-",TCode,"-",PropThreshold,".tif")
      
      if(!file.exists(save_file)){
        
        Data<-terra::rast(Files[grepl(Scenarios$Scenario[j],Files) & grepl(Scenarios$Time[j],Files) & grepl(Threshold_focal$Variable,Files)])
        
        X<-ClassifyFun(Data=Data,
                       VAR=Threshold_focal$Variable,
                       Threshold=Threshold_focal$threshold,
                       Direction=Threshold_focal$Direction,
                       PropThreshold=PropThreshold,
                       PropTDir=PropTDir)
        terra::writeRaster(X,filename=save_file,overwrite=T)
      }else{
        X<-terra::rast(save_file)
      }
      
      names(X)[2:terra::nlyr(X)]<-paste0(names(X)[2:terra::nlyr(X)],"_",Threshold_focal$Severity_class)
      if(Threshold_focal$Direction=="<"){
        names(X)[2:terra::nlyr(X)]<-gsub("_<","", names(X)[2:terra::nlyr(X)])
        names(X)[2:terra::nlyr(X)]<-paste0( names(X)[2:terra::nlyr(X)],"_L")
      }
      
      if(Threshold_focal$Direction==">"){
        names(X)[2:terra::nlyr(X)]<-gsub("_>","", names(X)[2:terra::nlyr(X)])
        names(X)[2:terra::nlyr(X)]<-paste0( names(X)[2:terra::nlyr(X)],"_H")
      }
      
      if(i==1){
        Hazards<-X
      }else{
        Hazards<-c(Hazards,X)
      }
    }
    
    keep<-which(names(Hazards) %in% paste0(Threshold_focal[,unique(Variable)],"_mean"))
    Hazards<-Hazards[[-keep[-seq(1,Threshold_focal[,length(unique(Variable))*3],3)]]]
    Hazards
  })
  
  names(Hazards)<-paste0(Scenarios$Scenario,"-",Scenarios$Time)
  
  return(Hazards)
}
#' HazCombWrapper
#'
#' Combines hazard data for different scenarios and calculates mean and prop values.
#'
#' @param Hazards A list of hazard datasets for different scenarios and time periods.
#' @param SaveDir The directory where the output files will be saved.
#' @param Scenarios A data frame containing scenario and time information.
#' @param FileName The name of the output file.
#' @param SelectedHaz The selected hazard for combining.
#' @return A list containing the combined mean and prop hazard data for each scenario.
#' @export
HazCombWrapper<-function(Hazards,SaveDir,Scenarios,FileName,SelectedHaz){
  scenario_names<-paste0(Scenarios$Scenario,"-",Scenarios$Time)
  
  # Mean
  if(!file.exists(filename=paste0(SaveDir,"/CombMean-",scenario_names[1],"-",FileName,".tif"))){
    HazCombMean<-lapply(1:nrow(Scenarios),FUN=function(j){
      
      X<-grep("_meanclass",names(Hazards[[j]]),value=T)
      X<-X[grepl(paste0(SelectedHaz,collapse = "|"),X)]
      
      ClassifyHazards(Data=Hazards[[j]][[X]])
    })
    names(HazCombMean)<-scenario_names
    
    for(i in 1:length(scenario_names)){
      terra::writeRaster(HazCombMean[[scenario_names[i]]][["RastReclass"]],filename=paste0(SaveDir,"/CombMean-",scenario_names[i],"-",FileName,".tif"),overwrite=T)
      fwrite(HazCombMean[[scenario_names[i]]][["Classes"]],file=paste0(SaveDir,"/CombMean-",scenario_names[i],"-",FileName,".csv"))
    }
    
  }else{
    
    HazCombMean<-lapply(scenario_names,FUN=function(scenario_name){
      list(
        RastReclass=terra::rast(paste0(SaveDir,"/CombMean-",scenario_name,"-",FileName,".tif")),
        Classes=data.table::fread(paste0(SaveDir,"/CombMean-",scenario_name,"-",FileName,".csv"))
      )
    })
    names(HazCombMean)<-scenario_names
    
  }
  
  # Prop
  if(!file.exists(filename=paste0(SaveDir,"/CombProp-",scenario_names[1],"-",FileName,".tif"))){
    HazCombProp<-lapply(1:length(scenario_names),FUN=function(j){
      X<-grep("_propclass",names(Hazards[[j]]),value=T)
      X<-X[grepl(paste0(SelectedHaz,collapse = "|"),X)]
      ClassifyHazards(Data=Hazards[[j]][[X]])
    })
    
    names(HazCombProp)<-scenario_names
    
    for(i in 1:length(scenario_names)){
      terra::writeRaster(HazCombProp[[scenario_names[i]]][["RastReclass"]],filename=paste0(SaveDir,"/CombProp-",scenario_names[i],"-",FileName,".tif"),overwrite=T)
      fwrite(HazCombProp[[scenario_names[i]]][["Classes"]],file=paste0(SaveDir,"/CombProp-",scenario_names[i],"-",FileName,".csv"))
    }
    
  }else{
    
    HazCombProp<-lapply(scenario_names,FUN=function(scenario_name){
      list(
        RastReclass=terra::rast(paste0(SaveDir,"/CombProp-",scenario_name,"-",FileName,".tif")),
        Classes=data.table::fread(paste0(SaveDir,"/CombProp-",scenario_name,"-",FileName,".csv"))
      )
    })
    names(HazCombProp)<-scenario_names
  }
  
  HazComb<-list(MeanHaz=HazCombMean,PropHaz=HazCombProp)
  
  return(HazComb)
}

#' ExtractHaz
#'
#' Extract hazard information and calculate related metrics for regions of interest.
#'
#' @param Regions Spatial polygons representing the regions of interest.
#' @param Reg.Field Field in the \code{Regions} data containing the region names.
#' @param Hazard Spatial raster representing the hazard data.
#' @param HazTab Table containing hazard labels and corresponding codes.
#' @param Exposure Spatial raster representing the exposure data.
#' @param Cropland Spatial raster representing the cropland data.
#' @param TotalPop Spatial raster representing the total population data.
#' @return A list containing extracted exposure and crop population data for each region.
#' @export
ExtractHaz<-function(Regions,Reg.Field,Hazard,HazTab,Exposure,Cropland,TotalPop){
  
  Exposure <- terra::mask(terra::crop(Exposure,Regions),Regions)
  Hazard<-terra::mask(terra::crop(Hazard,Regions),Regions)
  Cropland <- terra::mask(terra::crop(Cropland,Regions),Regions)
  TotalPop<-terra::mask(terra::crop(TotalPop,Regions),Regions)
  
  Hazard<-terra::resample(Hazard,Exposure,method="near")
  
  TotalPop<-terra::resample(TotalPop,Exposure,method="sum")
  Cropland<-terra::resample(Cropland,Exposure,method="sum")
  
  # Create hazard mask
  HazardMask<-Hazard
  hlevs<-levels(HazardMask)[[1]]
  N<-hlevs[hlevs$labels=="None","N"]
  if(nrow(hlevs)>1){
    HazardMask<-terra::classify(HazardMask,matrix(c(N,NA),ncol=2))
    levels(HazardMask)<-hlevs[hlevs$labels!="None",]
  }else{
    HazardMask[]<-NA
  }
  MaskedPop<-terra::mask(TotalPop,HazardMask)
  MaskedCrop<-terra::mask(Cropland,HazardMask)
  names(MaskedPop)<-"AtRisk_Pop"
  names(MaskedCrop)<-"AtRisk_Cropland_Area"
  
  
  Regions$Code<-1:length(Regions)*100
  REG<-terra::rasterize(Regions,Hazard,field="Code")
  HAZ<-Hazard+REG
  names(HAZ)<-"Hazard"
  
  total_pop_tab<-data.table(zonal(TotalPop,REG,fun=sum,na.rm=T))
  total_pop_tab[,Region:=terra::values(Regions)[match(total_pop_tab$Code,Regions$Code),Reg.Field]]
  
  total_crop_tab<-data.table(zonal(Cropland,REG,fun=sum,na.rm=T))
  total_crop_tab[,Region:=terra::values(Regions)[match(total_crop_tab$Code,Regions$Code),Reg.Field]]
  
  X<-data.table(zonal(Exposure,HAZ,fun=sum,na.rm=T))
  X[,Region:=floor(Hazard/100)*100]
  X[,Hazard:=Hazard-Region]
  X[,Region:=terra::values(Regions)[match(X$Region,Regions$Code),Reg.Field]]
  X[,Hazard:=HazTab[match(X$Hazard,Recode),Hazard]]
  X<-data.table::melt(X,id.vars = c("Hazard","Region"),value.name = "Value",variable.name = "Crop")
  
  Y<-data.table(zonal(c(TotalPop,Cropland),HAZ,fun=sum,na.rm=T))
  Y[,Region:=floor(Hazard/100)*100]
  Y[,Hazard:=Hazard-Region]
  Y[,Region:=terra::values(Regions)[match(Y$Region,Regions$Code),Reg.Field]]
  Y[,Hazard:=HazTab[match(Y$Hazard,Recode),Hazard]]
  Y[,TotalPop_sum:=sum(TotalPop),by=Region]
  Y[,Cropland_Area_sum:=sum(Cropland_Area),by=Region]
  
  REG2<-terra::rasterize(Regions,Hazard,field=Reg.Field)
  Areas<-data.table(zonal(cellSize(REG2,unit="ha"),REG2,sum,na.rm=T))
  setnames(Areas,Reg.Field,"Region")
  
  Y[,Admin_Area:=Areas[match(Y$Region,Areas$Region),area]]
  
  Y<-Y[!is.na(Hazard)
  ][,Cropland_Perc:=round(100*Cropland_Area_sum/Admin_Area ,2)
  ][,Cropland_Risk_Perc:=round(100*Cropland_Area/Cropland_Area_sum ,2)
  ][,Pop_Risk_Perc:=round(100*TotalPop/TotalPop_sum ,2)]
  
  Y<-melt(Y[,!c("Cropland_Area","TotalPop")],id.vars=c("Region","Admin_Area","Cropland_Area_sum","Cropland_Perc","TotalPop_sum","Hazard"))
  
  Y1<-dcast(Y,Region+Admin_Area+Cropland_Area_sum+Cropland_Perc+TotalPop_sum+Hazard ~variable,value.var = c("value"))[Admin_Area!=0]
  
  return(list(Exposure=X,CropPop=Y1))
  
}

#' HazXRegionWrapper
#'
#' This function extracts hazard data for different regions and saves it to a file.
#'
#' @param SaveDir The directory where the output file will be saved.
#' @param FileName The name of the output file.
#' @param Exposure The exposure data.
#' @param Geographies A list of geographical regions.
#' @param HazComb A list of hazard combinations.
#' @param Cropland The cropland data.
#' @param TotalPop The total population data.
#'
#' @return A list containing hazard data for each region.
#'
#' @importFrom miceadds load.Rdata2
#'
#' @export
HazXRegionWrapper<-function(SaveDir,FileName,Exposure,Geographies,HazComb,Cropland,TotalPop){
  SaveFile<-paste0(SaveDir,"/Tables-",FileName,".RData")
  if(!file.exists(SaveFile)){
    
    HazXRegion<-lapply(1:length(HazComb),FUN=function(j){
      
      Data<-lapply(1:length(HazComb[[j]]),FUN=function(i){
        
        
        HazExt1<-ExtractHaz(Regions=Geographies$admin1,
                            Reg.Field="admin_name",
                            Hazard=HazComb[[j]][[i]]$RastReclass,
                            HazTab=HazComb[[j]][[i]]$Classes,
                            Exposure=Exposure,
                            Cropland=Cropland,
                            TotalPop=TotalPop)
        
        HazExt2<-ExtractHaz(Regions=Geographies$admin2,
                            Reg.Field="admin_name",
                            Hazard=HazComb[[j]][[i]]$RastReclass,
                            HazTab=HazComb[[j]][[i]]$Classes,
                            Exposure=Exposure,
                            Cropland=Cropland,
                            TotalPop=TotalPop)
        
        
        HazExt0<-ExtractHaz(Regions=Geographies$admin0,
                            Reg.Field="admin_name",
                            Hazard=HazComb[[j]][[i]]$RastReclass,
                            HazTab=HazComb[[j]][[i]]$Classes,
                            Exposure=Exposure,
                            Cropland=Cropland,
                            TotalPop=TotalPop)
        
        list(Admin0=HazExt0,Admin1=HazExt1,Admin2=HazExt2)
      })
      
      names(Data)<-names(HazComb[[j]])
      Data
    })
    
    names(HazXRegion)<-names(HazComb)
    
    save(HazXRegion,file=SaveFile)
  }else{
    HazXRegion<-miceadds::load.Rdata2(SaveFile)
  }
  return(HazXRegion)
}

#' Harmonize Categorical Columns
#'
#' This function takes a list of two raster objects with categorical columns and harmonizes the category colors of the second raster object to match the category colors of the first raster object.
#'
#' @param x A list of two raster objects.
#' @param Palette The name of the palette to be used for colormapping.
#'
#' @return A list of two raster objects with harmonized category colors.
#' 
#' @export
harmonize_rast_cat_cols<-function(x,Palette){
  Levels<-levels(x)
  
  Levels[[1]]$Match<-as.numeric(match(Levels[[1]][,2],Levels[[2]][,2]))
  
  # Set NA values in map1 to a value not present in map2
  n_nas<-is.na(Levels[[1]]$Match)
  if(sum(n_nas)>0){
    new_vals<-(max(Levels[[2]][,1])+1):(max(Levels[[2]][,1])+sum(n_nas))
    Levels[[2]][nrow(Levels[[2]]):(nrow(Levels[[2]])+length(new_vals)-1),1]<-new_vals
    Levels[[2]][nrow(Levels[[2]]):(nrow(Levels[[2]])+length(new_vals)-1),2]<-Levels[[1]][n_nas,2]
  }
  
  
  Levels[[2]]$cols<-PalFun(PalName=Palette,N=nrow(Levels[[2]]),Names=Levels[[2]]$category)
  Levels[[1]]$cols<-Levels[[2]]$cols[as.numeric(match(Levels[[1]][,2],Levels[[2]][,2]))]
  
  coltab(x[[1]])<-Levels[[1]][,c(1,4)]
  coltab(x[[2]])<-Levels[[2]][,c(1,3)]
  
  return(x)
}

#' PalFun
#'
#' Description: Generate color palettes based on various sources.
#'
#' @param PalName A character string specifying the name of the palette.
#' @param N An integer specifying the number of colors in the palette.
#' @param Names A character vector containing names for the colors in the palette.
#' @param invert 
#' @param alpha
#'
#' @return A character vector representing the color palette.
#'
#' @examples
#' PalFun("magma", 5, c("color1", "color2", "color3", "color4", "color5"))
#' PalFun("Set1", 3, c("color1", "color2", "color3"))
#' PalFun("Royal2", 8, c("color1", "color2", "color3", "color4", "color5", "color6", "color7", "color8"))
#'
#' @importFrom data.table data.table
#' @importFrom wesanderson wes_palettes
#' @importFrom MetBrewer MetPalettes
#' @importFrom viridis viridis
#' @importFrom colorRampPalette colorRampPalette
PalFun<-function(PalName,N,Names=NA,invert=F,alpha=1) {
  Viridis<-data.table(Source="viridis",Palette=c("magma","inferno","plasma","viridis","cividis","rocket","mako","turbo"))
  Met<-data.table(Source="MetBrewer",Palette=names(MetBrewer::MetPalettes))
  Wes<-data.table(Source="Wes",Palette=names(wesanderson::wes_palettes))
  Palettes<-rbind(Viridis,Met,Wes)
  
  if(Palettes[Palette==PalName,Source]=="viridis"){
    PAL<-viridis::viridis(N,option=PalName)
  }
  
  if(Palettes[Palette==PalName,Source]=="MetBrewer"){
    PAL<-MetBrewer::met.brewer(name=PalName, n=N, type="continuous")
  }
  
  if(Palettes[Palette==PalName,Source]=="Wes"){
    if(N>length(wes_palettes[[PalName]])){
      PAL<-wesanderson::wes_palette(name=PalName, n=length(wes_palettes[[PalName]]), type="continuous")
      PAL<-colorRampPalette(PAL)(N)
    }else{
      PAL<-wesanderson::wes_palette(name=PalName, n=N, type="continuous")
    }
  }
  
  if(invert){
    PAL<-rev(PAL)
  }
  
  if(alpha!=1){
    PAL<-add.alpha(PAL,alpha=alpha)
  }
  
  if(!(is.na(Names)|is.null(Names))){
    names(PAL)<-Names
  }
  
  
  return(PAL)
}

#' Extract Trends Data
#'
#' This function extracts trends data from raster files based on given geographies and scenarios_x_hazards.
#'
#' @param Geographies A list of geographies to be aggregated by admin level chosen.
#' @param scenarios_x_hazards A data frame specifying scenarios, time periods, and hazards.
#' @param DataDir The directory containing the raster files.
#' @param SaveDir The directory to save the trends data.
#' @param haz_class A data frame specifying hazard classification.
#' @param haz_names A character vector specifying hazard names.
#'
#' @return A data frame with trends data.
#'
#' @export
trends_extract<-function(Geographies,scenarios_x_hazards,DataDir,SaveDir,haz_class,haz_names){
  
  trends_file<-paste0(SaveDir,"/trends.RData")
  
  if(!file.exists(trends_file)){
    
    files<-list.files(DataDir,".tif",full.names = T)
    
    data_ex<-rbindlist(lapply(1:length(Geographies),FUN=function(j){
      # aggregate geographies by admin level chosen
      extract_by<-Geographies[[j]]
      
      data<-rbindlist(lapply(1:nrow(scenarios_x_hazards),FUN=function(i){
        # Display progress
        cat('\r                                                                                                                     ')
        cat('\r',paste0(names(Geographies)[j]," | ",paste(unlist(scenarios_x_hazards[i,list(Scenario,Time,Hazard)]),collapse = "-")))
        flush.console()
        
        file<-files[grepl(scenarios_x_hazards$Scenario[i],files) & grepl(scenarios_x_hazards$Time[i],files) & grepl(scenarios_x_hazards$Hazard[i],files)]
        rast_dat<-terra::rast(file)
        data_ex_mean<-data.table(terra::extract(rast_dat,extract_by,fun=mean,na.rm=T))[,variable:="mean"]
        data_ex_sd<-data.table(terra::extract(rast_dat,extract_by,fun=sd,na.rm=T))[,variable:="sd"]
        data_ex<-rbind(data_ex_mean,data_ex_sd)
        data_ex[,admin_name:=rep(extract_by$admin_name,2)
        ][,scenario:=scenarios_x_hazards$Scenario[i]
        ][,time:=scenarios_x_hazards$Time[i]
        ][,hazard:=scenarios_x_hazards$Hazard[i]
        ][,adminlevel:=names(Geographies)[j]
        ][,ID:=NULL]
        
        data_ex<-melt.data.table(data_ex,id.vars=c("variable","admin_name","scenario","time","hazard","adminlevel"),variable.name = "year")
        data_ex[,year:=as.numeric(gsub("X","",year))]
        
        data_ex<-dcast.data.table(data_ex,admin_name+scenario+time+hazard+adminlevel+year~variable,value.var = "value")
        
        data_ex[,mean:=round(mean,2)][,sd:=round(sd,2)]
        
        data_ex
        
      }))
      
      data
      
    }))
    
    data_ex[,scenario2:=paste0(c(scenario[1],time[1]),collapse="-"),by=list(scenario,time)
    ][scenario2=="historic-historic",scenario2:="historic"][,year2:=as.numeric(factor(year,levels=sort(unique(year)))),by=scenario2]
    
    # Classify hazards
    for(i in 1:nrow(haz_class)){
      data_ex[grepl(haz_class[i,index_name],hazard) & 
                mean>=haz_class[i,lower_lim] & 
                mean<haz_class[i,upper_lim],class:=haz_class[i,description]]
    }
    
    save(data_ex,file=trends_file)
    
  }else{
    data_ex<-miceadds::load.Rdata2(file="trends.RData",path=SaveDir)
  }
  
  return(data_ex)
}

#' Trend Plot Line
#'
#' This function creates a line plot with shaded regions representing different severity classes.
#' 
#' @param data The input dataset.
#' @param haz_class The hazard class dataset.
#' @param haz_choice The selected hazard choices.
#' @param admin_choice The selected administrative choices.
#' @param scenario_choice The selected scenario choices.
#' @param adminlevel_choice The selected administrative level choices.
#' @param palette_choice The selected palette choices.
#' 
#' @return A line plot with shaded regions representing different severity classes.
#'
#' @import ggplot2
#' @importFrom data.table data.table, as.data.frame
#' @importFrom stringr strsplit
#' @export
trend_plot_line<-function(data,haz_class,haz_choice,admin_choice,scenario_choice,adminlevel_choice,palette_choice){
  # Convert data.table to data.frame
  dt <- as.data.frame(data[haz_name %in%  haz_choice & 
                             admin_name %in% admin_choice & 
                             scenario2==scenario_choice & 
                             adminlevel==adminlevel_choice])
  
  # Colours for increasing severity
  haz_choices<-unique(c(unique(dt$hazard),unlist(tstrsplit(unique(dt$hazard),"_",keep=1))))
  
  haz_pal<-PalFun(PalName=palette_choice,
                  N=haz_class[index_name %in% haz_choices,.N],
                  Names=haz_class[index_name %in% haz_choices,description])
  
  shading_table<-haz_class[index_name %in% haz_choices,list(index_name,lower_lim,upper_lim,class,description)
  ][,xmin:=min(dt$year)
  ][,xmax:=max(dt$year)
  ][,description:=factor(description,levels = haz_class[index_name %in% haz_choices][order(class,decreasing = T),description])]
  
  # Create the line plot
  g<-ggplot()+
    geom_line(data=dt, aes(x = year, y = mean, color = admin_name,lty=admin_name)) +
    geom_smooth(data=dt, aes(x = year, y = mean, color = admin_name,lty=admin_name),method=lm,se=F)+
    geom_rect(data = shading_table,
              aes(xmin =xmin, xmax = xmax, ymin = lower_lim, ymax = upper_lim,fill=description),
              alpha = 0.2) +
    labs(x = "Year", y = "Value", color = "Location",title=haz_choice,fill="Severity class",lty="Location") +
    ggplot_theme()+
    scale_fill_manual(values=haz_pal)
  
  return(g)
  
}

#' Trend Scenario Bars
#'
#' This function generates a bar plot of the mean values for different scenarios, given specific choices for hazard, administrative region, and administrative level.
#'
#' @param data A data frame containing the necessary data for the plot.
#' @param haz_choice A vector specifying the chosen hazard names.
#' @param admin_choice A vector specifying the chosen administrative region names.
#' @param adminlevel_choice A vector specifying the chosen administrative levels.
#'
#' @return A bar plot showing mean values for different scenarios, with separate bars for each administrative region.
#'
#' @import data.table
#' @import ggplot2
#'
#' @export
trend_scenario_bars<-function(data,
                              haz_choice,
                              admin_choice,
                              adminlevel_choice){
  dt2<-data[haz_name %in% haz_choice & 
              admin_name %in% admin_choice & 
              adminlevel==adminlevel_choice
  ][,list(sd=sd(mean,na.rm = T),mean=mean(mean,na.rm=T))
    ,by=list(admin_name,scenario,time,hazard,haz_name,adminlevel,scenario2)]
  
  # Colours for increasing severity
  haz_choice2<-unlist(tstrsplit(haz_choice,"_",keep=1))
  
  ggplot(data=dt2, aes(x = scenario2, y = mean,fill=scenario2)) + 
    geom_bar(stat = "identity")+
    facet_grid(~admin_name)+
    ggplot_theme()+
    labs(x = "Scenario", y = "Value", color = "Location",title=haz_choice)+
    coord_cartesian(ylim=c(min(dt2$mean)-(0.1*(range(dt2$mean)[2]- range(dt2$mean)[1])),max(dt2$mean)+(0.1*(range(dt2$mean)[2]- range(dt2$mean)[1]))))+
    theme(legend.position="none")+
    scale_x_discrete(guide = guide_axis(n.dodge = 3))
}

#' SxRtabFun
#'
#' Calculates summary statistics for hazards data and organizes the results
#'
#' @param Hazards A data frame containing hazards data
#' @param Plot_Vars A character vector specifying the plot variables
#' @param ExtractBy A character vector specifying the extraction variable
#' 
#' @return A data frame with summarized hazard data
#' 
#' @import data.table
#' @import stringr
#' @importFrom terra zonal
SxRtabFun<-function(Hazards,Plot_Vars,ExtractBy){
  Hazards<-Hazards[[paste0(rep(Plot_Vars,each=4),c("_prop","_mean","_exceedmean","_exceedmax"))]]
  Data<-data.table(terra::zonal(x=Hazards,z=ExtractBy,fun=mean,na.rm=T))
  Data_sd<-data.table(terra::zonal(x=Hazards,z=ExtractBy,fun=sd,na.rm=T))
  
  names(Data_sd)<-paste0(names(Data_sd),"-sd")
  
  colnames(Data)[1]<-"admin"
  colnames(Data_sd)[1]<-"admin"
  
  Data<-merge(Data,Data_sd)
  
  Data<-data.table(melt(Data,id.vars="admin"))
  Data[,Variable:=strsplit(as.character(variable),"_")][,Variable:=unlist(lapply(Variable,FUN=function(x){x[length(x)]}))]
  Data[,Hazard:=gsub(paste(paste0("_",unique(Variable)),collapse="|"),"",variable)][,variable:=NULL]
  
  Data<-dcast(Data,admin+Hazard~Variable,value.var="value")
  
  return(Data)
}

#' Prepare Table Data
#'
#' This function prepares table data based on the given parameters.
#'
#' @param Data The data as a list.
#' @param Method The method used for data filtering.
#' @param Scenario The scenario used for data filtering.
#' @param AdminLevel The administrative level used for data filtering.
#' @param A1 The input for filtering at administrative level 1.
#' @param A2 The input for filtering at administrative level 2.
#' @param Table The table to be prepared.
#'
#' @return The prepared table data.
PrepTable<-function(Data,Method,Scenario,AdminLevel,A1,A2,Table){
  Data<-Data[[Method]][[Scenario]][[AdminLevel]][[Table]]
  if(AdminLevel=="Admin2"){
    Data<-Data[Region %in% A2]
  }else{
    Data<-Data[Region %in% A1]
  }
  
  setnames(Data,"Region","Admin")
  
  Data
}

#' Hazard Index Calculation
#'
#' Calculates the hazard index for a set of hazards and scenarios.
#'
#' @param Hazards A list of hazard datasets.
#' @param verbose A logical value indicating whether to display progress information. Default is \code{TRUE}.
#' @param SaveDir The directory for saving the hazard index files.
#' @param crop_choice The crop choice for which the hazard index is calculated.
#'
#' @return A list of hazard index datasets.
#'
#' @export
hazard_index<-function(Data,hazards,verbose=T,SaveDir,crop_choice,severity_classes,PropThreshold){
  scenario_names<-names(Data)
  
  haz_index<-lapply(1:length(scenario_names),FUN = function(j){
    
    haz_index_filename<-paste0(SaveDir,"/hi_",crop_choice,"_",scenario_names[j],"-",PropThreshold,".tif")
    
    if(!file.exists(haz_index_filename)){
      
      # recurrence 
      data<-Data[[scenario_names[[j]]]]
      data<-data[[grep("_prop_",names(data),value=T)]]
      
      # Subtract severe and extreme from moderate, and severe from extreme
      # Note that this section is not generalization and works with fixed severity_classes table, in future we should improve this to be able
      # work with tables of different lengths.
      for(i in 1:length(hazards)){

        
        haz_levels<-grep(hazards[i],names(data),value=T)
        haz_levels<-unique(substr(haz_levels,nchar(haz_levels),nchar(haz_levels)))
        
        for(k in haz_levels){
          
          if(verbose){
            # Display progress
            cat('\r                                                                                                                     ')
            cat('\r',paste0("Scenario ", scenario_names[j]," | Hazard ",hazards[i],"-",i," | ",k))
            flush.console()
          }
          
          N<-paste0(hazards[i],"_prop_",severity_classes$class,"_",k)
          N1<-which(names(data)==N[1])
          N2<-which(names(data)==N[2])
          X<-data[[N[1]]]-data[[N[2]]]-data[[N[3]]]
          X[][X[]<0 & !is.na(X[])]<-0
          data[[N1]]<-X
          Y<-data[[N2]]-data[[N[3]]]
          Y[][Y[]<0 & !is.na(Y[])]<-0
          data[[N2]]<-Y
        }
      }
      
      for(i in 1:nrow(severity_classes)){
        N<-grep(severity_classes$class[i],names(data))
        data[[N]]<-data[[N]]*severity_classes$value[i]
      }
      
      haz_index<-terra::rast(lapply(1:length(hazards),FUN=function(i){
        haz_levels<-grep(hazards[i],names(data),value=T)
        haz_levels<-unique(substr(haz_levels,nchar(haz_levels),nchar(haz_levels)))
        
        X<-terra::rast(lapply(haz_levels,FUN=function(k){
          if(verbose){
            # Display progress
            cat('\r                                                                                                                     ')
            cat('\r',paste0("Scenario ", scenario_names[j]," | Hazard ",hazards[i],"-",i," | ",k))
            flush.console()
          }
          
        N<-paste0(hazards[i],"_prop_",severity_classes$class,"_",k)
        X<-terra::app(data[[N]],sum,na.rm=T)
        names(X)<-paste0(hazards[i],"_",k,"_hi")
        X
        }))
        
      }))
      
      terra::writeRaster(haz_index,file=haz_index_filename)
      
      haz_index
      
    }else{
      haz_index<-terra::rast(haz_index_filename)
    }
    
    haz_index
    
  })
  names(haz_index)<-scenario_names
  
  return(haz_index)
}

#' Add Alpha to Colours
#'
#' This function takes a vector of colours and adds an alpha (transparency) value to each colour.
#' The alpha value ranges from 0 (completely transparent) to 1 (completely opaque). Default alpha value is 1.
#'
#' @param col A vector of colours.
#' @param alpha The alpha value to add to each colour. Must be a numeric value between 0 and 1.
#'
#' @return A vector of modified colours with added alpha value.
#' @export
#'
#' @examples
add.alpha <- function(col, alpha=1){
  if(missing(col))
    stop("Please provide a vector of colours.")
  apply(sapply(col, col2rgb)/255, 2, 
        function(x) 
          rgb(x[1], x[2], x[3], alpha=alpha))  
}

#' Set Hazard Severity
#'
#' This function sets the severity of hazards for different scenarios and saves the result in a file. If the file already exists, it loads the data from the file.
#'
#' @param Hazards A list of hazard data for different scenarios.
#' @param verbose If TRUE, it displays progress.
#' @param SaveDir The directory to save the output files.
#' @param crop_choice The choice of crop for which hazards are being set.
#' @param severity_classes A table of severity classes.
#' @param PropThreshold A threshold for hazard propagation.
#'
#' @return A list of hazard severity data for different scenarios.
#'
#' @importFrom data.table data.table
#' @importFrom terra app
#' @importFrom terra rast
#' @importFrom terra writeRaster
#' @importFrom terra values
#' @importFrom terra levels
#' @importFrom terra nlyr
#'
#' @export
hazard_severity<-function(Hazards,verbose=T,SaveDir,crop_choice,severity_classes,PropThreshold){
  
  severity_classes2<-rbind(data.table(class="None",value=0),severity_classes)
  
  scenario_names<-names(Hazards)
  
  data<-lapply(1:length(Hazards),FUN = function(j){
    
    filename<-paste0(SaveDir,"/hs_",crop_choice,"_",scenario_names[j],"-",PropThreshold,".tif")
    
    if(!file.exists(filename)){
      
      # Subtract severe and extreme from moderate, and severe from extreme
      data<-terra::rast(lapply(1:length(hazards),FUN=function(i){
        
        if(verbose){
          # Display progress
          cat('\r                                                                                                                     ')
          cat('\r',paste0("Scenario ", scenario_names[j]," | Hazard ",hazards[i]))
          flush.console()
        }
        
        N<-paste0(hazards[i],"_propclass_",severity_classes$class)
        sev<-Hazards[[j]][[N]]
        sev<-terra::rast(lapply(1:nlyr(sev),FUN=function(k){
          sev[[k]]*severity_classes[k,value]
        }))
        sev<-terra::app(sev,max,na.rm=T)
        
        sev_vals<-unique(values(sev))
        sev_vals<-sev_vals[!is.na(sev_vals)]
        
        levels(sev)<-severity_classes2[value %in% sev_vals,list(value,class)]
        names(sev)<-paste(hazards[i],"_propclass_merged")
        
        sev
      }))
      
      terra::writeRaster(data,file=filename)
      
      data
      
    }else{
      data<-terra::rast(filename)
    }
    
    data
    
  })
  names(data)<-scenario_names
  
  return(data)
}

#' Create Breaks for Data
#'
#' This function takes a data object and creates breaks for the values in the data.
#'
#' @param data A data object.
#' 
#' @return A numeric vector of break points.
#' 
#' @examples
#' break_fun(data.frame(values = c(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))) # Output: c(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
#' break_fun(data.frame(values = c(1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5))) # Output: c(1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5)
#' 
#' @export
break_fun<-function(data){
  vals<-as.vector(values(data))
  min_val<-min(vals,na.rm=T)
  max_val<-max(vals,na.rm=T)
  div<-(max_val-min_val)/10
  dp<-log10(div)
  if(dp>0){dp<-ceiling(dp)} 
  if(dp<0){dp<-floor(dp)}
  dp<-10^dp
  max_val<-ceiling(max_val*10)/10
  min_val<-floor(min_val*10)/10
  breaks<-seq(min_val,max_val,dp)
  # breaks<-seq(min_val,max_val,length=n) # alternative approach but gives breaks with many decimal places.
  return(breaks)
}

#' Range function
#'
#' Calculates the range of a numeric vector, rounded to the nearest decimal place.
#'
#' @param data A numeric vector.
#' @return A vector containing the minimum and maximum values of the input vector.
#' @export
#'
#' @examples
range_fun<-function(data){
  vals<-as.vector(values(data))
  min_val<-min(vals,na.rm=T)
  max_val<-max(vals,na.rm=T)
  div<-(max_val-min_val)/10
  dp<-log10(div)
  if(dp>0){dp<-ceiling(dp)} 
  if(dp<0){dp<-floor(dp)}
  dp<-10^dp
  max_val<-ceiling(max_val*10)/10
  min_val<-floor(min_val*10)/10
  range<-c(min_val,max_val)
  # breaks<-seq(min_val,max_val,length=n) # alternative approach but gives breaks with many decimal places.
  return(range)
}
#' Classify Raster Data Based on a Threshold
#'
#' This function classifies raster data into two categories based on a specified threshold.
#' It allows for classification in either direction (greater than or less than the threshold).
#'
#' @param data A \code{SpatRaster} object from the \code{terra} package that you want to classify.
#' @param direction A character string indicating the direction of the classification.
#'                  Acceptable values include "G", "g", ">", "L", "l", "<".
#'                  "G" or ">" will classify values greater than the threshold as 1,
#'                  and "L" or "<" will classify values less than the threshold as 1.
#' @param threshold A numeric value representing the threshold for classification.
#' @param minval An optional numeric parameter representing the minimum value to be classified.
#'               Defaults to -99999 if not specified.
#' @param maxval An optional numeric parameter representing the maximum value to be classified.
#'               Defaults to 99999 if not specified.
#'
#' @return A \code{SpatRaster} object with the classification applied.
#' @examples
#' # Assuming 'rast' is a SpatRaster object
#' rast_classified <- rast_class(rast, direction = ">", threshold = 500)
#'
#' @export
#'
#' @import terra
rast_class <- function(data, direction, threshold, minval = -99999, maxval = 99999) {
  
  # Define the 'from' and 'to' vectors for the classification
  from <- c(minval, threshold)
  to <- c(threshold, maxval)
  
  # Determine the new values after classification based on the direction
  if (direction %in% c("G", "g", ">")) {
    becomes <- c(0, 1)
  }
  
  if (direction %in% c("L", "l", "<")) {
    becomes <- c(1, 0)
  }
  
  # Reclassify the raster data using terra's classify function
  data <- terra::classify(data, data.frame(from = from, to = to, becomes = becomes))
  
  return(data)
}
#' Calculate Interaction Risk
#'
#' This function applies a mask to the input data based on specified mask values, classifies the data into a binary format,
#' and then computes the mean of the resulting data. The mean data is named according to the `lyr_name` parameter and returned.
#' @param data A SpatRaster object representing the input data to be masked and analyzed.
#' @param interaction_mask_vals A vector of values used for masking the input data. These values define which cells in `data` should be considered for analysis.
#' @param lyr_name A character string specifying the name to be assigned to the output layer.
#' @return A SpatRaster object with the computed mean after masking and classification, named according to `lyr_name`.
#' @examples
#' # Assuming `raster_data` is a SpatRaster loaded with `terra` package
#' interaction_mask_vals <- c(1, 2, 3) # Example mask values
#' lyr_name <- "RiskLayer"
#' result <- int_risk(raster_data, interaction_mask_vals, lyr_name)
#' print(result)
#' @import terra
#' @export
int_risk <- function(data, interaction_mask_vals, lyr_name){
  data <- terra::mask(data, data, maskvalues=interaction_mask_vals, updatevalue=0)
  data <- terra::classify(data, data.table(from=1, to=999999, becomes=1))
  data <- terra::app(data, fun="mean", na.rm=TRUE)
  names(data) <- lyr_name
  return(data)
}
#' Combine and Process Climate Hazard Raster Files
#'
#' This function processes raster files for a specified climate hazard and scenario. It stacks or aggregates multiple years of data, 
#' handles missing values, and computes summary statistics (mean, max, etc.) as specified. It's designed to work within a larger 
#' workflow for analyzing climate-related risks to agriculture.
#'
#' @param i Integer, the index of the hazard and scenario combination to process.
#' @param folders_x_hazards A data table containing the mapping of folders to hazard variables.
#' @param model_names Character vector of model names involved in the analysis.
#' @param use_crop_cal Character, indicating whether crop calendars are used in the analysis.
#' @param r_cal Data frame or list containing crop calendar information for the region.
#' @param save_dir Character, the directory where processed raster files will be saved.
#' @param overwrite Logical, should existing files be replaced?
#' @return A character vector of filenames for the processed and saved raster files.
#' @examples
#' # This example assumes 'folders_x_hazards', 'model_names', and 'r_cal' are predefined.
#' result <- hazard_stacker(1, folders_x_hazards, model_names, "yes", r_cal, "./output")
#' @export
hazard_stacker<-function(i,folders_x_hazards,model_names,use_crop_cal,r_cal,save_dir,overwrite=F){
  # Extract the specific hazard and scenario based on the index 'i'
  variable<-folders_x_hazards$hazards[i]
  scenario<-folders_x_hazards$folders[i]
  
  # List all hazard files for the given scenario and variable, excluding "AVAIL.tif"
  haz_files<-list.files(paste0(scenario,"/",variable),".tif",recursive=F,full.names = T)
  haz_files<-haz_files[!grepl("AVAIL.tif",haz_files)]
  
  n<-0
  
  # Attempt to reload files if the first attempt returns NA, up to 10 tries
  while(is.na(haz_files[1]) & n<10){
    print(paste0("cc = ",use_crop_cal," | ",scenario,"-",variable))
    print(paste0("i=",i," | haz_files is returning NA | attempt = ",n))
    haz_files<-list.files(paste0(scenario,"/",variable),".tif",recursive=F,full.names = T)
    haz_files<-haz_files[!grepl("AVAIL.tif",haz_files)]
    n<-n+1
  }
  
  # Extract unique years from the file names
  years<-unique(as.numeric(gsub(".tif","",unlist(tstrsplit(unlist(tail(tstrsplit(haz_files,"/"),1)),"-",keep=2)))))
  
  # Process files differently if they contain "mean" or other statistical terms
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
    
    # Retrieve the statistical function (e.g., mean, max) for processing the hazard data
    stat<-unlist(haz_meta[variable.code==variable2,"function"])
    
    haz_files1<-haz_files[[k]]
    
    # Define the save name for the output raster
    savename<-paste0(save_dir,"/",scenario,"_",variable2,"_",stat,".tif")
    
    # Skip processing if the file already exists
    if(!file.exists(savename)|overwrite==T){
      # Special handling for TAI hazard type
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
          
          # Subset haz_rast to increase efficiency
          x = haz_rast[[(plant_min+12*(m-1)):(harvest_max+12*(m-1))]]
          
          haz_rast1<-terra::rapp(x,
                                 first=plant,
                                 last=harvest,
                                 fun=if(stat=="sum"){sum}else{
                                   if(stat=="max"){max}else{
                                     if(stat=="mean"){mean}else{if(stat=="min"){min}else{stop("invalid stat function supplied")}}}},
                                 na.rm=T)

          names(haz_rast1)<-years[m]
          haz_rast1
        }))
      }
      # Write the processed raster to file
      terra::writeRaster(haz_rast_years,savename,overwrite=T)
      # Clean up the memory
      rm(haz_rast_years)
      gc()
    }
    X[k]<-savename
  }
  
  X
  
}

#' Read and Process MapSPAM Data
#'
#' This function reads agricultural variable data for a given technology from MapSPAM, processes it, and saves it as a raster file. If the file already exists, it can be optionally overwritten or simply read from disk.
#'
#' @param variable Character string specifying the agricultural variable to process.
#' @param technology Character string specifying the technology level (e.g., "HighYield").
#' @param mapspam_dir Character string of the directory containing MapSPAM CSV files.
#' @param save_dir Character string of the directory where output raster files are saved.
#' @param base_rast Raster object specifying the base raster for resampling.
#' @param filename Character string specifying the base name for the output raster file.
#' @param ms_codes Data frame mapping MapSPAM codes to full crop names.
#' @param overwrite Logical indicating whether to overwrite existing files.
#' @return A raster object of the processed MapSPAM data.
#' @examples
#' base_rast <- terra::rast(system.file("ex/lux.tif", package="terra"))
#' ms_codes <- data.frame(Code = c("maize"), Fullname = c("Maize"))
#' data <- read_spam("production", "HighYield", "/path/to/mapspam", "/path/to/save", base_rast, "maize_highyield", ms_codes, TRUE)
#' @export
read_spam <- function(variable, technology, mapspam_dir, save_dir, base_rast, filename, ms_codes, overwrite) {
  # Construct the filename for the output raster file.
  ms_file <- paste0(save_dir, "/", filename, ".tif")
  
  # Check if the file exists and whether it should be overwritten. If it doesn't exist or should be overwritten, process the data.
  if (!file.exists(ms_file) | overwrite == T) {
    # Read the CSV file containing the MapSPAM data for the given variable and technology.
    data <- fread(paste0(mapspam_dir, "/SSA_", variable, "_", technology, ".csv"))
    
    # Prepare a list of crop names to filter from the MapSPAM data, based on the ms_codes lookup table.
    crops <- tolower(ms_codes$Code)
    
    # Identify the columns in the data that match the crop names, plus x and y coordinates.
    ms_fields <- c("x", "y", grep(paste0(crops, collapse = "|"), colnames(data), value=T))
    
    # Convert the selected columns of the CSV data into a raster, setting the projection to EPSG:4326.
    data <- terra::rast(data[, ..ms_fields], type="xyz", crs="EPSG:4326")
    
    # Clean up the names of the data columns, removing suffixes and matching them to full crop names from ms_codes.
    names(data) <- gsub("_a$|_h$|_i$|_l$|_r$|_s$", "", names(data))
    names(data) <- ms_codes[match(names(data), tolower(ms_codes$Code)), Fullname]
    
    # Convert the raster values from total values to values per hectare, based on cell size.
    data <- data / terra::cellSize(data, unit="ha")
    
    # Resample the raster data to match the resolution and extent of the base_rast.
    data <- terra::resample(data, base_rast)
    
    # Convert the resampled values back to total values by multiplying by the new cell size.
    data <- data * cellSize(data, unit="ha")
    
    # Save the processed raster data to a file.
    terra::writeRaster(data, filename=ms_file, overwrite=T)
  } else {
    # If the file exists and should not be overwritten, simply read the existing raster file.
    data <- terra::rast(ms_file)
  }
  
  # Return the raster data.
  return(data)
}

#' Spatial Data Extraction by Administrative Levels
#'
#' Extracts spatial data for specified administrative levels (e.g., countries, states/provinces, counties/districts) using precise area weighting. The function supports applying a specified summary function (e.g., mean) to the data during the extraction process.
#'
#' @param data Raster layer from which to extract data.
#' @param Geographies List containing `spatvect` or `sf` objects for each administrative level to be processed (`admin0`, `admin1`, `admin2`).
#' @param FUN Character string naming the function to apply when summarizing data for each administrative area. Defaults to "mean".
#' @param max_cells_in_memory Numeric, sets the maximum number of raster cells to keep in memory during the extraction process, influencing performance and memory usage.
#' @return A list of `data.frame` objects containing the extracted data merged with geographical metadata for each requested administrative level.
#' @examples
#' # Assuming 'data' is a raster layer and 'Geographies' is a list of sf objects for admin0, admin1, and admin2 levels:
#' extracted_data <- admin_extract(data, Geographies, FUN = "mean", max_cells_in_memory = 30000000)
#' @export
# The `admin_extract` function performs spatial extraction of data for specified administrative levels using exact extraction methods, then merges the extracted data with geographical metadata.
admin_extract <- function(data, Geographies, FUN = "mean", max_cells_in_memory = 3*10^7) {
  # Initialize an empty list to store the output data frames for each administrative level.
  output <- list()
  
  # Process administrative level 0 data if present in the Geographies list.
  if ("admin0" %in% names(Geographies)) {
    # Perform exact extraction of data for admin0 level, appending relevant columns and applying the specified function (e.g., mean).
    data0 <- exactextractr::exact_extract(data, sf::st_as_sf(Geographies$admin0), fun = FUN, append_cols = c("admin_name", "admin0_name", "iso3"), max_cells_in_memory = max_cells_in_memory)
    # Merge the extracted data with admin0 geographical metadata.
    data0 <- terra::merge(Geographies$admin0, data0)
    # Add the merged data frame to the output list under the admin0 key.
    output$admin0 <- data0
  }
  
  # Repeat the process for administrative level 1 data if present.
  if ("admin1" %in% names(Geographies)) {
    data1 <- exactextractr::exact_extract(data, sf::st_as_sf(Geographies$admin1), fun = FUN, append_cols = c("admin_name", "admin0_name", "admin1_name", "iso3"), max_cells_in_memory = max_cells_in_memory)
    data1 <- terra::merge(Geographies$admin1, data1)
    output$admin1 <- data1
  }
  
  # Repeat the process for administrative level 2 data if present.
  if ("admin2" %in% names(Geographies)) {
    data2 <- exactextractr::exact_extract(data, sf::st_as_sf(Geographies$admin2), fun = FUN, append_cols = c("admin_name", "admin0_name", "admin1_name", "admin2_name", "iso3"), max_cells_in_memory = max_cells_in_memory)
    data2 <- terra::merge(Geographies$admin2, data2)
    output$admin2 <- data2
  }
  
  # Return the list containing the merged data frames for each processed administrative level.
  return(output)
}

#' Wrap Spatial Data Extraction and Save as Parquet
#'
#' This function wraps around the `admin_extract` function to perform spatial data extraction for specified administrative levels. It processes and formats the extracted data, then saves it as Parquet files. If specified files already exist, they can be optionally overwritten or read directly.
#'
#' @param data Raster layer or object from which to extract data.
#' @param save_dir Directory where output Parquet files will be saved.
#' @param filename Base name for the output Parquet files.
#' @param FUN Aggregation function to apply during data extraction (e.g., "sum").
#' @param varname Name of the variable being processed, to be added to the output data.
#' @param Geographies List containing `spatvect` or `sf` objects for each administrative level to be processed.
#' @param overwrite Logical; if TRUE, existing Parquet files will be overwritten.
#' @return A data.table object containing the processed and formatted data from all administrative levels.
#' @examples
#' # Assuming 'data' is a raster layer, 'Geographies' is a list of sf objects, and other parameters are set:
#' processed_data <- admin_extract_wrap(data, "/path/to/save", "my_data", "sum", "my_variable", Geographies, FALSE)
#' @export
admin_extract_wrap <- function(data, save_dir, filename, FUN = "sum", varname, Geographies, overwrite = F) {
  
  # Define a mapping of administrative level names to short codes.
  levels <- c(admin0 = "adm0", admin1 = "adm1", admin2 = "adm2")
  
  # Create filenames for saving the output based on administrative level and aggregation function.
  file <- paste0(save_dir, "/", filename, "_adm_", FUN, ".parquet")
  file0 <- gsub("_adm_", "_adm0_", file)
  file1 <- gsub("_adm_", "_adm1_", file)
  file2 <- gsub("_adm_", "_adm2_", file)
  
  # Check if any of the files don't exist or if overwrite is enabled. If so, proceed with data extraction.
  if (!file.exists(file) | !file.exists(file1) | overwrite == T) {
    # Extract data for all specified administrative levels.
    data_ex <- admin_extract(data, Geographies, FUN = FUN)
    
    # Save the extracted data for each administrative level as a Parquet file.
    st_write_parquet(obj = sf::st_as_sf(data_ex$admin0), dsn = file0)
    st_write_parquet(obj = sf::st_as_sf(data_ex$admin1), dsn = file1)
    st_write_parquet(obj = sf::st_as_sf(data_ex$admin2), dsn = file2)
    
    # Process the extracted data to format it for analysis or further processing.
    data_ex <- rbindlist(lapply(1:length(levels), FUN = function(i) {
      level <- levels[i]
      print(level)
      
      # Convert the data to a data.table and remove specific columns.
      data <- data.table(data.frame(data_ex[[names(level)]]))
      data <- data[, !c("admin_name", "iso3")]
      
      # Determine the administrative level being processed and adjust the data accordingly.
      admin <- "admin0_name"
      if (level %in% c("adm1", "adm2")) {
        admin <- c(admin, "admin1_name")
        data <- suppressWarnings(data[, !"a1_a0"])
      }
      
      if (level == "adm2") {
        admin <- c(admin, "admin2_name")
        data <- suppressWarnings(data[, !"a2_a1_a0"])
      }
      
      # Adjust column names and reshape the data.
      colnames(data) <- gsub("_nam$", "_name", colnames(data))
      data <- data.table(melt(data, id.vars = admin))
      
      # Add and modify columns to include crop type and exposure information.
      data[, crop := gsub(paste0(FUN, "."), "", variable[1], fixed = T), by = variable]
      data[, exposure := varname]
      data[, variable := NULL]
      
      data
    }), fill = T)
    # Adjust the crop column formatting.
    data_ex[, crop := gsub(".", " ", crop, fixed = T)]
    
    # Save the processed data to a single Parquet file.
    arrow::write_parquet(data_ex, file)
  } else {
    # If the Parquet file exists and should not be overwritten, read it instead of processing data.
    data_ex <- arrow::read_parquet(file)
  }
  
  # Return the processed or read data.
  return(data_ex)
}


#' Process and Save Spatial Data by Severity and Administrative Level
#'
#' Iterates over specified severity levels to extract spatial data for given administrative levels (admin0, admin1, admin2) and saves the extracted data into Parquet files. Allows for overwriting existing files.
#'
#' @param files Vector of file paths pointing to raster data files to be processed.
#' @param save_dir Directory where the output Parquet files will be saved.
#' @param filename Base name to be used for generating the output file names.
#' @param severity Vector of severity classes to be processed.
#' @param overwrite Logical; if TRUE, existing Parquet files for the given severity and administrative level will be overwritten.
#' @param FUN Aggregation function (passed as a character string) to be applied during data extraction.
#' @param Geographies List containing `spatvect` or `sf` objects for each administrative level to be processed.
#' @examples
#' # Example usage assuming 'files', 'save_dir', 'filename', 'severity', and 'Geographies' are defined:
#' admin_extract_wrap2(files, save_dir, "my_data", c("high", "low"), FALSE, "mean", Geographies)
#' @export
admin_extract_wrap2 <- function(files, save_dir, filename, severity, overwrite = F, FUN = "mean", Geographies) {
  
  # Loop through each severity level provided in the 'severity' argument.
  for(SEV in tolower(severity)) {
    
    # Construct file paths for output Parquet files for administrative levels 0, 1, and 2.
    file0 <- paste0(save_dir, "/", filename, "_adm0_", SEV, ".parquet")
    file1 <- gsub("_adm0_", "_adm1_", file0)
    file2 <- gsub("_adm0_", "_adm2_", file0)
    
    # Load raster data that matches the current severity level from the provided files.
    data <- terra::rast(files[grepl(SEV, files)])
    
    # Process and save data for admin0 level if the file doesn't exist or if overwrite is enabled.
    if ((!file.exists(file0)) | overwrite == T) {
      # Display current progress.
      cat('\r', paste("Adm0 - Severity Class:", SEV))
      flush.console()
      
      # Extract data for the admin0 level using the `admin_extract` function.
      data_ex <- admin_extract(data, Geographies["admin0"], FUN = FUN)
      # Write the extracted data to a Parquet file.
      sfarrow::st_write_parquet(obj = sf::st_as_sf(data_ex$admin0), dsn = file0)
    }
    
    # Repeat the process for admin1 and admin2 levels.
    if ((!file.exists(file1)) | overwrite == T) {
      cat('\r', paste("Adm1 - Severity Class:", SEV))
      flush.console()
      
      data_ex <- admin_extract(data, Geographies["admin1"], FUN = FUN)
      sfarrow::st_write_parquet(obj = sf::st_as_sf(data_ex$admin1), dsn = file1)
    }
    
    if ((!file.exists(file2)) | overwrite == T) {
      cat('\r', paste("Adm2 - Severity Class:", SEV))
      flush.console()
      
      data_ex <- admin_extract(data, Geographies["admin2"], FUN = FUN)
      sfarrow::st_write_parquet(obj = sf::st_as_sf(data_ex$admin2), dsn = file2)
    }
  }
}

#' Restructure and Save Parquet Data Files by Severity
#'
#' For given severity levels, this function restructures spatial data stored in Parquet files by combining and refining it based on crops, livestock, scenarios, and hazards. It outputs restructured data into new Parquet files, supporting optional overwrite of existing files.
#'
#' @param filename Base name for reading input Parquet files.
#' @param save_dir Directory where the output Parquet files will be saved.
#' @param severity Vector of severity levels to process.
#' @param overwrite Logical; determines if existing output files should be overwritten.
#' @param crops Vector of crop names to include in the restructuring process.
#' @param livestock Vector of livestock names to include in the restructuring process.
#' @param Scenarios Data frame or list mapping scenarios to specific attributes for inclusion in the output.
#' @param hazards Vector of hazard types to include in the restructuring process.
#' @examples
#' # Assuming proper setup and existence of the necessary directories and data:
#' restructure_parquet("data_filename", "/path/to/save", c("High", "Medium", "Low"), FALSE, c("Wheat", "Maize"), c("Cattle"), Scenarios, c("Drought", "Flood"))
#' @export
restructure_parquet<-function(filename,save_dir,severity,overwrite=F,crops,livestock,Scenarios,hazards){
  severity<-tolower(severity)
  for(SEV in severity){
    file<-paste0(save_dir,"/",filename,"_adm_",SEV,".parquet")
    
    if((!file.exists(file))|overwrite==T){
      file0<-paste0(save_dir,"/",filename,"_adm0_",SEV,".parquet")
      file1<-gsub("_adm0_","_adm1_",file0)
      file2<-gsub("_adm0_","_adm2_",file0)
      
      files<-list(adm0=file0,adm1=file1,adm2=file2)
      
      # Read in geoparquet vectors, extract tabular data and join together
      data<-rbindlist(lapply(1:length(files),FUN=function(i){
        file<-files[i]
        level<-names(files[i])
        
        print(paste0(SEV,"-",level))
        
        admins<-"admin0_name"
        
        if(level %in% c("adm1","adm2")){
          admins<-c(admins,"admin1_name")
        }
        
        if(level=="adm2"){
          admins<-c(admins,"admin2_name")
        }
        
        data<-data.table(data.frame(terra::vect(sfarrow::st_read_parquet(files[[i]]))))
        data<-suppressWarnings(data[,!c("admin_name","iso3","a2_a1_a0","a1_a0")])
        
        colnames(data)<-gsub("_nam$","_name",colnames(data))
        
        melt(data,id.vars = admins)
      }),fill=T)
      data[,variable:=as.character(variable)]
      
      variable_old<-data[,unique(variable)]
      
      # Replace dots in hazard names with a "+"
      old<-c("dry[.]heat","dry[.]wet","heat[.]wet","dry[.]heat[.]wet")
      new<-c("dry+heat","dry+wet","heat+wet","dry+heat+wet")
      
      variable_old2<-stringi::stri_replace_all_regex(variable_old,pattern=old,replacement=new,vectorise_all = F)
      
      # Renaming of variable to allow splitting
      new<-paste0(Scenarios$combined,"-")
      old<-paste0(Scenarios[,paste0(Scenario,"[.]",Time)],"[.]")
      
      # Replace space in the crop names with a . to match the parquet column names
      new<-c(new,paste0("-",crops,"-"))
      old<-c(old,paste0("[.]",gsub(" ",".",crops,fixed = T),"[.]"))
      
      new<-c(new,paste0("-",livestock,"-"))
      old<-c(old,paste0("[.]",livestock,"[.]"))
      
      new<-c(new,paste0(c("any",hazards),"-"))
      old<-c(old,paste0(c("any",hazards),"[.]"))
      
      variable_new<-data.table(variable=stringi::stri_replace_all_regex(variable_old2,pattern=old,replacement=new,vectorise_all = F))
      
      # Note this method of merging a list back to the original table is much faster than the method employed in the hazards x exposure section
      split<-variable_new[,list(var_split=list(tstrsplit(variable[1],"-"))),by=variable]
      split_tab<-rbindlist(split$var_split)
      colnames(split_tab)<-c("scenario","timeframe","hazard","hazard_vars","crop","severity")
      split_tab$variable<-variable_old
      split_tab[,hazard_vars:=gsub(".","+",hazard_vars[1],fixed=T),by=hazard_vars
      ][,scenario:=unlist(tstrsplit(scenario[1],".",keep=2,fixed=T)),by=scenario
      ][,severity:=tolower(severity)]
      
      data<-merge(data,split_tab,all.x=T)
      data[,variable:=NULL]
      
      arrow::write_parquet(data,file)
    }
    
  }
  
}

#' Extract Hazard Risk and Exposure Data and Save to Parquet
#'
#' For given severity classes, this function extracts hazard risk and exposure data from raster files, optionally focusing on interactions or solo hazards. It allows for filtering out specific hazards or crops and saves the processed data in Parquet format for different administrative levels.
#'
#' @param severity_classes Data frame or list specifying the severity classes to process.
#' @param interactions Logical; if TRUE, only considers files indicating hazard interactions.
#' @param folder String specifying the directory containing raster files to be processed.
#' @param overwrite Logical; if TRUE, existing Parquet files will be overwritten.
#' @param rm_haz Vector of hazard names to be removed from the analysis.
#' @param rm_crop Vector of crop names to be removed from the analysis.
#' @examples
#' # Assuming setup and existence of the necessary directories and data:
#' haz_risk_exp_extract(severity_classes = df_severity, interactions = TRUE, folder = "/path/to/data", overwrite = FALSE, rm_haz = c("flood"), rm_crop = NULL)
#' @export
# Function to extract hazard risk and exposure data for specified severity classes and save in Parquet format.
haz_risk_exp_extract <- function(severity_classes, interactions, folder, overwrite = F, rm_haz = NULL, rm_crop = NULL) {
  
  # List all TIFF files in the specified folder.
  files <- list.files(folder, ".tif$", full.names = T)
  
  # Filter files based on whether interactions are considered.
  if (interactions) {
    files <- grep("-int-", files, value = T)
    filename <- "int"
  } else {
    files <- files[!grepl("-int-", files)]
    filename <- "solo"
  }
  
  # Process files for each severity class specified.
  for (SEV in tolower(severity_classes$class)) {
    
    # Filter files specific to the current severity class.
    files_ss <- files[grepl(SEV, files)]
    # Read the filtered files as raster objects.
    data <- terra::rast(files_ss)
    
    # Remove specified hazards and crops from the data if requested.
    if (!is.null(rm_haz)) {
      data <- data[[names(data)[!grepl(paste0(rm_haz, collapse = "|"), names(data))]]]
    }
    if (!is.null(rm_crop)) {
      data <- data[names(data)[!grepl(paste0(paste0("-", rm_crop, "-"), collapse = "|"), names(data))]]
    }
    
    # Define file paths for saving extracted data in Parquet format for admin levels 0, 1, and 2.
    file0 <- paste0(folder, "/", SEV, "_adm0_", filename, ".parquet")
    file1 <- gsub("_adm0_", "_adm1_", file0)
    file2 <- gsub("_adm0_", "_adm2_", file0)
    
    # Save the extracted data to Parquet files if they do not exist or if overwrite is enabled.
    if (!file.exists(file0) | overwrite == T) {
      cat('\r', paste("Risk x Exposure - admin extraction - adm0| severity:", SEV))
      flush.console()
      
      data_ex <- admin_extract(data, Geographies["admin0"], FUN = "sum")
      sfarrow::st_write_parquet(obj = sf::st_as_sf(data_ex$admin0), dsn = file0)
      rm(data_ex)
      gc()
    }
    # Repeat for admin levels 1 and 2.
    if (!file.exists(file1) | overwrite == T) {
      cat('\r', paste("Risk x Exposure - admin extraction - adm1| severity:", SEV))
      flush.console()
      
      data_ex <- admin_extract(data, Geographies["admin1"], FUN = "sum")
      sfarrow::st_write_parquet(obj = sf::st_as_sf(data_ex$admin1), dsn = file1)
      rm(data_ex)
      gc()
    }
    if (!file.exists(file2) | overwrite == T) {
      cat('\r', paste("Risk x Exposure - admin extraction - adm2| severity:", SEV))
      flush.console()
      
      data_ex <- admin_extract(data, Geographies["admin2"], FUN = "sum")
      sfarrow::st_write_parquet(obj = sf::st_as_sf(data_ex$admin2), dsn = file2)
      rm(data_ex)
      gc()
    }
  }
}

#' Recode and Restructure Data for Analysis
#'
#' This function recodes and restructures a dataset by renaming variables according to predefined mappings. It supports handling of crop names, livestock, severity levels, exposure variables, and hazards, including their interactions. The function prepares data for further analysis by adjusting variable names for consistency and facilitating their categorization.
#'
#' @param data A `data.table` containing the dataset to be restructured.
#' @param crops Vector of crop names to be adjusted in the dataset.
#' @param livestock Vector of livestock names to be adjusted.
#' @param Scenarios Data frame or list that contains scenario mappings.
#' @param exposure_var Name of the exposure variable to be adjusted.
#' @param severity Severity levels present in the data.
#' @param hazards Vector of hazard types to be adjusted in the dataset.
#' @param interaction Logical indicating if interaction effects are to be considered.
#' @return A `data.table` with recoded and restructured data.
#' @examples
#' # Assuming 'data' is your dataset and other parameters are defined:
#' recoded_data <- recode_restructure(data, crops, livestock, Scenarios, "exposure_var", "severity", hazards, FALSE)
#' @export
recode_restructure<-function(data,crops,livestock,Scenarios,exposure_var,severity,hazards,interaction){
  
  variable_old<-data[,as.character(unique(variable))]
  
  # Replace space in the crop names with a . to match the parquet column names
  new<-gsub(" ",".",crops,fixed=T)
  old<-crops
  
  variable_old2<-stringi::stri_replace_all_regex(variable_old,pattern=old,replacement=new,vectorise_all = F)
  
  # Replace . in crop names with a 
  new<-gsub(" ","_",crops,fixed = T)
  old<-gsub(" ",".",crops)
  
  variable_old2<-stringi::stri_replace_all_regex(variable_old2,pattern=old,replacement=new,vectorise_all = F)
  
  # Replace dots in hazard names with a "+"
  old<-c("dry[.]heat","dry[.]wet","heat[.]wet","dry[.]heat[.]wet")
  new<-c("dry+heat","dry+wet","heat+wet","dry+heat+wet")
  
  variable_old2<-stringi::stri_replace_all_regex(variable_old2,pattern=old,replacement=new,vectorise_all = F)
  
  # Renaming of variable to allow splitting
  new<-paste0(Scenarios$combined,"-")
  old<-paste0(Scenarios[,paste0(Scenario,".",Time)],".")
  
  # Replace space in the crop names with a . to match the parquet column names
  new<-c(new,paste0("-",crops,"-"))
  old<-c(old,paste0("[.]",gsub(" ","_",crops,fixed = T),"[.]"))
  
  new<-c(new,paste0("-",livestock,"-"))
  old<-c(old,paste0("[.]",livestock,"[.]"))
  
  new<-c(new,paste0("-",exposure_var))
  old<-c(old,paste0("[.]",exposure_var))
  
  new<-c(new,paste0(c("any",hazards),"-"))
  old<-c(old,paste0(c("any",hazards),"[.]"))
  
  new<-c(new,paste0(c("any",hazards),"-"))
  old<-c(old,paste0(c("any",hazards),"_"))
  
  # Temporary inclusion to deal with solo practice naming
  if(interaction==F){
    new<-c(new,paste0(c("any",hazards),"-"))
    old<-c(old,paste0(c("any",hazards),"[.]"))
  }
  
  variable_new<-data.table(variable=stringi::stri_replace_all_regex(variable_old2,pattern=old,replacement=new,vectorise_all = F))
  
  split<-variable_new[,list(var_split=list(tstrsplit(variable[1],"-"))),by=variable]
  split_tab<-rbindlist(split$var_split)
  colnames(split_tab)<-c("scenario","timeframe","hazard","hazard_vars","crop","severity","exposure")
  split_tab$variable<-variable_old
  
  split_tab[,hazard:=gsub(".","+",hazard[1],fixed=T),by=hazard
  ][,hazard_vars:=gsub(".","+",hazard_vars[1],fixed=T),by=hazard_vars
  ][,scenario:=unlist(tstrsplit(scenario[1],".",keep=2,fixed=T)),by=scenario
  ][,severity:=tolower(severity)]
  
  data<-data.table(merge(data,split_tab,all.x=T))
  data[,variable:=NULL]
  
  if(data[,any(is.na(hazard_vars))]){
    warning("There are na values in the hazard_vars field which indicates a non match between the split variable name table and the orginal data table provided")
  }
  
  return(data)
}

#' Recode and Restructure Data from Parquet Files
#'
#' Reads, recodes, and restructures data from Parquet files based on specified interactions, severity, administrative levels, crops, livestock, and hazards. The function saves the restructured data into a new Parquet file.
#'
#' @param folder Directory containing the input Parquet files.
#' @param file Base name for input Parquet files.
#' @param crops Vector of crop names to be considered in the recoding process.
#' @param livestock Vector of livestock names to be considered.
#' @param exposure_var The exposure variable to be considered in recoding.
#' @param severity Severity level to filter and process the data.
#' @param overwrite Logical indicating whether to overwrite existing output files.
#' @param interaction Logical indicating if interactions between hazards should be considered.
#' @param levels Administrative levels (e.g., "adm0", "adm1", "adm2") to be processed.
#' @param hazards Vector of hazard types to be considered in the recoding process.
#' @return Saves the restructured data as a new Parquet file in the specified directory. Does not return data to the R environment.
#' @examples
#' # Example usage:
#' recode_restructure_wrap(folder = "/path/to/folder",
#'                         file = "data",
#'                         crops = c("Wheat", "Maize"),
#'                         livestock = c("Cattle", "Sheep"),
#'                         exposure_var = "exposure",
#'                         severity = "high",
#'                         overwrite = TRUE,
#'                         interaction = TRUE,
#'                         levels = c("adm0", "adm1", "adm2"),
#'                         hazards = c("Drought", "Flood"))
#' @export
recode_restructure_wrap <- function(folder, file, crops, livestock, exposure_var, severity, overwrite, interaction = T, levels, hazards) {
  
  # Determine the filename based on interaction and severity parameters.
  if (interaction == T) {
    filename <- paste0(folder, "/", severity, "_", file, "_int.parquet")
  } else {
    filename <- paste0(folder, "/", severity, "_", file, "_solo.parquet")
  }
  
  # Check if the file should be created or overwritten.
  if ((!file.exists(filename)) | overwrite == T) {
    # Concatenate data from different administrative levels.
    data <- rbindlist(lapply(1:length(levels), FUN = function(i) {
      level <- levels[i]
      
      # Display current progress.
      cat('\r', paste("Risk x Exposure - ", exposure_var, " restructuring data | severity: ", severity, " | admin level:", level, " | interaction = ", interaction))
      flush.console()
      
      # Define the specific data file based on the current level and interaction.
      data_file <- if (interaction == T) {
        paste0(folder, "/", severity, "_", levels[i], "_int.parquet")
      } else {
        paste0(folder, "/", severity, "_", levels[i], "_solo.parquet")
      }
      
      # Read the specified Parquet file as a data table.
      data <- data.table(data.frame(sfarrow::st_read_parquet(data_file)))
      
      # Remove columns not needed for further analysis.
      data <- data[, !c("geometry", "iso3", "admin_name")]
      
      # Correct truncated administrative column names.
      colnames(data) <- gsub("_nam$", "_name", colnames(data))
      
      # Define the names of administrative columns to keep based on the level.
      admins <- "admin0_name"
      if (level %in% c("adm1", "adm2")) {
        admins <- c(admins, "admin1_name")
      }
      if (level == "adm2") {
        admins <- c(admins, "admin2_name")
      }
      
      # Reshape the data table for restructuring.
      data <- data.table(melt(data, id.vars = admins))
      
      # Apply the recoding and restructuring process.
      data <- recode_restructure(data = data, crops = crops, livestock = livestock, Scenarios = Scenarios, exposure_var = exposure_var, severity = severity, hazards = hazards, interaction = interaction)
      
      data
    }), fill = T)
    
    # Save the restructured data as a new Parquet file.
    arrow::write_parquet(data, filename)
  }
}

#' Apply High and Low Livestock Masks and Combine Data
#'
#' This function takes a dataset and applies separate masks for high and low categories of livestock, then combines these processed segments. It ensures that data is appropriately masked according to predefined criteria, facilitating differentiated analysis or treatment based on livestock density levels.
#'
#' @param data A numeric vector or matrix representing the original livestock data.
#' @param livestock_mask_high A numeric vector or matrix serving as the mask for high-density livestock data.
#' @param livestock_mask_low A numeric vector or matrix serving as the mask for low-density livestock data.
#' @return Returns a combined numeric vector of the processed data after applying both high and low masks.
#' @examples
#' # Assuming 'data', 'livestock_mask_high', and 'livestock_mask_low' are predefined:
#' data_processed <- split_livestock(data, livestock_mask_high, livestock_mask_low)
#' @export
split_livestock <- function(data, livestock_mask_high, livestock_mask_low) {
  names(data) <-gsub("chicken","poultry",names(data))
  names(data) <-gsub("pig$","pigs",names(data))
  data_names<-names(data)  
  
  names(livestock_mask_high)<-unlist(tstrsplit(names(livestock_mask_high),"_",keep=1))
  livestock_mask_high<-livestock_mask_high[[names(data)]]
  
  if(!all(names(data)==names(livestock_mask_high))){
    stop("Misalignment with mask and dataset names")
  }
  data_high <- data * livestock_mask_high
  names(data_high)<-paste0(names(data_high),"_highland")
  
  
  names(livestock_mask_low)<-unlist(tstrsplit(names(livestock_mask_low),"_",keep=1))
  livestock_mask_low<-livestock_mask_low[[names(data)]]
  
  if(!all(names(data)==names(livestock_mask_low))){
    stop("Misalignment with mask and dataset names")
  }
  data_low <- data * livestock_mask_low
  names(data_low)<-paste0(names(data_low),"_tropical")
  
  # Combine the high and low data into a single vector.
  data_joined <- c(data_low, data_high)
  return(data_joined)
}
#' Prepare FAO Data
#'
#' This function processes food and agriculture data from the FAO file,
#' subsetting it according to specified elements, units, and years, and augmenting
#' it with atlas commodity names, ISO3 country codes, and handling missing data.
#'
#' @param file A string representing the path to the FAO data file to be processed.
#' @param lps2fao A named vector where names correspond to atlas commodity names
#'        and values correspond to FAO item codes. If NULL, atlas names are assumed
#'        to be identical to FAO item names.
#' @param elements An optional vector of element names to subset the data.
#'        If NULL, no subsetting is done based on elements.
#' @param units An optional vector of unit names to subset the data.
#'        If NULL, no subsetting is done based on units.
#' @param remove_countries A vector of country names to be removed from the data.
#' @param keep_years A vector of years (as integers or strings without the 'Y' prefix)
#'        to specify which years' data should be retained.
#' @param atlas_iso3 A vector of ISO3 country codes representing countries to be
#'        kept in the dataset.
#' @return A data.table that has been subsetted, augmented, and potentially expanded
#'         with missing combinations of crops and countries, according to the parameters.
#' @export
#' @examples
#' prepare_fao_data("fao_data.csv", lps2fao = c("Wheat" = "15"),
#'                  elements = c("Production"), units = c("tonnes"),
#'                  remove_countries = c("China"), keep_years = 1990:2020,
#'                  atlas_iso3 = c("USA", "BRA", "ARG"))
prepare_fao_data <- function(file, lps2fao, elements = NULL, units = NULL, remove_countries, keep_years, atlas_iso3) {
  
  # Read data from the file
  data <- fread(file)
  
  # Subset data based on elements if provided
  if(!is.null(elements)) {
    data <- data[Element %in% elements]
  }
  
  # Subset data based on units if provided
  if(!is.null(units)) {
    data <- data[Unit %in% units]
  }
  
  # Add atlas commodity names or use item names if lps2fao is null
  if(!is.null(lps2fao)) {
    data <- data[Item %in% lps2fao][, atlas_name := names(lps2fao)[match(Item, lps2fao)]]
  } else {
    data[, atlas_name := Item]
  }
  
  # Convert Area Code (M49) to ISO3 codes and filter by atlas_iso3 countries
  data[, M49 := as.numeric(gsub("[']", "", `Area Code (M49)`))]
  data[, iso3 := countrycode(sourcevar = M49, origin = "un", destination = "iso3c")]
  data <- data[iso3 %in% atlas_iso3]
  
  # Remove specified countries
  data <- data[!Area %in% remove_countries]
  
  # Keep only the specified years
  keep_years <- paste0("Y", keep_years)
  keep_cols <- c("iso3", "atlas_name", keep_years)
  data <- data[, ..keep_cols]
  
  # Add missing crop-country combinations
  fao_countries <- unique(data[, .(iso3)])
  crops <- data[, unique(atlas_name)]
  
  # Generate missing crop entries for each country
  missing <- rbindlist(lapply(1:nrow(fao_countries), FUN = function(i) {
    country <- fao_countries[i, iso3]
    missing_crops <- crops[!crops %in% data[iso3 == country, atlas_name]]
    if(length(missing_crops) > 0) {
      data <- data.table(iso3 = country, atlas_name = missing_crops)
      data[, (keep_years) := NA]
      data
    } else {
      NULL
    }
  }))
  
  # Combine original and missing data
  data <- rbind(data, missing)
  
  # Handle missing countries
  missing_countries <- atlas_iso3[!atlas_iso3 %in% data$iso3]
  
  if(length(missing_countries) > 0) {
    missing <- rbindlist(lapply(1:length(missing_countries), FUN = function(i) {
      data <- data.table(iso3 = missing_countries[i], atlas_name = data[, unique(atlas_name)])
      data[, (keep_years) := NA]
      data
    }))
    
    data <- rbind(data, missing)
  }
  
  return(data)
}

#' African Countries and Their Neighbors
#'
#' This dataset provides a mapping of African countries to their neighboring countries. 
#' Each country is represented by its ISO 3166-1 alpha-3 code, and neighbors are listed 
#' in vectors of ISO 3166-1 alpha-3 codes. For island nations or countries without any 
#' land-based neighbors, an empty vector is provided.
#'
#' Usage:
#' 
#' data(african_neighbors)
#'
#' Format:
#'
#' A list where each element is named by the ISO 3166-1 alpha-3 code of an African country. 
#' Each element is a character vector containing the ISO 3166-1 alpha-3 codes of its neighboring countries.
#'
#' Details:
#'
#' The `african_neighbors` dataset can be used to explore geographic, economic, and 
#' environmental relationships between African countries and their neighbors. It is 
#' particularly useful for analyses that require understanding of regional dynamics, such 
#' as trade, migration, and environmental policy studies.
#'
#' Note:
#'
#' - Countries with no listed neighbors are primarily island nations.
#' - The dataset is based on current geopolitical boundaries as of [year]. Geopolitical 
#'   changes may necessitate updates to this dataset.
#'
#' Examples of usage:
#' 
#' Accessing neighbors of Kenya:
#' 
#' \dontrun{
#' neighbors_of_kenya <- african_neighbors[["KEN"]]
#' print(neighbors_of_kenya)
#' }
#'
#' Counting the number of neighbors for each country:
#'
#' \dontrun{
#' num_neighbors <- sapply(african_neighbors, length)
#' print(num_neighbors)
#' }
african_neighbors <- list(
  DZA = c("TUN", "LBY", "NER", "ESH", "MRT", "MLI", "MAR"),
  AGO = c("COG", "COD", "ZMB", "NAM"),
  BEN = c("BFA", "NER", "NGA", "TGO"),
  BWA = c("ZMB", "ZWE", "NAM", "ZAF"),
  BFA = c("MLI", "NER", "BEN", "TGO", "GHA", "CIV"),
  BDI = c("COD", "RWA", "TZA"),
  CPV = c(),
  CMR = c("NGA", "TCD", "CAF", "COG", "GAB", "GNQ"),
  CAF = c("TCD", "SDN", "COD", "COG", "CMR"),
  TCD = c("LBY", "SDN", "CAF", "CMR", "NGA", "NER"),
  COM = c(),
  COG = c("GAB", "CMR", "CAF", "COD", "AGO"),
  COD = c("CAF", "SSD", "UGA", "RWA", "BDI", "TZA", "ZMB", "AGO", "COG"),
  CIV = c("LBR", "GIN", "MLI", "BFA", "GHA"),
  DJI = c("ERI", "ETH", "SOM"),
  EGY = c("LBY", "SDN", "ISR", "PSE"),
  GNQ = c("CMR", "GAB"),
  ERI = c("ETH", "SDN", "DJI"),
  SWZ = c("MOZ", "ZAF"),
  ETH = c("ERI", "DJI", "SOM", "KEN", "SSD", "SDN"),
  GAB = c("CMR", "GNQ", "COG"),
  GMB = c("SEN"),
  GHA = c("CIV", "BFA", "TGO"),
  GIN = c("LBR", "SLE", "CIV", "MLI", "SEN"),
  GNB = c("SEN", "GIN"),
  KEN = c("ETH", "SOM", "SSD", "UGA", "TZA"),
  LSO = c("ZAF"),
  LBR = c("GIN", "CIV", "SLE"),
  LBY = c("TUN", "DZA", "NER", "TCD", "SDN", "EGY"),
  MDG = c(),
  MWI = c("MOZ", "TZA", "ZMB"),
  MLI = c("DZA", "NER", "BFA", "CIV", "GIN", "SEN", "MRT"),
  MRT = c("DZA", "ESH", "SEN", "MLI"),
  MAR = c("DZA", "ESH", "ESP"),
  MOZ = c("ZAF", "SWZ", "ZWE", "ZMB", "MWI", "TZA"),
  NAM = c("AGO", "BWA", "ZAF", "ZMB"),
  NER = c("DZA", "LBY", "TCD", "NGA", "BEN", "BFA", "MLI"),
  NGA = c("BEN", "CMR", "TCD", "NER"),
  RWA = c("BDI", "COD", "TZA", "UGA"),
  STP = c(),
  SEN = c("GMB", "GIN", "GNB", "MLI", "MRT"),
  SYC = c(),
  SLE = c("GIN", "LBR"),
  SOM = c("ETH", "DJI", "KEN"),
  ZAF = c("NAM", "BWA", "ZWE", "MOZ", "SWZ", "LSO"),
  SSD = c("CAF", "COD", "ETH", "KEN", "UGA"),
  SDN = c("EGY", "ERI", "ETH", "SSD", "CAF", "TCD", "LBY"),
  TZA = c("KEN", "UGA", "RWA", "BDI", "COD", "ZMB", "MWI", "MOZ"),
  TGO = c("BEN", "BFA", "GHA"),
  TUN = c("DZA", "LBY"),
  UGA = c("KEN", "SSD", "COD", "RWA", "TZA"),
  ZMB = c("AGO", "COD", "MWI", "MOZ", "NAM", "TZA", "ZWE"),
  ZWE = c("BWA", "MOZ", "ZAF", "ZMB")
)

#' African Countries Categorized by Region
#'
#' This dataset categorizes African countries into their respective regions: East Africa,
#' Southern Africa, West Africa, Central Africa, and North Africa. Each region is associated
#' with a vector of country codes (ISO 3166-1 alpha-3) representing the countries within that region.
#'
#' Usage:
#'
#' data(regions)
#'
#' Format:
#'
#' A list where each element is named by the region. Each element is a character vector 
#' containing the ISO 3166-1 alpha-3 codes of countries belonging to that region.
#'
#' Details:
#'
#' The `regions` dataset facilitates regional analyses by providing an easy way to group 
#' countries by their geographic regions. This is useful for studies focusing on economic, 
#' environmental, or political patterns within specific areas of Africa.
#'
#' Note:
#'
#' - The dataset reflects the current geopolitical region classifications as of [year].
#' - Changes in geopolitical boundaries or regional classifications may necessitate updates to this dataset.
#'
#' Examples of usage:
#' 
#' Accessing countries in Southern Africa:
#' 
#' \dontrun{
#' southern_africa_countries <- regions[["Southern_Africa"]]
#' print(southern_africa_countries)
#' }
#'
#' Counting the number of countries in each region:
#'
#' \dontrun{
#' num_countries_per_region <- sapply(regions, length)
#' print(num_countries_per_region)
#' }
regions <- list(
  East_Africa = c("BDI", "COM", "DJI", "ERI", "ETH", "KEN", "MDG", "MUS", "MWI", "RWA", "SYC", "SOM", "SSD", "TZA", "UGA"),
  Southern_Africa = c("BWA", "LSO", "NAM", "SWZ", "ZAF", "ZMB", "ZWE","MOZ"),
  West_Africa = c("BEN", "BFA", "CPV", "CIV", "GMB", "GHA", "GIN", "GNB", "LBR", "MLI", "MRT", "NER", "NGA", "SEN", "SLE", "TGO"),
  Central_Africa = c("AGO", "CMR", "CAF", "TCD", "COD", "COG", "GNQ", "GAB", "STP"),
  North_Africa = c("DZA", "EGY", "LBY", "MAR", "SDN", "TUN")
)

#' Map LPS names to FAOstat
lps2fao<-c(cattle_meat="Meat of cattle with the bone, fresh or chilled",
           cattle_milk="Raw milk of cattle",
           pig_meat="Meat of pig with the bone, fresh or chilled",
           poultry_eggs="Hen eggs in shell, fresh",
           poultry_meat="Meat of chickens, fresh or chilled",
           sheep_meat="Meat of sheep, fresh or chilled",
           sheep_milk="Raw milk of sheep",
           goat_meat="Meat of goat, fresh or chilled",
           goat_milk="Raw milk of goats")

#' Calculate Average Value of a Crop in Neighboring Countries
#'
#' This function computes the average value (e.g., production, yield) of a specified
#' crop across the neighboring countries of a given country. It uses a provided list
#' of neighboring countries and a data table containing the crop data.
#'
#' @param iso3 The ISO 3166-1 alpha-3 code of the country for which to find the
#'        average value of the specified crop in its neighboring countries.
#' @param crop The name of the crop for which the average value is to be calculated.
#' @param neighbors A list where each key is an ISO 3166-1 alpha-3 country code
#'        and the associated value is a vector of its neighboring countries' ISO codes.
#' @param data A data.table or data.frame that contains the crop data. It must
#'        include columns for country codes (ISO3), crop names, and the value
#'        field specified.
#' @param value_field The name of the field in `data` from which to calculate the
#'        average value (e.g., "production", "yield"). This field name is dynamically
#'        used to reference the relevant column in the data table.
#'
#' @return The average value of the specified crop across the neighboring countries
#'         of the given country. Returns `NA` if no data is available.
#' @examples
#' # Assuming `crop_data` is a data.table with columns "iso3", "atlas_name", and "production",
#' # and `neighbors_list` is similar to `african_neighbors`:
#' avg_prod <- avg_neighbors(iso3 = "KEN", crop = "Maize", neighbors = neighbors_list, data = crop_data, value_field = "production")
#' print(avg_prod)
avg_neighbors <- function(iso3, crop, neighbors, data, value_field) {
  # Retrieve the actual neighbors of the country using its ISO3 code from the provided list
  neighbors <- neighbors[[iso3]]
  
  # Temporarily rename the specified value field to "value" for easier manipulation
  setnames(data, value_field, "value")
  
  # Calculate the mean value of the specified crop across the neighbors
  N <- data[atlas_name == crop & iso3 %in% neighbors, mean(value, na.rm = TRUE)]
  
  # Return the calculated average
  return(N)
}

#' Calculate Average Crop Value in the Same Region Excluding the Given Country
#'
#' Computes the average value (e.g., production, yield) of a specified crop across countries
#' in the same geographic region as the given country, excluding the country itself. This function
#' leverages a predefined list categorizing countries into regions.
#'
#' @param iso3 The ISO 3166-1 alpha-3 code of the country used to determine the region
#'        for which the average crop value is calculated.
#' @param crop The name of the crop for which the average value is to be calculated.
#' @param regions A list where each key is a region name and the associated value is a
#'        vector of country codes (ISO 3166-1 alpha-3) belonging to that region.
#' @param data A data.table or data.frame that contains the crop data. It must
#'        include columns for country codes (ISO3), crop names, and the value
#'        field specified.
#' @param value_field The name of the field in `data` from which to calculate the
#'        average value (e.g., "production", "yield"). This field name is dynamically
#'        used to reference the relevant column in the data table.
#'
#' @return The average value of the specified crop across countries in the same region
#'         as the given country, excluding the country itself. Returns `NA` if no data is available.
#' @examples
#' # Assuming `crop_data` is a data.table with columns "iso3", "atlas_name", and "production",
#' # and `regions_list` categorizes countries into regions:
#' avg_prod_region <- avg_regions(iso3 = "KEN", crop = "Maize", regions = regions_list, data = crop_data, value_field = "production")
#' print(avg_prod_region)
avg_regions <- function(iso3, crop, regions, data, value_field) {
  # Identify the region to which the given country belongs
  region_focal <- names(regions)[sapply(regions, FUN = function(X) { iso3 %in% X })]
  
  # Retrieve the countries in the same region as the given country
  neighbors <- regions[[region_focal]]
  
  # Temporarily rename the specified value field to "value" for easier manipulation
  setnames(data, value_field, "value")
  
  # Calculate the mean value of the specified crop across countries in the same region,
  # excluding the given country
  iso3_target<-iso3
  N <- data[atlas_name == crop & iso3 %in% neighbors & iso3 != iso3_target, mean(value, na.rm = TRUE)]
  
  # Return the calculated average
  return(N)
}

#' Enhance Data with Mean Values from Neighbors, Region, and Continent
#'
#' This function augments a given dataset with additional columns representing the mean
#' values of a specified field across neighboring countries, the same region, and the
#' entire continent. It also calculates a composite mean that prioritizes the most
#' specific available data (neighbor, region, continent).
#'
#' @param data A data.table or data.frame containing the dataset to be enhanced.
#' @param value_field The name of the field in `data` from which to calculate mean values.
#' @param neighbors A list where each key is an ISO 3166-1 alpha-3 country code and the
#'        associated value is a vector of its neighboring countries' ISO codes.
#' @param regions A list categorizing countries into different regions, where each key
#'        is a region name and the associated value is a vector of country codes in that region.
#'
#' @return The original `data` augmented with new columns for mean values calculated
#'         from neighbors, the same region, and the entire continent. Additionally, a
#'         `value_field_mean_final` column is added, which is a composite value
#'         prioritizing neighbor, then regional, then continental averages.
#' @examples
#' # Assuming `crop_data` is a data.table with columns "iso3", "atlas_name", and "production",
#' # `neighbors_list` similar to `african_neighbors`, and `regions_list` categorizes
#' # countries into regions:
#' enhanced_data <- add_nearby(data = crop_data, value_field = "production",
#'                             neighbors = neighbors_list, regions = regions_list)
#' print(enhanced_data)
add_nearby <- function(data, value_field, neighbors, regions) {
  # Calculate and add the mean value from neighbors
  data[, mean_neighbors := avg_neighbors(iso3 = iso3,
                                           crop = atlas_name,
                                           neighbors = neighbors,
                                           data = copy(data),
                                           value_field = value_field),
       by = list(iso3, atlas_name)]
  
  # Calculate and add the mean value from the same region
  data[, mean_region := avg_regions(iso3 = iso3,
                                    crop = atlas_name,
                                    regions = regions,
                                    data = copy(data),
                                    value_field = value_field),
       by = list(iso3, atlas_name)]
  
  # Calculate and add the continental average
  setnames(data, value_field, "value")
  data[, mean_continent := mean(value, na.rm = TRUE), by = atlas_name]
  
  # Compute and add the composite value, prioritizing the most specific data available
  data[, mean_final := value
  ][is.na(mean_final), mean_final := mean_neighbors
  ][is.na(mean_final), mean_final := mean_region
  ][is.na(mean_final), mean_final := mean_continent]
  
  # Restore the original name of the value field
  setnames(data, "value", value_field)
  
  # Rename columns to reflect the mean values are related to the specified value field
  colnames(data) <- gsub("mean_", paste0(value_field, "_"), colnames(data))
  
  return(data)
}

#' Average Loss Function
#'
#' Calculates the average loss reduction by simulating normal distributions
#' with and without a specified change in standard deviation.
#' The reduction is expressed as a proportion of the total without the change.
#'
#' @param cv numeric, the initial coefficient of variation.
#' @param change numeric, the proposed change in standard deviation.
#' @param fixed logical, if TRUE, the change is capped at the value of cv.
#' @param reps integer, the number of repetitions for the simulation.
#' @return numeric, the average loss reduction proportion.
#' @examples
#' avloss(cv = 0.2, change = 0.05, fixed = TRUE, reps = 10000)
avloss <- function(cv, change, fixed = FALSE, reps = 10^6) {
  # Calculate co-efficient of variation
  x <- 1
  
  # Calculate new standard deviations
  if (fixed) {
    # Ensure fixed change does not exceed cv
    change <- min(change, cv)
    sd_with <- (cv - change) * x
  } else {
    sd_with <- (cv * (1 - change)) * x
  }
  
  sd_without <- cv * x
  
  # Avoid computation if the standard deviation would be negative
  if (sd_with < 0) {
    return(NA)
  }
  
  # Generate normal distributions
  with <- rnorm(n = reps, mean = x, sd = sd_with)
  without <- rnorm(n = reps, mean = x, sd = sd_without)
  
  # Direct calculation of sum of lower half without sorting
  with_lh <- sum(with[with <= median(with)])
  without_lh <- sum(without[without <= median(without)])
  
  # Calculate average loss reduction and express as proportion of total without innovation
  avloss <- (with_lh - without_lh) / sum(without)
  
  avloss[avloss < 0] <- 0
  
  return(avloss)
}
