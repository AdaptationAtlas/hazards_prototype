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
#' This function replaces infinite values in a vector or matrix with NA.
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
HazardWrapper<-function(Thresholds,FileName,SaveDir,PropThreshold,PropTDir,hazard_dir,Scenarios,verbose=F){
  Files<-list.files(hazard_dir,".tif",full.names = T)
  Files<-Files[grepl("historical|ENSEMBLE",Files) & !grepl("tif.aux.xml",Files)]
  
  Thresholds_unique<-unique(Thresholds[,list(Variable,Renamed,Severity_class)])
  
  Hazards<-lapply(1:nrow(Scenarios),FUN=function(j){
    for(i in 1:nrow(Thresholds_unique)){
      if(verbose){
        # Display progress
        cat('\r                                                                                                                     ')
        cat('\r',paste0("Scenario ", j," | Hazard ",i))
        flush.console()
      }
      
      Threshold_focal<-Thresholds[Variable==Thresholds_unique$Variable[i] & Severity_class==Thresholds_unique$Severity_class[i]]
      Threshold_focal[,Code:=paste0(Direction,threshold)
                      ][,Code:=gsub("<","L",Code)
                        ][,Code:=gsub(">","G",Code)]
      TCode<-Threshold_focal[,paste(Code,collapse="_")]
      
      
      save_file<-paste0(SaveDir,"/Haz-",Scenarios$Scenario[j],"-",Scenarios$Time[j],"-",Thresholds_unique[i,Variable],"-",TCode,".tif")
      
      if(!file.exists(save_file)){
        
        Data<-terra::rast(Files[grepl(Scenarios$Scenario[j],Files) & grepl(Scenarios$Time[j],Files) & grepl(Thresholds_unique$Variable[i],Files)])
        
        
        X<-ClassifyFun(Data=Data,
                       VAR=Thresholds_unique[i,Variable],
                       Threshold=Threshold_focal$threshold,
                       Direction=Threshold_focal$Direction,
                       PropThreshold=PropThreshold,
                       PropTDir=PropTDir)
        terra::writeRaster(X,filename=save_file,overwrite=T)
      }else{
        X<-terra::rast(save_file)
      }
      
      if(i==1){
        Hazards<-X
      }else{
        Hazards<-c(Hazards,X)
      }
    }
    
    Hazards
  })
  
  Thresholds_unique<-unique(Thresholds_unique[,list(Variable,Renamed)])
  
  Hazards<-lapply(1:length(Hazards),FUN=function(i){
    HAZ<-Hazards[[i]]
    names(HAZ)<-mgsub::mgsub(names(HAZ),Thresholds_unique[,paste0(Variable,"_")],Thresholds_unique[,paste0(Renamed,"_")])
    HAZ
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
PalFun<-function(PalName,N,Names) {
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
  names(PAL)<-Names
  
  return(PAL)
}

#' Adjusted USD Function
#'
#' This function adjusts a given value in USD currency based on the past and future exchange rates and indices.
#'
#' @param value A numeric value representing an amount in USD currency.
#' @param xrat_past A numeric value representing the past exchange rate.
#' @param xrat_fut A numeric value representing the future exchange rate.
#' @param index_past A numeric value representing the past index.
#' @param index_fut A numeric value representing the future index.
#' 
#' @return A numeric value representing the adjusted amount in USD currency.
#' 
#' @examples
#' adj_usd(100, 0.85, 0.90, 200, 220)
#' 
#' @export
adj_usd<-function(value,xrat_past,xrat_fut,index_past,index_fut){
  X<-value*xrat_past
  X<-X/index_past
  X<-X*index_fut
  X<-X/xrat_fut
  return(X)
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
  
  Data<-melt(Data,id.vars="admin")
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
hazard_index<-function(Hazards,verbose=T,SaveDir,crop_choice){
  scenario_names<-names(Hazards)
  severity_classes<-data.table(class=c("Moderate","Severe","Extreme"),value=c(1,2,3))
  
  haz_index<-lapply(1:length(scenario_names),FUN = function(j){
    
    haz_index_filename<-paste0(SaveDir,"/hi_",crop_choice,"_",scenario_names[j],".tif")
    
    if(!file.exists(haz_index_filename)){
      
      data<-Hazards[[scenario_names[[j]]]]
      
      # recurrence 
      recurrence<-data[[grep("_prop_",names(data),value=T)]]
      
      # Subtract severe and extreme from moderate, and severe from extreme
      for(i in 1:length(hazards)){
        
        if(verbose){
          # Display progress
          cat('\r                                                                                                                     ')
          cat('\r',paste0("Scenario ", scenario_names[j]," | Hazard ",hazards[i]))
          flush.console()
        }
        
        N<-paste0(hazards[i],"_prop_",severity_classes$class)
        N1<-which(names(recurrence)==N[1])
        N2<-which(names(recurrence)==N[2])
        X<-recurrence[[N[1]]]-recurrence[[N[2]]]-recurrence[[N[3]]]
        X[][X[]<0 & !is.na(X[])]<-0
        recurrence[[N1]]<-X
        Y<-recurrence[[N2]]-recurrence[[N[3]]]
        Y[][Y[]<0 & !is.na(Y[])]<-0
        recurrence[[N2]]<-Y
      }
      
      
      severity<-data[[grep("_propclass",names(data),value=T)]]
      
      
      for(i in 1:nrow(severity_classes)){
        N<-which(grepl(severity_classes$class[i],names(severity)))
        severity[[N]]<-severity[[N]]*severity_classes$value[i]
      }
      
      haz_index<-recurrence*severity
      
      haz_index<-terra::rast(lapply(1:length(hazards),FUN=function(i){
        N<-paste0(hazards[i],"_prop_",severity_classes$class)
        X<-terra::app(haz_index[[N]],sum,na.rm=T)
        names(X)<-paste0(hazards[i],"_hazard_index")
        X
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
