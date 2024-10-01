#' Download GeoBoundaries Data
#'
#' Downloads GeoBoundaries data files for specified ISO3 country codes and administrative levels.
#' See https://github.com/wmgeolab/geoBoundaries/tree/main/releaseData for the data
#'
#' @param savedir Directory to save the downloaded files.
#' @param iso3 Vector of ISO3 country codes
#' @param adm_levels Vector of administrative level codes (ADM0,ADM1,ADM2,ADM3,ADM4).
#' @param release Version/release of GeoBoundaries data (gbOpen,gbAuthoratative,gbHumanitarian).
#' @param attempts Number of attempts to download the file.
#'
#' @return None
#'
#' @examples
#' dl_geoboundaries(savedir = "data", 
#'                  iso3 = c("USA", "CAN"), 
#'                  adm_levels = c("ADM1", "ADM2"), 
#'                  release = "gbOpen", 
#'                  attempts = 3)
#'
#' @export
dl_geoboundaries<-function(savedir,iso3,adm_levels,release,attempts){


  if(!dir.exists(savedir)){
    dir.create(savedir,recursive=T)
  }
  
  url<-paste0("https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/",release)
  
  for(i in 1:length(iso3)){
    for(j in 1:length(adm_levels)){
      file<-paste0(iso3[i],"-",adm_levels[j],"-all.zip")
      
      dl_url<-paste0(url,"/",iso3[i],"/",adm_levels[j],"/geoBoundaries-",file)
      destfile<-paste0(savedir,"/",file)
      

      
      if(!file.exists(destfile)){
        try_download(dl_url, destfile,n=attempts)
        # Display progress
        cat('\r                                                ')
        cat('\r',paste0("Downloading file: ",dl_url))
        flush.console()
      } 
  
  }}

}
