# See https://github.com/wmgeolab/geoBoundaries/tree/main/releaseData for the data

try_download <- function(dl_url, destfile,n) {
  success <- FALSE  # Flag to track successful result
  result <- NULL  # Variable to store the result
  
  for (i in 1:n) {
    tryCatch(
      {
        download.file(dl_url, destfile,quiet=T)
        success <- TRUE  # Set flag to indicate success
        break  # Exit the loop if successful result obtained
      },
      error = function(e) {
        message(paste("Error retrying...", i, "/", n, sep = ""))
      }
    )
  }
  
  if (success) {
    "File downloaded successfully"
  } else {
    warning("Error: All attempts failed.")  # or any other desired error message
  }
}

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

release<-"gbOpen" # other options are agAuthoritative, gbHumanitarian
# adm_levels - options are ADM0 to ADM4
# consider integrating geoBoundaries/releaseData/geoBoundariesOpen-meta.csv to subset data available for iso3 x adm_levels x release


dl_geoboundaries(savedir="Data/geoboundaries",
                 iso3=read.csv("Data/metadata/countries.csv")$iso3,
                 adm_levels=c("ADM0","ADM1","ADM2"),
                 release="gbOpen",
                 attempts=3)
