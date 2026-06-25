#Get reruns file
#JRV, Jan 2023

# Shared Stage-0 setup (so common_data_root() is available when sourced standalone)
local({
  cargs <- commandArgs(FALSE)
  fa <- grep("^--file=", cargs, value = TRUE)
  base <- if (length(fa)) dirname(normalizePath(sub("^--file=", "", fa[1]))) else getwd()
  cand <- c(file.path(base, "..", "00_setup.R"), file.path(base, "00_setup.R"),
            "../00_setup.R", "00_setup.R")
  hit <- cand[file.exists(cand)][1]
  if (is.na(hit)) stop("00_setup.R not found from ", base)
  source(normalizePath(hit), local = FALSE)
})

getReruns <- function(newfiles=NULL) {
  # Base directory
  root <- common_data_root()
  
  # Data fixes
  bad_fls <- readRDS(paste0(root, "/atlas_hazards/cmip6/corrupted_files.RDS"))
  bad_fls <- 1:nrow(bad_fls) %>%
    purrr::map(.f=function(i) {
      bfls <- bad_fls$corrupted[i]
      if (!is.na(bfls[[1]][1])) {newrow <- bad_fls[i,]} else {newrow <- NA}
      return(newrow)
    })
  bad_fls <- bad_fls[!is.na(bad_fls)]
  bad_fls <- dplyr::bind_rows(bad_fls)
  
  # Add other files that i found manually. Note I hardcoded this at position 2 
  # of the list because it only happens there. But this would have to be coded 
  # better if there is another case anywhere else (which there isn't)
  bad_fls$corrupted[2][[1]] <- c(bad_fls$corrupted[2][[1]], newfiles)
  
  # Get full list of GCM x SSP x PERIOD x VARIABLE x YEAR x MONTH
  reruns_df <- data.frame()
  for (i in 1:nrow(bad_fls)) {
    tgcm <- bad_fls$gcm[i]
    tssp <- bad_fls$ssp[i]
    tprd <- bad_fls$prd[i]
    bfls <- bad_fls$corrupted[i]
    for (k in 1:length(bfls[[1]])) {
      tfile <- bfls[[1]][k]
      
      #get year, month, day
      bname <- basename(tfile) %>% 
        gsub("chirps-v2.0.", "", .) %>%
        gsub("Tmax.", "", .) %>%
        gsub("Tmin.", "", .) %>%
        gsub("\\.tif", "", .) %>%
        strsplit(., split=".", fixed=TRUE) %>%
        unlist(.)
      
      #get variable name
      if (length(grep("chirps-v2.0.", basename(tfile))) != 0) {vname <- "Prec"}
      if (length(grep("Tmax.", basename(tfile))) != 0) {vname <- "Tmax"}
      if (length(grep("Tmin.", basename(tfile))) != 0) {vname <- "Tmin"}
      
      #output row
      out_row <- data.frame(gcm=tgcm, ssp=tssp, prd=tprd, varname=vname,
                            yr=as.numeric(bname[1]), mn=bname[2])
      reruns_df <- rbind(reruns_df, out_row)
    }
  }
  return(reruns_df)
}
