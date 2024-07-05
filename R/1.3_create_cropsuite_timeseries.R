# Please run 0_server_setup.R before executing this script
# Note this only runs from CGlabs server as cropSuite is not publicly available yet
# If you are experiencing issues with the admin_extract functions, delete the exactextractr package and use this version:  remotes::install_github("isciences/exactextractr")

# 1) Load R functions & packages ####
packages <- c("terra","data.table","future","doFuture","sf","geoarrow","arrow")
p_load(char=packages)

# 2) Set-up workspace ####
# 2.1) Load admin boundaries #####
Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
  file<-geo_files_local[i]
  data<-arrow::open_dataset(file)
  data <- data |> sf::st_as_sf() |> terra::vect()
  data
})
names(Geographies)<-names(geo_files_local)

# Remove admin2 to speed things up
Geographies$admin2<-NULL

# 2.2) Create file index #####
cropsuite_dirs<-list_bottom_directories(cropsuite_raw_dir)
cropsuite_dirs<-cropsuite_dirs[!grepl("ipynb_checkpoints",cropsuite_dirs)]

file_index<-data.table(file_path=list.files(cropsuite_raw_dir,".tif$",full.names=T,recursive = T))
file_index[,file_name:=basename(file_path)
           ][,scenario:=unlist(tstrsplit(file_name,"_",keep=1))
             ][,timeframe:=unlist(tstrsplit(file_name,"_",keep=2))
               ][,crop:=unlist(tstrsplit(file_name,"_",keep=3))
                 ][,variable:=gsub(".tif","",unlist(tstrsplit(file_name,"_",keep=4)))]

data.table::fwrite(file_index,file.path(cropsuite_raw_dir,"file_index.csv"))

# 2.3) Load mapspam codes and vop #####
spamcodes<-data.table::fread(ms_codes_url)
crop_vop_tot<-arrow::read_parquet(file.path(exposure_dir,"crop_vop_adm_sum.parquet"))

# 3) Extract suitability data ####

# 3.1) Set suitability class thresholds #####
# (do not change the class names else you will need to propagate the changes to the extraction code)
thresholds <- data.table(
  from = c(0, 1, 33, 74),
  to = c(0, 32, 74, 100),
  becomes = c(0, 1, 2, 3),
  name = c("unsuitable", "marginal", "moderate", "high")
)  # Create a data.table defining the thresholds for suitability classes

cats <- thresholds[, list(becomes, name)]  # Extract the 'becomes' and 'name' columns from the thresholds data.table
setnames(cats, "becomes", "value")  # Rename the 'becomes' column to 'value'

# Create a data.table to capture changes in suitability from historical to future scenarios
thresholds_change <- data.table(expand.grid(historical = thresholds$becomes, future = thresholds$becomes * 10))
cats2 <- thresholds[, list(becomes, name)]
setnames(cats2, c("becomes", "name"), c("historical", "name_hist"))
thresholds_change <- merge(thresholds_change, cats2, all.x = TRUE)  # Merge with historical suitability class names

cats2 <- thresholds[, list(becomes, name)][, becomes := becomes * 10]  # Scale future suitability classes
setnames(cats2, c("becomes", "name"), c("future", "name_fut"))
thresholds_change <- merge(thresholds_change, cats2, by = "future")  # Merge with future suitability class names
thresholds_change[, name := paste0(name_hist, "->", name_fut)][, value := future + historical]  # Create transition names and values

diff_levels <- thresholds_change[, list(value, name)]  # Extract transition levels

overwrite <- FALSE  # Flag to control file overwriting

# 3.2) Extract data #####
# Loop through each variable related to suitability in file_index
cs_vars<-file_index[grep("suitability", variable), unique(variable)]
cs_vars<-cs_vars[!grepl("mc",cs_vars)]

for(var in cs_vars){
  files <- file_index[variable == var, file_path]  # Get file paths for the current variable
  crops <- file_index[variable == var, unique(crop)]  # Get unique crops for the current variable
  
  save_file_diff_class <- file.path(cropsuite_class_dir, paste0(var, "_diff_class.tif"))  # Define path for the classified difference file
  save_file_diff <- file.path(cropsuite_class_dir, paste0(var, "_diff.tif"))  # Define path for the difference file
  
  if (!file.exists(save_file_diff) | overwrite) {
    data_diff <- terra::rast(lapply(1:length(crops), FUN = function(i) {
      # Display progress
      cat('\r', strrep(' ', 150), '\r')
      cat("processing crop", i, "/", length(crops), crops[i])
      flush.console()  # Ensure console output is updated
      
      files_hist <- file_index[variable == var & crop == crops[i] & scenario == "historical", file_path]  # Get historical files
      files_fut <- file_index[variable == var & crop == crops[i] & scenario != "historical", file_path]  # Get future files
      
      data_hist <- terra::rast(files_hist)  # Load historical data
      data_fut <- terra::rast(files_fut)  # Load future data
      
      data_diff <- data_hist - data_fut  # Calculate the difference between historical and future data
      names(data_diff) <- gsub(".tif", "", basename(files_fut))  # Rename the layers of the difference raster
      data_diff
    }))
    
    terra::writeRaster(data_diff, save_file_diff, overwrite = TRUE)  # Save the difference raster
    
    data_diff_ex <- admin_extract_wrap(
      data = data_diff, 
      save_dir = cropsuite_class_dir, 
      filename = paste0(var, "_diff"), 
      FUN = NULL, 
      varname = NA, 
      Geographies, 
      overwrite = TRUE,
      modify_colnames = FALSE
    )  # Extract and save administrative level summaries
    
    # Melt
    data_diff_ex<-melt(data_diff_ex,id.vars=c("admin0_name","admin1_name","coverage_fraction"))
    
    # Clean out NAs or zeros
    data_diff_ex <- data_diff_ex[!is.na(value) & !is.na(coverage_fraction) & coverage_fraction > 0]
    
    optimize_histograms <- function(data, breaks = 10) {
      # Create a list to store histograms
      hist_dat <- hist(data, breaks = breaks, plot = FALSE)
      # Combine counts and breaks into a single data.table
      hist_dat <- paste0(bin_counts =hist_dat$counts,bin_width = hist_dat$breaks)
      
      return(hist_data)
    }
    
    # Caculate stats
    data_diff_ex<-data_diff_ex[,list(mean=weighted.mean(value,coverage_fraction),
                       min=min(value),
                       max=max(value),
                       median=Hmisc::wtd.quantile(value, coverage_fraction, probs = 0.5),
                       bin_counts =list(as.numeric(hist(value, breaks = 10, plot = FALSE)$counts)),
                       bin_width = list(as.numeric(hist(value, breaks = 10, plot = FALSE)$breaks))),
                       by=.(admin0_name,admin1_name,variable)]
    
    # Split variable name into cols and round decimal places
    data_diff_ex <- data_diff_ex[, scenario := unlist(tstrsplit(variable[1], "_", keep = 1)), by = variable
                                 ][, timeframe := unlist(tstrsplit(variable[1], "_", keep = 2)), by = variable
                                   ][, crop := unlist(tstrsplit(variable[1], "_", keep = 3)), by = variable
                                     ][, variable := "% change in suitability"
                                       ][, mean := round(mean,1)
                                         ][, median := round(median,1)] 
    
    # Append vop data to weight average yields
    
    
    save_file <- file.path(cropsuite_class_dir, paste0(var, "_diff", "_adm_mean.parquet"))
    arrow::write_parquet(data_diff_ex, save_file)  # Save the cleaned data to a parquet file
    
    # Classify differences
    data_diff_class <- terra::rast(lapply(1:length(crops), FUN = function(i) {
      # Display progress
      cat('\r', strrep(' ', 150), '\r')
      cat("processing crop", i, "/", length(crops), crops[i])
      flush.console()  # Ensure console output is updated
      
      files_hist <- file_index[variable == var & crop == crops[i] & scenario == "historical", file_path]  # Get historical files
      files_fut <- file_index[variable == var & crop == crops[i] & scenario != "historical", file_path]  # Get future files
      
      data_hist <- terra::rast(files_hist)  # Load historical data
      data_fut <- terra::rast(files_fut)  # Load future data
      
      data_hist_class <- classify(data_hist, thresholds[, list(from, to, becomes)])  # Classify historical data
      data_fut_class <- classify(data_fut, thresholds[, list(from, to, becomes)][, becomes := becomes * 10])  # Classify future data
      data_change <- data_hist_class + data_fut_class  # Calculate change in suitability classes
      
      names(data_change) <- gsub(".tif", "", basename(files_fut))  # Rename the layers of the change raster
      data_change
    }))
    
    # Assign category names to each layer of the classified raster
    dat_names <- names(data_diff_class)
    levels(data_diff_class) <- rep(list(diff_levels), nlyr(data_diff_class))
    
    names(data_diff_class) <- dat_names
    terra::writeRaster(data_diff_class, save_file_diff_class, overwrite = TRUE)  # Save the classified change raster
    
    # Get cell size of raster
    base_cellsize <- terra::cellSize(data_diff_class, unit = "km")
    
    # Historical suitable area
    thresholds_past <- thresholds_change[, value_past := 0][name_hist %in% c("med", "high"), value_past := 1][, list(value, value_past)]
    thresholds_past <- setnames(thresholds_past, c("value", "value_past"), c("from", "to"))
    data_class_past <- terra::classify(data_diff_class, thresholds_past)  # Classify historical suitable area
    data_class_past_area <- data_class_past * base_cellsize  # Calculate area
    names(data_class_past_area) <- paste0(names(data_class_past_area), "_past-area")
    
    # Future suitable area
    thresholds_fut <- thresholds_change[, value_fut := 0][name_fut %in% c("med", "high"), value_fut := 1][, list(value, value_fut)]
    thresholds_fut <- setnames(thresholds_fut, c("value", "value_fut"), c("from", "to"))
    data_class_fut <- terra::classify(data_diff_class, thresholds_fut)  # Classify future suitable area
    data_class_fut_area <- data_class_fut * base_cellsize  # Calculate area
    names(data_class_fut_area) <- paste0(names(data_class_fut_area), "_future-area")
    
    # Areas that have become more suitable
    # Areas that are >50% in future & where change is >5%
    data_mask <- data_class_fut
    data_mask <- terra::classify(data_mask, data.frame(from = 0, to = NA))
    increase <- terra::mask(data_diff, data_mask)
    increase5_area <- terra::classify(increase, data.frame(from = c(-200, 5), to = c(5, 200), becomes = c(0, 1)))
    increase5_area <- increase5_area * base_cellsize
    names(increase5_area) <- paste0(names(increase5_area), "_increase5-area")
    
    # Areas that are >50% in future & where change is >10%
    increase10_area <- terra::classify(increase, data.frame(from = c(-200, 10), to = c(10, 200), becomes = c(0, 1)))
    increase10_area <- increase10_area * base_cellsize
    names(increase10_area) <- paste0(names(increase10_area), "_increase10-area")
    
    # Areas that are >50% in future & where change is >20%
    increase20_area <- terra::classify(increase, data.frame(from = c(-200, 20), to = c(20, 200), becomes = c(0, 1)))
    increase20_area <- increase20_area * base_cellsize
    names(increase20_area) <- paste0(names(increase20_area), "_increase20-area")
    
    # Areas that have become less suitable
    # Areas that are >50% in past & where change is < -5%
    data_mask <- data_class_past
    data_mask <- terra::classify(data_mask, data.frame(from = 0, to = NA))
    decrease <- terra::mask(data_diff, data_mask)
    decrease5_area <- terra::classify(decrease, data.frame(from = c(-200, -5), to = c(-5, 200), becomes = c(1, 0)))
    decrease5_area <- decrease5_area * base_cellsize
    names(decrease5_area) <- paste0(names(decrease5_area), "_decrease5-area")
    
    # Areas that are >50% in past & where change is < -10%
    decrease10_area <- terra::classify(decrease, data.frame(from = c(-200, -10), to = c(-10, 200), becomes = c(1, 0)))
    decrease10_area <- decrease10_area * base_cellsize
    names(decrease10_area) <- paste0(names(decrease10_area), "_decrease10-area")
    
    # Areas that are >50% in past & where change is < -20%
    decrease20_area <- terra::classify(decrease, data.frame(from = c(-200, -20), to = c(-20, 200), becomes = c(1, 0)))
    decrease20_area <- decrease20_area * base_cellsize
    names(decrease20_area) <- paste0(names(decrease20_area), "_decrease20-area")
    
    data_area <- terra::rast(list(
      data_class_past_area,
      data_class_fut_area,
      increase5_area,
      increase10_area,
      increase20_area,
      decrease5_area,
      decrease10_area,
      decrease20_area
    ))
    
    data_area_ex <- admin_extract_wrap(
      data = data_area, 
      save_dir = cropsuite_class_dir, 
      filename = paste0(var, "_area-changes"), 
      FUN = "sum", 
      varname = NA, 
      Geographies, 
      overwrite = TRUE,
      modify_colnames = FALSE
    )  # Extract and save area changes at the administrative level
    
    data_area_ex <- data_area_ex[, variable := gsub("sum.", "", variable)
    ][, scenario := unlist(tstrsplit(variable[1], "_", keep = 1)), by = variable
    ][, timeframe := unlist(tstrsplit(variable[1], "_", keep = 2)), by = variable
    ][, crop := unlist(tstrsplit(variable[1], "_", keep = 3)), by = variable
    ][, stat := gsub(".tif", "", unlist(tstrsplit(variable[1], "_", keep = 5))), by = variable
    ][, variable := unlist(tstrsplit(variable[1], "_", keep = 4)), by = variable
    ][, unit := "km2"]
    
    data_area_ex_hist <- unique(data_area_ex[stat == "past.area"][, value_hist := value][, c("value", "scenario", "timeframe", "stat") := NULL])
    data_area_ex <- merge(data_area_ex[stat != "past.area"], data_area_ex_hist, all.x = TRUE)[stat != "future.area", value_hist := NA]
    
    data_area_ex[stat == "future.area", diff := value - value_hist
                 ][stat == "future.area", perc_change := round(100 * diff / value_hist, 1)]
    
    # Add in spam crop
    codes <- spamcodes[, list(Fullname, cropsuite_name)]
    setnames(codes, c("Fullname", "cropsuite_name"), c("crop_spam", "crop"))
    data_area_ex <- merge(data_area_ex, codes, by = "crop", all.x = TRUE)
    
    data_area_ex <- data_area_ex[, list(admin0_name, admin1_name, admin2_name, scenario, timeframe, crop, crop_spam, variable, stat, unit, value, value_hist, diff, perc_change)
    ][order(admin0_name, admin1_name, admin2_name, scenario, timeframe, crop, stat)
    ][, timeframe := gsub(".", "_", timeframe, fixed = TRUE)]
    
    save_file <- file.path(cropsuite_class_dir, paste0(var, "_area-changes", "_adm_sum.parquet"))
    arrow::write_parquet(data_area_ex, save_file)  # Save the summarized area changes to a parquet file
    
    save_file <- file.path(cropsuite_class_dir, paste0(var, "_area-changes", "_adm_sum_simple.parquet"))
    data_area_ex_simple <- data_area_ex[is.na(admin2_name)][, diff := round(diff, 0)][, value := round(value, 0)][, value_hist := round(value_hist, 0)]
    arrow::write_parquet(data_area_ex_simple, save_file)  # Save a simplified version of the summarized area changes to a parquet file
  }
}

# 3.3) Check results #####
(files<-list.files(cropsuite_class_dir,"_adm_",full.names = T))
head(arrow::read_parquet(files[4]))
