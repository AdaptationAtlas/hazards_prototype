# Please run 0_server_setup.R before executing this script
# Raw sos data is currently only available on cglabs server here "/home/jovyan/common_data/atlas_sos/seasonal_mean"

packages<-c("terra","data.table","pbapply")
pacman::p_load(char=packages)

# Process start of season data

# Combine sos (start of season) crop calendars
# Get a list of all .RData files in the directory containing raw sos data
files <- list.files(sos_raw_dir, ".RData")

# Load sos data estimated from historical data
# Here, SOS is calculated for each season and then averaged
sos_data <- rbindlist(lapply(files, FUN = function(file) {
  # Load each .RData file and extract the data
  X <- miceadds::load.Rdata2(file = file, path = sos_raw_dir)
  # Extract the admin0 (administrative region) from the file name by removing the .RData extension
  X$admin0 <- gsub(".RData", "", file)
  X
}), use.names = TRUE)[, list(admin0, x, y, S1, S2, E1, E2, LGP1, LGP2, Tot.Rain1, Tot.Rain2)]

# Fix seasonal allocation for specific countries (e.g., Angola)
# Identify rows where S1 is greater than 30, one of S1 or S2 is NA, and admin0 is Angola
N <- sos_data[, which(S1 > 30 & ((is.na(S1) + is.na(S2)) == 1) & admin0 == "Angola")]

# Adjust the seasonal data for these rows
sos_data[N, E2 := E1]
sos_data[N, E1 := NA]
sos_data[N, LGP2 := LGP1]
sos_data[N, LGP1 := NA]
sos_data[N, Tot.Rain2 := Tot.Rain1]
sos_data[N, Tot.Rain1 := NA]
sos_data[N, S2 := S1]
sos_data[N, S1 := NA]

# Remove the admin0 column as it is no longer needed
sos_data[, admin0 := NULL]

# Update dekads (10-day periods) to months
# Convert S1, S2, E1, and E2 from dekads to months
sos_data[, S1 := as.numeric(format(as.Date((S1 * 10), origin = as.Date("2024-01-01")), "%m"))]
sos_data[, S2 := as.numeric(format(as.Date((S2 * 10), origin = as.Date("2024-01-01")), "%m"))]
sos_data[, E1 := as.numeric(format(as.Date((E1 * 10), origin = as.Date("2024-01-01")), "%m"))]
sos_data[, E2 := as.numeric(format(as.Date((E2 * 10), origin = as.Date("2024-01-01")), "%m"))]

# Convert LGP (length of growing period) from dekads to months by dividing by 3 and rounding up
sos_data[, LGP1 := ceiling(LGP1 / 3)]
sos_data[, LGP2 := ceiling(LGP2 / 3)]

# If start and end month are the same, add a month to the end month
sos_data[S1 == E1, E1 := E1 + 1]
sos_data[S2 == E2, E2 := E2 + 1]

# Adjust end month if it exceeds 12 (wrap around to January)
sos_data[E1 == 13, E1 := 1]
sos_data[E2 == 13, E2 := 1]

# Identify rows where S1 is NA
N <- sos_data[, which(is.na(S1))]

# Swap S1 and S2, E1 and E2, LGP1 and LGP2 if S1, E1, or LGP1 is NA
sos_data[is.na(S1), c("S1", "S2") := list(S2, S1)]
sos_data[is.na(E1), c("E1", "E2") := list(E2, E1)]
sos_data[is.na(LGP1), c("LGP1", "LGP2") := list(LGP2, LGP1)]

# Convert the data frame to a raster
# Create a raster from the sos_data using the x, y coordinates and the specified variables
sos_rast <- terra::rast(as.data.frame(sos_data)[, c("x", "y", "S1", "S2", "E1", "E2", "LGP1", "LGP2")], 
                        type = "xyz", crs = "+proj=longlat +datum=WGS84 +no_defs", digits = 3, extent = NULL)

# Resample the raster to match the base raster's resolution and extent
sos_rast <- terra::resample(sos_rast, base_rast)

# Save the raster to a file
sos_file <- file.path(sos_dir, "sos.tif")
terra::writeRaster(sos_rast, sos_file)
