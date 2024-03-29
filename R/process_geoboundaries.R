require(terra)
require(stringr)

# South Asia
folder<-"Data/geoboundaries_SA"
# Sub-saharan Africa
folder<-"Data/geoboundaries_SSA"

admin2=terra::vect(paste0(folder,"/admin2.shp"))
admin1=terra::vect(paste0(folder,"/admin1.shp"))
admin0=terra::vect(paste0(folder,"/admin0.shp"))

# Create standard name field for each admin vector
admin2$admin_name<-str_to_title(admin2$shapeName)
admin1$admin_name<-str_to_title(admin1$shapeName)
admin0$admin_name<-str_to_title(admin0$shapeName)

# Create standard name field for each admin vector
base_rast<-terra::rast(extent = terra::ext(admin0), resolution = 0.007, crs = "epsg:4326")
# Admin0
admin0$admin0_name<-countrycode::countrycode(admin0$shapeGroup, origin = 'iso3c', destination = 'country.name')
admin0$admin_name<-admin0$admin0_name
admin0$iso3<-admin0$shapeGroup

# Admin1
admin1$admin0_name<-countrycode::countrycode(admin1$shapeGroup, origin = 'iso3c', destination = 'country.name')
admin1$iso3<-admin1$shapeGroup
admin1$admin1_name <- admin1$shapeName

# Admin2
admin2$admin0_name<-countrycode::countrycode(admin2$shapeGroup, origin = 'iso3c', destination = 'country.name')
admin2$iso3<-admin2$shapeGroup

# Several Admin0 fields are missing
admin0$ID<-1:length(admin0)
admin0_rast<-terra::rasterize(admin0, base_rast, field = "ID")
Mode <- function(x) {
  x<-x$value
  x<-x[!is.na(x)]
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

# Add Admin0 to Admin2
X<-exactextractr::exact_extract(admin0_rast,sf::st_as_sf(admin2),progress=F)
X<-sapply(X,Mode)
admin2$admin0_name<-admin0$admin0_name[match(X,admin0$ID)]
admin2$iso3<-admin0$iso3[match(X,admin0$ID)]

# Add Admin 1 to Admin2
admin1$ID <- 1:length(admin1)
admin1_rast<-terra::rasterize(admin1,base_rast,"ID")
X<-exactextractr::exact_extract(admin1_rast,sf::st_as_sf(admin2),progress=F)
X<-sapply(X,Mode)
admin2$admin1_name<-admin1$admin1_name[match(X,admin1$ID)]
admin2$admin2_name<-str_to_title(admin2$shapeName)

# There are few instances where the match doesn't work, we will need to address these
if(folder=="Data/geoboundaries_SSA"){
  data.frame(admin2[is.na(admin2$admin1_name)])
  missing_a1 <- c('Collines', 'Collines', 'Elobey Chico')
  admin2$admin1_name[is.na(admin2$admin1_name)] <- missing_a1
  # admin2$admin1_name[is.na(admin2$admin1_name)]<-c("Banjul","Banjul","Collines","Elobey Chico","Elobey Grande")
}

# Add missing iso3 codes to admin2
data.frame(admin2[is.na(admin2$admin0_name)])
admin2$iso3[is.na(admin2$admin0_name)]<-admin2$shapeGroup[is.na(admin2$admin0_name)]
# Update names for missing iso3 codes
admin2$admin0_name[is.na(admin2$admin0_name)]<-countrycode::countrycode(admin2$shapeGroup[is.na(admin2$admin0_name)], origin = 'iso3c', destination = 'country.name')



if(folder=="Data/geoboundaries_SSA"){
  # Some admin 1 names appear to be missing
  admin1[is.na(admin1$admin_name)]
  # NA polygon appears to be a duplicate, remove one of the duplicates
  N<-which(is.na(admin1$admin_name))
  admin1<-admin1[-N[2]]
  
  admin1$admin_name[is.na(admin1$admin_name)]<-"Collines"
  admin1$admin1_name[is.na(admin1$admin1_name)]<-"Collines"
  
  nrow(admin1[is.na(admin1$admin_name)]) == 0
}

admin0[, c("ID", "shapeName", "shapeISO", "shapeID", "shapeGroup", "shapeType")]<-NULL
admin1[, c("ID", "shapeName", "shapeISO", "shapeID", "shapeGroup", "shapeType")] <- NULL
admin2[, c("ID", "shapeName", "shapeISO", "shapeID", "shapeGroup", "shapeType")] <- NULL

### Merge polygons
# Pre-calc the correct unique aggregated rows

a0_check <- nrow(unique(as.data.frame(admin0)))
a1_check <- nrow(unique(as.data.frame(admin1)))
a2_check <- nrow(unique(as.data.frame(admin2)))

# Aggregate
admin0 <- terra::aggregate(admin0,
  by = "admin_name", count = FALSE, na.rm = FALSE)
if (a0_check != nrow(admin0)) stop("Admin 0 merge error")

# admin1$a1_a0 <- paste0(admin1$admin_name,"_", admin1$admin0_name)
admin1 <- terra::aggregate(admin1, by = c('admin1_name', 'admin0_name'), count = FALSE)
if (a1_check != nrow(admin1)) stop("Admin 1 merge error")

# admin2$a2_a1_a0 <- paste0(admin2$admin_name,"_", admin2$admin1_name, "_", admin2$admin0_name)
admin2 <- terra::aggregate(admin2, by = c('admin2_name', 'admin1_name', 'admin0_name'), count = FALSE)
if (a2_check != nrow(admin2)) stop("Admin 2 merge error")

# final sanity check
if (any(!sort(unique(admin0$admin_name)) == sort(unique(admin1$admin0_name)))) {
  stop("Not all Admin 0 match with Admin 1")
}
if (any(!sort(unique(admin0$admin_name)) == sort(unique(admin2$admin0_name)))) {
  stop("Not all Admin 0 match with Admin 1")
}

# Save processed files
terra::writeVector(admin0,file=paste0(folder,"/admin0_processed.gpkg"),overwrite=T)
terra::writeVector(admin1,file=paste0(folder,"/admin1_processed.gpkg"),overwrite=T)
terra::writeVector(admin2,file=paste0(folder,"/admin2_processed.gpkg"),overwrite=T)
