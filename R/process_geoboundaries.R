require(terra)
require(stringr)

admin2=terra::vect("Data/geoboundaries/admin2.shp")
admin1=terra::vect("Data/geoboundaries/admin1.shp")
admin0=terra::vect("Data/geoboundaries/admin0.shp")

# Create standard name field for each admin vector
admin2$admin_name<-str_to_title(admin2$shapeName)
admin1$admin_name<-str_to_title(admin1$shapeName)
admin0$admin_name<-str_to_title(admin0$shapeName)

# Create standard name field for each admin vector
base_rast<-terra::rast(list.files("Data/hazard_classified/annual",".tif",full.names = T))[[1]]

# Admin0
admin0$admin0_name<-countrycode::countrycode(admin0$shapeGroup, origin = 'iso3c', destination = 'country.name')
admin0$admin_name<-admin0$admin0_name
admin0$iso3<-admin0$shapeGroup

# Admin1
admin1$admin0_name<-countrycode::countrycode(admin1$shapeGroup, origin = 'iso3c', destination = 'country.name')
admin1$iso3<-admin1$shapeGroup

admin1$admin1_name<-admin1$shapeName
admin1$ID<-1:length(admin1)

# Admin2
admin2$admin0_name<-countrycode::countrycode(admin2$shapeGroup, origin = 'iso3c', destination = 'country.name')
admin2$iso3<-admin2$shapeGroup

# Several Admin0 fields are missing
admin0$ID<-1:length(admin0)
admin0_rast<-terra::rasterize(admin0,base_rast,"ID")
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
admin1_rast<-terra::rasterize(admin1,base_rast,"ID")

X<-exactextractr::exact_extract(admin1_rast,sf::st_as_sf(admin2),progress=F)
X<-sapply(X,Mode)
admin2$admin1_name<-admin1$admin1_name[match(X,admin1$ID)]
admin2$admin2_name<-str_to_title(admin2$shapeName)

# There are few instances where the match doesn't work, we will need to address these3
data.frame(admin2[is.na(admin2$admin1_name)])
admin2$admin1_name[is.na(admin2$admin1_name)]<-c("Banjul","Banjul","Collines","Elobey Chico","Elobey Grande")

data.frame(admin2[is.na(admin2$admin0_name)])
admin2$iso3[is.na(admin2$admin0_name)]<-admin2$shapeGroup[is.na(admin2$admin0_name)]
admin2$admin0_name[is.na(admin2$admin0_name)]<-countrycode::countrycode(admin2$shapeGroup[is.na(admin2$admin0_name)], origin = 'iso3c', destination = 'country.name')

data.frame(admin1[is.na(admin1$admin0_name)])
#admin1$shapeGroup[is.na(admin1$admin0_name)]<-countrycode::countrycode(admin1$shapeGroup[is.na(admin1$admin0_name)], origin = 'iso3c', destination = 'country.name')

admin0[,c("ID","shapeName","shapeISO","shapeID","shapeGroup","shapeType","agg_n")]<-NULL
admin1[,c("ID","shapeName","shapeISO","shapeID","shapeGroup","shapeType","agg_n")]<-NULL
admin2[,c("ID","shapeName","shapeISO","shapeID","shapeGroup","shapeType","agg_n")]<-NULL

# Merge polygons
admin0<-terra::aggregate(admin0,by="admin_name")
admin1<-terra::aggregate(admin1,by="admin_name")
admin2<-terra::aggregate(admin2,by="admin_name")

# Save processed files
terra::writeVector(admin0,file="Data/geoboundaries/admin0_processed.shp",overwrite=T)
terra::writeVector(admin1,file="Data/geoboundaries/admin1_processed.shp",overwrite=T)
terra::writeVector(admin2,file="Data/geoboundaries/admin2_processed.shp",overwrite=T)

