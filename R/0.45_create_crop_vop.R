#### 0 - functions and libraries
library(terra)
library(geoarrow)
library(arrow)
library(countrycode) # required by imported prepare_fao fn
library(data.table)
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

#### 1 - Read and subset initial data

### 1.1 Base Raster
path_base_raster <- "base_raster.tif"
if (!file.exists(path_base_raster)) {
  path_base_raster <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/base_raster.tif"
}
base_raster <- terra::rast(path_base_raster)

### 1.2 Admin Boundaries
path_admin0_vect <- geo_files_local[1]
if (any(grepl("Parquet", terra::gdal(drivers = TRUE)$name))) {
  geobound_vect <- terra::vect(path_admin0_vect)
} else {
  geobound_arrow <- arrow::open_dataset(path_admin0_vect)
  geobound_vect <- geobound_arrow |>
    sf::st_as_sf() |>
    terra::vect()
}
geobound_rast <- terra::rasterize(geobound_vect, base_raster, "iso3")
atlas_iso3 <- geobound_vect$iso3

### 1.3 Processing constants
remove_countries <- c("Ethiopia PDR", "Sudan (former)", "Cabo Verde", "Comoros", "Mauritius", "R\xe9union", "Seychelles")
target_year <- c(2014:2016)

### 1.4 Load SPAM codes
path_spamCode <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SpamCodes.csv"
ms_codes <- data.table::fread(path_spamCode)[, Code := toupper(Code)]
crops <- tolower(ms_codes[compound == "no", Code])

### 1.5 Load file for translation of spam to FAO stat names/codes
spam2fao <- fread("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SPAM2010_FAO_crops.csv")[
  short_spam2010 %in% crops & name_fao != "Mustard seed" &
    !(short_spam2010 %in% c("rcof", "smil", "pmil", "acof"))
]

spam2fao[short_spam2010 == "rape", name_fao_val := "Rape or colza seed"]

spam2fao_formatted <- c(
  setNames(spam2fao$name_fao_val, spam2fao$short_spam2010),
  setNames(c("Coffee, green", "Millet"), c("coff", "mill"))
)

### 1.6 Load Value of Production data from FAO
path_vop_africa_fao <- file.path(fao_dir, "Value_of_Production_E_Africa.csv")
path_vop_world_fao <- file.path(fao_dir, "Value_of_Production_E_All_Area_Groups.csv")
prod_value_africa <- fread(path_vop_africa_fao)

prod_value_usd_africa_fao <- prepare_fao_data(
  file = path_vop_africa_fao,
  spam2fao_formatted,
  elements = "Gross Production Value (constant 2014-2016 thousand US$)",
  remove_countries = remove_countries,
  keep_years = target_year,
  atlas_iso3 = atlas_iso3
)
sugb_fill <- data.frame(iso3 = unique(prod_value_usd_africa_fao$iso3))
sugb_fill$atlas_name <- "sugb"
sugb_fill$Y2014 <- NA
sugb_fill$Y2015 <- NA
sugb_fill$Y2016 <- NA

prod_value_usd_africa_fao <- rbind(prod_value_usd_africa_fao, sugb_fill)

value_usd_world <- fread(path_vop_world_fao)
value_usd_world <- value_usd_world[Area == "World" & Element == "Gross Production Value (constant 2014-2016 thousand US$)" & Item %in% spam2fao_formatted, .(Item, Y2014, Y2015, Y2016)]
prod_value_usd_world_fao <- merge(value_usd_world, data.table(Item = spam2fao_formatted, atlas_name = names(spam2fao_formatted)), all.x = T)

### 1.7 Load Producer Prices
path_prices_fao <- file.path(fao_dir, "Prices_E_Africa_NOFLAG.csv")

prod_price_africa_fao <- prepare_fao_data(
  file = path_prices_fao,
  spam2fao_formatted,
  elements = "Producer Price (USD/tonne)",
  remove_countries = remove_countries,
  keep_years = target_year, # 2014:2016
  atlas_iso3 = atlas_iso3
)

prod_price_africa_fao[, mean := mean(c(Y2016, Y2015, Y2014), na.rm = T), by = list(iso3, atlas_name)]

prod_price_africa_fao[!is.na(mean), .N]

prod_price_africa_fao <- add_nearby(data = prod_price_africa_fao, group_field = "atlas_name", value_field = "mean", neighbors = african_neighbors, regions)

### 1.8 Load FAO production estimates
path_prod_africa_fao <- file.path(fao_dir, "Production_Crops_Livestock_E_Africa_NOFLAG.csv")
prod_prod_world_fao <- file.path(fao_dir, "Production_Crops_Livestock_E_All_Area_Groups.csv")

prod_ton_africa_fao <- prepare_fao_data(
  file = path_prod_africa_fao,
  spam2fao_formatted,
  elements = "Production",
  units = "t",
  remove_countries = remove_countries,
  keep_years = target_year,
  atlas_iso3 = atlas_iso3
)


prod_ton_africa_fao[, prod_mean := mean(c(Y2014, Y2015, Y2016), na.rm = T), by = list(iso3, atlas_name)]

prod_world <- fread(prod_file_world)
prod_world <- prod_world[Area == "World" & Element == "Production" & Item %in% spam2fao_formatted & Unit == "t", .(Item, Y2014, Y2015, Y2016)]
prod_ton_world_fao <- merge(prod_world, data.table(Item = spam2fao_formatted, atlas_name = names(spam2fao_formatted)), all.x = T)

### 1.9 MapSpam Production data
temp_dir <- file.path(tempdir(), "mapspam_intermediate")
dir.create(temp_dir)
crop_prod_rast_spam <- read_spam(
  variable = "P",
  technology = "TA",
  mapspam_dir = mapspam_dir,
  save_dir = temp_dir,
  base_rast = base_raster,
  filename = "SPAM_crop_prod",
  ms_codes = ms_codes,
  overwrite = T
)

short_name <- ms_codes$Code[match(names(crop_prod_rast_spam), ms_codes$Fullname)]
names(crop_prod_rast_spam) <- short_name

acof <- crop_prod_rast_spam$ACOF
rcof <- crop_prod_rast_spam$RCOF
crop_prod_rast_spam$COFF <- acof + rcof
crop_prod_rast_spam$ACOF <- NULL
crop_prod_rast_spam$RCOF <- NULL

pmil <- crop_prod_rast_spam$PMIL
smil <- crop_prod_rast_spam$SMIL
crop_prod_rast_spam$MILL <- pmil + smil
crop_prod_rast_spam$PMIL <- NULL
crop_prod_rast_spam$SMIL <- NULL

#### 2 Infer the missing data for producer price and production values

### 2.1 At a Regional/continent level using neighboring production prices
# Price
prod_price_africa_fao2 <- prod_price_africa_fao[, list(iso3, atlas_name, mean, mean_neighbors, mean_region, mean_continent, mean_final)]
colnames(prod_price_africa_fao2) <- gsub("mean", "price_mean", colnames(prod_price_africa_fao2))

# Production
prod_ton_africa_fao2 <- prod_ton_africa_fao[, list(iso3, atlas_name, prod_mean)]

prod_filled <- merge(prod_ton_africa_fao2, prod_price_africa_fao2, all.x = TRUE)
prod_filled[, VoP := round(prod_mean * price_mean / 1000, 0)
  ][, VoP_neighbors := round(prod_mean * price_mean_neighbors / 1000, 0)
    ][, VoP_region := round(prod_mean * price_mean_region / 1000, 0)
      ][, VoP_continent := round(prod_mean * price_mean_continent / 1000, 0)]

neighbors_prod_value_usd <- merge(prod_value_usd_africa_fao,
  prod_filled[, list(iso3, atlas_name, VoP, VoP_neighbors, VoP_region, VoP_continent)],
  by = c("iso3", "atlas_name"),
  all.x = T
)

### 2.2 At global level avg
# Production
prod_ton_world_fao2 <- prod_ton_world_fao[, list(atlas_name, Y2015)]
setnames(prod_ton_world_fao2, "Y2015", "production")

# Value
prod_value_usd_world_fao2 <- prod_value_usd_world_fao[, list(atlas_name, Y2015)]
setnames(prod_value_usd_world_fao2, "Y2015", "value")

# Combine and calculate price
price_world <- merge(prod_ton_world_fao2, prod_value_usd_world_fao2, all.x = TRUE)
price_world[, price_world := value / production]

# Addneighbors_prod_value_usd
neighbors_prod_value_usd <- merge(neighbors_prod_value_usd, prod_ton_africa_fao[, list(iso3, atlas_name, prod_mean)], all.x = T, by = c("iso3", "atlas_name")) # using the af prod here as only calculating for af

# Add global vop to value table
neighbors_prod_value_usd <- merge(neighbors_prod_value_usd, price_world[, list(atlas_name, price_world)], all.x = T, by = c("atlas_name"))

# Estimate value
neighbors_prod_value_usd <- neighbors_prod_value_usd[, VoP_world := prod_mean * price_world][, !c("prod_mean", "price_world")]

### 2.3 - and a mixture of the two methods
# Data.table was doing some weird things and re-writing columns. base R method instead
VoP_hybrid0 <- with(
  neighbors_prod_value_usd,
  Reduce(
    \(x, y) ifelse(!is.na(x), x, y),
    list(VoP, VoP_neighbors, VoP_region, VoP_continent, VoP_world)
  )
)

VoP_hybrid1 <- with(
  neighbors_prod_value_usd,
  Reduce(
    \(x, y) ifelse(!is.na(x), x, y),
    list(VoP, VoP_neighbors, VoP_region, VoP_continent)
  )
)

VoP_hybrid2 <- with(
  neighbors_prod_value_usd,
  Reduce(
    \(x, y) ifelse(!is.na(x), x, y),
    list(VoP, VoP_world)
  )
)

VoP_hybrid3 <- with(
  neighbors_prod_value_usd,
  Reduce(
    \(x, y) ifelse(!is.na(x), x, y),
    list(VoP, VoP_continent)
  )
)

neighbors_prod_value_usd <- cbind(
  neighbors_prod_value_usd,
  VoP_hybrid0,
  VoP_hybrid1,
  VoP_hybrid2,
  VoP_hybrid3
)

#### 3 - Quality check and see what method works best
max_y <- neighbors_prod_value_usd[, max(max(VoP, na.rm = T), max(VoP_world, na.rm = T), max(VoP_hybrid1, na.rm = T))]

par(mfcol = c(2, 3)) # 3 rows, 1 column

# VoP
model <- lm(Y2015 ~ VoP, neighbors_prod_value_usd)
plot(neighbors_prod_value_usd$Y2015, neighbors_prod_value_usd$VoP, main = "VoP", xlab = "FAO", ylab = "Estimated", pch = 19, col = "red", ylim = c(0, max_y))
abline(a = 0, b = 1, , col = "blue")

# VoP_neighbors
model <- lm(Y2015 ~ VoP_neighbors, neighbors_prod_value_usd)
plot(neighbors_prod_value_usd$Y2015, neighbors_prod_value_usd$VoP_neighbors, main = "VoP_neighbors", xlab = "FAO", ylab = "Estimated", pch = 19, col = "red", ylim = c(0, max_y))
abline(a = 0, b = 1, , col = "blue")

# VoP_region
model <- lm(Y2015 ~ VoP_region, neighbors_prod_value_usd)
plot(neighbors_prod_value_usd$Y2015, neighbors_prod_value_usd$VoP_region, main = "VoP_region", xlab = "FAO", ylab = "Estimated", pch = 19, col = "red", ylim = c(0, max_y))
abline(a = 0, b = 1, , col = "blue")

# VoP_continent
model <- lm(Y2015 ~ VoP_continent, neighbors_prod_value_usd)
plot(neighbors_prod_value_usd$Y2015, neighbors_prod_value_usd$VoP_continent, main = "VoP_continent", xlab = "FAO", ylab = "Estimated", pch = 19, col = "red", ylim = c(0, max_y))
abline(a = 0, b = 1, , col = "blue")

# VoP_world
model <- lm(Y2015 ~ VoP_world, neighbors_prod_value_usd)
plot(neighbors_prod_value_usd$Y2015, neighbors_prod_value_usd$VoP_world, main = "VoP_world", xlab = "FAO", ylab = "Estimated", pch = 19, col = "red", ylim = c(0, max_y))
abline(a = 0, b = 1, , col = "blue")

# VoP_hybrid
model <- lm(Y2015 ~ VoP_hybrid1, neighbors_prod_value_usd)
plot(neighbors_prod_value_usd$Y2015, neighbors_prod_value_usd$VoP_hybrid1, main = "VoP_hybrid1", xlab = "FAO", ylab = "Estimated", pch = 19, col = "red", ylim = c(0, max_y))
abline(a = 0, b = 1, , col = "blue")

# VoP_hybrid
model <- lm(Y2015 ~ VoP_hybrid2, neighbors_prod_value_usd)
plot(neighbors_prod_value_usd$Y2015, neighbors_prod_value_usd$VoP_hybrid2, main = "VoP_hybrid2", xlab = "FAO", ylab = "Estimated", pch = 19, col = "red", ylim = c(0, max_y))
abline(a = 0, b = 1, , col = "blue")

results <- rbind(
  neighbors_prod_value_usd[!is.na(Y2015) & !is.na(VoP), .(
    source = "VoP",
    n = length(na.omit(Y2015 / VoP)),
    mean_prop = mean(Y2015 / VoP, na.rm = TRUE),
    mean_sd = sd(Y2015 / VoP, na.rm = TRUE),
    lm.adj.r.squared = summary(lm(Y2015 ~ VoP, neighbors_prod_value_usd))$adj.r.squared,
    rmse = sqrt(mean((Y2015 - predict(lm(Y2015 ~ VoP, neighbors_prod_value_usd)))^2, na.rm = TRUE)), # RMSE calculation
    mae = mean(abs(Y2015 - predict(lm(Y2015 ~ VoP)))) # Adding RMSE calculation
  )][, cv := mean_sd / mean_prop],
  neighbors_prod_value_usd[!is.na(Y2015) & !is.na(VoP_neighbors), list(
    source = "VoP_neighbors",
    n = length(na.omit(Y2015 / VoP_neighbors)),
    mean_prop = mean(Y2015 / VoP_neighbors, na.rm = T),
    mean_sd = sd(Y2015 / VoP_neighbors, na.rm = T),
    lm.adj.r.squared = summary(lm(Y2015 ~ VoP_neighbors, neighbors_prod_value_usd))$adj.r.squared,
    rmse = sqrt(mean((Y2015 - predict(lm(Y2015 ~ VoP_neighbors, neighbors_prod_value_usd)))^2, na.rm = TRUE)), # RMSE calculation
    mae = mean(abs(Y2015 - predict(lm(Y2015 ~ VoP_neighbors))))
  )][, cv := mean_sd / mean_prop],
  neighbors_prod_value_usd[!is.na(Y2015) & !is.na(VoP_region), list(
    source = "VoP_region",
    n = length(na.omit(Y2015 / VoP_region)),
    mean_prop = mean(Y2015 / VoP_region, na.rm = T),
    mean_sd = sd(Y2015 / VoP_region, na.rm = T),
    lm.adj.r.squared = summary(lm(Y2015 ~ VoP_region, neighbors_prod_value_usd))$adj.r.squared,
    rmse = sqrt(mean((Y2015 - predict(lm(Y2015 ~ VoP_region, neighbors_prod_value_usd)))^2, na.rm = TRUE)), # RMSE calculation
    mae = mean(abs(Y2015 - predict(lm(Y2015 ~ VoP_region))))
  )][, cv := mean_sd / mean_prop],
  neighbors_prod_value_usd[!is.na(Y2015) & !is.na(VoP_continent), list(
    source = "VoP_continent",
    n = length(na.omit(Y2015 / VoP_continent)),
    mean_prop = mean(Y2015 / VoP_continent, na.rm = T),
    mean_sd = sd(Y2015 / VoP_continent, na.rm = T),
    lm.adj.r.squared = summary(lm(Y2015 ~ VoP_continent, neighbors_prod_value_usd))$adj.r.squared,
    rmse = sqrt(mean((Y2015 - predict(lm(Y2015 ~ VoP_continent, neighbors_prod_value_usd)))^2, na.rm = TRUE)), # RMSE calculation
    mae = mean(abs(Y2015 - predict(lm(Y2015 ~ VoP_continent))))
  )][, cv := mean_sd / mean_prop],
  neighbors_prod_value_usd[!is.na(Y2015) & !is.na(VoP_world), list(
    source = "VoP_world",
    n = length(na.omit(Y2015 / VoP_world)),
    mean_prop = mean(Y2015 / VoP_world, na.rm = T),
    mean_sd = sd(Y2015 / VoP_world, na.rm = T),
    lm.adj.r.squared = summary(lm(Y2015 ~ VoP_world, neighbors_prod_value_usd))$adj.r.squared,
    rmse = sqrt(mean((Y2015 - predict(lm(Y2015 ~ VoP_world, neighbors_prod_value_usd)))^2, na.rm = TRUE)), # RMSE calculation
    mae = mean(abs(Y2015 - predict(lm(Y2015 ~ VoP_world))))
  )][, cv := mean_sd / mean_prop],
  neighbors_prod_value_usd[!is.na(Y2015) & !is.na(VoP_hybrid0), list(
    source = "VoP_hybrid0",
    n = length(na.omit(Y2015 / VoP_hybrid0)),
    mean_prop = mean(Y2015 / VoP_hybrid0, na.rm = T),
    mean_sd = sd(Y2015 / VoP_hybrid0, na.rm = T),
    lm.adj.r.squared = summary(lm(Y2015 ~ VoP_hybrid0, neighbors_prod_value_usd))$adj.r.squared,
    rmse = sqrt(mean((Y2015 - predict(lm(Y2015 ~ VoP_hybrid0, neighbors_prod_value_usd)))^2, na.rm = TRUE)), # RMSE calculation
    mae = mean(abs(Y2015 - predict(lm(Y2015 ~ VoP_hybrid0))))
  )][, cv := mean_sd / mean_prop],
  neighbors_prod_value_usd[!is.na(Y2015) & !is.na(VoP_hybrid1), list(
    source = "VoP_hybrid1",
    n = length(na.omit(Y2015 / VoP_hybrid1)),
    mean_prop = mean(Y2015 / VoP_hybrid1, na.rm = T),
    mean_sd = sd(Y2015 / VoP_hybrid1, na.rm = T),
    lm.adj.r.squared = summary(lm(Y2015 ~ VoP_hybrid1, neighbors_prod_value_usd))$adj.r.squared,
    rmse = sqrt(mean((Y2015 - predict(lm(Y2015 ~ VoP_hybrid1, neighbors_prod_value_usd)))^2, na.rm = TRUE)), # RMSE calculation
    mae = mean(abs(Y2015 - predict(lm(Y2015 ~ VoP_hybrid1))))
  )][, cv := mean_sd / mean_prop],
  neighbors_prod_value_usd[!is.na(Y2015) & !is.na(VoP_hybrid2), list(
    source = "VoP_hybrid2",
    n = length(na.omit(Y2015 / VoP_hybrid2)),
    mean_prop = mean(Y2015 / VoP_hybrid2, na.rm = T),
    mean_sd = sd(Y2015 / VoP_hybrid2, na.rm = T),
    lm.adj.r.squared = summary(lm(Y2015 ~ VoP_hybrid2, neighbors_prod_value_usd))$adj.r.squared,
    rmse = sqrt(mean((Y2015 - predict(lm(Y2015 ~ VoP_hybrid2, neighbors_prod_value_usd)))^2, na.rm = TRUE)), # RMSE calculation
    mae = mean(abs(Y2015 - predict(lm(Y2015 ~ VoP_hybrid2))))
  )][, cv := mean_sd / mean_prop],
  neighbors_prod_value_usd[!is.na(Y2015) & !is.na(VoP_hybrid3), list(
    source = "VoP_hybrid3",
    n = length(na.omit(Y2015 / VoP_hybrid3)),
    mean_prop = mean(Y2015 / VoP_hybrid3, na.rm = T),
    mean_sd = sd(Y2015 / VoP_hybrid3, na.rm = T),
    lm.adj.r.squared = summary(lm(Y2015 ~ VoP_hybrid3, neighbors_prod_value_usd))$adj.r.squared,
    rmse = sqrt(mean((Y2015 - predict(lm(Y2015 ~ VoP_hybrid3, neighbors_prod_value_usd)))^2, na.rm = TRUE)), # RMSE calculation
    mae = mean(abs(Y2015 - predict(lm(Y2015 ~ VoP_hybrid3))))
  )][, cv := mean_sd / mean_prop]
)

View(results)

#### 4 - Distribution across the rasters

### 4.1 Get VOP per admin 0 area and rasterize it
focal_year <- "Y2015"
final_vop <- copy(neighbors_prod_value_usd)
setnames(final_vop, focal_year, "value")

final_vop <- final_vop[, list(iso3, atlas_name, value, VoP_hybrid2)]
setnames(final_vop, "VoP_hybrid2", "VoP")
final_vop[is.na(value), value := VoP][, VoP := NULL]

final_vop2 <- final_vop[, .(
  value = if (all(is.na(value))) as.numeric(NA) else sum(value, na.rm = TRUE)
), by = .(atlas_name, iso3)]

final_vop_cast <- dcast(final_vop, iso3 ~ atlas_name)

final_vop_vect <- merge(geobound_vect, final_vop_cast, all.x = TRUE)

final_vop_rast <- terra::rast(lapply(unique(final_vop$atlas_name), FUN = function(NAME) {
  terra::rasterize(final_vop_vect, base_rast, field = NAME)
}))

### 4.2 Create the proportion of production raster

spam_prod_admin0 <- terra::zonal(crop_prod_rast_spam, geobound_rast, fun = "sum", na.rm = TRUE)
spam_prod_admin0_ex <- data.table::as.data.table(spam_prod_admin0)
iso3_levels <- levels(geobound_rast)[[1]]
spam_prod_admin0_ex <- merge(spam_prod_admin0_ex, iso3_levels, by = "iso3", all.x = TRUE)
spam_tot <- terra::rast(lapply(names(crop_prod_rast_spam), function(colname) {
  # Extract the column for the current variable and its corresponding ISO3 codes
  temp_data <- spam_prod_admin0_ex[, c("ID", colname), with = FALSE]
  # Create a reclassification matrix directly
  rcl <- as.matrix(temp_data) # Columns: iso3, value
  # Reclassify admin_rast based on the rcl matrix
  raster_layer <- terra::classify(geobound_rast, rcl = rcl, include.lowest = TRUE)
  return(raster_layer)
}))

names(spam_tot) <- names(crop_prod_rast_spam)

spam_prop <- crop_prod_rast_spam / spam_tot
names(spam_prop) <- names(crop_prod_rast_spam)

# Replace infinite values with NA
spam_prop[is.infinite(spam_prop)] <- NA

### 4.3 Now calcualte the per cell vop using proportion and FAO values

sorted_final_vop_rast <- final_vop_rast[[sort(names(final_vop_rast))]]
subset_sort_spam_prop <- spam_prop[[toupper(names(sorted_final_vop_rast))]]
spam_vop_usd2015_FAOspam <- sorted_final_vop_rast * subset_sort_spam_prop

coffee_usd2015 <- spam_vop_usd2015_FAOspam$coff
arcof <- acof + rcof
acof_usd2015 <- coffee_usd2015 * acof / arcof
spam_vop_usd2015_FAOspam$acof <- acof_usd2015
rcof_usd2015 <- coffee_usd2015 * rcof / arcof
spam_vop_usd2015_FAOspam$rcof <- rcof_usd2015

millet_usd2015 <- spam_vop_usd2015_FAOspam$mill
psmil <- pmil + smil
pmil_usd2015 <- millet_usd2015 * pmil / psmil
spam_vop_usd2015_FAOspam$pmil <- pmil_usd2015
smil_usd2015 <- millet_usd2015 * smil / psmil
spam_vop_usd2015_FAOspam$smil <- smil_usd2015

writeRaster(spam_vop_usd2015_FAOspam, file.path(mapspam_dir, "spam2020V1r2_SSA_Vusd15_TA.tif"))

#### 5 - Quality Control and data checks
plot(spam_vop_usd2015_FAOspam)

# Compare against the other datasets
country_totals <- round(exact_extract(spam_vop_usd2015_FAOspam, sf::st_as_sf(geobound_vect), fun = "sum"), 3)

country_totals <- cbind(geobound_vect, country_totals)

long_totals <- melt(
  data.table(as.data.frame(country_totals)),
  id.vars = c("admin_name", "admin0_name", "iso3"),
  variable.name = "atlas_name",
  value.name = "value"
)

long_totals$atlas_name <- gsub("sum.", "", long_totals$atlas_name)

FAO_totals <- copy(prod_value_usd_africa_fao)
FAO_totals$FAOmean <- round(rowMeans(FAO_totals[, c("Y2014", "Y2015", "Y2016")], na.rm = TRUE), 2)
QAQC <- merge(long_totals, FAO_totals[, c("iso3", "atlas_name", "FAOmean")], by = c("iso3", "atlas_name"))

QAQC$distance <- round(QAQC$value - QAQC$FAOmean, 2)

spamIntd <- arrow::read_parquet(file.path(exposure_dir, "crop_vop15_intd15_adm_sum.parquet"))
a0_intd15 <- subset(spamIntd, is.na(admin1_name))

a0_intd15$atlas_name <- ms_codes[match(a0_intd15$crop, tolower(ms_codes$Fullname)), "Code"]
a0_intd15$atlas_name <- tolower(a0_intd15$atlas_name)
a0_intd15$intd15 <- round(a0_intd15$value, 2)
a0_intd15$admin_name <- a0_intd15$admin0_name
QAQC <- merge(QAQC, a0_intd15[, c("atlas_name", "admin_name", "intd15")], by = c("atlas_name", "admin_name"))

model_fao <- lm(value ~ FAOmean, QAQC)
model_intd <- lm(value ~ intd15, QAQC)
plot(QAQC$value, QAQC$FAOmean, main = "VoP")
abline(a = 0, b = 1, , col = "blue")

plot(QAQC$value, QAQC$intd15, main = "VoP")
abline(a = 0, b = 1, , col = "blue")

summary(model_fao)
summary(model_intd)

View(QAQC)
