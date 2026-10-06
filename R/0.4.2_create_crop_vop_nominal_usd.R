# ==============================================================================
# Script Title: Estimate and Allocate Crop Value of Production Across Africa
# Author(s): Peter Steward, African Agricultural Adaptation Atlas
# Affiliation: Alliance of Bioversity International and CIAT
# Date Created: 2024-05-30
# Last Updated: 2026-10-01
# ==============================================================================

# Description:
# This script estimates the national Value of Production (VoP) for SPAM crops in
# Sub-Saharan Africa using FAOSTAT production, price, and value data. It then
# allocates these national VoP values spatially by combining them with SPAM
# gridded production rasters, disaggregated by technology (subsistence, low,
# high, irrigated). Price gaps are filled using a fallback hierarchy that draws
# from national time series, neighboring countries, regional and global medians.

# Key Steps:
# 1. Load base raster and admin boundaries used across the Atlas.
# 2. Load and clean SPAM crop codes and FAOSTAT name mappings.
# 3. Read FAO economic data (production value, prices, production volume)
#    for Africa and World totals, in both constant USD and I$.
# 4. Merge economic data by crop and country; clean anomalies manually (e.g., tea).
# 5. Estimate nominal USD price per tonne for key time windows (e.g., 2015, 2021)
#    using national FAOSTAT values, fallback substitution, and global benchmarks.
# 6. Apply final price surfaces to SPAM production rasters to create crop-level
#    VoP rasters by farming system (4 SPAM technologies × ~30 crops).
# 7. Output national and gridded VoP rasters in nominal USD per pixel.

# Input Datasets:
# - FAOSTAT CSVs: Production, Value of Production, Prices (Africa + World)
# - SPAM 2010 crop production rasters (by crop and technology)
# - Crop name mappings: SPAM codes to FAOSTAT names
# - Atlas base raster
# - Sub-Saharan admin boundaries (GeoArrow/Parquet)

# Output Files:
# - `crop_price_nominal-usd-2015-t.tif` — Estimated 2015 prices per crop (USD/tonne)
# - `variable=vop_nominal-usd-2015/spam_vop_nominal-usd-2015_<tech>.tif` — Gridded VoP rasters
# - `crop_price_nominal-usd-2021-t.tif` — Estimated 2021 prices per crop
# - `spam_vop_nominal-usd-2021_<tech>.tif` — VoP per pixel (2021 prices × SPAM production)
# - Intermediate tables for FAO–SPAM merging, price inference, and QA

# Notes:
# - VoP is calculated in nominal USD using SPAM production × inferred price.
# - The price is FAOSTAT's implied price (GPV current US$ / production) where available,
#   its producer price as first fallback, then neighbour / region / continent / world
#   MEDIANS; a within-item basis guard replaces auction / product-form / exchange-rate
#   prices with the item-median factor × the country's constant-I$ value (section 3,
#   R/price_fill.R; decision 2026-10-01).
# - Coffee and millet prices are duplicated onto arabica/robusta and pearl/small millet.
# - Output rasters are aligned to the Atlas base raster for downstream harmonization.

# ==============================================================================

## 0 - functions and libraries
pacman::p_load(terra, geoarrow, arrow, countrycode, data.table)
source(file.path(Sys.getenv("project_dir", getwd()), "R", "haz_functions.R"))   # local copy: a develop fix must reach the node run (was GitHub main)

## 1 - Read and subset initial data ####

### 1.1 Base Raster ####

# Grid = exposure_grid() (0_server_setup.R): EXPOSURE_RES = 0.05 | 0.25, required.
# Until 2026-09-23 this script hardcoded the 0.05 deg atlas_delta base raster
# while 0.4.0/0.4.1 sat on base_rast_path (0.25 deg under nexgddp), so the
# nominal-USD rasters were on a different grid from everything R/3 stacks them
# with. Now every output carries .eg$tag (res-05 / res-25). Issue #30.
.eg <- exposure_grid(caller = "0.4.2")
cat(sprintf("exposure grid = %s | res %.4f | tag %s\n", basename(.eg$path), .eg$res_deg, .eg$tag))
base_rast <- .eg$rast

### 1.2 Admin Boundaries #####
file <- geo_files_local[1]
geoboundaries <- read_parquet(file)
geoboundaries <- geoboundaries |>
  sf::st_as_sf() |>
  terra::vect()
geoboundaries <- aggregate(geoboundaries, "iso3")

### 1.3 Processing constants #####
remove_countries <- c("Ethiopia PDR", "Sudan (former)", "Cabo Verde", "Comoros", "Mauritius", "R\xe9union", "Seychelles")
atlas_iso3 <- geoboundaries$iso3
target_year <- c(2009:2023)

### 1.4 Load SPAM codes #####
path_spamCode <- file.path(Sys.getenv("project_dir", getwd()), "metadata", "SpamCodes.csv")
ms_codes <- data.table::fread(path_spamCode)[, Code := toupper(Code)][!is.na(Code)][, code_low := tolower(Code)]
crops <- tolower(ms_codes[compound == "no", Code])

### 1.5 Load file for translation of spam to FAO stat names/codes #####
spam2fao <- fread(file.path(Sys.getenv("project_dir", getwd()), "metadata", "SPAM2010_FAO_crops.csv"))[
  short_spam2010 %in% crops & name_fao != "Mustard seed" &
    !(short_spam2010 %in% c("rcof", "smil", "pmil", "acof"))
]

spam2fao[short_spam2010 == "rape", name_fao_val := "Rape or colza seed"]

spam2fao_formatted <- c(
  setNames(spam2fao$name_fao_val, spam2fao$short_spam2010),
  setNames(c("Coffee, green", "Millet"), c("coff", "mill"))
)

### 1.6 Load Value of Production data from FAO #####
#### 1.6.1) cusd15 ####
#### 1.6.1.1) Africa ####
element <- "Gross Production Value (constant 2014-2016 thousand US$)"
value_name <- "value_cusd15"
path_vop_africa_fao <- file.path(fao_dir, "Value_of_Production_E_Africa.csv")

prod_value_africa <- fread(path_vop_africa_fao)

prod_value_usd_africa_fao <- prepare_fao_data(
  file = path_vop_africa_fao,
  spam2fao_formatted,
  elements = element,
  remove_countries = remove_countries,
  keep_years = target_year,
  atlas_iso3 = atlas_iso3
)

prod_value_usd_africa_fao <- melt(prod_value_usd_africa_fao, id.vars = c("iso3", "atlas_name"), value.name = value_name, variable.name = "year")

#### 1.6.1.2) World ####
path_vop_world_fao <- file.path(fao_dir, "Value_of_Production_E_All_Area_Groups.csv")
value_usd_world <- fread(path_vop_world_fao)
value_usd_world <- value_usd_world[Area == "World" & Element == element & Item %in% spam2fao_formatted, c("Item", paste0("Y", target_year)), with = FALSE]
prod_value_usd_world_fao <- merge(value_usd_world, data.table(Item = spam2fao_formatted, atlas_name = names(spam2fao_formatted)), all.x = TRUE)
prod_value_usd_world_fao <- melt(prod_value_usd_world_fao[, !"Item"], id.vars = c("atlas_name"), value.name = value_name, variable.name = "year")

#### 1.6.2) intd15 ####
#### 1.6.2.1) Africa ####
element <- "Gross Production Value (constant 2014-2016 thousand I$)"
value_name <- "value_intd15"

prod_value_intd_africa_fao <- prepare_fao_data(
  file = path_vop_africa_fao,
  spam2fao_formatted,
  elements = element,
  remove_countries = remove_countries,
  keep_years = target_year,
  atlas_iso3 = atlas_iso3
)

prod_value_intd_africa_fao <- melt(prod_value_intd_africa_fao, id.vars = c("iso3", "atlas_name"), value.name = value_name, variable.name = "year")

#### 1.6.2.2) World ####
path_vop_world_fao <- file.path(fao_dir, "Value_of_Production_E_All_Area_Groups.csv")
value_intd_world <- fread(path_vop_world_fao)
value_intd_world <- value_intd_world[Area == "World" & Element == element & Item %in% spam2fao_formatted, c("Item", paste0("Y", target_year)), with = FALSE]
prod_value_intd_world_fao <- merge(value_intd_world, data.table(Item = spam2fao_formatted, atlas_name = names(spam2fao_formatted)), all.x = TRUE)
prod_value_intd_world_fao <- melt(prod_value_intd_world_fao[, !"Item"], id.vars = c("atlas_name"), value.name = value_name, variable.name = "year")

#### 1.6.3) current US$ (the implied-price numerator, 2026-10-01) ####
# Price method (archive/dispatches/HANDOVER_2026-10-01_exposure-intld-fixes.md item 3, Pete 2026-10-01): the own
# nominal price is FAO's gross production value in CURRENT US$ divided by production. Where a
# producer price is published the two are identical; where it is not, FAO's imputation still
# reaches the GPV, so coverage rises from 30 % to 68 % of (country, crop) pairs. Clipped against
# the World GPV / World production unit value per item-year (1.6.3.2), never against the
# producer-price world median - they differ per item (plantain 0.2x, yams 0.25x, tea 5x).
#### 1.6.3.1) Africa ####
element <- "Gross Production Value (current thousand US$)"
value_name <- "value_cusd"
prod_value_cusd_africa_fao <- prepare_fao_data(
  file = path_vop_africa_fao,
  spam2fao_formatted,
  elements = element,
  remove_countries = remove_countries,
  keep_years = target_year,
  atlas_iso3 = atlas_iso3
)
prod_value_cusd_africa_fao <- melt(prod_value_cusd_africa_fao, id.vars = c("iso3", "atlas_name"), value.name = value_name, variable.name = "year")

#### 1.6.3.2) World: GPV current US$ / production per item-year -> world implied price ####
value_cusd_world <- fread(path_vop_world_fao)
value_cusd_world <- value_cusd_world[Area == "World" & Element == element & Item %in% spam2fao_formatted, c("Item", paste0("Y", target_year)), with = FALSE]
prod_value_cusd_world_fao <- merge(value_cusd_world, data.table(Item = spam2fao_formatted, atlas_name = names(spam2fao_formatted)), all.x = TRUE)
prod_value_cusd_world_fao <- melt(prod_value_cusd_world_fao[, !"Item"], id.vars = c("atlas_name"), value.name = value_name, variable.name = "year")

#### 1.6.4) current SLC (local currency) GPV + GDP deflators: the stale-local-price test (2026-10-02) ####
# Where FAO has no producer price it carries a frozen local-currency price forward and converts it
# at the current official rate (Sudan millet: the 2013 price for 2019-21 while the deflator went x37).
# R/price_fill.R::stale_local_price() rejects an implied price whose REAL local unit value fell by
# more than half over the window. Evidence: archive/dispatches/HANDOVER_2026-10-01_exposure-intld-fixes.md, "Evidence".
element <- "Gross Production Value (current thousand SLC)"
value_name <- "value_slc"
prod_value_slc_africa_fao <- prepare_fao_data(
  file = path_vop_africa_fao,
  spam2fao_formatted,
  elements = element,
  remove_countries = remove_countries,
  keep_years = target_year,
  atlas_iso3 = atlas_iso3
)
prod_value_slc_africa_fao <- melt(prod_value_slc_africa_fao, id.vars = c("iso3", "atlas_name"), value.name = value_name, variable.name = "year")

gdp_deflator <- fread(def_file, encoding = "Latin-1")[Item == "GDP Deflator" & Element == "Value Standard Local Currency, 2015 prices" & Year %in% target_year]
gdp_deflator[, iso3 := countrycode(sourcevar = as.numeric(gsub("[']", "", `Area Code (M49)`)), origin = "un", destination = "iso3c", warn = FALSE)]
gdp_deflator <- gdp_deflator[!is.na(iso3) & iso3 %in% atlas_iso3, .(iso3, year = as.integer(Year), deflator = Value)]
if (!nrow(gdp_deflator)) stop("[0.4.2] GDP deflator table is empty: check ", def_file)

### 1.7 Load Producer Prices #####
#### 1.7.1) Africa ####

path_prices_fao <- file.path(fao_dir, "Prices_E_Africa_NOFLAG.csv")

prod_price_africa_fao <- prepare_fao_data(
  file = path_prices_fao,
  spam2fao_formatted,
  elements = "Producer Price (USD/tonne)",
  remove_countries = remove_countries,
  keep_years = target_year,
  atlas_iso3 = atlas_iso3
)

prod_price_africa_fao <- melt(prod_price_africa_fao, id.vars = c("iso3", "atlas_name"), value.name = "price_usd", variable.name = "year")

# 1.7.1.1) Filter out weird values ####
prod_price_africa_fao[atlas_name == "sugb" & iso3 == "NER", price_usd := NA]
prod_price_africa_fao[atlas_name == "teas" & iso3 %in% c("RWA", "BDI"), price_usd := NA]
prod_price_africa_fao[atlas_name == "toba" & iso3 == "SLE", price_usd := NA]
prod_price_africa_fao[atlas_name == "cowp" & iso3 %in% c("GNB"), price_usd := NA]
prod_price_africa_fao[atlas_name == "coco" & iso3 %in% c("GIN"), price_usd := NA]
prod_price_africa_fao[atlas_name == "oilp" & iso3 %in% c("BDI"), price_usd := NA]

#### 1.7.2) World ####

prod_price_world <- fread(fao_econ_file_world)
prod_price_world <- prod_price_world[Element == "Producer Price (USD/tonne)"]
prod_price_world[, M49 := as.numeric(gsub("[']", "", `Area Code (M49)`))]
prod_price_world[, iso3 := countrycode(sourcevar = M49, origin = "un", destination = "iso3c")]
prod_price_world <- merge(prod_price_world, data.table(Item = spam2fao_formatted, atlas_name = names(spam2fao_formatted)), by = "Item", all.x = TRUE)

prod_price_world <- prod_price_world[!is.na(atlas_name), .(iso3, atlas_name, Year, Value)]
setnames(prod_price_world, c("Value", "Year"), c("price_usd", "year"))

### 1.8 Load FAO production estimates #####
#### 1.8.1 Africa #####

path_prod_africa_fao <- file.path(fao_dir, "Production_Crops_Livestock_E_Africa_NOFLAG.csv")

prod_ton_africa_fao <- prepare_fao_data(
  file = path_prod_africa_fao,
  spam2fao_formatted,
  elements = "Production",
  units = "t",
  remove_countries = remove_countries,
  keep_years = target_year,
  atlas_iso3 = atlas_iso3
)

prod_ton_africa_fao <- melt(prod_ton_africa_fao, id.vars = c("iso3", "atlas_name"), value.name = "production_t", variable.name = "year")

#### 1.8.2 World #####
prod_prod_world_fao <- file.path(fao_dir, "Production_Crops_Livestock_E_All_Area_Groups.csv")

prod_world <- fread(prod_file_world)
prod_world <- prod_world[Area == "World" & Element == "Production" & Item %in% spam2fao_formatted & Unit == "t", c("Item", paste0("Y", target_year)), with = FALSE]
prod_ton_world_fao <- merge(prod_world, data.table(Item = spam2fao_formatted, atlas_name = names(spam2fao_formatted)), all.x = TRUE)

prod_ton_world_fao <- melt(prod_ton_world_fao[, !"Item"], id.vars = c("atlas_name"), value.name = "production_t", variable.name = "year")

# World implied price per item-year (USD/t): the clip reference for the implied own prices.
source(file.path(project_dir, "R", "price_fill.R"))   # by PATH: a develop fix must reach the node run
prod_price_world_implied <- merge(prod_value_cusd_world_fao, prod_ton_world_fao, by = c("atlas_name", "year"))
prod_price_world_implied[, price_usd := implied_price(value_cusd, production_t)]
prod_price_world_implied <- prod_price_world_implied[!is.na(price_usd), .(atlas_name, year = as.integer(gsub("Y", "", year)), price_usd)]
if (nrow(prod_price_world_implied) == 0) stop("[0.4.2] world implied price table is empty: check Value_of_Production_E_All_Area_Groups.csv has 'Gross Production Value (current thousand US$)' for Area == World")

### 1.9) Merge datasets ####
prod_merge <- merge(prod_value_usd_africa_fao, prod_value_intd_africa_fao, all.x = TRUE)
prod_merge <- merge(prod_merge, prod_value_cusd_africa_fao, all.x = TRUE)
prod_merge <- merge(prod_merge, prod_value_slc_africa_fao, all.x = TRUE)
prod_merge <- merge(prod_merge, prod_price_africa_fao, all.x = TRUE)
prod_merge <- merge(prod_merge, prod_ton_africa_fao, all.x = TRUE)
prod_merge[, year := as.integer(gsub("Y", "", year))]

## 2) Load spam Production data ######
spam_files <- data.table(path = list.files(file.path(mapspam_pro_dir, "variable=prod_t"), full.names = TRUE))
spam_files <- spam_files[grepl("tif$", path)]
spam_files[, variable := tstrsplit(basename(path), "_", keep = 2)][, tech := gsub(".tif", "", tstrsplit(basename(path), "_", keep = 4)), by = .I]

prod_rast <- lapply(spam_files$path, rast)
names(prod_rast) <- spam_files$tech
# SPAM prod_t is native 0.05 deg. On a coarser grid, sum-resample (mass-conserving,
# issue #9 pattern, mirrors 0.4.0) before multiplying by the price raster.
# 2026-10-05: price x production runs on SPAM's native 0.05 deg grid at BOTH resolutions and the
# value rasters are sum-resampled to the output grid afterwards, as 0.4.0 does. Multiplying on 0.25
# deg gave every cell ONE country's price for production that straddles a border (BEN cowpea at 0.25
# deg is 4x its 0.05 deg tonnage: Nigerian border production).
source(file.path(project_dir, "R", "vop_allocate.R"))   # rasterize_country(), resample_sum_checked()
alloc_grid <- exposure_grid(res = "0.05", caller = "0.4.2 allocation grid")$rast
prod_rast <- lapply(prod_rast, function(r) resample_sum_checked(r, alloc_grid, "0.4.2 prod to 0.05"))

## 3) Infer missing prices ####
# Nominal price per (country, crop) for each year window. Chain (Pete 2026-10-01, item 3 of
# archive/dispatches/HANDOVER_2026-10-01_exposure-intld-fixes.md; evidence in R/checks/probe_price_method_deepdive.R):
#   1. own IMPLIED price  = FAO GPV current US$ / FAO production, window median, after a
#      [1/PRICE_BAND, PRICE_BAND] clip against World GPV / World production for the same item-year
#   2. own PRODUCER price (FAOSTAT USD/t), window median, clipped against the world producer-price
#      median for the same crop-year                                   (first fallback)
#   3. the same two over a longer series (window start - 5 .. window end)
#   4. neighbours -> region -> continent -> world MEDIANS of other countries' own prices
#      (R/price_fill.R, 2026-09-27: medians, not means, after the clip)
#   5. basis guard: within an item, a country whose nominal / constant-I$ ratio sits beyond
#      BASIS_BAND x the item's cross-country median carries a different price basis (auction
#      vs farm gate, cherry vs green bean, leaf vs made tea); it falls back to the item-median
#      factor x its own constant-I$ GPV, price_source = "basis fallback".
# Every row says where its price came from (price_source), and the audit CSV in
# mapspam_pro_dir/fao_prices carries every candidate. The 2026-09-27 lesson stands: an
# unclipped own value (ZWE wheat 2022 at 67,170 USD/t, identical in both FAO series) spread by a
# mean fill broke the product; the clip is the primary lever, the median the second, and the
# basis guard the third. The hand-picked NA list in 1.7.1.1 is kept but no longer load-bearing.
# The pre-2026-10-01 "tea floor" hack (price < 600 -> mean of own prices) is gone: it was a
# one-crop basis guard by hand, and the general one now covers tea with the rest.
year_sets <- list(y2021 = 2019:2023, y2015 = 2014:2016, y2020 = 2019:2020)

PRICE_BAND      <- as.numeric(Sys.getenv("PRICE_BAND", PRICE_BAND_DEFAULT))   # keep own prices within [1/band, band] x world
PRICE_FILL_STAT <- Sys.getenv("PRICE_FILL_STAT", "median")                      # "mean" reproduces the pre-2026-09-27 behaviour (A/B only)
BASIS_BAND      <- as.numeric(Sys.getenv("BASIS_BAND", BASIS_BAND_DEFAULT))   # within-item nominal/intld band around the item median
BASIS_MIN_N     <- as.integer(Sys.getenv("BASIS_MIN_N", BASIS_MIN_N_DEFAULT)) # own-priced countries an item needs before its median is a reference

# 3.0) Stale local prices: reject the implied price of a pair whose real local-currency unit value
#      fell by more than STALE_REAL_RATIO over the window (frozen imputation x depreciating official rate)
STALE_REAL_RATIO <- as.numeric(Sys.getenv("STALE_REAL_RATIO", STALE_REAL_RATIO_DEFAULT))
.stale <- stale_local_price(prod_merge[, .(iso3, atlas_name, year, value_slc, production_t)], gdp_deflator, years = 2019:2023, max_real_ratio = STALE_REAL_RATIO)
prod_merge[, price_implied := implied_price(value_cusd, production_t)]
prod_merge[, stale_local_price := FALSE]
prod_merge[.stale$is_stale, on = c("iso3", "atlas_name"), stale_local_price := TRUE]
cat(sprintf("[0.4.2] stale local price test (real SLC/t ratio over 2019-2023 < %.2f, STALE_REAL_RATIO): %d of %d pairs rejected as frozen imputations; countries %s\n",
            STALE_REAL_RATIO, nrow(.stale$is_stale), nrow(.stale$stale), paste(sprintf("%s(%d)", names(table(.stale$is_stale$iso3)), as.integer(table(.stale$is_stale$iso3))), collapse = " ")))
if (nrow(.stale$is_stale)) print(.stale$stale[verdict == "stale local price", .(iso3, atlas_name, n_years, slc_ratio = signif(slc_ratio, 3), defl_ratio = signif(defl_ratio, 3), real_ratio = signif(real_ratio, 3))], nrows = 60)
prod_merge[stale_local_price == TRUE, price_implied := NA_real_]

# 3.1) Own prices, clipped against the matching world reference
.clip_impl <- clip_prices_to_world_band(prod_merge[, .(iso3, atlas_name, year, price_usd = price_implied)],
                                        world = prod_price_world_implied[, .(atlas_name, year, price_usd)], band = PRICE_BAND)
.clip_pp   <- clip_prices_to_world_band(prod_merge[, .(iso3, atlas_name, year, price_usd)],
                                        world = prod_price_world[, .(atlas_name, year, price_usd)], band = PRICE_BAND)
cat(sprintf("[0.4.2] price clip, band %.1fx world (PRICE_BAND): implied dropped %d of %d own observations (vs World GPV/production); producer price dropped %d of %d (vs world producer-price median); fill statistic = %s\n",
            PRICE_BAND, nrow(.clip_impl$dropped), prod_merge[!is.na(price_implied), .N], nrow(.clip_pp$dropped), prod_merge[!is.na(price_usd), .N], PRICE_FILL_STAT))
if (nrow(.clip_impl$dropped)) { cat("[0.4.2] implied prices clipped (worst 25):\n"); print(.clip_impl$dropped[1:min(25, .N)], nrows = 25) }
if (nrow(.clip_pp$dropped))   { cat("[0.4.2] producer prices clipped (worst 25):\n"); print(.clip_pp$dropped[1:min(25, .N)], nrows = 25) }
PRICE_PINS <- fread(file.path(project_dir, "metadata", "price_pins.csv"))
cat(sprintf("[0.4.2] evidence pins loaded: %d (%s)\n", nrow(PRICE_PINS), paste(PRICE_PINS[, paste0(iso3, ":", atlas_name, "=", price_usd_t)], collapse = ", ")))
# the per-(country, crop) grid every window fills: everything FAO reports production or value for
price_grid <- unique(prod_merge[, .(iso3, atlas_name)])

price_usd_list <- lapply(seq_along(year_sets), function(i) {
  yrs <- year_sets[[i]]; ymin <- min(yrs); ymax <- max(yrs); long <- (ymin - 5):ymax
  nm <- names(year_sets)[i]

  # 3.2) window medians of what the row is priced against: FAO production, constant-I$ GPV
  recent <- prod_merge[year %in% yrs, .(production_t = median(production_t, na.rm = TRUE),
                                        value_intd15 = median(value_intd15, na.rm = TRUE)), by = .(atlas_name, iso3)]
  recent <- merge(price_grid, recent, by = c("iso3", "atlas_name"), all.x = TRUE)

  # 3.3) own price: implied -> producer -> longer series of each
  own <- own_price_window(.clip_impl$kept, .clip_pp$kept, years = yrs, long_years = long)
  recent <- merge(recent, own, by = c("iso3", "atlas_name"), all.x = TRUE)

  # 3.4) world references for the window: implied (the fill-chain tail and the gate's reference) and producer price
  w_impl <- prod_price_world_implied[year %in% yrs, .(price_world = median(price_usd, na.rm = TRUE)), by = atlas_name]
  w_pp   <- prod_price_world[year %in% yrs, .(price_usd_global_pp = median(price_usd, na.rm = TRUE)), by = atlas_name]

  # 3.4b) window clip (cglabs Block B 2026-10-04): the per-year clip keeps a year whose world price
  # spiked, so the window MEDIAN can still sit beyond the band against the window world reference
  # (GNB sorghum 1,491 = 6.7x, ERI sesame 3,091 = 5.5x, both just inside the 4x basis band). The own
  # value is judged once more on the window, against the reference that matches its source.
  recent <- merge(recent, w_impl[, .(atlas_name, .w_impl = price_world)], by = "atlas_name", all.x = TRUE)
  recent <- merge(recent, w_pp[, .(atlas_name, .w_pp = price_usd_global_pp)], by = "atlas_name", all.x = TRUE)
  recent[, .ref := data.table::fifelse(grepl("implied", price_source), .w_impl, .w_pp)]
  .wc <- recent[!is.na(price_usd) & is.finite(.ref) & (price_usd > PRICE_BAND * .ref | price_usd < .ref / PRICE_BAND)]
  if (nrow(.wc)) {
    cat(sprintf("[0.4.2] %s window clip: %d own window values beyond %.0fx the window world reference -> fill chain\n", nm, nrow(.wc), PRICE_BAND))
    print(.wc[, .(iso3, atlas_name, price_source, price_usd = signif(price_usd, 4), world = signif(.ref, 4), ratio = signif(price_usd / .ref, 3))], nrows = 40)
    recent[.wc, on = c("iso3", "atlas_name"), `:=`(price_usd = NA_real_, price_source = NA_character_)]
  }
  recent[, c(".w_impl", ".w_pp", ".ref") := NULL]

  # 3.5) basis guard on the OWN prices, BEFORE the spatial fill (2026-10-05, cglabs C3 stop: GNB
  #      plantain took GIN's raw own price, 30.6 USD/t, as its "neighbours median" although the guard
  #      had rejected that price for GIN itself). A rejected own price is removed from the pool the
  #      fills draw on; the flagged country gets its basis fallback after the fill.
  bg <- apply_basis_guard(recent, value_field = "price_usd", prod_field = "production_t", intld_field = "value_intd15",
                          intld_unit = 1000, band = BASIS_BAND, min_n = BASIS_MIN_N)
  .fallback <- bg$flagged[, .(iso3, atlas_name, price_fallback = price_now)]
  recent <- bg$data
  if (nrow(.fallback)) recent[.fallback, on = c("iso3", "atlas_name"), price_usd := NA_real_]
  recent[price_source == "basis fallback", price_source := NA_character_]

  # 3.6) spatial fills on the other countries' OWN prices (medians), source named per row
  own_source <- recent[, .(iso3, atlas_name, own_source = price_source)]
  recent[, price_source := NULL]
  recent <- fill_price_robust(recent, value_field = "price_usd", group_field = "atlas_name",
                              neighbors = african_neighbors, regions = regions, world = w_impl, stat = PRICE_FILL_STAT)
  recent <- merge(recent, own_source, by = c("iso3", "atlas_name"), all.x = TRUE)
  recent[price_source == "own", price_source := own_source][, own_source := NULL]
  recent <- merge(recent, w_pp, by = "atlas_name", all.x = TRUE)
  if (nrow(.fallback)) {
    recent <- merge(recent, .fallback, by = c("iso3", "atlas_name"), all.x = TRUE)
    recent[!is.na(price_fallback), `:=`(price_usd_final = price_fallback, price_source = "basis fallback")][, price_fallback := NULL]
  }
  # filled prices outside the band: logged, not changed (a fill is another country's basis already)
  .info <- recent[!price_source %in% c(OWN_SOURCES, "basis fallback") & is.finite(basis_median) & is.finite(value_intd15) & value_intd15 > 0 & production_t > 0]
  .info[, r := price_usd_final * production_t / (value_intd15 * 1000)]
  .info <- .info[r > BASIS_BAND * basis_median | r < basis_median / BASIS_BAND]
  cat(sprintf("[0.4.2] %s basis guard (band %gx the item median of nominal/intld, items with >= %d own-priced countries, applied BEFORE the fill): %d own prices replaced by the item-median factor x constant-I$ value; %d filled prices outside the band left as they are\n",
              nm, BASIS_BAND, BASIS_MIN_N, nrow(bg$flagged), nrow(.info)))
  if (nrow(bg$flagged)) print(bg$flagged[, .(iso3, atlas_name, price_source, price_was = signif(price_was, 4), price_now = signif(price_now, 4), basis_ratio, basis_median, basis_n)], nrows = 60)
  if (nrow(.info)) print(.info[1:min(20, .N), .(iso3, atlas_name, price_source, price = signif(price_usd_final, 4), ratio = signif(r, 3), basis_median = signif(basis_median, 3))], nrows = 20)

  # 3.7) evidence pins (metadata/price_pins.csv): cited independent evidence overrides the chain for a named few
  pn <- apply_price_pins(recent, PRICE_PINS, value_field = "price_usd_final")
  recent <- pn$data
  if (nrow(pn$applied)) { cat(sprintf("[0.4.2] %s evidence pins applied: %d\n", nm, nrow(pn$applied))); print(pn$applied, nrows = 20) }
  recent[, stale_local_price := FALSE]; if (nrow(.stale$is_stale)) recent[.stale$is_stale, on = c("iso3", "atlas_name"), stale_local_price := TRUE]
  recent[, vop_usd_nominal := production_t * price_usd_global][, year := nm]
  recent
})

names(price_usd_list) <- paste0("nominal-usd-", gsub("y", "", names(year_sets)))

# Audit trail: where every price came from, and the ones furthest from the world reference.
ensure_dir(file.path(mapspam_pro_dir, "fao_prices"))
for (nm in names(price_usd_list)) {
  p <- price_usd_list[[nm]]
  src <- p[, .N, by = price_source][order(-N)]
  cat(sprintf("[0.4.2] %s fill sources: %s\n", nm, paste(sprintf("%s=%d", src$price_source, src$N), collapse = " | ")))
  if ("production_t" %in% names(p)) {
    own_share <- p[!is.na(production_t), sum(production_t[price_source %in% c(OWN_SOURCES, "basis fallback", "evidence pin")], na.rm = TRUE) / sum(production_t, na.rm = TRUE)]
    cat(sprintf("[0.4.2] %s own (incl. basis fallback) prices cover %.0f%% of rows and %.0f%% of FAO production\n", nm,
                100 * p[, mean(price_source %in% c(OWN_SOURCES, "basis fallback", "evidence pin"))], 100 * own_share))
  }
  top <- p[is.finite(price_usd_final / price_usd_global)][order(-abs(log(price_usd_final / price_usd_global)))][1:min(10, .N),
           .(iso3, atlas_name, price_source, price_usd_final = signif(price_usd_final, 4), price_usd_global = signif(price_usd_global, 4), ratio_world = signif(price_usd_final / price_usd_global, 3))]
  cat(sprintf("[0.4.2] %s furthest from the world implied price (own values within %.0fx by construction; a basis fallback may sit outside):\n", nm, PRICE_BAND)); print(top, nrows = 10)
  if (p[, any(!is.finite(price_usd_final))]) cat(sprintf("[0.4.2] WARN %s: %d rows with no price at all: %s\n", nm, p[!is.finite(price_usd_final), .N],
                                                        p[!is.finite(price_usd_final), paste(unique(atlas_name), collapse = ",")]))
  fwrite(p[, .(iso3, atlas_name, price_source, price_usd_implied, price_usd_producer, price_usd_implied_long, price_usd_producer_long, price_usd,
               price_usd_neighbors, price_usd_region, price_usd_continent, price_usd_global, price_usd_global_pp,
               basis_ratio, basis_median, stale_local_price, price_usd_final, production_t, value_intd15)],
         file.path(mapspam_pro_dir, "fao_prices", paste0("crop_price_", nm, "-t_fill-sources_", .eg$tag, ".csv")))
}

## 4) Multiply mapspam production by price ####

for (i in seq_along(price_usd_list)) {
  cat("Running period i =", i, "/", length(price_usd_list), "           \n")

  # Unit is t x usd/t = usd or 1000 intdlr x 1000 there should be no need for any unit conversions (e.g. x 1000)
  final_price <- copy(price_usd_list[[i]])
  setnames(final_price, "price_usd_final", "value", skip_absent = TRUE)

  # Duplicate coffee prices for arabica and robusta (they are lumped in faostat)
  final_price <- final_price[, list(iso3, atlas_name, value)]
  robusta <- final_price[atlas_name == "coff"][, atlas_name := "rcof"]
  final_price[atlas_name == "coff", atlas_name := "acof"]
  final_price <- rbind(final_price, robusta)

  # Duplicate millet prices for pearl millet and small millet (they are lumped in faostat)
  final_price <- final_price[, list(iso3, atlas_name, value)]
  pearl <- final_price[atlas_name == "mill"][, atlas_name := "pmil"]
  final_price[atlas_name == "mill", atlas_name := "smil"]
  final_price <- rbind(final_price, pearl)

  final_price <- merge(final_price, ms_codes[, .(code_low, Fullname)], by.x = "atlas_name", by.y = "code_low", all.x = TRUE)

  no_match <- final_price[is.na(Fullname)]
  if (nrow(no_match) > 0) {
    stop("Some crop names are not matching to ms_codes: ", no_match[, paste0(unique(atlas_name), collapse = ", ")])
  }

  final_price_cast <- dcast(final_price, iso3 ~ Fullname)

  # Convert value to vector then raster
  final_price_vect <- geoboundaries
  final_price_vect <- merge(final_price_vect, final_price_cast, all.x = TRUE)

  crop_names <- sort(names(final_price_cast)[-1])

  # #40 (2026-10-01): at 0.25 deg no cell centre falls inside the Seychelles, so a centre-only price
  # raster was NA there (SYC coconut 1.34 M USD -> 0). Since 2026-10-05 the price raster is built on
  # 0.05 deg (centre rule, touches as cover) and the value raster is sum-resampled afterwards, so a
  # 0.25 deg border cell no longer prices both countries' production at one country's price.
  # Centre rule, touches only as cover (rasterize_country): small islands (SYC, #40) and coastal cells
  # with an offshore centre get their country's price; land-border cells keep the centre's country.
  final_price_rast <- terra::rast(lapply(crop_names, FUN = function(NAME) rasterize_country(final_price_vect, alloc_grid, field = NAME)))
  names(final_price_rast) <- crop_names

  price_save_file <- file.path(mapspam_pro_dir, "fao_prices", paste0("crop_price_", names(price_usd_list)[i], "-t_", .eg$tag, ".tif"))
  ensure_dir(dirname(price_save_file))
  # the saved price raster is on the OUTPUT grid (informational); the multiplication below stays on 0.05
  terra::writeRaster(if (terra::compareGeom(alloc_grid, base_rast, stopOnError = FALSE)) final_price_rast else terra::rast(lapply(crop_names, function(NAME) rasterize_country(final_price_vect, base_rast, field = NAME))) |> stats::setNames(crop_names),
                     price_save_file, overwrite = TRUE)

  # Multiply national VoP by glw cell proportion
  for (j in seq_along(prod_rast)) {
    cat("Running period i =", i, "/", length(price_usd_list), "| spam system j =", j, "/", length(prod_rast), "           \n")
    prod_rast_focus <- prod_rast[[j]]
    spam_names <- names(prod_rast_focus)
    name_check <- !crop_names %in% spam_names
    if (sum(name_check) > 0) {
      stop("Non match in crop names between fao and spam:", crop_names[!name_check])
    }

    prod_rast_focus <- prod_rast_focus[[crop_names]]

    if (sum(names(prod_rast_focus) != names(final_price_rast)) > 0) {
      stop("Check order of rasters in price x production multiplication")
    }

    prod_vop <- prod_rast_focus * final_price_rast
    prod_vop <- round(resample_sum_checked(prod_vop, base_rast, "0.4.2 vop"), 0)

    save_file <- file.path(
      mapspam_pro_dir,
      paste0("variable=vop_", names(price_usd_list)[i]),
      paste0("spam_vop_", names(price_usd_list)[i], "_", names(prod_rast)[j], "_", .eg$tag, ".tif")
    )
    ensure_dir(dirname(save_file))
    terra::writeRaster(prod_vop, save_file, overwrite = TRUE)
  }
}
