# ==============================================================================
# 0.4.0_create_crop_vop_intld15.R
# Crop VoP in CONSTANT international dollars (I$), the crop analogue of the
# livestock 0.4.1 intld product: distribute FAOStat Gross Production Value
# (constant 2014-2016 I$) across pixels by each crop's SPAM production share.
# Output feeds R/3 hazard_exposure so crop + livestock VoP share ONE currency
# basis (const-I$, 2019-2023 window). Reinstated + modernized 2026-07-07 from
# commit 92cb0b0; supersedes the S3-legacy `spam_vop_intld15_all.tif` (which was
# NOT FAOStat-I$-aligned — QAQC ratio 1.21).
#
# Method (unchanged from 92cb0b0): spam_prop = pixel_prod / national_prod;
# spam_vop_intd = spam_prop x national_FAO_GPV_I$ -> country total == FAOStat GPV.
# Modernizations: base_rast_path (align livestock 0.4.1 + R/3 grid, NOT atlas_delta);
# FAO GPV = median(2019:2023) x1000 (match 0.4.1 + qaqc_vop_vs_faostat.R so the
# crop QAQC validates to ~1); logging + FORCE_OVERWRITE gate; dropped stray/
# interactive lines. RUN 0_server_setup.R first (like 0.4.1/0.4.2/0.4.4).
# ==============================================================================

pacman::p_load(terra, data.table, httr, countrycode, wbstats, arrow, geoarrow, dplyr, tidyr, pbapply)
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))
options(scipen = 999)
terra::gdalCache(60000)

.log040 <- function(msg) {
  cat(sprintf("[%s] [0.4.0] %s\n", format(Sys.time(), "%H:%M:%S"), msg))
  flush.console()
}
overwrite_crop <- nzchar(Sys.getenv("FORCE_OVERWRITE"))
.log040(sprintf("script start (FORCE_OVERWRITE=%s)", Sys.getenv("FORCE_OVERWRITE", "<unset>")))

ms_codes_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SpamCodes.csv"
spam2fao_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SPAM2010_FAO_crops.csv"

# 1) Geographies -------------------------------------------------------------
.log040("loading geoboundaries (admin0)")
geoboundaries <- arrow::read_parquet(geo_files_local[1]) |> sf::st_as_sf() |> terra::vect()
geoboundaries <- terra::aggregate(geoboundaries, "iso3")

# 2) MapSPAM production ------------------------------------------------------
# base_rast_path = the hazard/exposure grid used by 0.4.1 livestock + R/3.
# (The 92cb0b0 original used the atlas_delta base raster; that would put crop
# VoP on a different grid than livestock and break the R/3 crop+livestock stack.)
.log040("loading base raster + rasterizing admin0")
base_rast <- terra::rast(base_rast_path)
admin_rast <- terra::rasterize(geoboundaries, base_rast, field = "iso3")

ms_codes <- data.table::fread(ms_codes_url)[, Code := toupper(Code)][, Code_ifpri_2020 := toupper(Code_ifpri_2020)]
ms_codes <- ms_codes[compound == "no" & !is.na(Code_ifpri_2020) & !is.na(Code)]

spam_dir <- file.path(mapspam_pro_dir, "variable=prod_t")
files_raw <- list.files(spam_dir, ".tif$", full.names = TRUE)
.log040(sprintf("found %d SPAM prod_t files in %s", length(files_raw), spam_dir))
if (length(files_raw) == 0L) stop("No SPAM prod_t tifs found in ", spam_dir)

spam_dat <- pblapply(seq_along(files_raw), function(i) {
  dat <- terra::rast(files_raw[i])
  # FAOStat lumps arabica + robusta as one coffee item -> merge production so the
  # national coffee GPV distributes across both, then split back post-distribution.
  dat$coffee <- dat$`arabica coffee` + dat$`robusta coffee`
  # SPAM prod_t is native 0.05deg; admin_rast/base_rast is 0.25deg. Resample to
  # base (method="sum", mass-conserving) BEFORE the admin zonal + proportion, or
  # zonal/`raw_dat/spam_tot` hit "[zonal] extents do not match". Mirrors 0.4.1's
  # glw resample (L144). method="sum" conserves production totals (issue #9).
  if (!terra::compareGeom(dat, base_rast, stopOnError = FALSE)) {
    .src <- terra::global(dat, "sum", na.rm = TRUE)[, 1]
    dat <- terra::resample(dat, base_rast, method = "sum")
    .dst <- terra::global(dat, "sum", na.rm = TRUE)[, 1]
    if (any(abs(.dst / .src - 1) > 0.005, na.rm = TRUE)) {
      warning(sprintf("[0.4.0] SPAM prod mass not conserved on resample (tech %d): max dev %.3f%%",
                      i, 100 * max(abs(.dst / .src - 1), na.rm = TRUE)))
    }
  }
  dat
})
tech <- gsub(".tif", "", unlist(tstrsplit(basename(files_raw), "_", keep = 4)))
names(spam_dat) <- tech
.log040(sprintf("SPAM techs: %s", paste(tech, collapse = ", ")))

# 2.4) SPAM national totals by admin0 ----------------------------------------
iso3_levels <- levels(admin_rast)[[1]]
spam_prod_admin0_ex <- pblapply(seq_along(spam_dat), function(i) {
  dat <- spam_dat[[i]]
  ex_dat <- data.table(terra::zonal(dat, admin_rast, fun = "sum", na.rm = TRUE))
  ex_dat <- melt(ex_dat, id.vars = "iso3", variable.name = "Code", value.name = "prod")
  ex_dat <- merge(ex_dat, iso3_levels, by = "iso3", all.x = TRUE)
  ex_dat[, tech := names(spam_dat)[i]]
  ex_dat
})
names(spam_prod_admin0_ex) <- tech

spam2fao <- fread(spam2fao_url)[, short_spam2010 := toupper(short_spam2010)][short_spam2010 %in% c(ms_codes$Code, "COFF")]

# 2.6) Per-pixel production proportion (pixel prod / national prod) ----------
.log040("computing per-crop production proportions")
spam_prop <- lapply(seq_along(spam_prod_admin0_ex), function(i) {
  cat("Spam tech", i, "/", length(spam_prod_admin0_ex), "\n")
  dat <- spam_prod_admin0_ex[[i]]
  raw_dat <- spam_dat[[i]]
  crops <- names(raw_dat)
  spam_tot <- terra::rast(pblapply(seq_along(crops), function(j) {
    temp_data <- dat[Code == crops[j], .(ID, prod)]
    terra::classify(admin_rast, rcl = as.matrix(temp_data), include.lowest = TRUE)
  }))
  spam_prop <- raw_dat / spam_tot
  spam_prop[is.infinite(spam_prop)] <- NA
  names(spam_prop) <- crops
  spam_prop
})
names(spam_prop) <- names(spam_prod_admin0_ex)

# 3) FAOStat GPV (constant I$) -----------------------------------------------
vop_file_world <- file.path(fao_dir, "Value_of_Production_E_All_Data.csv")
if (!file.exists(vop_file_world)) {
  .log040("downloading FAOStat Value_of_Production_E_All_Data")
  url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Value_of_Production_E_All_Data.zip"
  zip_file_path <- file.path(fao_dir, basename(url))
  download.file(url, zip_file_path, mode = "wb")
  unzip(zip_file_path, exdir = fao_dir)
  unlink(zip_file_path)
}

target_year <- 2019:2023   # match livestock 0.4.1 year_set y2021 + qaqc window
element <- "Gross Production Value (constant 2014-2016 thousand I$)"
.log040("loading FAOStat GPV (constant I$)")
prod_value_i <- fread(vop_file_world, encoding = "Latin-1")
cols <- c("Item", "Element", "Area", "Area Code (M49)", paste0("Y", target_year))
prod_value_i <- prod_value_i[Element %in% element, ..cols]
prod_value_i[, M49 := as.numeric(gsub("[']", "", `Area Code (M49)`))]
prod_value_i[, iso3 := countrycode(sourcevar = M49, origin = "un", destination = "iso3c")]
prod_value_i <- prod_value_i[!is.na(iso3)]

prod_value_i[grep("Maize", Item), Item := "Maize (corn)"]
y_cols <- grep("^Y\\d{4}$", names(prod_value_i), value = TRUE)
prod_value_i <- prod_value_i[, lapply(.SD, sum, na.rm = TRUE), by = .(iso3, Item), .SDcols = y_cols]

# value = median across the window (thousand I$) — matches 0.4.1 vop_intd15 +
# qaqc_vop_vs_faostat.R so the crop QAQC validates to ~1. (The 92cb0b0 original
# used mean(2020:2022); realigned here for cross-commodity + QAQC consistency.)
prod_value_i[, value := apply(.SD, 1, median, na.rm = TRUE), .SDcols = y_cols]

prod_value_i <- merge(prod_value_i, spam2fao[, .(short_spam2010, name_fao_val)], by.x = "Item", by.y = "name_fao_val", all.x = TRUE)
prod_value_i <- prod_value_i[!is.na(short_spam2010)]
setnames(prod_value_i, "short_spam2010", "Code")

# 4) Distribute national GPV to SPAM production proportions ------------------
.log040("distributing FAO GPV by SPAM production share")
final_vop_i <- prod_value_i[, list(iso3, Code, value)]
final_vop_i <- merge(final_vop_i, iso3_levels, by = "iso3", all.x = TRUE)
final_vop_i <- final_vop_i[!is.na(ID)]
crops <- final_vop_i[, unique(Code)]

final_vop_i_rast <- terra::rast(pblapply(seq_along(crops), function(i) {
  temp_data <- final_vop_i[Code == crops[i], .(ID, value)]
  terra::classify(admin_rast, rcl = as.matrix(temp_data), include.lowest = TRUE)
}))
names(final_vop_i_rast) <- crops
names(final_vop_i_rast) <- spam2fao[match(names(final_vop_i_rast), spam2fao$short_spam2010), long_spam2010]

spam_prop_all <- spam_prop$all
spam_prop_all <- spam_prop_all[[names(spam_prop_all) %in% names(final_vop_i_rast)]]
spam_prop_all <- spam_prop_all[[sort(names(spam_prop_all))]]
final_vop_i_rast <- final_vop_i_rast[[sort(names(final_vop_i_rast))]]
if (!all(names(spam_prop_all) == names(final_vop_i_rast))) stop("crop layer name mismatch prop vs GPV")

spam_vop_intd <- spam_prop_all * final_vop_i_rast

# Split merged coffee GPV back into arabica/robusta by production share
coff <- spam_vop_intd$coffee
arcof <- spam_dat$all$`arabica coffee` + spam_dat$all$`robusta coffee`
acof <- coff * spam_dat$all$`arabica coffee` / arcof; names(acof) <- "arabica coffee"
rcof <- coff * spam_dat$all$`robusta coffee` / arcof; names(rcof) <- "robusta coffee"
spam_vop_intd$`arabica coffee` <- acof
spam_vop_intd$`robusta coffee` <- rcof
spam_vop_intd <- spam_vop_intd[[order(names(spam_vop_intd))]]
spam_vop_intd$coffee <- NULL

out_dir <- file.path(mapspam_pro_dir, "variable=vop_intld15-2021")
ensure_dir(out_dir)
save_file <- file.path(out_dir, "spam_vop_intld15-2021_all.tif")
if (!file.exists(save_file) || overwrite_crop) {
  .log040(sprintf("writing %s", save_file))
  terra::writeRaster(round(spam_vop_intd * 1000, 1), save_file, overwrite = TRUE)   # thousand I$ -> I$
}

# 5) Split into irrigated / rainfed by production share ----------------------
.log040("splitting VoP into irrigated / rainfed")
spam_prod_i <- spam_dat$irr[[order(names(spam_dat$irr))]]
spam_prod_a <- spam_dat$all[[order(names(spam_dat$all))]]
spam_prod_i_p <- spam_prod_i / spam_prod_a
spam_prod_i_p <- spam_prod_i_p[[names(spam_prod_i_p) %in% names(spam_vop_intd)]]

spam_vop_intd_i <- spam_prod_i_p * spam_vop_intd
sub_dat <- spam_vop_intd_i
sub_dat[is.na(sub_dat)] <- 0
spam_vop_intd_r <- spam_vop_intd - sub_dat

f_i <- file.path(out_dir, "spam_vop_intld15-2021_irr.tif")
f_r <- file.path(out_dir, "spam_vop_intld15-2021_rf-all.tif")
if (!file.exists(f_i) || overwrite_crop) terra::writeRaster(round(spam_vop_intd_i * 1000, 1), f_i, overwrite = TRUE)
if (!file.exists(f_r) || overwrite_crop) terra::writeRaster(round(spam_vop_intd_r * 1000, 1), f_r, overwrite = TRUE)

cat("\n===== 0.4.0_create_crop_vop_intld15.R COMPLETE at ",
    format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"), " =====\n", sep = "")
