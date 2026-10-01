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
# Method (92cb0b0, generalised 2026-10-01 in R/vop_allocate.R): spam_vop_intd =
# national_FAO_GPV_I$ x pixel_prod / national_prod of the ALLOCATION GROUP (the SPAM
# layers that share the FAO item: millet, coffee, the pooled banana family; #38), and
# NA where SPAM covers < VOP_COVERAGE_MIN of FAO production (#39) -> country total ==
# FAOStat GPV for every allocated pair, checked in-script.
# Modernizations: base_rast_path (align livestock 0.4.1 + R/3 grid, NOT atlas_delta);
# FAO GPV = median(2019:2023) x1000 (match 0.4.1 + qaqc_vop_vs_faostat.R so the
# crop QAQC validates to ~1); logging + FORCE_OVERWRITE gate; dropped stray/
# interactive lines. RUN 0_server_setup.R first (like 0.4.1/0.4.2/0.4.4).
# ==============================================================================

pacman::p_load(terra, data.table, httr, countrycode, wbstats, arrow, geoarrow, dplyr, tidyr, pbapply)
source(file.path(Sys.getenv("project_dir", getwd()), "R", "haz_functions.R"))   # local copy: a develop fix must reach the node run (was GitHub main)
options(scipen = 999)
terra::gdalCache(60000)

.log040 <- function(msg) {
  cat(sprintf("[%s] [0.4.0] %s\n", format(Sys.time(), "%H:%M:%S"), msg))
  flush.console()
}
overwrite_crop <- atlas_env_flag("FORCE_OVERWRITE", strict = TRUE)
.log040(sprintf("script start (FORCE_OVERWRITE=%s -> overwrite=%s)",
                Sys.getenv("FORCE_OVERWRITE", "<unset>"),
                atlas_env_flag("FORCE_OVERWRITE", strict = TRUE)))

spam2fao_url <- file.path(Sys.getenv("project_dir", getwd()), "metadata", "SPAM2010_FAO_crops.csv")

# 1) Geographies -------------------------------------------------------------
.log040("loading geoboundaries (admin0)")
geoboundaries <- arrow::read_parquet(geo_files_local[1]) |> sf::st_as_sf() |> terra::vect()
geoboundaries <- terra::aggregate(geoboundaries, "iso3")

# 2) MapSPAM production ------------------------------------------------------
# Grid = exposure_grid() (0_server_setup.R): EXPOSURE_RES = 0.05 | 0.25, required.
# Outputs are written once per resolution with the tag in the name (res-05 /
# res-25); R/3 reads the tag matching its hazard grid, 0.4.4 the tag matching
# its zonal grid (issue #30, p.steward 2026-09-23).
.eg <- exposure_grid(caller = "0.4.0")
.log040(sprintf("exposure grid = %s | res %.4f | tag %s", basename(.eg$path), .eg$res_deg, .eg$tag))
.log040("loading base raster + rasterizing admin0")
base_rast <- .eg$rast
# touches = TRUE (#40, 2026-10-01): a polygon that owns no cell CENTRE at 0.25 deg (the
# Seychelles) had no zone here, so its SPAM production (sum-resampled from 0.05 deg, present
# in the cell) got no national total and a NaN share; coastal cells whose centre is offshore
# were likewise outside every zone and their value was redistributed inland. 0.4.4 rasterises
# its zones with touches = TRUE already; this matches it.
admin_rast <- terra::rasterize(geoboundaries, base_rast, field = "iso3", touches = TRUE)

# SPAM layers are the long SPAM names ("pearl millet", "arabica coffee", ...); the mapping
# table says which FAO item(s) each one is valued under. 2026-10-01: the layer set comes from
# the rasters themselves, matched to the table - the old `Code_ifpri_2020` filter silently
# dropped small millet (#38).
spam2fao <- fread(spam2fao_url)
source(file.path(project_dir, "R", "vop_allocate.R"))   # by PATH: a develop fix must reach the node run
VOP_COVERAGE_MIN <- as.numeric(Sys.getenv("VOP_COVERAGE_MIN", VOP_COVERAGE_MIN_DEFAULT))   # #39 guard: SPAM national t / FAO production t

spam_dir <- file.path(mapspam_pro_dir, "variable=prod_t")
files_raw <- list.files(spam_dir, ".tif$", full.names = TRUE)
.log040(sprintf("found %d SPAM prod_t files in %s", length(files_raw), spam_dir))
if (length(files_raw) == 0L) stop("No SPAM prod_t tifs found in ", spam_dir)

spam_dat <- pblapply(seq_along(files_raw), function(i) {
  dat <- terra::rast(files_raw[i])
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
.log040(sprintf("SPAM techs: %s | layers (tech all): %s", paste(tech, collapse = ", "), paste(names(spam_dat$all), collapse = ", ")))

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

# 2.6) Allocation groups: which SPAM layers share which FAO items ------------------
# One FAO item over several SPAM layers (Millet -> pearl + small millet, Coffee -> arabica +
# robusta), several items into one layer (rapeseed <- rape or colza seed + mustard seed), and
# the pooled banana family (VOP_POOLED_ITEMS) are all one mechanism: value is distributed by
# each pixel's share of the GROUP's national production (R/vop_allocate.R, #38 / #39).
groups <- vop_item_groups(spam2fao, names(spam_dat$all))
.unmapped <- setdiff(names(spam_dat$all), unique(groups$layer))
if (length(.unmapped)) .log040(sprintf("WARN: SPAM layers with no FAO item in %s (no intld value): %s", basename(spam2fao_url), paste(.unmapped, collapse = ", ")))
.multi <- groups[, .(layers = uniqueN(layer), items = uniqueN(item)), by = group][layers > 1 | items > 1]
.log040(sprintf("allocation groups: %d groups over %d layers; compound: %s", uniqueN(groups$group), uniqueN(groups$layer),
                paste(sprintf("%s (%d layers, %d items)", .multi$group, .multi$layers, .multi$items), collapse = "; ")))

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
prod_value_i <- prod_value_i[Item %in% unique(groups$item) & is.finite(value) & value > 0, .(iso3, Item, value)]
.log040(sprintf("GPV rows (iso3 x item, window median > 0): %d over %d items", nrow(prod_value_i), uniqueN(prod_value_i$Item)))

# 3.1) FAOStat production (t), the coverage guard's denominator (#39) ---------------
.log040("loading FAOStat production (Africa) for the coverage guard")
fao_prod <- fread(prod_file, encoding = "Latin-1")[Element == "Production" & Unit == "t"]
fao_prod[, M49 := as.numeric(gsub("[']", "", `Area Code (M49)`))]
fao_prod[, iso3 := countrycode(sourcevar = M49, origin = "un", destination = "iso3c", warn = FALSE)]
fao_prod <- fao_prod[!is.na(iso3) & !Area %in% c("Ethiopia PDR", "Sudan (former)")]
fao_prod[grep("Maize", Item), Item := "Maize (corn)"]
fao_prod <- fao_prod[Item %in% unique(groups$item), c("iso3", "Item", paste0("Y", target_year)), with = FALSE]
fao_prod[, prod_t := apply(.SD, 1, median, na.rm = TRUE), .SDcols = paste0("Y", target_year)]
fao_prod <- fao_prod[is.finite(prod_t) & prod_t > 0, .(iso3, Item, prod_t)]

# 4) Distribute national GPV to SPAM production proportions ------------------
.log040("distributing FAO GPV by SPAM production share (per allocation group)")
spam_nat <- spam_prod_admin0_ex$all[, .(iso3, layer = as.character(Code), prod_t = prod)]
alloc <- vop_allocation_table(prod_value_i, fao_prod, spam_nat, groups, min_coverage = VOP_COVERAGE_MIN)
alloc <- alloc[iso3 %in% iso3_levels$iso3]            # FAO countries off this grid cannot be allocated
.guarded <- alloc[guarded == TRUE][order(-gpv)]
.unjudged <- alloc[reason == "no FAO production: coverage not judged"]
.log040(sprintf("allocation table: %d (iso3, group) pairs with GPV | %d guarded (NA, not distributed; VOP_COVERAGE_MIN=%.2f) holding %.2f B I$ | %d allocated without a FAO production row (coverage not judged)",
                nrow(alloc), nrow(.guarded), VOP_COVERAGE_MIN, sum(.guarded$gpv) / 1e6, nrow(.unjudged)))
if (nrow(.guarded)) {
  cat("[0.4.0] guarded pairs (value left NA):\n")
  print(.guarded[, .(iso3, group, gpv_kI = round(gpv), fao_prod_t = round(fao_prod_t), spam_prod_t = round(spam_prod_t), coverage = signif(coverage, 3), reason)], nrows = 200)
  .log040(sprintf("guarded countries: %s", paste(sprintf("%s(%d)", names(table(.guarded$iso3)), as.integer(table(.guarded$iso3))), collapse = " ")))
}
if (nrow(.unjudged)) .log040(sprintf("coverage not judged (allocated anyway): %s", paste(.unjudged[, paste0(iso3, ":", group)], collapse = " ")))
ensure_dir(file.path(mapspam_pro_dir, "fao_prices"))
.alloc_csv <- file.path(mapspam_pro_dir, "fao_prices", paste0("crop_vop_intld15-2021_allocation_", .eg$tag, ".csv"))
fwrite(alloc, .alloc_csv); .log040(sprintf("allocation audit written: %s", .alloc_csv))

spam_vop_intd <- vop_allocate_rasters(spam_dat$all, admin_rast, iso3_levels, alloc, groups, spam_nat)
spam_vop_intd <- spam_vop_intd[[sort(names(spam_vop_intd))]]

# Hard self-check: the zonal sum of what was written back equals what was allocated, per
# (country, group); guarded pairs come back empty. A silent miss here is #38 again.
.chk <- vop_check_totals(spam_vop_intd, admin_rast, iso3_levels, alloc, groups, tol = 1e-6)
if (!all(.chk$ok)) {
  print(.chk[ok == FALSE][1:min(40, .N)], nrows = 40)
  stop(sprintf("[0.4.0] allocation check failed for %d (iso3, group) pairs - see above", sum(!.chk$ok)))
}
.log040(sprintf("allocation check: %d pairs conserved to 1e-6, %d guarded pairs empty; continental total %.2f B I$",
                .chk[!is.na(value_alloc), .N], .chk[is.na(value_alloc), .N], sum(.chk$zonal_sum, na.rm = TRUE) / 1e6))

out_dir <- file.path(mapspam_pro_dir, "variable=vop_intld15-2021")
ensure_dir(out_dir)
save_file <- file.path(out_dir, paste0("spam_vop_intld15-2021_all_", .eg$tag, ".tif"))
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
if (!identical(names(spam_prod_i_p), names(spam_vop_intd))) stop("[0.4.0] layer order mismatch between the irrigated share and the VoP raster: ", paste(setdiff(names(spam_vop_intd), names(spam_prod_i_p)), collapse = ","))

spam_vop_intd_i <- spam_prod_i_p * spam_vop_intd
sub_dat <- spam_vop_intd_i
sub_dat[is.na(sub_dat)] <- 0
spam_vop_intd_r <- spam_vop_intd - sub_dat

f_i <- file.path(out_dir, paste0("spam_vop_intld15-2021_irr_", .eg$tag, ".tif"))
f_r <- file.path(out_dir, paste0("spam_vop_intld15-2021_rf-all_", .eg$tag, ".tif"))
if (!file.exists(f_i) || overwrite_crop) terra::writeRaster(round(spam_vop_intd_i * 1000, 1), f_i, overwrite = TRUE)
if (!file.exists(f_r) || overwrite_crop) terra::writeRaster(round(spam_vop_intd_r * 1000, 1), f_r, overwrite = TRUE)

cat("\n===== 0.4.0_create_crop_vop_intld15.R COMPLETE at ",
    format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"), " =====\n", sep = "")
