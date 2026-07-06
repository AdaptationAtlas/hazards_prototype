# =============================================================================
# qaqc_vop_vs_faostat.R
# -----------------------------------------------------------------------------
# QAQC (p.steward 2026-07-06): the gridded VoP surfaces distribute national
# FAOStat Gross Production Value across pixels. So the country total of the
# gridded VoP MUST come back close to the FAOStat national GPV it was built
# from. This checks exactly that, in **constant international dollars (I$)**, for
# BOTH the livestock and crop VoP rasters that R/3 consumes for hazard_exposure.
#
# WHY it matters now: the intld15 livestock output was previously nominal-USD
# (mislabel) while crop intld is constant-I$-2015 -> a currency mismatch that
# drove the ~7x cattle inflation. After the 0.4.1 fix (intld -> real vop_intd15)
# both should sit at ratio ~1 vs FAOStat I$. A ratio far from 1 flags a currency/
# units/mass-loss problem BEFORE any publish.
#
#   livestock ratio ~1  AND  crop ratio ~1   -> VoP base sound, currencies aligned
#   ratio >> 1 or << 1                        -> basis/units error -> do NOT publish
#
# RUN (cglabs, after re-baking 0.4.1 -> 0.4.4 -> R/3):  Rscript R/qaqc_vop_vs_faostat.R
# Read-only. Writes a CSV report to exposure_dir. ~2-3 min.
# =============================================================================

.qlog <- function(msg) {
  cat(sprintf("[%s] [qaqc-vop] %s\n", format(Sys.time(), "%H:%M:%S"), msg))
  flush.console()
}

suppressWarnings(suppressMessages({
  library(terra); library(data.table); library(arrow); library(sf)
}))

.qlog("sourcing haz_functions + 0_server_setup.R")
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))
source(file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R"))

YEARS <- 2019:2023   # match the vop_intld15-2021 window (year_sets$y2021)
FAO_I_ELEMENT <- "Gross Production Value (constant 2014-2016 thousand I$)"
remove_countries <- c("Ethiopia PDR", "Sudan (former)", "Cabo Verde", "Comoros", "Mauritius", "R\xe9union", "Seychelles")

# --- admin0 polygons ---------------------------------------------------------
.qlog("loading admin0 boundaries")
geob <- arrow::read_parquet(geo_files_local[1]) |> sf::st_as_sf() |> terra::vect()
geob <- terra::aggregate(geob, "iso3")
atlas_iso3 <- geob$iso3

vop_file <- file.path(fao_dir, "Value_of_Production_E_Africa.csv")

# --- helper: FAOStat national GPV (const I$, x1000) per iso3 x atlas_name -----
fao_gpv_i <- function(item_map) {
  d <- unique(prepare_fao_data(
    file = vop_file, item_map, elements = FAO_I_ELEMENT,
    remove_countries = remove_countries, keep_years = YEARS, atlas_iso3 = atlas_iso3
  ))
  d[, atlas_name := gsub(" (indigenous)", "", atlas_name)]
  d <- melt(d, id.vars = c("iso3", "atlas_name"), variable.name = "year", value.name = "gpv_i_k")
  # thousand I$ -> I$, median across the window per iso3 x atlas_name
  d[, .(fao_vop_i = median(gpv_i_k, na.rm = TRUE) * 1000), by = .(iso3, atlas_name)]
}

# --- helper: gridded VoP country total per layer ------------------------------
grid_adm0 <- function(r) {
  ex <- terra::extract(r, geob, fun = "sum", na.rm = TRUE, ID = FALSE)
  ex <- data.table(ex); ex[, iso3 := geob$iso3]
  melt(ex, id.vars = "iso3", variable.name = "layer", value.name = "grid_vop")
}

report <- list()

# =============================================================================
# LIVESTOCK
# =============================================================================
.qlog("LIVESTOCK: FAO I$ GPV + gridded VoP")
ls_vop_file <- file.path(glw2020_pro_dir, "variable=vop_intld15-2021", "glw4-2020_vop_intld15-2021.tif")
if (file.exists(ls_vop_file)) {
  # FAO livestock GPV uses indigenous meat items (matches 0.4.1)
  lps2fao_ind <- lps2fao
  lps2fao_ind[grep("Meat", lps2fao_ind)] <- paste0(lps2fao_ind[grep("Meat", lps2fao_ind)], " (indigenous)")
  fao_ls <- fao_gpv_i(lps2fao_ind)
  # map atlas_name -> glw species (cattle/goats/sheep/pigs/poultry)
  glw_of <- function(a) fifelse(grepl("cattle", a), "cattle",
                        fifelse(grepl("goat", a), "goats",
                        fifelse(grepl("sheep", a), "sheep",
                        fifelse(grepl("pig", a), "pigs",
                        fifelse(grepl("poultry|chicken", a), "poultry", NA_character_)))))
  fao_ls[, species := glw_of(atlas_name)]
  fao_ls_c <- fao_ls[!is.na(species), .(fao_vop_i = sum(fao_vop_i, na.rm = TRUE)), by = .(iso3, species)]

  g <- grid_adm0(terra::rast(ls_vop_file))
  g[, species := gsub("_highland|_tropical|_high|_low", "", layer)]
  g_c <- g[, .(grid_vop = sum(grid_vop, na.rm = TRUE)), by = .(iso3, species)]

  ls <- merge(g_c, fao_ls_c, by = c("iso3", "species"), all = TRUE)
  ls[, ratio := grid_vop / fao_vop_i][, commodity_type := "livestock"]
  report$livestock <- ls
  .qlog(sprintf("LIVESTOCK ratios: median=%.2f | within 0.9-1.1 = %d/%d | AGO cattle grid=%.2fM I$ fao=%.2fM I$ ratio=%.2f",
                ls[is.finite(ratio), median(ratio, na.rm = TRUE)],
                ls[is.finite(ratio) & abs(ratio - 1) <= 0.1, .N], ls[is.finite(ratio), .N],
                ls[iso3 == "AGO" & species == "cattle", grid_vop / 1e6],
                ls[iso3 == "AGO" & species == "cattle", fao_vop_i / 1e6],
                ls[iso3 == "AGO" & species == "cattle", ratio]))
} else {
  .qlog(sprintf("livestock VoP not found (%s) — re-bake 0.4.1 then re-run", ls_vop_file))
}

# =============================================================================
# CROP  (national total: robust to layer-name->FAO mapping differences)
# =============================================================================
.qlog("CROP: FAO I$ GPV + gridded VoP (national totals)")
crop_vop_file <- Sys.glob(file.path(mapspam_pro_dir, "variable=vop_intld15", "*intld15_all*.tif"))
if (length(crop_vop_file)) {
  spam2fao <- fread("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SPAM2010_FAO_crops.csv")
  spam_map <- setNames(spam2fao$name_fao_val, spam2fao$short_spam2010)
  spam_map <- spam_map[!is.na(spam_map) & nzchar(spam_map)]
  fao_cr <- fao_gpv_i(spam_map)
  fao_cr_tot <- fao_cr[, .(fao_vop_i = sum(fao_vop_i, na.rm = TRUE)), by = iso3]

  gc_ <- grid_adm0(terra::rast(crop_vop_file[1]))
  gc_tot <- gc_[, .(grid_vop = sum(grid_vop, na.rm = TRUE)), by = iso3]

  cr <- merge(gc_tot, fao_cr_tot, by = "iso3", all = TRUE)
  cr[, ratio := grid_vop / fao_vop_i][, `:=`(species = "ALL-CROPS", commodity_type = "crop")]
  report$crop <- cr
  .qlog(sprintf("CROP national-total ratios: median=%.2f | within 0.9-1.1 = %d/%d | file=%s",
                cr[is.finite(ratio), median(ratio, na.rm = TRUE)],
                cr[is.finite(ratio) & abs(ratio - 1) <= 0.1, .N], cr[is.finite(ratio), .N],
                basename(crop_vop_file[1])))
} else {
  .qlog(sprintf("crop VoP intld not found under %s/variable=vop_intld15 — check download", mapspam_pro_dir))
}

# --- write report + verdict --------------------------------------------------
out <- rbindlist(report, use.names = TRUE, fill = TRUE)
if (nrow(out)) {
  out_file <- file.path(exposure_dir, "qaqc_vop_vs_faostat.csv")
  fwrite(out[order(commodity_type, iso3, species)], out_file)
  .qlog(sprintf("wrote %s (%d rows)", out_file, nrow(out)))
  cat("\n=============================== QAQC VERDICT ===============================\n")
  cat("Gridded country VoP / FAOStat national GPV (constant I$). Target ~1.0.\n")
  for (ct in unique(out$commodity_type)) {
    s <- out[commodity_type == ct & is.finite(ratio)]
    cat(sprintf("  %-9s: median ratio %.2f | within 0.9-1.1: %d/%d | worst: %s\n",
                ct, s[, median(ratio, na.rm = TRUE)],
                s[abs(ratio - 1) <= 0.1, .N], nrow(s),
                s[order(-abs(log(ratio)))][1, sprintf("%s/%s=%.2f", iso3, species, ratio)]))
  }
  cat(" ratio ~1 both -> VoP base sound + currencies aligned (I$). Far from 1 -> basis/units error, DO NOT publish.\n")
  cat("===========================================================================\n")
} else {
  .qlog("no VoP rasters found — re-bake first, then re-run this QAQC.")
}
