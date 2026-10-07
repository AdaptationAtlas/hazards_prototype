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
  library(terra); library(data.table); library(arrow); library(sf); library(countrycode)
}))

.qlog("sourcing haz_functions + 0_server_setup.R")
source(file.path(Sys.getenv("project_dir", getwd()), "R", "haz_functions.R"))   # local copy: a develop fix must reach the node run (was GitHub main)
source(file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R"))
source(file.path(Sys.getenv("project_dir", getwd()), "R", "vop_allocate.R"))   # vop_apply_quantity_pins(): the gate applies the same pins 0.4.0 does

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
# collapse: how several FAO items belonging to ONE atlas_name are combined.
#   "median"             - median over every (item, year) cell. Correct only when the mapping is
#                          1:1, which it is for livestock species. The historical behaviour.
#   "item_median_then_sum" - median across the window PER ITEM, then SUM the items. This is exactly
#                          what 0.4.0 does (`R/0.4.0:164` medians each item's year window,
#                          `vop_allocate.R:113` sums items into the group), and a gate's reference
#                          has to be built the same way as the input it judges.
#
# Why this argument exists (2026-10-07, found on the node at B5 A4.1). The crop map is MANY items to
# one atlas_name: `vege` carries 26 FAO items, `rest` 21. Under "median" the denominator was the
# median of N items x 5 years - one typical item standing in for the whole group - so ZAF `temf`
# read 0.032 B against an 11-item sum of 2.70 B. Before the B5 code join the name join matched only
# 1-5 items per composite, so the collapse cost ~3 % and the gate read 1.03; with 57 renamed codes
# recovered the composites hold 11-26 items and the gate read 1.17 against a product that was right.
# A gate that fails a correct run, from the same family as the G6 lesson.
#
# pins: metadata/fao_quantity_pins.csv. 0.4.0 applies these to the GPV it allocates, so the gate's
# reference must apply them too or a pinned pair reads low by construction (CAF coffee 0.657).
fao_gpv_i <- function(item_map, by = "name", collapse = c("median", "item_median_then_sum"), pins = NULL) {
  collapse <- match.arg(collapse)
  d <- unique(prepare_fao_data(
    file = vop_file, item_map, elements = FAO_I_ELEMENT,
    remove_countries = remove_countries, keep_years = YEARS, atlas_iso3 = atlas_iso3, by = by
  ))
  d[, atlas_name := gsub(" (indigenous)", "", atlas_name)]
  ycols <- grep("^Y\\d{4}$", names(d), value = TRUE)
  if (collapse == "median") {
    dm <- melt(d, id.vars = intersect(c("iso3", "atlas_name", "item_code"), names(d)),
               measure.vars = ycols, variable.name = "year", value.name = "gpv_i_k")
    return(dm[, .(fao_vop_i = median(gpv_i_k, na.rm = TRUE) * 1000), by = .(iso3, atlas_name)])
  }
  # median across the window per ITEM, mirroring 0.4.0
  if (!"item_code" %in% names(d)) stop("fao_gpv_i(collapse = 'item_median_then_sum') needs per-item rows - call with by = 'code'")
  d[, value := apply(.SD, 1, median, na.rm = TRUE), .SDcols = ycols]
  d <- d[is.finite(value) & value > 0, .(iso3, item_code, atlas_name, value)]
  if (!is.null(pins) && nrow(pins)) {
    # The pin scales GPV by prod_t_pinned / prod_t_FAO, so the FAO production table is needed to
    # derive the ratio - passing NULL would leave the ratio NA and make the pin a silent no-op.
    # Read it exactly as 0.4.0 does: same file, same element/unit, same window, median per item.
    .pf <- fread(prod_file, encoding = "Latin-1")[Element == "Production" & Unit == "t"]
    .pc <- vop_item_code_col(.pf)
    data.table::setnames(.pf, .pc, "item_code")
    .pf[, item_code := suppressWarnings(as.integer(item_code))]
    .pf[, M49 := as.numeric(gsub("[']", "", `Area Code (M49)`))]
    .pf[, iso3 := countrycode(sourcevar = M49, origin = "un", destination = "iso3c", warn = FALSE)]
    .ycols <- paste0("Y", YEARS)
    .pf <- .pf[!is.na(iso3) & item_code %in% unique(pins$item_code), c("iso3", "item_code", .ycols), with = FALSE]
    .pf <- .pf[, lapply(.SD, sum, na.rm = TRUE), by = .(iso3, item_code), .SDcols = .ycols]
    .pf[, prod_t := apply(.SD, 1, median, na.rm = TRUE), .SDcols = .ycols]
    .pf <- .pf[is.finite(prod_t) & prod_t > 0, .(iso3, item_code, prod_t)]
    .q <- vop_apply_quantity_pins(d, .pf, pins)
    if (nrow(.q$log) && any(!is.finite(.q$log$ratio)))
      stop("fao_gpv_i: a quantity pin produced no ratio - the FAO production row is missing, so the gate's denominator would silently keep the unpinned value")
    d <- .q$gpv
    if (nrow(.q$log)) {
      .qlog(sprintf("crop denominator: %d FAOSTAT quantity pin(s) applied, as 0.4.0 does", nrow(.q$log)))
      print(.q$log[, .(iso3, item_code, gpv_before = signif(gpv_before, 4), gpv_after = signif(gpv_after, 4))])
    }
  }
  # then sum the items into the atlas_name, mirroring vop_allocate.R:113
  d[, .(fao_vop_i = sum(value, na.rm = TRUE) * 1000), by = .(iso3, atlas_name)]
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
# Exposure rasters carry a resolution tag (res-05 / res-25, issue #30). National totals are
# grid-independent, so any tag serves; default to the hazard grid (0.25), legacy untagged last.
.qtag <- sprintf("res-%02d", round(as.numeric(Sys.getenv("EXPOSURE_RES", "0.25")) * 100))
.first_existing <- function(...) { fs <- c(...); fs <- fs[file.exists(fs)]; if (length(fs)) fs[1] else NA_character_ }
ls_vop_file <- .first_existing(
  file.path(glw2020_pro_dir, "variable=vop_intld15-2021", paste0("glw4-2020_vop_intld15-2021_", .qtag, ".tif")),
  Sys.glob(file.path(glw2020_pro_dir, "variable=vop_intld15-2021", "glw4-2020_vop_intld15-2021_res-*.tif")),
  file.path(glw2020_pro_dir, "variable=vop_intld15-2021", "glw4-2020_vop_intld15-2021.tif"))
if (!is.na(ls_vop_file)) .qlog(sprintf("livestock VoP raster: %s", basename(ls_vop_file)))
if (!is.na(ls_vop_file) && file.exists(ls_vop_file)) {
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
# Prefer the 0.4.0 FAOStat-const-I$ output (what R/3 now uses); fall back to the
# S3-legacy file if 0.4.0 hasn't been run yet (so the QAQC still reports something).
crop_vop_file <- Sys.glob(file.path(mapspam_pro_dir, "variable=vop_intld15-2021", paste0("spam_vop_intld15-2021_all_", .qtag, ".tif")))
if (!length(crop_vop_file)) crop_vop_file <- Sys.glob(file.path(mapspam_pro_dir, "variable=vop_intld15-2021", "spam_vop_intld15-2021_all_res-*.tif"))[1]
if (!length(crop_vop_file) || is.na(crop_vop_file[1])) crop_vop_file <- Sys.glob(file.path(mapspam_pro_dir, "variable=vop_intld15-2021", "spam_vop_intld15-2021_all.tif"))
if (!length(crop_vop_file)) crop_vop_file <- Sys.glob(file.path(mapspam_pro_dir, "variable=vop_intld15", "*intld15_all*.tif"))
if (length(crop_vop_file)) {
  spam2fao <- fread(file.path(Sys.getenv("project_dir", getwd()), "metadata", "SPAM2010_FAO_crops.csv"))
  # B5 (2026-10-07): key the FAO denominator on the item CODE, exactly as 0.4.0 now allocates.
  # Keyed on the name, this denominator missed the renamed composite-group items that 0.4.0 now
  # values, so a correct re-bake would have read as an ~8 % crop over-allocation. The gate's
  # reference has to be built the same way as the product it judges (AGENTS.md).
  spam_map <- setNames(suppressWarnings(as.integer(spam2fao$code_fao)), spam2fao$short_spam2010)
  spam_map <- spam_map[!is.na(spam_map)]
  .qpin_f <- file.path(Sys.getenv("project_dir", getwd()), "metadata", "fao_quantity_pins.csv")
  .qpins <- if (file.exists(.qpin_f)) fread(.qpin_f) else NULL
  fao_cr <- fao_gpv_i(spam_map, by = "code", collapse = "item_median_then_sum", pins = .qpins)
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
