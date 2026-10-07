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
base_rast <- .eg$rast   # the OUTPUT grid
# 2026-10-05 (cglabs Block C stop, BEN cowpea): the allocation runs on SPAM's native 0.05 deg grid at
# BOTH resolutions, and the finished value rasters are sum-resampled to the output grid. National
# totals, the coverage guard and the allocation are therefore identical on the two grids by
# construction; res-25 is the mass-conserving aggregate of res-05.
source(file.path(project_dir, "R", "vop_allocate.R"))   # by PATH: a develop fix must reach the node run
alloc_grid <- exposure_grid(res = "0.05", caller = "0.4.0 allocation grid")$rast
geoboundaries$zid <- seq_len(nrow(geoboundaries))
admin_rast <- rasterize_country(geoboundaries, alloc_grid, field = "zid",
                                labels = data.frame(ID = geoboundaries$zid, iso3 = geoboundaries$iso3))
.log040(sprintf("allocation grid 0.05 deg (SPAM native); admin zones: centre rule, touches only as cover; %d of %d countries own cells",
                length(unique(stats::na.omit(terra::values(admin_rast)[, 1]))), nrow(geoboundaries)))

# SPAM layers are the long SPAM names ("pearl millet", "arabica coffee", ...); the mapping
# table says which FAO item(s) each one is valued under. 2026-10-01: the layer set comes from
# the rasters themselves, matched to the table - the old `Code_ifpri_2020` filter silently
# dropped small millet (#38).
spam2fao <- fread(spam2fao_url)
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
  if (!terra::compareGeom(dat, alloc_grid, stopOnError = FALSE)) {
    .src <- terra::global(dat, "sum", na.rm = TRUE)[, 1]
    dat <- terra::resample(dat, alloc_grid, method = "sum")
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
# 2026-10-03: read the AFRICA bulk file, the same vintage every other FAO input of this pass and
# both gates use (0.4.2, qaqc_vop_vs_faostat.R, the basis guard). Until now 0.4.0 alone read
# Value_of_Production_E_All_Data.csv, which on the node was a 2025-08 copy against a 2026-05 set
# everywhere else: the constant-I$ product would have been judged against a different release than
# it was built from. The Africa file carries the constant-I$ element for every country 0.4.0 can
# allocate (archive/dispatches/DISPATCH_cglabs_exposure_intld_fixes.md, cglabs Block A response 2026-10-03).
vop_file_africa <- file.path(fao_dir, "Value_of_Production_E_Africa.csv")
if (!file.exists(vop_file_africa)) stop("[0.4.0] missing ", vop_file_africa, " - stage the FAOSTAT QV Africa bulk file (same vintage as the Prices / Production files)")
.log040(sprintf("FAOStat GPV source: %s (mtime %s, %.0f MB)", basename(vop_file_africa), format(file.mtime(vop_file_africa), "%Y-%m-%d %H:%M"), file.size(vop_file_africa) / 1e6))

target_year <- 2019:2023   # match livestock 0.4.1 year_set y2021 + qaqc window
element <- "Gross Production Value (constant 2014-2016 thousand I$)"
.log040("loading FAOStat GPV (constant I$)")
prod_value_i <- fread(vop_file_africa, encoding = "Latin-1")
# B5 (2026-10-07): key on the FAOSTAT item CODE, not the item name. FAOSTAT renames items between
# releases, a renamed item stopped matching the mapping table's `name_fao_val`, and its value was
# dropped in silence - 59 of 127 composite-group codes, about 8 % of all-crop GPV. The name is kept
# only as a label, for logs and the vop_name_join_audit() diagnostic below. Keying on the code also
# subsumes the old `Maize` / `Maize (corn)` fold: both spellings carry item code 56.
.vop_code_col <- vop_item_code_col(prod_value_i)
cols <- c(.vop_code_col, "Item", "Element", "Area", "Area Code (M49)", paste0("Y", target_year))
prod_value_i <- prod_value_i[Element %in% element, ..cols]
data.table::setnames(prod_value_i, .vop_code_col, "item_code")
prod_value_i[, item_code := suppressWarnings(as.integer(item_code))]
prod_value_i[, M49 := as.numeric(gsub("[']", "", `Area Code (M49)`))]
prod_value_i[, iso3 := countrycode(sourcevar = M49, origin = "un", destination = "iso3c")]
prod_value_i <- prod_value_i[!is.na(iso3) & !Area %in% c("Ethiopia PDR", "Sudan (former)")]   # pre-split entities, as 0.4.2 / qaqc

y_cols <- grep("^Y\\d{4}$", names(prod_value_i), value = TRUE)
# One row per (iso3, item code). The label kept is the mapping table's spelling when FAOSTAT still
# uses it anywhere for this code, else FAOSTAT's own - so the audit below scores a code as "matched
# by name" exactly when the old name filter would have matched it.
.map_names <- unique(groups[, .(item_code, item)])
prod_value_i <- prod_value_i[!is.na(item_code),
  c(lapply(.SD, sum, na.rm = TRUE),
    .(Item = { mn <- .map_names$item[match(item_code[1], .map_names$item_code)]
               if (!is.na(mn) && mn %in% Item) mn else Item[1] })),
  by = .(iso3, item_code), .SDcols = y_cols]

# value = median across the window (thousand I$) — matches 0.4.1 vop_intd15 +
# qaqc_vop_vs_faostat.R so the crop QAQC validates to ~1. (The 92cb0b0 original
# used mean(2020:2022); realigned here for cross-commodity + QAQC consistency.)
prod_value_i[, value := apply(.SD, 1, median, na.rm = TRUE), .SDcols = y_cols]
prod_value_i <- prod_value_i[item_code %in% unique(groups$item_code) & is.finite(value) & value > 0, .(iso3, item_code, Item, value)]
.log040(sprintf("GPV rows (iso3 x item code, window median > 0): %d over %d item codes", nrow(prod_value_i), uniqueN(prod_value_i$item_code)))

# 3.0) B5 audit: what the code join recovers over the old name join ------------------
# Reported every run, on the live GPV table, so the 8 % claim is re-derived rather than asserted
# (handover 2026-10-07 §2 B5). Costs a couple of table ops.
.b5 <- vop_name_join_audit(prod_value_i, groups)
.log040(sprintf("B5 join audit: GPV matched by item CODE %.2f B I$ vs by NAME %.2f B I$ — recovered %.2f B I$ (%.1f%%) across %d renamed item codes",
                .b5$total$gpv_code / 1e6, .b5$total$gpv_name / 1e6, .b5$total$gpv_recovered / 1e6,
                100 * .b5$total$gpv_recovered / .b5$total$gpv_code, .b5$total$n_items_renamed))
if (nrow(.b5$renamed)) {
  cat("B5: groups by value recovered (thousand I$)\n"); print(.b5$by_group[gpv_recovered > 0], nrows = 50)
  cat("B5: item codes whose FAOSTAT name no longer matches metadata/SPAM2010_FAO_crops.csv\n"); print(.b5$renamed, nrows = 200)
}

# 3.1) FAOStat production (t), the coverage guard's denominator (#39) ---------------
.log040("loading FAOStat production (Africa) for the coverage guard")
fao_prod <- fread(prod_file, encoding = "Latin-1")[Element == "Production" & Unit == "t"]
.prod_code_col <- vop_item_code_col(fao_prod)
data.table::setnames(fao_prod, .prod_code_col, "item_code")
fao_prod[, item_code := suppressWarnings(as.integer(item_code))]
fao_prod[, M49 := as.numeric(gsub("[']", "", `Area Code (M49)`))]
fao_prod[, iso3 := countrycode(sourcevar = M49, origin = "un", destination = "iso3c", warn = FALSE)]
fao_prod <- fao_prod[!is.na(iso3) & !Area %in% c("Ethiopia PDR", "Sudan (former)")]
fao_prod <- fao_prod[item_code %in% unique(groups$item_code), c("iso3", "item_code", paste0("Y", target_year)), with = FALSE]
fao_prod <- fao_prod[, lapply(.SD, sum, na.rm = TRUE), by = .(iso3, item_code), .SDcols = paste0("Y", target_year)]
fao_prod[, prod_t := apply(.SD, 1, median, na.rm = TRUE), .SDcols = paste0("Y", target_year)]
fao_prod <- fao_prod[is.finite(prod_t) & prod_t > 0, .(iso3, item_code, prod_t)]
.log040(sprintf("FAO production rows (iso3 x item code, window median > 0): %d over %d item codes", nrow(fao_prod), uniqueN(fao_prod$item_code)))

# 3.2) FAOSTAT quantity pins (CAF / GIN coffee, 2026-10-07) -------------------
# A FAOSTAT production series can break without a real event. Coffee for the Central African
# Republic jumps from ~10 kt to ~300 kt after 2017 while the ICO puts real output at 2-6 kt, and
# Guinea has the same break. FAO's constant-I$ GPV is built from FAO's own production, so the break
# carries straight into the intld basis for those pairs - it was a documented caveat in
# docs/methods/nominal_price_method.md rather than a correction. Same discipline as the price pins:
# evidence and source in the CSV, nothing overridden silently, and only rows marked `applied` move a
# number (a `proposed` row is reported and ignored, so evidence can be reviewed first).
.qpin_file <- file.path(project_dir, "metadata", "fao_quantity_pins.csv")
if (file.exists(.qpin_file)) {
  .qpins <- fread(.qpin_file)
  .qp <- vop_apply_quantity_pins(prod_value_i, fao_prod, .qpins)
  prod_value_i <- .qp$gpv; fao_prod <- .qp$fao_prod
  if (nrow(.qp$log)) {
    .log040(sprintf("FAOSTAT quantity pins APPLIED to %d (iso3, item) pair(s):", nrow(.qp$log)))
    print(.qp$log[, .(iso3, item_code, prod_before = signif(prod_before, 4), prod_after = signif(prod_after, 4),
                      ratio = signif(ratio, 4), gpv_before = signif(gpv_before, 4), gpv_after = signif(gpv_after, 4),
                      scale_gpv, matched)])
    if (any(!.qp$log$matched)) stop("[0.4.0] a quantity pin matched no FAOSTAT row - check iso3 / item_code in ", basename(.qpin_file))
  }
  if (nrow(.qp$skipped)) {
    .log040(sprintf("FAOSTAT quantity pins NOT applied (status != 'applied' or no value): %d row(s) — recorded, not acted on",
                    nrow(.qp$skipped)))
    print(.qp$skipped[, .(iso3, item_code, item_label, prod_t, status, decided)])
  }
} else .log040(sprintf("no quantity pins file at %s — FAOSTAT quantities used as published", .qpin_file))

# 4) Distribute national GPV to SPAM production proportions ------------------
.log040("distributing FAO GPV by SPAM production share (per allocation group)")
spam_nat <- spam_prod_admin0_ex$all[, .(iso3, layer = as.character(Code), prod_t = prod)]
alloc <- vop_allocation_table(prod_value_i, fao_prod, spam_nat, groups, min_coverage = VOP_COVERAGE_MIN)
alloc <- alloc[iso3 %in% iso3_levels$iso3]            # FAO countries off this grid cannot be allocated
.outside <- alloc[reason == "country outside the SPAM release"]
.guarded <- alloc[guarded == TRUE & reason != "country outside the SPAM release"][order(-gpv)]
.unjudged <- alloc[reason == "no FAO production: coverage not judged"]
.covered_gpv <- alloc[reason != "country outside the SPAM release", sum(gpv)]
.log040(sprintf("allocation table: %d (iso3, group) pairs with GPV | outside the SPAM release: %d pairs in %d countries (%s), %.2f B I$, NA by design | inside: %.2f B I$, of which guarded %d pairs %.2f B I$ = %.1f%% (VOP_COVERAGE_MIN=%.2f) | %d allocated without a FAO production row (coverage not judged)",
                nrow(alloc), nrow(.outside), uniqueN(.outside$iso3), paste(sort(unique(.outside$iso3)), collapse = ","), sum(.outside$gpv) / 1e6,
                .covered_gpv / 1e6, nrow(.guarded), sum(.guarded$gpv) / 1e6, 100 * sum(.guarded$gpv) / .covered_gpv, VOP_COVERAGE_MIN, nrow(.unjudged)))
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

# 4.1) Constant-I$ price factor raster, I$ per tonne (#41, rebake item 8) ----
# The companion to 0.4.2's nominal price raster, and what makes R/3's prod_t tier usable: the tier
# carries hazard-affected TONNES, so money is applied afterwards as value = prod_t x factor and a
# price revision stops forcing a 0.4.x -> R/3 re-bake at both resolutions. Written on THIS run's
# allocation grid; run with EXPOSURE_RES=0.05 for the grid where pricing is correct, because a
# 0.25 deg border cell holds both countries' production but takes one country's factor.
.factor_rast <- vop_factor_rasters(admin_rast, iso3_levels, alloc, groups, spam_nat, layers = names(spam_dat$all))
.factor_rast <- .factor_rast[[sort(names(.factor_rast))]]
# Self-check, on the same footing as the allocation check below: the factor is the very multiplier
# vop_allocate_rasters() applied, so production x factor must reproduce the VoP raster cell by cell.
# If it does not, the physical tier and the value tier would disagree once money is applied.
.fc_layers <- intersect(names(.factor_rast), names(spam_vop_intd))
.fc_dev <- max(vapply(.fc_layers, function(ly) {
  d <- abs(spam_dat$all[[ly]] * .factor_rast[[ly]] / 1000 - spam_vop_intd[[ly]])
  .m <- terra::global(d, "max", na.rm = TRUE)[1, 1]
  if (is.finite(.m)) .m else 0
}, numeric(1)))
.fc_scale <- terra::global(spam_vop_intd, "max", na.rm = TRUE)[, 1]
.fc_scale <- max(c(.fc_scale[is.finite(.fc_scale)], 1))
if (.fc_dev / .fc_scale > 1e-6) stop(sprintf("[0.4.0] price factor does not reproduce the VoP raster: max abs deviation %.6g against a peak of %.6g over %d layers", .fc_dev, .fc_scale, length(.fc_layers)))
.log040(sprintf("price factor check: production x factor reproduces VoP to %.3g (peak %.3g) over %d layers", .fc_dev, .fc_scale, length(.fc_layers)))
# Name it for the grid it is ACTUALLY on, read off the raster, not for this run's EXPOSURE_RES.
# `alloc_grid` is pinned to 0.05 deg (L57) whatever EXPOSURE_RES says, because pricing has to happen
# on the fine grid - a 0.25 deg border cell belongs to one country but holds both countries'
# production. Tagging this file with the run's EXPOSURE_RES therefore produced a `_res-25.tif` that
# was byte-identical 0.05 deg data under a 0.25 deg name (caught on the node, 2026-10-07 A2). A
# filename is a claim like any other: derive the tag so it cannot drift from the content.
.fac_res <- terra::res(.factor_rast)[1]
.fac_tag <- sprintf("res-%02d", round(.fac_res * 100))
.factor_file <- file.path(mapspam_pro_dir, "fao_prices", paste0("crop_factor_intld15-2021-t_", .fac_tag, ".tif"))
terra::writeRaster(.factor_rast, .factor_file, overwrite = TRUE)
.log040(sprintf("constant-I$ price factor written: %s (%d layers, I$ per tonne, on the %.2f deg ALLOCATION grid regardless of EXPOSURE_RES=%s - price on the fine grid, aggregate after)",
                .factor_file, terra::nlyr(.factor_rast), .fac_res, .eg$tag))

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
  terra::writeRaster(round(resample_sum_checked(spam_vop_intd, base_rast, "0.4.0 all") * 1000, 1), save_file, overwrite = TRUE)   # thousand I$ -> I$, on the output grid
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
if (!file.exists(f_i) || overwrite_crop) terra::writeRaster(round(resample_sum_checked(spam_vop_intd_i, base_rast, "0.4.0 irr") * 1000, 1), f_i, overwrite = TRUE)
if (!file.exists(f_r) || overwrite_crop) terra::writeRaster(round(resample_sum_checked(spam_vop_intd_r, base_rast, "0.4.0 rf-all") * 1000, 1), f_r, overwrite = TRUE)

cat("\n===== 0.4.0_create_crop_vop_intld15.R COMPLETE at ",
    format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"), " =====\n", sep = "")
