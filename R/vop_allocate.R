# R/vop_allocate.R
# ================
# Allocation of national FAOSTAT gross production value onto SPAM pixels by production
# share, for R/0.4.0_create_crop_vop_intld15.R. Pure functions (terra + data.table), no I/O;
# 0.4.0 sources this file by path so a develop fix reaches the node run.
#
# Why this file exists (2026-10-01, issues #38 and #39, HANDOVER_2026-10-01_exposure-intld-fixes.md):
#   #38  FAOSTAT has one item "Millet"; SPAM has pearl millet and small millet. 0.4.0 joined GPV
#        to SPAM codes by item name, only pearl millet matched, and the whole national Millet
#        value landed on pearl-millet pixels (KEN 27 M I$ against 6 kUSD nominal). Coffee had a
#        hand-written share split; millet did not. Here every FAO item is distributed over ALL
#        the SPAM layers it covers, by each pixel's share of the GROUP's national production -
#        coffee, millet and any future compound item alike. Items a SPAM crop spans (rapeseed =
#        "Rape or colza seed" + "Mustard seed") are summed into the group.
#   #39  Where SPAM holds almost nothing for a country x crop (Sudan, 0.05 Mt in SPAM 2020 SSA
#        against ~15 Mt in FAOSTAT; Nigeria banana, 1.5 kt against 7 Mt) the national value
#        was placed on a sliver of cells and every admin unit holding one inherited the whole
#        country's value. A coverage guard compares SPAM national tonnage with FAOSTAT
#        production per (country, group) and leaves the pair NA, with a logged reason, when
#        SPAM covers less than min_coverage of it. Nigeria's banana family is the pooled case:
#        FAOSTAT reports it all as "Bananas" (7.4 Mt, no Plantains item), SPAM codes it almost
#        all as plantain, so "Bananas" + "Plantains and cooking bananas" form ONE group split
#        by SPAM banana + plantain share (VOP_POOLED_ITEMS). The same classification mismatch
#        runs the other way in Uganda (FAO: plantain only).
#   +    terra::classify() leaves values that match no row of the reclass matrix UNCHANGED, so
#        the old value raster carried the admin ID itself (in thousand I$) for every country x
#        crop with no GPV row - a small spurious value on a one-sided pair. others = NA here.
#
# Conservation property (checked by vop_check_totals): for every (country, group) that is not
# guarded, the zonal sum of the output over the group's layers equals the allocated GPV.

VOP_POOLED_ITEMS <- list(banpl = c("Bananas", "Plantains and cooking bananas"))
VOP_COVERAGE_MIN_DEFAULT <- 0.10

# spam2fao: metadata/SPAM2010_FAO_crops.csv (short_spam2010, long_spam2010, name_fao_val)
# layers:   SPAM raster layer names (long_spam2010, e.g. "pearl millet", "arabica coffee")
# pooled:   named list group -> FAO items that are one group regardless of the mapping
# Returns data.table(layer, code, item, group): one row per (layer, item) edge. A group is a
# connected component of the layer <-> item graph (plus the pooled edges), named after its
# items' SPAM codes joined by "+" unless it is a pooled group, which keeps the pooled name.
vop_item_groups <- function(spam2fao, layers, pooled = VOP_POOLED_ITEMS) {
  m <- data.table::as.data.table(spam2fao)[, .(layer = long_spam2010, code = tolower(short_spam2010), item = name_fao_val)]
  m <- m[!is.na(item) & nzchar(item) & layer %in% layers]
  m <- unique(m)
  if (!nrow(m)) stop("vop_item_groups: no SPAM layer matches the mapping table")
  # union-find over layers and items
  nodes <- unique(c(paste0("L:", m$layer), paste0("I:", m$item)))
  parent <- stats::setNames(nodes, nodes)
  find <- function(x) { while (parent[[x]] != x) x <- parent[[x]]; x }
  union <- function(a, b) { ra <- find(a); rb <- find(b); if (ra != rb) parent[[ra]] <<- rb }
  for (i in seq_len(nrow(m))) union(paste0("L:", m$layer[i]), paste0("I:", m$item[i]))
  for (g in names(pooled)) {
    its <- paste0("I:", pooled[[g]]); its <- its[its %in% nodes]
    if (length(its) > 1) for (j in 2:length(its)) union(its[1], its[j])
  }
  m[, root := vapply(paste0("L:", layer), find, character(1))]
  # name: pooled name if the component holds a pooled item, else codes joined
  m[, group := {
    pn <- names(pooled)[vapply(pooled, function(its) any(its %in% item), logical(1))]
    if (length(pn)) pn[1] else paste(sort(unique(code)), collapse = "+")
  }, by = root]
  m[, root := NULL]
  data.table::setorder(m, group, layer, item)
  m[]
}

# gpv:       data.table iso3, Item, value         (national GPV per FAO item, window median)
# fao_prod:  data.table iso3, Item, prod_t or NULL (FAO national production per item, window median)
# spam_prod: data.table iso3, layer, prod_t        (SPAM national totals for the tech being distributed)
# groups:    vop_item_groups()
# Returns one row per (iso3, group) with a GPV: gpv (sum over the group's items), n_items_valued,
# fao_prod_t (sum over items; NA if none), spam_prod_t (sum over layers; 0 if none), coverage =
# spam / fao, guarded (TRUE -> value_alloc NA) and reason in
#   {"ok", "no FAO production: coverage not judged", "SPAM has no production for the group",
#    "SPAM covers < min_coverage of FAO production"}.
vop_allocation_table <- function(gpv, fao_prod, spam_prod, groups, min_coverage = VOP_COVERAGE_MIN_DEFAULT) {
  stopifnot(all(c("iso3", "Item", "value") %in% names(gpv)), all(c("iso3", "layer", "prod_t") %in% names(spam_prod)))
  gi <- unique(groups[, .(item, group)])
  gl <- unique(groups[, .(layer, group)])
  g <- merge(data.table::as.data.table(gpv)[!is.na(value), .(iso3, Item, value)], gi, by.x = "Item", by.y = "item")
  a <- g[, .(gpv = sum(value), n_items_valued = .N), by = .(iso3, group)]
  if (!is.null(fao_prod)) {
    fp <- merge(data.table::as.data.table(fao_prod)[!is.na(prod_t), .(iso3, Item, prod_t)], gi, by.x = "Item", by.y = "item")[, .(fao_prod_t = sum(prod_t)), by = .(iso3, group)]
    a <- merge(a, fp, by = c("iso3", "group"), all.x = TRUE)
  } else a[, fao_prod_t := NA_real_]
  sp <- merge(data.table::as.data.table(spam_prod)[, .(iso3, layer, prod_t)], gl, by = "layer")[, .(spam_prod_t = sum(prod_t, na.rm = TRUE)), by = .(iso3, group)]
  a <- merge(a, sp, by = c("iso3", "group"), all.x = TRUE)
  a[is.na(spam_prod_t), spam_prod_t := 0]
  a[, coverage := data.table::fifelse(!is.na(fao_prod_t) & fao_prod_t > 0, spam_prod_t / fao_prod_t, NA_real_)]
  a[, reason := "ok"]
  a[is.na(coverage), reason := "no FAO production: coverage not judged"]
  a[!is.na(coverage) & coverage < min_coverage, reason := sprintf("SPAM covers < %.0f%% of FAO production", 100 * min_coverage)]
  a[spam_prod_t <= 0, reason := "SPAM has no production for the group"]
  a[, guarded := reason != "ok" & reason != "no FAO production: coverage not judged"]
  a[, value_alloc := data.table::fifelse(guarded, NA_real_, gpv)]
  data.table::setorder(a, iso3, group)
  a[]
}

# spam_all:   SpatRaster, layers named as in groups$layer, already on admin_rast's grid
# admin_rast: SpatRaster of zone IDs (integer), zones: data.frame/data.table ID, iso3
# alloc:      vop_allocation_table(); spam_prod: the national totals alloc was built from
# Returns a SpatRaster with one layer per layer of every group in alloc (all NA when nothing
# was allocated to it anywhere):
#   pixel value = value_alloc(iso3, group) x pixel production / national GROUP production.
# NA where the country has no allocation (no GPV, or guarded) or no SPAM production.
vop_allocate_rasters <- function(spam_all, admin_rast, zones, alloc, groups, spam_prod) {
  zones <- data.table::as.data.table(zones)[, .(ID = as.numeric(ID), iso3)]
  gl <- unique(groups[, .(layer, group)])
  sp <- merge(data.table::as.data.table(spam_prod)[, .(iso3, layer, prod_t)], gl, by = "layer")[, .(spam_prod_t = sum(prod_t, na.rm = TRUE)), by = .(iso3, group)]
  cls <- function(tab) {   # ID -> value, everything else NA (NOT the ID itself)
    rcl <- as.matrix(tab[, .(ID, v)])
    terra::classify(admin_rast, rcl = rcl, others = NA)
  }
  out <- list()
  for (g in sort(unique(alloc$group))) {
    ag <- merge(alloc[group == g, .(iso3, value_alloc)], zones, by = "iso3")
    tg <- merge(sp[group == g, .(iso3, spam_prod_t)], zones, by = "iso3")
    # a group with nothing allocated anywhere still gets its layers, all NA: the layer set of
    # the output must not depend on the data (R/3 and 0.4.4 address layers by name)
    r_val <- if (nrow(ag[!is.na(value_alloc)])) cls(ag[!is.na(value_alloc), .(ID, v = value_alloc)]) else terra::setValues(terra::rast(admin_rast), NA_real_)
    r_tot <- if (nrow(tg[spam_prod_t > 0])) cls(tg[spam_prod_t > 0, .(ID, v = spam_prod_t)]) else terra::setValues(terra::rast(admin_rast), NA_real_)
    for (ly in gl[group == g, layer]) {
      if (!ly %in% names(spam_all)) next
      r <- spam_all[[ly]] / r_tot * r_val
      names(r) <- ly
      out[[ly]] <- r
    }
  }
  if (!length(out)) stop("vop_allocate_rasters: nothing allocated")
  terra::rast(out)
}

# Zonal check of the conservation property. Returns one row per (iso3, group) in alloc with
# the zonal sum over the group's layers, the allocated value and their ratio (NA when nothing
# was allocated). `ok` is TRUE when |ratio - 1| <= tol for allocated pairs and the zonal sum is
# 0 or NA for guarded ones.
vop_check_totals <- function(vop_rast, admin_rast, zones, alloc, groups, tol = 1e-6) {
  zones <- data.table::as.data.table(zones)[, .(ID = as.numeric(ID), iso3)]
  z <- data.table::as.data.table(terra::zonal(vop_rast, admin_rast, fun = "sum", na.rm = TRUE))
  idcol <- names(z)[1]
  z <- data.table::melt(z, id.vars = idcol, variable.name = "layer", value.name = "zonal_sum")
  if (is.numeric(z[[idcol]])) { data.table::setnames(z, idcol, "ID"); z <- merge(z, zones, by = "ID") } else data.table::setnames(z, idcol, "iso3")
  z <- merge(z, unique(groups[, .(layer, group)]), by = "layer")[, .(zonal_sum = sum(zonal_sum, na.rm = TRUE)), by = .(iso3, group)]
  chk <- merge(alloc[, .(iso3, group, value_alloc, guarded)], z, by = c("iso3", "group"), all.x = TRUE)
  chk[, ratio := zonal_sum / value_alloc]
  chk[, ok := data.table::fifelse(!is.na(value_alloc), is.finite(ratio) & abs(ratio - 1) <= tol, is.na(zonal_sum) | zonal_sum == 0)]
  chk[]
}
