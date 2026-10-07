# R/vop_allocate.R
# ================
# Allocation of national FAOSTAT gross production value onto SPAM pixels by production
# share, for R/0.4.0_create_crop_vop_intld15.R. Pure functions (terra + data.table), no I/O;
# 0.4.0 sources this file by path so a develop fix reaches the node run.
#
# Why this file exists (2026-10-01, issues #38 and #39, archive/dispatches/HANDOVER_2026-10-01_exposure-intld-fixes.md):
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
#   B5   The layer <-> item graph and both FAOSTAT joins keyed on the item NAME until 2026-10-07.
#        FAOSTAT renames items between releases ("Vegetables fresh nes" -> "Other vegetables,
#        fresh n.e.c."), and a renamed item simply stopped matching: its value was dropped in
#        silence, with no guard to catch it, because a missing item is indistinguishable from an
#        item the country does not grow. 59 of the 127 FAO codes in the composite groups
#        (ocer / ofib / ooil / opul / orts / rest / temf / trof / vege) had moved, about 8 % of
#        all-crop FAO GPV. Everything now keys on `code_fao`, the FAOSTAT item code, which is
#        stable across renames; names are carried only as labels for logs and diagnostics.
#        `vop_name_join_audit()` re-derives the old name join beside the code join so a run can
#        print exactly what the change recovered.
#
# Conservation property (checked by vop_check_totals): for every (country, group) that is not
# guarded, the zonal sum of the output over the group's layers equals the allocated GPV.

# FAOSTAT item CODES (not names): Bananas, Plantains and cooking bananas.
VOP_POOLED_ITEMS <- list(banpl = c(486L, 489L))
VOP_COVERAGE_MIN_DEFAULT <- 0.10

# The FAOSTAT bulk files spell the item-code column differently across releases. Returns the
# column name present, or NULL.
vop_item_code_col <- function(x, required = TRUE) {
  cand <- c("Item Code", "Item Code (FAO)", "ItemCode", "item_code")
  hit <- cand[cand %in% names(x)][1]
  if (is.na(hit)) {
    if (required) stop("vop_item_code_col: no FAOSTAT item-code column found (looked for ",
                       paste(cand, collapse = " / "), "); columns present: ", paste(names(x), collapse = ", "))
    return(NULL)
  }
  hit
}

# spam2fao: metadata/SPAM2010_FAO_crops.csv (short_spam2010, long_spam2010, code_fao, name_fao_val)
# layers:   SPAM raster layer names (long_spam2010, e.g. "pearl millet", "arabica coffee")
# pooled:   named list group -> FAO item CODES that are one group regardless of the mapping
# Returns data.table(layer, code, item_code, item, group): one row per (layer, item) edge. A group
# is a connected component of the layer <-> item-code graph (plus the pooled edges), named after
# its items' SPAM codes joined by "+" unless it is a pooled group, which keeps the pooled name.
# `item` is the mapping table's name for the code, kept for logging only - nothing joins on it.
vop_item_groups <- function(spam2fao, layers, pooled = VOP_POOLED_ITEMS) {
  m <- data.table::as.data.table(spam2fao)[, .(layer = long_spam2010, code = tolower(short_spam2010),
                                               item_code = suppressWarnings(as.integer(code_fao)),
                                               item = name_fao_val)]
  .nocode <- m[is.na(item_code) & layer %in% layers]
  if (nrow(.nocode)) warning("vop_item_groups: ", nrow(.nocode), " mapping row(s) have no usable code_fao and are dropped: ",
                             paste(unique(.nocode$layer), collapse = ", "))
  m <- m[!is.na(item_code) & layer %in% layers]
  m <- unique(m)
  if (!nrow(m)) stop("vop_item_groups: no SPAM layer matches the mapping table")
  # union-find over layers and item codes
  nodes <- unique(c(paste0("L:", m$layer), paste0("I:", m$item_code)))
  parent <- stats::setNames(nodes, nodes)
  find <- function(x) { while (parent[[x]] != x) x <- parent[[x]]; x }
  union <- function(a, b) { ra <- find(a); rb <- find(b); if (ra != rb) parent[[ra]] <<- rb }
  for (i in seq_len(nrow(m))) union(paste0("L:", m$layer[i]), paste0("I:", m$item_code[i]))
  for (g in names(pooled)) {
    its <- paste0("I:", pooled[[g]]); its <- its[its %in% nodes]
    if (length(its) > 1) for (j in 2:length(its)) union(its[1], its[j])
  }
  m[, root := vapply(paste0("L:", layer), find, character(1))]
  # name: pooled name if the component holds a pooled item code, else SPAM codes joined
  m[, group := {
    pn <- names(pooled)[vapply(pooled, function(ic) any(ic %in% item_code), logical(1))]
    if (length(pn)) pn[1] else paste(sort(unique(code)), collapse = "+")
  }, by = root]
  m[, root := NULL]
  data.table::setorder(m, group, layer, item_code)
  m[]
}

# gpv:       data.table iso3, item_code, value         (national GPV per FAO item, window median)
# fao_prod:  data.table iso3, item_code, prod_t or NULL (FAO national production per item, window median)
# spam_prod: data.table iso3, layer, prod_t             (SPAM national totals for the tech distributed)
# groups:    vop_item_groups()
# gpv / fao_prod join on `item_code`, never on the item name (B5): FAOSTAT renames items between
# releases and a name join drops the renamed ones silently.
# Returns one row per (iso3, group) with a GPV: gpv (sum over the group's items), n_items_valued,
# fao_prod_t (sum over items; NA if none), spam_prod_t (sum over layers; 0 if none), coverage =
# spam / fao, guarded (TRUE -> value_alloc NA) and reason in
#   {"ok", "no FAO production: coverage not judged", "SPAM has no production for the group",
#    "SPAM covers < min_coverage of FAO production", "country outside the SPAM release"}.
vop_allocation_table <- function(gpv, fao_prod, spam_prod, groups, min_coverage = VOP_COVERAGE_MIN_DEFAULT) {
  stopifnot(all(c("iso3", "item_code", "value") %in% names(gpv)), all(c("iso3", "layer", "prod_t") %in% names(spam_prod)))
  gi <- unique(groups[, .(item_code, group)])
  gl <- unique(groups[, .(layer, group)])
  g <- merge(data.table::as.data.table(gpv)[!is.na(value), .(iso3, item_code, value)], gi, by = "item_code")
  a <- g[, .(gpv = sum(value), n_items_valued = .N), by = .(iso3, group)]
  if (!is.null(fao_prod)) {
    stopifnot(all(c("iso3", "item_code", "prod_t") %in% names(fao_prod)))
    fp <- merge(data.table::as.data.table(fao_prod)[!is.na(prod_t), .(iso3, item_code, prod_t)], gi, by = "item_code")[, .(fao_prod_t = sum(prod_t)), by = .(iso3, group)]
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
  # a country with no SPAM production in ANY group is outside the release's footprint (SPAM 2020
  # Adaptation Atlas is SSA: North Africa is absent). Same outcome (NA), different reason, and it
  # must not count against the guard's share (cglabs Block B 2026-10-04: 104 of 130 guarded pairs)
  ctry <- data.table::as.data.table(spam_prod)[, .(spam_country_t = sum(prod_t, na.rm = TRUE)), by = iso3]
  a <- merge(a, ctry, by = "iso3", all.x = TRUE)
  a[is.na(spam_country_t) | spam_country_t <= 0, reason := "country outside the SPAM release"]
  a[, spam_country_t := NULL]
  a[, guarded := reason != "ok" & reason != "no FAO production: coverage not judged"]
  a[, value_alloc := data.table::fifelse(guarded, NA_real_, gpv)]
  data.table::setorder(a, iso3, group)
  a[]
}

# B5 diagnostic. Re-derives the OLD name join beside the live code join on the same GPV table, so a
# run can state what keying on `code_fao` recovered instead of asserting it. `gpv` needs iso3,
# item_code, value and the FAOSTAT item name in `Item`; `groups` is vop_item_groups().
# Returns list(by_group, renamed, total): per-group value under each key, the (code, FAOSTAT name,
# mapping name) rows whose names disagree, and the headline totals. Pure, cheap, no I/O.
vop_name_join_audit <- function(gpv, groups) {
  stopifnot(all(c("iso3", "item_code", "value") %in% names(gpv)))
  g <- data.table::as.data.table(gpv)[!is.na(value) & value > 0]
  if (!"Item" %in% names(g)) stop("vop_name_join_audit: gpv needs the FAOSTAT item name in `Item` to replay the name join")
  gi <- unique(groups[, .(item_code, group, item_mapping = item)])
  d <- merge(g[, .(iso3, item_code, item_faostat = Item, value)], gi, by = "item_code")
  # the old join matched only where the FAOSTAT name equalled the mapping table's name_fao_val
  d[, by_name := item_faostat == item_mapping]
  by_group <- d[, .(gpv_code = sum(value), gpv_name = sum(value[by_name]),
                    n_items = data.table::uniqueN(item_code),
                    n_items_renamed = data.table::uniqueN(item_code[!by_name])), by = group]
  by_group[, gpv_recovered := gpv_code - gpv_name]
  data.table::setorder(by_group, -gpv_recovered)
  renamed <- unique(d[by_name == FALSE, .(item_code, item_faostat, item_mapping, group)])
  data.table::setorder(renamed, group, item_code)
  list(by_group = by_group[], renamed = renamed[],
       total = list(gpv_code = sum(by_group$gpv_code), gpv_name = sum(by_group$gpv_name),
                    gpv_recovered = sum(by_group$gpv_recovered),
                    n_items_renamed = data.table::uniqueN(renamed$item_code)))
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

# Constant-I$ price factor, one layer per SPAM layer, in I$ per tonne (#41, 2026-10-07). This is
# the companion to 0.4.2's nominal price raster and the thing that makes the physical hazard tier
# usable: R/3's prod_t tier carries hazard-affected TONNES, so money is applied afterwards as
#   value = prod_t x factor
# and a price revision never again forces a 0.4.x -> R/3 re-bake at both resolutions.
#
# The factor is allocated GPV / SPAM national tonnage per (iso3, group), which is exactly the
# multiplier vop_allocate_rasters() applies - so prod x factor reproduces the VoP raster by
# construction, not by coincidence, and fixture_vop_allocate.R asserts it. It is constant across a
# group's layers because the value is distributed BY production share: the whole point of the group.
# Guarded pairs are NA here as they are there, so a guarded pair cannot be priced by the back door.
#
# Units: alloc$value_alloc is thousand I$, so the factor is multiplied by 1000 to come out in I$/t,
# matching the I$ units 0.4.0 writes its VoP raster in.
#
# GRID: build this on the 0.05 deg allocation grid. A 0.25 deg border cell belongs to one country
# but holds production from both sides, so pricing there charges one country's factor to the
# neighbour's tonnage (the BEN/NGA cowpea lesson). Multiply on 0.05 and sum-resample afterwards.
vop_factor_rasters <- function(admin_rast, zones, alloc, groups, spam_prod, layers = NULL) {
  zones <- data.table::as.data.table(zones)[, .(ID = as.numeric(ID), iso3)]
  gl <- unique(groups[, .(layer, group)])
  sp <- merge(data.table::as.data.table(spam_prod)[, .(iso3, layer, prod_t)], gl, by = "layer")[, .(spam_prod_t = sum(prod_t, na.rm = TRUE)), by = .(iso3, group)]
  cls <- function(tab) terra::classify(admin_rast, rcl = as.matrix(tab[, .(ID, v)]), others = NA)
  out <- list()
  for (g in sort(unique(alloc$group))) {
    f <- merge(alloc[group == g, .(iso3, value_alloc)], sp[group == g, .(iso3, spam_prod_t)], by = "iso3", all.x = TRUE)
    f <- merge(f, zones, by = "iso3")
    f[, v := data.table::fifelse(!is.na(value_alloc) & is.finite(spam_prod_t) & spam_prod_t > 0,
                                 value_alloc * 1000 / spam_prod_t, NA_real_)]
    r <- if (nrow(f[!is.na(v)])) cls(f[!is.na(v), .(ID, v)]) else terra::setValues(terra::rast(admin_rast), NA_real_)
    # the layer set must not depend on the data: R/3 and 0.4.4 address layers by name
    for (ly in gl[group == g, layer]) {
      if (!is.null(layers) && !ly %in% layers) next
      rl <- r; names(rl) <- ly; out[[ly]] <- rl
    }
  }
  if (!length(out)) stop("vop_factor_rasters: nothing to price")
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


# Country-table rasterisation shared by 0.4.0 and 0.4.2 (2026-10-05). Each cell goes to the polygon
# that contains its CENTRE; touches = TRUE only FILLS cells no centre claimed (coastal cells whose centre
# is offshore, islands smaller than a cell - the Seychelles, #40). Used on the 0.05 deg allocation grid.
# Why the allocation grid is 0.05 (cglabs Block C stop, 2026-10-05): a 0.25 deg cell holds SPAM
# production from both sides of a border but belongs to ONE country. Benin's border cells carry
# Nigerian cowpea: BEN SPAM cowpea is 5.8 kt at 0.05 deg and 25.1 kt at 0.25 deg (identical under the
# centre rule and touches = TRUE - the rule is not the cause, the coarse cell is), so the coverage guard
# answered differently on the two grids (0.043 vs 0.186). Allocate and price on 0.05, then sum-resample.
# v: SpatVector; grid: SpatRaster template; field: column to burn (numeric) or, if `labels` is
# given, an id column whose levels are attached from `labels` (data.frame ID, <label>).
rasterize_country <- function(v, grid, field, labels = NULL) {
  centre <- terra::rasterize(v, grid, field = field, touches = FALSE)
  touch  <- terra::rasterize(v, grid, field = field, touches = TRUE)
  r <- terra::cover(centre, touch)
  if (!is.null(labels)) levels(r) <- labels
  r
}

# Sum-resample a value raster to the output grid with a mass check (the issue #9 pattern). A no-op
# when the grids already match.
resample_sum_checked <- function(r, to, caller = "resample", tol = 0.005) {
  if (terra::compareGeom(r, to, stopOnError = FALSE)) return(r)
  src <- terra::global(r, "sum", na.rm = TRUE)[, 1]
  out <- terra::resample(r, to, method = "sum")
  dst <- terra::global(out, "sum", na.rm = TRUE)[, 1]
  dev <- abs(dst / src - 1); dev[!is.finite(dev)] <- 0
  if (any(dev > tol)) warning(sprintf("[%s] mass not conserved on resample: max dev %.3f%% (layer %s)", caller, 100 * max(dev), names(r)[which.max(dev)]))
  out
}
