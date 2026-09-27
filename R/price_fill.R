# R/price_fill.R
# ==============
# Robust producer-price cleaning and gap-filling for R/0.4.2_create_crop_vop_nominal_usd.R.
#
# Why this file exists (2026-09-27). 0.4.2 filled missing FAOSTAT producer prices
# with add_nearby() from haz_functions.R: own price -> MEAN of neighbours -> MEAN of
# region (excluding self) -> MEAN of continent -> world median. Two things broke
# together after the FAO price files were refreshed (2026-05-15):
#   1. Zimbabwe wheat carried one currency-conversion artefact, 2022: 67,170 USD/t
#      (world ~290). With only two observations in the 2019-2023 window the median
#      was 33,790, i.e. 115x world, and it went straight into the raster.
#   2. mean() propagated it: Zambia and Botswana, with no own wheat price, inherited
#      ~11,700 USD/t as the "neighbours mean". Rwanda's oil palm fruit price (4,600
#      USD/t, 31x world) was the ONLY own price in East Africa, so the "region mean"
#      for 17 countries was Rwanda alone, and the continent mean carried it to the
#      rest at 8.9x.
# The republished exposure reference and the rebuilt hazard product both inherited
# this; the G6 value-drift gate caught it before publish (see
# DISPATCH_cglabs_r3_res25_rerun.md, 2026-09-27).
#
# What this does instead:
#   clip_prices_to_world_band()  drops any own observation whose ratio to the world
#                                median for that crop AND YEAR is outside [1/band, band]
#                                BEFORE any averaging, and returns the dropped rows so
#                                the run log shows them.
#   fill_price_robust()          neighbours -> region (excl. self) -> continent -> world,
#                                each a MEDIAN over countries' own (clipped) prices, with
#                                a `price_source` column saying which level was used.
#                                A single-country region is still that country, so the
#                                clip is the primary lever and the median the second.
#
# 0.4.2 sources this file by path (project_dir), not from GitHub main like
# haz_functions.R, so a fix here reaches the node run. Pure functions, no I/O.

PRICE_BAND_DEFAULT <- 5

# prices: data.table with iso3, atlas_name, year, price_usd (own observations, NA allowed)
# world:  data.table with atlas_name, year, price_usd (world-file observations; all
#         countries, or already the "World" aggregate) -> world median per crop-year
# Returns list(kept = prices with out-of-band price_usd set NA, dropped = the offending rows,
#              world_by_year = the medians used).
clip_prices_to_world_band <- function(prices, world, band = PRICE_BAND_DEFAULT) {
  stopifnot(all(c("iso3", "atlas_name", "year", "price_usd") %in% names(prices)),
            all(c("atlas_name", "year", "price_usd") %in% names(world)), band > 1)
  w <- world[!is.na(price_usd), .(price_world = stats::median(price_usd, na.rm = TRUE)), by = .(atlas_name, year)]
  p <- merge(data.table::copy(prices), w, by = c("atlas_name", "year"), all.x = TRUE)
  p[, ratio_world := price_usd / price_world]
  out <- !is.na(p$ratio_world) & (p$ratio_world > band | p$ratio_world < 1 / band)
  dropped <- p[out, .(iso3, atlas_name, year, price_usd, price_world, ratio_world = signif(ratio_world, 3))][order(-ratio_world)]
  p[out, price_usd := NA_real_]
  p[, c("price_world", "ratio_world") := NULL]
  list(kept = p, dropped = dropped, world_by_year = w)
}

# data:      one row per (iso3, atlas_name) with column `value_field` = own price (NA allowed)
# neighbors: named list iso3 -> character vector of neighbouring iso3
# regions:   named list region -> character vector of iso3
# world:     data.table atlas_name, price_world (one median per crop for the window); optional
# Adds <value_field>_neighbors / _region / _continent (medians of own prices), <value_field>_final,
# and price_source in {own, neighbours median, region median, continent median, world median, none}.
fill_price_robust <- function(data, value_field = "price_usd", group_field = "atlas_name",
                              neighbors, regions, world = NULL, stat = c("median", "mean")) {
  stat <- match.arg(stat)
  f <- if (stat == "median") function(x) if (all(is.na(x))) NA_real_ else stats::median(x, na.rm = TRUE)
       else function(x) if (all(is.na(x))) NA_real_ else mean(x, na.rm = TRUE)
  d <- data.table::copy(data)
  data.table::setnames(d, c(value_field, group_field), c("value", "group"))
  missing_iso <- d[!iso3 %in% unique(unlist(regions)), unique(iso3)]
  if (length(missing_iso)) stop("iso3 present in data but not in regions: ", paste(missing_iso, collapse = ","))
  own <- d[!is.na(value), .(iso3, group, value)]
  region_of <- data.table::rbindlist(lapply(names(regions), function(r) data.table::data.table(iso3 = regions[[r]], region = r)))
  region_of <- region_of[!duplicated(iso3)]
  own <- merge(own, region_of, by = "iso3", all.x = TRUE)
  d <- merge(d, region_of, by = "iso3", all.x = TRUE)
  # neighbours: median of the neighbours' OWN prices (never the country itself)
  d[, fill_neighbors := {
    nb <- neighbors[[iso3]]
    if (is.null(nb)) NA_real_ else f(own[group == .BY$group & iso3 %in% nb, value])
  }, by = .(iso3, group)]
  # region: median of the other members' own prices
  d[, fill_region := f(own[group == .BY$group & region == .BY$region & iso3 != .BY$iso3, value]), by = .(iso3, group, region)]
  # continent: median of every country's own price for the crop
  d[, fill_continent := f(own[group == .BY$group, value]), by = group]
  if (!is.null(world)) {
    w <- data.table::copy(world); data.table::setnames(w, c(group_field), "group", skip_absent = TRUE)
    d <- merge(d, w[, .(group, fill_world = price_world)], by = "group", all.x = TRUE)
  } else d[, fill_world := NA_real_]
  d[, price_source := data.table::fifelse(!is.na(value), "own",
                       data.table::fifelse(!is.na(fill_neighbors), "neighbours median",
                       data.table::fifelse(!is.na(fill_region), "region median",
                       data.table::fifelse(!is.na(fill_continent), "continent median",
                       data.table::fifelse(!is.na(fill_world), "world median", "none")))))]
  if (stat == "mean") d[, price_source := sub("median", "mean", price_source)]
  d[, fill_final := value]
  d[is.na(fill_final), fill_final := fill_neighbors]
  d[is.na(fill_final), fill_final := fill_region]
  d[is.na(fill_final), fill_final := fill_continent]
  d[is.na(fill_final), fill_final := fill_world]
  d[, region := NULL]
  data.table::setnames(d, c("value", "group", "fill_neighbors", "fill_region", "fill_continent", "fill_world", "fill_final"),
                       c(value_field, group_field, paste0(value_field, c("_neighbors", "_region", "_continent", "_global", "_final"))))
  d
}
