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
# 2026-10-01 (price-method decision, HANDOVER_2026-10-01_exposure-intld-fixes.md item 3):
#   implied_price()              FAOSTAT gross production value (current thousand US$) /
#                                production -> USD/t. Where FAO publishes a producer price
#                                this EQUALS it (ratio 1.000 on 973 country-item-years); its
#                                extra content is FAO's imputation of prices it does not
#                                publish, so it covers 68 % of (country, crop) pairs and 81 %
#                                of production after the clip, against 30 % / 41 % for the
#                                producer-price file. Clipped against World GPV / World
#                                production per item-year, never against the producer-price
#                                world median (they differ per item: plantain 0.2x, tea 5x).
#   own_price_window()           the own value per (iso3, crop) for a year window: implied
#                                window median -> producer-price window median -> the same
#                                two over a longer series, with the source named.
#   apply_basis_guard()          within-item consistency: nominal (price x FAO production)
#                                over the country's constant-I$ GPV, against the item's
#                                cross-country median of that ratio. A country beyond
#                                [1/band, band] x the median carries a different price BASIS
#                                (auction green coffee vs cherry, tea leaf vs made tea,
#                                seed cotton vs lint, export parity vs farm gate), which no
#                                price source fixes; it falls back to the item-median factor
#                                x its own constant-I$ value, price_source = "basis fallback".
#
# 0.4.2 sources this file by path (project_dir), not from GitHub main like
# haz_functions.R, so a fix here reaches the node run. Pure functions, no I/O.

PRICE_BAND_DEFAULT <- 5
BASIS_BAND_DEFAULT <- 4      # Pete 2026-10-01: start at 4x the item's cross-country median
BASIS_MIN_N_DEFAULT <- 5     # an item median over fewer own-priced countries is not a reference
OWN_SOURCES <- c("fao gpv implied", "fao producer price",
                 "fao gpv implied longer-series", "fao producer price longer-series")

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


# gpv: gross production value in thousand currency units (FAOSTAT), prod: tonnes.
# USD/t where both are positive and finite, NA otherwise (a zero or missing production
# must not become an infinite or zero price that the clip would then judge).
implied_price <- function(gpv, prod, gpv_unit = 1000) {
  ok <- is.finite(gpv) & is.finite(prod) & gpv > 0 & prod > 0
  out <- rep(NA_real_, length(gpv))
  out[ok] <- gpv[ok] * gpv_unit / prod[ok]
  out
}

# implied, producer: data.tables iso3, atlas_name, year, price_usd (already clipped; NA allowed)
# years: the window; long_years: the longer series tried when the window has nothing.
# One row per (iso3, atlas_name) seen in either input, with the four candidate medians,
# the chosen `price_usd` and `price_source` in OWN_SOURCES (NA price -> source NA: the
# spatial fill chain names the source from here on).
own_price_window <- function(implied, producer, years, long_years = years) {
  stopifnot(all(years %in% long_years))
  med <- function(d, yrs, nm) {
    d[year %in% yrs & !is.na(price_usd), stats::setNames(list(stats::median(price_usd)), nm), by = .(iso3, atlas_name)]
  }
  parts <- list(med(implied, years, "price_usd_implied"), med(producer, years, "price_usd_producer"),
                med(implied, long_years, "price_usd_implied_long"), med(producer, long_years, "price_usd_producer_long"))
  grid <- unique(data.table::rbindlist(list(implied[, .(iso3, atlas_name)], producer[, .(iso3, atlas_name)])))
  out <- Reduce(function(a, b) merge(a, b, by = c("iso3", "atlas_name"), all.x = TRUE), parts, grid)
  out[, price_usd := price_usd_implied][, price_source := data.table::fifelse(!is.na(price_usd), OWN_SOURCES[1], NA_character_)]
  out[is.na(price_usd) & !is.na(price_usd_producer), `:=`(price_usd = price_usd_producer, price_source = OWN_SOURCES[2])]
  out[is.na(price_usd) & !is.na(price_usd_implied_long), `:=`(price_usd = price_usd_implied_long, price_source = OWN_SOURCES[3])]
  out[is.na(price_usd) & !is.na(price_usd_producer_long), `:=`(price_usd = price_usd_producer_long, price_source = OWN_SOURCES[4])]
  out[]
}

# data: one row per (iso3, atlas_name) after the fill chain, with value_field (USD/t, the
#       price that will be used), price_source, prod_field (FAO production, t) and
#       intld_field (FAO GPV constant I$, in units of intld_unit).
# The ratio price x prod / (intld x intld_unit) is a price-level-times-deflator factor: ~1-2
# for most food crops, and NOT expected to be 1. Within one item it should be similar across
# countries, because the constant-I$ GPV uses ONE international price per item; a country
# far from the item's median therefore has its own price on a different basis. Only rows
# whose price is an OWN value (guard_sources) are corrected: a fill is another country's
# basis already. Rows with a filled price outside the band are returned in `info_fills`
# for the log, uncorrected. Items with fewer than min_n own-priced countries are skipped.
# Note what the fallback price IS: FAO's constant-I$ GPV is production x ONE international
# price per item, so intld / production is the same for every country and the fallback
# reduces to (item median ratio) x (international price) - one consistent USD/t per item,
# country-invariant. Equivalently the guard keeps an own price only while it sits within
# band x the item's cross-country median own price. On the 2026-05-14 FAO files at 4x it
# moves 37 of 1,705 y2021 rows: high-side auction / product-form prices (KEN coffee 4,146,
# BDI and SLE tobacco 9,000 / 5,800) and Eritrea's exchange-rate highs, and low-side
# exchange-rate regimes (Angola on 8 crops, Sudan 3, Guinea 5) - see the dispatch's probe
# output for the list.
# Returns list(data, flagged, medians, info_fills).
apply_basis_guard <- function(data, value_field = "price_usd_final", prod_field = "production_t", intld_field = "value_intd15",
                              intld_unit = 1000, band = BASIS_BAND_DEFAULT, min_n = BASIS_MIN_N_DEFAULT,
                              guard_sources = OWN_SOURCES) {
  stopifnot(band > 1, all(c("iso3", "atlas_name", "price_source", value_field, prod_field, intld_field) %in% names(data)))
  d <- data.table::copy(data)
  d[, basis_ratio := get(value_field) * get(prod_field) / (get(intld_field) * intld_unit)]
  d[!is.finite(basis_ratio) | basis_ratio <= 0, basis_ratio := NA_real_]
  med <- d[price_source %in% guard_sources & !is.na(basis_ratio), .(basis_median = stats::median(basis_ratio), basis_n = .N), by = atlas_name]
  d <- merge(d, med, by = "atlas_name", all.x = TRUE)
  d[, basis_out := !is.na(basis_ratio) & !is.na(basis_median) & basis_n >= min_n & (basis_ratio > band * basis_median | basis_ratio < basis_median / band)]
  flagged <- d[basis_out & price_source %in% guard_sources,
               .(iso3, atlas_name, price_source, price_was = get(value_field), basis_ratio = signif(basis_ratio, 3), basis_median = signif(basis_median, 3), basis_n,
                 price_now = basis_median * get(intld_field) * intld_unit / get(prod_field))][order(atlas_name, -abs(log(basis_ratio / basis_median)))]
  info_fills <- d[basis_out & !price_source %in% guard_sources,
                  .(iso3, atlas_name, price_source, price = get(value_field), basis_ratio = signif(basis_ratio, 3), basis_median = signif(basis_median, 3))][order(atlas_name, iso3)]
  d[basis_out & price_source %in% guard_sources, `:=`(price_tmp__ = basis_median * get(intld_field) * intld_unit / get(prod_field), price_source = "basis fallback")]
  d[price_source == "basis fallback", (value_field) := price_tmp__]
  d[, c("price_tmp__", "basis_out", "basis_n") := NULL]
  list(data = d[], flagged = flagged, medians = med[order(atlas_name)], info_fills = info_fills)
}
