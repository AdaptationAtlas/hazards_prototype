# Build a long-form parquet of raw FAOSTAT values (Africa) covering production,
# yield, 2014-16 constant USD / I$ value of production, export quantity + value,
# and import quantity + value. Run 0_server_setup.R first (defines `fao_dir`).
# Variables whose source CSV is missing are skipped.

pacman::p_load(data.table, arrow, countrycode)

# Relevance filter: for each (iso3, commodity) take the last 5 calendar years
# of vop_intd15 and average. Drop the commodity (across all variables) when
# that mean is below this share of the country's summed mean across commodities.
intd_share_threshold <- 0.0025
intd_window_years <- 5

# Set TRUE to upload the parquet to S3 after building.
# Requires 0_server_setup.R to have been sourced (loads upload_files_to_s3).
upload_to_s3 <- TRUE
s3_bucket    <- "s3://digital-atlas/domain=socioeconomic/type=production/source=faostat/region=ssa"
s3_file_name <- "variable=adm0_faostat.parquet"

# Commodity exclusions (regex, case-insensitive): drop FAO aggregate rollups,
# residual "other" / n.e.c. catchalls, and a curated list of items not used by
# the atlas workflow. Applied to all variables.
exclude_patterns <- c(
  # Aggregate rollups (sums of constituent items)
  "^Agriculture$", "^Beef and Buffalo Meat, primary$", "^Cereals, primary$",
  "^Citrus Fruit, Total$", "^Crops$", "^Eggs Primary$",
  "^Fibre Crops, Fibre Equivalent$", "^Food$", "^Fruit Primary$", "^Livestock$",
  "^Meat indigenous, total$", "^Meat, Poultry$", "^Meat, Total$",
  "^Milk, Total$", "^Non Food$", "^Pulses, Total$", "^Roots and Tubers, Total$",
  "^Sheep and Goat Meat$", "^Sugar Crops Primary$", "^Treenuts, Total$",
  "^Vegetables and Fruit Primary$", "^Vegetables Primary$",
  # Residual "other" / n.e.c. catchalls
  "^Other ", "n\\.e\\.c\\.",
  # Animal fats, offal, hides
  "^Edible offal", ", unrendered$", "^Fat of ", "^Pig fat, rendered$",
  "^Raw hides and skins", "^Tallow$",
  # Dairy/eggs (specific items)
  "^Cheese", "^Cream, fresh$", "^Butter", "^Ghee", "^Buttermilk",
  "^Skim Milk & Buttermilk", "^Skim milk", "^Yoghurt$", "^Whole milk powder$",
  "^Whole milk, condensed$", "^Whole milk, evaporated$",
  "^Evaporated & Condensed Milk$", "whey",
  # Meat species dropped entirely from the atlas scope (no indigenous variant
  # in any domain). Cattle / sheep / goat / pig / chicken / buffalo / horse
  # are handled by vop_only_exclude_patterns below.
  "^Meat of asses", "^Meat of ducks", "^Meat of geese", "^Game meat",
  # Plant products (specific items)
  "^Oilcrops, ", "^Coir,", "^Chicory roots$", "^Jute,",
  "^Oil of maize$", "^Pyrethrum", "^Hop cones$", "^Peppermint",
  "^Onions and shallots, dry", "^Brazil nuts", "^Silk-worm", "^Raw silk",
  # Beverages / processed
  "^Wine$", "^Beer of barley", "^Margarine",
  # Other animal products
  "^Natural honey$", "^Beeswax$", "^Snails,", "^Shorn wool",
  # Misc
  "^Molasses$", "^Mushrooms"
)

# Non-trade exclusions: applied to production, yield, and vop_* rows only.
# FAOSTAT's non-indigenous meat items (e.g. "Meat of cattle with the bone,
# fresh or chilled") are raw-slaughter quantities that INCLUDE imported live
# animals slaughtered domestically. The indigenous variant (suffix "...
# (indigenous)") is live-trade-adjusted and is the right read for "national
# production" / "value of production from country X's livestock". The
# indigenous variant exists only in QV; for QCL (production / yield) we drop
# the non-indigenous variant entirely - cattle / sheep / goat / pig / chicken
# / buffalo will show VoP rows but no production-tonnes or yield rows. For
# the TM trade rows, the non-indigenous variant IS the physical meat traded
# (there is no indigenous trade variant in FAOSTAT) - keep those rows.
non_trade_meat_excludes <- c(
  "^Meat of .*fresh or chilled$", "^Horse meat, fresh or chilled$"
)
trade_vars <- c("export_quantity", "export_value", "import_quantity", "import_value")

# Spice items to combine into a single "Spices, combined" commodity ####
# Production and VoP are summed; yield is production-weighted across items.
spice_patterns <- c(
  "^Anise, badian", "^Cinnamon", "^Cloves", "^Nutmeg, mace, cardamoms",
  "[(]Piper spp", "^Ginger,", "^Vanilla,"
)

# Translation tables ####
spam2fao <- fread(
  "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SPAM2010_FAO_crops.csv"
)

# Atlas livestock-name -> FAOSTAT Item-name lookup. For meats we list BOTH
# the indigenous variant (lives in QV only) and the non-indigenous variant
# (lives in QCL + TM). Duplicate atlas keys are intentional - match() still
# returns the first matching index, and names()[idx] returns the canonical
# atlas name. After commodity_clean_map runs further down, both variants
# rename to the same canonical commodity name ("Cattle meat" etc.), so they
# join cleanly across domains.
lps2fao <- c(
  cattle_meat  = "Meat of cattle with the bone, fresh or chilled (indigenous)",
  cattle_meat  = "Meat of cattle with the bone, fresh or chilled",
  cattle_milk  = "Raw milk of cattle",
  pig_meat     = "Meat of pig with the bone, fresh or chilled (indigenous)",
  pig_meat     = "Meat of pig with the bone, fresh or chilled",
  poultry_eggs = "Hen eggs in shell, fresh",
  poultry_meat = "Meat of chickens, fresh or chilled (indigenous)",
  poultry_meat = "Meat of chickens, fresh or chilled",
  sheep_meat   = "Meat of sheep, fresh or chilled (indigenous)",
  sheep_meat   = "Meat of sheep, fresh or chilled",
  sheep_milk   = "Raw milk of sheep",
  goat_meat    = "Meat of goat, fresh or chilled (indigenous)",
  goat_meat    = "Meat of goat, fresh or chilled",
  goat_milk    = "Raw milk of goats"
)

# Source files & variable mapping ####
sources <- list(
  production = list(
    file    = file.path(fao_dir, "Production_Crops_Livestock_E_Africa_NOFLAG.csv"),
    element = "Production"
  ),
  yield = list(
    file    = file.path(fao_dir, "Production_Crops_Livestock_E_Africa_NOFLAG.csv"),
    element = "Yield"
  ),
  vop_usd15 = list(
    file    = file.path(fao_dir, "Value_of_Production_E_Africa.csv"),
    element = "Gross Production Value (constant 2014-2016 thousand US$)"
  ),
  vop_intd15 = list(
    file    = file.path(fao_dir, "Value_of_Production_E_Africa.csv"),
    element = "Gross Production Value (constant 2014-2016 thousand I$)"
  ),
  # Trade domain. Element strings are FAOSTAT-canonical lowercase. Each
  # element string covers multiple element codes (one per unit, e.g. "Export
  # quantity" covers tonnes + head counts); the `unit` column preserves the
  # distinction downstream, same as for production. Imports surfaced here too
  # so the notebook can frame the "dependence on imports" narrative; reach is
  # limited to commodities the country also produces (production-anchored
  # filter), so pure-import commodities (e.g. wheat in non-wheat-producing
  # countries) won't appear without a future filter relaxation.
  export_quantity = list(
    file    = file.path(fao_dir, "Trade_CropsLivestock_E_Africa_NOFLAG.csv"),
    element = "Export quantity"
  ),
  export_value = list(
    file    = file.path(fao_dir, "Trade_CropsLivestock_E_Africa_NOFLAG.csv"),
    element = "Export value"
  ),
  import_quantity = list(
    file    = file.path(fao_dir, "Trade_CropsLivestock_E_Africa_NOFLAG.csv"),
    element = "Import quantity"
  ),
  import_value = list(
    file    = file.path(fao_dir, "Trade_CropsLivestock_E_Africa_NOFLAG.csv"),
    element = "Import value"
  )
)

# Helper: load one (file, element) pair into long form ####
read_fao_long <- function(file, element, variable) {
  if (!file.exists(file)) {
    message("Skipping ", variable, ": file not found (", file, ")")
    return(NULL)
  }
  dt <- fread(file)
  dt <- dt[Element == element]
  if (nrow(dt) == 0) {
    message("Skipping ", variable, ": no rows match element '", element, "'")
    return(NULL)
  }
  year_cols <- grep("^Y\\d{4}$", names(dt), value = TRUE)
  keep <- c("Area Code (M49)", "Item Code", "Item", "Unit", year_cols)
  dt <- dt[, ..keep]
  dt_long <- melt(
    dt,
    id.vars = c("Area Code (M49)", "Item Code", "Item", "Unit"),
    variable.name = "year",
    value.name = "value",
    variable.factor = FALSE
  )
  dt_long[, year := as.integer(sub("^Y", "", year))]
  dt_long[, m49 := as.integer(gsub("[']", "", `Area Code (M49)`))]
  dt_long[, iso3 := countrycode(m49, origin = "un", destination = "iso3c", warn = FALSE)]
  dt_long <- dt_long[!is.na(iso3) & !is.na(value)]
  dt_long[, variable := variable]
  setnames(dt_long, c("Item Code", "Item", "Unit"), c("item_code", "commodity", "unit"))
  dt_long[, list(iso3, item_code, commodity, year, variable, unit, value)]
}

# Build long table ####
long_list <- lapply(names(sources), function(v) {
  read_fao_long(sources[[v]]$file, sources[[v]]$element, v)
})
fao_long <- rbindlist(long_list, use.names = TRUE, fill = TRUE)

if (nrow(fao_long) == 0) {
  stop("No FAOSTAT source files found in ", fao_dir, " — run 0_server_setup.R section 3.5 first.")
}

# Trade-value deflation ####
# Append export_value_usd15 and import_value_usd15: nominal *_value rows
# deflated to 2014-2016 constant USD basis, so they're directly comparable
# with vop_usd15. Uses FAOSTAT GDP Deflator (Item Code 22024, Element
# "Value US$, 2015 prices", index 2015 = 100) per Reporter country.
# Anchor = country's mean deflator over 2014-2016; deflator_to_usd15 =
# anchor / value_year. Multiplying nominal by that factor maps to the
# anchor-period basis.
#
# Original nominal export_value / import_value rows are KEPT alongside
# for backward compatibility.
deflators_file <- file.path(fao_dir, "Deflators_E_All_Data_(Normalized).csv")
if (file.exists(deflators_file)) {
  defl <- fread(deflators_file,
    select = c("Area Code (M49)", "Item Code", "Element", "Year", "Value")
  )
  defl <- defl[`Item Code` == 22024L & Element == "Value US$, 2015 prices"]
  defl[, m49 := as.integer(gsub("[']", "", `Area Code (M49)`))]
  defl[, iso3 := countrycode(m49, origin = "un", destination = "iso3c", warn = FALSE)]
  defl <- defl[!is.na(iso3) & !is.na(Value) & Value > 0]
  defl_anchor <- defl[Year %in% 2014:2016, list(anchor = mean(Value)), by = iso3]
  defl <- merge(defl, defl_anchor, by = "iso3")
  defl[, deflator_to_usd15 := anchor / Value]
  defl <- defl[, list(iso3, year = Year, deflator_to_usd15)]

  trade_nominal <- fao_long[variable %in% c("export_value", "import_value")]
  trade_def <- merge(trade_nominal, defl, by = c("iso3", "year"), all.x = TRUE)
  trade_def <- trade_def[!is.na(deflator_to_usd15)]
  trade_def[, value := value * deflator_to_usd15]
  trade_def[, variable := paste0(variable, "_usd15")]
  trade_def[, unit := "Thousand US$ (constant 2014-2016)"]
  trade_def[, deflator_to_usd15 := NULL]
  fao_long <- rbindlist(list(fao_long, trade_def), use.names = TRUE, fill = TRUE)
  cat(sprintf(
    "Deflation: appended %d export_value_usd15 + %d import_value_usd15 rows (anchor 2014-2016).\n",
    sum(trade_def$variable == "export_value_usd15"),
    sum(trade_def$variable == "import_value_usd15")
  ))
} else {
  cat(sprintf(
    "Deflation: %s not found - skipping; trade values stay nominal.\n",
    deflators_file
  ))
}

# Attach atlas_name (SPAM short crop name OR atlas livestock name).
# Where one FAO Item Code maps to multiple SPAM short names (e.g. coffee, millet),
# concatenate them with '|' so the long table stays one row per FAO record.
crop_map <- spam2fao[, list(atlas_name = paste(sort(unique(short_spam2010)), collapse = "|")),
  by = list(item_code = code_fao)
]
fao_long <- merge(fao_long, crop_map, by = "item_code", all.x = TRUE)
ls_idx <- match(fao_long$commodity, lps2fao)
fao_long[!is.na(ls_idx), atlas_name := names(lps2fao)[ls_idx[!is.na(ls_idx)]]]

# Filter: drop excluded commodities, zeros, and trace per country ####
rows_before <- nrow(fao_long)
exclude_regex <- paste(exclude_patterns, collapse = "|")
excluded_mask <- grepl(exclude_regex, fao_long$commodity, ignore.case = TRUE)

# Drop non-indigenous meat variants from all non-trade variables. For QV
# (vop_*) FAOSTAT publishes both variants; we keep only the indigenous one.
# For QCL (production / yield) only the non-indigenous variant exists, so
# this drop is total - those species end up with VoP rows but no production
# tonnes / yield rows, by design. Trade rows (export_* / import_*) keep the
# non-indigenous variant because it IS the physical meat traded; there is no
# indigenous-trade variant in FAOSTAT.
meat_regex <- paste(non_trade_meat_excludes, collapse = "|")
meat_excluded_mask <- grepl(meat_regex, fao_long$commodity, ignore.case = TRUE) &
  !(fao_long$variable %in% trade_vars)

dropped_commodities <- sort(unique(fao_long$commodity[excluded_mask | meat_excluded_mask]))
cat("Excluding", length(dropped_commodities), "commodities by name:\n")
cat(paste0("  - ", dropped_commodities, collapse = "\n"), "\n")
fao_long <- fao_long[!(excluded_mask | meat_excluded_mask)]
fao_long <- fao_long[value > 0]

# Combine spices into a single synthetic commodity ####
spice_regex <- paste(spice_patterns, collapse = "|")
is_spice <- grepl(spice_regex, fao_long$commodity)
spices <- fao_long[is_spice]
fao_long <- fao_long[!is_spice]
if (nrow(spices)) {
  prod_vop <- spices[variable != "yield",
    list(value = sum(value)),
    by = list(iso3, year, variable, unit)
  ]
  spice_prod <- spices[variable == "production", list(iso3, year, item_code, prod = value)]
  spice_yield <- merge(spices[variable == "yield"], spice_prod,
    by = c("iso3", "year", "item_code")
  )
  yield_combined <- spice_yield[, list(value = sum(prod) / sum(prod / value)),
    by = list(iso3, year, variable, unit)
  ]
  spices_combined <- rbindlist(list(prod_vop, yield_combined), use.names = TRUE)
  spices_combined[, `:=`(
    item_code  = NA_integer_,
    commodity  = "Spices, combined",
    atlas_name = "rest"
  )]
  fao_long <- rbindlist(list(fao_long, spices_combined), use.names = TRUE, fill = TRUE)
  cat("Combined", uniqueN(spices$commodity), "spice items ->",
    nrow(spices_combined), "'Spices, combined' rows\n")
}

# Pre-rename meat-species items to the canonical commodity name before the
# production-anchored filter below. QV uses the "(indigenous)" suffix; TM uses
# the bare "fresh or chilled" string. Without this pre-rename the filter
# (which keys on commodity) treats the two as different commodities and drops
# the TM trade rows for cattle / sheep / goat / pig / chicken / buffalo /
# horse because no (iso3, non-indigenous-name) tuple exists in keep_groups.
# The commodity_clean_map at the bottom of the script does the same rename;
# applying it here too makes the rename idempotent.
meat_canonical_rename <- c(
  "Meat of buffalo, fresh or chilled"                           = "Buffalo meat",
  "Meat of buffalo, fresh or chilled (indigenous)"              = "Buffalo meat",
  "Meat of cattle with the bone, fresh or chilled"              = "Cattle meat",
  "Meat of cattle with the bone, fresh or chilled (indigenous)" = "Cattle meat",
  "Meat of chickens, fresh or chilled"                          = "Chicken meat",
  "Meat of chickens, fresh or chilled (indigenous)"             = "Chicken meat",
  "Meat of goat, fresh or chilled"                              = "Goat meat",
  "Meat of goat, fresh or chilled (indigenous)"                 = "Goat meat",
  "Meat of pig with the bone, fresh or chilled"                 = "Pig meat",
  "Meat of pig with the bone, fresh or chilled (indigenous)"    = "Pig meat",
  "Meat of sheep, fresh or chilled"                             = "Sheep meat",
  "Meat of sheep, fresh or chilled (indigenous)"                = "Sheep meat",
  "Horse meat, fresh or chilled"                                = "Horse meat",
  "Horse meat, fresh or chilled (indigenous)"                   = "Horse meat"
)
meat_hits <- match(fao_long$commodity, names(meat_canonical_rename))
fao_long[!is.na(meat_hits),
  commodity := unname(meat_canonical_rename[meat_hits[!is.na(meat_hits)]])
]

# Union-of-three relative filter. Replaces the prior single-rule
# 0.25%-of-vop_intd15 filter. A commodity is kept for a country if it
# passes the relative threshold on ANY of the three signals: production
# value (vop_intd15), export value, or import value. Each signal uses its
# own most-recent `intd_window_years` calendar years so trade variables
# with shorter histories aren't penalised. Resolves CR-064 (d) — small
# producers with meaningful trade flows (e.g. Angola coffee) survive.
share_threshold <- intd_share_threshold
window_years <- intd_window_years
keep_for <- function(dt, variable_name) {
  v <- dt[variable == variable_name & value > 0]
  if (nrow(v) == 0L) {
    return(data.table(iso3 = character(), commodity = character()))
  }
  max_y <- max(v$year)
  win <- (max_y - window_years + 1L):max_y
  means <- v[year %in% win,
    list(mean_v = mean(value)),
    by = list(iso3, commodity)
  ]
  means[, country_total := sum(mean_v), by = iso3]
  means[mean_v > share_threshold * country_total, list(iso3, commodity)]
}
keep_prod <- keep_for(fao_long, "vop_intd15")
keep_exports <- keep_for(fao_long, "export_value")
keep_imports <- keep_for(fao_long, "import_value")
keep_groups <- unique(rbindlist(list(keep_prod, keep_exports, keep_imports)))
# Anti-join captures rows whose (iso3, commodity) is NOT in keep_groups. Used
# by the "Other" aggregation downstream to roll dropped commodities into a
# residual bundle per (iso3, year, variable, type) so a country's totals
# stay reconcilable.
dropped_rows <- fao_long[!keep_groups, on = c("iso3", "commodity")]
fao_long <- fao_long[keep_groups, on = c("iso3", "commodity")]
cat(sprintf(
  "Filter (production vop_intd15): kept %d (iso3, commodity) pairs across %d countries.\n",
  nrow(keep_prod), uniqueN(keep_prod$iso3)
))
cat(sprintf(
  "Filter (export_value):          kept %d pairs across %d countries.\n",
  nrow(keep_exports), uniqueN(keep_exports$iso3)
))
cat(sprintf(
  "Filter (import_value):          kept %d pairs across %d countries.\n",
  nrow(keep_imports), uniqueN(keep_imports$iso3)
))
cat(sprintf(
  "Filter (union of 3, window = last %d yr, share > %g): kept %d pairs across %d countries.\n",
  window_years, share_threshold, nrow(keep_groups), uniqueN(keep_groups$iso3)
))
cat(sprintf(
  "Long-table rows after filter: %d / %d (%.1f%%).\n",
  nrow(fao_long), rows_before, 100 * nrow(fao_long) / rows_before
))

# Parent-mapping gate for processed exports ####
# Drop processed-export rows whose RAW parent commodity didn't pass the
# production-anchored keep_prod set for the same country. Stops re-exports
# of imported raw materials (e.g. wheat flour from a non-wheat-producing
# country) from polluting the "climate exposure" view.
#
# Applies ONLY to processed export rows. Raw exports, imports (any form),
# and all production rows are unaffected -- importing wheat flour IS real
# climate exposure for the importer; refining your own raw cocoa beans is
# real production for the exporter.
#
# No-op if the mapping CSV is missing or empty.
mapping_path <- file.path(project_dir, "metadata", "faostat_processed_to_raw.csv")
processed_export_vars <- c("export_quantity", "export_value", "export_value_usd15")
if (file.exists(mapping_path)) {
  mapping <- fread(mapping_path)
  mapping <- mapping[!is.na(parent_raw_item) & nzchar(parent_raw_item)]

  processed_mask <- fao_long$variable %in% processed_export_vars &
    fao_long$commodity %in% mapping$processed_item
  n_before <- sum(processed_mask)

  if (n_before > 0L) {
    processed_rows <- fao_long[processed_mask]
    processed_rows <- merge(
      processed_rows,
      mapping[, list(commodity = processed_item, parent_raw_item)],
      by = "commodity", all.x = TRUE
    )
    keep_processed <- processed_rows[
      keep_prod[, list(iso3, parent_raw_item = commodity)],
      on = c("iso3", "parent_raw_item"),
      nomatch = NULL
    ]
    keep_processed[, parent_raw_item := NULL]
    fao_long <- rbindlist(
      list(fao_long[!processed_mask], keep_processed),
      use.names = TRUE, fill = TRUE
    )
    cat(sprintf(
      "Parent-mapping gate: kept %d / %d processed-export rows (%.1f%%); dropped %d rows.\n",
      nrow(keep_processed), n_before,
      100 * nrow(keep_processed) / max(1L, n_before),
      n_before - nrow(keep_processed)
    ))
  } else {
    cat("Parent-mapping gate: no processed-export rows present; nothing to gate.\n")
  }
} else {
  cat(sprintf(
    "Parent-mapping gate: %s not found - skipping gate.\n",
    mapping_path
  ))
  mapping <- data.table(processed_item = character(), parent_raw_item = character(),
                        commodity_class = character())
}

# type + parent_raw columns ####
# Annotate every kept row with raw/processed type + (for processed) the FAO
# Item string of its raw parent. NA parent_raw on raw rows. Driven by the
# same metadata/faostat_processed_to_raw.csv used by the gate above; the
# values here are read pre-rename so they match the FAO-canonical Item
# names, not the commodity_clean_map shortened forms. Downstream notebooks
# can group processed rows back to their raw parent by joining on
# parent_raw -> commodity within their own filter scope.
fao_long[, parent_raw := mapping$parent_raw_item[match(commodity, mapping$processed_item)]]
fao_long[is.na(parent_raw) | !nzchar(parent_raw), parent_raw := NA_character_]
fao_long[, type := ifelse(is.na(parent_raw), "raw", "processed")]
cat(sprintf(
  "type column: %d raw rows, %d processed rows.\n",
  sum(fao_long$type == "raw"), sum(fao_long$type == "processed")
))

# commodity_class column ####
# {"crop", "livestock", "byproduct"} per the mapping CSV. Looked up by
# matching the (pre-rename) commodity name against mapping$processed_item.
# Items without a class default to "crop" with a build-time warning so the
# long tail can be curated in the CSV.
fao_long[, commodity_class := mapping$commodity_class[match(commodity, mapping$processed_item)]]
fao_long[!nzchar(commodity_class), commodity_class := NA_character_]
unclassified <- fao_long[is.na(commodity_class), unique(commodity)]
if (length(unclassified) > 0L) {
  cat(sprintf(
    paste0("WARNING: commodity_class not set for %d commodities ",
           "- defaulting to 'crop'. ",
           "Curate metadata/faostat_processed_to_raw.csv:\n"),
    length(unclassified)
  ))
  cat(paste0("  - ", unclassified, collapse = "\n"), "\n")
  fao_long[is.na(commodity_class), commodity_class := "crop"]
}
cat(sprintf(
  "commodity_class column: %s\n",
  paste(sprintf("%s=%d",
                fao_long[, .N, by = commodity_class][order(-N)]$commodity_class,
                fao_long[, .N, by = commodity_class][order(-N)]$N),
        collapse = ", ")
))

# "Other" aggregation ####
# Bundle commodities dropped by the union-of-3 filter into a single "Other"
# row per (iso3, year, variable, type). Lets the notebook reconcile the
# kept-commodities total to the country's national total. Yield is excluded
# because heterogeneous yields can't sum.
#
# Other rows carry their own explicit type / parent_raw / commodity_class
# values (set in the by-aggregate below) and are appended AFTER the type
# and commodity_class blocks above, so those blocks never see "Other" as a
# commodity and won't overwrite its metadata.
if (nrow(dropped_rows) > 0L) {
  dropped_rows[, parent_raw := mapping$parent_raw_item[match(commodity, mapping$processed_item)]]
  dropped_rows[!nzchar(parent_raw), parent_raw := NA_character_]
  dropped_rows[, type := ifelse(is.na(parent_raw), "raw", "processed")]

  summable_vars <- c(
    "production", "vop_usd15", "vop_intd15",
    "export_quantity", "export_value", "export_value_usd15",
    "import_quantity", "import_value", "import_value_usd15"
  )

  other_rows <- dropped_rows[
    variable %in% summable_vars,
    list(
      commodity = "Other",
      atlas_name = NA_character_,
      parent_raw = NA_character_,
      commodity_class = NA_character_,
      unit = first(unit),
      value = sum(value, na.rm = TRUE)
    ),
    by = list(iso3, year, variable, type)
  ]
  other_rows <- other_rows[is.finite(value) & value > 0]

  if (nrow(other_rows) > 0L) {
    intd_share <- merge(
      other_rows[variable == "vop_intd15", list(other_intd = sum(value)),
        by = list(iso3, type)
      ],
      fao_long[variable == "vop_intd15", list(kept_intd = sum(value)),
        by = list(iso3)
      ],
      by = "iso3", all.x = TRUE
    )
    intd_share[, pct := 100 * other_intd / pmax(kept_intd + other_intd, 1)]
    intd_share <- intd_share[order(-pct)]
    cat("Other aggregation: top 10 (iso3, type) by Other share of vop_intd15:\n")
    print(intd_share[1:min(10L, .N)])
    high_share <- intd_share[pct > 30]
    if (nrow(high_share) > 0L) {
      cat(sprintf(
        paste0("WARNING: %d (iso3, type) cells have Other > 30%% of vop_intd15 ",
               "- filter may be too aggressive.\n"),
        nrow(high_share)
      ))
    }
  }

  fao_long <- rbindlist(list(fao_long, other_rows), use.names = TRUE, fill = TRUE)
  cat(sprintf(
    "Other aggregation: appended %d Other rows (%d raw + %d processed) across summable variables.\n",
    nrow(other_rows),
    sum(other_rows$type == "raw"),
    sum(other_rows$type == "processed")
  ))
} else {
  cat("Other aggregation: no dropped rows (filter kept everything); skipping.\n")
}

# Friendly commodity names ####
# Original FAO Item strings are still reconstructable via item_code.
commodity_clean_map <- c(
  "Abaca, manila hemp, raw"                                      = "Abaca",
  "Almonds, in shell"                                            = "Almonds",
  "Bambara beans, dry"                                           = "Bambara beans",
  "Broad beans and horse beans, dry"                             = "Broad beans, dry",
  "Broad beans and horse beans, green"                           = "Broad beans, green",
  "Cabbages"                                                     = "Cabbage",
  "Cantaloupes and other melons"                                 = "Melons",
  "Cashew nuts, in shell"                                        = "Cashew nuts",
  "Cashewapple"                                                  = "Cashew apple",
  "Cassava, fresh"                                               = "Cassava",
  "Cauliflowers and broccoli"                                    = "Cauliflower and broccoli",
  "Chick peas, dry"                                              = "Chickpeas",
  "Chillies and peppers, dry (Capsicum spp., Pimenta spp.), raw" = "Chillies and peppers, dry",
  "Chillies and peppers, green (Capsicum spp. and Pimenta spp.)" = "Chillies and peppers, green",
  "Cocoa beans"                                                  = "Cocoa",
  "Coconuts, in shell"                                           = "Coconuts",
  "Coffee, green"                                                = "Coffee",
  "Cow peas, dry"                                                = "Cowpeas",
  "Eggplants (aubergines)"                                       = "Eggplants",
  "Green corn (maize)"                                           = "Sweet corn",
  "Groundnuts, excluding shelled"                                = "Groundnuts",
  "Hen eggs in shell, fresh"                                     = "Hen eggs",
  "Horse meat, fresh or chilled (indigenous)"                    = "Horse meat",
  "Karite nuts (sheanuts)"                                       = "Shea nuts",
  "Leeks and other alliaceous vegetables"                        = "Leeks",
  "Lentils, dry"                                                 = "Lentils",
  "Lettuce and chicory"                                          = "Lettuce",
  "Maize (corn)"                                                 = "Maize",
  "Meat of buffalo, fresh or chilled (indigenous)"               = "Buffalo meat",
  "Meat of buffalo, fresh or chilled"                            = "Buffalo meat",
  "Meat of camels, fresh or chilled (indigenous)"                = "Camel meat",
  "Meat of cattle with the bone, fresh or chilled (indigenous)"  = "Cattle meat",
  "Meat of cattle with the bone, fresh or chilled"               = "Cattle meat",
  "Meat of chickens, fresh or chilled (indigenous)"              = "Chicken meat",
  "Meat of chickens, fresh or chilled"                           = "Chicken meat",
  "Meat of goat, fresh or chilled (indigenous)"                  = "Goat meat",
  "Meat of goat, fresh or chilled"                               = "Goat meat",
  "Meat of pig with the bone, fresh or chilled (indigenous)"     = "Pig meat",
  "Meat of pig with the bone, fresh or chilled"                  = "Pig meat",
  "Meat of rabbits and hares, fresh or chilled (indigenous)"     = "Rabbit and hare meat",
  "Meat of sheep, fresh or chilled (indigenous)"                 = "Sheep meat",
  "Meat of sheep, fresh or chilled"                              = "Sheep meat",
  "Meat of turkeys, fresh or chilled (indigenous)"               = "Turkey meat",
  "Melonseed"                                                    = "Melon seed",
  "Natural rubber in primary forms"                              = "Natural rubber",
  "Pigeon peas, dry"                                             = "Pigeon peas",
  "Pistachios, in shell"                                         = "Pistachios",
  "Plantains and cooking bananas"                                = "Plantains",
  "Plums and sloes"                                              = "Plums",
  "Pumpkins, squash and gourds"                                  = "Pumpkins and squash",
  "Rape or colza seed"                                           = "Rapeseed",
  "Raw milk of buffalo"                                          = "Buffalo milk",
  "Raw milk of camel"                                            = "Camel milk",
  "Raw milk of cattle"                                           = "Cattle milk",
  "Raw milk of goats"                                            = "Goat milk",
  "Raw milk of sheep"                                            = "Sheep milk",
  "Seed cotton, unginned"                                        = "Seed cotton",
  "Soya beans"                                                   = "Soybeans",
  "Spices, combined"                                             = "Spices",
  "Tangerines, mandarins, clementines"                           = "Tangerines and mandarins",
  "Tea leaves"                                                   = "Tea",
  "Unmanufactured tobacco"                                       = "Tobacco",
  "Walnuts, in shell"                                            = "Walnuts"
)
hits <- match(fao_long$commodity, names(commodity_clean_map))
fao_long[!is.na(hits), commodity := unname(commodity_clean_map[hits[!is.na(hits)]])]

# Drop item_code from the output schema (was only needed internally for
# spam2fao joins and spice aggregation).
fao_long[, item_code := NULL]

setcolorder(fao_long, c(
  "iso3", "commodity", "atlas_name", "type", "parent_raw", "commodity_class",
  "year", "variable", "unit", "value"
))
setorder(fao_long, iso3, variable, commodity, year)

# Write parquet with build manifest as key/value metadata ####
out_file <- file.path(fao_dir, "faostat_long.parquet")
build_meta <- list(
  description = paste(
    "FAOSTAT long-form table for Africa: production, yield, 2014-16",
    "constant USD / I$ value of production, export quantity + value,",
    "and import quantity + value."
  ),
  source = "FAOSTAT bulk downloads: Production_Crops_Livestock, Value_of_Production, Trade_CropsLivestock.",
  source_files = paste(
    unique(vapply(sources, function(s) basename(s$file), character(1))),
    collapse = "; "
  ),
  variables = paste(
    sprintf("%s = %s", names(sources), vapply(sources, function(s) s$element, character(1))),
    collapse = " | "
  ),
  build_script = "R/0.4.5_create_faostat_long.R",
  build_time = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
  spam2fao_source = paste0(
    "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/",
    "main/metadata/SPAM2010_FAO_crops.csv"
  ),
  meat_variant = paste(
    "Indigenous (live-trade-adjusted) variants kept;",
    "non-indigenous duplicates dropped."
  ),
  spice_combine = paste(
    "Anise / Cinnamon / Cloves / Nutmeg / Pepper (Piper) / Ginger / Vanilla",
    "aggregated into 'Spices' (sum for production and VoP;",
    "production-weighted mean for yield)."
  ),
  filter_exclusions = sprintf(
    paste(
      "Dropped %d commodities by regex: FAO aggregate rollups,",
      "'n.e.c.' / 'Other ...' catchalls, and curated items",
      "(offals, fats, hides, cheese, butter, ghee, cream, milk",
      "powders/condensed/evap, yoghurt, asses/duck/goose/game meat,",
      "oilcrops cake/oil eq, coir, chicory roots, jute, oil of maize,",
      "pyrethrum, hop cones, peppermint, dry onions, brazil nuts,",
      "silk-worm, raw silk, wine, beer of barley, margarine,",
      "natural honey, beeswax, snails, shorn wool, molasses, mushrooms)."
    ),
    length(dropped_commodities)
  ),
  filter_relevance = sprintf(
    paste(
      "Per (iso3, commodity), mean vop_intd15 over the last %d calendar",
      "years (%d-%d) must exceed %g of the country's summed mean across",
      "commodities; decision applies to all four variables."
    ),
    intd_window_years, min(window), max(window), intd_share_threshold
  ),
  commodity_rename = paste(
    "FAO Item strings rewritten to friendlier names where useful (e.g.",
    "'Meat of cattle with the bone, fresh or chilled (indigenous)' ->",
    "'Cattle meat')."
  ),
  atlas_name_rule = paste(
    "atlas_name is the SPAM2010 short crop code (via spam2fao;",
    "multiple SPAM names for one FAO item joined with '|') OR the",
    "atlas livestock label via lps2fao; NA if no mapping."
  ),
  rows        = as.character(nrow(fao_long)),
  countries   = as.character(uniqueN(fao_long$iso3)),
  commodities = as.character(uniqueN(fao_long$commodity)),
  year_range  = paste(range(fao_long$year), collapse = "-")
)
# Factor-encode repeated string columns so Arrow stores them as dictionary
# types (smaller files, no behaviour change apart from columns coming back
# as factors when read with arrow::read_parquet).
factor_cols <- c("iso3", "commodity", "atlas_name", "variable", "unit")
fao_long[, (factor_cols) := lapply(.SD, as.factor), .SDcols = factor_cols]

tbl <- arrow::arrow_table(fao_long)
tbl <- tbl$ReplaceSchemaMetadata(build_meta)
arrow::write_parquet(tbl, out_file, compression = "zstd", compression_level = 9)

cat(
  "Wrote", nrow(fao_long), "rows to", out_file, "\n",
  "Variables:", paste(unique(fao_long$variable), collapse = ", "), "\n",
  "Countries:", uniqueN(fao_long$iso3),
  "| Commodities:", uniqueN(fao_long$commodity),
  "| Years:", paste(range(fao_long$year), collapse = "-"), "\n",
  "Atlas-name match rate:",
  sprintf("%.1f%%", 100 * mean(!is.na(fao_long$atlas_name))), "\n"
)

# Optional S3 upload ####
if (isTRUE(upload_to_s3)) {
  pacman::p_load(s3fs, paws.storage, progressr, progress, future, future.apply)
  if (!exists("upload_files_to_s3", mode = "function")) {
    stop("upload_files_to_s3() not found - run 0_server_setup.R first.")
  }
  upload_files_to_s3(
    files           = out_file,
    s3_file_names   = s3_file_name,
    selected_bucket = s3_bucket,
    max_attempts    = 3,
    overwrite       = TRUE,
    mode            = "public-read"
  )
  cat("Uploaded to", file.path(s3_bucket, s3_file_name), "\n")
}
