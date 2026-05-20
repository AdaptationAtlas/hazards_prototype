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

# Set TRUE to upload the parquet + mapping CSV to S3 after building.
# Requires 0_server_setup.R to have been sourced (loads upload_files_to_s3).
# Default OFF after v5 refactor; flip to TRUE manually when ready to republish.
upload_to_s3 <- FALSE
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
  # Trade-domain rollups surfaced by the cross-domain integrity check.
  # These are cross-commodity sums (SITC-style classes), not real items.
  "^Cereals$", "^Cereals and Preparations$", "^Crops and livestock products$",
  "^Fats and Oils \\(excluding Butter\\)$", "^Food Excluding Fish$",
  "^Fruit and Vegetables$", "^Total Merchandise Trade$",
  "^Vegetable Oil and Fat$", "^Non-food$", "^Cereal preparations total$",
  "^Sugar and Honey$", "^Dairy Products and Eggs$", "^Dairy Products$",
  "^Meat and Meat Preparations$", "^Dairy Products, milk equivalent$",
  "^Fodder and Feeding Stuff$", "^Non-edible Crude Materials$",
  "^Alcoholic Beverages$", "^Beverages$", "^Tobacco$",
  # FBS / SUA meat-equivalent rollups (TM-only items 2071 / 2073 / 2074).
  # Bovine Meat = Cattle + Buffalo; Pigmeat (meat equivalent) = pig + prep'ns;
  # Poultry Meat = Chicken + Turkey + Duck + Goose. Including alongside the
  # per-species rows would double-count, so we drop the aggregates.
  "^Bovine Meat$", " \\(meat equivalent\\)$", "^Poultry Meat$",
  # Non-alcoholic / non-edible trade rollups that previously slipped past.
  "^Non-alcoholic Beverages$", "^Non-edible Fats and Oils$",
  # Residual "other" / n.e.c. / n.e.s. catchalls. The plain "nes" variant
  # (no periods) is not caught by the n.e.c. regex, so use a word-boundary
  # match to drop items like "Cake, oilseeds nes" and "Crude Materials nes".
  "^Other ", "n\\.e\\.c\\.", "\\bnes\\b",
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

# Load processed-to-raw mapping (Item-Code keyed) and apply include filter ####
# The mapping CSV is committed to the repo and keyed on FAO Item Code (the
# stable identifier that survives commodity_clean_map renames). Rows with
# include = FALSE are heuristically-processed items whose parent_raw could not
# be resolved at generation time; they stay in the CSV as an audit trail but
# are dropped from the parquet build. Synthetic rows added later (Spices
# combined, Other) have item_code = NA and are not affected by this filter.
mapping_path <- file.path(project_dir, "metadata", "faostat_processed_to_raw.csv")
if (!file.exists(mapping_path)) {
  stop("Mapping CSV not found: ", mapping_path,
       " — regenerate via R/misc/generate_faostat_processed_to_raw.R first.")
}
mapping <- fread(mapping_path)
mapping_active <- mapping[include == TRUE]
n_before_include <- nrow(fao_long)
fao_long <- fao_long[is.na(item_code) | item_code %in% mapping_active$item_code]
cat(sprintf(
  "Mapping include filter: dropped %d rows (%.1f%%) for include = FALSE items.\n",
  n_before_include - nrow(fao_long),
  100 * (n_before_include - nrow(fao_long)) / n_before_include
))

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
# v5: joins are item-code-keyed throughout, so they're stable against the
# commodity_clean_map renames downstream. keep_prod is commodity-level (after
# meat canonical rename) so we first map it back to (iso3, item_code) via
# the actual vop_intd15 rows to get the join key the mapping CSV exposes.
processed_export_vars <- c("export_quantity", "export_value", "export_value_usd15")
prod_codes_lookup <- unique(fao_long[
  variable == "vop_intd15" & value > 0,
  list(iso3, commodity, item_code)
])
keep_prod_item_codes <- unique(
  merge(prod_codes_lookup, keep_prod,
        by = c("iso3", "commodity"))[, list(iso3, item_code)]
)

mapping_processed <- mapping_active[!is.na(parent_raw_item_code)]
processed_mask <- fao_long$variable %in% processed_export_vars &
  fao_long$item_code %in% mapping_processed$item_code
n_before_gate <- sum(processed_mask)

if (n_before_gate > 0L) {
  processed_rows <- fao_long[processed_mask]
  processed_rows <- merge(
    processed_rows,
    mapping_processed[, list(item_code, parent_raw_item_code)],
    by = "item_code", all.x = TRUE
  )
  keep_processed <- processed_rows[
    keep_prod_item_codes[, list(iso3, parent_raw_item_code = item_code)],
    on = c("iso3", "parent_raw_item_code"),
    nomatch = NULL
  ]
  keep_processed[, parent_raw_item_code := NULL]
  fao_long <- rbindlist(
    list(fao_long[!processed_mask], keep_processed),
    use.names = TRUE, fill = TRUE
  )
  cat(sprintf(
    "Parent-mapping gate: kept %d / %d processed-export rows (%.1f%%); dropped %d rows.\n",
    nrow(keep_processed), n_before_gate,
    100 * nrow(keep_processed) / max(1L, n_before_gate),
    n_before_gate - nrow(keep_processed)
  ))
} else {
  cat("Parent-mapping gate: no processed-export rows present; nothing to gate.\n")
}

# type + parent_raw + parent_raw_item_code + commodity_class columns ####
# Annotate every kept row with raw/processed type, parent_raw (FAO Item
# string), parent_raw_item_code (stable identifier), and commodity_class.
# Mapping lookup is keyed on item_code so it survives the commodity_clean_map
# rename downstream. parent_raw / parent_raw_item_code are NA on raw rows;
# type = "raw" when parent_raw_item_code is NA, "processed" otherwise.
mi <- match(fao_long$item_code, mapping_active$item_code)
fao_long[, parent_raw           := mapping_active$parent_raw_item[mi]]
fao_long[, parent_raw_item_code := mapping_active$parent_raw_item_code[mi]]
fao_long[, commodity_class      := mapping_active$commodity_class[mi]]
fao_long[, commodity_group      := mapping_active$commodity_group[mi]]
fao_long[!nzchar(parent_raw), parent_raw := NA_character_]
fao_long[!nzchar(commodity_class), commodity_class := NA_character_]
fao_long[!nzchar(commodity_group), commodity_group := NA_character_]
fao_long[, type := ifelse(is.na(parent_raw_item_code), "raw", "processed")]
# Synthetic-row defaults for commodity_group (item_code is NA so the mapping
# lookup misses; set the group explicitly so the wide-form pivot has a key).
fao_long[commodity == "Spices, combined", commodity_group := "Spices"]

# Items with no mapping hit (NA item_code on the row, or item_code not in
# mapping_active) default to commodity_class = "crop"; the include filter
# upstream guarantees this only fires for synthetic rows in steady state.
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
  "type column: %d raw rows, %d processed rows.\n",
  sum(fao_long$type == "raw"), sum(fao_long$type == "processed")
))
cat(sprintf(
  "commodity_class column: %s\n",
  paste(sprintf("%s=%d",
                fao_long[, .N, by = commodity_class][order(-N)]$commodity_class,
                fao_long[, .N, by = commodity_class][order(-N)]$N),
        collapse = ", ")
))

# Schema invariant I-2: production / yield rows must have type = "raw".
# FAOSTAT QCL covers primary commodities only, so violations are a tripwire
# against accidental scope drift, not a heavy filter. If this fires the FAO
# source schema may have shifted and the mapping CSV needs follow-up.
violations <- fao_long[
  variable %in% c("production", "yield") & type != "raw",
  unique(.SD), .SDcols = c("iso3", "item_code", "commodity", "variable", "type")
]
if (nrow(violations) > 0L) {
  cat(sprintf(
    "WARNING: %d production/yield rows have type != 'raw'. Dropping them.\n",
    nrow(violations)
  ))
  print(head(violations, 10))
  fao_long <- fao_long[!(variable %in% c("production", "yield") & type != "raw")]
} else {
  cat("Invariant I-2 OK: all production/yield rows have type = 'raw'.\n")
}

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
  di <- match(dropped_rows$item_code, mapping_active$item_code)
  dropped_rows[, parent_raw_item_code := mapping_active$parent_raw_item_code[di]]
  dropped_rows[, parent_raw           := mapping_active$parent_raw_item[di]]
  dropped_rows[, commodity_group      := mapping_active$commodity_group[di]]
  dropped_rows[!nzchar(parent_raw),     parent_raw     := NA_character_]
  dropped_rows[!nzchar(commodity_group), commodity_group := NA_character_]
  dropped_rows[, type := ifelse(is.na(parent_raw_item_code), "raw", "processed")]

  summable_vars <- c(
    "production", "vop_usd15", "vop_intd15",
    "export_quantity", "export_value", "export_value_usd15",
    "import_quantity", "import_value", "import_value_usd15"
  )

  # Invariant I-2 extends to the Other bundle: exclude (production / yield)
  # rows on processed commodities so the synthetic Other rows never violate
  # the rule that production/yield must be type = "raw".
  dropped_rows_for_other <- dropped_rows[
    !(variable %in% c("production", "yield") & type != "raw")
  ]

  other_rows <- dropped_rows_for_other[
    variable %in% summable_vars,
    list(
      item_code = NA_integer_,
      commodity = "Other",
      atlas_name = NA_character_,
      parent_raw = NA_character_,
      parent_raw_item_code = NA_integer_,
      commodity_class = NA_character_,
      commodity_group = "Other",
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

# Keep item_code as a parquet column (v5). Stable FAOSTAT identifier; gives
# downstream consumers a robust join key against other FAO bulks. NA on
# synthetic "Spices, combined" and "Other" rows.
setcolorder(fao_long, c(
  "iso3", "item_code", "commodity", "commodity_group", "atlas_name", "type",
  "parent_raw", "parent_raw_item_code", "commodity_class",
  "year", "variable", "unit", "value"
))
setorder(fao_long, iso3, variable, commodity, year)

# Build-time sanity + integrity checks ####

# Yield sanity check: FAOSTAT QCL Yield values in our extract are in kg/ha.
# Typical African Maize yields are 1,000 - 10,000 kg/ha (1 - 10 t/ha). Halts
# the build if the median is outside this range - catches a 10x unit error
# (e.g. an hg/ha source slipping in unconverted) before a bad parquet ships.
yield_check <- fao_long[
  variable == "yield" & commodity == "Maize" & year >= max(year) - 5,
  list(median_yield = median(value, na.rm = TRUE), unit = first(unit))
]
expected_low <- 1000L
expected_high <- 10000L
if (nrow(yield_check) == 0L ||
    is.na(yield_check$median_yield) ||
    yield_check$median_yield < expected_low ||
    yield_check$median_yield > expected_high) {
  stop(sprintf(
    paste0("Yield sanity check FAILED: median Maize yield = %.0f %s (expected ",
           "%d - %d kg/ha for African crops). Possible unit error (hg/ha vs kg/ha)."),
    yield_check$median_yield, yield_check$unit,
    expected_low, expected_high
  ))
}
cat(sprintf(
  "Yield sanity OK: median Maize yield = %.0f %s (last 5 yrs, expected %d - %d kg/ha).\n",
  yield_check$median_yield, yield_check$unit, expected_low, expected_high
))

# Cross-domain integrity check: surfaces commodity-name drift between
# production (QCL) and trade (TM) rows. Logs (iso3, commodity) cells that
# exist in only one domain to a CSV next to the parquet. Doesn't halt the
# build; catches the next cattle-meat-style mismatch for follow-up curation.
#
# v5: adds a `reason` column. Meat commodities are present in trade but
# absent in production by design (the non-indigenous variant carries the
# trade flow, the indigenous variant carries production but is dropped from
# trade by FAOSTAT) - labelled "meat-by-design" so a reviewer can ignore
# them. Everything else is labelled "review".
meat_by_design <- c(
  "Cattle meat", "Sheep meat", "Goat meat", "Pig meat", "Chicken meat",
  "Buffalo meat", "Camel meat", "Horse meat",
  "Rabbit and hare meat", "Turkey meat"
)
prod_items <- unique(fao_long[variable == "production", list(iso3, commodity)])
trade_items <- unique(fao_long[
  variable %in% c("export_value", "import_value"),
  list(iso3, commodity)
])
production_only <- fsetdiff(prod_items, trade_items, all = FALSE)
trade_only <- fsetdiff(trade_items, prod_items, all = FALSE)
production_only[, `:=`(side = "production_only", reason = "review")]
trade_only[, `:=`(
  side = "trade_only",
  reason = ifelse(commodity %in% meat_by_design, "meat-by-design", "review")
)]
mismatch_path <- file.path(fao_dir, "integrity_check_mismatches.csv")
fwrite(
  rbind(
    production_only[, list(iso3, commodity, side, reason)],
    trade_only[, list(iso3, commodity, side, reason)]
  ),
  mismatch_path
)
cat(sprintf(
  "Integrity check: wrote %d production-only + %d trade-only (iso3, commodity) cells to %s\n",
  nrow(production_only), nrow(trade_only), mismatch_path
))
cat(sprintf(
  "  reason breakdown: review=%d, meat-by-design=%d\n",
  sum(production_only$reason == "review") + sum(trade_only$reason == "review"),
  sum(trade_only$reason == "meat-by-design")
))

# Write parquet with build manifest as key/value metadata ####
out_file <- file.path(fao_dir, "faostat_long.parquet")
build_meta <- list(
  schema_version = "v5",
  description = paste(
    "FAOSTAT long-form table for Africa: production, yield, 2014-16",
    "constant USD / I$ value of production, export quantity + value,",
    "import quantity + value, deflated export_value_usd15 +",
    "import_value_usd15 (constant 2014-2016 USD). Adds type,",
    "parent_raw, parent_raw_item_code, commodity_class, item_code columns",
    "and an Other row per (iso3, year, variable, type) bundling",
    "sub-threshold commodities.",
    "v5: switched commodity_class + parent_raw lookups to FAO Item-Code",
    "keys (fixes the prior commodity_clean_map / mapping CSV string-mismatch",
    "where 16 livestock species defaulted to crop class); added 16 livestock",
    "entries to the mapping CSV; added ~20 aggregate-rollup exclude patterns;",
    "added 'reason' column to integrity_check_mismatches.csv; surfaced",
    "item_code as a new parquet column; introduced 'include' column in the",
    "mapping CSV with strict no-parent = no-inclusion semantics for",
    "heuristically-processed items; enforced production/yield = 'raw' at",
    "build time."
  ),
  schema_columns = paste(
    "iso3 | item_code | commodity | commodity_group (simplified name; raw and",
    "all derived rows share the same group, preserving meat / milk distinction",
    "for livestock; e.g. Cattle meat + Cattle hides + Bovine meat dried -> ",
    "'Cattle meat', Raw milk of cattle -> 'Cattle milk') | atlas_name |",
    "type {raw, processed} | parent_raw (FAO Item of raw parent for processed",
    "items; NA otherwise) | parent_raw_item_code (FAO Item Code of raw parent;",
    "NA for raw items) | commodity_class {crop, livestock, byproduct} | year |",
    "variable | unit | value"
  ),
  aggregation_rules = paste(
    "Aggregation by parent_raw_item_code (or parent_raw) is valid for",
    "value-type variables ONLY: vop_usd15, vop_intd15, export_value,",
    "export_value_usd15, import_value, import_value_usd15. Do NOT aggregate",
    "across (raw, processed) for production, yield, export_quantity,",
    "import_quantity - the units do not combine meaningfully across",
    "transformation states (1 t cocoa beans + 1 t cocoa butter is",
    "physically meaningless)."
  ),
  mapping_csv = paste(
    "metadata/faostat_processed_to_raw.csv (Item-Code keyed; also published",
    "to S3 alongside the parquet for methodology reference)."
  ),
  source = paste(
    "FAOSTAT bulk downloads: Production_Crops_Livestock, Value_of_Production,",
    "Trade_CropsLivestock, Deflators."
  ),
  source_files = paste(
    unique(c(
      vapply(sources, function(s) basename(s$file), character(1)),
      "Deflators_E_All_Data_(Normalized).csv"
    )),
    collapse = "; "
  ),
  variables = paste(
    sprintf("%s = %s", names(sources), vapply(sources, function(s) s$element, character(1))),
    " | export_value_usd15 = export_value * (anchor 2014-2016 / GDP-Deflator-USD-2015-prices)",
    " | import_value_usd15 = same deflator applied to import_value",
    collapse = " | "
  ),
  deflator = paste(
    "FAOSTAT Deflators_E_All_Data_(Normalized).csv,",
    "Item Code 22024 'GDP Deflator', Element 'Value US$, 2015 prices' (code 6179).",
    "Per-Reporter country: anchor = mean(deflator over 2014-2016);",
    "deflator_to_usd15 = anchor / deflator_value_year;",
    "nominal trade value * deflator_to_usd15 = constant 2014-2016 USD."
  ),
  build_script = "R/0.4.5_create_faostat_long.R",
  build_time = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
  spam2fao_source = paste0(
    "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/",
    "main/metadata/SPAM2010_FAO_crops.csv"
  ),
  processed_to_raw_mapping = paste(
    "metadata/faostat_processed_to_raw.csv (committed in the repo) provides",
    "the item_code -> (parent_raw_item_code, parent_raw_item, commodity_class)",
    "lookup used by the parent-mapping gate, the type column, and the",
    "commodity_class column. Includes an 'include' bool column: rows with",
    "include = FALSE are heuristically-processed items whose parent could not",
    "be resolved at generation time; they are filtered out of the parquet",
    "build but stay in the CSV as an audit trail."
  ),
  meat_variant = paste(
    "Indigenous (live-trade-adjusted) variants kept for vop_*;",
    "non-indigenous variants kept for trade rows (physical meat flows).",
    "Pre-rename collapses both variants to the same canonical commodity",
    "name so they join cleanly across QV / QCL / TM."
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
      "Union-of-three relative filter: for each (iso3, commodity), the mean",
      "over the most recent %d calendar years of vop_intd15 OR export_value",
      "OR import_value must exceed %g of the country's summed mean for that",
      "variable; passing any one signal keeps all variable rows for the pair.",
      "Each variable uses its own per-variable max-year window."
    ),
    intd_window_years, intd_share_threshold
  ),
  parent_mapping_gate = paste(
    "After the union filter, processed-export rows (export_quantity,",
    "export_value, export_value_usd15) are gated against keep_prod: the row",
    "survives only if its parent_raw_item_code is in the (iso3, item_code)",
    "set of commodities that passed the production (vop_intd15) filter for",
    "the same country. v5 keys the join on FAO Item Codes throughout so",
    "commodity_clean_map renames downstream cannot break it. Stops re-",
    "exports of imported raw materials from polluting the climate-exposure",
    "view. Imports + raw exports + production rows are not gated."
  ),
  other_aggregation = paste(
    "Commodities dropped by the union filter are bundled into a single",
    "'Other' row per (iso3, year, variable, type) over summable variables",
    "(production, vop_usd15, vop_intd15, export_quantity, export_value,",
    "export_value_usd15, import_quantity, import_value, import_value_usd15).",
    "Yield is excluded (heterogeneous yields can't sum). Other rows have",
    "item_code = NA, atlas_name = NA, parent_raw = NA, parent_raw_item_code",
    "= NA, commodity_class = NA; their type tag preserves the raw vs",
    "processed split."
  ),
  yield_sanity_check = paste(
    "Build-time assertion: median Maize yield over the last 5 years must",
    "be in [1000, 10000] kg/ha. Halts the build on failure - catches the",
    "hg/ha vs kg/ha unit trap."
  ),
  integrity_check = paste(
    "Build writes integrity_check_mismatches.csv listing (iso3, commodity)",
    "cells present in only one of production / trade, with a `reason`",
    "column: 'meat-by-design' for cattle / sheep / goat / pig / chicken /",
    "buffalo / camel / horse / rabbit / turkey meat (where the indigenous",
    "production variant and the non-indigenous trade variant collapse to",
    "the same commodity), 'review' for everything else. Reviewer eyeballs",
    "reason='review' rows for follow-up curation."
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
factor_cols <- c("iso3", "commodity", "commodity_group", "atlas_name",
                 "type", "commodity_class", "variable", "unit")
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
# Uploads BOTH the parquet and the Item-Code-keyed mapping CSV. The CSV
# travels alongside the parquet as methodology reference: downstream users
# can reconstruct the raw / processed / commodity_class lookup without
# cloning the repo.
if (isTRUE(upload_to_s3)) {
  pacman::p_load(s3fs, paws.storage, progressr, progress, future, future.apply)
  if (!exists("upload_files_to_s3", mode = "function")) {
    stop("upload_files_to_s3() not found - run 0_server_setup.R first.")
  }
  upload_files_to_s3(
    files           = c(out_file, mapping_path),
    s3_file_names   = c(s3_file_name, "faostat_processed_to_raw.csv"),
    selected_bucket = s3_bucket,
    max_attempts    = 3,
    overwrite       = TRUE,
    mode            = "public-read"
  )
  cat("Uploaded to", s3_bucket, ":\n",
      "  -", s3_file_name, "\n",
      "  - faostat_processed_to_raw.csv\n")
}
