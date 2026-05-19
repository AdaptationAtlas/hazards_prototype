# One-shot generator for metadata/faostat_processed_to_raw.csv.
#
# Reads the FAOSTAT Trade + Production bulk CSVs that 0_server_setup.R
# downloads to fao_dir, collects the unique FAO Item names, applies a set of
# regex / suffix heuristics to propose a parent_raw_item and commodity_class
# for each item, and writes the result to metadata/faostat_processed_to_raw.csv
# sorted by processed_item.
#
# Intended workflow: run this once (or whenever FAO publishes new items),
# review the CSV in git, manually curate any "unclassified" rows (commodity
# class blank), commit. The build script (R/0.4.5_create_faostat_long.R)
# reads this CSV at every run.
#
# Usage:
#   1. Make sure 0_server_setup.R has been run (so fao_dir + the bulk CSVs
#      exist).
#   2. Set fao_dir below if running outside the project bootstrap.
#   3. Rscript R/misc/generate_faostat_processed_to_raw.R
#   4. Eyeball metadata/faostat_processed_to_raw.csv, curate blanks, commit.

suppressMessages({
  library(data.table)
})

# Resolve fao_dir. If running inside a project session that sourced
# 0_server_setup.R, fao_dir is already defined. Otherwise default to the
# Mac local path so the script can be run standalone.
if (!exists("fao_dir") || !nzchar(fao_dir)) {
  fao_dir <- "/Users/pstewarda/Documents/rprojects/common_data/hazards_prototype/Data/fao"
}
if (!dir.exists(fao_dir)) {
  stop("fao_dir not found: ", fao_dir)
}

project_dir <- if (nzchar(Sys.getenv("project_dir"))) {
  Sys.getenv("project_dir")
} else {
  "/Users/pstewarda/Documents/rprojects/hazards_prototype"
}
out_path <- file.path(project_dir, "metadata", "faostat_processed_to_raw.csv")

cat("Reading FAO bulks from ", fao_dir, "\n", sep = "")
qcl <- fread(
  file.path(fao_dir, "Production_Crops_Livestock_E_Africa_NOFLAG.csv"),
  select = "Item"
)
tm <- fread(
  file.path(fao_dir, "Trade_CropsLivestock_E_Africa_NOFLAG.csv"),
  select = "Item"
)
items <- sort(unique(c(qcl$Item, tm$Item)))
cat(length(items), " unique Items across QCL + TM\n", sep = "")

# --- Livestock species + their products -----------------------------------
# Map each livestock derivative back to the canonical primary product the
# rest of the parquet uses ("Cattle meat", "Cattle milk", etc. via
# commodity_clean_map). We reference the FAOSTAT Item string here; the
# build script's commodity_clean_map renames it.
livestock_species <- c(
  "cattle", "buffalo", "sheep", "goat", "pig", "swine", "chicken",
  "chickens", "horse", "ass", "asses", "camel", "camels", "rabbit",
  "duck", "ducks", "goose", "geese", "turkey", "turkeys"
)
livestock_pattern <- paste(livestock_species, collapse = "|")

# --- Crop byproduct + processing suffixes ---------------------------------
# Each regex strips the suffix and returns the inferred parent.
heuristic_strip <- list(
  list(re = "^Flour of (.+)$",                 to = "\\1"),
  list(re = "^Flour, ?(.+)$",                  to = "\\1"),
  list(re = "^Bran of (.+)$",                  to = "\\1"),
  list(re = "^Husks of (.+)$",                 to = "\\1"),
  list(re = "^Cake of (.+)$",                  to = "\\1"),
  list(re = "^Meal of (.+)$",                  to = "\\1"),
  list(re = "^Oil of (.+)$",                   to = "\\1"),
  list(re = "(.+) [Oo]il, crude$",             to = "\\1"),
  list(re = "(.+) [Oo]il, refined$",           to = "\\1"),
  list(re = "(.+), refined$",                  to = "\\1, raw"),
  list(re = "(.+), roasted$",                  to = "\\1, green"),
  list(re = "^Sugar refined$",                 to = "Sugar cane"),
  list(re = "^Sugar Raw Centrifugal$",         to = "Sugar cane"),
  list(re = "^Molasses$",                      to = "Sugar cane"),
  list(re = "^Margarine.*$",                   to = "Soya beans")
)

# --- Per-species livestock derivative rules -------------------------------
# Returns parent_raw + class given a recognised livestock byproduct/derivative.
livestock_derivative <- function(item) {
  i <- tolower(item)
  # Primary meat items (and their indigenous variants) are RAW themselves.
  if (grepl("^meat of .*( with the bone)?(, fresh or chilled)?( \\(indigenous\\))?$", i)) {
    return(NULL)  # treat as raw, parent_raw = NA
  }
  # Milk = raw.
  if (grepl("^raw milk of ", i)) {
    return(NULL)
  }
  # Eggs = raw.
  if (grepl("^hen eggs in shell", i) || grepl("^eggs primary", i)) {
    return(NULL)
  }
  # Butter / cheese / cream / whey / yoghurt etc. -> parent species product.
  if (grepl("butter|cheese|cream|whey|yoghurt|condensed milk|evaporated|skim milk|whole milk|ghee|buttermilk",
            i)) {
    # Pull the species mentioned in the string.
    for (sp in livestock_species) {
      if (grepl(sp, i)) {
        sp_canonical <- switch(sp,
          chickens = "chicken",
          buffaloes = "buffalo",
          asses = "ass",
          camels = "camel",
          ducks  = "duck",
          geese  = "goose",
          turkeys = "turkey",
          sp
        )
        # Cattle byproducts -> "Raw milk of cattle".
        if (sp_canonical %in% c("cattle", "buffalo", "sheep", "goat", "camel")) {
          parent <- sprintf("Raw milk of %s", sp_canonical)
          if (sp_canonical == "sheep") parent <- "Raw milk of sheep"
          if (sp_canonical == "goat")  parent <- "Raw milk of goats"
          return(list(parent = parent, class = "byproduct"))
        }
      }
    }
    return(list(parent = NA_character_, class = "byproduct"))
  }
  # Hides, skins, wool -> parent species meat.
  if (grepl("hide|skin|wool|hair", i)) {
    for (sp in c("cattle", "buffalo", "sheep", "goat", "horse", "pig")) {
      if (grepl(sp, i)) {
        return(list(parent = sprintf("Meat of %s", sp), class = "byproduct"))
      }
    }
    return(list(parent = NA_character_, class = "byproduct"))
  }
  # Offal, fat, tallow -> parent species meat.
  if (grepl("offal|fat of|tallow|rendered|unrendered", i)) {
    for (sp in livestock_species) {
      if (grepl(sp, i)) {
        return(list(parent = sprintf("Meat of %s", sp), class = "byproduct"))
      }
    }
    return(list(parent = NA_character_, class = "byproduct"))
  }
  NULL
}

# --- Per-item classifier --------------------------------------------------
classify <- function(item) {
  # Skip clear aggregates / catchalls / n.e.c. — parquet build already excludes
  # them via exclude_patterns, but logging "blank" here keeps the CSV honest.
  if (grepl("^Other |n\\.e\\.c\\.", item)) {
    return(list(parent = "", class = ""))
  }
  # Livestock primary: items WHOSE Item string starts with "Meat of" /
  # "Raw milk of" / "Hen eggs" -> raw livestock item; parent NA, class livestock.
  if (grepl("^Meat of |^Raw milk of |^Hen eggs in shell|^Eggs Primary",
            item, ignore.case = TRUE)) {
    return(list(parent = NA_character_, class = "livestock"))
  }
  # Livestock derivative.
  ld <- livestock_derivative(item)
  if (!is.null(ld)) return(ld)
  # Live animals (cattle, sheep, etc. as a count) -> raw livestock.
  if (item %in% c("Cattle", "Buffalo", "Sheep", "Goats", "Swine / pigs",
                  "Chickens", "Camels", "Horses", "Asses", "Rabbits and hares",
                  "Ducks", "Geese", "Turkeys", "Poultry Birds",
                  "Sheep and Goats", "Cattle and Buffaloes")) {
    return(list(parent = NA_character_, class = "livestock"))
  }
  # Crop byproduct heuristics.
  for (h in heuristic_strip) {
    if (grepl(h$re, item)) {
      parent <- sub(h$re, h$to, item)
      # Capitalise first letter so the parent matches the FAO-canonical Item
      # casing (e.g. "barley" -> "Barley"). The commodity_clean_map in the
      # build script then renames "Barley" -> "Barley" (no-op for crops that
      # already use their canonical name).
      parent <- paste0(toupper(substr(parent, 1, 1)), substr(parent, 2, nchar(parent)))
      return(list(parent = parent, class = "byproduct"))
    }
  }
  # Default: assume raw crop.
  list(parent = NA_character_, class = "crop")
}

cat("Classifying ", length(items), " items...\n", sep = "")
out <- data.table(
  processed_item = items,
  parent_raw_item = NA_character_,
  commodity_class = NA_character_
)
for (k in seq_along(items)) {
  r <- classify(items[k])
  if (!is.null(r)) {
    out$parent_raw_item[k] <- r$parent
    out$commodity_class[k] <- r$class
  }
}

# Drop rows that were intentionally blanked (aggregate rollups; the build
# script's exclude_patterns will drop them anyway).
out <- out[!(parent_raw_item == "" & commodity_class == "")]

setorder(out, processed_item)
if (!dir.exists(dirname(out_path))) {
  dir.create(dirname(out_path), recursive = TRUE)
}
fwrite(out, out_path, na = "")
cat("wrote ", out_path, " (", nrow(out), " rows)\n", sep = "")

# Quick summary for the operator.
cat("\nBy class:\n")
print(out[, .N, by = commodity_class][order(-N)])
cat("\nProcessed items (have a parent_raw_item):\n")
print(out[!is.na(parent_raw_item) & nzchar(parent_raw_item), .N, by = commodity_class])
cat("\nFirst 20 processed items:\n")
print(out[!is.na(parent_raw_item) & nzchar(parent_raw_item)][1:20])
