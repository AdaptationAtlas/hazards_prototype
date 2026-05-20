# One-shot generator for metadata/faostat_processed_to_raw.csv.
#
# Reads the FAOSTAT Production_Crops_Livestock + Trade_CropsLivestock +
# Value_of_Production bulk CSVs, takes the union of (Item Code, Item), runs
# exclude_patterns from R/0.4.5_create_faostat_long.R to drop rollup
# categories, then applies suffix / species heuristics to propose a
# (parent_raw_item_code, parent_raw_item, commodity_class) tuple for each
# remaining Item. Every proposed parent code is validated against the set
# of real FAO Item Codes; unresolvable items are emitted with include = FALSE
# so a reviewer can grep for them and decide.
#
# Output columns (six):
#   item_code             int   (primary key; FAOSTAT stable identifier)
#   item                  char  (FAO Item string for human reference)
#   parent_raw_item_code  int   (NA for raw items)
#   parent_raw_item       char  (NA for raw items)
#   commodity_class       char  ("crop" / "livestock" / "byproduct")
#   include               bool  (TRUE = enters parquet; FALSE = audit-only)
#
# Workflow: run this once or whenever FAO publishes new items, then eyeball
# the CSV in git (especially include = FALSE rows + suspicious parents),
# curate by hand, commit. The build script reads this CSV at every run.
#
# Usage:
#   1. Make sure 0_server_setup.R has been run (so fao_dir + the bulk CSVs
#      exist).
#   2. Set project_dir and fao_dir below if running outside the project
#      bootstrap.
#   3. Rscript R/misc/generate_faostat_processed_to_raw.R
#   4. Review the printed include = FALSE list + curate as needed.

suppressMessages({
  library(data.table)
})

# 1) Setup ####
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

# 2) Load FAO bulks ####
cat("Reading FAO bulks from ", fao_dir, "\n", sep = "")
qcl <- fread(
  file.path(fao_dir, "Production_Crops_Livestock_E_Africa_NOFLAG.csv"),
  select = c("Item Code", "Item")
)
tm <- fread(
  file.path(fao_dir, "Trade_CropsLivestock_E_Africa_NOFLAG.csv"),
  select = c("Item Code", "Item")
)
qv <- fread(
  file.path(fao_dir, "Value_of_Production_E_Africa.csv"),
  select = c("Item Code", "Item")
)
items <- unique(rbind(qcl, tm, qv))
setnames(items, c("Item Code", "Item"), c("item_code", "item"))
items <- items[, list(item_code = as.integer(item_code), item)]
items <- unique(items)
setorder(items, item_code)
cat(nrow(items), " unique (item_code, item) tuples across QCL + TM + QV\n", sep = "")

# 3) Apply exclude_patterns from 0.4.5 to drop rollup categories ####
# Re-use the canonical exclude_patterns list so there's one source of truth
# for "what's a real FAO item." If 0.4.5 isn't sourcable in this environment
# (e.g. dependencies missing), fall back to a hard-coded copy.
exclude_patterns <- NULL
tryCatch(
  {
    # The 0.4.5 script defines exclude_patterns near the top; safer to extract
    # via parse than source() to avoid running the entire build.
    src <- readLines(file.path(project_dir, "R", "0.4.5_create_faostat_long.R"))
    start <- grep("^exclude_patterns <- c\\(", src)
    if (length(start) > 0L) {
      # Find the closing parenthesis.
      depth <- 0L
      end <- start[1]
      for (i in seq(start[1], length(src))) {
        depth <- depth +
          length(gregexpr("[(]", src[i])[[1]][gregexpr("[(]", src[i])[[1]] > 0]) -
          length(gregexpr("[)]", src[i])[[1]][gregexpr("[)]", src[i])[[1]] > 0])
        if (depth <= 0L) {
          end <- i
          break
        }
      }
      eval(parse(text = paste(src[start[1]:end], collapse = "\n")))
    }
  },
  error = function(e) message("Could not extract exclude_patterns from 0.4.5: ", conditionMessage(e))
)
if (is.null(exclude_patterns)) {
  message("Falling back to minimal exclude_patterns set.")
  exclude_patterns <- c("^Other ", "n\\.e\\.c\\.")
}
exclude_regex <- paste(exclude_patterns, collapse = "|")
items[, excluded := grepl(exclude_regex, item, ignore.case = TRUE)]
cat(sprintf("exclude_patterns dropped %d / %d items (rollups + curated exclusions).\n",
            sum(items$excluded), nrow(items)))
items_active <- items[!(excluded)]

# 4) Species -> primary product code lookups (resolved at runtime) ####
get_code <- function(pattern) {
  hits <- items_active[grepl(pattern, item, ignore.case = TRUE), item_code]
  if (length(hits) == 0L) {
    return(NA_integer_)
  }
  hits[1]
}
species_to_meat_code <- list(
  cattle  = get_code("^Meat of cattle.*\\(indigenous\\)$"),
  buffalo = get_code("^Meat of buffalo.*\\(indigenous\\)$"),
  sheep   = get_code("^Meat of sheep.*\\(indigenous\\)$"),
  goat    = get_code("^Meat of goat.*\\(indigenous\\)$"),
  pig     = get_code("^Meat of pig.*\\(indigenous\\)$"),
  chicken = get_code("^Meat of chickens.*\\(indigenous\\)$"),
  camel   = get_code("^Meat of camels.*\\(indigenous\\)$"),
  horse   = get_code("^Horse meat.*\\(indigenous\\)$"),
  rabbit  = get_code("^Meat of rabbits and hares.*\\(indigenous\\)$"),
  turkey  = get_code("^Meat of turkeys.*\\(indigenous\\)$")
)
species_to_milk_code <- list(
  cattle  = get_code("^Raw milk of cattle$"),
  buffalo = get_code("^Raw milk of buffalo$"),
  sheep   = get_code("^Raw milk of sheep$"),
  goat    = get_code("^Raw milk of goats$"),
  camel   = get_code("^Raw milk of camel$")
)
hen_egg_code <- get_code("^Hen eggs in shell, fresh$")

cat("Species lookup resolved:\n")
print(species_to_meat_code)
print(species_to_milk_code)
cat(sprintf("  hen_egg_code = %s\n", hen_egg_code))

# 5) Heuristic classifier ####
# Each helper returns either NULL (no match) or a list(parent_code, class).

# Detect a species token in an item string. Returns canonical species name
# (cattle / sheep / etc.) or NA.
detect_species <- function(item) {
  i <- tolower(item)
  if (grepl("of cattle\\b|^cattle\\b|^bovine", i)) return("cattle")
  if (grepl("of buffalo|^buffalo", i)) return("buffalo")
  if (grepl("of sheep|^sheep", i)) return("sheep")
  if (grepl("of goat|^goat|^goats", i)) return("goat")
  if (grepl("of pig|^pig|^swine|^pork", i)) return("pig")
  if (grepl("of chickens?|chicken meat", i)) return("chicken")
  if (grepl("of camels?", i)) return("camel")
  if (grepl("horse meat|^horse", i)) return("horse")
  if (grepl("of rabbits?", i)) return("rabbit")
  if (grepl("of turkeys?", i)) return("turkey")
  NA_character_
}

classify_livestock_derivative <- function(item) {
  i <- tolower(item)
  # Eggs derivatives.
  if (grepl("^eggs,? (dried|liquid|powder)", i)) {
    return(list(parent_code = hen_egg_code, class = "livestock"))
  }
  # Milk products (cheese, butter, ghee, yoghurt, buttermilk, cream, whey,
  # powdered milk, condensed/evaporated milk).
  if (grepl("butter|cheese|cream|whey|yoghurt|buttermilk|ghee", i) ||
      grepl("milk.*(powder|condensed|evaporated|skim|dry|dried)", i)) {
    sp <- detect_species(item)
    if (!is.na(sp) && !is.null(species_to_milk_code[[sp]])) {
      return(list(parent_code = species_to_milk_code[[sp]], class = "livestock"))
    }
    return(list(parent_code = NA_integer_, class = "livestock"))
  }
  # Offal / fat / tallow / hide / skin / wool / hair / sausage / cured meat.
  if (grepl("offal|fat of|tallow|rendered|unrendered|hide|skin|wool|hair|sausage|salted|smoked|cured|preparation|boneless",
            i)) {
    sp <- detect_species(item)
    if (!is.na(sp) && !is.null(species_to_meat_code[[sp]])) {
      return(list(parent_code = species_to_meat_code[[sp]], class = "livestock"))
    }
    return(list(parent_code = NA_integer_, class = "livestock"))
  }
  NULL
}

# Crop byproduct patterns: regex applied to the Item string; capture group 1
# is the parent crop name (which we then reverse-lookup against item codes).
crop_byproduct_patterns <- list(
  list(re = "^Flour of (.+)$",        cap = 1),
  list(re = "^Bran of (.+)$",         cap = 1),
  list(re = "^Husks of (.+)$",        cap = 1),
  list(re = "^Cake of (.+)$",         cap = 1),
  list(re = "^Meal of (.+)$",         cap = 1),
  list(re = "^Pellets of (.+)$",      cap = 1),
  list(re = "^Oil of (.+)$",          cap = 1),
  list(re = "^Oilcakes? of (.+)$",    cap = 1),
  list(re = "^(.+) [Oo]il, crude$",   cap = 1),
  list(re = "^(.+) [Oo]il, refined$", cap = 1),
  list(re = "^(.+), dried$",          cap = 1),
  list(re = "^(.+), shelled$",        cap = 1),
  list(re = "^(.+) [Jj]uice",         cap = 1),
  list(re = "^(.+), concentrated$",   cap = 1),
  list(re = "^Beer of (.+)$",         cap = 1)
)

# Reverse-lookup: given a candidate parent name (possibly lowercased / stripped),
# find a matching item_code in the FAO bulks. Returns NA_integer_ if no match.
# Strategy: try exact match first, then case-insensitive, then prefix match,
# then a "starts with" match. Returns the first hit.
find_parent_code <- function(parent_guess) {
  if (is.na(parent_guess) || !nzchar(parent_guess)) {
    return(NA_integer_)
  }
  # Title-case first letter for FAO conventions ("barley" -> "Barley").
  pg <- paste0(toupper(substr(parent_guess, 1, 1)),
               substr(parent_guess, 2, nchar(parent_guess)))
  # Exact match.
  hit <- items_active[item == pg, item_code]
  if (length(hit)) return(hit[1])
  # Case-insensitive.
  hit <- items_active[tolower(item) == tolower(pg), item_code]
  if (length(hit)) return(hit[1])
  # FAO often suffixes; try prefix match ("Barley" -> "Barley, pearled" etc.
  # NO -- we want the RAW item, not another byproduct). Match "<pg>" exactly
  # or "<pg>, fresh" or "<pg>, in shell" or "<pg>, raw".
  candidates <- c(pg, paste0(pg, ", fresh"), paste0(pg, ", in shell"),
                  paste0(pg, ", raw"), paste0(pg, " (corn)"),
                  paste0(pg, ", unginned"))
  for (c in candidates) {
    hit <- items_active[item == c, item_code]
    if (length(hit)) return(hit[1])
  }
  # Loose contains-match against the start of an item name.
  hit <- items_active[startsWith(tolower(item), tolower(pg)), item_code]
  if (length(hit)) return(hit[1])
  NA_integer_
}

# Per-item classifier returning (parent_code, parent_item, class, is_processed).
classify_item <- function(item_str) {
  # Livestock raw (Meat of X / Raw milk of X / Hen eggs).
  if (grepl("^Meat of |^Raw milk of |^Hen eggs in shell|^Horse meat",
            item_str, ignore.case = TRUE)) {
    return(list(parent_code = NA_integer_, class = "livestock", is_processed = FALSE))
  }
  # Live animals -> raw livestock.
  live_animals <- c(
    "Cattle", "Buffalo", "Sheep", "Goats", "Swine / pigs", "Chickens",
    "Camels", "Horses", "Asses", "Rabbits and hares", "Ducks", "Geese",
    "Turkeys", "Poultry Birds", "Sheep and Goats", "Cattle and Buffaloes"
  )
  if (item_str %in% live_animals) {
    return(list(parent_code = NA_integer_, class = "livestock", is_processed = FALSE))
  }
  # Livestock derivatives (butter / cheese / offal / hides / etc.).
  ld <- classify_livestock_derivative(item_str)
  if (!is.null(ld)) {
    return(list(parent_code = ld$parent_code,
                class       = ld$class,
                is_processed = TRUE))
  }
  # Sugar-related processed items.
  if (grepl("^Sugar refined$|^Sugar Raw Centrifugal$|^Molasses$|^Cane sugar, non-centrifugal$|^Refined sugar$",
            item_str)) {
    return(list(parent_code = find_parent_code("Sugar cane"),
                class = "byproduct", is_processed = TRUE))
  }
  # Margarine -> oil-crop parent (use Soya beans as a conventional default).
  if (grepl("^Margarine", item_str)) {
    return(list(parent_code = find_parent_code("Soya beans"),
                class = "byproduct", is_processed = TRUE))
  }
  # Coffee derivatives.
  if (grepl("^Coffee extracts|^Coffee substitutes|^Coffee, decaffeinated or roasted$|^Coffee, roasted",
            item_str)) {
    return(list(parent_code = find_parent_code("Coffee, green"),
                class = "byproduct", is_processed = TRUE))
  }
  # Cocoa derivatives.
  if (grepl("^Cocoa butter|^Cocoa paste|^Cocoa powder|^Chocolate",
            item_str)) {
    return(list(parent_code = find_parent_code("Cocoa beans"),
                class = "byproduct", is_processed = TRUE))
  }
  # Crop byproduct heuristics.
  for (h in crop_byproduct_patterns) {
    if (grepl(h$re, item_str)) {
      parent_guess <- sub(h$re, paste0("\\", h$cap), item_str)
      parent_code <- find_parent_code(parent_guess)
      return(list(parent_code = parent_code,
                  class       = "byproduct",
                  is_processed = TRUE))
    }
  }
  # Default: assume raw crop.
  list(parent_code = NA_integer_, class = "crop", is_processed = FALSE)
}

# 6) Apply classifier to every active item ####
cat("Classifying ", nrow(items_active), " active items...\n", sep = "")
results <- lapply(items_active$item, classify_item)
items_active[, parent_raw_item_code := vapply(results, function(r) r$parent_code, integer(1))]
items_active[, commodity_class := vapply(results, function(r) r$class, character(1))]
items_active[, is_processed := vapply(results, function(r) r$is_processed, logical(1))]

# 7) Resolve parent_raw_item (string) from parent_raw_item_code ####
item_lookup <- items_active[, list(item_code, item)]
items_active[, parent_raw_item := item_lookup$item[match(parent_raw_item_code, item_lookup$item_code)]]

# 8) Validate + assign include flag ####
valid_codes <- unique(items_active$item_code)
items_active[, include := TRUE]
# Processed but no resolvable parent -> include = FALSE.
items_active[
  is_processed == TRUE & is.na(parent_raw_item_code),
  include := FALSE
]
# Processed with parent code that doesn't exist in valid_codes -> include = FALSE.
# (Shouldn't fire because parent_raw_item_code comes from a reverse-lookup against
# items_active, but the assertion is cheap.)
items_active[
  is_processed == TRUE & !is.na(parent_raw_item_code) & !(parent_raw_item_code %in% valid_codes),
  `:=`(
    include = FALSE,
    parent_raw_item_code = NA_integer_,
    parent_raw_item = NA_character_
  )
]

# 9) Output the 6-column CSV ####
out <- items_active[, list(
  item_code,
  item,
  parent_raw_item_code,
  parent_raw_item,
  commodity_class,
  include
)]
setorder(out, item_code)
if (!dir.exists(dirname(out_path))) {
  dir.create(dirname(out_path), recursive = TRUE)
}
fwrite(out, out_path, na = "")
cat(sprintf("wrote %s (%d rows)\n", out_path, nrow(out)))

# 10) Summaries for the operator ####
cat("\nBy commodity_class:\n")
print(out[, .N, by = commodity_class][order(-N)])
cat("\nBy include:\n")
print(out[, .N, by = include])
cat("\nProcessed items with parent_raw resolved (sample of 15):\n")
print(out[!is.na(parent_raw_item_code), list(item, parent_raw_item, commodity_class)][1:15])

excluded <- out[include == FALSE]
if (nrow(excluded) > 0L) {
  cat(sprintf("\nWARNING: %d items flagged as processed but unresolvable -> include = FALSE\n",
              nrow(excluded)))
  cat("First 20 (curate by hand if a parent code can be supplied):\n")
  print(head(excluded[, list(item_code, item, commodity_class)], 20))
}
