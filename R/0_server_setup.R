# 0) Introduction ####
# This script configures an R environment for the hazards_prototype workflow by:
#  1) Installing/Loading necessary packages (pacman, remotes, data.table, httr, s3fs, etc.).
#  2) Setting environment variables and verifying/creating local directories.
#  3) Dynamically downloading or updating input data (e.g., geoboundaries, MapSpam data, FAO data,
#     livestock/water data from ISIMIP, etc.) from remote sources such as S3 or GitHub.
#  4) Making sure the project structure is consistent across local/remote sessions (e.g., cglabs).
#
# Once this script completes, your workspace is ready for more detailed hazards analysis.
# The workflow is dependent on the outputs of the https://github.com/AdaptationAtlas/hazards pipeline

# 0.1) Load packages and functions #####
# Install and load pacman if not already installed
if (!require("pacman", character.only = TRUE)) {
  install.packages("pacman")
  library(pacman)
}

# Use the isciences version of exactextractr, not the CRAN version
if (!require("exactextractr")) {
  remotes::install_github("isciences/exactextractr")
}

# List of packages to be installed/loaded via pacman
packages <- c("remotes", "data.table", "httr", "s3fs", "xml2", "paws.storage", "rvest", "glue", "jsonlite","terra")

# Use pacman to install and load the packages
pacman::p_load(char = packages)

# Silence pbapply's \r-spinner under non-interactive (nohup) runs. Under
# nohup capture the spinner becomes hundreds of "[----] 0%" / "/ |  \" lines
# in the log file. progressr handlers("void") does NOT touch pbapply — it
# has its own global option that has to be set independently. Setting it
# once here covers every script that sources 0_server_setup.R.
if (!interactive()) {
  if (requireNamespace("pbapply", quietly = TRUE)) {
    pbapply::pboptions(type = "none")
  }
}

# Source additional functions used in this workflow from GitHub
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

# 0.2) Set timeframes #####
# Possible timeframe calculations, e.g., "annual", "sos_primary_fixed_3", etc.
timeframe_choices <- c(
  "annual",
  "jagermeyr"
  # "sos_primary_eos",
  # "sos_primary_fixed_3",
  # "sos_primary_fixed_4",
  # "sos_primary_fixed_5",
  # "sos_secondary_eos",
  # "sos_secondary_fixed_3",
  # "sos_secondary_fixed_4",
  # "sos_secondary_fixed_5"
)

# 0.3) Set climate data source ####
# "nexgddp" or "atlas_delta". Was two unconditional assignments where the second
# always won, so the first was dead code and switching source meant editing this
# file. Now a real setting: override with ATLAS_CLIMDAT_SOURCE=atlas_delta.
# The default in metadata/hosts.json is "nexgddp", which is what line 2 of the
# old pair resolved to, so behaviour is unchanged.
# (resolved just below, once project_dir is known)

# 0.3) Record R-project location #####
# Function to add or update an environment variable in the .Renviron file
set_env_variable <- function(var_name, var_value, renviron_file = "~/.Renviron") {
  # Read the .Renviron file if it exists
  if (file.exists(renviron_file)) {
    env_vars <- readLines(renviron_file)
  } else {
    env_vars <- character(0)
  }

  # Check if the variable already exists
  var_exists <- grepl(paste0("^", var_name, "="), env_vars)

  if (any(var_exists)) {
    # Update the existing variable
    env_vars[var_exists] <- paste0(var_name, "=", var_value)
  } else {
    # Add the new variable
    env_vars <- c(env_vars, paste0(var_name, "=", var_value))
  }

  # Write the updated .Renviron file
  writeLines(env_vars, renviron_file)
}

# Check if project_dir is already set in the environment
if (!nzchar(Sys.getenv("project_dir"))) {
  project_dir <- getwd()
  Sys.setenv(project_dir = project_dir)

  # Add or update the project_dir variable in the .Renviron file
  set_env_variable("project_dir", project_dir)

  # Reload .Renviron so that the change takes effect in the current session
  readRenviron("~/.Renviron")
}

# Confirm project_dir was set
(project_dir <- Sys.getenv("project_dir"))

# The resolver. Pure - no setwd, no dir.create, no network; it only reads
# metadata/hosts.json and sets options(). Sourced here because everything from
# section 0.4 onward resolves through it.
source(file.path(project_dir, "R", "00_paths.R"))

climdat_source <- atlas_climdat_source()

# 0.4) Resolve this host and set the working directory #####
# Was a chain of five string equalities on project_dir with no else branch, so
# an unrecognised machine left working_dir undefined and died with
# "object 'working_dir' not found". Host profiles now live in
# metadata/hosts.json and a new machine is a stanza there, not a code edit.
#
# atlas_describe() prints how everything resolved and warns when a profile is
# marked UNVERIFIED or when a stale ~/.Renviron project_dir disagrees with the
# located repo.
working_dir <- atlas_working_dir()

# Retained because downstream code branches on them. Derived from the resolved
# host rather than from a literal path comparison.
Cglabs <- identical(atlas_host_id(), "cglabs")
Aflabs <- identical(atlas_host_id(), "afrilabs")

if (!dir.exists(working_dir)) {
  dir.create(working_dir, recursive = TRUE)
}

setwd(working_dir)

# 0.5) Indices directory (raw monthly hazard data) ####
# These used to be set only inside `if (Cglabs)`, so everywhere else they were
# UNDEFINED and R/1, R/2, R/2.1, R/2.2 and R/3 failed with an unhelpful
# "object 'indices_dir' not found". They are now resolved on every host.
#
# Defined-but-absent is strictly better than undefined: the path is reportable,
# and a script that genuinely needs the data calls
# atlas_dir("indices", require = TRUE) and gets an error naming the missing tree.
indices_dir <- atlas_dir("indices")
indices_dir2 <- atlas_dir("indices_seasonal")

if (!dir.exists(indices_dir)) {
  cat(
    "Monthly hazard indices are not staged on this host.\n",
    "  expected at: ", indices_dir, "\n",
    "  They are regenerated, not downloaded - see\n",
    "  metadata/catalogue/nexgddp-indices-monthly.json for the command,\n",
    "  or point at an existing copy with ATLAS_INDICES_DIR=/path\n",
    sep = ""
  )
}
cat("Climate data source = ", climdat_source, "\n")

# 0.6) Base Raster ####
if (climdat_source == "atlas_delta") {
  ## DEV NOTE - NEED TO UPDATE WITH MASKED BASE RAST ###
  # Load a reference/base raster used for resampling or extent alignment
  base_rast_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/base_raster.tif"
  base_rast <- terra::rast(base_rast_url)
  base_rast_path <- file.path(project_dir, "metadata", "base_raster.tif")
} else {
  base_rast_path <- "Data/base_rast.tif"

  if (!file.exists(base_rast_path)) {
    if (Cglabs) {
      # Derive the nexgddp base raster from the canonical CGlabs source.
      target_ext <- ext(-180, 180, -50, 50)
      base_rast <- terra::rast("/home/jovyan/common_data/nex-gddp-cmip6/pr/ssp126/ACCESS-CM2/pr_2021-01-01.tif")
      base_rast_cropped <- crop(base_rast, target_ext)
      terra::writeRaster(base_rast_cropped, base_rast_path, overwrite = TRUE)
      base_rast <- terra::rast(base_rast_path)
    } else {
      message(
        "nexgddp base_rast.tif not present at '", base_rast_path,
        "' and not running on CGlabs - skipping base_rast load. ",
        "Scripts that need a nexgddp base raster (e.g. 1_make_timeseries.R) ",
        "will need it staged manually."
      )
      base_rast <- NULL
    }
  } else {
    base_rast <- terra::rast(base_rast_path)
  }
}

# 1) Setup workspace ####
# Increase download timeout (in seconds) to avoid timeouts during large data pulls
options(timeout = 600)

# Increase GDAL cache size for faster raster processing
terra::gdalCache(60000)
# 2) Create directory structures ####
# 2.1) Local directories #####
atlas_data <- read_json(file.path(project_dir, "metadata/data.json"))

# 2.1.1) Outputs ######
# Create a hierarchical list for top-level data directories
atlas_dirs <- list()
atlas_dirs$data_dir <- "Data"

# Define subdirectories under 'data_dir'
# List of all subdirectories to create under the root directory
subdirs <- c(
  "hazard_timeseries",
  "hazard_timeseries_mean_month",
  "hazard_timeseries_class",
  "hazard_timeseries_risk",
  "hazard_risk",
  "hazard_timeseries_mean",
  "hazard_timeseries_sd",
  "hazard_timeseries_int",
  "hazard_risk_vop_usd",
  "hazard_risk_vop",
  "hazard_risk_ha",
  "hazard_risk_n",
  "hazard_risk_vop_reduced",
  "hazard_exposure",
  "roi",
  "exposure",
  "isimip_timeseries",
  "isimip_timeseries_mean",
  "isimip_timeseries_sd",
  "cropsuite_class",
  "chirts_chirps_hist"
)

# Assign paths for each subdir key
for (subdir in subdirs) {
  atlas_dirs$data_dir[[subdir]] <- file.path(atlas_dirs$data_dir[[1]], subdir)
}

# Subset of subdirs that need timeframe subfolders
timeframe_subdirs <- c(
  "hazard_timeseries",
  "hazard_timeseries_class",
  "hazard_timeseries_risk",
  "hazard_risk",
  "hazard_timeseries_mean",
  "hazard_timeseries_sd",
  "hazard_timeseries_int",
  "hazard_risk_vop_usd",
  "hazard_risk_vop",
  "hazard_risk_ha",
  "hazard_risk_n",
  "hazard_risk_vop_reduced"
)

non_timeframe_subdirs <- subdirs[!subdirs %in% timeframe_subdirs]

# Create non timeframe-based folders
invisible(lapply(non_timeframe_subdirs, function(key) {
  dir_path <- atlas_dirs$data_dir[[key]]
  if (!dir.exists(dir_path)) dir.create(dir_path, recursive = TRUE)
}))

# Create objects like roi_dir, exposure_dir, etc.
for (key in non_timeframe_subdirs) {
  var_name <- paste0(key, "_dir")
  assign(var_name, atlas_dirs$data_dir[[key]])
}

# 2.1.2) Inputs ######

# Create new entries in atlas_dirs$data_dir for various directories beyond hazard outputs
# atlas_dirs$data_dir$Boundaries         <- atlas_data$boundaries$alternate_paths$project # Created directly
atlas_dirs$data_dir$GLPS <- file.path(atlas_dirs$data_dir[[1]], "GLPS")
atlas_dirs$data_dir$cattle_heatstress <- file.path(atlas_dirs$data_dir[[1]], "cattle_heatstress")
atlas_dirs$data_dir$adaptive_capacity <- file.path(atlas_dirs$data_dir[[1]], "adaptive_capacity")
atlas_dirs$data_dir$atlas_pop <- file.path(atlas_dirs$data_dir[[1]], "atlas_pop")
# atlas_dirs$data_dir$commodity_masks    <- file.path(atlas_dirs$data_dir[[1]], "commodity_masks")
atlas_dirs$data_dir$GLW4 <- file.path(atlas_dirs$data_dir[[1]], "GLW4")
atlas_dirs$data_dir$GLW4_2020 <- file.path(atlas_dirs$data_dir[[1]], "GLW4_2020")
atlas_dirs$data_dir$livestock_vop <- file.path(atlas_dirs$data_dir[[1]], "livestock_vop")
atlas_dirs$data_dir$afr_highlands <- file.path(atlas_dirs$data_dir[[1]], "afr_highlands")
atlas_dirs$data_dir$fao <- file.path(atlas_dirs$data_dir[[1]], "fao")
# atlas_dirs$data_dir$mapspam_2020v1r2   <- atlas_data$mapspam_2020v1r2$alternate_paths$project # folder object mapspam_dir created directly from atlas_data
atlas_dirs$data_dir$sos <- file.path(atlas_dirs$data_dir[[1]], "sos")
atlas_dirs$data_dir$ggcmi <- file.path(atlas_dirs$data_dir[[1]], "ggcmi")
atlas_dirs$data_dir$hydrobasins <- file.path(atlas_dirs$data_dir[[1]], "hydrobasins")
atlas_dirs$data_dir$solution_tables <- file.path(atlas_dirs$data_dir[[1]], "solution_tables")

# Now create (if not present) each of these directories locally
boundaries_dir <- sub("/$", "", atlas_data$boundaries$alternate_paths$project)
if (!dir.exists(boundaries_dir)) {
  dir.create(boundaries_dir, recursive = TRUE)
}

boundaries_int_dir <- paste0(boundaries_dir, "intermediate")
if (!dir.exists(boundaries_int_dir)) {
  dir.create(boundaries_int_dir, recursive = TRUE)
}

glps_dir <- atlas_dirs$data_dir$GLPS
if (!dir.exists(glps_dir)) {
  dir.create(glps_dir, recursive = TRUE)
}

cattle_heatstress_dir <- atlas_dirs$data_dir$cattle_heatstress
if (!dir.exists(cattle_heatstress_dir)) {
  dir.create(cattle_heatstress_dir, recursive = TRUE)
}

ac_dir <- atlas_dirs$data_dir$adaptive_capacity
if (!dir.exists(ac_dir)) {
  dir.create(ac_dir, recursive = TRUE)
}

hpop_dir <- atlas_dirs$data_dir$atlas_pop
if (!dir.exists(hpop_dir)) {
  dir.create(hpop_dir, recursive = TRUE)
}

hpop_int_dir <- file.path(hpop_dir, "intermediate")
if (!dir.exists(hpop_int_dir)) {
  dir.create(hpop_int_dir, recursive = TRUE)
}

hpop_pro_dir <- file.path(hpop_dir, "processed")
if (!dir.exists(hpop_pro_dir)) {
  dir.create(hpop_pro_dir, recursive = TRUE)
}

# commodity_mask_dir <- atlas_dirs$data_dir$commodity_masks
#  if (!dir.exists(commodity_mask_dir)) {
#    dir.create(commodity_mask_dir, recursive = TRUE)
# }

glw_dir <- atlas_dirs$data_dir$GLW4
if (!dir.exists(glw_dir)) {
  dir.create(glw_dir, recursive = TRUE)
}

glw_pro_dir <- file.path(glw_dir, "processed")
if (!dir.exists(glw_pro_dir)) {
  dir.create(glw_pro_dir, recursive = TRUE)
}

glw_int_dir <- file.path(glw_dir, "intermediate")
if (!dir.exists(glw_int_dir)) {
  dir.create(glw_int_dir, recursive = TRUE)
}

# GLW4-2020 (used by 0.4.1_create_livestock_exposure.R after the
# July/Sept 2025 refactors 35375cf + 69d7b84).
glw2020_dir <- atlas_dirs$data_dir$GLW4_2020
if (!dir.exists(glw2020_dir)) {
  dir.create(glw2020_dir, recursive = TRUE)
}

glw2020_pro_dir <- file.path(glw2020_dir, "processed")
if (!dir.exists(glw2020_pro_dir)) {
  dir.create(glw2020_pro_dir, recursive = TRUE)
}

glw2020_int_dir <- file.path(glw2020_dir, "intermediate")
if (!dir.exists(glw2020_int_dir)) {
  dir.create(glw2020_int_dir, recursive = TRUE)
}

# ls_vop_dir <- atlas_dirs$data_dir$livestock_vop
# if (!dir.exists(ls_vop_dir)) {
#  dir.create(ls_vop_dir, recursive = TRUE)
# }

afr_highlands_dir <- atlas_dirs$data_dir$afr_highlands
if (!dir.exists(afr_highlands_dir)) {
  dir.create(afr_highlands_dir, recursive = TRUE)
}

fao_dir <- atlas_dirs$data_dir$fao
if (!dir.exists(fao_dir)) {
  dir.create(fao_dir, recursive = TRUE)
}

mapspam_dir <- sub("/$", "", atlas_data$mapspam_2020v1r2$alternate_paths$project)
if (!dir.exists(mapspam_dir)) {
  dir.create(mapspam_dir, recursive = TRUE)
}

mapspam_pro_dir <- "Data/mapspam/2020V1r2_SSA/processed"
if (!dir.exists(mapspam_pro_dir)) {
  dir.create(mapspam_pro_dir, recursive = TRUE)
}

sos_dir <- atlas_dirs$data_dir$sos
if (!dir.exists(sos_dir)) {
  dir.create(sos_dir, recursive = TRUE)
}

ggcmi_dir <- atlas_dirs$data_dir$ggcmi
if (!dir.exists(ggcmi_dir)) {
  dir.create(ggcmi_dir, recursive = TRUE)
}

hydrobasins_dir <- atlas_dirs$data_dir$hydrobasins
if (!dir.exists(hydrobasins_dir)) {
  dir.create(hydrobasins_dir, recursive = TRUE)
}

solution_tables_dir <- atlas_dirs$data_dir$solution_tables
if (!dir.exists(solution_tables_dir)) {
  dir.create(solution_tables_dir, recursive = TRUE)
}

# Raw source trees that sit outside Data/, under the shared bulk store.
# These were also CGlabs-only literals, leaving them undefined elsewhere -
# R/1.2_create_isimip_timeseries.R:13 uses isimip_raw_dir unguarded. Resolved on
# every host now, from the same host profile as everything else.
sos_raw_dir <- atlas_dir("sos_raw")
isimip_raw_dir <- atlas_dir("isimip_raw")
chirts_raw_dir <- atlas_dir("chirts_raw")
chirps_raw_dir <- atlas_dir("chirps_raw")
cropsuite_raw_dir <- atlas_dir("cropsuite_raw")

# Only create it where the bulk store actually exists, so a laptop without one
# does not acquire a stray empty ~/common_data/isimip.
if (dir.exists(atlas_common_data()) && !dir.exists(isimip_raw_dir)) {
  dir.create(isimip_raw_dir, recursive = TRUE)
}

# 2.2) Cloud directories (Atlas s3 bucket) #####
# 2.2.1) Set S3 directory structure ######
bucket_name <- "http://digital-atlas.s3.amazonaws.com"
bucket_name_s3 <- "s3://digital-atlas"

atlas_dirs$s3_dir <- list(bucket_name_s3)

# Define subdirectories under 's3_dir'
subdirs <- c(
  "hazard_timeseries",
  "haz_time_risk_dir",
  "hazard_risk"
)

for (sub in subdirs) {
  atlas_dirs$s3_dir[[sub]] <-
    file.path(atlas_dirs$s3_dir[[1]], sub)
}

hazard_timeseries_s3 <- atlas_dirs$s3_dir$hazard_timeseries

# 2.2.2) Create an S3FileSystem object for anonymous read access ######
s3 <- s3fs::S3FileSystem$new(anonymous = TRUE)

# 3) Data acquisition ####
# ---------------------------------------------------------------------------
# Paths are DECLARED here unconditionally. Data is FETCHED on demand.
#
# This section used to download ~15 datasets on every single setup, whether or
# not the run needed them. A laptop that only wanted to read one parquet still
# pulled the entire FAOSTAT bulk release first. Worse, a failure in any one
# block aborted setup for everyone - §3.12 (GGCMI) did exactly that on every
# host where the folder did not already hold precisely 40 files, because it used
# the magrittr `.` placeholder in a native `|>` pipe, which is a hard error.
#
# Acquisition now lives in ONE place - R/00_acquire.R, driven by the per-dataset
# recipes in metadata/catalogue/<id>.json - and runs only when asked:
#
#   atlas_require("mapspam-2020v1r2")        # from any script; idempotent
#   atlas_require_stage("3")                 # everything one stage consumes
#   ATLAS_PREFETCH=all Rscript ...           # the old eager behaviour
#   ATLAS_PREFETCH=faostat-bulk,glps Rscript ...
#
# Every fetch appends a receipt to <working_dir>/Data/_acquisition/<id>.jsonl
# recording when, from where, how many files and how many bytes - so a copy can
# be traced and recreated rather than guessed at.
#
# The declarations below are load-bearing and stay unconditional: geo_files_local
# alone is read by 27 other scripts. They name where a file WOULD be, whether or
# not it is present.
# ---------------------------------------------------------------------------

source(file.path(project_dir, "R", "00_acquire.R"))

# 3.0) File path declarations (no I/O) #####
update <- FALSE

# 3.0.1) Geoboundaries ######
admin_levels <- atlas_data$boundaries$params$level
regions <- atlas_data$boundaries$params$region[[2]] # 1 = 'global', 2 = 'africa'

geo_files_s3 <- file.path(
  bucket_name_s3,
  glue(
    atlas_data$boundaries$s3$path_pattern,
    region = regions,
    level = admin_levels
  )
)

geo_files_local <- file.path(boundaries_dir, basename(geo_files_s3))
names(geo_files_local) <- c("admin0", "admin1", "admin2")

# 3.0.2) GLW (2015 vintage) ######
glw_names <- c(
  poultry = "Ch", sheep = "Sh", pigs = "Pg", horses = "Ho",
  goats = "Gt", ducks = "Dk", buffalo = "Bf", cattle = "Ct"
)
glw_codes <- c(
  poultry = 6786792, sheep = 6769626, pigs = 6769654, horses = 6769681,
  goats = 6769696, ducks = 6769700, buffalo = 6770179, cattle = 6769711
)
glw_files <- file.path(glw_dir, paste0("5_", glw_names, "_2015_Da.tif"))

# 3.0.3) FAOSTAT ######
def_file <- paste0(fao_dir, "/Deflators_E_All_Data_(Normalized).csv")
fao_econ_file <- file.path(fao_dir, "Prices_E_Africa_NOFLAG.csv")
fao_econ_file_world <- file.path(fao_dir, "Prices_E_All_Data_(Normalized).csv")
prod_file <- file.path(fao_dir, "Production_Crops_Livestock_E_Africa_NOFLAG.csv")
prod_file_world <- file.path(fao_dir, "Production_Crops_Livestock_E_All_Area_Groups.csv")
vop_file <- file.path(fao_dir, "Value_of_Production_E_Africa.csv")
vop_file_world <- file.path(fao_dir, "Value_of_Production_E_All_Area_Groups.csv")
# Note: FAOSTAT's canonical filename uses 'CropsLivestock' (no underscore),
# unlike 'Crops_Livestock' above. Match the upstream spelling.
trade_file <- file.path(fao_dir, "Trade_CropsLivestock_E_Africa_NOFLAG.csv")
trade_file_world <- file.path(fao_dir, "Trade_CropsLivestock_E_All_Area_Groups.csv")

# 3.0.4) Highlands map ######
afr_highlands_file <- file.path(afr_highlands_dir, "afr-highlands.asc")

# 3.1) On-demand acquisition #####
.atlas_prefetch <- Sys.getenv("ATLAS_PREFETCH", unset = "")
if (nzchar(.atlas_prefetch)) {
  .atlas_ids <- if (identical(tolower(trimws(.atlas_prefetch)), "all")) {
    names(Filter(
      function(r) identical(r$transfer$strategy, "pull-from-origin") && !is.null(r$acquire),
      atlas_catalogue()
    ))
  } else {
    trimws(strsplit(.atlas_prefetch, ",", fixed = TRUE)[[1]])
  }
  cat(sprintf("ATLAS_PREFETCH: acquiring %d dataset(s)\n", length(.atlas_ids)))
  for (.id in .atlas_ids) {
    tryCatch(
      atlas_require(.id),
      error = function(e) {
        # One dataset failing must not abort setup for everything else -
        # the old section 3 did exactly that.
        cat("  prefetch FAILED for ", .id, ": ", conditionMessage(e), "\n", sep = "")
      }
    )
  }
  rm(.atlas_ids)
} else {
  cat("Data acquisition is on demand - nothing was downloaded.\n")
  cat("  atlas_require(\"<dataset-id>\")  fetch one dataset\n")
  cat("  atlas_require_stage(\"<stage>\")  fetch what a stage consumes\n")
  cat("  ATLAS_PREFETCH=all               restore the old eager behaviour\n")
  cat("  Rscript R/checks/73_catalogue.R --status   what this host already has\n")
}
rm(.atlas_prefetch)

# 4) Set data URLs ####
# 4.1) hazard class #####
haz_class_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_classes.csv"

# 4.2) hazard metadata #####
haz_meta_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_metadata.csv"

# 4.3) mapspam codes #####
ms_codes_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SpamCodes.csv"
spam2fao_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SPAM2010_FAO_crops.csv"

# 4.4) ecocrop ####
ecocrop_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/ecocrop.csv"

# 4.5) isimip metadata #####
isimip_meta_url <- "https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/isimip_water_var_metadata.csv"

# ---------------------------------------------------------------------------------------------
# End of script
cat("0_server_setup.R has completed successfully.\n")
cat("CGLabs = ", Cglabs, "\n")
#  ---------------------------------------------------------------------------------------------
