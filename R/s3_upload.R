if (!requireNamespace("AtlasDataManageR", quietly = TRUE)) {
  remotes::install_github("AdaptationAtlas/data-management/R/AtlasDataManageR")
}
library(AtlasDataManageR)

# Settings:
valid_timeframes <- c("annual", "jagermeyr", "sos_primary_eos", "")
args <- commandArgs(trailingOnly = TRUE)

if (length(args) > 0) {
  if (length(args) < 7) {
    stop(
      "Usage: Rscript upload_to_s3.R <timeframe> <intld> <usd> <ha> <glw4> <tif> <parquet> [workers]"
    )
  }
}

# Use CLI args if provided
if (length(args) >= 7) {
  TIMEFRAME <- args[[1]]
  if (!TIMEFRAME %in% valid_timeframes) {
    stop(
      "Invalid timeframe. Must be one of: ",
      paste(valid_timeframes, collapse = ", ")
    )
  }
  UPLOAD_HAZ_EXPOSURE_VOP_INTLD <- as.logical(args[[2]])
  UPLOAD_HAZ_EXPOSURE_VOP_USD <- as.logical(args[[3]])
  UPLOAD_HAZ_EXPOSURE_HA <- as.logical(args[[4]])
  UPLOAD_GLW4_EXPOSURE <- as.logical(args[[5]])
  UPLOAD_TIF <- as.logical(args[[6]])
  UPLOAD_PARQUET <- as.logical(args[[7]])
  PARALLEL_WORKERS <- as.integer(args[[8]] %||% 10)
} else {
  PARALLEL_WORKERS <- 10
  TIMEFRAME <- "annual"
  UPLOAD_HAZ_EXPOSURE_VOP_INTLD <- TRUE
  UPLOAD_HAZ_EXPOSURE_VOP_USD <- TRUE
  UPLOAD_HAZ_EXPOSURE_HA <- FALSE
  UPLOAD_GLW4_EXPOSURE <- TRUE
  UPLOAD_TIF <- TRUE
  UPLOAD_PARQUET <- TRUE
}

# Check if AWS credentials are set
if (
  !nzchar(Sys.getenv("AWS_ACCESS_KEY_ID")) ||
    !nzchar(Sys.getenv("AWS_SECRET_ACCESS_KEY"))
) {
  warning(
    "AWS credentials not found in env. Upload will fail unless defined in .aws/credentials"
  )
}

# Helpers:

# PTOT data is still being updated, so helper to stop upload for now
filter_PTOT <- function(files) grep("PTOT", files, value = TRUE, invert = TRUE)

parse_filename <- function(x, variable = NULL) {
  x <- tools::file_path_sans_ext(x)
  splits <- strsplit(x, "_")
  n <- lapply(splits, length)
  results <- vector(mode = "list", length = length(n))
  for (i in seq_along(results)) {
    file_split <- splits[[i]]
    len <- n[[i]]
    results[[i]] <- list(
      commodity = file_split[1],
      gcm = file_split[2],
      severity = file_split[3],
      interaction = ifelse(len == 7, file_split[4], "solo"),
      var = variable
    )
  }
  return(results)
}

build_s3_path <- function(parse_result) {
  lapply(
    parse_result,
    \(x) {
      sprintf(
        "source=atlas_cmip6/region=ssa/processing=hazard-risk-exposure/variable=%s/period=%s/model=%s/severity=%s/interaction=%s/%s.tif",
        x$var,
        TIMEFRAME,
        x$gcm,
        x$severity,
        x$interaction,
        x$commodity
      )
    }
  )
}

## -- Upload haz X exposure X vop USD -- ##
if (UPLOAD_HAZ_EXPOSURE_VOP_USD) {
  vop_usd_var <- "vop_usd15"
  if (UPLOAD_TIF) {
    uploader_haz_exp_usd <- AtlasDataManageR::S3DirUploader$new(
      upload_id = "haz-exp_vop-usd_tif",
      local_dir = paste0("Data/hazard_risk_vop_usd/", TIMEFRAME),
      s3_dir = "domain=hazard_exposure",
      bucket = "digital-atlas",
      file_pattern = ".tif",
      filter_fn = filter_PTOT,
      name_fn = \(x) {
        x_base <- basename(x)
        split <- parse_filename(x_base, variable = vop_usd_var)
        return(build_s3_path(split))
      },
      public = TRUE,
      recursive = FALSE
    )
    uploader_haz_exp_usd$upload_files_parallel(PARALLEL_WORKERS)
    uploader_haz_exp_usd$save_report()
  }
  if (UPLOAD_PARQUET) {
    uploader_haz_exp_usd_parquet <- AtlasDataManageR::S3DirUploader$new(
      upload_id = "haz-exp_vop-usd_parquet",
      local_dir = paste0("Data/hazard_risk_vop_usd/", TIMEFRAME),
      s3_dir = "domain=hazard_exposure",
      bucket = "digital-atlas",
      file_pattern = ".parquet$",
      name_fn = \(x) {
        x_base <- tools::file_path_sans_ext(basename(x))
        split <- strsplit(x_base, "_")
        names <- lapply(split, \(x) {
          sprintf(
            "source=atlas_cmip6/region=ssa/processing=hazard-risk-exposure/variable=%s/period=%s/model=%s/severity=%s/%s.parquet",
            vop_usd_var,
            TIMEFRAME,
            x[4],
            x[7],
            ifelse(x[5] == "int", "interaction", "solo")
          )
        })
      }
    )
    uploader_haz_exp_usd_parquet$upload_files_parallel(PARALLEL_WORKERS)
    uploader_haz_exp_usd_parquet$save_report()
  }
}

## -- Upload haz X exposure X vop international dollar-- ##
if (UPLOAD_HAZ_EXPOSURE_VOP_INTLD) {
  uploader_haz_exp_intld <- AtlasDataManageR::S3DirUploader$new(
    upload_id = "haz_exp_vop_intld",
    local_dir = paste0("Data/hazard_risk_vop/", TIMEFRAME),
    s3_dir = "domain=hazard_exposure",
    bucket = "digital-atlas",
    file_pattern = ".tif",
    filter_fn = filter_PTOT,
    name_fn = \(x) {
      x_base <- basename(x)
      split <- parse_filename(x_base, variable = "vop_intld15")
      return(build_s3_path(split))
    },
    public = TRUE,
    recursive = FALSE
  )
  uploader_haz_exp_intld$upload_files_parallel(PARALLEL_WORKERS)
  uploader_haz_exp_intld$save_report()
}

## -- Upload haz X exposure X vop harvested area -- ##
if (UPLOAD_HAZ_EXPOSURE_HA) {
  uploader_haz_exp_ha <- AtlasDataManageR::S3DirUploader$new(
    upload_id = "haz_exp_ha",
    local_dir = paste0("Data/hazard_risk_ha/", TIMEFRAME),
    s3_dir = "domain=hazard_exposure",
    bucket = "digital-atlas",
    file_pattern = ".tif",
    filter_fn = filter_PTOT,
    name_fn = \(x) {
      x_base <- basename(x)
      split <- parse_filename(x_base, variable = "ha")
      return(build_s3_path(split))
    },
    public = TRUE,
    recursive = FALSE
  )
  uploader_haz_exp_ha$upload_files_parallel(PARALLEL_WORKERS)
  uploader_haz_exp_ha$save_report()
}

## -- Upload GLW4 VOP (usd and intld) -- ##

if (UPLOAD_GLW4_EXPOSURE) {
  uploader_glw4 <- AtlasDataManageR::S3DirUploader$new(
    upload_id = "glw4",
    local_dir = "Data/GLW4/processed",
    s3_dir = "domain=exposure/type=livestock/source=glw4/region=ssa/time=2015/processing=atlas-harmonized",
    bucket = "digital-atlas",
    file_pattern = ".tif",
    name_fn = \(x) {
      x_base <- tools::file_path_sans_ext(basename(x))
      split <- strsplit(x_base, "_")
      s3_uri <- vapply(
        split,
        \(x) {
          sprintf(
            "variable=%s_%s/glw4_%s_%s.tif",
            x[2],
            gsub("number", "n", x[3]),
            x[2],
            gsub("number", "n", x[3])
          )
        },
        character(1)
      )
      return(s3_uri)
    },
    public = TRUE,
    recursive = FALSE
  )
  uploader_glw4$upload_files_parallel(PARALLEL_WORKERS)
  uploader_glw4$save_report()
}

# TODO:
# [-] - exposure x haz x ha
# [x] - Livestock Exposure/VOP
# [ ] - Un-hardcode spam path 0_serv setup line 413
# [x] - exposure x haz x vop
# [x] - exposure x haz x vop usd
# [ ] - hazard * risk timeseries
# [ ]
