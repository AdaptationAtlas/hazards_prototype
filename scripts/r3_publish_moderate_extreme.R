#!/usr/bin/env Rscript
# scripts/r3_publish_moderate_extreme.R
# =====================================
# Publish the moderate and extreme severity hazard_exposure parquets
# to S3, activating the Severity tier advanced control in the notebook.
#
# Background (CR-091):
#   R/3 produces all three severity tiers locally as a byproduct of
#   every Stage C run. Only the severe tier was ever published.
#   The notebook SQL already filters AND severity='${hazardSeverity}' —
#   once moderate/extreme S3 keys exist, the dropdown becomes functional
#   with no further notebook SQL changes.
#
# Local files (working_dir-relative after 0_server_setup.R setwd):
#   Data/hazard_risk_vop_usd/jagermeyr/haz-freq-exp_vop_nominal-usd-2021_ENSEMBLEmean_int_adm_moderate.parquet
#   Data/hazard_risk_vop_usd/jagermeyr/haz-freq-exp_vop_nominal-usd-2021_ENSEMBLEmean_int_adm_extreme.parquet
#
# S3 targets (sibling to existing severe key):
#   s3://digital-atlas/domain=hazard_exposure/.../severity=moderate/int=multi-hazard.parquet
#   s3://digital-atlas/domain=hazard_exposure/.../severity=extreme/int=multi-hazard.parquet
#
# Convention: download+upload with ACL="public-read" (NOT s3_file_copy — strips ACL).
#
# Usage: Rscript scripts/r3_publish_moderate_extreme.R [--dry-run]

source("R/0_server_setup.R")
suppressPackageStartupMessages({ pacman::p_load(s3fs) })

args    <- commandArgs(trailingOnly = TRUE)
DRY_RUN <- "--dry-run" %in% args

BUCKET  <- "digital-atlas"
S3_BASE <- paste0(
  "domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/",
  "processing=hazard-risk-exposure/variable=vop_nominal-usd21/",
  "period=jagermeyr/model=ENSEMBLEmean"
)

tiers <- c("moderate", "extreme")
local_dir <- atlas_dirs$data_dir$hazard_risk_vop_usd

cat("=== CR-091: publish moderate + extreme hazard_exposure", if(DRY_RUN) "[DRY RUN]" else "", "===\n")
cat("local_dir =", local_dir, "\n\n")

for (tier in tiers) {
  local_f <- file.path(local_dir, "jagermeyr",
    paste0("haz-freq-exp_vop_nominal-usd-2021_ENSEMBLEmean_int_adm_", tier, ".parquet"))
  s3_key  <- sprintf("%s/severity=%s/int=multi-hazard.parquet", S3_BASE, tier)
  s3_url  <- sprintf("s3://%s/%s", BUCKET, s3_key)
  bak_url <- sprintf("s3://%s/sandbox/backup/cr091_%s/int=multi-hazard.parquet",
                     BUCKET, tier)

  cat(sprintf("--- %s ---\n", toupper(tier)))
  if (!file.exists(local_f)) {
    cat(sprintf("  ERROR: local file not found: %s\n", local_f))
    cat("  Has Stage C run with FORCE_OVERWRITE=1?\n\n")
    next
  }
  cat(sprintf("  local  : %s (%.1f MB)\n", basename(local_f), file.size(local_f)/1e6))
  cat(sprintf("  s3     : %s\n", s3_url))

  # Check if S3 key already exists (s3fs throws on 404 rather than returning NULL)
  existing <- tryCatch(
    suppressWarnings(s3fs::s3_file_info(s3_url)),
    error = function(e) NULL
  )
  if (!is.null(existing)) {
    cat(sprintf("  backup : %s\n", bak_url))
    if (!DRY_RUN) {
      tmp_bak <- tempfile(fileext = ".parquet")
      s3fs::s3_file_download(s3_url, tmp_bak)
      s3fs::s3_file_upload(tmp_bak, bak_url, ACL = "public-read", overwrite = TRUE)
      unlink(tmp_bak)
      cat("  Backed up.\n")
    } else {
      cat("  [dry run] backup skipped\n")
    }
  }

  if (!DRY_RUN) {
    s3fs::s3_file_upload(local_f, s3_url, ACL = "public-read", overwrite = TRUE)
    info <- s3fs::s3_file_info(s3_url)
    cat(sprintf("  -> uploaded %s bytes (mtime %s)\n", info$size, info$modification_time))
  } else {
    cat("  [dry run] upload skipped\n")
  }
  cat("\n")
}

cat("=== PUBLISH", if(DRY_RUN) "DRY RUN" else "COMPLETE", "===\n")
if (!DRY_RUN) {
  cat("Severity tier dropdown in the notebook will now render moderate + extreme.\n")
  cat("Notebook nbData.json must also be updated (see atlas_notebooks).\n")
}
