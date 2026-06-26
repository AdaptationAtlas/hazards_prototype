# Download CMIP6 downscaled+bias corrected data from Nex-GDDP-CMIP6
# By: H. Achicanoy
# Alliance Bioversity International & CIAT, 2025

# R options + shared Stage-0 setup (data root, .log(), env run-controls; warn=-1 dropped)
local({
  cargs <- commandArgs(FALSE)
  fa <- grep("^--file=", cargs, value = TRUE)
  base <- if (length(fa)) dirname(normalizePath(sub("^--file=", "", fa[1]))) else getwd()
  cand <- c(file.path(base, "..", "00_setup.R"), file.path(base, "00_setup.R"),
            "../00_setup.R", "00_setup.R")
  hit <- cand[file.exists(cand)][1]
  if (is.na(hit)) stop("00_setup.R not found from ", base)
  source(normalizePath(hit), local = FALSE)
})

# Load libraries and functions
if(!require(pacman)){install.packages('pacman');library(pacman)} else {library(pacman)}
pacman::p_load(purrr,furrr,future,dplyr,httr)
grep2 <- Vectorize(grep, 'pattern')

urlFileExist <- function(url){
  HTTP_STATUS_OK <- 200
  hd <- httr::HEAD(url)
  status <- hd$all_headers[[1]]$status
  list(exists = status == HTTP_STATUS_OK)
}

scenario <- cfg_scenario("historical") # SCENARIO env: historical (default) | future

# # Available files to download
# fls <- readLines('https://nex-gddp-cmip6.s3-us-west-2.amazonaws.com/index_v1.1_md5.txt')
# nms <- strsplit(fls, split = '/') |> purrr::map(6) |> unlist()

# Root URL for downloads
root <- 'https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com/NEX-GDDP-CMIP6'

# Filters to apply
gcms <- cfg_gcms()   # GCMS env (default = ATLAS_GCMS, the 18); was hardcoded
vars <- c('pr','tasmax','tasmin','hurs','rsds','sfcWind')  # sfcWind added for FAO-56 PM ET0 (PET)
if (scenario == 'future') {
  ssps <- c('ssp126','ssp245','ssp370','ssp585')
  yrs <- 2021:2100
} else {
  if (scenario == 'historical') {
    ssps <- 'historical'
    yrs <- 1981:2014 # 1981:1994, 1995:2014, 1981:2014
  }
}

# Setup table
stp <- base::expand.grid(gcm = gcms, ssp = ssps, var = vars, yr = yrs, stringsAsFactors = F) |>
  base::as.data.frame(); rm(gcms, ssps, vars, yrs)
# Available files to download
wd <- file.path(common_data_root(), "nex-gddp-cmip6_raw")
dir.create(wd, F, T)
if (scenario == 'future') {
  outfile <- file.path(wd,'cmip6_future_v2.0_files_to_download.csv')
} else {
  if (scenario == 'historical') {
    outfile <- file.path(wd,'cmip6_baseline_v2.0_files_to_download.csv')
  }
}
if(!file.exists(outfile)) {
  plan(multisession, workers = 30)
  available_files <-  1:nrow(stp) |>
    furrr::future_map(.f = function(i) {
      input_dir <- paste0(root,'/',stp$gcm[i],'/',stp$ssp[i],'/r1i1p1f1/',stp$var[i])
      input_file <- c(paste0(stp$var[i],'_day_',stp$gcm[i],'_',stp$ssp[i],'_r1i1p1f1_gn_',stp$yr[i],'_v2.0.nc'),
                      paste0(stp$var[i],'_day_',stp$gcm[i],'_',stp$ssp[i],'_r1i1p1f1_gr_',stp$yr[i],'_v2.0.nc'),
                      paste0(stp$var[i],'_day_',stp$gcm[i],'_',stp$ssp[i],'_r1i1p1f1_gr1_',stp$yr[i],'_v2.0.nc'),
                      paste0(stp$var[i],'_day_',stp$gcm[i],'_',stp$ssp[i],'_r1i1p1f1_gn_',stp$yr[i],'_v1.2.nc'),
                      paste0(stp$var[i],'_day_',stp$gcm[i],'_',stp$ssp[i],'_r1i1p1f1_gr_',stp$yr[i],'_v1.2.nc'),
                      paste0(stp$var[i],'_day_',stp$gcm[i],'_',stp$ssp[i],'_r1i1p1f1_gr1_',stp$yr[i],'_v1.2.nc'),
                      paste0(stp$var[i],'_day_',stp$gcm[i],'_',stp$ssp[i],'_r1i1p1f1_gn_',stp$yr[i],'_v1.1.nc'),
                      paste0(stp$var[i],'_day_',stp$gcm[i],'_',stp$ssp[i],'_r1i1p1f1_gr_',stp$yr[i],'_v1.1.nc'),
                      paste0(stp$var[i],'_day_',stp$gcm[i],'_',stp$ssp[i],'_r1i1p1f1_gr1_',stp$yr[i],'_v1.1.nc'),
                      paste0(stp$var[i],'_day_',stp$gcm[i],'_',stp$ssp[i],'_r1i1p1f1_gn_',stp$yr[i],'.nc'),
                      paste0(stp$var[i],'_day_',stp$gcm[i],'_',stp$ssp[i],'_r1i1p1f1_gr_',stp$yr[i],'.nc'),
                      paste0(stp$var[i],'_day_',stp$gcm[i],'_',stp$ssp[i],'_r1i1p1f1_gr1_',stp$yr[i],'.nc'))
      
      #available files to download
      input_file_avl <- input_file[which(unlist(lapply(file.path(input_dir, input_file), urlFileExist)))]
      cndt3 <- grep(pattern = 'v2.0', x = input_file_avl)
      cndt2 <- grep(pattern = 'v1.2', x = input_file_avl)
      cndt1 <- grep(pattern = 'v1.1', x = input_file_avl)
      if (length(cndt3) > 0) {
        input_file_dwn <- input_file_avl[cndt3]
      } else {
        if (length(cndt2) > 0) {
          input_file_dwn <- input_file_avl[cndt2]
        } else {
          if (length(cndt1) > 0) {
            input_file_dwn <- input_file_avl[cndt1]
          } else {
            input_file_dwn <- input_file_avl
          }
        }
      }
      res <- data.frame(pth_dir = input_dir, file = input_file_dwn)
      return(res)
    }, .progress = T) |> dplyr::bind_rows()
  plan(sequential)
  gc(F,T,T)
  
  stp <- cbind(stp, available_files); rm(available_files)
  utils::write.csv(x = stp, file = outfile, row.names = F)
  
} else {
  stp <- utils::read.csv(outfile)
}

stp$version <- strsplit(x = stp$file, split = '_') |> purrr::map(8) |> base::unlist() |> gsub(pattern = '.nc', replacement = '', x = _)
table(stp$version)
# Per-run download scope. sfcWind added for FAO-56 PM ET0 (PET). NOTE: if the
# cached manifest CSV (above) predates sfcWind, delete it so the URL-probe re-runs
# and includes sfcWind rows; existing files are skipped on download (size check).
# Per-run var scope. sfcWind-only: the other vars' daily tifs already exist on
# cglabs (their raw .nc are deleted post-preprocess), so including them re-fetches
# ~370 GB needlessly (cglabs 2026-06-26). Widen this list only when a var's tifs
# are genuinely missing. If the cached manifest CSV predates sfcWind, delete it first.
stp <- stp |> dplyr::filter(var %in% c('sfcWind')) |> base::as.data.frame()

# Download files (parallel) - network/IO bound, so concurrent curls help a lot.
# Worker count via DL_WORKERS (default 16). Size-skip (>1e8 B) makes re-runs
# resumable: already-downloaded files are skipped, so a re-run only fills gaps.
# For maximum throughput on this PUBLIC bucket, `s5cmd cp --no-sign-request` or
# `aws s3 cp --no-sign-request` (many concurrent transfers) outperform R curl -
# consider those for a full multi-var bulk pull.
# DL_WORKERS default 32 (cglabs has good bandwidth); S3 won't throttle this -
# the practical cap is the local link, not the bucket. Each file gets a few
# retries with exponential backoff to ride out transient connection drops.
dl_workers <- as.integer(env_or("DL_WORKERS", "32"))
dl_tries   <- as.integer(env_or("DL_TRIES", "3"))
.log('Download: ', nrow(stp), ' files, ', dl_workers, ' workers, ', dl_tries, ' tries/file')
future::plan(future::multisession, workers = dl_workers)
ok <- 1:nrow(stp) |>
  furrr::future_map(.f = function(i) {
    outd <- paste0(wd,'/',stp$var[i],'2/',stp$ssp[i],'/',stp$gcm[i])
    dir.create(outd, F, T)
    outfile <- file.path(outd, stp$file[i])
    url <- file.path(stp$pth_dir[i], stp$file[i])
    # Byte-exact completeness via remote Content-Length: a fixed floor (1e8)
    # wrongly accepts a connection-truncated file that happens to exceed it
    # (cglabs flag D - sfcWind .nc are ~302 MB). Fall back to the floor if the
    # HEAD/Content-Length is unavailable.
    exp_sz <- tryCatch(as.numeric(httr::headers(httr::HEAD(url))[["content-length"]]),
                       error = function(e) NA_real_)
    complete <- function() {
      if (!file.exists(outfile)) return(FALSE)
      sz <- file.size(outfile)
      if (!is.na(exp_sz) && exp_sz > 0) isTRUE(sz == exp_sz) else sz >= 1e8
    }
    if (!complete()) {
      for (k in seq_len(dl_tries)) {
        tryCatch(download.file(url, outfile, method = 'curl', quiet = TRUE),
                 error = function(e) NULL, warning = function(w) NULL)
        if (complete()) break
        Sys.sleep(2^(k - 1))            # 1s, 2s, 4s, ... backoff
      }
    }
    complete()                          # TRUE if present + plausibly complete
  }, .progress = TRUE) |> unlist()
future::plan(future::sequential)
gc(F, T, T)
# Loud failure report (silent worker try() must not hide gaps; re-run fills them).
nfail <- sum(!ok)
if (nfail > 0) {
  .log(nfail, ' of ', length(ok), ' files FAILED/incomplete - re-run to retry gaps', level = 'WARN')
  print(utils::head(file.path(stp$pth_dir[!ok], stp$file[!ok]), 20))
} else {
  .log('Download complete: all ', length(ok), ' files present')
}