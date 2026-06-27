# Generate atlas_hazards structure for Nex-GDDP-CMIP6 indices
# Target directory: ~/common_data/atlas_nex-gddp_hazards/cmip6/indices
# By: H. Achicanoy
# Alliance of Bioversity International & CIAT, 2025

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
suppressMessages(library(pacman))
pacman::p_load(purrr)
list.files2 <- Vectorize(FUN = list.files, vectorize.args = 'pattern')

# Setup table -- scenario/gcms/window from 00_setup env helpers (Phase-2).
# Historic default window is now 1995:2014 (cfg_yrs) so the produced ID period dir
# (historical_<gcm>_1995_2014) matches the live Atlas baseline AND the de-saturated
# fast_calc_NDWS FX re-bake window. Pass YRS=1981:2014 to rebuild the long-window dir.
scenario <- cfg_scenario()                 # SCENARIO=historical|future
gcms <- cfg_gcms()                          # GCMS=csv override
overwrite_copy <- env_flag("BRIDGE_OVERWRITE", FALSE)  # BRIDGE_OVERWRITE=1 to replace stale ID files
if (scenario == 'future') {
  ssps <- cfg_ssps(scenario)
  prds <- cfg_prds()
  stp_tbl <- expand.grid(ssp = ssps, gcm = gcms, prd = prds, stringsAsFactors = F) |> base::as.data.frame() |> dplyr::arrange(gcm, ssp, prd)
  stp_tbl$folder <- paste0(stp_tbl$ssp,'_',stp_tbl$gcm,'_',stp_tbl$prd)
  stp_tbl$ini_year <- strsplit(stp_tbl$prd, '_') |> purrr::map(1) |> unlist() |> as.numeric()
  stp_tbl$end_year <- strsplit(stp_tbl$prd, '_') |> purrr::map(2) |> unlist() |> as.numeric()
} else {
  yrs <- cfg_yrs(scenario)                  # default historical=1995:2014; YRS= override
  ssps <- 'historical'
  stp_tbl <- expand.grid(ssp = ssps, gcm = gcms, stringsAsFactors = F) |> base::as.data.frame() |> dplyr::arrange(gcm, ssp)
  stp_tbl$folder <- paste0(stp_tbl$ssp,'_',stp_tbl$gcm)
  stp_tbl$ini_year <- min(yrs)
  stp_tbl$end_year <- max(yrs)
  stp_tbl$prd <- paste0(stp_tbl$ini_year,'_',stp_tbl$end_year)
}

root <- common_data_root()

# Available indices -- scope with BRIDGE_INDICES=NDWS (csv) for targeted re-copies.
indices <- c('TAVG','TMAX','TMIN','PTOT',
             'NDD',paste0('NTx',20:50),'NDWL0','NDWL50','NDWS',
             'TAI','HSH','THI')
bridge_idx <- Sys.getenv("BRIDGE_INDICES", unset = "")
if (nzchar(bridge_idx)) {
  sel <- trimws(strsplit(bridge_idx, ",", fixed = TRUE)[[1]])
  indices <- indices[indices %in% sel]
  if (!length(indices)) stop("BRIDGE_INDICES matched no known index: ", bridge_idx)
}
.log("bridge FX->ID | scenario=", scenario, " gcms=", length(gcms),
     " window=", min(stp_tbl$ini_year), "-", max(stp_tbl$end_year),
     " indices=", paste(indices, collapse=","), " overwrite=", overwrite_copy)

for (index in indices) {

  .log('copying index: ', index)

  for (i in 1:nrow(stp_tbl)) {
    
    # Origin path
    org_pth <- paste0(root,'/nex-gddp-cmip6_indices/',stp_tbl$ssp[i],'_',stp_tbl$gcm[i],'/',index)
    if (index %in% c('HSH','THI')) {
      fls2copy <- c(
        list.files2(path = org_pth, pattern = paste0(index,'_max-',stp_tbl$ini_year[i]:stp_tbl$end_year[i]), full.names = T) |> as.character(),
        list.files2(path = org_pth, pattern = paste0(index,'_mean-',stp_tbl$ini_year[i]:stp_tbl$end_year[i]), full.names = T) |> as.character()
      )
    } else {
      fls2copy <- list.files2(path = org_pth, pattern = paste0(index,'-',stp_tbl$ini_year[i]:stp_tbl$end_year[i]), full.names = T) |> as.character()
    }
    
    if (index %in% c('NTX30','NTX35')) {
      # Origin
      trg_index <- gsub('X','x',index)
      # Target path
      trg_pth <- paste0(root,'/atlas_nex-gddp_hazards/cmip6/indices/',stp_tbl$ssp[i],'_',stp_tbl$gcm[i],'_',stp_tbl$prd[i],'/',trg_index)
      dir.create(trg_pth, F, T)
      ok <- file.copy(from = fls2copy, to = file.path(trg_pth,gsub('X','x',basename(fls2copy))), overwrite = overwrite_copy)
      .log('  ', stp_tbl$folder[i], ' ', trg_index, ': copied ', sum(ok), '/', length(fls2copy))
    } else {
      # Target path
      if (scenario == 'future') {
        trg_pth <- paste0(root,'/atlas_nex-gddp_hazards/cmip6/indices/',stp_tbl$ssp[i],'_',stp_tbl$gcm[i],'_',stp_tbl$prd[i],'/',index)
      } else {
        trg_pth <- paste0(root,'/atlas_nex-gddp_hazards/cmip6/indices/',stp_tbl$ssp[i],'_',stp_tbl$gcm[i],'_',stp_tbl$prd[i],'/',index)
      }
      dir.create(trg_pth, F, T)
      ok <- file.copy(from = fls2copy, to = file.path(trg_pth,basename(fls2copy)), overwrite = overwrite_copy)
      .log('  ', stp_tbl$folder[i], ' ', index, ': copied ', sum(ok), '/', length(fls2copy))
    }
    
  }
  
}
