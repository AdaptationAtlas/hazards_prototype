# Diagnose WHY historic NDWS is saturated (~29 days everywhere) while future is
# normal (~22) - same code, so the driver must be the historic INPUT FORCING.
# NDWS = days ERATIO<0.5 = days soil-water demand (ET) outruns supply (rain).
# Saturation on EVERY pixel (incl. rainforest) => systematically too little water:
# historic rain ~0 (units/read), or historic ET (peest2) too high (rsds/temp units).
#
# Compares the daily INPUTS for one GCM between a historic month and a future
# month: pr (mm/day, expect 0-50), rsds (MJ m-2 day-1, expect ~5-30), tasmax/tasmin
# (degC). A broken input shows up as an obviously wrong magnitude in historic only.
#
# Usage: COMMON_DATA=~/common_data GCM=ACCESS-CM2 \
#        HMONTH=1995-07 FMONTH=ssp245:2050-07 Rscript 04_indices/probe_ndws_inputs.R

local({
  cargs <- commandArgs(FALSE); fa <- grep("^--file=", cargs, value = TRUE)
  base <- if (length(fa)) dirname(normalizePath(sub("^--file=", "", fa[1]))) else getwd()
  cand <- c(file.path(base,"..","00_setup.R"), file.path(base,"00_setup.R"), "../00_setup.R","00_setup.R")
  hit <- cand[file.exists(cand)][1]; if (is.na(hit)) stop("00_setup.R not found"); source(normalizePath(hit), local=FALSE)
})
suppressMessages(library(terra))
root <- common_data_root()
GCM  <- env_or("GCM", "ACCESS-CM2")
HMON <- env_or("HMONTH", "1995-07")                 # historic month yyyy-mm
FSPC <- env_or("FMONTH", "ssp245:2050-07")          # future ssp:yyyy-mm
fssp <- sub(":.*$", "", FSPC); fmon <- sub("^.*:", "", FSPC)

summ <- function(scn, ssp, ym, var) {
  d <- file.path(root, "nex-gddp-cmip6", var, ssp, GCM)
  yr <- sub("-.*$","",ym); mn <- sub("^.*-","",ym)
  ld <- as.integer(format(as.Date(paste0(ym,"-01"))+31, "%d")) # rough; just glob the month
  fls <- list.files(d, pattern = paste0("^",var,"_",ym,"-[0-9]{2}\\.tif$"), full.names = TRUE)
  if (!length(fls)) { cat(sprintf("  %-8s %-10s %-7s : NO FILES in %s\n", scn, var, ym, d)); return(invisible()) }
  r <- terra::rast(fls); v <- terra::values(r); v <- v[!is.na(v)]
  cat(sprintf("  %-8s %-7s %s : n=%d days  min=%.4g mean=%.4g max=%.4g  | monthly-sum(mean cell)=%.3g\n",
      scn, var, ym, terra::nlyr(r), min(v), mean(v), max(v),
      mean(terra::values(sum(r)), na.rm=TRUE)))
}

cat("=== NDWS input diagnosis:", GCM, "| historic", HMON, "vs future", fssp, fmon, "===\n")
cat("Expect: pr mm/day 0-50 (sum ~tens-hundreds); rsds MJ/m2/day ~5-30; tas degC.\n")
cat("-- HISTORIC --\n")
for (v in c("pr","rsds","tasmax","tasmin","hurs")) summ("historic","historical",HMON,v)
cat("-- FUTURE --\n")
for (v in c("pr","rsds","tasmax","tasmin","hurs")) summ("future",fssp,fmon,v)
cat("\nRead-off: any HISTORIC input with a wrong magnitude vs FUTURE = the driver.\n",
    "Esp. pr near 0 (units, missing *86400) or rsds off => over-depletion => saturation.\n", sep="")
