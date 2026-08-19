#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# probe_pipeline_coverage.sh
#
# Read-only coverage probe: for every climate-input and hazard-output dataset the
# hazards / hazards_prototype pipeline touches, report
#   (a) does it exist on this machine,
#   (b) how many files,
#   (c) the geographic extent / size / resolution of a sample file,
#   (d) whether the sample file carries DATA (non-NA) outside Africa.
#
# (d) matters because 04_indices crops to the CHIRPS extent, so an output can be
# global in extent while being all-NA outside the Africa input footprint. Extent
# alone does not answer "is this dataset global?".
#
# Usage:  bash scripts/probe_pipeline_coverage.sh [ > logs/coverage_probe.log 2>&1 ]
# Env:    COMMON  (default /home/jovyan/common_data)
#         HPROTO  (default $COMMON/hazards_prototype)
# Needs:  gdalinfo + gdallocationinfo (GDAL CLI). Falls back to Rscript+terra.
# Writes: nothing. Pure read.
# ------------------------------------------------------------------------------
set -uo pipefail

COMMON="${COMMON:-/home/jovyan/common_data}"
HPROTO="${HPROTO:-$COMMON/hazards_prototype}"
PREMIUM="${PREMIUM:-/home/jovyan/shared-data-premium}"

have_gdal=0
command -v gdalinfo >/dev/null 2>&1 && have_gdal=1

ts() { date +"%H:%M:%S"; }
hdr() { printf '\n=== [%s] %s ===\n' "$(ts)" "$*"; }

# Sample points: lon lat label  (outside-Africa probes first, Kenya as control)
POINTS="78.0,22.0,INDIA -47.0,-15.0,BRAZIL -98.0,39.0,USA 105.0,15.0,SE_ASIA 37.0,0.0,KENYA_ctl"

# describe <file>
describe() {
  local f="$1"
  if [ "$have_gdal" -eq 1 ]; then
    gdalinfo "$f" 2>/dev/null | grep -E "^Size is|^Pixel Size|^Upper Left|^Lower Right|NoData Value" | sed 's/^/      /'
  else
    Rscript -e 'suppressMessages(library(terra));r<-rast(commandArgs(TRUE)[1]);cat("      dim",paste(dim(r),collapse="x"),"\n      ext",paste(round(as.vector(ext(r)),3),collapse=", "),"\n      res",paste(res(r),collapse=", "),"\n")' "$f" 2>/dev/null
  fi
}

# valuecheck <file>  -> prints "PT=value" per sample point (NA/nodata means no data there)
valuecheck() {
  local f="$1" p lon lat lab out
  if [ "$have_gdal" -eq 1 ] && command -v gdallocationinfo >/dev/null 2>&1; then
    printf '      values:'
    for p in $POINTS; do
      lon="${p%%,*}"; lat="$(echo "$p" | cut -d, -f2)"; lab="${p##*,}"
      out=$(gdallocationinfo -valonly -wgs84 "$f" "$lon" "$lat" 2>/dev/null | head -1)
      [ -z "$out" ] && out="oob"
      printf ' %s=%s' "$lab" "$out"
    done
    printf '\n'
  else
    Rscript -e 'suppressMessages(library(terra));a<-commandArgs(TRUE);r<-rast(a[1])[[1]];
      p<-data.frame(lon=c(78,-47,-98,105,37),lat=c(22,-15,39,15,0),lab=c("INDIA","BRAZIL","USA","SE_ASIA","KENYA_ctl"));
      v<-terra::extract(r,p[,1:2])[,2];
      cat("      values:",paste0(p$lab,"=",ifelse(is.na(v),"NA",format(v,digits=4)),collapse=" "),"\n")' "$f" 2>/dev/null
  fi
}

# probe <label> <dir-or-file> <glob>
probe() {
  local label="$1" target="$2" glob="${3:-*.tif}" n sample
  printf '\n--- %s\n    path: %s\n' "$label" "$target"
  if [ -f "$target" ]; then
    printf '    exists: FILE\n'
    describe "$target"; valuecheck "$target"; return
  fi
  if [ ! -d "$target" ]; then
    printf '    exists: NO (absent on this machine)\n'; return
  fi
  n=$(find "$target" -maxdepth 3 -name "$glob" -type f 2>/dev/null | head -20001 | wc -l | tr -d ' ')
  sample=$(find "$target" -maxdepth 3 -name "$glob" -type f 2>/dev/null | sort | head -1)
  printf '    exists: DIR   files(%s, maxdepth3, capped 20k): %s\n' "$glob" "$n"
  if [ -n "$sample" ]; then
    printf '    sample: %s\n' "$sample"
    describe "$sample"; valuecheck "$sample"
  else
    printf '    sample: none found\n'
  fi
}

printf '######################################################################\n'
printf '# PIPELINE COVERAGE PROBE   host=%s  date=%s\n' "$(hostname)" "$(date -Iseconds)"
printf '# COMMON=%s\n# HPROTO=%s\n# gdal CLI=%s\n' "$COMMON" "$HPROTO" "$have_gdal"
printf '######################################################################\n'

hdr "A. RAW OBSERVED CLIMATE (source-native footprints)"
probe "CHIRPS v2 global daily (chirps_wrld) - the crop template used by ALL 04_indices" "$COMMON/chirps_wrld"
probe "CHIRTS daily Tmax"  "$COMMON/chirts/Tmax"
probe "CHIRTS daily Tmin"  "$COMMON/chirts/Tmin"
probe "CHIRTS daily RHum"  "$COMMON/chirts/RHum"
probe "AgERA5 solar radiation (raw)" "$COMMON/ecmwf_agera5"

hdr "B. NEX-GDDP-CMIP6 (climdat_source=nexgddp) - IS THE DAILY DATA GLOBAL?"
probe "nexgddp raw NetCDF (premium share)" "$PREMIUM/nex-gddp-cmip6_raw" "*.nc"
for v in pr tasmax tasmin rsds hurs sfcWind; do
  probe "nexgddp daily $v (all ssp/gcm)" "$COMMON/nex-gddp-cmip6/$v"
done
probe "nexgddp INDICES (producer 04 output)" "$COMMON/nex-gddp-cmip6_indices"
probe "nexgddp consumer indices tree" "$COMMON/atlas_nex-gddp_hazards/cmip6/indices"

hdr "C. ATLAS_DELTA / BIAS-CORRECTED DAILY (expected Africa-only by construction)"
probe "chirps_cmip6_africa (BC precip)"                 "$COMMON/chirps_cmip6_africa"
probe "chirts_cmip6_africa (BC Tmax/Tmin/RHum)"         "$COMMON/chirts_cmip6_africa"
probe "ecmwf_agera5_cmip6_africa (BC solar radiation)"  "$COMMON/ecmwf_agera5_cmip6_africa"
probe "atlas_delta indices tree"                        "$COMMON/atlas_hazards/cmip6/indices"

hdr "D. STATIC INPUTS THAT CAN SILENTLY BOUND COVERAGE"
probe "soils sscp (NDWS/NDWL denominator)" "$COMMON/atlas_hazards/soils/sscp_world.tif"
probe "soils ssat"                         "$COMMON/atlas_hazards/soils/ssat_world.tif"
probe "roi africa.tif (BC reference grid)" "$COMMON/atlas_hazards/roi/africa.tif"
probe "SoS seasonal_mean"                  "$COMMON/atlas_sos/seasonal_mean"
probe "crop calendar / jagermeyr"          "$COMMON/atlas_crop_calendar"

hdr "E. CONSUMER SIDE (hazards_prototype) - what actually reaches the Atlas"
probe "consumer base_rast"            "$HPROTO/Data/base_rast.tif"
probe "obs CHIRPS/CHIRTS COGs PTOT"   "$HPROTO/Data/chirts_chirps_hist/PTOT"
probe "obs COGs TMAX"                 "$HPROTO/Data/chirts_chirps_hist/TMAX"
probe "MapSPAM 2020V1r2_SSA processed (crop exposure)" "$HPROTO/Data/mapspam/2020V1r2_SSA/processed"
probe "GLW4 processed (livestock exposure)"            "$HPROTO/Data/GLW4/processed"
probe "GLW4_2020 processed"                            "$HPROTO/Data/GLW4_2020/processed"
probe "atlas_pop processed (human exposure)"           "$HPROTO/Data/atlas_pop/processed"

hdr "F. ADMIN BOUNDARIES (hard vector bound on any extraction)"
for g in "$HPROTO/Data/boundaries" "$HPROTO/Data/boundariesintermediate"; do
  printf '\n--- boundaries: %s\n' "$g"
  if [ -d "$g" ]; then
    find "$g" -maxdepth 2 -type f \( -name "*.parquet" -o -name "*.gpkg" -o -name "*.shp" \) 2>/dev/null | head -10 | sed 's/^/      /'
    if command -v ogrinfo >/dev/null 2>&1; then
      f=$(find "$g" -maxdepth 2 -type f \( -name "*.gpkg" -o -name "*.shp" \) 2>/dev/null | sort | head -1)
      [ -n "$f" ] && ogrinfo -so -al "$f" 2>/dev/null | grep -E "Feature Count|Extent" | sed 's/^/      /'
    fi
  else
    printf '      exists: NO\n'
  fi
done

hdr "G. DISK FOOTPRINT (how much a global rebake would multiply)"
for d in "$COMMON/nex-gddp-cmip6" "$COMMON/nex-gddp-cmip6_indices" "$COMMON/chirps_wrld" "$COMMON/chirts" \
         "$COMMON/chirps_cmip6_africa" "$COMMON/atlas_hazards/cmip6/indices" "$COMMON/atlas_nex-gddp_hazards"; do
  [ -d "$d" ] && printf '    %s\n' "$(du -sh "$d" 2>/dev/null)"
done

printf '\n=== [%s] PROBE COMPLETE ===\n' "$(ts)"
