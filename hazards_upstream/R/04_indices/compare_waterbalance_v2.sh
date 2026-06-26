#!/usr/bin/env bash
# =============================================================================
# Impact comparison: LEGACY water-balance (fast_calc_NDWS/NDWL0/NDWL50.R, peest2
# Priestley-Taylor PET) vs the V2 producer (fast_calc_waterbalance.R: FAO-56 PM
# ET0 + Rcpp kernel + deterministic AVAIL). Quantifies how NDWS/NDWL0/NDWL50
# change before we integrate v2.
#
# Compares at the SEED month (default 1995-01): both seed AVAIL=0, so the kernel
# is numerically identical to the legacy eabyep loop (already validated) and the
# ONLY difference is the PET method (FAO-56 PM vs peest2). That isolates the
# FAO-56-PM impact cleanly. (The deterministic-AVAIL #19 effect only shows in
# resumed/later months; run a later month too if you want the combined effect.)
#
# Backs up + restores the canonical NDWS/NDWL0/NDWL50/AVAIL for the target month
# so baked data is untouched. Requires sfcWind present (v2 needs the wind term).
#
# Usage:  GCM=ACCESS-ESM1-5 YR=1995 MN=01 COMMON_DATA=~/common_data \
#           bash 04_indices/compare_waterbalance_v2.sh
# =============================================================================
set -uo pipefail

GCM="${GCM:-ACCESS-ESM1-5}"
YR="${YR:-1995}"; MN="${MN:-01}"
COMMON_DATA="${COMMON_DATA:-$HOME/common_data}"
SDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="$COMMON_DATA/nex-gddp-cmip6_indices/historical_${GCM}"
BK="$(mktemp -d -t wbv2_cmp.XXXX)"
MNUM="$((10#$MN))"

echo "=== water-balance v2 vs legacy: historical_$GCM $YR-$MN ==="
echo "base=$BASE  scratch=$BK"

# Back up canonical target-month outputs (NDWS/NDWL0/NDWL50 + AVAIL) for all dirs.
for idx in NDWS NDWL0 NDWL50 AVAIL; do
  f="$BASE/$idx/$idx-$YR-$MN.tif"
  [ -f "$f" ] && cp -p "$f" "$BK/orig_$idx.tif"
done

run() { ( cd "$SDIR" && env COMMON_DATA="$COMMON_DATA" SCENARIO=historical \
            YRS="$YR:$YR" MONTHS="$MNUM" GCMS="$GCM" FORCE_OVERWRITE=1 \
            Rscript "$1" ) 2>&1 | grep -iE "compute:|error|Run config|Finish" || true; }

echo ">>> LEGACY (peest2): NDWS, NDWL0, NDWL50"
run fast_calc_NDWS.R
run fast_calc_NDWL0.R
run fast_calc_NDWL50.R
for idx in NDWS NDWL0 NDWL50; do cp -p "$BASE/$idx/$idx-$YR-$MN.tif" "$BK/legacy_$idx.tif"; done

echo ">>> V2 (FAO-56 PM + kernel + deterministic AVAIL): all three"
run fast_calc_waterbalance.R
for idx in NDWS NDWL0 NDWL50; do cp -p "$BASE/$idx/$idx-$YR-$MN.tif" "$BK/v2_$idx.tif"; done

# Restore canonical originals.
for idx in NDWS NDWL0 NDWL50 AVAIL; do
  [ -f "$BK/orig_$idx.tif" ] && cp -p "$BK/orig_$idx.tif" "$BASE/$idx/$idx-$YR-$MN.tif"
done
echo ">>> canonical outputs restored"

echo ">>> DIFF (v2 - legacy) per index"
Rscript -e '
suppressMessages(library(terra)); a<-commandArgs(TRUE); bk<-a[1]
for (idx in c("NDWS","NDWL0","NDWL50")) {
  L<-terra::rast(file.path(bk,paste0("legacy_",idx,".tif")))
  V<-terra::rast(file.path(bk,paste0("v2_",idx,".tif")))
  d<-terra::values(V-L); d<-d[!is.na(d)]; n<-length(d); ch<-sum(d!=0)
  cat(sprintf("%-7s mean(legacy)=%.2f mean(v2)=%.2f | changed %d/%d (%.1f%%) | dmean=%+.2f dmin=%+.0f dmax=%+.0f\n",
      idx, mean(terra::values(L),na.rm=TRUE), mean(terra::values(V),na.rm=TRUE),
      ch, n, 100*ch/n, mean(d), min(d), max(d)))
}' "$BK" 2>&1 | grep -vE 'libtiff|LIBTIFF|GDAL'

echo "=== done. legacy/v2 rasters kept in $BK (rm when finished) ==="
