#!/usr/bin/env bash
# =============================================================================
# hazards#19 AVAIL-fix IMPACT comparison — legacy vs fixed, on real data.
# Shows the effect of the deterministic prior-month AVAIL seed (NDWS_AVAIL_FIX)
# BEFORE we integrate it. Does NOT corrupt baked data: backs up the canonical
# target tif + AVAIL, recomputes the same month both ways into /tmp, diffs, then
# restores the originals.
#
# Mechanism it exercises = the #19 trigger: recompute a MID-series month while
# LATER months already exist on disk. Legacy seeds from the lexically-last AVAIL
# (a wrong, far month) -> saturation; fixed seeds from the true prior month.
#
# Usage (from anywhere on cglabs):
#   bash 04_indices/compare_avail_fix.sh
#   INDEX=NDWL0 GCM=EC-Earth3 TGT_YR=1996 TGT_MN=06 COMMON_DATA=~/common_data \
#     bash 04_indices/compare_avail_fix.sh
#
# Requires: the historical series for INDEX/GCM already baked (so a later AVAIL
# exists for legacy to mis-grab, and AVAIL-<prior month> exists for the fix).
# =============================================================================
set -uo pipefail

INDEX="${INDEX:-NDWS}"
GCM="${GCM:-ACCESS-ESM1-5}"
TGT_YR="${TGT_YR:-1996}"
TGT_MN="${TGT_MN:-06}"
COMMON_DATA="${COMMON_DATA:-$HOME/common_data}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

OUT_DIR="$COMMON_DATA/nex-gddp-cmip6_indices/historical_${GCM}/${INDEX}"
TGT_TIF="$OUT_DIR/${INDEX}-${TGT_YR}-${TGT_MN}.tif"
AVAIL_TGT="$OUT_DIR/AVAIL-${TGT_YR}-${TGT_MN}.tif"
# prior month (yr/mn arithmetic) — what the FIX should seed from
PRIOR="$(date -d "${TGT_YR}-${TGT_MN}-01 -1 day" +%Y-%m 2>/dev/null || python3 - "$TGT_YR" "$TGT_MN" <<'PY'
import sys,datetime
y,m=int(sys.argv[1]),int(sys.argv[2]); d=datetime.date(y,m,1)-datetime.timedelta(days=1)
print(f"{d.year:04d}-{d.month:02d}")
PY
)"
AVAIL_PRIOR="$OUT_DIR/AVAIL-${PRIOR}.tif"

BK="$(mktemp -d -t avail_cmp.XXXX)"
LEG="$BK/${INDEX}_legacy.tif"
FIX="$BK/${INDEX}_fixed.tif"

echo "=== hazards#19 AVAIL-fix impact: $INDEX historical_$GCM ${TGT_YR}-${TGT_MN} ==="
echo "out_dir=$OUT_DIR  prior=$PRIOR  scratch=$BK"

# preconditions
[ -f "$TGT_TIF" ]     || { echo "ABORT: target not baked: $TGT_TIF"; exit 3; }
[ -f "$AVAIL_PRIOR" ] || { echo "ABORT: prior-month AVAIL missing: $AVAIL_PRIOR (need in-order baked series)"; exit 3; }

# back up canonical outputs we are about to overwrite
cp -p "$TGT_TIF"   "$BK/orig_${INDEX}.tif"
[ -f "$AVAIL_TGT" ] && cp -p "$AVAIL_TGT" "$BK/orig_AVAIL.tif"

run() { # $1 = NDWS_AVAIL_FIX value
  ( cd "$SCRIPT_DIR" && env COMMON_DATA="$COMMON_DATA" SCENARIO=historical \
      YRS="${TGT_YR}:${TGT_YR}" MONTHS="$((10#$TGT_MN))" GCMS="$GCM" \
      FORCE_OVERWRITE=1 NDWS_AVAIL_FIX="$1" \
      Rscript "fast_calc_${INDEX}.R" ) 2>&1 | grep -iE "compute:|error|prior-month|run config" || true
}

echo ">>> LEGACY recompute (NDWS_AVAIL_FIX=0)"; run 0; cp -p "$TGT_TIF" "$LEG"
echo ">>> FIXED  recompute (NDWS_AVAIL_FIX=1)"; run 1; cp -p "$TGT_TIF" "$FIX"

# restore canonical originals (leave baked data exactly as found)
cp -p "$BK/orig_${INDEX}.tif" "$TGT_TIF"
[ -f "$BK/orig_AVAIL.tif" ] && cp -p "$BK/orig_AVAIL.tif" "$AVAIL_TGT"
echo ">>> canonical outputs restored"

echo ">>> DIFF legacy vs fixed"
Rscript -e '
a <- terra::rast(commandArgs(TRUE)[1]); b <- terra::rast(commandArgs(TRUE)[2])
d <- b - a                                   # fixed - legacy
v <- terra::values(d); v <- v[!is.na(v)]
n <- length(v); ch <- sum(v != 0)
cat(sprintf("cells (non-NA)        : %d\n", n))
cat(sprintf("cells changed        : %d (%.2f%%)\n", ch, 100*ch/n))
cat(sprintf("delta (fixed-legacy) : min=%.3f mean=%.3f max=%.3f\n", min(v), mean(v), max(v)))
cat(sprintf("legacy NDWS higher   : %d cells (legacy over-counts stress days = saturation)\n", sum(v < 0)))
cat(sprintf("fixed  NDWS higher   : %d cells\n", sum(v > 0)))
la <- terra::values(a); fb <- terra::values(b)
cat(sprintf("mean NDWS  legacy=%.3f  fixed=%.3f\n", mean(la,na.rm=TRUE), mean(fb,na.rm=TRUE)))
' "$LEG" "$FIX" 2>&1 | grep -vE 'libtiff|LIBTIFF'

echo "=== done. legacy=$LEG fixed=$FIX (kept for inspection; rm $BK when finished) ==="
