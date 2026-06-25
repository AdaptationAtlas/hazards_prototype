#!/usr/bin/env bash
# =============================================================================
# Stage-0 Phase-2 GATE - run on cglabs BEFORE sweeping the remaining stages.
#
# Validates the 04_indices migration to 00_setup.R at RUNTIME on live Data/,
# using the smallest possible scope: one GCM, one month (1995-01, the historical
# AVAIL seed month - self-contained, no prior-month AVAIL dependency).
#
# Usage (from anywhere on cglabs):
#   bash hazards_upstream/R/04_indices/gate_phase2_ndws.sh
#   GATE_GCM=EC-Earth3 COMMON_DATA=~/common_data bash .../gate_phase2_ndws.sh
#
# Exit 0 = GATE PASS (safe to sweep remaining stages). Non-zero = FAIL (stop).
# =============================================================================
set -uo pipefail

GCM="${GATE_GCM:-EC-Earth3}"
COMMON_DATA="${COMMON_DATA:-$HOME/common_data}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="$COMMON_DATA/nex-gddp-cmip6_indices/historical_${GCM}/NDWS"
NDWS_TIF="$OUT_DIR/NDWS-1995-01.tif"
AVAIL_TIF="$OUT_DIR/AVAIL-1995-01.tif"
LOG="$(mktemp -t ndws_gate.XXXX.log)"

pass=0; fail=0
ok()  { echo "  PASS: $1"; pass=$((pass+1)); }
bad() { echo "  FAIL: $1"; fail=$((fail+1)); }

run_ndws() { # extra env passed as args
  ( cd "$SCRIPT_DIR" && env COMMON_DATA="$COMMON_DATA" SCENARIO=historical \
      YRS=1995:1995 MONTHS=1 GCMS="$GCM" "$@" \
      Rscript fast_calc_NDWS.R ) 2>&1 | tee -a "$LOG"
}

echo "=== Stage-0 Phase-2 GATE: NDWS 1995-01 / $GCM ==="
echo "COMMON_DATA=$COMMON_DATA"
echo "out_dir=$OUT_DIR"
echo "log=$LOG"
echo

# Pre-req: inputs must exist, else the GATE itself is meaningless.
PR_PATH="$COMMON_DATA/nex-gddp-cmip6/pr/historical/$GCM"
if [ ! -d "$PR_PATH" ]; then
  echo "ABORT: input dir missing: $PR_PATH (pick another GATE_GCM that is present)."
  exit 3
fi

# --- 1. fresh compute -------------------------------------------------------
echo ">>> [1/4] fresh single-month compute"
rm -f "$NDWS_TIF" "$AVAIL_TIF"
: > "$LOG"
run_ndws
grep -q "Run config:.*n_gcms=1"        "$LOG" && ok "timestamped .log run-config marker (n_gcms=1)" || bad "missing run-config .log marker"
grep -q "NDWS compute: 1995-01"         "$LOG" && ok "single-month .log compute marker" || bad "missing compute marker"
[ -f "$NDWS_TIF" ]  && ok "wrote $(basename "$NDWS_TIF")"  || bad "NDWS output not written"
[ -f "$AVAIL_TIF" ] && ok "wrote $(basename "$AVAIL_TIF")" || bad "AVAIL output not written"

# --- 2. skip when present (should_skip, no FORCE) ---------------------------
echo ">>> [2/4] re-run without FORCE_OVERWRITE -> should SKIP"
: > "$LOG"
run_ndws
grep -q "NDWS compute: 1995-01" "$LOG" && bad "recomputed despite existing output (should_skip broken)" || ok "skipped existing output"

# --- 3. FORCE_OVERWRITE recompute (overwrite=TRUE, no abort) -----------------
echo ">>> [3/4] re-run with FORCE_OVERWRITE=1 -> should RECOMPUTE + overwrite"
before="$(stat -c %Y "$NDWS_TIF" 2>/dev/null || stat -f %m "$NDWS_TIF")"
sleep 1; : > "$LOG"
run_ndws FORCE_OVERWRITE=1
after="$(stat -c %Y "$NDWS_TIF" 2>/dev/null || stat -f %m "$NDWS_TIF")"
grep -qi "exists\|cannot overwrite" "$LOG" && bad "writeRaster aborted on existing file (missing overwrite=TRUE)" || ok "overwrote without abort"
grep -q "NDWS compute: 1995-01" "$LOG" && ok "FORCE_OVERWRITE recomputed" || bad "FORCE_OVERWRITE did not recompute"
[ "$after" != "$before" ] && ok "output mtime advanced (rewritten)" || bad "output not rewritten under FORCE"

# --- 4. loud-fail on missing inputs (Phase-1 stopifnot) ---------------------
echo ">>> [4/4] year with no inputs -> should FAIL LOUD (stopifnot), non-zero exit"
: > "$LOG"
( cd "$SCRIPT_DIR" && env COMMON_DATA="$COMMON_DATA" SCENARIO=historical \
    YRS=1850:1850 MONTHS=1 GCMS="$GCM" Rscript fast_calc_NDWS.R ) >>"$LOG" 2>&1
rc=$?
if [ $rc -ne 0 ] && grep -q "length(pr_fls) > 0\|length(tx_fls) > 0\|length(tm_fls) > 0\|length(sr_fls) > 0" "$LOG"; then
  ok "missing-input run aborted loud via stopifnot (rc=$rc)"
else
  bad "missing-input run did not fail loud (rc=$rc) - silent-failure not closed"
fi

echo
echo "=== GATE SUMMARY: $pass passed, $fail failed ==="
if [ "$fail" -eq 0 ]; then
  echo "GATE PASS - 04_indices migration validated at runtime. Safe to sweep 01/02/03/05/06."
  exit 0
else
  echo "GATE FAIL - do NOT sweep further. Inspect $LOG."
  exit 1
fi
