#!/usr/bin/env bash
# scripts/r21_rerun.sh — canonical R/2.1 relaunch wrapper
#
# Usage:
#   bash scripts/r21_rerun.sh                          # run all sections
#   bash scripts/r21_rerun.sh --skip-sec2              # skip extraction + combine
#   bash scripts/r21_rerun.sh --skip-sec2 --skip-sec3-1  # resume at sec 3.2
#   bash scripts/r21_rerun.sh --skip-sec3-4            # skip trend computation
#   bash scripts/r21_rerun.sh --no-overwrite           # skip already-written files
#
# All env vars accepted directly too:
#   FORCE_OVERWRITE=1 SKIP_R2_1_SEC3_4=1 bash scripts/r21_rerun.sh
#
# The wrapper:
#   1. Writes /tmp/r21_run_<stamp>.sh with the Rscript invocation
#   2. Launches under nohup, redirecting to logs/R21_pushdown_<stamp>.log
#   3. Auto-commits and pushes the log when the run completes (if creds available)

set -euo pipefail
cd "$(dirname "$0")/.."   # run from project root

# ---- Parse flags ----
FORCE_OVERWRITE="${FORCE_OVERWRITE:-1}"
SKIP_SEC2="${SKIP_R2_1_SEC2:-}"
SKIP_SEC3_1="${SKIP_R2_1_SEC3_1:-}"
SKIP_SEC3_2="${SKIP_R2_1_SEC3_2:-}"
SKIP_SEC3_3="${SKIP_R2_1_SEC3_3:-}"
SKIP_SEC3_4="${SKIP_R2_1_SEC3_4:-}"

for arg in "$@"; do
  case "$arg" in
    --skip-sec2)    SKIP_SEC2=1 ;;
    --skip-sec3-1)  SKIP_SEC3_1=1 ;;
    --skip-sec3-2)  SKIP_SEC3_2=1 ;;
    --skip-sec3-3)  SKIP_SEC3_3=1 ;;
    --skip-sec3-4)  SKIP_SEC3_4=1 ;;
    --no-overwrite) FORCE_OVERWRITE="" ;;
    *) echo "Unknown flag: $arg"; exit 1 ;;
  esac
done

# ---- Build env string ----
ENV_VARS=""
[ -n "$FORCE_OVERWRITE" ]  && ENV_VARS="FORCE_OVERWRITE=1 $ENV_VARS"
[ -n "$SKIP_SEC2" ]        && ENV_VARS="SKIP_R2_1_SEC2=1 $ENV_VARS"
[ -n "$SKIP_SEC3_1" ]      && ENV_VARS="SKIP_R2_1_SEC3_1=1 $ENV_VARS"
[ -n "$SKIP_SEC3_2" ]      && ENV_VARS="SKIP_R2_1_SEC3_2=1 $ENV_VARS"
[ -n "$SKIP_SEC3_3" ]      && ENV_VARS="SKIP_R2_1_SEC3_3=1 $ENV_VARS"
[ -n "$SKIP_SEC3_4" ]      && ENV_VARS="SKIP_R2_1_SEC3_4=1 $ENV_VARS"

export SETUP_SCRIPT="$HOME/atlas/hazards_prototype/R/0_server_setup.R"
export R21_SCRIPT="$HOME/atlas/hazards_prototype/R/2.1_create_monthly_haz_tables.R"

STAMP=$(date +%Y%m%d_%H%M%S)
LOG="logs/R21_pushdown_${STAMP}.log"
WRAPPER="/tmp/r21_run_${STAMP}.sh"

echo "R/2.1 launching: $ENV_VARS"
echo "Log: $LOG"

cat > "$WRAPPER" << SCRIPTEOF
#!/bin/bash
cd ~/atlas/hazards_prototype
${ENV_VARS} Rscript -e '
options(error = function() { traceback(2); quit(status=1, save="no") })
source(Sys.getenv("SETUP_SCRIPT"))
source(Sys.getenv("R21_SCRIPT"))
'
RC=\$?
echo "===== R/2.1 DONE (exit: \$RC) at \$(date -u '+%Y-%m-%d %H:%M:%S UTC') ====="
git add ${LOG}
git commit -m "auto: R/2.1 log ${STAMP} (exit \$RC)"
if git push origin develop 2>/dev/null; then
    echo "===== LOG COMMITTED AND PUSHED ====="
else
    echo "===== LOG COMMITTED LOCALLY — push skipped (no remote creds) ====="
fi
SCRIPTEOF

chmod +x "$WRAPPER"
nohup "$WRAPPER" > "$LOG" 2>&1 &
echo "PID=$!"
tail -f "$LOG"
