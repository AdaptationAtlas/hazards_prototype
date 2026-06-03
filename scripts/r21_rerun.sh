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
#   1. Pre-flight: checks AWS creds + git push creds; prompts to proceed or stop
#   2. Writes /tmp/r21_run_<stamp>.sh with the Rscript invocation
#   3. Launches under nohup, redirecting to logs/R21_pushdown_<stamp>.log
#   4. Auto-commits and pushes the log when the run completes

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

# ========================================================================
# PRE-FLIGHT CHECKS
# ========================================================================

echo ""
echo "========================================================"
echo " R/2.1 pre-flight checks"
echo "========================================================"

# ---- AWS credentials ----
AWS_OK=false
if [ -n "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${AWS_SECRET_ACCESS_KEY:-}" ]; then
  echo "[OK]  AWS creds: env vars AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY set"
  AWS_OK=true
elif [ -f "$HOME/.aws/credentials" ]; then
  echo "[OK]  AWS creds: ~/.aws/credentials present"
  AWS_OK=true
else
  echo ""
  echo "[WARN] AWS credentials not found."
  echo "       The pipeline reads/writes S3 via s3fs. Without creds it will fail"
  echo "       during S3 operations in 0_server_setup.R and push_to_s3 steps."
  echo ""
  echo "       To provide credentials, choose one of:"
  echo ""
  echo "       Option A — env vars (paste before running this script):"
  echo "         export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE"
  echo "         export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  echo ""
  echo "       Option B — credentials file:"
  echo "         mkdir -p ~/.aws"
  echo "         cat > ~/.aws/credentials << 'AWSEOF'"
  echo "         [default]"
  echo "         aws_access_key_id = AKIAIOSFODNN7EXAMPLE"
  echo "         aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  echo "         AWSEOF"
  echo ""
fi

# ---- Git push credentials ----
GIT_OK=false
if git ls-remote origin HEAD > /dev/null 2>&1; then
  echo "[OK]  Git: remote accessible — log auto-push will work"
  GIT_OK=true
else
  echo "[WARN] Git: cannot reach remote (push will be skipped at end of run)"
  echo "       Log will be committed locally. Push manually after run:"
  echo "         git push origin develop"
fi

# ---- Summary and proceed/stop ----
echo ""
echo "Section controls: FORCE_OVERWRITE=${FORCE_OVERWRITE:-0}" \
     "SEC2=${SKIP_SEC2:+SKIP} SEC3_1=${SKIP_SEC3_1:+SKIP}" \
     "SEC3_2=${SKIP_SEC3_2:+SKIP} SEC3_3=${SKIP_SEC3_3:+SKIP}" \
     "SEC3_4=${SKIP_SEC3_4:+SKIP}"
echo ""

if [ "$AWS_OK" = "false" ]; then
  printf "AWS creds missing. Proceed anyway? [y/N] "
  read -r REPLY
  if [[ ! "$REPLY" =~ ^[Yy]$ ]]; then
    echo "Aborted. Set AWS credentials and re-run."
    exit 1
  fi
  echo "Proceeding without AWS creds (S3 operations will fail)."
fi

echo "Starting R/2.1..."
echo ""

# ========================================================================
# BUILD AND LAUNCH WRAPPER
# ========================================================================

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
    echo "===== LOG COMMITTED LOCALLY — push failed (no remote creds) ====="
    echo "      Run: git push origin develop"
fi
SCRIPTEOF

chmod +x "$WRAPPER"
nohup "$WRAPPER" > "$LOG" 2>&1 &
echo "PID=$!"
tail -f "$LOG"
