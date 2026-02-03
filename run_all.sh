#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

CRON_BEGIN="# JOBPOSTTRACKER-BEGIN"
CRON_END="# JOBPOSTTRACKER-END"

install_cron() {
  local tmpfile
  tmpfile="$(mktemp)"
  crontab -l 2>/dev/null > "$tmpfile" || true

  # Remove existing block if present.
  if grep -q "$CRON_BEGIN" "$tmpfile"; then
    perl -0777 -i -pe "s/$CRON_BEGIN.*?$CRON_END\\n?//s" "$tmpfile"
  fi

  cat >> "$tmpfile" <<'CRON'
# JOBPOSTTRACKER-BEGIN
# Run batch runner every 15 minutes
*/15 * * * * /bin/bash /Users/varun/JobPostTracker/scripts/run_batch_15min.sh >> /Users/varun/JobPostTracker/state/cron_batch.log 2>&1

# Run inference worker every minute (keeps it alive)
* * * * * /bin/bash /Users/varun/JobPostTracker/scripts/run_inference_loop.sh >> /Users/varun/JobPostTracker/state/cron_inference.log 2>&1

# Send email digest every 15 minutes
*/15 * * * * /bin/bash /Users/varun/JobPostTracker/scripts/run_email_once.sh >> /Users/varun/JobPostTracker/state/cron_email.log 2>&1
# JOBPOSTTRACKER-END
CRON

  crontab "$tmpfile"
  rm -f "$tmpfile"
}

case "${1-}" in
  --cron-only)
    install_cron
    exit 0
    ;;
  --remove-cron)
    tmpfile="$(mktemp)"
    crontab -l 2>/dev/null > "$tmpfile" || true
    if grep -q "$CRON_BEGIN" "$tmpfile"; then
      perl -0777 -i -pe "s/$CRON_BEGIN.*?$CRON_END\\n?//s" "$tmpfile"
    fi
    # Remove legacy unmarked entries if present.
    grep -v "/Users/varun/JobPostTracker/scripts/run_batch_15min.sh" "$tmpfile" | \
      grep -v "/Users/varun/JobPostTracker/scripts/run_inference_loop.sh" | \
      grep -v "/Users/varun/JobPostTracker/scripts/run_email_once.sh" > "${tmpfile}.clean"
    mv "${tmpfile}.clean" "$tmpfile"
    crontab "$tmpfile"
    rm -f "$tmpfile"
    pkill -f "/Users/varun/JobPostTracker/scripts/run_inference_loop.sh" || true
    pkill -f "/Users/varun/JobPostTracker/scripts/run_batch_15min.sh" || true
    pkill -f "/Users/varun/JobPostTracker/scripts/run_email_once.sh" || true
    exit 0
    ;;
esac

install_cron

./scripts/run_batch_15min.sh &
./scripts/run_inference_loop.sh &
./scripts/run_email_once.sh &

wait
