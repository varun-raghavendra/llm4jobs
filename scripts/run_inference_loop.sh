#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

source .venv/bin/activate

LOCK_FILE="/tmp/jobposttracker_inference.lock"

cleanup() {
  rm -f "$LOCK_FILE"
}

if ! ( set -o noclobber; echo "$$" > "$LOCK_FILE" ) 2>/dev/null; then
  if [ -s "$LOCK_FILE" ]; then
    PID="$(cat "$LOCK_FILE" 2>/dev/null || true)"
    if [ -n "${PID-}" ] && kill -0 "$PID" 2>/dev/null; then
      echo "inference_loop already running pid=$PID"
      exit 0
    fi
  fi
  echo "$$" > "$LOCK_FILE"
fi

# Start worker in background so we can forward signals and wait on it
python -m tracker.inference_worker \
  --db ./state/snapshots.sqlite3 \
  --node-bin /Users/varun/.nvm/versions/node/v24.13.0/bin/node \
  --puppeteer-script ./job-alert/puppeteer_scraper/puppeteer_scrapper.js \
  --python-bin python \
  --extract-experience-py ./job-alert/extract_experience.py \
  --timeout-seconds 120 \
  --poll-sleep-seconds 2 \
  --verbose &
CHILD_PID=$!

# Forward INT/TERM to child and ensure cleanup
term_child() {
  if [ -n "${CHILD_PID-}" ] && kill -0 "$CHILD_PID" 2>/dev/null; then
    kill -TERM "$CHILD_PID" 2>/dev/null || true
    # give child a moment to exit
    sleep 1
    wait "$CHILD_PID" 2>/dev/null || true
  fi
  cleanup
}

trap term_child INT TERM EXIT

# Wait for child to finish; exit code reflects child's exit status
wait "$CHILD_PID"
EXIT_STATUS=$?
cleanup
exit $EXIT_STATUS
