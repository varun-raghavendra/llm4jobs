#!/usr/bin/env bash
set -euo pipefail

# Adjust paths to your repo layout
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

source .venv/bin/activate

# Run batch runner in background and forward signals to it for graceful shutdown
python -m tracker.batch_runner_threaded \
  --csv ./inputs/companies.csv \
  --db ./state/snapshots.sqlite3 \
  --node-workdir ./node_link_extractor \
  --node-bin /Users/varun/.nvm/versions/node/v24.13.0/bin/node \
  --node-timeout-seconds 180 \
  --max-workers 4 \
  -v &
CHILD_PID=$!

forward_term() {
  if [ -n "${CHILD_PID-}" ] && kill -0 "$CHILD_PID" 2>/dev/null; then
    kill -TERM "$CHILD_PID" 2>/dev/null || true
    sleep 1
    wait "$CHILD_PID" 2>/dev/null || true
  fi
}

trap forward_term INT TERM EXIT

wait "$CHILD_PID"
EXIT_STATUS=$?
exit $EXIT_STATUS
