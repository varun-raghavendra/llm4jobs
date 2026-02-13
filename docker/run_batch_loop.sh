#!/usr/bin/env bash
set -euo pipefail

cd /app
mkdir -p /app/state

INTERVAL_SECONDS="${BATCH_INTERVAL_SECONDS:-900}"

while true; do
  echo "[batch] start $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  python3 -m tracker.batch_runner_threaded \
    --csv ./inputs/companies.csv \
    --db ./state/snapshots.sqlite3 \
    --node-workdir ./node_link_extractor \
    --node-bin node \
    --node-timeout-seconds 180 \
    --max-workers 4 \
    -v
  echo "[batch] done $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  sleep "$INTERVAL_SECONDS"
done
