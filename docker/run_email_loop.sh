#!/usr/bin/env bash
set -euo pipefail

cd /app
mkdir -p /app/state

INTERVAL_SECONDS="${EMAIL_INTERVAL_SECONDS:-900}"

while true; do
  echo "[email] start $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  python3 -m tracker.email_service \
    --db ./state/snapshots.sqlite3 \
    --limit 200
  echo "[email] done $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  sleep "$INTERVAL_SECONDS"
done
