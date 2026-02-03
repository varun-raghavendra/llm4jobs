#!/usr/bin/env bash
set -euo pipefail

# scripts/control_run.sh
# Usage:
#   ./scripts/control_run.sh stop-and-wait --flag /tmp/jobtracker_resume.flag
#   ./scripts/control_run.sh stop            # just stop services
#   ./scripts/control_run.sh start           # start services immediately
#
# Behavior:
# - stop: gracefully stop tracker processes
# - stop-and-wait: stop, checkpoint & backup DB, then wait for flag file
#   to appear; when it appears, start `./run_all.sh` and exit.
# - start: run `./run_all.sh` (in tmux if available)

DB_PATH="state/snapshots.sqlite3"
FLAG_PATH="/tmp/jobtracker_resume.flag"
START_CMD="./run_all.sh"
WAIT_INTERVAL=3
KILL_TIMEOUT=10

usage() {
  echo "Usage: $0 {stop|stop-and-wait|start} [--flag PATH] [--cmd CMD]"
  exit 1
}

# parse args
if [ $# -lt 1 ]; then
  usage
fi
MODE="$1"; shift || true
while [ $# -gt 0 ]; do
  case "$1" in
    --flag) FLAG_PATH="$2"; shift 2 ;;
    --cmd) START_CMD="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; usage ;;
  esac
done

stop_services() {
  echo "Stopping tracker processes..."
  # polite termination
  pkill -f run_inference_loop.sh || true
  pkill -f inference_worker.py || true
  pkill -f run_batch_15min.sh || true
  # give time to exit
  for i in $(seq 1 $KILL_TIMEOUT); do
    if pgrep -f "run_inference_loop.sh|inference_worker.py|run_batch_15min.sh" > /dev/null; then
      sleep 1
    else
      break
    fi
  done
  # force kill if still alive
  pkill -9 -f run_inference_loop.sh || true
  pkill -9 -f inference_worker.py || true
  pkill -9 -f run_batch_15min.sh || true
  echo "Stopped (or forced stop)."
}

checkpoint_and_backup_db() {
  if [ ! -f "$DB_PATH" ]; then
    echo "DB not found at $DB_PATH; skipping checkpoint/backup."; return
  fi
  if command -v sqlite3 >/dev/null 2>&1; then
    echo "Running WAL checkpoint..."
    sqlite3 "$DB_PATH" "PRAGMA wal_checkpoint(FULL);" || true
  fi
  BACKUP="$DB_PATH."$(date +%Y%m%dT%H%M%S).bak
  echo "Backing up DB to $BACKUP"
  cp "$DB_PATH" "$BACKUP"
  echo "Backup done."
}

start_services() {
  echo "Starting services with: $START_CMD"
  if command -v tmux >/dev/null 2>&1; then
    tmux new -d -s jobtracker "$START_CMD"
    echo "Started in tmux session 'jobtracker'."
  else
    nohup $START_CMD >/dev/null 2>&1 &
    disown
    echo "Started in background (nohup)."
  fi
}

case "$MODE" in
  stop)
    stop_services
    checkpoint_and_backup_db
    exit 0
    ;;
  start)
    start_services
    exit 0
    ;;
  stop-and-wait)
    stop_services
    checkpoint_and_backup_db
    echo "Waiting for flag file: $FLAG_PATH"
    while true; do
      if [ -f "$FLAG_PATH" ]; then
        echo "Flag detected: $FLAG_PATH -- starting services"
        rm -f "$FLAG_PATH"
        start_services
        exit 0
      fi
      sleep $WAIT_INTERVAL
    done
    ;;
  *)
    usage
    ;;
esac
