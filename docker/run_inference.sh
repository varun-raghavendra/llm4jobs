#!/usr/bin/env bash
set -euo pipefail

cd /app
mkdir -p /app/state

python3 -m tracker.inference_worker \
  --db ./state/snapshots.sqlite3 \
  --node-bin node \
  --puppeteer-script ./job-alert/puppeteer_scraper/puppeteer_scrapper.js \
  --python-bin python3 \
  --extract-experience-py ./job-alert/extract_experience.py \
  --timeout-seconds 120 \
  --poll-sleep-seconds 2 \
  --verbose
