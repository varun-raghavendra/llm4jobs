# JobPostTracker

🔎 JobPostTracker finds job posts from company career pages, extracts experience requirements with an ML service, and queues interesting jobs for email digests.

---

## Overview

- Batch runner: fetches company pages, extracts links (Node/Puppeteer), diffs against previous snapshots and enqueues added job URLs.
- Inference worker: consumes job tasks, runs a Puppeteer job scraper + extraction pipeline (Node -> Python) that renders the page and calls an experience-extraction model.
- Storage: a local SQLite DB (`state/snapshots.sqlite3`) stores snapshots, diff queue, job tasks, and `job_details` (results + email state).

---

## Key folders

- `node_link_extractor/` — Node Puppeteer link extractor (`getAllLinks.js`).
- `job-alert/puppeteer_scraper/` — Node Puppeteer scraper that extracts job title & text (`puppeteer_scrapper.js`).
- `job-alert/extract_experience.py` — Python wrapper that calls an LLM to extract `min_years` from job text.
- `tracker/` — Python app: batch runner, inference worker, DB layer, and utilities.
- `scripts/` — convenience scripts: seeder, cron runs, etc.
- `state/` — runtime DBs, logs and snapshots.
- `inputs/companies.csv` — list of companies to track (company,url).

---

## What changed recently (safety & stability)

- Puppeteer scripts now:
  - Reuse an in-process Chromium instance where possible (minimizes churn).
  - Create a managed temporary `userDataDir` per process and clean it on exit.
  - Close pages after each scrape and ensure `page.close()` runs in finally blocks.
  - Register signal handlers and `uncaughtException` handlers to attempt cleanup.
  - Add a per-process page semaphore (`PPT_MAX_PAGES`) to bound concurrency.
  - Add a per-scrape watchdog (`PPT_SCRAPE_WATCHDOG_MS`) that forcibly closes stuck pages/browsers.
  - Optionally (opt-in) support a global cross-process slot cap via `PPT_GLOBAL_CHROME_SLOTS` to limit system-wide Chromium instances (disabled by default).

These changes reduce disk accumulation from browser profiles and guard against runaway Chromium processes.

---

## Environment variables

- PPT_MAX_PAGES (default 4) — pages per Node process (semaphore). Set lower to reduce memory per process.
- PPT_SCRAPE_WATCHDOG_MS (default 120000) — per-scrape watchdog timeout in ms.
- PPT_MAX_CHROME_HELPERS (default 15) — soft guard for macOS helper count.
- PPT_GLOBAL_CHROME_SLOTS (default 0 — disabled) — set to positive integer to enable system-wide Chromium instance cap.
- PPT_GLOBAL_SLOT_TIMEOUT_MS (default 30000) — how long to wait for a global slot.
- PPT_GLOBAL_CHROME_SLOTS_DIR — base dir for slot state (defaults to tmp dir)

For `extract_experience.py`, move the OpenAI key to environment:
```bash
export OPENAI_API_KEY="sk-..."
```

---

## Running locally

1. Install Node & Python dependencies:
   - Node (v24+ recommended) and `npm install` inside `node_link_extractor/` and `job-alert/puppeteer_scraper/` if you add packages.
   - Create and activate Python venv and `pip install -r requirements.txt` if present.

2. Seed current snapshots (threaded):
```bash
export PPT_GLOBAL_CHROME_SLOTS=4    # optional
export PPT_MAX_PAGES=2
python -m tracker.seed_current_snapshot_from_csv_threaded \
  --csv ./inputs/companies.csv \
  --db ./state/snapshots.sqlite3 \
  --node-workdir ./node_link_extractor \
  --node-bin $(which node) \
  --node-timeout-seconds 180 \
  --max-workers 8 \
  --clear-current-snapshot-first \
  -v
```

3. Run a batch (single thread for diagnostics):
```bash
python -m tracker.batch_runner --csv inputs/companies.csv --node-workdir ./node_link_extractor --max-workers 1
```

4. Start inference worker:
```bash
python -m tracker.inference_worker --puppeteer-script ./job-alert/puppeteer_scraper/puppeteer_scrapper.js --extract-experience-py ./job-alert/extract_experience.py --verbose
```

5. Cron helper: `./run_all.sh` installs cron entries and can run the set of scripts in foreground for debugging.

---

## Database notes

- Backup before destructive actions:
```bash
cp state/snapshots.sqlite3 state/snapshots.sqlite3.bak.$(date +%Y%m%dT%H%M%S)
```

- Export `job_details` to CSV:
```bash
sqlite3 -header -csv state/snapshots.sqlite3 "SELECT * FROM job_details;" > job_details.csv
```

- Delete all `job_details` (destructive):
```bash
sqlite3 state/snapshots.sqlite3 <<'SQL'
BEGIN;
DELETE FROM job_details;
COMMIT;
VACUUM;
SQL
```

- Diff queue: enqueuing occurs only when added URLs exist and an idempotent unique diff hash is not already present. The batch logs `diff_enqueued=true|false` to show whether a new diff row was created.

---

## Troubleshooting disk / runaway Chromium issues

- If macOS `System Data` balloons, likely causes:
  - Orphaned Chrome helper processes keeping deleted profile files open.
  - Swap or VM files during memory pressure.

- Useful commands:
```bash
pgrep -fl "Google Chrome for Testing Helper"  # list helper PIDs
pkill -f "Google Chrome for Testing Helper"    # gently kill
kill -9 $(pgrep -f "Google Chrome for Testing Helper")  # force kill if necessary
lsof +L1  # show open but deleted files keeping space
du -hd2 / | sort -hr | head -n 30  # find big folders
```

- Long-term: prefer reusing browsers, limit concurrency, mount persistent state off EFS (for Lambda), and add monitoring/alerts for rising process counts and disk usage.

---

## Security (secrets & commits)

If an API key was accidentally committed:
1. Revoke/rotate the key immediately.
2. Remove from history (use `git-filter-repo` or BFG) and force push a cleaned repository mirror.
3. Add pre-commit secret detection tools (`detect-secrets`, `git-secrets`) and move keys to env vars or secret manager.

Example `git-filter-repo` flow is documented in the repo issues and in the project notes.

---

## Deployment notes

- EC2/ECS is simpler and reliable for Puppeteer-heavy workloads; recommended instance size for moderate throughput: `m6i.xlarge` (4 vCPU, 16 GiB). For low-cost testing, `t3.large` or `m5.large` may suffice.
- Lambda can run headless Chromium but requires container images, EFS for persistence, and higher memory (>=4GB) and is more operationally complex.

---

## Development & tests

- Add a small integration test that runs a few scrapes and asserts cleanup of temporary `ppt-userdata-*` directories.
- Consider adding a persistent Node scraping server mode so Python workers can call an HTTP endpoint instead of spawning `node` per URL (big perf win).

---

## Contact / next steps

- If you want, I can:
  - add a small `scripts/verify_cleanup.sh` to run a local stress test and verify `userDataDir` cleanup, or
  - add the persistent worker server mode and change Python to call it for reduced startup overhead.

---

Thank you! 🚀
