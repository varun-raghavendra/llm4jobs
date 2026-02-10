# tracker/inference_worker.py
from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import subprocess
import time
import signal
from urllib.parse import urlparse
from typing import Optional, Tuple

from tracker.db import SQLiteState

LOG = logging.getLogger("tracker.inference_worker")
BLOCKED_HOSTS = {"errors.edgesuite.net"}


def is_http_url(value: str) -> bool:
    try:
        u = urlparse(value.strip())
    except Exception:
        return False
    return u.scheme in ("http", "https")


def should_skip_url(value: str) -> bool:
    if not is_http_url(value):
        return True
    try:
        host = urlparse(value.strip()).hostname or ""
    except Exception:
        return True
    return host in BLOCKED_HOSTS


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def run_pipeline(
    *,
    node_bin: str,
    puppeteer_script: str,
    python_bin: str,
    extract_experience_py: str,
    url: str,
    timeout_seconds: int,
) -> dict:
    """
    Runs:
      node puppeteer_script "url" | python extract_experience.py
    Returns parsed JSON from extract_experience.py stdout.
    """
    p1 = subprocess.Popen(
        [node_bin, puppeteer_script, url],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    p2 = subprocess.Popen(
        [python_bin, extract_experience_py],
        stdin=p1.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if p1.stdout:
        p1.stdout.close()

    # Start processes in their own session so we can kill entire process
    # groups (node may spawn Chrome children that should be terminated).
    # Use start_new_session to create a separate session/process group.
    # Recreate Popen objects with session if not already supported by caller.
    # (On Python >=3.8, start_new_session is supported.)
    # If start_new_session is not available on the platform, fallback to
    # leaving processes as-is and attempting individual kills.
    try:
        p1_pg = subprocess.Popen(
            [node_bin, puppeteer_script, url],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        # connect p2 stdin to p1_pg.stdout
        p2 = subprocess.Popen(
            [python_bin, extract_experience_py],
            stdin=p1_pg.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        if p1_pg.stdout:
            p1_pg.stdout.close()
        p1 = p1_pg
    except TypeError:
        # older Python; fall back to original behaviour
        p1 = subprocess.Popen(
            [node_bin, puppeteer_script, url],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        p2 = subprocess.Popen(
            [python_bin, extract_experience_py],
            stdin=p1.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if p1.stdout:
            p1.stdout.close()

    try:
        out2, err2 = p2.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        # Try to terminate the whole process group for p2 and p1 (if any)
        try:
            # kill p2 process
            p2.kill()
        except Exception:
            pass
        # kill node process group (which includes Chrome children)
        try:
            os.killpg(os.getpgid(p1.pid), signal.SIGTERM)
        except Exception:
            try:
                p1.kill()
            except Exception:
                pass
        # give a moment then escalate
        time.sleep(1)
        try:
            os.killpg(os.getpgid(p1.pid), signal.SIGKILL)
        except Exception:
            try:
                p1.kill()
            except Exception:
                pass
        raise RuntimeError("pipeline_timeout")

    out1, err1 = p1.communicate()

    if p1.returncode != 0:
        raise RuntimeError(f"puppeteer_failed rc={p1.returncode} stderr={(err1 or '')[:800]}")
    if p2.returncode != 0:
        raise RuntimeError(f"extract_experience_failed rc={p2.returncode} stderr={(err2 or '')[:800]}")

    try:
        return json.loads(out2.strip())
    except Exception as e:
        raise RuntimeError(f"invalid_json_from_extract_experience error={e} raw={(out2 or '')[:800]}")


def expand_one_diff(state: SQLiteState, owner: str) -> int:
    row = state.claim_diff_row(owner=owner)
    if not row:
        return 0
    try:
        urls = json.loads(row["added_urls_json"])
        if not isinstance(urls, list):
            urls = []
        urls = [str(u) for u in urls if u]
        urls = [u for u in urls if not should_skip_url(u)]
        inserted = state.add_job_tasks(site=row["site"], urls=urls)
        state.mark_diff_done(row["id"])
        return inserted
    except Exception as e:
        state.mark_diff_failed(row["id"], str(e))
        raise


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="./state/snapshots.sqlite3")
    ap.add_argument("--node-bin", default="node")
    ap.add_argument("--puppeteer-script", required=True)
    ap.add_argument("--python-bin", default="python")
    ap.add_argument("--extract-experience-py", required=True)
    ap.add_argument("--timeout-seconds", type=int, default=120)
    ap.add_argument("--poll-sleep-seconds", type=int, default=2)
    ap.add_argument("--max-jobs-per-run", type=int, default=0, help="0 means infinite loop")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(args.verbose)
    owner = f"{socket.gethostname()}:{os.getpid()}"
    LOG.info("inference_worker_start owner=%s db=%s", owner, args.db)

    processed = 0
    while True:
        state = SQLiteState(args.db)

        try:
            state.reap_stuck_diffs()
            state.reap_stuck_job_tasks()

            inserted = expand_one_diff(state, owner)
            if inserted:
                LOG.info("expanded_diff inserted_tasks=%d", inserted)

            claimed: Optional[Tuple[str, str]] = state.claim_job_task(owner=owner)
            if not claimed:
                state.close()
                time.sleep(args.poll_sleep_seconds)
                continue

            url, site = claimed
        finally:
            try:
                state.close()
            except Exception:
                pass

        if should_skip_url(url):
            LOG.info("job_skipped_invalid_url site=%s url=%s", site, url)
            state = SQLiteState(args.db)
            state.complete_job_task(url)
            state.close()
            continue

        try:
            result = run_pipeline(
                node_bin=args.node_bin,
                puppeteer_script=args.puppeteer_script,
                python_bin=args.python_bin,
                extract_experience_py=args.extract_experience_py,
                url=url,
                timeout_seconds=args.timeout_seconds,
            )

            job_title = str(result.get("job_title", "")).strip()
            min_years = int(result.get("min_years", 0) or 0)
            include_job = min_years < 4
            exclude_reason = None if include_job else "min_years_gte_4"

            state = SQLiteState(args.db)
            state.upsert_job_details(
                url=url,
                site=site,
                job_title=job_title,
                min_years=min_years,
                include_job=include_job,
                exclude_reason=exclude_reason,
                raw_json=result,
            )
            state.complete_job_task(url)
            state.close()

            processed += 1
            LOG.info("job_done site=%s min_years=%d title=%s", site, min_years, job_title[:80])

        except Exception as e:
            LOG.exception("job_failed site=%s url=%s", site, url)
            state = SQLiteState(args.db)
            state.fail_job_task(url, str(e), backoff_ms=30_000)
            state.close()

        if args.max_jobs_per_run and processed >= args.max_jobs_per_run:
            LOG.info("max_jobs_per_run_reached count=%d", processed)
            return


if __name__ == "__main__":
    main()
    
