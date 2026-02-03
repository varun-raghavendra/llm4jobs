# tracker/batch_runner.py

"""
Docstring for tracker.batch_runner

USAGE:

python -m tracker.batch_runner \
  --csv ./inputs/companies.csv \
  --node-workdir ./node_link_extractor \
  --db ./state/snapshots.sqlite3

"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import List, Optional

from tracker.config_loader import CompanyTarget, load_company_targets_csv
from tracker.db import SQLiteState, SnapshotRow, now_epoch_ms
from tracker.diffing import build_diff_payload, diff_links
from tracker.run_common import fetch_links_or_raise, setup_logging, snapshot_hash_for_links, json_sample

LOG = logging.getLogger("tracker.batch_runner")


@dataclass(frozen=True)
class CompanyRunResult:
    company: str
    url: str
    ok: bool
    error: Optional[str]
    old_link_count: int
    new_link_count: int
    added_url_count: int
    diff_enqueued: bool
    node_ms: int
    total_ms: int


def run_company_once(
    *,
    state: SQLiteState,
    company: str,
    url: str,
    node_workdir: str,
    node_bin: str,
    node_timeout_seconds: int,
) -> CompanyRunResult:
    t0 = time.perf_counter()

    old_links = state.get_current_links(company) or []
    LOG.info("company_start company=%s url=%s old_link_count=%d", company, url, len(old_links))

    node = fetch_links_or_raise(
        url=url,
        node_bin=node_bin,
        node_workdir=node_workdir,
        timeout_seconds=node_timeout_seconds,
        log=LOG,
    )

    new_links = node.links
    added_links, removed_links = diff_links(old_links, new_links)

    diff_payload = build_diff_payload(company, added_links)

    diff_enqueued = False
    if diff_payload.added_urls:
        diff_enqueued = state.enqueue_diff(
            site=company,
            diff_hash=diff_payload.diff_hash,
            added_urls=diff_payload.added_urls,
        )

    snapshot = SnapshotRow(
        site=company,
        url=url,
        ts_ms=now_epoch_ms(),
        snapshot_hash=snapshot_hash_for_links(new_links),
        links=new_links,
    )
    state.upsert_snapshot(snapshot)

    total_ms = int((time.perf_counter() - t0) * 1000)

    LOG.info(
        "company_done company=%s ok=true total_ms=%d node_ms=%d new_link_count=%d added=%d removed=%d diff_enqueued=%s",
        company,
        total_ms,
        node.node_ms,
        len(new_links),
        len(added_links),
        len(removed_links),
        str(diff_enqueued).lower(),
    )

    if LOG.isEnabledFor(logging.DEBUG) and added_links:
        LOG.debug("company_added_sample company=%s sample=%s", company, json_sample(added_links, 10))

    return CompanyRunResult(
        company=company,
        url=url,
        ok=True,
        error=None,
        old_link_count=len(old_links),
        new_link_count=len(new_links),
        added_url_count=len(diff_payload.added_urls),
        diff_enqueued=diff_enqueued,
        node_ms=node.node_ms,
        total_ms=total_ms,
    )


def run_batch(
    *,
    csv_path: str,
    db_path: str,
    node_workdir: str,
    node_bin: str,
    node_timeout_seconds: int,
    stop_on_error: bool,
) -> dict:
    started_ms = now_epoch_ms()
    t0 = time.perf_counter()

    LOG.info(
        "batch_start csv_path=%s db_path=%s node_workdir=%s node_bin=%s node_timeout_seconds=%d stop_on_error=%s",
        csv_path,
        db_path,
        node_workdir,
        node_bin,
        node_timeout_seconds,
        str(stop_on_error).lower(),
    )

    targets: List[CompanyTarget] = load_company_targets_csv(csv_path)
    LOG.info("targets_loaded count=%d", len(targets))

    state = SQLiteState(db_path)

    results: List[CompanyRunResult] = []
    ok_count = 0
    fail_count = 0

    try:
        for idx, t in enumerate(targets, start=1):
            LOG.info("progress %d/%d company=%s", idx, len(targets), t.company)
            try:
                r = run_company_once(
                    state=state,
                    company=t.company,
                    url=t.url,
                    node_workdir=node_workdir,
                    node_bin=node_bin,
                    node_timeout_seconds=node_timeout_seconds,
                )
                results.append(r)
                ok_count += 1
            except Exception:
                fail_count += 1
                LOG.exception("company_failed company=%s url=%s", t.company, t.url)

                results.append(
                    CompanyRunResult(
                        company=t.company,
                        url=t.url,
                        ok=False,
                        error="see logs for traceback",
                        old_link_count=0,
                        new_link_count=0,
                        added_url_count=0,
                        diff_enqueued=False,
                        node_ms=0,
                        total_ms=0,
                    )
                )

                if stop_on_error:
                    LOG.error("stop_on_error=true stopping_batch")
                    break
    finally:
        state.close()
        LOG.debug("db_closed path=%s", db_path)

    ended_ms = now_epoch_ms()
    duration_ms = ended_ms - started_ms
    wall_ms = int((time.perf_counter() - t0) * 1000)

    LOG.info(
        "batch_done total=%d ok=%d fail=%d duration_ms=%d wall_ms=%d",
        len(targets),
        ok_count,
        fail_count,
        duration_ms,
        wall_ms,
    )

    return {
        "csv_path": csv_path,
        "company_count_total": len(targets),
        "company_ok_count": ok_count,
        "company_fail_count": fail_count,
        "started_ts_ms": started_ms,
        "ended_ts_ms": ended_ms,
        "duration_ms": duration_ms,
        "results": [r.__dict__ for r in results],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="CSV with company,url columns")
    ap.add_argument("--db", default="./state/snapshots.sqlite3")
    ap.add_argument("--node-workdir", required=True, help="Path to folder containing index.js")
    ap.add_argument("--node-bin", default="node")
    ap.add_argument("--node-timeout-seconds", type=int, default=180)
    ap.add_argument("--stop-on-error", action="store_true")
    ap.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase logging verbosity (use -v for DEBUG)",
    )

    args = ap.parse_args()
    setup_logging(args.verbose)

    LOG.debug("env node_bin=%s cwd=%s pid=%s", args.node_bin, os.getcwd(), os.getpid())

    report = run_batch(
        csv_path=args.csv,
        db_path=args.db,
        node_workdir=args.node_workdir,
        node_bin=args.node_bin,
        node_timeout_seconds=args.node_timeout_seconds,
        stop_on_error=args.stop_on_error,
    )
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
