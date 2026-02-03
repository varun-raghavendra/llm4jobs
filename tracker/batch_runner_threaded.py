# tracker/batch_runner.py
from __future__ import annotations

import argparse
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Optional, Tuple

from tracker.config_loader import CompanyTarget, load_company_targets_csv
from tracker.db import SQLiteState, SnapshotRow, now_epoch_ms
from tracker.diffing import build_diff_payload, diff_links
from tracker.run_common import fetch_links_or_raise, json_sample, setup_logging, snapshot_hash_for_links

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


def _compute_delta_no_writes(
    *,
    db_path: str,
    db_lock: threading.Lock,
    company: str,
    url: str,
    node_workdir: str,
    node_bin: str,
    node_timeout_seconds: int,
) -> Tuple[List[str], List[str], List[str], int, int]:
    """
    Returns:
    new_links, added_links, removed_links, old_count, node_ms
    """
    # Read snapshot with a connection created in THIS worker thread.
    # Lock is optional for reads, but it avoids read versus write contention.
    with db_lock:
        state = SQLiteState(db_path)
        try:
            old_links = state.get_current_links(company) or []
        finally:
            state.close()

    old_count = len(old_links)

    node = fetch_links_or_raise(
        url=url,
        node_bin=node_bin,
        node_workdir=node_workdir,
        timeout_seconds=node_timeout_seconds,
        log=LOG,
    )
    new_links = node.links

    added_links, removed_links = diff_links(old_links, new_links)
    return new_links, added_links, removed_links, old_count, node.node_ms


def _commit_writes(
    *,
    db_path: str,
    company: str,
    url: str,
    new_links: List[str],
    added_links: List[str],
) -> Tuple[bool, int]:
    """
    Returns diff_enqueued, added_url_count
    """
    state = SQLiteState(db_path)
    try:
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

        return diff_enqueued, len(diff_payload.added_urls)
    finally:
        state.close()


def run_batch(
    *,
    csv_path: str,
    db_path: str,
    node_workdir: str,
    node_bin: str,
    node_timeout_seconds: int,
    stop_on_error: bool,
    max_workers: int,
) -> dict:
    started_ms = now_epoch_ms()
    batch_t0 = time.perf_counter()

    LOG.info(
        "batch_start csv_path=%s db_path=%s node_workdir=%s node_bin=%s node_timeout_seconds=%d stop_on_error=%s max_workers=%d",
        csv_path,
        db_path,
        node_workdir,
        node_bin,
        node_timeout_seconds,
        str(stop_on_error).lower(),
        max_workers,
    )

    targets: List[CompanyTarget] = load_company_targets_csv(csv_path)
    LOG.info("targets_loaded count=%d", len(targets))

    # Serialize DB writes (and optionally reads) across threads.
    db_lock = threading.Lock()

    results: List[CompanyRunResult] = []
    ok_count = 0
    fail_count = 0

    def worker(t: CompanyTarget) -> CompanyRunResult:
        t0 = time.perf_counter()
        LOG.info("company_start company=%s url=%s", t.company, t.url)

        try:
            new_links, added_links, removed_links, old_count, node_ms = _compute_delta_no_writes(
                db_path=db_path,
                db_lock=db_lock,
                company=t.company,
                url=t.url,
                node_workdir=node_workdir,
                node_bin=node_bin,
                node_timeout_seconds=node_timeout_seconds,
            )

            # Commit under lock so enqueue_diff + upsert_snapshot is atomic with respect to other threads.
            with db_lock:
                diff_enqueued, added_url_count = _commit_writes(
                    db_path=db_path,
                    company=t.company,
                    url=t.url,
                    new_links=new_links,
                    added_links=added_links,
                )

            total_ms = int((time.perf_counter() - t0) * 1000)

            LOG.info(
                "company_done company=%s ok=true total_ms=%d node_ms=%d old=%d new=%d added=%d removed=%d diff_enqueued=%s",
                t.company,
                total_ms,
                node_ms,
                old_count,
                len(new_links),
                added_url_count,
                len(removed_links),
                str(diff_enqueued).lower(),
            )

            if LOG.isEnabledFor(logging.DEBUG) and added_links:
                LOG.debug(
                    "company_added_sample company=%s sample=%s",
                    t.company,
                    json_sample(added_links, 10),
                )

            return CompanyRunResult(
                company=t.company,
                url=t.url,
                ok=True,
                error=None,
                old_link_count=old_count,
                new_link_count=len(new_links),
                added_url_count=added_url_count,
                diff_enqueued=diff_enqueued,
            )
        except Exception as e:
            LOG.exception("company_failed company=%s url=%s", t.company, t.url)
            return CompanyRunResult(
                company=t.company,
                url=t.url,
                ok=False,
                error=str(e),
                old_link_count=0,
                new_link_count=0,
                added_url_count=0,
                diff_enqueued=False,
            )

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(worker, t) for t in targets]

        for fut in as_completed(futures):
            r = fut.result()
            results.append(r)

            if r.ok:
                ok_count += 1
            else:
                fail_count += 1
                if stop_on_error:
                    LOG.error("stop_on_error=true cancelling_pending")
                    for f in futures:
                        f.cancel()
                    break

    ended_ms = now_epoch_ms()
    duration_ms = ended_ms - started_ms
    wall_ms = int((time.perf_counter() - batch_t0) * 1000)

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
        "--max-workers",
        type=int,
        default=4,
        help="Thread pool size",
    )

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
        max_workers=args.max_workers,
    )
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
