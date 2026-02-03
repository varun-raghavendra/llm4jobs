# seed_current_snapshot_from_csv.py

"""
Seeds the current_snapshot table from a CSV of companies.

For each company URL:
- Runs the Node link extractor
- Deduplicates links
- Writes a full replacement snapshot for that company
- Does NOT compute diffs or enqueue jobs

Supports parallel fetching with a bounded thread pool.
SQLite writes are serialized for safety.

USAGE:

python3 seed_current_snapshot_from_csv.py \
  --csv ./companies.csv \
  --db ./state/snapshots.sqlite3 \
  --node-workdir ./node_link_extractor \
  --node-bin node \
  --node-timeout-seconds 180 \
  --max-workers 4 \
  --clear-current-snapshot-first

FLAGS:

--csv PATH
    CSV file with columns: company,url

--db PATH
    Path to SQLite state database (default: ./state/snapshots.sqlite3)

--node-workdir PATH
    Directory containing Node extractor (index.js)

--node-bin BIN
    Node binary to use (default: node)

--node-timeout-seconds SECONDS
    Per-company timeout for Node extraction (default: 180)

--max-workers N
    Number of parallel worker threads (default: 4)

--clear-current-snapshot-first
    If set, DELETE FROM current_snapshot before seeding

--stop-on-error
    Stop immediately if any company fails

-v, --verbose
    Enable debug logging
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Optional, Tuple

from tracker.config_loader import CompanyTarget, load_company_targets_csv
from tracker.db import SQLiteState, SnapshotRow, now_epoch_ms
from tracker.run_common import fetch_links_or_raise, setup_logging, snapshot_hash_for_links

LOG = logging.getLogger("tracker.seed")


@dataclass(frozen=True)
class SeedResult:
    company: str
    url: str
    ok: bool
    error: Optional[str]
    link_count: int
    snapshot_hash: str


def seed_current_snapshot_from_csv(
    *,
    csv_path: str,
    db_path: str,
    node_workdir: str,
    node_bin: str,
    node_timeout_seconds: int,
    clear_current_snapshot_first: bool,
    stop_on_error: bool,
    max_workers: int,
) -> dict:
    targets: List[CompanyTarget] = load_company_targets_csv(csv_path)
    state = SQLiteState(db_path)
    db_lock = threading.Lock()

    results: List[SeedResult] = []
    ok_count = 0
    fail_count = 0

    try:
        if clear_current_snapshot_first:
            with db_lock:
                LOG.info("clear_current_snapshot_first=true deleting_current_snapshot")
                state.conn.execute("DELETE FROM current_snapshot;")
                state.conn.commit()

        def worker(t: CompanyTarget) -> Tuple[CompanyTarget, List[str], int]:
            node = fetch_links_or_raise(
                url=t.url,
                node_bin=node_bin,
                node_workdir=node_workdir,
                timeout_seconds=node_timeout_seconds,
                log=LOG,
            )
            return t, node.links, node.node_ms

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(worker, t) for t in targets]

            for fut in as_completed(futures):
                try:
                    t, links, node_ms = fut.result()

                    snapshot = SnapshotRow(
                        site=t.company,
                        url=t.url,
                        ts_ms=now_epoch_ms(),
                        snapshot_hash=snapshot_hash_for_links(links),
                        links=links,
                    )

                    with db_lock:
                        state.upsert_snapshot(snapshot)

                    ok_count += 1
                    results.append(
                        SeedResult(
                            company=t.company,
                            url=t.url,
                            ok=True,
                            error=None,
                            link_count=len(links),
                            snapshot_hash=snapshot.snapshot_hash,
                        )
                    )

                    LOG.info(
                        "seed_done company=%s ok=true node_ms=%d link_count=%d snapshot_hash=%s",
                        t.company,
                        node_ms,
                        len(links),
                        snapshot.snapshot_hash,
                    )

                except Exception as e:
                    fail_count += 1
                    LOG.exception("seed_failed")

                    results.append(
                        SeedResult(
                            company="",
                            url="",
                            ok=False,
                            error=str(e),
                            link_count=0,
                            snapshot_hash="",
                        )
                    )

                    if stop_on_error:
                        LOG.error("stop_on_error=true cancelling_pending")
                        for f in futures:
                            f.cancel()
                        break

    finally:
        state.close()

    return {
        "csv_path": csv_path,
        "db_path": db_path,
        "clear_current_snapshot_first": clear_current_snapshot_first,
        "company_count_total": len(targets),
        "company_ok_count": ok_count,
        "company_fail_count": fail_count,
        "results": [r.__dict__ for r in results],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="CSV with company,url columns")
    ap.add_argument("--db", default="./state/snapshots.sqlite3")
    ap.add_argument("--node-workdir", required=True, help="Folder containing index.js")
    ap.add_argument("--node-bin", default="node")
    ap.add_argument("--node-timeout-seconds", type=int, default=180)
    ap.add_argument(
        "--clear-current-snapshot-first",
        action="store_true",
        help="If set, DELETE FROM current_snapshot then repopulate from CSV",
    )
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

    report = seed_current_snapshot_from_csv(
        csv_path=args.csv,
        db_path=args.db,
        node_workdir=args.node_workdir,
        node_bin=args.node_bin,
        node_timeout_seconds=args.node_timeout_seconds,
        clear_current_snapshot_first=args.clear_current_snapshot_first,
        stop_on_error=args.stop_on_error,
        max_workers=args.max_workers,
    )
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        raise
