# seed_current_snapshot_from_csv.py

"""
Docstring for tracker.seed_current_snapshot_from_csv

USAGE:

python3 seed_current_snapshot_from_csv.py \
  --csv ./companies.csv \
  --db ./state/snapshots.sqlite3 \
  --node-workdir ./node_link_extractor \
  --node-bin node \
  --node-timeout-seconds 180 \
  --clear-current-snapshot-first
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from typing import List, Optional

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
    node_ms: int


def seed_current_snapshot_from_csv(
    *,
    csv_path: str,
    db_path: str,
    node_workdir: str,
    node_bin: str,
    node_timeout_seconds: int,
    clear_current_snapshot_first: bool,
    stop_on_error: bool,
) -> dict:
    targets: List[CompanyTarget] = load_company_targets_csv(csv_path)
    state = SQLiteState(db_path)

    results: List[SeedResult] = []
    ok_count = 0
    fail_count = 0

    try:
        if clear_current_snapshot_first:
            LOG.info("clear_current_snapshot_first=true deleting_current_snapshot")
            state.conn.execute("DELETE FROM current_snapshot;")
            state.conn.commit()

        for idx, t in enumerate(targets, start=1):
            LOG.info("progress %d/%d company=%s", idx, len(targets), t.company)

            try:
                node = fetch_links_or_raise(
                    url=t.url,
                    node_bin=node_bin,
                    node_workdir=node_workdir,
                    timeout_seconds=node_timeout_seconds,
                    log=LOG,
                )

                snapshot = SnapshotRow(
                    site=t.company,
                    url=t.url,
                    ts_ms=now_epoch_ms(),
                    snapshot_hash=snapshot_hash_for_links(node.links),
                    links=node.links,
                )

                state.upsert_snapshot(snapshot)

                ok_count += 1
                results.append(
                    SeedResult(
                        company=t.company,
                        url=t.url,
                        ok=True,
                        error=None,
                        link_count=len(node.links),
                        snapshot_hash=snapshot.snapshot_hash,
                        node_ms=node.node_ms,
                    )
                )

                LOG.info(
                    "seed_done company=%s ok=true node_ms=%d link_count=%d snapshot_hash=%s",
                    t.company,
                    node.node_ms,
                    len(node.links),
                    snapshot.snapshot_hash,
                )

            except Exception as e:
                fail_count += 1
                LOG.exception("seed_failed company=%s url=%s", t.company, t.url)

                results.append(
                    SeedResult(
                        company=t.company,
                        url=t.url,
                        ok=False,
                        error=str(e),
                        link_count=0,
                        snapshot_hash="",
                        node_ms=0,
                    )
                )

                if stop_on_error:
                    LOG.error("stop_on_error=true stopping_seed")
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
    )
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        raise
