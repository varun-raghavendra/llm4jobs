# tracker/runner.py
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import List

from tracker.db import SQLiteState, SnapshotRow, now_epoch_ms
from tracker.diffing import (
    build_diff_payload,
    dedupe_preserve_order,
    diff_links,
    sha256_hex,
    stable_json_dumps,
)
from tracker.node_client import fetch_links_via_node


@dataclass(frozen=True)
class RunSummary:
    site: str
    url: str
    old_link_count: int
    new_link_count: int
    added_link_count: int
    snapshot_written: bool
    diff_enqueued: bool


def snapshot_hash_for_links(links: List[str]) -> str:
    return sha256_hex(stable_json_dumps(links))


def run_once(
    *,
    site: str,
    url: str,
    db_path: str,
    node_workdir: str,
    node_bin: str,
    node_timeout_seconds: int,
) -> RunSummary:
    state = SQLiteState(db_path)
    try:
        old_links = state.get_current_links(site) or []

        node_result = fetch_links_via_node(
            node_bin=node_bin,
            node_workdir=node_workdir,
            url=url,
            timeout_seconds=node_timeout_seconds,
        )
        if node_result.returncode != 0:
            raise RuntimeError(
                "Node extractor failed\n"
                + f"returncode={node_result.returncode}\n"
                + f"stderr={node_result.raw_stderr}"
            )

        new_links = dedupe_preserve_order(node_result.links)

        added_links, _removed_links = diff_links(old_links, new_links)

        diff_payload = build_diff_payload(site, added_links)

        diff_enqueued = False
        if diff_payload.added_urls:
            diff_enqueued = state.enqueue_diff(
                site=site,
                diff_hash=diff_payload.diff_hash,
                added_urls=diff_payload.added_urls,
            )

        snapshot = SnapshotRow(
            site=site,
            url=url,
            ts_ms=now_epoch_ms(),
            snapshot_hash=snapshot_hash_for_links(new_links),
            links=new_links,
        )
        state.upsert_snapshot(snapshot)

        return RunSummary(
            site=site,
            url=url,
            old_link_count=len(old_links),
            new_link_count=len(new_links),
            added_link_count=len(added_links),
            snapshot_written=True,
            diff_enqueued=diff_enqueued,
        )
    finally:
        state.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--site", required=True)
    ap.add_argument("--url", required=True)
    ap.add_argument("--db", default="./state/snapshots.sqlite3")
    ap.add_argument("--node-workdir", required=True, help="Path to folder containing index.js")
    ap.add_argument("--node-bin", default="node")
    ap.add_argument("--node-timeout-seconds", type=int, default=180)
    args = ap.parse_args()

    summary = run_once(
        site=args.site,
        url=args.url,
        db_path=args.db,
        node_workdir=args.node_workdir,
        node_bin=args.node_bin,
        node_timeout_seconds=args.node_timeout_seconds,
    )
    print(json.dumps(summary.__dict__, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
