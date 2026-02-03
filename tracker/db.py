# tracker/db.py
from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple


def now_epoch_ms() -> int:
    return int(time.time() * 1000)


def stable_json_dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class SnapshotRow:
    site: str
    url: str
    ts_ms: int
    snapshot_hash: str
    links: List[str]


class SQLiteState:
    """
    One durable sqlite file
    - snapshots table for history
    - current_snapshot table for fast lookup of last good snapshot per site
    - diff_queue as an append only queue with idempotency

    Recovery
    - If a run crashes mid way, last committed snapshot remains valid
    - Queue is append only, status changes are updates
    - Idempotency avoids duplicate diffs
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(db_path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def close(self) -> None:
        self.conn.close()

    def _init_db(self) -> None:
        cur = self.conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA temp_store=MEMORY;")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshots (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              site TEXT NOT NULL,
              url TEXT NOT NULL,
              ts_ms INTEGER NOT NULL,
              snapshot_hash TEXT NOT NULL,
              links_json TEXT NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_snapshots_site_ts
            ON snapshots(site, ts_ms);
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS current_snapshot (
              site TEXT PRIMARY KEY,
              url TEXT NOT NULL,
              ts_ms INTEGER NOT NULL,
              snapshot_hash TEXT NOT NULL,
              links_json TEXT NOT NULL
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS diff_queue (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              site TEXT NOT NULL,
              created_ts_ms INTEGER NOT NULL,
              diff_hash TEXT NOT NULL,
              added_urls_json TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'PENDING',
              attempts INTEGER NOT NULL DEFAULT 0,
              last_error TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_diff_queue_site_hash
            ON diff_queue(site, diff_hash);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_diff_queue_status_created
            ON diff_queue(status, created_ts_ms);
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_tasks (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              site TEXT NOT NULL,
              url TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'PENDING',
              created_ts_ms INTEGER NOT NULL,
              updated_ts_ms INTEGER NOT NULL,
              owner TEXT,
              attempts INTEGER NOT NULL DEFAULT 0,
              last_error TEXT,
              backoff_until_ms INTEGER
            );
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_job_tasks_site_url
            ON job_tasks(site, url);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_job_tasks_status_created
            ON job_tasks(status, created_ts_ms);
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_details (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              site TEXT NOT NULL,
              url TEXT NOT NULL,
              job_title TEXT,
              min_years INTEGER NOT NULL DEFAULT 0,
              include_job INTEGER NOT NULL DEFAULT 1,
              exclude_reason TEXT,
              raw_json TEXT,
              created_ts_ms INTEGER NOT NULL,
              updated_ts_ms INTEGER NOT NULL,
              emailed_ts_ms INTEGER,
              digest_id TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_job_details_site_url
            ON job_details(site, url);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_job_details_email
            ON job_details(include_job, emailed_ts_ms, created_ts_ms);
            """
        )

        self.conn.commit()

        # Add missing columns for older DBs.
        self._ensure_column("diff_queue", "owner", "TEXT")
        self._ensure_column("diff_queue", "claimed_ts_ms", "INTEGER")
        self._ensure_column("diff_queue", "updated_ts_ms", "INTEGER")
        self._ensure_column("diff_queue", "backoff_until_ms", "INTEGER")

    def _ensure_column(self, table: str, column: str, col_type: str) -> None:
        cur = self.conn.cursor()
        cur.execute(f"PRAGMA table_info({table});")
        cols = {row["name"] for row in cur.fetchall()}
        if column in cols:
            return
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type};")
        self.conn.commit()

    def get_current_links(self, site: str) -> Optional[List[str]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT links_json
            FROM current_snapshot
            WHERE site = ?;
            """,
            (site,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return json.loads(row["links_json"])

    def upsert_snapshot(self, snapshot: SnapshotRow) -> None:
        """
        Transactional update
        - Always append to snapshots history
        - Update current_snapshot to point to latest
        """
        cur = self.conn.cursor()
        cur.execute("BEGIN;")
        try:
            cur.execute(
                """
                INSERT INTO snapshots(site, url, ts_ms, snapshot_hash, links_json)
                VALUES(?, ?, ?, ?, ?);
                """,
                (
                    snapshot.site,
                    snapshot.url,
                    snapshot.ts_ms,
                    snapshot.snapshot_hash,
                    stable_json_dumps(snapshot.links),
                ),
            )
            cur.execute(
                """
                INSERT INTO current_snapshot(site, url, ts_ms, snapshot_hash, links_json)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(site) DO UPDATE SET
                  url=excluded.url,
                  ts_ms=excluded.ts_ms,
                  snapshot_hash=excluded.snapshot_hash,
                  links_json=excluded.links_json;
                """,
                (
                    snapshot.site,
                    snapshot.url,
                    snapshot.ts_ms,
                    snapshot.snapshot_hash,
                    stable_json_dumps(snapshot.links),
                ),
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def enqueue_diff(
        self,
        *,
        site: str,
        diff_hash: str,
        added_urls: Sequence[str],
    ) -> bool:
        """
        Returns True if enqueued, False if already exists due to idempotency.
        """
        cur = self.conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO diff_queue(site, created_ts_ms, diff_hash, added_urls_json)
                VALUES(?, ?, ?, ?);
                """,
                (
                    site,
                    now_epoch_ms(),
                    diff_hash,
                    stable_json_dumps(list(added_urls)),
                ),
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            self.conn.rollback()
            return False

    def fetch_pending(self, limit: int = 50) -> List[dict]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT id, site, created_ts_ms, diff_hash, added_urls_json, status, attempts, last_error
            FROM diff_queue
            WHERE status = 'PENDING'
            ORDER BY created_ts_ms ASC
            LIMIT ?;
            """,
            (limit,),
        )
        rows = cur.fetchall()
        out: List[dict] = []
        for r in rows:
            out.append(
                {
                    "id": r["id"],
                    "site": r["site"],
                    "created_ts_ms": r["created_ts_ms"],
                    "diff_hash": r["diff_hash"],
                    "added_urls": json.loads(r["added_urls_json"]),
                    "status": r["status"],
                    "attempts": r["attempts"],
                    "last_error": r["last_error"],
                }
            )
        return out

    def clear_diff_queue(self) -> int:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM diff_queue;")
        self.conn.commit()
        return cur.rowcount

    def reap_stuck_diffs(self, timeout_ms: int = 10 * 60 * 1000) -> int:
        now = now_epoch_ms()
        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE diff_queue
            SET status = 'PENDING',
                owner = NULL,
                updated_ts_ms = ?,
                claimed_ts_ms = NULL
            WHERE status = 'IN_PROGRESS'
              AND claimed_ts_ms IS NOT NULL
              AND claimed_ts_ms <= ?;
            """,
            (now, now - timeout_ms),
        )
        self.conn.commit()
        return cur.rowcount

    def claim_diff_row(self, owner: str) -> Optional[sqlite3.Row]:
        now = now_epoch_ms()
        cur = self.conn.cursor()
        cur.execute("BEGIN IMMEDIATE;")
        try:
            cur.execute(
                """
                SELECT *
                FROM diff_queue
                WHERE status = 'PENDING'
                  AND (backoff_until_ms IS NULL OR backoff_until_ms <= ?)
                ORDER BY created_ts_ms ASC
                LIMIT 1;
                """,
                (now,),
            )
            row = cur.fetchone()
            if not row:
                self.conn.commit()
                return None

            cur.execute(
                """
                UPDATE diff_queue
                SET status = 'IN_PROGRESS',
                    owner = ?,
                    claimed_ts_ms = ?,
                    updated_ts_ms = ?,
                    attempts = attempts + 1
                WHERE id = ? AND status = 'PENDING';
                """,
                (owner, now, now, row["id"]),
            )
            self.conn.commit()
            if cur.rowcount != 1:
                return None
            return row
        except Exception:
            self.conn.rollback()
            raise

    def mark_diff_done(self, diff_id: int) -> None:
        now = now_epoch_ms()
        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE diff_queue
            SET status = 'DONE',
                updated_ts_ms = ?
            WHERE id = ?;
            """,
            (now, diff_id),
        )
        self.conn.commit()

    def mark_diff_failed(self, diff_id: int, error: str, backoff_ms: int = 30_000) -> None:
        now = now_epoch_ms()
        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE diff_queue
            SET status = 'PENDING',
                last_error = ?,
                backoff_until_ms = ?,
                updated_ts_ms = ?
            WHERE id = ?;
            """,
            (error, now + backoff_ms, now, diff_id),
        )
        self.conn.commit()

    def add_job_tasks(self, *, site: str, urls: Sequence[str]) -> int:
        if not urls:
            return 0
        now = now_epoch_ms()
        cur = self.conn.cursor()
        before = self.conn.total_changes
        cur.executemany(
            """
            INSERT OR IGNORE INTO job_tasks(site, url, status, created_ts_ms, updated_ts_ms)
            VALUES(?, ?, 'PENDING', ?, ?);
            """,
            [(site, str(u), now, now) for u in urls],
        )
        self.conn.commit()
        return self.conn.total_changes - before

    def reap_stuck_job_tasks(self, timeout_ms: int = 10 * 60 * 1000) -> int:
        now = now_epoch_ms()
        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE job_tasks
            SET status = 'PENDING',
                owner = NULL,
                updated_ts_ms = ?
            WHERE status = 'IN_PROGRESS'
              AND updated_ts_ms <= ?;
            """,
            (now, now - timeout_ms),
        )
        self.conn.commit()
        return cur.rowcount

    def claim_job_task(self, owner: str) -> Optional[Tuple[str, str]]:
        now = now_epoch_ms()
        cur = self.conn.cursor()
        cur.execute("BEGIN IMMEDIATE;")
        try:
            cur.execute(
                """
                SELECT id, url, site
                FROM job_tasks
                WHERE status IN ('PENDING', 'FAILED')
                  AND (backoff_until_ms IS NULL OR backoff_until_ms <= ?)
                ORDER BY created_ts_ms ASC
                LIMIT 1;
                """,
                (now,),
            )
            row = cur.fetchone()
            if not row:
                self.conn.commit()
                return None

            cur.execute(
                """
                UPDATE job_tasks
                SET status = 'IN_PROGRESS',
                    owner = ?,
                    updated_ts_ms = ?,
                    attempts = attempts + 1
                WHERE id = ? AND status IN ('PENDING', 'FAILED');
                """,
                (owner, now, row["id"]),
            )
            self.conn.commit()
            if cur.rowcount != 1:
                return None
            return (row["url"], row["site"])
        except Exception:
            self.conn.rollback()
            raise

    def complete_job_task(self, url: str) -> None:
        now = now_epoch_ms()
        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE job_tasks
            SET status = 'DONE',
                updated_ts_ms = ?
            WHERE url = ?;
            """,
            (now, url),
        )
        self.conn.commit()

    def fail_job_task(self, url: str, error: str, backoff_ms: int = 30_000) -> None:
        now = now_epoch_ms()
        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE job_tasks
            SET status = 'FAILED',
                last_error = ?,
                backoff_until_ms = ?,
                updated_ts_ms = ?
            WHERE url = ?;
            """,
            (error, now + backoff_ms, now, url),
        )
        self.conn.commit()

    def upsert_job_details(
        self,
        *,
        url: str,
        site: str,
        job_title: str,
        min_years: int,
        include_job: bool,
        exclude_reason: Optional[str],
        raw_json: Dict[str, Any],
    ) -> None:
        now = now_epoch_ms()
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO job_details(
              site, url, job_title, min_years, include_job, exclude_reason, raw_json,
              created_ts_ms, updated_ts_ms
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(site, url) DO UPDATE SET
              job_title=excluded.job_title,
              min_years=excluded.min_years,
              include_job=excluded.include_job,
              exclude_reason=excluded.exclude_reason,
              raw_json=excluded.raw_json,
              updated_ts_ms=excluded.updated_ts_ms;
            """,
            (
                site,
                url,
                job_title,
                int(min_years),
                1 if include_job else 0,
                exclude_reason,
                json.dumps(raw_json, ensure_ascii=False),
                now,
                now,
            ),
        )
        self.conn.commit()

    def list_jobs_ready_for_email(self, limit: int = 200) -> List[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT site, url, job_title, min_years, created_ts_ms
            FROM job_details
            WHERE min_years < 4
              AND emailed_ts_ms IS NULL
            ORDER BY created_ts_ms DESC
            LIMIT ?;
            """,
            (limit,),
        )
        rows = cur.fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "site": r["site"],
                    "url": r["url"],
                    "job_title": r["job_title"] or "",
                    "min_years": int(r["min_years"]),
                    "created_ts_ms": r["created_ts_ms"],
                }
            )
        return out

    def mark_jobs_emailed(self, *, urls: Sequence[str], digest_id: str) -> int:
        if not urls:
            return 0
        now = now_epoch_ms()
        cur = self.conn.cursor()
        before = self.conn.total_changes
        cur.executemany(
            """
            UPDATE job_details
            SET emailed_ts_ms = ?,
                digest_id = ?
            WHERE url = ? AND emailed_ts_ms IS NULL;
            """,
            [(now, digest_id, str(u)) for u in urls],
        )
        self.conn.commit()
        return self.conn.total_changes - before
