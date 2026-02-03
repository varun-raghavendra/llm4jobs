#!/usr/bin/env python3
"""
Recreate `state/emailed_jobs.csv` normalising the first column to ISO8601
`emailed_at` datetimes (UTC). Reads existing CSV at EMAILED_JOBS_CSV (default
`./state/emailed_jobs.csv`) and rewrites it with header:

emailed_at,site,url,job_title,min_years,created_ts_ms,digest_id

- If the file doesn't exist, exits with status 1.
- If the timestamp is numeric (epoch ms), converts to ISO8601 UTC.
- If already ISO, keeps it.

Usage:
  ./scripts/recreate_emailed_jobs_csv.py [--csv PATH]

"""
from __future__ import annotations

import argparse
import csv
import datetime
import os
import sys
from typing import List, Dict


def parse_timestamp(val: str) -> str:
    val = (val or "").strip()
    if not val:
        return ""
    # numeric epoch milliseconds
    try:
        if val.isdigit():
            ms = int(val)
            dt = datetime.datetime.utcfromtimestamp(ms / 1000.0)
            return dt.replace(microsecond=0).isoformat() + "Z"
    except Exception:
        pass
    # try parse common ISO formats
    try:
        # let datetime parse as much as possible
        parsed = datetime.datetime.fromisoformat(val.replace("Z", "+00:00"))
        return parsed.astimezone(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        # fallback: keep original
        return val


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=os.getenv("EMAILED_JOBS_CSV", "./state/emailed_jobs.csv"))
    args = ap.parse_args()

    csv_path = args.csv
    if not os.path.exists(csv_path):
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        return 1

    tmp_path = csv_path + ".tmp"
    rows: List[Dict[str, str]] = []

    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header is None:
            print("Empty CSV, nothing to do", file=sys.stderr)
            return 1
        # Normalize header names to lower-case
        header_lc = [h.strip().lower() for h in header]

        # Determine timestamp column index (prefer emailed_at, emailed_ts_ms, emailed_at, or first col)
        if "emailed_at" in header_lc:
            ts_idx = header_lc.index("emailed_at")
        elif "emailed_ts_ms" in header_lc:
            ts_idx = header_lc.index("emailed_ts_ms")
        elif "emailed_date" in header_lc and "emailed_time" in header_lc:
            # already split; we'll combine
            date_idx = header_lc.index("emailed_date")
            time_idx = header_lc.index("emailed_time")
            ts_idx = None
        else:
            ts_idx = 0

        for r in reader:
            # pad row if short
            while len(r) < len(header_lc):
                r.append("")
            if 'date_idx' in locals() and 'time_idx' in locals():
                date_raw = r[date_idx]
                time_raw = r[time_idx]
                # try to parse combined
                ts_dt = None
                if date_raw:
                    try:
                        ts_str = f"{date_raw} {time_raw}"
                        ts_dt = datetime.datetime.fromisoformat(date_raw)
                    except Exception:
                        ts_dt = None
                # fallback to numeric parse if needed
                if ts_dt is None and ts_idx is not None:
                    ts_raw = r[ts_idx]
                    ts_norm = parse_timestamp(ts_raw)
                    r_ts = ts_norm
                else:
                    r_ts = f"{date_raw} {time_raw}".strip()
                r[0:len(r)] = r  # keep row
                rows.append((r, r_ts))
            else:
                ts_raw = r[ts_idx] if ts_idx is not None else ""
                ts_norm = parse_timestamp(ts_raw)
                rows.append((r, ts_norm))

        # write normalized CSV with canonical header
        canonical_header = ["emailed_date", "emailed_time", "site", "url", "job_title", "min_years"]

        with open(tmp_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(canonical_header)
            for r, ts_norm in rows:
                # ts_norm expected to be ISO datetime or empty
                if ts_norm:
                    try:
                        parsed = datetime.datetime.fromisoformat(ts_norm.replace("Z", "+00:00"))
                        # convert to Mountain Time
                        try:
                            from zoneinfo import ZoneInfo
                            mt = parsed.astimezone(ZoneInfo("America/Denver"))
                        except Exception:
                            mt = parsed
                        emailed_date = mt.strftime("%Y-%m-%d")
                        emailed_time = mt.strftime("%I:%M:%S %p").lstrip("0")
                    except Exception:
                        emailed_date = ""
                        emailed_time = ""
                else:
                    emailed_date = ""
                    emailed_time = ""

                # build mapping from header to value
                row_map = {h: r[i] if i < len(r) else "" for i, h in enumerate(header_lc)}
                out_row = [
                    emailed_date,
                    emailed_time,
                    row_map.get("site", ""),
                    row_map.get("url", ""),
                    row_map.get("job_title", ""),
                    row_map.get("min_years", ""),
                ]
                writer.writerow(out_row)

    os.replace(tmp_path, csv_path)
    print(f"Recreated CSV: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
