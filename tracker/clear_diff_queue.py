# tracker/clear_diff_queue.py
from __future__ import annotations

import argparse
from tracker.db import SQLiteState


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="./state/snapshots.sqlite3")
    args = ap.parse_args()

    state = SQLiteState(args.db)
    n = state.clear_diff_queue()
    state.close()

    print(f"cleared_diff_queue rows_deleted={n}")


if __name__ == "__main__":
    main()
    