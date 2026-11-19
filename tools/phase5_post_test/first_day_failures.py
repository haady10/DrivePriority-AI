"""Post hoc, read-only: failures on the first day of the next period, seen in a replay input (review of 19 Nov 2026).

A failure enters a training or replay selection only if the drive has a record earlier in the same period, so a
failure on the first day of a period is in no selection of that period. The 2024 replay input carries failure
dates up to 31 days after the period (label window), so it shows the drives that were observed in 2024 and failed
on 1 January 2025, the first day of the test period. These are part of the 112 HDD-cohort failures that are in
none of the training and replay selections (manuscript Table II). Nothing is replayed.

Usage: python first_day_failures.py <replay_dir> <day YYYY-MM-DD> <out.json>
"""
from __future__ import annotations

import json
import sys

import duckdb


def main(replay_dir: str, day: str, out: str) -> None:
    con = duckdb.connect()
    src = f"read_parquet('{replay_dir.rstrip('/')}/part_*.parquet')"
    rows = con.execute(f"SELECT DISTINCT drive_id FROM {src} WHERE fail_date = DATE '{day}' ORDER BY 1").fetchall()
    res = {"replay_dir": replay_dir, "day": day, "drives_failing_that_day": len(rows), "drive_ids": [r[0] for r in rows]}
    json.dump(res, open(out, "w"), indent=1)
    print(json.dumps(res, indent=1))


if __name__ == "__main__":
    main(*sys.argv[1:4])
