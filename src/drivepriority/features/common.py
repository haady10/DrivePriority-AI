"""Shared helpers for Phase 2 profiling and Phase 3 dataset building.

Both scripts read the immutable Central Integration snapshot (read-only) with
DuckDB. Nothing here writes into the snapshot.
"""
from __future__ import annotations

import json
import os
import platform
from datetime import datetime, timezone
from pathlib import Path

import duckdb

# SMART attributes used for modelling. Counters are cumulative error counts
# (change features make sense); the others are usage or condition values.
COUNTER_ATTRS = (5, 187, 188, 197, 198, 199)
OTHER_ATTRS = (1, 7, 9, 193, 194, 241, 242)
MODEL_ATTRS = COUNTER_ATTRS + OTHER_ATTRS
# The traditional SMART baseline (Phase 1, section 20).
SMART_RULE_ATTRS = (5, 187, 188, 197, 198)

# Model text is normalised ONLY for identity (collapse repeated spaces, trim). The original
# model column is kept untouched. Backblaze models such as "WDC  WD40..." vs "WDC WD40..."
# would otherwise split one physical drive into two identities.
DRIVE_ID_SQL = "serial_number || '|' || regexp_replace(trim(coalesce(model, '')), '\\s+', ' ', 'g')"
MANUFACTURER_SQL = """
CASE
  WHEN model LIKE 'ST%' OR upper(model) LIKE 'SEAGATE%' THEN 'Seagate'
  WHEN upper(model) LIKE 'HGST%' OR upper(model) LIKE 'HITACHI%' THEN 'HGST'
  WHEN upper(model) LIKE 'TOSHIBA%' THEN 'Toshiba'
  WHEN upper(model) LIKE 'WDC%' OR upper(model) LIKE 'WD%' THEN 'WDC'
  ELSE 'Other'
END"""


def connect(memory_limit: str, temp_dir: str | Path, threads: int | None = None) -> duckdb.DuckDBPyConnection:
    Path(temp_dir).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET temp_directory='{Path(temp_dir).as_posix()}'")
    con.execute("SET preserve_insertion_order=false")
    if threads:
        con.execute(f"SET threads={int(threads)}")
    return con


def snapshot_glob(snapshot: str | Path) -> str:
    root = Path(snapshot).expanduser().resolve(strict=True)
    if not root.is_dir():
        raise SystemExit(f"Snapshot folder not found: {root}")
    files = list(root.rglob("*.parquet"))
    if not files:
        raise SystemExit(f"No .parquet files under {root}")
    return (root.as_posix() + "/**/*.parquet")


def create_source_view(con: duckdb.DuckDBPyConnection, snapshot: str | Path) -> list[str]:
    """Create view `src` over the snapshot. Returns available column names.

    SMART columns are DECIMAL(38,0) in the snapshot (exact). For modelling we
    cast to DOUBLE; the snapshot itself keeps the exact values.
    A SMART column that never appears in any quarter becomes NULL.
    """
    glob = snapshot_glob(snapshot)
    con.execute(
        f"CREATE OR REPLACE VIEW src_raw AS SELECT * FROM read_parquet('{glob}', union_by_name=true)"
    )
    cols = [r[0] for r in con.execute("DESCRIBE src_raw").fetchall()]
    for required in ("date", "serial_number", "model", "failure"):
        if required not in cols:
            raise SystemExit(f"Snapshot is missing required column '{required}'")
    select = [
        "CAST(date AS DATE) AS date",
        "serial_number",
        "model",
        f"{DRIVE_ID_SQL} AS drive_id",
        f"{MANUFACTURER_SQL} AS manufacturer_group",
        # Capacity <= 0 (Backblaze uses -1 for a failed read) is not a capacity: treat as NULL.
        ("CASE WHEN capacity_bytes > 0 THEN CAST(capacity_bytes AS DOUBLE) END AS capacity_bytes"
         if "capacity_bytes" in cols else "CAST(NULL AS DOUBLE) AS capacity_bytes"),
        "CAST(failure AS INTEGER) AS failure",
    ]
    for a in MODEL_ATTRS:
        c = f"smart_{a}_raw"
        select.append(f"CAST({c} AS DOUBLE) AS {c}" if c in cols else f"CAST(NULL AS DOUBLE) AS {c}")
    con.execute("CREATE OR REPLACE VIEW src AS SELECT " + ", ".join(select) + " FROM src_raw")
    return cols


def write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")


def run_metadata(args: dict) -> dict:
    return {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duckdb_version": duckdb.__version__,
        "python": platform.python_version(),
        "machine": platform.node(),
        "cpu_count": os.cpu_count(),
        "arguments": args,
    }
