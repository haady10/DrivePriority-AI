"""Synthetic checks for Phase 2 profiling and Phase 3 labels/features (no real data)."""
from __future__ import annotations

import json
from datetime import date, timedelta
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from drivepriority.analysis import profile
from drivepriority.features import build_dataset

D38 = pa.decimal128(38, 0)


def _write_snapshot(root, rows):
    by_q = {}
    for r in rows:
        q = (r["date"].year, (r["date"].month - 1) // 3 + 1)
        by_q.setdefault(q, []).append(r)
    for (y, q), rs in by_q.items():
        part = root / str(y) / f"Q{q}"
        part.mkdir(parents=True, exist_ok=True)
        cols = {
            "date": pa.array([r["date"] for r in rs], pa.date32()),
            "serial_number": pa.array([r["serial"] for r in rs], pa.string()),
            "model": pa.array([r["model"] for r in rs], pa.string()),
            "capacity_bytes": pa.array([4_000_787_030_016] * len(rs), pa.int64()),
            "failure": pa.array([r["failure"] for r in rs], pa.uint8()),
            "smart_5_raw": pa.array([None if r.get("s5") is None else Decimal(r["s5"]) for r in rs], D38),
            "smart_9_raw": pa.array([Decimal(24 * 100)] * len(rs), D38),
            "smart_197_raw": pa.array([Decimal(0)] * len(rs), D38),
        }
        pq.write_table(pa.table(cols), part / "part-00000000.parquet")


def _fleet():
    rows = []
    start, end = date(2020, 1, 1), date(2022, 12, 31)
    d = start
    while d <= end:
        # A: fails 2020-06-20, SMART 5 grows from 2020-06-01
        if d <= date(2020, 6, 20):
            s5 = max(0, (d - date(2020, 6, 1)).days)
            rows.append(dict(date=d, serial="A", model="ST4000DM000", failure=int(d == date(2020, 6, 20)), s5=s5))
        # B: healthy all period, s5 missing on 2020-03-03
        rows.append(dict(date=d, serial="B", model="ST4000DM000", failure=0,
                         s5=None if d == date(2020, 3, 3) else 0))
        # C: disappears without failure on 2020-05-10
        if d <= date(2020, 5, 10):
            rows.append(dict(date=d, serial="C", model="HGST HMS5C4040BLE640", failure=0, s5=0))
        # E: fails in the test year
        if d <= date(2022, 3, 15):
            rows.append(dict(date=d, serial="E", model="ST8000DM002", failure=int(d == date(2022, 3, 15)), s5=1))
        d += timedelta(days=1)
    rows.append(dict(date=date(2020, 2, 2), serial="B", model="ST4000DM000", failure=0, s5=0))  # duplicate day
    return rows


@pytest.fixture()
def built(tmp_path):
    snap = tmp_path / "snap"
    _write_snapshot(snap, _fleet())
    out = tmp_path / "out"
    rc = build_dataset.main(["--snapshot", str(snap), "--out", str(out), "--train-end", "2020-12-31",
                             "--valid-end", "2021-12-31", "--test-end", "2022-12-31",
                             "--train-rate", "1.0", "--eval-rate", "1.0", "--memory-limit", "1GB",
                             "--temp-dir", str(tmp_path / "tmp")])
    assert rc == 0
    tr = pq.read_table(out / "model_train.parquet").to_pandas()
    te = pq.read_table(out / "model_test.parquet").to_pandas()
    man = json.loads((out / "phase3_manifest.json").read_text())
    return tr, te, man, snap, tmp_path


def _row(df, serial, d):
    m = df[(df.drive_id.str.startswith(serial + "|")) & (df.date == d)]
    assert len(m) == 1, (serial, d, len(m))
    return m.iloc[0]


def test_labels_around_failure(built):
    tr, *_ = built
    fail = date(2020, 6, 20)
    assert _row(tr, "A", fail - timedelta(days=7)).label_7d == 1      # failure 7 days later -> positive
    assert _row(tr, "A", fail - timedelta(days=8)).label_7d == 0      # 8 days later -> outside 7d
    assert _row(tr, "A", fail - timedelta(days=8)).label_14d == 1
    assert _row(tr, "A", fail - timedelta(days=30)).label_30d == 1
    assert _row(tr, "A", fail - timedelta(days=31)).label_30d == 0
    # failure day itself and later rows are never model inputs
    assert tr[(tr.drive_id.str.startswith("A|")) & (tr.date >= fail)].empty


def test_unknown_not_healthy(built):
    tr, *_ = built
    gone = date(2020, 5, 10)
    assert _row(tr, "C", gone - timedelta(days=7)).label_7d == 0      # still seen 7 days later
    assert _row(tr, "C", gone - timedelta(days=3)).label_7d != _row(tr, "C", gone - timedelta(days=3)).label_7d  # NaN


def test_purge_at_split_end(built):
    tr, *_ = built
    r = _row(tr, "B", date(2020, 12, 20))
    assert r.label_7d == 0
    assert r.label_14d != r.label_14d   # 20 Dec + 14 crosses train end -> NULL
    assert r.label_30d != r.label_30d


def test_past_only_features(built):
    tr, *_ = built
    r = _row(tr, "A", date(2020, 6, 15))           # s5 = 14 today, 7 on 8 June
    assert r.smart_5_raw == 14 and r.smart_5_diff7 == 7
    assert r.smart_rule_flag == 1 and r.smart_rule_score == 14
    assert _row(tr, "B", date(2020, 3, 3)).smart_5_missing == 1
    assert _row(tr, "B", date(2020, 3, 4)).age_days == 100


def test_test_split_and_manifest(built):
    tr, te, man, *_ = built
    assert te.date.min() >= date(2022, 1, 1)
    assert _row(te, "E", date(2022, 3, 1)).label_30d == 1
    assert man["duplicate_drive_day_rows_collapsed"] == 1
    assert man["fleet_up_to_test_end"]["failed_drives"] == 2
    forbidden = set(man["never_use_as_features"])
    assert not forbidden & set(man["feature_columns"])


def test_profile_runs(built):
    *_, snap, tmp = built
    out = tmp / "profile"
    assert profile.main(["--snapshot", str(snap), "--out", str(out), "--memory-limit", "1GB",
                         "--temp-dir", str(tmp / "tmp2")]) == 0
    s = json.loads((out / "summary.json").read_text())
    assert s["failed_drives"] == 2 and s["duplicate_drive_day_rows"] == 1
    for f in ("by_year.csv", "by_model.csv", "missing_by_year.csv", "feature_signal.csv", "class_balance.csv"):
        assert (out / f).exists()
