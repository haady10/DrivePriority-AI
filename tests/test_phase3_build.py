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
    import csv as _csv
    rows = list(_csv.DictReader(open(out / "feature_signal.csv", encoding="utf-8")))
    assert set(rows[0]) == {"attribute", "grp", "rows", "missing_pct", "nonzero_pct",
                            "median_value_approx", "p99_value_approx"}
    a5 = {r["grp"]: r for r in rows if r["attribute"] == "smart_5_raw"}
    assert set(a5) == {"pre_failure_30d", "other"}
    assert float(a5["pre_failure_30d"]["nonzero_pct"]) > float(a5["other"]["nonzero_pct"])


def test_model_spacing_does_not_split_a_drive_and_bad_capacity_is_null(tmp_path):
    from datetime import date as _d
    rows = []
    for i in range(10):
        d = _d(2020, 1, 1) + timedelta(days=i)
        model = "WDC  WD40EFRX" if i % 2 else "WDC WD40EFRX"      # spacing differs day to day
        rows.append(dict(date=d, serial="S1", model=model, failure=0, s5=0))
    snap = tmp_path / "snap"
    _write_snapshot(snap, rows)
    out = tmp_path / "out"
    rc = build_dataset.main(["--snapshot", str(snap), "--out", str(out), "--train-end", "2020-01-31",
                             "--valid-end", "2020-02-29", "--test-end", "2020-03-31",
                             "--train-rate", "1.0", "--eval-rate", "1.0", "--memory-limit", "1GB",
                             "--temp-dir", str(tmp_path / "tmp")])
    assert rc == 0
    tr = pq.read_table(out / "model_train.parquet").to_pandas()
    assert tr.drive_id.nunique() == 1                      # one drive, not two
    assert set(tr.model) == {"WDC  WD40EFRX", "WDC WD40EFRX"}   # original model text preserved

    bad = tmp_path / "bad"
    _write_snapshot(bad, rows)
    part = next(bad.rglob("*.parquet"))
    t = pq.read_table(part)
    t = t.set_column(t.schema.get_field_index("capacity_bytes"), "capacity_bytes",
                     pa.array([-1] * t.num_rows, pa.int64()))
    pq.write_table(t, part)
    out2 = tmp_path / "out2"
    assert build_dataset.main(["--snapshot", str(bad), "--out", str(out2), "--train-end", "2020-01-31",
                               "--valid-end", "2020-02-29", "--test-end", "2020-03-31",
                               "--train-rate", "1.0", "--eval-rate", "1.0", "--memory-limit", "1GB",
                               "--temp-dir", str(tmp_path / "tmp2")]) == 0
    tr2 = pq.read_table(out2 / "model_train.parquet").to_pandas()
    assert tr2.capacity_tb.isna().all()                    # -1 never becomes a negative capacity


def test_duplicate_rule_audit_and_determinism(tmp_path):
    """Exact vs conflicting duplicate days: fixed keep rule, flag, audit files, same result on rerun."""
    rows = []
    d = date(2020, 1, 1)
    while d <= date(2020, 3, 31):
        rows.append(dict(date=d, serial="B", model="ST4000DM000", failure=0, s5=0))
        if d <= date(2020, 2, 20):
            rows.append(dict(date=d, serial="A", model="ST4000DM000", failure=int(d == date(2020, 2, 20)), s5=1))
        d += timedelta(days=1)
    rows.append(dict(date=date(2020, 1, 10), serial="B", model="ST4000DM000", failure=0, s5=0))    # exact copy
    rows.append(dict(date=date(2020, 1, 12), serial="B", model="ST4000DM000", failure=0, s5=7))    # conflict
    rows.append(dict(date=date(2020, 1, 14), serial="B", model="ST4000DM000", failure=0, s5=None))  # less complete
    rows.append(dict(date=date(2020, 2, 20), serial="A", model="ST4000DM000", failure=0, s5=9))    # failure-day conflict

    def run(tag):
        snap = tmp_path / f"snap_{tag}"
        _write_snapshot(snap, rows)
        out = tmp_path / f"out_{tag}"
        assert build_dataset.main(["--snapshot", str(snap), "--out", str(out), "--train-end", "2020-02-29",
                                   "--valid-end", "2020-03-15", "--test-end", "2020-03-31", "--train-rate", "1.0",
                                   "--eval-rate", "1.0", "--memory-limit", "1GB",
                                   "--temp-dir", str(tmp_path / f"t{tag}")]) == 0
        return out

    out = run("1")
    man = json.loads((out / "phase3_manifest.json").read_text())
    dup = man["duplicates"]
    assert man["duplicate_drive_day_rows_collapsed"] == 4
    assert dup["duplicate_groups"] == 4 and dup["exact_groups"] == 1 and dup["conflicting_groups"] == 3
    assert dup["conflicting_groups_involving_failure"] == 1
    assert "smart_5_raw" in dup["conflicting_groups_by_differing_column"]
    assert "dup_conflict" in man["never_use_as_features"] and "dup_conflict" not in man["feature_columns"]

    tr = pq.read_table(out / "model_train.parquet").to_pandas()
    b = lambda day: tr[(tr.drive_id.str.startswith("B|")) & (tr.date == day)].iloc[0]
    assert b(date(2020, 1, 10)).dup_conflict == 0 and b(date(2020, 1, 10)).smart_5_raw == 0
    assert b(date(2020, 1, 12)).dup_conflict == 1
    assert b(date(2020, 1, 14)).dup_conflict == 1 and b(date(2020, 1, 14)).smart_5_raw == 0  # complete row kept
    assert b(date(2020, 1, 15)).dup_conflict == 0
    # the failure date still comes from the failure=1 row, so the labels are intact
    a = tr[tr.drive_id.str.startswith("A|")]
    assert a[a.date >= date(2020, 2, 20)].empty
    assert (a[a.date == date(2020, 2, 13)].label_7d == 1).all()

    conf = pq.read_table(out / "duplicate_conflicts.parquet").to_pandas()
    assert len(conf) == 6 and conf["kept"].sum() == 3        # all original rows of the 3 conflicting days
    assert (out / "duplicate_audit.csv").exists()

    out2 = run("2")      # same input, same output
    tr2 = pq.read_table(out2 / "model_train.parquet").to_pandas()
    import pandas as pd
    pd.testing.assert_frame_equal(tr.reset_index(drop=True), tr2.reset_index(drop=True))


def test_diff_is_null_without_earlier_value(built):
    tr, *_ = built
    first = tr[tr.drive_id.str.startswith("B|")].sort_values("date").iloc[0]
    assert first.smart_5_diff7 != first.smart_5_diff7 and first.smart_5_diff30 != first.smart_5_diff30  # NaN
    assert _row(tr, "B", date(2020, 1, 2)).smart_5_diff7 == 0      # one earlier value -> a real zero change


def test_output_independent_of_input_row_order(tmp_path):
    """R14: shuffled input order (incl. conflicting duplicates) gives byte-identical model rows."""
    import random
    import pandas as pd
    rows = _fleet()
    rows.append(dict(date=date(2020, 2, 3), serial="B", model="ST4000DM000", failure=0, s5=4))   # conflict
    rows.append(dict(date=date(2020, 2, 4), serial="B", model="ST4000DM000", failure=0, s5=None))
    outs = []
    for tag, order in (("a", rows), ("b", list(reversed(rows))), ("c", random.Random(3).sample(rows, len(rows)))):
        snap = tmp_path / f"s{tag}"
        _write_snapshot(snap, order)
        out = tmp_path / f"o{tag}"
        assert build_dataset.main(["--snapshot", str(snap), "--out", str(out), "--train-end", "2020-12-31",
                                   "--valid-end", "2021-12-31", "--test-end", "2022-12-31", "--train-rate", "1.0",
                                   "--eval-rate", "1.0", "--memory-limit", "1GB", "--threads", "4",
                                   "--temp-dir", str(tmp_path / f"t{tag}")]) == 0
        outs.append({s: pq.read_table(out / f"model_{s}.parquet").to_pandas() for s in ("train", "valid", "test")})
    for other in outs[1:]:
        for s in ("train", "valid", "test"):
            pd.testing.assert_frame_equal(outs[0][s].reset_index(drop=True), other[s].reset_index(drop=True))
