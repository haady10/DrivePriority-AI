"""Full-fleet replay input (rule R18): the feature step must reproduce Phase 3 exactly on the sample drives.

The feature test needs no H2O. The scoring test trains tiny models first, so it is opt-in:
set DP_RUN_H2O_TESTS=1 (needs Java and the h2o package, a few minutes).
"""
from __future__ import annotations

import json
import os
from datetime import date, timedelta
from decimal import Decimal

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from drivepriority.features import build_dataset, hdd_filter
from drivepriority.simulation import full_fleet

D38 = pa.decimal128(38, 0)
SPLITS = ["--train-end", "2020-12-31", "--valid-end", "2021-12-31", "--test-end", "2022-12-31"]


def snapshot(root, n=300, seed=7):
    """Three years of daily rows: failing drives with rising counters, one SSD model, drives that are away
    for more than 31 days, missing SMART values and one duplicated day."""
    rng = np.random.default_rng(seed)
    start, end = date(2020, 1, 1), date(2022, 12, 31)
    days = (end - start).days + 1
    by_q: dict = {}
    for i in range(n):
        model = "TEST SSD 250GB" if i % 25 == 0 else ("ST4000DM000" if i % 2 else "HGST  HMS5C4040BLE640")
        fails = rng.uniform() < 0.3
        fail_idx = int(rng.integers(200, days - 5)) if fails else None
        first, last = int(rng.integers(0, 40)), (fail_idx if fails else days - 1)
        away = (int(rng.integers(380, 900)), int(rng.integers(35, 70))) if i % 7 == 0 else None
        for k in range(first, last + 1):
            if away and away[0] <= k < away[0] + away[1]:
                continue
            if rng.uniform() < 0.01:
                continue
            d = start + timedelta(days=k)
            risk = fails and (fail_idx - k) <= 25
            s5 = None if rng.uniform() < 0.02 else (int(rng.integers(5, 40)) + (25 - (fail_idx - k)) if risk else 0)
            s197 = int(rng.integers(1, 9)) if risk and rng.uniform() < 0.7 else 0
            row = (d, f"S{i:04d}", model, int(fails and k == fail_idx), s5, s197, 24 * (100 + k), 20 + i % 15)
            by_q.setdefault((d.year, (d.month - 1) // 3 + 1), []).append(row)
            if k == 420 and i % 11 == 0:
                by_q[(d.year, (d.month - 1) // 3 + 1)].append(row)          # exact duplicate drive-day
    for (y, q), rs in by_q.items():
        part = root / str(y) / f"Q{q}"
        part.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({
            "date": pa.array([r[0] for r in rs], pa.date32()),
            "serial_number": pa.array([r[1] for r in rs], pa.string()),
            "model": pa.array([r[2] for r in rs], pa.string()),
            "capacity_bytes": pa.array([4_000_787_030_016] * len(rs), pa.int64()),
            "failure": pa.array([r[3] for r in rs], pa.uint8()),
            "smart_5_raw": pa.array([None if r[4] is None else Decimal(r[4]) for r in rs], D38),
            "smart_197_raw": pa.array([Decimal(r[5]) for r in rs], D38),
            "smart_9_raw": pa.array([Decimal(r[6]) for r in rs], D38),
            "smart_194_raw": pa.array([Decimal(r[7]) for r in rs], D38),
        }), part / "part-00000000.parquet")


def phase3_hdd(tmp_path, eval_rate="0.4"):
    snap = tmp_path / "snap"
    snapshot(snap)
    p3 = tmp_path / "p3"
    assert build_dataset.main(["--snapshot", str(snap), "--out", str(p3), *SPLITS, "--train-rate", "0.5",
                               "--eval-rate", eval_rate, "--memory-limit", "1GB",
                               "--temp-dir", str(tmp_path / "t")]) == 0
    reg = tmp_path / "registry.csv"
    reg.write_text("registry_version,model_norm,device_type\nv1,ST4000DM000,HDD\n"
                   "v1,HGST HMS5C4040BLE640,HDD\nv1,TEST SSD 250GB,SSD\n", encoding="utf-8")
    hdd = tmp_path / "p3_hdd"
    assert hdd_filter.main(["--data", str(p3), "--out", str(hdd), "--registry", str(reg), "--memory-limit", "1GB",
                            "--temp-dir", str(tmp_path / "t")]) == 0
    return snap, hdd, reg


def run_features(tmp_path, snap, hdd, reg, split="valid", extra=()):
    out = tmp_path / "ff"
    rc = full_fleet.main(["features", "--snapshot", str(snap), "--phase3", str(hdd), "--registry", str(reg),
                          "--split", split, "--out", str(out), "--memory-limit", "1GB",
                          "--temp-dir", str(tmp_path / "t"), *extra])
    return rc, out


def test_calendar_helpers():
    q = full_fleet.quarters(date(2025, 1, 1), date(2026, 6, 30))
    assert [n for n, _, _ in q] == ["2025Q1", "2025Q2", "2025Q3", "2025Q4", "2026Q1", "2026Q2"]
    assert q[0][1:] == (date(2025, 1, 1), date(2025, 3, 31)) and q[-1][2] == date(2026, 6, 30)
    m = full_fleet.months(date(2024, 11, 15), date(2025, 2, 10))
    assert [(n, a.day, b.day) for n, a, b in m] == [("2024-11", 15, 30), ("2024-12", 1, 31), ("2025-01", 1, 31),
                                                   ("2025-02", 1, 10)]


def test_full_fleet_features_reproduce_the_phase3_sample_exactly(tmp_path):
    snap, hdd, reg = phase3_hdd(tmp_path)
    rc, out = run_features(tmp_path, snap, hdd, reg)
    assert rc == 0
    man = json.loads((out / "features_valid_manifest.json").read_text())
    rec = man["sample_reconciliation"]
    assert rec["passed"] and rec["rows_missing"] == 0 and rec["rows_different"] == 0 and rec["rows_checked"] > 10_000
    assert [q["quarter"] for q in man["quarters"]] == ["2021Q1", "2021Q2", "2021Q3", "2021Q4"]
    assert sum(q["returning_drives_fixed"] for q in man["quarters"]) > 0       # the long-absence path was used
    ff = pd.concat([pd.read_parquet(out / q["file"]) for q in man["quarters"]], ignore_index=True)
    sample = pd.read_parquet(hdd / "model_valid.parquet")
    # all HDD drives, not only the sample; no SSD; no duplicate drive-day; nothing on or after the failure day
    assert ff["drive_id"].nunique() > 1.5 * sample["drive_id"].nunique()
    assert not ff["model"].str.contains("SSD").any()
    assert not ff.duplicated(["drive_id", "date"]).any()
    assert not (ff["eval_fail_date"].notna() & (ff["date"] >= ff["eval_fail_date"])).any()
    assert str(ff["date"].min()) == "2021-01-01" and str(ff["date"].max()) == "2021-12-31"
    # independent re-check of the reconciliation in pandas, column by column
    both = sample.merge(ff, on=["drive_id", "date"], suffixes=("_p3", "_ff"))
    assert len(both) == len(sample)
    for c in man["feature_columns"] + ["smart_rule_score", "label_7d", "label_14d", "label_30d"]:
        a, b = both[f"{c}_p3"], both[f"{c}_ff"]
        assert ((a == b) | (a.isna() & b.isna())).all(), c
    # a drive that came back after more than 31 days carries the true gap, not an empty value
    back = ff[ff["days_since_prev_obs"] > 31]
    assert len(back) > 0 and back["days_since_prev_obs"].max() < 80


def test_feature_step_stops_on_a_wrong_sample_and_locks_the_test_split(tmp_path):
    snap, hdd, reg = phase3_hdd(tmp_path)
    with pytest.raises(SystemExit, match="locked"):
        run_features(tmp_path, snap, hdd, reg, split="test")
    # damage one value of the sample file: the reconciliation must notice and stop
    f = hdd / "model_valid.parquet"
    df = pd.read_parquet(f)
    df.loc[df.index[5], "smart_9_raw"] = df["smart_9_raw"].iloc[5] + 1
    df.to_parquet(f, index=False)
    with pytest.raises(SystemExit, match="do NOT reproduce"):
        run_features(tmp_path, snap, hdd, reg)
    other = tmp_path / "other_registry.csv"
    other.write_text(reg.read_text() + "v1,EXTRA,HDD\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="registry"):
        run_features(tmp_path, snap, hdd, other)


@pytest.mark.skipif(os.environ.get("DP_RUN_H2O_TESTS") != "1", reason="set DP_RUN_H2O_TESTS=1 to run H2O tests")
def test_full_fleet_scores_equal_phase4_on_sample_drives_and_feed_the_replay(tmp_path):
    pytest.importorskip("h2o")
    from drivepriority.models import train_h2o
    from drivepriority.simulation import run_phase5

    snap, hdd, reg = phase3_hdd(tmp_path)
    p4 = tmp_path / "p4"
    for h in (30, 14, 7):
        assert train_h2o.main(["--data", str(hdd), "--horizon", str(h), "--out", str(p4 / f"h{h}"),
                               "--models", "gbm,drf", "--max-mem-size", "1G", "--gbm-trees", "20",
                               "--drf-trees", "10", "--drf-depth", "6", "--duckdb-memory", "1GB",
                               "--port", str(54600 + 2 * h)]) == 0          # a fresh port per run
    rc, ff = run_features(tmp_path, snap, hdd, reg)
    assert rc == 0
    out = tmp_path / "ffs"
    assert full_fleet.main(["score", "--features", str(ff), "--phase4", str(p4), "--split", "valid",
                            "--out", str(out), "--max-mem-size", "1G", "--port", "54691",
                            "--duckdb-memory", "1GB"]) == 0
    man = json.loads((out / "replay_valid_manifest.json").read_text())
    assert man["sample_reconciliation"]["passed"] and len(man["months"]) == 12
    assert max(m["ml_h30_max_abs_difference"] for m in man["months"]) <= full_fleet.SCORE_TOLERANCE
    res = tmp_path / "res"
    cfg = tmp_path / "proto.json"
    raw = json.loads(open(run_phase5.load_protocol().path, encoding="utf-8").read())
    raw["splits"]["valid"] = {"first_day": "2021-01-01", "last_day": "2021-12-31"}
    raw["budget_per_10000"] = {"primary": 300, "sensitivity": [200, 500]}      # tiny fleet needs a larger share
    cfg.write_text(json.dumps(raw), encoding="utf-8")
    assert run_phase5.main(["--input", str(out / "replay_valid"), "--split", "valid", "--population", "full_fleet",
                            "--out", str(res), "--config", str(cfg), "--quick"]) == 0
    assert json.loads((res / "checks.json").read_text())["all_passed"]
