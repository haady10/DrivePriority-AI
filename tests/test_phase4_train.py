"""Phase 4 checks: metric helpers always run; the end-to-end H2O run is opt-in.

Set DP_RUN_H2O_TESTS=1 to also run the small H2O training on a synthetic Phase 3 output
(needs Java and the h2o package, about one minute).
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

from drivepriority.models import metrics

D38 = pa.decimal128(38, 0)


def test_recall_precision_at_k_and_ties():
    y = np.array([1, 0, 1, 0, 0, 0])
    s = np.array([0.9, 0.8, 0.7, 0.1, 0.1, 0.1])
    assert metrics.recall_precision_at_k(y, s, 1) == (0.5, 1.0)
    assert metrics.recall_precision_at_k(y, s, 3) == (1.0, 2 / 3)
    # k larger than the data is capped
    assert metrics.recall_precision_at_k(y, s, 99)[0] == 1.0


def test_evaluate_perfect_and_undefined_cases():
    y = np.array([0, 0, 0, 1, 1, 0, 0, 0, 0, 0])
    good = metrics.evaluate(y, y.astype(float) * 0.9 + 0.05, is_probability=True, rule_flag_count=2)
    assert good["roc_auc"] == 1.0 and good["average_precision"] == 1.0
    assert good["recall_at_smart_volume"] == 1.0 and good["alert_volume"] == 2
    none = metrics.evaluate(np.zeros(5), np.arange(5.0), is_probability=True)
    assert np.isnan(none["roc_auc"]) and none["positives"] == 0


def test_rule_metrics_and_calibration():
    y = np.array([1, 1, 0, 0, 0, 0])
    flag = np.array([1, 0, 1, 0, 0, 0])
    rm = metrics.rule_metrics(y, flag)
    assert rm["flagged_rows"] == 2 and rm["recall"] == 0.5 and rm["precision"] == 0.5
    rng = np.random.default_rng(0)
    p = rng.uniform(0, 1, 5000)
    yy = (rng.uniform(0, 1, 5000) < p).astype(int)
    cal = metrics.calibration_table(yy, p, bins=5)
    assert len(cal) == 5 and (cal["mean_predicted"] - cal["observed_rate"]).abs().max() < 0.08


# ---------------------------------------------------------------- end-to-end (opt-in)

def _synthetic_snapshot(root):
    rng = np.random.default_rng(7)
    start, end = date(2020, 1, 1), date(2022, 12, 31)
    days = (end - start).days + 1
    by_q: dict = {}
    for i in range(500):
        fails = rng.uniform() < 0.25
        fail_idx = int(rng.integers(60, days - 5)) if fails else None
        last = fail_idx if fails else days - 1
        first = int(rng.integers(0, 40))
        for k in range(first, last + 1):
            d = start + timedelta(days=k)
            risk = fails and (fail_idx - k) <= 20
            s5 = int(rng.integers(5, 40)) if risk else 0
            s197 = int(rng.integers(1, 9)) if risk and rng.uniform() < 0.7 else 0
            by_q.setdefault((d.year, (d.month - 1) // 3 + 1), []).append(
                (d, f"S{i:04d}", "ST4000DM000", int(fails and k == fail_idx), s5, s197, 24 * (100 + k)))
    for (y, q), rs in by_q.items():
        part = root / str(y) / f"Q{q}"
        part.mkdir(parents=True, exist_ok=True)
        cols = {
            "date": pa.array([r[0] for r in rs], pa.date32()),
            "serial_number": pa.array([r[1] for r in rs], pa.string()),
            "model": pa.array([r[2] for r in rs], pa.string()),
            "capacity_bytes": pa.array([4_000_787_030_016] * len(rs), pa.int64()),
            "failure": pa.array([r[3] for r in rs], pa.uint8()),
            "smart_5_raw": pa.array([Decimal(r[4]) for r in rs], D38),
            "smart_197_raw": pa.array([Decimal(r[5]) for r in rs], D38),
            "smart_9_raw": pa.array([Decimal(r[6]) for r in rs], D38),
        }
        pq.write_table(pa.table(cols), part / "part-00000000.parquet")


@pytest.mark.skipif(os.environ.get("DP_RUN_H2O_TESTS") != "1", reason="set DP_RUN_H2O_TESTS=1 to run H2O tests")
def test_train_h2o_end_to_end(tmp_path):
    pytest.importorskip("h2o")
    from drivepriority.features import build_dataset
    from drivepriority.models import train_h2o

    snap = tmp_path / "snap"
    _synthetic_snapshot(snap)
    p3 = tmp_path / "p3"
    assert build_dataset.main(["--snapshot", str(snap), "--out", str(p3), "--train-end", "2020-12-31",
                               "--valid-end", "2021-12-31", "--test-end", "2022-12-31", "--train-rate", "0.5",
                               "--eval-rate", "1.0", "--memory-limit", "1GB",
                               "--temp-dir", str(tmp_path / "t")]) == 0
    out = tmp_path / "p4"
    rc = train_h2o.main(["--data", str(p3), "--horizon", "14", "--out", str(out), "--models", "lr,glm,gbm,drf",
                         "--neg-keep", "0.5", "--max-mem-size", "1G", "--gbm-trees", "20", "--drf-trees", "10",
                         "--drf-depth", "6", "--duckdb-memory", "1GB"])
    assert rc == 0
    m = pd.read_csv(out / "metrics.csv")
    assert set(m["model"]) == {"lr", "glm", "gbm", "drf", "smart_rule_score"}
    assert set(m["split"]) == {"valid", "test"}
    gbm = m[(m["model"] == "gbm") & (m["split"] == "valid")].iloc[0]
    assert gbm["roc_auc"] > 0.8  # the synthetic signal is easy: SMART counters rise before failure
    checks = json.loads((out / "checks.json").read_text())
    assert checks["rows_at_or_after_failure"] == {"valid": 0, "test": 0}
    sc = pd.read_parquet(out / "scores_test.parquet")
    assert {"model", "drive_id", "date", "label", "eval_fail_date", "score"} <= set(sc.columns)
    assert sc["score"].between(0, 1).all()
    # Unknown-outcome drives are scored too (the inspection replay needs them) but not evaluated.
    assert sc["label"].isna().any() and (sc["label_known"] == sc["label"].notna()).all()
    assert {"smart_rule_flag", "smart_rule_score"} <= set(sc.columns)
    n_unknown = checks["unknown_labels_scored_not_evaluated"]["test"]
    assert n_unknown > 0
    per_model = sc.groupby("model").size()
    assert (per_model == checks["rows_scored"]["test"]).all()
    assert (sc[sc["model"] == "gbm"]["label"].isna().sum()) == n_unknown
    mt = m[(m["model"] == "gbm") & (m["split"] == "test")].iloc[0]
    assert mt["rows"] == checks["rows_scored"]["test"] - n_unknown  # metrics use known rows only
    assert not (out / "h2o_train.parquet").exists()  # temporary files are cleaned up
    assert not (out / "h2o_test_all.parquet").exists()


def test_port_in_use_stops_before_any_work(tmp_path):
    """Two runs must never share one H2O cluster: a busy port stops the run immediately."""
    import socket
    from drivepriority.models import train_h2o
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        port = srv.getsockname()[1]
        with pytest.raises(SystemExit) as exc:
            train_h2o.main(["--data", str(tmp_path / "none"), "--horizon", "7", "--out", str(tmp_path / "o"),
                            "--port", str(port)])
        assert "already in use" in str(exc.value)
