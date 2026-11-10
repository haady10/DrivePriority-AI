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
                         "--neg-keep", "0.5", "--not-protocol", "--max-mem-size", "1G", "--gbm-trees", "200",
                         "--drf-trees", "10", "--drf-depth", "6", "--duckdb-memory", "1GB", "--port", "54611"])
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
    # v4 protocol: history_days is excluded, GBM scores keep their ranking (review B1), flag never NULL.
    info = json.loads((out / "run_info.json").read_text())
    assert "history_days" not in info["features_used"] and "history_days" in checks["excluded_features"]
    imp = pd.read_csv(out / "importance_gbm.csv")
    assert "history_days" not in set(imp["variable"])
    h = checks["score_health"]["valid/gbm"]
    assert h["distinct_scores"] > 100, h
    assert h["rows_at_or_above_0.999999"] + h["rows_at_or_below_1e-12"] <= 0.001 * h["rows"], h
    # (synthetic data is trivially separable, so other models may still warn; the GBM must not saturate)
    assert not any(w.startswith("gbm:") and "saturated" in w for w in checks["warnings"])
    assert sc["smart_rule_flag"].notna().all()
    assert info["protocol_run"] is False and info["kit_version"] == "v4"
    for key in ("duckdb", "java", "code_sha256", "phase3_manifest_sha256", "resources", "git_commit"):
        assert key in info


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


# ---------------------------------------------------------------- review fixes (no H2O needed)

def test_top_k_ties_are_shared_fairly_and_order_free():
    """Rows tied at the cut-off share the remaining places; row order cannot change the result."""
    y = np.array([1, 0, 0, 0, 1, 0])
    s = np.array([0.9, 0.5, 0.5, 0.5, 0.5, 0.1])
    # K=3: 0.9 is in; 2 places left among 4 tied rows that hold 1 positive -> 0.5 expected hits.
    hits, tied = metrics.top_k_hits(y, s, 3)
    assert hits == 1.5 and tied == 4
    rec, prec = metrics.recall_precision_at_k(y, s, 3)
    assert rec == 0.75 and prec == 0.5
    rev = metrics.recall_precision_at_k(y[::-1], s[::-1], 3)
    assert rev == (rec, prec)
    # Same as the mean over many random tie-breaks.
    rng = np.random.default_rng(1)
    sims = []
    for _ in range(4000):
        perm = rng.permutation(len(y))
        order = np.lexsort((perm, -s))
        sims.append(y[order[:3]].sum())
    assert abs(np.mean(sims) - 1.5) < 0.05
    m = metrics.evaluate(y, s, is_probability=False)
    assert "tied_at_cut_top_1pct" in m


def test_score_health_flags_saturation():
    h = metrics.score_health(np.array([1.0, 1.0, 1.0, 0.0, 0.3, 1e-19]))
    assert h["rows_at_or_above_0.999999"] == 3 and h["rows_at_or_below_1e-12"] == 2
    assert h["largest_tie_rows"] == 3 and h["distinct_scores"] == 4


def test_non_protocol_neg_keep_stops(tmp_path):
    from drivepriority.models import train_h2o
    with pytest.raises(SystemExit) as exc:
        train_h2o.main(["--data", str(tmp_path), "--horizon", "7", "--out", str(tmp_path / "o"),
                        "--neg-keep", "0.1", "--port", "54631"])
    assert "protocol" in str(exc.value)


def test_manifest_forbidden_feature_stops(tmp_path):
    """A column listed in never_use_as_features (e.g. dup_conflict) can never become an input."""
    from drivepriority.models import train_h2o
    (tmp_path / "phase3_manifest.json").write_text(json.dumps({
        "feature_columns": ["smart_5_raw", "dup_conflict"], "never_use_as_features": ["dup_conflict"],
        "splits": {"train_end": "2020-12-31", "valid_end": "2021-12-31", "test_end": "2022-12-31"}}))
    with pytest.raises(SystemExit) as exc:
        train_h2o.main(["--data", str(tmp_path), "--horizon", "7", "--out", str(tmp_path / "o"), "--port", "54641"])
    assert "Forbidden" in str(exc.value) and "dup_conflict" in str(exc.value)


@pytest.fixture(scope="module")
def phase3_synth(tmp_path_factory):
    from drivepriority.features import build_dataset
    root = tmp_path_factory.mktemp("p3synth")
    _synthetic_snapshot(root / "snap")
    p3 = root / "p3"
    assert build_dataset.main(["--snapshot", str(root / "snap"), "--out", str(p3), "--train-end", "2020-12-31",
                               "--valid-end", "2021-12-31", "--test-end", "2022-12-31", "--train-rate", "0.5",
                               "--eval-rate", "1.0", "--memory-limit", "1GB", "--temp-dir", str(root / "t")]) == 0
    return p3


def test_prepare_files_labels_weights_thinning_and_rule_flag(phase3_synth, tmp_path):
    import duckdb
    from drivepriority.models import train_h2o
    manifest = json.loads((phase3_synth / "phase3_manifest.json").read_text())
    feats = [c for c in manifest["feature_columns"] if c not in train_h2o.EXCLUDED_FEATURES]
    outs = []
    for i in range(2):
        o = tmp_path / f"run{i}"
        o.mkdir()
        counts = train_h2o.prepare_files(phase3_synth, o, 30, feats, 0.25, "1GB", None)
        outs.append((o, counts))
    o, counts = outs[0]
    con = duckdb.connect()
    src = (phase3_synth / "model_train.parquet").as_posix()
    tr = (o / "h2o_train.parquet").as_posix()
    # Unknown labels never reach training as 0 (or at all).
    assert con.execute(f"SELECT count(*) FROM read_parquet('{tr}') WHERE y IS NULL").fetchone()[0] == 0
    n_pos_src, w_neg_src = con.execute(
        f"SELECT count(*) FILTER (WHERE label_30d = 1), sum(weight) FILTER (WHERE label_30d = 0) "
        f"FROM read_parquet('{src}')").fetchone()
    n_pos, w_neg = con.execute(f"SELECT count(*) FILTER (WHERE y = 1), sum(w) FILTER (WHERE y = 0) "
                               f"FROM read_parquet('{tr}')").fetchone()
    assert n_pos == n_pos_src                       # every positive is kept
    assert abs(w_neg / float(w_neg_src) - 1) < 0.03  # thinning weights restore the healthy total
    # Deterministic: a second run keeps exactly the same rows.
    a = con.execute(f"SELECT md5(string_agg(CAST(w AS VARCHAR) || CAST(y AS VARCHAR), ',' ORDER BY ALL)) "
                    f"FROM read_parquet('{tr}')").fetchone()[0]
    b = con.execute(f"SELECT md5(string_agg(CAST(w AS VARCHAR) || CAST(y AS VARCHAR), ',' ORDER BY ALL)) "
                    f"FROM read_parquet('{(outs[1][0] / 'h2o_train.parquet').as_posix()}')").fetchone()[0]
    assert a == b and outs[0][1] == outs[1][1]
    for split in ("valid", "test"):
        meta = (o / f"meta_{split}.parquet").as_posix()
        assert con.execute(f"SELECT count(*) FROM read_parquet('{meta}') WHERE smart_rule_flag IS NULL").fetchone()[0] == 0
        assert counts[split]["smart_rule_flag_vs_score_mismatch"] == 0
    assert "history_days" not in feats


def test_split_date_outside_manifest_range_stops(phase3_synth, tmp_path):
    import shutil
    from drivepriority.models import train_h2o
    bad = tmp_path / "p3bad"
    shutil.copytree(phase3_synth, bad)
    m = json.loads((bad / "phase3_manifest.json").read_text())
    m["splits"]["train_end"] = "2020-06-30"  # train rows now go past the stated end
    (bad / "phase3_manifest.json").write_text(json.dumps(m))
    with pytest.raises(SystemExit) as exc:
        train_h2o.main(["--data", str(bad), "--horizon", "7", "--out", str(tmp_path / "o"), "--port", "54651"])
    assert "outside its range" in str(exc.value)
