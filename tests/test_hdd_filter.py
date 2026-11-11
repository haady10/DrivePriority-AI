"""HDD-only cohort filter (rule R13): synthetic Phase 3 folder, no real data."""
from __future__ import annotations

import csv
import json
from datetime import date, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from drivepriority.features import hdd_filter

REG = [("model_norm", "device_type"), ("ST4000DM000", "HDD"), ("WDC WUH721816ALE6L4", "HDD"),
       ("Crucial SSD", "SSD"), ("DELLBOSS VD", "UNKNOWN")]


def _phase3(tmp_path, extra_model=None):
    d = tmp_path / "p3"
    d.mkdir()
    models = {"A": "ST4000DM000", "B": "WDC  WUH721816ALE6L4", "C": "Crucial SSD", "D": "DELLBOSS VD"}
    if extra_model:
        models["E"] = extra_model
    for split, (start, end) in {"train": (date(2020, 1, 1), date(2020, 1, 10)),
                                "valid": (date(2021, 1, 1), date(2021, 1, 5)),
                                "test": (date(2022, 1, 1), date(2022, 1, 5))}.items():
        rows = []
        for k, m in models.items():
            day = start
            while day <= end:
                rows.append(dict(drive_id=f"{k}|{m}", date=day, model=m, split=split, weight=1.0,
                                 eval_fail_date=(end + timedelta(days=1)) if k in "AC" else None,
                                 smart_5_raw=1.0, label_7d=1, label_14d=1, label_30d=None))
                day += timedelta(days=1)
        pq.write_table(pa.Table.from_pandas(pd.DataFrame(rows)), d / f"model_{split}.parquet")
    (d / "phase3_manifest.json").write_text(json.dumps({
        "splits": {"train_end": "2020-12-31", "valid_end": "2021-12-31", "test_end": "2022-12-31"},
        "feature_columns": ["smart_5_raw"], "split_stats": {}, "files": {}}), encoding="utf-8")
    (d / "feature_dictionary.csv").write_text("feature\nsmart_5_raw\n", encoding="utf-8")
    reg = tmp_path / "reg.csv"
    with reg.open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerows(REG)
    return d, reg


def _run(d, reg, out, tmp_path):
    return hdd_filter.main(["--data", str(d), "--registry", str(reg), "--out", str(out),
                            "--memory-limit", "1GB", "--temp-dir", str(tmp_path / "t")])


def test_keeps_only_hdd_and_reports_counts(tmp_path):
    d, reg = _phase3(tmp_path)
    out = tmp_path / "hdd"
    assert _run(d, reg, out, tmp_path) == 0
    tr = pq.read_table(out / "model_train.parquet").to_pandas()
    assert set(tr.drive_id.str[0]) == {"A", "B"}                       # double-space model still matches
    assert len(tr) == 20 and (tr.weight == 1.0).all()                   # rows and weights unchanged
    assert list(tr.columns) == list(pq.read_table(d / "model_train.parquet").to_pandas().columns)
    man = json.loads((out / "phase3_manifest.json").read_text())
    ba = man["cohort"]["before_after"]["train"]
    assert ba["before"]["rows"] == 40 and ba["after"]["rows"] == 20
    assert ba["before"]["drives"] == 4 and ba["after"]["drives"] == 2
    assert man["split_stats"]["train"]["rows"] == 20 and man["cohort"]["weights_unchanged"] is True
    ex = pd.read_csv(out / "hdd_exclusions.csv")
    assert set(ex.device_type) == {"SSD", "UNKNOWN"} and ex[ex.split == "train"].rows_removed.sum() == 20
    assert (out / "feature_dictionary.csv").exists()
    assert len(man["cohort"]["registry_sha256"]) == 64


def test_unknown_model_stops_and_nonempty_out_is_refused(tmp_path):
    d, reg = _phase3(tmp_path, extra_model="MYSTERY X1")
    with pytest.raises(SystemExit) as exc:
        _run(d, reg, tmp_path / "hdd", tmp_path)
    assert "not in the registry" in str(exc.value)
    (tmp_path / "second").mkdir()
    d2, reg2 = _phase3(tmp_path / "second")
    out = tmp_path / "second" / "hdd"
    assert _run(d2, reg2, out, tmp_path) == 0
    with pytest.raises(SystemExit):
        _run(d2, reg2, out, tmp_path)                                    # never overwrite
