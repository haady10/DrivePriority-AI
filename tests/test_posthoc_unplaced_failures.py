"""Amendment A1.7: the unplaced-failure query finds and explains failures that are in no selection."""
from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta

import pandas as pd

from drivepriority.posthoc import unplaced_failures as uf


def days(a: date, b: date):
    return [a + timedelta(days=i) for i in range((b - a).days + 1)]


def test_unplaced_failures_are_itemised_with_reasons(tmp_path):
    snap = tmp_path / "snap" / "2024" / "Q1"
    snap.mkdir(parents=True)
    recs = []

    def drive(serial, model, first, last, fail):
        for d in days(first, last):
            recs.append({"date": d, "serial_number": serial, "model": model, "failure": int(d == fail),
                         "capacity_bytes": 1})
    drive("A", "ST4000DM000", date(2023, 12, 1), date(2023, 12, 21), date(2023, 12, 21))   # training failure
    drive("B", "ST4000DM000", date(2024, 3, 5), date(2024, 3, 5), date(2024, 3, 5))       # failure is first record
    drive("C", "HGST HMS5C4040BLE640", date(2024, 12, 1), date(2025, 1, 1), date(2025, 1, 1))  # first day of test
    drive("D", "TOSHIBA MG07ACA14TA", date(2025, 4, 1), date(2025, 5, 10), date(2025, 5, 10))  # not in the replay
    drive("E", "Samsung SSD 850", date(2024, 1, 1), date(2024, 2, 1), date(2024, 2, 1))    # SSD: not in the cohort
    drive("F", "ST4000DM000", date(2024, 1, 1), date(2024, 6, 1), date(2024, 6, 1))        # 2024 replay failure
    drive("G", "ST4000DM000", date(2024, 1, 1), date(2024, 6, 1), None)                    # healthy
    pd.DataFrame(recs).to_parquet(snap / "part-0.parquet", index=False)

    reg = tmp_path / "registry.csv"
    reg.write_text("model_norm,device_type\nST4000DM000,HDD\nHGST HMS5C4040BLE640,HDD\nTOSHIBA MG07ACA14TA,HDD\n"
                   "Samsung SSD 850,SSD\n", encoding="utf-8")
    p3 = tmp_path / "p3"
    p3.mkdir()
    (p3 / "phase3_manifest.json").write_text(json.dumps({
        "splits": {"train_end": "2023-12-31", "valid_end": "2024-12-31", "test_end": "2026-06-30"},
        "cohort": {"name": "HDD-only", "registry_sha256": hashlib.sha256(reg.read_bytes()).hexdigest()}}))
    pd.DataFrame({"drive_id": ["A|ST4000DM000"] * 2, "date": [date(2023, 12, 1), date(2023, 12, 2)],
                  "eval_fail_date": [date(2023, 12, 21)] * 2}).to_parquet(p3 / "model_train.parquet", index=False)
    rv, rt = tmp_path / "rv", tmp_path / "rt"
    rv.mkdir()
    rt.mkdir()
    pd.DataFrame({"drive_id": ["F|ST4000DM000", "C|HGST HMS5C4040BLE640", "G|ST4000DM000"],
                  "date": [date(2024, 5, 1), date(2024, 12, 31), date(2024, 5, 1)],
                  "fail_date": [date(2024, 6, 1), date(2025, 1, 1), None]}).to_parquet(rv / "part_2024-05.parquet", index=False)
    pd.DataFrame({"drive_id": ["G|ST4000DM000"], "date": [date(2025, 2, 1)], "fail_date": [None]},
                 ).astype({"fail_date": "datetime64[ms]"}).to_parquet(rt / "part_2025-02.parquet", index=False)

    out = tmp_path / "out"
    assert uf.main(["--snapshot", str(tmp_path / "snap"), "--registry", str(reg), "--phase3", str(p3),
                    "--replay-valid", str(rv), "--replay-test", str(rt), "--out", str(out),
                    "--memory-limit", "1GB"]) == 0
    res = pd.read_csv(out / "unplaced_failures.csv").set_index("serial_number")
    assert sorted(res.index) == ["B", "C", "D"]
    assert res.loc["B", "reason"] == "no_record_before_failure"
    assert res.loc["C", "reason"] == "earlier_records_only_in_earlier_period"
    assert bool(res.loc["C", "failure_on_first_day_of_period"]) and res.loc["C", "period"] == "test"
    assert res.loc["D", "reason"] == "earlier_record_in_same_period" and res.loc["D", "make"] == "Toshiba"
    s = json.loads((out / "unplaced_failures_summary.json").read_text())
    assert s["counts"]["snapshot_failed_drives"] == 6 and s["counts"]["hdd_cohort_failed_drives"] == 5
    assert s["counts"]["unplaced"] == 3 and s["counts"]["training_failures"] == 1
