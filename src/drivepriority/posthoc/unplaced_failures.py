"""Amendment A1.7: itemise the HDD-cohort failures that are in no training or replay selection (read-only).

Table II of the manuscript holds 17,442 (training, to 2023) + 4,607 (2024 replay) + 6,826 (test replay) = 28,875
failures against 28,987 in the HDD cohort. This query lists the 112 others, drive by drive, with the reason:

  no_record_before_failure                 the failure day is the drive's first record
  earlier_records_only_in_earlier_period   records before the failure exist, but none in the failure's period
                                           (e.g. a drive seen in 2024 that failed on 1 January 2025)
  earlier_record_in_same_period            not explained by the selection rule: looked at one by one

Inputs: the integrated snapshot (read-only), the drive-type registry, the HDD-only Phase 3 folder
(model_train.parquet and phase3_manifest.json) and the two frozen replay inputs. Two scans of the snapshot.

  python -m drivepriority.posthoc.unplaced_failures --snapshot <snapshot folder> --phase3 <03_Phase3_FINAL_HDD> ^
      --replay-valid <replay_valid folder> --replay-test <replay_test folder> --out <new folder>
"""
from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import date, datetime, timezone
from pathlib import Path

from drivepriority.features.common import DRIVE_ID_SQL, connect, run_metadata, snapshot_glob
from drivepriority.posthoc.makes import MAKES, make_of_model
from drivepriority.simulation.replay_input import sha256_file, sq

NORM = "regexp_replace(trim(coalesce(model, '')), '\\s+', ' ', 'g')"
PUBLISHED = {"snapshot_failed_drives": 29222, "hdd_cohort_failed_drives": 28987, "training_failures": 17442,
             "replay_2024_failures": 4607, "replay_test_failures": 6826, "unplaced": 112}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--snapshot", required=True)
    ap.add_argument("--registry", default=str(Path(__file__).resolve().parents[3] / "configs" / "drive_type_registry_v1.csv"))
    ap.add_argument("--phase3", required=True, help="HDD-only Phase 3 folder (model_train.parquet, phase3_manifest.json)")
    ap.add_argument("--replay-valid", required=True)
    ap.add_argument("--replay-test", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--memory-limit", default="16GB")
    ap.add_argument("--threads", type=int, default=None)
    a = ap.parse_args(argv)

    t0 = time.time()
    out = Path(a.out).expanduser().resolve()
    out.mkdir(parents=True, exist_ok=True)
    p3 = Path(a.phase3).expanduser().resolve(strict=True)
    man = json.loads((p3 / "phase3_manifest.json").read_text(encoding="utf-8"))
    if (man.get("cohort") or {}).get("name") != "HDD-only":
        raise SystemExit("--phase3 must be the HDD-only Phase 3 folder")
    sp = man["splits"]
    train_end, valid_end, test_end = (date.fromisoformat(sp[k]) for k in ("train_end", "valid_end", "test_end"))
    reg = Path(a.registry).resolve(strict=True)
    if sha256_file(reg) != man["cohort"].get("registry_sha256"):
        raise SystemExit("The registry is not the one used for the HDD cohort (hash differs)")

    con = connect(a.memory_limit, out / "_duckdb_tmp", a.threads)
    glob = snapshot_glob(a.snapshot).replace("'", "''")
    con.execute(f"CREATE VIEW raw AS SELECT * FROM read_parquet('{glob}', union_by_name=true)")
    with reg.open(encoding="utf-8", newline="") as fh:
        rows = [(r["model_norm"], r["device_type"]) for r in csv.DictReader(fh)]
    con.execute("CREATE TEMP TABLE registry(model_norm VARCHAR, device_type VARCHAR)")
    con.executemany("INSERT INTO registry VALUES (?, ?)", rows)

    print("[1/3] Failure flags in the snapshot (scan 1 of 2) ...", flush=True)
    con.execute(f"""
        CREATE TEMP TABLE failed AS
        SELECT {DRIVE_ID_SQL} AS drive_id, any_value(serial_number) AS serial_number, any_value(model) AS model,
               min(CAST(date AS DATE)) AS fail_date, count(*) AS failure_rows
        FROM raw WHERE CAST(failure AS INTEGER) = 1 AND CAST(date AS DATE) <= DATE '{test_end}'
        GROUP BY 1""")
    con.execute(f"""
        CREATE TEMP TABLE failed_hdd AS
        SELECT f.*, CASE WHEN f.fail_date <= DATE '{train_end}' THEN 'train'
                         WHEN f.fail_date <= DATE '{valid_end}' THEN 'valid' ELSE 'test' END AS period,
                    CASE WHEN f.fail_date <= DATE '{train_end}' THEN DATE '2016-01-01'
                         WHEN f.fail_date <= DATE '{valid_end}' THEN DATE '{train_end}' + INTERVAL 1 DAY
                         ELSE DATE '{valid_end}' + INTERVAL 1 DAY END AS period_start
        FROM failed f JOIN registry r ON r.model_norm = {NORM.replace('model', 'f.model')}
        WHERE r.device_type = 'HDD'""")
    train_f = sq(p3 / "model_train.parquet")
    rv = sq(Path(a.replay_valid).resolve() / "part_*.parquet")
    rt = sq(Path(a.replay_test).resolve() / "part_*.parquet")
    con.execute(f"""CREATE TEMP TABLE sel AS
        SELECT DISTINCT drive_id, 'train' AS period FROM read_parquet('{train_f}')
            WHERE eval_fail_date IS NOT NULL AND eval_fail_date <= DATE '{train_end}'
        UNION ALL SELECT DISTINCT drive_id, 'valid' FROM read_parquet('{rv}')
            WHERE fail_date BETWEEN DATE '{train_end}' + INTERVAL 1 DAY AND DATE '{valid_end}'
        UNION ALL SELECT DISTINCT drive_id, 'test' FROM read_parquet('{rt}')
            WHERE fail_date BETWEEN DATE '{valid_end}' + INTERVAL 1 DAY AND DATE '{test_end}'""")
    con.execute(f"""CREATE TEMP TABLE present AS
        SELECT DISTINCT drive_id, 'train' AS period FROM read_parquet('{train_f}')
        UNION ALL SELECT DISTINCT drive_id, 'valid' FROM read_parquet('{rv}')
        UNION ALL SELECT DISTINCT drive_id, 'test' FROM read_parquet('{rt}')""")
    counts = {
        "snapshot_failed_drives": con.execute("SELECT count(*) FROM failed").fetchone()[0],
        "hdd_cohort_failed_drives": con.execute("SELECT count(*) FROM failed_hdd").fetchone()[0],
        "training_failures": con.execute("SELECT count(*) FROM sel WHERE period = 'train'").fetchone()[0],
        "replay_2024_failures": con.execute("SELECT count(*) FROM sel WHERE period = 'valid'").fetchone()[0],
        "replay_test_failures": con.execute("SELECT count(*) FROM sel WHERE period = 'test'").fetchone()[0],
    }
    con.execute("""CREATE TEMP TABLE unplaced AS
        SELECT f.* FROM failed_hdd f ANTI JOIN sel s ON s.drive_id = f.drive_id AND s.period = f.period""")
    counts["unplaced"] = con.execute("SELECT count(*) FROM unplaced").fetchone()[0]
    counts["selected_failures_not_in_hdd_cohort_or_other_period"] = con.execute(
        "SELECT count(*) FROM sel s ANTI JOIN failed_hdd f ON f.drive_id = s.drive_id AND f.period = s.period").fetchone()[0]
    print("  " + json.dumps(counts), flush=True)

    print("[2/3] Records of the unplaced drives (scan 2 of 2) ...", flush=True)
    serials = [r[0] for r in con.execute("SELECT DISTINCT serial_number FROM unplaced").fetchall()]
    if serials:
        lst = ", ".join("'" + s.replace("'", "''") + "'" for s in serials)
        con.execute(f"""CREATE TEMP TABLE hist AS
            SELECT {DRIVE_ID_SQL} AS drive_id, CAST(date AS DATE) AS date, CAST(failure AS INTEGER) AS failure
            FROM raw WHERE serial_number IN ({lst}) AND CAST(date AS DATE) <= DATE '{test_end}'""")
    else:
        con.execute("CREATE TEMP TABLE hist(drive_id VARCHAR, date DATE, failure INTEGER)")
    res = con.execute("""
        SELECT u.drive_id, u.serial_number, u.model, u.period, u.period_start, u.fail_date, u.failure_rows,
               min(h.date) AS first_seen, max(h.date) AS last_seen, count(h.date) AS rows_total,
               count(DISTINCT h.date) AS days_total,
               max(h.date) FILTER (WHERE h.date < u.fail_date) AS last_record_before_failure,
               count(DISTINCT h.date) FILTER (WHERE h.date < u.fail_date) AS days_before_failure,
               count(DISTINCT h.date) FILTER (WHERE h.date < u.fail_date AND h.date >= u.period_start)
                   AS days_before_failure_same_period,
               count(*) FILTER (WHERE h.date = u.fail_date) AS rows_on_failure_day,
               count(DISTINCT h.date) FILTER (WHERE h.date > u.fail_date) AS days_after_failure,
               bool_or(p.drive_id IS NOT NULL) AS drive_in_selection_of_that_period
        FROM unplaced u LEFT JOIN hist h ON h.drive_id = u.drive_id
        LEFT JOIN present p ON p.drive_id = u.drive_id AND p.period = u.period
        GROUP BY ALL ORDER BY u.fail_date, u.drive_id""").df()
    con.close()

    print("[3/3] Reasons and tables ...", flush=True)
    def reason(r):
        if r.days_before_failure == 0:
            return "no_record_before_failure"
        if r.days_before_failure_same_period == 0:
            return "earlier_records_only_in_earlier_period"
        return "earlier_record_in_same_period"
    res["reason"] = res.apply(reason, axis=1) if len(res) else []
    res["make"] = [MAKES[make_of_model(m)] for m in res.model] if len(res) else []
    res["gap_days_before_failure"] = [(f - l).days if l is not None and str(l) != "NaT" else None
                                      for f, l in zip(res.fail_date, res.last_record_before_failure)] if len(res) else []
    res["failure_on_first_day_of_period"] = ([str(f)[:10] == str(p)[:10] for f, p in zip(res.fail_date, res.period_start)]
                                             if len(res) else [])
    res.to_csv(out / "unplaced_failures.csv", index=False)
    by = (res.groupby(["period", "reason"]).size().rename("drives").reset_index().to_dict("records") if len(res) else [])
    summary = {
        "what": "HDD-cohort failures in no training or replay selection (amendment A1.7, read-only)",
        "counts": counts, "published": PUBLISHED,
        "differences_to_published": {k: counts[k] - v for k, v in PUBLISHED.items() if k in counts},
        "by_period_and_reason": by,
        "by_make": res.make.value_counts().to_dict() if len(res) else {},
        "phase3_manifest_sha256": sha256_file(p3 / "phase3_manifest.json"),
        "model_train_sha256": sha256_file(p3 / "model_train.parquet"),
        "registry_sha256": sha256_file(reg),
        "runtime_seconds": round(time.time() - t0, 1),
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run": run_metadata({k: str(v) for k, v in vars(a).items()}),
    }
    (out / "unplaced_failures_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    import shutil
    shutil.rmtree(out / "_duckdb_tmp", ignore_errors=True)
    print(json.dumps({k: summary[k] for k in ("counts", "differences_to_published", "by_period_and_reason")},
                     indent=2, default=str))
    print(f"Done in {summary['runtime_seconds']} s. Send back: {out / 'unplaced_failures.csv'} and "
          f"{out / 'unplaced_failures_summary.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
