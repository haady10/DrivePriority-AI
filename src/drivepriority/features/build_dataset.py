"""Phase 3: labels, past-only features and the train/validation/test modelling files.

Implements Phase 1 methodology sections 13-18 on the integrated snapshot (read-only):

* Drive identity  = serial_number + model. One row per drive per day. Duplicate drive-days
  are collapsed by a fixed rule: (1) a row with failure = 1 wins, (2) otherwise the row with
  the most non-null modelling values, (3) otherwise the smallest content hash. Nothing is
  averaged or invented. Groups whose rows DIFFER are flagged (dup_conflict = 1, never a
  model input) and written, with every original row, to duplicate_conflicts.parquet;
  duplicate_audit.csv and the manifest count exact vs conflicting groups and the differing
  columns (sampled drives only; the cleaning reports and the profile hold the fleet totals).
* Event           = first recorded failure flag of a drive. The failure day and any
  later rows are removed from model inputs (no leakage of the event itself).
* Labels (H = 7, 14, 30 days), for a prediction made at the end of day t:
      1     first failure in (t, t+H]
      0     no failure in (t, t+H] and the drive is still observed on/after t+H
      NULL  unknown (drive vanished without a failure flag, or t+H passes the end
            of the row's split = purge gap). Unknown is never treated as healthy.
* Features        = past-only: today's raw SMART values, missing flags, 7- and
  30-day changes of error counters, drive age, history length, capacity.
* SMART baseline  = smart_rule_flag (any of raw 5/187/188/197/198 > 0) and
  smart_rule_score (their sum), for the RQ1 comparison.
* Splits          = by date (defaults: train <= 2023-12-31, valid <= 2024-12-31,
  test <= 2026-06-30). Override for an interim run on fewer years.
* Sampling        = whole drives by SHA-256 hash of drive_id (never single rows):
      train rows: every drive with a failure inside the training period
                  + healthy drives with hash bucket < train-rate (weight 1/rate)
      valid/test: an outcome-independent hash sample (eval-rate) of ALL drives,
                  so prevalence and the simulated fleet stay realistic.

Outputs (in --out):
  model_train.parquet, model_valid.parquet, model_test.parquet
  feature_dictionary.csv, phase3_manifest.json, duplicate_audit.csv, duplicate_conflicts.parquet

Usage (Windows, from the repo root):
  .venv\\Scripts\\python -m drivepriority.features.build_dataset --snapshot "<Integrated_Data\\snapshot>" --out data\\processed\\phase3
"""
from __future__ import annotations

import argparse
import csv
import time
from datetime import date
from pathlib import Path

from drivepriority.features.common import (COUNTER_ATTRS, MODEL_ATTRS, OTHER_ATTRS,
                                           SMART_RULE_ATTRS, connect, create_source_view,
                                           run_metadata, write_json)

HORIZONS = (7, 14, 30)
ID_COLUMNS = ["drive_id", "date", "model", "manufacturer_group", "split", "weight", "eval_fail_date"]
LABEL_COLUMNS = [f"label_{h}d" for h in HORIZONS]
BASELINE_COLUMNS = ["smart_rule_flag", "smart_rule_score"]


def feature_definitions() -> list[tuple[str, str, str]]:
    """(name, SQL expression over table `obs` with window w7/w30, description)."""
    feats: list[tuple[str, str, str]] = []
    for a in MODEL_ATTRS:
        c = f"smart_{a}_raw"
        feats.append((c, c, f"SMART {a} raw value on day t"))
        feats.append((f"smart_{a}_missing", f"CAST({c} IS NULL AS INTEGER)", f"1 if SMART {a} not reported on day t"))
    for a in COUNTER_ATTRS:
        c = f"smart_{a}_raw"
        # NULL (not 0) when there is no earlier reported value inside the window, so a true zero
        # change is never confused with a missing comparison (count includes the current row).
        feats.append((f"smart_{a}_diff7",
                      f"CASE WHEN count({c}) OVER w7 >= 2 THEN {c} - first_value({c} IGNORE NULLS) OVER w7 END",
                      f"Change of SMART {a} over the last 7 days (past only; NULL if no earlier value in the window)"))
        feats.append((f"smart_{a}_diff30",
                      f"CASE WHEN count({c}) OVER w30 >= 2 THEN {c} - first_value({c} IGNORE NULLS) OVER w30 END",
                      f"Change of SMART {a} over the last 30 days (past only; NULL if no earlier value in the window)"))
    feats += [
        ("smart_194_mean7", "avg(smart_194_raw) OVER w7", "Mean temperature (SMART 194) over the last 7 days"),
        ("age_days", "smart_9_raw / 24.0", "Power-on age in days (SMART 9 hours / 24)"),
        ("history_days", "CAST(date - first_seen AS INTEGER)", "Days since the drive was first observed"),
        ("days_since_prev_obs", "CAST(date - lag(date) OVER wd AS INTEGER)", "Gap since previous observation (1 = daily)"),
        ("capacity_tb", "capacity_bytes / 1e12", "Drive capacity in TB"),
        ("error_counter_total", " + ".join(f"coalesce(smart_{a}_raw, 0)" for a in COUNTER_ATTRS),
         "Sum of the error counters 5/187/188/197/198/199"),
    ]
    return feats


def write_duplicate_audit(con, out: Path, test_end) -> dict:
    """Duplicate drive-day audit over the sampled drives (reads only the small duplicate set)."""
    cmp_cols = ["capacity_bytes", "failure"] + [f"smart_{x}_raw" for x in MODEL_ATTRS]
    tot = con.execute("""
        SELECT count(*), count(*) FILTER (WHERE NOT conflicting), count(*) FILTER (WHERE conflicting),
               count(*) FILTER (WHERE conflicting AND any_failure = 1), coalesce(sum(n_rows - 1), 0)
        FROM dup_groups""").fetchone()
    differing = {}
    if tot[2]:
        # aggregate per group first, then count groups where the column differs
        sub = ", ".join(
            f"(min({c}) <> max({c}) OR (count({c}) > 0 AND count({c}) < count(*))) AS {c}" for c in cmp_cols)
        row = con.execute(f"""
            SELECT {', '.join(f'count(*) FILTER (WHERE {c})' for c in cmp_cols)} FROM (
              SELECT {sub} FROM obs_all a SEMI JOIN (SELECT drive_id, date FROM dup_groups WHERE conflicting) g
                   USING (drive_id, date) GROUP BY drive_id, date)""").fetchone()
        differing = {c: n for c, n in zip(cmp_cols, row) if n}
    con.execute(f"""
        COPY (SELECT year(date) AS year, count(*) AS duplicate_groups,
                     count(*) FILTER (WHERE NOT conflicting) AS exact_groups,
                     count(*) FILTER (WHERE conflicting) AS conflicting_groups,
                     count(*) FILTER (WHERE conflicting AND any_failure = 1) AS conflicting_with_failure,
                     sum(n_rows - 1) AS extra_rows_removed
              FROM dup_groups GROUP BY 1 ORDER BY 1)
        TO '{(out / 'duplicate_audit.csv').as_posix()}' (HEADER)""")
    con.execute(f"""
        COPY (SELECT a.*, (a.rn = 1) AS kept FROM obs_all a
              SEMI JOIN (SELECT drive_id, date FROM dup_groups WHERE conflicting) g USING (drive_id, date)
              ORDER BY drive_id, date, rn)
        TO '{(out / 'duplicate_conflicts.parquet').as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
    return {"scope": "sampled drives only, date <= test_end (fleet totals: cleaning reports and profile)",
            "compared_columns": cmp_cols,
            "tie_break": "failure=1, then most non-null modelling values, then smallest content hash",
            "duplicate_groups": tot[0], "exact_groups": tot[1], "conflicting_groups": tot[2],
            "conflicting_groups_involving_failure": tot[3], "extra_rows_removed": tot[4],
            "conflicting_groups_by_differing_column": differing}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--snapshot", required=True, help="Folder of the integrated snapshot")
    p.add_argument("--out", default="data/processed/phase3")
    p.add_argument("--train-end", default="2023-12-31")
    p.add_argument("--valid-end", default="2024-12-31")
    p.add_argument("--test-end", default="2026-06-30")
    p.add_argument("--train-rate", type=float, default=0.02,
                   help="Share of healthy drives sampled for training (default 0.02 = 2%%)")
    p.add_argument("--eval-rate", type=float, default=0.10,
                   help="Share of ALL drives sampled for validation/test (default 0.10)")
    p.add_argument("--memory-limit", default="20GB")
    p.add_argument("--temp-dir", default="results/_duckdb_tmp")
    p.add_argument("--threads", type=int, default=None)
    a = p.parse_args(argv)

    train_end, valid_end, test_end = (date.fromisoformat(x) for x in (a.train_end, a.valid_end, a.test_end))
    if not (train_end < valid_end < test_end):
        raise SystemExit("Need train-end < valid-end < test-end")
    for r in (a.train_rate, a.eval_rate):
        if not 0 < r <= 1:
            raise SystemExit("Sampling rates must be in (0, 1]")

    t0 = time.time()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    con = connect(a.memory_limit, a.temp_dir, a.threads)
    create_source_view(con, a.snapshot)
    tr_b, ev_b = int(round(a.train_rate * 10000)), int(round(a.eval_rate * 10000))

    print("[1/5] Drive table and first failure events ...", flush=True)
    con.execute(f"""
        CREATE TEMP TABLE drives AS
        SELECT drive_id,
               min(date) AS first_seen, max(date) AS last_seen,
               min(date) FILTER (WHERE failure = 1) AS fail_date,
               count(*) FILTER (WHERE failure = 1) AS failure_flags,
               ('0x' || substr(sha256(drive_id), 1, 8))::UBIGINT % 10000 AS bucket
        FROM src WHERE date <= DATE '{test_end}' GROUP BY drive_id
    """)
    con.execute(f"""
        CREATE TEMP TABLE sel AS
        SELECT *,
               (fail_date IS NOT NULL AND fail_date <= DATE '{train_end}') OR bucket < {tr_b} AS in_train,
               bucket < {ev_b} AS in_eval,
               CASE WHEN fail_date IS NOT NULL AND fail_date <= DATE '{train_end}' THEN 1.0
                    ELSE 1.0 / {a.train_rate} END AS train_weight,
               1.0 / {a.eval_rate} AS eval_weight
        FROM drives
        WHERE ((fail_date IS NOT NULL AND fail_date <= DATE '{train_end}') OR bucket < {tr_b}) OR bucket < {ev_b}
    """)

    print("[2/5] Collecting rows of sampled drives (one row per drive-day) ...", flush=True)
    # Content signature of a record over the modelling columns (identical rows -> identical hash).
    sig_cols = ["s.capacity_bytes", "s.failure"] + [f"s.smart_{x}_raw" for x in MODEL_ATTRS]
    nn_expr = " + ".join(f"CAST({c} IS NOT NULL AS INTEGER)" for c in sig_cols[2:])
    con.execute(f"""
        CREATE TEMP TABLE obs_all AS
        SELECT s.*, d.first_seen, d.last_seen, d.fail_date, d.in_train, d.in_eval,
               d.train_weight, d.eval_weight,
               hash({', '.join(sig_cols)}) AS rec_sig,
               count(*) OVER (PARTITION BY s.drive_id, s.date) AS day_rows,
               row_number() OVER (PARTITION BY s.drive_id, s.date
                                  ORDER BY s.failure DESC, ({nn_expr}) DESC,
                                           hash({', '.join(sig_cols)}) ASC) AS rn
        FROM src s JOIN sel d USING (drive_id)
        WHERE s.date <= DATE '{test_end}'
    """)
    # Flag drive-days whose duplicate rows differ (computed on the small set of duplicated days only).
    con.execute("""
        CREATE TEMP TABLE dup_groups AS
        SELECT drive_id, date, count(*) AS n_rows,
               min(rec_sig) <> max(rec_sig) AS conflicting,
               max(failure) AS any_failure
        FROM obs_all WHERE day_rows > 1 GROUP BY drive_id, date
    """)
    con.execute("""
        CREATE TEMP VIEW obs AS
        SELECT a.* EXCLUDE (rn, day_rows, rec_sig),
               CAST(coalesce(g.conflicting, FALSE) AS INTEGER) AS dup_conflict
        FROM obs_all a LEFT JOIN dup_groups g USING (drive_id, date)
        WHERE a.rn = 1
    """)
    dup_rows = con.execute("SELECT coalesce(sum(n_rows - 1), 0) FROM dup_groups").fetchone()[0]

    print("[3/5] Past-only features, labels and splits ...", flush=True)
    feats = feature_definitions()
    feat_sql = ",\n               ".join(f"{expr} AS {name}" for name, expr, _ in feats)
    rule = " OR ".join(f"smart_{x}_raw > 0" for x in SMART_RULE_ATTRS)
    rule_score = " + ".join(f"coalesce(smart_{x}_raw, 0)" for x in SMART_RULE_ATTRS)
    split_end = (f"CASE split WHEN 'train' THEN DATE '{train_end}' "
                 f"WHEN 'valid' THEN DATE '{valid_end}' ELSE DATE '{test_end}' END")
    labels = []
    for h in HORIZONS:
        labels.append(f"""CASE WHEN date + INTERVAL {h} DAY > {split_end} THEN NULL
                     WHEN fail_date IS NOT NULL AND fail_date > date AND fail_date <= date + INTERVAL {h} DAY THEN 1
                     WHEN last_seen >= date + INTERVAL {h} DAY THEN 0
                     ELSE NULL END AS label_{h}d""")
    con.execute(f"""
        CREATE TEMP TABLE feat AS
        SELECT drive_id, date, model, manufacturer_group, first_seen, last_seen, fail_date,
               in_train, in_eval, train_weight, eval_weight, dup_conflict,
               {feat_sql},
               CAST(({rule}) AS INTEGER) AS smart_rule_flag,
               {rule_score} AS smart_rule_score
        FROM obs
        WINDOW wd  AS (PARTITION BY drive_id ORDER BY date),
               w7  AS (PARTITION BY drive_id ORDER BY date RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW),
               w30 AS (PARTITION BY drive_id ORDER BY date RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW)
    """)
    # Remove the failure day and anything after it from model inputs (features above were
    # computed first so that no window could read post-event rows: they only look backwards).
    con.execute(f"""
        CREATE TEMP TABLE final AS
        SELECT *, {', '.join(labels)} FROM (
          SELECT *, CASE WHEN date <= DATE '{train_end}' THEN 'train'
                         WHEN date <= DATE '{valid_end}' THEN 'valid' ELSE 'test' END AS split
          FROM feat WHERE fail_date IS NULL OR date < fail_date
        )
        WHERE (split = 'train' AND in_train) OR (split <> 'train' AND in_eval)
    """)

    print("[4/5] Writing Parquet files ...", flush=True)
    feature_names = [n for n, _, _ in feats]
    cols = (["drive_id", "date", "model", "manufacturer_group", "split",
             "CASE WHEN split = 'train' THEN train_weight ELSE eval_weight END AS weight",
             "fail_date AS eval_fail_date", "dup_conflict"] + feature_names + BASELINE_COLUMNS + LABEL_COLUMNS)
    files = {}
    for split in ("train", "valid", "test"):
        f = out / f"model_{split}.parquet"
        con.execute(f"""COPY (SELECT {', '.join(cols)} FROM final WHERE split = '{split}' ORDER BY date, drive_id)
                        TO '{f.as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
        files[split] = f.name

    print("[5/5] Manifest and feature dictionary ...", flush=True)
    stats = {}
    for split in ("train", "valid", "test"):
        row = con.execute(f"""
            SELECT count(*), count(DISTINCT drive_id), min(date), max(date),
                   count(DISTINCT drive_id) FILTER (WHERE fail_date IS NOT NULL AND fail_date <= {split_end})
            FROM final WHERE split = '{split}'""").fetchone()
        s = {"rows": row[0], "drives": row[1], "first_date": row[2], "last_date": row[3],
             "drives_with_failure_event": row[4], "labels": {}}
        for h in HORIZONS:
            lab = con.execute(f"""
                SELECT count(*) FILTER (WHERE label_{h}d = 1), count(*) FILTER (WHERE label_{h}d = 0),
                       count(*) FILTER (WHERE label_{h}d IS NULL)
                FROM final WHERE split = '{split}'""").fetchone()
            s["labels"][f"{h}d"] = {"positive": lab[0], "negative": lab[1], "unknown": lab[2],
                                    "positive_pct_of_known": round(100.0 * lab[0] / max(lab[0] + lab[1], 1), 4)}
        stats[split] = s

    fleet = con.execute(f"""
        SELECT count(*), count(fail_date), count(*) FILTER (WHERE failure_flags > 1),
               count(*) FILTER (WHERE fail_date IS NOT NULL AND last_seen > fail_date)
        FROM drives""").fetchone()
    dup_audit = write_duplicate_audit(con, out, test_end)
    with open(out / "feature_dictionary.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["feature", "definition_sql", "description", "role"])
        for n, e, d in feats:
            w.writerow([n, e, d, "model_input"])
        w.writerow(["smart_rule_flag", rule, "Traditional SMART alert (raw 5/187/188/197/198 > 0)", "baseline_only"])
        w.writerow(["smart_rule_score", rule_score, "Sum of the SMART baseline counters", "baseline_only"])
        for h in HORIZONS:
            w.writerow([f"label_{h}d", "see module docstring", f"First failure within {h} days after t", "target"])
        w.writerow(["weight", "1/sampling rate", "Inverse-probability sampling weight of the drive", "weight"])
        w.writerow(["eval_fail_date", "first failure date", "For evaluation/simulation ONLY", "never_a_feature"])
        w.writerow(["dup_conflict", "1 if the drive-day had duplicate rows that differ",
                    "For sensitivity analysis ONLY", "never_a_feature"])

    manifest = {
        "phase": "3 - labels, features, splits",
        "splits": {"train_end": str(train_end), "valid_end": str(valid_end), "test_end": str(test_end),
                   "purge_rule": "label is NULL when t + horizon passes the split end"},
        "sampling": {"unit": "whole drive (serial_number|model)", "hash": "sha256(drive_id) first 32 bits mod 10000",
                     "train_rate_healthy": a.train_rate, "eval_rate_all": a.eval_rate,
                     "train_rule": "all drives failing <= train_end + healthy hash sample",
                     "eval_rule": "outcome-independent hash sample of all drives"},
        "fleet_up_to_test_end": {"drives": fleet[0], "failed_drives": fleet[1],
                                 "drives_with_multiple_failure_flags": fleet[2],
                                 "drives_observed_after_first_failure": fleet[3]},
        "duplicate_drive_day_rows_collapsed": dup_rows,
        "duplicates": dup_audit,
        "feature_columns": feature_names,
        "baseline_columns": BASELINE_COLUMNS,
        "label_columns": LABEL_COLUMNS,
        "never_use_as_features": ["drive_id", "date", "split", "weight", "eval_fail_date", "model",
                                  "dup_conflict"] + LABEL_COLUMNS,
        "split_stats": stats,
        "files": files,
        "runtime_seconds": round(time.time() - t0, 1),
        "run": run_metadata(vars(a)),
    }
    write_json(out / "phase3_manifest.json", manifest)
    print(f"Done in {manifest['runtime_seconds']} s.")
    for split, s in stats.items():
        l30 = s["labels"]["30d"]
        print(f"  {split:5s}: {s['rows']:>12,} rows  {s['drives']:>9,} drives  "
              f"{s['drives_with_failure_event']:>7,} failed  30d pos/neg/unknown = "
              f"{l30['positive']:,}/{l30['negative']:,}/{l30['unknown']:,}")
    print(f"Output: {out.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
