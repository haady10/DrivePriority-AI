"""Phase 2 data profile over the integrated snapshot (feeds report Task 2 and Power BI).

Reads every row of the snapshot (read-only) and writes small CSV summaries:
  summary.json          totals, date range, missing calendar days
  by_year.csv           rows, drives, failures, annualized failure rate per year
  by_quarter.csv        same per quarter, plus observed days (coverage check)
  by_model.csv          per drive model (sorted by failures)
  missing_by_year.csv   % missing for every SMART *_raw column, per year
  feature_signal.csv    % non-zero in the 30 days before failure vs all other rows
  class_balance.csv     positive-row share for 7/14/30-day horizons, per year

Usage (Windows, from the repo root):
  .venv\\Scripts\\python -m drivepriority.analysis.profile --snapshot "<Integrated_Data\\snapshot>" --out results\\phase2_profile
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

from drivepriority.features.common import (MODEL_ATTRS, connect, create_source_view,
                                           run_metadata, write_json)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--snapshot", required=True, help="Folder of the integrated snapshot (contains YYYY/Qn/*.parquet)")
    p.add_argument("--out", default="results/phase2_profile")
    p.add_argument("--memory-limit", default="20GB")
    p.add_argument("--temp-dir", default="results/_duckdb_tmp")
    p.add_argument("--threads", type=int, default=None)
    a = p.parse_args(argv)

    t0 = time.time()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    con = connect(a.memory_limit, a.temp_dir, a.threads)
    all_cols = create_source_view(con, a.snapshot)
    print("[1/7] Scanning drives ...", flush=True)

    con.execute("""
        CREATE TEMP TABLE drives AS
        SELECT drive_id, any_value(model) AS model, any_value(manufacturer_group) AS manufacturer_group,
               max(capacity_bytes) AS capacity_bytes,
               min(date) AS first_seen, max(date) AS last_seen, count(*) AS drive_days,
               min(date) FILTER (WHERE failure = 1) AS fail_date,
               count(*) FILTER (WHERE failure = 1) AS failure_flags
        FROM src GROUP BY drive_id
    """)

    print("[2/7] Totals ...", flush=True)
    tot = con.execute("""
        SELECT count(*) AS rows, count(DISTINCT drive_id) AS drives,
               sum(failure) AS failure_flags, min(date) AS first_date, max(date) AS last_date,
               count(DISTINCT date) AS observed_days
        FROM src
    """).fetchone()
    missing_days = [str(r[0]) for r in con.execute("""
        SELECT d::DATE FROM range((SELECT min(date) FROM src), (SELECT max(date) FROM src) + INTERVAL 1 DAY,
                                   INTERVAL 1 DAY) t(d)
        WHERE d::DATE NOT IN (SELECT DISTINCT date FROM src) ORDER BY 1
    """).fetchall()]
    multi = con.execute("SELECT count(*) FROM drives WHERE failure_flags > 1").fetchone()[0]
    after = con.execute("SELECT count(*) FROM drives WHERE fail_date IS NOT NULL AND last_seen > fail_date").fetchone()[0]
    dup = con.execute("SELECT count(*) - count(DISTINCT (drive_id, date)) FROM src").fetchone()[0]

    print("[3/7] By year and quarter ...", flush=True)
    for name, period in (("by_year", "year(date)"),
                         ("by_quarter", "year(date) || 'Q' || quarter(date)")):
        con.execute(f"""
            COPY (
              SELECT {period} AS period, count(*) AS rows, count(DISTINCT drive_id) AS drives,
                     count(DISTINCT date) AS observed_days, sum(failure) AS failure_flags,
                     round(100.0 * sum(failure) / (count(*) / 365.0), 3) AS annualized_failure_rate_pct
              FROM src GROUP BY 1 ORDER BY 1
            ) TO '{(out / (name + '.csv')).as_posix()}' (HEADER)
        """)

    print("[4/7] By model ...", flush=True)
    con.execute(f"""
        COPY (
          SELECT model, manufacturer_group, round(max(capacity_bytes) / 1e12, 1) AS capacity_tb,
                 count(*) AS drives, sum(drive_days) AS drive_days,
                 count(fail_date) AS failed_drives,
                 round(100.0 * count(fail_date) / (sum(drive_days) / 365.0), 3) AS annualized_failure_rate_pct,
                 min(first_seen) AS first_seen, max(last_seen) AS last_seen
          FROM drives GROUP BY model, manufacturer_group ORDER BY failed_drives DESC, drives DESC
        ) TO '{(out / 'by_model.csv').as_posix()}' (HEADER)
    """)

    print("[5/7] Missing values per SMART column ...", flush=True)
    smart_cols = sorted([c for c in all_cols if c.startswith("smart_") and c.endswith("_raw")],
                        key=lambda c: int(c.split("_")[1]))
    if smart_cols:
        parts = ", ".join(f"round(100.0 * avg(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END), 2) AS {c}" for c in smart_cols)
        con.execute(f"CREATE TEMP TABLE miss AS SELECT year(date) AS year, {parts} FROM src_raw GROUP BY 1")
        con.execute(f"""
            COPY (SELECT year, attribute, missing_pct FROM miss
                  UNPIVOT (missing_pct FOR attribute IN ({', '.join(smart_cols)})) ORDER BY attribute, year)
            TO '{(out / 'missing_by_year.csv').as_posix()}' (HEADER)
        """)

    print("[6/7] Feature signal (30 days before failure vs other rows) ...", flush=True)
    con.execute("""
        CREATE TEMP VIEW tagged AS
        SELECT s.*, CASE WHEN d.fail_date IS NOT NULL AND s.date < d.fail_date
                              AND d.fail_date - s.date <= 30 THEN 'pre_failure_30d' ELSE 'other' END AS grp
        FROM src s JOIN drives d USING (drive_id)
        WHERE d.fail_date IS NULL OR s.date < d.fail_date
    """)
    sig = []
    for attr in MODEL_ATTRS:
        c = f"smart_{attr}_raw"
        sig.append(f"""
          SELECT '{c}' AS attribute, grp,
                 count(*) AS rows,
                 round(100.0 * avg(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END), 2) AS missing_pct,
                 round(100.0 * avg(CASE WHEN {c} > 0 THEN 1 ELSE 0 END), 3) AS nonzero_pct,
                 round(median({c}), 2) AS median_value,
                 round(quantile_cont({c}, 0.99), 2) AS p99_value
          FROM tagged GROUP BY grp""")
    con.execute(f"COPY ({' UNION ALL '.join(sig)} ORDER BY attribute, grp) TO "
                f"'{(out / 'feature_signal.csv').as_posix()}' (HEADER)")

    print("[7/7] Class balance per horizon ...", flush=True)
    con.execute(f"""
        COPY (
          SELECT year(s.date) AS year, count(*) AS usable_rows,
                 count(*) FILTER (WHERE d.fail_date - s.date BETWEEN 1 AND 7)  AS pos_7d,
                 count(*) FILTER (WHERE d.fail_date - s.date BETWEEN 1 AND 14) AS pos_14d,
                 count(*) FILTER (WHERE d.fail_date - s.date BETWEEN 1 AND 30) AS pos_30d,
                 round(100.0 * pos_30d / count(*), 4) AS pos_30d_pct
          FROM src s JOIN drives d USING (drive_id)
          WHERE d.fail_date IS NULL OR s.date < d.fail_date
          GROUP BY 1 ORDER BY 1
        ) TO '{(out / 'class_balance.csv').as_posix()}' (HEADER)
    """)

    summary = {
        "rows": tot[0], "drives": tot[1], "failure_flags": tot[2],
        "failed_drives": con.execute("SELECT count(fail_date) FROM drives").fetchone()[0],
        "first_date": tot[3], "last_date": tot[4], "observed_days": tot[5],
        "missing_calendar_days": missing_days,
        "drives_with_multiple_failure_flags": multi,
        "drives_observed_after_first_failure": after,
        "duplicate_drive_day_rows": dup,
        "smart_raw_columns_in_snapshot": len(smart_cols),
        "runtime_seconds": round(time.time() - t0, 1),
        "run": run_metadata(vars(a)),
    }
    write_json(out / "summary.json", summary)
    print(f"Done in {summary['runtime_seconds']} s. Rows={tot[0]:,} drives={tot[1]:,} "
          f"failed drives={summary['failed_drives']:,}. Output: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
