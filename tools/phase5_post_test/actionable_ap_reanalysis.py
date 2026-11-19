"""Post hoc, read-only re-analysis of the RQ3 "actionable" average precision (review of 19 Nov 2026).

Reads only the frozen replay inputs (daily scores, labels and failure dates of one period); nothing is
retrained or replayed. Three definitions are computed for every score:

  D0 original     exactly as src/drivepriority/simulation/prediction.py: positive = failure 8..31 days after
                  the decision day t; negatives = rows with label_30d == 0; rows with a failure 1..7 days
                  after t are dropped. Positives are kept even where label_30d is unknown, negatives are not.
                  (Must reproduce rq3_horizons.csv; this is the check that the re-implementation is exact.)
  D2 symmetric    same classes as D0 (rows 1..7 days before a failure dropped), but only decision days with a
                  complete 31-day follow-up inside the period (t <= period_end - 31), and a negative must be
                  known to survive day t+31 (later failure date, or the drive observed on or after t+31).
  D1 allocation   as D2, but rows 1..7 days before a failure are kept as NEGATIVES: a slot spent on a drive
                  that fails within a week is a wasted slot for the 7..30-day timely endpoint.

Scores are ranked as in prediction.py (missing score -> -1). AP is the step-wise average precision of
sklearn.metrics.average_precision_score, computed from per-score-value counts (exact, ties grouped).

Usage: python actionable_ap_reanalysis.py <replay_dir_with_part_*.parquet> <period_end YYYY-MM-DD> <out.json>
"""
from __future__ import annotations

import json
import sys

import duckdb
import numpy as np

SCORES = ["ml_h7", "ml_h14", "ml_h30", "gbm_h30", "smart_count", "smart_sum"]


def ap_auc_from_counts(values: np.ndarray, pos: np.ndarray, n: np.ndarray) -> dict:
    if len(values) == 0 or pos.sum() == 0 or pos.sum() == n.sum():
        return {"rows": int(n.sum()), "positives": int(pos.sum()), "prevalence": float("nan"),
                "average_precision": float("nan"), "roc_auc": float("nan"), "ap_over_prevalence": float("nan")}
    order = np.argsort(-values, kind="stable")
    pos, n = pos[order].astype(np.float64), n[order].astype(np.float64)
    tp, cnt = np.cumsum(pos), np.cumsum(n)
    P, N = tp[-1], cnt[-1] - tp[-1]
    precision = tp / cnt
    recall_step = pos / P
    ap = float(np.sum(recall_step * precision))
    neg = n - pos
    neg_above = np.concatenate([[0.0], np.cumsum(neg)[:-1]])          # negatives with a strictly higher score
    auc = float(np.sum(pos * (N - neg_above - neg) + 0.5 * pos * neg) / (P * N))
    return {"rows": int(cnt[-1]), "positives": int(P), "prevalence": float(P / cnt[-1]),
            "average_precision": ap, "roc_auc": auc, "ap_over_prevalence": ap / float(P / cnt[-1])}


def main(replay_dir: str, period_end: str, out: str) -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='5GB'")
    con.execute("SET preserve_insertion_order=false")
    src = replay_dir.rstrip("/") + "/part_*.parquet"
    con.execute(f"CREATE VIEW r AS SELECT * FROM read_parquet('{src}')")
    con.execute("CREATE TEMP TABLE ls AS SELECT drive_id, max(date) AS last_seen FROM r GROUP BY 1")
    cols = ", ".join(f"coalesce(CAST({s} AS DOUBLE), -1.0) AS {s}" for s in SCORES)
    con.execute(f"""CREATE TEMP TABLE x AS
        SELECT {cols},
               CASE WHEN r.fail_date IS NULL THEN NULL ELSE date_diff('day', r.date, r.fail_date) END AS gap,
               r.label_30d AS l30,
               (r.date <= DATE '{period_end}' - INTERVAL 31 DAY) AS full_follow,
               (r.fail_date IS NOT NULL OR ls.last_seen >= r.date + INTERVAL 31 DAY) AS known31
        FROM r JOIN ls USING (drive_id)""")
    defs = {
        "D0_original": "( (gap BETWEEN 8 AND 31) OR l30 = 0 ) AND NOT coalesce(gap BETWEEN 1 AND 7, false)",
        "D2_symmetric_conditional": "full_follow AND known31 AND NOT coalesce(gap BETWEEN 1 AND 7, false)",
        "D1_symmetric_allocation": "full_follow AND known31",
    }
    res = {"period_end": period_end, "definitions": {}}
    for d, keep in defs.items():
        res["definitions"][d] = {}
        for s in SCORES:
            a = con.execute(f"""SELECT {s} AS v, sum(CASE WHEN gap BETWEEN 8 AND 31 THEN 1 ELSE 0 END) AS p, count(*) AS n
                                FROM x WHERE {keep} GROUP BY 1""").fetchnumpy()
            res["definitions"][d][s] = ap_auc_from_counts(a["v"], a["p"], a["n"])
        extra = con.execute(f"""SELECT count(*) FILTER (WHERE gap BETWEEN 1 AND 7), count(*) FILTER (WHERE gap BETWEEN 8 AND 31)
                                 FROM x WHERE {keep}""").fetchone()
        res["definitions"][d]["_rows_failing_1_to_7d"] = int(extra[0])
        res["definitions"][d]["_rows_failing_8_to_31d"] = int(extra[1])
        print(d, {s: round(res["definitions"][d][s]["average_precision"], 6) for s in SCORES}, flush=True)
    json.dump(res, open(out, "w"), indent=1)


if __name__ == "__main__":
    main(*sys.argv[1:4])
