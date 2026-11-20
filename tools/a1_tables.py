"""Data behind the manuscript tables and text of amendment A1 (post hoc fairer SMART baselines).

Reads only published_results/phase5_A1/ (the two result zips and wording_rule.csv); no number is typed by hand.
Used by tools/manuscript_tables_figures.py. Can also be run on its own:  python tools/a1_tables.py <out.json>
"""
import io, json, sys, zipfile
from pathlib import Path
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
A1 = REPO / "published_results" / "phase5_A1"
SCORES = {"smart_count": "S1", "smart_count3": "S2", "smart_count_make_pct": "S3", "smart_count+quota": "S4",
          "smart_count3+quota": "S5", "ml_h30": "M1", "ml_h30+quota": "M2"}
POLICIES = ["next|fresh", "next|queue", "cool|fresh", "cool|queue"]


def _read(split: str, name: str) -> pd.DataFrame:
    z = zipfile.ZipFile(A1 / f"posthoc_A1_{split}_results.zip")
    return pd.read_csv(io.BytesIO(z.read(name)))


def a1_data() -> dict:
    out = {"cells": {}, "contrasts": {}, "run": {}, "rule": [], "checks": {}}
    for split, per in (("valid", "valid"), ("test", "test")):
        c = _read(split, "cells.csv")
        k = _read(split, "contrasts.csv")
        info = json.loads(zipfile.ZipFile(A1 / f"posthoc_A1_{split}_results.zip").read("run_info.json"))
        rep = json.loads(zipfile.ZipFile(A1 / f"posthoc_A1_{split}_results.zip").read("reproduction.json"))
        out["run"][per] = {"rows": info["rows"], "drives": info["drives"], "eligible": info["events"]["eligible"],
                           "reproduction_passed": rep["passed"], "values_compared": rep["values_compared"],
                           "violations": int(c.engine_rule_violations.sum()), "cells": int(len(c))}
        for _, r in c.iterrows():
            key = f"{per}|{r.tie_mode}|{int(r.budget_per_10000)}|{r.reinspection}|{r.scheduling}|{SCORES[r.score]}"
            out["cells"][key] = {"cov": r.coverage, "lo": r.coverage_ci_low, "hi": r.coverage_ci_high,
                                 "seagate": r.share_completed_Seagate, "distinct": r.distinct_drives_inspected,
                                 "completed": r.completed}
        for _, r in k.iterrows():
            key = f"{per}|{r.tie}|{int(r.budget)}|{r.reinspection}|{r.scheduling}|{r.contrast}"
            out["contrasts"][key] = [r.estimate, r.ci_low, r.ci_high]
        av = _read(split, f"smart_availability_by_make_{split}.csv")
        out["run"][per]["min_reported_share_5_197_198"] = float(av[["reported_share_5", "reported_share_197", "reported_share_198"]].min().min())
    w = pd.read_csv(A1 / "wording_rule.csv")
    out["rule"] = w.fillna("").to_dict(orient="records")
    # derived facts used in the text (all computed here, not typed)
    cells = out["cells"]
    for per in ("valid", "test"):
        best = {}
        for p in POLICIES:
            cand = [(cells[f"{per}|{t}|20|{p}|{s}"]["cov"], s, t) for t in ("drive", "day") for s in ("S1", "S2", "S3", "S4", "S5")]
            best[p] = max(cand)
        out["checks"][f"{per}|best_smart_B20"] = {p: [v, s, t] for p, (v, s, t) in best.items()}
        allc = [(v["cov"], k) for k, v in cells.items() if k.startswith(per + "|") and "|20|" in k]
        out["checks"][f"{per}|highest_B20"] = max(allc)
        con = out["contrasts"]
        for b in (10, 20, 50):
            for tie in ("drive", "day"):
                ml_all = [p for p in POLICIES if all(con[f"{per}|{tie}|{b}|{p}|M1-S{k}"][1] > 0 for k in range(1, 6))]
                q_all = [p for p in POLICIES if all(con[f"{per}|{tie}|{b}|{p}|{c}"][1] > 0 for c in ("M2-S4", "M2-S5"))]
                out["checks"][f"{per}|{tie}|{b}|ml_ahead_of_all_smart"] = ml_all
                out["checks"][f"{per}|{tie}|{b}|m2_ahead_of_s4_s5"] = q_all
        b20 = pd.DataFrame([v for k, v in cells.items() if k.startswith(per + "|") and "|20|" in k])
        out["checks"][f"{per}|spearman_distinct_cov_B20"] = float(b20["distinct"].corr(b20["cov"], method="spearman"))
        out["checks"][f"{per}|n_cells_B20"] = int(len(b20))
    return out


if __name__ == "__main__":
    dest = Path(sys.argv[1] if len(sys.argv) > 1 else "a1_data.json")
    dest.write_text(json.dumps(a1_data(), indent=1, default=float), encoding="utf-8")
    print(f"saved {dest}")
