"""Post hoc (read-only): drive-model cluster bootstrap of the amendment-A1 differences (20 Nov 2026).

Allowed by amendment A1.6 item 6 ("model-level ... intervals ... may be added from the saved per-event table, by the
same post hoc tools as for the frozen results. No replay."). Method identical to model_cluster_bootstrap.py
(18 Nov, manuscript Table IX): clusters are drive models (model string of drive_id 'serial|model'); every draw takes
the observed number of models with replacement (numpy default_rng(7), 4,000 draws, one index matrix per period);
an estimate is the sum of the per-event values over the drawn models divided by their number of events;
95% interval = 2.5 and 97.5 percentiles.

Input:  published_results/phase5_A1/posthoc_A1_{valid,test}_results.zip -> event_capture.parquet
        (one row per eligible event; one column per A1 cell = timely-capture share, mean over ten tie seeds)
Output: published_results/phase5_A1/A1_model_cluster_bootstrap.csv  (all differences, both periods, all settings)
        published_results/phase5_A1/A1_model_cluster_check.json     (reproduction of the frozen Table IX intervals)

Usage:  python tools/phase5_post_test/a1_model_cluster_bootstrap.py
"""
import io
import json
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
A1 = REPO / "published_results" / "phase5_A1"
PRIMARY = REPO / "docs" / "phase5_test_run_record" / "model_cluster_bootstrap.json"
SCORE = {"S1": "smart_count", "S2": "smart_count3", "S3": "smart_count_make_pct", "S4": "smart_count+quota",
         "S5": "smart_count3+quota", "M1": "ml_h30", "M2": "ml_h30+quota"}
# the nine families of amendment A1.6 item 2
FAMILIES = [("M1", "S1"), ("M1", "S2"), ("M1", "S3"), ("M1", "S4"), ("M1", "S5"),
            ("M2", "S4"), ("M2", "S5"), ("M2", "M1"), ("S4", "S1")]
POLICIES = [("next", "fresh"), ("next", "queue"), ("cool", "fresh"), ("cool", "queue")]
DRAWS, SEED = 4000, 7


def col(score: str, tie: str, budget: int, rei: str, sch: str) -> str:
    return f"{SCORE[score]}|tie-{tie}|B{budget}|{rei}|{sch}"


def run_period(events: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    models = events["drive_id"].str.split("|").str[1]
    grouped = events.groupby(models.rename("model"))
    n = grouped.size().to_numpy()
    m = len(n)
    idx = np.random.default_rng(SEED).integers(0, m, size=(DRAWS, m))
    n_drawn = n[idx].sum(axis=1)
    sums = grouped.sum(numeric_only=True)            # per-model sums of every capture column

    def ci(values: np.ndarray) -> tuple[float, float, float]:
        draws = values[idx].sum(axis=1) / n_drawn
        lo, hi = np.percentile(draws, [2.5, 97.5])
        return float(values.sum() / n.sum()), float(lo), float(hi)

    rows = []
    for tie in ("drive", "day"):
        for budget in (10, 20, 50):
            for rei, sch in POLICIES:
                for a, b in FAMILIES:
                    v = (sums[col(a, tie, budget, rei, sch)] - sums[col(b, tie, budget, rei, sch)]).to_numpy()
                    est, lo, hi = ci(v)
                    rows.append({"tie": tie, "budget": budget, "reinspection": rei, "scheduling": sch,
                                 "difference": f"{a}-{b}", "estimate": est, "model_ci_low": lo, "model_ci_high": hi})
    info = {"events": int(n.sum()), "models": int(m), "largest_model_events": int(n.max())}
    return pd.DataFrame(rows), info


def main() -> None:
    out, check = [], {"method": f"drive-model clusters, {DRAWS} draws, default_rng({SEED}), percentile 95%"}
    primary = json.loads(PRIMARY.read_text(encoding="utf-8"))
    for split, per in (("valid", "2024"), ("test", "test")):
        z = zipfile.ZipFile(A1 / f"posthoc_A1_{split}_results.zip")
        ev = pd.read_parquet(io.BytesIO(z.read("event_capture.parquet")))
        res, info = run_period(ev)
        res.insert(0, "period", per)
        for k, v in info.items():
            res[k] = v
        out.append(res)
        # gate: M1 - S1 with frozen ties at budget 20 must reproduce the frozen drive-model intervals (Table IX)
        key = "test" if split == "test" else "valid"
        worst = 0.0
        for rei, sch in POLICIES:
            r = res[(res.tie == "drive") & (res.budget == 20) & (res.reinspection == rei) & (res.scheduling == sch)
                    & (res.difference == "M1-S1")].iloc[0]
            ref = primary[key][f"{rei}|{sch}"]
            worst = max(worst, *(abs(x * 100 - y) for x, y in zip((r.estimate, r.model_ci_low, r.model_ci_high), ref)))
        check[per] = {**info, "primary_models": primary[key]["clusters"], "max_abs_diff_points_vs_table_IX": worst,
                      "reproduces_table_IX": bool(worst < 1e-9 and info["models"] == primary[key]["clusters"])}
    table = pd.concat(out, ignore_index=True)
    table.to_csv(A1 / "A1_model_cluster_bootstrap.csv", index=False)
    (A1 / "A1_model_cluster_check.json").write_text(json.dumps(check, indent=1), encoding="utf-8")
    print(json.dumps(check, indent=1))
    if not all(check[p]["reproduces_table_IX"] for p in ("2024", "test")):
        raise SystemExit("the frozen drive-model intervals were NOT reproduced; do not use this output")


if __name__ == "__main__":
    main()
