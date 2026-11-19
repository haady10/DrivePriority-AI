"""Post hoc, read-only: calendar-block bootstrap of the primary contrasts (review of 19 Nov 2026).

Event resampling treats failures as independent; failures that share a calendar week (site events, batch
removals, data gaps) may not be. Here whole calendar weeks of the failure date are resampled (4,000 resamples,
seed 11), keeping the pairing of cells within each event. Input: event_coverage.parquet (E*_timely_share =
mean over ten tie seeds). Nothing is replayed.

Usage: python time_block_bootstrap.py <test_event_coverage> <valid_event_coverage> <out.json>
"""
import json
import sys

import numpy as np
import pandas as pd

CONTRASTS = {"next|fresh": ("E2", "E1"), "next|queue": ("E6", "E5"), "cool|fresh": ("E4", "E3"), "cool|queue": ("E8", "E7")}


def run(path: str) -> dict:
    e = pd.read_parquet(path)
    e = e[e.status == "eligible"].copy()
    e["week"] = pd.to_datetime(e.fail_date).dt.to_period("W-SUN").astype(str)
    g = e.groupby("week")
    n = g.size().values
    idx = np.random.default_rng(11).integers(0, len(n), size=(4000, len(n)))
    N = n[idx].sum(1)
    out = {"events": int(n.sum()), "weeks": int(len(n)), "largest_week_events": int(n.max())}
    for k, (a, b) in CONTRASTS.items():
        v = (g[f"{a}_timely_share"].sum() - g[f"{b}_timely_share"].sum()).values
        out[k] = [v.sum() / n.sum() * 100, *np.percentile(v[idx].sum(1) / N * 100, [2.5, 97.5])]
    for k, (x, y) in {"int|fresh": (("E4", "E3"), ("E2", "E1")), "int|queue": (("E8", "E7"), ("E6", "E5"))}.items():
        v = ((g[f"{x[0]}_timely_share"].sum() - g[f"{x[1]}_timely_share"].sum())
             - (g[f"{y[0]}_timely_share"].sum() - g[f"{y[1]}_timely_share"].sum())).values
        out[k] = [v.sum() / n.sum() * 100, *np.percentile(v[idx].sum(1) / N * 100, [2.5, 97.5])]
    return out


if __name__ == "__main__":
    res = {"test": run(sys.argv[1]), "valid": run(sys.argv[2])}
    json.dump(res, open(sys.argv[3], "w"), indent=1)
    for p, r in res.items():
        print(p, {k: (v if isinstance(v, int) else [round(x, 1) for x in v]) for k, v in r.items()})
