"""Post hoc (read-only): drive-model cluster bootstrap of the primary contrasts and cells (18 Nov 2026).
Input: event_coverage.parquet of a replay (one row per event; E*_timely_share = mean over 10 tie seeds).
Clusters = drive models (model string from drive_id 'serial|model'). 4,000 resamples, seed 7."""
import sys, json, pandas as pd, numpy as np
out = {}
for per, path in (("test", sys.argv[1]), ("valid", sys.argv[2])):
    e = pd.read_parquet(path); e = e[e.status == "eligible"].copy(); e["model"] = e.drive_id.str.split("|").str[1]
    g = e.groupby("model"); n = g.size().values; M = len(n)
    idx = np.random.default_rng(7).integers(0, M, size=(4000, M))
    N = n[idx].sum(1)
    res = {"events": int(n.sum()), "clusters": int(M), "largest_cluster_events": int(n.max())}
    for c in [f"E{i}" for i in range(1, 9)]:
        v = g[f"{c}_timely_share"].sum().values
        res[c] = [v.sum() / n.sum() * 100, *np.percentile(v[idx].sum(1) / N * 100, [2.5, 97.5])]
    for k, (a, b) in {"next|fresh": ("E2", "E1"), "next|queue": ("E6", "E5"), "cool|fresh": ("E4", "E3"), "cool|queue": ("E8", "E7")}.items():
        v = (g[f"{a}_timely_share"].sum() - g[f"{b}_timely_share"].sum()).values
        res[k] = [v.sum() / n.sum() * 100, *np.percentile(v[idx].sum(1) / N * 100, [2.5, 97.5])]
    for k, (x, y) in {"int|fresh": (("E4", "E3"), ("E2", "E1")), "int|queue": (("E8", "E7"), ("E6", "E5"))}.items():
        v = ((g[f"{x[0]}_timely_share"].sum() - g[f"{x[1]}_timely_share"].sum()) - (g[f"{y[0]}_timely_share"].sum() - g[f"{y[1]}_timely_share"].sum())).values
        res[k] = [v.sum() / n.sum() * 100, *np.percentile(v[idx].sum(1) / N * 100, [2.5, 97.5])]
    out[per] = res
json.dump(out, open(sys.argv[3] if len(sys.argv) > 3 else "cluster_bootstrap.json", "w"), indent=1)
for per in out:
    print(per, {k: (v if isinstance(v, int) else [round(x, 1) for x in v]) for k, v in out[per].items()})
