"""Post hoc, read-only: timely capture by manufacturer with paired event-bootstrap intervals (manuscript Table XI, Fig. 5).

Reproducible replacement (20 Nov 2026) for the by-make table of 14 Nov 2026, whose generating script was not kept in
the repository. Input: event_coverage.parquet of a result archive (eligible events, E*_timely_share = mean over ten
tie seeds). Make from the model string (ST -> Seagate, TOSHIBA -> Toshiba, HGST -> HGST, WD or WUH -> WDC). Within
each make the events are resampled with the engine's own bootstrap (src/drivepriority/simulation/stats.py,
2,000 resamples, seed 20261111, 95% percentile intervals), so cells and differences are paired. "never_captured_%" is
the share of the make's eligible events with E*_timely_share = 0 in all eight cells. Nothing is replayed.

Usage: python by_make_table.py <result_dir_or_zip> <period label> <out.csv>
"""
from __future__ import annotations

import io
import sys
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from drivepriority.simulation.stats import bootstrap_means, interval  # noqa: E402

CELLS = ["E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8"]
DIFFS = {"d_next_fresh": ("E2", "E1"), "d_next_queue": ("E6", "E5"), "d_cool_fresh": ("E4", "E3"), "d_cool_queue": ("E8", "E7")}


def make_of(model: pd.Series) -> np.ndarray:
    return np.select([model.str.startswith("ST"), model.str.startswith("TOSHIBA"), model.str.startswith("HGST"),
                      model.str.startswith("WD") | model.str.startswith("WUH")], ["Seagate", "Toshiba", "HGST", "WDC"], "other")


def load(src: str) -> pd.DataFrame:
    if src.endswith(".zip"):
        return pd.read_parquet(io.BytesIO(zipfile.ZipFile(src).read("event_coverage.parquet")))
    return pd.read_parquet(Path(src) / "event_coverage.parquet")


def main(src: str, period: str, out: str) -> None:
    e = load(src)
    e = e[e.status == "eligible"].copy()
    e["make"] = make_of(e.drive_id.str.split("|").str[1])
    rows = []
    for m, g in e.groupby("make"):
        P = g[[f"{c}_timely_share" for c in CELLS]].to_numpy()
        B = bootstrap_means(P, 2000, 20261111)
        row = {"period": period, "make": m, "events": len(g)}
        for i, c in enumerate(CELLS):
            lo, hi = interval(B[:, i])
            row[c] = f"{P[:, i].mean() * 100:.1f} [{lo * 100:.1f},{hi * 100:.1f}]"
        for k, (a, b) in DIFFS.items():
            ia, ib = CELLS.index(a), CELLS.index(b)
            lo, hi = interval(B[:, ia] - B[:, ib])
            row[k] = f"{(P[:, ia] - P[:, ib]).mean() * 100:+.1f} [{lo * 100:+.1f},{hi * 100:+.1f}]"
        row["never_captured_%"] = round(float((P.max(axis=1) == 0).mean() * 100), 1)
        rows.append(row)
    pd.DataFrame(rows).to_csv(out, index=False)
    print(pd.DataFrame(rows).to_string(index=False))


if __name__ == "__main__":
    main(*sys.argv[1:4])
