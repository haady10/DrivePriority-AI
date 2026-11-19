"""Lightweight verification of the Phase 5 result archives (no training, no replay, no large inputs).

Checks, for the test archive and the 2024 development archive in published_results/phase5/:
  1. the SHA-256 of each ZIP against published_results/phase5/SHA256SUMS.txt;
  2. the eight primary timely-capture rates recomputed from the per-event table (event_coverage.parquet:
     mean over eligible events of E*_timely_share, itself the mean over ten tie seeds) against cell_results.csv;
  3. the primary paired contrasts and interactions recomputed from the same table against rq1_contrasts.csv;
  4. the saved seed-0 service logs against the per-event table: an eligible event is captured by seed 0 if its
     drive has a completed inspection 7 to 30 days before failure, and the earliest such lead must equal
     E*_earliest_lead_seed0; every service date is the decision date + 1; no drive is scheduled twice a day.

Usage: python tools/verify_results.py [published_results/phase5]
Needs pandas and pyarrow only. Exit code 0 if everything matches, 1 otherwise.
"""
from __future__ import annotations

import hashlib
import io
import sys
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

ARCHIVES = {"test": "phase5_test_full_results.zip", "2024": "phase5_valid_full_results.zip"}
CELLS = ["E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8"]
# ML minus SMART for (re-inspection, scheduling); E1/E2 next-fresh, E5/E6 next-queue, E3/E4 cool-fresh, E7/E8 cool-queue
DELTA = {"next|fresh": ("E2", "E1"), "next|queue": ("E6", "E5"), "cool|fresh": ("E4", "E3"), "cool|queue": ("E8", "E7")}
TOL = 1e-9


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def check_archive(z: zipfile.ZipFile) -> list[str]:
    bad: list[str] = []
    rd = lambda n: io.BytesIO(z.read(n))
    ev = pd.read_parquet(rd("event_coverage.parquet"))
    cells = pd.read_csv(rd("cell_results.csv"))
    con = pd.read_csv(rd("rq1_contrasts.csv"))
    el = ev[ev.status == "eligible"]
    prim = cells[cells.group == "rq1_primary"].set_index("e_cell")
    rate = {c: float(el[f"{c}_timely_share"].mean()) for c in CELLS}
    for c in CELLS:
        if int(prim.loc[c, "eligible_events"]) != len(el):
            bad.append(f"{c}: eligible events {len(el)} vs {int(prim.loc[c, 'eligible_events'])}")
        if abs(rate[c] - float(prim.loc[c, "coverage"])) > TOL:
            bad.append(f"{c}: rate {rate[c]:.12f} vs cell_results {float(prim.loc[c, 'coverage']):.12f}")
    p = con[con["set"] == "primary_B20"].set_index("contrast").estimate
    d = {k: rate[a] - rate[b] for k, (a, b) in DELTA.items()}
    want = {f"delta_ml_minus_smart|{k}": v for k, v in d.items()}
    want["interaction_reinspection|fresh"] = d["cool|fresh"] - d["next|fresh"]
    want["interaction_reinspection|queue"] = d["cool|queue"] - d["next|queue"]
    want["interaction_scheduling|next"] = d["next|queue"] - d["next|fresh"]
    want["interaction_scheduling|cool"] = d["cool|queue"] - d["cool|fresh"]
    want["interaction_three_way"] = want["interaction_reinspection|queue"] - want["interaction_reinspection|fresh"]
    for k, v in want.items():
        if abs(v - float(p[k])) > TOL:
            bad.append(f"contrast {k}: {v:.12f} vs rq1_contrasts {float(p[k]):.12f}")
    fail = pd.to_datetime(el.set_index("drive_id").fail_date)
    for c in CELLS:
        s = pd.read_parquet(rd(f"service_log_{c}.parquet"), columns=["decision_date", "service_date", "drive_id", "completed"])
        if not (pd.to_datetime(s.service_date) - pd.to_datetime(s.decision_date) == pd.Timedelta(days=1)).all():
            bad.append(f"{c}: a service date is not decision date + 1")
        if s.duplicated(["decision_date", "drive_id"]).any():
            bad.append(f"{c}: a drive is scheduled twice on one day")
        s = s[s.completed & s.drive_id.isin(fail.index)]
        lead = (fail.loc[s.drive_id].values - pd.to_datetime(s.service_date).values) / np.timedelta64(1, "D")
        t = pd.Series(lead, index=s.drive_id.values)
        earliest = t[(t >= 7) & (t <= 30)].groupby(level=0).max()
        got = el.drive_id.map(earliest).fillna(-1).astype(int).values
        saved = el[f"{c}_earliest_lead_seed0"].astype(int).values
        n = int((got != saved).sum())
        if n:
            bad.append(f"{c}: {n} events differ between the seed-0 log and E{c[1:]}_earliest_lead_seed0")
    return bad


def main(folder: str | None = None) -> int:
    root = Path(folder) if folder else Path(__file__).resolve().parents[1] / "published_results" / "phase5"
    sums = dict(line.split()[::-1] for line in (root / "SHA256SUMS.txt").read_text().splitlines() if line.strip())
    problems = 0
    for period, name in ARCHIVES.items():
        path = root / name
        ok_hash = sha256(path) == sums.get(name)
        bad = check_archive(zipfile.ZipFile(path))
        problems += (not ok_hash) + len(bad)
        print(f"{period}: SHA-256 {'ok' if ok_hash else 'MISMATCH'}; "
              f"rates, contrasts and seed-0 logs {'ok' if not bad else 'MISMATCH'}")
        for b in bad:
            print("   ", b)
    print("all checks passed" if problems == 0 else f"{problems} problem(s)")
    return 0 if problems == 0 else 1


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:2]))
