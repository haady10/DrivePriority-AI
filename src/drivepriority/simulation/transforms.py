"""RQ2 persistence rules written as running statistics of the daily score (protocol section 13).

    daily      priority = today's score                                  (no persistence)
    confirm c  priority = MINIMUM of the last c observed scores          ("warning on c days in a row")
    ewma a     priority = a * today + (1 - a) * previous priority        (smoothed score)
    retain h   priority = MAXIMUM of the last h + 1 observed scores      ("keep the warning h more days")

Why this form: "score above a threshold on c consecutive days" is the same statement as "the minimum
of the last c scores is above the threshold". Ranking drives by that minimum and taking the top K
therefore applies the classic rule with whatever threshold fills exactly K places, so every rule is
compared at the same list size without tuning a threshold.

History is over a drive's OBSERVED days. When a drive was not observed for more than `reset_gap`
calendar days its history starts again. Each value uses only that day and earlier days.
"""
from __future__ import annotations

import numpy as np

from .replay_input import ReplayData


def persistence_priority(data: ReplayData, score: np.ndarray, rule: dict, reset_gap: int = 7) -> np.ndarray:
    """Priority per row for one rule. `rule` is one entry of protocol rq2.rules."""
    kind = rule["kind"]
    if kind == "daily":
        return score.astype(np.float64).copy()
    if np.isnan(score).any():
        raise ValueError("persistence rules need a score on every row")
    out = np.empty(len(score), dtype=np.float64)
    last_seen = np.full(data.n_drives, -10**6, dtype=np.int64)
    if kind == "ewma":
        a = float(rule["alpha"])
        if not 0 < a <= 1:
            raise ValueError("ewma alpha must be in (0, 1]")
        state = np.full(data.n_drives, np.nan)
        for i in range(data.n_days):
            rows = data.rows_of_day(i)
            if rows.start == rows.stop:
                continue
            t = data.first_day + i
            d, x = data.drive[rows], score[rows]
            prev = state[d]
            fresh = np.isnan(prev) | ((t - last_seen[d]) > reset_gap)
            p = np.where(fresh, x, a * x + (1 - a) * prev)
            state[d], last_seen[d], out[rows] = p, t, p
        return out
    if kind in ("confirm", "retain"):
        width = int(rule["days"]) if kind == "confirm" else int(rule["days"]) + 1   # scores in the window
        if width < 2:
            raise ValueError("window must cover at least two days")
        hist = np.full((data.n_drives, width - 1), np.nan)                           # earlier scores, oldest first
        for i in range(data.n_days):
            rows = data.rows_of_day(i)
            if rows.start == rows.stop:
                continue
            t = data.first_day + i
            d, x = data.drive[rows], score[rows].astype(np.float64)
            prev = hist[d]
            prev[(t - last_seen[d]) > reset_gap] = np.nan
            if kind == "confirm":
                # not yet c observed days in a row: no confirmed warning, lowest priority
                p = np.where(np.isnan(prev).any(axis=1), -np.inf, np.minimum(x, np.nanmin(np.where(
                    np.isnan(prev), np.inf, prev), axis=1)))
            else:
                p = np.maximum(x, np.max(np.where(np.isnan(prev), -np.inf, prev), axis=1))
            hist[d] = np.concatenate([prev[:, 1:], x[:, None]], axis=1)
            last_seen[d], out[rows] = t, p
        return out
    raise ValueError(f"Unknown persistence rule kind: {kind}")
