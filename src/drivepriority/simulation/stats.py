"""Differences, interactions and their uncertainty (protocol sections 12 and 15).

Input everywhere: a matrix P with one row per ELIGIBLE EVENT and one column per cell. P[e, c] is the
share of tie seeds in which event e was captured in time in cell c (0..1). The coverage of a cell is
the column mean. One event belongs to one drive, so resampling rows is a drive-cluster bootstrap, and
because every resample keeps a row's values for all cells together, differences between cells are paired.

The intervals are conditional on the replay that happened: they do not cover retraining the models or
a different fleet.
"""
from __future__ import annotations

import numpy as np


def bootstrap_means(P: np.ndarray, reps: int, seed: int, chunk: int = 250) -> np.ndarray:
    """Column means of `reps` resamples of the rows of P (with replacement). Shape (reps, cells)."""
    n = P.shape[0]
    if n == 0:
        return np.full((reps, P.shape[1]), np.nan)
    rng = np.random.default_rng(seed)
    out = np.empty((reps, P.shape[1]))
    p = np.full(n, 1.0 / n)
    for start in range(0, reps, chunk):
        w = rng.multinomial(n, p, size=min(chunk, reps - start)).astype(np.float64)   # times each row is drawn
        out[start:start + len(w)] = (w @ P) / n
    return out


def interval(samples: np.ndarray, level: float = 0.95) -> tuple[float, float]:
    """Percentile interval of bootstrap samples."""
    lo = (1 - level) / 2
    return float(np.quantile(samples, lo)), float(np.quantile(samples, 1 - lo))


def rq1_contrasts(cells: dict) -> dict:
    """The predeclared RQ1 contrasts as weights on the eight cells.

    `cells` maps (score, reinspection, scheduling) -> column index, with score in {"smart", "ml"},
    reinspection in {"next", "cool"} and scheduling in {"fresh", "queue"}.
    Delta(r, g) = C(ml, r, g) - C(smart, r, g).
    """
    def delta(r, g):
        return {cells[("ml", r, g)]: 1.0, cells[("smart", r, g)]: -1.0}

    def minus(a, b):
        out = dict(a)
        for k, v in b.items():
            out[k] = out.get(k, 0.0) - v
        return out

    con = {}
    for r in ("next", "cool"):
        for g in ("fresh", "queue"):
            con[f"delta_ml_minus_smart|{r}|{g}"] = delta(r, g)
    for g in ("fresh", "queue"):
        con[f"interaction_reinspection|{g}"] = minus(delta("cool", g), delta("next", g))
    for r in ("next", "cool"):
        con[f"interaction_scheduling|{r}"] = minus(delta(r, "queue"), delta(r, "fresh"))
    con["interaction_three_way"] = minus(con["interaction_reinspection|queue"], con["interaction_reinspection|fresh"])
    for s in ("smart", "ml"):
        for g in ("fresh", "queue"):
            con[f"effect_cooldown|{s}|{g}"] = {cells[(s, "cool", g)]: 1.0, cells[(s, "next", g)]: -1.0}
        for r in ("next", "cool"):
            con[f"effect_queue|{s}|{r}"] = {cells[(s, r, "queue")]: 1.0, cells[(s, r, "fresh")]: -1.0}
    return con


def evaluate_contrasts(P: np.ndarray, contrasts: dict, reps: int, seed: int, level: float = 0.95) -> list[dict]:
    """Point estimate and paired bootstrap interval of every contrast (in coverage units, 0..1)."""
    point = P.mean(axis=0) if len(P) else np.full(P.shape[1], np.nan)
    boot = bootstrap_means(P, reps, seed)
    rows = []
    for name, weights in contrasts.items():
        w = np.zeros(P.shape[1])
        for col, val in weights.items():
            w[col] += val
        samples = boot @ w
        lo, hi = interval(samples, level) if len(P) else (np.nan, np.nan)
        rows.append({"contrast": name, "estimate": float(point @ w), "ci_low": lo, "ci_high": hi,
                     "interval_excludes_zero": bool(lo > 0 or hi < 0) if len(P) else False,
                     "share_of_resamples_above_zero": float((samples > 0).mean()) if len(P) else np.nan,
                     "events": int(P.shape[0])})
    return rows


def coverage_intervals(P: np.ndarray, reps: int, seed: int, level: float = 0.95) -> np.ndarray:
    """(cells, 2) array: bootstrap interval of each cell's coverage."""
    boot = bootstrap_means(P, reps, seed)
    lo = (1 - level) / 2
    return np.stack([np.quantile(boot, lo, axis=0), np.quantile(boot, 1 - lo, axis=0)], axis=1)
