"""RQ3 prediction quality per horizon, measured on drive-days (protocol section 14).

Three views of each score:
  own rows        each horizon's score against its own label, on every row where that label is known
  common rows     the same, but only on rows where the 7-, 14- and 30-day labels are ALL known
  actionable      against "the drive fails 8 to 31 days after the decision day" (= an inspection the
                  next day would be 7 to 30 days before the failure). Rows 1 to 7 days before the
                  failure are left out; negatives are rows with a known healthy 30-day label.

Unit: drive-day rows of the replay input, unweighted. Scores are judged as rankings only.
"""
from __future__ import annotations

import math

import numpy as np

from .replay_input import NO_FAIL, ReplayData


def _ap_auc(y: np.ndarray, s: np.ndarray) -> dict:
    from sklearn.metrics import average_precision_score, roc_auc_score
    n, pos = int(len(y)), int(y.sum())
    out = {"rows": n, "positives": pos, "prevalence": pos / n if n else math.nan,
           "average_precision": math.nan, "roc_auc": math.nan}
    if 0 < pos < n:
        out["average_precision"] = float(average_precision_score(y, s))
        out["roc_auc"] = float(roc_auc_score(y, s))
        out["ap_over_prevalence"] = out["average_precision"] / out["prevalence"]
    return out


def actionable_label(data: ReplayData, lead_min: int, lead_max: int) -> tuple[np.ndarray, np.ndarray]:
    """(label, keep) per row for the actionable view. gap = failure day minus decision day."""
    f = data.fail_day[data.drive].astype(np.int64)
    gap = np.where(f == NO_FAIL, -1, f - data.day)
    pos = (gap >= lead_min + 1) & (gap <= lead_max + 1)
    too_late = (gap >= 1) & (gap <= lead_min)
    healthy = data.labels[30] == 0
    keep = (pos | healthy) & ~too_late
    return pos.astype(np.int8), keep


def horizon_metrics(data: ReplayData, lead_min: int, lead_max: int) -> list[dict]:
    """One row per (score, view). Scores that are not in the input are skipped."""
    rows = []
    horizons = [h for h in (7, 14, 30) if h in data.labels and f"ml_h{h}" in data.scores]
    common = np.ones(len(data.day), dtype=bool)
    for h in horizons:
        common &= data.labels[h] >= 0
    act_y, act_keep = actionable_label(data, lead_min, lead_max) if 30 in data.labels else (None, None)
    names = [f"ml_h{h}" for h in horizons] + [n for n in ("gbm_h30", "smart_count", "smart_sum") if n in data.scores]
    for name in names:
        s = np.nan_to_num(data.scores[name], nan=-1.0)
        own_h = int(name.split("_h")[1]) if "_h" in name else 30
        if own_h in data.labels:
            known = data.labels[own_h] >= 0
            rows.append({"score": name, "view": f"own_label_{own_h}d", **_ap_auc(data.labels[own_h][known], s[known])})
            if len(horizons) > 1:
                rows.append({"score": name, "view": f"common_rows_label_{own_h}d",
                             **_ap_auc(data.labels[own_h][common], s[common])})
        if act_y is not None:
            rows.append({"score": name, "view": f"actionable_{lead_min + 1}_to_{lead_max + 1}d",
                         **_ap_auc(act_y[act_keep], s[act_keep])})
    return rows
