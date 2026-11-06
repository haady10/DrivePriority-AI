"""Evaluation helpers for Phase 4 (pure numpy/scikit-learn, no H2O needed).

All functions take the labelled rows of ONE split (label known, never NULL) and a score
(higher = riskier). Ranking metrics ignore the sampling weight because validation and test
rows all carry the same weight (an outcome-independent hash sample of the whole fleet).
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score

TOP_FRACTIONS = (0.001, 0.005, 0.01, 0.05)


def _top_k_mask(score: np.ndarray, k: int) -> np.ndarray:
    """Boolean mask of the k highest scores. Ties are broken by row position (deterministic)."""
    order = np.argsort(-score, kind="stable")
    mask = np.zeros(len(score), dtype=bool)
    mask[order[:k]] = True
    return mask


def recall_precision_at_k(y: np.ndarray, score: np.ndarray, k: int) -> tuple[float, float]:
    """(recall, precision) when the k highest-scoring rows are flagged."""
    y = np.asarray(y)
    k = int(min(max(k, 1), len(y)))
    hit = int(y[_top_k_mask(np.asarray(score, dtype=float), k)].sum())
    pos = int(y.sum())
    return (hit / pos if pos else math.nan), hit / k


def evaluate(y, score, *, is_probability: bool, rule_flag_count: int | None = None) -> dict:
    """Metrics of one model on one split.

    rule_flag_count: number of rows the traditional SMART rule flags on this split. When given,
    the model is also judged at the SAME alert volume (top rule_flag_count rows), which is the
    fair comparison for RQ1.
    """
    y = np.asarray(y, dtype=np.int8)
    s = np.asarray(score, dtype=float)
    n = int(len(y))
    pos = int(y.sum())
    out: dict = {"rows": n, "positives": pos, "prevalence": pos / n if n else math.nan}
    defined = 0 < pos < n
    out["average_precision"] = float(average_precision_score(y, s)) if defined else math.nan
    out["roc_auc"] = float(roc_auc_score(y, s)) if defined else math.nan
    if is_probability and defined:
        p = np.clip(s, 1e-9, 1 - 1e-9)
        out["brier"] = float(brier_score_loss(y, p))
        out["log_loss"] = float(log_loss(y, p, labels=[0, 1]))
    else:
        out["brier"] = math.nan
        out["log_loss"] = math.nan
    for frac in TOP_FRACTIONS:
        k = max(1, math.ceil(frac * n)) if n else 1
        rec, prec = recall_precision_at_k(y, s, k) if n else (math.nan, math.nan)
        out[f"recall_top_{frac * 100:g}pct"] = rec
        out[f"precision_top_{frac * 100:g}pct"] = prec
    if rule_flag_count is not None and n:
        rec, prec = recall_precision_at_k(y, s, max(int(rule_flag_count), 1))
        out["recall_at_smart_volume"] = rec
        out["precision_at_smart_volume"] = prec
        out["alert_volume"] = int(rule_flag_count)
    return out


def rule_metrics(y, flag) -> dict:
    """Precision / recall of the binary SMART rule itself (the traditional baseline)."""
    y = np.asarray(y, dtype=np.int8)
    f = np.asarray(flag, dtype=np.int8) > 0
    pos = int(y.sum())
    flagged = int(f.sum())
    hit = int(y[f].sum())
    return {"flagged_rows": flagged, "flagged_fraction": flagged / len(y) if len(y) else math.nan,
            "recall": hit / pos if pos else math.nan, "precision": hit / flagged if flagged else math.nan}


def calibration_table(y, p, bins: int = 10) -> pd.DataFrame:
    """Mean predicted probability vs observed failure rate in score quantile bins."""
    df = pd.DataFrame({"y": np.asarray(y, dtype=np.int8), "p": np.asarray(p, dtype=float)})
    if df.empty or df["p"].nunique() < 2:
        return pd.DataFrame(columns=["bin", "rows", "mean_predicted", "observed_rate"])
    df["bin"] = pd.qcut(df["p"].rank(method="first"), q=min(bins, len(df)), labels=False)
    g = df.groupby("bin", observed=True)
    return pd.DataFrame({"rows": g.size(), "mean_predicted": g["p"].mean(),
                         "observed_rate": g["y"].mean()}).reset_index()
