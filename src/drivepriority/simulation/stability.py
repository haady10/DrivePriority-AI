"""Watchlist turnover for RQ2 (protocol section 13).

The watchlist of day t is the top `mult x K_t` drives of the observed fleet by priority, BEFORE any
reinspection rule. It is a different object from the queue and from the service log. Every rule fills
the same number of places, so list size (occupancy) is matched by construction.
"""
from __future__ import annotations

import numpy as np

from .engine import capacity


class WatchlistTracker:
    """Follows one watchlist day by day and adds up its turnover."""

    def __init__(self, budget: int, mult: int = 1):
        self.budget, self.mult = budget, mult
        self.prev: np.ndarray | None = None
        self.days = 0                 # days that have a previous list to compare with
        self.sum_jaccard = 0.0
        self.entries = self.exits = self.exits_not_observed = 0
        self.member_days = 0
        self.first_list_size = 0
        self.list_days = 0

    def depth_needed(self, n: int) -> int:
        return self.mult * capacity(self.budget, n)

    def observe(self, i: int, t: int, n: int, ranked: np.ndarray, complete: bool, seen: np.ndarray) -> bool:
        size = self.mult * capacity(self.budget, n)
        if len(ranked) < size and not complete:
            return False
        today = np.sort(ranked[:size])
        self.list_days += 1
        self.member_days += len(today)
        if self.prev is None:
            self.first_list_size = len(today)
        else:
            both = np.intersect1d(today, self.prev, assume_unique=True)
            union = len(today) + len(self.prev) - len(both)
            self.sum_jaccard += (len(both) / union) if union else 1.0
            left = np.setdiff1d(self.prev, today, assume_unique=True)
            self.entries += len(today) - len(both)
            self.exits += len(left)
            self.exits_not_observed += int((seen[left] != t).sum())
            self.days += 1
        self.prev = today
        return True

    def summary(self) -> dict:
        """Turnover figures. Spells = first list + all later entries; mean stay = member-days / spells."""
        spells = self.first_list_size + self.entries
        mean_size = self.member_days / self.list_days if self.list_days else float("nan")
        return {
            "watchlist_mult": self.mult,
            "list_days": self.list_days,
            "mean_list_size": mean_size,
            "mean_jaccard": self.sum_jaccard / self.days if self.days else float("nan"),
            "entries_per_day": self.entries / self.days if self.days else float("nan"),
            "turnover": (self.entries / self.days / mean_size) if self.days and mean_size else float("nan"),
            "exits": self.exits,
            "exits_drive_not_observed": self.exits_not_observed,
            "exits_outranked": self.exits - self.exits_not_observed,
            "mean_days_on_list": self.member_days / spells if spells else float("nan"),
        }
