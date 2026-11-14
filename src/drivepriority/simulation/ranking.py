"""Daily priority order: highest score first, ties by the frozen tie key (protocol sections 5 and 6).

A ranker only ever looks at the rows of ONE day, so a ranking can never depend on later days.
Only the top of the list is needed each day, so `top()` sorts just the leading part (always whole
tie groups) and says whether the list it returns is the complete day.
"""
from __future__ import annotations

import numpy as np

from .hashing import tie_keys_day, tie_keys_drive
from .replay_input import ReplayData


class ScoreRanker:
    """Ranks the drives observed on a day by one score column (or any per-row priority array)."""

    def __init__(self, data: ReplayData, values: np.ndarray, seed: int, tie_mode: str = "drive"):
        if tie_mode not in ("drive", "day"):
            raise ValueError("tie_mode must be 'drive' or 'day'")
        if len(values) != len(data.day):
            raise ValueError("priority array must have one value per row")
        self.data = data
        self.values = np.where(np.isnan(values), -np.inf, values).astype(np.float64)   # missing = last
        self.tie_drive = tie_keys_drive(data.drive_key, seed)
        self.tie_mode = tie_mode

    def _tie(self, drives: np.ndarray, day: int) -> np.ndarray:
        u = self.tie_drive[drives]
        return u if self.tie_mode == "drive" else tie_keys_day(u, day)

    def sort_keys(self, rows: np.ndarray, day: int) -> tuple[np.ndarray, np.ndarray]:
        """(primary, secondary) keys of the given rows; ascending order of both = priority order."""
        return -self.values[rows], self._tie(self.data.drive[rows], day)

    def top(self, i: int, m: int) -> tuple[np.ndarray, bool]:
        """Drive indices of day i in priority order. Holds at least the first m places (more when the
        m-th place sits inside a tie group). The flag is True when the whole day is returned."""
        rows = self.data.rows_of_day(i)
        x = self.values[rows]
        n = len(x)
        if n == 0:
            return np.empty(0, dtype=np.int32), True
        if m >= n:
            sel = np.arange(n)
        else:
            cut = np.partition(x, n - m)[n - m]            # the m-th highest score
            sel = np.flatnonzero(x >= cut)
        d = self.data.drive[rows][sel]
        order = np.lexsort((self._tie(d, self.data.first_day + i), -x[sel]))
        return d[order], len(sel) == n


class InterleaveRanker:
    """Urgency-first (protocol section 14): rank 1 of each list, then rank 2 of each, and so on, in the
    order the rankers are given (7-day, 14-day, 30-day). A drive keeps its first (best) place."""

    def __init__(self, rankers: list[ScoreRanker]):
        self.rankers = rankers
        self.data = rankers[0].data

    def sort_keys(self, rows, day):
        raise NotImplementedError("the interleaved ranking is used with fresh Top-K only")

    def top(self, i: int, m: int) -> tuple[np.ndarray, bool]:
        lists, complete = [], True
        for r in self.rankers:
            lst, done = r.top(i, m)
            if not done:
                lst = lst[:m]          # beyond m the partial lists stop at different depths
            lists.append(lst)
            complete &= done
        k = len(lists)
        drives = np.concatenate(lists)
        place = np.concatenate([np.arange(len(lst), dtype=np.int64) * k + j for j, lst in enumerate(lists)])
        order = np.argsort(place, kind="stable")
        drives = drives[order]
        _, first = np.unique(drives, return_index=True)
        return drives[np.sort(first)], complete
