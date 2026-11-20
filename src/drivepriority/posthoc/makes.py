"""Manufacturer of a drive, a within-make percentile score and a make-quota ranking (amendment A1.2).

The quota ranker has the same interface as drivepriority.simulation.ranking.ScoreRanker (top, sort_keys,
tie_drive), so the frozen engine uses it unchanged. Like ScoreRanker it only ever looks at the rows of one day.
"""
from __future__ import annotations

import numpy as np

from drivepriority.simulation.hashing import tie_keys_day, tie_keys_drive
from drivepriority.simulation.replay_input import ReplayData

MAKES = ("Seagate", "HGST", "Toshiba", "WDC", "Other")


def make_of_model(model: str) -> int:
    """Index into MAKES. Same rule as Table XI (ST, HGST, TOSHIBA, WD or WUH), plus the long names
    'Seagate ...' and 'Hitachi ...' that the Phase 3 manufacturer column also maps."""
    m = (model or "").strip().upper()
    if m.startswith("ST") or m.startswith("SEAGATE"):
        return 0
    if m.startswith("HGST") or m.startswith("HITACHI"):
        return 1
    if m.startswith("TOSHIBA"):
        return 2
    if m.startswith("WD") or m.startswith("WUH"):
        return 3
    return 4


def make_of_drives(drive_ids: np.ndarray) -> np.ndarray:
    """int8 make index per drive; the drive id is 'serial|model'."""
    cache: dict[str, int] = {}
    out = np.empty(len(drive_ids), dtype=np.int8)
    for i, d in enumerate(drive_ids):
        model = str(d).split("|", 1)[1] if "|" in str(d) else ""
        if model not in cache:
            cache[model] = make_of_model(model)
        out[i] = cache[model]
    return out


def within_make_percentile(data: ReplayData, count: np.ndarray, make_of_drive: np.ndarray) -> np.ndarray:
    """Per row: share of the same make's drives observed that day whose count is strictly lower (0..1).
    `count` holds small whole numbers (the SMART count, 0..5). Only same-day rows enter a row's value."""
    c = np.asarray(count)
    if np.any(np.isnan(c)) or np.any(c < 0) or np.any(c != np.round(c)):
        raise ValueError("the count must be a whole number >= 0 on every row")
    n_c = int(c.max()) + 1 if len(c) else 1
    n_m = len(MAKES)
    size = data.n_days * n_m * n_c
    step = 10_000_000                                  # rows per chunk, to keep temporary arrays small
    hist = np.zeros(size, dtype=np.int64)
    for s in range(0, len(c), step):
        hist += np.bincount(_cell(data, c, make_of_drive, s, s + step, n_m, n_c), minlength=size)
    hist = hist.reshape(data.n_days, n_m, n_c)
    with np.errstate(invalid="ignore", divide="ignore"):
        table = ((np.cumsum(hist, axis=2) - hist) / hist.sum(axis=2, keepdims=True)).reshape(-1)
    out = np.empty(len(c), dtype=np.float64)
    for s in range(0, len(c), step):
        out[s:s + step] = table[_cell(data, c, make_of_drive, s, s + step, n_m, n_c)]
    return out


def _cell(data: ReplayData, c: np.ndarray, make_of_drive: np.ndarray, a: int, b: int, n_m: int, n_c: int) -> np.ndarray:
    """Flat index (day, make, count) of rows a..b."""
    day = (data.day[a:b] - data.first_day).astype(np.int64)
    mk = make_of_drive[data.drive[a:b]].astype(np.int64)
    return (day * n_m + mk) * n_c + c[a:b].astype(np.int64)


class QuotaRanker:
    """Make quotas by the Webster (Sainte-Lague) order. Inside each make: score, then tie key; a drive at
    place r of a make with n drives observed today gets the key (r + 0.5) / n; the day is ordered by that key,
    then by the tie key. Any first K places are the Webster (Sainte-Lague) allocation of K places to the makes,
    which is approximately proportional to each make's share of today's drives. It is NOT always within one place
    of the exact share (e.g. 73/9/9/9 drives, K = 7 gives 4/1/1/1 against 5.11 for the largest make); the bound is
    |a - K*s| <= 1/2 + s*n/2 for a make with share s among n makes with drives. (Docstring corrected on
    20 Nov 2026 after the A1 runs; the code is unchanged.)"""

    def __init__(self, data: ReplayData, values: np.ndarray, make_of_drive: np.ndarray, seed: int,
                 tie_mode: str = "drive"):
        if tie_mode not in ("drive", "day"):
            raise ValueError("tie_mode must be 'drive' or 'day'")
        if len(values) != len(data.day):
            raise ValueError("priority array must have one value per row")
        if len(make_of_drive) != data.n_drives:
            raise ValueError("one make per drive is needed")
        self.data = data
        self.values = np.where(np.isnan(values), -np.inf, values).astype(np.float64)
        self.make = np.asarray(make_of_drive, dtype=np.int64)
        self.n_makes = int(self.make.max()) + 1 if len(self.make) else 1
        self.tie_drive = tie_keys_drive(data.drive_key, seed)
        self.tie_mode = tie_mode
        self._i = None

    def _tie(self, drives: np.ndarray, day: int) -> np.ndarray:
        u = self.tie_drive[drives]
        return u if self.tie_mode == "drive" else tie_keys_day(u, day)

    def _day(self, i: int) -> None:
        if self._i == i:
            return
        rows = self.data.rows_of_day(i)
        d = self.data.drive[rows]
        v = self.values[rows]
        m = self.make[d]
        tk = self._tie(d, self.data.first_day + i)
        n = len(d)
        order = np.lexsort((tk, -v, m))                      # make, then score (high first), then tie key
        ms = m[order]
        counts = np.bincount(ms, minlength=self.n_makes)
        starts = np.concatenate([[0], np.cumsum(counts)[:-1]])
        key = np.empty(n, dtype=np.float64)
        key[order] = (np.arange(n) - starts[ms] + 0.5) / counts[ms]
        self._i, self._start, self._key, self._tk = i, rows.start, key, tk
        self._order = d[np.lexsort((tk, key))]

    def top(self, i: int, m: int) -> tuple[np.ndarray, bool]:
        self._day(i)
        n = len(self._order)
        return (self._order, True) if m >= n else (self._order[:m], False)

    def sort_keys(self, rows: np.ndarray, day: int) -> tuple[np.ndarray, np.ndarray]:
        self._day(int(day) - self.data.first_day)
        idx = np.asarray(rows) - self._start
        if len(idx) and (idx.min() < 0 or idx.max() >= len(self._key)):
            raise ValueError("sort_keys was given rows of another day")
        return self._key[idx], self._tk[idx]
