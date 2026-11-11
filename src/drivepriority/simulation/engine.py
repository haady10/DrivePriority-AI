"""The daily state machine of the inspection replay (protocol sections 3, 4, 7 and 8).

For every calendar day t, in this order:
  1. RESOLVE the inspections scheduled yesterday for today: completed if the drive has a row today,
     otherwise cancelled (the slot is lost and no cooldown starts).
  2. DECIDE at the end of day t, using only rows with date <= t: compute K_t, apply the policy's
     reinspection and scheduling rules to today's ranking and schedule at most K_t drives for t + 1.

The engine is never given failure dates or labels as an input to decisions. The only outcome column
it touches is the 30-day label of a COMPLETED inspection, which is copied into the service log after
the decision, for the workload tables.

Several policies (and watchlist trackers) can share one ranking, so one pass over the days serves
all cells that use the same score and tie seed.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

from .config import QueueConfig
from .replay_input import ReplayData

NEVER = -10**6
LABEL_NOT_COMPLETED = -2


@dataclass(frozen=True)
class PolicySpec:
    """One inspection policy. cooldown_days = 0 means next-day reinspection; queue = None means fresh Top-K."""
    name: str
    budget: int                       # inspections per 10,000 observed drives per day
    cooldown_days: int = 0
    queue: QueueConfig | None = None


@dataclass
class PolicyResult:
    """Everything a policy did. Service log: one row per scheduled inspection."""
    spec: PolicySpec
    decision_day: np.ndarray = None
    service_day: np.ndarray = None
    drive: np.ndarray = None
    completed: np.ndarray = None      # bool
    label30: np.ndarray = None        # 1 / 0 / -1 unknown / -2 not completed
    wait_days: np.ndarray = None      # queue only: schedule day minus admission day (0 for fresh Top-K)
    daily: dict = field(default_factory=dict)          # arrays per calendar day
    queue_ledger: dict = field(default_factory=dict)   # drive, admit_day, exit_day, exit_reason (0 scheduled, 1 expired, 2 left at the end)
    checks: dict = field(default_factory=dict)


def capacity(budget: int, n_observed: int) -> int:
    """K_t = floor(B * N_t / 10,000), before any eligibility rule (protocol section 4)."""
    return (budget * n_observed) // 10000


class _PolicyState:
    def __init__(self, spec: PolicySpec, data: ReplayData):
        self.spec = spec
        n_days, n_drives = data.n_days, data.n_drives
        self.last_completed = np.full(n_drives, NEVER, dtype=np.int64)
        self.pending = np.empty(0, dtype=np.int32)
        self.pending_wait = np.empty(0, dtype=np.int32)
        self.queue = np.empty(0, dtype=np.int32)
        self.admit_day = np.full(n_drives, -1, dtype=np.int64) if spec.queue else None
        self.log = {k: [] for k in ("decision_day", "service_day", "drive", "completed", "label30", "wait_days")}
        self.ledger = {k: [] for k in ("drive", "admit_day", "exit_day", "exit_reason")}
        z = lambda: np.zeros(n_days, dtype=np.int64)
        self.daily = {"observed": z(), "capacity": z(), "scheduled": z(), "completed": z(), "cancelled": z(),
                      "queue_length": z(), "admitted": z(), "expired": z()}
        self.problems: dict[str, int] = {"scheduled_above_capacity": 0, "drive_scheduled_twice_same_day": 0,
                                         "cooldown_violations": 0, "queue_duplicate_entries": 0,
                                         "queue_above_cap_after_admission": 0, "queue_expired_entry_kept": 0,
                                         "scheduled_drive_not_observed": 0}

    # ---- step 1: yesterday's schedule meets today's data
    def resolve(self, i: int, t: int, seen: np.ndarray, row_of: np.ndarray, label30: np.ndarray | None):
        p = self.pending
        if len(p) == 0:
            return
        done = seen[p] == t
        cd = self.spec.cooldown_days
        if cd and np.any((t - self.last_completed[p[done]]) < cd):
            self.problems["cooldown_violations"] += 1
        self.last_completed[p[done]] = t
        lab = np.full(len(p), LABEL_NOT_COMPLETED, dtype=np.int8)
        if label30 is not None and done.any():
            lab[done] = label30[row_of[p[done]]]
        elif done.any():
            lab[done] = -1
        self.log["decision_day"].append(np.full(len(p), t - 1, dtype=np.int32))
        self.log["service_day"].append(np.full(len(p), t, dtype=np.int32))
        self.log["drive"].append(p)
        self.log["completed"].append(done)
        self.log["label30"].append(lab)
        self.log["wait_days"].append(self.pending_wait)
        self.daily["completed"][i - 1] = int(done.sum())
        self.daily["cancelled"][i - 1] = int(len(p) - done.sum())
        self.pending = np.empty(0, dtype=np.int32)
        self.pending_wait = np.empty(0, dtype=np.int32)

    def _eligible(self, drives: np.ndarray, t: int) -> np.ndarray:
        cd = self.spec.cooldown_days
        if not cd:
            return np.ones(len(drives), dtype=bool)
        return ((t + 1) - self.last_completed[drives]) >= cd

    # ---- step 2: today's decision. Returns False (and changes nothing) when `ranked` is too short.
    def decide(self, i: int, t: int, n: int, ranked: np.ndarray, complete: bool, ranker, seen, row_of) -> bool:
        spec = self.spec
        K = capacity(spec.budget, n)
        if spec.queue is None:
            cand = ranked[self._eligible(ranked, t)] if spec.cooldown_days else ranked
            if len(cand) < K and not complete:
                return False
            picks, waits = cand[:K], np.zeros(min(K, len(cand)), dtype=np.int32)
            admitted = expired = 0
        else:
            q = spec.queue
            qa = self.admit_day[self.queue]
            alive = (t - qa) < q.expire_days
            gone, kept, kept_a = self.queue[~alive], self.queue[alive], qa[alive]
            room = max(0, min(q.admit_mult * K, q.cap_mult * K - len(kept)))
            new = np.empty(0, dtype=np.int32)
            if room:
                in_queue = self.admit_day[ranked] >= 0
                if len(gone):
                    in_queue &= ~np.isin(ranked, gone)
                cand = ranked[~in_queue & self._eligible(ranked, t)]
                if len(cand) < room and not complete:
                    return False
                new = cand[:room]
            members = np.concatenate([kept, new])
            member_a = np.concatenate([kept_a, np.full(len(new), t, dtype=np.int64)])
            ok = (seen[members] == t) & self._eligible(members, t)       # present today and allowed tomorrow
            m_ok, a_ok = members[ok], member_a[ok]
            primary, secondary = ranker.sort_keys(row_of[m_ok], t)
            order = np.lexsort((secondary, primary, a_ok))[:K]            # oldest admission, then today's rank
            picks, waits = m_ok[order], (t - a_ok[order]).astype(np.int32)
            stay = np.ones(len(members), dtype=bool)
            stay[np.flatnonzero(ok)[order]] = False
            # ---- commit
            self.admit_day[gone] = -1
            self.admit_day[new] = t
            self.admit_day[picks] = -1
            for arr, reason, day_a in ((gone, 1, qa[~alive]), (picks, 0, a_ok[order])):
                if len(arr):
                    self.ledger["drive"].append(arr)
                    self.ledger["admit_day"].append(day_a.astype(np.int32))
                    self.ledger["exit_day"].append(np.full(len(arr), t, dtype=np.int32))
                    self.ledger["exit_reason"].append(np.full(len(arr), reason, dtype=np.int8))
            self.queue = members[stay]
            admitted, expired = len(new), len(gone)
            if len(np.unique(members)) != len(members):
                self.problems["queue_duplicate_entries"] += 1
            if len(members) > max(q.cap_mult * K, len(kept)):
                self.problems["queue_above_cap_after_admission"] += 1
            if len(self.queue) and np.any((t - self.admit_day[self.queue]) >= q.expire_days):
                self.problems["queue_expired_entry_kept"] += 1
        if len(picks) > K:
            self.problems["scheduled_above_capacity"] += 1
        if len(np.unique(picks)) != len(picks):
            self.problems["drive_scheduled_twice_same_day"] += 1
        if len(picks) and np.any(seen[picks] != t):
            self.problems["scheduled_drive_not_observed"] += 1
        self.pending, self.pending_wait = picks.astype(np.int32), waits
        d = self.daily
        d["observed"][i], d["capacity"][i], d["scheduled"][i] = n, K, len(picks)
        d["queue_length"][i], d["admitted"][i], d["expired"][i] = len(self.queue), admitted, expired
        return True

    def depth_needed(self, n: int) -> int:
        K = capacity(self.spec.budget, n)
        blocked = K * max(0, self.spec.cooldown_days - 1)
        q = self.spec.queue
        return (K + blocked) if q is None else (K * (q.admit_mult + q.cap_mult) + blocked)

    def finish(self, data: ReplayData) -> PolicyResult:
        cat = lambda xs, dt: np.concatenate(xs).astype(dt) if xs else np.empty(0, dtype=dt)
        if self.spec.queue is not None and len(self.queue):
            self.ledger["drive"].append(self.queue)
            self.ledger["admit_day"].append(self.admit_day[self.queue].astype(np.int32))
            self.ledger["exit_day"].append(np.full(len(self.queue), data.last_day, dtype=np.int32))
            self.ledger["exit_reason"].append(np.full(len(self.queue), 2, dtype=np.int8))
        res = PolicyResult(
            spec=self.spec, decision_day=cat(self.log["decision_day"], np.int32),
            service_day=cat(self.log["service_day"], np.int32), drive=cat(self.log["drive"], np.int32),
            completed=cat(self.log["completed"], bool), label30=cat(self.log["label30"], np.int8),
            wait_days=cat(self.log["wait_days"], np.int32), daily=self.daily,
            queue_ledger={"drive": cat(self.ledger["drive"], np.int32),
                          "admit_day": cat(self.ledger["admit_day"], np.int32),
                          "exit_day": cat(self.ledger["exit_day"], np.int32),
                          "exit_reason": cat(self.ledger["exit_reason"], np.int8)})
        d = self.daily
        self.problems["scheduled_not_equal_completed_plus_cancelled"] = int(
            d["scheduled"].sum() != d["completed"].sum() + d["cancelled"].sum())
        self.problems["service_log_rows_not_equal_scheduled"] = int(len(res.drive) != d["scheduled"].sum())
        res.checks = dict(self.problems)
        return res


def run_replay(data: ReplayData, ranker, policies: list[PolicySpec], trackers: list | None = None,
               stop_after_day: int | None = None) -> list[PolicyResult]:
    """Replay all `policies` (and feed all watchlist `trackers`) over the calendar days of `data`.

    stop_after_day: last decision day to process (testing aid: a replay cut at day T must equal the
    first part of the full replay, which proves that later data is never used).
    """
    states = [_PolicyState(p, data) for p in policies]
    trackers = trackers or []
    label30 = data.labels.get(30)
    seen = np.full(data.n_drives, NEVER, dtype=np.int64)
    row_of = np.zeros(data.n_drives, dtype=np.int64)
    for i in range(data.n_days):
        t = data.first_day + i
        if stop_after_day is not None and t > stop_after_day + 1:
            break
        rows = data.rows_of_day(i)
        d = data.drive[rows]
        n = len(d)
        seen[d] = t
        row_of[d] = np.arange(rows.start, rows.stop)
        for st in states:
            st.resolve(i, t, seen, row_of, label30)
        if n == 0 or i == data.n_days - 1 or (stop_after_day is not None and t > stop_after_day):
            continue                                   # no data today, or tomorrow is outside the replay
        need = max([st.depth_needed(n) for st in states] + [tr.depth_needed(n) for tr in trackers] + [1])
        ranked, complete = ranker.top(i, int(need * 1.2) + 20)
        for st in states:
            if not st.decide(i, t, n, ranked, complete, ranker, seen, row_of):
                ranked, complete = ranker.top(i, n)    # rare: the short list ran out, rank the whole day
                if not st.decide(i, t, n, ranked, complete, ranker, seen, row_of):
                    raise RuntimeError("a decision failed on a complete ranking")
        for tr in trackers:
            if not tr.observe(i, t, n, ranked, complete, seen):
                ranked, complete = ranker.top(i, n)
                tr.observe(i, t, n, ranked, complete, seen)
    return [st.finish(data) for st in states]
