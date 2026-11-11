"""Failure events and what the simulated inspections achieved (protocol sections 10 and 11).

This is the only place where failure dates meet the service log, and it runs AFTER the replay.

    event      a drive's first recorded failure (one per drive at most)
    lead       failure day minus service day, in calendar days
    timely     at least one COMPLETED inspection with lead_min <= lead <= lead_max (7..30)

The eligible-event set depends only on the calendar and on which days a drive was observed. It never
depends on a score or a policy, so every cell is judged on exactly the same events.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .engine import PolicyResult
from .replay_input import NO_FAIL, ReplayData

REASONS = ("eligible", "burn_in", "no_opportunity")


@dataclass
class EventTable:
    drive: np.ndarray            # drive index of every failure inside the replay window
    fail_day: np.ndarray
    opportunity_days: np.ndarray  # service days in the timely window on which an inspection could complete
    reason: np.ndarray           # index into REASONS
    lead_min: int
    lead_max: int
    full_opportunity: int

    @property
    def eligible(self) -> np.ndarray:
        return self.reason == 0

    @property
    def n_eligible(self) -> int:
        return int(self.eligible.sum())

    def counts(self) -> dict:
        out = {r: int((self.reason == k).sum()) for k, r in enumerate(REASONS)}
        out["failures_in_window"] = len(self.drive)
        out["eligible_with_full_opportunity"] = int((self.eligible & (self.opportunity_days >= self.full_opportunity)).sum())
        return out


def build_events(data: ReplayData, lead_min: int, lead_max: int, full_opportunity: int = 24) -> EventTable:
    """List the failure events of the replay window and decide which are eligible.

    Eligible = (1) the whole window fail-lead_max .. fail-lead_min lies on service days of the replay,
    i.e. fail_day >= first_day + 1 + lead_max, and (2) at least one opportunity day: a service day s in
    that window with the drive observed on s - 1 (so it could be chosen) and on s (so it could be done).
    """
    f = data.fail_day
    ev = np.flatnonzero((f != NO_FAIL) & (f >= data.first_day) & (f <= data.last_day)).astype(np.int32)
    fail = f[ev].astype(np.int64)
    is_event = np.zeros(data.n_drives, dtype=bool)
    is_event[ev] = True
    rows = np.flatnonzero(is_event[data.drive])
    d, day = data.drive[rows], data.day[rows].astype(np.int64)
    order = np.lexsort((day, d))
    d, day = d[order], day[order]
    pair = (d[1:] == d[:-1]) & (day[1:] == day[:-1] + 1)       # observed on s - 1 and on s
    s, sd = day[1:][pair], d[1:][pair]
    lead = f[sd] - s
    good = (lead >= lead_min) & (lead <= lead_max)
    opp = np.bincount(sd[good], minlength=data.n_drives)[ev]
    reason = np.where(fail < data.first_day + 1 + lead_max, 1, np.where(opp == 0, 2, 0)).astype(np.int8)
    return EventTable(drive=ev, fail_day=fail, opportunity_days=opp, reason=reason,
                      lead_min=lead_min, lead_max=lead_max, full_opportunity=full_opportunity)


def event_outcomes(events: EventTable, res: PolicyResult, n_drives: int, lead_max_alt: int, lead_long: int) -> dict:
    """Per event (same order as events.drive): what the completed inspections of one replay achieved.
    An event counts once, however many inspections its drive received."""
    fail_of = np.full(n_drives, NO_FAIL, dtype=np.int64)
    fail_of[events.drive] = events.fail_day
    done = res.completed
    drv = res.drive[done]
    f = fail_of[drv]
    has = f != NO_FAIL
    drv, lead = drv[has], f[has] - res.service_day[done][has]
    lo, hi = events.lead_min, events.lead_max

    def any_in(mask):
        hit = np.zeros(n_drives, dtype=bool)
        hit[drv[mask]] = True
        return hit[events.drive]

    timely_mask = (lead >= lo) & (lead <= hi)
    earliest = np.full(n_drives, -1, dtype=np.int64)
    np.maximum.at(earliest, drv[timely_mask], lead[timely_mask])
    timely = any_in(timely_mask)
    any_prior = any_in(lead >= 1)
    late = any_in((lead >= 1) & (lead < lo))
    return {
        "timely": timely,
        "timely_alt_window": any_in((lead >= lo) & (lead <= lead_max_alt)),
        "timely_long": any_in((lead >= lead_long) & (lead <= hi)),
        "any_prior": any_prior,
        "late_only": late & ~timely,
        "never_before_failure": ~any_prior,
        "earliest_timely_lead": earliest[events.drive],
    }


def workload(res: PolicyResult) -> dict:
    """Slot accounting of one replay. Units: inspections (not events, not drive-days)."""
    d = res.daily
    done = res.completed
    completed = int(done.sum())
    distinct = int(len(np.unique(res.drive[done])))
    lab = res.label30[done]
    slots = int(d["capacity"].sum())
    out = {
        "decision_days": int((d["observed"] > 0).sum()),
        "mean_observed_drives": float(d["observed"][d["observed"] > 0].mean()) if (d["observed"] > 0).any() else 0.0,
        "mean_capacity": float(d["capacity"][d["observed"] > 0].mean()) if (d["observed"] > 0).any() else 0.0,
        "slots": slots,
        "scheduled": int(d["scheduled"].sum()),
        "completed": completed,
        "cancelled": int(d["cancelled"].sum()),
        "unused_slots": slots - int(d["scheduled"].sum()),
        "distinct_drives_inspected": distinct,
        "repeat_share": (1 - distinct / completed) if completed else float("nan"),
        "inspections_failure_within_30d": int((lab == 1).sum()),
        "inspections_no_failure_30d": int((lab == 0).sum()),
        "inspections_outcome_unknown": int((lab == -1).sum()),
    }
    known = out["inspections_failure_within_30d"] + out["inspections_no_failure_30d"]
    out["inspection_precision_30d"] = out["inspections_failure_within_30d"] / known if known else float("nan")
    if res.spec.queue is not None:
        w = res.wait_days
        led = res.queue_ledger
        out.update({
            "queue_mean_wait_days": float(w.mean()) if len(w) else float("nan"),
            "queue_max_wait_days": int(w.max()) if len(w) else 0,
            "queue_share_waited": float((w > 0).mean()) if len(w) else float("nan"),
            "queue_expired": int((led["exit_reason"] == 1).sum()),
            "queue_mean_length": float(d["queue_length"][d["observed"] > 0].mean()),
            "queue_max_length": int(d["queue_length"].max()),
        })
    return out
