"""Acceptance checks that need their own replays (protocol section 16).

No-future-information check
    Take a day T in the middle of the replay. Make a second copy of the input in which every score and
    every label on days after T is shuffled between rows, and every failure date is removed. Then, for
    every policy family of the run (both score sources, every budget, every queue setting, both tie
    modes, every sensitivity score, every persistence rule, every horizon, urgency-first):

      * replay the original input and the scrambled input up to T and require exactly the same service
        log (decision day, service day, drive, completed) row by row;
      * for the eight primary cells, also require that this log equals the first part of the full replay.

    Scores, persistence priorities and tie keys are computed on whole arrays before the day loop, so a
    step that looked ahead would show up here as a different decision before T.

Event-identity check
    The eligible events must be the same drives with the same failure days in every cell of a fleet,
    and must not depend on any score.
"""
from __future__ import annotations

import hashlib
from dataclasses import replace

import numpy as np

from .engine import PolicyResult, PolicySpec, run_replay
from .events import EventTable, build_events
from .ranking import InterleaveRanker, ScoreRanker
from .replay_input import NO_FAIL, ReplayData, add_derived_scores
from .transforms import persistence_priority

DERIVED = ("smart_count_sum", "random")


def scrambled_after(data: ReplayData, cut_day: int, seed: int = 20261112) -> ReplayData:
    """Copy of `data` with all scores and labels after `cut_day` shuffled between rows and no failure dates.
    Which drive has a row on which day is unchanged (that is the observation calendar, not a score)."""
    rng = np.random.default_rng(seed)
    future = np.flatnonzero(data.day > cut_day)
    scores = {}
    for name, v in data.scores.items():
        if name in DERIVED:
            continue                                  # rebuilt below from the scrambled columns
        w = v.copy()
        w[future] = v[future][rng.permutation(len(future))]
        scores[name] = w
    labels = {}
    for h, v in data.labels.items():
        w = v.copy()
        w[future] = v[future][rng.permutation(len(future))]
        labels[h] = w
    out = replace(data, scores=scores, labels=labels, fail_day=np.full(data.n_drives, NO_FAIL, dtype=np.int32))
    add_derived_scores(out)
    if "random" in data.scores:                       # the reference score is a hash of drive and day: unchanged
        assert np.array_equal(out.scores["random"], data.scores["random"])
    return out


def _same_log(a: PolicyResult, b: PolicyResult) -> bool:
    return (np.array_equal(a.decision_day, b.decision_day) and np.array_equal(a.service_day, b.service_day)
            and np.array_equal(a.drive, b.drive) and np.array_equal(a.completed, b.completed))


def no_future_information(data: ReplayData, families: list[dict], cut_day: int, seed: int,
                          primary_full: dict[str, PolicyResult] | None = None) -> dict:
    """families: [{"name", "make_ranker": f(data, seed) -> ranker, "specs": [PolicySpec]}].
    primary_full: full-replay results (tie seed `seed`) by spec name for the family called "primary_*"."""
    other = scrambled_after(data, cut_day)
    changed = int(sum(int((data.scores[k] != other.scores[k]).sum()) for k in other.scores if k not in DERIVED))
    failed, n_logs, n_rows, prefix_failed = [], 0, 0, []
    for fam in families:
        a = run_replay(data, fam["make_ranker"](data, seed), fam["specs"], stop_after_day=cut_day)
        b = run_replay(other, fam["make_ranker"](other, seed), fam["specs"], stop_after_day=cut_day)
        for spec, x, y in zip(fam["specs"], a, b):
            n_logs += 1
            n_rows += len(x.drive)
            if len(x.drive) == 0 or not _same_log(x, y):
                failed.append(f"{fam['name']}|{spec.name}")
            full = (primary_full or {}).get(f"{fam['name']}|{spec.name}")
            if full is not None:
                m = full.decision_day <= cut_day
                if not (np.array_equal(x.drive, full.drive[m]) and np.array_equal(x.completed, full.completed[m])
                        and np.array_equal(x.decision_day, full.decision_day[m])):
                    prefix_failed.append(f"{fam['name']}|{spec.name}")
    return {"cut_day_index": int(cut_day - data.first_day), "score_values_changed_after_cut": changed,
            "families": len(families), "service_logs_compared": n_logs, "service_log_rows_compared": n_rows,
            "logs_that_changed": failed, "primary_logs_not_equal_to_full_replay_prefix": prefix_failed,
            "primary_logs_checked_against_full_replay": len(primary_full or {}),
            "passed": bool(changed > 0 and n_logs > 0 and not failed and not prefix_failed)}


def event_key(data: ReplayData, events: EventTable) -> str:
    """Identity of the eligible events: SHA-256 of (drive id, failure day) of every eligible event."""
    el = events.eligible
    ids = data.drive_ids[events.drive[el]]
    lines = sorted(f"{i}|{int(f)}" for i, f in zip(ids, events.fail_day[el]))
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def events_do_not_depend_on_scores(data: ReplayData, events: EventTable, lead_min: int, lead_max: int,
                                   full_opportunity: int) -> bool:
    """Rebuild the event table from a copy without any score or label: it must be identical."""
    bare = replace(data, scores={}, labels={})
    again = build_events(bare, lead_min, lead_max, full_opportunity)
    return (np.array_equal(again.drive, events.drive) and np.array_equal(again.fail_day, events.fail_day)
            and np.array_equal(again.reason, events.reason)
            and np.array_equal(again.opportunity_days, events.opportunity_days))


def build_families(proto, data: ReplayData, policies_for) -> list[dict]:
    """Every policy family of a full run (the same lists as in run_phase5)."""
    B, B_sens, cd = proto.budget_primary, proto.budgets_sensitivity, proto.cooldown_days
    q0, q_alt = proto.queue_primary, proto.queues_sensitivity
    alt_mode = proto.raw["tie"]["sensitivity_mode"]
    rq2 = proto.raw["rq2"]
    by_score = lambda name, mode: (lambda d, seed: ScoreRanker(d, d.scores[name], seed, mode))
    fams = []
    for role, source in (("smart", proto.smart), ("ml", proto.ml)):
        specs = policies_for(B, cd, [None, q0])
        for b in B_sens:
            specs += policies_for(b, cd, [None, q0])
        specs += policies_for(B, cd, q_alt)
        fams.append({"name": f"primary_{role}", "make_ranker": by_score(source, proto.tie_mode), "specs": specs})
        fams.append({"name": f"tie_{alt_mode}_{role}", "make_ranker": by_score(source, alt_mode),
                     "specs": policies_for(B, cd, [None, q0])})
        for rule in rq2["rules"]:
            if rule["kind"] == "daily":
                continue
            fams.append({"name": f"rq2_{rule['name']}_{role}", "specs": policies_for(B, cd, [None]),
                         "make_ranker": (lambda d, seed, s=source, r=rule: ScoreRanker(
                             d, persistence_priority(d, d.scores[s], r, rq2["history_reset_gap_days"]), seed,
                             proto.tie_mode))})
    for source in proto.raw["scores"]["sensitivity"]:
        if source in data.scores:
            fams.append({"name": f"score_{source}", "make_ranker": by_score(source, proto.tie_mode),
                         "specs": policies_for(B, cd, [None, q0])})
    rq3 = [s for s in proto.raw["scores"]["rq3"] if s in data.scores]
    for source in rq3:
        if source != proto.ml:
            fams.append({"name": f"rq3_{source}", "make_ranker": by_score(source, proto.tie_mode),
                         "specs": policies_for(B, cd, [None])})
    if len(rq3) == 3:
        fams.append({"name": "rq3_urgency_first", "specs": policies_for(B, cd, [None]),
                     "make_ranker": lambda d, seed: InterleaveRanker(
                         [ScoreRanker(d, d.scores[s], seed, proto.tie_mode) for s in ("ml_h7", "ml_h14", "ml_h30")])})
    return fams
