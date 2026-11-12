"""Phase 5 checks on small fake fleets where the right answer is known in advance.

What is proven here (protocol section 16 and the Stage 3 list):
  * capacity, fresh Top-K, the 7-day cooldown boundary and the carry-forward queue give hand-worked picks
  * the fast engine equals a slow, independent reference simulator on random fleets
  * a perfect score catches every eligible event; a random score matches the chance formula
  * an event is counted once, however often its drive is inspected
  * decisions never use later days, failure dates or labels
  * ties are shared fairly and do not depend on row order
  * events: burn-in, opportunity and the 7..30 day lead boundaries
  * persistence rules, watchlist turnover, bootstrap contrasts, input building, test-split lock
"""
from __future__ import annotations

import json
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from drivepriority.simulation import checks as p5checks
from drivepriority.simulation import config as p5config
from drivepriority.simulation import run_phase5
from drivepriority.simulation.config import QueueConfig, load_protocol
from drivepriority.simulation.engine import PolicyResult, PolicySpec, capacity, run_replay
from drivepriority.simulation.events import build_events, event_outcomes, workload
from drivepriority.simulation.hashing import drive_keys, in_subsample, tie_keys_drive
from drivepriority.simulation.prediction import actionable_label, horizon_metrics
from drivepriority.simulation.ranking import InterleaveRanker, ScoreRanker
from drivepriority.simulation.replay_input import (add_derived_scores, day_number, load_replay_input,
                                                   prepare_from_phase4)
from drivepriority.simulation.stability import WatchlistTracker
from drivepriority.simulation.stats import bootstrap_means, evaluate_contrasts, rq1_contrasts
from drivepriority.simulation.transforms import persistence_priority

D0 = date(2024, 1, 1)
Q = QueueConfig(admit_mult=2, cap_mult=3, expire_days=3)


def make_data(tmp_path, records, last: date, first: date = D0, name="replay.parquet"):
    """records: dicts with drive_id, day (offset from `first`), score and optional fail (offset), smart_count,
    label_30d. Writes a replay table and loads it with the real loader."""
    df = pd.DataFrame(records)
    out = pd.DataFrame({
        "drive_id": df["drive_id"].astype(str),
        "date": [first + timedelta(days=int(d)) for d in df["day"]],
        "fail_date": [None if pd.isna(f) else first + timedelta(days=int(f)) for f in df.get("fail", pd.Series([None] * len(df)))],
        "smart_count": df.get("smart_count", pd.Series(np.zeros(len(df)))).astype("int8"),
        "smart_sum": df.get("smart_sum", df.get("smart_count", pd.Series(np.zeros(len(df))))).astype("float64"),
        "ml_h30": df["score"].astype("float64"),
    })
    if "label_30d" in df:
        out["label_30d"] = df["label_30d"].astype("Int8")
    for extra in ("ml_h7", "ml_h14", "label_7d", "label_14d"):
        if extra in df:
            out[extra] = df[extra].astype("Int8" if extra.startswith("label") else "float64")
    out["date"] = pd.to_datetime(out["date"]).dt.date
    path = tmp_path / name
    out.to_parquet(path, index=False)
    return load_replay_input(path, first, last)


def constant_fleet(tmp_path, scores: dict, days: int, absent: dict | None = None, **kw):
    """Every drive present every day (except `absent`: drive -> set of day offsets) with a fixed score."""
    absent = absent or {}
    recs = [{"drive_id": d, "day": t, "score": s} for d, s in scores.items() for t in range(days)
            if t not in absent.get(d, ())]
    return make_data(tmp_path, recs, D0 + timedelta(days=days - 1), **kw)


def served(res: PolicyResult, data, only_completed=True):
    """[(service day offset, drive id)] in log order."""
    m = res.completed if only_completed else np.ones(len(res.drive), dtype=bool)
    return [(int(s - data.first_day), str(data.drive_ids[d])) for s, d in zip(res.service_day[m], res.drive[m])]


def ranker(data, seed=0, mode="drive", name="ml_h30"):
    return ScoreRanker(data, data.scores[name], seed, mode)


# ------------------------------------------------------------------ capacity and the three policies

def test_capacity_rule_has_no_hidden_minimum():
    assert capacity(20, 15000) == 30          # the methodology example
    assert capacity(20, 499) == 0             # small fleets may get zero slots; never forced to one
    assert capacity(10, 28600) == 28 and capacity(50, 28600) == 143


def test_fresh_topk_next_day_inspects_the_same_top_drives_every_day(tmp_path):
    data = constant_fleet(tmp_path, {"A": 9, "B": 8, "C": 7, "D": 6}, days=5)
    res = run_replay(data, ranker(data), [PolicySpec("p", budget=5000)])[0]      # N = 4, K = 2
    assert served(res, data) == [(s, d) for s in (1, 2, 3, 4) for d in ("A", "B")]
    w = workload(res)
    assert w["slots"] == 8 and w["completed"] == 8 and w["distinct_drives_inspected"] == 2
    assert w["repeat_share"] == pytest.approx(0.75) and w["unused_slots"] == 0
    assert all(v == 0 for v in res.checks.values())


def test_cooldown_blocks_six_days_and_frees_on_the_seventh(tmp_path):
    data = constant_fleet(tmp_path, {"A": 9, "B": 8, "C": 7, "D": 6}, days=12)
    res = run_replay(data, ranker(data), [PolicySpec("p", budget=5000, cooldown_days=7)])[0]
    got = served(res, data)
    # A and B on day 1, C and D on day 2, nothing on days 3..7, A and B again on day 8 (1 + 7), C and D on day 9
    assert got == [(1, "A"), (1, "B"), (2, "C"), (2, "D"), (8, "A"), (8, "B"), (9, "C"), (9, "D")]
    assert workload(res)["unused_slots"] == 22 - 8        # 11 decision days x 2 slots
    assert res.checks["cooldown_violations"] == 0


def test_queue_serves_oldest_admission_first(tmp_path):
    data = constant_fleet(tmp_path, {"A": 9, "B": 8, "C": 7, "D": 6, "E": 5, "F": 4}, days=7)
    res = run_replay(data, ranker(data), [PolicySpec("q", budget=2000, queue=Q)])[0]          # N = 6, K = 1
    # hand-worked in the test plan: day0 admit A,B serve A; day1 admit A,C serve B; day2 admit B serve A; ...
    assert [d for _, d in served(res, data)] == ["A", "B", "A", "C", "B", "A"]
    assert res.wait_days.tolist() == [0, 1, 1, 2, 2, 2]
    assert all(v == 0 for v in res.checks.values())
    w = workload(res)
    assert w["queue_max_length"] <= 3 and w["queue_expired"] == 0 and w["queue_max_wait_days"] == 2


def test_queue_entry_expires_after_three_days_when_its_drive_is_away(tmp_path):
    scores = {"A": 9, "X": 8, "C": 4, "D": 3, "E": 2, "F": 1}
    data = constant_fleet(tmp_path, scores, days=6, absent={"X": {1, 2, 3, 4, 5}})
    res = run_replay(data, ranker(data), [PolicySpec("q", budget=2000, queue=Q)])[0]
    led = res.queue_ledger
    x = int(np.flatnonzero(data.drive_ids == "X")[0])
    hit = np.flatnonzero(led["drive"] == x)
    assert len(hit) == 1 and led["exit_reason"][hit[0]] == 1                 # expired, never scheduled
    assert led["exit_day"][hit[0]] - led["admit_day"][hit[0]] == 3
    assert "X" not in [d for _, d in served(res, data, only_completed=False)]


def test_absent_drive_cancels_the_inspection_and_wastes_the_slot(tmp_path):
    # A is the top drive but has no row on day 1, so the inspection scheduled for day 1 is cancelled
    data = constant_fleet(tmp_path, {"A": 9, "B": 8, "C": 7, "D": 6}, days=4, absent={"A": {1}})
    res = run_replay(data, ranker(data), [PolicySpec("p", budget=2500, cooldown_days=7)])[0]   # K = 1
    assert served(res, data, only_completed=False)[0] == (1, "A") and not res.completed[0]
    assert res.label30[0] == -2
    # the cancelled inspection does not start a cooldown: A is chosen again on day 2 for day 3
    assert (3, "A") in served(res, data)
    assert workload(res)["cancelled"] == 1


def test_day_without_any_data_cancels_and_makes_no_decision(tmp_path):
    recs = [{"drive_id": d, "day": t, "score": s} for d, s in {"A": 9, "B": 8, "C": 7, "D": 6}.items()
            for t in (0, 1, 3, 4)]                                           # day 2 is a source gap
    data = make_data(tmp_path, recs, D0 + timedelta(days=4))
    res = run_replay(data, ranker(data), [PolicySpec("p", budget=2500)])[0]
    assert served(res, data, only_completed=False) == [(1, "A"), (2, "A"), (4, "A")]
    assert res.completed.tolist() == [True, False, True]
    assert res.daily["capacity"].tolist() == [1, 1, 0, 1, 0]                 # no decision on day 2 or on the last day


# ------------------------------------------------------------------ fast engine against a slow reference

def reference_replay(data, tie, budget, cd, queue):
    """Slow, obvious re-implementation of the protocol with dictionaries (independent of engine.py)."""
    by_day: dict = {}
    for t, d, s in zip(data.day, data.drive, data.scores["ml_h30"]):
        by_day.setdefault(int(t), {})[int(d)] = float(s)
    last, pending, q, log = {}, [], {}, []
    for t in range(data.first_day, data.last_day + 1):
        present = by_day.get(t, {})
        for d, wait in pending:
            done = d in present
            if done:
                last[d] = t
            log.append((t, d, done, wait))
        pending = []
        if not present or t == data.last_day:
            continue
        K = budget * len(present) // 10000
        rank = sorted(present, key=lambda d: (-present[d], tie[d]))
        ok = lambda d: (not cd) or (t + 1 - last.get(d, -10**9) >= cd)
        if queue is None:
            pending = [(d, 0) for d in [d for d in rank if ok(d)][:K]]
            continue
        for d in [d for d, a in q.items() if t - a >= queue.expire_days]:
            del q[d]
        cap = queue.cap_mult * K
        if len(q) > cap:                      # step 1b: K fell, keep the first `cap` entries in service order
            order = sorted(q, key=lambda d: (q[d], d not in present, -present.get(d, 0.0), tie[d]))
            for d in order[cap:]:
                del q[d]
        room = max(0, min(queue.admit_mult * K, cap - len(q)))
        for d in rank:
            if room == 0:
                break
            if d not in q and ok(d):
                q[d] = t
                room -= 1
        cands = sorted([d for d in q if d in present and ok(d)], key=lambda d: (q[d], -present[d], tie[d]))
        pending = [(d, t - q[d]) for d in cands[:K]]
        for d, _ in pending:
            del q[d]
    return log


@pytest.mark.parametrize("fleet_seed", [1, 2, 3])
def test_engine_equals_the_slow_reference_on_random_fleets(tmp_path, fleet_seed):
    rng = np.random.default_rng(fleet_seed)
    recs = []
    for d in range(120):
        start, stop = int(rng.integers(0, 10)), int(rng.integers(30, 61))
        for t in range(start, stop):
            if rng.random() < 0.08 or (t == 20 and fleet_seed == 2):         # missing days; one fleet-wide gap
                continue
            recs.append({"drive_id": f"d{d:03d}", "day": t, "score": float(rng.integers(0, 6))})   # many ties
    data = make_data(tmp_path, recs, D0 + timedelta(days=60))
    specs = [PolicySpec("a", 500), PolicySpec("b", 500, cooldown_days=7), PolicySpec("c", 500, queue=Q),
             PolicySpec("d", 500, cooldown_days=7, queue=Q), PolicySpec("e", 900, cooldown_days=3,
                                                                       queue=QueueConfig(1, 2, 2))]
    for seed in (0, 5):
        tie = {i: int(k) for i, k in enumerate(tie_keys_drive(data.drive_key, seed))}
        for spec, res in zip(specs, run_replay(data, ranker(data, seed), specs)):
            want = reference_replay(data, tie, spec.budget, spec.cooldown_days, spec.queue)
            got = list(zip(res.service_day.tolist(), res.drive.tolist(), res.completed.tolist(), res.wait_days.tolist()))
            assert got == want, spec.name
            assert all(v == 0 for v in res.checks.values()), (spec.name, res.checks)


def test_short_ranking_list_gives_the_same_result_as_the_full_ranking(tmp_path):
    rng = np.random.default_rng(4)
    recs = [{"drive_id": f"d{d}", "day": t, "score": float(rng.integers(0, 3))} for d in range(400) for t in range(25)]
    data = make_data(tmp_path, recs, D0 + timedelta(days=24))

    class FullRanker(ScoreRanker):
        def top(self, i, m):
            return super().top(i, 10**9)

    specs = [PolicySpec("b", 300, cooldown_days=7), PolicySpec("d", 300, cooldown_days=7, queue=Q)]
    fast = run_replay(data, ranker(data), specs)
    full = run_replay(data, FullRanker(data, data.scores["ml_h30"], 0), specs)
    for x, y in zip(fast, full):
        assert np.array_equal(x.drive, y.drive) and np.array_equal(x.service_day, y.service_day)


# ------------------------------------------------------------------ known-answer fleets

def failing_fleet(tmp_path, n=1000, days=200, n_fail=40, seed=0, perfect=True):
    rng = np.random.default_rng(seed)
    fail = {int(d): int(f) for d, f in zip(rng.choice(n, n_fail, replace=False), rng.integers(45, days, n_fail))}
    recs = []
    for d in range(n):
        f = fail.get(d)
        for t in range(days if f is None else f):                           # no row on or after the failure day
            gap = None if f is None else f - t
            flagged = gap is not None and 1 <= gap <= 31
            recs.append({"drive_id": f"d{d:04d}", "day": t, "fail": f,
                         "score": 1.0 if (flagged and perfect) else 0.0,
                         "label_30d": 1 if (gap is not None and gap <= 30) else (0 if t + 30 < days else None)})
    return make_data(tmp_path, recs, D0 + timedelta(days=days - 1)), fail


def coverage_of(data, res, lead=(7, 30)):
    ev = build_events(data, *lead)
    out = event_outcomes(ev, res, data.n_drives, 29, 14)
    return ev, out, float(out["timely"][ev.eligible].mean())


def test_perfect_score_catches_every_eligible_event(tmp_path):
    data, _ = failing_fleet(tmp_path)
    res = run_replay(data, ranker(data), [PolicySpec("p", budget=50, cooldown_days=7)])[0]   # K = 5 per day
    ev, out, cov = coverage_of(data, res)
    assert ev.n_eligible == 40 and cov == 1.0
    assert out["never_before_failure"][ev.eligible].sum() == 0
    w = workload(res)                 # spare slots go to healthy drives, so precision is far below 1
    assert w["inspections_failure_within_30d"] >= ev.n_eligible and w["unused_slots"] == 0


def test_random_score_matches_the_chance_formula(tmp_path):
    data, _ = failing_fleet(tmp_path, n=1500, days=260, n_fail=500, seed=3, perfect=False)
    add_derived_scores(data)
    res = run_replay(data, ranker(data, name="random"), [PolicySpec("p", budget=100)])[0]
    ev, out, cov = coverage_of(data, res)
    # chance: each day K of N observed drives are picked uniformly at random, independently across days
    n_obs, cap = res.daily["observed"].astype(float), res.daily["capacity"].astype(float)
    expect = []
    for f in ev.fail_day[ev.eligible]:
        miss = 1.0
        for s in range(f - 30, f - 6):                                       # service days with lead 30..7
            i = s - 1 - data.first_day                                       # its decision day
            miss *= 1 - cap[i] / n_obs[i]
        expect.append(1 - miss)
    p = float(np.mean(expect))
    se = (p * (1 - p) / ev.n_eligible) ** 0.5
    assert 0.15 < p < 0.30 and abs(cov - p) < 4 * se


def test_an_event_is_counted_once_however_often_its_drive_is_inspected(tmp_path):
    data, fail = failing_fleet(tmp_path, n=300, days=120, n_fail=3, seed=5)
    res = run_replay(data, ranker(data), [PolicySpec("p", budget=400)])[0]    # K = 12, next-day reinspection
    ev, out, cov = coverage_of(data, res)
    f_of = data.fail_day[res.drive[res.completed]]
    lead = f_of - res.service_day[res.completed]
    timely_inspections = int(((f_of >= 0) & (lead >= 7) & (lead <= 30)).sum())
    assert timely_inspections > 3 * ev.n_eligible                             # many repeats inside the windows
    assert int(out["timely"][ev.eligible].sum()) == ev.n_eligible and cov == 1.0


# ------------------------------------------------------------------ no future information

def test_decisions_do_not_change_when_everything_after_day_T_changes(tmp_path):
    rng = np.random.default_rng(8)
    recs = [{"drive_id": f"d{d}", "day": t, "score": float(rng.random()), "label_30d": int(rng.integers(0, 2)),
             "fail": 55 if d < 5 else None}
            for d in range(200) for t in range(60 if d >= 5 else 55)]
    T = 30
    later = [dict(r, score=float(rng.random()), label_30d=1 - r["label_30d"]) if r["day"] > T else r for r in recs]
    later = [r for r in later if not (r["day"] > T + 5 and r["drive_id"] in ("d7", "d8"))]     # drives vanish later
    specs = [PolicySpec("b", 500, cooldown_days=7), PolicySpec("d", 500, cooldown_days=7, queue=Q)]
    a = make_data(tmp_path, recs, D0 + timedelta(days=59), name="a.parquet")
    b = make_data(tmp_path, later, D0 + timedelta(days=59), name="b.parquet")
    ra, rb = run_replay(a, ranker(a), specs), run_replay(b, ranker(b), specs)
    for x, y in zip(ra, rb):
        m, n = x.decision_day <= a.first_day + T, y.decision_day <= b.first_day + T
        assert m.sum() > 0 and served(x, a, False)[: m.sum()] == served(y, b, False)[: n.sum()]
    # a replay cut at day T equals the first part of the full replay
    cut = run_replay(a, ranker(a), specs, stop_after_day=a.first_day + T)
    for x, c in zip(ra, cut):
        m = x.decision_day <= a.first_day + T
        assert np.array_equal(c.drive, x.drive[m]) and np.array_equal(c.completed, x.completed[m])


def test_failure_dates_and_labels_never_reach_the_decisions(tmp_path):
    data, _ = failing_fleet(tmp_path, n=300, days=120, n_fail=20, seed=2)
    specs = [PolicySpec("a", 200), PolicySpec("d", 200, cooldown_days=7, queue=Q)]
    before = run_replay(data, ranker(data), specs)
    data.fail_day[:] = -1                                                     # wipe every outcome
    data.labels[30][:] = -1
    after = run_replay(data, ranker(data), specs)
    for x, y in zip(before, after):
        assert np.array_equal(x.drive, y.drive) and np.array_equal(x.completed, y.completed)


# ------------------------------------------------------------------ ties

def test_ties_are_shared_fairly_over_seeds(tmp_path):
    data = constant_fleet(tmp_path, {f"d{i:02d}": 1.0 for i in range(50)}, days=2)
    picks = np.zeros(50)
    for seed in range(600):
        res = run_replay(data, ranker(data, seed), [PolicySpec("p", budget=1000)])[0]          # K = 5 of 50
        picks[res.drive] += 1
    share = picks / 600
    assert abs(share.mean() - 0.1) < 1e-9 and share.min() > 0.05 and share.max() < 0.15


def test_tie_order_does_not_depend_on_row_order(tmp_path):
    rng = np.random.default_rng(1)
    recs = [{"drive_id": f"serial-{d}", "day": t, "score": float(d % 3)} for d in range(90) for t in range(10)]
    shuffled = [recs[i] for i in rng.permutation(len(recs))]
    a = make_data(tmp_path, recs, D0 + timedelta(days=9), name="a.parquet")
    b = make_data(tmp_path, shuffled, D0 + timedelta(days=9), name="b.parquet")
    spec = [PolicySpec("d", 1000, cooldown_days=7, queue=Q)]
    assert served(run_replay(a, ranker(a, 3), spec)[0], a) == served(run_replay(b, ranker(b, 3), spec)[0], b)
    # the key itself is fixed by MD5: the same on every machine
    assert int(drive_keys(["serial-0"])[0]) == int.from_bytes(__import__("hashlib").md5(b"serial-0").digest()[:8], "big")


def test_frozen_tie_order_repeats_and_the_daily_order_rotates(tmp_path):
    data = constant_fleet(tmp_path, {f"d{i:02d}": 1.0 for i in range(60)}, days=15)
    spec = [PolicySpec("p", budget=1000)]                                     # K = 6, next-day reinspection
    frozen = run_replay(data, ranker(data, 0, "drive"), spec)[0]
    daily = run_replay(data, ranker(data, 0, "day"), spec)[0]
    assert workload(frozen)["distinct_drives_inspected"] == 6                 # the same six tied drives every day
    assert workload(daily)["distinct_drives_inspected"] > 40                  # tied drives rotate


def test_urgency_first_interleaves_rank_by_rank(tmp_path):
    drives = ["A", "B", "C", "D", "E"]
    recs = [{"drive_id": d, "day": 0, "score": s30, "ml_h14": s14, "ml_h7": s7}
            for d, s7, s14, s30 in zip(drives, [5, 4, 3, 0, 0], [3, 5, 0, 4, 0], [4, 3, 0, 0, 5])]
    data = make_data(tmp_path, recs + [dict(r, day=1) for r in recs], D0 + timedelta(days=1))
    inter = InterleaveRanker([ranker(data, name=n) for n in ("ml_h7", "ml_h14", "ml_h30")])
    order, complete = inter.top(0, 5)
    # rank 1 of each list: A, B, E; rank 2: B (taken), D, A (taken); rank 3: C
    assert [str(data.drive_ids[d]) for d in order] == ["A", "B", "E", "D", "C"] and complete


# ------------------------------------------------------------------ events and lead boundaries

def manual_result(data, services):
    """A service log written by hand: [(drive id, service day offset, completed)]."""
    idx = {str(d): i for i, d in enumerate(data.drive_ids)}
    n = len(services)
    return PolicyResult(spec=PolicySpec("manual", 20),
                        decision_day=np.array([data.first_day + s - 1 for _, s, _ in services], dtype=np.int32),
                        service_day=np.array([data.first_day + s for _, s, _ in services], dtype=np.int32),
                        drive=np.array([idx[d] for d, _, _ in services], dtype=np.int32),
                        completed=np.array([c for _, _, c in services], dtype=bool),
                        label30=np.full(n, -1, dtype=np.int8), wait_days=np.zeros(n, dtype=np.int32))


def test_event_eligibility_burn_in_and_opportunity(tmp_path):
    recs = []
    for d, first_seen, f in (("early", 0, 30), ("ok", 0, 31), ("late_arrival", 85, 90), ("gappy", 0, 80), ("alive", 0, None)):
        for t in range(first_seen, 100 if f is None else f):
            if d == "gappy" and 45 <= t <= 75:                                # away during the whole window
                continue
            recs.append({"drive_id": d, "day": t, "score": 0.0, "fail": f})
    data = make_data(tmp_path, recs, D0 + timedelta(days=99))
    ev = build_events(data, 7, 30)
    status = {str(data.drive_ids[d]): ("eligible", "burn_in", "no_opportunity")[r] for d, r in zip(ev.drive, ev.reason)}
    assert status == {"early": "burn_in", "ok": "eligible", "late_arrival": "no_opportunity", "gappy": "no_opportunity"}
    opp = {str(data.drive_ids[d]): int(o) for d, o in zip(ev.drive, ev.opportunity_days)}
    assert opp["ok"] == 24                                                    # service days 1..24, all observed
    assert ev.counts()["eligible_with_full_opportunity"] == 1


def test_lead_boundaries_7_and_30_count_and_6_and_31_do_not(tmp_path):
    recs = [{"drive_id": d, "day": t, "score": 0.0, "fail": 60} for d in ("L6", "L7", "L30", "L31", "LC")
            for t in range(60)]
    data = make_data(tmp_path, recs, D0 + timedelta(days=99))
    ev = build_events(data, 7, 30)
    res = manual_result(data, [("L6", 54, True), ("L7", 53, True), ("L30", 30, True), ("L31", 29, True),
                               ("LC", 40, False),                              # cancelled: does not count
                               ("L7", 52, True), ("L7", 51, True)])           # repeats: still one event
    out = event_outcomes(ev, res, data.n_drives, 29, 14)
    by = lambda k: {str(data.drive_ids[d]): v for d, v in zip(ev.drive, out[k].tolist())}
    assert by("timely") == {"L6": False, "L7": True, "L30": True, "L31": False, "LC": False}
    assert by("timely_alt_window") == {"L6": False, "L7": True, "L30": False, "L31": False, "LC": False}
    assert by("late_only") == {"L6": True, "L7": False, "L30": False, "L31": False, "LC": False}
    assert by("any_prior")["L31"] and by("never_before_failure")["LC"]
    assert by("earliest_timely_lead")["L7"] == 9 and by("earliest_timely_lead")["L6"] == -1
    assert by("timely_long") == {"L6": False, "L7": False, "L30": True, "L31": False, "LC": False}


# ------------------------------------------------------------------ RQ2: persistence rules and turnover

def test_persistence_rules_on_a_hand_worked_sequence(tmp_path):
    recs = [{"drive_id": "A", "day": t, "score": s} for t, s in enumerate([1, 5, 3, 4])]
    recs += [{"drive_id": "A", "day": 13, "score": 2.0}]                      # 10 days later: history restarts
    data = make_data(tmp_path, recs, D0 + timedelta(days=13))
    s = data.scores["ml_h30"]
    f = lambda rule: persistence_priority(data, s, rule, reset_gap=7).tolist()
    assert f({"kind": "daily"}) == [1, 5, 3, 4, 2]
    assert f({"kind": "confirm", "days": 2}) == [-np.inf, 1, 3, 3, -np.inf]
    assert f({"kind": "confirm", "days": 3}) == [-np.inf, -np.inf, 1, 3, -np.inf]
    assert f({"kind": "retain", "days": 2}) == [1, 5, 5, 5, 2]
    assert f({"kind": "ewma", "alpha": 0.5}) == [1, 3, 3, 3.5, 2]


def test_confirm_rule_is_the_classic_consecutive_days_rule(tmp_path):
    rng = np.random.default_rng(6)
    recs = [{"drive_id": f"d{d}", "day": t, "score": float(rng.integers(0, 10))} for d in range(40) for t in range(12)]
    data = make_data(tmp_path, recs, D0 + timedelta(days=11))
    s = data.scores["ml_h30"]
    pr = persistence_priority(data, s, {"kind": "confirm", "days": 2})
    table = {(int(d), int(t)): float(x) for d, t, x in zip(data.drive, data.day, s)}
    for theta in (3, 6):
        for d, t, p in zip(data.drive, data.day, pr):
            classic = table[(int(d), int(t))] >= theta and table.get((int(d), int(t) - 1), -1) >= theta
            assert (p >= theta) == classic


def test_watchlist_turnover_matches_the_methodology_example():
    tr = WatchlistTracker(budget=10000, mult=1)                               # list size = number of observed drives x 1
    seen = np.full(10, 1)
    assert tr.observe(0, 0, 3, np.array([0, 1, 2]), True, seen)               # {A, B, C}
    assert tr.observe(1, 1, 3, np.array([1, 2, 3]), True, seen)               # {B, C, D}
    s = tr.summary()
    assert s["mean_jaccard"] == pytest.approx(0.5) and s["entries_per_day"] == 1 and s["exits"] == 1
    assert s["turnover"] == pytest.approx(1 / 3) and s["exits_outranked"] == 1 and s["mean_list_size"] == 3
    assert s["mean_days_on_list"] == pytest.approx(6 / 4)


def test_smoothing_lowers_turnover_on_a_noisy_score(tmp_path):
    rng = np.random.default_rng(9)
    base = rng.random(300)
    recs = [{"drive_id": f"d{d}", "day": t, "score": float(base[d] + rng.normal(0, 0.3))}
            for d in range(300) for t in range(40)]
    data = make_data(tmp_path, recs, D0 + timedelta(days=39))
    turn = {}
    for rule in ({"kind": "daily"}, {"kind": "ewma", "alpha": 0.2}):
        pr = persistence_priority(data, data.scores["ml_h30"], rule)
        tr = WatchlistTracker(budget=1000, mult=1)
        run_replay(data, ScoreRanker(data, pr, 0), [PolicySpec("p", 1000)], [tr])
        turn[rule["kind"]] = tr.summary()
        assert tr.summary()["mean_list_size"] == 30                           # occupancy matched by construction
    assert turn["ewma"]["turnover"] < 0.6 * turn["daily"]["turnover"]
    assert turn["ewma"]["mean_jaccard"] > turn["daily"]["mean_jaccard"]


# ------------------------------------------------------------------ statistics

def test_contrasts_and_paired_bootstrap():
    order = [(s, r, g) for s in ("smart", "ml") for r in ("next", "cool") for g in ("fresh", "queue")]
    cells = {k: j for j, k in enumerate(order)}
    rng = np.random.default_rng(0)
    P = (rng.random((400, 8)) < 0.3).astype(float)
    P[:, cells[("ml", "cool", "fresh")]] = np.maximum(P[:, cells[("smart", "cool", "fresh")]],
                                                      (rng.random(400) < 0.2))          # ML >= SMART here, paired
    con = rq1_contrasts(cells)
    rows = {r["contrast"]: r for r in evaluate_contrasts(P, con, reps=500, seed=1)}
    m = P.mean(axis=0)
    d = lambda r, g: m[cells[("ml", r, g)]] - m[cells[("smart", r, g)]]
    assert rows["delta_ml_minus_smart|cool|fresh"]["estimate"] == pytest.approx(d("cool", "fresh"))
    assert rows["delta_ml_minus_smart|cool|fresh"]["ci_low"] > 0               # paired gain is detected
    assert rows["interaction_reinspection|fresh"]["estimate"] == pytest.approx(d("cool", "fresh") - d("next", "fresh"))
    assert rows["interaction_three_way"]["estimate"] == pytest.approx(
        (d("cool", "queue") - d("next", "queue")) - (d("cool", "fresh") - d("next", "fresh")))
    again = {r["contrast"]: r for r in evaluate_contrasts(P, con, reps=500, seed=1)}
    assert again["interaction_three_way"]["ci_low"] == rows["interaction_three_way"]["ci_low"]   # reproducible
    same = evaluate_contrasts(np.repeat(P[:, :1], 8, axis=1), con, reps=200, seed=2)
    assert all(r["estimate"] == 0 and r["ci_low"] == 0 and r["ci_high"] == 0 for r in same)
    b = bootstrap_means(P, 300, 7)
    assert b.shape == (300, 8) and abs(b.mean(axis=0) - m).max() < 0.01


def test_fleet_halves_are_whole_drives_and_about_half():
    keys = drive_keys([f"d{i}" for i in range(4000)])
    a, b = in_subsample(keys, 0, 0.5), in_subsample(keys, 1, 0.5)
    assert 0.46 < a.mean() < 0.54 and 0.4 < (a == b).mean() < 0.6             # different halves per repetition
    assert np.array_equal(a, in_subsample(keys, 0, 0.5))


# ------------------------------------------------------------------ RQ3 prediction views

def test_actionable_label_is_failure_8_to_31_days_after_the_decision_day(tmp_path):
    recs = [{"drive_id": "F", "day": t, "score": 0.5, "fail": 50, "label_30d": 1 if 50 - t <= 30 else 0,
             "ml_h7": 0.1, "ml_h14": 0.1, "label_7d": 1 if 50 - t <= 7 else 0, "label_14d": 1 if 50 - t <= 14 else 0}
            for t in range(50)]
    recs += [{"drive_id": "H", "day": t, "score": 0.1, "label_30d": 0 if t < 60 else None, "ml_h7": 0.0, "ml_h14": 0.0,
              "label_7d": 0, "label_14d": 0} for t in range(90)]
    data = make_data(tmp_path, recs, D0 + timedelta(days=89))
    y, keep = actionable_label(data, 7, 30)
    f = data.drive_ids[data.drive] == "F"
    gap = 50 - (data.day[f] - data.first_day)
    assert set(gap[y[f] == 1]) == set(range(8, 32))
    assert not keep[f][(gap >= 1) & (gap <= 7)].any()                         # too late: left out, not a negative
    assert keep[f][gap == 31].all() and keep[f][gap == 40].all()
    h = data.drive_ids[data.drive] == "H"
    assert keep[h].sum() == 60 and y[h].sum() == 0                            # unknown 30-day outcome: left out
    rows = {(r["score"], r["view"]): r for r in horizon_metrics(data, 7, 30)}
    assert rows[("ml_h30", "actionable_8_to_31d")]["positives"] == 24
    assert rows[("ml_h30", "own_label_30d")]["positives"] == 30 and rows[("ml_h30", "own_label_30d")]["rows"] == 110
    assert rows[("ml_h30", "common_rows_label_30d")]["rows"] == 110


# ------------------------------------------------------------------ input building and the command line

def phase4_like(tmp_path, split="valid", n=400, days=150, n_fail=30, seed=0, first=D0, duplicate=False):
    """Phase 4 style score files (long format, four models) for h30, h14 and h7."""
    rng = np.random.default_rng(seed)
    fail = {int(d): int(f) for d, f in zip(rng.choice(n, n_fail, replace=False), rng.integers(40, days, n_fail))}
    rows = []
    for d in range(n):
        f = fail.get(d)
        for t in range(days if f is None else f):
            gap = None if f is None else f - t
            risk = 0.0 if gap is None else max(0.0, 1 - gap / 40)
            rows.append((f"Z{d:05d}", t, f, gap, risk))
    base = pd.DataFrame(rows, columns=["drive_id", "t", "f", "gap", "risk"])
    base["date"] = [first + timedelta(days=int(t)) for t in base["t"]]
    base["eval_fail_date"] = [None if pd.isna(f) else first + timedelta(days=int(f)) for f in base["f"]]
    base["row_id"] = np.arange(1, len(base) + 1)
    base["smart_rule_count"] = (base["risk"] * 3 + rng.random(len(base)) * 1.2).astype(int).clip(0, 5)
    base["smart_rule_score"] = base["smart_rule_count"] * rng.integers(1, 50, len(base)).astype(float)
    base.loc[base["smart_rule_count"] == 0, "smart_rule_score"] = np.nan
    base["smart_rule_flag"] = (base["smart_rule_count"] > 0).astype(int)
    for h in (30, 14, 7):
        label = np.where(base["gap"].notna() & (base["gap"] <= h), 1.0, np.where(base["t"] + h < days, 0.0, np.nan))
        parts = []
        for model, noise in (("lr", 0.5), ("glm", 0.4), ("gbm", 0.25), ("drf", 0.2)):
            p = base[["row_id", "drive_id", "date", "eval_fail_date", "smart_rule_flag", "smart_rule_score",
                      "smart_rule_count"]].copy()
            p["horizon"], p["model"], p["label"], p["weight"] = h, model, label, 10.0
            p["label_known"] = ~np.isnan(label)
            p["score"] = np.clip(base["risk"] * (h / 30) ** 0.3 + rng.normal(0, noise, len(base)) * 0.2, 0, 1)
            parts.append(p)
        out = pd.concat(parts, ignore_index=True)
        if duplicate and h == 30:
            out = pd.concat([out, out[out["model"] == "drf"].iloc[:3]], ignore_index=True)
        out["label"] = out["label"].astype("Int64")
        (tmp_path / "p4" / f"h{h}").mkdir(parents=True, exist_ok=True)
        out.to_parquet(tmp_path / "p4" / f"h{h}" / f"scores_{split}.parquet", index=False)
    return tmp_path / "p4", len(base)


def test_prepare_input_keeps_every_row_and_refuses_bad_files(tmp_path):
    p4, n_rows = phase4_like(tmp_path)
    man = prepare_from_phase4(p4, "valid", tmp_path / "replay_valid.parquet", D0, date(2024, 12, 31))
    assert man["rows"] == n_rows and man["horizons_present"] == [30, 14, 7]
    df = pd.read_parquet(tmp_path / "replay_valid.parquet")
    assert {"drive_id", "date", "fail_date", "label_30d", "label_14d", "label_7d", "smart_count", "smart_sum",
            "ml_h30", "gbm_h30", "ml_h14", "ml_h7"} <= set(df.columns)
    src = pd.read_parquet(p4 / "h30" / "scores_valid.parquet")
    drf = src[src["model"] == "drf"].set_index(["drive_id", "date"])["score"]
    got = df.set_index(["drive_id", "date"])["ml_h30"]
    assert len(got) == len(drf) and np.allclose(got.sort_index().to_numpy(), drf.sort_index().to_numpy())
    with pytest.raises(SystemExit, match="locked"):
        prepare_from_phase4(p4, "test", tmp_path / "x.parquet", D0, date(2024, 12, 31))
    (tmp_path / "dup").mkdir()
    bad, _ = phase4_like(tmp_path / "dup", duplicate=True)
    with pytest.raises(SystemExit, match="duplicate"):
        prepare_from_phase4(bad, "valid", tmp_path / "dup" / "r.parquet", D0, date(2024, 12, 31))


def test_loader_refuses_rows_on_or_after_the_failure_day(tmp_path):
    recs = [{"drive_id": "A", "day": t, "score": 0.1, "fail": 5} for t in range(7)]
    with pytest.raises(SystemExit, match="on or after"):
        make_data(tmp_path, recs, D0 + timedelta(days=9))


def test_protocol_file_holds_the_frozen_numbers():
    p = load_protocol()
    assert p.budget_primary == 20 and p.budgets_sensitivity == [10, 50]
    assert p.cooldown_days == 7 and p.queue_primary == Q
    assert p.tie_mode == "drive" and p.tie_seeds == list(range(10))
    assert (p.smart, p.ml) == ("smart_count", "ml_h30")
    e = p.raw["endpoint"]
    assert (e["lead_min"], e["lead_max"], e["lead_max_sensitivity"]) == (7, 30, 29)
    assert p.raw["bootstrap"] == {"reps": 2000, "seed": 20261111, "level": 0.95}
    assert [r["name"] for r in p.raw["rq2"]["rules"]] == ["daily", "confirm_2", "confirm_3", "ewma_0.5", "ewma_0.2",
                                                           "retain_2", "retain_6"]
    assert p.split_days("test") == (date(2025, 1, 1), date(2026, 6, 30))
    assert len(p.sha256) == 64


def test_command_line_end_to_end_and_test_split_lock(tmp_path):
    p4, _ = phase4_like(tmp_path, n=1200, days=150, n_fail=60)
    out = tmp_path / "out"
    rc = run_phase5.main(["--phase4", str(p4), "--split", "valid", "--population", "sample10", "--out", str(out),
                          "--quick"])
    assert rc == 0
    checks = json.loads((out / "checks.json").read_text())
    assert checks["all_passed"] and checks["engine_rule_violations_total"] == 0
    nf = checks["no_future_information"]
    assert nf["passed"] and nf["score_values_changed_after_cut"] > 0 and nf["logs_that_changed"] == []
    assert nf["families"] >= 20 and nf["primary_logs_checked_against_full_replay"] == 8
    assert checks["eligible_event_set_identical_in_every_cell"] and checks["eligible_events_do_not_depend_on_scores"]
    assert len(checks["eligible_event_set_sha256"]["all"]) == 64
    res = pd.read_csv(out / "cell_results.csv")
    prim = res[res["group"] == "rq1_primary"]
    assert sorted(prim["e_cell"]) == [f"E{i}" for i in range(1, 9)]
    assert prim["eligible_events"].nunique() == 1 and prim["coverage"].between(0, 1).all()
    assert (prim["scheduled"] <= prim["slots"]).all() and (prim["completed"] + prim["cancelled"] == prim["scheduled"]).all()
    con = pd.read_csv(out / "rq1_contrasts.csv")
    assert {"primary_B20", "budget_B10", "budget_B50", "tie_day", "score_gbm_h30"} <= set(con["set"])
    row = con[(con["set"] == "primary_B20") & (con["contrast"] == "delta_ml_minus_smart|cool|fresh")].iloc[0]
    e3 = prim[prim["e_cell"] == "E3"]["coverage"].iloc[0]
    e4 = prim[prim["e_cell"] == "E4"]["coverage"].iloc[0]
    assert row["estimate"] == pytest.approx(e4 - e3) and row["ci_low"] <= row["estimate"] <= row["ci_high"]
    rq2 = pd.read_csv(out / "rq2_stability.csv")
    assert len(rq2) == 7 * 2 * 2 and rq2["list1K_mean_size"].nunique() == 1  # every rule fills the same list size
    rq3 = pd.read_csv(out / "rq3_horizons.csv")
    assert {"ml_h7", "ml_h14", "ml_h30", "urgency_first_7_14_30"} <= set(rq3[rq3["table"] == "budgeted_coverage"]["score"])
    ev = pd.read_parquet(out / "event_coverage.parquet")
    assert (ev["status"] == "eligible").sum() == prim["eligible_events"].iloc[0]
    log = pd.read_parquet(out / "service_log_E8.parquet")
    assert not log.duplicated(["service_date", "drive_id"]).any()
    info = json.loads((out / "run_info.json").read_text())
    assert info["exploratory"] and info["quick_smoke_test_do_not_report"] and info["protocol_sha256"]
    assert info["protocol_is_the_frozen_file"] and info["test_authorization"] is None


# ------------------------------------------------------------------ review of 12 Nov 2026: B2, B3, B4, C1, C5

def test_queue_never_exceeds_the_cap_when_capacity_falls_sharply(tmp_path):
    # 100 drives for 4 days, then only 20: K falls from 10 to 2, the cap from 30 to 6 (review B2)
    rng = np.random.default_rng(3)
    recs = [{"drive_id": f"d{d:03d}", "day": t, "score": float(rng.random())}
            for d in range(100) for t in range(12) if t < 4 or d < 20]
    data = make_data(tmp_path, recs, D0 + timedelta(days=11))
    spec = PolicySpec("q", budget=1000, queue=Q)
    res = run_replay(data, ranker(data), [spec])[0]
    cap_day, qlen = 3 * res.daily["capacity"], res.daily["queue_length"]
    assert res.daily["capacity"][:5].tolist() == [10, 10, 10, 10, 2]
    assert qlen[3] > cap_day[4]                                   # the old queue was larger than the new cap
    assert (qlen <= cap_day).all() and sum(res.checks.values()) == 0
    led = res.queue_ledger
    cut = led["exit_reason"] == 3
    assert cut.sum() == res.daily["removed_over_cap"].sum() > 0 and res.daily["removed_over_cap"][4] > 0
    assert res.daily["admitted"][4] == 0                          # nothing is admitted on the day of the cut
    assert workload(res)["queue_removed_cap_fell"] == int(cut.sum())
    # the entries that were kept on that day are not younger than the ones that were removed
    day4 = data.first_day + 4
    removed_admit = led["admit_day"][cut & (led["exit_day"] == day4)]
    later = (led["exit_day"] > day4) & (led["admit_day"] < day4)
    assert led["admit_day"][later].max() <= removed_admit.min() + 1
    tie = {i: int(k) for i, k in enumerate(tie_keys_drive(data.drive_key, 0))}
    got = list(zip(res.service_day.tolist(), res.drive.tolist(), res.completed.tolist(), res.wait_days.tolist()))
    assert got == reference_replay(data, tie, 1000, 0, Q)


@pytest.mark.parametrize("cd", [0, 7])
def test_small_queue_1k_2k_2d_equals_fresh_topk(tmp_path, cd):
    # admit K, cap 2K: K entries come in and the same K are scheduled, so nothing ever waits (review note)
    rng = np.random.default_rng(11)
    recs = [{"drive_id": f"d{d:03d}", "day": t, "score": float(rng.integers(0, 8))}
            for d in range(300) for t in range(40) if rng.random() > 0.05]
    data = make_data(tmp_path, recs, D0 + timedelta(days=39))
    small = QueueConfig(admit_mult=1, cap_mult=2, expire_days=2)
    fresh, queued = run_replay(data, ranker(data), [PolicySpec("f", 300, cooldown_days=cd),
                                                    PolicySpec("q", 300, cooldown_days=cd, queue=small)])
    assert len(fresh.drive) > 100 and np.array_equal(fresh.completed, queued.completed)
    for a, b in zip(np.split(fresh.drive, np.flatnonzero(np.diff(fresh.service_day)) + 1),
                    np.split(queued.drive, np.flatnonzero(np.diff(queued.service_day)) + 1)):
        assert set(a.tolist()) == set(b.tolist())                 # the same drives every day
    assert queued.wait_days.max() == 0 and queued.daily["queue_length"].max() == 0


def _fam(name, make, specs):
    return {"name": name, "make_ranker": make, "specs": specs}


def test_scramble_check_passes_for_honest_scores_and_catches_a_look_ahead(tmp_path):
    rng = np.random.default_rng(21)
    recs = [{"drive_id": f"d{d:03d}", "day": t, "score": float(rng.random()), "smart_count": int(rng.integers(0, 4)),
             "label_30d": int(rng.integers(0, 2)), "fail": 58 if d < 6 else None}
            for d in range(150) for t in range(60 if d >= 6 else 58)]
    data = make_data(tmp_path, recs, D0 + timedelta(days=59))
    add_derived_scores(data)
    specs = [PolicySpec("a", 500), PolicySpec("b", 500, cooldown_days=7), PolicySpec("d", 500, cooldown_days=7, queue=Q)]
    cut = data.first_day + 30
    other = p5checks.scrambled_after(data, cut)
    before = data.day <= cut
    for k in ("ml_h30", "smart_count", "smart_count_sum"):
        assert np.array_equal(other.scores[k][before], data.scores[k][before])          # the past is untouched
    assert (other.scores["ml_h30"][~before] != data.scores["ml_h30"][~before]).mean() > 0.9
    assert (other.fail_day < 0).all() and np.array_equal(other.day, data.day) and np.array_equal(other.drive, data.drive)
    honest = [_fam("ml", lambda d, seed: ScoreRanker(d, d.scores["ml_h30"], seed), specs),
              _fam("ewma", lambda d, seed: ScoreRanker(d, persistence_priority(
                  d, d.scores["ml_h30"], {"name": "ewma_0.5", "kind": "ewma", "alpha": 0.5}, 7), seed), specs)]
    ok = p5checks.no_future_information(data, honest, cut, 0)
    assert ok["passed"] and ok["service_logs_compared"] == 6 and ok["service_log_rows_compared"] > 500

    def look_ahead(d, seed):          # a broken score: the mean of the drive over the WHOLE period
        mean = np.bincount(d.drive, d.scores["ml_h30"], d.n_drives) / np.maximum(np.bincount(d.drive, minlength=d.n_drives), 1)
        return ScoreRanker(d, mean[d.drive], seed)
    bad = p5checks.no_future_information(data, honest + [_fam("cheat", look_ahead, specs)], cut, 0)
    assert not bad["passed"] and bad["logs_that_changed"] and all(x.startswith("cheat|") for x in bad["logs_that_changed"])


def test_event_identity_key_and_independence_from_scores(tmp_path):
    data, _ = failing_fleet(tmp_path, n=300, days=120, n_fail=20, seed=2)
    ev = build_events(data, 7, 30, 24)
    key = p5checks.event_key(data, ev)
    assert len(key) == 64 and p5checks.events_do_not_depend_on_scores(data, ev, 7, 30, 24)
    data.scores["ml_h30"] = data.scores["ml_h30"][::-1].copy()                 # other scores: same events
    assert p5checks.event_key(data, build_events(data, 7, 30, 24)) == key
    moved = data.fail_day.copy()
    j = int(np.flatnonzero(moved >= 0)[0])
    moved[j] -= 1                                                               # one failure day moves: another key
    from dataclasses import replace
    assert p5checks.event_key(replace(data, fail_day=moved), build_events(replace(data, fail_day=moved), 7, 30, 24)) != key


def test_smart_count_sum_is_row_wise_and_orders_count_first(tmp_path):
    recs = [{"drive_id": f"d{d}", "day": t, "score": 0.0, "smart_count": c, "smart_sum": s}
            for t in range(3) for d, (c, s) in enumerate([(0, 0), (1, 5), (1, 9e11), (2, 1), (2, 2), (3, 0)])]
    data = make_data(tmp_path, recs, D0 + timedelta(days=2))
    add_derived_scores(data)
    v = data.scores["smart_count_sum"]
    day0 = v[data.day == data.first_day]
    assert np.all(np.diff(day0[np.argsort(data.drive[data.day == data.first_day])]) > 0)   # count first, then sum
    # row-wise: the value of a row does not change when all later rows are removed
    short = make_data(tmp_path, [r for r in recs if r["day"] == 0], D0, name="short.parquet")
    add_derived_scores(short)
    assert np.array_equal(np.sort(short.scores["smart_count_sum"]), np.sort(day0))


def test_protocol_hash_is_the_same_in_the_document_the_code_and_the_file(tmp_path):
    import hashlib
    import re
    root = p5config.DEFAULT_CONFIG.parent.parent
    raw = p5config.DEFAULT_CONFIG.read_bytes().replace(b"\r\n", b"\n")
    frozen = p5config.FROZEN_SHA256["phase5-v1"]
    assert hashlib.sha256(raw).hexdigest() == frozen == load_protocol().sha256 and load_protocol().is_frozen
    doc = (root / "docs" / "phase5_protocol.md").read_text(encoding="utf-8")
    assert re.search(r"SHA-256 of the frozen JSON: `([0-9a-f]{64})`", doc).group(1) == frozen
    # the hash recorded by the Windows dry run is the same file with Windows line endings
    crlf = raw.replace(b"\n", b"\r\n")
    assert hashlib.sha256(crlf).hexdigest() == p5config.FROZEN_SHA256_CRLF_COPY["phase5-v1"]
    win = tmp_path / "win.json"
    win.write_bytes(crlf)
    assert load_protocol(win).is_frozen                                         # line endings do not matter
    changed = json.loads(raw)
    changed["budget_per_10000"]["primary"] = 21
    other = tmp_path / "changed.json"
    other.write_text(json.dumps(changed), encoding="utf-8")
    assert not load_protocol(other).is_frozen                                   # one number changed: not frozen
    with pytest.raises(SystemExit, match="not the frozen"):
        p5config.check_test_authorization(load_protocol(other), "sample10")


def _authorize(tmp_path, monkeypatch, committed=True, **over):
    """Put an authorization file and a lock register in a temporary place. The command line has no option for
    either, so the tests move the fixed places themselves."""
    auth = {"protocol_sha256": p5config.FROZEN_SHA256["phase5-v1"], "review_document": "review.pdf",
            "authorized_by": "coordinator", "test_replays_authorized": {"sample10": 1, "full_fleet": 1}}
    auth.update(over)
    f = tmp_path / "phase5_test_authorization.json"
    f.write_text(json.dumps(auth), encoding="utf-8")
    monkeypatch.setattr(p5config, "DEFAULT_AUTHORIZATION", f)
    monkeypatch.setattr(p5config, "DEFAULT_LOCK_DIR", tmp_path / "register")
    monkeypatch.setattr(p5config, "committed_state", lambda *a, **k: (
        {"ok": True, "reason": "", "commit": "abc123"} if committed else
        {"ok": False, "reason": "the authorization file is not committed", "commit": "abc123"}))
    return f


def test_test_split_needs_authorization_and_runs_once_whatever_the_output_folder(tmp_path, monkeypatch):
    (tmp_path / "t").mkdir()
    p4t, _ = phase4_like(tmp_path / "t", split="test", n=300, days=100, n_fail=20, first=date(2025, 1, 1))
    lock = tmp_path / "register"
    base = ["--phase4", str(p4t), "--split", "test", "--population", "sample10"]
    run = lambda out, *more: run_phase5.main(base + ["--out", str(tmp_path / out), *more])
    monkeypatch.setattr(p5config, "DEFAULT_AUTHORIZATION", tmp_path / "missing.json")
    monkeypatch.setattr(p5config, "DEFAULT_LOCK_DIR", lock)
    with pytest.raises(SystemExit, match="locked"):                             # no switch
        run("o1")
    with pytest.raises(SystemExit, match="no authorization file"):              # switch, but no authorization
        run("o1", "--final-test-run")
    for flag in (["--authorization", "x.json"], ["--test-lock-dir", str(tmp_path)]):   # the overrides are gone
        with pytest.raises(SystemExit) as e:
            run("o1", "--final-test-run", *flag)
        assert e.value.code == 2                                                # argparse: unknown option
    _authorize(tmp_path, monkeypatch)
    with pytest.raises(SystemExit, match="--quick is refused"):
        run("o1", "--final-test-run", "--quick")
    with pytest.raises(SystemExit, match="--config is refused"):
        run("o1", "--final-test-run", "--config", str(p5config.DEFAULT_CONFIG))
    _authorize(tmp_path, monkeypatch, protocol_sha256="0" * 64)
    with pytest.raises(SystemExit, match="another protocol hash"):
        run("o1", "--final-test-run")
    _authorize(tmp_path, monkeypatch, authorized_by="")
    with pytest.raises(SystemExit, match="must name"):
        run("o1", "--final-test-run")
    _authorize(tmp_path, monkeypatch, test_replays_authorized={"sample10": 0, "full_fleet": 1})
    with pytest.raises(SystemExit, match="does not allow"):
        run("o1", "--final-test-run")
    _authorize(tmp_path, monkeypatch, committed=False)                          # a file that is not committed
    with pytest.raises(SystemExit, match="not committed"):
        run("o1", "--final-test-run")
    assert not lock.exists()                                                    # no refused attempt used the run
    # the register is outside the output folder: a NEW output folder does not give a second run
    _authorize(tmp_path, monkeypatch)
    auth = p5config.check_test_authorization(load_protocol(), "sample10")
    reg = p5config.claim_test_replay(load_protocol(), "sample10", auth, tmp_path / "o1")
    rec = json.loads(reg.read_text())
    assert len(rec) == 1 and rec[0]["git_commit"] == "abc123" and rec[0]["authorized_by"] == "coordinator"
    assert rec[0]["machine"] is not None and rec[0]["user"] and reg.parent == lock
    with pytest.raises(SystemExit, match="already replayed"):
        run("o2", "--final-test-run")
    with pytest.raises(SystemExit, match="already replayed"):
        p5config.claim_test_replay(load_protocol(), "sample10", auth, tmp_path / "o3")
    p5config.claim_test_replay(load_protocol(), "full_fleet", auth, tmp_path / "o4")       # counted per population
    assert len(json.loads(reg.read_text())) == 2


def test_authorization_must_be_committed_and_the_checkout_clean(tmp_path):
    import subprocess
    repo = tmp_path / "repo"
    (repo / "configs").mkdir(parents=True)
    (repo / "src" / "drivepriority" / "simulation").mkdir(parents=True)
    code = repo / "src" / "drivepriority" / "simulation" / "engine.py"
    code.write_text("x = 1\n")
    git = lambda *a: subprocess.run(["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t", *a],
                                    check=True, capture_output=True)
    auth = repo / "configs" / "phase5_test_authorization.json"
    assert p5config.committed_state(tmp_path / "nowhere", auth)["ok"] is False             # not a git checkout
    git("init", "-q")
    git("add", "-A")
    git("commit", "-q", "-m", "code")
    auth.write_text("{}")
    st = p5config.committed_state(repo, auth)
    assert not st["ok"] and "not committed" in st["reason"]                                # written, not committed
    git("add", "-A")
    git("commit", "-q", "-m", "authorize")
    st = p5config.committed_state(repo, auth)
    assert st["ok"] and len(st["commit"]) == 40
    code.write_text("x = 2\n")                                                             # code edited after commit
    st = p5config.committed_state(repo, auth)
    assert not st["ok"] and "differ from the commit" in st["reason"]


def test_share_above_zero_treats_rounding_noise_as_a_tie():
    from drivepriority.simulation import stats
    rng = np.random.default_rng(1)
    P = np.column_stack([rng.random(200), np.zeros(200)])
    P[:, 1] = P[:, 0]                                         # two identical cells: every resampled difference is 0
    row = evaluate_contrasts(P, {"same": {0: 1.0, 1: -1.0}}, 500, 3)[0]
    assert row["estimate"] == 0 and row["share_of_resamples_above_zero"] == 0
    noisy = P.copy()
    noisy[:, 1] = P[:, 0] * (1 + 2e-16) - 1e-17               # float noise only
    assert evaluate_contrasts(noisy, {"same": {0: 1.0, 1: -1.0}}, 500, 3)[0]["share_of_resamples_above_zero"] == 0
    assert stats.ZERO_TOLERANCE == 1e-12
    real = np.column_stack([np.ones(200), np.zeros(200)])     # a real difference is still counted
    assert evaluate_contrasts(real, {"d": {0: 1.0, 1: -1.0}}, 500, 3)[0]["share_of_resamples_above_zero"] == 1


def test_validation_replay_holds_no_failure_date_after_its_outcome_window(tmp_path):
    # a drive that fails 40 days after the last replay day: its failure date must not be kept (review B1)
    recs = [{"drive_id": d, "day": t, "score": 0.5, "fail": f} for d, f in (("near", 25), ("far", 60), ("ok", None))
            for t in range(20)]
    data = make_data(tmp_path, recs, D0 + timedelta(days=19))
    by = dict(zip(data.drive_ids.tolist(), data.fail_day.tolist()))
    assert by["near"] == data.first_day + 25 and by["far"] < 0 and by["ok"] < 0
