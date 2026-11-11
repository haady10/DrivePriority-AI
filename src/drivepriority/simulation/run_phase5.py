"""Phase 5 command line: build the replay input and run the frozen inspection simulation.

Decision-support simulation only: simulated inspections, no claim that failures are prevented.
Every rule and number comes from configs/phase5_protocol_v1.json (docs/phase5_protocol.md).

Typical use (validation year, 10% evaluation sample from Phase 4):

    python -m drivepriority.simulation.run_phase5 --phase4 <04_Phase4_FINAL folder with h30, h14, h7> ^
        --split valid --population sample10 --out <output folder>

The test split is locked: it needs --final-test-run and can be replayed only once per output folder.
--quick is for smoke tests only (2 tie seeds, few bootstrap resamples, no fleet-half reruns); its
results must never be reported.
"""
from __future__ import annotations

import argparse
import json
import platform
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from .config import Protocol, QueueConfig, load_protocol
from .engine import PolicyResult, PolicySpec, run_replay
from .events import EventTable, build_events, event_outcomes, workload
from .hashing import in_subsample
from .prediction import horizon_metrics
from .ranking import InterleaveRanker, ScoreRanker
from .replay_input import (EPOCH, ReplayData, add_derived_scores, load_replay_input, prepare_from_phase4,
                           sha256_file)
from .stability import WatchlistTracker
from .stats import coverage_intervals, evaluate_contrasts, rq1_contrasts
from .transforms import persistence_priority

E_CELLS = {("smart", "next", "fresh"): "E1", ("ml", "next", "fresh"): "E2", ("smart", "cool", "fresh"): "E3",
           ("ml", "cool", "fresh"): "E4", ("smart", "next", "queue"): "E5", ("ml", "next", "queue"): "E6",
           ("smart", "cool", "queue"): "E7", ("ml", "cool", "queue"): "E8"}


def to_date(day: int) -> date:
    return EPOCH + timedelta(days=int(day))


@dataclass
class Cell:
    """One experimental cell: a score, a persistence rule, a policy, a budget, a tie mode, a fleet."""
    group: str
    source: str
    rule: str
    spec: PolicySpec
    tie_mode: str
    fleet: str = "all"
    per_seed: list = field(default_factory=list)
    capture_sum: np.ndarray = None          # per eligible event: number of seeds with a timely capture
    first: PolicyResult = None              # seed 0 replay (kept only when asked)
    first_outcomes: dict = None
    checks: dict = field(default_factory=dict)

    @property
    def reinspection(self) -> str:
        return "cool" if self.spec.cooldown_days else "next"

    @property
    def scheduling(self) -> str:
        return "fresh" if self.spec.queue is None else self.spec.queue.tag

    @property
    def key(self) -> str:
        return "|".join([self.group, self.fleet, self.source, self.rule, self.reinspection, self.scheduling,
                         f"B{self.spec.budget}", f"tie-{self.tie_mode}"])

    def capture_share(self) -> np.ndarray:
        return self.capture_sum / max(1, len(self.per_seed))


def seed_summary(events: EventTable, out: dict, wl: dict) -> dict:
    """Event results of one replay on the eligible events, plus its workload."""
    el = events.eligible
    n = int(el.sum())
    share = lambda a: float(a[el].mean()) if n else float("nan")
    full = el & (events.opportunity_days >= events.full_opportunity)
    leads = out["earliest_timely_lead"][el & out["timely"]]
    captured = int(out["timely"][el].sum())
    q = (lambda p: float(np.quantile(leads, p))) if len(leads) else (lambda p: float("nan"))
    return {
        "eligible_events": n, "captured_timely": captured,
        "coverage": share(out["timely"]),
        "coverage_window_alt": share(out["timely_alt_window"]),
        "coverage_lead_long": share(out["timely_long"]),
        "any_prior_capture": share(out["any_prior"]),
        "late_only": share(out["late_only"]),
        "never_inspected_before_failure": share(out["never_before_failure"]),
        "coverage_full_opportunity_events": float(out["timely"][full].mean()) if full.any() else float("nan"),
        "full_opportunity_events": int(full.sum()),
        "earliest_lead_q25": q(0.25), "earliest_lead_median": q(0.5), "earliest_lead_q75": q(0.75),
        "inspections_per_timely_capture": (wl["completed"] / captured) if captured else float("nan"),
        **wl,
    }


def run_pass(data: ReplayData, events: EventTable, proto: Protocol, make_ranker, cells: list[Cell],
             seeds: list[int], make_trackers=None, keep_first: bool = False) -> list[dict]:
    """Replay all `cells` (they share one score and tie mode) for every tie seed. Returns the watchlist
    summaries averaged over seeds (empty when no trackers were asked for)."""
    e = proto.raw["endpoint"]
    tracker_rows: list[list[dict]] = []
    for seed in seeds:
        trackers = make_trackers() if make_trackers else []
        results = run_replay(data, make_ranker(seed), [c.spec for c in cells], trackers)
        for c, res in zip(cells, results):
            out = event_outcomes(events, res, data.n_drives, e["lead_max_sensitivity"], e["lead_long_min"])
            c.per_seed.append(seed_summary(events, out, workload(res)))
            hit = out["timely"][events.eligible].astype(np.float64)
            c.capture_sum = hit if c.capture_sum is None else c.capture_sum + hit
            for k, v in res.checks.items():
                c.checks[k] = c.checks.get(k, 0) + int(v)
            if keep_first and seed == seeds[0]:
                c.first, c.first_outcomes = res, out
        tracker_rows.append([t.summary() for t in trackers])
    if not tracker_rows or not tracker_rows[0]:
        return []
    merged = []
    for j in range(len(tracker_rows[0])):
        keys = tracker_rows[0][j].keys()
        merged.append({k: float(np.mean([r[j][k] for r in tracker_rows])) for k in keys})
    return merged


def cell_row(c: Cell) -> dict:
    """One line of cell_results.csv: mean over tie seeds, plus the lowest and highest seed for coverage."""
    df = pd.DataFrame(c.per_seed)
    row = {"cell": c.key, "group": c.group, "fleet": c.fleet, "score": c.source, "rule": c.rule,
           "reinspection": "7-day cooldown" if c.spec.cooldown_days else "next day",
           "scheduling": "fresh top-K" if c.spec.queue is None else f"queue {c.spec.queue.tag}",
           "budget_per_10000": c.spec.budget, "tie_mode": c.tie_mode, "tie_seeds": len(df)}
    row.update({k: float(df[k].mean()) for k in df.columns})
    row["coverage_min_seed"], row["coverage_max_seed"] = float(df["coverage"].min()), float(df["coverage"].max())
    return row


def policies_for(budget: int, cooldown: int, queues: list) -> list[PolicySpec]:
    out = []
    for cd in (0, cooldown):
        for q in queues:
            out.append(PolicySpec(name=f"B{budget}-{'cool' if cd else 'next'}-{'fresh' if q is None else q.tag}",
                                  budget=budget, cooldown_days=cd, queue=q))
    return out


def contrast_rows(label: str, eight: dict, proto: Protocol) -> list[dict]:
    """RQ1 contrasts for one set of eight cells: {(score, reinspection, scheduling): Cell}."""
    order = list(eight)
    P = np.stack([eight[k].capture_share() for k in order], axis=1)
    b = proto.raw["bootstrap"]
    rows = evaluate_contrasts(P, rq1_contrasts({k: j for j, k in enumerate(order)}), b["reps"], b["seed"], b["level"])
    for r in rows:
        r["set"] = label
    return rows


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--phase4", help="Phase 4 output folder with h30 (and h14, h7) subfolders; builds the replay input")
    ap.add_argument("--input", help="an existing replay_<split>.parquet (or a folder of parquet files)")
    ap.add_argument("--split", required=True, choices=("valid", "test"))
    ap.add_argument("--population", required=True, choices=("sample10", "full_fleet"),
                    help="sample10 = Phase 4 10%% drive sample (exploratory); full_fleet = all eligible HDDs (primary)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--config", help="protocol JSON (default: configs/phase5_protocol_v1.json)")
    ap.add_argument("--final-test-run", action="store_true",
                    help="unlock the test split (once, after the independent review)")
    ap.add_argument("--quick", action="store_true", help="smoke test only; results must not be reported")
    ap.add_argument("--drive-info", help="optional CSV: drive_id plus group columns (model, manufacturer, ...)")
    ap.add_argument("--duckdb-memory", default="4GB")
    a = ap.parse_args(argv)
    if not a.phase4 and not a.input:
        raise SystemExit("Give --phase4 (Phase 4 output folder) or --input (replay table)")

    t0 = time.time()
    proto = load_protocol(a.config)
    out = Path(a.out).expanduser().resolve()
    out.mkdir(parents=True, exist_ok=True)
    first_day, last_day = proto.split_days(a.split)
    lock = out / "TEST_REPLAY_LOCK.json"
    if a.split == "test":
        if not a.final_test_run:
            raise SystemExit("The test split is locked. It is replayed once, after the independent review, "
                             "with --final-test-run.")
        if lock.exists():
            raise SystemExit(f"The test year was already replayed into this folder ({lock}). A second replay is a "
                             f"deviation: log it in docs/phase5_protocol.md section 19 and use a new output folder.")
        lock.write_text(json.dumps({"started_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                    "protocol_sha256": proto.sha256, "quick": a.quick}, indent=2), encoding="utf-8")

    # ------------------------------------------------------------------ 1. replay input
    if a.input:
        in_path = Path(a.input).expanduser().resolve()
        prep = None
    else:
        in_path = out / f"replay_{a.split}.parquet"
        print(f"[1/7] Building the replay input from Phase 4 scores ({a.split}) ...", flush=True)
        prep = prepare_from_phase4(Path(a.phase4).expanduser().resolve(), a.split, in_path, first_day, last_day,
                                   allow_test=a.final_test_run, duckdb_memory=a.duckdb_memory)
    print(f"[2/7] Loading {in_path.name} ...", flush=True)
    data = load_replay_input(in_path, first_day, last_day)
    add_derived_scores(data)
    for needed in (proto.smart, proto.ml):
        if needed not in data.scores:
            raise SystemExit(f"The replay input has no '{needed}' score")
    e = proto.raw["endpoint"]
    events = build_events(data, e["lead_min"], e["lead_max"], e["full_opportunity_days"])
    print(f"  rows {len(data.day):,}; drives {data.n_drives:,}; days {data.n_days}; "
          f"events {events.counts()}", flush=True)
    if events.n_eligible == 0:
        raise SystemExit("No eligible failure event in this replay input; nothing to measure")

    seeds = proto.tie_seeds[:2] if a.quick else proto.tie_seeds
    boot = dict(proto.raw["bootstrap"])
    if a.quick:
        boot["reps"] = 200
        proto.raw["bootstrap"]["reps"] = 200
    B, B_sens, cd = proto.budget_primary, proto.budgets_sensitivity, proto.cooldown_days
    q0, q_alt = proto.queue_primary, proto.queues_sensitivity
    rq2 = proto.raw["rq2"]
    all_cells: list[Cell] = []
    stability_rows: list[dict] = []

    def trackers():
        return [WatchlistTracker(B, m) for m in rq2["watchlist_mult"]]

    def run_source(group, source, values, specs, tie_mode, *, rule="daily", fleet="all", dat=data, evs=events,
                   with_trackers=False, keep_first=False, groups=None):
        cells = [Cell(group=(groups[j] if groups else group), source=source, rule=rule, spec=s,
                      tie_mode=tie_mode, fleet=fleet) for j, s in enumerate(specs)]
        tr = run_pass(dat, evs, proto, lambda seed: ScoreRanker(dat, values, seed, tie_mode), cells, seeds,
                      trackers if with_trackers else None, keep_first)
        all_cells.extend(cells)
        for row in tr:
            stability_rows.append({"score": source, "rule": rule, "tie_mode": tie_mode, **row})
        return cells

    # ------------------------------------------------------------------ 2. RQ1: eight cells, budgets, queue settings
    print(f"[3/7] RQ1 replay: 8 primary cells, budgets {[B] + B_sens}, {len(seeds)} tie seeds ...", flush=True)
    primary: dict = {}
    by_budget: dict = {b: {} for b in B_sens}
    by_queue: dict = {q.tag: {} for q in q_alt}
    for role, source in (("smart", proto.smart), ("ml", proto.ml)):
        specs, groups = policies_for(B, cd, [None, q0]), ["rq1_primary"] * 4
        for b in B_sens:
            specs += policies_for(b, cd, [None, q0])
            groups += ["rq1_budget"] * 4
        specs += policies_for(B, cd, q_alt)
        groups += ["sens_queue"] * (2 * len(q_alt))
        cells = run_source("rq1", source, data.scores[source], specs, proto.tie_mode, with_trackers=True,
                           keep_first=True, groups=groups)
        for c in cells:
            sched = "fresh" if c.spec.queue is None else "queue"
            k = (role, c.reinspection, sched)
            if c.group == "rq1_primary":
                primary[k] = c
            elif c.group == "rq1_budget":
                by_budget[c.spec.budget][k] = c
            else:
                by_queue[c.spec.queue.tag][k] = c
    for tag in by_queue:                         # a queue sensitivity set reuses the primary fresh cells
        for k, c in primary.items():
            if k[2] == "fresh":
                by_queue[tag][k] = c

    # ------------------------------------------------------------------ 3. sensitivity: other scores, tie mode
    print("[4/7] Sensitivity: other score sources and the per-day tie order ...", flush=True)
    by_source: dict = {}
    for source in proto.raw["scores"]["sensitivity"]:
        if source not in data.scores:
            print(f"  note: score '{source}' is not in the replay input; skipped", flush=True)
            continue
        cells = run_source("sens_source", source, data.scores[source], policies_for(B, cd, [None, q0]), proto.tie_mode)
        by_source[source] = {(c.reinspection, "fresh" if c.spec.queue is None else "queue"): c for c in cells}
    alt_mode = proto.raw["tie"]["sensitivity_mode"]
    by_tie: dict = {}
    for role, source in (("smart", proto.smart), ("ml", proto.ml)):
        for c in run_source("sens_tie", source, data.scores[source], policies_for(B, cd, [None, q0]), alt_mode):
            by_tie[(role, c.reinspection, "fresh" if c.spec.queue is None else "queue")] = c

    # ------------------------------------------------------------------ 4. RQ2: persistence rules
    print(f"[5/7] RQ2: {len(rq2['rules'])} persistence rules x 2 scores x 2 reinspection rules ...", flush=True)
    rq2_cells: list[Cell] = []
    for source in (proto.smart, proto.ml):
        for rule in rq2["rules"]:
            if rule["kind"] == "daily":           # the daily rule is the primary fresh Top-K replay itself
                role = "smart" if source == proto.smart else "ml"
                rq2_cells += [primary[(role, "next", "fresh")], primary[(role, "cool", "fresh")]]
                continue
            pr = persistence_priority(data, data.scores[source], rule, rq2["history_reset_gap_days"])
            rq2_cells += run_source("rq2", source, pr, policies_for(B, cd, [None]), proto.tie_mode,
                                    rule=rule["name"], with_trackers=True)

    # ------------------------------------------------------------------ 5. RQ3: horizons
    print("[6/7] RQ3: horizons, urgency-first, prediction quality ...", flush=True)
    rq3_cells: list[Cell] = [primary[("ml", "next", "fresh")], primary[("ml", "cool", "fresh")]]
    rq3_scores = [s for s in proto.raw["scores"]["rq3"] if s in data.scores]
    for source in rq3_scores:
        if source != proto.ml:
            rq3_cells += run_source("rq3", source, data.scores[source], policies_for(B, cd, [None]), proto.tie_mode)
    if len(rq3_scores) == 3:
        cells = [Cell(group="rq3", source="urgency_first_7_14_30", rule="daily", spec=s, tie_mode=proto.tie_mode)
                 for s in policies_for(B, cd, [None])]
        run_pass(data, events, proto, lambda seed: InterleaveRanker(
            [ScoreRanker(data, data.scores[s], seed, proto.tie_mode) for s in ("ml_h7", "ml_h14", "ml_h30")]),
            cells, seeds)
        all_cells.extend(cells)
        rq3_cells += cells
    pred_rows = horizon_metrics(data, e["lead_min"], e["lead_max"]) if 30 in data.labels else []

    # ------------------------------------------------------------------ 6. fleet-competition sensitivity
    halves: list[dict] = []
    fs = proto.raw["fleet_subsample"]
    if not a.quick:
        print(f"[6/7] Fleet-competition sensitivity: {fs['reps']} random halves of the drives ...", flush=True)
        for rep in range(fs["reps"]):
            sub = data.subset_drives(in_subsample(data.drive_key, rep, fs["fraction"]))
            ev_sub = build_events(sub, e["lead_min"], e["lead_max"], e["full_opportunity_days"])
            eight = {}
            for role, source in (("smart", proto.smart), ("ml", proto.ml)):
                for c in run_source("fleet_half", source, sub.scores[source], policies_for(B, cd, [None, q0]),
                                    proto.tie_mode, fleet=f"half{rep}", dat=sub, evs=ev_sub):
                    eight[(role, c.reinspection, "fresh" if c.spec.queue is None else "queue")] = c
            halves.append(eight)

    # ------------------------------------------------------------------ 7. tables
    print("[7/7] Writing tables and checks ...", flush=True)
    rows = [cell_row(c) for c in all_cells]
    res = pd.DataFrame(rows)
    full_cells = [c for c in all_cells if c.fleet == "all"]
    P = np.stack([c.capture_share() for c in full_cells], axis=1)
    ci = coverage_intervals(P, boot["reps"], boot["seed"], boot["level"])
    ci_map = {c.key: ci[j] for j, c in enumerate(full_cells)}
    res["coverage_ci_low"] = [ci_map.get(k, (np.nan, np.nan))[0] for k in res["cell"]]
    res["coverage_ci_high"] = [ci_map.get(k, (np.nan, np.nan))[1] for k in res["cell"]]
    e_name = {c.key: E_CELLS[k] for k, c in primary.items()}
    res.insert(1, "e_cell", [e_name.get(k, "") for k in res["cell"]])
    front = ["cell", "e_cell", "group", "fleet", "score", "rule", "reinspection", "scheduling", "budget_per_10000",
             "tie_mode", "tie_seeds", "eligible_events", "coverage", "coverage_ci_low", "coverage_ci_high",
             "coverage_min_seed", "coverage_max_seed"]
    res = res[front + [c for c in res.columns if c not in front]]
    res.to_csv(out / "cell_results.csv", index=False)

    con = contrast_rows(f"primary_B{B}", primary, proto)
    for b in B_sens:
        con += contrast_rows(f"budget_B{b}", by_budget[b], proto)
    for tag, eight in by_queue.items():
        con += contrast_rows(f"queue_{tag}", eight, proto)
    con += contrast_rows(f"tie_{alt_mode}", by_tie, proto)
    for source, four in by_source.items():
        role = "ml" if source.startswith(("gbm", "ml")) else "smart"
        if source == "random":
            continue
        eight = {k: c for k, c in primary.items() if k[0] != role}
        eight.update({(role, r, g): c for (r, g), c in four.items()})
        con += contrast_rows(f"score_{source}", eight, proto)
    for rep, eight in enumerate(halves):
        con += contrast_rows(f"fleet_half{rep}", eight, proto)
    con_df = pd.DataFrame(con)
    con_df = con_df[["set"] + [c for c in con_df.columns if c != "set"]]
    con_df.to_csv(out / "rq1_contrasts.csv", index=False)

    # RQ2 table: stability of the watchlist joined to the timeliness of the matching replays
    stab = pd.DataFrame(stability_rows)
    rq2_rows = []
    for c in rq2_cells:
        r = cell_row(c)
        base = {"score": c.source, "rule": c.rule, "reinspection": r["reinspection"], "coverage": r["coverage"],
                "coverage_ci_low": ci_map[c.key][0], "coverage_ci_high": ci_map[c.key][1],
                "any_prior_capture": r["any_prior_capture"], "late_only": r["late_only"],
                "earliest_lead_median": r["earliest_lead_median"], "repeat_share": r["repeat_share"],
                "distinct_drives_inspected": r["distinct_drives_inspected"]}
        for m in rq2["watchlist_mult"]:
            s = stab[(stab["score"] == c.source) & (stab["rule"] == c.rule) & (stab["tie_mode"] == c.tie_mode)
                     & (stab["watchlist_mult"] == m)]
            if len(s):
                s = s.iloc[0]
                base.update({f"list{m}K_mean_jaccard": s["mean_jaccard"], f"list{m}K_turnover": s["turnover"],
                             f"list{m}K_entries_per_day": s["entries_per_day"],
                             f"list{m}K_mean_days_on_list": s["mean_days_on_list"],
                             f"list{m}K_exits_outranked": s["exits_outranked"],
                             f"list{m}K_exits_drive_not_observed": s["exits_drive_not_observed"],
                             f"list{m}K_mean_size": s["mean_list_size"]})
        rq2_rows.append(base)
    rq2_df = pd.DataFrame(rq2_rows)
    if len(rq2_df):          # non-dominated settings: no other rule has both more coverage and less turnover
        rq2_df["pareto_coverage_vs_turnover"] = False
        for _, g in rq2_df.groupby(["score", "reinspection"]):
            for idx, r in g.iterrows():
                dominated = ((g["coverage"] >= r["coverage"]) & (g["list1K_turnover"] <= r["list1K_turnover"])
                             & ((g["coverage"] > r["coverage"]) | (g["list1K_turnover"] < r["list1K_turnover"]))).any()
                rq2_df.loc[idx, "pareto_coverage_vs_turnover"] = not dominated
    rq2_df.to_csv(out / "rq2_stability.csv", index=False)

    rq3_rows = [{"table": "budgeted_coverage", **{k: v for k, v in cell_row(c).items() if k in (
        "score", "reinspection", "coverage", "any_prior_capture", "late_only", "earliest_lead_median",
        "repeat_share", "distinct_drives_inspected", "inspection_precision_30d")},
        "coverage_ci_low": ci_map[c.key][0], "coverage_ci_high": ci_map[c.key][1]} for c in rq3_cells]
    rq3_rows += [{"table": "prediction_quality", **r} for r in pred_rows]
    pd.DataFrame(rq3_rows).to_csv(out / "rq3_horizons.csv", index=False)

    # event table and logs of the eight primary cells (tie seed of the first replay)
    el = events.eligible
    ev = pd.DataFrame({"drive_id": data.drive_ids[events.drive], "fail_date": [to_date(d) for d in events.fail_day],
                       "opportunity_days": events.opportunity_days,
                       "status": np.array(["eligible", "burn_in", "no_opportunity"])[events.reason]})
    for k, c in primary.items():
        col = np.full(len(ev), np.nan)
        col[el] = c.capture_share()
        ev[f"{E_CELLS[k]}_timely_share"] = col
        ev[f"{E_CELLS[k]}_earliest_lead_seed{seeds[0]}"] = c.first_outcomes["earliest_timely_lead"]
    ev.to_parquet(out / "event_coverage.parquet", index=False)
    daily = None
    for k, c in primary.items():
        name, r = E_CELLS[k], c.first
        f = data.fail_day[r.drive].astype(np.int64)
        pd.DataFrame({"decision_date": [to_date(d) for d in r.decision_day],
                      "service_date": [to_date(d) for d in r.service_day], "drive_id": data.drive_ids[r.drive],
                      "completed": r.completed, "label_30d_at_service": r.label30, "queue_wait_days": r.wait_days,
                      "lead_days_to_failure": np.where(f >= 0, f - r.service_day, -1)}
                     ).to_parquet(out / f"service_log_{name}.parquet", index=False)
        if c.spec.queue is not None:
            led = r.queue_ledger
            pd.DataFrame({"drive_id": data.drive_ids[led["drive"]],
                          "admit_date": [to_date(d) for d in led["admit_day"]],
                          "exit_date": [to_date(d) for d in led["exit_day"]],
                          "exit_reason": np.array(["scheduled", "expired", "left_at_end"])[led["exit_reason"]]}
                         ).to_parquet(out / f"queue_ledger_{name}.parquet", index=False)
        if daily is None:
            daily = pd.DataFrame({"date": [to_date(data.first_day + i) for i in range(data.n_days)],
                                  "observed_drives": r.daily["observed"], "capacity_K": r.daily["capacity"]})
        for col in ("scheduled", "completed", "cancelled"):
            daily[f"{name}_{col}"] = r.daily[col]
        if c.spec.queue is not None:
            daily[f"{name}_queue_length"] = r.daily["queue_length"]
    daily.to_csv(out / "daily_capacity.csv", index=False)

    if a.drive_info:
        info = pd.read_csv(a.drive_info, dtype=str).drop_duplicates("drive_id").set_index("drive_id")
        evx = ev[ev["status"] == "eligible"].join(info, on="drive_id")
        sub_rows = []
        for gcol in info.columns:
            for val, g in evx.groupby(gcol, dropna=False):
                row = {"group": gcol, "value": val, "eligible_events": len(g),
                       "enough_events_for_a_claim": len(g) >= proto.raw["min_events_for_subgroup_claim"]}
                row.update({n: float(g[f"{n}_timely_share"].mean()) for n in sorted(E_CELLS.values())})
                sub_rows.append(row)
        pd.DataFrame(sub_rows).to_csv(out / "subgroups.csv", index=False)

    # ------------------------------------------------------------------ acceptance checks (protocol section 16)
    problems: dict = {}
    for c in all_cells:
        for k, v in c.checks.items():
            problems[k] = problems.get(k, 0) + v
    half = data.first_day + data.n_days // 2          # a replay cut in the middle must equal the full one up to there
    prefix_ok = True
    for k in (("ml", "cool", "fresh"), ("smart", "cool", "queue")):
        c = primary[k]
        src = proto.ml if k[0] == "ml" else proto.smart
        cut = run_replay(data, ScoreRanker(data, data.scores[src], seeds[0], proto.tie_mode), [c.spec],
                         stop_after_day=half)[0]
        m = c.first.decision_day <= half
        prefix_ok &= (np.array_equal(cut.drive, c.first.drive[m]) and np.array_equal(cut.completed, c.first.completed[m]))
    same_events = len({r["eligible_events"] for r in rows if r["fleet"] == "all"}) == 1
    cov_ok = bool(((res["coverage"] >= 0) & (res["coverage"] <= 1)).all())
    checks = {
        "engine_rule_violations": problems,
        "engine_rule_violations_total": int(sum(problems.values())),
        "eligible_event_set_identical_in_every_cell": bool(same_events),
        "coverage_between_0_and_1": cov_ok,
        "cut_replay_equals_full_replay_prefix": bool(prefix_ok),
        "input_no_rows_on_or_after_failure_day": True,     # load_replay_input stops otherwise
        "input_no_duplicate_drive_days": True,             # load_replay_input stops otherwise
        "events": events.counts(),
        "all_passed": bool(sum(problems.values()) == 0 and same_events and cov_ok and prefix_ok),
    }
    (out / "checks.json").write_text(json.dumps(checks, indent=2), encoding="utf-8")

    here = Path(__file__).resolve().parent
    info = {
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "what_this_is": "Decision-support simulation of inspection slots. Simulated inspections only; "
                        "no claim that failures are prevented.",
        "split": a.split, "population": a.population,
        "exploratory": a.population != "full_fleet" or a.quick,
        "quick_smoke_test_do_not_report": a.quick,
        "protocol_version": proto.raw["protocol_version"], "protocol_sha256": proto.sha256,
        "protocol": proto.raw, "tie_seeds_used": seeds, "bootstrap_reps_used": boot["reps"],
        "input": str(in_path), "input_sha256": sha256_file(in_path) if in_path.is_file() else None,
        "prepare_manifest": prep,
        "rows": int(len(data.day)), "drives": int(data.n_drives), "calendar_days": int(data.n_days),
        "first_day": str(first_day), "last_day": str(last_day),
        "code_sha256": {p.name: sha256_file(p) for p in sorted(here.glob("*.py"))},
        "python": sys.version.split()[0], "numpy": np.__version__, "pandas": pd.__version__,
        "machine": platform.node(), "os": platform.platform(),
        "total_seconds": round(time.time() - t0, 1),
    }
    (out / "run_info.json").write_text(json.dumps(info, indent=2, default=str), encoding="utf-8")
    if a.split == "test":
        lock.write_text(json.dumps({"finished_utc": info["created_utc"], "protocol_sha256": proto.sha256,
                                    "quick": a.quick, "all_checks_passed": checks["all_passed"]}, indent=2),
                        encoding="utf-8")

    show = res[res["group"] == "rq1_primary"][["e_cell", "score", "reinspection", "scheduling", "coverage",
                                                "coverage_ci_low", "coverage_ci_high", "repeat_share",
                                                "distinct_drives_inspected"]].sort_values("e_cell")
    tag = "EXPLORATORY (10% sample)" if a.population != "full_fleet" else "full fleet"
    print(f"\nRQ1 primary cells, {a.split}, {tag}, B={B} per 10,000, {events.n_eligible} eligible events:")
    print(show.to_string(index=False, float_format=lambda v: f"{v:.4f}"))
    print("\nChecks passed:" if checks["all_passed"] else "\nCHECKS FAILED:", json.dumps(
        {k: v for k, v in checks.items() if k not in ("events", "engine_rule_violations")}))
    if a.quick:
        print("QUICK smoke test: do not report these numbers.")
    print(f"Done in {info['total_seconds']} s. Output: {out}")
    return 0 if checks["all_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
