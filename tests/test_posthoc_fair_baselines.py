"""Amendment A1 (post hoc): make quotas, within-make percentile, side-file build, and the reproduction gate.

The end-to-end test builds a small 2024 fleet, runs the FROZEN Phase 5 command on it (that output plays the role
of the frozen archive), then runs the post hoc replay on the same input: the frozen SMART and ML cells, events and
contrasts must come out exactly, which is the gate the lab PC run has to pass.
"""
from __future__ import annotations

import json
import zipfile
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from drivepriority.posthoc import fair_baselines as fb
from drivepriority.posthoc.makes import MAKES, QuotaRanker, make_of_drives, make_of_model, within_make_percentile
from drivepriority.simulation import run_phase5
from drivepriority.simulation.config import QueueConfig
from drivepriority.simulation.engine import PolicySpec, run_replay
from drivepriority.simulation.ranking import ScoreRanker
from drivepriority.simulation.replay_input import load_replay_input

MODELS = ["ST4000DM000", "HGST HMS5C4040BLE640", "TOSHIBA MG07ACA14TA", "WDC WUH721816ALE6L4", "WUH721816ALE6L4",
          "Hitachi HDS5C3030ALA630"]
D0 = date(2024, 1, 1)


def fleet(n_drives=2400, n_days=366, seed=7, d0=D0):
    """A 2024 fleet with four makes, SMART counters (187/188 missing outside Seagate), failures and a useful ML
    score. Vectorised so the full year stays quick."""
    rng = np.random.default_rng(seed)
    makes = rng.choice(4, size=n_drives, p=[0.4, 0.15, 0.3, 0.15])
    names = np.array(["ST12000NM0008", "HGST HUH721212ALN604", "TOSHIBA MG08ACA16TA", "WDC WUH721816ALE6L4"])
    ids = np.array([f"SN{i:05d}|{names[m]}" for i, m in enumerate(makes)])
    start = rng.integers(0, 200, n_drives)
    stop = np.minimum(n_days - 1, start + rng.integers(150, 400, n_drives))
    fail = np.where(rng.random(n_drives) < 0.06, rng.integers(40, n_days, n_drives), -1)
    last = np.where((fail >= 0) & (fail - 1 < stop), fail - 1, stop)
    fail = np.where(fail > stop + 1, -1, fail)                 # a failure only right after the last record
    risk = rng.random(n_drives)
    lens = np.maximum(0, last - start + 1)
    drv = np.repeat(np.arange(n_drives), lens)
    day = np.concatenate([np.arange(a, b + 1) for a, b in zip(start, last) if b >= a])
    keep = rng.random(len(day)) > 0.03                         # some missing days
    drv, day = drv[keep], day[keep]
    near = (fail[drv] >= 0) & (fail[drv] - day <= 30)
    lam = 0.05 + 1.5 * near + 0.3 * risk[drv]
    df = pd.DataFrame({"drive_id": ids[drv], "date": [d0 + timedelta(days=int(d)) for d in day],
                       "fail_date": [d0 + timedelta(days=int(f)) if f >= 0 else None for f in fail[drv]],
                       "model": names[makes[drv]]})
    for x in (5, 187, 188, 197, 198):
        v = rng.poisson(lam).astype(float)
        if x in (187, 188):
            v[makes[drv] != 0] = np.nan
        if x == 197:
            v[rng.random(len(v)) < 0.02] = np.nan
        df[f"smart_{x}_raw"] = v
    df["ml_h30"] = np.clip(0.02 + 0.6 * near + 0.2 * risk[drv] + rng.normal(0, 0.1, len(drv)), 0, 1)
    df["smart_count"] = sum((df[f"smart_{x}_raw"].fillna(0) > 0).astype(int) for x in (5, 187, 188, 197, 198)).astype("int8")
    df["smart_sum"] = sum(df[f"smart_{x}_raw"].fillna(0) for x in (5, 187, 188, 197, 198)).astype("float64")
    df["label_30d"] = pd.array([None] * len(df), dtype="Int8")
    return df.sort_values(["date", "drive_id"]).reset_index(drop=True)


def test_preflight_times_2024_and_only_checks_the_test_period(tmp_path):
    df = fleet(n_drives=1200)
    rep, feat = write_inputs(tmp_path, df)
    out = tmp_path / "work"
    with pytest.raises(SystemExit, match="2024 preflight first"):
        dft = fleet(n_drives=600, n_days=546, d0=date(2025, 1, 1))
        rt, ft = write_inputs(tmp_path, dft, "test")
        fb.main(["preflight", "--replay", str(rt), "--features", str(ft), "--split", "test", "--out", str(out),
                 "--memory-limit", "1GB"])
    assert fb.main(["preflight", "--replay", str(rep), "--features", str(feat), "--split", "valid", "--out", str(out),
                    "--memory-limit", "1GB"]) == 0
    assert fb.main(["preflight", "--replay", str(rt), "--features", str(ft), "--split", "test", "--out", str(out),
                    "--memory-limit", "1GB"]) == 0
    r = json.loads((out / "preflight_test" / "preflight.json").read_text())
    assert r["ready"] and "one_month_replay_ok" not in r and r["estimate_minutes"]["total"] >= 0
    assert not (out / "preflight_test" / "results_test").exists()      # no test-period replay in the preflight


def write_inputs(tmp: Path, df: pd.DataFrame, split: str = "valid"):
    rep, feat = tmp / f"replay_{split}", tmp / f"features_{split}"
    rep.mkdir()
    feat.mkdir()
    cols = ["drive_id", "date", "fail_date", "label_30d", "smart_count", "smart_sum", "ml_h30"]
    month = pd.to_datetime(df.date).dt.strftime("%Y-%m")
    for m, g in df.groupby(month):
        g[cols].to_parquet(rep / f"part_{m}.parquet", index=False)
    q = pd.to_datetime(df.date).dt.year.astype(str) + "Q" + pd.to_datetime(df.date).dt.quarter.astype(str)
    for k, g in df.groupby(q):
        g[["drive_id", "date", "model"] + [f"smart_{x}_raw" for x in (5, 187, 188, 197, 198)]].sample(
            frac=1.0, random_state=1).to_parquet(feat / f"features_{split}_{k}.parquet", index=False)
    return rep, feat


# ---------------------------------------------------------------------------------------------- unit checks

def test_make_rule():
    assert [MAKES[make_of_model(m)] for m in MODELS] == ["Seagate", "HGST", "Toshiba", "WDC", "WDC", "HGST"]
    assert MAKES[make_of_model("Micron 5300")] == "Other"


def small_data(tmp_path, n=300, days=20, seed=3):
    rng = np.random.default_rng(seed)
    ids = np.array([f"S{i}|{MODELS[i % 4]}" for i in range(n)])
    recs = [{"drive_id": ids[i], "date": D0 + timedelta(days=d), "fail_date": None, "smart_count": int(rng.integers(0, 4)),
             "ml_h30": float(rng.random())} for d in range(days) for i in range(n) if rng.random() < 0.9]
    df = pd.DataFrame(recs).sort_values(["date", "drive_id"])
    df["smart_count"] = df.smart_count.astype("int8")
    df.to_parquet(tmp_path / "r.parquet", index=False)
    return load_replay_input(tmp_path / "r.parquet", D0, D0 + timedelta(days=days - 1))


def test_within_make_percentile_matches_brute_force(tmp_path):
    data = small_data(tmp_path)
    mk = make_of_drives(data.drive_ids)
    got = within_make_percentile(data, data.scores["smart_count"], mk)
    df = pd.DataFrame({"day": data.day, "make": mk[data.drive], "c": data.scores["smart_count"]})
    want = [((g.c < r.c).sum() / len(g)) for (_, r), g in
            zip(df.iterrows(), (df[(df.day == r.day) & (df.make == r.make)] for _, r in df.iterrows()))]
    assert np.allclose(got, want)


def test_quota_order_is_proportional_and_consistent(tmp_path):
    data = small_data(tmp_path)
    mk = make_of_drives(data.drive_ids)
    for tie in ("drive", "day"):
        r = QuotaRanker(data, data.scores["ml_h30"], mk, seed=2, tie_mode=tie)
        for i in range(data.n_days):
            order, complete = r.top(i, 10**9)
            assert complete
            rows = data.rows_of_day(i)
            n_m = np.bincount(mk[data.drive[rows]], minlength=len(MAKES))
            share = n_m / n_m.sum()
            n_present = int((n_m > 0).sum())
            for K in (1, 5, 17, 40):
                got = np.bincount(mk[order[:K]], minlength=len(MAKES))
                # the provable Webster bound (not "within one place", which can fail: see the next test)
                assert np.all(np.abs(got - K * share) <= 0.5 + share * n_present / 2 + 1e-9)
                # and the prefix is the Webster divisor allocation: rounding p/d for one common divisor d
                lo = max(((got[m] - 0.5) / n_m[m] for m in range(len(MAKES)) if n_m[m] and got[m]), default=0)
                hi = min(((got[m] + 0.5) / n_m[m] for m in range(len(MAKES)) if n_m[m]), default=np.inf)
                assert lo <= hi + 1e-12
            # inside a make the order follows the score
            for m in range(len(MAKES)):
                d = order[mk[order] == m]
                pos = {int(x): j for j, x in enumerate(data.drive[rows])}
                v = data.scores["ml_h30"][rows][[pos[int(x)] for x in d]]
                assert np.all(np.diff(v) <= 0)
            # sort_keys gives the same order as top()
            k1, k2 = r.sort_keys(np.arange(rows.start, rows.stop), data.first_day + i)
            assert np.array_equal(data.drive[rows][np.lexsort((k2, k1))], order)


def test_quota_can_miss_the_exact_share_by_more_than_one_place(tmp_path):
    """Counterexample from the V4 review (20 Nov 2026): 73/9/9/9 drives and K = 7 give 4/1/1/1 (exact 5.11)."""
    models = ["ST4000DM000"] * 73 + ["HGST HMS5C4040BLE640"] * 9 + ["TOSHIBA MG07ACA14TA"] * 9 + ["WDC WUH721816ALE6L4"] * 9
    rng = np.random.default_rng(0)
    pd.DataFrame({"drive_id": [f"S{i}|{m}" for i, m in enumerate(models)], "date": D0, "fail_date": None,
                  "smart_count": np.zeros(len(models), dtype="int8"), "ml_h30": rng.random(len(models))}
                 ).to_parquet(tmp_path / "w.parquet", index=False)
    data = load_replay_input(tmp_path / "w.parquet", D0, D0)
    mk = make_of_drives(data.drive_ids)
    order, _ = QuotaRanker(data, data.scores["ml_h30"], mk, seed=0, tie_mode="drive").top(0, 7)
    got = dict(zip(MAKES, np.bincount(mk[order], minlength=len(MAKES)).tolist()))
    assert got == {"Seagate": 4, "HGST": 1, "Toshiba": 1, "WDC": 1, "Other": 0}
    assert abs(4 - 7 * 0.73) > 1


def test_quota_with_one_make_equals_the_plain_ranking(tmp_path):
    data = small_data(tmp_path)
    one = np.zeros(data.n_drives, dtype=np.int8)
    for tie in ("drive", "day"):
        q, s = QuotaRanker(data, data.scores["ml_h30"], one, 5, tie), ScoreRanker(data, data.scores["ml_h30"], 5, tie)
        for i in range(data.n_days):
            assert np.array_equal(q.top(i, 10**9)[0], s.top(i, 10**9)[0])


def test_quota_ranker_in_the_engine_equals_scoreranker_on_the_quota_key(tmp_path):
    """The frozen engine with QuotaRanker = the frozen engine with ScoreRanker on the per-row quota key."""
    data = small_data(tmp_path, n=1500, days=25)
    mk = make_of_drives(data.drive_ids)
    q = QuotaRanker(data, data.scores["smart_count"], mk, seed=1, tie_mode="day")
    key = np.empty(len(data.day))
    for i in range(data.n_days):
        rows = data.rows_of_day(i)
        key[rows] = q.sort_keys(np.arange(rows.start, rows.stop), data.first_day + i)[0]
    specs = [PolicySpec("a", 20), PolicySpec("b", 20, cooldown_days=7),
             PolicySpec("c", 20, queue=QueueConfig(2, 3, 3)), PolicySpec("d", 50, cooldown_days=7, queue=QueueConfig(2, 3, 3))]
    ra = run_replay(data, QuotaRanker(data, data.scores["smart_count"], mk, 1, "day"), specs)
    rb = run_replay(data, ScoreRanker(data, -key, 1, "day"), specs)
    for x, y in zip(ra, rb):
        assert np.array_equal(x.drive, y.drive) and np.array_equal(x.service_day, y.service_day)
        assert sum(x.checks.values()) == 0


# ---------------------------------------------------------------------------------------------- build

class Args:
    def __init__(self, **kw):
        self.__dict__.update(kw)


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("a1")
    df = fleet()
    rep, feat = write_inputs(tmp, df)
    return tmp, df, rep, feat


def test_build_matches_every_row_and_counts(world):
    tmp, df, rep, feat = world
    out = tmp / "work_build"
    man = fb.cmd_build(Args(replay=str(rep), features=str(feat), split="valid", out=str(out), memory_limit="1GB"),
                       only=[f"2024-{m:02d}" for m in range(1, 13)])
    assert man["all_rows_matched_and_count_reproduced"] and man["rows"] == len(df)
    side = pd.read_parquet(out / "extras_valid")
    want = sum((df[f"smart_{x}_raw"].fillna(0) > 0).astype(int) for x in (5, 197, 198))
    assert np.array_equal(side.smart_count3.to_numpy(), want.to_numpy())
    av = pd.read_csv(out / "smart_availability_by_make_valid.csv").set_index("make")
    assert av.loc["Toshiba", "reported_share_187"] == 0 and av.loc["Seagate", "reported_share_187"] == 1


def test_build_stops_on_a_changed_count(world, tmp_path):
    tmp, df, rep, feat = world
    bad = tmp_path / "rep"
    bad.mkdir()
    for p in rep.glob("*.parquet"):
        t = pd.read_parquet(p)
        if p.name == "part_2024-02.parquet":
            t.loc[t.index[5], "smart_count"] = (t.loc[t.index[5], "smart_count"] + 1) % 5
        t.to_parquet(bad / p.name, index=False)
    with pytest.raises(SystemExit, match="differs from smart_count"):
        fb.cmd_build(Args(replay=str(bad), features=str(feat), split="valid", out=str(tmp_path / "o"),
                          memory_limit="1GB"), only=["2024-01", "2024-02"])


# ---------------------------------------------------------------------------------------------- end to end

def test_post_hoc_replay_reproduces_the_frozen_run(world, monkeypatch):
    tmp, df, rep, feat = world
    frozen = tmp / "frozen"
    assert run_phase5.main(["--input", str(rep), "--split", "valid", "--population", "full_fleet",
                            "--out", str(frozen)]) == 0
    arch = tmp / "frozen.zip"
    with zipfile.ZipFile(arch, "w") as z:
        for f in frozen.iterdir():
            if f.is_file():
                z.write(f, f.name)
    monkeypatch.setattr(fb, "REGISTER", tmp / "register.json")
    work = tmp / "work"
    fb.cmd_build(Args(replay=str(rep), features=str(feat), split="valid", out=str(work), memory_limit="1GB"),
                 only=[f"2024-{m:02d}" for m in range(1, 13)])
    info = fb.cmd_replay(Args(replay=str(rep), split="valid", out=str(work), post_hoc_test=False,
                              frozen_archive=str(arch), seeds=None, budgets=None, scores=None, memory_limit="1GB"))
    res = work / "results_valid"
    repro = json.loads((res / "reproduction.json").read_text())
    assert repro["passed"], repro["problems"][:5]
    assert repro["values_compared"] >= 32 * 9 + 8 + 48
    assert info["full_run"] and info["engine_rule_violations"] == 0
    cells = pd.read_csv(res / "cells.csv")
    assert len(cells) == 7 * 4 * 3 * 2 and set(cells.label) == {fb.LABEL}
    q = cells[(cells.score == "ml_h30+quota") & (cells.budget_per_10000 >= 20)]   # at B10 here K is 1 slot a day
    assert (q.share_completed_Seagate < 0.6).all()           # quotas spread the slots (fleet is 40% Seagate)
    con = pd.read_csv(res / "contrasts.csv")
    assert set(con.contrast) == {c for c, _, _ in fb.CONTRASTS}
    assert (res.parent / "posthoc_A1_valid_results.zip").is_file()
    reg = json.loads((tmp / "register.json").read_text())
    assert reg[-1]["finished_utc"] and reg[-1]["reproduction_passed"]
    # a second full run into the same folder is refused
    with pytest.raises(SystemExit, match="already holds results"):
        fb.cmd_replay(Args(replay=str(rep), split="valid", out=str(work), post_hoc_test=False,
                           frozen_archive=str(arch), seeds=None, budgets=None, scores=None, memory_limit="1GB"))
    # a changed frozen number makes the gate fail
    z = zipfile.ZipFile(arch)
    fz = pd.read_csv(z.open("cell_results.csv"))
    fz.loc[fz.e_cell == "E1", "coverage"] += 0.001
    bad = tmp / "bad.zip"
    with zipfile.ZipFile(bad, "w") as zb:
        for n in z.namelist():
            zb.writestr(n, fz.to_csv(index=False) if n == "cell_results.csv" else z.read(n))
    ev = pd.read_parquet(res / "event_capture.parquet")
    r2 = fb.reproduction(cells, ev, con, zipfile.ZipFile(bad))
    assert not r2["passed"] and any("coverage" in p for p in r2["problems"])


def test_side_files_with_swapped_drives_are_refused(world, tmp_path):
    """Two drives of one day with the same five-indicator count swapped in the side file: day and smart_count
    still align, so only the drive_id check (added 20 Nov 2026) can stop it."""
    tmp, df, rep, feat = world
    out = tmp_path / "w"
    fb.cmd_build(Args(replay=str(rep), features=str(feat), split="valid", out=str(out), memory_limit="1GB"),
                 only=[f"2024-{m:02d}" for m in range(1, 13)])
    side = out / "extras_valid"
    data, c3 = fb.load_inputs(rep, side, D0, date(2024, 12, 31), None)      # untouched side files load
    assert len(c3) == len(data.day)
    p = side / "part_2024-03.parquet"
    t = pd.read_parquet(p)
    swapped = False
    for _, g in t.groupby(["date", "smart_count"]):
        diff = g[g.smart_count3 != g.smart_count3.iloc[0]]
        if len(diff):
            perm = np.arange(len(t))
            i, j = t.index.get_loc(g.index[0]), t.index.get_loc(diff.index[0])
            perm[i], perm[j] = j, i
            swapped = True
            break
    assert swapped
    t = t.iloc[perm].reset_index(drop=True)
    t.to_parquet(p, index=False)
    with pytest.raises(SystemExit, match="drive_id differs"):
        fb.load_inputs(rep, side, D0, date(2024, 12, 31), None)


def test_test_period_is_guarded(world, monkeypatch, tmp_path):
    tmp, df, rep, feat = world
    monkeypatch.setattr(fb, "REGISTER", tmp_path / "register.json")
    a = Args(replay=str(rep), split="test", out=str(tmp_path / "w"), post_hoc_test=False, frozen_archive=None,
             seeds=None, budgets=None, scores=None, memory_limit="1GB")
    with pytest.raises(SystemExit, match="--post-hoc-test"):
        fb.cmd_replay(a)
    a.post_hoc_test, a.seeds = True, ["0"]
    with pytest.raises(SystemExit, match="partial run"):
        fb.cmd_replay(a)
    a.seeds = None
    monkeypatch.setattr(fb, "code_state", lambda: {"commit": "abc", "clean": True, "reason": "", "code_sha256": {}})
    with pytest.raises(SystemExit, match="no finished 2024 run"):
        fb.cmd_replay(a)
    fb.write_register([{"split": "valid", "commit": "abc", "clean": True, "full_run": True, "finished_utc": "x",
                        "reproduction_passed": True},
                       {"split": "test", "commit": "abc", "finished_utc": "y", "out": "earlier"}])
    with pytest.raises(SystemExit, match="already run"):
        fb.cmd_replay(a)


def test_summarise_wording_rule(tmp_path):
    rows = []
    for split, shift in (("valid", 0.02), ("test", 0.03)):
        for k in range(1, 6):
            for tie in ("drive", "day"):
                for r in ("next", "cool"):
                    for g in ("fresh", "queue"):
                        est = shift if not (k == 3 and r == "cool") else -0.01
                        rows.append({"split": split, "contrast": f"M1-S{k}", "tie": tie, "budget": 20, "reinspection": r,
                                     "scheduling": g, "estimate": est, "ci_low": est - 0.01, "ci_high": est + 0.01})
    df = pd.DataFrame(rows)
    for s in ("valid", "test"):
        (tmp_path / s).mkdir()
        df[df.split == s].to_csv(tmp_path / s / "contrasts.csv", index=False)
    fb.cmd_summarise(Args(valid=str(tmp_path / "valid"), test=str(tmp_path / "test"), out=str(tmp_path / "w.csv")))
    w = pd.read_csv(tmp_path / "w.csv")
    ml = w[w.claim == "ML better"].set_index(["reinspection", "scheduling"]).allowed
    assert ml[("next", "fresh")] and ml[("next", "queue")]
    assert not ml[("cool", "fresh")] and not ml[("cool", "queue")]
    # --out may also be a folder (it was a file-only argument before 20 Nov 2026)
    fb.cmd_summarise(Args(valid=str(tmp_path / "valid"), test=str(tmp_path / "test"), out=str(tmp_path / "outdir")))
    assert (tmp_path / "outdir" / "wording_rule.csv").read_text() == (tmp_path / "w.csv").read_text()


def test_parallel_seeds_give_identical_results(world):
    from drivepriority.simulation.config import load_protocol
    from drivepriority.simulation.events import build_events
    tmp, df, rep, feat = world
    data = load_replay_input(rep, D0, date(2024, 12, 31))
    proto = load_protocol()
    e = proto.raw["endpoint"]
    events = build_events(data, e["lead_min"], e["lead_max"], e["full_opportunity_days"])
    assert events.n_eligible > 50
    mk = make_of_drives(data.drive_ids)
    for quota in (False, True):
        a = fb.run_block(data, events, proto, "x", data.scores["ml_h30"], quota, mk, "day", [20, 50], [0, 1, 2], 1)
        b = fb.run_block(data, events, proto, "x", data.scores["ml_h30"], quota, mk, "day", [20, 50], [0, 1, 2], 3)
        assert a["checks"] == b["checks"]
        for k in a["rows"]:
            assert pd.DataFrame(a["rows"][k]).equals(pd.DataFrame(b["rows"][k]))
            assert np.array_equal(a["capture_sum"][k], b["capture_sum"][k])
