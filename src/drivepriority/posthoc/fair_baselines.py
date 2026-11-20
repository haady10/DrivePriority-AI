"""Amendment A1: post hoc fair SMART baselines (docs/phase5_amendment_A1_fair_baselines_2026-11-20.md).

Four steps, run on the PC that holds the frozen full-fleet replay inputs and feature files:

  preflight  Checks Python, packages, memory, disk and the input folders; builds and replays ONE month to time
             it; prints how long the full run should take. Writes only into <out>/preflight.
  build      For each month of the replay input: finds the same drive-days in the feature files, checks that the
             five-indicator SMART count recomputed from them equals the replay's smart_count on every row, and
             writes a small side file with smart_count3 (SMART 5, 197, 198 above zero) in the same row order.
             Also writes how often each SMART indicator is reported, by make.
  replay     The frozen engine on 7 scores x 4 policies x budgets 10/20/50 x frozen and rotating ties x 10 seeds.
             Reproduction gate: the frozen SMART and ML cells must come out exactly as in the frozen archive.
  summarise  (any machine, no data needed) The wording rule of A1.6 from the 2024 and test result folders.

Typical use on the lab PC (2024 first; the test period only after a passed 2024 run of the same commit):

  python -m drivepriority.posthoc.fair_baselines preflight --replay C:\\DP-out\\scores_valid2\\replay_valid ^
      --features C:\\DP-out\\features_valid2 --split valid --out C:\\DP-out\\posthoc_A1
  python -m drivepriority.posthoc.fair_baselines build  (same arguments)
  python -m drivepriority.posthoc.fair_baselines replay --replay ... --split valid --out C:\\DP-out\\posthoc_A1

Nothing reads or writes the frozen results except the frozen archive in published_results/phase5 (read-only).
Decision-support simulation on old data: no claim that any failure is prevented.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import platform
import shutil
import subprocess
import sys
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from drivepriority.posthoc.makes import MAKES, QuotaRanker, make_of_drives, make_of_model, within_make_percentile
from drivepriority.simulation.config import load_protocol
from drivepriority.simulation.engine import run_replay
from drivepriority.simulation.events import build_events, event_outcomes, workload
from drivepriority.simulation.ranking import ScoreRanker
from drivepriority.simulation.replay_input import ReplayData, load_replay_input, sha256_file, sq
from drivepriority.simulation.run_phase5 import policies_for, seed_summary
from drivepriority.simulation.stats import bootstrap_means, interval

REPO = Path(__file__).resolve().parents[3]
AMENDMENT = REPO / "docs" / "phase5_amendment_A1_fair_baselines_2026-11-20.md"
CONTROLLED = ("configs", "src/drivepriority/simulation", "src/drivepriority/posthoc",
              "docs/phase5_amendment_A1_fair_baselines_2026-11-20.md")
REGISTER = Path.home() / ".drivepriority" / "posthoc_A1_runs.json"
ARCHIVE = {"valid": "phase5_valid_full_results.zip", "test": "phase5_test_full_results.zip"}
LABEL = "post hoc (amendment A1)"
SMART5 = (5, 187, 188, 197, 198)
SMART3 = (5, 197, 198)
TOL = 1e-12

# name -> (base score column, quota?)
SCORES = {
    "smart_count": ("smart_count", False),                    # S1 (reproduces the frozen SMART cells)
    "smart_count3": ("smart_count3", False),                  # S2
    "smart_count_make_pct": ("smart_count_make_pct", False),  # S3
    "smart_count+quota": ("smart_count", True),               # S4
    "smart_count3+quota": ("smart_count3", True),             # S5
    "ml_h30": ("ml_h30", False),                              # M1 (reproduces the frozen ML cells)
    "ml_h30+quota": ("ml_h30", True),                         # M2
}
SMART_VARIANTS = ["smart_count", "smart_count3", "smart_count_make_pct", "smart_count+quota", "smart_count3+quota"]
TIE_MODES = ("drive", "day")
E_CELLS = {"E1": ("smart_count", "next", "fresh"), "E2": ("ml_h30", "next", "fresh"),
           "E3": ("smart_count", "cool", "fresh"), "E4": ("ml_h30", "cool", "fresh"),
           "E5": ("smart_count", "next", "queue"), "E6": ("ml_h30", "next", "queue"),
           "E7": ("smart_count", "cool", "queue"), "E8": ("ml_h30", "cool", "queue")}


def log(out: Path | None, msg: str) -> None:
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    if out is not None:
        with open(out / "progress.log", "a", encoding="utf-8") as fh:
            fh.write(line + "\n")


def month_bounds(name: str) -> tuple[date, date]:
    y, m = int(name[:4]), int(name[5:7])
    nxt = date(y + m // 12, m % 12 + 1, 1)
    return date(y, m, 1), nxt - timedelta(days=1)


def part_files(folder: Path) -> list[Path]:
    files = sorted(Path(folder).glob("part_*.parquet"))
    if not files:
        raise SystemExit(f"No part_YYYY-MM.parquet files in {folder}")
    return files


def expected_months(first: date, last: date) -> list[str]:
    out, d = [], date(first.year, first.month, 1)
    while d <= last:
        out.append(f"{d.year}-{d.month:02d}")
        d = date(d.year + d.month // 12, d.month % 12 + 1, 1)
    return out


def ram_bytes() -> int | None:
    try:
        if os.name == "nt":
            import ctypes

            class MS(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                            ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                            ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                            ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                            ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
            s = MS()
            s.dwLength = ctypes.sizeof(MS)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(s))
            return int(s.ullTotalPhys)
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) * 1024
    except Exception:
        return None
    return None


def git(*args) -> subprocess.CompletedProcess | None:
    try:
        return subprocess.run(["git", "-C", str(REPO), *args], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.SubprocessError):
        return None


def code_state() -> dict:
    """Commit and whether the controlled files equal the commit (amendment A1.5)."""
    files = {p.relative_to(REPO).as_posix(): sha256_file(p)
             for p in sorted((REPO / "src" / "drivepriority" / "posthoc").glob("*.py"))
             + sorted((REPO / "src" / "drivepriority" / "simulation").glob("*.py")) + [AMENDMENT]
             if p.is_file()}
    head = git("rev-parse", "HEAD")
    if head is None or head.returncode != 0:
        return {"commit": None, "clean": False, "reason": "git is not available or this is not a checkout",
                "code_sha256": files}
    tracked = git("ls-files", "--error-unmatch", AMENDMENT.relative_to(REPO).as_posix())
    dirty = git("status", "--porcelain", "--", *CONTROLLED)
    reason = ""
    if tracked is None or tracked.returncode != 0:
        reason = "the amendment file is not committed"
    elif dirty is None or dirty.returncode != 0 or dirty.stdout.strip():
        reason = "files differ from the commit: " + " ".join((dirty.stdout if dirty else "").split())
    return {"commit": head.stdout.strip(), "clean": not reason, "reason": reason, "code_sha256": files}


def read_register() -> list:
    try:
        return json.loads(REGISTER.read_text(encoding="utf-8")) if REGISTER.is_file() else []
    except (OSError, ValueError):
        return []


def write_register(runs: list) -> None:
    REGISTER.parent.mkdir(parents=True, exist_ok=True)
    REGISTER.write_text(json.dumps(runs, indent=2), encoding="utf-8")


# ====================================================================== build

def build_month(con, part: Path, feat_glob: str, month: str, out_file: Path) -> dict:
    """One month: side file with smart_count3 in the replay part's row order, plus checks and SMART availability."""
    ma, mb = month_bounds(month)
    c5 = " + ".join(f"CAST(coalesce(f.smart_{x}_raw > 0, FALSE) AS INTEGER)" for x in SMART5)
    c3 = " + ".join(f"CAST(coalesce(f.smart_{x}_raw > 0, FALSE) AS INTEGER)" for x in SMART3)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE r AS
        SELECT file_row_number AS rn, drive_id, CAST(date AS DATE) AS date, CAST(smart_count AS INTEGER) AS smart_count
        FROM read_parquet('{sq(part)}', file_row_number = true)""")
    cols = ", ".join(f"f.smart_{x}_raw" for x in SMART5)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE f AS
        SELECT drive_id, CAST(date AS DATE) AS date, model, {', '.join(f'smart_{x}_raw' for x in SMART5)}
        FROM read_parquet('{feat_glob}') WHERE date BETWEEN DATE '{ma}' AND DATE '{mb}'""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE j AS
        SELECT r.rn, r.drive_id, r.date, r.smart_count, f.drive_id IS NULL AS missing, f.model, {cols},
               ({c5}) AS count5, ({c3}) AS count3
        FROM r LEFT JOIN f ON f.drive_id = r.drive_id AND f.date = r.date""")
    n_r, n_f, n_j, n_rn = con.execute("""SELECT (SELECT count(*) FROM r), (SELECT count(*) FROM f),
                                                (SELECT count(*) FROM j), (SELECT count(DISTINCT rn) FROM j)""").fetchone()
    missing, differ = con.execute("""SELECT count(*) FILTER (WHERE missing),
                                            count(*) FILTER (WHERE NOT missing AND count5 <> smart_count) FROM j""").fetchone()
    d0, d1 = con.execute("SELECT min(date), max(date) FROM r").fetchone()
    rec = {"month": month, "replay_rows": n_r, "feature_rows_in_month": n_f, "joined_rows": n_j,
           "rows_without_features": missing, "rows_count5_differs": differ,
           "replay_dates": [str(d0), str(d1)]}
    problems = []
    if n_j != n_r or n_rn != n_r:
        problems.append("a drive-day matched more than one feature row")
    if missing:
        problems.append(f"{missing:,} replay rows have no feature row")
    if differ:
        problems.append(f"{differ:,} rows: the five-indicator count from the features differs from smart_count")
    if n_f != n_r:
        problems.append(f"the features hold {n_f:,} rows in this month, the replay input {n_r:,}")
    if d0 is not None and (d0 < ma or d1 > mb):
        problems.append(f"replay dates {d0}..{d1} are outside {month}")
    rec["problems"] = problems
    if problems:
        return rec
    con.execute(f"""COPY (SELECT drive_id, date, CAST(smart_count AS TINYINT) AS smart_count,
                                 CAST(count3 AS TINYINT) AS smart_count3 FROM j ORDER BY rn)
                    TO '{sq(out_file)}' (FORMAT parquet, COMPRESSION zstd)""")
    rec["file"], rec["sha256"], rec["replay_part_sha256"] = out_file.name, sha256_file(out_file), sha256_file(part)
    make = ("CASE WHEN upper(trim(model)) LIKE 'ST%' OR upper(trim(model)) LIKE 'SEAGATE%' THEN 'Seagate' "
            "WHEN upper(trim(model)) LIKE 'HGST%' OR upper(trim(model)) LIKE 'HITACHI%' THEN 'HGST' "
            "WHEN upper(trim(model)) LIKE 'TOSHIBA%' THEN 'Toshiba' "
            "WHEN upper(trim(model)) LIKE 'WD%' OR upper(trim(model)) LIKE 'WUH%' THEN 'WDC' ELSE 'Other' END")
    agg = ", ".join(f"count(smart_{x}_raw) AS reported_{x}, count(*) FILTER (WHERE smart_{x}_raw > 0) AS positive_{x}"
                    for x in SMART5)
    rec["availability"] = con.execute(f"""SELECT {make} AS make, count(*) AS rows, {agg},
                                                 count(*) FILTER (WHERE count5 >= 4) AS rows_count_4_or_5
                                          FROM j GROUP BY 1 ORDER BY 1""").df().to_dict("records")
    return rec


def cmd_build(a, only: list[str] | None = None, out_dir: Path | None = None) -> dict:
    import duckdb
    proto = load_protocol()
    first, last = proto.split_days(a.split)
    out = Path(out_dir or a.out).expanduser().resolve()
    side = out / f"extras_{a.split}"
    side.mkdir(parents=True, exist_ok=True)
    tmp = out / "_duckdb_tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    parts = part_files(Path(a.replay))
    months = [p.stem[5:] for p in parts]
    want = expected_months(first, last)
    if only is None and months != want:
        raise SystemExit(f"The replay folder holds months {months[0]}..{months[-1]} ({len(months)}); "
                         f"the {a.split} period needs {want[0]}..{want[-1]} ({len(want)})")
    feats = sorted(Path(a.features).glob(f"features_{a.split}_*.parquet"))
    if not feats:
        raise SystemExit(f"No features_{a.split}_*.parquet in {a.features}")
    feat_glob = sq(Path(a.features).resolve() / f"features_{a.split}_*.parquet")
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{a.memory_limit}'")
    con.execute(f"SET temp_directory='{sq(tmp)}'")
    t0 = time.time()
    report = []
    for p, mth in zip(parts, months):
        if only is not None and mth not in only:
            continue
        t1 = time.time()
        rec = build_month(con, p, feat_glob, mth, side / p.name)
        rec["seconds"] = round(time.time() - t1, 1)
        report.append(rec)
        log(out if out_dir is None else None,
            f"build {a.split} {mth}: {rec['replay_rows']:,} rows, {len(rec['problems'])} problem(s), {rec['seconds']} s")
        if rec["problems"]:
            (out / f"build_{a.split}_FAILED.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
            raise SystemExit(f"{mth}: " + "; ".join(rec["problems"]) + ". Nothing after this month was written. "
                             "Stop and send the FAILED file.")
    con.close()
    shutil.rmtree(tmp, ignore_errors=True)
    avail = []
    for rec in report:
        for r in rec.pop("availability"):
            avail.append({"month": rec["month"], **r})
    av = pd.DataFrame(avail)
    if len(av):
        tot = av.drop(columns=["month"]).groupby("make").sum().reset_index()
        for x in SMART5:
            tot[f"reported_share_{x}"] = tot[f"reported_{x}"] / tot["rows"]
            tot[f"positive_share_{x}"] = tot[f"positive_{x}"] / tot["rows"]
        tot.to_csv(out / f"smart_availability_by_make_{a.split}.csv", index=False)
    manifest = {"what": f"Amendment A1 side files for the {a.split} replay input ({LABEL})", "split": a.split,
                "replay_folder": str(Path(a.replay).resolve()), "features_folder": str(Path(a.features).resolve()),
                "features_sha256": {f.name: sha256_file(f) for f in feats} if only is None else "not hashed (one month)",
                "months": report, "rows": int(sum(r["replay_rows"] for r in report)),
                "all_rows_matched_and_count_reproduced": all(not r["problems"] for r in report),
                "code": code_state(), "seconds": round(time.time() - t0, 1),
                "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    (out / f"build_{a.split}_manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    if out_dir is None:
        log(out, f"build {a.split} done: {manifest['rows']:,} rows in {manifest['seconds']} s; side files in {side}")
    return manifest


# ====================================================================== replay

def load_inputs(replay: Path, extras: Path, first: date, last: date, out: Path | None) -> tuple[ReplayData, np.ndarray]:
    """The frozen replay input with the frozen loader, plus smart_count3 from the side files, checked row by row."""
    import pyarrow as pa
    import pyarrow.dataset as ds
    log(out, f"loading the replay input {replay} ...")
    data = load_replay_input(replay, first, last)
    for k in [k for k in data.scores if k not in ("smart_count", "ml_h30")]:
        del data.scores[k]                                    # not needed here; frees memory
    r_parts = [p.name for p in part_files(replay)]
    x_parts = [p.name for p in part_files(extras)]
    if r_parts != x_parts:
        raise SystemExit(f"The side files ({len(x_parts)}) do not match the replay parts ({len(r_parts)}); run build again")
    tbl = ds.dataset(str(extras), format="parquet").to_table(columns=["date", "smart_count", "smart_count3"])
    day = tbl["date"].cast(pa.date32()).cast(pa.int32()).to_numpy().astype(np.int32)
    sc = tbl["smart_count"].to_numpy().astype(np.float64)
    c3 = tbl["smart_count3"].to_numpy().astype(np.float64)
    del tbl
    if len(day) != len(data.day):
        raise SystemExit(f"Side files hold {len(day):,} rows, the replay input {len(data.day):,}")
    if len(day) and not np.all(day[1:] >= day[:-1]):                 # same rule as the frozen loader
        order = np.argsort(day, kind="stable")
        day, sc, c3 = day[order], sc[order], c3[order]
    if not np.array_equal(day, data.day) or not np.array_equal(sc, data.scores["smart_count"]):
        raise SystemExit("The side files are not row-aligned with the replay input (day or smart_count differ); "
                         "run build again")
    if np.any(c3 > sc):
        raise SystemExit("smart_count3 is larger than smart_count on some row; stopping")
    log(out, f"  {len(day):,} rows, {data.n_drives:,} drives, {data.n_days} days; side files aligned on every row")
    return data, c3


def ranker_for(data, values, quota, make_of_drive, seed, tie_mode):
    if quota:
        return QuotaRanker(data, values, make_of_drive, seed, tie_mode)
    return ScoreRanker(data, values, seed, tie_mode)


def cell_id(score: str, tie: str, budget: int, reinsp: str, sched: str) -> str:
    return f"{score}|tie-{tie}|B{budget}|{reinsp}|{sched}"


def run_block(data, events, proto, score, values, quota, make_of_drive, tie, budgets, seeds, workers=1) -> dict:
    """All policies and budgets of one score and tie order, every tie seed. Returns per-cell seed rows,
    capture sums over seeds (per eligible event) and engine rule counts.

    Seeds run in parallel threads when workers > 1 (numpy sorting releases the GIL). Each seed is independent
    and the results are added up in seed order, so the numbers are the same for any number of workers."""
    from concurrent.futures import ThreadPoolExecutor
    e = proto.raw["endpoint"]
    specs = []
    for b in budgets:
        specs += policies_for(b, proto.cooldown_days, [None, proto.queue_primary])
    keys = [cell_id(score, tie, s.budget, "cool" if s.cooldown_days else "next",
                    "fresh" if s.queue is None else "queue") for s in specs]
    mk = make_of_drive

    def one_seed(seed):
        res_all = run_replay(data, ranker_for(data, values, quota, mk, seed, tie), specs)
        got = []
        for res in res_all:
            out = event_outcomes(events, res, data.n_drives, e["lead_max_sensitivity"], e["lead_long_min"])
            row = seed_summary(events, out, workload(res))
            done = res.drive[res.completed]
            shares = np.bincount(mk[done], minlength=len(MAKES)) / max(1, len(done))
            row.update({f"share_completed_{m}": float(shares[j]) for j, m in enumerate(MAKES)})
            row["seed"] = seed
            got.append((row, out["timely"][events.eligible].astype(np.float64), dict(res.checks)))
        return got

    if workers > 1 and len(seeds) > 1:
        with ThreadPoolExecutor(max_workers=min(workers, len(seeds))) as ex:
            per_seed = list(ex.map(one_seed, seeds))
    else:
        per_seed = [one_seed(s) for s in seeds]
    rows = {k: [] for k in keys}
    cap = {k: np.zeros(events.n_eligible) for k in keys}
    checks = {k: {} for k in keys}
    for got in per_seed:                                   # seed order, as in the frozen run
        for k, (row, hit, chk) in zip(keys, got):
            rows[k].append(row)
            cap[k] += hit
            for c, v in chk.items():
                checks[k][c] = checks[k].get(c, 0) + int(v)
    return {"rows": rows, "capture_sum": cap, "checks": checks,
            "specs": {k: {"budget": s.budget, "cooldown_days": s.cooldown_days,
                          "queue": None if s.queue is None else s.queue.tag} for k, s in zip(keys, specs)}}


def save_block(path: Path, block: dict, tag: str) -> None:
    np.savez_compressed(path.with_suffix(".npz"), **{k: v for k, v in block["capture_sum"].items()})
    meta = {"tag": tag, "rows": block["rows"], "checks": block["checks"], "specs": block["specs"]}
    path.with_suffix(".json").write_text(json.dumps(meta, default=float), encoding="utf-8")


def load_block(path: Path, tag: str) -> dict | None:
    j, z = path.with_suffix(".json"), path.with_suffix(".npz")
    if not (j.is_file() and z.is_file()):
        return None
    meta = json.loads(j.read_text(encoding="utf-8"))
    if meta.get("tag") != tag:
        return None
    with np.load(z) as f:
        cap = {k: f[k] for k in f.files}
    return {"rows": meta["rows"], "capture_sum": cap, "checks": meta["checks"], "specs": meta["specs"]}


def frozen_archive(split: str, path: str | None) -> zipfile.ZipFile:
    p = Path(path) if path else REPO / "published_results" / "phase5" / ARCHIVE[split]
    if not p.is_file():
        raise SystemExit(f"The frozen archive is missing: {p}")
    sums = REPO / "published_results" / "phase5" / "SHA256SUMS.txt"
    if path is None and sums.is_file():
        want = dict(line.split()[::-1] for line in sums.read_text().splitlines() if line.strip())
        if want.get(p.name) != sha256_file(p):
            raise SystemExit(f"The frozen archive {p.name} does not have its published SHA-256")
    return zipfile.ZipFile(p)


def reproduction(cells: pd.DataFrame, ev: pd.DataFrame, contrasts: pd.DataFrame, z: zipfile.ZipFile) -> dict:
    """Amendment A1.4: the frozen SMART and ML cells, events and contrasts must come out exactly."""
    fz = pd.read_csv(io.BytesIO(z.read("cell_results.csv")))
    fz = fz[(fz.fleet == "all") & (fz["rule"] == "daily") & fz.score.isin(["smart_count", "ml_h30"])]
    fev = pd.read_parquet(io.BytesIO(z.read("event_coverage.parquet")))
    fev = fev[fev.status == "eligible"].reset_index(drop=True)
    fcon = pd.read_csv(io.BytesIO(z.read("rq1_contrasts.csv")))
    problems, compared = [], 0
    groups = {("drive", 20): "rq1_primary", ("drive", 10): "rq1_budget", ("drive", 50): "rq1_budget",
              ("day", 20): "sens_tie"}
    fields = ["eligible_events", "coverage", "coverage_min_seed", "coverage_max_seed", "completed",
              "distinct_drives_inspected", "repeat_share", "coverage_ci_low", "coverage_ci_high"]
    for (tie, b), grp in groups.items():
        for score in ("smart_count", "ml_h30"):
            for r in ("next", "cool"):
                for g in ("fresh", "queue"):
                    sched = "fresh" if g == "fresh" else "q2-3-3"
                    key = f"{grp}|all|{score}|daily|{r}|{sched}|B{b}|tie-{tie}"
                    f = fz[fz.cell == key]
                    mine = cells[cells.cell == cell_id(score, tie, b, r, g)]
                    if len(f) != 1 or len(mine) != 1:
                        problems.append(f"{key}: not found")
                        continue
                    for col in fields:
                        compared += 1
                        if abs(float(f.iloc[0][col]) - float(mine.iloc[0][col])) > TOL * max(1.0, abs(float(f.iloc[0][col]))):
                            problems.append(f"{key} {col}: frozen {f.iloc[0][col]!r}, here {mine.iloc[0][col]!r}")
    if not np.array_equal(ev["drive_id"].astype(str).to_numpy(), fev["drive_id"].astype(str).to_numpy()):
        problems.append("the eligible events differ from the frozen event table")
    else:
        for e_name, (score, r, g) in E_CELLS.items():
            compared += 1
            col = cell_id(score, "drive", 20, r, g)
            if np.max(np.abs(ev[col].to_numpy() - fev[f"{e_name}_timely_share"].to_numpy())) > TOL:
                problems.append(f"{e_name}: per-event capture shares differ from the frozen archive")
    sets = {"primary_B20": ("drive", 20), "budget_B10": ("drive", 10), "budget_B50": ("drive", 50),
            "tie_day": ("day", 20)}
    for set_name, (tie, b) in sets.items():
        for r in ("next", "cool"):
            for g in ("fresh", "queue"):
                f = fcon[(fcon["set"] == set_name) & (fcon.contrast == f"delta_ml_minus_smart|{r}|{g}")]
                mine = contrasts[(contrasts.contrast == "M1-S1") & (contrasts.tie == tie) & (contrasts.budget == b)
                                 & (contrasts.reinspection == r) & (contrasts.scheduling == g)]
                if len(f) != 1 or len(mine) != 1:
                    problems.append(f"contrast {set_name} {r}/{g}: not found")
                    continue
                for col in ("estimate", "ci_low", "ci_high"):
                    compared += 1
                    if abs(float(f.iloc[0][col]) - float(mine.iloc[0][col])) > TOL:
                        problems.append(f"contrast {set_name} {r}/{g} {col}: frozen {f.iloc[0][col]}, here {mine.iloc[0][col]}")
    return {"passed": not problems, "values_compared": compared, "problems": problems}


CONTRASTS = ([(f"M1-S{k + 1}", "ml_h30", s) for k, s in enumerate(SMART_VARIANTS)]
             + [("M2-S4", "ml_h30+quota", "smart_count+quota"), ("M2-S5", "ml_h30+quota", "smart_count3+quota"),
                ("M2-M1", "ml_h30+quota", "ml_h30"), ("S4-S1", "smart_count+quota", "smart_count")])


def cmd_replay(a, out_override: Path | None = None, only_days: tuple[date, date] | None = None,
               replay_dir: Path | None = None, extras_dir: Path | None = None) -> dict:
    proto = load_protocol()
    out = Path(out_override or a.out).expanduser().resolve()
    res_dir = out / f"results_{a.split}"
    res_dir.mkdir(parents=True, exist_ok=True)
    blocks_dir = res_dir / "blocks"
    blocks_dir.mkdir(exist_ok=True)
    seeds = proto.tie_seeds if not a.seeds else [int(s) for s in a.seeds]
    workers = int(getattr(a, "workers", 0) or 0) or max(1, min(10, (os.cpu_count() or 2) // 3))
    budgets = [proto.budget_primary] + proto.budgets_sensitivity if not a.budgets else [int(b) for b in a.budgets]
    scores = list(SCORES) if not a.scores else a.scores
    for s in scores:
        if s not in SCORES:
            raise SystemExit(f"Unknown score {s}; known: {list(SCORES)}")
    full = (seeds == proto.tie_seeds and sorted(budgets) == sorted([proto.budget_primary] + proto.budgets_sensitivity)
            and scores == list(SCORES) and only_days is None)
    state = code_state()
    reg_entry = None
    if out_override is None:
        if a.split == "test":
            if not a.post_hoc_test:
                raise SystemExit("The test period needs --post-hoc-test (amendment A1.5).")
            if not full:
                raise SystemExit("A partial run (--seeds, --budgets or --scores) is refused on the test period.")
            if not state["clean"]:
                raise SystemExit(f"The test period needs a clean, committed checkout: {state['reason']}")
            runs = read_register()
            ok_valid = [r for r in runs if r["split"] == "valid" and r.get("commit") == state["commit"]
                        and r.get("clean") and r.get("full_run") and r.get("finished_utc")
                        and r.get("reproduction_passed")]
            if not ok_valid:
                raise SystemExit("The test period is refused: no finished 2024 run of this commit (full, from a clean "
                                 f"checkout) with a passed reproduction gate is recorded on this machine ({REGISTER}). "
                                 "Run --split valid first.")
            done_test = [r for r in runs if r["split"] == "test" and r.get("finished_utc")]
            if done_test:
                raise SystemExit(f"The test period was already run under amendment A1 ({done_test[0]['out']}). "
                                 "A second run needs amendment A1-b.")
        if full and (res_dir / "cells.csv").exists():
            raise SystemExit(f"{res_dir} already holds results; use a new --out folder")
        runs = read_register()
        reg_entry = {"split": a.split, "commit": state["commit"], "clean": state["clean"], "out": str(res_dir),
                     "started_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                     "machine": platform.node(), "full_run": full}
        runs.append(reg_entry)
        write_register(runs)
    t0 = time.time()
    first, last = only_days or proto.split_days(a.split)
    replay = Path(replay_dir or a.replay).expanduser().resolve()
    extras = Path(extras_dir or (out / f"extras_{a.split}")).expanduser().resolve()
    t_load = time.time()
    data, c3 = load_inputs(replay, extras, first, last, out if out_override is None else None)
    make_of_drive = make_of_drives(data.drive_ids)
    data.scores["smart_count3"] = c3
    data.scores["smart_count_make_pct"] = within_make_percentile(data, data.scores["smart_count"], make_of_drive)
    t_load_done = time.time()
    e = proto.raw["endpoint"]
    events = build_events(data, e["lead_min"], e["lead_max"], e["full_opportunity_days"])
    lg = (lambda m: log(res_dir, m)) if out_override is None else (lambda m: None)
    lg(f"events {events.counts()}; drives by make " +
       ", ".join(f"{m} {int((make_of_drive == j).sum()):,}" for j, m in enumerate(MAKES)))
    tag_base = hashlib.sha256(json.dumps({"proto": proto.sha256, "seeds": seeds, "budgets": budgets,
                                          "rows": int(len(data.day)), "first": str(first), "last": str(last),
                                          "code": state["code_sha256"]}, sort_keys=True).encode()).hexdigest()
    blocks = {}
    for score in scores:
        base, quota = SCORES[score]
        for tie in TIE_MODES:
            name = f"{score.replace('+', '_')}__{tie}"
            tag = f"{tag_base}|{score}|{tie}"
            blk = load_block(blocks_dir / name, tag)
            if blk is None:
                t1 = time.time()
                blk = run_block(data, events, proto, score, data.scores[base], quota, make_of_drive, tie, budgets, seeds,
                                workers)
                save_block(blocks_dir / name, blk, tag)
                lg(f"{score}, ties {tie}: {len(seeds)} seeds x {len(blk['rows'])} cells in {time.time() - t1:.0f} s")
            else:
                lg(f"{score}, ties {tie}: loaded from an earlier start of this run")
            blocks[(score, tie)] = blk
    t_rep_done = time.time()
    # ---------------------------------------------------------------- tables
    keys, rows = [], []
    for (score, tie), blk in blocks.items():
        for k, seed_rows in blk["rows"].items():
            df = pd.DataFrame(seed_rows)
            sp = blk["specs"][k]
            row = {"cell": k, "score": score, "base_score": SCORES[score][0], "quota": SCORES[score][1],
                   "tie_mode": tie, "budget_per_10000": sp["budget"],
                   "reinspection": "cool" if sp["cooldown_days"] else "next",
                   "scheduling": "fresh" if sp["queue"] is None else "queue", "tie_seeds": len(df)}
            row.update({c: float(df[c].mean()) for c in df.columns if c != "seed"})
            row["coverage_min_seed"], row["coverage_max_seed"] = float(df.coverage.min()), float(df.coverage.max())
            row["engine_rule_violations"] = int(sum(blk["checks"][k].values()))
            rows.append(row)
            keys.append(k)
    cells = pd.DataFrame(rows)
    P = np.stack([blocks[(r["score"], r["tie_mode"])]["capture_sum"][r["cell"]] / r["tie_seeds"] for r in rows], axis=1)
    b = proto.raw["bootstrap"]
    B = bootstrap_means(P, b["reps"], b["seed"])
    lo, hi = (1 - b["level"]) / 2, 1 - (1 - b["level"]) / 2
    cells["coverage_ci_low"] = np.quantile(B, lo, axis=0)
    cells["coverage_ci_high"] = np.quantile(B, hi, axis=0)
    cells.insert(0, "label", LABEL)
    col = {k: j for j, k in enumerate(keys)}
    con_rows = []
    for tie in TIE_MODES:
        for bud in budgets:
            for r in ("next", "cool"):
                for g in ("fresh", "queue"):
                    for name, x, y in CONTRASTS:
                        kx, ky = cell_id(x, tie, bud, r, g), cell_id(y, tie, bud, r, g)
                        if kx not in col or ky not in col:
                            continue
                        s = B[:, col[kx]] - B[:, col[ky]]
                        cl, ch = interval(s, b["level"])
                        est = float(P[:, col[kx]].mean() - P[:, col[ky]].mean())
                        con_rows.append({"label": LABEL, "contrast": name, "minuend": x, "subtrahend": y,
                                         "tie": tie, "budget": bud, "reinspection": r, "scheduling": g,
                                         "estimate": est, "ci_low": cl, "ci_high": ch,
                                         "interval_excludes_zero": bool(cl > 0 or ch < 0), "events": P.shape[0]})
    contrasts = pd.DataFrame(con_rows)
    el = events.eligible
    ev = pd.DataFrame({"drive_id": data.drive_ids[events.drive[el]],
                       "make": np.array(MAKES)[make_of_drive[events.drive[el]]],
                       "fail_date": [date(1970, 1, 1) + timedelta(days=int(d)) for d in events.fail_day[el]],
                       "opportunity_days": events.opportunity_days[el]})
    ev = pd.concat([ev, pd.DataFrame(P.astype(np.float64), columns=keys)], axis=1)
    if out_override is not None:
        return {"cells": cells, "contrasts": contrasts, "events": ev, "seconds": time.time() - t0,
                "load_seconds": t_load_done - t_load, "replay_seconds": t_rep_done - t_load_done}
    cells.to_csv(res_dir / "cells.csv", index=False)
    contrasts.to_csv(res_dir / "contrasts.csv", index=False)
    ev.to_parquet(res_dir / "event_capture.parquet", index=False)
    models = pd.Series([str(d).split("|", 1)[-1] for d in data.drive_ids]).value_counts()
    pd.DataFrame({"model": models.index, "drives": models.values,
                  "make": [MAKES[make_of_model(m)] for m in models.index]}
                 ).to_csv(res_dir / "make_by_model.csv", index=False)
    rep = {"passed": False, "values_compared": 0, "problems": ["not run: partial run"]}
    if full:
        rep = reproduction(cells, ev, contrasts, frozen_archive(a.split, a.frozen_archive))
    (res_dir / "reproduction.json").write_text(json.dumps(rep, indent=2), encoding="utf-8")
    violations = int(cells.engine_rule_violations.sum())
    info = {"label": LABEL, "amendment": AMENDMENT.name, "split": a.split, "full_run": full, "workers": workers,
            "first_day": str(first), "last_day": str(last), "rows": int(len(data.day)), "drives": int(data.n_drives),
            "events": events.counts(), "seeds": seeds, "budgets": budgets, "scores": scores, "tie_modes": TIE_MODES,
            "protocol_sha256": proto.sha256, "protocol_is_frozen_file": proto.is_frozen, "code": state,
            "replay_input_sha256": {p.name: sha256_file(p) for p in part_files(replay)},
            "side_files_sha256": {p.name: sha256_file(p) for p in part_files(extras)},
            "reproduction_passed": rep["passed"], "engine_rule_violations": violations,
            "python": sys.version.split()[0], "numpy": np.__version__, "pandas": pd.__version__,
            "machine": platform.node(), "os": platform.platform(), "cpu_count": os.cpu_count(),
            "total_seconds": round(time.time() - t0, 1),
            "finished_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    for f in (out / f"build_{a.split}_manifest.json", out / f"smart_availability_by_make_{a.split}.csv"):
        if f.is_file():
            shutil.copy2(f, res_dir / f.name)
    (res_dir / "run_info.json").write_text(json.dumps(info, indent=2, default=str), encoding="utf-8")
    runs = read_register()
    for r in runs:
        if r.get("out") == reg_entry["out"] and r.get("started_utc") == reg_entry["started_utc"]:
            r.update({"finished_utc": info["finished_utc"], "reproduction_passed": rep["passed"] and violations == 0})
    write_register(runs)
    lg(f"reproduction gate: {'PASSED' if rep['passed'] else 'FAILED'} ({rep['values_compared']} values); "
       f"engine rule violations {violations}")
    show = cells[(cells.budget_per_10000 == proto.budget_primary)].pivot_table(
        index=["tie_mode", "reinspection", "scheduling"], columns="score", values="coverage")[list(scores)]
    print("\nCoverage at budget 20 (share of eligible failures inspected 7 to 30 days before failure):")
    print((show * 100).round(1).to_string())
    zipname = out / f"posthoc_A1_{a.split}_results.zip"
    with zipfile.ZipFile(zipname, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(res_dir.iterdir()):
            if f.is_file():
                zf.write(f, f.name)
    lg(f"Done in {info['total_seconds']} s. Send this file back: {zipname}")
    return info


# ====================================================================== preflight

def cmd_preflight(a) -> int:
    proto = load_protocol()
    out = Path(a.out).expanduser().resolve()
    pre = out / f"preflight_{a.split}"
    if pre.exists():
        shutil.rmtree(pre)
    pre.mkdir(parents=True)
    rep, ok = {"split": a.split}, True
    import duckdb
    import pyarrow
    import pyarrow.parquet as pq
    rep["python"] = sys.version.split()[0]
    rep["packages"] = {"duckdb": duckdb.__version__, "pyarrow": pyarrow.__version__, "pandas": pd.__version__,
                       "numpy": np.__version__}
    ram = ram_bytes()
    rep["ram_gb"] = round(ram / 2**30, 1) if ram else None
    rep["cpu_count"] = os.cpu_count()
    rep["free_disk_gb_at_out"] = round(shutil.disk_usage(out).free / 2**30, 1)
    st = code_state()
    rep["git_commit"], rep["checkout_clean"], rep["checkout_note"] = st["commit"], st["clean"], st["reason"]
    first, last = proto.split_days(a.split)
    parts = part_files(Path(a.replay))
    months = [p.stem[5:] for p in parts]
    want = expected_months(first, last)
    rep["replay_months"] = f"{months[0]}..{months[-1]} ({len(months)})"
    if months != want:
        ok = False
        rep["problem_months"] = f"expected {want[0]}..{want[-1]} ({len(want)})"
    rows = sum(pq.ParquetFile(p).metadata.num_rows for p in parts)
    rep["replay_rows"] = rows
    rep["replay_gb"] = round(sum(p.stat().st_size for p in parts) / 2**30, 2)
    feats = sorted(Path(a.features).glob(f"features_{a.split}_*.parquet"))
    rep["feature_files"] = [f.name for f in feats]
    rep["features_gb"] = round(sum(f.stat().st_size for f in feats) / 2**30, 2)
    if not feats:
        ok = False
    # memory: the frozen loader holds the whole table; about 110 bytes per row at the peak (estimate)
    need_gb = rows * 110 / 2**30
    rep["estimated_peak_memory_gb"] = round(need_gb, 1)
    if ram and need_gb > 0.85 * ram / 2**30:
        rep["memory_warning"] = "the estimated peak is close to or above the installed memory"
    # one month: build and replay, timed
    m0 = months[len(months) // 2]
    t = time.time()
    man = cmd_build(a, only=[m0], out_dir=pre)
    rep["build_one_month_seconds"] = round(time.time() - t, 1)
    rep["build_one_month_passed"] = man["all_rows_matched_and_count_reproduced"]
    ok &= rep["build_one_month_passed"]
    side_size = sum(p.stat().st_size for p in (pre / f"extras_{a.split}").glob("*.parquet"))
    rep["side_files_gb_estimate"] = round(side_size * len(months) / 2**30, 2)
    ma, mb = month_bounds(m0)
    days = (last - first).days + 1
    month_days = (mb - ma).days + 1
    passes = len(SCORES) * len(TIE_MODES) * len(proto.tie_seeds)
    if a.split == "test":
        # No test-period replay before the one run of amendment A1: the timing comes from the 2024 preflight.
        v = out / "preflight_valid" / "preflight.json"
        if not v.is_file():
            raise SystemExit("Run the 2024 preflight first (it times the replay; the test period is not replayed here)")
        vt = json.loads(v.read_text(encoding="utf-8"))["timing_one_month_seconds"]
        scale = (rows / len(months)) / max(1, vt["rows_per_month"])     # test fleet size against the 2024 fleet
        per_pass, load_month = vt["one_pass"] * scale, vt["load"] * scale
        res = None
    else:
        one = pre / "one_month"
        one.mkdir()
        shutil.copy2(Path(a.replay) / f"part_{m0}.parquet", one / f"part_{m0}.parquet")

        class A:
            pass
        b = A()
        w = max(1, min(4, (os.cpu_count() or 2) // 4))
        b.split, b.seeds, b.budgets, b.scores, b.frozen_archive, b.workers = a.split, [str(s) for s in range(w)], \
            None, ["smart_count", "ml_h30+quota"], None, w
        res = cmd_replay(b, out_override=pre, only_days=(ma, mb), replay_dir=one, extras_dir=pre / f"extras_{a.split}")
        per_pass = res["replay_seconds"] / (2 * len(TIE_MODES) * w)   # 2 scores x 2 tie orders x w seeds
        load_month = res["load_seconds"]
        shutil.rmtree(one, ignore_errors=True)
        rep["one_month_replay_ok"] = bool(len(res["cells"]))
        ok &= rep["one_month_replay_ok"]
    est_replay = per_pass * (days / month_days) * passes
    est_build = rep["build_one_month_seconds"] * len(months)
    est_load = load_month * len(months) + 120
    rep["timing_one_month_seconds"] = {"load": round(load_month, 2), "one_pass": round(per_pass, 2), "days": month_days,
                                       "rows_per_month": int(rows / len(months))}
    rep["estimate_minutes"] = {"build": round(est_build / 60), "replay": round(est_replay / 60),
                               "load_and_tables": round(est_load / 60),
                               "total": round((est_build + est_replay + est_load) / 60)}
    rep["ready"] = bool(ok)
    (pre / "preflight.json").write_text(json.dumps(rep, indent=2, default=str), encoding="utf-8")
    print("\n" + json.dumps(rep, indent=2, default=str))
    print(f"\nPREFLIGHT {'OK' if ok else 'FAILED'}. Estimated time for the {a.split} period: about "
          f"{rep['estimate_minutes']['total']} minutes (an estimate). Report: {pre / 'preflight.json'}")
    return 0 if ok else 2


# ====================================================================== summarise (no data needed)

def cmd_summarise(a) -> int:
    """Amendment A1.6 wording rule from the 2024 and test result folders (or zip files)."""
    def read(src, name):
        p = Path(src)
        if p.suffix == ".zip":
            return pd.read_csv(io.BytesIO(zipfile.ZipFile(p).read(name)))
        return pd.read_csv(p / name)
    cv, ct = read(a.valid, "contrasts.csv"), read(a.test, "contrasts.csv")
    rows = []
    for r in ("next", "cool"):
        for g in ("fresh", "queue"):
            for sign, word in ((1, "ML better"), (-1, "SMART better")):
                ok_all, why = True, []
                for k in range(1, 6):
                    for tie in TIE_MODES:
                        t = ct[(ct.contrast == f"M1-S{k}") & (ct.tie == tie) & (ct.budget == 20)
                               & (ct.reinspection == r) & (ct.scheduling == g)].iloc[0]
                        v = cv[(cv.contrast == f"M1-S{k}") & (cv.tie == tie) & (cv.budget == 20)
                               & (cv.reinspection == r) & (cv.scheduling == g)].iloc[0]
                        good = (t.ci_low > 0 if sign > 0 else t.ci_high < 0) and np.sign(v.estimate) == sign
                        if not good:
                            ok_all = False
                            why.append(f"S{k}/{tie}")
                rows.append({"reinspection": r, "scheduling": g, "claim": word, "allowed": ok_all,
                             "fails_against": " ".join(why)})
    out = pd.DataFrame(rows)
    print(out.to_string(index=False))
    if a.out:
        dest = Path(a.out)
        if dest.is_dir() or not dest.suffix:          # a folder was given: write wording_rule.csv inside it
            dest.mkdir(parents=True, exist_ok=True)
            dest = dest / "wording_rule.csv"
        out.to_csv(dest, index=False)
        print(f"saved {dest}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="step", required=True)

    def common(p, need_features=True):
        p.add_argument("--replay", required=True, help="folder of the frozen replay input (part_YYYY-MM.parquet)")
        if need_features:
            p.add_argument("--features", required=True, help="folder of the full-fleet feature files")
        p.add_argument("--split", required=True, choices=("valid", "test"))
        p.add_argument("--out", required=True, help="work folder for amendment A1 (new folder)")
        p.add_argument("--memory-limit", default="12GB", help="DuckDB memory limit for the build step")

    p = sub.add_parser("preflight", help="check this PC and time one month")
    common(p)
    p.set_defaults(func=cmd_preflight)
    p = sub.add_parser("build", help="side files with smart_count3, checked row by row")
    common(p)
    p.set_defaults(func=cmd_build)
    p = sub.add_parser("replay", help="the post hoc replay with the reproduction gate")
    common(p, need_features=False)
    p.add_argument("--post-hoc-test", action="store_true", help="needed for the test period (amendment A1.5)")
    p.add_argument("--frozen-archive", help="frozen result zip (default: published_results/phase5)")
    p.add_argument("--seeds", nargs="*", help="testing only: fewer tie seeds (not reportable)")
    p.add_argument("--budgets", nargs="*", help="testing only: fewer budgets (not reportable)")
    p.add_argument("--scores", nargs="*", help="testing only: fewer scores (not reportable)")
    p.add_argument("--workers", type=int, default=0,
                   help="tie seeds run at the same time (default: CPU cores / 3, at most 10); same results for any value")
    p.set_defaults(func=cmd_replay)
    p = sub.add_parser("summarise", help="wording rule A1.6 from the two result folders or zips")
    p.add_argument("--valid", required=True)
    p.add_argument("--test", required=True)
    p.add_argument("--out")
    p.set_defaults(func=cmd_summarise)
    a = ap.parse_args(argv)
    r = a.func(a)
    return r if isinstance(r, int) else 0


if __name__ == "__main__":
    raise SystemExit(main())
