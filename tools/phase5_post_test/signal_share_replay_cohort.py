"""Post hoc, read-only: SMART-signal availability on the replay's own event cohort (review of 19 Nov 2026).

Replaces the separate snapshot query behind the earlier "6,834 failed drives" (serial-only identity, no HDD
filter, first failure inside the period, inner join on prior rows) by the canonical replay cohort: eligible
first-failure events of the HDD replay (drive = serial|model), from event_coverage.parquet. For each event it
reads the frozen replay input rows of the 30 days before failure (f-30 .. f-1) and records whether the SMART
count (number of SMART 5/187/188/197/198 above zero, missing counted as not above zero) was above zero on any
of those days. Also splits the never-captured events by inspection history (seed-0 logs, as
never_caught_logs.py). Nothing is retrained or replayed.

Usage: python signal_share_replay_cohort.py <result_dir> <replay_dir> <out.json>
"""
from __future__ import annotations

import json
import sys

import duckdb
import numpy as np
import pandas as pd


def make_of(model: pd.Series) -> np.ndarray:
    return np.select([model.str.startswith("ST"), model.str.startswith("TOSHIBA"), model.str.startswith("HGST"),
                      model.str.startswith("WD") | model.str.startswith("WUH")], ["Seagate", "Toshiba", "HGST", "WDC"], "other")


def main(result_dir: str, replay_dir: str, out: str) -> None:
    e = pd.read_parquet(f"{result_dir}/event_coverage.parquet")
    el = e[e.status == "eligible"].copy()
    el["caught"] = (el[[f"E{i}_timely_share" for i in range(1, 9)]] > 0).any(axis=1)
    el["make"] = make_of(el.drive_id.str.split("|").str[1])
    el["fail_date"] = pd.to_datetime(el.fail_date)
    # inspection history from the seed-0 service logs (completed inspections before failure)
    logs = []
    for i in range(1, 9):
        s = pd.read_parquet(f"{result_dir}/service_log_E{i}.parquet", columns=["service_date", "drive_id", "completed"])
        logs.append(s[s.completed & s.drive_id.isin(el.drive_id)])
    s = pd.concat(logs)
    s["service_date"] = pd.to_datetime(s.service_date)
    s = s.merge(el[["drive_id", "fail_date"]], on="drive_id")
    s = s[(s.fail_date - s.service_date).dt.days >= 1]
    el["ever_inspected_seed0"] = el.drive_id.isin(set(s.drive_id))

    con = duckdb.connect()
    con.execute("SET memory_limit='5GB'")
    con.register("ev", el[["drive_id", "fail_date"]])
    sig = con.execute(f"""
        SELECT ev.drive_id, max(CASE WHEN r.smart_count > 0 THEN 1 ELSE 0 END) AS any_signal, count(*) AS days_seen
        FROM ev JOIN read_parquet('{replay_dir.rstrip('/')}/part_*.parquet') r
          ON r.drive_id = ev.drive_id
         AND r.date BETWEEN CAST(ev.fail_date AS DATE) - INTERVAL 30 DAY AND CAST(ev.fail_date AS DATE) - INTERVAL 1 DAY
        GROUP BY 1""").df()
    el = el.merge(sig, on="drive_id", how="left")
    el["any_signal"] = el.any_signal.fillna(0).astype(int)
    el["days_seen"] = el.days_seen.fillna(0).astype(int)

    res = {"eligible_events": int(len(el)), "events_with_rows_in_window": int((el.days_seen > 0).sum()),
           "median_days_seen": float(el.days_seen.median()),
           "any_signal_share_all": float(el.any_signal.mean()), "by_make": {}, "never_captured": {}}
    for m, g in el.groupby("make"):
        res["by_make"][m] = {"events": int(len(g)), "any_signal_share": float(g.any_signal.mean()),
                             "never_captured": int((~g.caught).sum())}
    # drive models with at least 40 eligible events (the threshold of never_caught_test.py)
    el["model"] = el.drive_id.str.split("|").str[1]
    res["by_model_min40"] = {m: {"events": int(len(g)), "any_signal_share": float(g.any_signal.mean()),
                                 "never_captured_share": float((~g.caught).mean())}
                             for m, g in el.groupby("model") if len(g) >= 40}
    nc = el[~el.caught]
    a = nc[~nc.ever_inspected_seed0]
    res["never_captured"] = {
        "events": int(len(nc)), "any_signal_share": float(nc.any_signal.mean()),
        "never_inspected_seed0": int(len(a)), "never_inspected_any_signal_share": float(a.any_signal.mean()),
        "never_inspected_no_signal": int((a.any_signal == 0).sum()),
        "captured_any_signal_share": float(el[el.caught].any_signal.mean()),
        "never_inspected_by_make": {m: {"events": int(len(g)), "no_signal": int((g.any_signal == 0).sum())}
                                    for m, g in a.groupby("make")},
    }
    json.dump(res, open(out, "w"), indent=1)
    print(json.dumps(res, indent=1))


if __name__ == "__main__":
    main(*sys.argv[1:4])
