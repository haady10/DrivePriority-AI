"""Post hoc, read-only: SMART-count facts of the replay population (review of 19 Nov 2026).

Replaces the snapshot query behind the earlier "drives on 2 June 2025" and "855 Seagate drives with a count of
4 or 5" (all drives of that day, no HDD filter) by the replay's own population: the frozen replay input rows of
one day (HDD cohort, drive = serial|model). Also checks, for every decision day of the period, whether the drives
with a SMART count of 4 or 5 outnumber the day's capacity K_t = floor(20 * N_t / 10,000): if they do on every
day and none of them is outside Seagate, the next-day/fresh Top-K SMART policy (E1) schedules only Seagate
drives on every day under every tie seed. Make is derived from the model string as in T3_by_make.csv.

Usage: python smart_count_day_facts.py <replay_dir> <day YYYY-MM-DD> <out.json>
"""
from __future__ import annotations

import json
import sys

import duckdb

MAKE = """CASE WHEN model LIKE 'ST%' THEN 'Seagate' WHEN model LIKE 'TOSHIBA%' THEN 'Toshiba'
              WHEN model LIKE 'HGST%' THEN 'HGST' WHEN model LIKE 'WD%' OR model LIKE 'WUH%' THEN 'WDC' ELSE 'other' END"""


def main(replay_dir: str, day: str, out: str) -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    src = f"read_parquet('{replay_dir.rstrip('/')}/part_*.parquet')"
    con.execute(f"CREATE VIEW r0 AS SELECT *, split_part(drive_id, '|', 2) AS model FROM {src}")
    con.execute(f"CREATE VIEW r AS SELECT *, {MAKE} AS make FROM r0")
    rows = con.execute(f"""SELECT make, count(*) AS drives, max(smart_count) AS highest_count,
                                  count(*) FILTER (WHERE smart_count >= 4) AS count_4_or_5,
                                  count(*) FILTER (WHERE smart_count >= 1) AS count_1_or_more
                           FROM r WHERE date = DATE '{day}' GROUP BY 1 ORDER BY 2 DESC""").fetchall()
    n_day = con.execute(f"SELECT count(*) FROM r WHERE date = DATE '{day}'").fetchone()[0]
    daily = con.execute("""WITH d AS (SELECT date, count(*) AS n, count(*) FILTER (WHERE smart_count >= 4) AS c45,
                                             count(*) FILTER (WHERE smart_count >= 4 AND make <> 'Seagate') AS c45_other
                                      FROM r GROUP BY date)
                           SELECT count(*), sum(CASE WHEN c45 >= floor(20 * n / 10000) THEN 1 ELSE 0 END),
                                  min(c45 - floor(20 * n / 10000)), max(c45_other) FROM d""").fetchone()
    share = con.execute("SELECT make, count(*) * 1.0 / sum(count(*)) OVER () FROM r GROUP BY 1").fetchall()
    res = {"day": day, "drives_observed": int(n_day), "capacity_B20": int(20 * n_day // 10000),
           "by_make": {m: {"drives": int(a), "highest_count": int(b), "count_4_or_5": int(c), "count_1_or_more": int(e)}
                       for m, a, b, c, e in rows},
           "all_days": {"days": int(daily[0]), "days_with_count_4_or_5_at_least_K": int(daily[1]),
                        "smallest_margin": float(daily[2]), "largest_non_seagate_count_4_or_5": int(daily[3])},
           "drive_day_share_by_make": {m: float(s) for m, s in share}}
    json.dump(res, open(out, "w"), indent=1)
    print(json.dumps(res, indent=1))


if __name__ == "__main__":
    main(*sys.argv[1:4])
