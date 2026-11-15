"""Read-only: make of every scheduled and completed inspection in each primary cell's saved service log (tie seed 0),
over all decision days. Make from the model name in drive_id. Changes nothing."""
import sys, pandas as pd, numpy as np
d = sys.argv[1]
def make(m):
    return np.select([m.str.startswith("ST"), m.str.startswith("TOSHIBA"), m.str.startswith("HGST"),
                      m.str.startswith("WD") | m.str.startswith("WUH")], ["Seagate", "Toshiba", "HGST", "WDC"], "other")
for i in range(1, 9):
    s = pd.read_parquet(f"{d}/service_log_E{i}.parquet", columns=["decision_date", "drive_id", "completed"])
    s["make"] = make(s.drive_id.str.split("|").str[1])
    c = s[s.completed]
    tot = c.make.value_counts()
    days = s.decision_date.nunique()
    nonsg_days = s[s.make != "Seagate"].decision_date.nunique()
    print(f"E{i}: scheduled {len(s):,}  completed {len(c):,}  decision days {days}  "
          f"days with any non-Seagate scheduled {nonsg_days}  completed by make: "
          + ", ".join(f"{k} {v:,} ({100*v/len(c):.2f}%)" for k, v in tot.items())
          + f"  distinct drives by make: " + ", ".join(f"{k} {v}" for k, v in c.groupby('make').drive_id.nunique().items()))
