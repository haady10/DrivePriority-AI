"""Read-only: for never-caught eligible test events, were they ever inspected (completed, seed-0 logs, any cell),
and how far before failure? Uses event_coverage + service_log_E1..E8. Changes nothing."""
import sys, pandas as pd, numpy as np
d=sys.argv[1]
e=pd.read_parquet(f"{d}/event_coverage.parquet"); el=e[e.status=="eligible"].copy()
el["caught"]=(el[[f"E{i}_timely_share" for i in range(1,9)]]>0).any(axis=1)
el["model"]=el.drive_id.str.split("|").str[1]
el["make"]=np.select([el.model.str.startswith("ST"),el.model.str.startswith("TOSHIBA"),el.model.str.startswith("HGST"),el.model.str.startswith("WD")],["Seagate","Toshiba","HGST","WDC"],"other")
el["fail"]=pd.to_datetime(el.fail_date)
logs=[]
for i in range(1,9):
    s=pd.read_parquet(f"{d}/service_log_E{i}.parquet",columns=["service_date","drive_id","completed"])
    s=s[s.completed & s.drive_id.isin(el.drive_id)]; s["cell"]=f"E{i}"; logs.append(s)
s=pd.concat(logs); s["service_date"]=pd.to_datetime(s.service_date)
s=s.merge(el[["drive_id","fail"]],on="drive_id"); s["lead"]=(s.fail-s.service_date).dt.days
s=s[s.lead>=1]
g=s.groupby("drive_id").lead.agg(lambda x:set(np.select([x>30,(x>=7)&(x<=30),x<7],["early","timely","late"],"x")))
el["kinds"]=el.drive_id.map(g)
nc=el[~el.caught].copy()
def cat(k):
    if not isinstance(k,set): return "A never inspected in any policy"
    if k=={"early"}: return "B inspected only >30 days before failure (too early)"
    if k=={"late"}: return "C inspected only 1-6 days before failure (too late)"
    if k=={"early","late"}: return "D both too early and too late, never in 7-30"
    return "E timely on seed 0 only via another seed rule (check)"
nc["cat"]=nc.kinds.map(cat)
print("never caught:",len(nc),"of",len(el))
print(nc.cat.value_counts().to_string())
print(pd.crosstab(nc.cat,nc.make,margins=True).to_string())
