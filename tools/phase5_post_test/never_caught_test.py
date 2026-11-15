"""Read-only: which eligible test failures are never timely-captured by any of the 8 cells (any tie seed),
and were they inspected at all (seed 0 earliest lead)? Reads event_coverage.parquet only."""
import sys, pandas as pd
e = pd.read_parquet(sys.argv[1]); el = e[e.status=="eligible"].copy()
cells=[f"E{i}" for i in range(1,9)]
el["model"]=el.drive_id.str.split("|").str[1]
fam=el.model.str.extract(r"^([A-Za-z]+)",expand=False).str.upper()
el["make"]=fam.where(fam!="ST","SEAGATE").replace({"WUH":"WDC","WDC":"WDC"})
el.loc[el.model.str.startswith("ST"),"make"]="SEAGATE"
el.loc[el.model.str.upper().str.startswith("WD"),"make"]="WDC"
el["caught"]=(el[[c+"_timely_share" for c in cells]]>0).any(axis=1)
nc=el[~el.caught].copy(); print("eligible",len(el),"never caught",len(nc),f"{100*len(nc)/len(el):.1f}%")
print(el.groupby("make").agg(events=("caught","size"),never=("caught",lambda s:int((~s).sum()))).assign(pct=lambda d:(100*d.never/d.events).round(1)).sort_values("events",ascending=False))
leads=nc[[c+"_earliest_lead_seed0" for c in cells]]
nc["never_inspected_any_cell"]=(leads<0).all(axis=1)
nc["only_too_late"]=((leads>=0)&(leads<7)).any(axis=1)&~((leads>30).any(axis=1))
nc["any_too_early"]=(leads>30).any(axis=1)
print("\namong never-caught (seed-0 logs):")
print(" never inspected at all in any cell:",int(nc.never_inspected_any_cell.sum()), f"{100*nc.never_inspected_any_cell.mean():.1f}%")
print(" inspected, but only in the last 6 days:",int(nc.only_too_late.sum()))
print(" inspected >30 days before (too early) in some cell:",int(nc.any_too_early.sum()))
print(" other (inspected only on seeds !=0 or mix):",len(nc)-int(nc.never_inspected_any_cell.sum()+nc.only_too_late.sum()+(nc.any_too_early&~nc.only_too_late).sum()))
print("\nopportunity_days (days observed in the window) never-caught vs caught, median:",nc.opportunity_days.median(),el[el.caught].opportunity_days.median())
print(" never-caught with opportunity_days<10:",int((nc.opportunity_days<10).sum()))
print(pd.crosstab(nc.make,nc.never_inspected_any_cell,margins=True))
m=el.groupby("model").agg(events=("caught","size"),never=("caught",lambda s:int((~s).sum())))
m=m[m.events>=40].assign(pct=lambda d:(100*d.never/d.events).round(1)).sort_values("never",ascending=False)
print("\nmodels with >=40 events, by never-caught count:\n",m.head(15).to_string())
