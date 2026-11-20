"""Rebuilds the data behind every manuscript table (data.json) and the six figures from the published results.

Reads only published_results/phase5/*.zip (extracted to a temporary folder), published_results/phase5_post_test/ and
(since V4) published_results/phase5_A1/ through tools/a1_tables.py.
No number is typed by hand. The manuscript text itself is not part of this repository.

Usage: python tools/manuscript_tables_figures.py <out_dir>    (needs pandas, pyarrow, matplotlib)
"""
import json, re, sys
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

import tempfile, zipfile
REPO = Path(__file__).resolve().parents[1]
PR = REPO / "published_results" / "phase5_post_test"
OUT = Path(sys.argv[1] if len(sys.argv) > 1 else "manuscript_build"); FIG = OUT / "fig"; FIG.mkdir(exist_ok=True, parents=True)
_tmp = Path(tempfile.mkdtemp(prefix="dp_results_"))
for _name, _sub in (("phase5_test_full_results.zip", "fulltest"), ("phase5_valid_full_results.zip", "fullval")):
    zipfile.ZipFile(REPO / "published_results" / "phase5" / _name).extractall(_tmp / _sub)
T, V = _tmp / "fulltest", _tmp / "fullval"

def r1(x): return f"{round(float(x) * 100 + 1e-9, 1):.1f}"          # share -> percent, 1 dp
def sgn(x):
    v = round(float(x) * 100 + (1e-9 if x >= 0 else -1e-9), 1)
    return f"{v:+.1f}".replace("-", "−")
def ci(lo, hi): return f"[{sgn(lo)}, {sgn(hi)}]"
def cip(lo, hi): return f"[{r1(lo)}, {r1(hi)}]"

cells = {k: pd.read_csv(p / "cell_results.csv") for k, p in (("test", T), ("valid", V))}
con = {k: pd.read_csv(p / "rq1_contrasts.csv") for k, p in (("test", T), ("valid", V))}
rq2 = {k: pd.read_csv(p / "rq2_stability.csv") for k, p in (("test", T), ("valid", V))}
rq3 = {k: pd.read_csv(p / "rq3_horizons.csv") for k, p in (("test", T), ("valid", V))}
D = {}

# ---------- Table: primary cells ----------
order = ["E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8"]
name = {"smart_count": "SMART", "ml_h30": "ML"}
rei = {"next day": "next day", "7-day cooldown": "cooldown"}
rows = []; work = []
for e in order:
    t = cells["test"].query("group=='rq1_primary' and e_cell==@e").iloc[0]
    v = cells["valid"].query("group=='rq1_primary' and e_cell==@e").iloc[0]
    sch = "fresh" if "fresh" in t.scheduling else "queue"
    rows.append([e, name[t.score], rei[t.reinspection], sch, r1(t.coverage), cip(t.coverage_ci_low, t.coverage_ci_high),
                 r1(v.coverage), cip(v.coverage_ci_low, v.coverage_ci_high)])
    work.append([e, f"{t.completed:,.0f}", f"{t.cancelled:,.0f}", f"{t.distinct_drives_inspected:,.0f}", r1(t.repeat_share),
                 f"{t.inspections_per_timely_capture:.0f}", r1(t.any_prior_capture), f"{t.earliest_lead_median:.0f}"])
D["primary"] = rows; D["workload"] = work
t0 = cells["test"].query("group=='rq1_primary'").iloc[0]; v0 = cells["valid"].query("group=='rq1_primary'").iloc[0]
D["facts"] = {k: {"eligible": int(x.eligible_events), "days": int(x.decision_days), "meanN": float(x.mean_observed_drives),
                  "meanK": float(x.mean_capacity), "slots": int(x.slots)} for k, x in (("test", t0), ("valid", v0))}

# ---------- Table: contrasts across analysis sets ----------
def getc(per, s, p):
    d = con[per]; return d[(d["set"] == s) & (d["contrast"] == "delta_ml_minus_smart|" + p)].iloc[0]
sets = [("primary_B20", "Primary (frozen ties)"), ("tie_day", "Day-rotating ties"),
        ("score_smart_count_sum", "SMART count + sum"), ("score_gbm_h30", "GBM for DRF"),
        ("score_smart_sum", "SMART raw sum"), ("budget_B10", "Budget 10"), ("budget_B50", "Budget 50"), ("queue_q1-2-2", "Queue 1K/2K/2 d"), ("queue_q2-5-7", "Queue 2K/5K/7 d")]
pairs = ["next|fresh", "next|queue", "cool|fresh", "cool|queue"]
ct = []; raw = {}
for s, lab in sets:
    for per, tag in (("test", "Test"), ("valid", "2024")):
        row = [lab if per == "test" else "", tag]
        for p in pairs:
            x = getc(per, s, p)
            row.append(sgn(x.estimate)); row.append(ci(x.ci_low, x.ci_high))
            raw[f"{s}|{per}|{p}"] = [float(x.estimate), float(x.ci_low), float(x.ci_high), bool(x.interval_excludes_zero)]
        ct.append(row)
D["contrasts"] = ct; D["contrast_raw"] = raw
eff = {}
for per in ("test", "valid"):
    for c in con[per].query("set=='primary_B20'").itertuples():
        eff[f"{per}|{c.contrast}"] = [sgn(c.estimate), ci(c.ci_low, c.ci_high)]
D["effects"] = eff
halves = {}
for per in ("test", "valid"):
    for p in pairs:
        xs = [getc(per, 'fleet_half' + str(i), p).estimate for i in range(5)]
        halves[f"{per}|{p}"] = [sgn(min(xs)), sgn(max(xs))]
D["halves"] = halves

# ---------- sensitivity coverages ----------
sens = {}
for per in ("test", "valid"):
    c = cells[per]
    for grp in ("sens_tie", "sens_source", "rq1_budget", "sens_queue"):
        for x in c[c.group == grp].itertuples():
            sens[f"{per}|{grp}|{x.score}|{x.reinspection}|{x.scheduling}|B{int(x.budget_per_10000)}|{x.tie_mode}"] = \
                [r1(x.coverage), cip(x.coverage_ci_low, x.coverage_ci_high), f"{x.distinct_drives_inspected:,.0f}", r1(x.repeat_share)]
D["sens"] = sens

# ---------- by make ----------
# reproducible by-make table (tools/phase5_post_test/by_make_table.py, engine bootstrap, seed 20261111)
bm = pd.concat([pd.read_csv(PR / "by_make_test.csv"), pd.read_csv(PR / "by_make_2024.csv")], ignore_index=True)
def first(s): return s.split(" ")[0]
mk = []
for per in ("test", "valid"):
    for m in ("Seagate", "HGST", "Toshiba", "WDC"):
        x = bm.query("period==@per and make==@m").iloc[0]
        mk.append([per, m, f"{int(x.events):,}"] + [first(x[e]) for e in order] + [x.d_next_fresh, x.d_cool_fresh, f"{x['never_captured_%']:.1f}"])
D["bymake"] = mk
D["bymake_raw"] = bm.to_dict(orient="records")

# ---------- RQ2 ----------
q = []
for sc in ("ml_h30", "smart_count"):
    for rule in ("daily", "confirm_2", "confirm_3", "ewma_0.5", "ewma_0.2", "retain_2", "retain_6"):
        a = rq2["test"].query("score==@sc and rule==@rule and reinspection=='next day'").iloc[0]
        b = rq2["test"].query("score==@sc and rule==@rule and reinspection=='7-day cooldown'").iloc[0]
        q.append([sc, rule, f"{a.list1K_mean_jaccard:.3f}", f"{a.list1K_entries_per_day:.1f}", r1(a.coverage), r1(b.coverage)])
D["rq2"] = q
D["rq2_valid_daily"] = {sc: f"{rq2['valid'][(rq2['valid'].score == sc) & (rq2['valid'].rule == 'daily')].iloc[0].list1K_mean_jaccard:.3f}" for sc in ("ml_h30", "smart_count")}

# ---------- RQ3 ----------
h = []
lab3 = {"ml_h7": "DRF 7-day", "ml_h14": "DRF 14-day", "ml_h30": "DRF 30-day", "urgency_first_7_14_30": "Urgency-first"}
for sc in ("ml_h7", "ml_h14", "ml_h30", "urgency_first_7_14_30"):
    row = [lab3[sc]]
    for per in ("test", "valid"):
        for re_ in ("next day", "7-day cooldown"):
            x = rq3[per].query("table=='budgeted_coverage' and score==@sc and reinspection==@re_").iloc[0]
            row += [r1(x.coverage)]
            if per == "test": row += [cip(x.coverage_ci_low, x.coverage_ci_high)]
    h.append(row)
D["rq3"] = h
pq = []
lab = {"ml_h7": "DRF 7-day", "ml_h14": "DRF 14-day", "ml_h30": "DRF 30-day", "gbm_h30": "GBM 30-day", "smart_count": "SMART count"}
for sc in lab:
    row = [lab[sc]]
    for per in ("test", "valid"):
        own = rq3[per][(rq3[per].table == "prediction_quality") & (rq3[per].score == sc) & rq3[per]["view"].str.startswith("own_label")].iloc[0]
        act = rq3[per][(rq3[per].table == "prediction_quality") & (rq3[per].score == sc) & (rq3[per]["view"] == "actionable_8_to_31d")].iloc[0]
        row += [f"{own.average_precision:.3f}", f"{own.roc_auc:.3f}", f"{act.average_precision:.3f}"]
    pq.append(row)
D["predq"] = pq
x = rq3["test"][(rq3["test"].table == "prediction_quality") & (rq3["test"]["view"] == "own_label_30d")].iloc[0]
D["rows_scored_test_30d"] = [int(x.rows), int(x.positives), float(x.prevalence)]
rnd = {per: [r1(x.coverage), f"{x.distinct_drives_inspected:,.0f}"] for per in ("test", "valid")
       for x in cells[per].query("group=='sens_source' and score=='random'").head(1).itertuples()}
D["random"] = rnd

# ---------- V3 additions (strict review of 19 Nov 2026): all read from saved files, nothing typed by hand ----------
A = PR
def mk_of(model):
    return ("Seagate" if model.startswith("ST") else "Toshiba" if model.startswith("TOSHIBA") else "HGST" if model.startswith("HGST")
            else "WDC" if (model.startswith("WD") or model.startswith("WUH")) else "other")
v3 = {"rot": {}, "seagate": {}, "rot_raw": {}}
for per in ("test", "valid"):
    c = cells[per]; P = T if per == "test" else V
    v3["rot"][per] = {}; v3["seagate"][per] = {}; v3["rot_raw"][per] = {}
    for x in c[c.group == "rq1_primary"].itertuples():
        y = c[(c.group == "sens_tie") & (c.score == x.score) & (c.reinspection == x.reinspection) & (c.scheduling == x.scheduling)].iloc[0]
        v3["rot"][per][x.e_cell] = [r1(y.coverage), cip(y.coverage_ci_low, y.coverage_ci_high), f"{y.distinct_drives_inspected:,.0f}"]
        v3["rot_raw"][per][x.e_cell] = float(y.coverage)
        lg = pd.read_parquet(P / f"service_log_{x.e_cell}.parquet", columns=["drive_id", "completed"])
        lg = lg[lg.completed]
        v3["seagate"][per][x.e_cell] = r1((lg.drive_id.str.split("|").str[1].map(mk_of) == "Seagate").mean())
tb = json.load(open(A / "time_block_bootstrap.json"))
fmt3 = lambda v: [sgn(v[0] / 100), ci(v[1] / 100, v[2] / 100)]
v3["tb"] = {per: {k: fmt3(v) for k, v in tb[per].items() if isinstance(v, list)} | {"weeks": tb[per]["weeks"], "largest": tb[per]["largest_week_events"]}
            for per in ("test", "valid")}
apn = {"D0_original": "D0", "D2_symmetric_conditional": "D2", "D1_symmetric_allocation": "D1"}
v3["ap"] = {}
for per in ("test", "valid"):
    a = json.load(open(A / f"actionable_ap_{'test' if per == 'test' else '2024'}.json"))["definitions"]
    v3["ap"][per] = {apn[k]: {"rows": d["ml_h7"]["rows"], "pos": d["ml_h7"]["positives"], "imminent": d["_rows_failing_1_to_7d"],
                              "prev": d["ml_h7"]["prevalence"],
                              **{s: f"{d[s]['average_precision']:.4f}" for s in ("ml_h7", "ml_h14", "ml_h30", "gbm_h30", "smart_count", "smart_sum")}}
                     for k, d in a.items()}
v3["sig"] = {per: json.load(open(A / f"signal_share_{'test' if per == 'test' else '2024'}.json")) for per in ("test", "valid")}
v3["day"] = json.load(open(A / "smart_count_day_facts_test.json"))
v3["day_valid"] = json.load(open(A / "smart_count_day_facts_2024.json"))
sec = {}
for per in ("test", "valid"):
    for x in cells[per].query("group=='rq1_primary'").itertuples():
        sec[f"{per}|{x.e_cell}"] = {"win": r1(x.coverage_window_alt), "long": r1(x.coverage_lead_long), "full": r1(x.coverage_full_opportunity_events),
                                    "full_n": int(x.full_opportunity_events), "late": r1(x.late_only), "never": r1(x.never_inspected_before_failure),
                                    "lead": f"{x.earliest_lead_median:.0f} ({x.earliest_lead_q25:.0f}–{x.earliest_lead_q75:.0f})",
                                    "prec": f"{x.inspection_precision_30d * 100:.1f}", "any": r1(x.any_prior_capture)}
v3["sec"] = sec
D["v3"] = v3
# ---------- V4: amendment A1 (post hoc fairer SMART baselines), from published_results/phase5_A1 ----------
sys.path.insert(0, str(Path(__file__).resolve().parent))
from a1_tables import a1_data
D["a1"] = a1_data()
json.dump(D, open(OUT / "data.json", "w"), indent=1, ensure_ascii=False)

# =================== FIGURES ===================
BLUE, ORANGE, INK, MUTE, GRID = "#2a78d6", "#eb6834", "#0b0b0b", "#52514e", "#d9d8d2"
plt.rcParams.update({"font.family": "Liberation Serif", "font.size": 7.5, "axes.edgecolor": MUTE, "axes.linewidth": 0.6,
                     "xtick.color": MUTE, "ytick.color": MUTE, "text.color": INK, "axes.labelcolor": INK,
                     "xtick.major.width": 0.5, "ytick.major.width": 0.5, "xtick.major.size": 2.5, "ytick.major.size": 2.5,
                     "savefig.dpi": 600, "figure.dpi": 150, "pdf.fonttype": 42})
W = 3.45
def clean(ax):
    for s in ("top", "right"): ax.spines[s].set_visible(False)

# ---- Fig 1: pipeline ----
fig, ax = plt.subplots(figsize=(W, 4.15)); ax.set_xlim(0, 10); ax.set_ylim(0, 12.6); ax.axis("off")
steps = [("Backblaze Drive Stats archives", "42 quarterly ZIP files, Jan 2016 to Jun 2026"),
         ("Per-quarter cleaning and validation", "schema, exact SMART values, duplicates, provenance evidence"),
         ("Central integration (one snapshot)", "709,242,263 drive-days, 501,437 drives, read-only"),
         ("Labels, past-only features, time splits", "HDD cohort; 7-, 14- and 30-day labels; train, validation, test by date"),
         ("Model training (H2O): LR, GLM, GBM, DRF", "protocol frozen first; primary model chosen on 2024 only"),
         ("Full-fleet scoring with the saved models", "every HDD, every day of 2024 and of the test period"),
         ("Stateful inspection replay", "decide on day t, inspect on t+1; capacity, cooldown, queue; 10 tie seeds"),
         ("Event-level evaluation", "timely capture 7 to 30 days before failure; paired bootstrap")]
y = 12.25; hbox = 1.18
for i, (a, b) in enumerate(steps):
    ax.add_patch(FancyBboxPatch((0.25, y - hbox), 9.5, hbox, boxstyle="round,pad=0.02,rounding_size=0.12", fc="#f4f6fa" if i < 3 else ("#fdf3ee" if i >= 6 else "#f7f7f5"), ec=MUTE, lw=0.6))
    ax.text(5, y - 0.40, a, ha="center", va="center", fontsize=7.6, fontweight="bold")
    ax.text(5, y - 0.86, b, ha="center", va="center", fontsize=6.3, color=MUTE)
    if i < len(steps) - 1:
        ax.annotate("", xy=(5, y - hbox - 0.36), xytext=(5, y - hbox - 0.02), arrowprops=dict(arrowstyle="-|>", color=MUTE, lw=0.7, mutation_scale=7))
    y -= hbox + 0.38
fig.savefig(FIG / "fig1_pipeline.png", bbox_inches="tight", pad_inches=0.02); plt.close(fig)

# ---- Fig 2: timeline ----
fig, ax = plt.subplots(figsize=(W, 1.6)); ax.set_xlim(-37.5, 6.5); ax.set_ylim(-2.75, 2.9); ax.axis("off")
ax.plot([-36, 0], [0, 0], color=MUTE, lw=0.8)
ax.add_patch(plt.Rectangle((-30, -0.4), 23, 0.8, fc=BLUE, alpha=0.22, ec=BLUE, lw=0.6))
ax.add_patch(plt.Rectangle((-6, -0.4), 5, 0.8, fc="#b9b8b2", alpha=0.45, ec=MUTE, lw=0.5, hatch="////"))
for xx, lab_ in ((-30, "f \u2212 30"), (-7, "f \u2212 7"), (0, "f")):
    ax.plot([xx, xx], [-0.62, 0.62], color=INK, lw=0.7); ax.text(xx, -2.35, lab_, ha="center", va="center", fontsize=7)
ax.text(-18.5, -1.1, "timely: lead 7 to 30 days", ha="center", va="center", fontsize=6.8)
ax.text(-3.4, -1.1, "too late\n(1 to 6)", ha="center", va="center", fontsize=6.0, color=MUTE, linespacing=0.95)
ax.text(-33.6, -1.1, "too early\n(> 30)", ha="center", va="center", fontsize=6.0, color=MUTE, linespacing=0.95)
ax.text(0.7, 0.0, "first recorded\nfailure", ha="left", va="center", fontsize=6.0, color=MUTE, linespacing=0.95)
ax.annotate("", xy=(-20.2, 0.45), xytext=(-21.6, 2.15), arrowprops=dict(arrowstyle="-|>", color=ORANGE, lw=0.8, mutation_scale=7))
ax.plot([-21.7], [2.25], marker="s", ms=4, color=ORANGE); ax.plot([-20], [0.0], marker="o", ms=4.5, color=ORANGE, zorder=5)
ax.text(-22.6, 2.3, "decision at end of day t (data up to t only)", ha="right", va="center", fontsize=6.4)
ax.text(-19.6, 1.2, "completed inspection on day s = t + 1", ha="left", va="center", fontsize=6.4)
ax.text(-18.5, -1.75, "lead = f \u2212 s", ha="center", va="center", fontsize=6.4, style="italic", color=MUTE)
fig.savefig(FIG / "fig2_timeline.png", bbox_inches="tight", pad_inches=0.02); plt.close(fig)

# ---- Fig 3: primary cells ----
fig, ax = plt.subplots(figsize=(W, 2.25)); clean(ax)
groups = [("next day\nfresh Top-K", "E1", "E2"), ("next day\nqueue", "E5", "E6"), ("7-day cooldown\nfresh Top-K", "E3", "E4"), ("7-day cooldown\nqueue", "E7", "E8")]
for gi, (lab_, es, em) in enumerate(groups):
    for e, col, dx, mkr in ((es, BLUE, -0.17, "o"), (em, ORANGE, 0.17, "s")):
        t = cells["test"].query("group=='rq1_primary' and e_cell==@e").iloc[0]; v = cells["valid"].query("group=='rq1_primary' and e_cell==@e").iloc[0]
        ax.bar(gi + dx, t.coverage * 100, width=0.3, color=col, lw=0, zorder=2)
        ax.errorbar(gi + dx, t.coverage * 100, yerr=[[(t.coverage - t.coverage_ci_low) * 100], [(t.coverage_ci_high - t.coverage) * 100]], color=INK, lw=0.7, capsize=1.6, capthick=0.6, zorder=3)
        ax.plot(gi + dx, v.coverage * 100, marker="D", ms=3.2, mfc="white", mec=INK, mew=0.7, ls="none", zorder=4)
        top = max(t.coverage_ci_high, v.coverage + 0.012)
        if e == es:   # SMART: test capture with day-rotating ties (pre-specified sensitivity); ML is unchanged by the tie rule
            rc = cells["test"][(cells["test"].group == "sens_tie") & (cells["test"].score == t.score) & (cells["test"].reinspection == t.reinspection)
                               & (cells["test"].scheduling == t.scheduling)].iloc[0].coverage
            ax.plot(gi + dx - 0.1, rc * 100, marker="^", ms=3.8, mfc=INK, mec="white", mew=0.4, ls="none", zorder=5)
            top = max(top, rc + 0.012)
        ax.text(gi + dx, top * 100 + 1.3, r1(t.coverage), ha="center", va="bottom", fontsize=6.4)
ax.set_xticks(range(4)); ax.set_xticklabels([g[0] for g in groups], fontsize=6.8); ax.tick_params(axis="x", length=0)
ax.set_ylabel("Timely capture (% of eligible failures)", fontsize=7); ax.set_ylim(0, 62); ax.set_yticks(range(0, 61, 10))
ax.yaxis.grid(True, color=GRID, lw=0.5, zorder=0); ax.set_axisbelow(True)
hs = [plt.Rectangle((0, 0), 1, 1, fc=BLUE), plt.Rectangle((0, 0), 1, 1, fc=ORANGE), plt.Line2D([], [], marker="D", ms=3.2, mfc="white", mec=INK, mew=0.7, ls="none"),
      plt.Line2D([], [], marker="^", ms=3.8, mfc=INK, mec="white", mew=0.4, ls="none")]
ax.legend(hs, ["SMART count (test)", "ML 30-day DRF (test)", "2024 validation", "SMART, rotating ties (test)"], loc="upper left", frameon=False,
          fontsize=6.4, handlelength=1.2, borderaxespad=0.1, labelspacing=0.25)
fig.savefig(FIG / "fig3_primary.png", bbox_inches="tight", pad_inches=0.02); plt.close(fig)

# ---- Fig 4: forest of contrasts ----
fsets = [("primary_B20", "Primary\n(frozen ties)"), ("tie_day", "Day-rotating\nties"), ("score_smart_count_sum", "SMART count\n+ sum"),
         ("score_gbm_h30", "GBM in place\nof DRF"), ("budget_B10", "Budget 10\nper 10,000"), ("budget_B50", "Budget 50\nper 10,000")]
fig, axs = plt.subplots(1, 2, figsize=(W, 2.55), sharey=True, gridspec_kw={"wspace": 0.08})
for ax, (p, ttl) in zip(axs, (("next|fresh", "Next-day re-inspection"), ("cool|fresh", "7-day cooldown"))):
    clean(ax); ax.axvline(0, color=MUTE, lw=0.7, zorder=1)
    for i, (s, _) in enumerate(fsets):
        yy = len(fsets) - 1 - i
        for per, dy, kw in (("test", 0.14, dict(marker="o", ms=3.6, mfc=INK, mec=INK)), ("valid", -0.14, dict(marker="D", ms=3.2, mfc="white", mec=INK, mew=0.7))):
            e, lo, hi, _z = raw[f"{s}|{per}|{p}"]
            ax.plot([lo * 100, hi * 100], [yy + dy, yy + dy], color=INK, lw=0.8, zorder=2); ax.plot(e * 100, yy + dy, ls="none", zorder=3, **kw)
    ax.set_title(ttl + ", fresh Top-K", fontsize=6.8, pad=3); ax.xaxis.grid(True, color=GRID, lw=0.5, zorder=0); ax.set_axisbelow(True)
    ax.set_xlabel("ML − SMART (points)", fontsize=6.8); ax.tick_params(axis="y", length=0)
axs[0].set_yticks(range(len(fsets))); axs[0].set_yticklabels([l for _, l in fsets][::-1], fontsize=6.4)
axs[0].set_xlim(-2, 20); axs[1].set_xlim(-9, 12); axs[0].set_ylim(-0.6, len(fsets) - 0.4)
hs = [plt.Line2D([], [], marker="o", ms=3.6, color=INK, lw=0.8), plt.Line2D([], [], marker="D", ms=3.2, mfc="white", mec=INK, color=INK, lw=0.8)]
fig.legend(hs, ["Test (2025 to Jun 2026)", "Validation (2024)"], loc="lower center", ncol=2, frameon=False, fontsize=6.4, bbox_to_anchor=(0.55, -0.13), handlelength=1.6)
fig.savefig(FIG / "fig4_contrasts.png", bbox_inches="tight", pad_inches=0.02); plt.close(fig)

# ---- Fig 5: by make ----
def parse(s):
    m = re.match(r"([\d.]+) \[([\d.]+),([\d.]+)\]", s); return [float(g) for g in m.groups()]
fig, axs = plt.subplots(1, 2, figsize=(W, 2.1), sharey=True, gridspec_kw={"wspace": 0.07})
makes = ["Seagate", "HGST", "Toshiba", "WDC"]
for ax, (es, em, ttl) in zip(axs, (("E1", "E2", "Next day, fresh Top-K"), ("E3", "E4", "7-day cooldown, fresh Top-K"))):
    clean(ax)
    for i, m in enumerate(makes):
        x = bm.query("period=='test' and make==@m").iloc[0]
        for e, col, dx in ((es, BLUE, -0.18), (em, ORANGE, 0.18)):
            v, lo, hi = parse(x[e]); ax.bar(i + dx, v, width=0.32, color=col, lw=0, zorder=2)
            ax.errorbar(i + dx, v, yerr=[[v - lo], [hi - v]], color=INK, lw=0.6, capsize=1.4, capthick=0.5, zorder=3)
            ax.text(i + dx, hi + 1.5, f"{v:.0f}" if v >= 1 else "0", ha="center", va="bottom", fontsize=6)
    ax.set_xticks(range(4)); ax.set_xticklabels(makes, fontsize=6.6); ax.tick_params(axis="x", length=0)
    ax.set_title(ttl, fontsize=6.8, pad=3); ax.yaxis.grid(True, color=GRID, lw=0.5, zorder=0); ax.set_axisbelow(True); ax.set_ylim(0, 84)
axs[0].set_ylabel("Timely capture (%)", fontsize=7)
hs = [plt.Rectangle((0, 0), 1, 1, fc=BLUE), plt.Rectangle((0, 0), 1, 1, fc=ORANGE)]
fig.legend(hs, ["SMART count", "ML 30-day DRF"], loc="lower center", ncol=2, frameon=False, fontsize=6.4, bbox_to_anchor=(0.54, -0.085), handlelength=1.2)
fig.savefig(FIG / "fig5_bymake.png", bbox_inches="tight", pad_inches=0.02); plt.close(fig)

# ---- Fig 6: RQ2 ----
fig, ax = plt.subplots(figsize=(W, 2.05)); clean(ax)
lbl = {"daily": "daily", "confirm_2": "confirm 2", "confirm_3": "confirm 3", "ewma_0.5": "EWMA 0.5", "ewma_0.2": "EWMA 0.2", "retain_2": "retain 2", "retain_6": "retain 6"}
off = {"daily": (-0.0011, 0.0, "right", "center"), "confirm_2": (0.0, -0.22, "center", "top"), "confirm_3": (0.0, -0.22, "center", "top"),
       "ewma_0.5": (0.0011, 0.0, "left", "center"), "ewma_0.2": (0.0, 0.2, "center", "bottom"), "retain_2": (0.0, 0.2, "center", "bottom"), "retain_6": (0.0, 0.2, "center", "bottom")}
d = rq2["test"].query("score=='ml_h30' and reinspection=='7-day cooldown'")
for x in d.itertuples():
    ax.plot(x.list1K_mean_jaccard, x.coverage * 100, marker="s", ms=4.5, color=ORANGE, mec="white", mew=0.5, zorder=3)
    dx, dy, ha, va = off[x.rule]; ax.text(x.list1K_mean_jaccard + dx, x.coverage * 100 + dy, lbl[x.rule], ha=ha, va=va, fontsize=6.4)
ax.set_xlabel("Day-to-day overlap of the top-K list (mean Jaccard)", fontsize=7); ax.set_ylabel("Timely capture (%)", fontsize=7)
ax.set_xlim(0.899, 0.95); ax.set_ylim(40.5, 45.5); ax.yaxis.grid(True, color=GRID, lw=0.5, zorder=0); ax.set_axisbelow(True)
fig.savefig(FIG / "fig6_rq2.png", bbox_inches="tight", pad_inches=0.02); plt.close(fig)
print("ok")
