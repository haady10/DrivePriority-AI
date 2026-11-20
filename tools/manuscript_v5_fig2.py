"""Fig. 2 of manuscript V5 (20 Nov 2026): three amendment-A1 differences on the test period at B = 20, with event
(thick) and drive-model (thin) 95% intervals. Reads only published_results/phase5_A1/ (through tools/a1_tables.py).

Usage: python tools/manuscript_v5_fig2.py <out.png>     (needs pandas, pyarrow, matplotlib)"""
import sys
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).resolve().parent))
from a1_tables import a1_data
D = a1_data()
OUT = Path(sys.argv[1] if len(sys.argv) > 1 else "fig_v5_a1diff.png")
POLS = [("next|fresh", "Next day,\nfresh"), ("next|queue", "Next day,\nqueue"), ("cool|fresh", "Cooldown,\nfresh"), ("cool|queue", "Cooldown,\nqueue")]
DIFFS = [("M1-S1", "M1 − S1 (forest − frozen count)", "#2a78d6", "o"),
         ("M1-S4", "M1 − S4 (forest − quota count)", "#eb6834", "s"),
         ("M2-S4", "M2 − S4 (both with quotas)", "#1baf7a", "D")]
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#d9d8d4"
plt.rcParams.update({"font.family": "serif", "font.serif": ["Times New Roman", "Liberation Serif", "DejaVu Serif"],
                     "font.size": 7, "axes.edgecolor": MUTED, "axes.linewidth": 0.6, "xtick.color": MUTED, "ytick.color": INK})
fig, axes = plt.subplots(1, 2, figsize=(3.45, 2.75), sharey=True, dpi=300)
for ax, (tie, title) in zip(axes, [("drive", "Frozen ties"), ("day", "Rotating ties")]):
    for gi, (pol, _) in enumerate(POLS):
        for j, (c, _, col, mk) in enumerate(DIFFS):
            y = gi * 4 + j
            e = D["contrasts"][f"test|{tie}|20|{pol}|{c}"]
            m = D["model_ci"][f"test|{tie}|20|{pol}|{c}"]
            ax.plot([m[1] * 100, m[2] * 100], [y, y], color=col, lw=0.8, solid_capstyle="butt", zorder=2)
            ax.plot([e[1] * 100, e[2] * 100], [y, y], color=col, lw=2.6, solid_capstyle="butt", zorder=3)
            ax.plot(e[0] * 100, y, marker=mk, ms=3.2, color=col, mec="#fcfcfb", mew=0.5, zorder=4)
    ax.axvline(0, color=INK, lw=0.6, zorder=1)
    ax.set_xlim(-25, 25)
    ax.set_xticks([-20, -10, 0, 10, 20])
    ax.grid(axis="x", color=GRID, lw=0.4, zorder=0)
    ax.set_title(title, fontsize=7.5, color=INK, pad=3)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.tick_params(length=2, width=0.5)
    ax.set_xlabel("Points, test period, B = 20", fontsize=6.5, color=MUTED)
axes[0].set_yticks([gi * 4 + 1 for gi in range(4)])
axes[0].set_yticklabels([lab for _, lab in POLS], fontsize=6.5)
axes[0].invert_yaxis()
handles = [plt.Line2D([], [], color=col, marker=mk, lw=2.6, ms=3.2, mec="#fcfcfb", mew=0.5, label=lab) for _, lab, col, mk in DIFFS]
fig.legend(handles=handles, loc="upper center", ncol=1, frameon=False, fontsize=6.5, bbox_to_anchor=(0.56, 1.0), handlelength=1.6)
fig.tight_layout(rect=(0, 0, 1, 0.80), w_pad=0.6)
fig.savefig(OUT, bbox_inches="tight", pad_inches=0.02, facecolor="white")
print("saved", OUT)
