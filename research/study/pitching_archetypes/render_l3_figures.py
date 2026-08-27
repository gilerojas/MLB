"""Phase 4b -- render the Level-3 figure.

The whole L3 result in one panel: real pitcher data against three nulls of
increasing strictness. Beating Nulls A and B looks like a discovery until Null C
-- a single Gaussian cloud with the real covariance and no clusters in it by
construction -- tracks the real data exactly.

    ./mlb_env.nosync/bin/python research/study/pitching_archetypes/render_l3_figures.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from render_malliscore_article_graphics import COLORS, configure_style  # noqa: E402

OUT = Path(__file__).resolve().parent / "outputs"
DPI = 150


def main() -> None:
    configure_style()
    res = pd.read_csv(OUT / "l3_cluster_stability.csv")

    fig = plt.figure(figsize=(1200 / DPI, 675 / DPI), dpi=DPI)
    fig.text(0.075, 0.935, "Pitchers don't cluster either — the giveaway is the third null",
             fontsize=16.5, fontweight=700, color=COLORS["ink"])
    fig.text(0.075, 0.888,
             "275 starters, 2024–25. Bootstrap label agreement against nulls of "
             "increasing strictness.",
             fontsize=9.5, color=COLORS["muted"])
    fig.text(0.075, 0.855,
             "Only Null C keeps the real correlations — and it has no clusters in it at all.",
             fontsize=9.5, color=COLORS["muted"])

    ax = fig.add_axes([0.075, 0.235, 0.85, 0.55])
    ax.plot(res["k"], res["ari_real"], "-o", color=COLORS["forest"], lw=2.4, ms=6,
            label="real pitchers", zorder=5)
    ax.plot(res["k"], res["ari_null_gaussian"], "-o", color=COLORS["orange"], lw=2.4,
            ms=6, label="Null C — one Gaussian cloud, no clusters by construction", zorder=4)
    ax.plot(res["k"], res["ari_null_blocks"], "--s", color=COLORS["muted"], lw=1.5,
            ms=4, alpha=0.8, label="Null B — blocks shuffled between pitchers")
    ax.plot(res["k"], res["ari_null_columns"], "--^", color=COLORS["line"], lw=1.5,
            ms=4, label="Null A — every column shuffled")

    ax.set_xlabel("k (number of pitcher archetypes)")
    ax.set_ylabel("bootstrap stability (ARI)")
    ax.set_xticks(res["k"])
    ax.set_ylim(0, 1.0)
    ax.legend(frameon=False, fontsize=8.5, loc="upper right", ncol=1)
    ax.grid(axis="y", color=COLORS["line"], lw=0.7, alpha=0.7)
    ax.set_axisbelow(True)

    fig.text(0.075, 0.055,
             "Real pitchers beat the weak nulls easily — but that only proves the features "
             "correlate, which was never in doubt.\nAgainst a structureless cloud carrying "
             "those same correlations they partition no better, and lose outright at k=2 and 4.",
             fontsize=9, color=COLORS["muted"], linespacing=1.5)

    path = OUT / "l3_pitchers_vs_nulls.png"
    fig.savefig(path, dpi=DPI, facecolor=COLORS["paper"])
    plt.close(fig)
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
