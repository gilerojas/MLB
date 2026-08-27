"""Phase 5b -- render the Level-4 figure.

Each target's reliability ceiling against what the model actually reached. The
ceiling is the honest denominator: a target that is mostly noise cannot be
predicted by anything, so failing on it says nothing about the coordinates.

    ./mlb_env.nosync/bin/python research/study/pitching_archetypes/render_l4_figures.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from render_malliscore_article_graphics import COLORS, configure_style  # noqa: E402

OUT = Path(__file__).resolve().parent / "outputs"
DPI = 150

LABELS = {
    "platoon_split": "platoon split",
    "tto_penalty": "3rd-time-through penalty",
    "swstr_rate": "swinging-strike rate",
    "gb_rate": "ground-ball rate",
    "k_rate": "strikeout rate",
    "bb_rate": "walk rate",
}


def main() -> None:
    configure_style()
    rel = pd.read_csv(OUT / "l4_target_reliability.csv").set_index("target")
    pred = pd.read_csv(OUT / "l4_prediction.csv")
    got = pred[pred["model"] == "baseline + coords"].set_index("target")["cv_r2"]
    base = pred[pred["model"] == "baseline (quality+workload)"].set_index("target")["cv_r2"]

    order = ["gb_rate", "swstr_rate", "k_rate", "bb_rate", "tto_penalty", "platoon_split"]
    y = np.arange(len(order))

    fig = plt.figure(figsize=(1200 / DPI, 675 / DPI), dpi=DPI)
    fig.text(0.05, 0.935, "The coordinates describe a pitcher, not his matchups",
             fontsize=16.5, fontweight=700, color=COLORS["ink"])
    fig.text(0.05, 0.885,
             "179 starters, 2024–25. Grey shows how much of each target is even predictable;",
             fontsize=9.5, color=COLORS["muted"])
    fig.text(0.05, 0.851,
             "the bars inside it show how far a cross-validated model actually got.",
             fontsize=9.5, color=COLORS["muted"])

    ax = fig.add_axes([0.27, 0.34, 0.62, 0.46])
    ax.barh(y, [rel.loc[t, "ceiling_r2"] for t in order], height=0.66,
            color=COLORS["line"], label="attainable ceiling (target reliability)")
    ax.barh(y, [max(base.get(t, 0), 0) for t in order], height=0.66,
            color=COLORS["olive"], label="baseline: pitcher quality + workload")
    ax.barh(y, [max(got.get(t, 0), 0) for t in order], height=0.32,
            color=COLORS["forest"], label="baseline + continuum coordinates")

    ax.set_yticks(y)
    ax.set_yticklabels([LABELS[t] for t in order])
    ax.invert_yaxis()
    ax.set_xlabel("out-of-sample R²", labelpad=6)
    ax.set_xlim(0, 1.0)
    ax.grid(axis="x", color=COLORS["line"], lw=0.7, alpha=0.7)
    ax.set_axisbelow(True)
    ax.legend(frameon=False, fontsize=8, loc="upper center",
              bbox_to_anchor=(0.46, -0.24), ncol=3, handlelength=1.2,
              columnspacing=1.4)

    # order is [4 descriptive, 2 conditioning]; with the axis inverted the split
    # sits between rows 3 and 4.
    ax.axhline(3.5, color=COLORS["ink"], lw=1, alpha=0.35)
    ax.text(0.99, -0.46, "traits the arsenal actually determines",
            fontsize=8, color=COLORS["muted"], ha="right", style="italic")
    ax.text(0.99, 4.45, "matchup questions — the targets are almost pure noise",
            fontsize=8, color=COLORS["muted"], ha="right", style="italic")

    fig.text(0.05, 0.045,
             "Ground-ball rate is the clearest win: pitcher quality predicts none of it, arsenal "
             "shape predicts a third.\nThe two matchup targets are ~92% and ~81% noise at this "
             "sample size, so nothing could have predicted them.",
             fontsize=9, color=COLORS["muted"], linespacing=1.5)

    path = OUT / "l4_ceiling_vs_achieved.png"
    fig.savefig(path, dpi=DPI, facecolor=COLORS["paper"])
    plt.close(fig)
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
