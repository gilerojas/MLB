"""Phase 2c -- render the Level-1 exploration figures.

Two claims from Phase 2/2b that are easier to see than to read:

  l1_shape_continuum.png    the mirrored movement space is one connected sheet
                            with density ridges, and the crowdsourced pitch_type
                            labels sit on top of it as overlapping blobs, not
                            islands.
  l1_stability_vs_null.png  bootstrap stability of the real data against a
                            marginal-preserving null. If the two curves track
                            each other, the "clusters" are not structure.

    ./mlb_env.nosync/bin/python research/study/pitching_archetypes/render_l1_figures.py
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

from research.study.pitching_archetypes.explore_shape_space import load  # noqa: E402
from research.study.pitching_archetypes.features import SEED  # noqa: E402

OUT = Path(__file__).resolve().parent / "outputs"
DPI = 150

LABEL_TYPES = ["FF", "SI", "FC", "SL", "ST", "CU", "CH", "FS", "KC", "SV"]


def fig_continuum(df: pd.DataFrame) -> None:
    rng = np.random.default_rng(SEED)
    d = df.iloc[rng.choice(len(df), size=200_000, replace=False)]

    fig = plt.figure(figsize=(1200 / DPI, 675 / DPI), dpi=DPI)
    fig.text(0.065, 0.935, "Two ridges, one connected sheet — and labels that overlap it",
             fontsize=16.5, fontweight=700, color=COLORS["ink"])
    fig.text(0.065, 0.888,
             "2025 regular season, 705,059 pitches, mirrored into a right-handed frame.",
             fontsize=9.5, color=COLORS["muted"])
    fig.text(0.065, 0.855,
             "The two lobes are joined by a populated bridge, and the labels overlap it.",
             fontsize=9.5, color=COLORS["muted"])

    ax1 = fig.add_axes([0.075, 0.155, 0.375, 0.63])
    hb = ax1.hexbin(d["pfx_x_mir_in"], d["pfx_z_in"], gridsize=70, bins="log",
                    cmap="BuGn", mincnt=1, linewidths=0)
    ax1.set_xlabel("horizontal break, in   (− arm side  →  + glove side)")
    ax1.set_ylabel("induced vertical break, in", labelpad=2)
    ax1.set_title("density of all pitches", fontsize=11, color=COLORS["muted"], pad=8)
    cb = fig.colorbar(hb, ax=ax1, fraction=0.045, pad=0.02)
    cb.set_label("pitches (log)", fontsize=8, color=COLORS["muted"])
    cb.ax.tick_params(labelsize=7)

    ax2 = fig.add_axes([0.575, 0.155, 0.375, 0.63])
    ax2.hexbin(d["pfx_x_mir_in"], d["pfx_z_in"], gridsize=70, bins="log",
               cmap="Greys", mincnt=1, linewidths=0, alpha=0.45)
    palette = plt.get_cmap("tab10")
    for i, pt in enumerate(LABEL_TYPES):
        sub = d[d["pitch_type"] == pt]
        if len(sub) < 200:
            continue
        # One-sigma ellipse per label: if labels were families, these would
        # tile the space. They overlap heavily instead.
        cov = np.cov(sub["pfx_x_mir_in"], sub["pfx_z_in"])
        vals, vecs = np.linalg.eigh(cov)
        ang = np.degrees(np.arctan2(*vecs[:, -1][::-1]))
        w, h = 2 * np.sqrt(vals[::-1])
        e = matplotlib.patches.Ellipse(
            (sub["pfx_x_mir_in"].mean(), sub["pfx_z_in"].mean()),
            w, h, angle=ang, facecolor="none", edgecolor=palette(i % 10), lw=1.8)
        ax2.add_patch(e)
        ax2.annotate(pt, (sub["pfx_x_mir_in"].mean(), sub["pfx_z_in"].mean()),
                     color=palette(i % 10), fontsize=9, fontweight=700,
                     ha="center", va="center")
    ax2.set_xlim(ax1.get_xlim())
    ax2.set_ylim(ax1.get_ylim())
    ax2.set_xlabel("horizontal break, in   (− arm side  →  + glove side)")
    ax2.set_title("crowdsourced pitch_type labels, 1-sigma", fontsize=11,
                  color=COLORS["muted"], pad=8)

    path = OUT / "l1_shape_continuum.png"
    fig.savefig(path, dpi=DPI, facecolor=COLORS["paper"])
    plt.close(fig)
    print(f"wrote {path}")


def fig_stability() -> None:
    res = pd.read_csv(OUT / "l1_q3b_stability_vs_null.csv")

    fig = plt.figure(figsize=(1200 / DPI, 675 / DPI), dpi=DPI)
    fig.text(0.075, 0.925, "Real pitch data is barely more clusterable than noise",
             fontsize=16.5, fontweight=700, color=COLORS["ink"])
    fig.text(0.075, 0.873,
             "Bootstrap label agreement (adjusted Rand) across resampled halves, against",
             fontsize=9.5, color=COLORS["muted"])
    fig.text(0.075, 0.840,
             "a null with identical marginals and no joint structure.",
             fontsize=9.5, color=COLORS["muted"])

    ax = fig.add_axes([0.075, 0.235, 0.85, 0.555])
    ax.plot(res["k"], res["ari_real"], "-o", color=COLORS["forest"], lw=2,
            ms=5, label="real shape data")
    ax.plot(res["k"], res["ari_null"], "-o", color=COLORS["orange"], lw=2,
            ms=5, label="marginal-preserving null")
    ax.fill_between(res["k"], res["ari_null"], res["ari_real"],
                    where=res["ari_real"] >= res["ari_null"],
                    color=COLORS["olive"], alpha=0.35, lw=0)
    ax.axhline(0, color=COLORS["line"], lw=1)
    ax.set_xlabel("k (number of pitch families)")
    ax.set_ylabel("bootstrap stability (ARI)")
    ax.set_xticks(res["k"])
    ax.set_ylim(0.5, 1.02)
    ax.legend(frameon=False, fontsize=9, loc="lower left")
    ax.grid(axis="y", color=COLORS["line"], lw=0.7, alpha=0.7)
    ax.set_axisbelow(True)

    fig.text(0.075, 0.055,
             "No k separates real structure from noise. The shaded excess is small,\n"
             "non-monotone, and negative at k=6, 11, 12 and 18 — a continuum, not a taxonomy.",
             fontsize=9, color=COLORS["muted"], linespacing=1.5)

    path = OUT / "l1_stability_vs_null.png"
    fig.savefig(path, dpi=DPI, facecolor=COLORS["paper"])
    plt.close(fig)
    print(f"wrote {path}")


def main() -> None:
    OUT.mkdir(exist_ok=True)
    configure_style()
    fig_continuum(load())
    fig_stability()


if __name__ == "__main__":
    main()
