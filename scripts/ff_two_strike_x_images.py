#!/usr/bin/env python3
"""Create X-ready image cards for the two-strike FF case study."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch


REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "outputs" / "case_studies" / "ff_two_strike_success"
IMG_DIR = OUT_DIR / "x_images"

COLORS = {
    "charcoal": "#1A2530",
    "off_white": "#F5F2ED",
    "slate": "#9AA7B3",
    "green": "#66BB6A",
    "forest": "#2E7D32",
    "orange": "#E8712B",
    "cream": "#EDE8E0",
    "red": "#E74C3C",
}


def pct(v: float, nd: int = 1) -> str:
    return f"{v * 100:.{nd}f}%"


def base_fig():
    fig = plt.figure(figsize=(12, 6.75), dpi=150, facecolor=COLORS["charcoal"])
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    return fig, ax


def pill(ax, x, y, w, h, text, color=COLORS["orange"], text_color=COLORS["charcoal"]):
    ax.add_patch(
        FancyBboxPatch(
            (x, y),
            w,
            h,
            boxstyle="round,pad=0.012,rounding_size=0.02",
            linewidth=0,
            facecolor=color,
            alpha=0.95,
        )
    )
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", color=text_color, fontsize=14, weight="bold")


def footer(ax, note="2025 local sample | 132 pitchers | weighted by two-strike pitch count"):
    ax.text(0.06, 0.045, note, color=COLORS["slate"], fontsize=11, ha="left", va="center")
    ax.text(0.94, 0.045, "@Mallitalytics", color=COLORS["slate"], fontsize=12, ha="right", va="center", weight="bold")


def card_02_weighted_bars(corr: pd.DataFrame):
    rows = corr.set_index("metric").loc[["k_minus_bb_pct", "ff_velo", "ff_ivb_in", "ff_csw_pct"]].reset_index()
    labels = ["K-BB%", "FF velo", "FF IVB", "FF CSW%"]
    vals = rows["weighted_pearson_r"].astype(float).to_numpy()

    fig = plt.figure(figsize=(12, 6.75), dpi=150, facecolor=COLORS["charcoal"])
    ax = fig.add_axes([0.12, 0.18, 0.78, 0.58], facecolor=COLORS["charcoal"])
    y = np.arange(len(vals))
    bars = ax.barh(y, vals, color=[COLORS["orange"], COLORS["green"], COLORS["green"], COLORS["green"]], height=0.48)
    ax.set_yticks(y, labels=labels, color=COLORS["off_white"], fontsize=17, weight="bold")
    ax.set_xlim(0, 0.38)
    ax.invert_yaxis()
    ax.tick_params(axis="x", colors=COLORS["slate"], labelsize=12)
    ax.grid(axis="x", color="white", alpha=0.12)
    for spine in ax.spines.values():
        spine.set_visible(False)
    for bar, val in zip(bars, vals):
        ax.text(val + 0.01, bar.get_y() + bar.get_height() / 2, f"+{val:.3f}", color=COLORS["off_white"], va="center", fontsize=16, weight="bold")

    title = fig.add_axes([0, 0, 1, 1])
    title.axis("off")
    title.text(0.06, 0.88, "The signal is stronger in the pitch traits", color=COLORS["off_white"], fontsize=34, weight="bold")
    title.text(0.06, 0.81, "Weighted Pearson r vs two-strike FF%", color=COLORS["slate"], fontsize=17)
    footer(title)
    fig.savefig(IMG_DIR / "02_weighted_correlations.png", bbox_inches="tight", pad_inches=0)
    plt.close(fig)


def card_03_scatter(sample: pd.DataFrame):
    fig = plt.figure(figsize=(12, 6.75), dpi=150, facecolor=COLORS["charcoal"])
    ax = fig.add_axes([0.10, 0.17, 0.82, 0.60], facecolor=COLORS["charcoal"])
    x = sample["two_strike_ff_pct"] * 100
    y = sample["k_minus_bb_pct"] * 100
    sizes = np.clip(sample["two_strike_pitches"] / 4, 25, 175)
    ax.scatter(x, y, s=sizes, c=sample["ff_ivb_in"], cmap="Greens", alpha=0.76, edgecolor=COLORS["off_white"], linewidth=0.35)
    z = np.polyfit(x, y, 1)
    xs = np.linspace(float(x.min()), float(x.max()), 120)
    ax.plot(xs, z[0] * xs + z[1], color=COLORS["orange"], linewidth=2.4)
    for _, r in sample.sort_values("k_minus_bb_pct", ascending=False).head(6).iterrows():
        ax.annotate(
            str(r["pitcher_name"]).split(" ")[-1],
            (r["two_strike_ff_pct"] * 100, r["k_minus_bb_pct"] * 100),
            xytext=(5, 5),
            textcoords="offset points",
            fontsize=9,
            color=COLORS["off_white"],
        )
    ax.set_xlabel("Four-seam share with two strikes", color=COLORS["off_white"], fontsize=14, weight="bold")
    ax.set_ylabel("K-BB%", color=COLORS["off_white"], fontsize=14, weight="bold")
    ax.tick_params(colors=COLORS["slate"], labelsize=11)
    ax.grid(color="white", alpha=0.12)
    for spine in ax.spines.values():
        spine.set_color("#324252")

    title = fig.add_axes([0, 0, 1, 1])
    title.axis("off")
    title.text(0.06, 0.89, "Usage helps, but it is not the whole story", color=COLORS["off_white"], fontsize=31, weight="bold")
    title.text(0.06, 0.82, "Bubble size = two-strike pitch count | color = FF IVB", color=COLORS["slate"], fontsize=15)
    footer(title)
    fig.savefig(IMG_DIR / "03_ff_usage_vs_kbb_scatter.png", bbox_inches="tight", pad_inches=0)
    plt.close(fig)


def card_04_examples(sample: pd.DataFrame):
    cols = ["pitcher_name", "two_strike_ff_pct", "two_strike_pitches", "k_minus_bb_pct", "ff_velo", "ff_ivb_in", "ff_csw_pct"]
    leaders = sample.sort_values("two_strike_ff_pct", ascending=False).head(8)[cols]

    fig, ax = base_fig()
    ax.text(0.06, 0.88, "Highest two-strike four-seam usage", color=COLORS["off_white"], fontsize=34, weight="bold")
    ax.text(0.06, 0.815, "Sorted by 2K FF% | usage needs the quality context beside it", color=COLORS["slate"], fontsize=17)

    headers = ["Pitcher", "2K FF%", "2K P", "K-BB%", "Velo", "IVB", "FF CSW"]
    xs = [0.07, 0.39, 0.51, 0.61, 0.72, 0.81, 0.91]
    y0 = 0.72
    for x, h in zip(xs, headers):
        ax.text(x, y0, h, color=COLORS["orange"], fontsize=13, weight="bold", ha="right" if h != "Pitcher" else "left")
    ax.plot([0.06, 0.94], [0.69, 0.69], color=COLORS["slate"], alpha=0.35, linewidth=1)

    y = 0.64
    for _, r in leaders.iterrows():
        vals = [
            str(r["pitcher_name"]),
            pct(r["two_strike_ff_pct"]),
            f"{int(r['two_strike_pitches'])}",
            pct(r["k_minus_bb_pct"]),
            f"{r['ff_velo']:.1f}",
            f"{r['ff_ivb_in']:.1f}",
            pct(r["ff_csw_pct"]),
        ]
        for x, val, h in zip(xs, vals, headers):
            ax.text(
                x,
                y,
                val,
                color=COLORS["off_white"] if h == "Pitcher" else COLORS["cream"],
                fontsize=14,
                ha="right" if h != "Pitcher" else "left",
                va="center",
                weight="bold" if h == "Pitcher" else "normal",
            )
        y -= 0.065

    footer(ax)
    fig.savefig(IMG_DIR / "04_trust_backed_by_traits_table.png", bbox_inches="tight", pad_inches=0)
    plt.close(fig)


def main() -> None:
    IMG_DIR.mkdir(parents=True, exist_ok=True)
    corr = pd.read_csv(OUT_DIR / "correlations_ff_2025.csv")
    sample = pd.read_csv(OUT_DIR / "pitcher_two_strike_ff_sample_2025.csv")
    card_02_weighted_bars(corr)
    card_03_scatter(sample)
    card_04_examples(sample)
    print(f"Wrote images to {IMG_DIR}")


if __name__ == "__main__":
    main()
