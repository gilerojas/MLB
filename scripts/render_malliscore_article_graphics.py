#!/usr/bin/env python3
"""Render evidence graphics for the MalliScore methodology article."""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

import matplotlib.font_manager as font_manager
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from PIL import Image, ImageChops


ROOT = Path(__file__).resolve().parents[1]
STUDY_OUTPUTS = ROOT / "research" / "study" / "malliscore_validation" / "outputs"
DEFAULT_OUT = ROOT / "outputs" / "articles" / "malliscore_v4"
LOGO_PATH = ROOT / "new_malli_logo" / "logo_horizontal.png"
FONT_PATH = ROOT / "assets" / "fonts" / "Montserrat.ttf"

COLORS = {
    "ink": "#2E3A43",
    "forest": "#4E7B62",
    "olive": "#A5B884",
    "orange": "#F97D34",
    "paper": "#F2EFE9",
    "muted": "#6F777C",
    "line": "#D8D2C7",
    "white": "#FBFAF7",
}

WIDTH = 1200
HEIGHT = 675
DPI = 150


def configure_style() -> None:
    if FONT_PATH.is_file():
        try:
            from fontTools.ttLib import TTFont
            from fontTools.varLib.instancer import instantiateVariableFont

            font_cache = Path(tempfile.gettempdir()) / "mallitalytics-matplotlib-fonts"
            font_cache.mkdir(parents=True, exist_ok=True)
            for weight, name in ((400, "Regular"), (700, "Bold")):
                static_path = font_cache / f"Montserrat-{name}.ttf"
                if not static_path.is_file():
                    variable = TTFont(FONT_PATH)
                    static = instantiateVariableFont(
                        variable,
                        {"wght": weight},
                        inplace=False,
                        updateFontNames=True,
                    )
                    static.save(static_path)
                font_manager.fontManager.addfont(str(static_path))
            family = "Montserrat"
        except (ImportError, KeyError, OSError):
            font_manager.fontManager.addfont(str(FONT_PATH))
            family = font_manager.FontProperties(fname=str(FONT_PATH)).get_name()
    else:
        family = "DejaVu Sans"
    plt.rcParams.update(
        {
            "font.family": family,
            "font.size": 11,
            "axes.facecolor": COLORS["paper"],
            "figure.facecolor": COLORS["paper"],
            "text.color": COLORS["ink"],
            "axes.labelcolor": COLORS["ink"],
            "xtick.color": COLORS["muted"],
            "ytick.color": COLORS["muted"],
            "axes.edgecolor": COLORS["line"],
            "axes.titleweight": 700,
        }
    )


def new_figure(title: str, subtitle: str) -> tuple[plt.Figure, plt.Axes]:
    fig = plt.figure(figsize=(WIDTH / DPI, HEIGHT / DPI), dpi=DPI)
    ax = fig.add_axes([0.075, 0.20, 0.85, 0.59])
    fig.text(0.075, 0.905, title, fontsize=16.5, fontweight=700, color=COLORS["ink"])
    fig.text(0.075, 0.855, subtitle, fontsize=9.5, color=COLORS["muted"])
    fig.add_artist(
        plt.Line2D(
            [0.075, 0.105],
            [0.945, 0.945],
            transform=fig.transFigure,
            color=COLORS["orange"],
            linewidth=4,
            solid_capstyle="butt",
        )
    )
    return fig, ax


def add_footer(fig: plt.Figure, source: str) -> None:
    fig.add_artist(
        plt.Line2D(
            [0.075, 0.925],
            [0.105, 0.105],
            transform=fig.transFigure,
            color=COLORS["line"],
            linewidth=0.8,
        )
    )
    if LOGO_PATH.is_file():
        logo = Image.open(LOGO_PATH).convert("RGBA")
        rgb = logo.convert("RGB")
        background = Image.new("RGB", rgb.size, rgb.getpixel((0, 0)))
        mask = ImageChops.difference(rgb, background).convert("L").point(lambda value: 255 if value > 18 else 0)
        bbox = mask.getbbox()
        if bbox:
            logo = logo.crop(bbox)
        logo_ax = fig.add_axes([0.075, 0.029, 0.15, 0.052])
        logo_ax.imshow(logo)
        logo_ax.axis("off")
    fig.text(0.925, 0.052, source, ha="right", va="center", fontsize=8, color=COLORS["muted"])


def save(fig: plt.Figure, output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, facecolor=COLORS["paper"], dpi=DPI)
    plt.close(fig)
    return output


def render_zero_collapse(output_dir: Path) -> Path:
    comparison = pd.read_csv(STUDY_OUTPUTS / "v4_candidate_comparison.csv")
    v3 = comparison[comparison["candidate"] == "A. V3 as shipped"].sort_values("season")
    v4 = comparison[comparison["candidate"] == "C. B + baserunners per BF"].sort_values("season")
    seasons = v3["season"].astype(str).tolist()
    v3_counts = v3["exact_zeros"].astype(int).to_numpy()
    v4_counts = v4["exact_zeros"].astype(int).to_numpy()

    fig, ax = new_figure(
        "V4 REMOVES THE ZERO-SCORE COLLAPSE",
        "Exact MalliScore zeroes among MLB starter outings",
    )
    ax.set_position([0.075, 0.205, 0.57, 0.56])
    x = np.arange(len(seasons))
    width = 0.28
    bars_v3 = ax.bar(x - width / 2, v3_counts, width, color=COLORS["orange"], label="V3")
    ax.bar(x + width / 2, v4_counts, width, color=COLORS["forest"], label="V4")
    ax.scatter(x + width / 2, np.full(len(x), 0.45), s=42, color=COLORS["forest"], zorder=4)

    for bar, value in zip(bars_v3, v3_counts):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            value + 1,
            str(value),
            ha="center",
            va="bottom",
            fontsize=12,
            fontweight=700,
            color=COLORS["ink"],
        )
    for xpos, value in zip(x + width / 2, v4_counts):
        ax.text(xpos, 2.1, str(value), ha="center", va="bottom", fontsize=10, fontweight=700, color=COLORS["forest"])

    ax.set_xticks(x, seasons, fontsize=11, fontweight=600)
    ax.set_ylim(0, max(v3_counts) + 8)
    ax.set_ylabel("OUTINGS AT EXACTLY 0", fontsize=9, fontweight=600, labelpad=12)
    ax.grid(axis="y", color=COLORS["line"], linewidth=0.7, alpha=0.75)
    ax.set_axisbelow(True)
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.legend(frameon=False, loc="upper left", ncols=2, fontsize=9)

    fig.text(0.725, 0.665, "76", fontsize=43, fontweight=700, color=COLORS["orange"], ha="center")
    fig.text(0.725, 0.585, "collapsed starts", fontsize=11, fontweight=600, color=COLORS["ink"], ha="center")
    fig.text(0.725, 0.532, "across 2024-2026", fontsize=10, color=COLORS["muted"], ha="center")
    fig.text(0.805, 0.665, "→", fontsize=33, fontweight=700, color=COLORS["line"], ha="center")
    fig.text(0.873, 0.665, "0", fontsize=43, fontweight=700, color=COLORS["forest"], ha="center")
    fig.text(0.80, 0.405, "2024 V3 zeroes", fontsize=9, fontweight=700, color=COLORS["muted"], ha="center")
    fig.text(0.80, 0.348, "Game Score v2: -19 to +21", fontsize=11, fontweight=650, color=COLORS["ink"], ha="center")
    fig.text(0.80, 0.285, "V4 MalliScore: 4.3 to 19.8", fontsize=11, fontweight=650, color=COLORS["forest"], ha="center")

    add_footer(fig, "Study: 7,479 starts · 2024 through July 26, 2026")
    return save(fig, output_dir / "01_v4_zero_collapse.png")


def render_disagreement(output_dir: Path) -> Path:
    labels = ["SWSTR%", "INNINGS", "EARNED RUNS"]
    malli_values = np.array([13.1, 17.8 / 3.0, 3.1])
    game_values = np.array([10.1, 14.4 / 3.0, 1.0])
    formats = ["{:.1f}%", "{:.1f}", "{:.1f}"]

    fig, ax = new_figure(
        "WHEN MALLISCORE AND GAME SCORE DISAGREE",
        "Average profile of the clearest 2024 disagreement cases",
    )
    ax.remove()
    positions = [(0.075, 0.235, 0.25, 0.49), (0.375, 0.235, 0.25, 0.49), (0.675, 0.235, 0.25, 0.49)]

    for index, (label, malli, game, fmt, pos) in enumerate(zip(labels, malli_values, game_values, formats, positions)):
        panel = fig.add_axes(pos)
        max_value = max(malli, game) * 1.22
        panel.barh([1], [malli], height=0.31, color=COLORS["forest"])
        panel.barh([0], [game], height=0.31, color=COLORS["ink"], alpha=0.82)
        panel.text(0, 1.53, label, fontsize=11, fontweight=700, color=COLORS["ink"])
        panel.text(malli + max_value * 0.025, 1, fmt.format(malli), va="center", fontsize=14, fontweight=700, color=COLORS["forest"])
        panel.text(game + max_value * 0.025, 0, fmt.format(game), va="center", fontsize=14, fontweight=700, color=COLORS["ink"])
        panel.set_xlim(0, max_value)
        panel.set_ylim(-0.65, 1.8)
        panel.set_yticks([])
        panel.set_xticks([])
        panel.spines[:].set_visible(False)
        if index == 0:
            panel.text(0.01 * max_value, 1, "MalliScore higher", va="center", fontsize=8, fontweight=700, color=COLORS["white"])
            panel.text(0.01 * max_value, 0, "Game Score higher", va="center", fontsize=8, fontweight=700, color=COLORS["white"])

    fig.text(0.075, 0.162, "MalliScore higher: n=109", fontsize=8.5, color=COLORS["forest"], fontweight=600)
    fig.text(0.275, 0.162, "Game Score higher: n=99", fontsize=8.5, color=COLORS["ink"], fontweight=600)
    add_footer(fig, "Game Score v2 benchmark · Disagreement defined by percentile gap")
    return save(fig, output_dir / "02_game_score_disagreement.png")


def render_weight_sensitivity(output_dir: Path) -> Path:
    surface = pd.read_csv(STUDY_OUTPUTS / "weight_sensitivity_surface.csv")
    rho = surface["spearman_vs_v3"].to_numpy()
    minimum = float(np.min(rho))
    above = float(np.mean(rho > 0.98) * 100)

    fig, ax = new_figure(
        "20,000 WEIGHT SYSTEMS, NEARLY THE SAME ORDER",
        "Spearman rank correlation with MalliScore V3 across feasible weight combinations · 2024",
    )
    ax.set_position([0.075, 0.235, 0.85, 0.52])
    bins = np.linspace(0.96, 1.0, 34)
    counts, edges, patches = ax.hist(rho, bins=bins, color=COLORS["forest"], edgecolor=COLORS["paper"], linewidth=0.8)
    for patch, left in zip(patches, edges[:-1]):
        if left < 0.98:
            patch.set_facecolor(COLORS["orange"])
    ax.axvline(0.98, color=COLORS["ink"], linewidth=1.2, linestyle=(0, (4, 4)))
    ax.text(0.9804, max(counts) * 0.94, ".980", fontsize=9, fontweight=700, color=COLORS["ink"], va="top")
    ax.set_xlim(0.96, 1.001)
    ax.set_xlabel("RANK CORRELATION WITH THE ORIGINAL SCORE", fontsize=9, fontweight=600, labelpad=13)
    ax.set_ylabel("WEIGHT COMBINATIONS", fontsize=9, fontweight=600, labelpad=13)
    ax.grid(axis="y", color=COLORS["line"], linewidth=0.7, alpha=0.75)
    ax.set_axisbelow(True)
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.tick_params(axis="y", length=0)

    fig.text(0.12, 0.705, f"MINIMUM\n{minimum:.3f}", fontsize=11, fontweight=700, color=COLORS["orange"], linespacing=1.45)
    fig.text(0.73, 0.705, f"{above:.0f}% ABOVE .980", fontsize=13, fontweight=700, color=COLORS["forest"])
    add_footer(fig, "Development season: 1,908 starts · Each vector preserves pillar weight sums")
    return save(fig, output_dir / "03_weight_sensitivity.png")


def render_predictive_boundary(output_dir: Path) -> Path:
    signal = pd.read_csv(STUDY_OUTPUTS / "predictive_signal.csv")
    rows = signal[signal["comparison"] == "MalliScore over GSv2"].copy()
    order = ["next_swstr_pct", "next_xwoba_allowed", "next_k_minus_bb_pct", "next_game_whip"]
    labels = ["Next-start SwStr%", "Next-start xwOBA", "Next-start K-BB%", "Next-start WHIP"]
    rows = rows.set_index("target").loc[order].reset_index()

    fig, ax = new_figure(
        "MALLISCORE ADDS NO MEANINGFUL NEXT-START SIGNAL",
        "Incremental R² beyond recent form and Game Score v2 · 95% confidence intervals",
    )
    ax.set_position([0.235, 0.225, 0.69, 0.535])
    y = np.arange(len(rows))[::-1]
    gains = rows["gain"].to_numpy()
    low = rows["ci_low"].to_numpy()
    high = rows["ci_high"].to_numpy()
    xerr = np.vstack([gains - low, high - gains])
    ax.errorbar(
        gains,
        y,
        xerr=xerr,
        fmt="o",
        markersize=7,
        color=COLORS["forest"],
        ecolor=COLORS["forest"],
        elinewidth=2.2,
        capsize=4,
        capthick=1.5,
    )
    ax.axvline(0, color=COLORS["ink"], linewidth=1.2)
    ax.axvspan(-0.005, 0.005, color=COLORS["olive"], alpha=0.18)
    ax.set_yticks(y, labels, fontsize=10.5, fontweight=600)
    ax.set_xlim(-0.006, 0.006)
    ax.set_xlabel("INCREMENTAL R²", fontsize=9, fontweight=600, labelpad=12)
    ax.grid(axis="x", color=COLORS["line"], linewidth=0.7, alpha=0.75)
    ax.set_axisbelow(True)
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.tick_params(axis="y", length=0, pad=10)
    ax.text(0.00485, 3.28, "±.005 practical threshold", ha="right", va="center", fontsize=8, color=COLORS["muted"])
    add_footer(fig, "n=2,056 linked starts · All four tests: NULL_CONFIRMED")
    return save(fig, output_dir / "04_next_start_signal.png")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    configure_style()
    outputs = [
        render_zero_collapse(args.out),
        render_disagreement(args.out),
        render_weight_sensitivity(args.out),
        render_predictive_boundary(args.out),
    ]
    for output in outputs:
        print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
