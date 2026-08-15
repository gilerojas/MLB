#!/usr/bin/env python3
"""Render evidence graphics for the MalliScore methodology article."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

import matplotlib.font_manager as font_manager
import matplotlib.patches as patches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from PIL import Image, ImageChops

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.pitching_performances.malli_score import OutingRawMetrics, malliscore_v4

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


def render_score_architecture(output_dir: Path) -> Path:
    fig, ax = new_figure(
        "HOW MALLISCORE IS BUILT",
        "One starting-pitcher outing, two performance pillars, and one workload adjustment",
    )
    ax.remove()
    canvas = fig.add_axes([0.075, 0.205, 0.85, 0.55])
    canvas.set_xlim(0, 1)
    canvas.set_ylim(0, 1)
    canvas.axis("off")

    def pillar(x: float, y: float, width: float, height: float, color: str, title: str, rows: list[tuple[str, str]]) -> None:
        canvas.add_patch(
            patches.FancyBboxPatch(
                (x, y),
                width,
                height,
                boxstyle="round,pad=0.008,rounding_size=0.012",
                facecolor=COLORS["white"],
                edgecolor=COLORS["line"],
                linewidth=1.0,
            )
        )
        canvas.add_patch(patches.Rectangle((x, y + height - 0.035), width, 0.035, color=color, linewidth=0))
        canvas.text(x + 0.025, y + height - 0.09, title, fontsize=13, fontweight=700, color=COLORS["ink"])
        row_y = y + height - 0.17
        for label, weight in rows:
            canvas.text(x + 0.025, row_y, label, fontsize=9.2, color=COLORS["ink"])
            canvas.text(x + width - 0.025, row_y, weight, ha="right", fontsize=9.2, fontweight=700, color=color)
            row_y -= 0.075

    pillar(
        0.00,
        0.39,
        0.30,
        0.54,
        COLORS["forest"],
        "DOMINANCE",
        [("Swinging strikes", "30%"), ("Called strikes", "25%"), ("Chase rate", "20%"), ("xwOBA allowed", "25%")],
    )
    pillar(
        0.00,
        0.04,
        0.30,
        0.26,
        COLORS["orange"],
        "RUN PREVENTION",
        [("Reach Rate Allowed", "40%"), ("Earned runs", "35%"), ("Home runs", "25%")],
    )

    canvas.annotate("", xy=(0.49, 0.52), xytext=(0.32, 0.52), arrowprops={"arrowstyle": "-|>", "color": COLORS["ink"], "lw": 1.6})
    canvas.annotate("", xy=(0.49, 0.42), xytext=(0.32, 0.17), arrowprops={"arrowstyle": "-|>", "color": COLORS["ink"], "lw": 1.6})
    canvas.add_patch(
        patches.Ellipse(
            (0.56, 0.47),
            width=0.30,
            height=0.46,
            facecolor=COLORS["paper"],
            edgecolor=COLORS["ink"],
            linewidth=1.4,
        )
    )
    canvas.text(0.56, 0.575, "BALANCED", ha="center", fontsize=9.2, fontweight=700, color=COLORS["ink"])
    canvas.text(0.56, 0.465, "CORE", ha="center", fontsize=15, fontweight=700, color=COLORS["ink"])
    canvas.text(0.56, 0.375, "harmonic mean", ha="center", fontsize=8.2, color=COLORS["muted"])

    canvas.annotate("", xy=(0.74, 0.47), xytext=(0.70, 0.47), arrowprops={"arrowstyle": "-|>", "color": COLORS["ink"], "lw": 1.6})
    canvas.text(0.82, 0.75, "WORKLOAD", ha="center", fontsize=12, fontweight=700, color=COLORS["ink"])
    canvas.text(0.82, 0.68, "completed outs first", ha="center", fontsize=8.8, color=COLORS["muted"])
    work_x = np.array([0.74, 0.82, 0.90, 0.96])
    work_y = np.array([0.34, 0.53, 0.61, 0.66])
    canvas.plot(work_x, work_y, color=COLORS["forest"], linewidth=2.4)
    for x, y, label in zip(work_x, work_y, ["4 IP\n0.69x", "6 IP\n1.00x", "7 IP\n1.04x", "9 IP\n1.10x"]):
        canvas.scatter(x, y, s=35, color=COLORS["forest"], zorder=3)
        canvas.text(x, y - 0.09, label, ha="center", va="top", fontsize=7.8, fontweight=600, color=COLORS["ink"])

    canvas.text(0.56, 0.14, "CORE × WORKLOAD", ha="center", fontsize=12, fontweight=700, color=COLORS["forest"])
    add_footer(fig, "Current production formula · Starter outings only")
    return save(fig, output_dir / "01_score_architecture.png")


def render_disagreement(output_dir: Path) -> Path:
    article_stats = json.loads((STUDY_OUTPUTS / "article_complete_season_stats.json").read_text())
    disagreement = article_stats["disagreement"]
    labels = ["SWSTR%", "INNINGS", "EARNED RUNS"]
    malli_values = np.array(
        [disagreement["malli_swstr"], disagreement["malli_ip"], disagreement["malli_er"]]
    )
    game_values = np.array(
        [disagreement["gs_swstr"], disagreement["gs_ip"], disagreement["gs_er"]]
    )
    formats = ["{:.1f}%", "{:.1f}", "{:.1f}"]

    fig, ax = new_figure(
        "WHEN MALLISCORE AND GAME SCORE DISAGREE",
        "Average profile of the disagreement cases, complete 2024-2025 and 2026 through Aug 13",
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

    fig.text(
        0.075,
        0.162,
        f"MalliScore higher: n={disagreement['malli_n']:,}",
        fontsize=8.5,
        color=COLORS["forest"],
        fontweight=600,
    )
    fig.text(
        0.285,
        0.162,
        f"Game Score higher: n={disagreement['gs_n']:,}",
        fontsize=8.5,
        color=COLORS["ink"],
        fontweight=600,
    )
    add_footer(fig, "Game Score v2 benchmark · Disagreement defined by percentile gap")
    return save(fig, output_dir / "02_game_score_disagreement.png")


def render_weight_sensitivity(output_dir: Path) -> Path:
    surface = pd.read_csv(STUDY_OUTPUTS / "weight_sensitivity_surface.csv")
    rho = surface["spearman_vs_production"].to_numpy()
    minimum = float(np.min(rho))
    above = float(np.mean(rho > 0.98) * 100)

    fig, ax = new_figure(
        "20,000 WEIGHT SYSTEMS, NEARLY THE SAME ORDER",
        "Spearman rank correlation with the baseline weighting across feasible combinations · 2024",
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
    ax.set_xlabel("RANK CORRELATION WITH THE CURRENT V4 SCORE", fontsize=9, fontweight=600, labelpad=13)
    ax.set_ylabel("WEIGHT COMBINATIONS", fontsize=9, fontweight=600, labelpad=13)
    ax.grid(axis="y", color=COLORS["line"], linewidth=0.7, alpha=0.75)
    ax.set_axisbelow(True)
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.tick_params(axis="y", length=0)

    fig.text(0.12, 0.685, f"MINIMUM\n{minimum:.3f}", fontsize=11, fontweight=700, color=COLORS["orange"], linespacing=1.45)
    fig.text(0.925, 0.788, f"{above:.0f}% ABOVE .980", ha="right", fontsize=12.5, fontweight=700, color=COLORS["forest"])
    add_footer(fig, "Complete 2024 regular season: 4,749 starts · Each vector preserves pillar weight sums")
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


def render_same_line(output_dir: Path) -> Path:
    """Two outings with an identical traditional line and a 12.7-point MalliScore gap."""
    fig, ax = new_figure(
        "SAME LINE. DIFFERENT PERFORMANCE.",
        "7.0 IP · 4 H · 1 BB · 0 ER · 7 K · Game Score v2 of 79 — for both pitchers",
    )
    ax.remove()
    canvas = fig.add_axes([0.075, 0.175, 0.85, 0.59])
    canvas.set_xlim(0, 1)
    canvas.set_ylim(0, 1)
    canvas.axis("off")

    columns = (
        (0.335, "SHOTA IMANAGA", "May 18, 2024 vs PIT", 25.0, 42.5, 14.8, ".262", 69.4, 72.2, COLORS["forest"]),
        (0.695, "MICHAEL WACHA", "July 19, 2024 vs CWS", 8.4, 16.3, 20.0, ".236", 49.3, 59.5, COLORS["ink"]),
    )

    labels = ["Swinging strikes", "Chase rate", "Called strikes", "xwOBA allowed"]
    label_y = [0.775, 0.675, 0.575, 0.475]
    for label, y in zip(labels, label_y):
        canvas.text(0.0, y, label, fontsize=9.6, color=COLORS["muted"])
    canvas.text(0.0, 0.30, "DOMINANCE", fontsize=9.6, fontweight=700, color=COLORS["muted"])
    canvas.text(0.0, 0.10, "MALLISCORE", fontsize=9.6, fontweight=700, color=COLORS["muted"])

    for x, name, when, swstr, chase, called, xwoba, dominance, score, color in columns:
        canvas.text(x, 0.95, name, ha="center", fontsize=11.5, fontweight=700, color=color)
        canvas.text(x, 0.885, when, ha="center", fontsize=8.4, color=COLORS["muted"])
        values = [f"{swstr:.1f}%", f"{chase:.1f}%", f"{called:.1f}%", xwoba]
        emphasis = [True, True, False, False]
        for value, y, strong in zip(values, label_y, emphasis):
            canvas.text(
                x, y, value, ha="center",
                fontsize=15 if strong else 12,
                fontweight=700 if strong else 600,
                color=color if strong else COLORS["muted"],
            )
        canvas.add_patch(patches.Rectangle((x - 0.115, 0.275), 0.23, 0.048, facecolor=COLORS["line"], linewidth=0))
        canvas.add_patch(
            patches.Rectangle((x - 0.115, 0.275), 0.23 * dominance / 100.0, 0.048, facecolor=color, linewidth=0)
        )
        canvas.text(x, 0.215, f"{dominance:.1f}", ha="center", fontsize=11, fontweight=700, color=color)
        canvas.text(x, 0.055, f"{score:.1f}", ha="center", fontsize=30, fontweight=700, color=color)

    canvas.plot([0.515, 0.515], [0.02, 0.93], color=COLORS["line"], linewidth=1.0)
    add_footer(fig, "Identical traditional line and Game Score · Production scoring path")
    return save(fig, output_dir / "05_same_line.png")


def render_worked_example(output_dir: Path) -> Path:
    """One verified outing carried from raw line to final score."""
    fig, ax = new_figure(
        "ONE OUTING, ALL THE WAY THROUGH",
        "Matthew Liberatore vs Toronto · August 2, 2026 · 6.0 IP, 1 H, 1 BB, 0 ER, 7 K, 85 pitches",
    )
    ax.remove()
    canvas = fig.add_axes([0.075, 0.185, 0.85, 0.58])
    canvas.set_xlim(0, 1)
    canvas.set_ylim(0, 1)
    canvas.axis("off")

    canvas.text(0.0, 0.94, "WHAT HE DID", fontsize=9.5, fontweight=700, color=COLORS["muted"])
    inputs = [
        ("Swinging strikes", "12.9%"),
        ("Called strikes", "20.0%"),
        ("Chase rate", "39.6%"),
        ("xwOBA allowed", ".148"),
        ("Reach Rate Allowed", ".100"),
    ]
    row_y = 0.80
    for label, value in inputs:
        canvas.text(0.0, row_y, label, fontsize=9.6, color=COLORS["ink"])
        canvas.text(0.265, row_y, value, ha="right", fontsize=9.6, fontweight=700, color=COLORS["ink"])
        row_y -= 0.105

    canvas.annotate(
        "", xy=(0.345, 0.50), xytext=(0.295, 0.50),
        arrowprops={"arrowstyle": "-|>", "color": COLORS["ink"], "lw": 1.6},
    )

    canvas.text(0.375, 0.94, "TWO PILLARS", fontsize=9.5, fontweight=700, color=COLORS["muted"])
    for y, label, value, color in (
        (0.72, "Dominance", 66.3, COLORS["forest"]),
        (0.47, "Run prevention", 73.8, COLORS["orange"]),
    ):
        canvas.text(0.375, y + 0.10, label, fontsize=9.6, color=COLORS["ink"])
        canvas.add_patch(patches.Rectangle((0.375, y), 0.245, 0.055, facecolor=COLORS["line"], linewidth=0))
        canvas.add_patch(patches.Rectangle((0.375, y), 0.245 * value / 100.0, 0.055, facecolor=color, linewidth=0))
        canvas.text(0.632, y + 0.027, f"{value:.1f}", va="center", fontsize=12, fontweight=700, color=color)

    canvas.text(
        0.375, 0.29, "Harmonic mean  →  core 69.8",
        fontsize=10, fontweight=700, color=COLORS["ink"],
    )
    canvas.text(
        0.375, 0.19, "× 1.003 workload  (6 IP, 85 pitches)",
        fontsize=9.2, color=COLORS["muted"],
    )

    canvas.annotate(
        "", xy=(0.755, 0.50), xytext=(0.705, 0.50),
        arrowprops={"arrowstyle": "-|>", "color": COLORS["ink"], "lw": 1.6},
    )

    canvas.add_patch(
        patches.FancyBboxPatch(
            (0.775, 0.30),
            0.225,
            0.44,
            boxstyle="round,pad=0.008,rounding_size=0.02",
            facecolor=COLORS["white"],
            edgecolor=COLORS["line"],
            linewidth=1.0,
        )
    )
    canvas.text(0.888, 0.655, "MALLISCORE", ha="center", fontsize=9, fontweight=700, color=COLORS["muted"])
    canvas.text(0.888, 0.455, "70.0", ha="center", fontsize=38, fontweight=700, color=COLORS["forest"])
    canvas.text(0.888, 0.365, "top 2% of all starts", ha="center", fontsize=8.2, color=COLORS["muted"])

    strip_y = 0.10
    canvas.add_patch(patches.Rectangle((0.775, strip_y), 0.225, 0.035, facecolor=COLORS["line"], linewidth=0))
    for value, label, color in ((44.5, "median 44.5", COLORS["muted"]), (70.0, None, COLORS["forest"])):
        x = 0.775 + 0.225 * (value - 5.0) / (87.4 - 5.0)
        canvas.plot([x, x], [strip_y - 0.022, strip_y + 0.057], color=color, linewidth=2.0)
        if label:
            canvas.text(x, strip_y - 0.05, label, ha="center", fontsize=7, color=color)
    canvas.text(0.775, strip_y + 0.078, "5", fontsize=7, color=COLORS["muted"])
    canvas.text(1.0, strip_y + 0.078, "87", ha="right", fontsize=7, color=COLORS["muted"])

    add_footer(fig, "Verified production scoring path · Percentile vs 13,028 starts, 2024-2026")
    return save(fig, output_dir / "04_worked_example.png")


def load_v4_study_scores() -> np.ndarray:
    """Use production V4 scores from the complete-season outing dataset."""
    outings = pd.read_parquet(STUDY_OUTPUTS / "outings_2024_2026.parquet")
    if "malli_score_v4" not in outings.columns:
        raise RuntimeError("outings_2024_2026.parquet is missing malli_score_v4; rebuild the dataset")
    return pd.to_numeric(outings["malli_score_v4"], errors="coerce").dropna().to_numpy()


def render_score_distribution(output_dir: Path) -> Path:
    scores = load_v4_study_scores()
    percentile = {threshold: float((scores <= threshold).mean() * 100) for threshold in (50, 60, 70)}

    fig, ax = new_figure(
        "HOW TO READ MALLISCORE",
        "Distribution of 13,028 MLB starter outings · V4 production formula · 2024-2025 complete, 2026 through Aug 13",
    )
    ax.set_position([0.075, 0.305, 0.85, 0.43])
    bins = np.arange(5, 91, 5)
    counts, edges, bars = ax.hist(
        scores,
        bins=bins,
        color=COLORS["ink"],
        edgecolor=COLORS["paper"],
        linewidth=1.0,
    )
    for left, bar in zip(edges[:-1], bars):
        if left >= 70:
            bar.set_facecolor(COLORS["orange"])
        elif left >= 60:
            bar.set_facecolor(COLORS["forest"])
        elif left >= 50:
            bar.set_facecolor(COLORS["olive"])
        else:
            bar.set_alpha(0.72)

    max_count = float(max(counts))
    for threshold, color in (
        (50, COLORS["olive"]),
        (60, COLORS["forest"]),
        (70, COLORS["orange"]),
    ):
        ax.axvline(threshold, color=color, linewidth=1.8, linestyle=(0, (3, 3)))

    ax.set_xlim(5, 90)
    ax.set_ylim(0, max_count * 1.12)
    ax.set_ylabel("STARTER OUTINGS", fontsize=9, fontweight=700, labelpad=12)
    ax.set_xticks(np.arange(10, 91, 10))
    ax.grid(axis="y", color=COLORS["line"], linewidth=0.7, alpha=0.75)
    ax.set_axisbelow(True)
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.tick_params(axis="y", length=0)
    fig.text(
        0.075,
        0.225,
        "50 is not average. The median start scores 44.4.",
        fontsize=9.8,
        fontweight=600,
        color=COLORS["ink"],
    )
    for x, label, value, color in (
        (0.075, "50+  STRONG", f"Top {100 - percentile[50]:.0f}% of starts", COLORS["olive"]),
        (0.365, "60+  ELITE", f"Top {100 - percentile[60]:.0f}% of starts", COLORS["forest"]),
        (0.655, "70+  RARE", f"Top {100 - percentile[70]:.0f}% of starts", COLORS["orange"]),
    ):
        fig.text(x, 0.177, label, fontsize=9.4, fontweight=700, color=color)
        fig.text(x, 0.142, value, fontsize=8.5, color=COLORS["muted"])
    add_footer(fig, "Data: MLB / Statcast warehouse · Actual starter outings only")
    return save(fig, output_dir / "06_score_distribution.png")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    configure_style()
    outputs = [
        render_score_architecture(args.out),
        render_disagreement(args.out),
        render_weight_sensitivity(args.out),
        render_same_line(args.out),
        render_worked_example(args.out),
        render_predictive_boundary(args.out),
        render_score_distribution(args.out),
    ]
    for output in outputs:
        print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
