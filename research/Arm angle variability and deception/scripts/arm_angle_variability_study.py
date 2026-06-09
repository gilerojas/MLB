#!/usr/bin/env python3
"""
Case study: does arm-angle variability relate to pitcher results?

The tunneling hypothesis is that less arm-angle variation can improve deception.
This script tests that idea at pitcher level with:

- overall arm-angle standard deviation
- overall arm-angle p90-p10 range
- weighted within-pitch-type arm-angle standard deviation
- weighted between-pitch-type arm-angle standard deviation

It skips macOS/iCloud dataless warehouse files to avoid blocked reads.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

import matplotlib

matplotlib.use(os.environ.get("MPLBACKEND", "Agg"))
import matplotlib.pyplot as plt


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUT = REPO / "research" / "Arm angle variability and deception"
DATALLESS_FLAG = 0x40000000
SWING_DESCRIPTIONS = {
    "foul_bunt",
    "foul",
    "hit_into_play",
    "swinging_strike",
    "foul_tip",
    "swinging_strike_blocked",
    "missed_bunt",
    "bunt_foul_tip",
}
WHIFF_DESCRIPTIONS = {"swinging_strike", "foul_tip", "swinging_strike_blocked"}
CALLED_STRIKE_DESCRIPTIONS = {"called_strike"}


def is_dataless(path: Path) -> bool:
    try:
        return bool(getattr(path.stat(), "st_flags", 0) & DATALLESS_FLAG)
    except OSError:
        return True


def discover_parquets(warehouse: Path, season: int, stage: str) -> list[Path]:
    root = warehouse / str(season) / stage / "pitches_enriched"
    if not root.is_dir():
        return []
    return [p for p in sorted(root.glob("*.parquet")) if not is_dataless(p)]


def raw_path_for_parquet(pq: Path) -> Path | None:
    m = re.match(r"game_(\d+)_(\d{8})_pitches_enriched\.parquet$", pq.name)
    if not m:
        return None
    raw_root = pq.parent.parent / "raw"
    stem = f"game_{m.group(1)}_{m.group(2)}_feed_live"
    for suffix in (".json.gz", ".json"):
        p = raw_root / f"{stem}{suffix}"
        if p.is_file():
            return p
    return None


def load_pitcher_names(parquets: list[Path], target_ids: set[int]) -> dict[int, str]:
    names: dict[int, str] = {}
    for pq in parquets:
        if target_ids.issubset(names.keys()):
            break
        rp = raw_path_for_parquet(pq)
        if rp is None or is_dataless(rp):
            continue
        try:
            opener = gzip.open if rp.suffix == ".gz" else open
            with opener(rp, "rt", encoding="utf-8") as f:
                feed = json.load(f)
        except Exception:
            continue
        for pdata in ((feed.get("gameData") or {}).get("players") or {}).values():
            if not isinstance(pdata, dict):
                continue
            pid = pdata.get("id")
            full = pdata.get("fullName")
            if pid is None or not full:
                continue
            try:
                ipid = int(pid)
            except (TypeError, ValueError):
                continue
            if ipid in target_ids:
                names[ipid] = str(full)
    return names


def normalize_description(s: pd.Series) -> pd.Series:
    d = s.astype(str).str.strip().str.lower().str.replace(" ", "_", regex=False).str.replace(",", "")
    return d.where(~d.str.contains("in_play|hit_into_play", na=False, regex=True), "hit_into_play")


def read_pitch_table(parquets: list[Path]) -> pd.DataFrame:
    columns = [
        "game_pk",
        "game_date",
        "pitcher",
        "pitch_type",
        "arm_angle",
        "description",
        "events",
        "estimated_woba_using_speedangle",
        "delta_pitcher_run_exp",
    ]
    if not parquets:
        return pd.DataFrame(columns=columns)
    frames: list[pd.DataFrame] = []
    for root in sorted({p.parent for p in parquets}):
        root_files = sorted(root.glob("*.parquet"))
        selected = [p for p in parquets if p.parent == root]
        if len(selected) == len(root_files):
            try:
                frames.append(ds.dataset(root, format="parquet").to_table(columns=columns).to_pandas())
                continue
            except Exception as exc:
                print(f"dataset scan failed for {root}: {exc}; falling back to per-file reads", flush=True)
        for pq in selected:
            try:
                frames.append(pd.read_parquet(pq, columns=columns))
            except Exception as exc:
                print(f"skip {pq}: {exc}", flush=True)
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=columns)
    df["desc_norm"] = normalize_description(df["description"])
    return df


def weighted_corr(x: pd.Series, y: pd.Series, w: pd.Series) -> float:
    valid = pd.concat([x, y, w], axis=1).replace([np.inf, -np.inf], np.nan).dropna()
    if len(valid) < 3:
        return float("nan")
    xv = valid.iloc[:, 0].astype(float).to_numpy()
    yv = valid.iloc[:, 1].astype(float).to_numpy()
    wv = valid.iloc[:, 2].astype(float).to_numpy()
    if wv.sum() <= 0:
        return float("nan")
    xbar = np.average(xv, weights=wv)
    ybar = np.average(yv, weights=wv)
    cov = np.average((xv - xbar) * (yv - ybar), weights=wv)
    vx = np.average((xv - xbar) ** 2, weights=wv)
    vy = np.average((yv - ybar) ** 2, weights=wv)
    return float(cov / np.sqrt(vx * vy)) if vx > 0 and vy > 0 else float("nan")


def weighted_sd(values: pd.Series, weights: pd.Series) -> float:
    valid = pd.concat([values, weights], axis=1).replace([np.inf, -np.inf], np.nan).dropna()
    if len(valid) < 2:
        return float("nan")
    v = valid.iloc[:, 0].astype(float).to_numpy()
    w = valid.iloc[:, 1].astype(float).to_numpy()
    if w.sum() <= 0:
        return float("nan")
    mean = np.average(v, weights=w)
    return float(np.sqrt(np.average((v - mean) ** 2, weights=w)))


def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    valid = pd.concat([values, weights], axis=1).replace([np.inf, -np.inf], np.nan).dropna()
    if valid.empty:
        return float("nan")
    v = valid.iloc[:, 0].astype(float).to_numpy()
    w = valid.iloc[:, 1].astype(float).to_numpy()
    return float(np.average(v, weights=w)) if w.sum() > 0 else float("nan")


def build_pitcher_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["is_swing"] = df["desc_norm"].isin(SWING_DESCRIPTIONS)
    df["is_whiff"] = df["desc_norm"].isin(WHIFF_DESCRIPTIONS)
    df["is_csw"] = df["is_whiff"] | df["desc_norm"].isin(CALLED_STRIKE_DESCRIPTIONS)

    base = (
        df.groupby("pitcher")
        .agg(
            total_pitches=("pitch_type", "size"),
            arm_angle_n=("arm_angle", "count"),
            arm_angle_mean=("arm_angle", "mean"),
            arm_angle_sd=("arm_angle", "std"),
            arm_angle_p10=("arm_angle", lambda s: float(pd.to_numeric(s, errors="coerce").quantile(0.10))),
            arm_angle_p90=("arm_angle", lambda s: float(pd.to_numeric(s, errors="coerce").quantile(0.90))),
            swings=("is_swing", "sum"),
            whiffs=("is_whiff", "sum"),
            csw=("is_csw", "sum"),
            rv_sum=("delta_pitcher_run_exp", "sum"),
            bbe_xwoba=("estimated_woba_using_speedangle", "mean"),
        )
        .reset_index()
    )
    base["arm_angle_p90_p10"] = base["arm_angle_p90"] - base["arm_angle_p10"]

    pa = df[df["events"].notna()].copy()
    pa["is_k"] = pa["events"].isin(["strikeout", "strikeout_double_play"])
    pa["is_bb"] = pa["events"].isin(["walk", "intent_walk"])
    pa_agg = pa.groupby("pitcher").agg(pa=("events", "size"), k=("is_k", "sum"), bb=("is_bb", "sum")).reset_index()

    pitch_type_rows = []
    for pitcher, sub in df.dropna(subset=["arm_angle"]).groupby("pitcher"):
        pt = (
            sub.groupby("pitch_type")
            .agg(
                pt_pitches=("arm_angle", "size"),
                pt_arm_angle_mean=("arm_angle", "mean"),
                pt_arm_angle_sd=("arm_angle", "std"),
            )
            .reset_index()
        )
        eligible = pt[pt["pt_pitches"] >= 20].copy()
        pitch_type_rows.append(
            {
                "pitcher": pitcher,
                "pitch_type_count_20": int(len(eligible)),
                "within_pitch_type_arm_angle_sd": weighted_mean(eligible["pt_arm_angle_sd"], eligible["pt_pitches"]),
                "within_pitch_type_arm_angle_sd_spread": weighted_sd(eligible["pt_arm_angle_sd"], eligible["pt_pitches"]),
                "between_pitch_type_arm_angle_sd": weighted_sd(eligible["pt_arm_angle_mean"], eligible["pt_pitches"]),
            }
        )
    pt_agg = pd.DataFrame(pitch_type_rows)

    out = base.merge(pa_agg, on="pitcher", how="left").merge(pt_agg, on="pitcher", how="left")
    out["k_pct"] = out["k"] / out["pa"]
    out["bb_pct"] = out["bb"] / out["pa"]
    out["k_minus_bb_pct"] = out["k_pct"] - out["bb_pct"]
    out["whiff_pct"] = out["whiffs"] / out["swings"]
    out["csw_pct"] = out["csw"] / out["total_pitches"]
    out["rv_per_100"] = out["rv_sum"] / out["total_pitches"] * 100
    out["arm_angle_coverage"] = out["arm_angle_n"] / out["total_pitches"]
    return out


def corr_table(df: pd.DataFrame, x_cols: list[tuple[str, str]], y_cols: list[tuple[str, str]], weight_col: str) -> pd.DataFrame:
    rows = []
    for x, x_label in x_cols:
        for y, y_label in y_cols:
            sub = df[[x, y, weight_col]].replace([np.inf, -np.inf], np.nan).dropna()
            rows.append(
                {
                    "x": x,
                    "x_label": x_label,
                    "y": y,
                    "y_label": y_label,
                    "n": len(sub),
                    "pearson_r": sub[x].corr(sub[y], method="pearson"),
                    "spearman_rho": sub[x].corr(sub[y], method="spearman"),
                    "weighted_pearson_r": weighted_corr(sub[x], sub[y], sub[weight_col]),
                }
            )
    return pd.DataFrame(rows)


def fmt_pct(v: float) -> str:
    return "--" if pd.isna(v) else f"{v * 100:.1f}%"


def fmt_num(v: float, n: int = 2) -> str:
    return "--" if pd.isna(v) else f"{v:.{n}f}"


def write_summary(sample: pd.DataFrame, corr: pd.DataFrame, season: int, out_path: Path) -> None:
    low = sample[sample["within_pitch_type_arm_angle_sd"] <= sample["within_pitch_type_arm_angle_sd"].quantile(0.25)]
    high = sample[sample["within_pitch_type_arm_angle_sd"] >= sample["within_pitch_type_arm_angle_sd"].quantile(0.75)]
    key = corr[corr["x"].eq("within_pitch_type_arm_angle_sd")].copy()
    lines = [
        f"# Arm-angle variability and pitcher results, {season}",
        "",
        f"Sample: {len(sample)} pitchers with sufficient total pitches, arm-angle pitches, and plate appearances.",
        "",
        "Definitions:",
        "",
        "- `arm_angle_sd`: overall pitch-to-pitch standard deviation of arm angle.",
        "- `arm_angle_p90_p10`: middle-80% arm-angle spread.",
        "- `within_pitch_type_arm_angle_sd`: average arm-angle spread inside each pitch type, weighted by pitch-type volume.",
        "- `within_pitch_type_arm_angle_sd_spread`: how uneven the pitch-type SDs are from pitch to pitch.",
        "- `between_pitch_type_arm_angle_sd`: spread between pitch-type mean arm angles, weighted by pitch-type volume.",
        "",
        "## Correlations for within-pitch-type variability",
        "",
        "| Result | n | Pearson r | Spearman rho | Weighted r |",
        "|---|---:|---:|---:|---:|",
    ]
    for _, r in key.iterrows():
        lines.append(
            f"| {r['y_label']} | {int(r['n'])} | {r['pearson_r']:.3f} | {r['spearman_rho']:.3f} | {r['weighted_pearson_r']:.3f} |"
        )
    lines.extend(
        [
            "",
            "Negative correlations vs K-BB%, K%, whiff, or CSW support the consistency/deception hypothesis. Positive correlations vs xwOBA also support it. In this warehouse, positive run value is pitcher-favorable, so negative correlation vs run value supports the same idea.",
            "",
            "## Low vs high within-pitch-type arm-angle variability",
            "",
            "| Metric | Low variability quartile | High variability quartile | Gap low-high |",
            "|---|---:|---:|---:|",
        ]
    )
    metrics = [
        ("Within pitch-type angle SD", "within_pitch_type_arm_angle_sd", False),
        ("Overall angle SD", "arm_angle_sd", False),
        ("K-BB%", "k_minus_bb_pct", True),
        ("K%", "k_pct", True),
        ("BB%", "bb_pct", True),
        ("Whiff/swing", "whiff_pct", True),
        ("CSW%", "csw_pct", True),
        ("xwOBA on BBE", "bbe_xwoba", False),
        ("Run value / 100", "rv_per_100", False),
    ]
    for label, col, is_pct in metrics:
        lv = low[col].mean()
        hv = high[col].mean()
        gap = lv - hv
        if is_pct:
            lines.append(f"| {label} | {fmt_pct(lv)} | {fmt_pct(hv)} | {fmt_pct(gap)} |")
        else:
            lines.append(f"| {label} | {fmt_num(lv)} | {fmt_num(hv)} | {fmt_num(gap)} |")

    lines.extend(
        [
            "",
            "## Lowest within-pitch-type variability",
            "",
            "| Pitcher | Within PT SD | Overall SD | K-BB% | Whiff/swing | CSW% | xwOBA | Pitches |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for _, r in sample.sort_values("within_pitch_type_arm_angle_sd").head(15).iterrows():
        lines.append(
            f"| {r['pitcher_name']} | {fmt_num(r['within_pitch_type_arm_angle_sd'])} | {fmt_num(r['arm_angle_sd'])} | "
            f"{fmt_pct(r['k_minus_bb_pct'])} | {fmt_pct(r['whiff_pct'])} | {fmt_pct(r['csw_pct'])} | "
            f"{fmt_num(r['bbe_xwoba'], 3)} | {int(r['total_pitches'])} |"
        )

    lines.extend(
        [
            "",
            "## Working interpretation",
            "",
            "- The cleanest tunneling test is within-pitch-type arm-angle variability, because overall variability can be inflated by pitch mix.",
            "- If lower within-pitch-type variability maps to better results, that supports the idea that release consistency contributes to deception.",
            "- If between-pitch-type spread matters more, the story is not only consistency; it may be whether pitch types are visibly released from different slots.",
            "",
        ]
    )
    out_path.write_text("\n".join(lines), encoding="utf-8")


def scatter_plot(sample: pd.DataFrame, out_path: Path, season: int) -> None:
    fig, ax = plt.subplots(figsize=(10, 7), dpi=180)
    x = sample["within_pitch_type_arm_angle_sd"]
    y = sample["k_minus_bb_pct"] * 100
    sizes = np.clip(sample["arm_angle_n"] / 20, 20, 110)
    ax.scatter(x, y, s=sizes, alpha=0.7, color="#2E7D32", edgecolor="white", linewidth=0.4)
    if len(sample) >= 3:
        z = np.polyfit(x, y, 1)
        xs = np.linspace(float(x.min()), float(x.max()), 100)
        ax.plot(xs, z[0] * xs + z[1], color="#E8712B", linewidth=2)
    for _, r in sample.sort_values("k_minus_bb_pct", ascending=False).head(8).iterrows():
        ax.annotate(
            str(r["pitcher_name"]).split(" ")[-1],
            (r["within_pitch_type_arm_angle_sd"], r["k_minus_bb_pct"] * 100),
            xytext=(4, 4),
            textcoords="offset points",
            fontsize=7,
        )
    ax.set_title(f"Arm-angle consistency vs K-BB%, {season}", loc="left", fontsize=15, fontweight="bold")
    ax.set_xlabel("Within-pitch-type arm-angle SD")
    ax.set_ylabel("K-BB%")
    ax.grid(alpha=0.18)
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--stage", default="regular_season")
    ap.add_argument("--warehouse", type=Path, default=REPO / "data" / "warehouse" / "mlb")
    ap.add_argument("--min-pitches", type=int, default=1000)
    ap.add_argument("--min-arm-angle", type=int, default=500)
    ap.add_argument("--min-pa", type=int, default=200)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()

    parquets = discover_parquets(args.warehouse, args.season, args.stage)
    if not parquets:
        raise SystemExit(f"No local parquets found for {args.season}/{args.stage}")
    print(f"Reading {len(parquets)} local parquets...", flush=True)
    df = read_pitch_table(parquets)
    print(f"Loaded {len(df):,} pitches", flush=True)

    table = build_pitcher_table(df)
    sample = table[
        (table["total_pitches"] >= args.min_pitches)
        & (table["arm_angle_n"] >= args.min_arm_angle)
        & (table["pa"] >= args.min_pa)
        & table["within_pitch_type_arm_angle_sd"].notna()
    ].copy()
    target_ids = {int(x) for x in sample["pitcher"].dropna().astype(int)}
    names = load_pitcher_names(parquets, target_ids)
    table["pitcher_name"] = table["pitcher"].map(names).fillna(table["pitcher"].astype("Int64").astype(str))
    sample["pitcher_name"] = sample["pitcher"].map(names).fillna(sample["pitcher"].astype("Int64").astype(str))

    x_cols = [
        ("arm_angle_sd", "Overall arm-angle SD"),
        ("arm_angle_p90_p10", "Arm-angle p90-p10"),
        ("within_pitch_type_arm_angle_sd", "Within-pitch-type arm-angle SD"),
        ("between_pitch_type_arm_angle_sd", "Between-pitch-type arm-angle SD"),
    ]
    y_cols = [
        ("k_minus_bb_pct", "K-BB%"),
        ("k_pct", "K%"),
        ("bb_pct", "BB%"),
        ("whiff_pct", "Whiff/swing"),
        ("csw_pct", "CSW%"),
        ("rv_per_100", "Run value / 100"),
        ("bbe_xwoba", "xwOBA on BBE"),
    ]
    corr = corr_table(sample, x_cols, y_cols, "arm_angle_n")

    data_dir = args.out_dir / "data"
    img_dir = args.out_dir / "images"
    data_dir.mkdir(parents=True, exist_ok=True)
    img_dir.mkdir(parents=True, exist_ok=True)
    table.to_csv(data_dir / f"arm_angle_variability_full_{args.season}.csv", index=False)
    sample.to_csv(data_dir / f"arm_angle_variability_sample_{args.season}.csv", index=False)
    corr.to_csv(data_dir / f"arm_angle_variability_correlations_{args.season}.csv", index=False)
    write_summary(sample, corr, args.season, data_dir / f"arm_angle_variability_summary_{args.season}.md")
    scatter_plot(sample, img_dir / f"arm_angle_within_pt_sd_vs_kbb_{args.season}.png", args.season)

    print(f"Sample pitchers: {len(sample)}")
    print(corr[corr["x"].eq("within_pitch_type_arm_angle_sd")].to_string(index=False))
    print(f"Wrote outputs to {args.out_dir}")


if __name__ == "__main__":
    main()
