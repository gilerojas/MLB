#!/usr/bin/env python3
"""
Compare pitcher-level deception/stuff contributors.

The goal is not to prove causality. It is to rank which measurable ingredients
carry more signal against whiff, CSW, K-BB, and contact quality:

- arm-angle slot separation across pitch types
- release-side / release-height separation across pitch types
- extension separation across pitch types
- velocity separation across pitch types
- movement separation across pitch types
- location separation across pitch types
- pitch-mix diversity

Skips macOS/iCloud dataless files.
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
DEFAULT_OUT = REPO / "research" / "Deception contributors"
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
        "pitcher",
        "pitch_type",
        "release_speed",
        "release_extension",
        "release_pos_x",
        "release_pos_z",
        "pfx_x",
        "pfx_z",
        "plate_x",
        "plate_z",
        "arm_angle",
        "description",
        "events",
        "estimated_woba_using_speedangle",
        "delta_pitcher_run_exp",
    ]
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
    df["pfx_x_in"] = pd.to_numeric(df["pfx_x"], errors="coerce") * -12
    df["pfx_z_in"] = pd.to_numeric(df["pfx_z"], errors="coerce") * 12
    return df


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


def entropy_from_counts(counts: pd.Series) -> float:
    p = counts.astype(float) / counts.sum()
    p = p[p > 0]
    if len(p) <= 1:
        return 0.0
    return float(-(p * np.log(p)).sum() / np.log(len(p)))


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


def movement_separation(pt: pd.DataFrame) -> float:
    valid = pt.dropna(subset=["pt_pfx_x_in_mean", "pt_pfx_z_in_mean", "pt_pitches"])
    if len(valid) < 2:
        return float("nan")
    wx = weighted_sd(valid["pt_pfx_x_in_mean"], valid["pt_pitches"])
    wz = weighted_sd(valid["pt_pfx_z_in_mean"], valid["pt_pitches"])
    return float(np.sqrt(wx**2 + wz**2))


def build_pitcher_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["is_swing"] = df["desc_norm"].isin(SWING_DESCRIPTIONS)
    df["is_whiff"] = df["desc_norm"].isin(WHIFF_DESCRIPTIONS)
    df["is_csw"] = df["is_whiff"] | df["desc_norm"].isin(CALLED_STRIKE_DESCRIPTIONS)

    base = (
        df.groupby("pitcher")
        .agg(
            total_pitches=("pitch_type", "size"),
            swings=("is_swing", "sum"),
            whiffs=("is_whiff", "sum"),
            csw=("is_csw", "sum"),
            rv_sum=("delta_pitcher_run_exp", "sum"),
            bbe_xwoba=("estimated_woba_using_speedangle", "mean"),
            avg_velo=("release_speed", "mean"),
            avg_extension=("release_extension", "mean"),
        )
        .reset_index()
    )
    pa = df[df["events"].notna()].copy()
    pa["is_k"] = pa["events"].isin(["strikeout", "strikeout_double_play"])
    pa["is_bb"] = pa["events"].isin(["walk", "intent_walk"])
    pa_agg = pa.groupby("pitcher").agg(pa=("events", "size"), k=("is_k", "sum"), bb=("is_bb", "sum")).reset_index()

    rows = []
    for pitcher, sub in df.groupby("pitcher"):
        pt = (
            sub.groupby("pitch_type")
            .agg(
                pt_pitches=("pitch_type", "size"),
                pt_arm_angle_mean=("arm_angle", "mean"),
                pt_arm_angle_sd=("arm_angle", "std"),
                pt_release_pos_x_mean=("release_pos_x", "mean"),
                pt_release_pos_z_mean=("release_pos_z", "mean"),
                pt_extension_mean=("release_extension", "mean"),
                pt_velo_mean=("release_speed", "mean"),
                pt_pfx_x_in_mean=("pfx_x_in", "mean"),
                pt_pfx_z_in_mean=("pfx_z_in", "mean"),
                pt_plate_x_mean=("plate_x", "mean"),
                pt_plate_z_mean=("plate_z", "mean"),
            )
            .reset_index()
        )
        eligible = pt[pt["pt_pitches"] >= 20].copy()
        counts = eligible["pt_pitches"]
        rows.append(
            {
                "pitcher": pitcher,
                "pitch_type_count_20": int(len(eligible)),
                "pitch_mix_entropy": entropy_from_counts(counts) if len(eligible) else np.nan,
                "arm_angle_between_sd": weighted_sd(eligible["pt_arm_angle_mean"], counts),
                "arm_angle_within_sd": weighted_mean(eligible["pt_arm_angle_sd"], counts),
                "release_side_between_sd": weighted_sd(eligible["pt_release_pos_x_mean"], counts),
                "release_height_between_sd": weighted_sd(eligible["pt_release_pos_z_mean"], counts),
                "extension_between_sd": weighted_sd(eligible["pt_extension_mean"], counts),
                "velo_between_sd": weighted_sd(eligible["pt_velo_mean"], counts),
                "movement_between_2d_sd": movement_separation(eligible),
                "hb_between_sd": weighted_sd(eligible["pt_pfx_x_in_mean"], counts),
                "ivb_between_sd": weighted_sd(eligible["pt_pfx_z_in_mean"], counts),
                "plate_x_between_sd": weighted_sd(eligible["pt_plate_x_mean"], counts),
                "plate_z_between_sd": weighted_sd(eligible["pt_plate_z_mean"], counts),
            }
        )
    feat = pd.DataFrame(rows)
    out = base.merge(pa_agg, on="pitcher", how="left").merge(feat, on="pitcher", how="left")
    out["k_pct"] = out["k"] / out["pa"]
    out["bb_pct"] = out["bb"] / out["pa"]
    out["k_minus_bb_pct"] = out["k_pct"] - out["bb_pct"]
    out["whiff_pct"] = out["whiffs"] / out["swings"]
    out["csw_pct"] = out["csw"] / out["total_pitches"]
    out["rv_per_100"] = out["rv_sum"] / out["total_pitches"] * 100
    return out


def corr_table(df: pd.DataFrame, features: list[tuple[str, str]], outcomes: list[tuple[str, str]], weight_col: str) -> pd.DataFrame:
    rows = []
    for x, x_label in features:
        for y, y_label in outcomes:
            sub = df[[x, y, weight_col]].replace([np.inf, -np.inf], np.nan).dropna()
            rows.append(
                {
                    "feature": x,
                    "feature_label": x_label,
                    "outcome": y,
                    "outcome_label": y_label,
                    "n": len(sub),
                    "pearson_r": sub[x].corr(sub[y], method="pearson"),
                    "spearman_rho": sub[x].corr(sub[y], method="spearman"),
                    "weighted_pearson_r": weighted_corr(sub[x], sub[y], sub[weight_col]),
                }
            )
    return pd.DataFrame(rows)


def standardized_ols(df: pd.DataFrame, features: list[tuple[str, str]], outcome: str) -> pd.DataFrame:
    cols = [f for f, _ in features] + [outcome]
    sub = df[cols].replace([np.inf, -np.inf], np.nan).dropna()
    if len(sub) < len(features) + 5:
        return pd.DataFrame()
    x = sub[[f for f, _ in features]].astype(float)
    y = sub[outcome].astype(float)
    xz = (x - x.mean()) / x.std(ddof=0)
    yz = (y - y.mean()) / y.std(ddof=0)
    xmat = np.column_stack([np.ones(len(xz)), xz.to_numpy()])
    coef = np.linalg.lstsq(xmat, yz.to_numpy(), rcond=None)[0][1:]
    pred = xmat @ np.r_[0, coef]
    r2 = 1 - float(np.sum((yz.to_numpy() - pred) ** 2) / np.sum((yz.to_numpy() - yz.mean()) ** 2))
    return pd.DataFrame(
        {
            "outcome": outcome,
            "feature": [f for f, _ in features],
            "feature_label": [label for _, label in features],
            "standardized_beta": coef,
            "abs_beta": np.abs(coef),
            "n": len(sub),
            "model_r2_in_sample": r2,
        }
    ).sort_values("abs_beta", ascending=False)


def write_summary(corr: pd.DataFrame, coef: pd.DataFrame, season: int, out_path: Path) -> None:
    focus = ["k_minus_bb_pct", "whiff_pct", "csw_pct", "bbe_xwoba"]
    lines = [
        f"# Deception contributor comparison, {season}",
        "",
        "This is a pitcher-level signal ranking, not a causal model.",
        "",
        "## Best univariate signals by outcome",
        "",
    ]
    for outcome in focus:
        sub = corr[corr["outcome"].eq(outcome)].copy()
        sub["abs_weighted"] = sub["weighted_pearson_r"].abs()
        sub = sub.sort_values("abs_weighted", ascending=False).head(8)
        label = sub["outcome_label"].iloc[0] if not sub.empty else outcome
        lines.extend([f"### {label}", "", "| Feature | Weighted r | Pearson r | Spearman rho |", "|---|---:|---:|---:|"])
        for _, r in sub.iterrows():
            lines.append(
                f"| {r['feature_label']} | {r['weighted_pearson_r']:.3f} | {r['pearson_r']:.3f} | {r['spearman_rho']:.3f} |"
            )
        lines.append("")

    lines.extend(["## Multivariable standardized coefficients", ""])
    for outcome in focus:
        sub = coef[coef["outcome"].eq(outcome)].sort_values("abs_beta", ascending=False).head(8)
        if sub.empty:
            continue
        lines.extend(
            [
                f"### {outcome}",
                "",
                f"In-sample R2: {sub['model_r2_in_sample'].iloc[0]:.3f}",
                "",
                "| Feature | Standardized beta |",
                "|---|---:|",
            ]
        )
        for _, r in sub.iterrows():
            lines.append(f"| {r['feature_label']} | {r['standardized_beta']:.3f} |")
        lines.append("")

    lines.extend(
        [
            "## Working interpretation",
            "",
            "- If a feature has a larger absolute correlation with whiff/CSW/K-BB, it is carrying more standalone signal.",
            "- If it remains large in the standardized multivariable table, it carries signal after sharing the model with the other features.",
            "- This does not prove hitter pitch recognition directly; it identifies which measurable proxies are most worth investigating next.",
            "",
        ]
    )
    out_path.write_text("\n".join(lines), encoding="utf-8")


def plot_feature_rank(corr: pd.DataFrame, outcome: str, out_path: Path) -> None:
    sub = corr[corr["outcome"].eq(outcome)].copy()
    sub["abs_weighted"] = sub["weighted_pearson_r"].abs()
    sub = sub.sort_values("abs_weighted", ascending=True).tail(10)
    fig, ax = plt.subplots(figsize=(10, 6), dpi=180)
    colors = ["#2E7D32" if v >= 0 else "#E8712B" for v in sub["weighted_pearson_r"]]
    ax.barh(sub["feature_label"], sub["weighted_pearson_r"], color=colors)
    ax.axvline(0, color="#333333", linewidth=1)
    ax.set_title(f"Deception proxy signals vs {sub['outcome_label'].iloc[0]}", loc="left", fontsize=14, fontweight="bold")
    ax.set_xlabel("Weighted Pearson r")
    ax.grid(axis="x", alpha=0.18)
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--stage", default="regular_season")
    ap.add_argument("--warehouse", type=Path, default=REPO / "data" / "warehouse" / "mlb")
    ap.add_argument("--min-pitches", type=int, default=1000)
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
        & (table["pa"] >= args.min_pa)
        & (table["pitch_type_count_20"] >= 3)
    ].copy()
    names = load_pitcher_names(parquets, {int(x) for x in sample["pitcher"].dropna().astype(int)})
    table["pitcher_name"] = table["pitcher"].map(names).fillna(table["pitcher"].astype("Int64").astype(str))
    sample["pitcher_name"] = sample["pitcher"].map(names).fillna(sample["pitcher"].astype("Int64").astype(str))

    features = [
        ("arm_angle_between_sd", "Arm-angle separation"),
        ("release_side_between_sd", "Release-side separation"),
        ("release_height_between_sd", "Release-height separation"),
        ("extension_between_sd", "Extension separation"),
        ("avg_extension", "Average extension"),
        ("velo_between_sd", "Velocity separation"),
        ("movement_between_2d_sd", "Movement separation"),
        ("hb_between_sd", "HB separation"),
        ("ivb_between_sd", "IVB separation"),
        ("plate_x_between_sd", "Horizontal location separation"),
        ("plate_z_between_sd", "Vertical location separation"),
        ("pitch_mix_entropy", "Pitch-mix diversity"),
        ("avg_velo", "Average velocity"),
    ]
    outcomes = [
        ("k_minus_bb_pct", "K-BB%"),
        ("k_pct", "K%"),
        ("bb_pct", "BB%"),
        ("whiff_pct", "Whiff/swing"),
        ("csw_pct", "CSW%"),
        ("rv_per_100", "Run value / 100"),
        ("bbe_xwoba", "xwOBA on BBE"),
    ]
    corr = corr_table(sample, features, outcomes, "total_pitches")
    coef_frames = [standardized_ols(sample, features, outcome) for outcome, _ in outcomes]
    coef = pd.concat([c for c in coef_frames if not c.empty], ignore_index=True)

    data_dir = args.out_dir / "data"
    img_dir = args.out_dir / "images"
    data_dir.mkdir(parents=True, exist_ok=True)
    img_dir.mkdir(parents=True, exist_ok=True)
    table.to_csv(data_dir / f"deception_contributors_full_{args.season}.csv", index=False)
    sample.to_csv(data_dir / f"deception_contributors_sample_{args.season}.csv", index=False)
    corr.to_csv(data_dir / f"deception_contributors_correlations_{args.season}.csv", index=False)
    coef.to_csv(data_dir / f"deception_contributors_standardized_ols_{args.season}.csv", index=False)
    write_summary(corr, coef, args.season, data_dir / f"deception_contributors_summary_{args.season}.md")
    for outcome in ("k_minus_bb_pct", "whiff_pct", "csw_pct", "bbe_xwoba"):
        plot_feature_rank(corr, outcome, img_dir / f"feature_rank_vs_{outcome}_{args.season}.png")

    print(f"Sample pitchers: {len(sample)}")
    print(corr[corr["outcome"].isin(["k_minus_bb_pct", "whiff_pct", "csw_pct", "bbe_xwoba"])].sort_values(["outcome", "weighted_pearson_r"]).to_string(index=False))
    print(f"Wrote outputs to {args.out_dir}")


if __name__ == "__main__":
    main()
