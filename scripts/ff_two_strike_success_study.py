#!/usr/bin/env python3
"""
Case study: does two-strike four-seam usage correlate with pitcher success?

The script reads local Statcast-style ``pitches_enriched`` parquets and writes
CSV/Markdown/PNG outputs under ``outputs/case_studies/ff_two_strike_success``.
It intentionally avoids season summary files because those may be iCloud
placeholders on this workstation.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

import matplotlib

matplotlib.use(os.environ.get("MPLBACKEND", "Agg"))
import matplotlib.pyplot as plt


REPO = Path(__file__).resolve().parent.parent
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
FASTBALL_FAMILY = {"FF", "SI", "FC"}
DATALLESS_FLAG = 0x40000000


def is_dataless(path: Path) -> bool:
    try:
        return bool(getattr(path.stat(), "st_flags", 0) & DATALLESS_FLAG)
    except OSError:
        return True


def discover_parquets(warehouse: Path, season: int, stage: str) -> list[Path]:
    root = warehouse / str(season) / stage / "pitches_enriched"
    if not root.is_dir():
        return []
    out: list[Path] = []
    for p in sorted(root.glob("*.parquet")):
        if is_dataless(p):
            continue
        out.append(p)
    return out


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


def load_pitcher_names(parquets: list[Path], target_ids: set[int] | None = None) -> dict[int, str]:
    names: dict[int, str] = {}
    for pq in parquets:
        if target_ids is not None and target_ids.issubset(names.keys()):
            break
        rp = raw_path_for_parquet(pq)
        if rp is None:
            continue
        if is_dataless(rp):
            continue
        try:
            opener = gzip.open if rp.suffix == ".gz" else open
            with opener(rp, "rt", encoding="utf-8") as f:
                feed = json.load(f)
        except Exception:
            continue
        players = (feed.get("gameData") or {}).get("players") or {}
        for pdata in players.values():
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
            if target_ids is None or ipid in target_ids:
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
        "at_bat_number",
        "pitch_number",
        "pitch_type",
        "release_speed",
        "pfx_x",
        "pfx_z",
        "strikes",
        "description",
        "events",
        "estimated_woba_using_speedangle",
        "delta_pitcher_run_exp",
    ]
    if not parquets:
        return pd.DataFrame(columns=columns)
    roots = sorted({p.parent for p in parquets})
    frames: list[pd.DataFrame] = []
    for root in roots:
        root_files = sorted(root.glob("*.parquet"))
        selected = [p for p in parquets if p.parent == root]
        if len(selected) != len(root_files):
            for pq in selected:
                try:
                    frames.append(pd.read_parquet(pq, columns=columns))
                except Exception as file_exc:
                    print(f"skip {pq}: {file_exc}", flush=True)
            continue
        try:
            dataset = ds.dataset(root, format="parquet")
            frames.append(dataset.to_table(columns=columns).to_pandas())
        except Exception as exc:
            print(f"dataset scan failed for {root}: {exc}; falling back to per-file reads", flush=True)
            for pq in sorted(root.glob("*.parquet")):
                try:
                    frames.append(pd.read_parquet(pq, columns=columns))
                except Exception as file_exc:
                    print(f"skip {pq}: {file_exc}", flush=True)
    if not frames:
        return pd.DataFrame(columns=columns)
    df = pd.concat(frames, ignore_index=True)
    df["desc_norm"] = normalize_description(df["description"])
    return df


def pitcher_table(df: pd.DataFrame, names: dict[int, str] | None = None) -> pd.DataFrame:
    df = df.copy()
    df["is_two_strike"] = df["strikes"].eq(2)
    df["is_ff"] = df["pitch_type"].eq("FF")
    df["is_fastball_family"] = df["pitch_type"].isin(FASTBALL_FAMILY)
    df["is_swing"] = df["desc_norm"].isin(SWING_DESCRIPTIONS)
    df["is_whiff"] = df["desc_norm"].isin(WHIFF_DESCRIPTIONS)
    df["is_called_strike"] = df["desc_norm"].isin(CALLED_STRIKE_DESCRIPTIONS)
    df["is_csw"] = df["is_whiff"] | df["is_called_strike"]

    g = df.groupby("pitcher", dropna=False)
    base = g.agg(
        total_pitches=("pitch_type", "size"),
        two_strike_pitches=("is_two_strike", "sum"),
        two_strike_ff=("is_ff", lambda s: int((s & df.loc[s.index, "is_two_strike"]).sum())),
        two_strike_fastball_family=(
            "is_fastball_family",
            lambda s: int((s & df.loc[s.index, "is_two_strike"]).sum()),
        ),
        swings=("is_swing", "sum"),
        whiffs=("is_whiff", "sum"),
        csw=("is_csw", "sum"),
        rv_sum=("delta_pitcher_run_exp", "sum"),
        bbe_xwoba=("estimated_woba_using_speedangle", "mean"),
    ).reset_index()

    pa = df[df["events"].notna()].copy()
    pa["is_k"] = pa["events"].isin(["strikeout", "strikeout_double_play"])
    pa["is_bb"] = pa["events"].isin(["walk", "intent_walk"])
    pa_agg = pa.groupby("pitcher").agg(pa=("events", "size"), k=("is_k", "sum"), bb=("is_bb", "sum")).reset_index()

    ff = df[df["pitch_type"].eq("FF")].copy()
    ff_agg = (
        ff.groupby("pitcher")
        .agg(
            ff_pitches=("pitch_type", "size"),
            ff_velo=("release_speed", "mean"),
            ff_hb_in=("pfx_x", lambda s: float(pd.to_numeric(s, errors="coerce").mean() * -12)),
            ff_ivb_in=("pfx_z", lambda s: float(pd.to_numeric(s, errors="coerce").mean() * 12)),
            ff_swings=("is_swing", "sum"),
            ff_whiffs=("is_whiff", "sum"),
            ff_csw=("is_csw", "sum"),
        )
        .reset_index()
    )

    out = base.merge(pa_agg, on="pitcher", how="left").merge(ff_agg, on="pitcher", how="left")
    names = names or {}
    out["pitcher_name"] = out["pitcher"].map(names).fillna(out["pitcher"].astype("Int64").astype(str))
    out["two_strike_ff_pct"] = out["two_strike_ff"] / out["two_strike_pitches"]
    out["two_strike_fastball_family_pct"] = out["two_strike_fastball_family"] / out["two_strike_pitches"]
    out["k_pct"] = out["k"] / out["pa"]
    out["bb_pct"] = out["bb"] / out["pa"]
    out["k_minus_bb_pct"] = out["k_pct"] - out["bb_pct"]
    out["whiff_pct"] = out["whiffs"] / out["swings"]
    out["csw_pct"] = out["csw"] / out["total_pitches"]
    out["rv_per_100"] = out["rv_sum"] / out["total_pitches"] * 100
    out["ff_whiff_pct"] = out["ff_whiffs"] / out["ff_swings"]
    out["ff_csw_pct"] = out["ff_csw"] / out["ff_pitches"]
    return out


def corr_rows(df: pd.DataFrame, x: str, metrics: list[tuple[str, str]]) -> pd.DataFrame:
    rows = []
    for col, label in metrics:
        sub = df[[x, col]].replace([np.inf, -np.inf], np.nan).dropna()
        rows.append(
            {
                "x": x,
                "metric": col,
                "label": label,
                "n": len(sub),
                "pearson_r": sub[x].corr(sub[col], method="pearson"),
                "spearman_rho": sub[x].corr(sub[col], method="spearman"),
            }
        )
    return pd.DataFrame(rows)


def weighted_corr(x: pd.Series, y: pd.Series, w: pd.Series) -> float:
    valid = pd.concat([x, y, w], axis=1).replace([np.inf, -np.inf], np.nan).dropna()
    if len(valid) < 3:
        return float("nan")
    xv = valid.iloc[:, 0].astype(float).to_numpy()
    yv = valid.iloc[:, 1].astype(float).to_numpy()
    wv = valid.iloc[:, 2].astype(float).to_numpy()
    if np.any(wv < 0) or wv.sum() <= 0:
        return float("nan")
    xbar = np.average(xv, weights=wv)
    ybar = np.average(yv, weights=wv)
    cov = np.average((xv - xbar) * (yv - ybar), weights=wv)
    vx = np.average((xv - xbar) ** 2, weights=wv)
    vy = np.average((yv - ybar) ** 2, weights=wv)
    if vx <= 0 or vy <= 0:
        return float("nan")
    return float(cov / np.sqrt(vx * vy))


def weighted_corr_rows(df: pd.DataFrame, x: str, weight: str, metrics: list[tuple[str, str]]) -> pd.DataFrame:
    rows = []
    for col, label in metrics:
        sub = df[[x, col, weight]].replace([np.inf, -np.inf], np.nan).dropna()
        rows.append(
            {
                "x": x,
                "metric": col,
                "label": label,
                "weight": weight,
                "n": len(sub),
                "weighted_pearson_r": weighted_corr(sub[x], sub[col], sub[weight]),
            }
        )
    return pd.DataFrame(rows)


def fmt_pct(v: float) -> str:
    return "--" if pd.isna(v) else f"{v * 100:.1f}%"


def fmt_num(v: float, nd: int = 3) -> str:
    return "--" if pd.isna(v) else f"{v:.{nd}f}"


def write_plot(df: pd.DataFrame, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 7), dpi=180)
    x = df["two_strike_ff_pct"] * 100
    y = df["k_minus_bb_pct"] * 100
    sizes = np.clip(df["total_pitches"] / 18, 18, 95)
    ax.scatter(x, y, s=sizes, alpha=0.68, color="#1f77b4", edgecolor="white", linewidth=0.45)
    if len(df) >= 3:
        z = np.polyfit(x, y, 1)
        xs = np.linspace(float(x.min()), float(x.max()), 100)
        ax.plot(xs, z[0] * xs + z[1], color="#d62728", linewidth=2.0)
    for _, r in df.nlargest(8, "k_minus_bb_pct").iterrows():
        ax.annotate(
            str(r["pitcher_name"]).split(" ")[-1],
            (r["two_strike_ff_pct"] * 100, r["k_minus_bb_pct"] * 100),
            xytext=(4, 4),
            textcoords="offset points",
            fontsize=7,
        )
    ax.axhline(df["k_minus_bb_pct"].median() * 100, color="#777777", linewidth=0.9, linestyle=":")
    ax.axvline(df["two_strike_ff_pct"].median() * 100, color="#777777", linewidth=0.9, linestyle=":")
    ax.set_title("Two-strike FF% vs pitcher success", loc="left", fontsize=16, fontweight="bold")
    ax.set_xlabel("Four-seam share with two strikes")
    ax.set_ylabel("K-BB%")
    ax.grid(alpha=0.18)
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def write_markdown(
    sample: pd.DataFrame,
    corr: pd.DataFrame,
    family_corr: pd.DataFrame,
    out_path: Path,
    season: int,
    min_pitches: int,
    min_two_strike: int,
) -> None:
    high = sample[sample["two_strike_ff_pct"] >= sample["two_strike_ff_pct"].quantile(0.75)]
    low = sample[sample["two_strike_ff_pct"] <= sample["two_strike_ff_pct"].quantile(0.25)]
    metrics = [
        ("K-BB%", "k_minus_bb_pct", True),
        ("K%", "k_pct", True),
        ("BB%", "bb_pct", True),
        ("Run value / 100", "rv_per_100", False),
        ("xwOBA on BBE", "bbe_xwoba", False),
        ("FF velo", "ff_velo", False),
        ("FF HB", "ff_hb_in", False),
        ("FF IVB", "ff_ivb_in", False),
        ("FF whiff/swing", "ff_whiff_pct", True),
    ]

    lines = [
        f"# Two-strike FF% and pitcher success, {season}",
        "",
        f"Sample: {len(sample)} pitchers with at least {min_pitches} pitches and {min_two_strike} two-strike pitches.",
        "",
        f"Median two-strike pitches in sample: {sample['two_strike_pitches'].median():.0f}. Mean: {sample['two_strike_pitches'].mean():.0f}.",
        "",
        "## Main correlations, unweighted",
        "",
        "| Relationship | n | Pearson r | Spearman rho |",
        "|---|---:|---:|---:|",
    ]
    for _, r in corr.iterrows():
        lines.append(
            f"| 2-strike FF% vs {r['label']} | {int(r['n'])} | {r['pearson_r']:.3f} | {r['spearman_rho']:.3f} |"
        )
    if "weighted_pearson_r" in corr.columns:
        weighted = corr.dropna(subset=["weighted_pearson_r"])
        lines.extend(
            [
                "",
                "## Main correlations, weighted by two-strike pitch count",
                "",
                "| Relationship | n | Weighted Pearson r |",
                "|---|---:|---:|",
            ]
        )
        for _, r in weighted.iterrows():
            lines.append(
                f"| 2-strike FF% vs {r['label']} | {int(r['n'])} | {r['weighted_pearson_r']:.3f} |"
            )
    lines.extend(["", "## Fastball-family check", "", "| Relationship | n | Pearson r | Spearman rho |", "|---|---:|---:|---:|"])
    for _, r in family_corr.iterrows():
        lines.append(
            f"| 2-strike FF/SI/FC% vs {r['label']} | {int(r['n'])} | {r['pearson_r']:.3f} | {r['spearman_rho']:.3f} |"
        )

    lines.extend(["", "## High vs low two-strike FF usage", "", "| Metric | Top quartile | Bottom quartile | Gap |", "|---|---:|---:|---:|"])
    for label, col, pct in metrics:
        hv = high[col].mean()
        lv = low[col].mean()
        gap = hv - lv
        if pct:
            lines.append(f"| {label} | {fmt_pct(hv)} | {fmt_pct(lv)} | {fmt_pct(gap)} |")
        else:
            lines.append(f"| {label} | {fmt_num(hv)} | {fmt_num(lv)} | {fmt_num(gap)} |")

    lines.extend(
        [
            "",
            "## Leaders by two-strike FF%",
            "",
            "| Pitcher | 2K FF% | 2K pitches | K-BB% | RV/100 | FF velo | FF HB | FF IVB | FF whiff/swing | Pitches |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for _, r in sample.sort_values("two_strike_ff_pct", ascending=False).head(15).iterrows():
        lines.append(
            f"| {r['pitcher_name']} | {fmt_pct(r['two_strike_ff_pct'])} | {int(r['two_strike_pitches'])} | "
            f"{fmt_pct(r['k_minus_bb_pct'])} | {fmt_num(r['rv_per_100'], 2)} | {fmt_num(r['ff_velo'], 1)} | "
            f"{fmt_num(r['ff_hb_in'], 1)} | {fmt_num(r['ff_ivb_in'], 1)} | {fmt_pct(r['ff_whiff_pct'])} | {int(r['total_pitches'])} |"
        )

    lines.extend(
        [
            "",
            "## Best pitchers by K-BB%",
            "",
            "| Pitcher | K-BB% | 2K FF% | 2K pitches | 2K FF/SI/FC% | RV/100 | FF velo | FF HB | FF IVB | FF whiff/swing |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for _, r in sample.sort_values("k_minus_bb_pct", ascending=False).head(15).iterrows():
        lines.append(
            f"| {r['pitcher_name']} | {fmt_pct(r['k_minus_bb_pct'])} | {fmt_pct(r['two_strike_ff_pct'])} | "
            f"{int(r['two_strike_pitches'])} | {fmt_pct(r['two_strike_fastball_family_pct'])} | {fmt_num(r['rv_per_100'], 2)} | "
            f"{fmt_num(r['ff_velo'], 1)} | {fmt_num(r['ff_hb_in'], 1)} | {fmt_num(r['ff_ivb_in'], 1)} | {fmt_pct(r['ff_whiff_pct'])} |"
        )

    lines.extend(
        [
            "",
            "## Working interpretation",
            "",
            "- Two-strike FF% is a positive signal, but not a standalone success formula.",
            "- Because this is a percentage stat, denominator matters. The weighted correlations give more influence to pitchers with larger two-strike samples.",
            "- The stronger baseball interpretation is conditional: pitchers who keep throwing FF with two strikes usually have enough velocity/shape/command to make that pitch survive in put-away counts.",
            "- The survivorship bias matters. Bad four-seams get hidden; good four-seams keep showing up when the pitcher needs an out.",
            "",
        ]
    )
    out_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--stage", default="regular_season")
    ap.add_argument("--warehouse", type=Path, default=REPO / "data" / "warehouse" / "mlb")
    ap.add_argument("--min-pitches", type=int, default=1000)
    ap.add_argument("--min-two-strike", type=int, default=150)
    ap.add_argument("--out-dir", type=Path, default=REPO / "outputs" / "case_studies" / "ff_two_strike_success")
    args = ap.parse_args()

    parquets = discover_parquets(args.warehouse, args.season, args.stage)
    if not parquets:
        raise SystemExit(f"No parquets found for {args.season}/{args.stage}")

    print(f"Reading pitch tables from {len(parquets)} parquets...", flush=True)
    df = read_pitch_table(parquets)
    print(f"Loaded {len(df):,} pitches", flush=True)

    table = pitcher_table(df)
    sample = table[
        (table["total_pitches"] >= args.min_pitches)
        & (table["two_strike_pitches"] >= args.min_two_strike)
        & table["pa"].notna()
    ].copy()
    target_ids = {int(x) for x in sample["pitcher"].dropna().astype(int)}
    print(f"Reading names for {len(target_ids)} sample pitchers from raw feeds...", flush=True)
    names = load_pitcher_names(parquets, target_ids)
    print(f"Loaded {len(names)} sample pitcher names", flush=True)
    table["pitcher_name"] = table["pitcher"].map(names).fillna(table["pitcher"].astype("Int64").astype(str))
    sample["pitcher_name"] = sample["pitcher"].map(names).fillna(sample["pitcher"].astype("Int64").astype(str))
    sample = sample.sort_values("two_strike_ff_pct", ascending=False)

    metrics = [
        ("k_minus_bb_pct", "K-BB%"),
        ("k_pct", "K%"),
        ("bb_pct", "BB%"),
        ("rv_per_100", "run value / 100 pitches"),
        ("bbe_xwoba", "xwOBA on batted balls"),
        ("whiff_pct", "overall whiff/swing"),
        ("csw_pct", "CSW%"),
        ("ff_velo", "FF velocity"),
        ("ff_hb_in", "FF HB"),
        ("ff_ivb_in", "FF IVB"),
        ("ff_whiff_pct", "FF whiff/swing"),
        ("ff_csw_pct", "FF CSW%"),
    ]
    corr = corr_rows(sample, "two_strike_ff_pct", metrics)
    weighted = weighted_corr_rows(sample, "two_strike_ff_pct", "two_strike_pitches", metrics)
    corr = corr.merge(weighted[["metric", "weighted_pearson_r"]], on="metric", how="left")
    family_corr = corr_rows(sample, "two_strike_fastball_family_pct", metrics)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    table.to_csv(args.out_dir / f"pitcher_two_strike_ff_full_{args.season}.csv", index=False)
    sample.to_csv(args.out_dir / f"pitcher_two_strike_ff_sample_{args.season}.csv", index=False)
    corr.to_csv(args.out_dir / f"correlations_ff_{args.season}.csv", index=False)
    family_corr.to_csv(args.out_dir / f"correlations_fastball_family_{args.season}.csv", index=False)
    write_plot(sample, args.out_dir / f"two_strike_ff_vs_kbb_{args.season}.png")
    write_markdown(
        sample,
        corr,
        family_corr,
        args.out_dir / f"two_strike_ff_success_{args.season}.md",
        args.season,
        args.min_pitches,
        args.min_two_strike,
    )

    print(f"Sample pitchers: {len(sample)}")
    print(corr.to_string(index=False))
    print(f"Wrote outputs to {args.out_dir}")


if __name__ == "__main__":
    main()
