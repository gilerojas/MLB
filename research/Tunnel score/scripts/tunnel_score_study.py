#!/usr/bin/env python3
"""
Pitch-pair tunnel score study.

Question:
Does pitch 2 perform better when it follows a previous pitch with a similar
release look but different velocity/movement/location?

Tunnel score:
    release_similarity * post_release_separation

This is an exploratory first pass. It pairs consecutive pitches within the same
plate appearance and evaluates outcome on the second pitch.
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
DEFAULT_OUT = REPO / "research" / "Tunnel score"
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
CHASE_ZONES = {11, 12, 13, 14}


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
        "batter",
        "at_bat_number",
        "pitch_number",
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
        "zone",
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


def robust_z(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    med = s.median()
    iqr = s.quantile(0.75) - s.quantile(0.25)
    scale = iqr / 1.349 if iqr and np.isfinite(iqr) else s.std()
    if not scale or not np.isfinite(scale):
        scale = 1.0
    return (s - med) / scale


def pair_consecutive_pitches(df: pd.DataFrame) -> pd.DataFrame:
    sort_cols = ["game_pk", "at_bat_number", "pitch_number"]
    df = df.sort_values(sort_cols).copy()
    group_cols = ["game_pk", "at_bat_number", "pitcher", "batter"]
    prev = df.groupby(group_cols, dropna=False).shift(1)
    pairs = pd.DataFrame(
        {
            "game_pk": df["game_pk"],
            "game_date": df["game_date"],
            "pitcher": df["pitcher"],
            "batter": df["batter"],
            "at_bat_number": df["at_bat_number"],
            "pitch_number": df["pitch_number"],
            "pitch_type_1": prev["pitch_type"],
            "pitch_type_2": df["pitch_type"],
            "desc_2": df["desc_norm"],
            "events_2": df["events"],
            "zone_2": df["zone"],
            "xwoba_2": df["estimated_woba_using_speedangle"],
            "rv_2": df["delta_pitcher_run_exp"],
        }
    )
    metric_cols = [
        "release_speed",
        "release_extension",
        "release_pos_x",
        "release_pos_z",
        "pfx_x_in",
        "pfx_z_in",
        "plate_x",
        "plate_z",
        "arm_angle",
    ]
    for col in metric_cols:
        pairs[f"{col}_1"] = prev[col]
        pairs[f"{col}_2"] = df[col]
        pairs[f"d_{col}"] = pairs[f"{col}_2"] - pairs[f"{col}_1"]
        pairs[f"abs_d_{col}"] = pairs[f"d_{col}"].abs()

    pairs = pairs[pairs["pitch_type_1"].notna()].copy()
    pairs["is_same_pitch_type"] = pairs["pitch_type_1"].eq(pairs["pitch_type_2"])
    pairs["is_swing_2"] = pairs["desc_2"].isin(SWING_DESCRIPTIONS)
    pairs["is_whiff_2"] = pairs["desc_2"].isin(WHIFF_DESCRIPTIONS)
    pairs["is_csw_2"] = pairs["is_whiff_2"] | pairs["desc_2"].isin(CALLED_STRIKE_DESCRIPTIONS)
    pairs["is_chase_2"] = pairs["is_swing_2"] & pairs["zone_2"].isin(CHASE_ZONES)
    pairs["is_bip_2"] = pairs["desc_2"].eq("hit_into_play")
    return pairs


def score_pairs(pairs: pd.DataFrame) -> pd.DataFrame:
    out = pairs.copy()
    release_components = [
        "abs_d_arm_angle",
        "abs_d_release_pos_x",
        "abs_d_release_pos_z",
        "abs_d_release_extension",
    ]
    stuff_separation_components = [
        "abs_d_release_speed",
        "abs_d_pfx_x_in",
        "abs_d_pfx_z_in",
    ]
    location_components = [
        "abs_d_plate_x",
        "abs_d_plate_z",
    ]
    separation_components = stuff_separation_components + location_components
    for col in release_components + separation_components:
        out[f"z_{col}"] = robust_z(out[col])

    out["release_dissimilarity"] = np.sqrt(sum(out[f"z_{c}"] ** 2 for c in release_components))
    out["release_similarity"] = 1 / (1 + out["release_dissimilarity"])
    out["stuff_separation"] = np.sqrt(sum(out[f"z_{c}"] ** 2 for c in stuff_separation_components))
    out["post_release_separation"] = np.sqrt(sum(out[f"z_{c}"] ** 2 for c in separation_components))
    out["movement_separation"] = np.sqrt(out["z_abs_d_pfx_x_in"] ** 2 + out["z_abs_d_pfx_z_in"] ** 2)
    out["location_separation"] = np.sqrt(out["z_abs_d_plate_x"] ** 2 + out["z_abs_d_plate_z"] ** 2)
    out["tunnel_stuff_score"] = out["release_similarity"] * out["stuff_separation"]
    out["tunnel_score"] = out["release_similarity"] * out["post_release_separation"]
    return out


def weighted_corr(x: pd.Series, y: pd.Series, w: pd.Series | None = None) -> float:
    if w is None:
        valid = pd.concat([x, y], axis=1).replace([np.inf, -np.inf], np.nan).dropna()
        return float(valid.iloc[:, 0].corr(valid.iloc[:, 1])) if len(valid) >= 3 else float("nan")
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


def bucket_summary(pairs: pd.DataFrame) -> pd.DataFrame:
    scored = pairs.replace([np.inf, -np.inf], np.nan).dropna(subset=["tunnel_score"]).copy()
    scored["tunnel_bucket"] = pd.qcut(scored["tunnel_score"], 4, labels=["Low", "Mid-low", "Mid-high", "High"], duplicates="drop")
    return (
        scored.groupby("tunnel_bucket", observed=True)
        .agg(
            pitch_pairs=("tunnel_score", "size"),
            tunnel_score=("tunnel_score", "mean"),
            tunnel_stuff_score=("tunnel_stuff_score", "mean"),
            release_similarity=("release_similarity", "mean"),
            stuff_separation=("stuff_separation", "mean"),
            post_release_separation=("post_release_separation", "mean"),
            whiff_pct=("is_whiff_2", "mean"),
            csw_pct=("is_csw_2", "mean"),
            chase_pct=("is_chase_2", "mean"),
            rv_per_100=("rv_2", lambda s: s.sum() / len(s) * 100),
            xwoba_bip=("xwoba_2", "mean"),
        )
        .reset_index()
    )


def corr_summary(pairs: pd.DataFrame) -> pd.DataFrame:
    features = [
        ("tunnel_score", "Tunnel score"),
        ("tunnel_stuff_score", "Tunnel stuff score"),
        ("release_similarity", "Release similarity"),
        ("stuff_separation", "Stuff separation"),
        ("post_release_separation", "Post-release separation"),
        ("movement_separation", "Movement separation"),
        ("location_separation", "Location separation"),
        ("abs_d_release_speed", "Velocity gap"),
        ("abs_d_arm_angle", "Arm-angle gap"),
        ("abs_d_release_pos_x", "Release-side gap"),
        ("abs_d_release_pos_z", "Release-height gap"),
        ("abs_d_release_extension", "Extension gap"),
    ]
    outcomes = [
        ("is_whiff_2", "Whiff"),
        ("is_csw_2", "CSW"),
        ("is_chase_2", "Chase"),
        ("rv_2", "Run value"),
        ("xwoba_2", "xwOBA on BIP"),
    ]
    rows = []
    for x, xl in features:
        for y, yl in outcomes:
            sub = pairs[[x, y]].replace([np.inf, -np.inf], np.nan).dropna()
            rows.append(
                {
                    "feature": x,
                    "feature_label": xl,
                    "outcome": y,
                    "outcome_label": yl,
                    "n": len(sub),
                    "pearson_r": sub[x].corr(sub[y]) if len(sub) >= 3 else np.nan,
                    "spearman_rho": sub[x].corr(sub[y], method="spearman") if len(sub) >= 3 else np.nan,
                }
            )
    return pd.DataFrame(rows)


def pitcher_summary(pairs: pd.DataFrame, names: dict[int, str]) -> pd.DataFrame:
    out = (
        pairs.groupby("pitcher")
        .agg(
            pitch_pairs=("tunnel_score", "size"),
            tunnel_score=("tunnel_score", "mean"),
            tunnel_stuff_score=("tunnel_stuff_score", "mean"),
            release_similarity=("release_similarity", "mean"),
            stuff_separation=("stuff_separation", "mean"),
            post_release_separation=("post_release_separation", "mean"),
            whiff_pct=("is_whiff_2", "mean"),
            csw_pct=("is_csw_2", "mean"),
            chase_pct=("is_chase_2", "mean"),
            rv_per_100=("rv_2", lambda s: s.sum() / len(s) * 100),
            xwoba_bip=("xwoba_2", "mean"),
        )
        .reset_index()
    )
    out["pitcher_name"] = out["pitcher"].map(names).fillna(out["pitcher"].astype("Int64").astype(str))
    return out


def write_summary(bucket: pd.DataFrame, corr: pd.DataFrame, pitcher: pd.DataFrame, season: int, out_path: Path) -> None:
    lines = [
        f"# Tunnel score study, {season}",
        "",
        "Tunnel score = release similarity × post-release separation.",
        "",
        "Tunnel stuff score = release similarity × velocity/movement separation.",
        "",
        "Release similarity uses arm-angle, release-side, release-height, and extension gaps between consecutive pitches in the same plate appearance.",
        "",
        "Post-release separation uses velocity, movement, and final location gaps. The stuff-only score excludes final location to avoid blending deception with command/execution.",
        "",
        "## Tunnel-score buckets",
        "",
        "| Bucket | Pairs | Tunnel | Stuff tunnel | Release sim | Stuff sep | Full sep | Whiff% | CSW% | Chase% | RV/100 | xwOBA BIP |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, r in bucket.iterrows():
        lines.append(
            f"| {r['tunnel_bucket']} | {int(r['pitch_pairs'])} | {r['tunnel_score']:.3f} | {r['tunnel_stuff_score']:.3f} | "
            f"{r['release_similarity']:.3f} | {r['stuff_separation']:.3f} | {r['post_release_separation']:.3f} | "
            f"{r['whiff_pct']*100:.1f}% | {r['csw_pct']*100:.1f}% | "
            f"{r['chase_pct']*100:.1f}% | {r['rv_per_100']:.2f} | {r['xwoba_bip']:.3f} |"
        )

    lines.extend(["", "## Correlations vs pitch-2 outcomes", "", "| Feature | Outcome | n | Pearson r | Spearman rho |", "|---|---|---:|---:|---:|"])
    focus = corr[corr["outcome"].isin(["is_whiff_2", "is_csw_2", "is_chase_2", "rv_2"])]
    focus = focus[focus["feature"].isin(["tunnel_score", "tunnel_stuff_score", "release_similarity", "stuff_separation", "post_release_separation", "movement_separation", "location_separation", "abs_d_release_speed"])]
    for _, r in focus.iterrows():
        lines.append(f"| {r['feature_label']} | {r['outcome_label']} | {int(r['n'])} | {r['pearson_r']:.3f} | {r['spearman_rho']:.3f} |")

    lines.extend(
        [
            "",
            "## Pitcher leaders by average tunnel stuff score",
            "",
            "| Pitcher | Pairs | Stuff tunnel | Release sim | Stuff sep | Whiff% | CSW% |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    leaders = pitcher[pitcher["pitch_pairs"] >= 300].sort_values("tunnel_stuff_score", ascending=False).head(15)
    for _, r in leaders.iterrows():
        lines.append(
            f"| {r['pitcher_name']} | {int(r['pitch_pairs'])} | {r['tunnel_stuff_score']:.3f} | {r['release_similarity']:.3f} | "
            f"{r['stuff_separation']:.3f} | {r['whiff_pct']*100:.1f}% | {r['csw_pct']*100:.1f}% |"
        )

    lines.extend(
        [
            "",
            "## Working interpretation",
            "",
            "- This tests tunneling more directly than season-level pitcher correlations.",
            "- The score rewards pairs that look similar at release but separate later through velo/movement/location.",
            "- Next refinement: separate same-pitch vs different-pitch pairs, count state, batter handedness, and pitch location intent.",
            "",
        ]
    )
    out_path.write_text("\n".join(lines), encoding="utf-8")


def plot_bucket(bucket: pd.DataFrame, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(9, 6), dpi=180)
    ax.plot(bucket["tunnel_bucket"].astype(str), bucket["whiff_pct"] * 100, marker="o", label="Whiff%")
    ax.plot(bucket["tunnel_bucket"].astype(str), bucket["csw_pct"] * 100, marker="o", label="CSW%")
    ax.plot(bucket["tunnel_bucket"].astype(str), bucket["chase_pct"] * 100, marker="o", label="Chase%")
    ax.set_title("Pitch-2 outcomes by tunnel-score bucket", loc="left", fontsize=14, fontweight="bold")
    ax.set_ylabel("Rate")
    ax.grid(alpha=0.18)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--stage", default="regular_season")
    ap.add_argument("--warehouse", type=Path, default=REPO / "data" / "warehouse" / "mlb")
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()

    parquets = discover_parquets(args.warehouse, args.season, args.stage)
    if not parquets:
        raise SystemExit(f"No local parquets found for {args.season}/{args.stage}")
    print(f"Reading {len(parquets)} local parquets...", flush=True)
    df = read_pitch_table(parquets)
    print(f"Loaded {len(df):,} pitches", flush=True)
    pairs = score_pairs(pair_consecutive_pitches(df))
    pairs = pairs.dropna(subset=["tunnel_score", "rv_2"]).copy()
    names = load_pitcher_names(parquets, {int(x) for x in pairs["pitcher"].dropna().astype(int).unique()})
    bucket = bucket_summary(pairs)
    corr = corr_summary(pairs)
    pitcher = pitcher_summary(pairs, names)

    data_dir = args.out_dir / "data"
    img_dir = args.out_dir / "images"
    data_dir.mkdir(parents=True, exist_ok=True)
    img_dir.mkdir(parents=True, exist_ok=True)
    pairs.to_csv(data_dir / f"tunnel_score_pitch_pairs_{args.season}.csv", index=False)
    bucket.to_csv(data_dir / f"tunnel_score_buckets_{args.season}.csv", index=False)
    corr.to_csv(data_dir / f"tunnel_score_correlations_{args.season}.csv", index=False)
    pitcher.to_csv(data_dir / f"tunnel_score_pitchers_{args.season}.csv", index=False)
    write_summary(bucket, corr, pitcher, args.season, data_dir / f"tunnel_score_summary_{args.season}.md")
    plot_bucket(bucket, img_dir / f"tunnel_score_buckets_{args.season}.png")

    print(f"Pitch pairs: {len(pairs):,}")
    print(bucket.to_string(index=False))
    print(corr[corr["feature"].eq("tunnel_score")].to_string(index=False))
    print(f"Wrote outputs to {args.out_dir}")


if __name__ == "__main__":
    main()
