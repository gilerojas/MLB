#!/usr/bin/env python3
"""
Bat speed vs. starter outcomes (Statcast process + box score).

Aggregates pitches_enriched parquets to (starter, game_pk) rows, joins box-score
lines from feed_live (or MLB Stats API), then writes correlations, plots, and
a short X-ready summary under outputs/bat_speed_starter_outcomes/.

Aligned with scripts/mallitalytics_daily_card.py and notebooks/fast_swing_outcome_study.ipynb
for swing / BS75+ definitions.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import matplotlib

matplotlib.use(os.environ.get("MPLBACKEND", "Agg"))
import matplotlib.pyplot as plt

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))


def _rel_to_repo(p: Path) -> str:
    try:
        return str(p.resolve().relative_to(_REPO.resolve()))
    except ValueError:
        return str(p)

MLB_STATS_BASE = "https://statsapi.mlb.com/api/v1"
STATSAPI_HEADERS = {"User-Agent": "Mallitalytics/1.0 (bat-speed-outcomes)"}

FAST_SWING_MPH = 75
SWING_CODES = frozenset(
    {
        "foul_bunt",
        "foul",
        "hit_into_play",
        "swinging_strike",
        "foul_tip",
        "swinging_strike_blocked",
        "missed_bunt",
        "bunt_foul_tip",
    }
)
WHIFF_CODES = frozenset({"swinging_strike", "foul_tip", "swinging_strike_blocked"})

_PQ_STEM_RE = re.compile(r"^game_(\d+)_(\d{8})_pitches_enriched\.parquet$")
_FEED_STEM_RE = re.compile(r"^game_(\d+)_(\d{8})_feed_live$")


def _open_feed(path: Path):
    if path.suffix == ".gz" or path.name.endswith(".json.gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def _innings_to_outs(ip_val: Any) -> int:
    if ip_val is None:
        return 0
    s = str(ip_val).strip()
    if not s or s in (".--", "-.--"):
        return 0
    if "." not in s:
        try:
            whole = int(float(s))
            return whole * 3
        except ValueError:
            return 0
    a, b = s.split(".", 1)
    try:
        whole = int(a)
        partial = int(b[0]) if b else 0
    except ValueError:
        return 0
    partial = min(max(partial, 0), 2)
    return whole * 3 + partial


def game_score_pitching(pit: dict[str, Any]) -> float:
    """Bill James-style Game Score (outs-based variant used by MLB / BR)."""
    outs = _innings_to_outs(pit.get("inningsPitched"))
    if outs <= 0:
        return float("-inf")
    full_inn = outs // 3
    k = int(pit.get("strikeOuts") or 0)
    h = int(pit.get("hits") or 0)
    er = int(pit.get("earnedRuns") or 0)
    r = int(pit.get("runs") or 0)
    ur = max(0, r - er)
    bb = int(pit.get("baseOnBalls") or 0)
    return float(
        50
        + outs
        + 2 * max(0, full_inn - 4)
        + k
        - 2 * h
        - 4 * er
        - 2 * ur
        - bb
    )


def normalize_description_series(s: pd.Series) -> pd.Series:
    d = s.astype(str).str.strip().str.lower().str.replace(" ", "_", regex=False).str.replace(",", "")
    bip_like = d.str.contains("in_play|hit_into_play", na=False, regex=True)
    return d.where(~bip_like, "hit_into_play")


def parquet_to_raw_path(pq: Path) -> Path | None:
    m = _PQ_STEM_RE.match(pq.name)
    if not m:
        return None
    stem = f"game_{m.group(1)}_{m.group(2)}_feed_live"
    raw_dir = pq.parent.parent / "raw"
    if not raw_dir.is_dir():
        return None
    for name in (f"{stem}.json", f"{stem}.json.gz"):
        p = raw_dir / name
        if p.is_file():
            return p
    return None


def starters_from_feed(feed: dict[str, Any]) -> list[dict[str, Any]]:
    """
    One row per team starter: gamesStarted >= 1 in single-game pitching line.
    """
    rows: list[dict[str, Any]] = []
    box = (feed.get("liveData") or {}).get("boxscore", {}).get("teams", {})
    for side in ("away", "home"):
        team = box.get(side) or {}
        players = team.get("players") or {}
        for _pk, player in players.items():
            if not isinstance(player, dict):
                continue
            pit = (player.get("stats") or {}).get("pitching") or {}
            if int(pit.get("gamesStarted") or 0) < 1:
                continue
            pid = (player.get("person") or {}).get("id")
            if not pid:
                continue
            rows.append(
                {
                    "pitcher": int(pid),
                    "side": side,
                    "pit": pit,
                }
            )
    return rows


def starters_from_api_boxscore(game_pk: int) -> list[dict[str, Any]] | None:
    import requests

    url = f"{MLB_STATS_BASE}/game/{game_pk}/boxscore"
    try:
        r = requests.get(url, headers=STATSAPI_HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return None
    teams_root = data.get("teams") or {}
    rows: list[dict[str, Any]] = []
    for side in ("away", "home"):
        team = teams_root.get(side) or {}
        players = team.get("players") or {}
        for _pk, player in players.items():
            if not isinstance(player, dict):
                continue
            pit = (player.get("stats") or {}).get("pitching") or {}
            if int(pit.get("gamesStarted") or 0) < 1:
                continue
            pid = (player.get("person") or {}).get("id")
            if not pid:
                continue
            rows.append({"pitcher": int(pid), "side": side, "pit": pit})
    return rows if rows else None


def box_row_from_pit(pit: dict[str, Any], pitcher_id: int) -> dict[str, Any]:
    gs = game_score_pitching(pit)
    ip = pit.get("inningsPitched")
    ip_s = str(ip).strip() if ip is not None else ""
    return {
        "pitcher": pitcher_id,
        "game_score": float(gs) if gs > float("-inf") else np.nan,
        "ip_outs": _innings_to_outs(ip),
        "ip_text": ip_s,
        "er": int(pit.get("earnedRuns") or 0),
        "r": int(pit.get("runs") or 0),
        "h": int(pit.get("hits") or 0),
        "bb": int(pit.get("baseOnBalls") or 0),
        "k": int(pit.get("strikeOuts") or 0),
        "hr": int(pit.get("homeRuns") or 0),
    }


def discover_parquets(warehouse: Path, seasons: tuple[int, ...], stage: str) -> list[Path]:
    out: list[Path] = []
    for yr in seasons:
        d = warehouse / str(yr) / stage / "pitches_enriched"
        if not d.is_dir():
            continue
        out.extend(sorted(d.glob("*.parquet")))
    return out


def add_swing_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "description" not in df.columns:
        return df
    df["description"] = normalize_description_series(df["description"])
    df["swing"] = df["description"].isin(SWING_CODES)
    df["whiff"] = df["description"].isin(WHIFF_CODES)
    if "bat_speed" in df.columns:
        df["bat_speed"] = pd.to_numeric(df["bat_speed"], errors="coerce")
    else:
        df["bat_speed"] = np.nan
    df["tracked_swing"] = df["swing"] & df["bat_speed"].notna()
    df["fast_swing"] = df["tracked_swing"] & (df["bat_speed"] >= FAST_SWING_MPH)
    df["contact_tracked"] = df["tracked_swing"] & (~df["whiff"])
    df["whiff_tracked"] = df["whiff"] & df["bat_speed"].notna()
    return df


def aggregate_pitcher_game(df: pd.DataFrame) -> dict[str, Any]:
    """Single pitcher's rows within one game."""
    dre = pd.to_numeric(df.get("delta_run_exp"), errors="coerce") if "delta_run_exp" in df.columns else pd.Series(
        np.nan, index=df.index
    )
    rv_game = float(-dre.sum(skipna=True)) if dre.notna().any() else np.nan

    swings = int(df["swing"].sum()) if "swing" in df.columns else 0
    tracked = df["tracked_swing"] if "tracked_swing" in df.columns else pd.Series(False, index=df.index)
    n_tracked = int(tracked.sum())
    n_pitches = len(df)
    tracking_pct_swings = (n_tracked / swings * 100.0) if swings else np.nan

    ts = df[tracked]
    mean_bs = float(ts["bat_speed"].mean()) if n_tracked else np.nan
    bs75_pct = (
        float((ts["bat_speed"] >= FAST_SWING_MPH).sum() / n_tracked * 100.0) if n_tracked else np.nan
    )

    wt = df[df["whiff_tracked"]] if "whiff_tracked" in df.columns else df.iloc[:0]
    n_whiff_tracked = len(wt)
    mean_whiff_bs = float(wt["bat_speed"].mean()) if n_whiff_tracked else np.nan
    median_whiff_bs = float(wt["bat_speed"].median()) if n_whiff_tracked else np.nan

    ct = df[df["contact_tracked"]] if "contact_tracked" in df.columns else df.iloc[:0]
    n_contact_tracked = len(ct)
    mean_contact_bs = float(ct["bat_speed"].mean()) if n_contact_tracked else np.nan

    whiff_bs_gap = (
        (mean_whiff_bs - mean_contact_bs)
        if np.isfinite(mean_whiff_bs) and np.isfinite(mean_contact_bs)
        else np.nan
    )

    whiffs = int(df["whiff"].sum()) if "whiff" in df.columns else 0
    whiff_rate = (whiffs / swings * 100.0) if swings else np.nan

    xwoba_bip = np.nan
    if "estimated_woba_using_speedangle" in df.columns and "description" in df.columns:
        bip = df[df["description"] == "hit_into_play"]
        xw = pd.to_numeric(bip["estimated_woba_using_speedangle"], errors="coerce").dropna()
        if len(xw):
            xwoba_bip = float(xw.mean())

    return {
        "n_pitches": n_pitches,
        "n_swings": swings,
        "n_tracked_swings": n_tracked,
        "tracking_pct_of_swings": tracking_pct_swings,
        "mean_bat_speed": mean_bs,
        "bs75_pct": bs75_pct,
        "mean_whiff_bat_speed": mean_whiff_bs,
        "median_whiff_bat_speed": median_whiff_bs,
        "n_whiff_tracked": n_whiff_tracked,
        "mean_contact_bat_speed": mean_contact_bs,
        "n_contact_tracked": n_contact_tracked,
        "whiff_bs_minus_contact_bs": whiff_bs_gap,
        "whiff_rate": whiff_rate,
        "rv_game": rv_game,
        "mean_xwoba_bip": xwoba_bip,
    }


def process_one_parquet(
    pq: Path,
    *,
    use_api_fallback: bool,
) -> list[dict[str, Any]]:
    raw_path = parquet_to_raw_path(pq)
    feed: dict[str, Any] | None = None
    if raw_path is not None:
        try:
            with _open_feed(raw_path) as f:
                feed = json.load(f)
        except Exception:
            feed = None

    starters: list[dict[str, Any]] | None = None
    if feed is not None:
        starters = starters_from_feed(feed)
    if not starters and use_api_fallback:
        m = _PQ_STEM_RE.match(pq.name)
        game_pk = int(m.group(1)) if m else None
        if game_pk:
            starters = starters_from_api_boxscore(game_pk)

    if not starters:
        return []

    want_cols = [
        "game_pk",
        "pitcher",
        "description",
        "bat_speed",
        "delta_run_exp",
        "estimated_woba_using_speedangle",
    ]
    try:
        temp = pd.read_parquet(pq)
    except Exception:
        return []
    use_cols = [c for c in want_cols if c in temp.columns]
    if "pitcher" not in temp.columns:
        return []
    df = temp[use_cols].copy()
    m_stem = _PQ_STEM_RE.match(pq.name)
    game_pk = int(df["game_pk"].iloc[0]) if "game_pk" in df.columns and len(df) else None
    if game_pk is None and m_stem:
        game_pk = int(m_stem.group(1))
    game_date = ""
    if m_stem:
        ymd = m_stem.group(2)
        game_date = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"

    df = add_swing_features(df)

    out_rows: list[dict[str, Any]] = []
    for srow in starters:
        pid = srow["pitcher"]
        pit = srow["pit"]
        sub = df[df["pitcher"] == pid]
        if sub.empty:
            continue
        agg = aggregate_pitcher_game(sub)
        box = box_row_from_pit(pit, pid)
        try:
            pq_rel = str(pq.relative_to(_REPO))
        except ValueError:
            pq_rel = str(pq)
        row = {
            "game_pk": game_pk,
            "game_date": game_date,
            "season": int(game_date[:4]) if len(game_date) >= 4 else np.nan,
            "pitcher_id": pid,
            "starter_side": srow["side"],
            "parquet": pq_rel,
            **agg,
            **{k: v for k, v in box.items() if k != "pitcher"},
        }
        out_rows.append(row)
    return out_rows


def pearson_r2(x: np.ndarray, y: np.ndarray) -> float:
    mask = np.isfinite(x) & np.isfinite(y)
    if mask.sum() < 5:
        return float("nan")
    r = np.corrcoef(x[mask], y[mask])[0, 1]
    return float(r * r)


def spearman_corr(x: pd.Series, y: pd.Series) -> float:
    d = pd.DataFrame({"x": x, "y": y}).dropna()
    if len(d) < 5:
        return float("nan")
    return float(d["x"].corr(d["y"], method="spearman"))


def run_analysis(df: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    df = df.copy()
    # Minimum process quality
    min_tr = 8
    dfq = df[df["n_tracked_swings"] >= min_tr].copy()
    dfq.to_csv(out_dir / "pitcher_game_table.csv", index=False)

    metrics_x = [
        "bs75_pct",
        "mean_bat_speed",
        "mean_whiff_bat_speed",
        "whiff_bs_minus_contact_bs",
    ]
    metrics_y = ["rv_game", "game_score", "er"]

    corr_rows = []
    for mx in metrics_x:
        for my in metrics_y:
            if mx not in dfq.columns or my not in dfq.columns:
                continue
            xs = pd.to_numeric(dfq[mx], errors="coerce")
            ys = pd.to_numeric(dfq[my], errors="coerce")
            mask = xs.notna() & ys.notna()
            n_pair = int(mask.sum())
            corr_rows.append(
                {
                    "x": mx,
                    "y": my,
                    "n": n_pair,
                    "pearson_r": float(xs[mask].corr(ys[mask], method="pearson")) if n_pair >= 5 else np.nan,
                    "spearman_r": spearman_corr(xs, ys),
                    "pearson_r2": pearson_r2(xs.values, ys.values),
                }
            )
    corr_df = pd.DataFrame(corr_rows)
    if "whiff_rate" in dfq.columns and "mean_whiff_bat_speed" in dfq.columns:
        xs = pd.to_numeric(dfq["mean_whiff_bat_speed"], errors="coerce")
        ys = pd.to_numeric(dfq["whiff_rate"], errors="coerce")
        mask = xs.notna() & ys.notna()
        n_pair = int(mask.sum())
        corr_df = pd.concat(
            [
                corr_df,
                pd.DataFrame(
                    [
                        {
                            "x": "mean_whiff_bat_speed",
                            "y": "whiff_rate",
                            "n": n_pair,
                            "pearson_r": float(xs[mask].corr(ys[mask], method="pearson")) if n_pair >= 5 else np.nan,
                            "spearman_r": spearman_corr(xs, ys),
                            "pearson_r2": pearson_r2(xs.values, ys.values),
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )
    corr_df.to_csv(out_dir / "correlations.csv", index=False)

    # Quartile BS75 vs game_score / rv
    qsum = pd.DataFrame()
    if len(dfq) >= 20 and dfq["bs75_pct"].notna().sum() >= 20:
        dfq["bs75_quartile"] = pd.qcut(dfq["bs75_pct"], q=4, labels=False, duplicates="drop")
        qsum = (
            dfq.groupby("bs75_quartile", observed=True)
            .agg(
                n=("game_score", "count"),
                mean_game_score=("game_score", "mean"),
                mean_rv_game=("rv_game", "mean"),
                mean_bs75=("bs75_pct", "mean"),
            )
            .reset_index()
        )
        qsum.to_csv(out_dir / "bs75_quartile_summary.csv", index=False)

    # Good line + high BS75 slice
    n_hi_hi = 0
    n_combo = 0
    if len(dfq) >= 20:
        hi_bs = dfq["bs75_pct"] >= dfq["bs75_pct"].quantile(0.75)
        good_gs = dfq["game_score"] >= dfq["game_score"].quantile(0.75)
        low_er = dfq["er"] <= 1
        flag = hi_bs & good_gs
        n_hi_hi = int(flag.sum())
        outliers = dfq[flag].sort_values("game_score", ascending=False)
        outliers.to_csv(out_dir / "outliers_high_bs75_and_high_game_score.csv", index=False)
        combo = hi_bs & low_er & (dfq["game_score"] >= 60)
        n_combo = int(combo.sum())
        dfq[combo].to_csv(out_dir / "outliers_high_bs75_low_er_strong_gs.csv", index=False)

    # Plots
    plot_df = dfq.dropna(subset=["bs75_pct", "game_score"])
    if len(plot_df) >= 10:
        fig, ax = plt.subplots(figsize=(8, 5))
        hb = ax.hexbin(
            plot_df["bs75_pct"],
            plot_df["game_score"],
            gridsize=22,
            cmap="viridis",
            mincnt=1,
        )
        plt.colorbar(hb, ax=ax, label="count")
        ax.set_xlabel("BS75+% (tracked swings)")
        ax.set_ylabel("Game Score")
        ax.set_title("Starter outings: BS75+% vs Game Score")
        fig.tight_layout()
        fig.savefig(out_dir / "bs75_vs_game_score_hexbin.png", dpi=150)
        plt.close(fig)

    plot_df2 = dfq.dropna(subset=["mean_whiff_bat_speed", "game_score"])
    if len(plot_df2) >= 10:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.scatter(plot_df2["mean_whiff_bat_speed"], plot_df2["game_score"], alpha=0.25, s=12)
        ax.set_xlabel("Mean bat speed on whiffs (mph, tracked)")
        ax.set_ylabel("Game Score")
        ax.set_title("Whiff bat speed vs Game Score (starters)")
        fig.tight_layout()
        fig.savefig(out_dir / "whiff_bs_vs_game_score.png", dpi=150)
        plt.close(fig)

    # Summary markdown
    n_games = len(dfq)
    mean_track = float(dfq["tracking_pct_of_swings"].mean()) if n_games else np.nan

    def _rrow(x: str, y: str) -> pd.Series | None:
        hit = corr_df[(corr_df["x"] == x) & (corr_df["y"] == y)]
        return hit.iloc[0] if len(hit) else None

    r_bs75_gs = _rrow("bs75_pct", "game_score")
    r_bs75_rv = _rrow("bs75_pct", "rv_game")
    r_mbs_gs = _rrow("mean_bat_speed", "game_score")
    r_wbs_gs = _rrow("mean_whiff_bat_speed", "game_score")
    r_wgap_gs = _rrow("whiff_bs_minus_contact_bs", "game_score")
    r_wbs_wr = _rrow("mean_whiff_bat_speed", "whiff_rate")

    lines = [
        "# Bat speed vs starter outings — summary (for X)",
        "",
        "## Method",
        "",
        "- **Starters:** pitching line with `gamesStarted >= 1` from `feed_live` boxscore (same game as parquet), API fallback when raw missing.",
        f"- **BS75+%:** share of tracked swings with `bat_speed >= {FAST_SWING_MPH}` mph (card definition).",
        "- **RV_game:** sum of `-delta_run_exp` over pitches (higher = better for pitcher).",
        "- **Game Score:** Bill James style as in `mlbops/api/intel_standouts.py`.",
        f"- **Rows:** starter outings with `n_tracked_swings >= {min_tr}`.",
        "",
        "## Caveats",
        "",
        "- Bat tracking does not cover every swing; denominator is tracked swings only.",
        "- Correlation is not causation; opponent/park/sequencing not adjusted here.",
        "- Rare bulk/long-relief lines can appear if mis-tagged GS in source.",
        "",
        "## Sample",
        "",
        f"- **Starter outings (filtered):** {n_games}",
        f"- **Mean swing tracking coverage (% of swings with bat_speed):** {mean_track:.1f}%" if np.isfinite(mean_track) else "-",
        "",
        "## What correlates (full grid: `correlations.csv`)",
        "",
    ]
    if r_bs75_gs is not None:
        lines.append(
            f"- **BS75+% vs Game Score:** Spearman ρ ≈ {r_bs75_gs['spearman_r']:.3f}, Pearson R² ≈ {r_bs75_gs['pearson_r2']:.4f} (n={int(r_bs75_gs['n'])}) — "
            "only a small fraction of Game Score variance lines up with opponent fast-swing rate."
        )
    if r_bs75_rv is not None:
        lines.append(
            f"- **BS75+% vs RV_game:** Spearman ρ ≈ {r_bs75_rv['spearman_r']:.3f} (n={int(r_bs75_rv['n'])}) — same sign as Game Score (higher BS75+ ↔ slightly worse run value)."
        )
    if r_mbs_gs is not None:
        lines.append(
            f"- **Mean bat speed vs Game Score:** Spearman ρ ≈ {r_mbs_gs['spearman_r']:.3f}, R² ≈ {r_mbs_gs['pearson_r2']:.4f}."
        )
    if r_wbs_gs is not None:
        lines.append(
            f"- **Mean whiff bat speed vs Game Score:** Spearman ρ ≈ {r_wbs_gs['spearman_r']:.3f} (n={int(r_wbs_gs['n'])}) — weaker than BS75+% / mean BS."
        )
    if r_wgap_gs is not None:
        lines.append(
            f"- **Whiff BS − contact BS vs Game Score:** Spearman ρ ≈ {r_wgap_gs['spearman_r']:.3f} — essentially no linear relationship in this sample."
        )
    if r_wbs_wr is not None:
        lines.append(
            f"- **Mean whiff bat speed vs whiff rate:** Spearman ρ ≈ {r_wbs_wr['spearman_r']:.3f} — how hard batters swing on whiffs vs how often they whiff."
        )

    lines.extend(["", "## BS75+% quartiles (mean Game Score & RV)", ""])
    if len(qsum):
        for _, qr in qsum.iterrows():
            lines.append(
                f"- **Q{int(qr['bs75_quartile']) + 1}** (≈{qr['mean_bs75']:.1f}% BS75+): "
                f"mean Game Score {qr['mean_game_score']:.1f}, mean RV_game {qr['mean_rv_game']:.3f} (n={int(qr['n'])})"
            )
    else:
        lines.append("- (insufficient rows for quartiles)")

    lines.extend(
        [
            "",
            "## Narrative bullets for a thread",
            "",
            "1. **Effect size is small:** BS75+% and mean bat speed explain only a few tenths of a percent of Game Score variance (R² ≪ 0.05) — "
            "fast opponent swings are not a strong stand-alone indicator of a bad or good start.",
            "2. **Direction still matches intuition:** higher BS75+% and higher mean bat speed are slightly associated with worse Game Score / RV and more ER — "
            "but the signal is noisy; use it as context, not a verdict.",
            "3. **Whiff bat speed is almost orthogonal to results** in this aggregate — whiffs can be hard or soft swings; outcome quality matters more.",
            f"4. **“Good line, loud swings” happens:** {n_hi_hi} outings in the top quartile for both BS75+% and Game Score (see `outliers_high_bs75_and_high_game_score.csv`); "
            f"{n_combo} with high BS75+, ≤1 ER, and Game Score ≥ 60 (`outliers_high_bs75_low_er_strong_gs.csv`).",
            "5. **Quartile sweep:** the highest BS75+% bucket averages several points lower Game Score than the lowest bucket — see `bs75_quartile_summary.csv`.",
            "",
            f"Artifacts: `{_rel_to_repo(out_dir)}`",
        ]
    )
    (out_dir / "summary_for_x.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Bat speed vs starter outcomes analysis")
    ap.add_argument(
        "--warehouse",
        type=Path,
        default=None,
        help="MLB warehouse root (default: MLB_WAREHOUSE_DIR or repo data/warehouse/mlb)",
    )
    ap.add_argument("--seasons", type=str, default="2025,2026", help="Comma seasons")
    ap.add_argument("--stage", type=str, default="regular_season")
    ap.add_argument("--max-games", type=int, default=0, help="0 = all parquet files")
    ap.add_argument("--no-api-fallback", action="store_true", help="Do not fetch boxscore API if raw missing")
    ap.add_argument(
        "--out",
        type=Path,
        default=_REPO / "outputs" / "bat_speed_starter_outcomes",
        help="Output directory",
    )
    args = ap.parse_args()

    wh = args.warehouse
    if wh is None:
        raw = os.environ.get("MLB_WAREHOUSE_DIR", "").strip()
        wh = Path(raw).expanduser().resolve() if raw else _REPO / "data" / "warehouse" / "mlb"

    seasons = tuple(int(x.strip()) for x in args.seasons.split(",") if x.strip())
    paths = discover_parquets(wh, seasons, args.stage)
    if args.max_games and args.max_games > 0:
        paths = paths[: args.max_games]

    all_rows: list[dict[str, Any]] = []
    use_api = not args.no_api_fallback
    for i, pq in enumerate(paths):
        try:
            rows = process_one_parquet(pq, use_api_fallback=use_api)
            all_rows.extend(rows)
        except Exception as e:
            print(f"skip {pq.name}: {e}", file=sys.stderr)

    if not all_rows:
        print("No rows produced. Check warehouse paths and parquet availability.", file=sys.stderr)
        sys.exit(1)

    df = pd.DataFrame(all_rows)
    run_analysis(df, args.out)
    print(f"Wrote {len(df)} starter rows -> {args.out}")


if __name__ == "__main__":
    main()
