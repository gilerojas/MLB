#!/usr/bin/env python3
"""
Analyze Paul Skenes (MLB ID 694973) vs Dominican Republic WBC 2026 hitters
for the 2025 MLB regular season using Statcast (via pybaseball).

Outputs:
- skenes_vs_dr_2025_pitch_by_pitch.csv  (all pitches vs DR WBC hitters)
- skenes_vs_dr_2025_batter_summary.csv  (per-batter & overall results)

Run from repo root:
  python MLB/scripts/skenes_vs_dr_2025.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

try:
    from pybaseball import statcast_pitcher
except ImportError as e:
    print(
        "ERROR: pybaseball is required for this analysis. "
        "Install it with `pip install pybaseball` and try again.",
        file=sys.stderr,
    )
    raise


PITCHER_ID = 694973  # Paul Skenes

# Dominican Republic WBC 2026 hitters from wbc_2026_rosters.csv
# (team_name == 'Dominican Republic' and position_abbrev != 'P')
DR_HITTERS = {
    682663: "Agustín Ramírez",
    642708: "Amed Rosario",
    669224: "Austin Wells",
    467793: "Carlos Santana",
    665487: "Fernando Tatis Jr.",
    672695: "Geraldo Perdomo",
    665161: "Jeremy Peña",
    665742: "Juan Soto",
    677594: "Julio Rodríguez",
    691406: "Junior Caminero",
    516809: "Junior Lake",
    606466: "Ketel Marte",
    592518: "Manny Machado",
    665833: "Oneil Cruz",
    665489: "Vladimir Guerrero Jr.",
}


def load_skenes_2025() -> pd.DataFrame:
    """
    Load all Statcast pitches thrown by Skenes in the 2025 regular season.

    We pull the full 2025 range and then filter to game_type == 'R'
    to exclude Spring Training / postseason if present.
    """
    # Wide date window that safely covers the 2025 MLB season
    print("Downloading Statcast data for Skenes 2025 (this may take a bit)...")
    df = statcast_pitcher("2025-01-01", "2025-12-31", PITCHER_ID)
    if df is None or df.empty:
        raise RuntimeError("No Statcast data returned for Skenes in 2025.")

    df = df.copy()

    # Filter to regular season if game_type column is available
    if "game_type" in df.columns:
        before = len(df)
        df = df[df["game_type"] == "R"].copy()
        print(f"Filtered to regular season: {before} → {len(df)} pitches")
    else:
        print(
            "Warning: 'game_type' column not found; keeping all games in 2025 "
            "(may include non-regular-season)."
        )

    return df


def filter_vs_dr_hitters(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only pitches where the batter is a DR WBC 2026 hitter."""
    mask = df["batter"].isin(DR_HITTERS.keys())
    subset = df[mask].copy()
    if subset.empty:
        print(
            "No pitches found where Skenes faced any DR WBC 2026 hitter in 2025.",
            file=sys.stderr,
        )
    # Attach a readable batter_name column for convenience
    subset["batter_name"] = subset["batter"].map(DR_HITTERS)
    return subset


def summarize_batter_results(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a simple per-batter summary of results vs Skenes.

    Uses Statcast 'events' and 'description' columns. This is intentionally
    compact but gives a quick view of how each hitter handled him.
    """
    if df.empty:
        return pd.DataFrame()

    # Plate appearances: last pitch of each at-bat
    # Use game_pk + at_bat_number to identify PAs.
    grouping_cols = ["game_pk", "at_bat_number", "batter", "batter_name"]
    last_pitch = (
        df.sort_values(["game_pk", "at_bat_number", "pitch_number"])
        .groupby(grouping_cols, as_index=False)
        .tail(1)
    )

    def is_hit(ev: str | float) -> bool:
        if not isinstance(ev, str):
            return False
        return ev in {
            "single",
            "double",
            "triple",
            "home_run",
        }

    def is_hr(ev: str | float) -> bool:
        return isinstance(ev, str) and ev == "home_run"

    def is_bb(ev: str | float) -> bool:
        return isinstance(ev, str) and ev in {"walk", "hit_by_pitch", "intent_walk"}

    def is_k(ev: str | float, desc: str | float) -> bool:
        if isinstance(ev, str) and ev in {"strikeout", "strikeout_double_play"}:
            return True
        if isinstance(desc, str) and desc.startswith("strikeout"):
            return True
        return False

    last_pitch = last_pitch.copy()
    last_pitch["is_hit"] = last_pitch["events"].apply(is_hit)
    last_pitch["is_hr"] = last_pitch["events"].apply(is_hr)
    last_pitch["is_bb"] = last_pitch["events"].apply(is_bb)
    last_pitch["is_k"] = last_pitch.apply(
        lambda r: is_k(r.get("events"), r.get("description")), axis=1
    )

    # ABs: PAs that are not walks/HBP and not sacrifices
    def is_sac(ev: str | float) -> bool:
        return isinstance(ev, str) and ev in {"sac_fly", "sac_bunt"}

    last_pitch["is_sac"] = last_pitch["events"].apply(is_sac)
    last_pitch["is_ab"] = ~last_pitch["is_bb"] & ~last_pitch["is_sac"]

    grp = last_pitch.groupby(["batter", "batter_name"], dropna=False)

    summary = grp.agg(
        PA=("batter", "size"),
        AB=("is_ab", "sum"),
        H=("is_hit", "sum"),
        HR=("is_hr", "sum"),
        BB=("is_bb", "sum"),
        SO=("is_k", "sum"),
    ).reset_index()

    # Rate stats
    summary["AVG"] = summary["H"] / summary["AB"].replace(0, pd.NA)
    summary["OBP"] = (summary["H"] + summary["BB"]) / summary["PA"].replace(
        0, pd.NA
    )

    # Overall row
    overall = summary[["PA", "AB", "H", "HR", "BB", "SO"]].sum(numeric_only=True)
    if overall["PA"] > 0:
        overall_row = {
            "batter": 0,
            "batter_name": "ALL_DR_HITTERS",
            "PA": overall["PA"],
            "AB": overall["AB"],
            "H": overall["H"],
            "HR": overall["HR"],
            "BB": overall["BB"],
            "SO": overall["SO"],
            "AVG": overall["H"] / overall["AB"] if overall["AB"] > 0 else pd.NA,
            "OBP": (overall["H"] + overall["BB"]) / overall["PA"],
        }
        summary = pd.concat([summary, pd.DataFrame([overall_row])], ignore_index=True)

    summary = summary.sort_values(["batter_name", "batter"]).reset_index(drop=True)
    return summary


def main() -> int:
    df_all = load_skenes_2025()
    df_dr = filter_vs_dr_hitters(df_all)

    out_dir = Path("MLB") / "outputs" / "pitching_matchups"
    out_dir.mkdir(parents=True, exist_ok=True)

    pitch_by_pitch_path = out_dir / "skenes_vs_dr_2025_pitch_by_pitch.csv"
    batter_summary_path = out_dir / "skenes_vs_dr_2025_batter_summary.csv"

    # Always write the pitch-by-pitch subset (even if empty)
    df_dr.to_csv(pitch_by_pitch_path, index=False)
    print(f"Wrote pitch-by-pitch data to {pitch_by_pitch_path} ({len(df_dr)} pitches)")

    summary = summarize_batter_results(df_dr)
    summary.to_csv(batter_summary_path, index=False)
    print(
        f"Wrote batter summary to {batter_summary_path} "
        f"({len(summary)} rows, including overall if applicable)"
    )

    if df_dr.empty:
        print(
            "Note: No actual matchups were found; the CSVs are written but empty.",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

