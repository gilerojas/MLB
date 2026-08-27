"""Phase 1 -- build the pitch-level shape matrix.

One row per pitch, carrying the physical shape columns the archetype study needs
plus enough context to aggregate to pitcher-seasons later. Reads the enriched
Statcast parquets directly: the existing `pitch_rows_v5` study cache drops every
movement column (no pfx, no spin_axis, no arm_angle), so it cannot be reused.

    ./mlb_env.nosync/bin/python research/study/pitching_archetypes/build_pitch_dataset.py --years 2025

Output: research/study/.cache/archetype_pitches_<years>.parquet (untracked).

Coverage is reported, never silently imputed. `arm_angle`, `bat_speed` and
friends only exist from 2023 on, and a pitch missing any shape column is kept in
the cache and excluded at the modelling step, so the loss is visible.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from research.study.pitching_archetypes.features import (  # noqa: E402
    add_shape_features,
    mirror_handedness,
)

CACHE = ROOT / "research/study/.cache"
WAREHOUSE = ROOT / "data/warehouse/mlb"

# Everything Level 1-3 could need, and nothing else -- these files are 2,400 per
# season and column pruning is most of the read time.
COLUMNS = [
    "game_pk", "game_date", "game_year", "pitcher", "batter", "stand", "p_throws",
    "pitch_type", "pitch_name", "release_speed", "release_spin_rate",
    "release_extension", "release_pos_x", "release_pos_z", "pfx_x", "pfx_z",
    "spin_axis", "arm_angle", "plate_x", "plate_z", "zone", "description",
    "events", "balls", "strikes", "inning", "inning_topbot", "at_bat_number",
    "pitch_number", "n_thruorder_pitcher", "delta_run_exp",
    "estimated_woba_using_speedangle", "woba_value", "woba_denom",
    "launch_speed", "launch_angle", "bb_type",
]


def _load_registry(year: int) -> dict[int, str]:
    path = WAREHOUSE / str(year) / "players_registry.json"
    if not path.exists():
        return {}
    raw = json.loads(path.read_text())
    return {int(k): v.get("fullName", "") for k, v in raw.items()}


def _read_season(year: int) -> pd.DataFrame:
    d = WAREHOUSE / str(year) / "regular_season" / "pitches_enriched"
    files = sorted(d.glob("*_pitches_enriched.parquet"))
    if not files:
        raise SystemExit(f"no enriched parquets under {d}")

    available = set(pq.ParquetFile(files[0]).schema.names)
    cols = [c for c in COLUMNS if c in available]
    missing = [c for c in COLUMNS if c not in available]
    if missing:
        print(f"  {year}: columns absent from schema -> {missing}")

    frames = []
    for i, f in enumerate(files, 1):
        frames.append(pq.read_table(f, columns=cols).to_pandas())
        if i % 400 == 0:
            print(f"  {year}: {i}/{len(files)} games")
    df = pd.concat(frames, ignore_index=True)
    print(f"  {year}: {len(files)} games, {len(df):,} pitches")
    return df


def _flag_starters(df: pd.DataFrame) -> pd.DataFrame:
    """A pitcher is that game's starter for his side if he threw its first pitch.

    Derived from the pitch stream rather than the boxscore so this phase stays
    independent of the feed_live files, which cover only part of 2024/2025.
    """
    df = df.sort_values(["game_pk", "at_bat_number", "pitch_number"])
    first = (
        df.groupby(["game_pk", "inning_topbot"], observed=True)["pitcher"]
        .first()
        .rename("starter_id")
        .reset_index()
    )
    df = df.merge(first, on=["game_pk", "inning_topbot"], how="left")
    df["is_starter"] = df["pitcher"] == df["starter_id"]
    return df.drop(columns=["starter_id"])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", nargs="+", type=int, default=[2025])
    args = ap.parse_args()

    CACHE.mkdir(parents=True, exist_ok=True)

    frames = []
    names: dict[int, str] = {}
    for year in args.years:
        frames.append(_read_season(year))
        names.update(_load_registry(year))
    df = pd.concat(frames, ignore_index=True)

    df = _flag_starters(df)
    df["pitcher_name"] = df["pitcher"].map(names)
    df = mirror_handedness(df)
    df = add_shape_features(df)

    shape_cols = ["release_speed", "pfx_x_mir_in", "pfx_z_in", "release_spin_rate",
                  "spin_axis_mir", "arm_angle", "release_extension"]
    print("\ncoverage (non-null share of all pitches):")
    for c in shape_cols:
        if c in df.columns:
            print(f"  {c:24s} {df[c].notna().mean():6.3f}")
    unnamed = df["pitcher_name"].isna().mean()
    print(f"  {'pitcher_name':24s} {1 - unnamed:6.3f}")

    tag = "-".join(str(y) for y in args.years)
    out = CACHE / f"archetype_pitches_{tag}.parquet"
    df.to_parquet(out, index=False)
    print(f"\nwrote {out}  ({len(df):,} rows x {df.shape[1]} cols)")


if __name__ == "__main__":
    main()
