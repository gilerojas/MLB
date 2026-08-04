"""Phase 1 — build the MalliScore validation dataset.

One row per starting-pitcher outing, 2024-2026, carrying everything the study needs:
V3 inputs and outputs under both normalizations, Game Score v1/v2 benchmarks,
leak-free rolling pitcher baselines, and a link to the pitcher's next start.

    ./mlb_env.nosync/bin/python research/study/malliscore_validation/build_dataset.py

Season roles are enforced downstream, not here: 2024 is development, 2025 is
validation and is read once at the Phase 6 gate, 2026 is final confirmation.

Two upstream measurement issues are recorded per row rather than silently absorbed:

  * `xwoba_allowed` is a mean over PA-ending rows where `pa_xwoba` falls back to
    `woba_value` for non-batted-ball outcomes (starter_outings.py:179-185), i.e. a
    mixed estimator, and it is imputed to the league mean 0.320 when absent
    (starter_outings.py:278). `xwoba_imputed` flags the latter.
  * Earned runs exist only in raw feed_live files, which cover 40% of 2024 games
    and 54% of 2025. Rows without true ER are dropped rather than defaulted to 2
    as `malli_score_from_actual` does.
"""

from __future__ import annotations

import contextlib
import gzip
import io
import json
import sys
import warnings
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from research.study.malliscore_validation.game_score import add_game_scores  # noqa: E402
from research.study.starter_outings import aggregate_starter_outings  # noqa: E402
from src.pitching_performances.malli_score import (  # noqa: E402
    MALLISCORE_VERSION,
    OutingRawMetrics,
    default_league_norms,
    malliscore_v2,
    refine_league_norms,
)

CACHE = ROOT / "research/study/.cache"
PITCH_CACHE = CACHE / "pitch_rows_v5_2024_2025_2026_2024-2025-2026_full.parquet"
BOX_CACHE = CACHE / "box_context_v5_2024_2025_2026_2024-2025-2026_full.parquet"
RUNS_CACHE = CACHE / "starter_runs_allowed_2024_2026.parquet"
OUT_DIR = Path(__file__).parent / "outputs"
OUT_PATH = OUT_DIR / "outings_2024_2026.parquet"

SEASONS = (2024, 2025, 2026)
ROLL_WINDOWS = (3, 5, 10)
COMPONENTS = [
    "swstr_pct",
    "called_strike_pct",
    "chase_pct",
    "xwoba_allowed",
    "game_whip",
    "earned_runs",
    "home_runs",
    "outs",
    "pitches",
]
CARRY_FORWARD = ["malli_score", "game_score_v1", "game_score_v2", "k_minus_bb_pct", *COMPONENTS]


def extract_runs_allowed(seasons=SEASONS) -> pd.DataFrame:
    """Pull total runs allowed per starter from the raw feeds.

    The cached box context stores earned runs only, but Game Score v2 is defined on
    total runs. Roughly 7-8% of runs are unearned, which at -3 points per run is too
    large to ignore in the study's primary benchmark.
    """
    if RUNS_CACHE.exists():
        return pd.read_parquet(RUNS_CACHE)

    rows: list[dict] = []
    for season in seasons:
        feeds = sorted((ROOT / f"data/warehouse/mlb/{season}/regular_season/raw").glob(
            "*feed_live.json.gz"
        ))
        print(f"  {season}: parsing {len(feeds)} feeds for runs allowed...", flush=True)
        for path in feeds:
            try:
                with gzip.open(path, "rt", encoding="utf-8") as fh:
                    feed = json.load(fh)
            except Exception:
                continue
            game_pk = feed.get("gamePk")
            teams = ((feed.get("liveData") or {}).get("boxscore") or {}).get("teams") or {}
            for team_box in teams.values():
                for player in (team_box.get("players") or {}).values():
                    stats = (player.get("stats") or {}).get("pitching") or {}
                    if not stats or int(stats.get("gamesStarted") or 0) < 1:
                        continue
                    try:
                        pitcher = int((player.get("person") or {}).get("id"))
                    except (TypeError, ValueError):
                        continue
                    rows.append(
                        {"game_pk": game_pk, "pitcher": pitcher, "runs": stats.get("runs")}
                    )

    out = pd.DataFrame(rows).drop_duplicates(subset=["game_pk", "pitcher"])
    out["runs"] = pd.to_numeric(out["runs"], errors="coerce")
    RUNS_CACHE.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(RUNS_CACHE, index=False)
    print(f"  cached {len(out):,} starter run lines -> {RUNS_CACHE.name}")
    return out


def raw_metrics(row) -> OutingRawMetrics:
    outs = max(1, int(round(float(row.outs))))
    return OutingRawMetrics(
        swstr_pct=float(row.swstr_pct),
        called_strike_pct=float(row.called_strike_pct),
        chase_pct=float(row.chase_pct),
        xwoba_allowed=float(row.xwoba_allowed),
        game_whip=(float(row.hits) + float(row.walks)) / (outs / 3.0),
        earned_runs=max(0, int(round(float(row.earned_runs)))),
        home_runs=max(0, int(round(float(row.home_runs)))),
        pitches=max(1, int(round(float(row.pitches)))),
        outs=outs,
    )


def add_v3_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Score every outing under fixed priors and, separately, under same-slate norms.

    Only render.py uses slate norms in production while every other consumer uses
    fixed priors, so the two scales coexist today. Carrying both makes the drift
    measurable in Phase 2.
    """
    out = df.copy()
    raws = [raw_metrics(r) for r in out.itertuples()]
    out["game_whip"] = [r.game_whip for r in raws]

    fixed = default_league_norms()
    scored_fixed = [malliscore_v2(r, fixed) for r in raws]
    for key in ("dominance_score", "run_prevention_score", "core_score", "workload", "malli_score"):
        out[key] = [s[key] for s in scored_fixed]

    # Same-slate norms are refined per game_date, mirroring the daily board.
    out["_i"] = range(len(out))
    slate_scores = pd.Series(index=out.index, dtype=float)
    slate_dom = pd.Series(index=out.index, dtype=float)
    slate_rp = pd.Series(index=out.index, dtype=float)
    for _, slate in out.groupby("game_date"):
        idx = slate["_i"].tolist()
        norms = refine_league_norms([raws[i] for i in idx])
        for i, label in zip(idx, slate.index):
            s = malliscore_v2(raws[i], norms)
            slate_scores.at[label] = s["malli_score"]
            slate_dom.at[label] = s["dominance_score"]
            slate_rp.at[label] = s["run_prevention_score"]
    out["malli_score_slate"] = slate_scores
    out["dominance_score_slate"] = slate_dom
    out["run_prevention_score_slate"] = slate_rp
    out["malli_slate_delta"] = out["malli_score_slate"] - out["malli_score"]
    return out.drop(columns="_i")


def add_rolling_baselines(df: pd.DataFrame) -> pd.DataFrame:
    """Leak-free rolling pitcher baselines: shifted, so the current start is excluded.

    These are the "prior pitcher ability" control for Phase 5. Any leakage here
    would manufacture predictive signal, so the shift happens before the roll.
    """
    out = df.sort_values(["pitcher", "game_date", "game_pk"]).copy()
    grouped = out.groupby("pitcher", sort=False)
    for col in CARRY_FORWARD:
        if col not in out.columns:
            continue
        prior = grouped[col].shift(1)
        for window in ROLL_WINDOWS:
            out[f"{col}_roll{window}"] = prior.groupby(out["pitcher"], sort=False).transform(
                lambda s, w=window: s.rolling(w, min_periods=max(2, w // 2)).mean()
            )
    out["career_start_index"] = grouped.cumcount()
    out["rest_days"] = grouped["game_date"].diff().dt.days
    return out


def add_next_start(df: pd.DataFrame) -> pd.DataFrame:
    """Link each outing to the pitcher's next start within the same season."""
    out = df.sort_values(["pitcher", "season", "game_date", "game_pk"]).copy()
    grouped = out.groupby(["pitcher", "season"], sort=False)
    for col in CARRY_FORWARD:
        if col in out.columns:
            out[f"next_{col}"] = grouped[col].shift(-1)
    out["next_game_date"] = grouped["game_date"].shift(-1)
    out["next_start_gap_days"] = (out["next_game_date"] - out["game_date"]).dt.days
    out["has_next_start"] = out["next_malli_score"].notna()
    return out


def main() -> None:
    warnings.filterwarnings("ignore")
    print(f"Building MalliScore validation dataset (V3 = {MALLISCORE_VERSION})")

    print("\n1. Loading cached warehouse extracts")
    pitches = pd.read_parquet(PITCH_CACHE)
    box = pd.read_parquet(BOX_CACHE)
    print(f"   {len(pitches):,} pitch rows | {len(box):,} box lines")

    print("\n2. Extracting total runs allowed from raw feeds")
    runs = extract_runs_allowed()

    print("\n3. Aggregating starter outings")
    with contextlib.redirect_stdout(io.StringIO()):
        outings = aggregate_starter_outings(pitches, box)
    print(f"   {len(outings):,} starter outings before ER filter")

    before = len(outings)
    outings = outings[outings["earned_runs"].notna()].copy()
    print(f"   {len(outings):,} with true boxscore ER ({before - len(outings):,} dropped)")

    outings["game_date"] = pd.to_datetime(outings["game_date"])
    outings["xwoba_imputed"] = outings["xwoba_allowed"].eq(0.320)
    outings = outings.merge(runs, on=["game_pk", "pitcher"], how="left")

    print("\n4. Scoring V3 under fixed and same-slate norms")
    outings = add_v3_scores(outings)

    print("5. Adding Game Score v1/v2 benchmarks")
    outings = add_game_scores(outings)
    bf = pd.to_numeric(outings["batters_faced"], errors="coerce").clip(lower=1)
    outings["k_minus_bb_pct"] = (outings["strikeouts"] - outings["walks"]) / bf * 100.0

    print("6. Adding leak-free rolling baselines")
    outings = add_rolling_baselines(outings)

    print("7. Linking next starts")
    outings = add_next_start(outings)

    outings = outings.sort_values(["season", "game_date", "game_pk", "pitcher"]).reset_index(
        drop=True
    )
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    outings.to_parquet(OUT_PATH, index=False)

    print(f"\nWrote {len(outings):,} rows x {outings.shape[1]} cols -> {OUT_PATH.name}")
    summary = outings.groupby("season").agg(
        outings=("pitcher", "size"),
        pitchers=("pitcher", "nunique"),
        malli_mean=("malli_score", "mean"),
        gs_v2_mean=("game_score_v2", "mean"),
        with_next=("has_next_start", "sum"),
        runs_imputed=("gs_v2_runs_imputed", "sum"),
        xwoba_imputed=("xwoba_imputed", "sum"),
    )
    print(summary.round(2).to_string())


if __name__ == "__main__":
    main()
