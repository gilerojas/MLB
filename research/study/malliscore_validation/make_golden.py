"""Regenerate the frozen MalliScore V3 golden fixture.

Run only when a change to V3 is deliberate:

    ./mlb_env.nosync/bin/python research/study/malliscore_validation/make_golden.py

The fixture pins V3 output for 50 real starter outings spanning the full score
range, including the pathological exact-zero cases. `tests/test_malliscore_golden.py`
asserts against it so V4 work cannot silently alter V3.
"""

from __future__ import annotations

import contextlib
import io
import json
import sys
import warnings
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from research.study.starter_outings import aggregate_starter_outings  # noqa: E402
from src.pitching_performances.malli_score import (  # noqa: E402
    MALLISCORE_VERSION,
    OutingRawMetrics,
    default_league_norms,
    malliscore_v2,
)

CACHE = ROOT / "research/study/.cache"
PITCH_CACHE = CACHE / "pitch_rows_v5_2024_2025_2026_2024-2025-2026_full.parquet"
BOX_CACHE = CACHE / "box_context_v5_2024_2025_2026_2024-2025-2026_full.parquet"
GOLDEN = ROOT / "tests/fixtures/malliscore_v3_golden.json"
N_CASES = 50


def raw_of(row) -> OutingRawMetrics:
    """Build V3 inputs from an aggregated starter outing row."""
    outs = max(1, int(round(float(row.outs))))
    ip = outs / 3.0
    return OutingRawMetrics(
        swstr_pct=float(row.swstr_pct),
        called_strike_pct=float(row.called_strike_pct),
        chase_pct=float(row.chase_pct),
        xwoba_allowed=float(row.xwoba_allowed),
        game_whip=(float(row.hits) + float(row.walks)) / ip,
        earned_runs=max(0, int(round(float(row.earned_runs)))),
        home_runs=max(0, int(round(float(row.home_runs)))),
        pitches=max(1, int(round(float(row.pitches)))),
        outs=outs,
    )


def main() -> None:
    warnings.filterwarnings("ignore")
    pitches = pd.read_parquet(PITCH_CACHE)
    box = pd.read_parquet(BOX_CACHE)
    with contextlib.redirect_stdout(io.StringIO()):
        outings = aggregate_starter_outings(pitches, box)
    outings = outings[outings["earned_runs"].notna()].copy()

    norms = default_league_norms()
    outings = outings.sort_values(["game_pk", "pitcher"]).reset_index(drop=True)
    outings["_ms"] = [malliscore_v2(raw_of(r), norms)["malli_score"] for r in outings.itertuples()]

    # Deterministic stratified sample across the score range, with the exact-zero
    # cases and both extremes forced in.
    nonzero = outings[outings["_ms"] > 0].copy()
    nonzero["_bin"] = pd.qcut(nonzero["_ms"], 23, labels=False, duplicates="drop")
    picked = pd.concat(
        [
            outings[outings["_ms"] <= 0].head(4),
            nonzero.groupby("_bin", group_keys=False).apply(lambda g: g.head(2)),
            nonzero.nlargest(2, "_ms"),
            nonzero.nsmallest(2, "_ms"),
        ]
    )
    picked = (
        picked.drop_duplicates(subset=["game_pk", "pitcher"])
        .sort_values(["game_pk", "pitcher"])
        .head(N_CASES)
    )

    cases = []
    for row in picked.itertuples():
        raw = raw_of(row)
        cases.append(
            {
                "season": int(row.season),
                "game_pk": int(row.game_pk),
                "pitcher": int(row.pitcher),
                "pitcher_name": str(row.pitcher_name),
                "game_date": str(row.game_date),
                "input": {
                    k: (float(v) if isinstance(v, float) else int(v))
                    for k, v in raw.__dict__.items()
                },
                "expected": {k: round(float(v), 10) for k, v in malliscore_v2(raw, norms).items()},
            }
        )

    GOLDEN.parent.mkdir(parents=True, exist_ok=True)
    GOLDEN.write_text(
        json.dumps(
            {
                "malliscore_version": MALLISCORE_VERSION,
                "description": (
                    "Frozen MalliScore V3 outputs for 50 real starter outings (2024-2026). "
                    "Regenerate only via research/study/malliscore_validation/make_golden.py "
                    "and only when a V3 change is deliberate."
                ),
                "norms": "default_league_norms()",
                "n_cases": len(cases),
                "cases": cases,
            },
            indent=2,
        )
    )
    scores = pd.Series([c["expected"]["malli_score"] for c in cases])
    print(f"wrote {len(cases)} cases -> {GOLDEN.relative_to(ROOT)}")
    print(f"score range {scores.min():.2f}-{scores.max():.2f} | exact zeros {(scores <= 0).sum()}")


if __name__ == "__main__":
    main()
