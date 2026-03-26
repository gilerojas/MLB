"""
Leaderboard endpoints — read from player_season_boxscore parquets.

GET /leaderboards/batting   — season batting leaders
GET /leaderboards/pitching  — season pitching leaders
"""
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/leaderboards", tags=["leaderboards"])

WAREHOUSE = Path(__file__).parent.parent.parent / "data" / "warehouse" / "mlb"


def _find_parquet(kind: str, season: int) -> Optional[Path]:
    """Find player_season_boxscore_{kind}_{season}*.parquet in warehouse."""
    matches = sorted(WAREHOUSE.glob(f"player_season_boxscore_{kind}*{season}*.parquet"))
    if matches:
        return matches[-1]
    # Also search inside year subdirectories
    matches = sorted(WAREHOUSE.glob(f"{season}/player_season_boxscore_{kind}*.parquet"))
    return matches[-1] if matches else None


@router.get("/batting")
def batting_leaders(
    season: int = Query(2025),
    sort_by: str = Query("ops", description="Column to sort by"),
    min_pa: int = Query(50, description="Minimum plate appearances"),
    limit: int = Query(25, ge=1, le=100),
):
    try:
        import pandas as pd
    except ImportError:
        raise HTTPException(status_code=500, detail="pandas not available in this environment.")

    parquet = _find_parquet("batting", season)
    if not parquet:
        raise HTTPException(status_code=404, detail=f"Batting parquet not found for {season}.")

    df = pd.read_parquet(parquet)

    # Filter
    if "pa" in df.columns:
        df = df[df["pa"] >= min_pa]
    elif "plateAppearances" in df.columns:
        df = df[df["plateAppearances"] >= min_pa]

    # Sort
    if sort_by not in df.columns:
        available = list(df.columns)
        raise HTTPException(status_code=400, detail=f"Column '{sort_by}' not found. Available: {available}")

    df = df.sort_values(sort_by, ascending=False).head(limit)
    return {"season": season, "sort_by": sort_by, "leaders": df.to_dict(orient="records")}


@router.get("/pitching")
def pitching_leaders(
    season: int = Query(2025),
    sort_by: str = Query("era", description="Column to sort by (lower = better for ERA)"),
    min_ip: float = Query(20.0, description="Minimum innings pitched"),
    limit: int = Query(25, ge=1, le=100),
    ascending: bool = Query(True, description="Sort ascending (True for ERA, False for K)"),
):
    try:
        import pandas as pd
    except ImportError:
        raise HTTPException(status_code=500, detail="pandas not available in this environment.")

    parquet = _find_parquet("pitching", season)
    if not parquet:
        raise HTTPException(status_code=404, detail=f"Pitching parquet not found for {season}.")

    df = pd.read_parquet(parquet)

    if "ip" in df.columns:
        df = df[df["ip"] >= min_ip]
    elif "inningsPitched" in df.columns:
        df = df[df["inningsPitched"] >= min_ip]

    if sort_by not in df.columns:
        available = list(df.columns)
        raise HTTPException(status_code=400, detail=f"Column '{sort_by}' not found. Available: {available}")

    df = df.sort_values(sort_by, ascending=ascending).head(limit)
    return {"season": season, "sort_by": sort_by, "leaders": df.to_dict(orient="records")}
