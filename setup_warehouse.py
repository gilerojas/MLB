# scripts/setup_mlb_warehouse.py
"""
Creates directory structure for a given season.
"""
import os
from pathlib import Path

STAGES = ["spring_training", "regular_season", "all_star", "playoffs"]
DATA_TYPES = ["boxscore", "linescore", "pbp", "raw", "pitches_enriched"]
PLAYOFF_ROUNDS = ["wild_card", "division", "championship", "world_series"]

def setup_season(year: int, base_path: Path):
    season_path = base_path / str(year)
    
    for stage in STAGES:
        stage_path = season_path / stage
        
        if stage == "playoffs":
            # Create playoff round subfolders
            for round_name in PLAYOFF_ROUNDS:
                round_path = stage_path / round_name
                for dtype in DATA_TYPES:
                    (round_path / dtype).mkdir(parents=True, exist_ok=True)
        else:
            # Standard stage structure
            for dtype in DATA_TYPES:
                (stage_path / dtype).mkdir(parents=True, exist_ok=True)
        
        # Create schedule file
        schedule_file = stage_path / f"schedule_{stage}_{year}.csv"
        schedule_file.touch(exist_ok=True)
    
    # Create manifest
    manifest = season_path / f"games_manifest_{year}.csv"
    manifest.touch(exist_ok=True)
    
    # Create logs
    (season_path / "logs").mkdir(exist_ok=True)
    
    print(f"✅ Warehouse structure created for {year}")

if __name__ == "__main__":
    base = Path("data/warehouse/mlb")
    for year in [2024, 2025]:
        setup_season(year, base)