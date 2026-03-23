"""
Backfill 2025-26 LIDOM Season
==============================
Extracts completed games with stage selection control.

Process:
1. Get schedule for specified stage(s)
2. Filter for completed games
3. Extract boxscore, linescore, PBP for each
4. Save to warehouse with proper structure

Usage:
    python /Users/gilrojasb/Desktop/Mallitalytics_VS/LIDOM/scripts/backfill_2025.py --stage RR           # Only Round Robin
    python backfill_2025.py --stage SR           # Only Regular Season
    python backfill_2025.py --stage all          # Both stages (default)
    python backfill_2025.py --stage RR --force   # Force re-extract RR

Author: Mallitalytics
Project: LIDOM 2026
"""

import sys
from pathlib import Path
import argparse

# Add scripts directory to path
sys.path.append(str(Path(__file__).parent))

from lidom_api import LIDOM_API
import pandas as pd
from datetime import datetime
import json

# ============================================================================
# CONFIGURATION
# ============================================================================

SEASON = 2025

# Game types for LIDOM stages
GAME_TYPES = {
    "R": "SR",  # Regular Season → SR
    "L": "RR",  # League Championship (Round Robin) → RR
    "W": "SF"   # World Series (Serie Final) → SF
}

# Reverse mapping (stage code → API game type)
STAGE_TO_API = {
    "SR": "R",
    "RR": "L",
    "SF": "W"
}

# Base warehouse root
WAREHOUSE_BASE = Path("data/warehouse/lidom_api/2025-26")

# Stage-specific paths
WAREHOUSE_PATHS = {
    "SR": WAREHOUSE_BASE,       # Regular season uses base path
    "RR": WAREHOUSE_BASE / "RR", # Round Robin gets dedicated directory
    "SF": WAREHOUSE_BASE / "SF"  # Serie Final gets dedicated directory
}

# Create all directories
for stage, base_path in WAREHOUSE_PATHS.items():
    base_path.mkdir(parents=True, exist_ok=True)
    (base_path / "raw").mkdir(exist_ok=True)
    (base_path / "boxscore").mkdir(exist_ok=True)
    (base_path / "linescore").mkdir(exist_ok=True)
    (base_path / "pbp").mkdir(exist_ok=True)
    (base_path / "logs").mkdir(exist_ok=True)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_warehouse_root(stage):
    """Get appropriate warehouse path based on game stage."""
    return WAREHOUSE_PATHS[stage]


def format_date_for_filename(date_str):
    """Convert YYYY-MM-DD to YYYYMMDD."""
    return date_str.replace("-", "")


def save_game_data(api, game_pk, game_date, stage):
    """
    Extract and save all data for a single game.
    ALL-OR-NOTHING: Either all 9 files succeed, or nothing is saved.
    
    Args:
        api: LIDOM_API instance
        game_pk: Game ID
        game_date: Game date (YYYY-MM-DD)
        stage: Game stage ("SR" or "RR")
    
    Returns:
        Dict with extraction results
    """
    WAREHOUSE_ROOT = get_warehouse_root(stage)
    date_formatted = format_date_for_filename(game_date)
    
    results = {
        "game_pk": game_pk,
        "date": game_date,
        "stage": stage,
        "boxscore_raw": False,
        "boxscore_team": False,
        "boxscore_batting": False,
        "boxscore_pitching": False,
        "linescore_raw": False,
        "linescore_table": False,
        "pbp_raw": False,
        "pbp_plays": False,
        "pbp_pitches": False,
        "extraction_error": None
    }
    
    # Temporary storage - we'll only save if ALL succeed
    temp_data = {}
    
    try:
        # ===== EXTRACT EVERYTHING FIRST (no saving yet) =====
        
        # Boxscore raw
        boxscore = api.get_boxscore(game_pk)
        if boxscore:
            temp_data["boxscore_raw"] = boxscore
            results["boxscore_raw"] = True
        
        # Team stats
        team_stats = api.extract_team_stats(game_pk)
        if team_stats is not None and len(team_stats) > 0:
            temp_data["boxscore_team"] = team_stats
            results["boxscore_team"] = True
        
        # Batting
        batting = api.extract_player_stats(game_pk, "batting")
        if batting is not None and len(batting) > 0:
            temp_data["boxscore_batting"] = batting
            results["boxscore_batting"] = True
        
        # Pitching
        pitching = api.extract_player_stats(game_pk, "pitching")
        if pitching is not None and len(pitching) > 0:
            temp_data["boxscore_pitching"] = pitching
            results["boxscore_pitching"] = True
        
        # Linescore raw
        linescore = api.get_linescore(game_pk)
        if linescore:
            temp_data["linescore_raw"] = linescore
            results["linescore_raw"] = True
        
        # Linescore table
        linescore_table = api.extract_linescore_table(game_pk)
        if linescore_table is not None and len(linescore_table) > 0:
            temp_data["linescore_table"] = linescore_table
            results["linescore_table"] = True
        
        # PBP raw
        pbp = api.get_pbp(game_pk)
        if pbp:
            temp_data["pbp_raw"] = pbp
            results["pbp_raw"] = True
        
        # Plays
        plays = api.extract_plays(game_pk)
        if plays is not None and len(plays) > 0:
            temp_data["pbp_plays"] = plays
            results["pbp_plays"] = True
        
        # Pitches
        pitches = api.extract_pitches(game_pk)
        if pitches is not None and len(pitches) > 0:
            temp_data["pbp_pitches"] = pitches
            results["pbp_pitches"] = True
        
        # ===== VALIDATE: All 9 files must be present =====
        expected_files = ["boxscore_raw", "boxscore_team", "boxscore_batting", "boxscore_pitching",
                         "linescore_raw", "linescore_table", "pbp_raw", "pbp_plays", "pbp_pitches"]
        
        success_count = sum(1 for k in expected_files if results.get(k) is True)
        
        if success_count != len(expected_files):
            missing = [k for k in expected_files if not results.get(k)]
            raise Exception(f"Incomplete extraction - missing: {', '.join(missing)}")
        
        # ===== ALL VALIDATED - NOW SAVE =====
        
        # Raw JSONs
        if "boxscore_raw" in temp_data:
            filepath = WAREHOUSE_ROOT / "raw" / f"game_{game_pk}_{date_formatted}_boxscore.json"
            api.save_json(temp_data["boxscore_raw"], filepath)
        
        if "linescore_raw" in temp_data:
            filepath = WAREHOUSE_ROOT / "raw" / f"game_{game_pk}_{date_formatted}_linescore.json"
            api.save_json(temp_data["linescore_raw"], filepath)
        
        if "pbp_raw" in temp_data:
            filepath = WAREHOUSE_ROOT / "raw" / f"game_{game_pk}_{date_formatted}_pbp.json"
            api.save_json(temp_data["pbp_raw"], filepath)
        
        # CSVs
        if "boxscore_team" in temp_data:
            filepath = WAREHOUSE_ROOT / "boxscore" / f"game_{game_pk}_{date_formatted}_team_stats.csv"
            temp_data["boxscore_team"].to_csv(filepath, index=False)
        
        if "boxscore_batting" in temp_data:
            filepath = WAREHOUSE_ROOT / "boxscore" / f"game_{game_pk}_{date_formatted}_batting.csv"
            temp_data["boxscore_batting"].to_csv(filepath, index=False)
        
        if "boxscore_pitching" in temp_data:
            filepath = WAREHOUSE_ROOT / "boxscore" / f"game_{game_pk}_{date_formatted}_pitching.csv"
            temp_data["boxscore_pitching"].to_csv(filepath, index=False)
        
        if "linescore_table" in temp_data:
            filepath = WAREHOUSE_ROOT / "linescore" / f"game_{game_pk}_{date_formatted}_linescore.csv"
            temp_data["linescore_table"].to_csv(filepath, index=False)
        
        if "pbp_plays" in temp_data:
            filepath = WAREHOUSE_ROOT / "pbp" / f"game_{game_pk}_{date_formatted}_plays.csv"
            temp_data["pbp_plays"].to_csv(filepath, index=False)
        
        if "pbp_pitches" in temp_data:
            filepath = WAREHOUSE_ROOT / "pbp" / f"game_{game_pk}_{date_formatted}_pitches.csv"
            temp_data["pbp_pitches"].to_csv(filepath, index=False)
    
    except Exception as e:
        # Capture error - NO FILES SAVED
        results["extraction_error"] = str(e)
        
        # Reset all success flags
        for key in ["boxscore_raw", "boxscore_team", "boxscore_batting", "boxscore_pitching",
                   "linescore_raw", "linescore_table", "pbp_raw", "pbp_plays", "pbp_pitches"]:
            results[key] = False
        
        print(f"   ❌ EXTRACTION FAILED: {e}")
        print(f"   🗑️  No partial files saved (all-or-nothing)")
    
    return results


# ============================================================================
# MAIN BACKFILL
# ============================================================================

def run_backfill(stages_to_extract=["all"], force_refresh=False):
    """
    Execute backfill for specified stage(s).
    
    Args:
        stages_to_extract: List of stages to extract ["SR", "RR", or "all"]
        force_refresh: If True, re-extract all games. If False, skip existing.
    """
    
    # Parse stages
    if "all" in stages_to_extract:
        stages = ["SR", "RR", "SF"]
    else:
        stages = [s.upper() for s in stages_to_extract]
    
    print("="*70)
    print("🚀 LIDOM 2025-26 SEASON BACKFILL")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Stages: {', '.join(stages)}")
    print(f"♻️  Mode: {'FORCE REFRESH' if force_refresh else 'INCREMENTAL'}")
    print("="*70)
    
    # Initialize API
    api = LIDOM_API(rate_limit=1.5)
    
    # Get schedules for selected stages only
    all_games = []
    
    for stage in stages:
        if stage not in STAGE_TO_API:
            print(f"\n⚠️  Unknown stage: {stage}. Skipping.")
            continue
        
        game_type_api = STAGE_TO_API[stage]
        
        print(f"\n📅 Fetching {stage} schedule (gameType='{game_type_api}')...")
        
        schedule = api.get_schedule(season=SEASON, game_type=game_type_api)
        
        if schedule:
            # Add stage info to each game
            for game in schedule:
                game["stage"] = stage
            
            all_games.extend(schedule)
            print(f"   ✅ Found {len(schedule)} games")
        else:
            print(f"   ⚠️  No games found for gameType='{game_type_api}'")
    
    if not all_games:
        print("\n❌ Could not fetch any games. Exiting.")
        return
    
    print(f"\n✅ Total games scheduled: {len(all_games)}")
    
    # Filter completed games
    completed = [g for g in all_games if g["status"] in ["Final", "Completed Early", "Game Over"]]
    
    print(f"✅ {len(completed)} games completed")
    print(f"⏳ {len(all_games) - len(completed)} games remaining")
    
    if not completed:
        print("\n⚠️  No completed games found yet.")
        return
    
    # Show date range and stage breakdown
    dates = sorted([g["date"] for g in completed])
    print(f"\n📊 Date range: {dates[0]} to {dates[-1]}")
    
    # Count games by stage
    for stage in stages:
        stage_games = [g for g in completed if g["stage"] == stage]
        
        if stage_games:
            stage_emoji = "⚾" if stage == "SR" else "🏆" if stage == "RR" else "👑"
            stage_name = "Regular Season" if stage == "SR" else "Round Robin" if stage == "RR" else "Serie Final"
            
            print(f"\n{stage_emoji} {stage_name} ({stage}):")
            print(f"   Total: {len(stage_games)} games")
            print(f"   Date range: {min(g['date'] for g in stage_games)} to {max(g['date'] for g in stage_games)}")
    
    # ===== CHECK EXISTING FILES =====
    if not force_refresh:
        existing_games = {}
        
        # Check each stage's directory
        for stage in stages:
            warehouse_path = WAREHOUSE_PATHS[stage]
            stage_existing = set()
            
            for file in (warehouse_path / "boxscore").glob("game_*_team_stats.csv"):
                game_pk = int(file.stem.split("_")[1])
                stage_existing.add(game_pk)
            
            existing_games[stage] = stage_existing
            
            if stage_existing:
                print(f"\n📦 {stage}: Found {len(stage_existing)} games already extracted")
        
        # Filter for new games only
        new_games = [g for g in completed if g["gamePk"] not in existing_games.get(g["stage"], set())]
        
        if not new_games:
            print("\n✅ All games already extracted. Nothing to do.")
            print("💡 Use --force flag to re-extract all games.")
            return
        
        print(f"\n🆕 {len(new_games)} new games to extract")
        
        # Show breakdown by stage
        for stage in stages:
            new_stage = [g for g in new_games if g["stage"] == stage]
            
            if new_stage:
                stage_emoji = "⚾" if stage == "SR" else "🏆" if stage == "RR" else "👑"
                print(f"   {stage_emoji} {stage}: {len(new_stage)} games → {WAREHOUSE_PATHS[stage]}")
        
        games_to_extract = new_games
    else:
        print(f"\n♻️  FORCE REFRESH: Re-extracting all {len(completed)} games")
        games_to_extract = completed
    
    # Extract each game
    print("\n" + "="*70)
    print("🎯 EXTRACTING GAMES")
    print("="*70)
    
    extraction_log = []
    failed_games = []  # Track failed extractions
    
    for i, game in enumerate(games_to_extract, 1):
        game_pk = game["gamePk"]
        date = game["date"]
        away = game["away_team"]
        home = game["home_team"]
        stage = game["stage"]
        
        # Stage indicator emoji
        stage_emoji = "⚾" if stage == "SR" else "🏆" if stage == "RR" else "👑"
        
        print(f"\n[{i}/{len(games_to_extract)}] {stage_emoji} {stage} | Game {game_pk} ({date})")
        print(f"   {away} @ {home}")
        print(f"   📂 Saving to: {get_warehouse_root(stage)}")
        
        # Extract all data
        results = save_game_data(api, game_pk, date, stage)
        
        # Log results
        results.update({
            "game_pk": game_pk,
            "date": date,
            "stage": stage,
            "away_team": away,
            "home_team": home,
            "extracted_at": datetime.now().isoformat()
        })
        extraction_log.append(results)
        
        # Validate completeness
        expected_files = ["boxscore_raw", "boxscore_team", "boxscore_batting", "boxscore_pitching",
                         "linescore_raw", "linescore_table", "pbp_raw", "pbp_plays", "pbp_pitches"]
        
        success_count = sum(1 for k in expected_files if results.get(k) is True)
        
        # Check if extraction had errors
        if results.get("extraction_error"):
            print(f"   ❌ FAILED: {results['extraction_error']}")
            failed_games.append({
                "game_pk": game_pk,
                "date": date,
                "stage": stage,
                "error": results['extraction_error']
            })
        elif success_count == len(expected_files):
            print(f"   ✅ {success_count}/{len(expected_files)} files extracted - COMPLETE")
        else:
            print(f"   ⚠️  {success_count}/{len(expected_files)} files extracted - INCOMPLETE")
            missing = [k for k in expected_files if not results.get(k)]
            print(f"   ⚠️  Missing: {', '.join(missing)}")
            failed_games.append({
                "game_pk": game_pk,
                "date": date,
                "stage": stage,
                "error": f"Incomplete extraction - missing: {missing}"
            })
    
    # Report failed games
    if failed_games:
        print("\n" + "="*70)
        print("⚠️  FAILED/INCOMPLETE EXTRACTIONS")
        print("="*70)
        for failed in failed_games:
            print(f"   Game {failed['game_pk']} ({failed['date']}) - {failed['stage']}")
            print(f"      Error: {failed['error']}")
    
    # Save extraction logs (separate for each stage)
    for stage in stages:
        stage_log = [log for log in extraction_log if log.get("stage") == stage]
        
        if not stage_log:
            continue
        
        log_file = WAREHOUSE_PATHS[stage] / "logs" / "extraction_log.json"
        
        # Load existing log if exists
        if log_file.exists():
            with open(log_file, "r") as f:
                existing_log = json.load(f)
            stage_log = existing_log + stage_log
        
        with open(log_file, "w") as f:
            json.dump(stage_log, f, indent=2)
        
        print(f"\n💾 {stage} extraction log saved: {log_file}")
    
    print("\n" + "="*70)
    print("📊 BACKFILL SUMMARY")
    print("="*70)
    
    print(f"\n✅ Extraction complete!")
    print(f"   Games processed: {len(games_to_extract)}")
    
    # Show success/failure breakdown
    if failed_games:
        print(f"   ❌ Failed: {len(failed_games)}")
        print(f"   ✅ Successful: {len(games_to_extract) - len(failed_games)}")
    else:
        print(f"   ✅ All extractions successful!")
    
    # Breakdown by stage
    for stage in stages:
        stage_extracted = [log for log in extraction_log if log.get("stage") == stage]
        
        if stage_extracted:
            stage_emoji = "⚾" if stage == "SR" else "🏆" if stage == "RR" else "👑"
            stage_files = sum(sum(1 for k, v in log.items() if k not in ["game_pk", "date", "stage", "away_team", "home_team", "extracted_at", "extraction_error"] and v is True) for log in stage_extracted)
            
            print(f"\n   {stage_emoji} {stage}:")
            print(f"      Games: {len(stage_extracted)}")
            print(f"      Files: {stage_files}")
            print(f"      Location: {WAREHOUSE_PATHS[stage]}")


# ============================================================================
# QUICK STATS
# ============================================================================

def show_quick_stats(stages=["SR", "RR", "SF"]):
    """Show quick stats from extracted games."""
    
    print("\n" + "="*70)
    print("📊 QUICK STATS")
    print("="*70)
    
    all_team_stats = []
    
    # Load from specified stages
    for stage in stages:
        if stage not in WAREHOUSE_PATHS:
            continue
        
        warehouse_path = WAREHOUSE_PATHS[stage]
        team_stats_files = list((warehouse_path / "boxscore").glob("*_team_stats.csv"))
        
        for file in team_stats_files:
            df = pd.read_csv(file)
            df["stage"] = stage
            all_team_stats.append(df)
    
    if not all_team_stats:
        print("\n⚠️  No team stats files found")
        return
    
    combined = pd.concat(all_team_stats, ignore_index=True)
    
    print(f"\n📊 Total team performances: {len(combined)}")
    
    # Stats by stage
    for stage in stages:
        stage_data = combined[combined["stage"] == stage]
        
        if len(stage_data) == 0:
            continue
        
        stage_emoji = "⚾" if stage == "SR" else "🏆" if stage == "RR" else "👑"
        stage_name = "Regular Season" if stage == "SR" else "Round Robin" if stage == "RR" else "Serie Final"
        
        print(f"\n{stage_emoji} {stage_name}:")
        print(f"   Games: {len(stage_data) // 2}")
        
        # Top scoring games
        print(f"\n   🔥 Highest scoring games:")
        top_scoring = stage_data.nlargest(3, "runs")
        print("   " + top_scoring[["team", "runs", "hits", "game_pk"]].to_string(index=False).replace("\n", "\n   "))
        
        # Team averages
        try:
            stage_data_copy = stage_data.copy()
            
            # Convert avg and ops to float
            if stage_data_copy["avg"].dtype == "object":
                stage_data_copy["avg"] = stage_data_copy["avg"].str.replace(".", "").astype(float) / 1000
            
            if stage_data_copy["ops"].dtype == "object":
                stage_data_copy["ops"] = stage_data_copy["ops"].str.replace(".", "").astype(float) / 1000
            
            team_avg = stage_data_copy.groupby("team").agg({
                "runs": "mean",
                "hits": "mean",
                "avg": "mean",
                "ops": "mean"
            }).round(3)
            
            print(f"\n   📊 Team averages:")
            print("   " + team_avg.sort_values("runs", ascending=False).to_string().replace("\n", "\n   "))
        
        except Exception as e:
            print(f"\n   ⚠️  Could not calculate team averages: {e}")


# ============================================================================
# EXECUTION
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract LIDOM game data by stage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract only Round Robin games
  python backfill_2025.py --stage RR
  
  # Extract only Regular Season games
  python backfill_2025.py --stage SR
  
  # Extract only Serie Final games
  python backfill_2025.py --stage SF
  
  # Extract all stages
  python backfill_2025.py --stage all
  
  # Force re-extract Serie Final
  python backfill_2025.py --stage SF --force
        """
    )
    
    parser.add_argument(
        "--stage",
        type=str,
        choices=["SR", "RR", "SF", "all"],
        default="all",
        help="Stage to extract: SR (Regular Season), RR (Round Robin), SF (Serie Final), or all"
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-extract all games (ignore existing files)"
    )
    
    args = parser.parse_args()
    
    # Convert stage to list for run_backfill
    if args.stage == "all":
        stages_to_extract = ["all"]
        stats_stages = ["SR", "RR", "SF"]  # Show all for quick stats
    else:
        stages_to_extract = [args.stage]
        stats_stages = [args.stage]  # Show only the extracted stage
    
    # Run backfill
    run_backfill(stages_to_extract=stages_to_extract, force_refresh=args.force)
    
    # Show quick stats (only for extracted stages)
    show_quick_stats(stages=stats_stages)
    
    print("\n✅ Done! Check the warehouse for extracted data.")
    
    if args.stage in ["SR", "all"]:
        print(f"📂 Regular Season: {WAREHOUSE_PATHS['SR'].absolute()}")
    if args.stage in ["RR", "all"]:
        print(f"📂 Round Robin: {WAREHOUSE_PATHS['RR'].absolute()}")
    if args.stage in ["SF", "all"]:
        print(f"📂 Serie Final: {WAREHOUSE_PATHS['SF'].absolute()}")