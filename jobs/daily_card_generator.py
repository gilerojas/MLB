"""
Daily card generator — runs every morning at 10 AM ET.

1. Reads player_watchlist.json → seeds/refreshes DB watchlist
2. Fetches yesterday's completed games from MLB Stats API
3. Finds warehouse feed files for those games
4. Scores each watchlist player's performance in their game
5. Generates batter + pitcher cards via FastAPI
6. Inserts drafts into content_queue DB

Usage:
    python jobs/daily_card_generator.py               # uses yesterday
    python jobs/daily_card_generator.py --date 2025-04-15
    python jobs/daily_card_generator.py --seed-watchlist   # only seed DB, no cards
"""
import argparse
import gzip
import json
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

from api.db.database import get_db, insert_queue_item, get_watchlist

FASTAPI_BASE = "http://localhost:8000"
WAREHOUSE = REPO_ROOT / "data" / "warehouse" / "mlb"
WATCHLIST_JSON = Path(__file__).parent / "player_watchlist.json"

# Score thresholds per priority tier — only generate if above threshold
BATTER_MIN_SCORE = {1: 0, 2: 2, 3: 5, 4: 8, 5: 10}
PITCHER_MIN_SCORE = {1: 0, 2: 1, 3: 3, 4: 5, 5: 8}


def seed_watchlist_to_db():
    """Load player_watchlist.json into player_watchlist table."""
    players = json.loads(WATCHLIST_JSON.read_text())
    with get_db() as conn:
        for p in players:
            conn.execute(
                """
                INSERT INTO player_watchlist
                    (player_id, player_name, position, team_abbrev, active, priority, notes)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(player_id) DO UPDATE SET
                    player_name = excluded.player_name,
                    position    = excluded.position,
                    team_abbrev = excluded.team_abbrev,
                    active      = excluded.active,
                    priority    = excluded.priority,
                    notes       = excluded.notes
                """,
                (
                    p["player_id"],
                    p["player_name"],
                    p.get("position"),
                    p.get("team_abbrev"),
                    1 if p.get("active", True) else 0,
                    p.get("priority", 5),
                    p.get("notes", ""),
                ),
            )
    print(f"  Seeded {len(players)} players to watchlist DB.")


def get_final_games(game_date: str) -> list[dict]:
    """Fetch completed MLB games for a date."""
    try:
        resp = requests.get(
            "https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "date": game_date},
            timeout=15,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  Warning: MLB API error: {e}")
        return []

    games = []
    for day in resp.json().get("dates", []):
        for g in day.get("games", []):
            status = g.get("status", {}).get("detailedState", "")
            if "Final" in status or "Completed" in status:
                games.append(
                    {"game_pk": g["gamePk"], "game_type": g.get("gameType", "R")}
                )
    return games


def find_feed_path(game_pk: int, game_date: str) -> Path | None:
    """Locate the feed_live file for a game in the warehouse."""
    date_compact = game_date.replace("-", "")
    # Try all year/stage combos
    for year in [game_date[:4]]:
        for stage in ["regular_season", "spring_training", "playoffs"]:
            raw_dir = WAREHOUSE / year / stage / "raw"
            for pattern in [
                f"game_{game_pk}_{date_compact}_feed_live.json.gz",
                f"game_{game_pk}_{date_compact}_feed_live.json",
            ]:
                p = raw_dir / pattern
                if p.exists():
                    return p
    return None


def load_feed(feed_path: Path) -> dict:
    if feed_path.suffix == ".gz":
        with gzip.open(feed_path, "rt", encoding="utf-8") as f:
            return json.load(f)
    return json.loads(feed_path.read_text())


def score_batter(stats: dict) -> float:
    """Simple 'noteworthy' score for a batter's game line."""
    hits = float(stats.get("hits", 0))
    rbi = float(stats.get("rbi", 0))
    hr = float(stats.get("homeRuns", 0))
    sb = float(stats.get("stolenBases", 0))
    bb = float(stats.get("baseOnBalls", 0))
    return hits + rbi + hr * 3 + sb * 2 + bb * 0.5


def score_pitcher(stats: dict) -> float:
    """Simple 'noteworthy' score for a pitcher's outing."""
    ip_raw = stats.get("inningsPitched", "0.0")
    try:
        ip = float(ip_raw)
    except (ValueError, TypeError):
        ip = 0.0
    k = float(stats.get("strikeOuts", 0))
    er = float(stats.get("earnedRuns", 0))
    return ip + k * 0.5 - er * 2


def get_game_performers(feed: dict, watchlist_ids: set) -> dict:
    """
    Returns {'batters': [(player_id, player_name, score)],
             'pitchers': [(player_id, player_name, score)]}
    for watchlist players who appeared + top non-watchlist performer.
    """
    boxscore = feed.get("liveData", {}).get("boxscore", {})

    batters = []
    pitchers = []

    for side in ("home", "away"):
        team = boxscore.get("teams", {}).get(side, {})
        players = team.get("players", {})
        for _key, player in players.items():
            pid = player.get("person", {}).get("id")
            pname = player.get("person", {}).get("fullName", "Unknown")
            if not pid:
                continue

            bat_stats = player.get("stats", {}).get("batting", {})
            pit_stats = player.get("stats", {}).get("pitching", {})

            if bat_stats.get("atBats", 0) or bat_stats.get("plateAppearances", 0):
                bs = score_batter(bat_stats)
                batters.append((pid, pname, bs, pid in watchlist_ids))

            if pit_stats.get("pitchesThrown", 0) or pit_stats.get("inningsPitched"):
                ps = score_pitcher(pit_stats)
                pitchers.append((pid, pname, ps, pid in watchlist_ids))

    batters.sort(key=lambda x: x[2], reverse=True)
    pitchers.sort(key=lambda x: x[2], reverse=True)
    return {"batters": batters, "pitchers": pitchers}


def trigger_batter_card(player_id: int, feed_path: Path, player_name: str, game_date: str):
    try:
        resp = requests.post(
            f"{FASTAPI_BASE}/cards/batter",
            json={
                "player_id": player_id,
                "feed_path": str(feed_path),
                "dark": False,
                "tweet_text": f"🦇 {player_name} | {game_date} #Mallitalytics #MLB",
            },
            timeout=120,
        )
        if resp.status_code == 200:
            data = resp.json()
            print(f"    ✓ Batter card queued: {player_name} (id={data['id']})")
        else:
            print(f"    ✗ Batter card failed for {player_name}: {resp.text[:200]}")
    except Exception as e:
        print(f"    ✗ Batter card error for {player_name}: {e}")


def trigger_pitcher_card(player_id: int, game_date: str, player_name: str):
    try:
        resp = requests.post(
            f"{FASTAPI_BASE}/cards/pitcher",
            json={
                "player_id": player_id,
                "game_date": game_date,
                "dark": False,
                "tweet_text": f"⚾ {player_name} | {game_date} #Mallitalytics #MLB",
            },
            timeout=120,
        )
        if resp.status_code == 200:
            data = resp.json()
            print(f"    ✓ Pitcher card queued: {player_name} (id={data['id']})")
        else:
            print(f"    ✗ Pitcher card failed for {player_name}: {resp.text[:200]}")
    except Exception as e:
        print(f"    ✗ Pitcher card error for {player_name}: {e}")


def run(game_date: str):
    print(f"\n{'='*60}")
    print(f"  Daily Card Generator — {game_date}")
    print(f"{'='*60}")

    print("\n[1/4] Seeding watchlist DB from JSON…")
    seed_watchlist_to_db()

    watchlist = get_watchlist(active_only=True)
    watchlist_map = {p["player_id"]: p for p in watchlist}
    watchlist_ids = set(watchlist_map.keys())
    pitcher_ids = {p["player_id"] for p in watchlist if p.get("position") in ("pitcher", "two-way")}
    batter_ids = {p["player_id"] for p in watchlist if p.get("position") in ("batter", "two-way")}

    print(f"\n[2/4] Fetching final games for {game_date}…")
    games = get_final_games(game_date)
    print(f"  Found {len(games)} final game(s).")

    if not games:
        print("  No final games found — nothing to generate.")
        return

    print(f"\n[3/4] Finding warehouse files and scoring performances…")
    cards_generated = 0

    for game in games:
        pk = game["game_pk"]
        feed_path = find_feed_path(pk, game_date)
        if not feed_path:
            print(f"  game {pk}: no feed file found, skipping.")
            continue

        print(f"\n  game {pk} ({feed_path.name})")
        try:
            feed = load_feed(feed_path)
        except Exception as e:
            print(f"    Error loading feed: {e}")
            continue

        performers = get_game_performers(feed, watchlist_ids)

        # Process watchlist batters in this game
        for pid, pname, score, in_watchlist in performers["batters"]:
            if pid not in batter_ids:
                continue
            priority = watchlist_map[pid]["priority"]
            min_score = BATTER_MIN_SCORE.get(priority, 10)
            if score >= min_score:
                print(f"    Batter: {pname} (score={score:.1f}, priority={priority})")
                trigger_batter_card(pid, feed_path, pname, game_date)
                cards_generated += 1
            else:
                print(f"    Batter: {pname} — below threshold (score={score:.1f} < {min_score}), skipping.")

        # Process watchlist pitchers in this game
        for pid, pname, score, in_watchlist in performers["pitchers"]:
            if pid not in pitcher_ids:
                continue
            priority = watchlist_map[pid]["priority"]
            min_score = PITCHER_MIN_SCORE.get(priority, 8)
            if score >= min_score:
                print(f"    Pitcher: {pname} (score={score:.1f}, priority={priority})")
                trigger_pitcher_card(pid, game_date, pname)
                cards_generated += 1
            else:
                print(f"    Pitcher: {pname} — below threshold (score={score:.1f} < {min_score}), skipping.")

        # Top non-watchlist batter (breakout catch)
        top_bat = next(
            (b for b in performers["batters"] if b[0] not in batter_ids and b[2] >= 8),
            None,
        )
        if top_bat:
            pid, pname, score, _ = top_bat
            print(f"    Breakout batter: {pname} (score={score:.1f}) → generating card")
            trigger_batter_card(pid, feed_path, pname, game_date)
            cards_generated += 1

        # Top non-watchlist pitcher
        top_pit = next(
            (p for p in performers["pitchers"] if p[0] not in pitcher_ids and p[2] >= 5),
            None,
        )
        if top_pit:
            pid, pname, score, _ = top_pit
            print(f"    Breakout pitcher: {pname} (score={score:.1f}) → generating card")
            trigger_pitcher_card(pid, game_date, pname)
            cards_generated += 1

    print(f"\n[4/4] Done — {cards_generated} card(s) queued for review.")


def main():
    parser = argparse.ArgumentParser(description="Mallitalytics Daily Card Generator")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--seed-watchlist", action="store_true", help="Only seed watchlist DB and exit")
    args = parser.parse_args()

    if args.seed_watchlist:
        seed_watchlist_to_db()
        return

    game_date = args.date or str(date.today() - timedelta(days=1))
    run(game_date)


if __name__ == "__main__":
    main()
