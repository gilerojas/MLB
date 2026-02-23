"""
Inspect a raw feed_live JSON and write a human-readable breakdown of its structure.
Usage: python scripts/inspect_feed_structure.py [path_to_feed.json or .json.gz]
"""
import json
import gzip
import sys
from pathlib import Path


def open_feed(path: Path):
    if path.suffix == ".gz" or path.name.endswith(".json.gz"):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def sample(v, maxlen=60):
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        return repr(v[:maxlen] + ("..." if len(v) > maxlen else ""))
    if isinstance(v, list):
        return f"[ list of {len(v)} items ]"
    if isinstance(v, dict):
        return f"{len(v)} keys"
    return str(type(v).__name__)


def describe_dict(d, indent="", max_values=5):
    lines = []
    for i, (k, v) in enumerate(d.items()):
        if i >= max_values and len(d) > max_values:
            lines.append(f"{indent}  ... and {len(d) - max_values} more keys\n")
            break
        if isinstance(v, dict) and len(v) > 0 and k not in ("coordinates", "breaks", "call", "type"):
            lines.append(f"{indent}  {k}:\n")
            lines.append(describe_dict(v, indent + "    ", max_values=8))
        elif isinstance(v, list) and len(v) > 0:
            lines.append(f"{indent}  {k}: [ list of {len(v)} items ]\n")
            if len(v) <= 2:
                for j, item in enumerate(v):
                    if isinstance(item, dict):
                        lines.append(f"{indent}    [{j}] {len(item)} keys\n")
                        lines.append(describe_dict(item, indent + "      ", max_values=6))
                    else:
                        lines.append(f"{indent}    [{j}] {sample(item)}\n")
        else:
            lines.append(f"{indent}  {k}: {sample(v)}\n")
    return "".join(lines)


def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    if not path or not path.exists():
        # Default: find first raw feed in warehouse
        base = Path("data/warehouse/mlb")
        cand = list(base.rglob("raw/*_feed_live.json*"))[:1]
        if not cand:
            print("No feed file found. Pass path: python scripts/inspect_feed_structure.py <path>")
            return
        path = cand[0]
    feed = open_feed(path)
    out_path = Path("docs/FEED_LIVE_STRUCTURE.md")

    lines = []
    lines.append("# Raw feed_live JSON — Structure reference\n\n")
    lines.append(f"**Source:** `{path.name}`  \n")
    lines.append("Use this to decide what can be moved to a per-season document (immutable or slow-changing) vs kept per game.\n\n")
    lines.append("---\n\n")

    # Root
    lines.append("## Root\n\n")
    lines.append("| Key | Type | Notes |\n")
    lines.append("|-----|------|-------|\n")
    for k in feed:
        v = feed[k]
        if k == "gameData":
            lines.append("| `gameData` | object | **Largely immutable** — teams, players, venue. Can be one doc per season. |\n")
        elif k == "liveData":
            lines.append("| `liveData` | object | **Per game** — plays, linescore, boxscore. Keep per game. |\n")
        elif k == "copyright":
            lines.append("| `copyright` | string | Legal text. Can omit or keep once. |\n")
        elif k == "gamePk":
            lines.append("| `gamePk` | number | Game ID. |\n")
        elif k == "link":
            lines.append("| `link` | string | API path. Can derive from gamePk. |\n")
        elif k == "metaData":
            lines.append("| `metaData` | object | Timestamp, game events. Optional. |\n")
        else:
            lines.append(f"| `{k}` | {type(v).__name__} | |\n")
    lines.append("\n")

    # gameData
    gd = feed.get("gameData", {})
    lines.append("## gameData (immutable / per-season candidate)\n\n")
    lines.append("Everything here is **game setup**: teams, roster, venue, weather, officials. Same players appear in many games; venue/weather are fixed for the game. Good candidate to store **once per season** (e.g. merged players + teams) and **omit from per-game raw**.\n\n")
    lines.append("| Key | Description | Synthesize per season? |\n")
    lines.append("|-----|-------------|------------------------|\n")
    for k in sorted(gd.keys()):
        v = gd[k]
        if k == "players":
            lines.append("| `players` | Roster: ID → fullName, birthDate, height, weight, primaryPosition, batSide, pitchHand, strikeZoneTop/Bottom, etc. | ✅ Yes — one master players doc per season. |\n")
        elif k == "teams":
            lines.append("| `teams` | away/home: id, name, link, etc. | ✅ Yes — teams change rarely. |\n")
        elif k == "venue":
            lines.append("| `venue` | id, name, location, timeZone, fieldInfo | ✅ Yes — venues are fixed. |\n")
        elif k == "game":
            lines.append("| `game` | pk, type, season, doubleHeader, gameNumber | Per game but small; could keep in game header. |\n")
        elif k == "datetime":
            lines.append("| `datetime` | dateTime, officialDate, dayNight, time | Per game; small. |\n")
        elif k == "status":
            lines.append("| `status` | abstractGameState, detailedState, statusCode | Per game; small. |\n")
        elif k in ("weather", "gameInfo", "review", "flags", "probablePitchers", "officialScorer", "primaryDatacaster", "moundVisits", "officialVenue", "absChallenges", "alerts"):
            lines.append(f"| `{k}` | {sample(v)} | Optional; can omit or keep in small game header. |\n")
        else:
            lines.append(f"| `{k}` | {sample(v)} | |\n")
    lines.append("\n")

    # gameData.players — one example
    players = gd.get("players", {})
    if players:
        pid, p = next(iter(players.items()))
        lines.append("### gameData.players — one example (all keys)\n\n")
        lines.append("These fields repeat for every player in every game. Storing them once per season saves space.\n\n")
        lines.append("```\n")
        for k in sorted(p.keys()):
            lines.append(f"  {k}: {sample(p[k])}\n")
        lines.append("```\n\n")

    # gameData.teams
    teams = gd.get("teams", {})
    if teams:
        lines.append("### gameData.teams\n\n")
        lines.append("```\n")
        for side in ("away", "home"):
            t = teams.get(side, {})
            if isinstance(t, dict):
                lines.append(f"  {side}: {list(t.keys())}\n")
        lines.append("```\n\n")

    # liveData
    ld = feed.get("liveData", {})
    lines.append("## liveData (per game — keep)\n\n")
    lines.append("Play-by-play, linescore, boxscore. **Changes every game**; keep per game (or flatten to parquet).\n\n")
    lines.append("| Key | Description |\n")
    lines.append("|-----|-------------|\n")
    lines.append("| `plays` | allPlays, currentPlay, scoringPlays, playsByInning — the main pitch-by-pitch data. |\n")
    lines.append("| `linescore` | currentInning, innings[], teams (runs by inning), balls/strikes/outs. |\n")
    lines.append("| `boxscore` | teams (batting/pitching lines), officials, info, topPerformers. |\n")
    lines.append("| `decisions` | Win/loss/save (often empty until game ends). |\n")
    lines.append("| `leaders` | hitDistance, hitSpeed, pitchSpeed — post-game. |\n")
    lines.append("\n")

    # plays.allPlays — one play, one pitch
    plays = ld.get("plays", {}).get("allPlays", [])
    if plays:
        lines.append("### liveData.plays.allPlays — one play (at-bat) example\n\n")
        p0 = plays[0]
        lines.append("**Top-level keys:** " + ", ".join(p0.keys()) + "\n\n")
        lines.append("```\n")
        lines.append(describe_dict(p0, "", max_values=20))
        lines.append("```\n\n")
        evs = p0.get("playEvents", [])
        pitch_ev = next((e for e in evs if e.get("isPitch") and e.get("pitchData")), None)
        if pitch_ev:
            lines.append("### liveData.plays.allPlays[].playEvents[] — one pitch example\n\n")
            lines.append("**Top-level keys:** " + ", ".join(pitch_ev.keys()) + "\n\n")
            lines.append("```\n")
            lines.append(describe_dict(pitch_ev, "", max_values=25))
            lines.append("```\n\n")
            pd = pitch_ev.get("pitchData", {})
            if pd:
                lines.append("**pitchData sub-keys:** " + ", ".join(pd.keys()) + "\n\n")
                coords = pd.get("coordinates", {})
                breaks = pd.get("breaks", {})
                if coords:
                    lines.append("**pitchData.coordinates:** " + ", ".join(coords.keys()) + "\n\n")
                if breaks:
                    lines.append("**pitchData.breaks:** " + ", ".join(breaks.keys()) + "\n\n")
            det = pitch_ev.get("details", {})
            if det:
                lines.append("**details sub-keys:** " + ", ".join(det.keys()) + "\n\n")

    lines.append("---\n\n")
    lines.append("## Summary: what to leave out per game\n\n")
    lines.append("| Store once per season | Omit or keep minimal per game |\n")
    lines.append("|------------------------|----------------------------------|\n")
    lines.append("| **gameData.players** (roster bios) | **liveData** (plays, linescore, boxscore) — keep per game or as parquet |\n")
    lines.append("| **gameData.teams** (away/home info) | **gameData.game** (pk, type, season) — tiny; can keep in game header |\n")
    lines.append("| **gameData.venue** | **gameData.datetime**, **status** — small |\n")
    lines.append("| **gameData.weather** (optional) | **metaData**, **copyright**, **link** — omit or derive |\n")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("".join(lines), encoding="utf-8")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
