#!/usr/bin/env python3
"""
Genera el texto del tweet automatizado con los juegos de MLB del día.

Formato por defecto: fecha + "X matchups to start:" + lista con • AWAY @ HOME 1:05p ET.
Sin frases fijas ni hashtags; listo para correr toda la temporada.

Uso:
  python scripts/tweet_games_of_the_day.py              # hoy
  python scripts/tweet_games_of_the_day.py --date 2026-02-21
  python scripts/tweet_games_of_the_day.py --format tweet
  python scripts/tweet_games_of_the_day.py --intro "..." --hashtag "#OpeningDay"  # override puntual
"""
import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

BASE_URL = "https://statsapi.mlb.com/api/v1"
SPORT_ID_MLB = 1

# team id → abreviatura (3 letras, estándar MLB)
TEAM_ABBREV = {
    108: "LAA", 109: "ARI", 110: "BAL", 111: "BOS", 112: "CHC", 113: "CIN",
    114: "CLE", 115: "COL", 116: "DET", 117: "HOU", 118: "KC", 119: "LAD",
    120: "WSH", 121: "NYM", 133: "OAK", 134: "PIT", 135: "SD", 136: "SEA",
    137: "SF", 138: "STL", 139: "TB", 140: "TEX", 141: "TOR", 142: "MIN",
    143: "PHI", 144: "ATL", 145: "CWS", 146: "MIA", 147: "NYY", 158: "MIL",
}


def fetch_schedule_for_date(date: str) -> list[dict]:
    """Juegos del día desde el schedule API (date YYYY-MM-DD)."""
    url = f"{BASE_URL}/schedule"
    params = {"sportId": SPORT_ID_MLB, "date": date}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    games = []
    for d in data.get("dates", []):
        games.extend(d.get("games", []))
    return games


def abbrev(team: dict) -> str:
    tid = team.get("id")
    return TEAM_ABBREV.get(tid, team.get("name", "???")[:3].upper())


def game_time_et(game_date_iso: str | None) -> str:
    """Hora ET 24h para formato compacto."""
    if not game_date_iso:
        return "TBD"
    try:
        dt = datetime.fromisoformat(game_date_iso.replace("Z", "+00:00"))
        et = dt.astimezone(ZoneInfo("America/New_York"))
        return et.strftime("%H:%M")
    except Exception:
        return "TBD"


def game_time_et_12h(game_date_iso: str | None) -> str:
    """Hora ET en 12h con sufijo a/p, e.g. 1:05p ET."""
    if not game_date_iso:
        return "TBD"
    try:
        dt = datetime.fromisoformat(game_date_iso.replace("Z", "+00:00"))
        et = dt.astimezone(ZoneInfo("America/New_York"))
        h = et.hour
        m = et.minute
        if h == 0:
            return f"12:{m:02d}a ET"
        if h < 12:
            return f"{h}:{m:02d}a ET"
        if h == 12:
            return f"12:{m:02d}p ET"
        return f"{h - 12}:{m:02d}p ET"
    except Exception:
        return "TBD"


def game_line(g: dict, include_scores: bool) -> str:
    away = g.get("teams", {}).get("away", {})
    home = g.get("teams", {}).get("home", {})
    away_team = abbrev(away.get("team", {}))
    home_team = abbrev(home.get("team", {}))
    status = (g.get("status") or {}).get("detailedState", "")
    time_str = game_time_et(g.get("gameDate"))

    if include_scores and status == "Final" and "score" in away and "score" in home:
        return f"{away_team} {away.get('score', 0)} @ {home_team} {home.get('score', 0)} F"
    if status == "In Progress" and "score" in away and "score" in home:
        return f"{away_team} {away.get('score', 0)} @ {home_team} {home.get('score', 0)}"
    return f"{away_team} @ {home_team} {time_str}"


def game_bullet_line(g: dict) -> str:
    """Una línea tipo bullet para el post: • AWAY @ HOME 1:05p ET."""
    away = g.get("teams", {}).get("away", {})
    home = g.get("teams", {}).get("home", {})
    away_team = abbrev(away.get("team", {}))
    home_team = abbrev(home.get("team", {}))
    time_str = game_time_et_12h(g.get("gameDate"))
    return f"• {away_team} @ {home_team} {time_str}"


def build_post_full(
    games: list[dict],
    date_str: str,
    *,
    intro: str | None = None,
    closing: str | None = None,
    cta: str | None = None,
    signoff: str | None = None,
) -> str:
    """
    Builds the post: date header, "X matchups to start:", and bullet list (• AWAY @ HOME 1:05p ET).
    Optional intro/closing/cta/signoff for one-off overrides; defaults are minimal for automation.
    """
    if not games:
        return intro or f"No games scheduled for {date_str}."

    games = sorted(games, key=lambda g: (g.get("gameDate") or ""))
    n = len(games)
    matchups_line = "One matchup to start:" if n == 1 else f"{n} matchups to start:"
    bullets = [game_bullet_line(g) for g in games]
    block = "\n".join([matchups_line] + bullets)

    day_fmt = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b")
    intro_line = intro or f"MLB today ({day_fmt})"
    out = f"{intro_line}\n\n{block}"
    if closing:
        out += f"\n{closing}"
    if cta:
        out += f"\n{cta}"
    if signoff:
        out += f"\n{signoff}"
    return out


def build_tweet(games: list[dict], date_str: str, include_scores: bool, max_len: int = 280) -> str:
    """Arma el texto del tweet compacto con los juegos del día."""
    day_fmt = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b")  # "15 Jun"
    intro = f"MLB hoy ({day_fmt})"
    if not games:
        return f"{intro}\n\nSin juegos programados."

    lines = [game_line(g, include_scores) for g in games]
    body = " · ".join(lines)
    tweet = f"{intro}\n\n{body}"

    if len(tweet) > max_len:
        body_max = max_len - len(intro) - 4
        parts = []
        for ln in lines:
            candidate = " · ".join(parts + [ln]) if parts else ln
            if len(candidate) <= body_max:
                parts.append(ln)
            else:
                break
        if not parts:
            parts = [lines[0][: body_max - 3] + "…"]
        tweet = f"{intro}\n\n{' · '.join(parts)}"
        if len(lines) > len(parts):
            tweet += f" (+{len(lines) - len(parts)} más)"
    return tweet


def main():
    ap = argparse.ArgumentParser(description="Tweet automatizado: juegos del día (MLB)")
    ap.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"), help="Fecha YYYY-MM-DD (default: hoy)")
    ap.add_argument(
        "--format",
        choices=("full", "tweet"),
        default="full",
        help="full = fecha + bullets; tweet = una línea hasta 280 chars",
    )
    ap.add_argument("--include-scores", action="store_true", help="Incluir marcadores (solo --format tweet)")
    ap.add_argument("--intro", help="Override intro (ej. día especial)")
    ap.add_argument("--closing", help="Línea extra después de la lista")
    ap.add_argument("--hashtag", help="Hashtag al final (ej. #OpeningDay); si no se pasa, no se añade")
    args = ap.parse_args()

    games = fetch_schedule_for_date(args.date)

    if args.format == "full":
        signoff = f"⚾ {args.hashtag}" if args.hashtag else None
        text = build_post_full(
            games,
            args.date,
            intro=args.intro,
            closing=args.closing,
            signoff=signoff,
        )
    else:
        text = build_tweet(games, args.date, args.include_scores)

    print(text)
    print(f"\n({len(text)} caracteres)")


if __name__ == "__main__":
    main()
