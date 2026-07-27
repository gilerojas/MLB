"""Tweet copy for the daily pitcher showdown."""

from __future__ import annotations


def _fmt(value: float | None, decimals: int = 2) -> str:
    return "—" if value is None else f"{float(value):.{decimals}f}"


def build_showdown_tweet(showdown: dict, *, max_len: int = 280) -> str:
    away = showdown["away"]
    home = showdown["home"]
    away_recent = away["recent"]
    home_recent = home["recent"]
    lines = [
        f"Today's arm duel: {away['name']} vs {home['name']}.",
        "",
        (
            f"-> {away['name'].split()[-1]}: {away_recent.get('ip')} IP, "
            f"{away_recent.get('strikeouts')} K, {_fmt(away_recent.get('era'))} ERA in his last 3"
        ),
        (
            f"-> {home['name'].split()[-1]}: {home_recent.get('ip')} IP, "
            f"{home_recent.get('strikeouts')} K, {_fmt(home_recent.get('era'))} ERA in his last 3"
        ),
        "",
        (
            f"{away['team']} @ {home['team']} · {showdown.get('game_time') or 'TBD'}"
        ),
    ]
    tweet = "\n".join(lines)
    if len(tweet) <= max_len:
        return tweet
    lines.pop(-2)
    tweet = "\n".join(lines)
    if len(tweet) <= max_len:
        return tweet
    return tweet[: max_len - 1].rstrip() + "…"
