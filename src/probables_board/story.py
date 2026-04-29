"""
Slate story context and template-driven tweet copy.

Uses live regular-season standings when possible; falls back to
``data/warehouse/mlb/{season}/team_standings_regular_season.csv`` if the API call fails.
"""
from __future__ import annotations

import csv
import re
from datetime import datetime
from pathlib import Path
from typing import Any

# Matchup pairs (abbr) that get a "marquee" mention when on today's slate
_RIVALRY_PAIRS: set[frozenset[str]] = {
    frozenset({"NYY", "BOS"}),
    frozenset({"LAD", "SF"}),
    frozenset({"CHC", "STL"}),
    frozenset({"NYM", "PHI"}),
    frozenset({"ATL", "NYM"}),
    frozenset({"HOU", "TEX"}),
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _parse_streak(streak_code: str | None) -> tuple[str, int] | None:
    if not streak_code or len(streak_code) < 2:
        return None
    s = str(streak_code).strip().upper()
    k = s[0]
    if k not in ("W", "L"):
        return None
    m = re.match(r"^[WL](\d+)$", s)
    if not m:
        return None
    return ("W" if k == "W" else "L", int(m.group(1)))


def _display_team_name(abbrev: str, st: dict[str, Any] | None) -> str:
    if st and st.get("team_name"):
        return str(st["team_name"])
    return abbrev


def _era_value(era_s: str | None) -> float | None:
    if not era_s or str(era_s).strip() in ("—", "-", "N/A", "na"):
        return None
    try:
        return float(str(era_s).strip().replace("∞", "999"))
    except ValueError:
        return None


def _load_warehouse_standings_by_id(season: int) -> dict[int, dict[str, Any]]:
    p = _repo_root() / "data" / "warehouse" / "mlb" / str(season) / "team_standings_regular_season.csv"
    if not p.is_file():
        return {}
    out: dict[int, dict[str, Any]] = {}
    try:
        with p.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                raw_id = row.get("team_id")
                if not raw_id:
                    continue
                try:
                    tid = int(raw_id)
                except ValueError:
                    continue
                if tid:
                    out[tid] = dict(row)
    except OSError:
        return {}
    return out


def _fetch_live_standings_by_id(season: int) -> dict[int, dict[str, Any]]:
    try:
        from src.ingestion.season_exports import fetch_standings_regular_season
    except ImportError:
        return {}
    try:
        rows = fetch_standings_regular_season(season)
    except Exception:
        return {}
    m: dict[int, dict[str, Any]] = {}
    for r in rows:
        tid = r.get("team_id")
        if tid is not None:
            m[int(tid)] = r
    return m


def build_slate_story_context(
    date_str: str,
    rows: list[dict[str, Any]],
    *,
    prefer_live: bool = True,
) -> dict[str, Any]:
    """
    Return standings keyed by team_id for teams on the slate, plus source metadata.
    """
    try:
        season = int(date_str[:4])
    except (ValueError, TypeError):
        season = datetime.now().year

    by_id: dict[int, dict[str, Any]] = {}
    source: str = "none"

    if prefer_live:
        by_id = _fetch_live_standings_by_id(season)
        if by_id:
            source = "live"
    if not by_id:
        by_id = _load_warehouse_standings_by_id(season)
        if by_id:
            source = "warehouse"

    on_slate: set[int] = set()
    for r in rows:
        for k in ("away_team_id", "home_team_id"):
            v = r.get(k)
            if v is not None:
                on_slate.add(int(v))

    filtered: dict[int, dict[str, Any]] = {t: by_id[t] for t in on_slate if t in by_id}
    return {
        "date_str": date_str,
        "season": season,
        "standings_source": source,
        "by_team_id": filtered,
    }


def _streak_hook(
    abbrev: str, st: dict[str, Any] | None, kind: str, n: int
) -> tuple[str, float, str]:
    """Return (line, sort_score, category) — category is ``streak_L`` or ``streak_W`` for diversity."""
    name = _display_team_name(abbrev, st)
    # Same bar for both: 3+ games, and symmetric score so a hot team isn't buried under cold teams.
    base = 45.0 + min(n, 20) * 1.0
    if kind == "L" and n >= 3:
        line = (
            f"{name} look to snap a {n}-game losing streak."
            if n > 1
            else f"{name} look to bounce back after a loss."
        )
        return (line, base, "streak_L")
    if kind == "W" and n >= 3:
        line = f"{name} ride a {n}-game win streak into today."
        return (line, base, "streak_W")
    return ("", 0.0, "")


def _rivalry_hook(row: dict[str, Any]) -> tuple[str, float, str]:
    a = row.get("away_abbr") or "?"
    h = row.get("home_abbr") or "?"
    if frozenset({a, h}) not in _RIVALRY_PAIRS:
        return ("", 0.0, "")
    tm = row.get("time_et") or ""
    tbit = f" ({tm} ET)" if tm and tm != "TBD" else ""
    return (f"Marquee: {a} @ {h}{tbit}.", 35.0, "rivalry")


def _ace_hook(row: dict[str, Any]) -> tuple[str, float, str]:
    ap = row.get("away_pitcher") or {}
    hp = row.get("home_pitcher") or {}
    e1 = _era_value(ap.get("era"))
    e2 = _era_value(hp.get("era"))
    n1, n2 = ap.get("name", "?"), hp.get("name", "?")
    a, h = row.get("away_abbr", "?"), row.get("home_abbr", "?")
    if e1 is not None and e2 is not None and e1 < 2.0 and e2 < 2.0:
        return (
            f"Pitching spotlight: {n1} vs {n2} ({a} @ {h}) — sub-2.00 ERAs in the show.",
            32.0,
            "ace_pair",
        )
    if e1 is not None and e2 is not None and (e1 + e2) / 2.0 < 2.5:
        return (
            f"Rotation strength: {n1} / {n2} square off in {a} @ {h}.",
            25.0,
            "ace_pair",
        )
    return ("", 0.0, "")


def _slate_lowest_era_hook(rows: list[dict[str, Any]]) -> tuple[str, float, str]:
    """One lineup-wide pitcher note — best ERA among today's probables (when notable)."""
    best: tuple[float, str, str] | None = None  # era, name, team_abbr
    for r in rows:
        aabbr, habbr = r.get("away_abbr") or "?", r.get("home_abbr") or "?"
        for pit, team_abbr in (
            (r.get("away_pitcher") or {}, aabbr),
            (r.get("home_pitcher") or {}, habbr),
        ):
            e = _era_value(pit.get("era"))
            if e is None:
                continue
            name = (pit.get("name") or "?").strip()
            if best is None or e < best[0]:
                best = (e, name, team_abbr)
    if not best:
        return ("", 0.0, "")
    e, name, team_abbr = best
    # Only call out when the number is a real selling point (early-season noise still ok).
    if e > 2.75:
        return ("", 0.0, "")
    return (
        f"Arm to watch: {name} ({team_abbr}, {e:.2f} ERA) — best ERA among today's listed probables.",
        34.0,
        "slate_era",
    )


def _race_snippet(st: dict[str, Any] | None) -> tuple[str, float, str]:
    if not st:
        return ("", 0.0, "")
    for label, raw in (
        ("wild card", st.get("wild_card_games_back")),
        ("division", st.get("division_games_back")),
        ("standings", st.get("games_back")),
    ):
        if raw in (None, "", "-", "—"):
            continue
        s = str(raw).strip()
        if s in ("-", "—", "0.0", "0"):
            continue
        try:
            g = float(s.replace("+", ""))
        except ValueError:
            continue
        if 0 < g <= 1.0:
            name = st.get("team_abbrev", "Team")
            return (f"Race watch: {name} {g} GB ({label}).", 22.0, "race")
    return ("", 0.0, "")


def _collect_hooks(
    rows: list[dict[str, Any]], by_team_id: dict[int, dict[str, Any]]
) -> list[tuple[str, float, str, str]]:
    """Return list of (text, score, key, category) for de-duping and slate balance."""
    hooks: list[tuple[str, float, str, str]] = []

    for tid, st in by_team_id.items():
        ab = str(st.get("team_abbrev") or "").upper()
        sc = st.get("streak_code")
        parsed = _parse_streak(sc)
        if not parsed:
            continue
        k, n = parsed
        text, scv, cat = _streak_hook(ab, st, k, n)
        if not text or not cat:
            continue
        hooks.append((text, scv, f"streak_{tid}_{k}{n}", cat))

    for r in rows:
        text, scv, cat = _rivalry_hook(r)
        if text and cat:
            hooks.append((text, scv, f"rivalry_{r.get('away_abbr')}_{r.get('home_abbr')}", cat))
        text, scv, cat = _ace_hook(r)
        if text and cat:
            hooks.append((text, scv, f"ace_{r.get('away_abbr')}_{r.get('home_abbr')}", cat))

    for tid, st in by_team_id.items():
        text, scv, cat = _race_snippet(st)
        if text and cat:
            hooks.append((text, scv, f"race_{tid}", cat))

    text, scv, cat = _slate_lowest_era_hook(rows)
    if text and cat:
        hooks.append((text, scv, "slate_lowest_era", cat))

    hooks.sort(key=lambda x: -x[1])
    return hooks


def _pick_diverse_hooks(
    scored: list[tuple[str, float, str, str]], max_hooks: int = 2
) -> list[str]:
    """
    First line = highest score. Second line = best *different* category if any;
    otherwise next-best overall. Avoids back-to-back "losing streak" blurbs when
    a win streak, race note, or pitcher hook is available.
    """
    if not scored or max_hooks < 1:
        return []
    by_score = sorted(scored, key=lambda x: -x[1])
    first: tuple[str, str] | None = None  # (text, category)
    for text, _sc, _key, cat in by_score:
        if not text or not cat:
            continue
        first = (text, cat)
        break
    if not first:
        return []
    if max_hooks < 2:
        return [first[0]]
    t1, c1 = first

    def _too_similar(a: str, b: str) -> bool:
        if len(a) <= 5 or len(b) <= 5:
            return False
        ha, hb = a[:40], b[:40]
        return ha in hb or hb in ha

    for text, _sc, _key, cat in by_score:
        if not text or text == t1 or cat == c1:
            continue
        if _too_similar(t1, text):
            continue
        return [t1, text]
    for text, _sc, _key, _cat in by_score:
        if not text or text == t1:
            continue
        if _too_similar(t1, text):
            continue
        return [t1, text]
    return [t1]


def _short_cal(date_str: str) -> str:
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return date_str
    return f"{dt.strftime('%b')} {dt.day}"


def _long_date(date_str: str) -> str:
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return date_str
    return dt.strftime("%d %b %Y")


def build_story_tweet(
    date_str: str,
    rows: list[dict[str, Any]],
    max_len: int,
    *,
    hashtag: str = "#Mallitalytics",
    headline: str = "slate",
    context: dict[str, Any] | None = None,
) -> str:
    """
    Template-driven story tweet. ``headline`` = ``"slate"`` (Games of Day) or ``"probables"`` (board).

    If ``context`` is None, it is built via :func:`build_slate_story_context`.
    """
    tag = (hashtag.strip() or "#Mallitalytics").rstrip()
    if context is None:
        context = build_slate_story_context(date_str, rows, prefer_live=True)
    by_team_id = context.get("by_team_id") or {}
    n_games = len(rows)

    if headline == "probables":
        first = f"Probable starters — {_long_date(date_str)} {tag}"
    else:
        first = f"MLB slate for {_short_cal(date_str)} {tag}"

    if n_games == 0:
        out = f"{first}\n\nNo games on the schedule.\n"
        return out if len(out) <= max_len else out[: max_len - 1] + "…"

    scored = _collect_hooks(rows, by_team_id)
    hooks = _pick_diverse_hooks(scored, max_hooks=2)
    if not hooks and n_games:
        hooks = [
            f"{n_games} games on the docket; probables, season W-L, and first pitch (ET) on the card."
        ]

    body = "\n\n".join(hooks) if hooks else f"{n_games} games today."
    closer = f"\n\n{n_games} games — full slate, probables, and season W-L/ERA on the card. All times ET."
    out = f"{first}\n\n{body}{closer}"
    if len(out) <= max_len:
        return out

    # Shrink: drop closer phrase first, then one hook, then hard truncate
    # (``first`` line already includes ``tag``; do not repeat it in footers)
    short_footer = f"{n_games} games on the card."
    shorter = f"{first}\n\n{body}\n\n{short_footer}"
    if len(shorter) <= max_len:
        return shorter

    if len(hooks) > 1:
        body1 = "\n\n".join(hooks[:1])
        shorter2 = f"{first}\n\n{body1}{closer}"
        if len(shorter2) <= max_len:
            return shorter2
        shorter3 = f"{first}\n\n{body1}\n\n{short_footer}"
        if len(shorter3) <= max_len:
            return shorter3

    if hooks:
        one = f"{first}\n\n{hooks[0]}{closer}"
        if len(one) <= max_len:
            return one
    final_fb = f"{first}\n\n{n_games} games. Probables and lines on the card."
    if len(final_fb) <= max_len:
        return final_fb
    return out[: max_len - 1] + "…"
