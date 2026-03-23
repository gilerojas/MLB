#!/usr/bin/env python3
"""
Leaders tweet: top hitters and pitchers for a season/stage.

Reads raw feed_live JSON from the warehouse (<season>/<stage>/raw/),
aggregates boxscore batting and pitching stats, and prints a tweet-ready summary.
No pitch-level or sabermetric data—uses only boxscore stats from the feed.

Ranking:
  --rank-by composite (default): weighted “performance” (OPS×volume+HR/PA for bats;
  K×√IP + K/9 + IP − ERA damp for arms). Better than single-stat for ST samples.
  --rank-by stat: sort by --bat-stat / --pit-stat only (legacy).

Auto min AB / min IP = population percentile + clamps (see --ab-pop-pct / --ip-pop-pct).

CLI reference (run from repo root)
------------------------------------

  # Defaults: season=2026, stage=spring_training, top_n=5, auto floors (AB ~pct 0.55, IP ~pct 0.55)
  python scripts/st_leaders_tweet.py

  python scripts/st_leaders_tweet.py --season 2025

  # Stage = carpeta bajo data/warehouse/mlb/<season>/ (mismo esquema que load_mlb_warehouse)
  python scripts/st_leaders_tweet.py --season 2026 --stage spring_training
  python scripts/st_leaders_tweet.py --season 2026 --stage regular_season
  python scripts/st_leaders_tweet.py --season 2026 --stage all_star
  python scripts/st_leaders_tweet.py --season 2026 --stage playoffs
  python scripts/st_leaders_tweet.py --season 2026 --stage "playoffs/wild_card"
  python scripts/st_leaders_tweet.py --season 2026 --stage "playoffs/division"
  python scripts/st_leaders_tweet.py --season 2026 --stage "playoffs/championship"
  python scripts/st_leaders_tweet.py --season 2026 --stage "playoffs/world_series"

  # Warehouse distinto (ruta relativa al repo o absoluta)
  python scripts/st_leaders_tweet.py --warehouse data/warehouse/mlb

  # Top N líderes (bateo y pitcheo)
  python scripts/st_leaders_tweet.py --top-n 3
  python scripts/st_leaders_tweet.py --top-n 10

  # Mínimos fijos (anula el cálculo automático por percentil)
  python scripts/st_leaders_tweet.py --min-ab 20 --min-ip 8

  # Ajustar solo el “smart floor”: percentil de la población (0–1) + clamps internos AB/IP
  python scripts/st_leaders_tweet.py --ab-pop-pct 0.45 --ip-pop-pct 0.50

  # Sin llamar a la API de MLB para nombres (más rápido / offline; usa nombres del feed)
  python scripts/st_leaders_tweet.py --no-api-names

  # Ejemplo: ST temprano con pocos datos — relajar umbrales
  python scripts/st_leaders_tweet.py --min-ab 12 --min-ip 5
  python scripts/st_leaders_tweet.py --ab-pop-pct 0.40 --ip-pop-pct 0.45

  # Orden por una stat concreta (sin score compuesto)
  python scripts/st_leaders_tweet.py --rank-by stat --bat-stat hr
  python scripts/st_leaders_tweet.py --rank-by stat --pit-stat k9

  # Score compuesto (default): mostrar número de perf opcional
  python scripts/st_leaders_tweet.py --show-score

Ver también: python scripts/st_leaders_tweet.py --help
"""

import argparse
import gzip
import json
import math
import re
import sys
from pathlib import Path

import requests

# Project root so we can import src.* when run as a script
_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.ingestion.mlb_warehouse_schema import GAME_TYPE_TO_STAGE

# Sorting stat for leaderboards (boxscore aggregates only)
BAT_STAT_CHOICES = ("ops", "avg", "hr", "rbi", "tb", "runs", "slg", "obp")
PIT_STAT_CHOICES = ("era", "k", "ip", "whip", "k9")
RANK_BY_CHOICES = ("stat", "composite")

# Composite score weights (tune here). Higher = better for both.
# Bat: quality (OPS) × sublinear PA volume + HR rate + hit rate bumps.
PA_REF = 15.0  # ~min AB scale; sqrt(PA/PA_REF) ≈ 1 at 15 PA
BAT_OPS_WEIGHT = 1.0
BAT_HR_RATE_MULT = 10.0  # HR/PA scaled bonus
BAT_HIT_RATE_MULT = 2.5  # H/AB extra (on top of OPS)

# Pit: K×√IP + K/9×√IP + IP bank − ERA dampening (so 18 IP / big K beats 9 IP / 0 ERA).
PIT_K_VOL_WEIGHT = 0.55
PIT_K9_VOL_WEIGHT = 0.28
PIT_IP_FLAT_WEIGHT = 0.12
PIT_ERA_DAMP_OFFSET = 2.0  # 5/(ERA+offset); higher offset = less ERA separation

STAGE_TO_GAME_TYPES: dict[str, list[str]] = {}
for _gt, _stage in GAME_TYPE_TO_STAGE.items():
    STAGE_TO_GAME_TYPES.setdefault(_stage, []).append(_gt)


def _open_raw(path: Path):
    """Open raw file as text; supports .json and .json.gz."""
    if path.suffix == ".gz" or path.name.endswith(".json.gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def _innings_to_float(ip_str: str) -> float:
    """Convert MLB innings string (e.g. '1.0', '0.2', '2.1') to decimal innings."""
    if not ip_str or ip_str in (".--", "-.--"):
        return 0.0
    s = str(ip_str).strip()
    if "." not in s:
        try:
            return float(s)
        except ValueError:
            return 0.0
    a, b = s.split(".", 1)
    try:
        whole = int(a)
        partial = int(b)  # 1 = 1 out, 2 = 2 outs
    except ValueError:
        return 0.0
    return whole + partial / 3.0


def find_stage_raw_paths(warehouse: Path, season: int, stage: str) -> list[Path]:
    """
    Find all raw feed_live paths for a given stage in the given season.
    Prefers .json over .json.gz when both exist for the same game.
    """
    base = warehouse / str(season) / stage / "raw"
    if not base.exists():
        return []

    by_key: dict[str, Path] = {}
    for raw_path in base.glob("game_*_feed_live.json*"):
        if not (
            raw_path.name.endswith(".json")
            or raw_path.name.endswith(".json.gz")
        ):
            continue
        name = raw_path.name
        if name.endswith(".json.gz"):
            stem = name[:-7]
        else:
            stem = name[:-5]
        m = re.match(r"game_(\d+)_(\d+)_feed_live", stem)
        if not m:
            continue
        key = m.group(1) + "_" + m.group(2)
        if key not in by_key or (
            raw_path.name.endswith(".json")
            and not raw_path.name.endswith(".json.gz")
        ):
            by_key[key] = raw_path
    return sorted(by_key.values())


def aggregate_boxscore_from_raw(warehouse: Path, season: int, stage: str) -> tuple[dict, dict]:
    """
    Aggregate batting and pitching stats from all stage raw feed_live files.

    Returns:
        batting_totals: player_id (str) -> { atBats, hits, homeRuns, rbi, runs, name, team }
        pitching_totals: player_id (str) -> { ip, strikeOuts, earnedRuns, hits, baseOnBalls, name, team }
    """
    paths = find_stage_raw_paths(warehouse, season, stage)
    batting_totals: dict[str, dict] = {}
    pitching_totals: dict[str, dict] = {}

    bat_keys = [
        "atBats",
        "hits",
        "homeRuns",
        "rbi",
        "runs",
        "strikeOuts",
        "baseOnBalls",
        "hitByPitch",
        "sacFlies",
        "doubles",
        "triples",
        "plateAppearances",
        "totalBases",
        "stolenBases",
    ]

    for raw_path in paths:
        try:
            with _open_raw(raw_path) as f:
                feed = json.load(f)
        except Exception:
            continue

        game_data = feed.get("gameData", {})
        teams = game_data.get("teams", {})
        away_abbrev = (teams.get("away") or {}).get("abbreviation") or "?"
        home_abbrev = (teams.get("home") or {}).get("abbreviation") or "?"

        box = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
        for side, abbrev in [("away", away_abbrev), ("home", home_abbrev)]:
            players = (box.get(side) or {}).get("players") or {}
            for player_id, p in players.items():
                person = (p.get("person") or {})
                full_name = person.get("fullName") or f"ID {player_id}"
                stats = p.get("stats") or {}

                bat = stats.get("batting") or {}
                if bat and (bat.get("atBats") or 0) > 0:
                    agg = batting_totals.setdefault(
                        player_id,
                        {k: 0 for k in bat_keys}
                        | {"name": full_name, "team": abbrev},
                    )
                    for k in bat_keys:
                        v = bat.get(k)
                        if isinstance(v, (int, float)):
                            agg[k] = agg.get(k, 0) + v
                    agg["name"] = full_name
                    agg["team"] = abbrev

                pit = stats.get("pitching") or {}
                if pit and (pit.get("inningsPitched") or pit.get("outs", 0)):
                    ip_val = pit.get("inningsPitched")
                    if isinstance(ip_val, str):
                        ip_float = _innings_to_float(ip_val)
                    else:
                        ip_float = float(ip_val or 0)
                    if ip_float <= 0:
                        continue
                    agg = pitching_totals.setdefault(
                        player_id,
                        {
                            "ip": 0.0,
                            "strikeOuts": 0,
                            "earnedRuns": 0,
                            "runs": 0,
                            "hits": 0,
                            "baseOnBalls": 0,
                            "battersFaced": 0,
                            "homeRuns": 0,
                            "hitByPitch": 0,
                            "name": full_name,
                            "team": abbrev,
                        },
                    )
                    agg["ip"] += ip_float
                    for k in (
                        "strikeOuts",
                        "earnedRuns",
                        "runs",
                        "hits",
                        "baseOnBalls",
                        "battersFaced",
                        "homeRuns",
                        "hitByPitch",
                    ):
                        v = pit.get(k)
                        if isinstance(v, (int, float)):
                            agg[k] = agg.get(k, 0) + v
                    agg["name"] = full_name
                    agg["team"] = abbrev

    return batting_totals, pitching_totals


def fetch_player_name_team(player_id: str | int) -> tuple[str, str]:
    """
    Fetch player's full name and current team abbreviation from MLB Stats API.
    player_id can be string like "ID647304" or int 647304.
    """
    pid = int(str(player_id).lstrip("ID")) if isinstance(player_id, str) else int(player_id)
    url = f"https://statsapi.mlb.com/api/v1/people?personIds={pid}&hydrate=currentTeam"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json().get("people", [])
        if not data:
            return f"ID {pid}", "MLB"
        data = data[0]
        name = data.get("fullName", f"ID {pid}")
        team = (data.get("currentTeam") or {}).get("abbreviation") or "MLB"
        return name, team
    except Exception:
        return f"ID {pid}", "MLB"


def _batting_rates(agg: dict) -> dict:
    """Derived AVG/OBP/SLG/OPS from boxscore totals."""
    ab = agg.get("atBats") or 0
    h = agg.get("hits") or 0
    tb = agg.get("totalBases") or 0
    bb = agg.get("baseOnBalls") or 0
    hbp = agg.get("hitByPitch") or 0
    sf = agg.get("sacFlies") or 0
    hr = agg.get("homeRuns") or 0
    rbi = agg.get("rbi") or 0
    runs = agg.get("runs") or 0
    avg = (h / ab) if ab else 0.0
    obp_denom = ab + bb + hbp + sf
    obp = (h + bb + hbp) / obp_denom if obp_denom > 0 else 0.0
    slg = tb / ab if ab > 0 else 0.0
    ops = obp + slg
    return {
        "ab": ab,
        "h": h,
        "tb": tb,
        "hr": hr,
        "rbi": rbi,
        "runs": runs,
        "avg": avg,
        "obp": obp,
        "slg": slg,
        "ops": ops,
    }


def effective_pa(agg: dict) -> int:
    """Plate appearances from boxscore; fallback to AB+BB+HBP+SF."""
    pa = agg.get("plateAppearances") or 0
    if pa > 0:
        return int(pa)
    ab = agg.get("atBats") or 0
    bb = agg.get("baseOnBalls") or 0
    hbp = agg.get("hitByPitch") or 0
    sf = agg.get("sacFlies") or 0
    return int(ab + bb + hbp + sf)


def composite_batter_score(agg: dict) -> float:
    """
    Single number: OPS + volume + power + contact, not raw AVG/OPS in one game.
    Uses PA so walks count as "turnos" vs 10 AB vs 40 AB.
    """
    r = _batting_rates(agg)
    ab = r["ab"]
    pa = effective_pa(agg)
    if ab < 1 or pa < 1:
        return float("-inf")
    pa = max(pa, ab)
    ops = r["ops"]
    hr = float(r["hr"])
    h = float(r["h"])
    hr_rate = hr / float(pa)
    hit_rate = h / float(ab)
    # Sublinear volume: .300 over 40 PA beats .300 over 10 PA
    vol = math.sqrt(pa / PA_REF)
    score = (
        BAT_OPS_WEIGHT * ops * vol
        * (1.0 + BAT_HR_RATE_MULT * hr_rate)
        + BAT_HIT_RATE_MULT * hit_rate * math.sqrt(pa / PA_REF)
    )
    return float(score)


def composite_pitcher_score(agg: dict) -> float:
    """
    Rewards strikeouts, innings pitched, and K/9; ERA dampens without erasing
    high-volume strikeout arms (e.g. 18 IP / 23 K vs 9 IP / 0 ERA).
    """
    ip = float(agg.get("ip") or 0.0)
    if ip < 1e-6:
        return float("-inf")
    k = int(agg.get("strikeOuts") or 0)
    er = int(agg.get("earnedRuns") or 0)
    era = (er * 9.0) / ip
    k9 = (k * 9.0) / ip
    vol = math.sqrt(ip)
    era_factor = 5.0 / (era + PIT_ERA_DAMP_OFFSET)
    score = (
        era_factor
        * (
            PIT_K_VOL_WEIGHT * (k * vol) / 11.0
            + PIT_K9_VOL_WEIGHT * (k9 * vol) / 12.0
        )
        + PIT_IP_FLAT_WEIGHT * ip
    )
    return float(score)


def top_batters_from_aggregates(
    batting_totals: dict[str, dict],
    min_ab: int = 15,
    top_n: int = 5,
    stat: str = "ops",
    rank_by: str = "stat",
) -> list[tuple[str, dict]]:
    """
    Top batters by chosen stat (min atBats) or by composite score.
    Returns list of (player_id, agg_dict) for top_n.
    """
    stat = (stat or "ops").strip().lower()
    if stat not in BAT_STAT_CHOICES:
        stat = "ops"
    rank_by = (rank_by or "stat").strip().lower()
    if rank_by not in RANK_BY_CHOICES:
        rank_by = "stat"

    candidates = [
        (pid, agg)
        for pid, agg in batting_totals.items()
        if (agg.get("atBats") or 0) >= min_ab
    ]
    if not candidates:
        return []

    if rank_by == "composite":
        scored = [(composite_batter_score(agg), pid, agg) for pid, agg in candidates]
        scored.sort(key=lambda x: x[0], reverse=True)
        return [(pid, agg) for _, pid, agg in scored[:top_n]]

    def sort_key_ops(item):
        pid, agg = item
        r = _batting_rates(agg)
        ab = r["ab"]
        if ab <= 0:
            return -1.0, 0, -1.0, 0, 0
        return r["ops"], ab, r["avg"], r["hr"], r["h"]

    def sort_key(item):
        pid, agg = item
        r = _batting_rates(agg)
        ab = r["ab"]
        if ab <= 0:
            return (0.0,) * 8
        if stat == "ops":
            return sort_key_ops(item)
        if stat == "avg":
            return (r["avg"], r["ops"], r["hr"], r["h"], ab)
        if stat == "hr":
            return (r["hr"], r["ops"], r["tb"], ab)
        if stat == "rbi":
            return (r["rbi"], r["hr"], r["ops"], ab)
        if stat == "tb":
            return (r["tb"], r["hr"], r["ops"], ab)
        if stat == "runs":
            return (r["runs"], r["hr"], r["ops"], ab)
        if stat == "slg":
            return (r["slg"], r["ops"], r["hr"], ab)
        if stat == "obp":
            return (r["obp"], r["ops"], r["hr"], ab)
        return sort_key_ops(item)

    candidates.sort(key=sort_key, reverse=True)
    return candidates[:top_n]


def top_pitchers_from_aggregates(
    pitching_totals: dict[str, dict],
    min_ip: float = 5.0,
    top_n: int = 5,
    stat: str = "era",
    rank_by: str = "stat",
) -> list[tuple[str, dict]]:
    """
    Top pitchers by chosen rate/count (min IP) or by composite score.
    Returns list of (player_id, agg_dict) for top_n.
    """
    stat = (stat or "era").strip().lower()
    if stat not in PIT_STAT_CHOICES:
        stat = "era"
    rank_by = (rank_by or "stat").strip().lower()
    if rank_by not in RANK_BY_CHOICES:
        rank_by = "stat"

    candidates = [
        (pid, agg)
        for pid, agg in pitching_totals.items()
        if (agg.get("ip") or 0) >= min_ip
    ]
    if not candidates:
        return []

    if rank_by == "composite":
        scored = [(composite_pitcher_score(agg), pid, agg) for pid, agg in candidates]
        scored.sort(key=lambda x: x[0], reverse=True)
        return [(pid, agg) for _, pid, agg in scored[:top_n]]

    def era_sort(item):
        pid, agg = item
        ip = agg.get("ip") or 0
        if ip <= 0:
            return (999.0, 0.0, 0.0)
        er = agg.get("earnedRuns") or 0
        era = (er * 9.0) / ip
        k = agg.get("strikeOuts") or 0
        return (era, -k, -ip)

    def sort_key_asc(item):
        """Lower tuple value = better row position (ERA, WHIP)."""
        pid, agg = item
        ip = agg.get("ip") or 0.0
        if ip <= 0:
            return (999.0,)
        h = agg.get("hits") or 0
        bb = agg.get("baseOnBalls") or 0
        k = agg.get("strikeOuts") or 0
        er = agg.get("earnedRuns") or 0
        era = (er * 9.0) / ip
        whip = (h + bb) / ip
        k9 = (k * 9.0) / ip

        if stat == "era":
            return era_sort(item)
        if stat == "whip":
            return (whip, era, -k)
        if stat == "k":
            return (-k, -ip, era)
        if stat == "ip":
            return (-ip, -k, era)
        if stat == "k9":
            return (-k9, -k, -ip)
        return era_sort(item)

    candidates.sort(key=sort_key_asc)
    return candidates[:top_n]


def _auto_floor(
    values: list[float],
    population_pct: float,
    low: float,
    high: float,
    fallback_frac: float = 0.3,
) -> float:
    """Choose a threshold from a population percentile and clamp it."""
    if not values:
        return low
    values = sorted(values)
    n = len(values)
    if n < 10:
        base = values[-1] * fallback_frac
    else:
        pct = min(max(population_pct, 0.01), 0.99)
        idx = max(0, int(pct * n) - 1)
        base = values[idx]
    return max(low, min(base, high))


def auto_min_ab(batting_totals: dict[str, dict], population_pct: float = 0.55) -> int:
    """
    Choose an AB floor from the population of batters with AB > 0.

    Default percentile (~median-ish workload) + hard floor so tiny samples (5–10 AB)
    don't dominate OPS leaderboards. Clamped to [15, 35]. Small samples: fraction of max AB.
    """
    abs_list = sorted(
        int(agg.get("atBats") or 0)
        for agg in batting_totals.values()
        if (agg.get("atBats") or 0) > 0
    )
    if not abs_list:
        return 15

    # Slightly higher fallback when few players so we don't default to min 5 AB territory.
    base = _auto_floor(
        [float(x) for x in abs_list],
        population_pct,
        15.0,
        35.0,
        fallback_frac=0.45,
    )
    return int(round(base))


def auto_min_ip(pitching_totals: dict[str, dict], population_pct: float = 0.55) -> float:
    """
    Choose an IP floor from the population of pitchers with IP > 0.

    Default percentile + floor so one-inning outings don't dominate ERA. Clamped to [6.0, 18.0].
    """
    ips = sorted(
        float(agg.get("ip") or 0.0)
        for agg in pitching_totals.values()
        if (agg.get("ip") or 0.0) > 0.0
    )
    if not ips:
        return 6.0

    return round(
        _auto_floor(ips, population_pct, 6.0, 18.0, fallback_frac=0.45),
        1,
    )


def _format_bat_line(bat_stat: str, agg: dict, full_name: str, team: str) -> str:
    """One leaderboard row; emphasizes bat_stat."""
    r = _batting_rates(agg)
    ab, h, hr = r["ab"], r["h"], int(r["hr"])
    avg_str = f".{int(round(r['avg'] * 1000)):03d}" if ab else ".000"
    ops_str = f"{r['ops']:.3f}"
    base = f"{full_name} ({team})"
    if bat_stat == "ops":
        return f"{base} {avg_str} AVG, {ops_str} OPS, {h}/{ab} H/AB, {hr} HR"
    if bat_stat == "avg":
        return f"{base} {avg_str} AVG ({h}/{ab}), {ops_str} OPS, {hr} HR"
    if bat_stat == "hr":
        return f"{base} {hr} HR · {ops_str} OPS · {avg_str} AVG · {h}/{ab} H/AB"
    if bat_stat == "rbi":
        return f"{base} {int(r['rbi'])} RBI · {hr} HR · {ops_str} OPS · {h}/{ab}"
    if bat_stat == "tb":
        return f"{base} {int(r['tb'])} TB · {hr} HR · {ops_str} OPS · {h}/{ab}"
    if bat_stat == "runs":
        return f"{base} {int(r['runs'])} R · {hr} HR · {ops_str} OPS · {h}/{ab}"
    if bat_stat == "slg":
        return f"{base} {r['slg']:.3f} SLG · {ops_str} OPS · {avg_str} AVG · {h}/{ab}"
    if bat_stat == "obp":
        return f"{base} {r['obp']:.3f} OBP · {ops_str} OPS · {avg_str} AVG · {h}/{ab}"
    return f"{base} {avg_str} AVG, {ops_str} OPS, {h}/{ab} H/AB, {hr} HR"


def _format_pit_line(pit_stat: str, agg: dict, full_name: str, team: str) -> str:
    ip = agg.get("ip") or 0.0
    er = agg.get("earnedRuns") or 0
    k = agg.get("strikeOuts") or 0
    h = agg.get("hits") or 0
    bb = agg.get("baseOnBalls") or 0
    era = (er * 9.0 / ip) if ip else 0.0
    whip = ((h + bb) / ip) if ip else 0.0
    k9 = (k * 9.0 / ip) if ip else 0.0
    base = f"{full_name} ({team})"
    if pit_stat == "era":
        return f"{base} {era:.2f} ERA · {ip:.1f} IP · {int(k)} K"
    if pit_stat == "k":
        return f"{base} {int(k)} K · {ip:.1f} IP · {era:.2f} ERA"
    if pit_stat == "ip":
        return f"{base} {ip:.1f} IP · {int(k)} K · {era:.2f} ERA"
    if pit_stat == "whip":
        return f"{base} {whip:.2f} WHIP · {era:.2f} ERA · {ip:.1f} IP · {int(k)} K"
    if pit_stat == "k9":
        return f"{base} {k9:.1f} K/9 · {int(k)} K · {ip:.1f} IP · {era:.2f} ERA"
    return f"{base} {ip:.1f} IP, {era:.2f} ERA, {int(k)} K"


def _format_bat_composite_line(
    agg: dict, full_name: str, team: str, show_score: bool
) -> str:
    r = _batting_rates(agg)
    pa = effective_pa(agg)
    sc = composite_batter_score(agg)
    base = f"{full_name} ({team})"
    line = (
        f"{base} {r['ops']:.3f} OPS · {pa} PA · {int(r['hr'])} HR · "
        f"{int(r['h'])}/{r['ab']} H/AB"
    )
    if show_score:
        line += f" · perf {sc:.2f}"
    return line


def _format_pit_composite_line(
    agg: dict, full_name: str, team: str, show_score: bool
) -> str:
    ip = float(agg.get("ip") or 0.0)
    k = int(agg.get("strikeOuts") or 0)
    h = int(agg.get("hits") or 0)
    bb = int(agg.get("baseOnBalls") or 0)
    er = int(agg.get("earnedRuns") or 0)
    era = (er * 9.0 / ip) if ip else 0.0
    k9 = (k * 9.0 / ip) if ip else 0.0
    whip = ((h + bb) / ip) if ip else 0.0
    sc = composite_pitcher_score(agg)
    base = f"{full_name} ({team})"
    line = (
        f"{base} {k9:.1f} K/9 · {int(k)} K · {ip:.1f} IP · {whip:.2f} WHIP · {era:.2f} ERA"
    )
    if show_score:
        line += f" · perf {sc:.2f}"
    return line


def build_tweet(
    batting_totals: dict[str, dict],
    pitching_totals: dict[str, dict],
    season: int,
    stage: str,
    min_ab: int = 15,
    min_ip: float = 5.0,
    use_api_names: bool = True,
    top_n: int = 5,
    bat_stat: str = "ops",
    pit_stat: str = "era",
    rank_by: str = "composite",
    show_score: bool = False,
) -> str:
    bat_stat = (bat_stat or "ops").strip().lower()
    if bat_stat not in BAT_STAT_CHOICES:
        bat_stat = "ops"
    pit_stat = (pit_stat or "era").strip().lower()
    if pit_stat not in PIT_STAT_CHOICES:
        pit_stat = "era"
    rank_by = (rank_by or "composite").strip().lower()
    if rank_by not in RANK_BY_CHOICES:
        rank_by = "composite"

    bats = top_batters_from_aggregates(
        batting_totals,
        min_ab=min_ab,
        top_n=top_n,
        stat=bat_stat,
        rank_by=rank_by,
    )
    arms = top_pitchers_from_aggregates(
        pitching_totals,
        min_ip=min_ip,
        top_n=top_n,
        stat=pit_stat,
        rank_by=rank_by,
    )

    if not bats and not arms:
        return f"{stage.replace('_', ' ').title()} {season} leaders not available yet (no data)."

    name_cache: dict[str, tuple[str, str]] = {}

    bat_parts: list[str] = []
    for pid, agg in bats:
        if use_api_names and pid not in name_cache:
            name_cache[pid] = fetch_player_name_team(pid)
        if use_api_names:
            full_name, api_team = name_cache[pid]
            team = (agg.get("team") or api_team) if api_team == "MLB" else api_team
        else:
            full_name = agg.get("name") or f"ID {pid}"
            team = agg.get("team") or "?"

        if rank_by == "composite":
            bat_parts.append(
                _format_bat_composite_line(agg, full_name, team, show_score)
            )
        else:
            bat_parts.append(_format_bat_line(bat_stat, agg, full_name, team))

    arm_parts: list[str] = []
    for pid, agg in arms:
        if use_api_names and pid not in name_cache:
            name_cache[pid] = fetch_player_name_team(pid)
        if use_api_names:
            full_name, api_team = name_cache[pid]
            team = (agg.get("team") or api_team) if api_team == "MLB" else api_team
        else:
            full_name = agg.get("name") or f"ID {pid}"
            team = agg.get("team") or "?"
        if rank_by == "composite":
            arm_parts.append(
                _format_pit_composite_line(agg, full_name, team, show_score)
            )
        else:
            arm_parts.append(_format_pit_line(pit_stat, agg, full_name, team))

    intro = f"{stage.replace('_', ' ').title()} {season} leaders so far"
    if rank_by == "composite":
        filter_line = (
            f"(top {top_n} · rank=composite perf · min {min_ab} AB, {min_ip:.1f} IP)"
        )
    else:
        filter_line = (
            f"(top {top_n} · bats by {bat_stat.upper()}, arms by {pit_stat.upper()} "
            f"· min {min_ab} AB, {min_ip:.1f} IP)"
        )

    if bat_parts:
        bats_block = "Bats:\n- " + "\n- ".join(bat_parts)
    else:
        bats_block = "Bats: —"

    if arm_parts:
        arms_block = "Arms:\n- " + "\n- ".join(arm_parts)
    else:
        arms_block = "Arms: —"

    return f"{intro}\n{filter_line}\n\n{bats_block}\n{arms_block}"


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Stage leaders tweet from raw JSON. Default --rank-by composite (OPS×PA+HR; "
            "pitching K×IP+K/9−ERA). Use --rank-by stat for single-stat sorts."
        )
    )
    ap.add_argument(
        "--season",
        type=int,
        default=2026,
        help="Season year (default: 2026)",
    )
    ap.add_argument(
        "--stage",
        type=str,
        default="spring_training",
        choices=sorted(STAGE_TO_GAME_TYPES.keys()),
        help="Warehouse stage to aggregate (default: spring_training)",
    )
    ap.add_argument(
        "--warehouse",
        type=Path,
        default=Path("data/warehouse/mlb"),
        help="Warehouse root (default: data/warehouse/mlb)",
    )
    ap.add_argument(
        "--min-ab",
        type=int,
        default=None,
        help="Minimum at-bats for hitters (default: auto from AB population percentile)",
    )
    ap.add_argument(
        "--min-ip",
        type=float,
        default=None,
        help="Minimum innings for pitchers (default: auto from IP population percentile)",
    )
    ap.add_argument(
        "--ab-pop-pct",
        type=float,
        default=0.55,
        help="Population percentile for auto AB floor (default: 0.55; clamped 15–35 AB)",
    )
    ap.add_argument(
        "--ip-pop-pct",
        type=float,
        default=0.55,
        help="Population percentile for auto IP floor (default: 0.55; clamped 6–18 IP)",
    )
    ap.add_argument(
        "--top-n",
        type=int,
        default=5,
        help="Number of leaders for bats and arms (default: 5)",
    )
    ap.add_argument(
        "--no-api-names",
        action="store_true",
        help="Use names from feed only (no MLB API lookup)",
    )
    ap.add_argument(
        "--bat-stat",
        type=str,
        default="ops",
        choices=list(BAT_STAT_CHOICES),
        help=f"Rank hitters by this stat (choices: {', '.join(BAT_STAT_CHOICES)})",
    )
    ap.add_argument(
        "--pit-stat",
        type=str,
        default="era",
        choices=list(PIT_STAT_CHOICES),
        help=f"Rank pitchers by this stat (choices: {', '.join(PIT_STAT_CHOICES)})",
    )
    ap.add_argument(
        "--rank-by",
        type=str,
        default="composite",
        choices=list(RANK_BY_CHOICES),
        help="composite = weighted perf score; stat = --bat-stat / --pit-stat only",
    )
    ap.add_argument(
        "--show-score",
        action="store_true",
        help="Append internal perf score (composite mode only)",
    )
    args = ap.parse_args()

    raw_paths = find_stage_raw_paths(args.warehouse, args.season, args.stage)
    if not raw_paths:
        print(f"No raw feed files found under {args.warehouse / str(args.season) / args.stage / 'raw'}")
        sys.exit(1)

    batting_totals, pitching_totals = aggregate_boxscore_from_raw(
        args.warehouse,
        args.season,
        args.stage,
    )

    # Auto floors from stage distributions when not specified.
    min_ab = (
        args.min_ab
        if args.min_ab is not None
        else auto_min_ab(batting_totals, population_pct=args.ab_pop_pct)
    )
    min_ip = (
        args.min_ip
        if args.min_ip is not None
        else auto_min_ip(pitching_totals, population_pct=args.ip_pop_pct)
    )

    tweet = build_tweet(
        batting_totals,
        pitching_totals,
        args.season,
        args.stage,
        min_ab=min_ab,
        min_ip=min_ip,
        use_api_names=not args.no_api_names,
        top_n=args.top_n,
        bat_stat=args.bat_stat,
        pit_stat=args.pit_stat,
        rank_by=args.rank_by,
        show_score=args.show_score,
    )
    print(tweet)
    print(f"\n({len(tweet)} chars)")


if __name__ == "__main__":
    main()
