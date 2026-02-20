"""
MLB Warehouse backfill: raw feed + Statcast → pitches_enriched.

Por cada juego genera:
  1. raw: game_{pk}_{date}_feed_live.json (feed/live desde API)
  2. pitches_enriched: game_{pk}_{date}_pitches_enriched.parquet (join Statcast + play_id)

Evita duplicados: si raw y pitches_enriched existen, se salta (usa --force para sobrescribir).
"""
import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests
from pybaseball import statcast_single_game
from tqdm import tqdm

from .mlb_warehouse_schema import (
    ALL_STAGES_GAME_TYPES,
    COLUMNS_TO_KEEP,
    FEED_LIVE_URL,
    SPORT_ID_MLB,
    BASE_URL,
    GAME_TYPE_TO_STAGE,
)


def get_stage_from_game_type(game_type: str) -> str:
    """gameType → stage folder."""
    gt = (game_type or "").strip().upper()
    return GAME_TYPE_TO_STAGE.get(gt, "regular_season")


def fetch_schedule(season: int, game_type: str) -> list[dict]:
    """Obtiene juegos del schedule API."""
    url = f"{BASE_URL}/schedule"
    params = {"sportId": SPORT_ID_MLB, "season": season, "gameType": game_type}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    games = []
    for d in data.get("dates", []):
        for g in d.get("games", []):
            games.append(g)
    return games


def fetch_feed(game_pk: int) -> dict | None:
    """Obtiene feed/live desde MLB API."""
    url = FEED_LIVE_URL.format(game_pk=game_pk)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def extract_play_ids_from_feed(feed: dict, game_pk: int) -> pd.DataFrame:
    """Extrae (game_pk, inning, at_bat_index, pitch_number, play_id) del raw."""
    rows = []
    for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
        inn = play["about"]["inning"]
        ab_idx = play["atBatIndex"]
        for ev in play.get("playEvents", []):
            if not ev.get("isPitch") or "pitchData" not in ev:
                continue
            rows.append({
                "game_pk": game_pk,
                "inning": inn,
                "at_bat_index": ab_idx,
                "pitch_number": ev.get("pitchNumber"),
                "play_id": ev.get("playId"),
            })
    return pd.DataFrame(rows)


def ensure_raw(warehouse: Path, game: dict, force: bool) -> tuple[Path | None, bool]:
    """
    Asegura que existe raw. Si no existe, lo obtiene de la API.
    Returns (raw_path, was_created). None si falló.
    """
    game_pk = game["gamePk"]
    game_type = game.get("gameType", "R")
    official_date = game.get("officialDate", "").replace("-", "")
    season = game.get("season", "")

    stage = get_stage_from_game_type(game_type)
    raw_dir = warehouse / season / stage / "raw"
    raw_path = raw_dir / f"game_{game_pk}_{official_date}_feed_live.json"

    if raw_path.exists() and not force:
        return raw_path, False

    try:
        feed = fetch_feed(game_pk)
    except Exception as e:
        return None, False

    raw_dir.mkdir(parents=True, exist_ok=True)
    with open(raw_path, "w") as f:
        json.dump(feed, f, indent=2, ensure_ascii=False)

    return raw_path, True


def _process_one(raw_path: Path, out_dir: Path, delay: float) -> bool:
    """Wrapper: delay + process_pitches_enriched (para workers)."""
    if delay > 0:
        time.sleep(delay)
    return process_pitches_enriched(raw_path, out_dir)


def process_pitches_enriched(raw_path: Path, out_dir: Path) -> bool:
    """
    Procesa un juego: raw → Statcast merge → pitches_enriched.parquet.
    """
    name = raw_path.stem
    m = re.match(r"game_(\d+)_(\d+)_feed_live", name)
    if not m:
        return False
    game_pk = int(m.group(1))
    date = m.group(2)

    with open(raw_path) as f:
        feed = json.load(f)

    df_feed_ids = extract_play_ids_from_feed(feed, game_pk)
    if df_feed_ids.empty:
        return False

    df_sc = statcast_single_game(game_pk)
    if df_sc is None or df_sc.empty:
        return False

    df_sc = df_sc.copy()
    df_sc["at_bat_index"] = df_sc["at_bat_number"] - 1

    key_cols = ["game_pk", "inning", "at_bat_index", "pitch_number"]
    merged = df_sc.merge(df_feed_ids, on=key_cols, how="left")

    cols = [c for c in COLUMNS_TO_KEEP if c in merged.columns]
    out_df = merged[cols].copy()

    out_path = out_dir / f"game_{game_pk}_{date}_pitches_enriched.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(out_path, index=False)
    return True


def schedule_game_to_row(g: dict, stage: str) -> dict:
    """Un juego del schedule → fila para CSV/post."""
    teams = g.get("teams", {})
    away = teams.get("away", {}).get("team", {})
    home = teams.get("home", {}).get("team", {})
    venue = g.get("venue", {}) or {}
    return {
        "date": g.get("officialDate", ""),
        "game_time": g.get("gameDate", "")[:19] if g.get("gameDate") else "",
        "away_team": away.get("name", ""),
        "home_team": home.get("name", ""),
        "venue": venue.get("name", ""),
        "game_pk": g.get("gamePk"),
        "stage": stage,
    }


def save_schedule_only(warehouse: Path, season: int, game_type: str | None, all_stages: bool) -> None:
    """
    Crea carpeta del año, descarga schedule(s) y guarda JSON + CSV para publicar.
    """
    year_dir = warehouse / str(season)
    year_dir.mkdir(parents=True, exist_ok=True)

    if all_stages:
        games_by_pk: dict[int, dict] = {}
        for gt in ALL_STAGES_GAME_TYPES:
            batch = fetch_schedule(season, gt)
            for g in batch:
                if g["gamePk"] not in games_by_pk:
                    games_by_pk[g["gamePk"]] = g
        games = list(games_by_pk.values())
        stages_used = ALL_STAGES_GAME_TYPES
    else:
        gt = game_type or "R"
        games = fetch_schedule(season, gt)
        stages_used = [gt]

    # Por stage: guardar JSON
    by_stage: dict[str, list[dict]] = {}
    for g in games:
        gt = (g.get("gameType") or "R").strip().upper()
        stage = get_stage_from_game_type(gt)
        by_stage.setdefault(stage, []).append(g)

    for stage, stage_games in by_stage.items():
        out_json = year_dir / f"schedule_{stage}.json"
        with open(out_json, "w") as f:
            json.dump(stage_games, f, indent=2, ensure_ascii=False)
        print(f"  {out_json} ({len(stage_games)} juegos)")

    # Un solo CSV con todas las filas para publicar
    rows = []
    for g in games:
        gt = (g.get("gameType") or "R").strip().upper()
        stage = get_stage_from_game_type(gt)
        rows.append(schedule_game_to_row(g, stage))

    df = pd.DataFrame(rows)
    df = df.sort_values(["date", "game_pk"])
    out_csv = year_dir / "schedule_post.csv"
    df.to_csv(out_csv, index=False)
    print(f"  {out_csv} ({len(df)} filas)")
    print(f"\n✅ Schedule {season} guardado en {year_dir}")


def find_raw_files(warehouse: Path, years: list[int]) -> list[tuple[Path, Path]]:
    """Retorna [(raw_path, pitches_enriched_dir), ...]"""
    results = []
    for year in years:
        base = warehouse / str(year)
        for raw_path in base.rglob("**/raw/*_feed_live.json"):
            rel = raw_path.relative_to(base)
            out_dir = base / rel.parent.parent / "pitches_enriched"
            results.append((raw_path, out_dir))
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Backfill MLB warehouse: raw + pitches_enriched por juego"
    )
    parser.add_argument("--warehouse", type=Path, default=Path("data/warehouse/mlb"))
    parser.add_argument("--years", nargs="+", type=int, default=[2024, 2025])
    parser.add_argument("--season", type=int, help="Temporada para fetch (schedule API)")
    parser.add_argument(
        "--game-type",
        default="R",
        help="gameType: R=regular, S=spring, A=all_star, F/D/L/W=playoffs",
    )
    parser.add_argument(
        "--all-stages",
        action="store_true",
        help="Obtener toda la temporada: S, R, A, F, D, L, W",
    )
    parser.add_argument(
        "--from-raw",
        action="store_true",
        help="Solo procesar raw existente (no fetch feed)",
    )
    parser.add_argument("--force", action="store_true", help="Sobrescribir aunque exista")
    parser.add_argument("--quiet", action="store_true", help="Sin barra de progreso")
    parser.add_argument(
        "--workers",
        type=int,
        default=3,
        help="Workers en paralelo para pitches_enriched (default: 3)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="Segundos de espera antes de cada llamada Statcast (default: 0.25)",
    )
    parser.add_argument(
        "--schedule-only",
        action="store_true",
        help="Solo descargar schedule(s), crear carpeta del año y guardar JSON + CSV para publicar",
    )
    args = parser.parse_args()

    if args.schedule_only:
        if not args.season:
            parser.error("--season requerido con --schedule-only")
        save_schedule_only(
            args.warehouse,
            args.season,
            args.game_type,
            args.all_stages,
        )
        return

    if args.from_raw:
        pairs = find_raw_files(args.warehouse, args.years)
        games_to_process = [(r, o) for r, o in pairs]
        print(f"Modo --from-raw: {len(games_to_process)} juegos con raw encontrados")
    else:
        if not args.season:
            parser.error("--season requerido cuando no usas --from-raw")
        if args.all_stages:
            games_by_pk: dict[int, dict] = {}
            for gt in ALL_STAGES_GAME_TYPES:
                batch = fetch_schedule(args.season, gt)
                for g in batch:
                    # No sobrescribir: preservar el gameType original para la carpeta correcta
                    if g["gamePk"] not in games_by_pk:
                        games_by_pk[g["gamePk"]] = g
                if batch:
                    print(f"  {gt}: {len(batch)} juegos")
            games = list(games_by_pk.values())
            print(f"Schedule: {len(games)} juegos únicos ({args.season}, all stages)")
        else:
            games = fetch_schedule(args.season, args.game_type)
            print(f"Schedule: {len(games)} juegos ({args.season}, type={args.game_type})")

        games_to_process = []
        desc_raw = "Raw (feed)"
        it_raw = tqdm(games, desc=desc_raw, unit="game", disable=args.quiet)
        for g in it_raw:
            raw_path, created = ensure_raw(args.warehouse, g, args.force)
            if raw_path is None:
                continue
            stage = get_stage_from_game_type(g.get("gameType"))
            season = str(g.get("season", args.season))
            out_dir = args.warehouse / season / stage / "pitches_enriched"
            games_to_process.append((raw_path, out_dir))

    # Filtrar: solo procesar los que no tienen enriched (o --force)
    to_process = []
    skipped = 0
    for raw_path, out_dir in games_to_process:
        name = raw_path.stem
        m = re.match(r"game_(\d+)_(\d+)_feed_live", name)
        if not m:
            continue
        game_pk, date = m.group(1), m.group(2)
        enriched_path = out_dir / f"game_{game_pk}_{date}_pitches_enriched.parquet"
        if enriched_path.exists() and not args.force:
            skipped += 1
        else:
            to_process.append((raw_path, out_dir))

    ok = 0
    workers = max(1, args.workers)
    desc = "pitches_enriched"
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(_process_one, r, o, args.delay): (r, o)
            for r, o in to_process
        }
        it = tqdm(
            as_completed(futures),
            total=len(futures),
            desc=desc,
            unit="game",
            disable=args.quiet,
        )
        for fut in it:
            try:
                if fut.result():
                    ok += 1
            except Exception:
                pass

    (tqdm.write if not args.quiet else print)(f"\n✅ Procesados {ok} | Omitidos (duplicados): {skipped}")


if __name__ == "__main__":
    main()
