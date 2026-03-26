"""
MLB Warehouse backfill: raw feed + Statcast → pitches_enriched.

Por cada juego genera:
  1. raw: game_{pk}_{date}_feed_live.json.gz (feed/live, minified + gzip; .json legacy soportado)
  2. opcional: {season}/players_registry.json (bios de gameData.players, deduplicadas)
  3. pitches_enriched: game_{pk}_{date}_pitches_enriched.parquet (join Statcast + play_id)

Evita duplicados: si raw y pitches_enriched existen, se salta (usa --force para sobrescribir).

Al ejecutar sin argumentos (botón Run): intenta backfill de días faltantes. Si hay datos
en el warehouse, descarga desde el día siguiente al último hasta ayer; si no hay datos,
descarga los últimos 7 días. Usa temporada = año calendario de hoy y game type R (regular).
Para spring training u otros tipos: --game-type S (etc.). Para cargas por fecha:
--last-days N, --dates YYYY-MM-DD ..., o --from-raw.
"""
import argparse
import gzip
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

# Defer pybaseball import: it can hang or raise (e.g. github circular import) in some envs.
# Import only when process_pitches_enriched runs.

try:
    from .mlb_warehouse_schema import (
        ALL_STAGES_GAME_TYPES,
        COLUMNS_TO_KEEP,
        FEED_LIVE_URL,
        SPORT_ID_MLB,
        BASE_URL,
        GAME_TYPE_TO_STAGE,
    )
    from .player_registry import merge_game_data_players_from_feed, season_registry_path
except ImportError:
    # Run as script (e.g. Run button): add project root and use absolute import
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    if str(_PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(_PROJECT_ROOT))
    from src.ingestion.mlb_warehouse_schema import (
        ALL_STAGES_GAME_TYPES,
        COLUMNS_TO_KEEP,
        FEED_LIVE_URL,
        SPORT_ID_MLB,
        BASE_URL,
        GAME_TYPE_TO_STAGE,
    )
    from src.ingestion.player_registry import merge_game_data_players_from_feed, season_registry_path

# Repo root: walk up from this file until we find a dir containing data/warehouse/mlb (so Run works from any cwd)
_here = Path(__file__).resolve().parent
_PROJECT_ROOT = _here.parent.parent
for _ in range(2):
    _wh = _PROJECT_ROOT / "data" / "warehouse" / "mlb"
    if _wh.exists() or (_PROJECT_ROOT / "data").exists():
        break
    _PROJECT_ROOT = _PROJECT_ROOT.parent
# When no CLI dates given, default Run fetches this many days (including yesterday) so you get missed days
DEFAULT_BACKFILL_DAYS = 7


def get_stage_from_game_type(game_type: str) -> str:
    """gameType → stage folder."""
    gt = (game_type or "").strip().upper()
    return GAME_TYPE_TO_STAGE.get(gt, "regular_season")


def is_game_final(g: dict) -> bool:
    """True si el juego ya terminó (no Scheduled/Preview). Evita descargar feeds de juegos futuros."""
    return (g.get("status") or {}).get("abstractGameState") == "Final"


def fetch_schedule(season: int, game_type: str) -> list[dict]:
    """Obtiene juegos del schedule API (toda la temporada para ese gameType)."""
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


def fetch_schedule_for_dates(
    season: int, game_type: str, dates: list[str]
) -> list[dict]:
    """Obtiene juegos solo para las fechas dadas (una llamada por fecha). Ideal para cargas diarias."""
    games_by_pk: dict[int, dict] = {}
    gt_upper = (game_type or "R").strip().upper()
    for date_str in dates:
        url = f"{BASE_URL}/schedule"
        params = {"sportId": SPORT_ID_MLB, "date": date_str}
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        for d in data.get("dates", []):
            for g in d.get("games", []):
                if (g.get("gameType") or "").strip().upper() != gt_upper:
                    continue
                try:
                    if int(g.get("season")) != int(season):
                        continue
                except (TypeError, ValueError):
                    continue
                if g["gamePk"] not in games_by_pk:
                    games_by_pk[g["gamePk"]] = g
    return list(games_by_pk.values())


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


RAW_BASENAME = "feed_live"
# New raws: gzip only. Legacy plain .json still read by _open_raw / find_raw_files.
RAW_SUFFIX_GZ = ".json.gz"


def _raw_stem(path: Path) -> str:
    """Base name without .json or .json.gz for feed_live files."""
    name = path.name
    if name.endswith(".json.gz"):
        return name[:-7]
    if name.endswith(".json"):
        return name[:-5]
    return path.stem


def _open_raw(path: Path):
    """Open raw file as text; supports .json and .json.gz."""
    if path.suffix == ".gz" or path.name.endswith(".json.gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def ensure_raw(warehouse: Path, game: dict, force: bool) -> tuple[Path | None, bool]:
    """
    Asegura que existe raw. Si no existe, lo obtiene de la API.
    Guarda como ``.json.gz`` (minified JSON, sin indent) — ~70–85% menos disco que .json suelto.
    Si ya existe ``.json`` legacy, se respeta y no se vuelve a descargar.
    Actualiza ``{season}/players_registry.json`` con ``gameData.players`` (bios deduplicadas).
    Returns (raw_path, was_created). None si falló.
    """
    game_pk = game["gamePk"]
    game_type = game.get("gameType", "R")
    official_date = game.get("officialDate", "").replace("-", "")
    season = game.get("season", "")

    stage = get_stage_from_game_type(game_type)
    raw_dir = warehouse / str(season) / stage / "raw"
    stem = f"game_{game_pk}_{official_date}_{RAW_BASENAME}"
    raw_path_gz = raw_dir / f"{stem}{RAW_SUFFIX_GZ}"
    raw_path_json = raw_dir / f"{stem}.json"

    if raw_path_gz.exists() and not force:
        return raw_path_gz, False
    if raw_path_json.exists() and not force:
        return raw_path_json, False

    try:
        feed = fetch_feed(game_pk)
    except Exception:
        return None, False

    raw_dir.mkdir(parents=True, exist_ok=True)
    with gzip.open(raw_path_gz, "wt", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False)

    try:
        reg_path = season_registry_path(warehouse, season)
        merge_game_data_players_from_feed(feed, reg_path)
    except Exception:
        pass

    return raw_path_gz, True


def _process_one(raw_path: Path, out_dir: Path, delay: float) -> bool:
    """Wrapper: delay + process_pitches_enriched (para workers)."""
    if delay > 0:
        time.sleep(delay)
    return process_pitches_enriched(raw_path, out_dir)


def process_pitches_enriched(raw_path: Path, out_dir: Path) -> bool:
    """
    Procesa un juego: raw → Statcast merge → pitches_enriched.parquet.
    Acepta raw como .json o .json.gz.
    """
    name = _raw_stem(raw_path)
    m = re.match(r"game_(\d+)_(\d+)_feed_live", name)
    if not m:
        return False
    game_pk = int(m.group(1))
    date = m.group(2)

    with _open_raw(raw_path) as f:
        feed = json.load(f)

    df_feed_ids = extract_play_ids_from_feed(feed, game_pk)
    if df_feed_ids.empty:
        return False

    try:
        from pybaseball import statcast_single_game
    except (ImportError, AttributeError) as e:
        if "github" in str(e).lower() or "GithubObject" in str(e):
            import sys
            print(
                "ERROR: pybaseball import failed (often due to PyGithub circular import). "
                "Use a different env or install/update PyGithub. For --from-raw only, no Statcast is needed.",
                file=sys.stderr,
                flush=True,
            )
        raise
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


def get_latest_game_date_in_warehouse(
    warehouse: Path, season: int, game_type: str
) -> date | None:
    """
    Scan warehouse for the latest game date that has pitches_enriched for the given
    season and game type. Returns that date or None if none found.
    """
    stage = get_stage_from_game_type(game_type)
    enriched_dir = warehouse / str(season) / stage / "pitches_enriched"
    if not enriched_dir.exists():
        return None
    max_d = None
    for path in enriched_dir.glob("game_*_*_pitches_enriched.parquet"):
        stem = path.stem  # game_{pk}_{date}_pitches_enriched
        parts = stem.split("_")
        if len(parts) >= 3:
            try:
                d = date(
                    int(parts[2][:4]),
                    int(parts[2][4:6]),
                    int(parts[2][6:8]),
                )
                if max_d is None or d > max_d:
                    max_d = d
            except (ValueError, IndexError):
                continue
    return max_d


def find_raw_files(warehouse: Path, years: list[int]) -> list[tuple[Path, Path]]:
    """Retorna [(raw_path, pitches_enriched_dir), ...]. Incluye .json y .json.gz; prefiere .gz."""
    by_key: dict[tuple[str, str], tuple[Path, Path]] = {}
    for year in years:
        base = warehouse / str(year)
        for raw_path in base.rglob("**/raw/*_feed_live.json*"):
            if not (raw_path.name.endswith(".json") or raw_path.name.endswith(".json.gz")):
                continue
            name = _raw_stem(raw_path)
            m = re.match(r"game_(\d+)_(\d+)_feed_live", name)
            if not m:
                continue
            key = (m.group(1), m.group(2))
            rel = raw_path.relative_to(base)
            out_dir = base / rel.parent.parent / "pitches_enriched"
            if key not in by_key or (
                raw_path.name.endswith(".json") and not raw_path.name.endswith(".json.gz")
            ):
                by_key[key] = (raw_path, out_dir)
    return list(by_key.values())


def main():
    # Show something immediately when Run from IDE (stdout may be buffered)
    print("MLB warehouse backfill ...", flush=True)
    parser = argparse.ArgumentParser(
        description="Backfill MLB warehouse: raw + pitches_enriched por juego"
    )
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=None,
        help="Carpeta warehouse (default: data/warehouse/mlb respecto al proyecto)",
    )
    parser.add_argument("--years", nargs="+", type=int, default=[2024, 2025])
    parser.add_argument("--season", type=int, help="Temporada para fetch (schedule API)")
    parser.add_argument(
        "--game-type",
        default="R",
        help="gameType: R=regular (default), S=spring, A=all_star, F/D/L/W=playoffs",
    )
    parser.add_argument(
        "--dates",
        nargs="+",
        metavar="YYYY-MM-DD",
        help="Solo estos días (ej: --dates 2026-03-21 2026-03-22). Eficiente para cargas diarias.",
    )
    parser.add_argument(
        "--last-days",
        type=int,
        metavar="N",
        help="Solo los últimos N días hasta ayer (ej: --last-days 1 = ayer). Requiere --season.",
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
    parser.add_argument(
        "--refresh-schedule",
        action="store_true",
        help=(
            "After ingest (not with --from-raw): rewrite schedule_{stage}.json and schedule_post.csv "
            "for this season and --game-type using the full season schedule API (e.g. regular season → "
            "schedule_regular_season.json + flat CSV for posts)."
        ),
    )
    parser.add_argument(
        "--max-games",
        type=int,
        default=None,
        metavar="N",
        help="Máximo de juegos a procesar en pitches_enriched (útil para no esperar 95 juegos de una vez)",
    )
    args = parser.parse_args()
    args.game_type = (args.game_type or "R").strip().upper()

    # Resolve warehouse so "Run" works from any cwd
    if args.warehouse is None:
        args.warehouse = _PROJECT_ROOT / "data" / "warehouse" / "mlb"
    elif not args.warehouse.is_absolute():
        args.warehouse = _PROJECT_ROOT / args.warehouse

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
        # Default run (no CLI dates): always fetch last N days so "Run" gets missed games without CLI
        default_run = not (args.dates or args.last_days is not None or args.all_stages)
        if default_run:
            print(f"Warehouse: {args.warehouse.resolve()}")
            today = date.today()
            yesterday = today - timedelta(days=1)
            if args.season is None:
                args.season = today.year
            stage = get_stage_from_game_type(args.game_type)
            # Optionally backfill from latest warehouse date; otherwise use last N days
            latest = get_latest_game_date_in_warehouse(
                args.warehouse, args.season, args.game_type
            )
            if latest is not None:
                start = latest + timedelta(days=1)
                if start <= yesterday:
                    dates_list = [
                        (start + timedelta(days=k)).strftime("%Y-%m-%d")
                        for k in range((yesterday - start).days + 1)
                    ]
                    if len(dates_list) > 14:
                        dates_list = dates_list[-14:]
                    args.dates = dates_list
                    print(
                        f"Backfill: {len(dates_list)} día(s) ({args.dates[0]} a {args.dates[-1]}), "
                        f"temporada {args.season}, stage={stage} (gameType={args.game_type})"
                    )
                else:
                    # Caught up: still use last N days so any missed day is included
                    n_days = DEFAULT_BACKFILL_DAYS
                    args.dates = [
                        (yesterday - timedelta(days=k)).strftime("%Y-%m-%d")
                        for k in range(n_days - 1, -1, -1)
                    ]
                    print(
                        f"Run por defecto: últimos {len(args.dates)} día(s) ({args.dates[0]} a {args.dates[-1]}), "
                        f"temporada {args.season}, stage={stage} (gameType={args.game_type})"
                    )
            else:
                n_days = DEFAULT_BACKFILL_DAYS
                args.dates = [
                    (yesterday - timedelta(days=k)).strftime("%Y-%m-%d")
                    for k in range(n_days - 1, -1, -1)
                ]
                print(
                    f"Run por defecto: últimos {len(args.dates)} día(s) ({args.dates[0]} a {args.dates[-1]}), "
                    f"warehouse={args.warehouse}, temporada {args.season}, stage={stage} (gameType={args.game_type})"
                )
        elif args.season is None:
            parser.error("--season requerido cuando no usas --from-raw (o usa sin argumentos para ayer + stage automático)")
        if args.dates or args.last_days is not None:
            # Carga por fecha(s): solo esos días (eficiente para diario)
            if args.last_days is not None:
                today = date.today()
                dates = [
                    (today - timedelta(days=k)).strftime("%Y-%m-%d")
                    for k in range(1, args.last_days + 1)
                ]
            else:
                dates = args.dates
            games = fetch_schedule_for_dates(args.season, args.game_type, dates)
            print(
                f"Schedule: {len(games)} juegos ({args.season}, type={args.game_type}, {len(dates)} día(s))"
            )
        elif args.all_stages:
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

        n_total = len(games)
        games = [g for g in games if is_game_final(g)]
        if n_total > len(games):
            (tqdm.write if not args.quiet else print)(
                f"  Solo juegos Final: {len(games)} (omitidos {n_total - len(games)} programados/futuros)"
            )

        # --max-games limits both raw fetch and pitches_enriched (smoke tests / partial runs)
        if args.max_games is not None and args.max_games > 0:
            games = games[: args.max_games]
            if not args.quiet:
                print(
                    f"  --max-games={args.max_games}: solo {len(games)} juego(s) Final",
                    flush=True,
                )

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
        name = _raw_stem(raw_path)
        m = re.match(r"game_(\d+)_(\d+)_feed_live", name)
        if not m:
            continue
        game_pk, game_date = m.group(1), m.group(2)
        enriched_path = out_dir / f"game_{game_pk}_{game_date}_pitches_enriched.parquet"
        if enriched_path.exists() and not args.force:
            skipped += 1
        else:
            to_process.append((raw_path, out_dir))

    if not to_process:
        (tqdm.write if not args.quiet else print)(
            f"\n✅ Nada que procesar. Omitidos (ya tienen parquet): {skipped}"
        )
        if (
            args.refresh_schedule
            and not args.from_raw
            and args.season is not None
        ):
            if not args.quiet:
                print(
                    "\nRefreshing schedule artifacts (full season for this game-type) ...",
                    flush=True,
                )
            save_schedule_only(
                args.warehouse, args.season, args.game_type, all_stages=False
            )
        return

    n_to_process = len(to_process)
    if args.max_games is not None and args.max_games > 0:
        to_process = to_process[: args.max_games]
        (tqdm.write if not args.quiet else print)(
            f"\n  Procesando {len(to_process)} de {n_to_process} juegos (--max-games={args.max_games})"
        )
    (tqdm.write if not args.quiet else print)(
        f"\n  Pitches_enriched: {len(to_process)} juegos (Statcast puede tardar ~15–60 s por juego). Omitidos: {skipped}"
    )

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

    if (
        args.refresh_schedule
        and not args.from_raw
        and args.season is not None
    ):
        if not args.quiet:
            print(
                "\nRefreshing schedule artifacts (full season for this game-type) ...",
                flush=True,
            )
        save_schedule_only(
            args.warehouse, args.season, args.game_type, all_stages=False
        )


if __name__ == "__main__":
    sys.exit(main() or 0)
