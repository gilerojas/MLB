#!/usr/bin/env python3
"""
Audit de gameTypes y stages: qué devuelve la API MLB y cómo mapeamos a carpetas.

Ejecutar antes del backfill completo para validar:
  - Cuántos juegos devuelve cada gameType
  - Si hay superposición (mismo game_pk en varios tipos)
  - Qué gameType trae cada juego (del cuerpo de la respuesta)
  - Carpeta destino por tipo

Uso:
  python scripts/audit_stages.py --season 2025
  python scripts/audit_stages.py --season 2024  # temporada pasada completa
"""
import argparse
import sys
from collections import defaultdict
from pathlib import Path

# Permitir import desde raíz del proyecto
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests

from src.ingestion.mlb_warehouse_schema import (
    ALL_STAGES_GAME_TYPES,
    BASE_URL,
    GAME_TYPE_TO_STAGE,
    SPORT_ID_MLB,
)

GAME_TYPE_LABELS = {
    "S": "Spring Training",
    "R": "Regular Season",
    "A": "All-Star",
    "P": "Playoffs (generic)",
    "F": "Wild Card",
    "D": "Division Series",
    "L": "League Championship (LCS)",
    "C": "Championship",
    "W": "World Series",
}


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


def main():
    parser = argparse.ArgumentParser(description="Auditar gameTypes y stages antes del backfill")
    parser.add_argument("--season", type=int, default=2025)
    args = parser.parse_args()

    # 1. Fetch por tipo
    games_by_type: dict[str, list[dict]] = {}
    for gt in ALL_STAGES_GAME_TYPES:
        games = fetch_schedule(args.season, gt)
        games_by_type[gt] = games

    # 2. game_pk → set de gameTypes (sin duplicar si el mismo juego viene 2x en un tipo)
    pk_to_types: dict[int, set[str]] = defaultdict(set)
    pk_to_game: dict[int, dict] = {}
    for gt, games in games_by_type.items():
        seen_pk = set()
        for g in games:
            pk = g["gamePk"]
            pk_to_types[pk].add(gt)
            if pk not in pk_to_game:
                pk_to_game[pk] = g
            seen_pk.add(pk)

    # 3. Superposiciones: mismo game_pk en DIFERENTES gameTypes
    overlaps = {pk: sorted(types) for pk, types in pk_to_types.items() if len(types) > 1}

    # 4. Reporte
    print("=" * 70)
    print(f"AUDIT STAGES - Season {args.season}")
    print("=" * 70)

    print("\n--- Juegos por gameType (parámetro API) ---")
    print(f"{'Type':<6} {'Label':<30} {'Count':>6}  Stage (carpeta)")
    print("-" * 70)
    for gt in ALL_STAGES_GAME_TYPES:
        count = len(games_by_type[gt])
        stage = GAME_TYPE_TO_STAGE.get(gt, "?")
        label = GAME_TYPE_LABELS.get(gt, "?")
        print(f"  {gt:<4} {label:<30} {count:>6}  {stage}")

    print(f"\n--- Total único (por game_pk): {len(pk_to_game)} juegos ---")

    if overlaps:
        print(f"\n⚠️  SUPERPOSICIONES: {len(overlaps)} juegos aparecen en >1 gameType")
        for pk, types in list(overlaps.items())[:15]:
            g = pk_to_game[pk]
            gt_in_body = g.get("gameType", "?")
            date = g.get("officialDate", "?")
            print(f"   game_pk={pk}  date={date}  gameType en body={gt_in_body}  ← en: {types}")
        if len(overlaps) > 15:
            print(f"   ... y {len(overlaps) - 15} más")
    else:
        print("\n✅ Sin superposiciones entre tipos: cada game_pk en un solo gameType")

    # 5. gameType que trae la API en el cuerpo vs parámetro
    print("\n--- gameType en cuerpo de respuesta (primer juego por tipo) ---")
    for gt in ALL_STAGES_GAME_TYPES:
        games = games_by_type[gt]
        if games:
            g = games[0]
            body_gt = g.get("gameType", "N/A")
            pk = g["gamePk"]
            date = g.get("officialDate", "?")
            match = "✓" if body_gt == gt else "⚠️ distinto"
            print(f"  Parám {gt} → body gameType={body_gt}  (game_{pk}, {date})  {match}")

    # 6. Stages que comparten tipos
    print("\n--- Stages que comparten gameTypes ---")
    stage_to_types: dict[str, list[str]] = defaultdict(list)
    for gt, stage in GAME_TYPE_TO_STAGE.items():
        stage_to_types[stage].append(gt)
    for stage, types in sorted(stage_to_types.items()):
        if len(types) > 1:
            used = [t for t in types if t in ALL_STAGES_GAME_TYPES]
            with_games = [t for t in used if games_by_type.get(t)]
            print(f"  {stage}: {used}  (con juegos: {with_games})")

    # 7. Recomendación: tipos que devuelven 0
    empty = [gt for gt in ALL_STAGES_GAME_TYPES if not games_by_type[gt]]
    if empty:
        print(f"\n📋 Tipos con 0 juegos en {args.season}: {empty} (opcional eliminarlos de ALL_STAGES)")
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
