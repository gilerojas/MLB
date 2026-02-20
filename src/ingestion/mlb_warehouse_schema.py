"""
MLB Warehouse schema and constants.

Stage mapping from gameType, Statcast column selection for pitches_enriched.
"""

# MLB Stats API
SPORT_ID_MLB = 1
BASE_URL = "https://statsapi.mlb.com/api/v1"
FEED_LIVE_URL = "https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"

# gameTypes para --all-stages (temporada completa)
# S=spring, R=regular, A=all_star | F=wild_card, D=division, L=championship, W=world_series
# C y P devuelven 0 en 2025; usar scripts/audit_stages.py para validar otras temporadas
ALL_STAGES_GAME_TYPES = ["S", "R", "A", "F", "D", "L", "W"]

# gameType → warehouse stage path (relative to season)
GAME_TYPE_TO_STAGE = {
    "S": "spring_training",
    "R": "regular_season",
    "A": "all_star",
    "P": "playoffs",
    "F": "playoffs/wild_card",
    "D": "playoffs/division",
    "L": "playoffs/championship",
    "W": "playoffs/world_series",
    "C": "playoffs/championship",
}


def get_stage_from_game(game: dict) -> str:
    """Derive warehouse stage from game dict (schedule API)."""
    gt = (game.get("gameType") or "").strip().upper()
    return GAME_TYPE_TO_STAGE.get(gt, "regular_season")


# pitches_enriched: columnas a mantener (COLUMNS_AUDIT)
COLUMNS_TO_KEEP = [
    "game_pk", "game_date", "game_type", "game_year",
    "pitcher", "batter", "player_name", "inning", "inning_topbot",
    "play_id",  # del feed (trazabilidad al raw)
    "at_bat_number", "pitch_number", "stand", "p_throws",
    "home_team", "away_team",
    "pitch_type", "pitch_name",
    "release_speed", "release_spin_rate",
    "release_extension", "release_pos_x", "release_pos_y", "release_pos_z",
    "pfx_x", "pfx_z", "spin_axis",
    "plate_x", "plate_z", "zone",
    "type", "description", "events", "des", "bb_type",
    "balls", "strikes",
    "launch_speed", "launch_angle", "launch_speed_angle",
    "hit_distance_sc", "hit_location", "hc_x", "hc_y",
    "estimated_woba_using_speedangle", "estimated_ba_using_speedangle",
    "estimated_slg_using_speedangle", "woba_value", "woba_denom",
    "home_score", "away_score", "home_score_diff",
    "outs_when_up", "on_1b", "on_2b", "on_3b",
    "n_thruorder_pitcher",
    "delta_home_win_exp", "delta_run_exp", "delta_pitcher_run_exp",
    "home_win_exp", "bat_win_exp", "babip_value",
    "effective_speed", "sz_top", "sz_bot",
    "if_fielding_alignment", "of_fielding_alignment",
    "age_pit", "age_bat", "arm_angle",
    "bat_speed", "swing_length", "attack_angle", "attack_direction", "hyper_speed",
]
