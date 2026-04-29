"""Games of the day: schedule rows + branded slate image."""

from .fetch import build_game_rows_for_date
from .render import render_games_of_day_board

__all__ = ["build_game_rows_for_date", "render_games_of_day_board"]
