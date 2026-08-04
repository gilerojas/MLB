"""Daily pitching performances table card."""

from .malli_score import MALLISCORE_V4_VERSION, malliscore_v2, malliscore_v4

__all__ = [
    "MALLISCORE_V4_VERSION",
    "build_pitching_performance_rows",
    "malliscore_v2",
    "malliscore_v4",
    "render_pitching_performance_table",
]


def __getattr__(name: str):
    if name in {"build_pitching_performance_rows", "render_pitching_performance_table"}:
        from .render import build_pitching_performance_rows, render_pitching_performance_table

        return {
            "build_pitching_performance_rows": build_pitching_performance_rows,
            "render_pitching_performance_table": render_pitching_performance_table,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
