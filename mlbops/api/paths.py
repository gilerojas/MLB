"""
Resolved paths for mlbops / FastAPI.

Google Drive is the source of truth: use rclone (see scripts/pull_mlbops_from_drive.sh
and .github/workflows) to sync into a *local mirror*. By default that mirror lives at:

  {MLB repo root}/data/warehouse/mlb

Override with env vars if your mirror lives elsewhere (e.g. another disk or Drive File Stream).

| Variable | Purpose |
|----------|---------|
| MLB_REPO_ROOT | Root of the MLB git repo (parent of data/, jobs/, morning_intel/) |
| MLB_WAREHOUSE_DIR | Local **cache** of warehouse parquets (sync target for Drive `MLB/warehouse/mlb`) |
| MLB_INTEL_SNAPSHOTS_DIR | Local **cache** of intel JSON (sync target for `MLB/morning_intel/snapshots`) |
| MLBOPS_TWEET_MAX_CHARS | Max length for queue tweet text / card defaults (default 10000; X Premium long posts) |
| MLBOPS_REDRAFT_META_MAX_CHARS | Max JSON chars passed to Claude/Grok as queue redraft context (default 16000) |
| MLBOPS_REDRAFT_PITCHER_TWEET_MIN | Preferred min chars for pitcher_card redrafts (default 220) |
| MLBOPS_REDRAFT_PITCHER_TWEET_MAX | Preferred max chars for pitcher_card redrafts (default 320) |
| MLBOPS_REDRAFT_BATTER_TWEET_MIN | Preferred min chars for batter_card redrafts (default 220) |
| MLBOPS_REDRAFT_BATTER_TWEET_MAX | Preferred max chars for batter_card redrafts (default 320) |
| MLBOPS_REDRAFT_MAX_TOKENS | LLM max_tokens for queue redraft (default 320; enough for ~320-char 3-para posts without cutoff) |
"""
from __future__ import annotations

import os
from pathlib import Path


def get_repo_root() -> Path:
    env = os.environ.get("MLB_REPO_ROOT", "").strip()
    if env:
        return Path(env).expanduser().resolve()
    return Path(__file__).resolve().parents[2]


def _env_warehouse_is_doc_placeholder(raw: str) -> bool:
    """True when MLB_WAREHOUSE_DIR is still a copy-paste from .env.example (not a real path)."""
    s = raw.replace("\\", "/").strip().lower()
    if not s:
        return False
    if "path/to/your" in s:
        return True
    if "path/to/local" in s and "mirror" in s:
        return True
    return False


def get_warehouse_dir() -> Path:
    env = os.environ.get("MLB_WAREHOUSE_DIR", "").strip()
    if env and not _env_warehouse_is_doc_placeholder(env):
        return Path(env).expanduser().resolve()
    return get_repo_root() / "data" / "warehouse" / "mlb"


def safe_is_dir(path: Path) -> bool:
    """
    True if path exists and is a directory.

    Google Drive File Stream (and similar) can raise TimeoutError or OSError (e.g. ETIMEDOUT)
    from pathlib.stat(); treat those as not usable so FastAPI routes do not 500.
    """
    try:
        return path.is_dir()
    except (OSError, TimeoutError):
        return False


def safe_non_empty_file(path: Path) -> bool:
    """True if path is a regular file with size > 0. False on missing path or cloud fs stat timeout."""
    try:
        return path.is_file() and path.stat().st_size > 0
    except (OSError, TimeoutError):
        return False


def get_intel_snapshots_dir() -> Path:
    env = os.environ.get("MLB_INTEL_SNAPSHOTS_DIR", "").strip()
    if env:
        return Path(env).expanduser().resolve()
    return get_repo_root() / "morning_intel" / "snapshots"


def get_outputs_dir() -> Path:
    return get_repo_root() / "outputs"


def get_tweet_max_chars() -> int:
    """Max tweet length for queue / card defaults (X Premium / long posts). Env: MLBOPS_TWEET_MAX_CHARS."""
    try:
        raw = os.environ.get("MLBOPS_TWEET_MAX_CHARS", "10000").strip() or "10000"
        n = int(raw)
        return max(1, min(250_000, n))
    except ValueError:
        return 10_000


def truncate_tweet_text_to_cap(text: str, cap: int | None = None) -> str:
    """
    Enforce max length without slicing mid-word when possible (avoids tails like "... sta" or "... Ho").
    If cap is None, uses get_tweet_max_chars().
    """
    if cap is None:
        cap = get_tweet_max_chars()
    if cap <= 0:
        return ""
    s = text or ""
    if len(s) <= cap:
        return s
    chunk = s[:cap]
    if chunk and chunk[-1].isspace():
        return chunk.rstrip()
    sp = chunk.rfind(" ")
    # Keep at least ~60% of cap so we don't over-trim short caps
    if sp > 0 and sp >= int(cap * 0.55):
        chunk = chunk[:sp]
    return chunk.rstrip(" ,;:-—")


def get_redraft_meta_max_chars() -> int:
    """Max characters of JSON context in queue redraft prompts (not tweet length). Env: MLBOPS_REDRAFT_META_MAX_CHARS."""
    try:
        raw = os.environ.get("MLBOPS_REDRAFT_META_MAX_CHARS", "16000").strip() or "16000"
        n = int(raw)
        return max(500, min(100_000, n))
    except ValueError:
        return 16_000


def get_redraft_pitcher_tweet_target_range() -> tuple[int, int]:
    """
    Preferred character band for pitcher_card redrafts (short-first X posts).
    Env: MLBOPS_REDRAFT_PITCHER_TWEET_MIN, MLBOPS_REDRAFT_PITCHER_TWEET_MAX.
    """
    try:
        mn = int(os.environ.get("MLBOPS_REDRAFT_PITCHER_TWEET_MIN", "220").strip() or "220")
        mx = int(os.environ.get("MLBOPS_REDRAFT_PITCHER_TWEET_MAX", "320").strip() or "320")
        mn = max(120, min(500, mn))
        mx = max(mn, min(600, mx))
        return mn, mx
    except ValueError:
        return 220, 320


def get_redraft_batter_tweet_target_range() -> tuple[int, int]:
    """
    Preferred character band for batter_card redrafts (same short-first contract as pitchers).
    Env: MLBOPS_REDRAFT_BATTER_TWEET_MIN, MLBOPS_REDRAFT_BATTER_TWEET_MAX.
    """
    try:
        mn = int(os.environ.get("MLBOPS_REDRAFT_BATTER_TWEET_MIN", "220").strip() or "220")
        mx = int(os.environ.get("MLBOPS_REDRAFT_BATTER_TWEET_MAX", "320").strip() or "320")
        mn = max(120, min(500, mn))
        mx = max(mn, min(600, mx))
        return mn, mx
    except ValueError:
        return 220, 320


def get_redraft_max_tokens() -> int:
    """Max completion tokens for queue redraft (Claude and Grok). Env: MLBOPS_REDRAFT_MAX_TOKENS."""
    try:
        n = int(os.environ.get("MLBOPS_REDRAFT_MAX_TOKENS", "320").strip() or "320")
        return max(80, min(512, n))
    except ValueError:
        return 320
