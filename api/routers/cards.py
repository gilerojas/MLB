"""
Card generation endpoints — wrap existing Python scripts via subprocess.

POST /cards/batter   → runs batter_card_daily.py
POST /cards/pitcher  → runs mallitalytics_daily_card.py
POST /cards/hr-tracker → runs hr_tracker_daily.py
"""
import re
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from api.db.database import insert_queue_item

REPO_ROOT = Path(__file__).parent.parent.parent
OUTPUTS_ROOT = REPO_ROOT / "outputs"
MLB_PYTHON = str(REPO_ROOT / "mlb_env" / "bin" / "python")
STATIC_BASE = "http://localhost:8000/static"

router = APIRouter(prefix="/cards", tags=["cards"])


def _run_script(cmd: list[str]) -> tuple[str, str]:
    """Run a card script and return (stdout, stderr). Raises HTTPException on failure."""
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )
    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=f"Script failed:\n{result.stderr[-2000:]}",
        )
    return result.stdout, result.stderr


def _extract_saved_path(stdout: str) -> Optional[Path]:
    """Parse '→ Saved: /path/to/file.png' from script stdout."""
    for line in stdout.splitlines():
        # Match both arrow styles the scripts use
        m = re.search(r"(?:→|->)\s*Saved:\s*(.+\.png)", line)
        if m:
            return Path(m.group(1).strip())
    return None


def _image_url(abs_path: Path) -> str:
    rel = abs_path.relative_to(OUTPUTS_ROOT)
    return f"{STATIC_BASE}/{rel}"


def _game_date_from_feed(feed_path: str) -> str:
    """Extract YYYYMMDD from feed filename and convert to YYYY-MM-DD."""
    m = re.search(r"_(\d{8})_feed", feed_path)
    if m:
        d = m.group(1)
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return str(date.today())


def _season_from_feed(feed_path: str) -> int:
    m = re.search(r"/(\d{4})/", feed_path)
    return int(m.group(1)) if m else date.today().year


def _stage_from_feed(feed_path: str) -> str:
    for stage in ("regular_season", "spring_training", "playoffs", "all_star"):
        if stage in feed_path:
            return stage
    return "regular_season"


def _game_pk_from_feed(feed_path: str) -> Optional[int]:
    m = re.search(r"game_(\d+)_", feed_path)
    return int(m.group(1)) if m else None


# ──────────────────────────────────────────────────────────
# Request models
# ──────────────────────────────────────────────────────────

class BatterCardRequest(BaseModel):
    player_id: int
    feed_path: str
    dark: bool = False
    tweet_text: Optional[str] = None


class PitcherCardRequest(BaseModel):
    player_id: int
    game_date: str          # YYYY-MM-DD  (or "yesterday")
    dark: bool = False
    tweet_text: Optional[str] = None
    parquet_path: Optional[str] = None  # explicit parquet override


class HRTrackerRequest(BaseModel):
    game_date: Optional[str] = None    # YYYY-MM-DD, defaults to yesterday
    tweet_text: Optional[str] = None


# ──────────────────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────────────────

@router.post("/batter")
def generate_batter_card(req: BatterCardRequest):
    cmd = [
        MLB_PYTHON, "scripts/batter_card_daily.py",
        "--batter", str(req.player_id),
        "--feed", req.feed_path,
    ]
    if req.dark:
        cmd.append("--dark")

    stdout, _ = _run_script(cmd)
    out_path = _extract_saved_path(stdout)
    if not out_path or not out_path.exists():
        raise HTTPException(status_code=500, detail="Card PNG not found after generation.")

    game_date = _game_date_from_feed(req.feed_path)
    game_pk = _game_pk_from_feed(req.feed_path)
    season = _season_from_feed(req.feed_path)
    stage = _stage_from_feed(req.feed_path)

    # Build default tweet text from filename if not provided
    tweet = req.tweet_text or f"🦇 Batter card — player {req.player_id} | {game_date} #Mallitalytics"

    item_id = insert_queue_item(
        content_type="batter_card",
        title=out_path.stem,
        tweet_text=tweet,
        image_path=str(out_path),
        image_url=_image_url(out_path),
        game_date=game_date,
        season=season,
        stage=stage,
        game_pk=game_pk,
        player_id=req.player_id,
    )

    return {"id": item_id, "image_url": _image_url(out_path), "tweet_text": tweet, "image_path": str(out_path)}


@router.post("/pitcher")
def generate_pitcher_card(req: PitcherCardRequest):
    cmd = [
        MLB_PYTHON, "scripts/mallitalytics_daily_card.py",
        "--pitchers", str(req.player_id),
        "--date", req.game_date,
    ]
    if req.dark:
        cmd.append("--dark")
    if req.parquet_path:
        cmd += ["--parquet", req.parquet_path, "--pitcher", str(req.player_id)]

    stdout, _ = _run_script(cmd)
    out_path = _extract_saved_path(stdout)
    if not out_path or not out_path.exists():
        raise HTTPException(status_code=500, detail="Pitcher card PNG not found after generation.")

    game_date = req.game_date if req.game_date != "yesterday" else str(date.today() - timedelta(days=1))
    tweet = req.tweet_text or f"⚾ Pitcher card — player {req.player_id} | {game_date} #Mallitalytics"

    item_id = insert_queue_item(
        content_type="pitcher_card",
        title=out_path.stem,
        tweet_text=tweet,
        image_path=str(out_path),
        image_url=_image_url(out_path),
        game_date=game_date,
        season=int(game_date[:4]),
        stage="regular_season",
        player_id=req.player_id,
    )

    return {"id": item_id, "image_url": _image_url(out_path), "tweet_text": tweet, "image_path": str(out_path)}


@router.post("/hr-tracker")
def generate_hr_tracker(req: HRTrackerRequest):
    game_date = req.game_date or str(date.today() - timedelta(days=1))
    cmd = [
        MLB_PYTHON, "scripts/hr_tracker_daily.py",
        "--date", game_date,
        "--format", "all",
        "--output-dir", str(OUTPUTS_ROOT),
    ]

    stdout, _ = _run_script(cmd)
    out_path = _extract_saved_path(stdout)
    if not out_path or not out_path.exists():
        raise HTTPException(status_code=500, detail="HR tracker PNG not found after generation.")

    # Extract tweet text from stdout (hr_tracker prints it before the image line)
    tweet_lines = []
    for line in stdout.splitlines():
        if line.strip() and not line.startswith(" ") and "→" not in line and "->" not in line:
            tweet_lines.append(line)
    tweet = req.tweet_text or "\n".join(tweet_lines[:10]) or f"💥 Home Runs — {game_date} #Mallitalytics"

    item_id = insert_queue_item(
        content_type="hr_tracker",
        title=f"HR Tracker {game_date}",
        tweet_text=tweet[:280],
        image_path=str(out_path),
        image_url=_image_url(out_path),
        game_date=game_date,
        season=int(game_date[:4]),
        stage="regular_season",
    )

    return {"id": item_id, "image_url": _image_url(out_path), "tweet_text": tweet[:280], "image_path": str(out_path)}


@router.get("/{item_id}/preview")
def get_card_preview(item_id: int):
    from api.db.database import get_queue_item
    item = get_queue_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Queue item not found.")
    return {
        "id": item["id"],
        "image_url": item["image_url"],
        "tweet_text": item["tweet_text"],
        "status": item["status"],
        "title": item["title"],
        "player_name": item["player_name"],
        "game_date": item["game_date"],
    }
