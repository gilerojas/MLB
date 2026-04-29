"""
Intel snapshots from morning_intel/snapshots/intel_YYYY-MM-DD.json

GET /intel/snapshots       — list recent snapshot anchors
GET /intel/snapshots/{anchor} — full JSON for one date
POST /intel/run          — spawn morning_intel/morning_intel.py (gated by MLBOPS_ALLOW_INTEL_RUN)
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from datetime import date, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Optional, cast

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from api.intel_standouts import WindowId, compute_daily_standouts
from api.paths import get_intel_snapshots_dir, get_repo_root, safe_is_dir

REPO_ROOT = get_repo_root()
INTEL_SNAPSHOT_DIR = get_intel_snapshots_dir()
ANCHOR_RE = re.compile(r"^intel_(\d{4}-\d{2}-\d{2})\.json$")

router = APIRouter(prefix="/intel", tags=["intel"])


class StandoutWindow(str, Enum):
    yesterday = "yesterday"
    d7 = "7d"
    d14 = "14d"
    month = "month"


def intel_run_allowed() -> bool:
    v = os.environ.get("MLBOPS_ALLOW_INTEL_RUN", "").strip().lower()
    return v in ("1", "true", "yes", "on")


class IntelRunRequest(BaseModel):
    """Hub-triggered run defaults skip side effects (notify / Claude / card queue)."""

    dry_run: bool = False
    skip_notify: bool = Field(default=True, description="Skip Twilio/WhatsApp etc.")
    skip_claude: bool = Field(default=True, description="Skip tweet draft generation")
    skip_cards: bool = Field(default=True, description="Skip card script queue inserts")


def _tail(text: str, max_chars: int = 12000) -> str:
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    return "…" + text[-max_chars:]


def _parse_anchor(name: str) -> Optional[str]:
    m = ANCHOR_RE.match(name)
    return m.group(1) if m else None


def _list_snapshot_files() -> list[tuple[str, Path]]:
    if not safe_is_dir(INTEL_SNAPSHOT_DIR):
        return []
    out: list[tuple[str, Path]] = []
    for p in INTEL_SNAPSHOT_DIR.iterdir():
        if not p.is_file():
            continue
        anchor = _parse_anchor(p.name)
        if anchor:
            out.append((anchor, p))
    out.sort(key=lambda x: x[0], reverse=True)
    return out


def _list_snapshots_sync(days: int, include_body: bool, limit: Optional[int]) -> dict[str, Any]:
    """Disk + JSON parse for include_body — must not run on the asyncio event loop."""
    files = _list_snapshot_files()
    cutoff = date.today() - timedelta(days=days)
    items: list[dict[str, Any]] = []
    for anchor_str, path in files:
        try:
            ad = date.fromisoformat(anchor_str)
        except ValueError:
            continue
        if ad < cutoff:
            continue
        entry: dict[str, Any] = {
            "anchor": anchor_str,
            "path": str(path.relative_to(REPO_ROOT)),
        }
        if include_body:
            try:
                entry["data"] = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as e:
                entry["error"] = str(e)
        else:
            try:
                st = path.stat()
                entry["size_bytes"] = st.st_size
                entry["modified_at"] = int(st.st_mtime)
            except OSError:
                pass
        items.append(entry)
    if limit is not None:
        items = items[:limit]
    return {"snapshots": items, "count": len(items)}


@router.get("/snapshots")
async def list_snapshots(
    days: int = Query(7, ge=1, le=365, description="How many recent calendar days to include (anchor date >= today − days)"),
    limit: Optional[int] = Query(
        None,
        ge=1,
        le=90,
        description="Return at most this many snapshots, newest first (after date filter). Hub uses 1 for latest-only feed.",
    ),
    include_body: bool = Query(False, description="If true, embed full JSON per snapshot"),
):
    return await run_in_threadpool(_list_snapshots_sync, days, include_body, limit)


def _run_morning_intel_sync(req: IntelRunRequest) -> dict[str, Any]:
    if not intel_run_allowed():
        raise HTTPException(
            status_code=403,
            detail="Hub intel runs are disabled. Set MLBOPS_ALLOW_INTEL_RUN=1 on the API process.",
        )
    repo = get_repo_root()
    py = repo / "mlb_env" / "bin" / "python"
    if not py.is_file():
        py = Path(sys.executable)
    script = repo / "morning_intel" / "morning_intel.py"
    if not script.is_file():
        raise HTTPException(
            status_code=500,
            detail=f"morning_intel/morning_intel.py not found under repo root {repo}",
        )
    cmd: list[str] = [str(py), str(script)]
    if req.dry_run:
        cmd.append("--dry-run")
    if req.skip_notify:
        cmd.append("--skip-notify")
    if req.skip_claude:
        cmd.append("--skip-claude")
    if req.skip_cards:
        cmd.append("--skip-cards")
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    t0 = time.monotonic()
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(repo),
            capture_output=True,
            text=True,
            timeout=900,
            env=env,
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Morning intel subprocess exceeded 15 minute timeout.")
    elapsed = round(time.monotonic() - t0, 2)
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "duration_sec": elapsed,
        "stdout_tail": _tail(proc.stdout or ""),
        "stderr_tail": _tail(proc.stderr or ""),
        "command": cmd,
    }


@router.post("/run")
async def run_morning_intel(req: IntelRunRequest = IntelRunRequest()) -> dict[str, Any]:
    """Execute morning_intel.py on the server. Disabled unless MLBOPS_ALLOW_INTEL_RUN is set."""
    return await run_in_threadpool(_run_morning_intel_sync, req)


def _get_snapshot_sync(anchor: str) -> Any:
    try:
        date.fromisoformat(anchor)
    except ValueError:
        raise HTTPException(status_code=400, detail="anchor must be YYYY-MM-DD")
    path = INTEL_SNAPSHOT_DIR / f"intel_{anchor}.json"
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Snapshot not found.")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"Invalid JSON: {e}")


@router.get("/snapshots/{anchor}")
async def get_snapshot(anchor: str):
    return await run_in_threadpool(_get_snapshot_sync, anchor)


def _daily_standouts_sync(window_val: str, limit: int) -> dict[str, Any]:
    return compute_daily_standouts(window=cast(WindowId, window_val), limit=limit)


@router.get("/daily-standouts")
async def daily_standouts(
    window: StandoutWindow = Query(
        StandoutWindow.yesterday,
        description="yesterday | 7d | 14d | month (month = last 30 days)",
    ),
    limit: int = Query(25, ge=1, le=60),
):
    """
    Top single-game pitching lines (Game Score) and batting lines (Malli line score)
    from `{season}/regular_season/raw` feed_live files only. Used by the Intel hub to queue cards.
    """
    return await run_in_threadpool(_daily_standouts_sync, window.value, limit)
