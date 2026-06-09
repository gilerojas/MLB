"""
Mallitalytics MLB Content Hub — FastAPI server
Run from repo: cd mlbops && uvicorn api.main:app --port 8000 --reload
"""
import os
from pathlib import Path

from dotenv import load_dotenv

# mlbops/.env — e.g. MLBOPS_ALLOW_INTEL_RUN=1 (hub “Regenerate snapshot”)
# override=True: mlbops/.env wins over shell-inherited vars (e.g. stale MLB_WAREHOUSE_DIR from profile).
load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool

from api.paths import (
    get_intel_snapshots_dir,
    get_outputs_dir,
    get_redraft_batter_tweet_target_range,
    get_redraft_max_tokens,
    get_redraft_meta_max_chars,
    get_redraft_pitcher_tweet_target_range,
    get_repo_root,
    get_tweet_max_chars,
    get_warehouse_dir,
    safe_is_dir,
)
from api.routers import analytics, briefing, cards, fantasy, insights, intel, leaderboards, live, queue, schedule, watchlist
from api.routers import system_readiness

OUTPUTS_DIR = get_outputs_dir()
OUTPUTS_DIR.mkdir(exist_ok=True)

# Allow both hostnames — Next often runs as http://127.0.0.1:3000 (see start_hub.sh).
# Client-side pages (Queue, Cards toolbar, etc.) call the API from the browser; CORS must
# include the hub origin. Schedule/Briefing use Server Components (Node → API, no CORS).
_cors = os.getenv(
    "HUB_CORS_ORIGINS",
    "http://localhost:3000,http://127.0.0.1:3000",
)
_allow_origins = [o.strip() for o in _cors.split(",") if o.strip()]
_hub_port = os.getenv("MLBOPS_HUB_PORT", "").strip()
if _hub_port.isdigit():
    for _base in ("http://127.0.0.1", "http://localhost"):
        _o = f"{_base}:{_hub_port}"
        if _o not in _allow_origins:
            _allow_origins.append(_o)

_cors_mw: dict = {
    "allow_origins": _allow_origins,
    "allow_credentials": True,
    "allow_methods": ["*"],
    "allow_headers": ["*"],
}
# Any localhost port — fixes Queue "Failed to fetch" when the hub is on 3001+ without
# matching HUB_CORS_ORIGINS (e.g. only MLBOPS_HUB_PORT in .env). Set MLBOPS_STRICT_CORS=1
# to allow only the explicit list above (e.g. locked-down deploys).
if os.getenv("MLBOPS_STRICT_CORS", "").strip().lower() not in ("1", "true", "yes"):
    _cors_mw["allow_origin_regex"] = r"^http://(127\.0\.0\.1|localhost)(:\d+)?$"

app = FastAPI(
    title="Mallitalytics MLB Hub",
    description="Content queue API for MLB card generation and Twitter posting.",
    version="1.0.0",
)

app.add_middleware(CORSMiddleware, **_cors_mw)

# Serve generated card PNGs
app.mount("/static", StaticFiles(directory=str(OUTPUTS_DIR)), name="static")

app.include_router(cards.router)
app.include_router(analytics.router)
app.include_router(insights.router)
app.include_router(queue.router)
app.include_router(fantasy.router)
app.include_router(schedule.router)
app.include_router(leaderboards.router)
app.include_router(intel.router)
app.include_router(briefing.router)
app.include_router(watchlist.router)
app.include_router(live.router)
app.include_router(system_readiness.router)


@app.get("/")
def root():
    return {
        "service": "Mallitalytics MLB Ops API",
        "health": "/health",
        "docs": "/docs",
        "paths": "/system/paths",
        "readiness": "/system/readiness",
        "briefing": "/briefing",
        "intel": "/intel/snapshots",
        "intel_run": "POST /intel/run (needs MLBOPS_ALLOW_INTEL_RUN=1)",
        "queue": "/queue",
        "analytics": "/analytics/performance",
        "fantasy": "/fantasy/streamers",
    }


@app.get("/system/paths")
def system_paths():
    """Resolved data paths (Drive mirror defaults). Set MLB_REPO_ROOT / MLB_WAREHOUSE_DIR / MLB_INTEL_SNAPSHOTS_DIR to override."""
    from api.routers.intel import intel_run_allowed

    wh = get_warehouse_dir()
    snap = get_intel_snapshots_dir()
    rw_min, rw_max = get_redraft_pitcher_tweet_target_range()
    rb_min, rb_max = get_redraft_batter_tweet_target_range()
    return {
        "repo_root": str(get_repo_root()),
        "warehouse_dir": str(wh),
        "warehouse_exists": safe_is_dir(wh),
        "intel_snapshots_dir": str(snap),
        "intel_snapshots_exists": safe_is_dir(snap),
        "outputs_dir": str(get_outputs_dir()),
        "intel_run_allowed": intel_run_allowed(),
        "tweet_max_chars": get_tweet_max_chars(),
        "redraft_meta_max_chars": get_redraft_meta_max_chars(),
        "redraft_max_tokens": get_redraft_max_tokens(),
        "redraft_pitcher_tweet_min": rw_min,
        "redraft_pitcher_tweet_max": rw_max,
        "redraft_batter_tweet_min": rb_min,
        "redraft_batter_tweet_max": rb_max,
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/system/sync-drive")
async def sync_drive():
    """Stream rclone pull line-by-line so the hub can show live progress.

    Yields plain-text lines as rclone runs. Final sentinel lines:
      __SYNC_OK__ <ISO-timestamp>
      __SYNC_FAIL__ returncode=<N>
    """
    import subprocess
    from datetime import datetime as _dt

    from api.routers.intel import intel_run_allowed
    from fastapi import HTTPException
    from fastapi.responses import StreamingResponse

    if not intel_run_allowed():
        raise HTTPException(status_code=403, detail="Set MLBOPS_ALLOW_INTEL_RUN=1 to enable.")

    script = get_repo_root() / "scripts" / "pull_mlbops_from_drive.sh"
    if not script.is_file():
        raise HTTPException(status_code=404, detail=f"Sync script not found: {script}")

    def _generate():
        proc = subprocess.Popen(
            ["bash", str(script)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=str(get_repo_root()),
        )
        assert proc.stdout is not None
        buf = ""
        while True:
            chunk = proc.stdout.read(256)
            if not chunk:
                break
            buf += chunk
            # rclone stats use \r to overwrite; treat both \r and \n as line separators
            while True:
                for sep in ("\n", "\r"):
                    idx = buf.find(sep)
                    if idx != -1:
                        line = buf[:idx].strip()
                        buf = buf[idx + 1:]
                        if line:
                            yield line + "\n"
                        break
                else:
                    break
        if buf.strip():
            yield buf.strip() + "\n"
        proc.wait()
        if proc.returncode == 0:
            ts = _dt.utcnow().isoformat() + "Z"
            sentinel = get_repo_root() / "data" / ".last_drive_sync"
            try:
                sentinel.parent.mkdir(parents=True, exist_ok=True)
                sentinel.write_text(ts)
            except OSError:
                pass
            yield f"__SYNC_OK__ {ts}\n"
        else:
            yield f"__SYNC_FAIL__ returncode={proc.returncode}\n"

    return StreamingResponse(_generate(), media_type="text/plain")
