"""
GET /system/readiness — fast pipeline health for hub checklist (bounded globs only).
"""
from __future__ import annotations

import platform
import re
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter

from api.paths import get_intel_snapshots_dir, get_repo_root, get_warehouse_dir, safe_is_dir
from api.routers.intel import intel_run_allowed

router = APIRouter(prefix="/system", tags=["system"])

ANCHOR_RE = re.compile(r"^intel_(\d{4}-\d{2}-\d{2})\.json$")
_STAGES = ("regular_season", "postseason", "spring_training")
_ET = ZoneInfo("America/New_York")
# Warn if Drive sentinel older than this many hours
_DRIVE_SYNC_WARN_HOURS = 24
# Warn if intel snapshot anchor is older than this many days vs today (ET)
_INTEL_ANCHOR_WARN_DAYS = 2
_INGEST_LOG_PATH = Path(os.environ.get("MLBOPS_DAILY_INGEST_LOG", "/logs/daily_ingest.log"))


def _last_drive_sync_utc() -> Optional[str]:
    root = get_repo_root()
    sentinel = root / "data" / ".last_drive_sync"
    try:
        t = sentinel.read_text().strip()
        return t or None
    except OSError:
        return None


def _drive_runtime_check_enabled() -> bool:
    """Drive sync is a local-dev mirror check, not a VPS production dependency."""
    backend = os.environ.get("MLBOPS_DB_BACKEND", "sqlite").strip().lower()
    runtime = os.environ.get("MLBOPS_RUNTIME", "").strip().lower()
    return runtime not in {"vps", "production", "prod"} and backend not in {"postgres", "postgresql", "pg"}


def _parse_iso_utc(s: str) -> Optional[datetime]:
    s = s.strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        ts = datetime.fromisoformat(s)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
    except ValueError:
        return None


def _latest_intel_anchor() -> Optional[str]:
    d = get_intel_snapshots_dir()
    if not safe_is_dir(d):
        return None
    best: Optional[tuple[date, str]] = None
    try:
        for p in d.iterdir():
            if not p.is_file():
                continue
            m = ANCHOR_RE.match(p.name)
            if not m:
                continue
            ad = date.fromisoformat(m.group(1))
            if best is None or ad > best[0]:
                best = (ad, m.group(1))
    except (OSError, ValueError):
        return None
    return best[1] if best else None


def _season_year_et() -> int:
    return datetime.now(_ET).year


def _ymd_compact(d: date) -> str:
    return d.strftime("%Y%m%d")


def _hits_for_date(wh: Path, season: int, ymd: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for stage in _STAGES:
        base = wh / str(season) / stage / "pitches_enriched"
        if not safe_is_dir(base):
            out[stage] = 0
            continue
        try:
            n = len(list(base.glob(f"game_*_{ymd}_pitches_enriched.parquet")))
        except OSError:
            n = 0
        out[stage] = n
    return out


def _total_hits(h: dict[str, int]) -> int:
    return sum(h.values())


def _latest_parquet_date(wh: Path, season: int) -> dict[str, Any]:
    latest: Optional[str] = None
    counts: dict[str, dict[str, int]] = {}
    if not safe_is_dir(wh):
        return {"date": None, "count": 0, "by_stage": {}}
    pattern = re.compile(r"game_\d+_(\d{8})_pitches_enriched\.parquet$")
    for stage in _STAGES:
        base = wh / str(season) / stage / "pitches_enriched"
        if not safe_is_dir(base):
            continue
        try:
            for p in base.glob("game_*_pitches_enriched.parquet"):
                m = pattern.match(p.name)
                if not m:
                    continue
                ymd = m.group(1)
                counts.setdefault(ymd, {s: 0 for s in _STAGES})
                counts[ymd][stage] += 1
                if latest is None or ymd > latest:
                    latest = ymd
        except OSError:
            pass
    if latest:
        by_stage = counts.get(latest, {s: 0 for s in _STAGES})
        count = sum(by_stage.values())
        pretty = f"{latest[:4]}-{latest[4:6]}-{latest[6:]}"
    else:
        by_stage = {s: 0 for s in _STAGES}
        count = 0
        pretty = None
    return {"date": pretty, "ymd": latest, "count": count, "by_stage": by_stage}


def _tail_lines(path: Path, limit: int = 12) -> list[str]:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []
    return lines[-limit:]


def _ingest_status() -> dict[str, Any]:
    path = _INGEST_LOG_PATH
    exists = False
    updated_at = None
    age_minutes = None
    tail: list[str] = []
    try:
        st = path.stat()
        exists = path.is_file()
        updated = datetime.fromtimestamp(st.st_mtime, timezone.utc)
        updated_at = updated.isoformat().replace("+00:00", "Z")
        age_minutes = int((datetime.now(timezone.utc) - updated).total_seconds() // 60)
        tail = _tail_lines(path)
    except OSError:
        pass
    last_line = tail[-1] if tail else ""
    status: Literal["ok", "warn", "unknown"] = "unknown"
    if tail:
        status = "ok" if "ingest end" in last_line or "exists=True" in last_line else "warn"
    return {
        "log_path": str(path),
        "log_exists": exists,
        "updated_at_utc": updated_at,
        "age_minutes": age_minutes,
        "status": status,
        "tail": tail,
        "schedule": os.environ.get("MLBOPS_INGEST_CRON_LABEL", "02:30, 06:30, 10:30 server time"),
    }


def _build_readiness() -> dict[str, Any]:
    now_et = datetime.now(_ET)
    today_d = now_et.date()
    yesterday_d = today_d - timedelta(days=1)
    y_ymd = _ymd_compact(yesterday_d)
    t_ymd = _ymd_compact(today_d)

    wh = get_warehouse_dir()
    repo = get_repo_root()
    intel_dir = get_intel_snapshots_dir()
    season = _season_year_et()

    warehouse_exists = safe_is_dir(wh)
    yesterday_hits = _hits_for_date(wh, season, y_ymd) if warehouse_exists else {s: 0 for s in _STAGES}
    today_hits = _hits_for_date(wh, season, t_ymd) if warehouse_exists else {s: 0 for s in _STAGES}
    y_total = _total_hits(yesterday_hits)
    t_total = _total_hits(today_hits)
    latest_parquet = _latest_parquet_date(wh, season) if warehouse_exists else {"date": None, "count": 0, "by_stage": {}}
    ingest = _ingest_status()

    last_sync = _last_drive_sync_utc()
    last_sync_ok = last_sync is not None
    drive_stale = False
    if last_sync:
        ts = _parse_iso_utc(last_sync)
        if ts:
            now = datetime.now(timezone.utc)
            ts_u = ts.astimezone(timezone.utc)
            if now - ts_u > timedelta(hours=_DRIVE_SYNC_WARN_HOURS):
                drive_stale = True
        else:
            drive_stale = True

    anchor = _latest_intel_anchor()
    today_s = today_d.isoformat()
    intel_stale = False
    if anchor is None:
        intel_stale = True
    else:
        ad = date.fromisoformat(anchor)
        if (today_d - ad).days > _INTEL_ANCHOR_WARN_DAYS:
            intel_stale = True

    allowed = intel_run_allowed()

    checks: list[dict[str, Any]] = []

    if warehouse_exists:
        checks.append(
            {
                "id": "warehouse_dir",
                "ok": True,
                "severity": "ok",
                "label": "Warehouse directory",
                "detail": f"Readable at {wh}",
                "action": None,
            }
        )
    else:
        checks.append(
            {
                "id": "warehouse_dir",
                "ok": False,
                "severity": "block",
                "label": "Warehouse directory",
                "detail": f"Missing or not a directory: {wh}. Set MLB_WAREHOUSE_DIR to the warehouse volume or run ingest.",
                "action": "ingest",
            }
        )

    if y_total > 0:
        checks.append(
            {
                "id": "pitches_enriched_yesterday",
                "ok": True,
                "severity": "ok",
                "label": "Pitches (yesterday ET)",
                "detail": f"Found {y_total} file(s) for {y_ymd} (glob: game_*_{y_ymd}_pitches_enriched.parquet).",
                "action": None,
            }
        )
    else:
        checks.append(
            {
                "id": "pitches_enriched_yesterday",
                "ok": False,
                "severity": "warn",
                "label": "Pitches (yesterday ET)",
                "detail": f"No parquets for {yesterday_d.isoformat()} under …/{season}/*/pitches_enriched/. Cards/stats for that date may fail until ingest completes.",
                "action": "ingest",
            }
        )

    if t_total > 0:
        checks.append(
            {
                "id": "pitches_enriched_today",
                "ok": True,
                "severity": "ok",
                "label": "Pitches (today ET)",
                "detail": f"Found {t_total} file(s) for {t_ymd}.",
                "action": None,
            }
        )
    else:
        checks.append(
            {
                "id": "pitches_enriched_today",
                "ok": False,
                "severity": "warn",
                "label": "Pitches (today ET)",
                "detail": f"No parquets yet for {today_s}. Normal before first pitch; run ingest after games if needed.",
                "action": "ingest",
            }
        )

    if not _drive_runtime_check_enabled():
        checks.append(
            {
                "id": "runtime_storage",
                "ok": True,
                "severity": "ok",
                "label": "VPS storage",
                "detail": "Runtime uses mounted warehouse/output volumes. Google Drive is not in the live path.",
                "action": None,
            }
        )
    elif last_sync_ok and not drive_stale:
        checks.append(
            {
                "id": "drive_sync",
                "ok": True,
                "severity": "ok",
                "label": "Drive mirror (last sync)",
                "detail": f"data/.last_drive_sync: {last_sync[:19]}…",
                "action": None,
            }
        )
    elif not last_sync_ok:
        checks.append(
            {
                "id": "drive_sync",
                "ok": False,
                "severity": "warn",
                "label": "Local mirror (last Drive sync)",
                "detail": "No data/.last_drive_sync found. Local dev may need a mirror refresh or a manual ingest.",
                "action": "sync_drive",
            }
        )
    else:
        checks.append(
            {
                "id": "drive_sync",
                "ok": False,
                "severity": "warn",
                "label": "Local mirror (last Drive sync)",
                "detail": f"Last sync may be stale (> {_DRIVE_SYNC_WARN_HOURS}h) or unparseable: {last_sync!r}.",
                "action": "sync_drive",
            }
        )

    if anchor and not intel_stale:
        checks.append(
            {
                "id": "intel_snapshot",
                "ok": True,
                "severity": "ok",
                "label": "Intel snapshot",
                "detail": f"Latest: intel_{anchor}.json",
                "action": None,
            }
        )
    else:
        checks.append(
            {
                "id": "intel_snapshot",
                "ok": False,
                "severity": "warn",
                "label": "Intel snapshot",
                "detail": (
                    f"No recent snapshot{'' if anchor is None else f' (anchor {anchor} is older than {_INTEL_ANCHOR_WARN_DAYS}d vs today)'}. "
                    "Regenerate on the dashboard."
                ),
                "action": "run_intel",
            }
        )

    if allowed:
        checks.append(
            {
                "id": "intel_ops",
                "ok": True,
                "severity": "ok",
                "label": "Hub intel / Drive operations",
                "detail": "MLBOPS_ALLOW_INTEL_RUN=1 — sync and Regenerate enabled.",
                "action": None,
            }
        )
    else:
        checks.append(
            {
                "id": "intel_ops",
                "ok": False,
                "severity": "warn",
                "label": "Hub intel / Drive operations",
                "detail": "Set MLBOPS_ALLOW_INTEL_RUN=1 on the API process (./start_hub.sh does this by default).",
                "action": "open_settings",
            }
        )

    has_block = any(c["severity"] == "block" for c in checks)
    has_warn = any(c["severity"] == "warn" for c in checks)
    if has_block:
        overall: Literal["block", "warn", "ok"] = "block"
    elif has_warn:
        overall = "warn"
    else:
        overall = "ok"

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "overall": overall,
        "environment": {
            "repo_root": str(repo),
            "warehouse_dir": str(wh),
            "intel_snapshots_dir": str(intel_dir),
            "machine_hint": platform.node() or "",
            "runtime": os.environ.get("MLBOPS_RUNTIME", ""),
            "db_backend": os.environ.get("MLBOPS_DB_BACKEND", ""),
            "outputs_dir": os.environ.get("MLBOPS_OUTPUTS_DIR", ""),
        },
        "vps": {
            "enabled": not _drive_runtime_check_enabled(),
            "runtime": os.environ.get("MLBOPS_RUNTIME", ""),
            "db_backend": os.environ.get("MLBOPS_DB_BACKEND", ""),
            "warehouse_dir": str(wh),
            "outputs_dir": os.environ.get("MLBOPS_OUTPUTS_DIR", ""),
            "google_drive_live_path": _drive_runtime_check_enabled(),
        },
        "ingest": ingest,
        "flags": {
            "intel_run_allowed": allowed,
        },
        "drive_sync": {
            "last_sync_utc": last_sync,
        },
        "warehouse": {
            "exists": warehouse_exists,
            "season": season,
            "latest": latest_parquet,
            "pitches_enriched": {
                "yesterday_ymd": y_ymd,
                "yesterday_by_stage": yesterday_hits,
                "yesterday_total": y_total,
                "today_ymd": t_ymd,
                "today_by_stage": today_hits,
                "today_total": t_total,
            },
        },
        "intel": {
            "latest_snapshot_anchor": anchor,
        },
        "checks": checks,
    }


@router.get("/readiness")
def get_readiness():
    return _build_readiness()
