"""Arm-angle estimation helpers for MLB Ops.

The fallback model is TJStats' public Model C from
https://github.com/tnestico/arm-angle-model. It estimates Savant-style arm
angle in degrees from horizontal (0 = sidearm, 90 = overhand).
"""

from __future__ import annotations

import gzip
import json
import math
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


TJ_MODEL_NAME = "tnestico_model_c"
TJ_MODEL_MAE_DEGREES = 2.65


def height_inches(height: str | int | float | None) -> int | None:
    """Parse MLB height strings such as ``6' 3"`` to inches."""
    if height is None:
        return None
    if isinstance(height, (int, float)) and not pd.isna(height):
        return int(height)
    nums = [int(x) for x in re.findall(r"\d+", str(height))]
    if len(nums) >= 2:
        return nums[0] * 12 + nums[1]
    if len(nums) == 1 and nums[0] > 40:
        return nums[0]
    return None


def estimate_tj_model_c(
    rpx_ft: pd.Series | np.ndarray | float,
    rpz_ft: pd.Series | np.ndarray | float,
    height_in: int | float,
    is_rhp: bool,
) -> pd.Series | float:
    """Estimate arm angle using TJStats' deployed extended linear model."""
    rpx = np.abs(rpx_ft)
    rpz = rpz_ft
    h = float(height_in)
    hand = 1.0 if is_rhp else 0.0
    out = (
        -56.6597
        + (-9.9659) * rpx
        + 27.2051 * rpz
        + 0.1526 * h
        + (-0.3255) * hand
        + 0.5192 * rpx * rpx
        + (-2.3327) * rpz * rpz
        + 0.0860 * rpx * rpz
        + (-59.6462) * (rpx * 12.0 / h)
        + 41.3050 * (rpz * 12.0 / h)
    )
    if isinstance(rpx_ft, pd.Series):
        return pd.Series(out, index=rpx_ft.index)
    return float(out) if np.isscalar(rpx_ft) else out


def _open_raw(path: Path):
    if path.suffix == ".gz" or path.name.endswith(".json.gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "r", encoding="utf-8")


def raw_path_for_parquet(parquet_path: str | Path) -> Path | None:
    """Find the raw feed/live file next to a pitches_enriched parquet."""
    pq = Path(parquet_path)
    m = re.match(r"game_(\d+)_(\d{8})_pitches_enriched\.parquet$", pq.name)
    if not m:
        return None
    raw_dir = pq.parent.parent / "raw"
    stem = f"game_{m.group(1)}_{m.group(2)}_feed_live"
    for suffix in (".json.gz", ".json"):
        path = raw_dir / f"{stem}{suffix}"
        if path.exists():
            return path
    return None


def feed_release_points(raw_path: str | Path, pitcher_id: int) -> pd.DataFrame:
    """Return per-pitch feed-derived release candidates keyed by ``play_id``.

    ``x_plus`` is the physical trajectory projection and usually matches
    Statcast ``release_pos_x``. ``x_minus`` follows the alternate horizontal
    transform documented by TJStats for feed/live inference. We keep both so
    callers can choose and audit the preprocessing.
    """
    path = Path(raw_path)
    with _open_raw(path) as f:
        feed = json.load(f)

    rows: list[dict[str, Any]] = []
    for play in (((feed.get("liveData") or {}).get("plays") or {}).get("allPlays") or []):
        matchup = play.get("matchup") or {}
        pitcher = matchup.get("pitcher") or {}
        if int(pitcher.get("id") or -1) != int(pitcher_id):
            continue
        for ev in play.get("playEvents") or []:
            pdata = ev.get("pitchData") or {}
            coords = pdata.get("coordinates") or {}
            if not ev.get("isPitch") or not coords:
                continue
            try:
                ext = float(pdata["extension"])
                y0 = float(coords["y0"])
                vy0 = float(coords["vY0"])
                x0 = float(coords["x0"])
                vx0 = float(coords["vX0"])
                ax = float(coords["aX"])
                z0 = float(coords["z0"])
                vz0 = float(coords["vZ0"])
                az = float(coords["aZ"])
            except (KeyError, TypeError, ValueError):
                continue
            if abs(vy0) < 1e-9:
                continue
            rel_y = 60.5 - ext
            dt = (rel_y - y0) / vy0
            rows.append(
                {
                    "play_id": ev.get("playId"),
                    "feed_rpx_plus": x0 + vx0 * dt + 0.5 * ax * dt * dt,
                    "feed_rpx_minus": x0 - vx0 * dt - 0.5 * ax * dt * dt,
                    "feed_rpz": z0 + vz0 * dt + 0.5 * az * dt * dt,
                    "feed_extension": ext,
                }
            )
    return pd.DataFrame(rows)


def add_effective_arm_angle(
    df: pd.DataFrame,
    *,
    height: str | int | float | None,
    hand: str | None,
    raw_path: str | Path | None = None,
    pitcher_id: int | None = None,
    feed_x_mode: str = "plus",
    allow_tj_fallback: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Add ``effective_arm_angle`` and source metadata to a pitch table.

    Official ``arm_angle`` is preferred pitch-by-pitch. Missing values fall
    back to TJStats Model C using feed-derived release points when a raw feed is
    available and matched by ``play_id``; otherwise parquet release coordinates
    are used.
    """
    out = df.copy()
    meta: dict[str, Any] = {
        "arm_angle_model": TJ_MODEL_NAME,
        "arm_angle_model_mae_degrees": TJ_MODEL_MAE_DEGREES,
        "arm_angle_feed_x_mode": feed_x_mode,
        "official_arm_angle_n": int(out["arm_angle"].notna().sum()) if "arm_angle" in out.columns else 0,
        "derived_arm_angle_n": 0,
        "derived_arm_angle_source": None,
        "feed_arm_angle_n": 0,
        "parquet_arm_angle_n": 0,
    }
    effective = pd.Series(np.nan, index=out.index, dtype="float64")
    if "arm_angle" in out.columns:
        effective = pd.to_numeric(out["arm_angle"], errors="coerce")

    if not allow_tj_fallback:
        out["effective_arm_angle"] = effective
        meta["arm_angle_source"] = "official" if meta["official_arm_angle_n"] else "unavailable"
        meta["arm_angle_height_in"] = height_inches(height)
        meta["tj_fallback_disabled"] = True
        meta["effective_arm_angle_mean"] = float(effective.dropna().mean()) if effective.notna().any() else None
        return out, meta

    h = height_inches(height)
    if h is None or h <= 0:
        out["effective_arm_angle"] = effective
        meta["arm_angle_source"] = "official" if meta["official_arm_angle_n"] else "unavailable"
        meta["arm_angle_height_in"] = None
        return out, meta
    meta["arm_angle_height_in"] = int(h)

    is_rhp = str(hand or "").upper().startswith("R")

    feed_vals = pd.Series(np.nan, index=out.index, dtype="float64")
    if raw_path and pitcher_id is not None and "play_id" in out.columns:
        try:
            feed = feed_release_points(raw_path, int(pitcher_id))
            if not feed.empty:
                x_col = "feed_rpx_minus" if feed_x_mode == "minus" else "feed_rpx_plus"
                feed["feed_arm_angle"] = estimate_tj_model_c(feed[x_col], feed["feed_rpz"], h, is_rhp)
                joined = out[["play_id"]].merge(
                    feed[["play_id", "feed_arm_angle"]],
                    on="play_id",
                    how="left",
                )
                feed_vals = pd.Series(joined["feed_arm_angle"].to_numpy(), index=out.index)
        except Exception as exc:
            meta["arm_angle_feed_error"] = str(exc)[:200]

    feed_mask = effective.isna() & feed_vals.notna()
    effective.loc[feed_mask] = feed_vals.loc[feed_mask]
    meta["feed_arm_angle_n"] = int(feed_mask.sum())

    pq_vals = pd.Series(np.nan, index=out.index, dtype="float64")
    if {"release_pos_x", "release_pos_z"}.issubset(out.columns):
        valid = out["release_pos_x"].notna() & out["release_pos_z"].notna()
        pq_vals.loc[valid] = estimate_tj_model_c(out.loc[valid, "release_pos_x"], out.loc[valid, "release_pos_z"], h, is_rhp)

    pq_mask = effective.isna() & pq_vals.notna()
    effective.loc[pq_mask] = pq_vals.loc[pq_mask]
    meta["parquet_arm_angle_n"] = int(pq_mask.sum())
    meta["derived_arm_angle_n"] = int(meta["feed_arm_angle_n"] + meta["parquet_arm_angle_n"])

    if meta["official_arm_angle_n"] and not meta["derived_arm_angle_n"]:
        source = "official"
    elif meta["official_arm_angle_n"] and meta["derived_arm_angle_n"]:
        source = "official_plus_tj_model"
    elif meta["feed_arm_angle_n"]:
        source = "tj_model_feed"
    elif meta["parquet_arm_angle_n"]:
        source = "tj_model_parquet"
    else:
        source = "unavailable"
    meta["arm_angle_source"] = source
    meta["effective_arm_angle_mean"] = float(effective.dropna().mean()) if effective.notna().any() else None
    out["effective_arm_angle"] = effective
    return out, meta
