"""Shared feature definitions for the pitching archetypes study.

Two things live here because every phase needs them and neither may drift:

  1. Handedness mirroring. Raw Statcast horizontal quantities are signed in the
     catcher's frame, so a LHP and a RHP throwing the *same* pitch have opposite
     signs. Clustering on the raw values recovers handedness, not stuff — the
     first component becomes "is he left-handed", which is already known and
     tells us nothing. Every horizontal quantity is therefore mirrored into a
     right-handed frame, and `p_throws` is carried as a descriptor instead of a
     feature (see RESEARCH_LOG.md, L0-D2).

  2. The Level-1 shape feature block. What makes two *pitches* the same pitch,
     independent of who threw it.

Spin axis is circular (359 deg and 1 deg are 2 deg apart, not 358). It is never
used as a raw number; it enters as sin/cos so the metric is honest.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

SEED = 20260827

# Signed in the catcher's frame -> flip for LHP so both hands share one frame.
MIRROR_COLS = ("pfx_x", "release_pos_x", "plate_x", "attack_direction", "hc_x_unused")

# Level-1: what makes two pitches the same pitch. Deliberately excludes
# arm_angle / release_pos / extension -- those are properties of the *pitcher*,
# and including them would cluster pitchers under the guise of clustering
# pitches (RESEARCH_LOG.md, L0-D3).
SHAPE_FEATURES = (
    "release_speed",
    "pfx_x_mir_in",
    "pfx_z_in",
    "release_spin_rate",
    "spin_axis_sin",
    "spin_axis_cos",
)

# Pitcher-identity traits. Descriptors at Level 1, features at Level 3.
IDENTITY_FEATURES = (
    "arm_angle",
    "release_extension",
    "release_pos_x_mir",
    "release_pos_z",
)


def mirror_handedness(df: pd.DataFrame) -> pd.DataFrame:
    """Add right-handed-frame copies of every hand-signed quantity.

    Originals are kept untouched; mirrored columns take a `_mir` suffix so no
    phase can silently consume the wrong frame.
    """
    out = df.copy()
    is_lhp = (out["p_throws"] == "L").to_numpy()
    flip = np.where(is_lhp, -1.0, 1.0)

    for col in ("pfx_x", "release_pos_x", "plate_x", "attack_direction"):
        if col in out.columns:
            out[f"{col}_mir"] = out[col].to_numpy(dtype="float64") * flip

    # Spin axis is a compass bearing (0-360, 180 = pure backspin/"12 o'clock"
    # tilt convention in Statcast). Mirroring is reflection across the 0-180
    # meridian, not negation.
    if "spin_axis" in out.columns:
        axis = out["spin_axis"].to_numpy(dtype="float64")
        out["spin_axis_mir"] = np.where(is_lhp, (360.0 - axis) % 360.0, axis)

    return out


def add_shape_features(df: pd.DataFrame) -> pd.DataFrame:
    """Derive the Level-1 shape block from mirrored columns."""
    out = df.copy()
    # Statcast pfx_* ship in feet; inches is the unit every pitching-design
    # source quotes, and it keeps the two movement axes on a comparable scale
    # to velocity before standardisation.
    out["pfx_x_mir_in"] = out["pfx_x_mir"] * 12.0
    out["pfx_z_in"] = out["pfx_z"].astype("float64") * 12.0

    rad = np.deg2rad(out["spin_axis_mir"].to_numpy(dtype="float64"))
    out["spin_axis_sin"] = np.sin(rad)
    out["spin_axis_cos"] = np.cos(rad)

    out["release_pos_x_mir"] = out["release_pos_x_mir"].astype("float64")
    return out
