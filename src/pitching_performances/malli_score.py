"""MalliScore V3: descriptive single-outing pitching performance index."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

TARGET_OUTS = 18
MIN_WORKLOAD_SCALAR = 0.50
MAX_WORKLOAD_SCALAR = 1.10
MIN_EFFICIENCY_SCALAR = 0.985
MAX_EFFICIENCY_SCALAR = 1.015
_Z_INDEX_SCALE = 15.0
_Z_INDEX_CENTER = 50.0

# League priors (per-outing); used when season sample is thin.
_DEFAULT_MEANS: dict[str, float] = {
    "swstr_pct": 11.0,
    "called_strike_pct": 18.0,
    "chase_pct": 28.0,
    "xwoba_allowed": 0.320,
    "game_whip": 1.20,
    "log1p_er": math.log1p(2),
    "log1p_hr": math.log1p(0.8),
}

_DEFAULT_STDS: dict[str, float] = {
    "swstr_pct": 4.0,
    "called_strike_pct": 5.0,
    "chase_pct": 8.0,
    "xwoba_allowed": 0.080,
    "game_whip": 0.50,
    "log1p_er": 0.60,
    "log1p_hr": 0.45,
}

_DOMINANCE_WEIGHTS = {
    "swstr_pct": 0.30,
    "called_strike_pct": 0.25,
    "chase_pct": 0.20,
    "xwoba_allowed": 0.25,
}

_RUN_PREVENTION_WEIGHTS = {
    "game_whip": 0.40,
    "log1p_er": 0.35,
    "log1p_hr": 0.25,
}


@dataclass(frozen=True)
class OutingRawMetrics:
    swstr_pct: float
    called_strike_pct: float
    chase_pct: float
    xwoba_allowed: float
    game_whip: float
    earned_runs: int
    home_runs: int
    pitches: int
    outs: int


@dataclass(frozen=True)
class LeagueNorms:
    means: dict[str, float]
    stds: dict[str, float]


def default_league_norms() -> LeagueNorms:
    return LeagueNorms(means=dict(_DEFAULT_MEANS), stds=dict(_DEFAULT_STDS))


def refine_league_norms(
    samples: list[OutingRawMetrics],
    *,
    base: LeagueNorms | None = None,
    blend: float = 0.35,
) -> LeagueNorms:
    """Blend default priors with same-slate sample moments when available."""
    base = base or default_league_norms()
    if len(samples) < 8:
        return base

    rows: list[dict[str, float]] = []
    for s in samples:
        chase = s.chase_pct if math.isfinite(s.chase_pct) else _DEFAULT_MEANS["chase_pct"]
        rows.append(
            {
                "swstr_pct": s.swstr_pct,
                "called_strike_pct": s.called_strike_pct,
                "chase_pct": chase,
                "xwoba_allowed": s.xwoba_allowed,
                "game_whip": s.game_whip,
                "log1p_er": math.log1p(max(s.earned_runs, 0)),
                "log1p_hr": math.log1p(max(s.home_runs, 0)),
            }
        )

    means = dict(base.means)
    stds = dict(base.stds)
    b = max(0.0, min(1.0, blend))
    for key in means:
        vals = [r[key] for r in rows if math.isfinite(r.get(key, math.nan))]
        if len(vals) < 8:
            continue
        mu = sum(vals) / len(vals)
        if len(vals) > 1:
            var = sum((v - mu) ** 2 for v in vals) / (len(vals) - 1)
            sigma = math.sqrt(var)
        else:
            sigma = base.stds[key]
        if sigma <= 1e-9:
            sigma = base.stds[key]
        means[key] = (1.0 - b) * base.means[key] + b * mu
        stds[key] = (1.0 - b) * base.stds[key] + b * sigma
    return LeagueNorms(means=means, stds=stds)


def _z_score(value: float, mean: float, std: float, *, invert: bool = False) -> float:
    if not math.isfinite(value) or std <= 1e-9:
        return 0.0
    z = (value - mean) / std
    return -z if invert else z


def _z_to_index(z: float) -> float:
    return max(0.0, min(100.0, _Z_INDEX_CENTER + _Z_INDEX_SCALE * z))


def _weighted_z(
    values: dict[str, float],
    norms: LeagueNorms,
    weights: dict[str, float],
    *,
    invert: set[str] | None = None,
) -> float:
    invert = invert or set()
    total_w = 0.0
    total_z = 0.0
    for key, weight in weights.items():
        if weight <= 0:
            continue
        val = values.get(key, math.nan)
        if not math.isfinite(val):
            continue
        mu = norms.means[key]
        sigma = norms.stds[key]
        z = _z_score(val, mu, sigma, invert=(key in invert))
        total_z += weight * z
        total_w += weight
    if total_w <= 0:
        return 0.0
    return total_z / total_w


def dominance_score(raw: OutingRawMetrics, norms: LeagueNorms) -> float:
    chase = raw.chase_pct if math.isfinite(raw.chase_pct) else norms.means["chase_pct"]
    values = {
        "swstr_pct": raw.swstr_pct,
        "called_strike_pct": raw.called_strike_pct,
        "chase_pct": chase,
        "xwoba_allowed": raw.xwoba_allowed,
    }
    z = _weighted_z(values, norms, _DOMINANCE_WEIGHTS, invert={"xwoba_allowed"})
    return _z_to_index(z)


def run_prevention_score(raw: OutingRawMetrics, norms: LeagueNorms) -> float:
    values = {
        "game_whip": raw.game_whip,
        "log1p_er": math.log1p(max(raw.earned_runs, 0)),
        "log1p_hr": math.log1p(max(raw.home_runs, 0)),
    }
    z = _weighted_z(
        values,
        norms,
        _RUN_PREVENTION_WEIGHTS,
        invert={"game_whip", "log1p_er", "log1p_hr"},
    )
    return _z_to_index(z)


def harmonic_mean(a: float, b: float) -> float:
    if a <= 0 or b <= 0:
        return 0.0
    return (2.0 * a * b) / (a + b)


def workload_scalar(
    pitches: int,
    outs: int,
    *,
    target_outs: int = TARGET_OUTS,
    min_scalar: float = MIN_WORKLOAD_SCALAR,
    max_scalar: float = MAX_WORKLOAD_SCALAR,
    min_efficiency: float = MIN_EFFICIENCY_SCALAR,
    max_efficiency: float = MAX_EFFICIENCY_SCALAR,
) -> float:
    """Outs-first workload multiplier.

    Smooth per-out curve:
    - 4 IP is a clear penalty.
    - 5 IP to 5.2 IP climbs steadily toward full starter credit.
    - 6 IP is near neutral.
    - Each out after 6 IP adds a small capped bonus.

    Pitch efficiency is intentionally narrow: it nudges the curve but should not
    overpower the completed-out shape.
    """
    p = max(int(pitches), 1)
    o = max(int(outs), 1)
    floor_outs = max(target_outs - 6, 1)

    if o < floor_outs:
        base = min_scalar + (0.70 - min_scalar) * (o / float(floor_outs))
    elif o < target_outs:
        base = 0.70 + (1.00 - 0.70) * ((o - floor_outs) / float(target_outs - floor_outs))
    else:
        base = 1.00 + min((o - target_outs) * 0.012, max_scalar - 1.00)

    pitches_per_out = p / float(o)
    efficiency = max(min_efficiency, min(max_efficiency, 1.0 + (5.0 - pitches_per_out) * 0.01))
    return max(min_scalar, min(max_scalar, base * efficiency))


def malliscore_v2(raw: OutingRawMetrics, norms: LeagueNorms) -> dict[str, float]:
    dom = dominance_score(raw, norms)
    run_prev = run_prevention_score(raw, norms)
    core = harmonic_mean(dom, run_prev)
    workload = workload_scalar(raw.pitches, raw.outs)
    final = max(0.0, min(100.0, core * workload))
    return {
        "dominance_score": dom,
        "run_prevention_score": run_prev,
        "core_score": core,
        "workload": workload,
        "malli_score": final,
        "malli_score_v2": final,
    }


def score_outing_dict(
    metric: dict[str, Any],
    box: Any,
    official_pitches: int,
    norms: LeagueNorms,
) -> dict[str, float]:
    """Score one outing from pitch metrics + boxscore line."""
    outs = _outs_from_ip(str(getattr(box, "ip", "0.0")))
    ip = outs / 3.0 if outs else 0.0
    bf = max(int(getattr(box, "batters_faced", 0)), 0)

    swstr_pct = float(metric["whiffs"]) / official_pitches * 100.0 if official_pitches else math.nan
    called = int(metric.get("called_strikes", 0))
    called_strike_pct = called / official_pitches * 100.0 if official_pitches else math.nan
    chase_pct = float(metric.get("chase_pct", math.nan))
    xwoba_allowed = float(metric.get("xwoba_allowed", math.nan))
    game_whip = (int(box.hits) + int(box.walks)) / ip if ip else math.nan

    raw = OutingRawMetrics(
        swstr_pct=swstr_pct,
        called_strike_pct=called_strike_pct,
        chase_pct=chase_pct,
        xwoba_allowed=xwoba_allowed,
        game_whip=game_whip,
        earned_runs=int(box.er),
        home_runs=int(box.home_runs),
        pitches=official_pitches,
        outs=outs,
    )
    scored = malliscore_v2(raw, norms)
    k_pct = box.strikeouts / bf * 100.0 if bf else math.nan
    bb_pct = box.walks / bf * 100.0 if bf else math.nan
    hr_pct = box.home_runs / bf * 100.0 if bf else math.nan
    game_era = box.er * 9.0 / ip if ip else math.nan

    return {
        "swstr_pct": swstr_pct,
        "called_strike_pct": called_strike_pct,
        "csw_pct": float(metric["csw"]) / official_pitches * 100.0 if official_pitches else math.nan,
        "k_pct": k_pct,
        "bb_pct": bb_pct,
        "hr_pct": hr_pct,
        "game_era": game_era,
        "game_whip": game_whip,
        "dominance_score": scored["dominance_score"],
        "run_prevention_score": scored["run_prevention_score"],
        "core_score": scored["core_score"],
        "process_score": scored["dominance_score"],
        "result_score": scored["run_prevention_score"],
        "workload": scored["workload"],
        "malli_score": scored["malli_score"],
        "malli_score_v2": scored["malli_score_v2"],
    }


def _outs_from_ip(ip_val: str) -> int:
    s = str(ip_val or "0").strip()
    if "." not in s:
        try:
            return int(float(s)) * 3
        except ValueError:
            return 0
    whole, frac = s.split(".", 1)
    try:
        return int(whole) * 3 + min(max(int((frac or "0")[0]), 0), 2)
    except ValueError:
        return 0
