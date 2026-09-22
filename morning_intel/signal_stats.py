"""
Signal layer for morning intel — turns raw deltas into ranked surprise.

The engine used to rank anomalies by ``abs(delta)``. That sorts toward whichever
metric is noisiest at the smallest sample, so the newsletter filled with pitch-mix
swings on 50-pitch windows and 3-barrels-in-10-BBE batters. Ranking by delta over
its own standard error fixes that without season-specific threshold tuning: in
April the standard error is large and little clears the bar, in September it
shrinks and real velocity losses surface.
"""
from __future__ import annotations

import math

# Opportunities at which a metric carries roughly half its weight against the
# league prior. Used for regression-to-the-mean shrinkage and for blending a
# prior season into an early-season baseline.
STABILIZATION = {
    "avg_velo_mph": 40,      # pitches — a physical measurement, stabilizes fastest
    "chase_pct": 120,
    "whiff_pct": 150,
    "avg_EV_mph": 45,
    "xwoba_on_BIP": 60,
    "barrel_pct": 80,        # rare binary event — slowest of the batted-ball rates
    "mix": 250,              # usage is a decision, not a skill; needs the most rope
}

# Metrics stored as percentages (0-100) rather than proportions.
_PCT_METRICS = {"chase_pct", "whiff_pct", "barrel_pct"}


def stabilization_for(metric: str) -> int:
    """Stabilization constant for a metric name, including ``mix_XX_pct`` forms."""
    if metric.startswith("mix_"):
        return STABILIZATION["mix"]
    return STABILIZATION.get(metric, 100)


def pooled_rate_se(x_window: float, n_window: int, x_base: float, n_base: int) -> float:
    """
    Standard error of a difference in rates, in proportion units.

    ``x_*`` are event counts, ``n_*`` trial counts. Uses the pooled proportion,
    which is the right null for "did this rate actually move".
    """
    if n_window <= 0 or n_base <= 0:
        return float("inf")
    p = (x_window + x_base) / (n_window + n_base)
    if p <= 0.0 or p >= 1.0:
        return float("inf")
    return math.sqrt(p * (1.0 - p) * (1.0 / n_window + 1.0 / n_base))


def rate_se_from_pcts(pct_window: float, n_window: int, pct_base: float, n_base: int) -> float:
    """Pooled rate SE expressed in percentage points, from two percentages."""
    x_w = pct_window / 100.0 * n_window
    x_b = pct_base / 100.0 * n_base
    se = pooled_rate_se(x_w, n_window, x_b, n_base)
    return se * 100.0 if math.isfinite(se) else se


# League dispersion of the continuous metrics, measured over the 2026 warehouse
# (41K batted balls, 238K pitches). A standard deviation taken from a ten-ball
# window is barely an estimate, so it is regularized toward these.
LEAGUE_SD = {
    "avg_EV_mph": 15.28,
    "xwoba_on_BIP": 0.372,
    "avg_velo_mph": 4.85,   # median within-pitcher SD, not the league-wide 6.24
}
_SD_PRIOR_N = 20.0


def regularized_sd(sd_sample: float, n: int, metric: str) -> float:
    """Pull a small-sample standard deviation toward the league value for its metric."""
    prior = LEAGUE_SD.get(metric)
    if prior is None:
        return sd_sample
    if not math.isfinite(sd_sample) or n <= 1:
        return prior
    w = n / (n + _SD_PRIOR_N)
    return math.sqrt(w * sd_sample ** 2 + (1.0 - w) * prior ** 2)


def mean_diff_se(
    sd_window: float,
    n_window: int,
    sd_base: float,
    n_base: int,
    metric: str | None = None,
) -> float:
    """
    Standard error of a difference in means, from the two pool deviations.

    When ``metric`` is given the deviations are regularized toward the league
    value first, which stops a freakishly tight ten-ball window from reading as a
    near-certain finding.
    """
    if n_window <= 1 or n_base <= 1:
        return float("inf")
    if metric:
        sd_window = regularized_sd(sd_window, n_window, metric)
        sd_base = regularized_sd(sd_base, n_base, metric)
    if not (math.isfinite(sd_window) and math.isfinite(sd_base)):
        return float("inf")
    var = (sd_window ** 2) / n_window + (sd_base ** 2) / n_base
    if var <= 0.0:
        return float("inf")
    return math.sqrt(var)


# Typical between-outing spread of a single pitch type's usage, in percentage
# points, and the weight given to it. A variance estimated from three starts is
# itself extremely noisy — left raw it produces z-scores in the dozens whenever
# three outings happen to agree closely.
#
# Measured over 1,933 pitcher/pitch-type pairs with 5+ outings in the 2026
# warehouse: p25 5.7pp, median 8.0pp, p75 11.0pp. The median is the prior.
_MIX_PRIOR_SD_PP = 8.0
_MIX_PRIOR_DF = 4.0


def cluster_se(outing_rates: list[float], base_rate: float) -> float:
    """
    Standard error of a usage rate when the real unit of decision is the outing.

    Treating a 53-pitch window as 53 independent draws badly understates the noise
    in pitch mix: a pitcher picks a plan per start, so the pitches inside one
    outing are heavily correlated. Measuring spread *between* outings is what makes
    a shift carried by a single start read as the coin flip it is.

    The sample variance is regularized toward a typical between-outing variance,
    because with two degrees of freedom the raw estimate is unusable on its own.
    """
    n = len(outing_rates)
    if n < 2:
        return float("inf")
    mean = sum(outing_rates) / n
    df = n - 1
    var_sample = sum((r - mean) ** 2 for r in outing_rates) / df
    var_reg = (df * var_sample + _MIX_PRIOR_DF * _MIX_PRIOR_SD_PP ** 2) / (df + _MIX_PRIOR_DF)
    return math.sqrt(var_reg / n)


def two_sided_p(z: float) -> float:
    """Two-sided normal tail probability for a z-score."""
    return math.erfc(abs(z) / math.sqrt(2.0))


def benjamini_hochberg(p_values: list[float], q: float = 0.05) -> list[bool]:
    """
    Which of these tests survive at false-discovery rate ``q``.

    Around 1,600 player-metric comparisons run every morning, so a fixed |z|
    cutoff alone would admit roughly twenty pure-chance findings a day. This is
    what makes the list mean "expect about one in twenty of these to be noise"
    rather than "these crossed an arbitrary line", and it tightens or loosens
    itself as the number of qualifying players changes through the season.
    """
    n = len(p_values)
    if n == 0:
        return []
    order = sorted(range(n), key=lambda i: p_values[i])
    keep = [False] * n
    cutoff_rank = -1
    for rank, idx in enumerate(order, start=1):
        if p_values[idx] <= q * rank / n:
            cutoff_rank = rank
    for rank, idx in enumerate(order, start=1):
        if rank <= cutoff_rank:
            keep[idx] = True
    return keep


def zed(delta: float, se: float) -> float:
    """Delta in standard errors. Zero when the SE is undefined."""
    if se is None or not math.isfinite(se) or se <= 0.0:
        return 0.0
    return delta / se


def shrink(delta: float, n_window: int, metric: str) -> float:
    """
    Regress an observed delta toward zero by how little data stands behind it.

    A 27-point barrel swing on 10 BBE reports as roughly 3 points; the same swing
    on 200 BBE survives nearly intact.
    """
    k = stabilization_for(metric)
    if n_window <= 0:
        return 0.0
    return delta * (n_window / (n_window + k))


def blend_prior(obs: float, n_obs: int, prior: float, metric: str) -> float:
    """
    Blend a current-season observation with a prior-season value.

    Early in a season ``n_obs`` is small and the result sits near the prior; by
    midseason it converges on the observation. One formula covers the whole
    calendar, so April needs no special-casing.
    """
    if prior is None or not math.isfinite(prior):
        return obs
    if n_obs <= 0:
        return prior
    k = stabilization_for(metric)
    w = n_obs / (n_obs + k)
    return w * obs + (1.0 - w) * prior


def annotate(item: dict, se: float) -> dict:
    """Attach se/z/shrunk_delta to an anomaly dict in place, then return it."""
    delta = float(item["delta"])
    n_w = int(item.get("n_window") or 0)
    metric = str(item.get("metric") or "")
    item["se"] = round(se, 4) if se is not None and math.isfinite(se) else None
    item["z"] = round(zed(delta, se), 2)
    item["shrunk_delta"] = round(shrink(delta, n_w, metric), 3)
    return item
