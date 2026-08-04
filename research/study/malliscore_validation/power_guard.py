"""Statistical power guard for the MalliScore validation study.

The study runs on the box-backed outings we already have rather than re-ingesting
missing raw feeds, so every reported statistic has to declare whether it was
actually resolvable at that sample size. No result in this study is reported
without a verdict from this module.

Four verdicts. Two thresholds drive them, and keeping them separate matters:

    mde         minimum *detectable* effect -- a property of the sample size.
    meaningful  minimum *meaningful* effect -- a judgment about what would actually
                change V4. Supplied by the analysis, never derived from n.

Conflating the two makes NULL_CONFIRMED unreachable, because at any n the CI
half-width is approximately the MDE, so a true null always straddles it.

    RESOLVED         CI excludes the null and the effect is large enough to matter.
    RESOLVED_TRIVIAL CI excludes the null but the effect is below `meaningful`.
                     Statistically real, practically negligible -- must not drive V4.
    NULL_CONFIRMED   CI is tight and lies entirely inside +/- meaningful. A real
                     finding -- "there is nothing here" -- not a failure.
    UNDERPOWERED     CI spans the null and is wider than `meaningful`. The test
                     could not answer, and nothing may be concluded from it.

An UNDERPOWERED finding may not enter V4. It goes to the article as an open
question, and to the backlog as evidence for re-ingesting the missing 2024/2025
raw feeds (see docs/MALLISCORE_V3_SPEC.md and the study README).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Sequence

import numpy as np
from scipy import stats

RESOLVED = "RESOLVED"
RESOLVED_TRIVIAL = "RESOLVED_TRIVIAL"
NULL_CONFIRMED = "NULL_CONFIRMED"
UNDERPOWERED = "UNDERPOWERED"

VERDICTS = (RESOLVED, RESOLVED_TRIVIAL, NULL_CONFIRMED, UNDERPOWERED)

DEFAULT_BOOTSTRAP = 2000
SEED = 20260802

# Smallest correlation this study treats as practically meaningful. A |r| below
# this would not change a design decision about MalliScore no matter how tight
# its confidence interval is.
MEANINGFUL_R = 0.10


@dataclass
class Result:
    """One reported statistic, with everything needed to judge whether to trust it."""

    name: str
    effect: float
    n: int
    ci_low: float
    ci_high: float
    mde: float
    verdict: str
    meaningful: float = np.nan
    null: float = 0.0
    notes: str = ""
    extra: dict = field(default_factory=dict)

    @property
    def usable(self) -> bool:
        """Whether this result may inform a V4 design decision."""
        return self.verdict in (RESOLVED, NULL_CONFIRMED)

    def as_row(self) -> dict:
        return {
            "name": self.name,
            "effect": self.effect,
            "n": self.n,
            "ci_low": self.ci_low,
            "ci_high": self.ci_high,
            "mde": self.mde,
            "meaningful": self.meaningful,
            "null": self.null,
            "verdict": self.verdict,
            "usable": self.usable,
            "notes": self.notes,
            **self.extra,
        }

    def __str__(self) -> str:
        flag = {RESOLVED: " ", RESOLVED_TRIVIAL: ".", NULL_CONFIRMED: "0", UNDERPOWERED: "!"}[
            self.verdict
        ]
        return (
            f"{flag} {self.name:<44} {self.effect:+8.4f}  "
            f"[{self.ci_low:+7.4f},{self.ci_high:+7.4f}]  n={self.n:<6d} "
            f"mde={self.mde:.4f}  {self.verdict}"
        )


def verdict(
    effect: float,
    ci: tuple[float, float],
    mde: float,
    null: float = 0.0,
    meaningful: float | None = None,
) -> str:
    """Classify a statistic against its CI, its MDE, and what would actually matter.

    `meaningful` defaults to the MDE, which reduces to "can this design see it at
    all"; callers should pass a substantive threshold whenever one exists.
    """
    lo, hi = ci
    if not np.isfinite(lo) or not np.isfinite(hi):
        return UNDERPOWERED
    thresh = mde if meaningful is None or not np.isfinite(meaningful) else meaningful
    if lo > null or hi < null:
        return RESOLVED if abs(effect - null) >= thresh else RESOLVED_TRIVIAL
    # CI spans the null: either we can rule out a meaningful effect, or we cannot.
    if max(abs(lo - null), abs(hi - null)) <= thresh:
        return NULL_CONFIRMED
    return UNDERPOWERED


def bootstrap_ci(
    stat_fn: Callable[..., float],
    *arrays: Sequence[float],
    n_boot: int = DEFAULT_BOOTSTRAP,
    alpha: float = 0.05,
    seed: int = SEED,
) -> tuple[float, float]:
    """Percentile bootstrap CI. Arrays are resampled jointly (paired)."""
    cols = [np.asarray(a, dtype=float) for a in arrays]
    n = len(cols[0])
    if n < 3:
        return (np.nan, np.nan)
    rng = np.random.default_rng(seed)
    draws = np.empty(n_boot, dtype=float)
    for i in range(n_boot):
        idx = rng.integers(0, n, n)
        try:
            draws[i] = stat_fn(*[c[idx] for c in cols])
        except Exception:
            draws[i] = np.nan
    draws = draws[np.isfinite(draws)]
    if draws.size < n_boot * 0.5:
        return (np.nan, np.nan)
    return tuple(np.percentile(draws, [100 * alpha / 2, 100 * (1 - alpha / 2)]))


def mde_correlation(n: int, alpha: float = 0.05, power: float = 0.80) -> float:
    """Smallest |r| detectable at this n, via the Fisher z transform."""
    if n < 4:
        return np.inf
    z_a = stats.norm.ppf(1 - alpha / 2)
    z_b = stats.norm.ppf(power)
    z = (z_a + z_b) / np.sqrt(n - 3)
    return float(np.tanh(z))


def mde_mean_diff(n1: int, n2: int, sd: float, alpha: float = 0.05, power: float = 0.80) -> float:
    """Smallest difference in means detectable between two groups."""
    if n1 < 2 or n2 < 2 or not np.isfinite(sd) or sd <= 0:
        return np.inf
    z_a = stats.norm.ppf(1 - alpha / 2)
    z_b = stats.norm.ppf(power)
    return float((z_a + z_b) * sd * np.sqrt(1 / n1 + 1 / n2))


def check_correlation(
    name: str,
    x: Sequence[float],
    y: Sequence[float],
    *,
    method: str = "pearson",
    meaningful: float = MEANINGFUL_R,
    n_boot: int = DEFAULT_BOOTSTRAP,
    seed: int = SEED,
    notes: str = "",
) -> Result:
    """Correlation with bootstrap CI, MDE and verdict. NaN pairs are dropped."""
    xa, ya = np.asarray(x, dtype=float), np.asarray(y, dtype=float)
    mask = np.isfinite(xa) & np.isfinite(ya)
    xa, ya = xa[mask], ya[mask]
    n = int(mask.sum())
    fn = {
        "pearson": lambda a, b: stats.pearsonr(a, b)[0],
        "spearman": lambda a, b: stats.spearmanr(a, b)[0],
        "kendall": lambda a, b: stats.kendalltau(a, b)[0],
    }[method]
    if n < 4:
        return Result(name, np.nan, n, np.nan, np.nan, np.inf, UNDERPOWERED,
                      meaningful=meaningful, notes=notes)
    r = float(fn(xa, ya))
    ci = bootstrap_ci(fn, xa, ya, n_boot=n_boot, seed=seed)
    mde = mde_correlation(n)
    return Result(name, r, n, ci[0], ci[1], mde, verdict(r, ci, mde, 0.0, meaningful),
                  meaningful=meaningful, notes=notes, extra={"method": method})


def check_mean_diff(
    name: str,
    a: Sequence[float],
    b: Sequence[float],
    *,
    meaningful: float | None = None,
    n_boot: int = DEFAULT_BOOTSTRAP,
    seed: int = SEED,
    notes: str = "",
) -> Result:
    """Difference in means (a - b) with bootstrap CI, MDE and verdict.

    `meaningful` should be given in score points for MalliScore comparisons -- e.g.
    1.0 means "a shift smaller than one point does not change any decision".
    """
    aa = np.asarray(a, dtype=float)
    bb = np.asarray(b, dtype=float)
    aa, bb = aa[np.isfinite(aa)], bb[np.isfinite(bb)]
    n1, n2 = len(aa), len(bb)
    if n1 < 2 or n2 < 2:
        return Result(name, np.nan, n1 + n2, np.nan, np.nan, np.inf, UNDERPOWERED,
                      meaningful=meaningful or np.nan, notes=notes)
    diff = float(aa.mean() - bb.mean())
    rng = np.random.default_rng(seed)
    draws = np.array(
        [
            aa[rng.integers(0, n1, n1)].mean() - bb[rng.integers(0, n2, n2)].mean()
            for _ in range(n_boot)
        ]
    )
    ci = tuple(np.percentile(draws, [2.5, 97.5]))
    sd = float(np.sqrt((aa.var(ddof=1) + bb.var(ddof=1)) / 2))
    mde = mde_mean_diff(n1, n2, sd)
    thresh = mde if meaningful is None else meaningful
    return Result(name, diff, n1 + n2, ci[0], ci[1], mde, verdict(diff, ci, mde, 0.0, thresh),
                  meaningful=thresh, notes=notes, extra={"n_a": n1, "n_b": n2})


def check_statistic(
    name: str,
    stat_fn: Callable[..., float],
    *arrays: Sequence[float],
    mde: float,
    null: float = 0.0,
    meaningful: float | None = None,
    n_boot: int = DEFAULT_BOOTSTRAP,
    seed: int = SEED,
    notes: str = "",
) -> Result:
    """Generic escape hatch: any paired statistic, with an explicitly supplied MDE."""
    cols = [np.asarray(a, dtype=float) for a in arrays]
    mask = np.all([np.isfinite(c) for c in cols], axis=0)
    cols = [c[mask] for c in cols]
    n = int(mask.sum())
    thresh = mde if meaningful is None else meaningful
    if n < 4:
        return Result(name, np.nan, n, np.nan, np.nan, mde, UNDERPOWERED, thresh, null, notes)
    effect = float(stat_fn(*cols))
    ci = bootstrap_ci(stat_fn, *cols, n_boot=n_boot, seed=seed)
    return Result(name, effect, n, ci[0], ci[1], mde,
                  verdict(effect, ci, mde, null, thresh), thresh, null, notes)


class Register:
    """Collects results so a phase can report every verdict, including failures."""

    def __init__(self, phase: str) -> None:
        self.phase = phase
        self.results: list[Result] = []

    def add(self, result: Result) -> Result:
        self.results.append(result)
        return result

    def extend(self, results: Sequence[Result]) -> None:
        self.results.extend(results)

    @property
    def underpowered(self) -> list[Result]:
        return [r for r in self.results if r.verdict == UNDERPOWERED]

    @property
    def trivial(self) -> list[Result]:
        return [r for r in self.results if r.verdict == RESOLVED_TRIVIAL]

    def to_frame(self):
        import pandas as pd

        return pd.DataFrame([r.as_row() for r in self.results])

    def summary(self) -> str:
        counts = {v: sum(r.verdict == v for r in self.results) for v in VERDICTS}
        lines = [
            f"\n{'=' * 78}",
            f"POWER REGISTER — {self.phase}",
            f"{'=' * 78}",
            f"  {len(self.results)} tests: {counts[RESOLVED]} resolved, "
            f"{counts[RESOLVED_TRIVIAL]} resolved-but-trivial, "
            f"{counts[NULL_CONFIRMED]} null-confirmed, {counts[UNDERPOWERED]} UNDERPOWERED",
        ]
        if self.trivial:
            lines.append("\n  Statistically real but too small to matter:")
            lines += [f"    - {r.name} (effect={r.effect:+.4f} < {r.meaningful:.4f})"
                      for r in self.trivial]
        if self.underpowered:
            lines.append("\n  Could not be answered at this sample size:")
            lines += [f"    - {r.name} (n={r.n}, mde={r.mde:.4f})" for r in self.underpowered]
            lines.append("  These may not inform V4.")
        return "\n".join(lines)
