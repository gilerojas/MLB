# MalliScore V3 — Frozen Specification

**Version:** `3.0.0` (`MALLISCORE_VERSION` in `src/pitching_performances/malli_score.py`)
**Status:** Frozen. Pinned by `tests/test_malliscore_golden.py`.
**Scope:** Descriptive index of a single starting-pitcher outing. Not a projection, not a talent estimate.

This document is transcribed from the code, not from design notes. Where earlier
documents disagree with it, this document is correct — see [Known divergences](#known-divergences-in-earlier-documents).

## Inputs

`OutingRawMetrics` — nine fields, seven of which are normalized:

| field | source | direction |
|---|---|---|
| `swstr_pct` | whiffs / official pitches × 100 | higher is better |
| `called_strike_pct` | called strikes / official pitches × 100 | higher is better |
| `chase_pct` | chases / out-of-zone pitches × 100 | higher is better |
| `xwoba_allowed` | mean xwOBA over PA-ending events | **lower** is better |
| `game_whip` | (hits + walks) / IP | **lower** is better |
| `earned_runs` | boxscore, count | **lower** is better |
| `home_runs` | boxscore, count | **lower** is better |
| `pitches` | boxscore, count | workload only |
| `outs` | boxscore, count | workload only |

`earned_runs` and `home_runs` are floored at 0 and enter as `log1p(x)`.

## League norms

Per-outing priors, applied as fixed constants unless a caller supplies refined norms:

| key | mean | std |
|---|---|---|
| `swstr_pct` | 11.0 | 4.0 |
| `called_strike_pct` | 18.0 | 5.0 |
| `chase_pct` | 28.0 | 8.0 |
| `xwoba_allowed` | 0.320 | 0.080 |
| `game_whip` | 1.20 | 0.50 |
| `log1p_er` | `log1p(2)` ≈ 1.0986 | 0.60 |
| `log1p_hr` | `log1p(0.8)` ≈ 0.5878 | 0.45 |

`refine_league_norms(samples, blend=0.35)` optionally shrinks these toward
same-slate sample moments: `norm = 0.65 × prior + 0.35 × slate`. It requires ≥8
samples overall and ≥8 finite values per key, and uses sample (n−1) variance.

**Only `src/pitching_performances/render.py:438` calls it.** Every other consumer —
the fantasy streamer, the shadow ledger, the projection experiments, the daily card —
uses `default_league_norms()`. Daily-board scores and shadow-ledger scores are
therefore not on the same scale.

## Weights

```
Dominance          swstr_pct 0.30 | called_strike_pct 0.25 | chase_pct 0.20 | xwoba_allowed 0.25
Run prevention     game_whip 0.40 | log1p_er         0.35 | log1p_hr  0.25
```

Both sum to 1.00. These are *nominal* weights. Realized influence differs because
the priors are miscalibrated relative to observed spread — see
`docs/MALLISCORE_V4_DECISION.md`.

## Pipeline

```
z_i        = (value_i - mean_i) / std_i        negated for lower-is-better metrics
pillar_z   = Σ(w_i · z_i) / Σ(w_i)             sum only over metrics with finite values
pillar     = clamp(50 + 15 · pillar_z, 0, 100)
core       = 2·D·R / (D + R)                   0 if either pillar is <= 0
workload   = see below
MalliScore = clamp(core · workload, 0, 100)
```

Four consequences worth stating explicitly, because they are not obvious:

1. **The z-clamp saturates at ±3.33.** Anything beyond that is indistinguishable.
2. **A pillar at 0 forces the final score to exactly 0**, via the harmonic mean,
   regardless of the other pillar. This fires on ~1.06% of real outings, always
   through run prevention, always driven by WHIP.
3. **Missing metrics are not treated as average.** `_weighted_z` skips them and
   renormalizes over surviving weights, so a missing metric is imputed to the
   weighted mean of the metrics that *are* present. `chase_pct` is the exception:
   it falls back explicitly to the league mean.
4. **`log1p_er` and `log1p_hr` are counts, not rates.** A longer outing accumulates
   more of both, so run prevention is implicitly length-dependent before the
   workload multiplier is applied.

## Workload

Outs-first, three-segment piecewise curve. `target_outs = 18`, `floor_outs = 12`:

```
outs < 12    base = 0.50 + (0.70 - 0.50) · (outs / 12)
12 <= o < 18 base = 0.70 + (1.00 - 0.70) · ((outs - 12) / 6)
outs >= 18   base = 1.00 + min((outs - 18) · 0.012, 0.10)

efficiency   = clamp(1.0 + (5.0 - pitches/outs) · 0.01, 0.985, 1.015)
workload     = clamp(base · efficiency, 0.50, 1.10)
```

`pitches` and `outs` are floored at 1, so `workload_scalar(p, 0)` is treated as 1 out.

At 90 pitches:

| outs | IP | workload |
|---|---|---|
| 6 | 2.0 | 0.5910 |
| 9 | 3.0 | 0.6402 |
| 12 | 4.0 | 0.6895 |
| 15 | 5.0 | 0.8415 |
| 18 | 6.0 | **1.0000** |
| 21 | 7.0 | 1.0434 |
| 24 | 8.0 | 1.0854 |
| 27 | 9.0 | 1.1000 |

18 outs is exactly neutral. The ceiling is reached at 27 outs. Efficiency is
deliberately narrow — it saturates at 3.5 and 6.5 pitches per out and can move the
result by at most ±1.5%.

## Worked example

6.0 IP, 90 pitches, 4 baserunners, 1 ER, 0 HR, 14.0 SwStr%, 18.0 Called%, 32.0 Chase%, .280 xwOBA.

Dominance z: `(0.30·0.750 + 0.25·0.000 + 0.20·0.500 + 0.25·0.500) / 1.00 = 0.4500`
→ `50 + 15 · 0.45 = ` **56.750**

Run prevention z: `(0.40·1.0667 + 0.35·0.6758 + 0.25·1.3062) / 1.00 = 0.9897`
→ `50 + 15 · 0.9897 = ` **64.846**

Core: `2 · 56.750 · 64.846 / (56.750 + 64.846) = ` **60.528**
Workload: 18 outs → base 1.000; 5.0 pitches/out → efficiency 1.000 → **1.0000**

**MalliScore = 60.528**

## Score distribution (2024 + 2025, 4,428 box-backed starter outings)

mean 43.7 · sd 15.0 · median 44.8 · p5 18.5 · p95 66.5 · max 86.6 · exactly 0 on 1.06%
Season means: 44.1 (2024), 43.3 (2025).

## Known divergences in earlier documents

Two documents describe a **7-inning (21-out) workload model that has never existed
in shipped code**. Git history confirms `outs / 21` appears only in the prose brief
(2026-07-10) and never in `src/`; the committed module has used `TARGET_OUTS = 18`
since it landed on 2026-07-17.

| claim | earlier docs | actual V3 |
|---|---|---|
| full volume credit | 21 outs | 18 outs |
| volume shape | `min(outs / 21, 1.0)` | three-segment piecewise |
| efficiency | `clamp(5 / ppo, 0.85, 1.10)` | `clamp(1 + (5 − ppo)·0.01, 0.985, 1.015)` |
| workload clamp | 0.50 – 1.15 | 0.50 – **1.10** |

Affected: `research/MalliScore workload model/publication_brief.md`,
`research/MalliScore workload model/final_tweet_thread.md`, and
`docs/MLBOPS_OVERVIEW.md` (corrected).

`research/study/pillar_malli_projection.py:189,201` also clamps workload to 1.15
rather than 1.10, so projected pillar scores can exceed anything V3 can produce.

## Naming

The module docstring says V3, the function is `malliscore_v2()`, the output carries
both `malli_score` and `malli_score_v2`, and some docs say V2. All refer to this
same formula, now versioned `3.0.0`. The V2/V3 names are retained for compatibility.

Unrelated: `mlbops/api/intel_standouts.py:137` defines a *batter* line score also
stored under a `malli_score` field. It shares the name only.
