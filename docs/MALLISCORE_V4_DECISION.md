# MalliScore V4 — Decision Memo

**Status:** SHIPPED FOR ACTUAL OUTINGS. V3 remains frozen for historical reproduction
and projection paths that do not yet estimate HBP.
**Study:** `research/study/malliscore_validation/`
**Frozen predecessor:** [MALLISCORE_V3_SPEC.md](MALLISCORE_V3_SPEC.md)

V4 is implemented as `malliscore_v4()` and powers actual-outing Pitching Index scores.
Generated CSV and queue metadata record `malli_score_version=4.0.0`; the image continues
to display the public name `MalliScore` without visual version clutter.

## Register of tests that could not be answered

Per the study's power guard, findings resting on an underpowered test may not inform V4.
Two did not resolve at the available sample size:

| test | n | effect | 95% CI | why it matters |
|---|---|---|---|---|
| elite next start: MalliScore over GSv2 (AUC) | 2,056 | +0.0001 | [−0.0130, +0.0133] | cannot say whether MalliScore improves elite-start classification |
| within-pitcher: MalliScore over GSv2 (R²) | 2,056 | +0.0032 | [−0.0039, +0.0100] | cannot say whether beating your own baseline predicts anything |

Neither informed the decision. Both go to the article as open questions. Re-ingesting the
missing 2024/2025 raw feeds via `src/ingestion/load_mlb_warehouse.py` would roughly double
the sample if we want to settle them.

Everything else resolved: 15/15 in the architecture audit, 10/10 in reliability and
benchmarking. The study was adequately powered for the questions that drove V4.

## What the study actually found

**The weights were never the problem.** Across 20,000 weight vectors sampled over the
entire feasible region, rank agreement with V3 never fell below Spearman 0.963, and 97%
of the region agreed above 0.98. The weights are weakly identified: many interpretable
vectors produce a nearly identical metric. Precision like 27.4/23.8/21.1/27.7 would imply
certainty the data does not support. **V4 therefore keeps V3's weights unchanged.**

Three real defects sat elsewhere.

**1. The priors were guesses, and one was badly wrong.**

| metric | prior σ | observed σ (2024) | ratio |
|---|---|---|---|
| game_whip | 0.50 | 1.01 | **2.02** |
| called_strike_pct | 5.00 | 3.96 | **0.79** |
| swstr_pct | 4.00 | 4.61 | 1.15 |
| xwoba_allowed | 0.080 | 0.089 | 1.11 |

Ratios reproduced in 2025, so this is miscalibration and not noise. The effect is that
nominal weight ≠ realized influence. Perturbing each input by one observed standard
deviation and measuring the change in the final score:

| metric | nominal weight | realized | drift |
|---|---|---|---|
| game_whip | 0.40 | **0.56** | +0.16 |
| log1p_er | 0.35 | 0.24 | −0.11 |
| log1p_hr | 0.25 | 0.20 | −0.05 |
| swstr_pct | 0.30 | 0.33 | +0.03 |
| xwoba_allowed | 0.25 | 0.29 | +0.04 |
| called_strike_pct | 0.25 | 0.19 | −0.06 |
| chase_pct | 0.20 | 0.19 | −0.01 |

Run prevention also moves the final score **1.66×** as much as dominance (62% vs 38%),
despite the harmonic mean giving them equal nominal standing.

**2. WHIP broke the clamp, and the harmonic mean turned that into a zero.**

WHIP carries innings pitched in the denominator, so a short blow-up is unbounded: median
1.20, p99 4.50, max 21.0, skew 6.5, kurtosis 92. Against the prior σ of 0.50 the p99
outing sits at z = −6.6, double the ±3.33 that saturates the 0–100 index. Run prevention
pinned to exactly 0, and `harmonic_mean` returns 0 whenever either pillar is ≤ 0.

Result: **76 outings across 2024–2026 scored exactly 0.** In 2024 alone those 22 outings
spanned Game Score v2 from −19 to +21 — a 40-point range of badness collapsed onto a
single number. 21 of the worst 95 outings in the season were exact ties.

**3. WHIP is also the least reliable input in the metric.**

Split-half reliability, pitcher-seasons with ≥10 starts:

| input | reliability |
|---|---|
| SwStr% | 0.784 |
| dominance pillar | 0.700 |
| xwOBA allowed | 0.502 |
| **run prevention pillar** | **0.328** |
| **WHIP** | **0.291** |

The noisiest signal in MalliScore was also its loudest.

## What was checked and left alone

**The workload multiplier is not double-counting.** Outs correlate 0.63 with run
prevention and 0.96 with workload, which looked like length being counted twice. It is
not a denominator artifact: the outs↔WHIP link (0.667) and the outs↔baserunners-per-BF
link (0.695) are the same size, so the relationship is **selection** — good starts
genuinely run longer. The curve is unchanged.

**Redundancy was never the problem.** All seven inputs have VIF below 2.4. SwStr% and
called-strike rate are *negatively* correlated (−0.28): they capture genuinely different
approaches, not the same thing twice.

**The fusion choice barely matters.** Harmonic, geometric and arithmetic agree at
Kendall τ 0.97–0.99. Harmonic is retained.

**Slate normalization is not worth its complexity.** Fixed and slate-refined norms
correlate 0.998, and slate norms do not reduce calendar drift (1.63 vs 1.69 points of
month-to-month spread). Since only `render.py:438` uses them while every other consumer
uses fixed priors, the daily board and shadow ledger are currently on different scales.
**Recommendation: standardize on fixed norms everywhere.** Not bundled into V4.

## The change

V4 makes exactly two changes to V3:

1. **League norms measured on 2024 starter outings** instead of assumed.
2. **Reach Rate Allowed**, `(H + BB + HBP) / BF`, replaces WHIP in run prevention.

Weights, harmonic fusion, workload curve and clamps are all unchanged.

```
swstr_pct           11.78 ± 4.61        reach_rate_allowed   0.313 ± 0.112
called_strike_pct   16.17 ± 3.96        log1p_er             1.053 ± 0.623
chase_pct           28.48 ± 7.85        log1p_hr             0.429 ± 0.452
xwoba_allowed       0.321 ± 0.089
```

The original study candidate used `(H + BB) / BF`. Before shipment, an explicit
7,479-outing comparison added HBP because it is a pitcher-created reach. The HBP-inclusive
rate retained zero collapsed outings, changed a daily top-five member on only 8.8% of
slates relative to the no-HBP candidate, and moved scores by 0.39 points on average.

## How the candidate was chosen

Five candidates were scored on the criteria agreed before the study — interpretability 30%,
cross-season stability 25%, non-redundancy 20%, credible disagreement with Game Score 15%,
incremental next-start signal 10%.

| candidate | interp. | stability | non-redund. | disagree. | total |
|---|---|---|---|---|---|
| **C. empirical priors + baserunners/BF** | 0.90 | 0.431 | 0.612 | 0.979 | **0.697** |
| E. C + reliability-weighted run prevention | 0.75 | 0.429 | 0.659 | 0.977 | 0.661 |
| D. C + xwOBA moved to run prevention | 0.85 | 0.468 | 0.423 | 0.978 | 0.653 |
| B. empirical priors only | 0.80 | 0.439 | 0.330 | 0.986 | 0.614 |
| A. V3 as shipped | 0.70 | 0.401 | 0.304 | 0.976 | 0.567 |

**This margin is not robust, and that must be stated.** Interpretability is the one
judgment input. Re-scoring it at random 2,000 times changes the winner **66% of the time**
— C, D and E are not separated by the measured criteria. C was chosen because it makes the
fewest changes that fix the demonstrated defect, and D's higher reliability (0.468, a
resolved gain over V3) is a genuine argument for revisiting the pillar split later.

Candidate D deserves a note: moving xwOBA out of dominance drops pillar correlation from
0.567 to 0.395, because xwOBA allowed is a *result* sitting in the pillar named for
*process*. That is a real design flaw in V3, left unfixed in V4, and the strongest
candidate for V5.

## Validation

Decided on 2024. Then run once against 2025, once against 2026, with no re-tuning.

| | 2024 (dev) | 2025 (validation) | 2026 (confirmation) |
|---|---|---|---|
| V3 exact zeros | 22 | 25 | 29 |
| **V4 exact zeros** | **0** | **0** | **0** |
| V3 reliability | 0.401 | 0.550 | 0.677 |
| V4 reliability | 0.431 | 0.573 | 0.689 |
| paired gap vs V3 | +0.030 | **+0.023 RESOLVED** | +0.012 not resolved |
| Spearman vs GSv2 | 0.939 | 0.926 | 0.930 |

**The defect fix reproduces perfectly: zero collapsed outings in all three seasons.**

**The reliability gain does not.** It shrinks monotonically — +0.030, +0.023, +0.012 — and
is not resolved on 2026. Per the study's own rules this is reported, not repaired. The
honest reading: V4's reliability advantage over V3 is real but small, and consistent with
the fact that it only changes one of seven inputs. The case for V4 rests on removing the
exact-zero collapse, not on measurably better reliability.

The 22 outings V3 collapsed to zero in 2024 now span 4.3 to 19.8 under V4, tracking Game
Score v2 across its −19 to +21 range.

## Where MalliScore stands against Game Score

MalliScore is **more reliable than Game Score**, and the paired test resolves it:

| comparison | gap | 95% CI | verdict |
|---|---|---|---|
| MalliScore vs GSv2, ≥10 starts | +0.115 | [+0.069, +0.172] | RESOLVED |
| MalliScore vs GSv1, ≥10 starts | +0.113 | [+0.061, +0.176] | RESOLVED |

Comparing the two metrics' marginal confidence intervals would have shown them overlapping
and concluded nothing. They are measured on the same pitchers, so the correct test
bootstraps the difference — and it resolves cleanly.

Agreement with Game Score v2 sits at Spearman 0.942, and disagreement is systematic in the
intended direction:

| | n | SwStr% | outs | ER |
|---|---|---|---|---|
| MalliScore higher | 109 | 13.1 | 17.8 | 3.1 |
| Game Score higher | 99 | 10.1 | 14.4 | 1.0 |

MalliScore favours long, high-whiff starts that gave up runs; Game Score favours short,
low-whiff starts that did not. No outing lands in opposite tails of the two metrics — they
never strongly contradict each other.

## What MalliScore does not do

MalliScore adds **no** predictive information about a pitcher's next start beyond his
recent form: four NULL_CONFIRMED results across next-start SwStr%, xwOBA, K−BB% and WHIP.
Game Score v2 adds nothing either. Both describe what happened; neither forecasts.

This is the correct result for a descriptive index and it independently corroborates the
existing finding that per-game variance is the ceiling for starter projection.

## Honest limitations

- Priors are fit on 2024 only, which keeps validation clean but means they are estimated
  on 1,908 outings. Refreshing them on all three seasons is a deliberate versioned change,
  not a silent one.
- Earned runs exist for only 40% of 2024 games and 54% of 2025. Coverage is
  non-random in time — 2024 starts in late March, 2025 raw feeds not until 12 April.
- Starters only. The workload curve is starter-shaped and relievers are excluded by
  construction; they need their own curve and priors.
- `corr(outs, MalliScore) = 0.85`. MalliScore is substantially a measure of length. That
  is deliberate — it is an index of *complete outing quality* — but it should be said out
  loud rather than discovered by a reader.
