# MalliScore Validation & Calibration Study

Empirical audit of MalliScore, which was built by judgment and never tested against data.
Freezes the shipped formula as **V3**, audits its architecture on 2024–2026 starter
outings, and produces an evidence-backed, still-experimental **V4**.

- Frozen V3 spec: [`docs/MALLISCORE_V3_SPEC.md`](../../../docs/MALLISCORE_V3_SPEC.md)
- Decision memo: [`docs/MALLISCORE_V4_DECISION.md`](../../../docs/MALLISCORE_V4_DECISION.md)

**V4 is shipped for actual outings.** V3 remains frozen for historical reproduction and
for projection paths that do not yet estimate HBP. V3 output is byte-identical to before
the study and pinned by `tests/test_malliscore_golden.py`.

The study selected `(H + BB) / BF`. The production follow-up compared an HBP-inclusive
Reach Rate across the same 7,479 outings and adopted `(H + BB + HBP) / BF`: zero-score
collapse remained eliminated, average score movement was 0.39 points, and daily top-five
membership changed on 8.8% of slates relative to the no-HBP candidate.

## Running it

Order matters — later phases read the dataset the first one builds.

```bash
PY=./mlb_env.nosync/bin/python
$PY research/study/malliscore_validation/build_dataset.py        # ~2 min, parses raw feeds
$PY research/study/malliscore_validation/audit_architecture.py
$PY research/study/malliscore_validation/reliability_benchmark.py
$PY research/study/malliscore_validation/weight_sensitivity.py
$PY research/study/malliscore_validation/predictive_signal.py
$PY research/study/malliscore_validation/decide_v4.py
```

Everything is seeded (`SEED = 20260802`) and reruns to identical numbers. Outputs land in
`outputs/`; none of it is tracked.

## Modules

| file | role |
|---|---|
| `power_guard.py` | verdicts for every statistic — see below |
| `game_score.py` | Bill James GSv1 (lifted from two duplicated copies) and Tango GSv2 (new) |
| `build_dataset.py` | Phase 1 — one row per starter outing, 2024–2026 |
| `audit_architecture.py` | Phase 2 — priors, clamps, influence, overlap, WHIP, fusion, workload, normalization |
| `reliability_benchmark.py` | Phase 3 — split-half reliability, Game Score agreement, disagreement cases |
| `weight_sensitivity.py` | Phase 4 — 20,000 weight vectors over the feasible region |
| `predictive_signal.py` | Phase 5 — nested next-start models |
| `decide_v4.py` | Phase 6 — candidates, decision, validation, confirmation |
| `make_golden.py` | regenerates the V3 golden fixture; run only for a deliberate V3 change |

## The power guard

The study runs on existing box-backed coverage rather than re-ingesting missing feeds, so
every statistic declares whether it was actually resolvable. Two thresholds, kept separate:
`mde` (what the sample size can detect) and `meaningful` (what would change a decision).
Conflating them makes `NULL_CONFIRMED` unreachable, since at any n the CI half-width is
roughly the MDE.

| verdict | meaning |
|---|---|
| `RESOLVED` | CI excludes the null and the effect is large enough to matter |
| `RESOLVED_TRIVIAL` | statistically real, practically negligible — must not drive V4 |
| `NULL_CONFIRMED` | CI is tight and excludes any meaningful effect. A finding, not a failure |
| `UNDERPOWERED` | the test could not answer. **May not inform V4** |

Two tests came back `UNDERPOWERED`; both are listed in the decision memo and neither
informed V4.

## Season roles

| season | outings | pitchers ≥10 starts | role |
|---|---|---|---|
| 2024 | 1,908 | 92 | development — all exploration and tuning |
| 2025 | 2,520 | 126 | validation — read once, at the Phase 6 gate |
| 2026 (thru 7/26) | 3,051 | 152 | confirmation — read once, at the end |

Coverage is non-random in time: earned runs come only from raw `feed_live` files, which
cover 40% of 2024 games and 54% of 2025.

## Headline results

- **The weights were never the problem.** Rank agreement never drops below Spearman 0.963
  anywhere in the feasible region; 97% of it agrees above 0.98. The weights are weakly
  identified and V4 leaves them alone.
- **The priors were.** `game_whip` assumed σ 0.50 against an observed 1.01, inflating its
  realized weight to 0.56 against a nominal 0.40.
- **76 outings scored exactly 0** across three seasons, because WHIP blew past the z-clamp
  and the harmonic mean zeroed the result. In 2024 those 22 outings spanned a 40-point
  Game Score range. V4 eliminates this in all three seasons.
- **MalliScore is more reliable than Game Score**: +0.115 [+0.069, +0.172], RESOLVED. The
  marginal CIs overlap; the paired test is the correct one and it resolves.
- **MalliScore predicts nothing about the next start** beyond recent form — and neither
  does Game Score.
- **Two published documents describe a workload model that never shipped** (21 outs vs the
  actual 18). Correction notices added; original text preserved as the record.

## Known issues this study did not fix

- xwOBA allowed is a *result* sitting in the pillar named for *process*. Moving it drops
  pillar correlation from 0.567 to 0.395. The strongest candidate for V5.
- Slate-refined norms are used by `render.py:438` alone while every other consumer uses
  fixed priors, so the daily board and shadow ledger are on different scales. The study
  recommends standardizing on fixed norms; V4 does not bundle it.
- `src/pitching_performances/malli_score 2.py` is a byte-identical dead copy.
- Ace/bust cutoffs are duplicated across four files instead of imported.
