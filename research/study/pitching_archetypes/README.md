# Pitching Archetypes

Do pitchers fall into natural types — the power arm, the crafty deceiver, the
lefty who lives off a fastball/changeup pair — and can those types be recovered
from pitch data rather than asserted from the eye test?

The study is deliberately staged so that each level can return a negative answer
and stop. **Level 1 did exactly that**, and the finding is worth more than the
taxonomy would have been: pitch shape space is a *continuum*, not a set of
discrete families. See [`RESEARCH_LOG.md`](RESEARCH_LOG.md) — every design
decision and finding is numbered there and nothing is edited after the fact.

**Levels**

| level | question | status |
|---|---|---|
| L0 | design decisions before touching data | done |
| L1 | does a pitch-shape taxonomy exist? | **done — no, it's a continuum** |
| L2 | represent each pitcher's arsenal as a distribution over shape space | frontier |
| L3 | do *pitchers* cluster, and do the clusters mean anything? | open |
| L4 | does an archetype label add signal over the features it came from? | open |

L3 is a genuinely separate question from L1: a continuous pitch surface does not
prevent pitchers from clustering, because arsenal *combinations* are constrained
by what is physically compatible.

## Running it

Order matters — later phases read the cache the first one builds.

```bash
PY=./mlb_env.nosync/bin/python
$PY research/study/pitching_archetypes/build_pitch_dataset.py --years 2025  # ~3 min
$PY research/study/pitching_archetypes/explore_shape_space.py               # ~6 min
$PY research/study/pitching_archetypes/stability_vs_null.py                 # ~5 min
$PY research/study/pitching_archetypes/render_l1_figures.py
```

Seed is `20260827` everywhere; reruns reproduce identical numbers. Outputs land
in `outputs/` and are untracked.

## Modules

| file | role |
|---|---|
| `features.py` | handedness mirroring, shape/identity feature blocks, seed |
| `build_pitch_dataset.py` | Phase 1 — pitch-level shape matrix from the enriched parquets |
| `explore_shape_space.py` | Phase 2 — mirroring audit, PCA, k sweep over both feature blocks |
| `stability_vs_null.py` | Phase 2b — bootstrap stability against a marginal-preserving null |
| `render_l1_figures.py` | Phase 2c — the two Level-1 figures |

## Data

Reads `data/warehouse/mlb/<year>/regular_season/pitches_enriched/*.parquet`
directly. The existing `research/study/.cache/pitch_rows_v5*` cache **cannot** be
reused: it carries no movement columns at all — no `pfx_*`, no `spin_axis`, no
`arm_angle`. Phase 1 writes its own cache to
`research/study/.cache/archetype_pitches_<years>.parquet`.

2025 coverage on every shape column is 99.4–99.6%. Nothing is imputed; pitches
missing a shape column are kept in the cache and dropped at the modelling step so
the loss stays visible.

## Method notes worth carrying to any writeup

Three things decided the Level-1 result, and each is a trap the obvious approach
falls into:

1. **Mirror handedness, don't cluster on it.** Statcast horizontal quantities are
   signed in the catcher's frame. Unmirrored, the geometry partly encodes a fact
   already known from a single column (L0-D2, L1-F1).
2. **Bootstrap stability cannot select k on its own.** Fewer clusters are
   mechanically easier to reproduce across resamples, so raw ARI always favours
   small k. Reading its maximum here would have selected k=4 for reasons having
   nothing to do with baseball (L1-F3).
3. **Compare against a null that keeps the marginals.** Shuffling each feature
   column independently preserves all four distributions and destroys only the
   joint structure. Real pitch data beat that null by almost nothing — and lost
   to it at four values of k (L1-F5).

## Outputs

| file | contents |
|---|---|
| `l1_q1_mirroring_audit.csv` | handedness AUC and PC1 correlation, raw vs mirrored frame |
| `l1_q2_pca_variance.csv`, `l1_q2_pca_loadings.csv` | PCA of the mirrored shape space |
| `l1_q3_k_sweep.csv` | silhouette, GMM BIC, bootstrap ARI per k, both feature blocks |
| `l1_q3b_stability_vs_null.csv` | real vs null bootstrap stability and excess, per k |
| `l1_q4_family_vs_pitchtype.csv` | k=4 family centroids, purity, and top-two `pitch_type` |
| `l1_shape_continuum.png` | density of the mirrored movement space + label ellipses |
| `l1_stability_vs_null.png` | the stability-vs-null curve |
