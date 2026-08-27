# Pitching Archetypes

Do pitchers fall into natural types — the power arm, the crafty deceiver, the
lefty who lives off a fastball/changeup pair — and can those types be recovered
from pitch data rather than asserted from the eye test?

The study is deliberately staged so that each level can return a negative answer
and stop. Three of the four did. See [`RESEARCH_LOG.md`](RESEARCH_LOG.md) — every
design decision and finding is numbered there, and nothing is edited after the
fact.

**The answer is no, and the no is the result.** Neither pitches nor pitchers fall
into discrete types; both lie on continua. What survives is better than a
taxonomy: pitchers get stable, interpretable **coordinates** rather than labels,
and those coordinates carry real information about what a pitcher does —
predicting a third of ground-ball rate that pitcher quality predicts none of.
They do not, however, predict how a pitcher's edge shifts by matchup, and the
matchup targets turn out to be too noisy per-pitcher to have answered that
either way.

**Levels**

| level | question | status |
|---|---|---|
| L0 | design decisions before touching data | done |
| L1 | does a pitch-shape taxonomy exist? | **done — no, it's a continuum** |
| L2 | represent each pitcher's arsenal as a distribution over shape space | **done — profiles are a stable fingerprint** |
| L3 | do *pitchers* cluster, and do the clusters mean anything? | **done — no, a continuum again** |
| L4 | does position on the continuum predict anything? | **done — traits yes, matchups no** |

L3 was a genuinely separate question from L1 — a continuous pitch surface does
not prevent pitchers from clustering, since arsenal *combinations* are
constrained by what is physically compatible. It came back negative too. What
survives is better than a label: pitchers get **coordinates** on a small number
of interpretable axes (slot and separation plane, horizontal spread, raw
velocity), and coordinates condition downstream analysis at least as well as
categories while carrying more information.

## Running it

Order matters — later phases read the cache the first one builds.

```bash
PY=./mlb_env.nosync/bin/python
# L1 -- pitch shape space
$PY research/study/pitching_archetypes/build_pitch_dataset.py --years 2025  # ~3 min
$PY research/study/pitching_archetypes/explore_shape_space.py               # ~6 min
$PY research/study/pitching_archetypes/stability_vs_null.py                 # ~5 min
$PY research/study/pitching_archetypes/render_l1_figures.py

# L2 -- arsenal profiles
$PY research/study/pitching_archetypes/build_pitch_dataset.py --years 2024 2025 2026  # ~9 min
$PY research/study/pitching_archetypes/build_arsenal_profiles.py            # ~4 min

# L3 -- do pitchers cluster?
$PY research/study/pitching_archetypes/cluster_pitchers.py                  # ~2 min
$PY research/study/pitching_archetypes/render_l3_figures.py

# L4 -- does position predict anything?
$PY research/study/pitching_archetypes/predict_from_position.py             # ~3 min
$PY research/study/pitching_archetypes/render_l4_figures.py
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
| `build_arsenal_profiles.py` | Phase 3 (L2) — pitcher-season profiles over the soft basis |
| `cluster_pitchers.py` | Phase 4 (L3) — pitcher clustering against three nulls |
| `render_l3_figures.py` | Phase 4b — the Level-3 figure |
| `predict_from_position.py` | Phase 5 (L4) — target reliability, then prediction from coordinates |
| `render_l4_figures.py` | Phase 5b — the Level-4 figure |

## Data

Reads `data/warehouse/mlb/<year>/regular_season/pitches_enriched/*.parquet`
directly. The existing `research/study/.cache/pitch_rows_v5*` cache **cannot** be
reused: it carries no movement columns at all — no `pfx_*`, no `spin_axis`, no
`arm_angle`. Phase 1 writes its own cache to
`research/study/.cache/archetype_pitches_<years>.parquet`.

Only **2024, 2025 and 2026** are usable. `pitches_enriched/` is empty for
2021–2023 — those seasons were never enriched — so this is a three-season study.

2024/2025 coverage on every shape column is 99.2–99.6%. Two gaps are carried
rather than imputed, both visible in the outputs:

- **2026 `arm_angle` coverage is 3.6%** (vs 99%+ in 2024/2025) — the in-season
  2026 pipeline is not populating it. Each pitcher-season carries an
  `arm_angle_cov` column and L3 gates on it (L2-D5).
- **2026 is partial**, ending 2026-08-13.

Nothing is imputed; pitches missing a shape column are kept in the cache and
dropped at the modelling step so the loss stays visible.

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
4. **Measure a target's reliability before trying to predict it.** An individual
   starter's platoon split is ~92% noise over two seasons and his
   third-time-through penalty ~81%. Failing to predict those says nothing about
   the predictor — the ceiling is the honest denominator, and without it a null
   result is uninterpretable (L4-D1, L4-F1).
5. **A shuffle null is not enough to claim clustering.** At L3 the real pitchers
   beat both shuffle nulls decisively, which looks like a discovery and is not
   one — it only proves the features co-vary. The test that settles it is a
   single Gaussian with the real covariance: it keeps every correlation and has
   no clusters in it by construction. Real pitchers did not beat it (L3-D4,
   L3-F1).

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
| `l2_basis_centroids.csv` | the 16-cell soft basis in readable units — a coordinate system, not pitch types |
| `l2_arsenal_profiles.parquet` / `.csv` | one row per pitcher-season: cell distribution (pooled, vsL, vsR), arm angle, extension, release point, velocity envelope, arsenal breadth |
| `l2_fingerprint_check.csv` | L2-F1 — same-pitcher vs different-pitcher profile similarity |
| `l3_cluster_stability.csv` | per k: silhouette and bootstrap ARI for real data and all three nulls |
| `l3_continuum_axes.csv` | L3-F2 — principal axes of the pitcher cloud, correlated against readable features |
| `l3_cluster_summary.csv`, `l3_pitcher_assignments.csv` | descriptive k=8 grouping (did **not** survive the nulls) |
| `l3_pitchers_vs_nulls.png` | the decisive Level-3 figure |
| `l4_target_reliability.csv` | split-half reliability per target — the ceiling any model can reach |
| `l4_prediction.csv` | cross-validated R² for baseline, baseline+coordinates, and coordinates alone |
| `l4_ceiling_vs_achieved.png` | the Level-4 figure |
