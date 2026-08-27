# Pitching Archetypes — Research Log

Append-only. Every design decision and every finding gets an ID, so a claim in an
article can be traced back to the phase that produced it. Nothing is edited after
the fact; corrections are new entries that supersede old ones by ID.

Levels:

- **L0** — design decisions taken before touching data
- **L1** — does a pitch-shape taxonomy exist? *(this log's current frontier)*
- **L2** — represent each pitcher's arsenal in shape space
- **L3** — do *pitchers* cluster, and do the clusters mean anything?
- **L4** — does an archetype label add predictive signal over the features it came from?

Seed `20260827` throughout. Every phase reruns to identical numbers.

---

## L0 — Design decisions

### L0-D1 · Unsupervised clustering, not KNN
The original sketch reached for k-nearest-neighbours. KNN is supervised and needs
labels that do not exist here. The task is unsupervised: k-means for cheap
geometry reads, Gaussian mixtures where soft membership matters (a pitcher being
70% one thing and 30% another is truer to baseball than a hard label, and is the
whole point of an archetype framing).

### L0-D2 · Mirror handedness; never cluster on it
Statcast horizontal quantities are signed in the catcher's frame, so the same
pitch has opposite signs from a LHP and a RHP. Left unmirrored, handedness — a
fact already known from a single column — competes to dominate the geometry.
All hand-signed quantities (`pfx_x`, `release_pos_x`, `plate_x`,
`attack_direction`) are flipped into a right-handed frame; `spin_axis` is
reflected across the 0–180 meridian, not negated. `p_throws` is then carried as
a *descriptor*, which is what makes it usable for matchup work later.
Implemented in `features.py:mirror_handedness`. Audited by L1-F1.

### L0-D3 · Level 1 clusters pitches on shape only
`arm_angle`, `release_extension` and `release_pos_*` are properties of the
*pitcher*, not of the pitch. Including them in a pitch-level clustering would
recover pitchers under the guise of recovering pitch families. They are held
back as `IDENTITY_FEATURES` and enter at L3.

### L0-D4 · Spin axis is circular
359° and 1° are 2° apart. Spin axis never enters a distance calculation as a raw
number; it enters as `sin`/`cos`. Superseded in effect by L1-D5, which drops it
from the core block entirely.

### L0-D5 · Ignore `pitch_type` while fitting; use it only to evaluate
`pitch_type` is a crowdsourced classifier output, not ground truth. Fitting to it
would guarantee the study rediscovers it. It is held out and used as an
evaluation axis (L1-F4).

---

## L1 — Does a pitch-shape taxonomy exist?

**Data.** 2025 regular season, all 2,430 games, 712,528 pitches from the enriched
Statcast parquets. After dropping nulls and eight junk/novelty pitch types
(EP, PO, SC, UN, FA, CS, KN, FO): **705,059 pitches, 823 pitchers**.
Shape-column coverage is 99.4–99.6%; nothing is imputed.

**Note on cache reuse.** The existing `pitch_rows_v5` study cache could not be
reused — it carries no movement columns at all (no `pfx_*`, no `spin_axis`, no
`arm_angle`). Phase 1 reads the enriched parquets directly and writes its own.

### L1-D5 · Core feature block is movement, not movement + spin axis
`spin_axis_sin` correlates **0.88** with mirrored horizontal break and
`spin_axis_cos` **−0.80** with vertical break. Spin axis is very nearly the polar
form of the movement vector; carrying both double-weights movement direction
against velocity. Rather than argue it, both blocks were swept side by side
(L1-F3) and the movement-only block was no less stable. The four-feature block —
`release_speed`, `pfx_x_mir_in`, `pfx_z_in`, `release_spin_rate` — is the core.
Spin axis is retained in the cache as a descriptor.

*Caveat carried forward:* GMM BIC is **not** comparable across the two blocks
(different dimensionality, different likelihood scale). Only the within-block BIC
curve and the cross-block ARI comparison were read.

### L1-F1 · Mirroring works, and the residual is real signal ✅
Predicting handedness from the shape features, 4-fold CV AUC:

| frame | handedness AUC | \|corr(PC1, is_LHP)\| |
|---|---|---|
| raw catcher's frame | 0.749 | 0.147 |
| mirrored RHP frame | 0.659 | **0.015** |

PC1's entanglement with handedness drops by ~10×, which is the thing that
mattered. The residual 0.659 AUC is **not** leakage — it is the genuine fact
that left-handers throw slower and lean on different shapes. That belongs in the
data. Two lessons: the raw frame was less catastrophic than assumed (0.749, not
~1.0), because both hands throw shapes spanning both signs; and mirroring buys
its benefit in the *principal axis*, not in the classifier.

### L1-F2 · The space is two-dimensional and smooth
PCA on the mirrored four-feature block:

| | PC1 | PC2 | PC3 | PC4 |
|---|---|---|---|---|
| explained var | 0.572 | 0.285 | 0.095 | 0.048 |
| cumulative | 0.572 | 0.857 | 0.952 | 1.000 |

PC1 (57%) loads −0.56 velocity, +0.56 horizontal break, −0.54 vertical break: the
classic ride-and-velocity ↔ sweep axis. PC2 (29%) is almost pure spin rate
(0.80). **86% of pitch shape lives in two dimensions.** That is the first hint
this is a surface, not a set of islands.

### L1-F3 · Every separation criterion declines monotonically in k
Silhouette peaks at the smallest k tried and falls without a knee (0.370 at k=4 →
0.210 at k=18). GMM BIC improves monotonically and never turns over. Raw
bootstrap ARI also declines monotonically. **No interior k is selected by any
criterion.**

Critically, raw bootstrap stability *cannot* select k: fewer clusters are
mechanically easier to reproduce across resamples, so ARI always favours small k.
Reading its maximum would have picked k=4 for reasons having nothing to do with
baseball. This is the trap L1-F5 was built to close.

### L1-F4 · Empirical families cut across the crowdsourced labels ✅
At k=4 on the movement block, family purity against `pitch_type` is 0.41–0.68 —
the families are *not* the labels:

| family | velo | h-break | v-break | spin | top type | second type (share) |
|---|---|---|---|---|---|---|
| 0 | 87.2 | +3.6 | 4.2 | 2410 | SL 0.57 | FC 0.29 |
| 1 | 94.5 | −9.8 | 13.7 | 2298 | FF 0.68 | SI 0.28 |
| 2 | 86.7 | −13.5 | 3.8 | 1703 | CH 0.60 | FS 0.21 |
| 3 | 81.2 | +11.0 | −5.8 | 2626 | CU 0.41 | ST 0.33 |

The cutter/slider boundary and the curveball/sweeper boundary are label
conventions, not physical discontinuities — exactly the hypothesis. The FF/SI
merge is the same story on the arm side.

### L1-F5 · Against a proper null, the space is a continuum ❌ *(decisive)*
Each k was re-run on a **marginal-preserving null**: every feature column shuffled
independently, which preserves all four marginal distributions exactly and
destroys only the joint structure. The null still clusters — k-means always
returns k partitions — so the meaningful quantity is
`excess = ARI_real − ARI_null`.

| k | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| real | .99 | .99 | .94 | .97 | .98 | .96 | .87 | .78 | .87 | .87 | .83 | .78 | .74 | .75 | .66 |
| null | .70 | .96 | .97 | .83 | .95 | .95 | .73 | .89 | .94 | .81 | .71 | .62 | .59 | .62 | .68 |
| excess | +.29 | +.03 | −.03 | +.15 | +.03 | +.01 | +.14 | **−.10** | **−.08** | +.06 | +.12 | +.16 | +.15 | +.13 | **−.03** |

The excess is small, non-monotone, and **negative at k=6, 11, 12 and 18** —
structureless noise partitions *more* stably than real pitch data at those k. No
k separates real structure from chance.

**Verdict: pitch shape space is a continuum, not a taxonomy.** The density map
(`l1_shape_continuum.png`) shows what the numbers say: an arm-side lobe and a
glove-side lobe joined by a fully populated bridge, with the `pitch_type` label
ellipses overlapping each other on top of it. There are density *ridges*. There
are no *gaps*. Discrete pitch families would require gaps.

This is a real finding, not a failed phase. It is also, independently, the
correct explanation for why public "pitch type" classifiers disagree with each
other so often: they are drawing arbitrary lines through a dense continuum.

### L1-D6 · Consequence — Level 2 uses coordinates, not labels
Hard pitch-family labels are abandoned. A pitch is represented by its **position**
in the continuous shape space (PC1/PC2, or a soft GMM responsibility vector), and
a pitcher's arsenal becomes a **distribution over that surface** rather than a
composition over discrete families. This supersedes the three-level plan's
Level-2 design.

### L1-Q1 · Open question for L3
Whether *pitches* cluster and whether *pitchers* cluster are separate questions.
Pitchers may still form discrete archetypes even on a continuous pitch surface,
because arsenal *combinations* are constrained by what is physically compatible
and teachable — you cannot pair an elite sweeper with an elite splitter from the
same slot at the same effort. L1 does not bear on this. L3 must test it with the
same null discipline, and must be allowed to return the same negative answer.

---

## Status

L1 complete. Frontier: **L2**, per L1-D6.
