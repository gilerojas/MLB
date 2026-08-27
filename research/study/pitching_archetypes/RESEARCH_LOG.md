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

## L2 — Representing an arsenal

**Data.** 2024, 2025 and 2026 pooled: 1,941,083 pitches after filtering.
2023 and earlier are **not available** — those seasons were never enriched, so
`pitches_enriched/` is empty for 2021–2023. The study is a three-season study.

**1,475 pitcher-seasons** clear the 400-pitch floor, from 773 distinct pitchers;
**581** of those are starter-seasons (≥80% of pitches thrown as SP).

### L2-D1 · The 16-cell basis is a coordinate system, not a taxonomy
L1 killed hard family labels, but a pitcher can still be described by *where* his
pitches sit. A 16-component Gaussian mixture is fit once over the pooled seasons
and every pitch receives a vector of membership weights rather than a label. K=16
was chosen as a resolution — the point where L1's stability had clearly broken
down (ARI < 0.9), so no one can mistake it for a claim about how many pitch types
exist — fine enough to resolve arsenal shape, coarse enough that each cell still
holds thousands of pitches. **No downstream phase may treat a cell as a pitch
type.**

### L2-D2 · Fit the basis once, pooled across seasons
A basis refit per season would make coordinates incomparable year to year and
would silently destroy the persistence tests that L2-F1 and L3 both depend on.

### L2-D3 · Split the distribution by batter handedness
Each pitcher-season carries the pooled 16-cell vector plus separate vectors
`vsL` and `vsR`. Half of the original intuition — the lefty who lives off a
changeup does so *against right-handed hitters* — is invisible in the pooled
vector and lives entirely in this split.

### L2-D4 · Identity features enter here
`arm_angle`, `release_extension` and release point were held out of L1 by L0-D3
because they describe the pitcher rather than the pitch. At L2 that is exactly
what is wanted. Added alongside them: `fb_velo` (95th pct), `slow_velo`
(10th pct), `velo_spread`, `arsenal_breadth` = exp(entropy) of the cell vector,
and movement dispersion. `arsenal_breadth` reads as "how many pitches does he
really have" and, unlike a count of distinct `pitch_type` values, does not
depend on the label conventions L1 discredited.

### L2-D5 · 2026 `arm_angle` is a known ingest gap, carried not imputed
Coverage by season: 2024 **99.2%**, 2025 **99.5%**, 2026 **3.6%**. The in-season
2026 pipeline is not populating `arm_angle`. Every pitcher-season therefore
carries an `arm_angle_cov` column, and **L3 must gate on it** rather than
consume a median drawn from a handful of pitches. 2026 is also partial — the
cache ends 2026-08-13. Not imputed, not dropped, just visible.

### L2-F1 · The representation is a fingerprint ✅ *(decisive)*
The load-bearing question at this level: is an arsenal profile a stable property
of a pitcher, or is it noise? A pitcher's profile in one season must resemble
**his own** profile the next season more than it resembles a stranger's. Cosine
similarity on the pooled cell distribution:

| comparison | n | mean cosine | sd |
|---|---|---|---|
| same pitcher, consecutive seasons | 702 | **0.936** | 0.057 |
| different pitchers | 3,503 | **0.475** | 0.211 |
| separation | | **Cohen's d = 2.98** | |

An enormous gap. Arsenals are both highly stable within a pitcher and highly
distinctive between pitchers, which is precisely the precondition L3 needs. Note
what this does *not* say: that pitchers fall into groups. It says each pitcher
has a consistent, individual signature — a necessary condition for archetypes,
not evidence of them.

### L2-F2 · Profiles are interpretable on inspection ✅
2025 spot check against known pitcher identities:

| pitcher | arm angle | fb velo | velo spread | breadth | top cell |
|---|---|---|---|---|---|
| Tyler Rogers | **−60.7°** | 84.7 | 10.8 | **2.99** | 80mph, glove-side, sinking |
| Kyle Hendricks | 42.9° | 87.5 | **9.5** | 6.83 | 87mph arm-side run, 40% |
| Clayton Kershaw | 54.9° | 90.0 | 17.5 | 8.07 | 91mph neutral, 32% |
| Chris Sale | **8.3°** | 96.9 | 19.7 | 8.09 | 80mph sweep, 22% |
| Logan Gilbert | 39.7° | 96.6 | 15.1 | 6.71 | 87mph neutral, 28% |
| Paul Skenes | 23.4° | 99.1 | 14.9 | 9.58 | 95mph arm-side ride, 28% |
| Tarik Skubal | 49.7° | 99.1 | 12.3 | **10.01** | 96mph ride, 21% |

Rogers' negative arm angle correctly recovers a submariner, and his breadth of
3.0 correctly recovers a three-pitch reliever. Hendricks has the narrowest
velocity spread in the group. Sale's 8.3° recovers a genuine low-slot lefty.
Nothing here was tuned — these fall out of the pipeline.

### L2-Q1 · Open question for L3
The `vsL`/`vsR` split triples the feature width (16 pooled + 16 + 16, plus
identity and velocity blocks) against only 581 starter-seasons. L3 must reduce
dimension before clustering or it will fit noise, and the reduction has to happen
*inside* any resampling loop, not before it, or the null test is contaminated.

---

## L3 — Do pitchers cluster?

**Data.** 2024 + 2025 only. 2026 is excluded: `arm_angle` is 3.6% covered
(L2-D5) and the local cache is partial through 2026-08-13 because in-season
ingest moved to the VPS and the local copy stopped updating. **275 starters**
(≥80% of pitches as SP), 136 of them present in both seasons, 57 features.

### L3-D1 · One row per pitcher, not per pitcher-season
L2-F1 measured within-pitcher, across-season similarity at cosine 0.936 — a
pitcher's two seasons are near-duplicates. Left as separate rows they would land
on both sides of every bootstrap split and inflate stability for reasons having
nothing to do with clustering. Seasons are averaged to one row per pitcher.

### L3-D2 · Centered log-ratio on the compositional blocks
The cell vectors sum to one, so their components are not free to vary
independently and plain Euclidean distance on them is not meaningful. CLR maps
each composition into real space where it is. GMM responsibilities are strictly
positive, so no zero-handling is needed beyond an underflow floor.

### L3-D3 · Dimension reduction inside the resampling loop
57 features against 275 rows. PCA (10 components, 83.9% of variance) is refit on
whatever rows it is handed, so inside a bootstrap it is refit per split. Fitting
it once on all rows would let a held-out half inform its own projection and
quietly contaminate every stability number.

### L3-D4 · Three nulls, and only the third one tests the question
This is the methodological core of the level, and getting it wrong would have
produced a false discovery.

- **Null A** — every column shuffled independently. Destroys all joint structure.
- **Null B** — whole blocks shuffled between pitchers, each block kept intact.
  Every pitcher gets some real pitcher's pooled arsenal, another's `vsL`, and so
  on. Destroys only the association *between* blocks.
- **Null C** — a single multivariate Gaussian with the real data's mean and
  covariance. Keeps every correlation exactly and contains **no clusters
  anywhere by construction.**

Nulls A and B are the obvious choices and they are nearly useless here. Beating
them establishes only that pitcher features co-vary, which was never in doubt.
Null C is the only one that isolates the actual question: *is this cloud lumpy,
or is it one smooth cloud?*

### L3-F1 · Pitchers do not form discrete archetypes ❌ *(decisive)*
| k | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| real | .63 | .61 | .54 | .66 | .55 | .49 | .50 | .49 | .45 | .45 | .43 |
| Null A | .14 | .13 | .14 | .14 | .14 | .14 | .14 | .14 | .16 | .13 | .14 |
| Null B | .34 | .37 | .35 | .29 | .27 | .27 | .25 | .28 | .25 | .26 | .25 |
| **Null C** | **.79** | **.55** | **.61** | **.55** | **.44** | **.41** | **.36** | **.35** | **.34** | **.32** | **.31** |
| excess vs C | −.16 | +.06 | −.06 | +.12 | +.11 | +.08 | +.15 | +.14 | +.12 | +.13 | +.12 |

Real pitchers crush Nulls A and B — which, read alone, looks exactly like the
discovery the study set out to find. Against Null C it evaporates. Mean excess
across k is **+0.073**, and the real data **loses outright at k=2 and k=4**.

Silhouette says the same thing independently: real 0.117–0.153 against the
Gaussian cloud's 0.096–0.156. At k=2 the structureless cloud scores *higher*
than the real pitchers (0.156 vs 0.153).

**Verdict: pitcher archetypes, as discrete types, do not exist.** The groups
k-means returns are slices of one continuous cloud. Inspecting them confirms it —
at k=8 the groups differ mainly by arm angle (33° → 53°) and fastball velocity
(92.6 → 97.0), which are two continuous knobs, not eight kinds of pitcher.

This is the same result as L1, one level up. Pitches lie on a continuum; so do
the pitchers built out of them.

### L3-F2 · The continuum has named axes, and those are usable ✅
A cloud with no clusters still has shape. PCA on the pitcher matrix, correlated
against interpretable features:

| axis | var | reads as | low end | high end |
|---|---|---|---|---|
| **PC1** | 24.1% | **slot & separation plane** — arm angle +0.62, vertical movement spread +0.66, horizontal spread −0.66 | Alex Wood, Tanner Houck, Landen Roupp | Tyler Glasnow, Dylan Cease, Triston McKenzie |
| **PC2** | 13.7% | **horizontal spread** — mov_x_sd +0.56, arm angle −0.32 | Marco Gonzales, Lucas Giolito, James Paxton | Dustin May, Clarke Schmidt, Joe Boyle |
| **PC3** | 10.0% | **raw velocity** — fb_velo +0.60 | Chris Flexen, Clayton Kershaw, Mitch Spence | Luis Castillo, Cam Schlittler, Keaton Winn |
| PC4 | 8.3% | vertical spread, inverted | | |

The four axes together carry only 56% of variance — diffuse, which is itself
consistent with a continuum rather than a small number of types. But the axes
are real, stable and interpretable, and they are what replaces the archetype
label: **a pitcher gets coordinates, not a category.**

### L3-Q1 · What L4 must now ask
The original motivation was never the label for its own sake — it was whether
knowing a pitcher's type conditions anything. That question survives L3-F1
intact, because coordinates condition things just as well as categories do, and
carry more information. L4 should test the continuous axes, not a cluster ID:
does position on PC1/PC3 interact with opposing-lineup handedness or with
`n_thruorder_pitcher` to predict `delta_run_exp` beyond the pitcher's own
rate stats? A negative there would close the study; a positive one is a new
feature family for the starter projection ceiling
(see `research/study/BETTING_MARKET_VALUE_RESEARCH.md`).

---

## Status

L1, L2, L3 complete. **Two decisive negatives and one usable positive:** neither
pitches nor pitchers form discrete types, and the right representation of a
pitcher is a position on a continuum. Frontier: **L4** — does that position
predict anything?
