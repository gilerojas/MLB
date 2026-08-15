# Building MalliScore - Article Research Packet

## Article brief

**Working title:** Building MalliScore: A Modern Way to Evaluate a Pitching Performance

**Article type:** Metric or method article

**Primary audience:** Curious baseball fans who understand the box score and common rate statistics but do not need to know model-building terminology.

**Reader question:** What does MalliScore add to a pitching line or Game Score, and has its design been tested?

**Primary destination:** X Articles

**Desired reader response:** Understand why MalliScore rewards active lineup control,
recognize that run prevention and dominance are related but not interchangeable, and know
what to inspect when MalliScore disagrees with a traditional pitching line.

**Notice:** A pitching result and the way the pitcher produced it are related, but not identical.

**Thesis:**

- The conventional view says the pitching line summarizes the start.
- That misses that clean run prevention can be produced through contact management or through active lineup control.
- The evidence shows that MalliScore separates those paths with dominance, run prevention, and workload, while remaining a reliable descriptive second opinion.
- This matters because disagreements with Game Score reveal whether a start was driven more by the result, contact management, or the pitcher's ability to power through hitters.

**Primary reader value:** Clarity

**Secondary reader value:** Reference

**Scope:** The construction, validation, interpretation, and limitations of MalliScore for individual MLB starting-pitcher outings.

**Boundary:** The article does not claim that MalliScore measures true talent, predicts the next start, replaces Game Score, or has uniquely optimal weights.

**Best real example and narrative spine:** Shota Imanaga vs Pittsburgh on May 18, 2024,
and Michael Wacha vs the White Sox on July 19, 2024. The article opens with this comparison,
returns to it after the formula, and closes with its practical question.

**Opening example:** Shota Imanaga (2024-05-18 vs PIT) and Michael Wacha (2024-07-19 vs CWS).
Identical traditional lines — 7.0 IP, 4 H, 1 BB, 0 ER, 7 K — and an identical Game Score v2 of
79, but MalliScore 72.2 against 59.5. The whole gap sits in Dominance (69.4 vs 49.3), driven by
swinging strikes (25.0% vs 8.4%) and chase (42.5% vs 16.3%). Wacha was better on xwOBA allowed
(.236 vs .262) and called strikes (20.0% vs 14.8%), so the case is "different excellence," not
"one pitcher was worse." Official boxscores confirm HBP = 0 in both starts, so this comparison
also holds on the current Reach Rate Allowed production formula. The article must distinguish
contact management from active lineup control without claiming either start proves a higher future
floor or ceiling.

**Research-only examples:** Matthew Liberatore remains the verified calculation example.
Lance Lynn and Blair Henley remain the verified lower-tail comparison. Neither appears in
the X draft because adding more cases weakened the single Imanaga-Wacha narrative spine.

**Best disagreement pattern:** MalliScore-favored starts were longer and generated more whiffs but allowed more earned runs; Game Score-favored starts were shorter and cleaner on the scoreboard.

**Human context beat:** Bill James created Game Score to summarize an individual start; Tom Tango later updated its run and home-run treatment. MalliScore is presented as a second opinion, not a dismissal of that work.

**Reader payoff:** MalliScore gives curious fans a clearer explanation of how a start was
built. In roto and category leagues, it gives fantasy managers one postgame view of the
workload, bat-missing, and run-prevention balance behind innings or quality starts,
strikeouts, ERA, and WHIP. It is not a projection or start-sit instruction.

**Interpretation bands:** V4 production scores, 13,028-start complete-season sample. Median 44.4;
50+ is the top 35%, 60+ is the top 12%, and 70+ is the top 2%. Public wording: 50 to
59 is a strong start, 60 to 69 is elite, and 70+ is rare territory. Do not call 50
average or treat 100 as a realistic target.

## Claim ledger

| Claim | Type | Evidence | Public wording constraint |
|---|---|---|---|
| Dataset contains 13,028 starts | DATA | Complete 2024/2025 RS + 2026 through Aug 13 | State season roles and cutoff |
| 76 V3 outings scored exactly 0 | DATA | V4 decision memo | Explain the WHIP/clamp mechanism |
| V4 has zero exact-zero collapses in all three seasons | DATA | V4 validation table | Call this the primary V4 justification |
| Weight sensitivity never fell below Spearman .965 | DATA | 20,000-vector V4 study, 2024 dev season | Do not claim weights are optimal; state the season |
| Production reliability advantage vs GSv2 is +.118 | DATA | Paired bootstrap on V4 scores, n=520 | Define reliability; do not call it accuracy |
| V3 reliability advantage vs GSv2 was +.115 | DATA | Paired bootstrap | Superseded in the article by the production number |
| V4 reliability gains shrink by season | DATA | V4 validation table | Do not sell V4 as a large reliability leap |
| Neither score predicts the next start beyond form | DATA | Four NULL_CONFIRMED tests | Call MalliScore descriptive |
| RRA is cleaner than WHIP for this role | INFERENCE | Tail behavior and denominator analysis | Say cleaner for this job, not universally superior |
| xwOBA may belong in Run Prevention | INFERENCE | Candidate D audit | Present as open V5 question |
| Game Score remains useful | INFERENCE | Strong agreement and established role | Represent the alternative fairly |

## Verified worked example

Matthew Liberatore vs TOR, 2026-08-02:

```text
6.0 IP, 85 pitches, 1 H, 1 BB, 0 HBP, 0 ER, 0 HR, 7 K, 20 BF
SwStr% 12.941
Called Strike% 20.000
Chase% 39.583
xwOBA allowed .148391
RRA .100
Dominance 66.3
Run Prevention 73.8
Core 69.8
Workload 1.003
MalliScore V4 70.0
MalliScore V3 67.2
```

Source: production VPS warehouse, generated through `scripts/pitching_performances_daily.py` and `malliscore_v4()` on 2026-08-11.

## Study facts retained in the draft

- Development: 2024 complete regular season, 4,749 outings.
- Validation: 2025 complete regular season, 4,764 outings.
- Confirmation: 2026 through August 13, 3,515 outings.
- V3 exact zeroes: 22, 25, and 29 by season.
- V4 exact zeroes: 0, 0, and 0.
- V4 reliability vs V3: +.020, +.024, +.013 by season; each paired gap resolved, but all are small.
- Spearman agreement with Game Score v2: .933, .929, .928 by season.
- V3 MalliScore vs Game Score v2 paired reliability gap: +.115, 95% CI +.069 to +.172.

### Recomputed on the production formula for the article (2026-08-13)

Real per-outing HBP was extracted from the raw `feed_live` boxscores and all 13,028 study
outings were scored through `malliscore_v4()`. The complete-season rebuild is the new
article sample. The production V4 norms remain frozen.

- Score distribution: median 44.4, p90 61.2, p99 72.5, max 87.4, min 5.0, zero exact zeroes.
- Split-half reliability, pooled 2024-2026, min 10 starts (n=520): MalliScore V4 .691, GSv2 .573.
- Paired reliability gap vs GSv2: **+.118, 95% CI +.082 to +.159**, resolved.
- Season-level Spearman agreement with GSv2: .933 / .929 / .928.
- MalliScore-favored disagreement cases, all seasons: n=746, SwStr 12.8%, 5.9 IP, 3.2 ER.
- Game Score-favored disagreement cases, all seasons: n=806, SwStr 10.5%, 4.7 IP, 1.1 ER.
- Opening examples: Henley 2024-04-08 = 11.2 (dom 33.0, RP 16.6, workload .509);
  Lynn 2024-07-06 = 11.4 (dom 28.6, RP 13.4, workload .624). Only 40 starts scored below 12.
- Liberatore's 70.0 sits at the 98th percentile of the study window.

Superseded 2024-only V3 disagreement figures, retained for provenance:
n=109 SwStr 13.1% / 17.8 outs / 3.1 ER, and n=99 SwStr 10.1% / 14.4 outs / 1.0 ER.

### Why Dominance earns half the score (added 2026-08-13)

Split-half reliability by input, pooled 2024-2026, min 8 starts. This is the article's
strongest justification for the architecture and now appears as a table in the Dominance
section.

SwStr% .810 · Chase% .683 · Called strike% .684 · Outs .706 · xwOBA allowed .589 ·
RRA .454 · Earned runs .371 · Home runs .266 · Dominance pillar .748 ·
Run Prevention pillar .420 · MalliScore V4 .663 · Game Score v2 .545.

Reading: swing-and-miss is the most repeatable thing a starter does; runs and home runs are
the least. Game Score is built almost entirely from the volatile inputs, which is the
mechanism behind MalliScore's reliability edge.

Supporting: SwStr% and called-strike% correlate at -.256 per start and -.387 per
pitcher-season, so the two Dominance paths are genuinely distinct rather than one signal.

### REJECTED: "whiffs raise a pitcher's floor" (tested 2026-08-13)

Hypothesis: an edge in swing-and-miss should give a pitcher a higher floor than
contact-management does. **Not supported. Must not appear in the article.**

Unmatched, the effect looks real — 5+ ER starts occur in 12.7% of high-whiff pitcher-seasons
against 17.4% of low-whiff. But that is a quality confound. Holding season-long earned runs
roughly constant, it flattens or reverses: good-ER 6.4% vs 7.5%, mid-ER 16.0% vs 15.1%
(reversed), poor-ER 21.9% vs 22.7%.

The MalliScore-floor version of the claim is additionally circular: MalliScore weights SwStr%
at 30% of Dominance, so high-whiff pitchers post a higher MalliScore floor by construction.
Publishing it would be measuring the formula against itself, and it would contradict the
article's own next-start null result.

### Known blind spot now stated in limitations

Neither Dominance path credits inducing weak contact directly. Pitcher-seasons pairing
top-third run prevention with bottom-third dominance: 7 of 191 (>=15 starts). Small but real,
and MalliScore under-serves them. Michael Wacha's opening start is a single-game instance of
the same archetype, which is why the opening comparison is framed as different excellence
rather than one pitcher being worse.

## Adversarial review checklist

- Do not imply the 20,000-vector analysis discovered the correct weights.
- Do not describe split-half reliability as accuracy or predictive power.
- Keep the V3 Game Score reliability result distinct from the smaller V4-over-V3 gains.
- Do not claim RRA is a universal replacement for WHIP.
- State that xwOBA in Dominance is a known conceptual weakness.
- State that 2024 and 2025 include every played regular-season game; 2026 is through August 13.
- State that two Game Score comparisons were underpowered.
- Preserve the descriptive-versus-predictive boundary.

## Visual plan

1. **Cover:** MalliScore title with a simplified Dominance / Run Prevention / Workload architecture. No dense formula on the cover.
2. **Score architecture:** Inputs flowing into the two pillars, harmonic core, then workload adjustment.
3. **Same-line comparison:** Imanaga and Wacha with identical Game Score and different process indicators.
4. **Disagreement chart:** MalliScore-favored versus Game Score-favored starts using SwStr%, outs, and ER.
5. **Interpretation histogram:** V4 score distribution with 50, 60, and 70 thresholds,
   showing why 50 is already above a typical start.

Shipped set (`assets/`, rendered by `scripts/render_malliscore_article_graphics.py`):
`01_score_architecture`, `02_game_score_disagreement`, `03_weight_sensitivity`,
`04_worked_example`, and `05_same_line`. Only `01_score_architecture`, `05_same_line`, and
`02_game_score_disagreement` belong in the X Article. `06_score_distribution` is added as
the practical interpretation graphic. The sensitivity, worked-example,
and next-start visuals remain research assets because the prose carries those secondary
findings with less friction for this audience.

## Publishing strategy

Use the X Article as the first and master editorial edition. Complete its mobile preview,
cover crop, audience setting, links, alt text, and launch package before adapting anything
for another platform.

Medium and LinkedIn decisions come after the X launch. Their future versions may preserve
the analytical core, but they are not part of the current approval scope.

Recommended order:

1. X Article
2. Medium mirror with canonical URL
3. LinkedIn article or newsletter edition

## Platform wrappers

### X launch post

I built MalliScore to answer a question the pitching line cannot fully settle:

How was the performance built?

I tested the formula across 13,028 MLB starts to see whether its components, weights, and boundaries hold up against real pitching performances.

The full method, evidence, and limits:

[X ARTICLE URL]

### Medium note

Originally published by Mallitalytics on X. The analysis and methodology are unchanged in this edition.

### LinkedIn feed introduction

What should a single-game pitching score measure?

I tested MalliScore across 13,028 MLB starts to learn whether the weights worked, where the formula failed, and what it adds beside Game Score. The most important finding was not the one I expected.
