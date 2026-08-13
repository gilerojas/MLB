# Building MalliScore - Article Research Packet

## Article brief

**Working title:** Building MalliScore: A Modern Way to Evaluate Pitching Performance

**Article type:** Metric or method article

**Primary audience:** Curious baseball fans who understand the box score and common rate statistics but do not need to know model-building terminology.

**Reader question:** What does MalliScore add to a pitching line or Game Score, and has its design been tested?

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

**Best real example:** Matthew Liberatore vs Toronto, August 2, 2026. Verified from the production VPS warehouse and V4 scoring path.

**Opening example:** Shota Imanaga (2024-05-18 vs PIT) and Michael Wacha (2024-07-19 vs CWS).
Identical traditional lines — 7.0 IP, 4 H, 1 BB, 0 ER, 7 K — and an identical Game Score v2 of
79, but MalliScore 72.2 against 59.5. The whole gap sits in Dominance (69.4 vs 49.3), driven by
swinging strikes (25.0% vs 8.4%) and chase (42.5% vs 16.3%). Wacha was better on xwOBA allowed
(.236 vs .262) and called strikes (20.0% vs 14.8%), so the case is "different excellence," not
"one pitcher was worse." Official boxscores confirm HBP = 0 in both starts, so this comparison
also holds on the current Reach Rate Allowed production formula. The article must distinguish
contact management from active lineup control without claiming either start proves a higher future
floor or ceiling.

**Secondary example (reverse direction):** Lance Lynn 2024-07-06 (11.4) and Blair Henley
2024-04-08 (11.2) — very different lines, Game Score 32 points apart, MalliScore effectively
tied at the floor. Retained as a compact paragraph inside the payoff section.

**Best disagreement pattern:** MalliScore-favored starts were longer and generated more whiffs but allowed more earned runs; Game Score-favored starts were shorter and cleaner on the scoreboard.

**Human context beat:** Bill James created Game Score to summarize an individual start; Tom Tango later updated its run and home-run treatment. MalliScore is presented as a second opinion, not a dismissal of that work.

**What to watch:** When MalliScore and the line disagree, inspect whiffs, called strikes, chases, contact quality, baserunner frequency, and completed outs.

## Claim ledger

| Claim | Type | Evidence | Public wording constraint |
|---|---|---|---|
| Dataset contains 7,479 starts | DATA | Study README and outputs | State season roles and cutoff |
| 76 V3 outings scored exactly 0 | DATA | V4 decision memo | Explain the WHIP/clamp mechanism |
| V4 has zero exact-zero collapses in all three seasons | DATA | V4 validation table | Call this the primary V4 justification |
| Weight sensitivity never fell below Spearman .963 | DATA | 20,000-vector study, 2024 dev season | Do not claim weights are optimal; state the season |
| Production reliability advantage vs GSv2 is +.137 | DATA | Paired bootstrap on production scores, n=367 | Define reliability; do not call it accuracy |
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

- Development: 2024, 1,908 outings.
- Validation: 2025, 2,520 outings.
- Confirmation: 2026 through July 26, 3,051 outings.
- V3 exact zeroes: 22, 25, and 29 by season.
- V4 exact zeroes: 0, 0, and 0.
- V4 reliability vs V3: +.030, +.023, +.012 by season; only 2025 resolved.
- Spearman agreement with Game Score v2: .939, .926, .930 by season.
- V3 MalliScore vs Game Score v2 paired reliability gap: +.115, 95% CI +.069 to +.172.

### Recomputed on the production formula for the article (2026-08-13)

Real per-outing HBP was extracted from the raw `feed_live` boxscores and all 7,479 study
outings were rescored through `malliscore_v4()`. The pipeline reproduces the published V3
paired gap exactly (+.1154, CI +.0689/+.1717), which validates the replication.

- Score distribution: median 44.5, p90 61.2, p99 72.7, max 87.4, min 5.0, zero exact zeroes.
- Split-half reliability, pooled 2024-2026, min 10 starts (n=367): MalliScore .604, GSv2 .467.
- Paired reliability gap vs GSv2: **+.137, 95% CI +.088 to +.198**, resolved.
- Season-level Spearman agreement with GSv2: .938 / .925 / .929.
- MalliScore-favored disagreement cases, all seasons: n=424, SwStr 12.9%, 5.9 IP, 3.2 ER.
- Game Score-favored disagreement cases, all seasons: n=432, SwStr 10.4%, 4.7 IP, 1.1 ER.
- Opening examples: Henley 2024-04-08 = 11.2 (dom 33.0, RP 16.6, workload .509);
  Lynn 2024-07-06 = 11.4 (dom 28.6, RP 13.4, workload .624). Only 23 starts scored below 12.
- Liberatore's 70.0 sits at the 98th percentile of the study window.

Superseded 2024-only V3 disagreement figures, retained for provenance:
n=109 SwStr 13.1% / 17.8 outs / 3.1 ER, and n=99 SwStr 10.1% / 14.4 outs / 1.0 ER.

### Why Dominance earns half the score (added 2026-08-13)

Split-half reliability by input, pooled 2024-2026, min 8 starts. This is the article's
strongest justification for the architecture and now appears as a table in the Dominance
section.

SwStr% .775 · Chase% .640 · Called strike% .610 · Outs .653 · xwOBA allowed .493 ·
WHIP .317 · Earned runs .348 · Home runs .211 · Dominance pillar .673 ·
Run Prevention pillar .389 · MalliScore .605 · Game Score v2 .487.

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
- State that earned-run coverage is incomplete and non-random.
- State that two Game Score comparisons were underpowered.
- Preserve the descriptive-versus-predictive boundary.

## Visual plan

1. **Cover:** MalliScore title with a simplified Dominance / Run Prevention / Workload architecture. No dense formula on the cover.
2. **Formula graphic:** Inputs flowing into the two pillars, harmonic core, then workload adjustment.
3. **Worked example:** Matthew Liberatore's line with the 66.3 / 73.8 pillars and 70.0 final score.
4. **Score architecture:** The two pillars, harmonic-mean core, and outs-first workload adjustment.
5. **Disagreement chart:** MalliScore-favored versus Game Score-favored starts using SwStr%, outs, and ER.

Shipped set (`assets/`, rendered by `scripts/render_malliscore_article_graphics.py`):
`01_score_architecture`, `02_game_score_disagreement`, `03_weight_sensitivity`,
`04_worked_example`. The next-start forest plot (`04_next_start_signal`) is still rendered
by the script but was pulled from the article: it visualises a null in the most technical
form available, and the prose carries that finding better for this audience.

## Publishing strategy

Use the X Article as the master edition. X supports headings, links, lists, and embedded visual media, and the existing baseball audience is already there.

Publish the same core body on Medium after the X edition is live. In Medium's advanced settings, mark the story as originally published elsewhere and set the X Article URL as the canonical source.

Use the same body for LinkedIn, preferably as the first edition of a recurring Mallitalytics analysis newsletter. Change only the short feed introduction and final platform-specific call to action; do not rewrite the analytical argument.

Recommended order:

1. X Article
2. Medium mirror with canonical URL
3. LinkedIn article or newsletter edition

## Platform wrappers

### X launch post

I built MalliScore to answer a question the pitching line cannot fully settle:

How was the performance built?

I tested the formula across 7,479 MLB starts to see whether its components, weights, and boundaries hold up against real pitching performances.

The full method, evidence, and limits:

[X ARTICLE URL]

### Medium note

Originally published by Mallitalytics on X. The analysis and methodology are unchanged in this edition.

### LinkedIn feed introduction

What should a single-game pitching score measure?

I tested MalliScore across 7,479 MLB starts to learn whether the weights worked, where the formula failed, and what it adds beside Game Score. The most important finding was not the one I expected.
