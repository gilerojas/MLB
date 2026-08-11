# Building MalliScore - Article Research Packet

## Article brief

**Working title:** Building MalliScore: A Modern Way to Evaluate Pitching Performance

**Article type:** Metric or method article

**Primary audience:** Curious baseball fans who understand the box score and common rate statistics but do not need to know model-building terminology.

**Reader question:** What does MalliScore add to a pitching line or Game Score, and has its design been tested?

**Notice:** A pitching result and the way the pitcher produced it are related, but not identical.

**Thesis:**

- The conventional view says the pitching line summarizes the start.
- That misses how the pitcher created the result.
- The evidence shows that dominance, run prevention, and workload provide a reliable second opinion, while V4 fixes a demonstrated tail failure in the original formula.
- This matters because disagreements with Game Score reveal whether a start was driven more by process, result, or depth.

**Primary reader value:** Clarity

**Secondary reader value:** Reference

**Scope:** The construction, validation, interpretation, and limitations of MalliScore V4 for individual MLB starting-pitcher outings.

**Boundary:** The article does not claim that MalliScore measures true talent, predicts the next start, replaces Game Score, or has uniquely optimal weights.

**Best real example:** Matthew Liberatore vs Toronto, August 2, 2026. Verified from the production VPS warehouse and V4 scoring path.

**Best failure example:** Lance Lynn, July 6, 2024, and Blair Henley, April 8, 2024. Both scored 0 under V3 despite a 32-point Game Score v2 separation.

**Best disagreement pattern:** MalliScore-favored starts were longer and generated more whiffs but allowed more earned runs; Game Score-favored starts were shorter and cleaner on the scoreboard.

**Human context beat:** Bill James created Game Score to summarize an individual start; Tom Tango later updated its run and home-run treatment. MalliScore is presented as a second opinion, not a dismissal of that work.

**What to watch:** When MalliScore and the line disagree, inspect whiffs, called strikes, chases, contact quality, baserunner frequency, and completed outs.

## Claim ledger

| Claim | Type | Evidence | Public wording constraint |
|---|---|---|---|
| Dataset contains 7,479 starts | DATA | Study README and outputs | State season roles and cutoff |
| 76 V3 outings scored exactly 0 | DATA | V4 decision memo | Explain the WHIP/clamp mechanism |
| V4 has zero exact-zero collapses in all three seasons | DATA | V4 validation table | Call this the primary V4 justification |
| Weight sensitivity never fell below Spearman .963 | DATA | 20,000-vector study | Do not claim weights are optimal |
| V3 reliability advantage vs GSv2 was +.115 | DATA | Paired bootstrap | Define reliability; do not call it accuracy |
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
- MalliScore-favored disagreement cases: n=109, SwStr 13.1%, 17.8 outs, 3.1 ER.
- Game Score-favored disagreement cases: n=99, SwStr 10.1%, 14.4 outs, 1.0 ER.

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
4. **V3 to V4 repair:** Distribution of the 76 V3 zeroes and their separation under V4.
5. **Disagreement chart:** MalliScore-favored versus Game Score-favored starts using SwStr%, outs, and ER.

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

I tested the formula across 7,479 MLB starts, found where V3 broke, and rebuilt the bottom of the scale.

The full method, evidence, and limits:

[X ARTICLE URL]

### Medium note

Originally published by Mallitalytics on X. The analysis and methodology are unchanged in this edition.

### LinkedIn feed introduction

What should a single-game pitching score measure?

I tested MalliScore across 7,479 MLB starts to learn whether the weights worked, where the formula failed, and what it adds beside Game Score. The most important finding was not the one I expected.
