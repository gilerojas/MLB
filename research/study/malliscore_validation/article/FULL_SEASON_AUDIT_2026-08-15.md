# MalliScore Article Full-Season Audit

**Audit date:** August 15, 2026
**Formula audited:** MalliScore V4 production formula
**Article:** `ARTICLE.md`

## Scope

The study dataset was rebuilt from the current warehouse caches and rescored through
`malliscore_v4()` using official H, BB, HBP, and batters faced.

| Season | Starter outings | Coverage |
|---|---:|---|
| 2024 | 4,749 | Every played regular-season game |
| 2025 | 4,764 | Every played regular-season game |
| 2026 through Aug. 13 | 3,515 | In-progress season |
| **Total** | **13,028** | Complete historical regular seasons plus current 2026 window |

The only 2024 schedule record without a feed was game 746577, Astros at Guardians on
September 29. MLB marked it cancelled for rain before play, so it contains no pitcher
outing and does not reduce played-game coverage.

## Reproduced Article Claims

- Distribution: median 44.4, 90th percentile 61.2, 99th percentile 72.5, maximum 87.4,
  and zero V4 exact-zero scores.
- Interpretation thresholds: 50 or higher is the top 34.9%, 60 or higher is the top
  12.0%, and 70 or higher is the top 1.9%.
- Split-half reliability, pitcher-seasons with at least 10 starts: MalliScore V4 .691,
  Game Score v2 .573, paired gap +.118 with 95% CI +.082 to +.159, n=520.
- Seasonal Spearman agreement with Game Score v2: .933 in 2024, .929 in 2025, and .928
  in 2026 through August 13.
- Median-rank disagreement groups: MalliScore-favored starts averaged 12.8% swinging
  strikes, 5.9 innings, and 3.2 earned runs. Game Score-favored starts averaged 10.5%,
  4.7 innings, and 1.1 earned runs.
- Weight sensitivity: across 20,000 feasible V4 weight combinations in 2024, the lowest
  Spearman correlation with V4 was .965 and 96.2% exceeded .980.
- Out-of-sample 2024 to 2025 predictive tests found no meaningful MalliScore V4 gain over
  Game Score v2 for next-start SwStr%, xwOBA allowed, K-BB%, or WHIP.

## Corrections Made During Audit

1. **Game Score now includes valid 0.0-IP starts.** Brandon Pfaadt's May 31, 2025 start
   was a real 0.0-IP outing. The study previously treated it as missing. The Game Score
   implementation now rejects only missing or negative outs.
2. **Article disagreement cases are rank-based.** MalliScore and Game Score live on
   different point scales, so their disagreement is defined from within-season percentile
   ranks, not by subtracting raw scores.
3. **Weight sensitivity and predictive scripts now use V4.** Earlier script paths still
   referenced V3 columns even though the public article describes V4.
4. **Evidence graphics read generated study outputs.** The disagreement chart no longer
   relies on manually entered counts or averages.

## What Did Not Change

- The live MalliScore V4 formula, weights, workload curve, and frozen V4 norms.
- The article's central thesis: MalliScore is descriptive and distinguishes how a start
  was built. It is not a next-start projection.

## Remaining Boundary

The V4 norms remain frozen from the original 2024 development work. The completed data
validates the shipped formula but does not silently recalibrate it. Any new norms or
architectural change should be evaluated and versioned as a future formula release.
