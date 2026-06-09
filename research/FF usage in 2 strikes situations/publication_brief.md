# Case Study Brief: Two-Strike FF% and Pitcher Success

## Core Finding

The intuition is directionally right, but the effect is conditional rather than absolute.

Pitchers who keep throwing four-seam fastballs with two strikes tend to be a little better by K-BB%, and they tend to have better four-seam traits. But two-strike FF% by itself is not a strong success metric. It looks more like a trust/quality signal: if a pitcher is still willing to use FF in put-away counts, the pitch probably has enough velocity, shape, command, or deception to survive there.

## Evidence

2025 local sample: 132 pitchers, 431,038 pitches scanned, minimum 1,000 pitches and 150 two-strike pitches.

- 2-strike FF% vs K-BB%: Pearson r = 0.192, weighted r = 0.199.
- 2-strike FF% vs K% Pearson r = 0.191, weighted r = 0.201.
- 2-strike FF% vs FF velocity: Pearson r = 0.278, weighted r = 0.294.
- 2-strike FF% vs FF IVB: Pearson r = 0.291, weighted r = 0.335.
- 2-strike FF% vs FF HB: Pearson r = -0.051, weighted r = -0.012.
- 2-strike FF% vs FF CSW%: Pearson r = 0.278, weighted r = 0.316.
- Top-quartile two-strike FF users beat bottom-quartile users by 2.7 percentage points of K-BB%.
- Top-quartile two-strike FF users averaged 94.5 mph on FF vs 93.0 mph for the bottom quartile.

2026 local sample through May 22: 173 pitchers, 150,526 pitches scanned, minimum 300 pitches and 50 two-strike pitches.

- 2-strike FF% vs K-BB%: Pearson r = 0.120, weighted r = 0.152.
- 2-strike FF% vs K%: Pearson r = 0.196, weighted r = 0.223.
- 2-strike FF% vs FF velocity: Pearson r = 0.264, weighted r = 0.265.
- 2-strike FF% vs FF IVB: Pearson r = 0.307, weighted r = 0.288.
- 2-strike FF% vs FF HB: Pearson r = -0.115, weighted r = -0.132.
- 2-strike FF% vs FF whiff/swing: Pearson r = 0.288, weighted r = 0.317.
- 2-strike FF% vs FF CSW%: Pearson r = 0.302, weighted r = 0.310.
- Top-quartile two-strike FF users beat bottom-quartile users by 1.9 percentage points of K-BB%.
- Top-quartile two-strike FF users had a 5.9 percentage-point edge in FF whiff/swing.

## Denominator Treatment

The concern about rate reliability is valid. Thirty percent on 20 two-strike pitches is not as stable as 30% on 300 two-strike pitches.

The study now reports both:

- Unweighted correlations: every pitcher counts once.
- Weighted correlations: pitchers are weighted by their two-strike pitch count.

This does not magically make the result causal, but it reduces the influence of noisy small denominators. In both 2025 and 2026, weighting nudges the K-BB% relationship upward, which supports the idea that the signal is not only coming from low-volume noise.

## Important Twist

When grouping FF/SI/FC as a broader fastball family, the success relationship weakens or turns negative. That matters.

The four-seam specifically carries the signal better than "fastball" generally. Two-strike sinkers and cutters may be used for different jobs: weak contact, chase setup, count management, or avoiding barrels. The case study should stay focused on FF unless we explicitly split fastballs by type and role.

## X Thread Draft

1. Pitching thought I wanted to test:

   Do pitchers who trust the four-seam in two-strike counts tend to be better?

   The intuition: if you still throw FF when the hitter is protecting, your fastball probably has real traits.

2. I tested 2-strike FF% against pitcher success.

   Success metrics: K%, BB%, K-BB%, run value/100, xwOBA on BBE, whiff rate, CSW, FF velo, FF IVB/HB, FF whiff, FF CSW.

3. The answer: yes, but lightly.

   In the 2025 local sample, 2-strike FF% had a +0.19 Pearson correlation with K-BB% and +0.20 when weighted by two-strike pitch count.

4. The stronger signal was fastball quality.

   2-strike FF% correlated better with FF velocity (+0.28), FF IVB (+0.29), and FF CSW% (+0.28) than with total run prevention.

5. Top-quartile 2-strike FF users in 2025:

   K-BB%: 15.7%
   Bottom quartile: 13.0%

   FF velo:
   Top quartile: 94.5
   Bottom quartile: 93.0

6. So I would not say "throw more fastballs with two strikes."

   I would say: if a pitcher can throw FF with two strikes and survive, that tells us something about the pitch.

7. The interesting finding:

   The broader FF/SI/FC fastball-family bucket did not show the same success signal.

   Four-seam trust is different from generic fastball usage.

8. Denominator note:

   Because this is a usage rate, I also weighted correlations by each pitcher's number of two-strike pitches.

   The relationship with K-BB% got slightly stronger, not weaker.

9. Best interpretation:

   Two-strike FF% is not the cause of being good.

   It is evidence that the pitcher owns a fastball he can trust when the at-bat reaches its highest-leverage pitch-selection moment.

## Article Angle

Working title: "The Trust Pitch: What Two-Strike Four-Seam Usage Tells Us About Fastball Quality"

Structure:

1. Open with the pitching-card question: pitch mix is not just what a pitcher throws, but what he trusts when he needs the out.
2. Define the test: two-strike FF% by pitcher, compared with K-BB%, K%, BB%, RV/100, xwOBA, whiff, CSW, denominator-weighted correlations, and FF-specific quality.
3. Show the main result: weak-to-moderate positive relationship with K-BB%, stronger relationship with FF quality.
4. Add the physics layer: IVB shows a stronger relationship than HB in these samples.
5. Use examples: Skenes, Wheeler, Greene, Crochet, Skubal, Woo, Misiorowski.
6. Explain the caveat: high usage is not automatically good; bad fastballs get hidden in two-strike counts.
7. Close with the scouting/application takeaway: two-strike FF% should be read as a trust signal, then validated by velocity, ride, location, whiff, and damage allowed.

## Data Caveat

The current run skips macOS/iCloud `dataless` parquet/raw files to avoid blocked reads. Treat these as exploratory local-warehouse results until the full 2025 regular season is materialized locally and rerun.
