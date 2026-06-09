# Case Study Brief: Arm-Angle Variability and Deception

## Corrected Core Finding

The original theory was:

> Less arm-angle variation should improve tunneling and deception.

The corrected first pass says something more specific:

> Overall arm-angle spread can hide different mechanisms. In the 2025 sample, pure within-pitch-type repeatability showed only a weak relationship with results, while between-pitch-type slot separation and overall spread showed clearer negative relationships with K-BB%.

That means the better research question is not simply:

> Is this pitcher repeatable?

It is:

> Are his pitch types coming from meaningfully different release-angle buckets, and does that make pitch ID easier?

## Why Overall SD Can Mislead

Two pitchers can have nearly identical overall arm-angle SD for different reasons:

- Pitcher A may repeat each pitch fairly well, but throw different pitch types from distinct arm-angle clusters.
- Pitcher B may throw all pitch types from similar average slots, but be less consistent inside each pitch type.

Both can produce similar overall SD, but the baseball interpretation is different.

## Real Example: Framber Valdez vs Tarik Skubal

Both had almost the same overall arm-angle SD in the local 2025 sample:

- Framber Valdez: `3.50°`
- Tarik Skubal: `3.51°`

But the composition was different.

Framber’s pitch-type average arm angles were packed closer together:

- SI: `42.12°`
- CU: `41.96°`
- CH: `40.92°`
- SL: `38.31°`
- FF: `41.94°`

Skubal’s pitch-type average arm angles were more separated:

- FF: `52.44°`
- CH: `46.32°`
- SI: `50.03°`
- SL: `49.09°`
- CU: `53.36°`

So the same overall SD can mean different things. Framber’s overall spread comes more from same-pitch variation, while Skubal’s comes more from pitch-type slot separation.

## Corrected 2025 Correlations

Sample: 132 pitchers from the local 2025 regular-season warehouse sample.

Within-pitch-type arm-angle SD vs results:

- K-BB%: Pearson r = `-0.023`, weighted r = `-0.014`
- K%: Pearson r = `-0.036`, weighted r = `-0.029`
- Whiff/swing: Pearson r = `-0.040`, weighted r = `-0.038`
- CSW%: Pearson r = `-0.054`, weighted r = `-0.061`
- xwOBA on BBE: Pearson r = `+0.099`, weighted r = `+0.089`

Between-pitch-type arm-angle SD vs results:

- K-BB%: Pearson r = `-0.185`, weighted r = `-0.176`
- K%: Pearson r = `-0.146`, weighted r = `-0.140`
- CSW%: Pearson r = `-0.123`, weighted r = `-0.136`
- xwOBA on BBE: Pearson r = `+0.094`, weighted r = `+0.094`

Overall arm-angle SD vs results:

- K-BB%: Pearson r = `-0.141`, weighted r = `-0.132`
- K%: Pearson r = `-0.130`, weighted r = `-0.123`
- CSW%: Pearson r = `-0.116`, weighted r = `-0.131`
- xwOBA on BBE: Pearson r = `+0.122`, weighted r = `+0.119`

## Updated Interpretation

The data does not strongly prove that same-pitch arm-angle repeatability alone drives results.

The stronger first-pass signal is that greater release-angle separation across pitch types, and greater overall spread, tends to correlate with worse results.

That could support a tunneling/deception idea:

> If different pitch types are coming from visibly different arm-angle buckets, hitters may get earlier pitch-ID cues.

But it could also reflect other things:

- pitch mix
- pitcher handedness and repertoire design
- role/starter profile
- measurement coverage
- mechanical changes across counts or fatigue

## Better Article Angle

Working title:

`The Tunnel Starts at Release: What Arm Angle Can and Cannot Tell Us About Deception`

The honest story:

1. Overall arm-angle variation is not enough.
2. We need to split the problem into within-pitch repeatability and between-pitch slot separation.
3. Real pitcher examples show why the same overall SD can mean different things.
4. In the corrected 2025 sample, between-pitch slot separation showed a clearer relationship with results than same-pitch repeatability.
5. This should become a pitching-card diagnostic, not a one-number grade.

## Current Caveat

This is still a local-warehouse, dataless-skipping exploratory run. The next step should be to add controls for pitch mix and handedness, then rerun on a complete materialized season.
