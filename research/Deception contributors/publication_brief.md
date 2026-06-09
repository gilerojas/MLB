# Case Study Brief: Which Variables Contribute More to Deception?

## Goal

The original question was whether arm-angle separation contributes to deception. The broader version asks:

> Among measurable pitch-level variables, which ones carry the strongest signal against whiff, CSW, K-BB, and contact quality?

This study compares pitcher-level separation/shape variables:

- arm-angle separation across pitch types
- release-side separation
- release-height separation
- extension separation
- average extension
- velocity separation
- movement separation
- horizontal and vertical location separation
- pitch-mix diversity
- average velocity

## Core Finding

Arm-angle separation matters directionally, but it is not the strongest signal.

In the 2025 local sample, the strongest standalone signals were:

1. Vertical location separation
2. Average velocity
3. Average extension
4. Velocity separation
5. Release-height/release-side separation
6. Arm-angle separation

The better conclusion:

> Deception is not one variable. It is a stack: where pitches start, how they move, how fast they arrive, and where they are located.

## Key 2025 Results

Sample: 132 pitchers.

### K-BB%

Best weighted correlations:

- Vertical location separation: `+0.311`
- Average velocity: `+0.242`
- Average extension: `+0.241`
- Release-height separation: `-0.179`
- Arm-angle separation: `-0.176`
- Release-side separation: `-0.174`
- Velocity separation: `+0.144`

### Whiff/swing

Best weighted correlations:

- Vertical location separation: `+0.365`
- Average velocity: `+0.286`
- Average extension: `+0.251`
- Pitch-mix diversity: `-0.235`
- Release-height separation: `-0.170`
- Horizontal location separation: `-0.149`
- HB separation: `-0.148`
- IVB separation: `+0.144`
- Velocity separation: `+0.115`

### CSW%

Best weighted correlations:

- Vertical location separation: `+0.230`
- Velocity separation: `+0.165`
- Movement separation: `+0.163`
- Release-side separation: `-0.159`
- Extension separation: `+0.148`
- Average extension: `+0.188`
- Release-height separation: `-0.147`
- IVB separation: `+0.142`
- Average velocity: `+0.138`

### xwOBA on BBE

Best weighted correlations:

- Average velocity: `-0.209`
- Release-height separation: `+0.138`
- Velocity separation: `-0.125`
- Average extension: `-0.121`
- Vertical location separation: `-0.101`
- Arm-angle separation: `+0.093`

## How to Interpret the Signs

Positive is not always good or bad. It depends on the result metric.

- Positive vs K-BB%, whiff, CSW: better.
- Negative vs xwOBA: better.
- For release/arm-angle separation, negative vs K-BB/whiff/CSW suggests too much visual separation may reduce deception.
- For velocity/location/movement separation, positive vs whiff/CSW can mean the arsenal creates meaningful separation for hitters to cover.

## Important Takeaway

Arm-angle separation fits the tunneling idea, but it is modest.

The stronger insight is that deception appears to be a blend of:

- visual similarity at release
- velocity contrast
- extension / perceived time-to-plate
- movement contrast
- vertical attack/location separation
- raw pitch quality

If we build this into a pitching card, the right output is not one “deception score” from arm angle. It should be a grouped diagnostic:

1. Release disguise: arm angle, release side, release height, extension.
2. Arsenal separation: velocity and movement separation.
3. Execution lanes: vertical and horizontal location separation.
4. Outcome validation: whiff, CSW, chase, xwOBA.

## Recommended Next Study

Move from pitcher-level aggregation to pitch-pair or pitch-sequence level.

Better question:

> Does a pitch perform better when it follows a previous pitch with similar release but different velocity/movement/location?

That would test tunneling more directly than season-level pitcher correlations.
