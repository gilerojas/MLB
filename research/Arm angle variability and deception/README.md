# Arm Angle Variability and Deception

Research package testing whether variability in pitcher arm angle relates to results.

## Hypothesis

Less variation in arm angle may contribute to better tunneling and deception because hitters see a more repeatable release look across pitches.

## Main Conclusion

The corrected 2025 local sample supports a narrower version of the idea.

Overall arm-angle SD can hide different mechanisms. A pitcher can have a large overall spread because each pitch type is inconsistent, or because different pitch types come from distinct arm-angle clusters.

After correcting the metric definitions, pure same-pitch repeatability (`within_pitch_type_arm_angle_sd`) had only a weak relationship with results. Between-pitch-type slot separation and overall spread showed clearer negative relationships with K-BB%.

The better working question is:

> Are pitch types coming from release-angle buckets that are different enough to give hitters early pitch-ID clues?

## Key Files

- `publication_brief.md` — corrected publishable framing and conclusion.
- `data/arm_angle_variability_summary_2025.md` — generated 2025 study summary.
- `data/arm_angle_variability_correlations_2025.csv` — full 2025 correlation table.
- `data/arm_angle_variability_sample_2025.csv` — filtered 2025 pitcher sample.
- `images/arm_angle_within_pt_sd_vs_kbb_2025.png` — first-pass scatter plot.
- `scripts/arm_angle_variability_study.py` — reproducible study script.

## Caveats

- The study skips macOS/iCloud `dataless` files to avoid blocked reads.
- 2026 local arm-angle coverage is too sparse for validation right now.
- This is correlation, not causal proof.
