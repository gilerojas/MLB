# Deception Contributors

Research package comparing measurable contributors to pitcher deception and outcomes.

## Main Conclusion

Arm-angle separation carries a modest directional signal, but it is not the strongest standalone contributor.

In the 2025 local sample, vertical location separation, average velocity, velocity separation, and release-height/release-side variables generally carried stronger signals against whiff, CSW, K-BB, and xwOBA.

## Key Files

- `publication_brief.md` — concise findings and framing.
- `data/deception_contributors_summary_2025.md` — generated analysis summary.
- `data/deception_contributors_correlations_2025.csv` — univariate correlation table.
- `data/deception_contributors_standardized_ols_2025.csv` — standardized multivariable coefficients.
- `data/deception_contributors_sample_2025.csv` — filtered pitcher-level sample.
- `images/` — feature-ranking charts by outcome.
- `scripts/deception_contributors_study.py` — reproducible study script.

## Caveats

- This is pitcher-level and correlational.
- It does not directly measure hitter pitch recognition.
- It skips macOS/iCloud `dataless` files.
- The next stronger design should be pitch-sequence or pitch-pair level.

