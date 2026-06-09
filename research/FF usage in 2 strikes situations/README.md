# FF Usage in 2 Strikes Situations

Case study on whether pitchers who trust the four-seam fastball (`FF`) in two-strike counts tend to be more successful.

## Conclusion

Two-strike `FF%` is not a standalone success metric. It is better read as a trust signal.

Pitchers do not become good simply because they throw more four-seamers with two strikes. More often, they keep throwing four-seamers with two strikes because the pitch has enough quality to survive there.

The signal is stronger when `2K FF%` is paired with four-seam quality indicators: velocity, IVB, and CSW%.

## Publishable Files

- `final_tweet_thread.md` — X thread draft with image placement notes.
- `final_article_draft.md` — Medium/LinkedIn article draft.
- `publication_brief.md` — concise findings, evidence, and framing.

## Images

Located in `images/`.

- `03_ff_usage_vs_kbb_scatter.png` — scatter plot of `2K FF%` vs `K-BB%`.
- `02_weighted_correlations.png` — weighted correlation bars.
- `04_trust_backed_by_traits_table.png` — leaders sorted by `2K FF%`.
- `two_strike_ff_vs_kbb_2025.png` and `two_strike_ff_vs_kbb_2026.png` — original study scatter exports.

## Data

Located in `data/`.

- `pitcher_two_strike_ff_sample_2025.csv` and `pitcher_two_strike_ff_sample_2026.csv` — filtered pitcher samples.
- `pitcher_two_strike_ff_full_2025.csv` and `pitcher_two_strike_ff_full_2026.csv` — full pitcher tables.
- `correlations_ff_2025.csv` and `correlations_ff_2026.csv` — four-seam correlation outputs.
- `correlations_fastball_family_2025.csv` and `correlations_fastball_family_2026.csv` — FF/SI/FC comparison outputs.
- `two_strike_ff_success_2025.md` and `two_strike_ff_success_2026.md` — generated study summaries.

## Scripts

Located in `scripts/`.

- `ff_two_strike_success_study.py` — builds the study data, correlations, summaries, and scatter plots.
- `ff_two_strike_x_images.py` — builds the X-ready image cards.

## Caveat

The current run skips macOS/iCloud `dataless` warehouse files to avoid blocked reads. Treat this as an exploratory local-warehouse study until the full 2025 regular-season warehouse is materialized locally and rerun.
