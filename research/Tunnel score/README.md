# Tunnel Score

Pitch-pair research package for testing whether consecutive pitches perform better when pitch 2 has a similar release look but separates later.

## Current Status

The first-pass naive tunnel score is implemented and tested.

The result is not yet positive for whiff/CSW in the raw all-count pool. The likely issue is that pitch-pair context matters too much for one universal score.

## Key Files

- `publication_brief.md` — current finding and recommended next version.
- `data/tunnel_score_summary_2025.md` — generated summary.
- `data/tunnel_score_pitch_pairs_2025.csv` — pitch-pair dataset.
- `data/tunnel_score_buckets_2025.csv` — tunnel-score bucket outcomes.
- `data/tunnel_score_correlations_2025.csv` — correlations vs pitch-2 outcomes.
- `data/tunnel_score_pitchers_2025.csv` — pitcher-level tunnel-score averages.
- `images/tunnel_score_buckets_2025.png` — first-pass bucket chart.
- `scripts/tunnel_score_study.py` — reproducible script.

## Current Formula

`tunnel_score = release_similarity × post_release_separation`

`tunnel_stuff_score = release_similarity × velocity/movement separation`

The stuff-only version avoids final location, which can blend deception with command and pitch intent.

## Recommended Next Step

Redo this by pitch-pair family and count bucket, especially:

- FF → SL
- FF → CH
- SI → SL
- FF → CB
- two-strike counts
- pitcher-ahead counts

