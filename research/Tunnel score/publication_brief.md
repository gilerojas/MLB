# Case Study Brief: Tunnel Score

## Concept

The tunnel score idea is promising:

> Does pitch 2 perform better when pitch 1 created a similar release look, but pitch 2 separated later through velocity, movement, or location?

First-pass formulas:

- `release_similarity`: similarity in arm angle, release side, release height, and extension between consecutive pitches in the same plate appearance.
- `post_release_separation`: difference in velocity, movement, and final location.
- `tunnel_score = release_similarity × post_release_separation`
- `tunnel_stuff_score = release_similarity × velocity/movement separation`

The stuff-only version avoids mixing deception with final pitch location.

## First-Pass Result

The concept is interesting, but the naive formula did not produce a positive whiff/CSW signal in the raw all-count pitch-pair pool.

2025 local sample:

- Pitch pairs: `317,851`
- Tunnel score vs whiff: `r = -0.017`
- Tunnel score vs CSW: `r = -0.042`
- Tunnel stuff score vs whiff: `r = +0.000`
- Tunnel stuff score vs CSW: `r = +0.002`
- Tunnel stuff score vs chase: `r = +0.011`

Bucket result:

| Bucket | Whiff% | CSW% | Chase% | xwOBA BIP |
|---|---:|---:|---:|---:|
| Low tunnel | 13.5% | 25.9% | 15.7% | 0.303 |
| Mid-low | 14.0% | 26.3% | 16.0% | 0.301 |
| Mid-high | 13.4% | 25.1% | 16.2% | 0.305 |
| High tunnel | 12.5% | 22.0% | 16.7% | 0.314 |

## Interpretation

This does **not** mean tunneling is useless.

It means the naive score is probably mixing too many baseball contexts:

- count state
- pitch type pair
- same-pitch vs different-pitch pairs
- intended location vs actual location
- waste pitches
- chase pitches
- batter handedness
- pitcher role and arsenal shape

The full tunnel score was especially contaminated by final location separation. Big location separation often means the second pitch finishes farther from the prior pitch, which can reduce called strikes and whiffs even if it may increase chase.

## Better Next Version

The next tunnel-score study should segment before scoring:

1. Only different-pitch pairs.
2. Same pitcher/batter handedness buckets.
3. Count buckets: pitcher ahead, even, hitter ahead, two strikes.
4. Remove obvious waste pitches or analyze them separately.
5. Score pitch-pair families separately, such as:
   - FF → SL
   - FF → CH
   - SI → SL
   - FF → CB
6. Use `tunnel_stuff_score` as the primary score, not full location-included score.
7. Compare expected outcomes within pitch-pair family and count bucket.

## Best Framing

The current finding is a useful research checkpoint:

> A simple universal tunnel score does not work out of the box. Tunneling probably has to be evaluated by pitch-pair type and count context.

That is still a good article angle because it prevents overclaiming.

Potential title:

`The Tunnel Score Problem: Why "Same Look, Different Finish" Needs Context`

