# Home Run Derby Winner: Before vs. After Study

## Question

Do recent Home Run Derby winners perform differently immediately after the event?

This is an observational comparison, not a causal estimate. It cannot separate the Derby from regression, injuries, schedule strength, normal variance or other changes occurring around the All-Star break.

## Sample

The last seven completed Derbies with a meaningful post-event regular-season sample:

| Derby | Winner | Date |
|---|---|---|
| 2025 | Cal Raleigh | July 14, 2025 |
| 2024 | Teoscar Hernández | July 15, 2024 |
| 2023 | Vladimir Guerrero Jr. | July 10, 2023 |
| 2022 | Juan Soto | July 18, 2022 |
| 2021 | Pete Alonso | July 12, 2021 |
| 2019 | Pete Alonso | July 8, 2019 |
| 2018 | Bryce Harper | July 16, 2018 |

There was no Derby in 2020. The 2026 winner is excluded because no meaningful post-Derby sample exists yet.

## Method

- Source: MLB Stats API regular-season hitter game logs.
- Comparison: the winner's final 30 games before the Derby versus first 30 games after it.
- The Derby and All-Star Game are not included because only regular-season game logs are used.
- OPS, HR per 100 PA, K% and BB% are recalculated from game-level counting statistics.
- The chart shows trailing 15-game OPS aligned by games relative to the Derby, from game -40 through game +40.
- The same OPS scale is used for every player.

## Results

| Winner | 30G OPS before | 30G OPS after | Delta | HR/100 PA | K% | BB% |
|---|---:|---:|---:|---:|---:|---:|
| Cal Raleigh, 2025 | 1.002 | .772 | -.230 | 8.7 → 6.9 | 21.7 → 32.1 | 17.4 → 9.9 |
| Teoscar Hernández, 2024 | .802 | .846 | +.044 | 4.7 → 5.4 | 26.6 → 28.7 | 7.8 → 7.0 |
| Vladimir Guerrero Jr., 2023 | .718 | .752 | +.034 | 3.1 → 3.8 | 15.7 → 15.3 | 7.9 → 13.0 |
| Juan Soto, 2022 | 1.052 | .895 | -.157 | 5.7 → 3.0 | 10.6 → 15.0 | 24.4 → 22.6 |
| Pete Alonso, 2021 | .764 | .871 | +.107 | 6.1 → 6.0 | 19.3 → 18.0 | 7.0 → 12.0 |
| Pete Alonso, 2019 | 1.098 | .759 | -.339 | 7.6 → 6.2 | 18.9 → 30.8 | 14.4 → 14.6 |
| Bryce Harper, 2018 | .736 | 1.095 | +.359 | 3.1 → 5.5 | 30.0 → 25.2 | 20.8 → 10.2 |

Four winners improved their 30-game OPS and three declined. The direction and size of the changes vary substantially, including opposite outcomes for Pete Alonso's two winning seasons. This small sample does not support a consistent post-Derby performance effect.

## Reproduction

```bash
./mlb_env/bin/python scripts/hr_derby_winner_pre_post.py --refresh
```

The first run caches MLB Stats API responses and player headshots under `outputs/hr_derby_winner_study/cache`. Later renders can run offline from that cache.

## Official Derby References

- https://www.mlb.com/news/home-run-derby-history-c283844278
- https://www.mlb.com/stories/recent-home-run-derby-winners
