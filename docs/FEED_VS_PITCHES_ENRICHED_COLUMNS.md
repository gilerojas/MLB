# Feed flattened vs pitches_enriched — column map

Reference table from comparing **flattened feed_live (allPlays)** to **pitches_enriched.parquet** (Statcast + play_id). Use this to know what comes from the feed vs Statcast and what overlaps.

**Hit data (exit velo, launch angle):** The feed **does** provide it. When a pitch is put in play (`details.isInPlay`), the same pitch event has a **hitData** object with `launchSpeed`, `launchAngle`, `totalDistance`, `trajectory`, `hardness`, `location`, `coordinates`. The notebook's `flatten_one_pitch()` now includes these as `launch_speed_feed`, `launch_angle_feed`, etc., so you get exit velo and launch angle from the feed and can compare with Statcast's `launch_speed`, `launch_angle`.

## Summary

- **Both:** Same or very close values (play_id, game_pk, inning, pitch_number, zone, plate_x, plate_z, pfx_x, pfx_z, balls, strikes, sz_top, sz_bot). Side-by-side checks show **release_speed ≈ startSpeed**, zone and location align with tiny float differences — data is consistent across sources.
- **Statcast only:** Needed for bat_speed, swing_length, launch_*, win expectancy, player/team IDs, etc.
- **Feed only:** plateTime, startTime/endTime, typeConfidence, call_code/call_description, is_strike/is_ball, spin_rate/spin_direction (feed names).

---

## Statcast only (in pitches_enriched, not in flattened feed)

| Column | Notes |
|--------|--------|
| age_bat, age_pit | |
| arm_angle | |
| at_bat_number | Feed has at_bat_index (0-based). |
| attack_angle, attack_direction | |
| away_score, away_team, home_score, home_team, home_score_diff | |
| babip_value | |
| **bat_speed** | **Key motivation for pybaseball.** |
| bat_win_exp | |
| batter, pitcher, player_name | |
| bb_type | |
| delta_home_win_exp, delta_pitcher_run_exp, delta_run_exp | |
| des, description, events | Outcome text. |
| effective_speed | |
| estimated_ba_using_speedangle, estimated_slg_using_speedangle, estimated_woba_using_speedangle | |
| game_date, game_type, game_year | |
| hc_x, hc_y, hit_distance_sc, hit_location | |
| home_win_exp | |
| hyper_speed | |
| if_fielding_alignment, of_fielding_alignment | |
| inning_topbot | |
| launch_angle, launch_speed, launch_speed_angle | |
| n_thruorder_pitcher | |
| on_1b, on_2b, on_3b, outs_when_up | |
| p_throws, stand | |
| pitch_name, pitch_type | Statcast codes/names. |
| release_extension, release_pos_x, release_pos_y, release_pos_z | |
| release_speed, release_spin_rate, spin_axis | Feed has startSpeed, spin_rate, spin_direction. |
| swing_length | |
| type | S/B/X. |
| woba_denom, woba_value | |

---

## Feed only (in flattened feed, not in pitches_enriched)

| Column | Notes |
|--------|--------|
| at_bat_index | 0-based; Statcast has at_bat_number. |
| call_code, call_description | Umpire call (e.g. "B", "Ball"). |
| endSpeed, startSpeed | Feed names; Statcast has release_speed. |
| endTime, startTime | Per-pitch timestamps. |
| extension | Release extension (Statcast: release_extension). |
| is_ball, is_strike, is_in_play | Booleans. |
| **launch_speed_feed, launch_angle_feed** | **From feed hitData** — exit velo and launch angle when ball is put in play. Same concept as Statcast launch_speed, launch_angle. |
| total_distance_feed, trajectory_feed, hardness_feed, hit_location_feed, coord_x_feed, coord_y_feed | Rest of hitData (distance, trajectory, hardness, location, coordinates). |
| outs | Count at pitch time. |
| pitch_type_code, pitch_type_desc | Feed pitch type. |
| plateTime | Time to plate (seconds). |
| spin_direction, spin_rate | Feed; Statcast has spin_axis, release_spin_rate. |
| typeConfidence | Umpire call confidence. |

---

## Both (overlap)

| Column | Notes |
|--------|--------|
| balls, strikes | Count. |
| game_pk | |
| inning | |
| pfx_x, pfx_z | Movement. |
| pitch_number | |
| plate_x, plate_z | Location at plate. |
| play_id | Join key; same in both. |
| sz_top, sz_bot | Strike zone. |
| zone | Zone number. |

---

## Value alignment (same game, merge on play_id)

- **release_speed (enriched) vs startSpeed (feed):** Match (e.g. 85.6 vs 85.6).
- **zone_enriched vs zone_feed:** Match in almost all rows; occasional difference (e.g. 11 vs 1) may be encoding or typo in one source.
- **plate_x, plate_z:** Tiny float differences (e.g. -0.40 vs -0.399464) — expected; data is consistent.

This table is the **key reference** for deciding which columns to keep when building a feed-sourced Parquet or when merging feed + Statcast.
