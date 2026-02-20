# Warehouse data (git)

We only commit **up to 3 files per season** for context:

1. `schedule_post.csv` — flat schedule for posting (date, teams, venue, game_pk).
2. `schedule_{stage}.json` — full schedule per stage (e.g. `schedule_spring_training.json`).
3. `season_context.md` — optional short note (e.g. “2025 full season backfill done”).

All game files (`*_feed_live.json`, `*_pitches_enriched.parquet`) and the `raw/` and `pitches_enriched/` directories are gitignored.
