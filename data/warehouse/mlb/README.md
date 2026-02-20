# Warehouse data (git)

We only commit **up to 3 files per season** for context:

1. `schedule_post.csv` — flat schedule for posting (date, teams, venue, game_pk).
2. `schedule_{stage}.json` — full schedule per stage (e.g. `schedule_spring_training.json`).
3. `season_context.md` — optional short note (e.g. “2025 full season backfill done”).

All game files and `raw/` / `pitches_enriched/` dirs are gitignored. To add the 3 docs (first time or new season), use **force add**:

```bash
git add -f .gitignore \
  data/warehouse/mlb/README.md \
  data/warehouse/mlb/2025/season_context.md \
  data/warehouse/mlb/2026/schedule_post.csv \
  data/warehouse/mlb/2026/schedule_spring_training.json \
  data/warehouse/mlb/2026/season_context.md
```

For a new year, add the new paths in `.gitignore` under the "3 docs per season" section, then `git add -f data/warehouse/mlb/YYYY/...`.
