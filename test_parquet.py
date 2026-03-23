import json
import pandas as pd
from pathlib import Path

feed_path = "data/warehouse/mlb/2024/regular_season/raw/game_746255_20240921_feed_live.json"
pq_path = feed_path.replace("raw", "pitches_enriched").replace("_feed_live.json", "_pitches_enriched.parquet")

df = pd.read_parquet(pq_path)
df_chap = df[df.batter == 656305]
print(df_chap[['play_id', 'pitch_name', 'estimated_ba_using_speedangle', 'plate_x', 'plate_z', 'description', 'events']].head(3))
