import json
import pandas as pd
from collections import defaultdict
from pathlib import Path

feed_path = "data/warehouse/mlb/2024/regular_season/raw/game_746255_20240921_feed_live.json"
batter_id = 656305

with open(feed_path) as f:
    data = json.load(f)

pq_path = feed_path.replace("raw", "pitches_enriched").replace("_feed_live.json", "_pitches_enriched.parquet")
try:
    df_pq = pd.read_parquet(pq_path)
    df_pq = df_pq[df_pq.batter == batter_id]
except Exception as e:
    print(f"Failed to load parquet: {e}")
    df_pq = pd.DataFrame()

all_plays = data["liveData"]["plays"]["allPlays"]

pa_log = []
batted_balls = []
pitches_data = [] # For strikezone

for play in all_plays:
    if play.get("matchup", {}).get("batter", {}).get("id") != batter_id:
        continue
    
    matchup = play.get("matchup", {})
    pitcher_name = matchup.get("pitcher", {}).get("fullName", "Unknown")
    inning = play.get("about", {}).get("inning", 0)
    
    result = play.get("result", {})
    event = result.get("event", "")
    rbi = result.get("rbi", 0)
    
    play_id = play.get("playEvents", [])[-1].get("playId", "") if play.get("playEvents") else ""
    
    # number of pitches
    pitches_in_pa = [ev for ev in play.get("playEvents", []) if ev.get("isPitch")]
    num_pitches = len(pitches_in_pa)
    
    # get contact info
    contact_in_pa = None
    for ev in pitches_in_pa:
        play_event_id = ev.get("playId", "")
        # Look up this pitch in parquet
        pq_row = df_pq[df_pq.play_id == play_event_id]
        if not pq_row.empty:
            pq_row = pq_row.iloc[0]
            pitch_type = pq_row.pitch_name
            px = pq_row.plate_x
            pz = pq_row.plate_z
            xba = pq_row.estimated_ba_using_speedangle
        else:
            pitch_type = ev.get("details", {}).get("type", {}).get("description", "Unknown")
            px = ev.get("pitchData", {}).get("coordinates", {}).get("pX")
            pz = ev.get("pitchData", {}).get("coordinates", {}).get("pZ")
            xba = None
            
        desc = ev.get("details", {}).get("description", "")
        
        pitches_data.append({
            "px": px,
            "pz": pz,
            "pitch_type": pitch_type,
            "desc": desc,
            "event": event if ev == pitches_in_pa[-1] else "" # associate PA event with last pitch
        })
        
        hit_d = ev.get("hitData")
        if hit_d:
            contact_in_pa = {
                "result": event,
                "ev": hit_d.get("launchSpeed"),
                "la": hit_d.get("launchAngle"),
                "dist": hit_d.get("totalDistance"),
                "pitch_type": pitch_type,
                "xba": xba,
            }
            batted_balls.append(contact_in_pa)
            
    pa_log.append({
        "inning": inning,
        "pitcher": pitcher_name.split(" ")[-1], # last name
        "event": event,
        "num_pitches": num_pitches,
        "contact": contact_in_pa,
        "rbi": rbi
    })

print(pa_log[0])
print(batted_balls[0])
print(pitches_data[0])
