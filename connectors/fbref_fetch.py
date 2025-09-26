#!/usr/bin/env python3
"""
FBref team stats via soccerdata — API-safe version
- Removes deprecated 'cache=' init arg
- Retries & writes last-good cache on success
- Writes: data/sd_fbref_team_stats.csv
"""

import os, time
import pandas as pd

DATA_DIR = "data"
OUT = os.path.join(DATA_DIR, "sd_fbref_team_stats.csv")
CACHE = os.path.join(DATA_DIR, "sd_fbref_team_stats.cache.csv")
COMP = "ENG-Premier League"
SEASON = "2024-2025"

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    tries = 3
    df = None
    err = None
    for _ in range(tries):
        try:
            import soccerdata as sd
            fb = sd.FBref()  # no 'cache' kwarg with newer versions
            df = fb.read_team_season_stats(competition=COMP, season=SEASON)
            break
        except Exception as e:
            err = e
            time.sleep(2)

    if df is None or df.empty:
        # fallback to cache if present
        if os.path.exists(CACHE):
            df = pd.read_csv(CACHE)
        else:
            # write an empty CSV with expected minimal columns to avoid pipeline crash
            pd.DataFrame(columns=["team","league","season","shots","shots_against","xg","xga","possession","date"]).to_csv(OUT, index=False)
            raise SystemExit(f"FBref fetch failed and no cache available: {err}")

    # normalize minimal expected columns if missing
    for c in ["team","league","season","shots","shots_against","xg","xga","possession","date"]:
        if c not in df.columns:
            df[c] = None

    df.to_csv(OUT, index=False)
    try:
        df.to_csv(CACHE, index=False)
    except Exception:
        pass
    print(f"Wrote {len(df)} rows to {OUT}")

if __name__ == "__main__":
    main()