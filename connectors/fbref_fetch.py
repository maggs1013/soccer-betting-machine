#!/usr/bin/env python3
"""
FBref team stats via soccerdata (API 2025)
- Uses read_team_season_stats with stat_type + seasons list
- Fetches multiple stat_types (standard, shooting, passing, keeper)
- Merges into one DataFrame keyed on [team, season, league]
- Retries & writes last-good cache on success
- Writes: data/sd_fbref_team_stats.csv
"""

import os, time
import pandas as pd

DATA_DIR = "data"
OUT = os.path.join(DATA_DIR, "sd_fbref_team_stats.csv")
CACHE = os.path.join(DATA_DIR, "sd_fbref_team_stats.cache.csv")

COMP = "ENG-Premier League"
SEASON = "2024-2025"   # single season; wrap in [SEASON] for API

STAT_TYPES = ["standard", "shooting", "passing", "keeper"]

def fetch_stat_type(stat_type: str):
    import soccerdata as sd
    fb = sd.FBref()
    df = fb.read_team_season_stats(
        stat_type=stat_type,
        competition=COMP,
        seasons=[SEASON]
    )
    if df is None or df.empty:
        return None
    # add suffix to columns to avoid clashes
    suffix = f"_{stat_type}"
    core_cols = ["team", "season", "league"]
    df = df.reset_index(drop=False)
    df = df.rename(columns={c: f"{c}{suffix}" for c in df.columns if c not in core_cols})
    return df

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    tries = 2
    err = None
    merged = None

    for _ in range(tries):
        try:
            parts = []
            for st in STAT_TYPES:
                print(f"Fetching FBref stat_type={st} …")
                df = fetch_stat_type(st)
                if df is not None:
                    parts.append(df)
            if parts:
                # merge on keys
                merged = parts[0]
                for df in parts[1:]:
                    merged = pd.merge(merged, df, on=["team","season","league"], how="outer")
            break
        except Exception as e:
            err = e
            time.sleep(2)

    if merged is None or merged.empty:
        if os.path.exists(CACHE):
            merged = pd.read_csv(CACHE)
            print(f"⚠️ FBref fetch failed ({err}), using cache with {len(merged)} rows")
        else:
            # Write empty stub to keep pipeline alive
            pd.DataFrame(columns=["team","league","season"]).to_csv(OUT, index=False)
            raise SystemExit(f"FBref fetch failed and no cache available: {err}")

    # Write outputs
    merged.to_csv(OUT, index=False)
    try:
        merged.to_csv(CACHE, index=False)
    except Exception:
        pass

    print(f"✅ Wrote {len(merged)} rows to {OUT}")

if __name__ == "__main__":
    main()