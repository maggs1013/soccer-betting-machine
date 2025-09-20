# scripts/fetch_soccerdata.py
# Pull team stats and ratings via soccerdata (FBref + FiveThirtyEight).
# Football-Data is handled separately in fetch_football_data.py.

import os
import pandas as pd
from soccerdata import FBref, FiveThirtyEight
from datetime import datetime

DATA = "data"
os.makedirs(DATA, exist_ok=True)

SEASON = datetime.now().year  # e.g., 2025 → 2025/26 season
FBREF_LEAGUES = [
    "ENG-Premier League",
    "ESP-La Liga",
    "ITA-Serie A",
    "GER-Bundesliga",
    "FRA-Ligue 1",
]

def fbref_team_stats():
    """Fetch FBref team stats (shooting includes xG)."""
    try:
        fb = FBref(leagues=FBREF_LEAGUES, seasons=SEASON)
        shooting = fb.read_team_season_stats(stat_type="shooting")

        # Normalize xG column names if present
        if "xg_for" not in shooting.columns and "xg" in shooting.columns:
            shooting = shooting.rename(columns={"xg": "xg_for"})
        if "xg_against" not in shooting.columns and "xga" in shooting.columns:
            shooting = shooting.rename(columns={"xga": "xg_against"})

        out = shooting.rename(
            columns={
                "xg_for": "xg_for_fbref",
                "xg_against": "xg_against_fbref",
            }
        )
        out.to_csv(os.path.join(DATA, "sd_fbref_team_stats.csv"), index=False)
        print("[OK] sd_fbref_team_stats.csv", len(out))
    except Exception as e:
        print("[WARN] FBref fetch failed:", e)

def fivethirtyeight_spi():
    """Fetch FiveThirtyEight SPI ratings."""
    try:
        spi = FiveThirtyEight(
            leagues=["mls","epl","liga","serie-a","bundesliga","ligue-1"]
        ).read_team_ratings()
        spi.to_csv(os.path.join(DATA, "sd_538_spi.csv"), index=False)
        print("[OK] sd_538_spi.csv", len(spi))
    except Exception as e:
        print("[WARN] FiveThirtyEight fetch failed:", e)

if __name__ == "__main__":
    fbref_team_stats()
    fivethirtyeight_spi()