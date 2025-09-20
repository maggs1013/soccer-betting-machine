# scripts/fetch_soccerdata.py
# Pull team stats and ratings via soccerdata (FBref + FiveThirtyEight),
# and normalize columns so downstream merges are reliable.

import os
import pandas as pd
from soccerdata import FBref, FiveThirtyEight
from datetime import datetime

DATA = "data"
os.makedirs(DATA, exist_ok=True)

SEASON = datetime.now().year  # e.g., 2025 → season 2025/26
FBREF_LEAGUES = [
    "ENG-Premier League",
    "ESP-La Liga",
    "ITA-Serie A",
    "GER-Bundesliga",
    "FRA-Ligue 1",
]

def normalize_fbref_team_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Rename FBref columns to stable names used downstream."""
    if df.empty:
        return df

    # lowercase for easy detection
    base_cols = {c.lower(): c for c in df.columns}

    # team name: squad → team
    if "team" not in df.columns:
        if "squad" in base_cols:
            df = df.rename(columns={base_cols["squad"]: "team"})
        elif "team" not in df.columns and "Team" in df.columns:
            df = df.rename(columns={"Team": "team"})
        else:
            # create a placeholder to avoid KeyError
            df["team"] = pd.NA

    # matches: mp → matches
    if "matches" not in df.columns:
        if "mp" in base_cols:
            df = df.rename(columns={base_cols["mp"]: "matches"})
        else:
            df["matches"] = pd.NA

    # xG for/against → xg_for_fbref / xg_against_fbref
    lc = {c.lower(): c for c in df.columns}
    # Try common names
    xg_for_col = lc.get("xg", None) or lc.get("xg_for", None) or lc.get("npxg", None)
    xg_against_col = lc.get("xga", None) or lc.get("xg_against", None) or lc.get("npxga", None)

    if xg_for_col and "xg_for_fbref" not in df.columns:
        df = df.rename(columns={xg_for_col: "xg_for_fbref"})
    if xg_against_col and "xg_against_fbref" not in df.columns:
        df = df.rename(columns={xg_against_col: "xg_against_fbref"})

    # if they still don't exist, create empty columns so merge script never crashes
    if "xg_for_fbref" not in df.columns:
        df["xg_for_fbref"] = pd.NA
    if "xg_against_fbref" not in df.columns:
        df["xg_against_fbref"] = pd.NA

    # keep typical identifiers if available
    keep = [c for c in ["league", "season", "team", "matches", "xg_for_fbref", "xg_against_fbref"] if c in df.columns]
    return df[keep].copy()

def fbref_team_stats():
    """Fetch FBref team stats (shooting includes xG) and normalize columns."""
    try:
        fb = FBref(leagues=FBREF_LEAGUES, seasons=SEASON)
        shooting = fb.read_team_season_stats(stat_type="shooting")
        out = normalize_fbref_team_stats(shooting)
        out.to_csv(os.path.join(DATA, "sd_fbref_team_stats.csv"), index=False)
        print("[OK] sd_fbref_team_stats.csv", len(out), "rows; cols:", list(out.columns))
    except Exception as e:
        print("[WARN] FBref fetch failed:", e)

def fivethirtyeight_spi():
    """Fetch FiveThirtyEight SPI ratings."""
    try:
        spi = FiveThirtyEight(leagues=["mls","epl","liga","serie-a","bundesliga","ligue-1"]).read_team_ratings()
        spi.to_csv(os.path.join(DATA, "sd_538_spi.csv"), index=False)
        print("[OK] sd_538_spi.csv", len(spi))
    except Exception as e:
        print("[WARN] FiveThirtyEight fetch failed:", e)

if __name__ == "__main__":
    fbref_team_stats()
    fivethirtyeight_spi()