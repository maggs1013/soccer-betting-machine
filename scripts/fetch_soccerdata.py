# scripts/fetch_soccerdata.py
# Pull FBref team shooting stats (includes xG) and FiveThirtyEight SPI
# via soccerdata, then normalize columns so downstream merges are reliable.

import os
import pandas as pd
from datetime import datetime
from soccerdata import FBref, FiveThirtyEight

DATA = "data"
os.makedirs(DATA, exist_ok=True)

# Target season (soccerdata expects an "anchor" year for the season, e.g., 2025 → 2025/26)
SEASON = datetime.now().year

# Big 5 leagues (string names used by soccerdata FBref backend)
FBREF_LEAGUES = [
    "ENG-Premier League",
    "ESP-La Liga",
    "ITA-Serie A",
    "GER-Bundesliga",
    "FRA-Ligue 1",
]

def normalize_fbref(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize FBref 'shooting' team table to stable columns:
      team, league, season, matches, xg_for_fbref, xg_against_fbref
    FBref tables vary; common names: Squad/team, MP, xG, xGA, etc.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["team","league","season","matches","xg_for_fbref","xg_against_fbref"])

    # Standardize lower-case map
    lower_map = {c.lower(): c for c in df.columns}

    # TEAM
    if "team" not in df.columns:
        if "squad" in lower_map:
            df = df.rename(columns={lower_map["squad"]: "team"})
        elif "team" in lower_map:
            df = df.rename(columns={lower_map["team"]: "team"})
        else:
            df["team"] = pd.NA

    # LEAGUE (soccerdata often includes "league")
    if "league" not in df.columns:
        if "comp" in lower_map:
            df = df.rename(columns={lower_map["comp"]: "league"})
        else:
            df["league"] = pd.NA

    # SEASON
    if "season" not in df.columns:
        if "season" in lower_map:
            df = df.rename(columns={lower_map["season"]: "season"})
        else:
            df["season"] = SEASON

    # MATCHES
    if "matches" not in df.columns:
        if "mp" in lower_map:
            df = df.rename(columns={lower_map["mp"]: "matches"})
        elif "games" in lower_map:
            df = df.rename(columns={lower_map["games"]: "matches"})
        else:
            df["matches"] = pd.NA

    # xG for / against → xg_for_fbref / xg_against_fbref
    xg_for = lower_map.get("xg") or lower_map.get("xg_for") or lower_map.get("npxg")
    xg_against = lower_map.get("xga") or lower_map.get("xg_against") or lower_map.get("npxga")

    if xg_for and "xg_for_fbref" not in df.columns:
        df = df.rename(columns={xg_for: "xg_for_fbref"})
    if xg_against and "xg_against_fbref" not in df.columns:
        df = df.rename(columns={xg_against: "xg_against_fbref"})

    # Guarantee target columns exist
    for c in ["xg_for_fbref","xg_against_fbref"]:
        if c not in df.columns:
            df[c] = pd.NA

    keep = [c for c in ["team","league","season","matches","xg_for_fbref","xg_against_fbref"] if c in df.columns]
    out = df[keep].copy()

    # Clean team text a bit
    out["team"] = out["team"].astype(str).str.replace(r"\s+\(.*\)$","",regex=True).str.strip()
    return out

def fbref_team_stats():
    try:
        fb = FBref(leagues=FBREF_LEAGUES, seasons=SEASON)
        # "shooting" team stats includes xG/xGA
        raw = fb.read_team_season_stats(stat_type="shooting")
        out = normalize_fbref(raw)
        out.to_csv(os.path.join(DATA, "sd_fbref_team_stats.csv"), index=False)
        print("[OK] sd_fbref_team_stats.csv", len(out), "rows • cols:", list(out.columns))
        # coverage print
        n_xg = out["xg_for_fbref"].notna().sum() if "xg_for_fbref" in out.columns else 0
        print(f"[INFO] FBref xG present for {n_xg}/{len(out)} teams")
    except Exception as e:
        print("[WARN] FBref fetch failed:", e)
        # Write header-only to avoid downstream KeyErrors
        pd.DataFrame(columns=["team","league","season","matches","xg_for_fbref","xg_against_fbref"]).to_csv(
            os.path.join(DATA,"sd_fbref_team_stats.csv"), index=False
        )

def fivethirtyeight_spi():
    try:
        spi = FiveThirtyEight(
            leagues=["mls","epl","liga","serie-a","bundesliga","ligue-1"]
        ).read_team_ratings()
        spi.to_csv(os.path.join(DATA, "sd_538_spi.csv"), index=False)
        print("[OK] sd_538_spi.csv", len(spi))
    except Exception as e:
        print("[WARN] FiveThirtyEight fetch failed:", e)
        pd.DataFrame().to_csv(os.path.join(DATA,"sd_538_spi.csv"), index=False)

if __name__ == "__main__":
    fbref_team_stats()
    fivethirtyeight_spi()