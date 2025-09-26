#!/usr/bin/env python3
"""
rolling_form_build.py — compute last5/last10 form from HIST and merge to fixtures/enriched
Inputs:
  data/HIST_matches.csv           (date, league, home_team, away_team, fthg, ftag, etc.)
  data/UPCOMING_7D_enriched.csv   (output target)
Outputs:
  data/UPCOMING_7D_enriched.csv   (updated in-place with last5/last10 stats)
Adds (per side):
  home_last5_goals_for, home_last5_goals_against, home_last5_points,
  away_last5_goals_for, ... and same for last10
Safe: if HIST missing → skip gracefully.
"""

import os
import pandas as pd
import numpy as np

DATA="data"
HIST = os.path.join(DATA,"HIST_matches.csv")
UP   = os.path.join(DATA,"UPCOMING_7D_enriched.csv")

REQUIRED = ["date","home_team","away_team","fthg","ftag"]

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def points(hg, ag):
    if pd.isna(hg) or pd.isna(ag): return pd.NA, pd.NA
    if hg>ag: return 3, 0
    if hg<ag: return 0, 3
    return 1, 1

def build_team_history(df):
    # Long format per team with date sorting
    rows=[]
    for _, r in df.iterrows():
        rows.append({"team": r["home_team"], "date": r["date"], "gf": r["fthg"], "ga": r["ftag"], "pts": points(r["fthg"], r["ftag"])[0]})
        rows.append({"team": r["away_team"], "date": r["date"], "gf": r["ftag"], "ga": r["fthg"], "pts": points(r["fthg"], r["ftag"])[1]})
    td = pd.DataFrame(rows)
    try:
        td["date"] = pd.to_datetime(td["date"], errors="coerce")
        td = td.sort_values(["team","date"])
    except Exception:
        pass
    return td

def roll_stats(td, n=5):
    # rolling sums (exclude current fixture date)
    td[f"last{n}_gf"] = td.groupby("team")["gf"].rolling(n, min_periods=1).sum().reset_index(level=0, drop=True).shift(1)
    td[f"last{n}_ga"] = td.groupby("team")["ga"].rolling(n, min_periods=1).sum().reset_index(level=0, drop=True).shift(1)
    td[f"last{n}_pts"]= td.groupby("team")["pts"].rolling(n, min_periods=1).sum().reset_index(level=0, drop=True).shift(1)
    return td

def main():
    up = safe_read(UP)
    hist = safe_read(HIST)
    if up.empty or hist.empty or not set(REQUIRED).issubset(hist.columns):
        up.to_csv(UP, index=False)
        print("rolling_form_build: missing inputs; wrote enriched unchanged")
        return

    # Build and compute rolling
    td = build_team_history(hist)
    for n in [5,10]:
        td = roll_stats(td, n=n)

    # Map last5/10 to upcoming fixtures by team name & approximate date
    # Use the latest row per team (history up to now)
    latest = td.sort_values("date").groupby("team").tail(1)

    # Join to enriched for both home and away
    for n in [5,10]:
        maps = {
            f"home_last{n}_goals_for": ("home_team", f"last{n}_gf"),
            f"home_last{n}_goals_against": ("home_team", f"last{n}_ga"),
            f"home_last{n}_points": ("home_team", f"last{n}_pts"),
            f"away_last{n}_goals_for": ("away_team", f"last{n}_gf"),
            f"away_last{n}_goals_against": ("away_team", f"last{n}_ga"),
            f"away_last{n}_points": ("away_team", f"last{n}_pts"),
        }
        for outcol, (side_col, src) in maps.items():
            up = up.merge(latest[["team", src]].rename(columns={"team": side_col, src: outcol}),
                          on=side_col, how="left")

    up.to_csv(UP, index=False)
    print(f"rolling_form_build: updated {UP} rows={len(up)}")

if __name__ == "__main__":
    main()