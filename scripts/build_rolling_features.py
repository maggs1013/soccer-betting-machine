#!/usr/bin/env python3
"""
build_rolling_features.py  —  robust team-form builder

Purpose
-------
Compute rolling form features from historical matches per team, safely:
  • last5_ppg, last10_ppg
  • last5_gdpg, last10_gdpg
  • last5_xgpg / xgapg, last10_xgpg / xgapg (if xG fields exist; else NaN)

Inputs (data/):
  HIST_matches.csv   (date, home_team, away_team, home_goals, away_goals, optional xg columns)

Output (data/):
  team_form_features.csv
"""

import os
import numpy as np
import pandas as pd

DATA = "data"
HIST = os.path.join(DATA, "HIST_matches.csv")
OUT  = os.path.join(DATA, "team_form_features.csv")

def safe_read(path):
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

def to_long(df):
    # Build long format: team, date, gf, ga, xg, xga, pts
    need = {"date","home_team","away_team","home_goals","away_goals"}
    if df.empty or not need.issubset(df.columns):
        return pd.DataFrame(columns=["date","team","gf","ga","xg","xga","pts"])

    d = df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d = d.dropna(subset=["date"]).sort_values("date")

    # goals
    h = d[["date","home_team","home_goals","away_goals"]].rename(
        columns={"home_team":"team","home_goals":"gf","away_goals":"ga"}
    )
    a = d[["date","away_team","home_goals","away_goals"]].rename(
        columns={"away_team":"team","home_goals":"ga","away_goals":"gf"}
    )

    # points
    # home: win=3 if home_goals>away_goals else draw=1 else 0; away inverse
    h["pts"] = np.where(d["home_goals"] > d["away_goals"], 3, np.where(d["home_goals"]==d["away_goals"], 1, 0))
    a["pts"] = np.where(d["away_goals"] > d["home_goals"], 3, np.where(d["home_goals"]==d["away_goals"], 1, 0))

    # optional xG
    xg_cols = {"home_xg":"xg", "away_xg":"xg", "home_xga":"xga", "away_xga":"xga"}
    # try common historical names too
    for src, tgt in [("home_xg","xg"), ("away_xg","xg"), ("home_xga","xga"), ("away_xga","xga"),
                     ("home_xg_total","xg"), ("away_xg_total","xg"),
                     ("home_xga_total","xga"), ("away_xga_total","xga")]:
        if src in d.columns:
            # append to temp frames
            if src.startswith("home_"):
                h[tgt] = d[src]
            else:
                a[tgt] = d[src]

    # ensure xg/xga exist
    for col in ["xg","xga"]:
        if col not in h.columns: h[col] = np.nan
        if col not in a.columns: a[col] = np.nan

    long = pd.concat([h, a], ignore_index=True)
    long = long[["date","team","gf","ga","xg","xga","pts"]].sort_values(["team","date"])
    return long

def roll_feats(g):
    # g is per-team sorted by date; compute rolling windows
    g = g.copy().sort_values("date")
    for n in (5, 10):
        # points per game
        g[f"last{n}_ppg"] = g["pts"].rolling(n, min_periods=1).mean()
        # goal diff per game
        g[f"last{n}_gdpg"] = (g["gf"] - g["ga"]).rolling(n, min_periods=1).mean()
        # xg pg (if available)
        g[f"last{n}_xgpg"]   = g["xg"].rolling(n, min_periods=1).mean()
        g[f"last{n}_xgapg"]  = g["xga"].rolling(n, min_periods=1).mean()
    return g

def main():
    df = safe_read(HIST)
    long = to_long(df)
    if long.empty:
        pd.DataFrame(columns=[
            "team","last5_ppg","last5_gdpg","last5_xgpg","last5_xgapg",
            "last10_ppg","last10_gdpg","last10_xgpg","last10_xgapg"
        ]).to_csv(OUT, index=False)
        print(f"[WARN] {HIST} missing/empty; wrote empty {OUT}")
        return

    out = []
    for team, g in long.groupby("team"):
        gg = roll_feats(g)
        # take the most recent row for each team as current form
        r = gg.iloc[-1]
        out.append({
            "team": team,
            "last5_ppg":  r["last5_ppg"],  "last5_gdpg":  r["last5_gdpg"],
            "last5_xgpg": r["last5_xgpg"], "last5_xgapg": r["last5_xgapg"],
            "last10_ppg": r["last10_ppg"], "last10_gdpg": r["last10_gdpg"],
            "last10_xgpg":r["last10_xgpg"],"last10_xgapg":r["last10_xgapg"],
        })

    out_df = pd.DataFrame(out)
    out_df.to_csv(OUT, index=False)
    print(f"[OK] build_rolling_features: wrote {OUT} rows={len(out_df)}")

if __name__ == "__main__":
    main()