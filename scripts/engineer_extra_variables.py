#!/usr/bin/env python3
"""
engineer_extra_variables.py

Add engineered variables into data/UPCOMING_7D_enriched.csv:

Windows (per team):
  - PPG: last3, last5, last7, last10, season-to-date
  - xGPG: last3, last5, last7, last10, season-to-date (if available)
Contrasts:
  - ppg_momentum_3_10     = last3_ppg  - last10_ppg
  - ppg_momentum_5_season = last5_ppg  - season_ppg
  - xg_momentum_3_10      = last3_xgpg - last10_xgpg
  - xg_momentum_5_season  = last5_xgpg - season_xgpg
Stability:
  - goal_volatility_5/10  = variance of goals over last N
League context:
  - engine_league_gpg_current_season
Fatigue / Efficiency:
  - travel_fatigue = km / rest_days
  - finishing_eff  = last5 goals pg / last5 xg pg (if xg available)

All engineered columns are prefixed with 'engine_' where appropriate.
"""

import os
import numpy as np
import pandas as pd

DATA = "data"
UP = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
HIST = os.path.join(DATA, "HIST_matches.csv")
FORM = os.path.join(DATA, "team_form_features.csv")

def safe_read(path, cols=None):
    if not os.path.exists(path):
        return pd.DataFrame(columns=cols or [])
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=cols or [])
    if cols:
        for c in cols:
            if c not in df.columns:
                df[c] = np.nan
    return df

def to_season(dt: pd.Series) -> pd.Series:
    dt = pd.to_datetime(dt, errors="coerce")
    return dt.dt.year

def build_long_hist(H):
    H["date"] = pd.to_datetime(H["date"], errors="coerce")
    H = H.dropna(subset=["date"]).sort_values("date")
    # Long format goals
    h = H[["date","home_team","home_goals","away_goals","league"]].rename(
        columns={"home_team":"team","home_goals":"gf"})
    h["ga"] = H["away_goals"]
    a = H[["date","away_team","home_goals","away_goals","league"]].rename(
        columns={"away_team":"team","away_goals":"gf"})
    a["ga"] = H["home_goals"]
    long = pd.concat([h,a], ignore_index=True)
    long["season"] = to_season(long["date"])
    # points
    long["pts"] = 0
    long.loc[long["gf"] > long["ga"], "pts"] = 3
    long.loc[long["gf"] == long["ga"], "pts"] = 1
    return long.sort_values(["team","date"])

def rolling_mean(series, n):
    return series.rolling(n, min_periods=1).mean()

def rolling_var(series, n):
    return series.rolling(n, min_periods=2).var()

def main():
    up = safe_read(UP)
    if up.empty:
        print(f"[WARN] {UP} missing/empty; nothing to engineer.")
        return
    if "league" not in up.columns:
        up["league"] = "GLOBAL"
    up["date"] = pd.to_datetime(up.get("date", pd.NaT), errors="coerce")

    # Ensure columns exist
    for c in ("home_team","away_team","rest_days_home","rest_days_away","home_travel_km","away_travel_km"):
        if c not in up.columns: up[c] = np.nan

    # Travel fatigue
    def fatigue(km, days):
        try:
            km  = float(km)  if np.isfinite(km)  else np.nan
            days= float(days)if np.isfinite(days)else np.nan
            if not np.isfinite(km) or not np.isfinite(days) or days <= 0: return np.nan
            return km / days
        except Exception:
            return np.nan
    up["engine_home_travel_fatigue"] = [fatigue(k,d) for k,d in zip(up["home_travel_km"], up["rest_days_home"])]
    up["engine_away_travel_fatigue"] = [fatigue(k,d) for k,d in zip(up["away_travel_km"], up["rest_days_away"])]

    # Historical data
    H = safe_read(HIST, ["date","home_team","away_team","home_goals","away_goals","league"])
    if H.empty:
        # fallback default league gpg
        up["engine_league_gpg_current_season"] = 2.60
        # create placeholders for engineered features
        for side in ("home","away"):
            for nm in ("last3_ppg","last5_ppg","last7_ppg","last10_ppg","season_ppg",
                       "last3_xgpg","last5_xgpg","last7_xgpg","last10_xgpg","season_xgpg",
                       "ppg_momentum_3_10","ppg_momentum_5_season","xg_momentum_3_10","xg_momentum_5_season",
                       "goal_volatility_5","goal_volatility_10","finish_eff"):
                up[f"engine_{side}_{nm}"] = np.nan
        up.to_csv(UP, index=False)
        print(f"[OK] engineered variables (fallback only) → {UP}")
        return

    H["date"] = pd.to_datetime(H["date"], errors="coerce")
    H = H.dropna(subset=["date"]).sort_values("date")
    H["season"] = to_season(H["date"])

    # League goals per game (current season by league)
    H["gpg"] = (H["home_goals"] + H["away_goals"]).astype(float)
    league_gpg_season = H.groupby(["league","season"], as_index=False)["gpg"].mean().rename(columns={"gpg":"engine_league_gpg_current_season"})
    up["season"] = up["date"].dt.year.fillna(H["season"].max())
    up = up.merge(league_gpg_season, on=["league","season"], how="left").drop(columns=["season"])
    if "engine_league_gpg_current_season" in up.columns:
        med = up["engine_league_gpg_current_season"].median()
        up["engine_league_gpg_current_season"] = up["engine_league_gpg_current_season"].fillna(med if np.isfinite(med) else 2.60)
    else:
        up["engine_league_gpg_current_season"] = 2.60

    # Build long format; compute rolling windows per team
    long = build_long_hist(H)  # columns: team,date,league,season, gf,ga, pts
    # If you have team-level xG history, you can integrate here; else approximate via form file for xG windows
    form = safe_read(FORM, ["team","last5_xgpg"])  # used for finishing efficiency baseline

    # compute rolling stats per team
    windows = [3,5,7,10]
    per_team = []
    for team, g in long.groupby("team"):
        g = g.sort_values("date")
        row = {"team": team}
        # PPG windows
        for n in windows:
            row[f"last{n}_ppg"] = rolling_mean(g["pts"], n).iloc[-1]
        # Season PPG
        season = g["season"].iloc[-1]
        g_season = g[g["season"] == season]
        row["season_ppg"] = g_season["pts"].mean() if not g_season.empty else np.nan

        # Goals rolling (for volatility & proxy xG if needed)
        for n in (5,10):
            row[f"goal_volatility_{n}"] = rolling_var(g["gf"], n).iloc[-1]

        # xGPG windows (if no direct team xG timeline, approximate via last5_xgpg from form)
        # We leave last3/7/10_xgpg NaN unless you maintain historical xG per match.
        for n in windows:
            row[f"last{n}_xgpg"] = np.nan

        # finishing efficiency baseline if form has last5_xgpg
        if not form.empty and team in set(form["team"]):
            xg5 = float(form[form["team"]==team]["last5_xgpg"].iloc[0])
            gf5 = rolling_mean(g["gf"], 5).iloc[-1]
            row["finish_eff"] = (gf5 / xg5) if (xg5 and np.isfinite(xg5)) else np.nan
            row["last5_xgpg"] = xg5 if np.isfinite(xg5) else np.nan  # fill last5_xgpg
        else:
            row["finish_eff"] = np.nan

        per_team.append(row)

    tv = pd.DataFrame(per_team)

    # Momentum (contrasts)
    def add_momentum(tdf, short, long, prefix):
        s = tdf.get(f"last{short}_{prefix}")
        l = tdf.get(f"last{long}_{prefix}")
        if s is None or l is None:
            return np.nan
        return s - l

    tv["ppg_momentum_3_10"]     = tv["last3_ppg"]  - tv["last10_ppg"]
    tv["ppg_momentum_5_season"]  = tv["last5_ppg"]  - tv["season_ppg"]
    tv["xg_momentum_3_10"]       = tv["last3_xgpg"] - tv["last10_xgpg"]
    tv["xg_momentum_5_season"]   = tv["last5_xgpg"] - tv["season_xgpg"] if "season_xgpg" in tv else np.nan

    # Merge to UPCOMING for home/away
    def merge_side(up_df, tdf, side):
        pref = f"engine_{side}_"
        up_df = up_df.merge(tdf.add_prefix(f"{side}_"), left_on=f"{side}_team", right_on=f"{side}_team", how="left")
        # rename selected engineered columns
        rename_map = {}
        for col in tdf.columns:
            if col == "team": continue
            rename_map[f"{side}_{col}"] = f"{pref}{col}"
        up_df.rename(columns=rename_map, inplace=True)
        return up_df

    up = merge_side(up, tv, "home")
    up = merge_side(up, tv, "away")

    up.to_csv(UP, index=False)
    print(f"[OK] engineered variables merged → {UP}")

if __name__ == "__main__":
    main()