#!/usr/bin/env python3
"""
engineer_extra_variables.py
Add engineered variables into UPCOMING_7D_enriched.csv:
  - engine_home_ppg_momentum, engine_away_ppg_momentum
  - engine_home_xg_momentum,  engine_away_xg_momentum
  - engine_home_last3_ppg,    engine_away_last3_ppg
  - engine_home_season_ppg,   engine_away_season_ppg
  - engine_league_gpg_current_season
  - engine_home_travel_fatigue, engine_away_travel_fatigue
  - engine_home_finish_eff, engine_away_finish_eff
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

def season_col(dt):
    # Season by calendar year of the date (customize if you keep league-specific seasons)
    return dt.year

def build_long_hist(H):
    H["date"] = pd.to_datetime(H["date"], errors="coerce")
    H = H.dropna(subset=["date"]).sort_values("date")
    # Long format for PPG calc
    h = H[["date","home_team","home_goals","away_goals","league"]].rename(
        columns={"home_team":"team","home_goals":"gf"})
    h["ga"] = H["away_goals"]
    a = H[["date","away_team","home_goals","away_goals","league"]].rename(
        columns={"away_team":"team","away_goals":"gf"})
    a["ga"] = H["home_goals"]
    long = pd.concat([h,a], ignore_index=True)
    long["season"] = season_col(long["date"])
    # points
    long["pts"] = 0
    # reconstruct match result points in long
    # For home rows in h: already lost original role. Approximate: if gf>ga -> 3, if equal -> 1, else 0
    long.loc[long["gf"]>long["ga"], "pts"] = 3
    long.loc[long["gf"]==long["ga"], "pts"] = 1
    return long

def rolling_lastn(series, n):
    return series.rolling(n, min_periods=1).mean()

def main():
    up = safe_read(UP)
    if up.empty:
        print(f"[WARN] {UP} missing/empty; nothing to engineer.")
        return
    if "league" not in up.columns: up["league"]="GLOBAL"
    up["date"] = pd.to_datetime(up.get("date", pd.NaT), errors="coerce")

    # Base presence
    for c in ("home_team","away_team","rest_days_home","rest_days_away","home_travel_km","away_travel_km",
              "home_last5_ppg","away_last5_ppg","home_last10_ppg","away_last10_ppg",
              "home_last5_xgpg","away_last5_xgpg","home_last10_xgpg","away_last10_xgpg"):
        if c not in up.columns: up[c] = np.nan

    # 1) Momentum (last5 - last10)
    up["engine_home_ppg_momentum"] = up["home_last5_ppg"] - up["home_last10_ppg"]
    up["engine_away_ppg_momentum"] = up["away_last5_ppg"] - up["away_last10_ppg"]
    up["engine_home_xg_momentum"]  = up["home_last5_xgpg"] - up["home_last10_xgpg"]
    up["engine_away_xg_momentum"]  = up["away_last5_xgpg"] - up["away_last10_xgpg"]

    # 2) Travel fatigue: km / rest_days
    def fatigue(km, days):
        try:
            km = float(km) if np.isfinite(km) else np.nan
            days = float(days) if np.isfinite(days) else np.nan
            if not np.isfinite(km) or not np.isfinite(days) or days<=0: return np.nan
            return km/days
        except Exception:
            return np.nan
    up["engine_home_travel_fatigue"] = [fatigue(k,d) for k,d in zip(up["home_travel_km"], up["rest_days_home"])]
    up["engine_away_travel_fatigue"] = [fatigue(k,d) for k,d in zip(up["away_travel_km"], up["rest_days_away"])]

    # 3) League GPG (current season)
    H = safe_read(HIST, ["date","home_team","away_team","home_goals","away_goals","league"])
    if not H.empty:
        H["date"] = pd.to_datetime(H["date"], errors="coerce")
        H = H.dropna(subset=["date"]).sort_values("date")
        H["season"] = season_col(H["date"])
        H["gpg"] = (H["home_goals"] + H["away_goals"]).astype(float)
        league_season = H.groupby(["league","season"], as_index=False)["gpg"].mean().rename(columns={"gpg":"engine_league_gpg_current_season"})
        # Merge by league + season inferred from upcoming date
        up["season"] = up["date"].dt.year.fillna(H["season"].max() if "season" in H else pd.NaT)
        up = up.merge(league_season, on=["league","season"], how="left")
        up.drop(columns=["season"], inplace=True)
        if "engine_league_gpg_current_season" in up.columns:
            med = up["engine_league_gpg_current_season"].median()
            up["engine_league_gpg_current_season"] = up["engine_league_gpg_current_season"].fillna(med if np.isfinite(med) else 2.60)
        else:
            up["engine_league_gpg_current_season"] = 2.60
    else:
        up["engine_league_gpg_current_season"] = 2.60

    # 4) Last-3 PPG and Season-to-date PPG per team (from HIST)
    last3_home, last3_away = [], []
    season_ppg_home, season_ppg_away = [], []

    if not H.empty:
        long = build_long_hist(H)
        # For each team, precompute rolling last3 PPG and season-to-date PPG keyed by most recent date
        # We will approximate by taking last known values today.
        def team_stats(team):
            g = long[long["team"]==team].copy()
            if g.empty: return (np.nan, np.nan)
            g = g.sort_values("date")
            g["ppg3"] = rolling_lastn(g["pts"], 3)
            # season-to-date PPG: average pts in current season
            latest = g.iloc[-1]
            season = latest["season"]
            season_g = g[g["season"]==season]
            return (g["ppg3"].iloc[-1], season_g["pts"].mean() if not season_g.empty else np.nan)

        # Build dict caches
        teams = pd.unique(pd.concat([up["home_team"], up["away_team"]], ignore_index=True).dropna())
        cache = {t: team_stats(t) for t in teams}

        for ht, at in zip(up["home_team"], up["away_team"]):
            last3_home.append(cache.get(ht, (np.nan,np.nan))[0])
            last3_away.append(cache.get(at, (np.nan,np.nan))[0])
            season_ppg_home.append(cache.get(ht, (np.nan,np.nan))[1])
            season_ppg_away.append(cache.get(at, (np.nan,np.nan))[1])
    else:
        last3_home = [np.nan]*len(up); last3_away = [np.nan]*len(up)
        season_ppg_home = [np.nan]*len(up); season_ppg_away = [np.nan]*len(up)

    up["engine_home_last3_ppg"] = last3_home
    up["engine_away_last3_ppg"] = last3_away
    up["engine_home_season_ppg"] = season_ppg_home
    up["engine_away_season_ppg"] = season_ppg_away

    # 5) Finishing efficiency from team_form_features (last5 goals / last5 xg)
    form = safe_read(FORM, ["team","last5_xgpg"])
    eff_rows = []
    if not H.empty:
        # derive last-5 goals per game for each team from HIST
        H["date"] = pd.to_datetime(H["date"], errors="coerce")
        H = H.dropna(subset=["date"]).sort_values("date")
        h = H[["date","home_team","home_goals"]].rename(columns={"home_team":"team","home_goals":"gf"})
        a = H[["date","away_team","away_goals"]].rename(columns={"away_team":"team","away_goals":"gf"})
        long = pd.concat([h,a], ignore_index=True).sort_values("date")
        for team, g in long.groupby("team"):
            g = g.sort_values("date")
            g["gpg5"] = g["gf"].rolling(5, min_periods=1).mean()
            eff_rows.append({"team": team, "last5_gpg": g["gpg5"].iloc[-1]})
        eff = pd.DataFrame(eff_rows)
        if not eff.empty and not form.empty:
            eff = eff.merge(form, on="team", how="left")
            eff["engine_finish_eff"] = eff["last5_gpg"] / eff["last5_xgpg"].replace(0, np.nan)
            eff = eff[["team","engine_finish_eff"]]
            up = up.merge(eff.add_prefix("home_"), left_on="home_team", right_on="home_team", how="left")
            up = up.merge(eff.add_prefix("away_"), left_on="away_team", right_on="away_team", how="left")
            up.rename(columns={
                "home_engine_finish_eff":"engine_home_finish_eff",
                "away_engine_finish_eff":"engine_away_finish_eff"
            }, inplace=True)
        else:
            up["engine_home_finish_eff"] = np.nan
            up["engine_away_finish_eff"] = np.nan
    else:
        up["engine_home_finish_eff"] = np.nan
        up["engine_away_finish_eff"] = np.nan

    # Save
    up.to_csv(UP, index=False)
    print(f"[OK] engineered extra variables merged → {UP}")

if __name__ == "__main__":
    main()