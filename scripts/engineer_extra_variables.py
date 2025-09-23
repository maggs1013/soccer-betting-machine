#!/usr/bin/env python3
"""
engineer_extra_variables.py
Compute additional variables and merge into UPCOMING_7D_enriched.csv:

- engine_home_ppg_momentum, engine_away_ppg_momentum  (last5_ppg - last10_ppg)
- engine_home_xg_momentum,  engine_away_xg_momentum   (last5_xgpg - last10_xgpg)
- engine_league_gpg                                 (league average goals/game)
- engine_home_travel_fatigue, engine_away_travel_fatigue (km / rest_days, safe)
- engine_home_finish_eff, engine_away_finish_eff     (last5 goals pg / last5 xg pg)

Inputs:
  data/UPCOMING_7D_enriched.csv
  data/HIST_matches.csv
  data/team_form_features.csv  (already used in your pipeline)

Output:
  data/UPCOMING_7D_enriched.csv   (updated in-place with new columns)
"""

import os
import numpy as np
import pandas as pd

DATA = "data"
UP_PATH = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
HIST   = os.path.join(DATA, "HIST_matches.csv")
FORM   = os.path.join(DATA, "team_form_features.csv")

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

def main():
    up = safe_read(UP_PATH)
    if up.empty:
        print(f"[WARN] {UP_PATH} missing/empty; nothing to engineer.")
        return

    # Ensure expected identifiers
    if "league" not in up.columns:
        up["league"] = "GLOBAL"
    for side in ("home","away"):
        if f"{side}_team" not in up.columns:
            up[f"{side}_team"] = np.nan

    # Momentum from existing form features (already merged in upstream pipeline)
    # If some columns are missing, fill with NaN
    for base in ("home","away"):
        for c in ("last5_ppg","last10_ppg","last5_xgpg","last10_xgpg","rest_days"):
            col = f"{base}_{c}" if c != "rest_days" else f"rest_days_{base}"
            if col not in up.columns:
                # accept both patterns; normalize to home_last5_ppg, etc. exist upstream usually
                if c == "rest_days":
                    # we standardize to rest_days_home/away presence in other scripts
                    pass
                else:
                    up[f"{base}_{c}"] = np.nan

    up["engine_home_ppg_momentum"] = up.get("home_last5_ppg", np.nan) - up.get("home_last10_ppg", np.nan)
    up["engine_away_ppg_momentum"] = up.get("away_last5_ppg", np.nan) - up.get("away_last10_ppg", np.nan)

    up["engine_home_xg_momentum"]  = up.get("home_last5_xgpg", np.nan) - up.get("home_last10_xgpg", np.nan)
    up["engine_away_xg_momentum"]  = up.get("away_last5_xgpg", np.nan) - up.get("away_last10_xgpg", np.nan)

    # League tempo: average goals per game per league from HIST
    hist = safe_read(HIST, ["league","home_goals","away_goals"])
    if hist.empty:
        league_gpg = pd.DataFrame({"league":["GLOBAL"], "engine_league_gpg":[2.60]})
    else:
        if "league" not in hist.columns:
            hist["league"] = "GLOBAL"
        hist["gpg"] = (hist["home_goals"] + hist["away_goals"]).astype(float)
        league_gpg = hist.groupby("league", as_index=False)["gpg"].mean().rename(columns={"gpg":"engine_league_gpg"})
    up = up.merge(league_gpg, on="league", how="left")
    up["engine_league_gpg"] = up["engine_league_gpg"].fillna(up["engine_league_gpg"].median() if "engine_league_gpg" in up.columns else 2.60)

    # Travel fatigue: km / rest_days (safe division)
    def fatigue(km, days):
        try:
            km = float(km) if np.isfinite(km) else np.nan
            days = float(days) if np.isfinite(days) else np.nan
            if not np.isfinite(km) or not np.isfinite(days) or days <= 0:
                return np.nan
            return km / days
        except Exception:
            return np.nan
    # Standardize rest day column names if needed
    if "rest_days_home" not in up.columns and "home_rest_days" in up.columns:
        up["rest_days_home"] = up["home_rest_days"]
    if "rest_days_away" not in up.columns and "away_rest_days" in up.columns:
        up["rest_days_away"] = up["away_rest_days"]

    up["engine_home_travel_fatigue"] = [fatigue(k, d) for k, d in zip(up.get("home_travel_km", np.nan), up.get("rest_days_home", np.nan))]
    up["engine_away_travel_fatigue"] = [fatigue(k, d) for k, d in zip(up.get("away_travel_km", np.nan), up.get("rest_days_away", np.nan))]

    # Finishing efficiency (rolling) from HIST (last 5 matches per team)
    eff_rows = []
    if not hist.empty:
        # Need per team recent goals and xg (if available); fallback to goals only if no xg
        # We'll approximate with goals per match from last 5 and xg from team_form_features last5_xgpg
        form = safe_read(FORM, ["team","last5_xgpg"])
        # Build last-5 goals per game from HIST
        H = safe_read(HIST, ["date","home_team","away_team","home_goals","away_goals"])
        if not H.empty:
            H["date"] = pd.to_datetime(H["date"], errors="coerce")
            H = H.dropna(subset=["date"]).sort_values("date")
            long = []
            # Home rows
            h = H[["date","home_team","home_goals"]].rename(columns={"home_team":"team","home_goals":"gf"})
            a = H[["date","away_team","away_goals"]].rename(columns={"away_team":"team","away_goals":"gf"})
            long = pd.concat([h,a], ignore_index=True)
            for team, g in long.groupby("team"):
                g = g.sort_values("date")
                g["gf5"] = g["gf"].rolling(5, min_periods=1).mean()
                eff_rows.append({"team": team, "last5_gpg": g["gf5"].iloc[-1]})
        eff = pd.DataFrame(eff_rows)
        if not eff.empty:
            if not form.empty:
                eff = eff.merge(form, on="team", how="left")
                eff["engine_finish_eff"] = eff["last5_gpg"] / eff["last5_xgpg"].replace(0, np.nan)
            else:
                eff["engine_finish_eff"] = np.nan
            eff = eff[["team","engine_finish_eff"]]

            # Merge for home and away
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

    # Save back
    up.to_csv(UP_PATH, index=False)
    print(f"[OK] engineered variables merged into {UP_PATH}")

if __name__ == "__main__":
    main()