# scripts/build_rolling_features.py
# Build team rolling form features from HIST_matches.csv:
#   last5/last10: points_per_game, goal_diff_per_game, xg_for_per_game, xg_against_per_game
# Output: data/team_form_features.csv

import os, pandas as pd, numpy as np

DATA = "data"
IN_H = os.path.join(DATA, "HIST_matches.csv")
OUT  = os.path.join(DATA, "team_form_features.csv")

def safe_read(p):
    return pd.read_csv(p) if os.path.exists(p) else pd.DataFrame()

def wdl_points(gf, ga):
    if pd.isna(gf) or pd.isna(ga): return np.nan
    if gf > ga: return 3
    if gf == ga: return 1
    return 0

def main():
    hist = safe_read(IN_H)
    if hist.empty or not {"date","home_team","away_team","home_goals","away_goals"}.issubset(hist.columns):
        pd.DataFrame(columns=[
            "team","last5_ppg","last5_gdpg","last5_xgpg","last5_xgapg",
            "last10_ppg","last10_gdpg","last10_xgpg","last10_xgapg"
        ]).to_csv(OUT, index=False)
        print("[WARN] HIST empty or missing columns; wrote header-only form features.")
        return

    # If xG per match isn’t present, approximate with gf/ga (fallback)
    has_xg = {"home_xg","away_xg"}.issubset(hist.columns)
    hist["date"] = pd.to_datetime(hist["date"], errors="coerce")

    # Build stacked match list per team with derived stats
    home = hist[["date","home_team","home_goals","away_goals"]].copy()
    home["team"] = home["home_team"]; home["gf"] = home["home_goals"]; home["ga"] = home["away_goals"]
    if has_xg:
        home["xg_for"] = hist["home_xg"]; home["xg_against"] = hist["away_xg"]
    else:
        home["xg_for"] = home["gf"]; home["xg_against"] = home["ga"]
    away = hist[["date","away_team","home_goals","away_goals"]].copy()
    away["team"] = away["away_team"]; away["gf"] = away["away_goals"]; away["ga"] = away["home_goals"]
    if has_xg:
        away["xg_for"] = hist["away_xg"]; away["xg_against"] = hist["home_xg"]
    else:
        away["xg_for"] = away["gf"]; away["xg_against"] = away["ga"]

    stack = pd.concat([home[["date","team","gf","ga","xg_for","xg_against"]],
                       away[["date","team","gf","ga","xg_for","xg_against"]]], ignore_index=True)
    stack.sort_values(["team","date"], inplace=True)
    stack["pts"] = [wdl_points(r.gf, r.ga) for r in stack.itertuples(index=False)]

    # Rolling windows per team
    feats = []
    for team, df in stack.groupby("team"):
        df = df.dropna(subset=["date"]).sort_values("date")
        for w in (5,10):
            ppg  = df["pts"].rolling(w).mean()
            gdpg = (df["gf"] - df["ga"]).rolling(w).mean()
            xgpg = df["xg_for"].rolling(w).mean()
            xgapg= df["xg_against"].rolling(w).mean()
            df[f"last{w}_ppg"]  = ppg
            df[f"last{w}_gdpg"] = gdpg
            df[f"last{w}_xgpg"] = xgpg
            df[f"last{w}_xgapg"]= xgapg
        latest = df.iloc[-1]
        feats.append({
            "team": team,
            "last5_ppg": latest.get("last5_ppg"), "last5_gdpg": latest.get("last5_gdpg"),
            "last5_xgpg": latest.get("last5_xgpg"), "last5_xgapg": latest.get("last5_xgapg"),
            "last10_ppg": latest.get("last10_ppg"), "last10_gdpg": latest.get("last10_gdpg"),
            "last10_xgpg": latest.get("last10_xgpg"), "last10_xgapg": latest.get("last10_xgapg"),
        })

    out = pd.DataFrame(feats)
    out.to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} rows={len(out)}")

if __name__ == "__main__":
    main()