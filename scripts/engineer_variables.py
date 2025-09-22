# scripts/engineer_variables.py
# Derives engineered variables agreed by council+experts.
# Inputs: HIST_matches.csv, xg_metrics_hybrid.csv, team_statsbomb_features.csv
# Outputs: engineered_features.csv (per team)

import os, pandas as pd, numpy as np

DATA = "data"
HIST = os.path.join(DATA, "HIST_matches.csv")
HYB  = os.path.join(DATA, "xg_metrics_hybrid.csv")
SBF  = os.path.join(DATA, "team_statsbomb_features.csv")
OUT  = os.path.join(DATA, "engineered_features.csv")

def safe(p): return pd.read_csv(p) if os.path.exists(p) else pd.DataFrame()

def main():
    hist = safe(HIST); hyb = safe(HYB); sbf = safe(SBF)
    if hist.empty: 
        pd.DataFrame(columns=["team"]).to_csv(OUT,index=False); return
    hist["date"] = pd.to_datetime(hist["date"], errors="coerce")

    # Rest days
    rest = []
    for t in pd.unique(pd.concat([hist["home_team"], hist["away_team"]]).dropna()):
        games = hist[(hist["home_team"]==t)|(hist["away_team"]==t)].sort_values("date")
        games["prev"] = games["date"].shift(1)
        games["rest_days"] = (games["date"]-games["prev"]).dt.days
        games["games_last14"] = (games["date"]-games["prev"]).dt.days.apply(lambda d: 1 if d and d<=14 else 0)
        rest.append(games.assign(team=t)[["date","team","rest_days","games_last14"]])
    restdf = pd.concat(rest, ignore_index=True)

    # Finishing luck = Goals – xG (rolling)
    hist["result_home"] = np.where(hist["home_goals"]>hist["away_goals"],1,np.where(hist["home_goals"]==hist["away_goals"],0.5,0))
    # approximate xG if available
    if "home_xg" in hist.columns and "away_xg" in hist.columns:
        hist["finishing_luck_home"] = hist["home_goals"]-hist["home_xg"]
        hist["finishing_luck_away"] = hist["away_goals"]-hist["away_xg"]

    # GK dependence: from sbf
    gk = pd.DataFrame()
    if not sbf.empty:
        gk = sbf.groupby("team", as_index=False).agg({
            "psxg_minus_goals_sb":"mean"
        }).rename(columns={"psxg_minus_goals_sb":"gk_dependence"})

    # Merge to per-team summary
    feats = hyb.merge(gk,on="team",how="left") if not hyb.empty else gk
    feats.to_csv(OUT,index=False)
    print(f"[OK] wrote {OUT} rows={len(feats)}")

if __name__=="__main__":
    main()