#!/usr/bin/env python3
import os
import pandas as pd
import numpy as np
from datetime import datetime

RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

P1X2 = os.path.join(RUN_DIR, "PREDICTIONS_7D.csv")
PBTTS = os.path.join(RUN_DIR, "PREDICTIONS_BTTS_7D.csv")
PTOT  = os.path.join(RUN_DIR, "PREDICTIONS_TOTALS_7D.csv")
CONS  = os.path.join(RUN_DIR, "CONSISTENCY_CHECKS.csv")
ACT   = os.path.join(RUN_DIR, "ACTIONABILITY_REPORT.csv")

FRACTIONAL_KELLY = 0.4
PER_BET_MAX = 0.02      # 2%
DAILY_CAP  = 0.07       # 7%

def kelly_fraction(p, o):
    # p=win prob, o=decimal odds payoff
    edge = p*o - 1
    if o <= 1 or p<=0 or p>=1: return 0.0
    f = edge / (o-1)
    return max(0.0, f)

def main():
    # Example: apply Kelly only to 1X2 Home bets; mirror for others as you expand
    p = pd.read_csv(P1X2)
    # Expect columns: fixture_id, league, pH, pD, pA, oddsH, oddsD, oddsA (if you include odds in PREDICTIONS_7D)
    if not set(["oddsH","oddsD","oddsA"]).issubset(p.columns):
        p["oddsH"]=np.nan; p["oddsD"]=np.nan; p["oddsA"]=np.nan

    p["kelly_home"] = [kelly_fraction(ph, oh) for ph,oh in zip(p["pH"].fillna(0), p["oddsH"].fillna(0))]
    p["stake_home"] = (FRACTIONAL_KELLY * p["kelly_home"]).clip(0, PER_BET_MAX)

    # Cluster caps stub: reduce stake if many in same league
    league_counts = p["league"].value_counts().to_dict()
    p["cluster_haircut"] = p["league"].map(lambda lg: min(1.0, 5.0/max(1, league_counts.get(lg,1))))
    p["stake_home"] *= p["cluster_haircut"]

    # Daily cap scaling
    total = p["stake_home"].sum()
    scale = min(1.0, DAILY_CAP/max(1e-9, total)) if total>0 else 1.0
    p["stake_home"] *= scale
    p["cap_scale"] = scale

    # Merge consistency flags
    if os.path.exists(CONS):
        c = pd.read_csv(CONS)[["fixture_id","flag_goals_vs_totals","flag_over_vs_btts"]]
        p = p.merge(c, on="fixture_id", how="left")
    else:
        p["flag_goals_vs_totals"] = 0
        p["flag_over_vs_btts"] = 0

    # Actionability
    act = p[["fixture_id","league","pH","pD","pA","oddsH","stake_home","cap_scale",
             "flag_goals_vs_totals","flag_over_vs_btts"]].copy()
    act.rename(columns={"stake_home":"stake"}, inplace=True)
    act.to_csv(ACT, index=False)
    print("ACTIONABILITY_REPORT.csv written:", len(act))

if __name__ == "__main__":
    main()