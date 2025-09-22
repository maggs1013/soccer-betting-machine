#!/usr/bin/env python3
import os
import pandas as pd
import numpy as np
from datetime import datetime

RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

P1X2 = os.path.join(RUN_DIR, "PREDICTIONS_7D.csv")            # existing
PTOT = os.path.join(RUN_DIR, "PREDICTIONS_TOTALS_7D.csv")      # new
PBTTS = os.path.join(RUN_DIR, "PREDICTIONS_BTTS_7D.csv")       # new
CONSIST = os.path.join(RUN_DIR, "CONSISTENCY_CHECKS.csv")

# Simple implied-goals proxy from 1X2: μ ≈ 2.3 + 0.8*(pH - pA) (toy heuristic; refine later)
def implied_goals_from_1x2(ph, pd, pa):
    return 2.3 + 0.8*(ph - pa)

def main():
    x2 = pd.read_csv(P1X2)
    bt = pd.read_csv(PBTTS) if os.path.exists(PBTTS) else pd.DataFrame(columns=["fixture_id","p_btts_yes"])
    tt = pd.read_csv(PTOT) if os.path.exists(PTOT) else pd.DataFrame(columns=["fixture_id","p_over","p_under"])

    df = x2.merge(tt, on="fixture_id", how="left").merge(bt, on="fixture_id", how="left")
    df["implied_goals_1x2"] = implied_goals_from_1x2(df["pH"], df["pD"], df["pA"])
    df["delta_goals"] = np.abs(df["implied_goals_1x2"] - (df["p_over"]*3.1 + (1-df["p_over"])*2.1))
    df["flag_goals_vs_totals"] = (df["delta_goals"] > 0.4).astype(int)

    # Over vs BTTS coherence
    df["flag_over_vs_btts"] = ((df["p_over"] > 0.58) & (df["p_btts_yes"] < 0.50)).astype(int)

    # Placeholder for handicap coherence (requires spread pricing)
    df["flag_favorite_vs_handicap"] = 0

    out = df[["fixture_id","league","implied_goals_1x2","p_over","p_btts_yes","delta_goals",
              "flag_goals_vs_totals","flag_over_vs_btts","flag_favorite_vs_handicap"]]
    out.to_csv(CONSIST, index=False)
    print("CONSISTENCY_CHECKS.csv written:", len(out))

if __name__ == "__main__":
    main()