#!/usr/bin/env python3
import os
import pandas as pd
import numpy as np
from datetime import datetime

RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

# Primary (RUN) paths
P1X2_RUN = os.path.join(RUN_DIR, "PREDICTIONS_7D.csv")
PTOT_RUN = os.path.join(RUN_DIR, "PREDICTIONS_TOTALS_7D.csv")
PBTTS_RUN= os.path.join(RUN_DIR, "PREDICTIONS_BTTS_7D.csv")

# Fallback (DATA) paths
P1X2_DATA = os.path.join("data", "PREDICTIONS_7D.csv")
PTOT_DATA = os.path.join("data", "PREDICTIONS_TOTALS_7D.csv")
PBTTS_DATA= os.path.join("data", "PREDICTIONS_BTTS_7D.csv")

OUT = os.path.join(RUN_DIR, "CONSISTENCY_CHECKS.csv")

def read_first(*paths):
    for p in paths:
        if os.path.exists(p):
            try:
                return pd.read_csv(p)
            except Exception:
                pass
    return pd.DataFrame()

def implied_goals_from_1x2(ph, pd, pa):
    # toy heuristic; refine later
    return 2.3 + 0.8*(ph - pa)

def main():
    x2 = read_first(P1X2_RUN, P1X2_DATA)
    tt = read_first(PTOT_RUN, PTOT_DATA)
    bt = read_first(PBTTS_RUN, PBTTS_DATA)

    if x2.empty:
        # Write empty file so pipeline doesn’t crash
        pd.DataFrame(columns=[
            "fixture_id","league","implied_goals_1x2","p_over","p_btts_yes",
            "delta_goals","flag_goals_vs_totals","flag_over_vs_btts","flag_favorite_vs_handicap"
        ]).to_csv(OUT, index=False)
        print(f"[WARN] No 1X2 predictions; wrote empty {OUT}")
        return

    if "fixture_id" not in x2.columns:
        print("[WARN] PREDICTIONS_7D.csv missing fixture_id; adding placeholder.")
        x2["fixture_id"] = range(len(x2))
    if "league" not in x2.columns:
        x2["league"] = "GLOBAL"

    # minimal totals / BTTS frames
    if tt.empty: tt = pd.DataFrame(columns=["fixture_id","p_over","p_under"])
    if bt.empty: bt = pd.DataFrame(columns=["fixture_id","p_btts_yes"])

    df = x2.merge(tt, on="fixture_id", how="left").merge(bt, on="fixture_id", how="left")

    # Safe fills
    for c in ["p_over","p_btts_yes"]:
        if c not in df.columns: df[c] = np.nan

    df["implied_goals_1x2"] = implied_goals_from_1x2(df["pH"], df["pD"], df["pA"])
    df["delta_goals"] = np.abs(df["implied_goals_1x2"] - (df["p_over"]*3.1 + (1.0 - df["p_over"])*2.1))

    df["flag_goals_vs_totals"] = ((df["delta_goals"] > 0.4) & df["p_over"].notna()).astype(int)
    df["flag_over_vs_btts"]   = ((df["p_over"] > 0.58) & (df["p_btts_yes"] < 0.50)).fillna(0).astype(int)
    df["flag_favorite_vs_handicap"] = 0  # placeholder until spread is added

    out = df[[
        "fixture_id","league","implied_goals_1x2","p_over","p_btts_yes","delta_goals",
        "flag_goals_vs_totals","flag_over_vs_btts","flag_favorite_vs_handicap"
    ]]
    out.to_csv(OUT, index=False)
    print("CONSISTENCY_CHECKS.csv written:", len(out))

if __name__ == "__main__":
    main()