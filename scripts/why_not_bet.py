#!/usr/bin/env python3
"""
why_not_bet.py — log vetoed / zero-stake fixtures
Inputs:
  data/UPCOMING_fixtures.csv
  data/ACTIONABILITY_REPORT.csv
Outputs:
  data/WHY_NOT_BET.csv

Columns:
  fixture_id, home_team, away_team, league, reasons, final_stake
"""

import os, pandas as pd

DATA = "data"
FIX  = os.path.join(DATA, "UPCOMING_fixtures.csv")
ACT  = os.path.join(DATA, "ACTIONABILITY_REPORT.csv")
OUT  = os.path.join(DATA, "WHY_NOT_BET.csv")

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def main():
    fx = safe_read(FIX)
    act = safe_read(ACT)

    if fx.empty or act.empty:
        pd.DataFrame(columns=["fixture_id","home_team","away_team","league","reasons","final_stake"]).to_csv(OUT,index=False)
        print("why_not_bet: missing inputs; wrote header-only"); return

    # Ensure fixture_id
    if "fixture_id" not in fx.columns:
        def mk_id(r):
            d=str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
            h=str(r.get("home_team","NA")).strip().lower().replace(" ","_")
            a=str(r.get("away_team","NA")).strip().lower().replace(" ","_")
            return f"{d}__{h}__vs__{a}"
        fx["fixture_id"]=fx.apply(mk_id, axis=1)

    # Merge
    merged = fx.merge(act[["fixture_id","league","final_stake","reasons"]], on="fixture_id", how="left")

    # Flag only fixtures with stake <= 0
    vetoed = merged[(merged["final_stake"].isna()) | (merged["final_stake"]<=0)]
    vetoed = vetoed[["fixture_id","home_team","away_team","league","reasons","final_stake"]].fillna("")

    vetoed.to_csv(OUT,index=False)
    print(f"why_not_bet: wrote {OUT} rows={len(vetoed)}")

if __name__ == "__main__":
    main()