#!/usr/bin/env python3
"""
priors_availability.py — convert lineup & schedule to goal-shift priors
Inputs:  data/UPCOMING_7D_enriched.csv
Outputs: data/PRIORS_AVAIL.csv  (fixture_id, avail_goal_shift_home, avail_goal_shift_away)
"""

import os, numpy as np, pandas as pd
DATA="data"
ENR =os.path.join(DATA,"UPCOMING_7D_enriched.csv")
OUT =os.path.join(DATA,"PRIORS_AVAIL.csv")

def safe_read(p): 
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def main():
    up=safe_read(ENR)
    if up.empty:
        pd.DataFrame(columns=["fixture_id","avail_goal_shift_home","avail_goal_shift_away"]).to_csv(OUT,index=False)
        print("priors_availability: no enriched; wrote header-only"); return

    if "fixture_id" not in up.columns:
        def mk_id(r):
            d=str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
            h=str(r.get("home_team","NA")).strip().lower().replace(" ","_")
            a=str(r.get("away_team","NA")).strip().lower().replace(" ","_")
            return f"{d}__{h}__vs__{a}"
        up["fixture_id"]=up.apply(mk_id, axis=1)

    def g(c): return pd.to_numeric(up.get(c, np.nan), errors="coerce")

    avail_d = g("home_avail") - g("away_avail")
    exp_d   = g("home_exp_starters_pct") - g("away_exp_starters_pct")
    rest_d  = g("home_rest_days") - g("away_rest_days")
    g7_d    = g("home_games_last7") - g("away_games_last7")
    g14_d   = g("home_games_last14") - g("away_games_last14")
    travel_d= g("home_travel_km") - g("away_travel_km")

    # Simple linear shift, scaled to goal-space ~ [-0.6, +0.6]
    w = dict(avail=0.60, exp=0.45, rest=0.05, g7=-0.05, g14=-0.03, travel=-0.04)
    home_shift = (w["avail"]*avail_d.fillna(0) + w["exp"]*exp_d.fillna(0)
                  + w["rest"]*rest_d.fillna(0) + w["g7"]*g7_d.fillna(0)
                  + w["g14"]*g14_d.fillna(0)   + w["travel"]*travel_d.fillna(0)).clip(-0.6, 0.6)
    away_shift = -home_shift

    pd.DataFrame({"fixture_id":up["fixture_id"],
                  "avail_goal_shift_home":home_shift.round(3),
                  "avail_goal_shift_away":away_shift.round(3)}).to_csv(OUT,index=False)
    print(f"priors_availability: wrote {OUT} rows={len(up)}")

if __name__ == "__main__":
    main()