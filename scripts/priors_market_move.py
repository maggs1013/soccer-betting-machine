#!/usr/bin/env python3
"""
priors_market_move.py — market informed score
Inputs:  data/UPCOMING_7D_enriched.csv
Outputs: data/PRIORS_MKT.csv (fixture_id, market_informed_score)
"""

import os, numpy as np, pandas as pd
DATA="data"; ENR=os.path.join(DATA,"UPCOMING_7D_enriched.csv")
OUT=os.path.join(DATA,"PRIORS_MKT.csv")

def safe_read(p): 
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def z(x): 
    x=pd.to_numeric(x, errors="coerce")
    mu=x.mean(skipna=True); sd=x.std(skipna=True)
    if not np.isfinite(sd) or sd==0: return x*0
    return (x-mu)/sd

def main():
    up=safe_read(ENR)
    if up.empty:
        pd.DataFrame(columns=["fixture_id","market_informed_score"]).to_csv(OUT,index=False)
        print("priors_market_move: no enriched; wrote header-only"); return

    if "fixture_id" not in up.columns:
        def mk_id(r):
            d=str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
            h=str(r.get("home_team","NA")).strip().lower().replace(" ","_")
            a=str(r.get("away_team","NA")).strip().lower().replace(" ","_")
            return f"{d}__{h}__vs__{a}"
        up["fixture_id"]=up.apply(mk_id, axis=1)

    # Features (safe)
    ou_move = z(up.get("ou_move", np.nan))
    hh = z(up.get("h2h_home_move", np.nan)); aw = z(up.get("h2h_away_move", np.nan)); dr = z(up.get("h2h_draw_move", np.nan))
    disp = z(up.get("bookmaker_count", np.nan))
    openf= up.get("has_opening_odds", 0).fillna(0).astype(float)
    closef=up.get("has_closing_odds", 0).fillna(0).astype(float)

    # Score: +ve if moves align + presence of opening/closing; -ve if dispersion high without timing
    score = (0.4*ou_move.fillna(0) + 0.25*(hh.fillna(0)-aw.fillna(0)) + 0.10*dr.fillna(0)
             + 0.15*(openf+closef) - 0.20*(disp.fillna(0)>1.5).astype(float))
    pd.DataFrame({"fixture_id":up["fixture_id"], "market_informed_score":score.round(3)}).to_csv(OUT,index=False)
    print(f"priors_market_move: wrote {OUT} rows={len(up)}")

if __name__ == "__main__":
    main()