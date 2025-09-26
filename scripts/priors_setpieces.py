#!/usr/bin/env python3
"""
priors_setpieces.py — dead-ball xG prior
Inputs:  data/UPCOMING_7D_enriched.csv
Outputs: data/PRIORS_SETPIECE.csv (fixture_id, sp_xg_prior_home, sp_xg_prior_away)
"""

import os, numpy as np, pandas as pd
DATA="data"
ENR =os.path.join(DATA,"UPCOMING_7D_enriched.csv")
OUT =os.path.join(DATA,"PRIORS_SETPIECE.csv")

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def main():
    up=safe_read(ENR)
    if up.empty:
        pd.DataFrame(columns=["fixture_id","sp_xg_prior_home","sp_xg_prior_away"]).to_csv(OUT,index=False)
        print("priors_setpieces: no enriched; wrote header-only"); return

    if "fixture_id" not in up.columns:
        def mk_id(r):
            d=str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
            h=str(r.get("home_team","NA")).strip().lower().replace(" ","_")
            a=str(r.get("away_team","NA")).strip().lower().replace(" ","_")
            return f"{d}__{h}__vs__{a}"
        up["fixture_id"]=up.apply(mk_id, axis=1)

    def g(c): return pd.to_numeric(up.get(c, np.nan), errors="coerce")
    sp   = g("home_setpiece_share") - g("away_setpiece_share")
    crd_d= g("home_avg_cards") - g("away_avg_cards")   # seasonal priors
    ref  = g("ref_avg_cards")                          # referee prior

    # proxy dead-ball xG prior in [ -0.35 , +0.35 ]
    prior = (0.60*sp.fillna(0) + 0.25*crd_d.fillna(0) + 0.15*(ref.fillna(0)-3.5)/3.5).clip(-0.35, 0.35)
    home = prior; away = -prior

    pd.DataFrame({"fixture_id":up["fixture_id"],
                  "sp_xg_prior_home":home.round(3),
                  "sp_xg_prior_away":away.round(3)}).to_csv(OUT,index=False)
    print(f"priors_setpieces: wrote {OUT} rows={len(up)}")

if __name__ == "__main__":
    main()