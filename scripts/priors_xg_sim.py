#!/usr/bin/env python3
"""
priors_xg_sim.py — bivariate-Poisson goal mean priors
Inputs:  data/UPCOMING_7D_enriched.csv
Outputs: data/PRIORS_XG_SIM.csv   (fixture_id, xg_mu_home, xg_mu_away, xg_total_mu)

Heuristic mapping (explainable):
xg_mu_home ~ sigmoid( a0
    + a1*home_form_xg - a2*away_form_xg
    + a3*passing_accuracy_diff
    + a4*pressures90_diff + a5*tackles90_diff
    + a6*setpiece_share_diff
    + a7*keeper_psxg_prevented_diff*(-1)
    + a8*availability_diff
    + a9*exp_starters_pct_diff
    + a10*home_rest_days - a11*away_rest_days
    + a12*market_dispersion*(-1)
)
xg_mu_away symmetrical with sign flips on diffs.
"""

import os, numpy as np, pandas as pd

DATA="data"
ENR =os.path.join(DATA,"UPCOMING_7D_enriched.csv")
OUT =os.path.join(DATA,"PRIORS_XG_SIM.csv")

def safe_read(p): 
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def sigmoid(z): 
    z=np.clip(z, -12, 12)
    return 1.0/(1.0+np.exp(-z))

def main():
    up=safe_read(ENR)
    if up.empty:
        pd.DataFrame(columns=["fixture_id","xg_mu_home","xg_mu_away","xg_total_mu"]).to_csv(OUT,index=False)
        print("priors_xg_sim: no enriched; wrote header-only"); return

    if "fixture_id" not in up.columns:
        def mk_id(r):
            d=str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
            h=str(r.get("home_team","NA")).strip().lower().replace(" ","_")
            a=str(r.get("away_team","NA")).strip().lower().replace(" ","_")
            return f"{d}__{h}__vs__{a}"
        up["fixture_id"]=up.apply(mk_id, axis=1)

    def g(col): return pd.to_numeric(up.get(col, np.nan), errors="coerce")

    # Features (safe coercion)
    form_h = g("home_form_xg");   form_a = g("away_form_xg")
    pass_diff = g("home_pass_pct") - g("away_pass_pct")
    press_diff= g("home_pressures90") - g("away_pressures90")
    tkl_diff  = g("home_tackles90")   - g("away_tackles90")
    sp_diff   = g("home_setpiece_share") - g("away_setpiece_share")
    gk_prev_d = g("home_gk_psxg_prevented") - g("away_gk_psxg_prevented")
    avail_d   = g("home_avail") - g("away_avail")
    exp_d     = g("home_exp_starters_pct") - g("away_exp_starters_pct")
    rest_d    = g("home_rest_days") - g("away_rest_days")
    books     = g("bookmaker_count")

    # Weights (simple, transparent; can be learned later)
    a0= 0.10
    a1= 0.80; a2= 0.80
    a3= 0.30; a4= 0.20; a5= 0.10
    a6= 0.20
    a7= 0.25
    a8= 0.40; a9= 0.30
    a10=0.05; a11=0.05
    a12=0.02

    home_lin = (a0
        + a1*(form_h.fillna(0)) - a2*(form_a.fillna(0))
        + a3*pass_diff.fillna(0)
        + a4*press_diff.fillna(0) + a5*tkl_diff.fillna(0)
        + a6*sp_diff.fillna(0)
        - a7*gk_prev_d.fillna(0)
        + a8*avail_d.fillna(0) + a9*exp_d.fillna(0)
        + a10*rest_d.fillna(0)
        - a12*books.fillna(0)
    )

    away_lin = (a0
        + a1*(form_a.fillna(0)) - a2*(form_h.fillna(0))
        - a3*pass_diff.fillna(0)
        - a4*press_diff.fillna(0) - a5*tkl_diff.fillna(0)
        - a6*sp_diff.fillna(0)
        + a7*gk_prev_d.fillna(0)
        - a8*avail_d.fillna(0) - a9*exp_d.fillna(0)
        - a11*rest_d.fillna(0)
        - a12*books.fillna(0)
    )

    # Map to plausible goal means (0.2..3.5)
    xg_mu_home = 0.2 + 3.3*sigmoid(home_lin)
    xg_mu_away = 0.2 + 3.3*sigmoid(away_lin)
    xg_total_mu= xg_mu_home + xg_mu_away

    out=pd.DataFrame({"fixture_id":up["fixture_id"],
                      "xg_mu_home":xg_mu_home.round(3),
                      "xg_mu_away":xg_mu_away.round(3),
                      "xg_total_mu":xg_total_mu.round(3)})
    out.to_csv(OUT,index=False)
    print(f"priors_xg_sim: wrote {OUT} rows={len(out)}")

if __name__ == "__main__":
    main()