#!/usr/bin/env python3
"""
priors_uncertainty.py — uncertainty penalty from SPI CI width & OU
Inputs:  data/UPCOMING_7D_enriched.csv
Outputs: data/PRIORS_UNC.csv (fixture_id, uncertainty_penalty)
"""

import os, numpy as np, pandas as pd
DATA="data"; ENR=os.path.join(DATA,"UPCOMING_7D_enriched.csv")
OUT=os.path.join(DATA,"PRIORS_UNC.csv")

def safe_read(p): 
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def main():
    up=safe_read(ENR)
    if up.empty:
        pd.DataFrame(columns=["fixture_id","uncertainty_penalty"]).to_csv(OUT,index=False)
        print("priors_uncertainty: no enriched; wrote header-only"); return

    if "fixture_id" not in up.columns:
        def mk_id(r):
            d=str(r.get("date","NA")).replace("-","").replace("T","_").replace(":", "")
            h=str(r.get("home_team","NA")).strip().lower().replace(" ","_")
            a=str(r.get("away_team","NA")).strip().lower().replace(" ","_")
            return f"{d}__{h}__vs__{a}"
        up["fixture_id"]=up.apply(mk_id, axis=1)

    from numpy import clip
    w = pd.to_numeric(up.get("home_spi_ci_width", np.nan), errors="coerce") - pd.to_numeric(up.get("away_spi_ci_width", np.nan), errors="coerce")
    ou = pd.to_numeric(up.get("ou_main_total", np.nan), errors="coerce")
    pen = clip((w.fillna(0).abs()/2.0) * (1.0 + (ou.fillna(2.5)-2.5).abs()/2.0), 0, 2.0)
    pd.DataFrame({"fixture_id":up["fixture_id"], "uncertainty_penalty":pen.round(3)}).to_csv(OUT,index=False)
    print(f"priors_uncertainty: wrote {OUT} rows={len(up)}")

if __name__ == "__main__":
    main()