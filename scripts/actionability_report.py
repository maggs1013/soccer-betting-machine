# scripts/actionability_report.py
# Marks which upcoming predictions have bookmaker odds (Kelly actionable) vs probability-only.
# Output: data/ACTIONABILITY_REPORT.csv

import os, pandas as pd, numpy as np

DATA="data"
UP  = os.path.join(DATA,"UPCOMING_7D_enriched.csv")
PR  = os.path.join(DATA,"PREDICTIONS_7D.csv")
OUT = os.path.join(DATA,"ACTIONABILITY_REPORT.csv")

def main():
    if not (os.path.exists(UP) and os.path.exists(PR)):
        pd.DataFrame(columns=["date","home_team","away_team","has_odds","pH","pD","pA","kelly_H","kelly_D","kelly_A"]).to_csv(OUT,index=False)
        print("[WARN] Missing inputs; wrote empty actionability report."); return
    up=pd.read_csv(UP); pr=pd.read_csv(PR)
    if up.empty or pr.empty:
        pd.DataFrame(columns=["date","home_team","away_team","has_odds","pH","pD","pA","kelly_H","kelly_D","kelly_A"]).to_csv(OUT,index=False)
        print("[WARN] Empty inputs; wrote empty actionability report."); return

    need={"date","home_team","away_team"}
    if not need.issubset(up.columns) or not need.issubset(pr.columns):
        pd.DataFrame(columns=["date","home_team","away_team","has_odds","pH","pD","pA","kelly_H","kelly_D","kelly_A"]).to_csv(OUT,index=False)
        print("[WARN] Missing keys; wrote empty."); return

    has_odds = up.get("home_odds_dec").notna() & up.get("draw_odds_dec").notna() & up.get("away_odds_dec").notna() if \
               {"home_odds_dec","draw_odds_dec","away_odds_dec"}.issubset(up.columns) else pd.Series(False,index=up.index)
    up2 = up[["date","home_team","away_team"]].copy()
    up2["has_odds"] = has_odds

    rep = up2.merge(pr, on=["date","home_team","away_team"], how="left")
    rep.to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} rows={len(rep)} | odds_coverage={(rep['has_odds'].mean()*100 if len(rep) else 0):.1f}%")

if __name__ == "__main__":
    main()