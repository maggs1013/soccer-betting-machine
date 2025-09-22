# scripts/edge_distribution.py
# Edge distribution by league, odds bucket, and side (H/D/A) using upcoming predictions.
# Reads: data/UPCOMING_7D_enriched.csv, data/PREDICTIONS_7D.csv
# Writes: data/EDGE_DISTRIBUTION.csv

import os, pandas as pd, numpy as np

DATA="data"
UP  = os.path.join(DATA,"UPCOMING_7D_enriched.csv")
PR  = os.path.join(DATA,"PREDICTIONS_7D.csv")
OUT = os.path.join(DATA,"EDGE_DISTRIBUTION.csv")

def implied(d):
    try:
        d=float(d); return 1.0/d if d>0 else np.nan
    except: return np.nan

def main():
    if not (os.path.exists(UP) and os.path.exists(PR)):
        pd.DataFrame(columns=["league","odds_bucket","side","n","avg_edge","pct_positive","mean_kelly_if_odds"]).to_csv(OUT,index=False)
        print(f"[WARN] Missing inputs; wrote empty {OUT}")
        return
    up=pd.read_csv(UP); pr=pd.read_csv(PR)
    if "league" not in up.columns: up["league"]="GLOBAL"
    df=up.merge(pr, on=["date","home_team","away_team"], how="inner")
    if df.empty:
        pd.DataFrame(columns=["league","odds_bucket","side","n","avg_edge","pct_positive","mean_kelly_if_odds"]).to_csv(OUT,index=False)
        print(f"[WARN] Merge produced empty; wrote empty {OUT}")
        return

    # market implied probs
    df["mH"]=df["home_odds_dec"].map(implied)
    df["mD"]=df["draw_odds_dec"].map(implied)
    df["mA"]=df["away_odds_dec"].map(implied)
    s=df["mH"]+df["mD"]+df["mA"]
    df["mH"]=df["mH"]/s; df["mD"]=df["mD"]/s; df["mA"]=df["mA"]/s

    # edge = model prob - market prob
    df["edge_H"]=df["pH"]-df["mH"]
    df["edge_D"]=df["pD"]-df["mD"]
    df["edge_A"]=df["pA"]-df["mA"]

    # odds bucket by min price among H/D/A (crude)
    min_odds=df[["home_odds_dec","draw_odds_dec","away_odds_dec"]].min(axis=1)
    bins=[0,1.8,2.2,3.0,5.0,10.0,999]
    labels=["<=1.8","(1.8,2.2]","(2.2,3.0]","(3.0,5.0]","(5.0,10.0]","10+"]
    df["odds_bucket"]=pd.cut(min_odds, bins=bins, labels=labels, include_lowest=True)

    rows=[]
    for side,edge_col,kelly_col in [("H","edge_H","kelly_H"),("D","edge_D","kelly_D"),("A","edge_A","kelly_A")]:
        grp=df.groupby(["league","odds_bucket"])
        for (lg,bk), g in grp:
            if g.empty: continue
            e=g[edge_col].dropna()
            n=len(e)
            if n==0: continue
            rows.append({
                "league": lg,
                "odds_bucket": str(bk),
                "side": side,
                "n": n,
                "avg_edge": float(e.mean()),
                "pct_positive": float((e>0).mean()*100.0),
                "mean_kelly_if_odds": float(g.get(kelly_col, pd.Series(0,index=g.index)).fillna(0).mean())
            })
    pd.DataFrame(rows).to_csv(OUT,index=False)
    print(f"[OK] wrote {OUT}")

if __name__ == "__main__":
    main()