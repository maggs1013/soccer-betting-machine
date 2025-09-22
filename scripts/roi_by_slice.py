# scripts/roi_by_slice.py
# Walk-forward ROI by week × league × odds bucket using a simple Kelly-capped strategy on HIST.
# Strategy: pick the side with highest positive edge (market vs Elo-blend) per match, bet min(kelly, 0.05).
# Reads: data/HIST_matches.csv, data/model_blend.json
# Writes: data/ROI_BY_SLICE.csv

import os, json, numpy as np, pandas as pd

DATA="data"
HIST=os.path.join(DATA,"HIST_matches.csv")
BLND=os.path.join(DATA,"model_blend.json")
OUT =os.path.join(DATA,"ROI_BY_SLICE.csv")

def implied(d):
    try:
        d=float(d); return 1.0/d if d>0 else np.nan
    except: return np.nan

def strip_vig(h,d,a):
    s=h+d+a
    if not np.isfinite(s) or s<=0: return (np.nan,np.nan,np.nan)
    return (h/s,d/s,a/s)

def elo_prob(Rh,Ra,ha=60.0):
    pH_core=1/(1+10**(-((Rh-Ra+ha)/400)))
    pD=0.18+0.10*np.exp(-abs(Rh-Ra)/200.0)
    pH=(1-pD)*pH_core
    pA=1-pH-pD
    eps=1e-6
    return (max(eps,min(1-eps,pH)), max(eps,min(1-eps,pD)), max(eps,min(1-eps,pA)))

def build_elo(df):
    R={}
    for r in df.itertuples(index=False):
        h,a=r.home_team,r.away_team
        R.setdefault(h,1500.0); R.setdefault(a,1500.0)
        Eh=1/(1+10**(-((R[h]-R[a]+60)/400)))
        if pd.isna(r.home_goals) or pd.isna(r.away_goals): continue
        score=1.0 if r.home_goals>r.away_goals else (0.5 if r.home_goals==r.away_goals else 0.0)
        K=20
        R[h]=R[h]+K*(score-Eh); R[a]=R[a]+K*((1.0-score)-(1.0-Eh))
    return R

def kelly(p, odds, cap=0.05):
    try:
        b=float(odds)-1.0
        if b<=0 or not np.isfinite(p): return 0.0
        q=1.0-p; k=(b*p - q)/b
        return float(max(0.0, min(k, cap)))
    except: return 0.0

def main():
    if not (os.path.exists(HIST) and os.path.exists(BLND)):
        pd.DataFrame(columns=["week","league","odds_bucket","n_bets","roi","hit_rate","avg_kelly","turnover"]).to_csv(OUT,index=False)
        print(f"[WARN] Missing inputs; wrote empty {OUT}"); return

    df=pd.read_csv(HIST)
    if "league" not in df.columns: df["league"]="GLOBAL"
    df["date"]=pd.to_datetime(df["date"], errors="coerce")
    df=df.dropna(subset=["date"]).sort_values("date")
    df["week"]=df["date"].dt.isocalendar().week.astype(int)

    mb=json.load(open(BLND,"r"))
    w_global=float(mb.get("w_market_global",0.85))
    w_leagues=mb.get("w_market_leagues",{}) or {}

    # market probs
    if {"home_odds_dec","draw_odds_dec","away_odds_dec"}.issubset(df.columns):
        mH=df["home_odds_dec"].map(implied); mD=df["draw_odds_dec"].map(implied); mA=df["away_odds_dec"].map(implied)
        m=np.array([strip_vig(h,d,a) for h,d,a in zip(mH,mD,mA)],dtype=float)
    else:
        pd.DataFrame(columns=["week","league","odds_bucket","n_bets","roi","hit_rate","avg_kelly","turnover"]).to_csv(OUT,index=False)
        print(f"[WARN] HIST lacks odds; wrote empty {OUT}"); return

    # elo probs
    R={}; e=[]
    for r in df.itertuples(index=False):
        h,a=r.home_team,r.away_team
        R.setdefault(h,1500.0); R.setdefault(a,1500.0)
        e.append(elo_prob(R[h],R[a]))
        Eh=1/(1+10**(-((R[h]-R[a]+60)/400)))
        if pd.isna(r.home_goals) or pd.isna(r.away_goals): continue
        score=1.0 if r.home_goals>r.away_goals else (0.5 if r.home_goals==r.away_goals else 0.0)
        K=20
        R[h]=R[h]+K*(score-Eh); R[a]=R[a]+K*((1.0-score)-(1.0-Eh))
    e=np.array(e,dtype=float)

    y=np.where(df["home_goals"]>df["away_goals"],0, np.where(df["home_goals"]==df["away_goals"],1,2))

    rows=[]
    bins=[0,1.8,2.2,3.0,5.0,10.0,999]
    labels=["<=1.8","(1.8,2.2]","(2.2,3.0]","(3.0,5.0]","(5.0,10.0]","10+"]

    for wk, dW in df.groupby("week"):
        # per-league blend
        for lg, dWL in dW.groupby("league"):
            idx=dWL.index.values
            w=w_leagues.get(lg,w_global)
            blend = w*m[idx] + (1-w)*e[idx]
            s=blend.sum(axis=1,keepdims=True); s[s==0]=1.0; blend=blend/s
            # choose best positive edge side vs market prob
            impl=m[idx]; # already stripped
            edges = blend - impl
            sides = np.argmax(edges, axis=1)
            # odds bucket by chosen side odds
            chosen_odds=[]
            stakes=[]; pnl=[]
            hits=[]
            for i,row in enumerate(dWL.itertuples(index=False)):
                side=sides[i]
                if side==0:
                    p=blend[i,0]; imp=impl[i,0]; odds=row.home_odds_dec
                    res = 1 if row.home_goals>row.away_goals else 0
                elif side==1:
                    p=blend[i,1]; imp=impl[i,1]; odds=row.draw_odds_dec
                    res = 1 if row.home_goals==row.away_goals else 0
                else:
                    p=blend[i,2]; imp=impl[i,2]; odds=row.away_odds_dec
                    res = 1 if row.home_goals<row.away_goals else 0
                edge = p - imp
                if not np.isfinite(odds) or not np.isfinite(edge) or edge<=0:
                    stakes.append(0.0); pnl.append(0.0); hits.append(0); chosen_odds.append(np.nan); continue
                k = kelly(p, odds, cap=0.05)
                stakes.append(k)
                chosen_odds.append(odds)
                # outcome P/L with fractional Kelly stake
                win = (odds-1.0)*k if res==1 else -k
                pnl.append(win); hits.append(res)

            dWL2 = dWL.copy()
            dWL2["side"]=sides
            dWL2["stake"]=stakes
            dWL2["pnl"]=pnl
            dWL2["hit"]=hits
            dWL2["chosen_odds"]=chosen_odds
            dWL2["odds_bucket"]=pd.cut(dWL2["chosen_odds"], bins=bins, labels=labels, include_lowest=True)

            for bk, g in dWL2.groupby("odds_bucket"):
                n=int((g["stake"]>0).sum())
                if n==0:
                    rows.append({"week":int(wk),"league":lg,"odds_bucket":str(bk),"n_bets":0,"roi":0.0,"hit_rate":0.0,"avg_kelly":0.0,"turnover":0.0})
                    continue
                turnover=float(g["stake"].sum())
                roi=float(g["pnl"].sum())/turnover if turnover>0 else 0.0
                hit_rate=float(g[g["stake"]>0]["hit"].mean()*100.0) if n>0 else 0.0
                avg_kelly=float(g["stake"].mean())
                rows.append({"week":int(wk),"league":lg,"odds_bucket":str(bk),
                             "n_bets":n,"roi":roi,"hit_rate":hit_rate,"avg_kelly":avg_kelly,"turnover":turnover})

    pd.DataFrame(rows).to_csv(OUT,index=False)
    print(f"[OK] wrote {OUT}")

if __name__ == "__main__":
    main()