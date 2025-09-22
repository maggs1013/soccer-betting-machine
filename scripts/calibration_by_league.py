# scripts/calibration_by_league.py
# Export per-league reliability bins for home win probability and ECE.
# Recomputes blend per league from HIST + model_blend.json (no feature model needed).
# Reads: data/HIST_matches.csv, data/model_blend.json
# Writes: data/CALIBRATION_BY_LEAGUE.csv

import os, json, numpy as np, pandas as pd

DATA = "data"
HIST = os.path.join(DATA, "HIST_matches.csv")
BLND = os.path.join(DATA, "model_blend.json")
OUT  = os.path.join(DATA, "CALIBRATION_BY_LEAGUE.csv")

def implied(x):
    try:
        x = float(x); return 1.0/x if x>0 else np.nan
    except: return np.nan

def strip_vig(h,d,a):
    s = h+d+a
    if not np.isfinite(s) or s<=0: return (np.nan,np.nan,np.nan)
    return (h/s,d/s,a/s)

def elo_prob(Rh,Ra,ha=60.0):
    pH_core = 1/(1+10**(-((Rh-Ra+ha)/400)))
    pD = 0.18 + 0.10*np.exp(-abs(Rh-Ra)/200.0)
    pH = (1-pD)*pH_core
    pA = 1 - pH - pD
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
        R[h]=R[h]+K*(score-Eh)
        R[a]=R[a]+K*((1.0-score)-(1.0-Eh))
    return R

def main():
    if not os.path.exists(HIST) or not os.path.exists(BLND):
        pd.DataFrame(columns=["league","bin","p_mid","n","avg_pred_home","emp_home","ece_weighted"]).to_csv(OUT,index=False)
        print(f"[WARN] Missing inputs; wrote empty {OUT}")
        return

    df = pd.read_csv(HIST)
    if df.empty:
        pd.DataFrame(columns=["league","bin","p_mid","n","avg_pred_home","emp_home","ece_weighted"]).to_csv(OUT,index=False)
        print(f"[WARN] HIST empty; wrote empty {OUT}")
        return

    if "league" not in df.columns: df["league"]="GLOBAL"
    df["date"]=pd.to_datetime(df["date"],errors="coerce")
    df=df.dropna(subset=["date"]).sort_values("date")
    mb=json.load(open(BLND,"r"))
    w_global=float(mb.get("w_market_global",0.85))
    w_leagues=mb.get("w_market_leagues",{}) or {}

    # market probs
    if {"home_odds_dec","draw_odds_dec","away_odds_dec"}.issubset(df.columns):
        mH=df["home_odds_dec"].map(implied); mD=df["draw_odds_dec"].map(implied); mA=df["away_odds_dec"].map(implied)
        m = np.array([strip_vig(h,d,a) for h,d,a in zip(mH,mD,mA)],dtype=float)
    else:
        m = np.array([(np.nan,np.nan,np.nan)]*len(df),dtype=float)

    # elo probs
    R={}
    e=[]
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

    y = np.where(df["home_goals"]>df["away_goals"],0,np.where(df["home_goals"]==df["away_goals"],1,2))

    rows=[]
    bins=np.linspace(0,1,11)
    for lg, dL in df.groupby("league"):
        idx=dL.index.values
        w=w_leagues.get(lg,w_global)
        b = np.where(np.isnan(m[idx]).any(axis=1)[:,None], e[idx], w*m[idx] + (1-w)*e[idx])
        s=b.sum(axis=1,keepdims=True); s[s==0]=1.0; b=b/s
        pH=b[:,0]; yH=(y[idx]==0).astype(int)
        # bin
        bin_idx=np.digitize(pH, bins, right=True)-1
        bin_idx[bin_idx<0]=0; bin_idx[bin_idx>8]=9
        total=int(len(pH)); ece_num=0
        for bi in range(10):
            mask=bin_idx==bi; n=int(mask.sum())
            p_mid=(bins[bi]+bins[bi+1])/2
            if n==0:
                rows.append({"league":lg,"bin":bi,"p_mid":p_mid,"n":0,"avg_pred_home":np.nan,"emp_home":np.nan,"ece_weighted":0.0})
                continue
            avg_pred=float(np.mean(pH[mask]))
            emp=float(np.mean(yH[mask]))
            ece_num += n*abs(avg_pred-emp)
            rows.append({"league":lg,"bin":bi,"p_mid":p_mid,"n":n,"avg_pred_home":avg_pred,"emp_home":emp,"ece_weighted":None})
        ece = ece_num/total if total>0 else 0.0
        # write ece on bin -1 row
        rows.append({"league":lg,"bin":"ECE","p_mid":None,"n":total,"avg_pred_home":None,"emp_home":None,"ece_weighted":ece})

    pd.DataFrame(rows).to_csv(OUT,index=False)
    print(f"[OK] wrote {OUT}")

if __name__ == "__main__":
    main()