# scripts/calibration_report.py
# Reliability by bins (pred vs actual) and Expected Calibration Error for the "blend" model on HIST.
# Outputs: data/CALIBRATION_TABLE.csv (per bin), data/CALIBRATION_SUMMARY.csv (ECE).

import os, json, numpy as np, pandas as pd

DATA = "data"
HIST = os.path.join(DATA, "HIST_matches.csv")
BLEND = os.path.join(DATA, "model_blend.json")
OUT_TAB = os.path.join(DATA, "CALIBRATION_TABLE.csv")
OUT_SUM = os.path.join(DATA, "CALIBRATION_SUMMARY.csv")

def implied(dec):
    try: d=float(dec); return 1.0/d if d>0 else np.nan
    except: return np.nan

def strip_vig(h,d,a):
    s=h+d+a
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
        Rh,Ra=R[h],R[a]
        Eh=1/(1+10**(-((Rh-Ra+60)/400)))
        if pd.isna(r.home_goals) or pd.isna(r.away_goals): continue
        score=1.0 if r.home_goals>r.away_goals else (0.5 if r.home_goals==r.away_goals else 0.0)
        K=20
        R[h]=Rh+K*(score-Eh)
        R[a]=Ra+K*((1.0-score)-(1.0-Eh))
    return R

def main():
    if not os.path.exists(HIST):
        pd.DataFrame().to_csv(OUT_TAB,index=False); pd.DataFrame().to_csv(OUT_SUM,index=False)
        print("[WARN] HIST missing; wrote empty calibration files."); return
    df=pd.read_csv(HIST)
    need={"date","home_team","away_team","home_goals","away_goals","home_odds_dec","draw_odds_dec","away_odds_dec"}
    if not need.issubset(df.columns):
        pd.DataFrame().to_csv(OUT_TAB,index=False); pd.DataFrame().to_csv(OUT_SUM,index=False)
        print("[WARN] HIST lacks required columns; wrote empty calibration files."); return
    df["date"]=pd.to_datetime(df["date"], errors="coerce")
    df.sort_values("date", inplace=True)

    # market probs
    mH = df["home_odds_dec"].map(implied); mD = df["draw_odds_dec"].map(implied); mA = df["away_odds_dec"].map(implied)
    m = np.array([strip_vig(h,d,a) for h,d,a in zip(mH,mD,mA)], dtype=float)

    # model probs via Elo
    R={}
    e=[]
    for r in df.itertuples(index=False):
        h,a=r.home_team,r.away_team
        R.setdefault(h,1500.0); R.setdefault(a,1500.0)
        Rh,Ra=R[h],R[a]
        e.append(elo_prob(Rh,Ra))
        # update
        Eh=1/(1+10**(-((Rh-Ra+60)/400)))
        if pd.isna(r.home_goals) or pd.isna(r.away_goals): continue
        score=1.0 if r.home_goals>r.away_goals else (0.5 if r.home_goals==r.away_goals else 0.0)
        K=20
        R[h]=Rh+K*(score-Eh)
        R[a]=Ra+K*((1.0-score)-(1.0-Eh))
    e=np.array(e,dtype=float)

    try:
        w=json.load(open(BLEND,"r")).get("w_market",0.85)
    except:
        w=0.85
    blend=w*m + (1-w)*e
    blend = blend/(blend.sum(axis=1, keepdims=True))

    # Labels
    y = np.where(df["home_goals"]>df["away_goals"],0,np.where(df["home_goals"]==df["away_goals"],1,2))

    # Binning (home prob bins for illustration; repeat for draw/away if desired)
    pH = blend[:,0]
    bins = np.linspace(0,1,11)  # 10 bins
    idx = np.digitize(pH, bins, right=True)-1
    idx[idx<0]=0; idx[idx>8]=9

    rows=[]
    for b in range(10):
        mask = idx==b
        n = int(mask.sum())
        if n==0:
            rows.append({"bin":b,"p_mid": (bins[b]+bins[b+1])/2, "n":0, "avg_pred":np.nan, "emp_home":np.nan})
            continue
        avg_pred=float(pH[mask].mean())
        emp=float((y[mask]==0).mean())
        rows.append({"bin":b,"p_mid": (bins[b]+bins[b+1])/2, "n":n, "avg_pred":avg_pred, "emp_home":emp})
    tab=pd.DataFrame(rows)
    # ECE
    ece = float( ( (tab["n"] * (tab["avg_pred"]-tab["emp_home"]).abs()).sum() ) / max(1, tab["n"].sum()) )
    tab.to_csv(OUT_TAB,index=False)
    pd.DataFrame([{"metric":"ECE_home","value":ece}]).to_csv(OUT_SUM,index=False)
    print(f"[OK] wrote {OUT_TAB} (bins={len(tab)}) and {OUT_SUM} (ECE={ece:.4f})")

if __name__ == "__main__":
    main()