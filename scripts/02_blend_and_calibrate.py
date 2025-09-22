# scripts/02_blend_and_calibrate.py
# Learn market↔model blend weight (w_market) and fit isotonic calibration, no CLI args.
# Reads:  data/HIST_matches.csv
# Writes: data/model_blend.json, data/calibrator.pkl

import os, json, pickle, numpy as np, pandas as pd
from sklearn.isotonic import IsotonicRegression
from math import log

DATA = "data"
HIST = os.path.join(DATA, "HIST_matches.csv")
BLND = os.path.join(DATA, "model_blend.json")
CAL  = os.path.join(DATA, "calibrator.pkl")

def implied(d):
    try: d=float(d); return 1.0/d if d>0 else np.nan
    except: return np.nan

def strip_vig(h,d,a):
    s = h+d+a
    if not np.isfinite(s) or s<=0: return (np.nan,np.nan,np.nan)
    return (h/s, d/s, a/s)

def elo_prob(Rh, Ra, ha=60.0):
    pH_core = 1/(1+10**(-((Rh - Ra + ha)/400)))
    pD = 0.18 + 0.10*np.exp(-abs(Rh - Ra)/200.0)
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

def logloss(y,p):
    eps=1e-12
    return -log(max(eps,p[y]))

def main():
    # Fallback defaults
    default_w = 0.85
    default_cal = {"home":None,"draw":None,"away":None}

    if not os.path.exists(HIST):
        json.dump({"w_market":default_w}, open(BLND,"w"))
        pickle.dump(default_cal, open(CAL,"wb"))
        print("[WARN] HIST missing; wrote default blend/calibrator."); return

    df = pd.read_csv(HIST)
    need = {"date","home_team","away_team","home_goals","away_goals","home_odds_dec","draw_odds_dec","away_odds_dec"}
    if not need.issubset(df.columns) or df.empty:
        json.dump({"w_market":default_w}, open(BLND,"w"))
        pickle.dump(default_cal, open(CAL,"wb"))
        print("[WARN] HIST lacks required columns; wrote default blend/calibrator."); return

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")

    # Market probs
    mH = df["home_odds_dec"].map(implied)
    mD = df["draw_odds_dec"].map(implied)
    mA = df["away_odds_dec"].map(implied)
    m = np.array([strip_vig(h,d,a) for h,d,a in zip(mH,mD,mA)], dtype=float)

    # Model (Elo) probs, fitted cumulatively
    R={}
    e=[]
    for r in df.itertuples(index=False):
        h,a=r.home_team,r.away_team
        R.setdefault(h,1500.0); R.setdefault(a,1500.0)
        e.append(elo_prob(R[h], R[a]))
        # update Elo on result
        Eh=1/(1+10**(-((R[h]-R[a]+60)/400)))
        if pd.isna(r.home_goals) or pd.isna(r.away_goals): continue
        score=1.0 if r.home_goals>r.away_goals else (0.5 if r.home_goals==r.away_goals else 0.0)
        K=20
        R[h]=R[h]+K*(score-Eh); R[a]=R[a]+K*((1.0-score)-(1.0-Eh))
    e = np.array(e, dtype=float)

    # labels
    y = np.where(df["home_goals"]>df["away_goals"],0,np.where(df["home_goals"]==df["away_goals"],1,2))

    # Learn weight with simple grid search
    weights = np.linspace(0.55, 0.95, 9)  # market-dominant range
    best_w, best_ll = default_w, 1e18
    for w in weights:
        blend = w*m + (1-w)*e
        blend = blend / blend.sum(axis=1, keepdims=True)
        ll = np.mean([logloss(y_i, blend[i]) for i,y_i in enumerate(y)])
        if ll < best_ll:
            best_ll = ll; best_w = w

    json.dump({"w_market": float(best_w)}, open(BLND,"w"))
    print(f"[OK] learned w_market={best_w:.2f} (avg logloss={best_ll:.4f}) → {BLND}")

    # Fit isotonic calibrators one-vs-rest on blended probs
    blend = best_w*m + (1-best_w)*e
    blend = blend / blend.sum(axis=1, keepdims=True)
    pH, pD, pA = blend[:,0], blend[:,1], blend[:,2]

    iso_H = IsotonicRegression(out_of_bounds="clip").fit(pH, (y==0).astype(int))
    iso_D = IsotonicRegression(out_of_bounds="clip").fit(pD, (y==1).astype(int))
    iso_A = IsotonicRegression(out_of_bounds="clip").fit(pA, (y==2).astype(int))
    pickle.dump({"home":iso_H,"draw":iso_D,"away":iso_A}, open(CAL,"wb"))
    print(f"[OK] saved isotonic calibrator → {CAL}")

if __name__ == "__main__":
    main()