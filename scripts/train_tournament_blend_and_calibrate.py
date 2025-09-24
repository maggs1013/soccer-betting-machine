#!/usr/bin/env python3
"""
train_tournament_blend_and_calibrate.py
Learn market↔model blend weights & calibrators using **UEFA tournaments only** (UCL/UEL/UECL).

Inputs:
  data/HIST_matches.csv  (must include: date, home_team, away_team, home_goals, away_goals;
                         optional: home_odds_dec, draw_odds_dec, away_odds_dec, league)

Outputs:
  data/model_blend_tournaments.json
  data/calibrator_tournaments.pkl
"""

import os, json, pickle, numpy as np, pandas as pd
from sklearn.isotonic import IsotonicRegression

DATA = "data"
HIST = os.path.join(DATA, "HIST_matches.csv")
BLND_T = os.path.join(DATA, "model_blend_tournaments.json")
CAL_T  = os.path.join(DATA, "calibrator_tournaments.pkl")

UEFA_TOKENS = [
    "champions league","uefa champions","ucl",
    "europa league","uefa europa","uel",
    "conference league","uefa europa conference","uecl"
]

def is_uefa(s):
    if not isinstance(s, str): return False
    s2 = s.lower()
    return any(tok in s2 for tok in UEFA_TOKENS)

def implied(dec):
    try: d=float(dec); return 1.0/d if d>0 else np.nan
    except: return np.nan

def strip_vig(pH,pD,pA):
    s = pH+pD+pA
    if not np.isfinite(s) or s<=0: return (np.nan,np.nan,np.nan)
    return (pH/s,pD/s,pA/s)

def elo_triplet(Rh,Ra,ha=50.0):
    # Slightly lower HA by default for tournaments
    pH_core = 1.0/(1.0+10.0**(-((Rh-Ra+ha)/400.0)))
    pD = 0.18 + 0.10*np.exp(-abs(Rh-Ra)/200.0)
    pH = (1.0-pD)*pH_core; pA = 1.0 - pH - pD
    eps=1e-6
    return (max(eps,min(1-eps,pH)), max(eps,min(1-eps,pD)), max(eps,min(1-eps,pA)))

def fit_iso(p, ybin):
    try:
        iso = IsotonicRegression(out_of_bounds="clip")
        iso.fit(p, ybin)
        return iso
    except Exception:
        return None

def learn_weight(pM, pE, yy, default_w=0.85):
    grid = np.linspace(0.55, 0.95, 9)
    mask = np.isfinite(pM).all(axis=1)
    if mask.sum()==0: return float(default_w)
    PM, PE, Y = pM[mask], pE[mask], yy[mask].astype(int)
    best_w, best_ll = float(default_w), 1e18
    for w in grid:
        B = w*PM + (1.0-w)*PE
        s = B.sum(axis=1, keepdims=True); s[s==0]=1.0; B=B/s
        ll = -np.log(np.take_along_axis(B, Y.reshape(-1,1), axis=1).clip(1e-12)).mean()
        if ll < best_ll:
            best_ll, best_w = ll, float(w)
    return best_w

def main():
    if not os.path.exists(HIST):
        json.dump({"w_market_global":0.85}, open(BLND_T,"w"))
        pickle.dump({"global":{"home":None,"draw":None,"away":None}}, open(CAL_T,"wb"))
        print("[WARN] HIST missing; wrote tournament defaults."); return

    df = pd.read_csv(HIST)
    need = {"date","home_team","away_team","home_goals","away_goals"}
    if not need.issubset(df.columns) or df.empty:
        json.dump({"w_market_global":0.85}, open(BLND_T,"w"))
        pickle.dump({"global":{"home":None,"draw":None,"away":None}}, open(CAL_T,"wb"))
        print("[WARN] HIST lacks required columns; wrote tournament defaults."); return

    if "league" not in df.columns: df["league"]="GLOBAL"
    df["date"]=pd.to_datetime(df["date"], errors="coerce")
    df=df.dropna(subset=["date"]).sort_values("date")

    # UEFA-only
    df = df[df["league"].astype(str).apply(is_uefa)]
    if df.empty:
        json.dump({"w_market_global":0.85}, open(BLND_T,"w"))
        pickle.dump({"global":{"home":None,"draw":None,"away":None}}, open(CAL_T,"wb"))
        print("[WARN] No UEFA rows; wrote tournament defaults."); return

    y = np.where(df["home_goals"]>df["away_goals"],0,
        np.where(df["home_goals"]==df["away_goals"],1,2)).astype(int)

    have_odds = {"home_odds_dec","draw_odds_dec","away_odds_dec"}.issubset(df.columns)
    if have_odds:
        mH=df["home_odds_dec"].map(implied); mD=df["draw_odds_dec"].map(implied); mA=df["away_odds_dec"].map(implied)
        m = np.array([strip_vig(h,d,a) for h,d,a in zip(mH,mD,mA)], dtype=float)
    else:
        m = np.full((len(df),3), np.nan, dtype=float)

    R={}; e=[]
    for r in df.itertuples(index=False):
        h,a=r.home_team, r.away_team
        R.setdefault(h,1500.0); R.setdefault(a,1500.0)
        e.append(elo_triplet(R[h],R[a],ha=50.0))
        Eh=1.0/(1.0+10.0**(-((R[h]-R[a]+50.0)/400.0)))
        if not (pd.isna(r.home_goals) or pd.isna(r.away_goals)):
            score = 1.0 if r.home_goals>r.away_goals else (0.5 if r.home_goals==r.away_goals else 0.0)
            K=20.0
            R[h]=R[h]+K*(score-Eh); R[a]=R[a]+K*((1.0-score)-(1.0-Eh))
    e=np.array(e, dtype=float)

    w_global = learn_weight(m, e, y, default_w=0.85)
    json.dump({"w_market_global": float(w_global)}, open(BLND_T,"w"))
    print(f"[OK] tournament w_market_global={w_global:.2f} → {BLND_T}")

    # Global calibrators for tournaments
    B = np.where(np.isfinite(m).all(axis=1)[:,None], w_global*m + (1.0-w_global)*e, e)
    s = B.sum(axis=1, keepdims=True); s[s==0]=1.0; B=B/s
    isoH = fit_isotonic(B[:,0], (y==0).astype(int))
    isoD = fit_isotonic(B[:,1], (y==1).astype(int))
    isoA = fit_isotonic(B[:,2], (y==2).astype(int))
    pickle.dump({"global":{"home":isoH,"draw":isoD,"away":isoA}}, open(CAL_T,"wb"))
    print(f"[OK] tournament calibrators saved → {CAL_T}")

if __name__ == "__main__":
    main()