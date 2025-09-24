#!/usr/bin/env python3
# Learn market↔model blend weights (global + per-league) and fit per-league
# isotonic calibrators from DOMESTIC matches only, with time-decay and
# weight guardrails to avoid extreme market dominance.

import os, json, pickle, numpy as np, pandas as pd
from sklearn.isotonic import IsotonicRegression

DATA = "data"
HIST = os.path.join(DATA, "HIST_matches.csv")
BLND = os.path.join(DATA, "model_blend.json")
CAL  = os.path.join(DATA, "calibrator.pkl")

# UEFA detection
UEFA_TOKENS = [
    "champions league","uefa champions","ucl",
    "europa league","uefa europa","uel",
    "conference league","uefa europa conference","uecl","super cup"
]
def is_uefa(s):
    if not isinstance(s,str): return False
    s2 = s.lower(); return any(tok in s2 for tok in UEFA_TOKENS)

# Decay + guardrails
HALF_LIFE_DAYS = 365.0      # slow decay for blend learning
W_MIN, W_MAX     = 0.70, 0.90

def implied(dec):
    try: d=float(dec); return 1.0/d if d>0 else np.nan
    except: return np.nan

def strip_vig(pH,pD,pA):
    s=pH+pD+pA
    if not np.isfinite(s) or s<=0: return (np.nan,np.nan,np.nan)
    return (pH/s,pD/s,pA/s)

def elo_triplet(Rh,Ra,ha=60.0):
    pH_core=1.0/(1.0+10.0**(-((Rh-Ra+ha)/400.0)))
    pD=0.18+0.10*np.exp(-abs(Rh-Ra)/200.0)
    pH=(1.0-pD)*pH_core; pA=1.0-pH-pD
    eps=1e-6
    return (max(eps,min(1-eps,pH)), max(eps,min(1-eps,pD)), max(eps,min(1-eps,pA)))

def fit_iso(p, ybin):
    try:
        iso=IsotonicRegression(out_of_bounds="clip"); iso.fit(p, ybin); return iso
    except Exception: return None

def weighted_logloss(B: np.ndarray, y: np.ndarray, w: np.ndarray) -> float:
    """
    B: (n,3) probabilities (already normalized)
    y: (n,) integer labels {0,1,2}
    w: (n,) nonnegative weights (time-decay)
    Return weighted average of -log p_y.
    Shape-safe: flattens values to 1D before np.average.
    """
    eps = 1e-12
    # pick the predicted prob for the true class
    pick = np.take_along_axis(B, y.reshape(-1,1), axis=1).clip(eps)
    vals = (-np.log(pick)).reshape(-1)           # (n,)
    ww   = np.asarray(w, dtype=float).reshape(-1)
    if ww.size != vals.size:
        # lengths must match – safest fallback: uniform weights
        ww = np.ones_like(vals, dtype=float)
    return float(np.average(vals, weights=ww))

def learn_weight(pM, pE, y, w, default_w=0.85):
    grid = np.linspace(W_MIN, W_MAX, 9)
    mask = np.isfinite(pM).all(axis=1)
    if mask.sum()==0: return float(default_w)
    PM, PE, YY, WW = pM[mask], pE[mask], y[mask], w[mask]
    best_w, best_ll = float(default_w), 1e18
    for ww in grid:
        B = ww*PM + (1.0-ww)*PE
        s = B.sum(axis=1, keepdims=True); s[s==0]=1.0; B=B/s
        ll = weighted_logloss(B, YY, WW)
        if ll < best_ll:
            best_ll, best_w = ll, float(ww)
    return best_w

def main():
    default_w   = 0.85
    default_cal = {"home":None,"draw":None,"away":None}

    if not os.path.exists(HIST):
        json.dump({"w_market_global":default_w,"w_market_leagues":{}}, open(BLND,"w"))
        pickle.dump({"global":default_cal,"per_league":{}}, open(CAL,"wb"))
        print("[WARN] HIST missing; wrote defaults."); return

    df = pd.read_csv(HIST)
    need={"date","home_team","away_team","home_goals","away_goals"}
    if not need.issubset(df.columns) or df.empty:
        json.dump({"w_market_global":default_w,"w_market_leagues":{}}, open(BLND,"w"))
        pickle.dump({"global":default_cal,"per_league":{}}, open(CAL,"wb"))
        print("[WARN] HIST lacks required columns; wrote defaults."); return

    if "league" not in df.columns: df["league"]="GLOBAL"
    df["date"]=pd.to_datetime(df["date"], errors="coerce")
    df=df.dropna(subset=["date"]).sort_values("date")

    # DOMESTIC ONLY
    df = df[~df["league"].astype(str).apply(is_uefa)]
    if df.empty:
        json.dump({"w_market_global":default_w,"w_market_leagues":{}}, open(BLND,"w"))
        pickle.dump({"global":default_cal,"per_league":{}}, open(CAL,"wb"))
        print("[WARN] No domestic rows after UEFA filter; wrote defaults.")
        return

    y = np.where(df["home_goals"]>df["away_goals"],0,
        np.where(df["home_goals"]==df["away_goals"],1,2)).astype(int)

    # time-decay weights
    max_date = df["date"].max()
    days_back = (pd.Timestamp(max_date) - df["date"]).dt.days.clip(lower=0).astype(float)
    w_decay = np.power(0.5, days_back / HALF_LIFE_DAYS)

    have_odds = {"home_odds_dec","draw_odds_dec","away_odds_dec"}.issubset(df.columns)
    if have_odds:
        mH=df["home_odds_dec"].map(implied); mD=df["draw_odds_dec"].map(implied); mA=df["away_odds_dec"].map(implied)
        m = np.array([strip_vig(h,d,a) for h,d,a in zip(mH,mD,mA)], dtype=float)
    else:
        m = np.full((len(df),3), np.nan, dtype=float)

    # Elo (sequential)
    R={}; e=[]
    for r in df.itertuples(index=False):
        h,a=r.home_team, r.away_team
        R.setdefault(h,1500.0); R.setdefault(a,1500.0)
        e.append(elo_triplet(R[h],R[a],ha=60.0))
        Eh=1.0/(1.0+10.0**(-((R[h]-R[a]+60.0)/400.0)))
        if not (pd.isna(r.home_goals) or pd.isna(r.away_goals)):
            score=1.0 if r.home_goals>r.away_goals else (0.5 if r.home_goals==r.away_goals else 0.0)
            K=20.0
            R[h]=R[h]+K*(score-Eh); R[a]=R[a]+K*((1.0-score)-(1.0-Eh))
    e=np.array(e, dtype=float)

    # Global weight (decay + guardrails)
    w_global = learn_weight(m, e, y, w_decay, default_w=default_w)
    w_global = float(np.clip(w_global, W_MIN, W_MAX))

    # Per-league weights
    w_leagues={}
    for lg, grp in df.groupby("league").groups.items():
        idx = np.array(list(grp), dtype=int)
        if len(idx) < 150: continue
        wl = learn_weight(m[idx], e[idx], y[idx], w_decay[idx], default_w=default_w)
        w_leagues[lg] = float(np.clip(wl, W_MIN, W_MAX))

    json.dump({"w_market_global": w_global, "w_market_leagues": w_leagues}, open(BLND,"w"))
    print(f"[OK] learned w_market_global={w_global:.2f} | per-league={len(w_leagues)} → {BLND}")

    # Calibrators
    def blend_probs(w, M, E):
        B = np.where(np.isfinite(M).all(axis=1)[:,None], w*M + (1.0-w)*E, E)
        s=B.sum(axis=1, keepdims=True); s[s==0]=1.0; return B/s

    def fit_triplet(B, yy):
        isoH = fit_iso(B[:,0], (yy==0).astype(int))
        isoD = fit_iso(B[:,1], (yy==1).astype(int))
        isoA = fit_iso(B[:,2], (yy==2).astype(int))
        return {"home":isoH,"draw":isoD,"away":isoA}

    # Global
    B = blend_probs(w_global, m, e)
    cal_global = fit_triplet(B, y)

    # Per-league
    cal_leagues={}
    for lg, grp in df.groupby("league").groups.items():
        idx = np.array(list(grp), dtype=int)
        if len(idx) < 150 or lg not in w_leagues: continue
        wL = w_leagues[lg]
        BL = blend_probs(wL, m[idx], e[idx])
        cal_L = fit_triplet(BL, y[idx])
        if all(cal_L[k] is not None for k in ("home","draw","away")):
            cal_leagues[lg] = cal_L

    pickle.dump({"global": cal_global, "per_league": cal_leagues}, open(CAL,"wb"))
    print(f"[OK] calibrators saved: global + {len(cal_leagues)} leagues → {CAL}")

if __name__ == "__main__":
    main()