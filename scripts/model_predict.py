# scripts/model_predict.py
# Predict next-7-day matches:
#   - Feature model probs (fH/fD/fA) if available
#   - Market probs from Odds API if present
#   - Elo fallback model probs if needed
#   - Blend with learned w_market (applied to Market vs Model_COMBO)
#   - Isotonic calibration on blended probs
#   - Kelly > 0 only if odds exist
#
# Writes: data/PREDICTIONS_7D.csv

import os, json, pickle, numpy as np, pandas as pd

DATA="data"
UP   = os.path.join(DATA,"UPCOMING_7D_enriched.csv")
FP   = os.path.join(DATA,"feature_proba_upcoming.csv")   # new
BL   = os.path.join(DATA,"model_blend.json")
CAL  = os.path.join(DATA,"calibrator.pkl")
OUT  = os.path.join(DATA,"PREDICTIONS_7D.csv")

def implied(d):
    try: d=float(d); return 1.0/d if d>0 else np.nan
    except: return np.nan

def strip_vig(ph,pd,pa):
    s = ph+pd+pa
    if not np.isfinite(s) or s<=0: return (np.nan,np.nan,np.nan)
    return (ph/s,pd/s,pa/s)

def elo_prob(elo_h, elo_a, ha=60.0):
    pH_core = 1/(1+10**(-((elo_h - elo_a + ha)/400)))
    pD = 0.18 + 0.10*np.exp(-abs(elo_h - elo_a)/200.0)
    pH = (1-pD)*pH_core
    pA = 1 - pH - pD
    eps=1e-6
    return (max(eps,min(1-eps,pH)), max(eps,min(1-eps,pD)), max(eps,min(1-eps,pA)))

def load_json(p, default):
    try:
        with open(p,"r") as f: return json.load(f)
    except: return default

def load_cal(p):
    try:
        with open(p,"rb") as f: return pickle.load(f)
    except: return {"home":None,"draw":None,"away":None}

def main():
    # Base fixtures
    if not os.path.exists(UP):
        pd.DataFrame(columns=["date","home_team","away_team","pH","pD","pA","kelly_H","kelly_D","kelly_A"]).to_csv(OUT,index=False)
        print("[WARN] Upcoming missing; wrote header-only preds."); return
    df = pd.read_csv(UP)
    if df.empty:
        pd.DataFrame(columns=["date","home_team","away_team","pH","pD","pA","kelly_H","kelly_D","kelly_A"]).to_csv(OUT,index=False)
        print("[WARN] Upcoming empty; wrote header-only preds."); return

    # Market probs
    if {"home_odds_dec","draw_odds_dec","away_odds_dec"}.issubset(df.columns):
        mH = df["home_odds_dec"].map(implied); mD = df["draw_odds_dec"].map(implied); mA = df["away_odds_dec"].map(implied)
        m = np.array([strip_vig(h,d,a) for h,d,a in zip(mH,mD,mA)], dtype=float)
    else:
        m = np.array([(np.nan,np.nan,np.nan)]*len(df), dtype=float)

    # Feature model probs (if present)
    fproba = pd.DataFrame(columns=["date","home_team","away_team","fH","fD","fA"])
    if os.path.exists(FP):
        try: fproba = pd.read_csv(FP)
        except Exception: fproba = pd.DataFrame(columns=["date","home_team","away_team","fH","fD","fA"])
    df2 = df.merge(fproba, on=["date","home_team","away_team"], how="left")

    # Elo fallback probs
    elo_h = df2.get("elo_home", pd.Series(1500.0, index=df2.index)).fillna(1500.0)
    elo_a = df2.get("elo_away", pd.Series(1500.0, index=df2.index)).fillna(1500.0)
    e = np.array([elo_prob(elo_h.loc[i], elo_a.loc[i]) for i in df2.index], dtype=float)

    # Model combo = feature if available else Elo
    f_ok = df2[["fH","fD","fA"]].notna().all(axis=1).values
    model_combo = np.where(f_ok[:,None], df2[["fH","fD","fA"]].values, e)

    # Blend (market vs model_combo) with learned w_market
    w_market = load_json(BL, {"w_market":0.85})["w_market"]
    blend = np.where(np.isnan(m).any(axis=1)[:,None],   # if market missing -> use model only
                     model_combo,
                     w_market*m + (1-w_market)*model_combo)
    # normalize
    blend = blend / blend.sum(axis=1, keepdims=True)

    # Calibration
    cal = load_cal(CAL)
    pH, pD, pA = blend[:,0], blend[:,1], blend[:,2]
    if cal.get("home") is not None: pH = cal["home"].transform(pH)
    if cal.get("draw") is not None: pD = cal["draw"].transform(pD)
    if cal.get("away") is not None: pA = cal["away"].transform(pA)
    s = pH+pD+pA; pH/=s; pD/=s; pA/=s

    # Kelly only if odds present
    def kelly(p, dec, cap=0.10):
        try:
            b=float(dec)-1.0
            if b<=0 or not np.isfinite(p): return 0.0
            q=1-p; k=(b*p - q)/b
            return float(max(0.0, min(k, cap)))
        except: return 0.0

    kH = [kelly(h, o) for h,o in zip(pH, df2.get("home_odds_dec", pd.Series(np.nan, index=df2.index)))]
    kD = [kelly(d, o) for d,o in zip(pD, df2.get("draw_odds_dec", pd.Series(np.nan, index=df2.index)))]
    kA = [kelly(a, o) for a,o in zip(pA, df2.get("away_odds_dec", pd.Series(np.nan, index=df2.index)))]

    out = pd.DataFrame({
        "date": df2.get("date"), "home_team": df2.get("home_team"), "away_team": df2.get("away_team"),
        "pH": pH, "pD": pD, "pA": pA,
        "kelly_H": kH, "kelly_D": kD, "kelly_A": kA
    })
    out.sort_values(["date","pH","pD","pA"], ascending=[True, False, False, False], inplace=True)
    out.to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} rows={len(out)}")

if __name__ == "__main__":
    main()