# scripts/model_predict.py
# Predict next-7-day matches using learned blend + calibration.
# Works without odds: outputs pH/pD/pA; Kelly columns set to 0 when odds missing.

import os, json, pickle, numpy as np, pandas as pd

DATA="data"
UP  = os.path.join(DATA,"UPCOMING_7D_enriched.csv")
BL  = os.path.join(DATA,"model_blend.json")
CAL = os.path.join(DATA,"calibrator.pkl")
OUT = os.path.join(DATA,"PREDICTIONS_7D.csv")

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
    return max(1e-6,min(1-1e-6,pH)), max(1e-6,min(1-1e-6,pD)), max(1e-6,min(1-1e-6,pA))

def load_json(p, default):
    try:
        with open(p,"r") as f: return json.load(f)
    except: return default

def load_cal(p):
    try:
        with open(p,"rb") as f: return pickle.load(f)
    except: return {"home":None,"draw":None,"away":None}

def main():
    if not os.path.exists(UP):
        pd.DataFrame(columns=["date","home_team","away_team","pH","pD","pA","kelly_H","kelly_D","kelly_A"]).to_csv(OUT,index=False)
        print("[WARN] Upcoming file missing; wrote header-only preds.")
        return

    df = pd.read_csv(UP)
    if df.empty:
        pd.DataFrame(columns=["date","home_team","away_team","pH","pD","pA","kelly_H","kelly_D","kelly_A"]).to_csv(OUT,index=False)
        print("[WARN] Upcoming empty; wrote header-only preds.")
        return

    # Market probs from API odds if present
    if {"home_odds_dec","draw_odds_dec","away_odds_dec"}.issubset(df.columns):
        mH = df["home_odds_dec"].map(implied); mD = df["draw_odds_dec"].map(implied); mA = df["away_odds_dec"].map(implied)
        m = pd.DataFrame([strip_vig(h,d,a) for h,d,a in zip(mH,mD,mA)], columns=["mH","mD","mA"])
    else:
        m = pd.DataFrame({"mH":np.nan,"mD":np.nan,"mA":np.nan}, index=df.index)

    # Model (Elo) probs — use priors if present in enriched file
    elo_h = df.get("elo_home", pd.Series(1500.0, index=df.index)).fillna(1500.0)
    elo_a = df.get("elo_away", pd.Series(1500.0, index=df.index)).fillna(1500.0)
    model = df.apply(lambda r: elo_prob(elo_h.loc[r.name], elo_a.loc[r.name]), axis=1, result_type="expand")
    model.columns = ["eH","eD","eA"]

    # Blend + calibration
    w_market = load_json(BL, {"w_market":0.85})["w_market"]
    cal = load_cal(CAL)

    pH = w_market*m["mH"].fillna(model["eH"]) + (1-w_market)*model["eH"]
    pD = w_market*m["mD"].fillna(model["eD"]) + (1-w_market)*model["eD"]
    pA = w_market*m["mA"].fillna(model["eA"]) + (1-w_market)*model["eA"]
    s = pH+pD+pA; pH/=s; pD/=s; pA/=s

    if cal.get("home") is not None:
        pH = pd.Series(cal["home"].transform(pH), index=pH.index)
    if cal.get("draw") is not None:
        pD = pd.Series(cal["draw"].transform(pD), index=pD.index)
    if cal.get("away") is not None:
        pA = pd.Series(cal["away"].transform(pA), index=pA.index)
    s = pH+pD+pA; pH/=s; pD/=s; pA/=s

    # Kelly stakes = 0 if odds are missing
    def kelly(p, dec, cap=0.10):
        try:
            b=float(dec)-1.0
            if b<=0 or not np.isfinite(p): return 0.0
            q=1-p; k=(b*p - q)/b
            return float(max(0.0, min(k, cap)))
        except: return 0.0

    kH = [kelly(h, o) for h,o in zip(pH, df.get("home_odds_dec", pd.Series(np.nan, index=df.index)))]
    kD = [kelly(d, o) for d,o in zip(pD, df.get("draw_odds_dec", pd.Series(np.nan, index=df.index)))]
    kA = [kelly(a, o) for a,o in zip(pA, df.get("away_odds_dec", pd.Series(np.nan, index=df.index)))]

    out = pd.DataFrame({
        "date": df.get("date"), "home_team": df.get("home_team"), "away_team": df.get("away_team"),
        "pH": pH, "pD": pD, "pA": pA,
        "kelly_H": kH, "kelly_D": kD, "kelly_A": kA
    })
    out.sort_values(["date","pH","pD","pA"], ascending=[True, False, False, False], inplace=True)
    out.to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} rows={len(out)}")

if __name__ == "__main__":
    main()