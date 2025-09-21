# scripts/train_calibrated_blend.py
# Learn a scalar blend weight between market and model probabilities by minimizing log-loss,
# then fit isotonic calibration on the blended probs. Saves:
#   data/model_blend.json  -> {"w_market": float}
#   data/calibrator.pkl    -> scikit-learn IsotonicRegression for home/draw/away (one-vs-rest)

import os, json, pickle, numpy as np, pandas as pd
from sklearn.isotonic import IsotonicRegression
from math import log

DATA = "data"
HIST = os.path.join(DATA, "HIST_matches.csv")
FORM = os.path.join(DATA, "team_form_features.csv")
BLEND_PATH = os.path.join(DATA, "model_blend.json")
CAL_PATH   = os.path.join(DATA, "calibrator.pkl")

def safe_read(p): return pd.read_csv(p) if os.path.exists(p) else pd.DataFrame()

def implied(dec):
    try:
        d=float(dec); return 1.0/d if d>0 else np.nan
    except: return np.nan

def strip_vig(ph,pd,pa):
    s = ph + pd + pa
    if not np.isfinite(s) or s<=0: return (np.nan,np.nan,np.nan)
    return (ph/s, pd/s, pa/s)

def elo_prob(Rh,Ra,ha=60.0):
    # Rh, Ra are Elo ratings; convert to home win prob base; add draw via heuristic
    p_home_core = 1/(1+10**(-((Rh - Ra + ha)/400)))
    # draw prob increases when teams close
    diff = Rh - Ra
    p_draw = 0.18 + 0.10*np.exp(-abs(diff)/200.0)
    p_home = (1-p_draw)*p_home_core
    p_away = 1 - p_home - p_draw
    return max(1e-6,min(1-1e-6,p_home)), max(1e-6,min(1-1e-6,p_draw)), max(1e-6,min(1-1e-6,p_away))

def build_elo(hist):
    teams = pd.unique(pd.concat([hist["home_team"],hist["away_team"]]).dropna())
    R = {t:1500.0 for t in teams}
    rows=[]
    for r in hist.sort_values("date").itertuples(index=False):
        h, a = r.home_team, r.away_team
        if pd.isna(h) or pd.isna(a): continue
        Rh, Ra = R.get(h,1500.0), R.get(a,1500.0)
        Eh = 1/(1+10**(-((Rh - Ra + 60)/400)))
        hg, ag = r.home_goals, r.away_goals
        if pd.isna(hg) or pd.isna(ag): continue
        score = 1.0 if hg>ag else (0.5 if hg==ag else 0.0)
        K=20
        R[h] = Rh + K*(score - Eh)
        R[a] = Ra + K*((1.0-score) - (1.0-Eh))
        rows.append((r.date,h,a,R[h],R[a]))
    elo_df = pd.DataFrame(rows, columns=["date","home_team","away_team","elo_home","elo_away"])
    return R, elo_df

def logloss(y, p):
    # y one-hot index 0(home),1(draw),2(away); p 3-vector
    eps=1e-12
    return -sum(np.log(max(eps, p[i])) for i in range(3) if y==i)

def main():
    hist = safe_read(HIST)
    if hist.empty or not {"date","home_team","away_team","home_goals","away_goals",
                          "home_odds_dec","draw_odds_dec","away_odds_dec"}.issubset(hist.columns):
        # write defaults
        with open(BLEND_PATH,"w") as f: json.dump({"w_market":0.70}, f)
        with open(CAL_PATH,"wb") as f: pickle.dump({"home":None,"draw":None,"away":None}, f)
        print("[WARN] HIST lacks required columns; wrote default blend/calibrator.")
        return

    hist["date"] = pd.to_datetime(hist["date"], errors="coerce")
    # Market probs
    mH = hist["home_odds_dec"].map(implied); mD = hist["draw_odds_dec"].map(implied); mA = hist["away_odds_dec"].map(implied)
    m = pd.DataFrame([strip_vig(mH,iD,mA) for mH,iD,mA in zip(mH,mD,mA)], columns=["mH","mD","mA"])

    # Model probs via Elo
    _, elo_df = build_elo(hist)
    df = hist.merge(elo_df[["date","home_team","away_team","elo_home","elo_away"]],
                    on=["date","home_team","away_team"], how="left")
    p_model = df.apply(lambda r: elo_prob(r["elo_home"] if pd.notna(r["elo_home"]) else 1500.0,
                                          r["elo_away"] if pd.notna(r["elo_away"]) else 1500.0), axis=1, result_type="expand")
    p_model.columns = ["eH","eD","eA"]

    X = pd.concat([df[["date","home_team","away_team","home_goals","away_goals"]], m, p_model], axis=1).dropna(subset=["mH","mD","mA","eH","eD","eA"])
    if X.empty:
        with open(BLEND_PATH,"w") as f: json.dump({"w_market":0.70}, f)
        with open(CAL_PATH,"wb") as f: pickle.dump({"home":None,"draw":None,"away":None}, f)
        print("[WARN] No samples with both market and model; default blend saved.")
        return

    # True labels
    y = []
    for hg,ag in zip(X["home_goals"], X["away_goals"]):
        y.append(0 if hg>ag else (1 if hg==ag else 2))
    y = np.array(y)

    # Learn scalar weight by grid search
    weights = np.linspace(0.5, 0.95, 10)  # market dominance range
    best_w, best_ll = 0.70, 1e18
    for w in weights:
        pH = w*X["mH"] + (1-w)*X["eH"]
        pD = w*X["mD"] + (1-w)*X["eD"]
        pA = w*X["mA"] + (1-w)*X["eA"]
        # normalize safety
        s = pH+pD+pA; pH/=s; pD/=s; pA/=s
        ll = np.mean([logloss(y_i, (h,d,a)) for y_i,h,d,a in zip(y,pH,pD,pA)])
        if ll < best_ll:
            best_ll, best_w = ll, w

    with open(BLEND_PATH,"w") as f:
        json.dump({"w_market": float(best_w)}, f)
    print(f"[OK] learned blend weight w_market={best_w:.2f} (mean logloss={best_ll:.4f})")

    # Isotonic calibration one-vs-rest on blended probs
    pH = best_w*X["mH"] + (1-best_w)*X["eH"]
    pD = best_w*X["mD"] + (1-best_w)*X["eD"]
    pA = best_w*X["mA"] + (1-best_w)*X["eA"]
    s = pH+pD+pA; pH/=s; pD/=s; pA/=s

    iso_H = IsotonicRegression(out_of_bounds="clip").fit(pH, (y==0).astype(int))
    iso_D = IsotonicRegression(out_of_bounds="clip").fit(pD, (y==1).astype(int))
    iso_A = IsotonicRegression(out_of_bounds="clip").fit(pA, (y==2).astype(int))
    with open(CAL_PATH,"wb") as f:
        pickle.dump({"home":iso_H,"draw":iso_D,"away":iso_A}, f)
    print("[OK] saved isotonic calibrator.")

if __name__ == "__main__":
    main()