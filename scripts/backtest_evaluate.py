# scripts/backtest_evaluate.py
# Rolling, out-of-sample backtest on HIST_matches.csv.
# Computes log-loss & Brier for: Market-only, Model-only (Elo), and Learned Blend (w_market).
# Outputs:
#   data/BACKTEST_SUMMARY.csv
#   data/BACKTEST_BY_WEEK.csv

import os, json, numpy as np, pandas as pd
from math import log

DATA = "data"
HIST = os.path.join(DATA, "HIST_matches.csv")
BLEND = os.path.join(DATA, "model_blend.json")
OUT_SUM = os.path.join(DATA, "BACKTEST_SUMMARY.csv")
OUT_WEEK= os.path.join(DATA, "BACKTEST_BY_WEEK.csv")

def implied(dec):
    try: d = float(dec); return 1.0/d if d>0 else np.nan
    except: return np.nan

def strip_vig(pH,pD,pA):
    s = pH+pD+pA
    if not np.isfinite(s) or s<=0: return (np.nan,np.nan,np.nan)
    return (pH/s, pD/s, pA/s)

def elo_prob(Rh, Ra, ha=60.0):
    pH_core = 1/(1+10**(-((Rh - Ra + ha)/400)))
    pD = 0.18 + 0.10*np.exp(-abs(Rh - Ra)/200.0)
    pH = (1-pD)*pH_core
    pA = 1 - pH - pD
    # clamp
    eps = 1e-6
    return (max(eps,min(1-eps,pH)), max(eps,min(1-eps,pD)), max(eps,min(1-eps,pA)))

def logloss(y,p):
    eps=1e-12
    return -log(max(eps,p[y]))

def brier(y,p):
    # y one-hot index 0/1/2; p tuple/list (3)
    return sum(( (1 if i==y else 0) - p[i])**2 for i in range(3))/3.0

def build_elo(df):
    R = {}
    for r in df.itertuples(index=False):
        h, a = r.home_team, r.away_team
        R.setdefault(h,1500.0); R.setdefault(a,1500.0)
        Rh, Ra = R[h], R[a]
        Eh = 1/(1+10**(-((Rh - Ra + 60)/400)))
        if pd.isna(r.home_goals) or pd.isna(r.away_goals): continue
        score = 1.0 if r.home_goals>r.away_goals else (0.5 if r.home_goals==r.away_goals else 0.0)
        K=20
        R[h] = Rh + K*(score - Eh)
        R[a] = Ra + K*((1.0-score) - (1.0-Eh))
    return R

def main():
    if not os.path.exists(HIST):
        pd.DataFrame(columns=["model","metric","value"]).to_csv(OUT_SUM,index=False)
        pd.DataFrame(columns=["week","model","logloss","brier"]).to_csv(OUT_WEEK,index=False)
        print("[WARN] HIST missing; wrote empty backtest files.")
        return

    df = pd.read_csv(HIST)
    need = {"date","home_team","away_team","home_goals","away_goals","home_odds_dec","draw_odds_dec","away_odds_dec"}
    if not need.issubset(df.columns):
        pd.DataFrame(columns=["model","metric","value"]).to_csv(OUT_SUM,index=False)
        pd.DataFrame(columns=["week","model","logloss","brier"]).to_csv(OUT_WEEK,index=False)
        print("[WARN] HIST lacks required columns; wrote empty backtest files.")
        return

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["week"] = df["date"].dt.isocalendar().week.astype(int)
    df.sort_values("date", inplace=True)

    # precompute labels
    y = np.where(df["home_goals"]>df["away_goals"], 0, np.where(df["home_goals"]==df["away_goals"], 1, 2))

    # market probs
    mH = df["home_odds_dec"].map(implied)
    mD = df["draw_odds_dec"].map(implied)
    mA = df["away_odds_dec"].map(implied)
    m_probs = np.array([strip_vig(h,d,a) for h,d,a in zip(mH,mD,mA)], dtype=float)

    # model probs via Elo (fit Elo cumulatively up to each week)
    weekly = []
    uniq_weeks = sorted(df["week"].dropna().unique())
    for wk in uniq_weeks:
        train = df[df["week"] < wk]
        test  = df[df["week"] == wk]
        if train.empty or test.empty:
            continue
        R = build_elo(train)
        e_probs = []
        for r in test.itertuples(index=False):
            Rh = R.get(r.home_team,1500.0)
            Ra = R.get(r.away_team,1500.0)
            e_probs.append(elo_prob(Rh, Ra))
        e_probs = np.array(e_probs, dtype=float)

        # blended with learned weight (use saved weight if present; else 0.85 default)
        try:
            w = json.load(open(BLEND,"r")).get("w_market",0.85)
        except:
            w = 0.85
        idx = test.index
        m_p = m_probs[idx]
        blend = w*m_p + (1-w)*e_probs
        # normalize
        s = blend.sum(axis=1, keepdims=True); blend = blend/s

        # metrics
        yw = y[idx]
        # Market
        ll_m = np.mean([logloss(y_i, m_p[i]) for i,y_i in enumerate(yw)])
        br_m = np.mean([brier(y_i, m_p[i]) for i,y_i in enumerate(yw)])
        # Elo
        ll_e = np.mean([logloss(y_i, e_probs[i]) for i,y_i in enumerate(yw)])
        br_e = np.mean([brier(y_i, e_probs[i]) for i,y_i in enumerate(yw)])
        # Blend
        ll_b = np.mean([logloss(y_i, blend[i]) for i,y_i in enumerate(yw)])
        br_b = np.mean([brier(y_i, blend[i]) for i,y_i in enumerate(yw)])

        weekly += [
            {"week": int(wk), "model":"market", "logloss": ll_m, "brier": br_m},
            {"week": int(wk), "model":"elo",    "logloss": ll_e, "brier": br_e},
            {"week": int(wk), "model":"blend",  "logloss": ll_b, "brier": br_b},
        ]

    wkdf = pd.DataFrame(weekly)
    if wkdf.empty:
        pd.DataFrame(columns=["model","metric","value"]).to_csv(OUT_SUM,index=False)
        pd.DataFrame(columns=["week","model","logloss","brier"]).to_csv(OUT_WEEK,index=False)
        print("[WARN] Not enough data to compute weekly backtest.")
        return

    # overall averages
    summ = (wkdf.groupby("model", as_index=False)
                  .agg(logloss=("logloss","mean"), brier=("brier","mean")))
    summ = summ.melt("model", var_name="metric", value_name="value")
    summ.to_csv(OUT_SUM, index=False)
    wkdf.to_csv(OUT_WEEK, index=False)
    print(f"[OK] wrote {OUT_SUM} and {OUT_WEEK}")

if __name__ == "__main__":
    main()