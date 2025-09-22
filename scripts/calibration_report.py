#!/usr/bin/env python3
# Reliability by bins (pred vs actual) and Expected Calibration Error for the blended model on HIST.
# Reads: data/HIST_matches.csv, data/model_blend.json (+ optional odds fields)
# Writes:
#   data/CALIBRATION_TABLE.csv, data/CALIBRATION_SUMMARY.csv
#   runs/YYYY-MM-DD/CALIBRATION_TABLE.csv, runs/YYYY-MM-DD/CALIBRATION_SUMMARY.csv
#
# Notes:
# - Robust odds columns: (oddsH/oddsD/oddsA) or (home_odds_dec/draw_odds_dec/away_odds_dec)
# - Robust blend JSON: {"w_market_global", "w_market_leagues"} OR legacy {"w_market"}.

import os, json, numpy as np, pandas as pd
from datetime import datetime

DATA = "data"
HIST = os.path.join(DATA, "HIST_matches.csv")
BLEND = os.path.join(DATA, "model_blend.json")

OUT_TAB_DATA = os.path.join(DATA, "CALIBRATION_TABLE.csv")
OUT_SUM_DATA = os.path.join(DATA, "CALIBRATION_SUMMARY.csv")

RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)
OUT_TAB_RUN = os.path.join(RUN_DIR, "CALIBRATION_TABLE.csv")
OUT_SUM_RUN = os.path.join(RUN_DIR, "CALIBRATION_SUMMARY.csv")

def implied(dec):
    try:
        d = float(dec)
        return 1.0/d if d > 0 else np.nan
    except:
        return np.nan

def strip_vig(h, d, a):
    s = h + d + a
    if not np.isfinite(s) or s <= 0:
        return (np.nan, np.nan, np.nan)
    return (h/s, d/s, a/s)

def elo_triplet(Rh, Ra, ha=60.0):
    pH_core = 1.0 / (1.0 + 10.0 ** (-((Rh - Ra + ha) / 400.0)))
    pD = 0.18 + 0.10 * np.exp(-abs(Rh - Ra) / 200.0)
    pH = (1.0 - pD) * pH_core
    pA = 1.0 - pH - pD
    eps = 1e-6
    return (max(eps,min(1-eps,pH)), max(eps,min(1-eps,pD)), max(eps,min(1-eps,pA)))

def build_elo(df):
    R = {}
    for r in df.itertuples(index=False):
        h, a = r.home_team, r.away_team
        R.setdefault(h, 1500.0); R.setdefault(a, 1500.0)
        Rh, Ra = R[h], R[a]
        Eh = 1.0/(1.0 + 10.0 ** (-((Rh - Ra + 60.0) / 400.0)))
        if pd.isna(r.home_goals) or pd.isna(r.away_goals):
            continue
        score = 1.0 if r.home_goals > r.away_goals else (0.5 if r.home_goals == r.away_goals else 0.0)
        K = 20.0
        R[h] = Rh + K * (score - Eh)
        R[a] = Ra + K * ((1.0 - score) - (1.0 - Eh))
    return R

def main():
    # Input checks
    if not os.path.exists(HIST):
        pd.DataFrame().to_csv(OUT_TAB_DATA, index=False); pd.DataFrame().to_csv(OUT_SUM_DATA, index=False)
        pd.DataFrame().to_csv(OUT_TAB_RUN, index=False);  pd.DataFrame().to_csv(OUT_SUM_RUN, index=False)
        print("[WARN] HIST missing; wrote empty calibration files.")
        return

    df = pd.read_csv(HIST)
    required = {"date","home_team","away_team","home_goals","away_goals"}
    if not required.issubset(df.columns):
        pd.DataFrame().to_csv(OUT_TAB_DATA, index=False); pd.DataFrame().to_csv(OUT_SUM_DATA, index=False)
        pd.DataFrame().to_csv(OUT_TAB_RUN, index=False);  pd.DataFrame().to_csv(OUT_SUM_RUN, index=False)
        print("[WARN] HIST lacks required columns; wrote empty calibration files.")
        return

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")

    # market probs (support both naming schemes)
    if {"oddsH","oddsD","oddsA"}.issubset(df.columns):
        mH = df["oddsH"].map(implied); mD = df["oddsD"].map(implied); mA = df["oddsA"].map(implied)
    elif {"home_odds_dec","draw_odds_dec","away_odds_dec"}.issubset(df.columns):
        mH = df["home_odds_dec"].map(implied); mD = df["draw_odds_dec"].map(implied); mA = df["away_odds_dec"].map(implied)
    else:
        m = np.full((len(df),3), np.nan, dtype=float)
        use_market = False
    if 'm' not in locals():
        m = np.array([strip_vig(h,d,a) for h,d,a in zip(mH,mD,mA)], dtype=float)
        use_market = True

    # model probs via Elo
    R = {}
    e = []
    for r in df.itertuples(index=False):
        h, a = r.home_team, r.away_team
        R.setdefault(h, 1500.0); R.setdefault(a, 1500.0)
        Rh, Ra = R[h], R[a]
        e.append(elo_triplet(Rh, Ra))
        Eh = 1.0/(1.0 + 10.0 ** (-((Rh - Ra + 60.0) / 400.0)))
        if pd.isna(r.home_goals) or pd.isna(r.away_goals): 
            continue
        score = 1.0 if r.home_goals > r.away_goals else (0.5 if r.home_goals == r.away_goals else 0.0)
        K = 20.0
        R[h] = Rh + K * (score - Eh)
        R[a] = Ra + K * ((1.0 - score) - (1.0 - Eh))
    e = np.array(e, dtype=float)

    # blend weight
    w = 0.85
    if os.path.exists(BLEND):
        try:
            jb = json.load(open(BLEND, "r"))
            if isinstance(jb, dict):
                if "w_market" in jb:  # legacy single weight
                    w = float(jb.get("w_market", w))
                w = float(jb.get("w_market_global", w))
        except Exception:
            pass

    if use_market:
        blend = w*m + (1.0 - w)*e
        s = blend.sum(axis=1, keepdims=True); s[s <= 0] = 1.0
        blend = blend / s
    else:
        blend = e  # no market, fallback to Elo only

    # Labels & Binning for home prob
    y = np.where(df["home_goals"] > df["away_goals"], 0, np.where(df["home_goals"] == df["away_goals"], 1, 2))
    pH = blend[:,0]
    bins = np.linspace(0, 1, 11)  # 10 bins
    idx = np.digitize(pH, bins, right=True) - 1
    idx[idx < 0] = 0; idx[idx > 8] = 9

    rows = []
    for b in range(10):
        mask = (idx == b)
        n = int(mask.sum())
        p_mid = (bins[b] + bins[b+1]) / 2.0
        if n == 0:
            rows.append({"bin": b, "p_mid": p_mid, "n": 0, "avg_pred": np.nan, "emp_home": np.nan})
            continue
        avg_pred = float(pH[mask].mean())
        emp = float((y[mask] == 0).mean())
        rows.append({"bin": b, "p_mid": p_mid, "n": n, "avg_pred": avg_pred, "emp_home": emp})
    tab = pd.DataFrame(rows)

    ece = float(((tab["n"] * (tab["avg_pred"] - tab["emp_home"]).abs()).sum()) / max(1, tab["n"].sum()))
    sum_df = pd.DataFrame([{"metric": "ECE_home", "value": ece}])

    # Write both to data/ and runs/YYYY-MM-DD/
    tab.to_csv(OUT_TAB_DATA, index=False)
    sum_df.to_csv(OUT_SUM_DATA, index=False)
    tab.to_csv(OUT_TAB_RUN, index=False)
    sum_df.to_csv(OUT_SUM_RUN, index=False)
    print(f"[OK] wrote CALIBRATION_TABLE ({len(tab)}) & CALIBRATION_SUMMARY (ECE={ece:.4f}) to data/ and {RUN_DIR}/")

if __name__ == "__main__":
    main()