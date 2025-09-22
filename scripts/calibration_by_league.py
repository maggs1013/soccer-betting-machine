#!/usr/bin/env python3
# Export per-league reliability bins for home-win probability and ECE.
# Reads: data/HIST_matches.csv, data/model_blend.json (+ optional odds fields)
# Writes: data/CALIBRATION_BY_LEAGUE.csv  AND  runs/YYYY-MM-DD/CALIBRATION_BY_LEAGUE.csv
#
# Notes:
# - Robust to odds column names: (oddsH/oddsD/oddsA) or (home_odds_dec/draw_odds_dec/away_odds_dec)
# - Robust to blend JSON: supports {"w_market_global", "w_market_leagues"} OR legacy {"w_market"}

import os, json, numpy as np, pandas as pd
from datetime import datetime

DATA = "data"
HIST = os.path.join(DATA, "HIST_matches.csv")
BLND = os.path.join(DATA, "model_blend.json")
OUT_DATA  = os.path.join(DATA, "CALIBRATION_BY_LEAGUE.csv")
RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)
OUT_RUN = os.path.join(RUN_DIR, "CALIBRATION_BY_LEAGUE.csv")

def implied(x):
    try:
        x = float(x)
        return 1.0/x if x > 0 else np.nan
    except:
        return np.nan

def strip_vig(h,d,a):
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
        Eh = 1.0/(1.0 + 10.0 ** (-((R[h] - R[a] + 60.0) / 400.0)))
        if pd.isna(r.home_goals) or pd.isna(r.away_goals): 
            continue
        score = 1.0 if r.home_goals > r.away_goals else (0.5 if r.home_goals == r.away_goals else 0.0)
        K = 20.0
        R[h] = R[h] + K * (score - Eh)
        R[a] = R[a] + K * ((1.0 - score) - (1.0 - Eh))
    return R

def get_market_probs(df):
    # Accept either canonical oddsH/oddsD/oddsA OR legacy *_odds_dec
    H = None; D = None; A = None
    if {"oddsH","oddsD","oddsA"}.issubset(df.columns):
        H, D, A = df["oddsH"], df["oddsD"], df["oddsA"]
    elif {"home_odds_dec","draw_odds_dec","away_odds_dec"}.issubset(df.columns):
        H, D, A = df["home_odds_dec"], df["draw_odds_dec"], df["away_odds_dec"]
    else:
        return np.full((len(df),3), np.nan, dtype=float)

    mH = H.map(implied); mD = D.map(implied); mA = A.map(implied)
    return np.array([strip_vig(h,d,a) for h,d,a in zip(mH,mD,mA)], dtype=float)

def main():
    # Check inputs
    if not os.path.exists(HIST):
        pd.DataFrame(columns=["league","bin","p_mid","n","avg_pred_home","emp_home","ece_weighted"]).to_csv(OUT_DATA, index=False)
        pd.DataFrame(columns=["league","bin","p_mid","n","avg_pred_home","emp_home","ece_weighted"]).to_csv(OUT_RUN, index=False)
        print(f"[WARN] Missing {HIST}; wrote empty calibration-by-league files.")
        return

    df = pd.read_csv(HIST)
    if df.empty:
        pd.DataFrame(columns=["league","bin","p_mid","n","avg_pred_home","emp_home","ece_weighted"]).to_csv(OUT_DATA, index=False)
        pd.DataFrame(columns=["league","bin","p_mid","n","avg_pred_home","emp_home","ece_weighted"]).to_csv(OUT_RUN, index=False)
        print("[WARN] HIST empty; wrote empty outputs.")
        return

    if "league" not in df.columns: 
        df["league"] = "GLOBAL"
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")

    # Blend weights (support both schemas)
    w_global = 0.85
    w_leagues = {}
    if os.path.exists(BLND):
        try:
            mb = json.load(open(BLND, "r"))
            if isinstance(mb, dict):
                if "w_market" in mb:  # legacy single weight
                    w_global = float(mb.get("w_market", 0.85))
                w_global = float(mb.get("w_market_global", w_global))
                w_leagues = mb.get("w_market_leagues", {}) or {}
        except Exception:
            pass

    # Market probabilities (vig-stripped) and Elo probs
    m = get_market_probs(df)

    R = {}
    e = []
    for r in df.itertuples(index=False):
        h, a = r.home_team, r.away_team
        R.setdefault(h, 1500.0); R.setdefault(a, 1500.0)
        e.append(elo_triplet(R[h], R[a]))
        Eh = 1.0/(1.0 + 10.0 ** (-((R[h]-R[a]+60.0)/400.0)))
        if pd.isna(r.home_goals) or pd.isna(r.away_goals): 
            continue
        score = 1.0 if r.home_goals > r.away_goals else (0.5 if r.home_goals == r.away_goals else 0.0)
        K = 20.0
        R[h] = R[h] + K * (score - Eh)
        R[a] = R[a] + K * ((1.0 - score) - (1.0 - Eh))
    e = np.array(e, dtype=float)

    # Labels: 0=home,1=draw,2=away
    y = np.where(df["home_goals"] > df["away_goals"], 0, np.where(df["home_goals"] == df["away_goals"], 1, 2))

    # Bin & ECE by league (home prob only for ECE_home)
    rows = []
    bins = np.linspace(0, 1, 11)
    for lg, dL in df.groupby("league"):
        idx = dL.index.values
        # choose per-league weight if provided
        w = float(w_leagues.get(lg, w_global))
        # if any market prob is NaN for a row -> use Elo for that row
        m_sub = m[idx]
        e_sub = e[idx]
        b = np.where(np.isnan(m_sub).any(axis=1)[:,None], e_sub, w*m_sub + (1.0 - w)*e_sub)
        s = b.sum(axis=1, keepdims=True); s[s <= 0] = 1.0
        b /= s

        pH = b[:,0]
        yH = (y[idx] == 0).astype(int)

        bin_idx = np.digitize(pH, bins, right=True) - 1
        bin_idx[bin_idx < 0] = 0
        bin_idx[bin_idx > 8] = 9

        total = int(len(pH))
        ece_num = 0.0
        for bi in range(10):
            mask = (bin_idx == bi)
            n = int(mask.sum())
            p_mid = (bins[bi] + bins[bi+1]) / 2.0
            if n == 0:
                rows.append({"league": lg, "bin": bi, "p_mid": p_mid, "n": 0,
                             "avg_pred_home": np.nan, "emp_home": np.nan, "ece_weighted": 0.0})
                continue
            avg_pred = float(np.mean(pH[mask]))
            emp = float(np.mean(yH[mask]))
            ece_num += n * abs(avg_pred - emp)
            rows.append({"league": lg, "bin": bi, "p_mid": p_mid, "n": n,
                         "avg_pred_home": avg_pred, "emp_home": emp, "ece_weighted": None})
        ece = ece_num / max(1, total)
        rows.append({"league": lg, "bin": "ECE", "p_mid": None, "n": total,
                     "avg_pred_home": None, "emp_home": None, "ece_weighted": ece})

    out = pd.DataFrame(rows)
    out.to_csv(OUT_DATA, index=False)
    out.to_csv(OUT_RUN, index=False)
    print(f"[OK] wrote CALIBRATION_BY_LEAGUE to {OUT_DATA} and {OUT_RUN} (rows={len(out)})")

if __name__ == "__main__":
    main()