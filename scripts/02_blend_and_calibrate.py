# scripts/02_blend_and_calibrate.py
# Learn market↔model blend weights (global + per-league) and fit per-league isotonic calibrators.
# Inputs (data/):
#   HIST_matches.csv  (requires: date, home_team, away_team, home_goals, away_goals;
#                     optional but preferred: home_odds_dec, draw_odds_dec, away_odds_dec, league)
# Outputs:
#   model_blend.json  -> {"w_market_global": float, "w_market_leagues": {"LeagueName": float, ...}}
#   calibrator.pkl    -> {"global":{"home":Iso,"draw":Iso,"away":Iso},
#                         "per_league":{"LeagueName":{"home":Iso,"draw":Iso,"away":Iso}, ...}}
#
# Notes:
# - If odds are missing, falls back to Elo-only for that slice (weights default to 0.85).
# - If a league has too few samples to calibrate, it inherits global calibrators.

import os, json, pickle, numpy as np, pandas as pd
from math import log
from sklearn.isotonic import IsotonicRegression

DATA = "data"
HIST = os.path.join(DATA, "HIST_matches.csv")
BLND = os.path.join(DATA, "model_blend.json")
CAL  = os.path.join(DATA, "calibrator.pkl")

# -------------------------- Utilities --------------------------

def implied(dec):
    try:
        d = float(dec)
        return 1.0/d if d > 0 else np.nan
    except Exception:
        return np.nan

def strip_vig(pH, pD, pA):
    s = pH + pD + pA
    if not np.isfinite(s) or s <= 0:
        return (np.nan, np.nan, np.nan)
    return (pH/s, pD/s, pA/s)

def elo_prob(Rh, Ra, ha=60.0):
    # Convert Elo ratings to 1X2 probabilities
    pH_core = 1.0 / (1.0 + 10.0 ** (-((Rh - Ra + ha) / 400.0)))
    pD = 0.18 + 0.10 * np.exp(-abs(Rh - Ra) / 200.0)
    pH = (1.0 - pD) * pH_core
    pA = 1.0 - pH - pD
    eps = 1e-6
    return (max(eps, min(1.0-eps, pH)),
            max(eps, min(1.0-eps, pD)),
            max(eps, min(1.0-eps, pA)))

def build_elo(df):
    # Fit Elo sequentially over df (sorted by date)
    R = {}
    for r in df.itertuples(index=False):
        h, a = r.home_team, r.away_team
        R.setdefault(h, 1500.0); R.setdefault(a, 1500.0)
        Rh, Ra = R[h], R[a]
        Eh = 1.0 / (1.0 + 10.0 ** (-((Rh - Ra + 60.0) / 400.0)))
        if pd.isna(r.home_goals) or pd.isna(r.away_goals):
            continue
        score = 1.0 if r.home_goals > r.away_goals else (0.5 if r.home_goals == r.away_goals else 0.0)
        K = 20.0
        R[h] = Rh + K * (score - Eh)
        R[a] = Ra + K * ((1.0 - score) - (1.0 - Eh))
    return R

def logloss_one(y_idx, p_vec):
    # y_idx: 0(home)/1(draw)/2(away); p_vec: (pH,pD,pA)
    eps = 1e-12
    return -log(max(eps, p_vec[y_idx]))

def fit_isotonic(p, y_binary):
    # p: probabilities for one class; y_binary: 0/1
    try:
        iso = IsotonicRegression(out_of_bounds="clip")
        iso.fit(p, y_binary)
        return iso
    except Exception:
        return None

# -------------------------- Main --------------------------

def main():
    # Defaults
    default_w = 0.85
    default_cal = {"home": None, "draw": None, "away": None}

    # Load history
    if not os.path.exists(HIST):
        json.dump({"w_market_global": default_w, "w_market_leagues": {}}, open(BLND, "w"))
        pickle.dump({"global": default_cal, "per_league": {}}, open(CAL, "wb"))
        print("[WARN] HIST missing; wrote default blend/calibrator.")
        return

    df = pd.read_csv(HIST)
    # Mandatory columns
    need = {"date", "home_team", "away_team", "home_goals", "away_goals"}
    if not need.issubset(df.columns) or df.empty:
        json.dump({"w_market_global": default_w, "w_market_leagues": {}}, open(BLND, "w"))
        pickle.dump({"global": default_cal, "per_league": {}}, open(CAL, "wb"))
        print("[WARN] HIST lacks required columns; wrote default blend/calibrator.")
        return

    # Optional leagues/odds
    if "league" not in df.columns:
        df["league"] = "GLOBAL"

    # Parse dates / sort
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")

    # Labels (0/1/2)
    y = np.where(df["home_goals"] > df["away_goals"], 0,
                 np.where(df["home_goals"] == df["away_goals"], 1, 2))

    # Market probabilities (may be NaN)
    if {"home_odds_dec", "draw_odds_dec", "away_odds_dec"}.issubset(df.columns):
        mH = df["home_odds_dec"].map(implied)
        mD = df["draw_odds_dec"].map(implied)
        mA = df["away_odds_dec"].map(implied)
        m_probs = np.array([strip_vig(h, d, a) for h, d, a in zip(mH, mD, mA)], dtype=float)
    else:
        m_probs = np.array([(np.nan, np.nan, np.nan)] * len(df), dtype=float)

    # Elo model probabilities (fit sequentially over full df)
    R = {}
    e_probs = []
    for r in df.itertuples(index=False):
        h, a = r.home_team, r.away_team
        R.setdefault(h, 1500.0); R.setdefault(a, 1500.0)
        e_probs.append(elo_prob(R[h], R[a]))
        # Update Elo on the result
        Eh = 1.0 / (1.0 + 10.0 ** (-((R[h] - R[a] + 60.0) / 400.0)))
        if pd.isna(r.home_goals) or pd.isna(r.away_goals):
            continue
        score = 1.0 if r.home_goals > r.away_goals else (0.5 if r.home_goals == r.away_goals else 0.0)
        K = 20.0
        R[h] = R[h] + K * (score - Eh)
        R[a] = R[a] + K * ((1.0 - score) - (1.0 - Eh))
    e_probs = np.array(e_probs, dtype=float)

    # ---------- Learn Global Weight ----------
    # Skip samples where market is NaN (use Elo-only for that portion)
    weights = np.linspace(0.55, 0.95, 9)  # market-dominant range
    def learn_weight(p_market, p_model, yidx):
        best_w, best_ll = default_w, 1e18
        # If market completely missing, return default and rely on model
        if np.all(~np.isfinite(p_market)):
            return default_w, None
        mask = np.isfinite(p_market).all(axis=1)
        if not mask.any():
            return default_w, None
        pM = p_market[mask]; pE = p_model[mask]; y0 = yidx[mask]
        for w in weights:
            blend = w * pM + (1.0 - w) * pE
            # normalize
            s = blend.sum(axis=1, keepdims=True); s[s == 0] = 1.0
            blend = blend / s
            ll = np.mean([logloss_one(yy, blend[i]) for i, yy in enumerate(y0)])
            if ll < best_ll:
                best_ll = ll; best_w = w
        return best_w, best_ll

    w_global, ll_global = learn_weight(m_probs, e_probs, y)

    # ---------- Learn Per-League Weights ----------
    leagues = sorted(df["league"].dropna().unique())
    w_leagues = {}
    for lg in leagues:
        maskL = df["league"] == lg
        if maskL.sum() < 150:  # minimum samples to learn a league-specific weight
            continue
        wL, _ = learn_weight(m_probs[maskL], e_probs[maskL], y[maskL])
        w_leagues[lg] = float(wL)

    # Fallback: if no league-specific learned, w_leagues remains {}
    model_blend = {"w_market_global": float(w_global), "w_market_leagues": w_leagues}
    json.dump(model_blend, open(BLND, "w"))
    print(f"[OK] learned w_market_global={w_global:.2f} | leagues learned={len(w_leagues)} → {BLND}")

    # ---------- Fit Global Calibrators ----------
    # Use global blend on all samples
    blend_global = np.where(np.isnan(m_probs).any(axis=1)[:, None],
                            e_probs,
                            w_global * m_probs + (1.0 - w_global) * e_probs)
    s = blend_global.sum(axis=1, keepdims=True); s[s == 0] = 1.0
    blend_global = blend_global / s
    pH, pD, pA = blend_global[:, 0], blend_global[:, 1], blend_global[:, 2]
    iso_H_g = fit_isotonic(pH, (y == 0).astype(int))
    iso_D_g = fit_isotonic(pD, (y == 1).astype(int))
    iso_A_g = fit_isotonic(pA, (y == 2).astype(int))
    cal_global = {"home": iso_H_g, "draw": iso_D_g, "away": iso_A_g}

    # ---------- Fit Per-League Calibrators ----------
    cal_leagues = {}
    for lg in leagues:
        maskL = df["league"] == lg
        nL = int(maskL.sum())
        if nL < 150 or lg not in w_leagues:
            continue
        wL = w_leagues[lg]
        blendL = np.where(np.isnan(m_probs[maskL]).any(axis=1)[:, None],
                          e_probs[maskL],
                          wL * m_probs[maskL] + (1.0 - wL) * e_probs[maskL])
        s = blendL.sum(axis=1, keepdims=True); s[s == 0] = 1.0
        blendL = blendL / s
        pH, pD, pA = blendL[:, 0], blendL[:, 1], blendL[:, 2]
        iso_H = fit_isotonic(pH, (y[maskL] == 0).astype(int))
        iso_D = fit_isotonic(pD, (y[maskL] == 1).astype(int))
        iso_A = fit_isotonic(pA, (y[maskL] == 2).astype(int))
        # Only keep if all three calibrators exist
        if iso_H is not None and iso_D is not None and iso_A is not None:
            cal_leagues[lg] = {"home": iso_H, "draw": iso_D, "away": iso_A}

    pickle.dump({"global": cal_global, "per_league": cal_leagues}, open(CAL, "wb"))
    print(f"[OK] saved calibrators: global + {len(cal_leagues)} leagues → {CAL}")

if __name__ == "__main__":
    main()