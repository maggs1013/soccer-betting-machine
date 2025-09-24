#!/usr/bin/env python3
# Learn market↔model blend weights (global + per-league) and fit per-league
# isotonic calibrators from **DOMESTIC-ONLY** matches (UEFA excluded).
#
# Inputs (data/):
#   HIST_matches.csv  (requires: date, home_team, away_team, home_goals, away_goals;
#                     optional: home_odds_dec, draw_odds_dec, away_odds_dec, league)
#
# Outputs:
#   model_blend.json  -> {"w_market_global": float,
#                         "w_market_leagues": {"LeagueName": float, ...}}
#   calibrator.pkl    -> {"global":{"home":Iso,"draw":Iso,"away":Iso},
#                         "per_league":{"LeagueName":{"home":Iso,"draw":Iso,"away":Iso}, ...}}
#
# Notes:
# - UEFA comps (UCL/UEL/UECL) do NOT enter weight learning or calibrators.
# - If odds are missing, rows fall back to Elo-only in those slices.
# - If a league has too few samples, it inherits the global calibrators.

import os, json, pickle, numpy as np, pandas as pd
from sklearn.isotonic import IsotonicRegression

DATA = "data"
HIST = os.path.join(DATA, "HIST_matches.csv")
BLND = os.path.join(DATA, "model_blend.json")
CAL  = os.path.join(DATA, "calibrator.pkl")

# --- UEFA competition detection ------------------------------------------------
UEFA_TOKENS = [
    "champions league", "uefa champions", "ucl",
    "europa league", "uefa europa", "uel",
    "conference league", "uefa europa conference", "uecl",
    "super cup"
]

def is_uefa(league_str):
    if not isinstance(league_str, str): return False
    s = league_str.lower()
    return any(tok in s for tok in UEFA_TOKENS)

# --- Utility functions ---------------------------------------------------------

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

def elo_triplet(Rh, Ra, ha=60.0):
    """Convert Elo ratings to (pH, pD, pA) with a draw prior decaying by rating gap."""
    pH_core = 1.0 / (1.0 + 10.0 ** (-((Rh - Ra + ha) / 400.0)))
    pD = 0.18 + 0.10 * np.exp(-abs(Rh - Ra) / 200.0)
    pH = (1.0 - pD) * pH_core
    pA = 1.0 - pH - pD
    eps = 1e-6
    return (max(eps, min(1.0-eps, pH)),
            max(eps, min(1.0-eps, pD)),
            max(eps, min(1.0-eps, pA)))

def fit_isotonic(prob, y_bin):
    """Safe isotonic fit; returns None if it fails."""
    try:
        iso = IsotonicRegression(out_of_bounds="clip")
        iso.fit(prob, y_bin)
        return iso
    except Exception:
        return None

def learn_weight(p_market, p_model, y_idx, default_w=0.85):
    """
    Learn a single blend weight w in [0.55, 0.95] minimizing log-loss on rows
    where market probs are finite. Returns default_w if not learnable.
    """
    grid = np.linspace(0.55, 0.95, 9)
    # Mask rows where market is finite across all three outcomes
    mask = np.isfinite(p_market).all(axis=1)
    if mask.sum() == 0:
        return float(default_w)

    Pm = p_market[mask]
    Pe = p_model[mask]
    y  = y_idx[mask].astype(int)

    best_w, best_ll = float(default_w), 1e18
    for w in grid:
        B = w * Pm + (1.0 - w) * Pe
        s = B.sum(axis=1, keepdims=True)
        s[s == 0] = 1.0
        B = B / s
        # vectorized log-loss
        ll = -np.log(np.take_along_axis(B, y.reshape(-1,1), axis=1).clip(1e-12)).mean()
        if ll < best_ll:
            best_ll, best_w = ll, float(w)
    return best_w

# --- Main ----------------------------------------------------------------------

def main():
    default_w   = 0.85
    default_cal = {"home": None, "draw": None, "away": None}

    if not os.path.exists(HIST):
        json.dump({"w_market_global": default_w, "w_market_leagues": {}}, open(BLND, "w"))
        pickle.dump({"global": default_cal, "per_league": {}}, open(CAL, "wb"))
        print("[WARN] HIST missing; wrote defaults.")
        return

    df = pd.read_csv(HIST)
    need = {"date", "home_team", "away_team", "home_goals", "away_goals"}
    if not need.issubset(df.columns) or df.empty:
        json.dump({"w_market_global": default_w, "w_market_leagues": {}}, open(BLND, "w"))
        pickle.dump({"global": default_cal, "per_league": {}}, open(CAL, "wb"))
        print("[WARN] HIST lacks required columns; wrote defaults.")
        return

    # League & date hygiene
    if "league" not in df.columns:
        df["league"] = "GLOBAL"
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")

    # **DOMESTIC ONLY** for learning/calibration
    df = df[~df["league"].astype(str).apply(is_uefa)]
    if df.empty:
        # No domestic data—fall back to defaults
        json.dump({"w_market_global": default_w, "w_market_leagues": {}}, open(BLND, "w"))
        pickle.dump({"global": default_cal, "per_league": {}}, open(CAL, "wb"))
        print("[WARN] No domestic rows after UEFA filter; wrote defaults.")
        return

    # Labels
    y = np.where(df["home_goals"] > df["away_goals"], 0,
        np.where(df["home_goals"] == df["away_goals"], 1, 2)).astype(int)

    # Market probabilities (may be NaN)
    have_odds = {"home_odds_dec", "draw_odds_dec", "away_odds_dec"}.issubset(df.columns)
    if have_odds:
        mH = df["home_odds_dec"].map(implied)
        mD = df["draw_odds_dec"].map(implied)
        mA = df["away_odds_dec"].map(implied)
        m_probs = np.array([strip_vig(h, d, a) for h, d, a in zip(mH, mD, mA)], dtype=float)
    else:
        m_probs = np.full((len(df), 3), np.nan, dtype=float)

    # Elo probabilities (sequential over time)
    R = {}
    e_probs = []
    for r in df.itertuples(index=False):
        h, a = r.home_team, r.away_team
        R.setdefault(h, 1500.0); R.setdefault(a, 1500.0)
        e_probs.append(elo_triplet(R[h], R[a]))
        # update Elo using result
        Eh = 1.0 / (1.0 + 10.0 ** (-((R[h] - R[a] + 60.0) / 400.0)))
        if not (pd.isna(r.home_goals) or pd.isna(r.away_goals)):
            score = 1.0 if r.home_goals > r.away_goals else (0.5 if r.home_goals == r.away_goals else 0.0)
            K = 20.0
            R[h] = R[h] + K * (score - Eh)
            R[a] = R[a] + K * ((1.0 - score) - (1.0 - Eh))
    e_probs = np.array(e_probs, dtype=float)

    # --- Learn global weight (domestic only) ---
    w_global = learn_weight(m_probs, e_probs, y, default_w=default_w)

    # --- Learn per-league weights (domestic leagues only) ---
    w_leagues = {}
    for lg, idxs in df.groupby("league").groups.items():
        idx = np.array(list(idxs), dtype=int)
        if len(idx) < 150:
            continue
        w_l = learn_weight(m_probs[idx], e_probs[idx], y[idx], default_w=default_w)
        w_leagues[lg] = float(w_l)

    # Write model_blend.json
    json.dump({"w_market_global": float(w_global), "w_market_leagues": w_leagues}, open(BLND, "w"))
    print(f"[OK] learned w_market_global={w_global:.2f} | per-league={len(w_leagues)} → {BLND}")

    # --- Calibrators (isotonic) ---
    def blend_probs(w, mP, eP):
        B = np.where(np.isfinite(mP).all(axis=1)[:, None], w * mP + (1.0 - w) * eP, eP)
        s = B.sum(axis=1, keepdims=True)
        s[s == 0] = 1.0
        return B / s

    def fit_triplet(B, yy):
        isoH = fit_isotonic(B[:, 0], (yy == 0).astype(int))
        isoD = fit_isotonic(B[:, 1], (yy == 1).astype(int))
        isoA = fit_isotonic(B[:, 2], (yy == 2).astype(int))
        return {"home": isoH, "draw": isoD, "away": isoA}

    # Global calibrators
    B_global = blend_probs(w_global, m_probs, e_probs)
    cal_global = fit_triplet(B_global, y)

    # Per-league calibrators (only if enough samples & league weight exists)
    cal_leagues = {}
    for lg, idxs in df.groupby("league").groups.items():
        idx = np.array(list(idxs), dtype=int)
        if len(idx) < 150 or lg not in w_leagues:
            continue
        wL = w_leagues[lg]
        B_L = blend_probs(wL, m_probs[idx], e_probs[idx])
        cal_L = fit_triplet(B_L, y[idx])
        # keep only if all three are fitted
        if all(cal_L[k] is not None for k in ("home","draw","away")):
            cal_leagues[lg] = cal_L

    # Persist
    pickle.dump({"global": cal_global, "per_league": cal_leagues}, open(CAL, "wb"))
    print(f"[OK] saved calibrators (global + {len(cal_leagues)} leagues) → {CAL}")

if __name__ == "__main__":
    main()