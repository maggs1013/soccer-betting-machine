# scripts/model_predict.py
# Predict upcoming matches using:
#   - Feature model probabilities if available (feature_proba_upcoming.csv)
#   - Elo fallback built from HIST
#   - Market probabilities from Odds API (if present)
# Blend = per-league w_market if available, else global; then apply per-league isotonic calibration if available.
# Kelly > 0 only when odds present.
#
# Inputs (data/):
#   UPCOMING_7D_enriched.csv   (date, home_team, away_team, optional: league, odds)
#   feature_proba_upcoming.csv (date, home_team, away_team, fH, fD, fA) [optional]
#   HIST_matches.csv
#   model_blend.json           (w_market_global + per_league)
#   calibrator.pkl             (global + per_league calibrators)
# Output:
#   PREDICTIONS_7D.csv

import os, json, pickle, numpy as np, pandas as pd

DATA="data"
UP   = os.path.join(DATA,"UPCOMING_7D_enriched.csv")
FP   = os.path.join(DATA,"feature_proba_upcoming.csv")
HIST = os.path.join(DATA,"HIST_matches.csv")
BL   = os.path.join(DATA,"model_blend.json")
CAL  = os.path.join(DATA,"calibrator.pkl")
OUT  = os.path.join(DATA,"PREDICTIONS_7D.csv")

def implied(d):
    try:
        d = float(d)
        return 1.0/d if d > 0 else np.nan
    except Exception:
        return np.nan

def strip_vig(ph, pd_, pa):
    s = ph + pd_ + pa
    if not np.isfinite(s) or s <= 0:
        return (np.nan, np.nan, np.nan)
    return (ph/s, pd_/s, pa/s)

def elo_prob(Rh, Ra, ha=60.0):
    pH_core = 1.0 / (1.0 + 10.0 ** (-((Rh - Ra + ha) / 400.0)))
    pD = 0.18 + 0.10 * np.exp(-abs(Rh - Ra) / 200.0)
    pH = (1.0 - pD) * pH_core
    pA = 1.0 - pH - pD
    eps = 1e-6
    return (max(eps, min(1.0-eps, pH)),
            max(eps, min(1.0-eps, pD)),
            max(eps, min(1.0-eps, pA)))

def build_elo_ratings(hist_df):
    R = {}
    hist_df = hist_df.copy()
    hist_df["date"] = pd.to_datetime(hist_df["date"], errors="coerce")
    hist_df = hist_df.dropna(subset=["date"]).sort_values("date")
    for r in hist_df.itertuples(index=False):
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

def load_json(p, default):
    try:
        with open(p, "r") as f: return json.load(f)
    except Exception:
        return default

def load_cal(p):
    try:
        with open(p, "rb") as f: return pickle.load(f)
    except Exception:
        return {"global":{"home":None,"draw":None,"away":None}, "per_league":{}}

def kelly(p, dec, cap=0.10):
    try:
        b = float(dec) - 1.0
        if b <= 0 or not np.isfinite(p): return 0.0
        q = 1.0 - p
        k = (b * p - q) / b
        return float(max(0.0, min(k, cap)))
    except Exception:
        return 0.0

def main():
    # Load upcoming
    if not os.path.exists(UP):
        pd.DataFrame(columns=["date","home_team","away_team","pH","pD","pA","kelly_H","kelly_D","kelly_A"]).to_csv(OUT, index=False)
        print("[WARN] Upcoming missing; wrote header-only predictions.")
        return
    up = pd.read_csv(UP)
    if up.empty:
        pd.DataFrame(columns=["date","home_team","away_team","pH","pD","pA","kelly_H","kelly_D","kelly_A"]).to_csv(OUT, index=False)
        print("[WARN] Upcoming empty; wrote header-only predictions.")
        return
    if "league" not in up.columns:
        up["league"] = "GLOBAL"

    # Feature model probabilities (optional)
    fproba = pd.DataFrame(columns=["date","home_team","away_team","fH","fD","fA"])
    if os.path.exists(FP):
        try: fproba = pd.read_csv(FP)
        except Exception: pass
    df = up.merge(fproba, on=["date","home_team","away_team"], how="left")

    # Market probabilities (optional)
    if {"home_odds_dec","draw_odds_dec","away_odds_dec"}.issubset(df.columns):
        mH = df["home_odds_dec"].map(implied)
        mD = df["draw_odds_dec"].map(implied)
        mA = df["away_odds_dec"].map(implied)
        m_probs = np.array([strip_vig(h, d, a) for h, d, a in zip(mH, mD, mA)], dtype=float)
    else:
        m_probs = np.array([(np.nan, np.nan, np.nan)] * len(df), dtype=float)

    # Elo fallback (build from HIST)
    R = {}
    if os.path.exists(HIST):
        try:
            hist = pd.read_csv(HIST)
            need = {"date","home_team","away_team","home_goals","away_goals"}
            if need.issubset(hist.columns) and not hist.empty:
                R = build_elo_ratings(hist)
        except Exception:
            R = {}
    elo_p = np.array([elo_prob(R.get(ht,1500.0), R.get(at,1500.0)) for ht,at in zip(df["home_team"], df["away_team"])], dtype=float)

    # Model combo: feature probs if full row present, else Elo
    f_ok = df[["fH","fD","fA"]].notna().all(axis=1).values
    model_combo = np.where(f_ok[:,None],
                           df[["fH","fD","fA"]].fillna(0.0).values,
                           elo_p)

    # Load blend weights and calibrators
    blend = load_json(BL, {"w_market_global":0.85, "w_market_leagues":{}})
    w_global = blend.get("w_market_global", 0.85)
    w_league = blend.get("w_market_leagues", {})
    cals = load_cal(CAL)
    cal_global = cals.get("global", {"home":None,"draw":None,"away":None})
    cal_leagues = cals.get("per_league", {})

    # Blend per row using league-specific weight if available
    leagues = df["league"].astype(str).values
    blended = np.zeros((len(df), 3), dtype=float)
    for i in range(len(df)):
        w = w_league.get(leagues[i], w_global)
        m_vec = m_probs[i]
        e_vec = model_combo[i]
        if np.isnan(m_vec).any():
            vec = e_vec
        else:
            vec = w * m_vec + (1.0 - w) * e_vec
        s = vec.sum()
        blended[i] = vec / s if s > 0 else e_vec

    # Apply per-league calibrators when available
    pH, pD, pA = blended[:,0], blended[:,1], blended[:,2]
    pH_cal = np.zeros_like(pH); pD_cal = np.zeros_like(pD); pA_cal = np.zeros_like(pA)
    for i in range(len(df)):
        lg = leagues[i]
        cal = cal_leagues.get(lg, None)
        if cal is None or any(v is None for v in [cal.get("home"), cal.get("draw"), cal.get("away")]):
            cal = cal_global
        isoH, isoD, isoA = cal.get("home"), cal.get("draw"), cal.get("away")
        ph_i, pd_i, pa_i = pH[i], pD[i], pA[i]
        if isoH is not None: ph_i = isoH.transform([ph_i])[0]
        if isoD is not None: pd_i = isoD.transform([pd_i])[0]
        if isoA is not None: pa_i = isoA.transform([pa_i])[0]
        s = ph_i + pd_i + pa_i
        if s <= 0:
            ph_i, pd_i, pa_i = pH[i], pD[i], pA[i]
            s = ph_i + pd_i + pa_i
        pH_cal[i], pD_cal[i], pA_cal[i] = ph_i/s, pd_i/s, pa_i/s

    # Kelly only when odds exist
    kH = [kelly(h, o) for h,o in zip(pH_cal, df.get("home_odds_dec", pd.Series(np.nan, index=df.index)))]
    kD = [kelly(d, o) for d,o in zip(pD_cal, df.get("draw_odds_dec", pd.Series(np.nan, index=df.index)))]
    kA = [kelly(a, o) for a,o in zip(pA_cal, df.get("away_odds_dec", pd.Series(np.nan, index=df.index)))]

    out = pd.DataFrame({
        "date": df.get("date"),
        "league": df.get("league"),
        "home_team": df.get("home_team"),
        "away_team": df.get("away_team"),
        "pH": pH_cal, "pD": pD_cal, "pA": pA_cal,
        "kelly_H": kH, "kelly_D": kD, "kelly_A": kA
    })
    # Sort by date then by highest Kelly component
    out["top_kelly"] = np.nanmax(np.vstack([out["kelly_H"].values,
                                            out["kelly_D"].values,
                                            out["kelly_A"].values]).T, axis=1)
    if "date" in out.columns:
        out.sort_values(["date","top_kelly"], ascending=[True, False], inplace=True)
    out.to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} rows={len(out)}")

if __name__ == "__main__":
    main()