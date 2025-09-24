#!/usr/bin/env python3
# Predict upcoming matches using:
#   • Feature model probs (if available) or Elo fallback
#   • Market probs from odds (if present)
# Blend & calibrate using DOMESTIC or TOURNAMENT files per fixture.
# Kelly shown for display; final caps handled downstream.

import os, json, pickle
import numpy as np
import pandas as pd
from typing import Any, Dict, Tuple
from datetime import datetime

DATA = "data"
RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

UP   = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
FP   = os.path.join(DATA, "feature_proba_upcoming.csv")
HIST = os.path.join(DATA, "HIST_matches.csv")
BL_DOM = os.path.join(DATA, "model_blend.json")
CAL_DOM= os.path.join(DATA, "calibrator.pkl")
BL_T   = os.path.join(DATA, "model_blend_tournaments.json")
CAL_T  = os.path.join(DATA, "calibrator_tournaments.pkl")
OUT_DATA = os.path.join(DATA, "PREDICTIONS_7D.csv")
OUT_RUN  = os.path.join(RUN_DIR, "PREDICTIONS_7D.csv")

EPS = 1e-9

def implied(dec: float) -> float:
    try: d=float(dec); return 1.0/d if d>0 else np.nan
    except: return np.nan

def strip_vig(ph, pd_, pa) -> Tuple[float,float,float]:
    s = ph + pd_ + pa
    if not np.isfinite(s) or s <= 0: return (np.nan,np.nan,np.nan)
    return (ph/s, pd_/s, pa/s)

def elo_triplet(Rh, Ra, ha=60.0) -> Tuple[float,float,float]:
    pH_core = 1.0/(1.0+10.0**(-((Rh-Ra+ha)/400.0)))
    pD = 0.18 + 0.10*np.exp(-abs(Rh-Ra)/200.0)
    pH = (1.0-pD)*pH_core; pA = 1.0 - pH - pD
    pH = min(max(pH, EPS), 1.0 - EPS)
    pD = min(max(pD, EPS), 1.0 - EPS)
    pA = max(EPS, 1.0 - pH - pD)
    return (pH, pD, pA)

def build_elo_ratings(hist_df: pd.DataFrame) -> Dict[str, float]:
    R: Dict[str, float] = {}
    if hist_df.empty: return R
    df = hist_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
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

def load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r") as f: return json.load(f)
    except Exception:
        return default

def load_calibrators(path: str, default: Dict[str, Any]) -> Dict[str, Any]:
    try:
        with open(path, "rb") as f:
            obj = pickle.load(f)
            return obj if isinstance(obj, dict) else default
    except Exception:
        return default

def apply_isotonic_triplet(cal_triplet: Dict[str, Any], pH: float, pD: float, pA: float) -> Tuple[float,float,float]:
    isoH, isoD, isoA = cal_triplet.get("home"), cal_triplet.get("draw"), cal_triplet.get("away")
    ph = float(isoH.transform([pH])[0]) if isoH is not None else pH
    pd_ = float(isoD.transform([pD])[0]) if isoD is not None else pD
    pa = float(isoA.transform([pA])[0]) if isoA is not None else pA
    s = ph + pd_ + pa
    if s <= 0: return (pH, pD, pA)
    return (ph/s, pd_/s, pa/s)

def kelly(p: float, dec: float, cap: float = 0.10) -> float:
    try:
        b = float(dec) - 1.0
        if b <= 0 or not np.isfinite(p): return 0.0
        q = 1.0 - p
        k = (b * p - q) / b
        return float(max(0.0, min(k, cap)))
    except Exception:
        return 0.0

def canonical_fixture_id(row: pd.Series) -> str:
    date = str(row.get("date", "NA")).replace("-", "")
    h = str(row.get("home_team", "NA")).strip().lower().replace(" ", "_")
    a = str(row.get("away_team", "NA")).strip().lower().replace(" ", "_")
    return f"{date}__{h}__vs__{a}"

def ensure_league(df: pd.DataFrame) -> pd.DataFrame:
    if "league" not in df.columns: df["league"] = "GLOBAL"
    df["league"] = df["league"].astype(str)
    return df

def is_tournament_bucket(cb):
    return str(cb) in ("UCL","UEL","UECL")

def main():
    # Load upcoming
    header = ["fixture_id","date","league","home_team","away_team","pH","pD","pA","oddsH","oddsD","oddsA","kelly_H","kelly_D","kelly_A","top_kelly"]
    if not os.path.exists(UP):
        for p in (OUT_DATA, OUT_RUN):
            pd.DataFrame(columns=header).to_csv(p, index=False)
        print("[WARN] UPCOMING_7D_enriched.csv missing; wrote header-only.")
        return

    up = pd.read_csv(UP)
    if up.empty:
        for p in (OUT_DATA, OUT_RUN):
            pd.DataFrame(columns=header).to_csv(p, index=False)
        print("[WARN] UPCOMING_7D_enriched.csv empty; wrote header-only.")
        return

    up = ensure_league(up)
    if "fixture_id" not in up.columns:
        up["fixture_id"] = up.apply(canonical_fixture_id, axis=1)

    # Optional feature probs
    fproba = pd.DataFrame(columns=["date","home_team","away_team","fH","fD","fA"])
    if os.path.exists(FP):
        try: fproba = pd.read_csv(FP)
        except: pass
    df = up.merge(fproba, on=["date","home_team","away_team"], how="left")

    # Odds: use columns already on UPCOMING (home_odds_dec*, etc.)
    if {"home_odds_dec","draw_odds_dec","away_odds_dec"}.issubset(df.columns):
        df["oddsH"], df["oddsD"], df["oddsA"] = df["home_odds_dec"], df["draw_odds_dec"], df["away_odds_dec"]
    else:
        for c in ["oddsH","oddsD","oddsA"]: df[c] = np.nan

    # Market implied probabilities (vig-stripped)
    m_probs = np.full((len(df),3), np.nan, dtype=float)
    if {"oddsH","oddsD","oddsA"}.issubset(df.columns):
        mH = df["oddsH"].map(implied); mD = df["oddsD"].map(implied); mA = df["oddsA"].map(implied)
        m_probs[:] = np.array([strip_vig(h,d,a) for h,d,a in zip(mH,mD,mA)], dtype=float)

    # Elo fallback from HIST (one rating table; HA switched per comp)
    R = {}
    if os.path.exists(HIST):
        try:
            hist = pd.read_csv(HIST)
            need = {"date","home_team","away_team","home_goals","away_goals"}
            if need.issubset(hist.columns) and not hist.empty:
                R = build_elo_ratings(hist)
        except:
            R = {}
    # HA: 60 for domestic; 50 for UEFA (better behavior for neutral/european contexts)
    ha_dom, ha_t = 60.0, 50.0
    comp = df.get("engine_comp_bucket", pd.Series(["Domestic"]*len(df)))
    elo_arr = np.array([
        elo_triplet(R.get(ht,1500.0), R.get(at,1500.0), ha=(ha_t if is_tournament_bucket(cb) else ha_dom))
        for ht,at,cb in zip(df["home_team"], df["away_team"], comp)
    ], dtype=float)

    # Model combo: feature probs if full, else Elo
    have_f = set(["fH","fD","fA"]).issubset(df.columns)
    f_ok = df[["fH","fD","fA"]].notna().all(axis=1).values if have_f else np.zeros(len(df), dtype=bool)
    feature_arr = df[["fH","fD","fA"]].fillna(0.0).values if have_f and f_ok.any() else np.zeros_like(elo_arr)
    model_combo = np.where(f_ok[:,None], feature_arr, elo_arr)

    # Load blends/calibrators
    blend_dom = load_json(BL_DOM, {"w_market_global": 0.85, "w_market_leagues": {}})
    blend_t   = load_json(BL_T,   {"w_market_global": blend_dom.get("w_market_global", 0.85)})

    cal_dom = load_calibrators(CAL_DOM, {"global":{"home":None,"draw":None,"away":None},"per_league":{}})
    cal_t   = load_calibrators(CAL_T,   {"global":{"home":None,"draw":None,"away":None}})

    w_dom_global = float(blend_dom.get("w_market_global", 0.85))
    w_dom_leagues= blend_dom.get("w_market_leagues", {}) or {}
    w_t_global   = float(blend_t.get("w_market_global", w_dom_global))

    # Blend + calibrate per row
    leagues = df["league"].astype(str).values
    pH_cal = np.zeros(len(df)); pD_cal = np.zeros(len(df)); pA_cal = np.zeros(len(df))
    for i in range(len(df)):
        is_t = is_tournament_bucket(comp.iloc[i] if "engine_comp_bucket" in df.columns else "Domestic")
        # choose weight
        if is_t:
            w = w_t_global
            cal_trip = cal_t.get("global", {"home":None,"draw":None,"away":None})
        else:
            w = float(w_dom_leagues.get(leagues[i], w_dom_global))
            cal_trip = cal_dom.get("per_league", {}).get(leagues[i],
                        cal_dom.get("global", {"home":None,"draw":None,"away":None}))
        m_vec = m_probs[i]; e_vec = model_combo[i]
        vec = e_vec if np.isnan(m_vec).any() else (w*m_vec + (1.0-w)*e_vec)
        s = vec.sum(); vec = (vec/s) if s>0 else e_vec
        ph, pd_, pa = apply_isotonic_triplet(cal_trip, vec[0], vec[1], vec[2])
        pH_cal[i], pD_cal[i], pA_cal[i] = ph, pd_, pa

    # Kelly (display only)
    kH = [kelly(h, o) for h,o in zip(pH_cal, df.get("oddsH", pd.Series(np.nan, index=df.index)))]
    kD = [kelly(d, o) for d,o in zip(pD_cal, df.get("oddsD", pd.Series(np.nan, index=df.index)))]
    kA = [kelly(a, o) for a,o in zip(pA_cal, df.get("oddsA", pd.Series(np.nan, index=df.index)))]

    out = pd.DataFrame({
        "fixture_id": df.get("fixture_id"),
        "date": df.get("date"),
        "league": df.get("league"),
        "home_team": df.get("home_team"),
        "away_team": df.get("away_team"),
        "pH": pH_cal, "pD": pD_cal, "pA": pA_cal,
        "oddsH": df.get("oddsH"), "oddsD": df.get("oddsD"), "oddsA": df.get("oddsA"),
        "kelly_H": kH, "kelly_D": kD, "kelly_A": kA
    })

    # normalize & sort
    s = out[["pH","pD","pA"]].sum(axis=1).replace(0, np.nan)
    out["pH"] = out["pH"]/s; out["pD"] = out["pD"]/s; out["pA"] = out["pA"]/s
    out["top_kelly"] = np.nanmax(np.vstack([
        out["kelly_H"].fillna(0.0).values,
        out["kelly_D"].fillna(0.0).values,
        out["kelly_A"].fillna(0.0).values]).T, axis=1)
    if "date" in out.columns:
        try:
            out["date"] = pd.to_datetime(out["date"], errors="coerce")
            out = out.sort_values(["date","top_kelly"], ascending=[True, False])
        except: pass

    out.to_csv(OUT_DATA, index=False)
    out.to_csv(OUT_RUN, index=False)
    print(f"[OK] wrote {OUT_DATA} and {OUT_RUN} rows={len(out)}")

if __name__ == "__main__":
    main()