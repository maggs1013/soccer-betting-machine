#!/usr/bin/env python3
# Predict upcoming matches using:
#   • Feature model probabilities if available (feature_proba_upcoming.csv)
#   • Elo fallback built from HIST
#   • Market probabilities from odds (if present)
# Blend = per-league w_market if available (fallback to global), then per-league isotonic calibration if available.
# Kelly columns are included for convenience, but risk sizing/caps should still be handled downstream.
#
# Inputs (data/):
#   UPCOMING_7D_enriched.csv
#   feature_proba_upcoming.csv
#   HIST_matches.csv
#   model_blend.json
#   calibrator.pkl
#   odds_upcoming.csv
#
# Outputs:
#   data/PREDICTIONS_7D.csv
#   runs/YYYY-MM-DD/PREDICTIONS_7D.csv

import os
import json
import pickle
from typing import Tuple, Dict, Any
import numpy as np
import pandas as pd
from datetime import datetime

DATA = "data"
RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

UP   = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
FP   = os.path.join(DATA, "feature_proba_upcoming.csv")
HIST = os.path.join(DATA, "HIST_matches.csv")
BL   = os.path.join(DATA, "model_blend.json")
CAL  = os.path.join(DATA, "calibrator.pkl")
ODDS = os.path.join(DATA, "odds_upcoming.csv")

OUT_DATA = os.path.join(DATA, "PREDICTIONS_7D.csv")
OUT_RUN  = os.path.join(RUN_DIR, "PREDICTIONS_7D.csv")

EPS = 1e-9

def implied(dec: float) -> float:
    try:
        dec = float(dec)
        return 1.0 / dec if dec > 0 else np.nan
    except Exception:
        return np.nan

def strip_vig(ph: float, pd_: float, pa: float) -> Tuple[float,float,float]:
    s = ph + pd_ + pa
    if not np.isfinite(s) or s <= 0:
        return (np.nan, np.nan, np.nan)
    return (ph/s, pd_/s, pa/s)

def elo_triplet(Rh: float, Ra: float, ha: float = 60.0) -> Tuple[float,float,float]:
    pH_core = 1.0 / (1.0 + 10.0 ** (-((Rh - Ra + ha) / 400.0)))
    pD = 0.18 + 0.10 * np.exp(-abs(Rh - Ra) / 200.0)
    pH = (1.0 - pD) * pH_core
    pA = 1.0 - pH - pD
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
        h, a = getattr(r, "home_team"), getattr(r, "away_team")
        R.setdefault(h, 1500.0); R.setdefault(a, 1500.0)
        Rh, Ra = R[h], R[a]
        Eh = 1.0 / (1.0 + 10.0 ** (-((Rh - Ra + 60.0) / 400.0)))
        hg, ag = getattr(r, "home_goals", np.nan), getattr(r, "away_goals", np.nan)
        if pd.isna(hg) or pd.isna(ag):
            continue
        score = 1.0 if hg > ag else (0.5 if hg == ag else 0.0)
        K = 20.0
        R[h] = Rh + K * (score - Eh)
        R[a] = Ra + K * ((1.0 - score) - (1.0 - Eh))
    return R

def load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return default

def load_calibrators(path: str) -> Dict[str, Any]:
    try:
        with open(path, "rb") as f:
            obj = pickle.load(f)
            if isinstance(obj, dict): return obj
    except Exception:
        pass
    return {"global": {"home": None, "draw": None, "away": None}, "per_league": {}}

def apply_isotonic_triplet(cal_triplet: Dict[str, Any], pH: float, pD: float, pA: float) -> Tuple[float,float,float]:
    isoH, isoD, isoA = cal_triplet.get("home"), cal_triplet.get("draw"), cal_triplet.get("away")
    ph = float(isoH.transform([pH])[0]) if isoH is not None else pH
    pd_ = float(isoD.transform([pD])[0]) if isoD is not None else pD
    pa = float(isoA.transform([pA])[0]) if isoA is not None else pA
    s = ph + pd_ + pa
    if s <= 0:
        return (pH, pD, pA)
    return (ph/s, pd_/s, pa/s)

def kelly(p: float, dec: float, cap: float = 0.10) -> float:
    try:
        b = float(dec) - 1.0
        if b <= 0 or not np.isfinite(p): return 0.0
        q = 1.0 - p
        k = (b * p - q) / b
        if not np.isfinite(k): return 0.0
        return float(max(0.0, min(k, cap)))
    except Exception:
        return 0.0

def canonical_fixture_id(row: pd.Series) -> str:
    date = str(row.get("date", "NA")).replace("-", "")
    h = str(row.get("home_team", "NA")).strip().lower().replace(" ", "_")
    a = str(row.get("away_team", "NA")).strip().lower().replace(" ", "_")
    return f"{date}__{h}__vs__{a}"

def ensure_league(df: pd.DataFrame) -> pd.DataFrame:
    if "league" not in df.columns:
        df["league"] = "GLOBAL"
    df["league"] = df["league"].astype(str)
    return df

def standardize_odds_columns(od: pd.DataFrame) -> pd.DataFrame:
    if od.empty:
        return pd.DataFrame(columns=["fixture_id","oddsH","oddsD","oddsA","odds_over","total_line","num_books","bookmaker"])
    df = od.copy()
    rename = {
        "home_odds": "oddsH", "draw_odds": "oddsD", "away_odds": "oddsA",
        "home_odds_dec": "oddsH", "draw_odds_dec": "oddsD", "away_odds_dec": "oddsA",
        "over_odds": "odds_over", "total": "total_line", "total_odds": "odds_over"
    }
    for k, v in rename.items():
        if k in df.columns and v not in df.columns:
            df[v] = df[k]
    for c in ["oddsH","oddsD","oddsA","odds_over","total_line"]:
        if c not in df.columns:
            df[c] = np.nan
    if "bookmaker" not in df.columns:
        df["bookmaker"] = "unknown"
    if "num_books" not in df.columns:
        try:
            df["num_books"] = df.groupby("fixture_id")["bookmaker"].transform("nunique")
        except Exception:
            df["num_books"] = 1
    return df[["fixture_id","oddsH","oddsD","oddsA","odds_over","total_line","num_books","bookmaker"]].drop_duplicates("fixture_id")

def main():
    # 1) Upcoming
    if not os.path.exists(UP):
        header = ["fixture_id","date","league","home_team","away_team","pH","pD","pA","oddsH","oddsD","oddsA","kelly_H","kelly_D","kelly_A","top_kelly"]
        pd.DataFrame(columns=header).to_csv(OUT_DATA, index=False)
        pd.DataFrame(columns=header).to_csv(OUT_RUN, index=False)
        print("[WARN] UPCOMING_7D_enriched.csv missing; wrote header-only predictions.")
        return
    up = pd.read_csv(UP)
    if up.empty:
        header = ["fixture_id","date","league","home_team","away_team","pH","pD","pA","oddsH","oddsD","oddsA","kelly_H","kelly_D","kelly_A","top_kelly"]
        pd.DataFrame(columns=header).to_csv(OUT_DATA, index=False)
        pd.DataFrame(columns=header).to_csv(OUT_RUN, index=False)
        print("[WARN] UPCOMING_7D_enriched.csv empty; wrote header-only predictions.")
        return

    up = ensure_league(up)
    if "fixture_id" not in up.columns:
        up["fixture_id"] = up.apply(canonical_fixture_id, axis=1)

    # 2) Optional feature probabilities
    fproba = pd.DataFrame(columns=["date","home_team","away_team","fH","fD","fA"])
    if os.path.exists(FP):
        try:
            fproba = pd.read_csv(FP)
        except Exception:
            pass
    df = up.merge(fproba, on=["date","home_team","away_team"], how="left", suffixes=("",""))

    # 3) Optional odds
    if os.path.exists(ODDS):
        od = standardize_odds_columns(pd.read_csv(ODDS))
    else:
        od = pd.DataFrame(columns=["fixture_id","oddsH","oddsD","oddsA","odds_over","total_line","num_books","bookmaker"])
    df = df.merge(od, on="fixture_id", how="left")

    # 4) Market implied
    m_probs = np.full((len(df), 3), np.nan, dtype=float)
    if {"oddsH","oddsD","oddsA"}.issubset(df.columns):
        mH = df["oddsH"].map(implied); mD = df["oddsD"].map(implied); mA = df["oddsA"].map(implied)
        m_probs[:] = np.array([strip_vig(h,d,a) for h,d,a in zip(mH,mD,mA)], dtype=float)

    # 5) Elo
    elo_R = {}
    if os.path.exists(HIST):
        try:
            hist = pd.read_csv(HIST)
            need = {"date","home_team","away_team","home_goals","away_goals"}
            if need.issubset(hist.columns) and not hist.empty:
                elo_R = build_elo_ratings(hist)
        except Exception:
            elo_R = {}
    elo_arr = np.array([elo_triplet(elo_R.get(ht,1500.0), elo_R.get(at,1500.0))
                        for ht, at in zip(df["home_team"], df["away_team"])], dtype=float)

    # 6) Model combo
    have_f = set(["fH","fD","fA"]).issubset(df.columns)
    f_ok = df[["fH","fD","fA"]].notna().all(axis=1).values if have_f else np.zeros(len(df), dtype=bool)
    feature_arr = df[["fH","fD","fA"]].fillna(0.0).values if f_ok.any() else np.zeros_like(elo_arr)
    model_combo = np.where(f_ok[:, None], feature_arr, elo_arr)

    # 7) Weights + calibrators
    blend = load_json(BL, {"w_market_global": 0.85, "w_market_leagues": {}})
    w_global = float(blend.get("w_market_global", 0.85))
    w_league = blend.get("w_market_leagues", {}) or {}
    cals = load_calibrators(CAL)
    cal_global = cals.get("global", {"home": None, "draw": None, "away": None})
    cal_leagues = cals.get("per_league", {}) or {}

    leagues = df["league"].astype(str).values
    blended = np.zeros_like(model_combo)
    for i in range(len(df)):
        w = float(w_league.get(leagues[i], w_global))
        m_vec = m_probs[i]; e_vec = model_combo[i]
        vec = e_vec if np.isnan(m_vec).any() else (w*m_vec + (1.0-w)*e_vec)
        s = vec.sum()
        blended[i] = vec / s if s > 0 else e_vec

    # 9) Calibrate
    pH,pD,pA = blended[:,0], blended[:,1], blended[:,2]
    pH_cal = np.empty_like(pH); pD_cal = np.empty_like(pD); pA_cal = np.empty_like(pA)
    for i in range(len(df)):
        lg = leagues[i]
        cal_trip = cal_leagues.get(lg, cal_global)
        ph_i, pd_i, pa_i = apply_isotonic_triplet(cal_trip, pH[i], pD[i], pA[i])
        pH_cal[i], pD_cal[i], pA_cal[i] = ph_i, pd_i, pa_i

    # 10) Kelly (display only)
    kH = [kelly(h, d) for h, d in zip(pH_cal, df.get("oddsH", pd.Series(np.nan, index=df.index)))]
    kD = [kelly(d, d_od) for d, d_od in zip(pD_cal, df.get("oddsD", pd.Series(np.nan, index=df.index)))]
    kA = [kelly(a, d) for a, d in zip(pA_cal, df.get("oddsA", pd.Series(np.nan, index=df.index)))]

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

    for legacy, canon in [("home_odds_dec","oddsH"),("draw_odds_dec","oddsD"),("away_odds_dec","oddsA")]:
        if legacy not in out.columns: out[legacy] = out[canon]

    s = out[["pH","pD","pA"]].sum(axis=1).replace(0, np.nan)
    out["pH"] = out["pH"]/s; out["pD"] = out["pD"]/s; out["pA"] = out["pA"]/s

    out["top_kelly"] = np.nanmax(
        np.vstack([
            out["kelly_H"].fillna(0.0).values,
            out["kelly_D"].fillna(0.0).values,
            out["kelly_A"].fillna(0.0).values
        ]).T, axis=1
    )

    if "date" in out.columns:
        try:
            out["date"] = pd.to_datetime(out["date"], errors="coerce")
            out = out.sort_values(["date","top_kelly"], ascending=[True, False])
        except Exception:
            pass

    # WRITE to data/ and runs/
    out.to_csv(OUT_DATA, index=False)
    out.to_csv(OUT_RUN, index=False)
    print(f"[OK] wrote {OUT_DATA} and {OUT_RUN} rows={len(out)}")

if __name__ == "__main__":
    main()