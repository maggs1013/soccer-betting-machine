#!/usr/bin/env python3
"""
model_accuracy_report.py
Accuracy vs market/Elo across slices. UEFA is reported separately from Big5/Other.
"""

import os, json
import numpy as np
import pandas as pd
from datetime import datetime

DATA = "data"
RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

HIST = os.path.join(DATA, "HIST_matches.csv")
BLND = os.path.join(DATA, "model_blend.json")

BIG5_TOKENS = ["premier league","la liga","bundesliga","serie a","ligue 1","english premier"]
UEFA_TOKENS = ["champions league","europa league","conference league","uefa champions","uefa europa","uefa europa conference","ucl","uel"]

def in_tokens(s, toks):
    if not isinstance(s,str): return False
    ls = s.lower()
    return any(tok in ls for tok in toks)

def tier_of(league):
    if in_tokens(league, UEFA_TOKENS): return "UEFA"
    if in_tokens(league, BIG5_TOKENS): return "Big5"
    return "Other"

ODDS_BUCKETS = [(0,1.50), (1.50,2.00), (2.00,3.00), (3.00,10.00), (10.00,999.0)]
ODDS_BUCKET_LABELS = ["<=1.50","(1.50,2.00]","(2.00,3.00]","(3.00,10.00]","10+"]

def implied(dec):
    try: d=float(dec); return 1.0/d if d>0 else np.nan
    except: return np.nan

def normalize_probs(p):
    s = p.sum(axis=1, keepdims=True); s[s<=0]=1.0
    return p/s

def elo_triplet(Rh,Ra,ha=60.0):
    pH_core = 1.0/(1.0+10.0**(-((Rh-Ra+ha)/400.0)))
    pD = 0.18 + 0.10*np.exp(-abs(Rh-Ra)/200.0)
    pH = (1.0-pD)*pH_core; pA = 1.0 - pH - pD
    eps=1e-6
    return (max(eps,min(1-eps,pH)), max(eps,min(1-eps,pD)), max(eps,min(1-eps,pA)))

def logloss_row(y_idx, p_vec):
    eps=1e-12; return -np.log(max(eps, p_vec[y_idx]))

def brier_row(y_idx, p_vec):
    target=[0.0,0.0,0.0]; target[y_idx]=1.0
    return float(np.mean([(pi-ti)**2 for pi,ti in zip(p_vec,target)]))

def eval_block(P_slice, y_slice):
    mask = np.isfinite(P_slice).all(axis=1)
    pred = np.full(len(P_slice), -1)
    pred[mask] = np.argmax(P_slice[mask], axis=1)
    hits = np.where(mask, (pred == y_slice).astype(float), np.nan)
    ll   = np.full(len(P_slice), np.nan)
    br   = np.full(len(P_slice), np.nan)
    vidx = np.where(mask)[0]
    for i in vidx:
        ll[i] = logloss_row(int(y_slice[i]), P_slice[i])
        br[i] = brier_row(int(y_slice[i]), P_slice[i])
    return hits, ll, br

def main():
    if not os.path.exists(HIST):
        for f in ["MODEL_ACCURACY_SUMMARY.csv","MODEL_ACCURACY_BY_WEEK.csv",
                  "MODEL_ACCURACY_BY_ODDS.csv","MODEL_ACCURACY_BY_BETTYPE.csv",
                  "MODEL_ACCURACY_BY_LEAGUE_TIER.csv"]:
            pd.DataFrame().to_csv(os.path.join(RUN_DIR,f), index=False)
        print("[WARN] HIST missing; wrote empty accuracy reports."); return

    df = pd.read_csv(HIST)
    need = {"date","home_team","away_team","home_goals","away_goals"}
    if not need.issubset(df.columns) or df.empty:
        for f in ["MODEL_ACCURACY_SUMMARY.csv","MODEL_ACCURACY_BY_WEEK.csv",
                  "MODEL_ACCURACY_BY_ODDS.csv","MODEL_ACCURACY_BY_BETTYPE.csv",
                  "MODEL_ACCURACY_BY_LEAGUE_TIER.csv"]:
            pd.DataFrame().to_csv(os.path.join(RUN_DIR,f), index=False)
        print("[WARN] HIST lacks required columns; wrote empty accuracy reports."); return

    if "league" not in df.columns: df["league"]="GLOBAL"
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    y = np.where(df["home_goals"]>df["away_goals"],0,
         np.where(df["home_goals"]==df["away_goals"],1,2))

    # Probs: market / elo / blend
    have_odds = {"home_odds_dec","draw_odds_dec","away_odds_dec"}.issubset(df.columns)
    if have_odds:
        mH=df["home_odds_dec"].map(implied); mD=df["draw_odds_dec"].map(implied); mA=df["away_odds_dec"].map(implied)
        m_probs = normalize_probs(np.vstack([mH,mD,mA]).T)
    else:
        m_probs = np.full((len(df),3), np.nan)

    # Elo
    R={}; e_rows=[]
    for r in df.itertuples(index=False):
        h,a=r.home_team, r.away_team
        R.setdefault(h,1500.0); R.setdefault(a,1500.0)
        e_rows.append(elo_triplet(R[h],R[a]))
        Eh = 1.0/(1.0+10.0**(-((R[h]-R[a]+60.0)/400.0)))
        if pd.isna(r.home_goals) or pd.isna(r.away_goals): continue
        score = 1.0 if r.home_goals>r.away_goals else (0.5 if r.home_goals==r.away_goals else 0.0)
        K=20.0
        R[h]=R[h]+K*(score-Eh); R[a]=R[a]+K*((1.0-score)-(1.0-Eh))
    e_probs = np.array(e_rows, dtype=float)

    # Blend
    w_global, w_leagues = 0.85, {}
    if os.path.exists(BLND):
        try:
            mb=json.load(open(BLND,"r"))
            w_global=float(mb.get("w_market_global", w_global))
            w_leagues=mb.get("w_market_leagues", {}) or {}
        except Exception:
            pass

    leagues = df["league"].astype(str).values
    blendP = np.zeros_like(e_probs)
    for i in range(len(df)):
        w = float(w_leagues.get(leagues[i], w_global))
        if have_odds and np.isfinite(m_probs[i]).all():
            v = w*m_probs[i] + (1.0-w)*e_probs[i]
            s = v.sum(); v = v/s if s>0 else e_probs[i]
        else:
            v = e_probs[i]
        blendP[i] = v

    # 1) Summary by league + global
    rows=[]
    for name, P in [("market",m_probs),("elo",e_probs),("blend",blendP)]:
        for lg, grp in df.groupby("league").groups.items():
            sl = np.array(list(grp), dtype=int)
            h,ll,br = eval_block(P[sl], y[sl])
            rows.append({"league": lg, "model": name,
                         "n": int(np.isfinite(P[sl]).all(axis=1).sum()),
                         "hit_rate": float(np.nanmean(h)),
                         "logloss": float(np.nanmean(ll)),
                         "brier": float(np.nanmean(br))})
        h,ll,br = eval_block(P, y)
        rows.append({"league": "GLOBAL", "model": name,
                     "n": int(np.isfinite(P).all(axis=1).sum()),
                     "hit_rate": float(np.nanmean(h)),
                     "logloss": float(np.nanmean(ll)),
                     "brier": float(np.nanmean(br))})
    pd.DataFrame(rows).to_csv(os.path.join(RUN_DIR,"MODEL_ACCURACY_SUMMARY.csv"), index=False)

    # 2) By week
    df["iso_year"] = df["date"].dt.isocalendar().year
    df["iso_week"] = df["date"].dt.isocalendar().week
    rows=[]
    for (yr,wk), idxs in df.groupby(["iso_year","iso_week"]).groups.items():
        sl = np.array(list(idxs), dtype=int)
        for name,P in [("market",m_probs[sl]),("elo",e_probs[sl]),("blend",blendP[sl])]:
            h,ll,br = eval_block(P, y[sl])
            rows.append({"iso_year": int(yr), "iso_week": int(wk), "model": name,
                         "n": int(np.isfinite(P).all(axis=1).sum()),
                         "hit_rate": float(np.nanmean(h)),
                         "logloss": float(np.nanmean(ll)),
                         "brier": float(np.nanmean(br))})
    pd.DataFrame(rows).to_csv(os.path.join(RUN_DIR,"MODEL_ACCURACY_BY_WEEK.csv"), index=False)

    # 3) By odds bucket
    rows=[]
    if have_odds:
        fav = np.nanmin(np.vstack([df["home_odds_dec"],df["draw_odds_dec"],df["away_odds_dec"]]).T, axis=1)
        cats = pd.cut(fav, bins=[b[0] for b in ODDS_BUCKETS]+[ODDS_BUCKETS[-1][1]],
                      labels=ODDS_BUCKET_LABELS, include_lowest=True)
        cats = cats.astype(str).values
        for lab in ODDS_BUCKET_LABELS:
            mask = (cats==lab)
            if mask.sum()==0:
                for name in ("market","elo","blend"):
                    rows.append({"odds_bucket": lab, "model": name, "n": 0,
                                 "hit_rate": np.nan, "logloss": np.nan, "brier": np.nan})
                continue
            for name,P in [("market",m_probs[mask]),("elo",e_probs[mask]),("blend",blendP[mask])]:
                h,ll,br = eval_block(P, y[mask])
                rows.append({"odds_bucket": lab, "model": name,
                             "n": int(np.isfinite(P).all(axis=1).sum()),
                             "hit_rate": float(np.nanmean(h)),
                             "logloss": float(np.nanmean(ll)),
                             "brier": float(np.nanmean(br))})
    pd.DataFrame(rows).to_csv(os.path.join(RUN_DIR,"MODEL_ACCURACY_BY_ODDS.csv"), index=False)

    # 4) By bet type (1X2 reported; BTTS/Totals baselines)
    rows=[]
    h,ll,br = eval_block(blendP, y)
    rows.append({"bet_type":"1X2","model":"blend","n":int(np.isfinite(blendP).all(axis=1).sum()),
                 "hit_rate": float(np.nanmean(h)), "logloss": float(np.nanmean(ll)), "brier": float(np.nanmean(br))})
    btts_y = ((df["home_goals"]>0) & (df["away_goals"]>0)).astype(int).values
    rows.append({"bet_type":"BTTS","model":"label-baseline","n":int(len(btts_y)),
                 "hit_rate": float(np.mean(btts_y)), "logloss": np.nan, "brier": np.nan})
    tot_y = (df["home_goals"] + df["away_goals"] > 2.5).astype(int).values
    rows.append({"bet_type":"Totals_2.5","model":"label-baseline","n":int(len(tot_y)),
                 "hit_rate": float(np.mean(tot_y)), "logloss": np.nan, "brier": np.nan})
    pd.DataFrame(rows).to_csv(os.path.join(RUN_DIR,"MODEL_ACCURACY_BY_BETTYPE.csv"), index=False)

    # 5) By league tier (UEFA separate)
    tiers = df["league"].apply(tier_of).values
    rows=[]
    for tier in ("Big5","UEFA","Other"):
        mask = (tiers==tier)
        if mask.sum()==0:
            for name in ("market","elo","blend"):
                rows.append({"league_tier": tier, "model": name, "n": 0,
                             "hit_rate": np.nan, "logloss": np.nan, "brier": np.nan})
            continue
        for name,P in [("market",m_probs[mask]),("elo",e_probs[mask]),("blend",blendP[mask])]:
            h,ll,br = eval_block(P, y[mask])
            rows.append({"league_tier": tier, "model": name,
                         "n": int(np.isfinite(P).all(axis=1).sum()),
                         "hit_rate": float(np.nanmean(h)),
                         "logloss": float(np.nanmean(ll)),
                         "brier": float(np.nanmean(br))})
    pd.DataFrame(rows).to_csv(os.path.join(RUN_DIR,"MODEL_ACCURACY_BY_LEAGUE_TIER.csv"), index=False)

    print("[OK] Accuracy reports written (UEFA separated).")

if __name__ == "__main__":
    main()