#!/usr/bin/env python3
# Identify systematic losing slices and mark vetoes (league × odds bucket).
# Reads: data/HIST_matches.csv, data/model_blend.json
# Writes:
#   data/ANTI_MODEL_VETOES.csv        (slice, reason)           ← minimal for schema gate
#   data/VETO_HISTORY.csv             (league, bucket, n, roi…) ← detailed for humans
#   runs/YYYY-MM-DD copies of both

import os, json, numpy as np, pandas as pd
from datetime import datetime

DATA = "data"
RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

HIST = os.path.join(DATA, "HIST_matches.csv")
BLND = os.path.join(DATA, "model_blend.json")

VETO_MIN_DATA  = os.path.join(DATA, "ANTI_MODEL_VETOES.csv")
VETO_MIN_RUN   = os.path.join(RUN_DIR, "ANTI_MODEL_VETOES.csv")
VETO_FULL_DATA = os.path.join(DATA, "VETO_HISTORY.csv")
VETO_FULL_RUN  = os.path.join(RUN_DIR, "VETO_HISTORY.csv")

def implied(x):
    try: x = float(x); return 1.0/x if x > 0 else np.nan
    except: return np.nan

def strip_vig(h, d, a):
    s = h + d + a
    if not np.isfinite(s) or s <= 0: return (np.nan, np.nan, np.nan)
    return (h/s, d/s, a/s)

def elo_prob(Rh, Ra, ha=60.0):
    pH_core = 1.0 / (1.0 + 10.0 ** (-((Rh - Ra + ha)/400.0)))
    pD = 0.18 + 0.10 * np.exp(-abs(Rh - Ra)/200.0)
    pH = (1.0 - pD) * pH_core
    pA = 1.0 - pH - pD
    eps = 1e-6
    return (max(eps, min(1-eps, pH)), max(eps, min(1-eps, pD)), max(eps, min(1-eps, pA)))

def build_elo(df):
    R = {}
    for r in df.itertuples(index=False):
        h, a = r.home_team, r.away_team
        R.setdefault(h,1500.0); R.setdefault(a,1500.0)
        Eh = 1.0/(1.0 + 10.0 ** (-((R[h]-R[a]+60)/400.0)))
        if pd.isna(r.home_goals) or pd.isna(r.away_goals): continue
        score = 1.0 if r.home_goals > r.away_goals else (0.5 if r.home_goals == r.away_goals else 0.0)
        K = 20
        R[h] = R[h] + K*(score - Eh); R[a] = R[a] + K*((1.0 - score) - (1.0 - Eh))
    return R

def kelly(p, odds, cap=0.05):
    try:
        b = float(odds) - 1.0
        if b <= 0 or not np.isfinite(p): return 0.0
        q = 1.0 - p
        k = (b*p - q) / b
        return float(max(0.0, min(k, cap)))
    except: return 0.0

def write_empty():
    pd.DataFrame(columns=["slice","reason"]).to_csv(VETO_MIN_DATA, index=False)
    pd.DataFrame(columns=["league","odds_bucket","n_bets","roi","veto"]).to_csv(VETO_FULL_DATA, index=False)
    pd.read_csv(VETO_MIN_DATA).to_csv(VETO_MIN_RUN, index=False)
    pd.read_csv(VETO_FULL_DATA).to_csv(VETO_FULL_RUN, index=False)

def main():
    if not os.path.exists(HIST):
        write_empty(); print("[WARN] HIST missing; wrote empty veto files."); return
    df = pd.read_csv(HIST)
    need = {"home_odds_dec","draw_odds_dec","away_odds_dec","home_goals","away_goals"}
    if df.empty or not need.issubset(df.columns):
        write_empty(); print("[WARN] HIST lacks fields; wrote empty veto files."); return

    if "league" not in df.columns: df["league"] = "GLOBAL"
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")

    w_global, w_leagues = 0.85, {}
    if os.path.exists(BLND):
        try:
            mb = json.load(open(BLND, "r"))
            w_global = float(mb.get("w_market_global", 0.85))
            w_leagues = mb.get("w_market_leagues", {}) or {}
        except Exception:
            pass

    mH = df["home_odds_dec"].map(implied); mD = df["draw_odds_dec"].map(implied); mA = df["away_odds_dec"].map(implied)
    m = np.array([strip_vig(h, d, a) for h, d, a in zip(mH, mD, mA)], dtype=float)

    R = {}; e = []
    for r in df.itertuples(index=False):
        h, a = r.home_team, r.away_team
        R.setdefault(h,1500.0); R.setdefault(a,1500.0)
        e.append(elo_prob(R[h], R[a]))
        Eh = 1.0/(1.0 + 10.0 ** (-((R[h]-R[a]+60)/400.0)))
        if pd.isna(r.home_goals) or pd.isna(r.away_goals): continue
        score = 1.0 if r.home_goals > r.away_goals else (0.5 if r.home_goals == r.away_goals else 0.0)
        K = 20
        R[h] = R[h] + K*(score - Eh); R[a] = R[a] + K*((1.0 - score) - (1.0 - Eh))
    e = np.array(e, dtype=float)

    bins = [0,1.8,2.2,3.0,5.0,10.0,999]
    labels = ["<=1.8","(1.8,2.2]","(2.2,3.0]","(3.0,5.0]","(5.0,10.0]","10+"]

    rows = []
    simple_rows = []
    for lg, dL in df.groupby("league"):
        w = w_leagues.get(lg, w_global)
        idx = dL.index.values
        blend = w*m[idx] + (1.0 - w)*e[idx]
        s = blend.sum(axis=1, keepdims=True); s[s==0] = 1.0; blend = blend/s
        impl = m[idx]

        sides = np.argmax(blend - impl, axis=1)
        stakes, pnls, chosen_odds = [], [], []
        for i, row in enumerate(dL.itertuples(index=False)):
            if sides[i] == 0:
                p, odds = blend[i,0], row.home_odds_dec
                res = 1 if row.home_goals > row.away_goals else 0
            elif sides[i] == 1:
                p, odds = blend[i,1], row.draw_odds_dec
                res = 1 if row.home_goals == row.away_goals else 0
            else:
                p, odds = blend[i,2], row.away_odds_dec
                res = 1 if row.home_goals < row.away_goals else 0

            if not np.isfinite(odds) or p <= 0:
                stakes.append(0.0); pnls.append(0.0); chosen_odds.append(np.nan); continue
            k = kelly(p,odds,cap=0.05)
            stakes.append(k); chosen_odds.append(odds)
            pnls.append((odds-1.0)*k if res==1 else -k)

        d2 = dL.copy()
        d2["stake"] = stakes; d2["pnl"] = pnls; d2["chosen_odds"] = chosen_odds
        d2["odds_bucket"] = pd.cut(d2["chosen_odds"], bins=bins, labels=labels, include_lowest=True)

        for bk, g in d2.groupby("odds_bucket"):
            n = int((g["stake"] > 0).sum())
            if n < 30:
                rows.append({"league": lg, "odds_bucket": str(bk), "n_bets": n, "roi": 0.0, "veto": "N/A"})
                continue
            turnover = float(g["stake"].sum())
            roi = float(g["pnl"].sum())/turnover if turnover > 0 else 0.0
            veto = "Y" if roi < -0.02 else "N"
            rows.append({"league": lg, "odds_bucket": str(bk), "n_bets": n, "roi": roi, "veto": veto})
            if veto == "Y":
                simple_rows.append({
                    "slice": f"{lg} :: odds_bucket={bk}",
                    "reason": f"ROI {roi:.3f} < -0.02 on {n} bets"
                })

    pd.DataFrame(rows).to_csv(VETO_FULL_DATA, index=False)
    pd.DataFrame(simple_rows if simple_rows else [], columns=["slice","reason"]).to_csv(VETO_MIN_DATA, index=False)
    pd.read_csv(VETO_FULL_DATA).to_csv(VETO_FULL_RUN, index=False)
    pd.read_csv(VETO_MIN_DATA).to_csv(VETO_MIN_RUN, index=False)
    print(f"[OK] wrote {VETO_FULL_DATA} and {VETO_MIN_DATA}")

if __name__ == "__main__":
    main()