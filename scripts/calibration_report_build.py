#!/usr/bin/env python3
"""
calibration_report_build.py — per-league calibration by market
Inputs (best-effort; uses whatever exists):
  data/PREDICTIONS_7D.csv           (if includes 'actual' or 'outcome', calibrates current window)
  data/BACKTEST_BY_WEEK.csv         (if present: league, market, prob, actual)
  data/BACKTEST_SUMMARY.csv         (optional)
Outputs:
  reports/CALIBRATION_REPORT.csv    (league, market, prob_bin, n, avg_prob, hit_rate, ece_bin, ece_weighted)
Notes:
- If no actuals found, writes a header-only report and exits.
- Markets expected: '1X2', 'BTTS', 'OU' (flexible; use 'market' column if present).
"""

import os
import numpy as np
import pandas as pd

DATA="data"
REP="reports"
os.makedirs(REP, exist_ok=True)

P1 = os.path.join(DATA,"PREDICTIONS_7D.csv")
B1 = os.path.join(DATA,"BACKTEST_BY_WEEK.csv")
OUT= os.path.join(REP,"CALIBRATION_REPORT.csv")

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def bin_calib(df, league_col="league", market_col="market", prob_col="prob", actual_col="actual", bins=10):
    if df.empty or not {league_col, prob_col, actual_col}.issubset(df.columns):
        return pd.DataFrame()
    df = df.dropna(subset=[prob_col, actual_col])
    df["prob_bin"] = pd.cut(df[prob_col], bins=np.linspace(0,1,bins+1), include_lowest=True)
    grp = df.groupby([league_col, market_col, "prob_bin"], dropna=False, observed=True)
    agg = grp.agg(n=("actual","count"),
                  avg_prob=(prob_col,"mean"),
                  hit_rate=(actual_col,"mean")).reset_index()
    agg["ece_bin"] = (agg["avg_prob"] - agg["hit_rate"]).abs()
    # Weighted ECE per league/market
    totals = agg.groupby([league_col, market_col])["n"].transform("sum").replace(0, np.nan)
    agg["ece_weighted"] = (agg["ece_bin"] * agg["n"] / totals)
    return agg

def main():
    rows = []
    # Try predictions with actuals
    pred = safe_read(P1)
    if not pred.empty:
        # heuristics for columns
        league = "league" if "league" in pred.columns else None
        market = "market" if "market" in pred.columns else "1X2"
        actual = "actual" if "actual" in pred.columns else ("outcome" if "outcome" in pred.columns else None)
        prob   = "prob"   if "prob"   in pred.columns else None
        if league and actual and prob:
            tmp = pred.copy()
            if market != "market":
                tmp["market"] = market
            rows.append(bin_calib(tmp, league_col=league, market_col="market", prob_col=prob, actual_col=actual))
    # Try backtest
    bt = safe_read(B1)
    if not bt.empty and {"league","market","prob","actual"}.issubset(bt.columns):
        rows.append(bin_calib(bt, league_col="league", market_col="market", prob_col="prob", actual_col="actual"))

    if rows:
        rep = pd.concat(rows, axis=0, ignore_index=True)
    else:
        rep = pd.DataFrame(columns=["league","market","prob_bin","n","avg_prob","hit_rate","ece_bin","ece_weighted"])

    rep.to_csv(OUT, index=False)
    print(f"calibration_report_build: wrote {OUT} rows={len(rep)}")

if __name__ == "__main__":
    main()