#!/usr/bin/env python3
"""
make_run_index.py
Outputs a one-screen JSON index of run health.

Adds:
  - blend_vs_market_logloss_delta_last8w (negative is good)
"""

import os, json
import pandas as pd
import numpy as np
from datetime import datetime

RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

def safe_csv(name):
    p = os.path.join(RUN_DIR, name)
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except: return pd.DataFrame()

def main():
    idx = {"timestamp_utc": datetime.utcnow().isoformat(timespec="seconds")}

    pred = safe_csv("PREDICTIONS_7D.csv")
    idx["n_fixtures"] = int(pred["fixture_id"].nunique()) if "fixture_id" in pred.columns else int(len(pred))

    act = safe_csv("ACTIONABILITY_REPORT.csv")
    idx["n_edges"] = int((act["stake"] > 0).sum()) if "stake" in act.columns else 0

    feas = safe_csv("EXECUTION_FEASIBILITY.csv")
    if not feas.empty and "feasible" in feas.columns:
        base = feas.drop_duplicates("fixture_id")
        idx["feasibility_pct"] = float(100.0 * base["feasible"].fillna(0).astype(int).mean())
    else:
        idx["feasibility_pct"] = None

    cal = safe_csv("CALIBRATION_SUMMARY.csv")
    if not cal.empty:
        if {"metric","value"}.issubset(cal.columns):
            try:
                idx["ece_weighted"] = float(cal.loc[cal["metric"].str.contains("ECE", na=False),"value"].astype(float).mean())
            except: idx["ece_weighted"] = None
        elif "ece_weighted" in cal.columns:
            try: idx["ece_weighted"] = float(cal["ece_weighted"].astype(float).iloc[0])
            except: idx["ece_weighted"] = None
    else:
        idx["ece_weighted"] = None

    cons = safe_csv("CONSISTENCY_CHECKS.csv")
    if not cons.empty:
        flags = 0
        if "flag_goals_vs_totals" in cons.columns:
            flags += int(cons["flag_goals_vs_totals"].fillna(0).astype(int).sum())
        if "flag_over_vs_btts" in cons.columns:
            flags += int(cons["flag_over_vs_btts"].fillna(0).astype(int).sum())
        idx["consistency_flags"] = int(flags)
    else:
        idx["consistency_flags"] = 0

    veto = safe_csv("ANTI_MODEL_VETOES.csv")
    idx["veto_slices"] = int(len(veto)) if not veto.empty else 0

    cov = safe_csv("ODDS_COVERAGE_REPORT.csv")
    if not cov.empty:
        try:
            base = cov.drop_duplicates("fixture_id")
            n = int(base["fixture_id"].nunique())
            idx["coverage_hint"] = f"{n} fixtures with odds rows"
        except:
            idx["coverage_hint"] = "report present"
    else:
        idx["coverage_hint"] = "no coverage report"

    # New KPI: blend vs market logloss delta over last 8 ISO weeks
    accw = safe_csv("MODEL_ACCURACY_BY_WEEK.csv")
    kpi = None
    if not accw.empty and {"iso_year","iso_week","model","logloss","n"}.issubset(accw.columns):
        try:
            accw["key"] = accw["iso_year"].astype(str) + "-" + accw["iso_week"].astype(str).str.zfill(2)
            keys = sorted(accw["key"].unique())[-8:]
            a8 = accw[accw["key"].isin(keys)]
            def wavg(df):
                df = df.dropna(subset=["logloss"])
                return np.average(df["logloss"], weights=df["n"]) if (len(df) and df["n"].sum()>0) else np.nan
            bl = wavg(a8[a8["model"]=="blend"])
            mk = wavg(a8[a8["model"]=="market"])
            if np.isfinite(bl) and np.isfinite(mk):
                kpi = float(bl - mk)  # negative is good
        except: kpi = None
    idx["blend_vs_market_logloss_delta_last8w"] = kpi

    with open(os.path.join(RUN_DIR, "_INDEX.json"), "w") as f:
        json.dump(idx, f, indent=2)
    print("[OK] _INDEX.json written with KPI.")

if __name__ == "__main__":
    main()