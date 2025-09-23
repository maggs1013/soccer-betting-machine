#!/usr/bin/env python3
"""
make_run_index.py
Build a one-screen JSON index of run health so the Council can triage fast.

Output:
  runs/YYYY-MM-DD/_INDEX.json
Keys:
  - n_fixtures
  - n_edges (stake>0)
  - feasibility_pct (fixtures feasible)
  - ece_weighted (from CALIBRATION_SUMMARY; tolerant schema)
  - consistency_flags (count of flagged fixtures)
  - veto_slices (count of rows in ANTI_MODEL_VETOES.csv)
  - coverage_hint (from ODDS_COVERAGE_REPORT if present)
  - timestamp_utc
"""

import os, json
import pandas as pd
import numpy as np
from datetime import datetime

RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

def safe_read(path):
    if not os.path.exists(path): return pd.DataFrame()
    try: return pd.read_csv(path)
    except Exception: return pd.DataFrame()

def main():
    idx = {"timestamp_utc": datetime.utcnow().isoformat(timespec="seconds")}

    pred = safe_read(os.path.join(RUN_DIR, "PREDICTIONS_7D.csv"))
    idx["n_fixtures"] = int(pred["fixture_id"].nunique()) if "fixture_id" in pred.columns else int(len(pred))

    act = safe_read(os.path.join(RUN_DIR, "ACTIONABILITY_REPORT.csv"))
    if not act.empty and "stake" in act.columns:
        idx["n_edges"] = int((act["stake"] > 0).sum())
    else:
        idx["n_edges"] = 0

    feas = safe_read(os.path.join(RUN_DIR, "EXECUTION_FEASIBILITY.csv"))
    if not feas.empty and "feasible" in feas.columns:
        base = feas.drop_duplicates("fixture_id")
        idx["feasibility_pct"] = float(100.0 * base["feasible"].fillna(0).astype(int).mean())
    else:
        idx["feasibility_pct"] = None

    cal = safe_read(os.path.join(RUN_DIR, "CALIBRATION_SUMMARY.csv"))
    ece = None
    if not cal.empty:
        # tolerate either metric/value or ece_weighted
        if {"metric","value"}.issubset(cal.columns):
            # try to find ECE home metric row or mean
            try:
                ece = float(cal.loc[cal["metric"].str.contains("ECE", na=False), "value"].astype(float).mean())
            except Exception:
                ece = float(cal["value"].astype(float).mean())
        elif "ece_weighted" in cal.columns:
            try:
                ece = float(cal["ece_weighted"].astype(float).iloc[0])
            except Exception:
                ece = None
    idx["ece_weighted"] = ece

    cons = safe_read(os.path.join(RUN_DIR, "CONSISTENCY_CHECKS.csv"))
    if not cons.empty:
        flags = 0
        if "flag_goals_vs_totals" in cons.columns:
            flags += int(cons["flag_goals_vs_totals"].fillna(0).astype(int).sum())
        if "flag_over_vs_btts" in cons.columns:
            flags += int(cons["flag_over_vs_btts"].fillna(0).astype(int).sum())
        idx["consistency_flags"] = int(flags)
    else:
        idx["consistency_flags"] = 0

    veto = safe_read(os.path.join(RUN_DIR, "ANTI_MODEL_VETOES.csv"))
    idx["veto_slices"] = int(len(veto)) if not veto.empty else 0

    cov = safe_read(os.path.join(RUN_DIR, "ODDS_COVERAGE_REPORT.csv"))
    if not cov.empty:
        # very light hint: % fixtures with odds present
        try:
            base = cov.drop_duplicates("fixture_id")
            n = int(base["fixture_id"].nunique())
            idx["coverage_hint"] = f"{n} fixtures with odds rows"
        except Exception:
            idx["coverage_hint"] = "report present"
    else:
        idx["coverage_hint"] = "no coverage report"

    out_path = os.path.join(RUN_DIR, "_INDEX.json")
    with open(out_path, "w") as f:
        json.dump(idx, f, indent=2)
    print("[OK] _INDEX.json written:", out_path)

if __name__ == "__main__":
    main()