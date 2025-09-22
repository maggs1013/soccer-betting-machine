#!/usr/bin/env python3
"""
Schema check for critical artifacts before bundling.
Fails fast if required files are missing or columns are wrong.
"""

import os, sys
import pandas as pd
from datetime import datetime

RUN_DATE = datetime.utcnow().strftime("%Y-%m-%d")
RUN_DIR = os.path.join("runs", RUN_DATE)
DATA_DIR = "data"

# What to check: {filename: required_columns}
CHECKS = {
    "PREDICTIONS_7D.csv": ["fixture_id","pH","pD","pA","oddsH","oddsD","oddsA"],
    "PREDICTIONS_BTTS_7D.csv": ["fixture_id","p_btts_yes"],
    "PREDICTIONS_TOTALS_7D.csv": ["fixture_id","p_over","p_under"],
    "CONSISTENCY_CHECKS.csv": ["fixture_id","league"],
    "ACTIONABILITY_REPORT.csv": ["fixture_id","league","stake"],
    "COUNCIL_BRIEFING.md": None,  # content-only
    "CALIBRATION_SUMMARY.csv": ["metric","value"],  # or ece_weighted depending on script
    "ROI_BY_SLICE.csv": None,
    "ODDS_COVERAGE_REPORT.csv": None,
    "DATA_QUALITY_REPORT.csv": None,
    "ANTI_MODEL_VETOES.csv": ["slice","reason"],
    "FEATURE_IMPORTANCE.csv": None,
    "FEATURE_DRIFT.csv": None,
    "LINE_MOVE_LOG.csv": None,
    "EXECUTION_FEASIBILITY.csv": ["fixture_id","league","feasible"],
    "PER_LEAGUE_BLEND_WEIGHTS.csv": ["league","w_market"]
}

def check_csv(path, req_cols):
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    if req_cols is None:
        return
    df = pd.read_csv(path, nrows=1)
    missing = [c for c in req_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{path} missing required columns {missing}")

def main():
    errors = []
    for fname, cols in CHECKS.items():
        # look in RUN dir first, then data/ as fallback
        path_run = os.path.join(RUN_DIR, fname)
        path_data = os.path.join(DATA_DIR, fname)
        path = path_run if os.path.exists(path_run) else path_data
        try:
            check_csv(path, cols)
        except Exception as e:
            errors.append(str(e))
    if errors:
        print("❌ Schema check failed:")
        for e in errors:
            print(" -", e)
        sys.exit(1)
    print("✅ Schema check passed")

if __name__ == "__main__":
    main()