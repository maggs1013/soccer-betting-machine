#!/usr/bin/env python3
"""
Schema check for critical artifacts before bundling.
Fails fast if required files are missing or mandatory columns are absent.
Allows reasonable extra columns that don't break downstream.
"""

import os, sys
import pandas as pd
from datetime import datetime

RUN_DATE = datetime.utcnow().strftime("%Y-%m-%d")
RUN_DIR = os.path.join("runs", RUN_DATE)
DATA_DIR = "data"

# Accept either style for calibration summary
CAL_OK_SETS = [
    {"metric","value"},
    {"ece_weighted"}
]

# What to check: {filename: required_columns}
# Only require the *critical* subset; extras are OK
CHECKS = {
    "PREDICTIONS_7D.csv": ["fixture_id","pH","pD","pA","oddsH","oddsD","oddsA"],
    "PREDICTIONS_BTTS_7D.csv": ["fixture_id","p_btts_yes"],
    "PREDICTIONS_TOTALS_7D.csv": ["fixture_id","p_over","p_under"],
    "CONSISTENCY_CHECKS.csv": ["fixture_id","league"],
    "ACTIONABILITY_REPORT.csv": ["fixture_id","league","stake"],
    "COUNCIL_BRIEFING.md": None,                 # presence-only
    "ROI_BY_SLICE.csv": None,
    "ODDS_COVERAGE_REPORT.csv": None,
    "DATA_QUALITY_REPORT.csv": None,
    "ANTI_MODEL_VETOES.csv": ["slice","reason"], # minimal gate
    "FEATURE_IMPORTANCE.csv": None,
    "FEATURE_DRIFT.csv": None,
    "LINE_MOVE_LOG.csv": None,
    "EXECUTION_FEASIBILITY.csv": ["fixture_id","league","feasible"],
    "PER_LEAGUE_BLEND_WEIGHTS.csv": ["league","w_market"], # allow extras
}

def first_path(name: str) -> str:
    path_run = os.path.join(RUN_DIR, name)
    path_data = os.path.join(DATA_DIR, name)
    return path_run if os.path.exists(path_run) else path_data

def check_csv(path: str, req_cols):
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    if req_cols is None:
        return
    df = pd.read_csv(path, nrows=1)
    have = set(df.columns)
    missing = [c for c in req_cols if c not in have]
    if missing:
        raise ValueError(f"{path} missing required columns {missing}")
    # No error if there are extra columns

def main():
    errors = []

    # Standard checks
    for fname, cols in CHECKS.items():
        try:
            check_csv(first_path(fname), cols)
        except Exception as e:
            errors.append(str(e))

    # Special check for CALIBRATION_SUMMARY.csv
    cal_path = first_path("CALIBRATION_SUMMARY.csv")
    if not os.path.exists(cal_path):
        errors.append(f"{cal_path} not found")
    else:
        try:
            df = pd.read_csv(cal_path, nrows=1)
            have = set(df.columns)
            if not any(ok.issubset(have) for ok in CAL_OK_SETS):
                errors.append(f"{cal_path} has unexpected columns {sorted(have)} (expected one of {CAL_OK_SETS})")
        except Exception as e:
            errors.append(f"{cal_path} unreadable: {e}")

    if errors:
        print("❌ Schema check failed:")
        for e in errors:
            print(" -", e)
        sys.exit(1)
    print("✅ Schema check passed")

if __name__ == "__main__":
    main()