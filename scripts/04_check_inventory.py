#!/usr/bin/env python3
"""
04_check_inventory.py
Lightweight inventory & schema audit for today's pipeline.

Inputs (data/):
  - HIST_matches.csv
  - UPCOMING_7D_enriched.csv
  - PREDICTIONS_7D.csv
  - odds_upcoming.csv (optional)
  - Any other files you consider "must-exist" for the day

Output (data/):
  - DATA_INVENTORY_REPORT.csv   (rows of {kind,path,status,detail})

Notes:
  • No CLI args required.
  • Never raises; always writes a report (so the workflow keeps moving).
  • Focuses on presence + header sanity, not content correctness.
"""

import os
import pandas as pd
import numpy as np

DATA = "data"
OUT  = os.path.join(DATA, "DATA_INVENTORY_REPORT.csv")

# Presence checks (file must exist)
REQUIRED_FILES = [
    "HIST_matches.csv",
    "UPCOMING_7D_enriched.csv",
    "PREDICTIONS_7D.csv",
]

# Optional-but-useful files (report as WARN if missing)
OPTIONAL_FILES = [
    "odds_upcoming.csv",
    "feature_proba_upcoming.csv",
    "team_form_features.csv",
    "xg_metrics_hybrid.csv",
    "team_statsbomb_features.csv",
    "sd_538_spi.csv",
]

# Minimal schema expectations for a few key files (skip if file missing)
SCHEMAS = {
    "HIST_matches.csv": {"date","home_team","away_team","home_goals","away_goals"},
    "UPCOMING_7D_enriched.csv": {"fixture_id","league","home_team","away_team"},
    "PREDICTIONS_7D.csv": {"fixture_id","pH","pD","pA"},
    # odds file can vary, but if present we prefer these canonical names
    "odds_upcoming.csv": {"fixture_id"},  # oddsH/oddsD/oddsA are mapped later if absent
}

def check_file(path, must_exist=True):
    """Return (status, detail) without raising."""
    if not os.path.exists(path):
        return ("ERROR" if must_exist else "WARN", "missing")
    try:
        # Try to read at least headers
        _ = pd.read_csv(path, nrows=1)
        return ("OK", "readable")
    except Exception as e:
        return ("ERROR", f"unreadable: {e}")

def check_schema(path, required_cols):
    if not os.path.exists(path):
        return ("WARN", "file_missing_for_schema_check")
    try:
        df = pd.read_csv(path, nrows=1)
        have = set(df.columns)
        missing = [c for c in required_cols if c not in have]
        if missing:
            return ("WARN", f"missing_columns:{missing}")
        return ("OK", "schema_ok")
    except Exception as e:
        return ("ERROR", f"schema_unreadable:{e}")

def main():
    rows = []

    # Required
    for fn in REQUIRED_FILES:
        path = os.path.join(DATA, fn)
        status, detail = check_file(path, must_exist=True)
        rows.append({"kind":"required","path":path,"status":status,"detail":detail})

    # Optional
    for fn in OPTIONAL_FILES:
        path = os.path.join(DATA, fn)
        status, detail = check_file(path, must_exist=False)
        rows.append({"kind":"optional","path":path,"status":status,"detail":detail})

    # Schema sanity
    for fn, req in SCHEMAS.items():
        path = os.path.join(DATA, fn)
        status, detail = check_schema(path, req)
        rows.append({"kind":"schema","path":path,"status":status,"detail":detail})

    # Joinability sanity: predictions ↔ enriched by fixture_id
    pred_p = os.path.join(DATA, "PREDICTIONS_7D.csv")
    enr_p  = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
    if os.path.exists(pred_p) and os.path.exists(enr_p):
        try:
            pr = pd.read_csv(pred_p, usecols=["fixture_id"])
            en = pd.read_csv(enr_p,  usecols=["fixture_id"])
            inter = pr["fixture_id"].isin(en["fixture_id"]).sum()
            rows.append({"kind":"joinability","path":"pred↔enriched","status":"OK",
                         "detail":f"pred_fixture_ids_found={inter}/{len(pr)}"})
        except Exception as e:
            rows.append({"kind":"joinability","path":"pred↔enriched","status":"WARN",
                         "detail":f"check_failed:{e}"})
    else:
        rows.append({"kind":"joinability","path":"pred↔enriched","status":"WARN","detail":"one_or_both_files_missing"})

    # Odds presence for Kelly (optional)
    odds_p = os.path.join(DATA, "odds_upcoming.csv")
    if os.path.exists(odds_p):
        try:
            od = pd.read_csv(odds_p, nrows=5)
            cols = set(od.columns)
            wants = {"fixture_id"}  # oddsH/oddsD/oddsA may be added later; not required here
            miss = [c for c in wants if c not in cols]
            if miss:
                rows.append({"kind":"schema","path":odds_p,"status":"WARN","detail":f"missing_columns:{miss}"})
            else:
                rows.append({"kind":"schema","path":odds_p,"status":"OK","detail":"min_columns_ok"})
        except Exception as e:
            rows.append({"kind":"schema","path":odds_p,"status":"WARN","detail":f"read_fail:{e}"})

    # Persist
    rep = pd.DataFrame(rows, columns=["kind","path","status","detail"])
    rep.to_csv(OUT, index=False)
    print(f"[OK] DATA_INVENTORY_REPORT written: {OUT} rows={len(rep)}")

if __name__ == "__main__":
    main()