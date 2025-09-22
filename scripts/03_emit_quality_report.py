#!/usr/bin/env python3
"""
03_emit_quality_report.py
Emit simple, robust data-quality diagnostics for today's run.

Inputs (data/):
  - UPCOMING_7D_enriched.csv
  - PREDICTIONS_7D.csv            (should include oddsH/oddsD/oddsA if available)
  - odds_upcoming.csv             (optional; helps compute num_books coverage)

Output (data/):
  - DATA_QUALITY_REPORT.csv       (key checks + counts)

Notes:
  • No CLI args required (consistent with the rest of the pipeline).
  • Never fails the job; if inputs are missing, it writes a minimal report.
"""

import os
import pandas as pd
import numpy as np

DATA = "data"
ENRICHED = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
PRED     = os.path.join(DATA, "PREDICTIONS_7D.csv")
ODDS     = os.path.join(DATA, "odds_upcoming.csv")
OUT      = os.path.join(DATA, "DATA_QUALITY_REPORT.csv")

def safe_read(path, cols=None):
    if not os.path.exists(path):
        return pd.DataFrame(columns=cols or [])
    try:
        df = pd.read_csv(path)
        if cols:
            for c in cols:
                if c not in df.columns:
                    df[c] = np.nan
        return df
    except Exception:
        return pd.DataFrame(columns=cols or [])

def main():
    enr = safe_read(ENRICHED)
    prd = safe_read(PRED)
    odd = safe_read(ODDS)

    checks = []

    # --- Enriched fixtures ---
    checks.append({"check":"enriched_rows", "value": int(len(enr))})
    checks.append({"check":"enriched_has_fixture_id", "value": int("fixture_id" in enr.columns)})
    checks.append({"check":"enriched_has_league",     "value": int("league" in enr.columns)})
    # minimal feature coverage (sample)
    for col in ["home_team","away_team","rest_days_home","rest_days_away","home_gk_rating","away_gk_rating"]:
        checks.append({"check":f"enriched_has_{col}", "value": int(col in enr.columns)})

    # --- Predictions ---
    checks.append({"check":"pred_rows", "value": int(len(prd))})
    for col in ["fixture_id","pH","pD","pA"]:
        checks.append({"check":f"pred_has_{col}", "value": int(col in prd.columns)})

    # odds coverage in predictions
    if {"oddsH","oddsD","oddsA"}.issubset(prd.columns):
        with_odds = prd[["oddsH","oddsD","oddsA"]].notna().all(axis=1).sum()
        checks.append({"check":"pred_with_all_three_odds", "value": int(with_odds)})
        checks.append({"check":"pred_with_all_three_odds_pct",
                       "value": float(100.0 * with_odds / max(1, len(prd)))})
    else:
        checks.append({"check":"pred_with_all_three_odds", "value": 0})
        checks.append({"check":"pred_with_all_three_odds_pct", "value": 0.0})

    # --- Odds file diagnostics (books / dispersion) ---
    if not odd.empty:
        checks.append({"check":"odds_rows", "value": int(len(odd))})
        if "num_books" not in odd.columns and "bookmaker" in odd.columns:
            # derive num_books per fixture if not present
            odd["num_books"] = odd.groupby("fixture_id")["bookmaker"].transform("nunique")
        if "num_books" in odd.columns:
            by_fix = odd.drop_duplicates("fixture_id")[["fixture_id","num_books"]]
            gte2 = (by_fix["num_books"] >= 2).sum()
            checks.append({"check":"fixtures_with_>=2_books", "value": int(gte2)})
            checks.append({"check":"fixtures_with_>=2_books_pct",
                           "value": float(100.0 * gte2 / max(1, len(by_fix)))})
        else:
            checks.append({"check":"fixtures_with_>=2_books", "value": 0})
            checks.append({"check":"fixtures_with_>=2_books_pct", "value": 0.0})
    else:
        checks.append({"check":"odds_rows", "value": 0})
        checks.append({"check":"fixtures_with_>=2_books", "value": 0})
        checks.append({"check":"fixtures_with_>=2_books_pct", "value": 0.0})

    # --- Joinability check (pred vs enriched by fixture_id) ---
    if "fixture_id" in prd.columns and "fixture_id" in enr.columns:
        joined = prd["fixture_id"].isin(enr["fixture_id"]).sum()
        checks.append({"check":"pred_fixture_ids_found_in_enriched", "value": int(joined)})
        checks.append({"check":"pred_fixture_ids_found_in_enriched_pct",
                       "value": float(100.0 * joined / max(1, len(prd)))})
    else:
        checks.append({"check":"pred_fixture_ids_found_in_enriched", "value": 0})
        checks.append({"check":"pred_fixture_ids_found_in_enriched_pct", "value": 0.0})

    # Persist
    pd.DataFrame(checks).to_csv(OUT, index=False)
    print(f"[OK] DATA_QUALITY_REPORT written: {OUT}")

if __name__ == "__main__":
    main()