#!/usr/bin/env python3
"""
feature_patcher.py — generate model-ready diffs & coherence features
Inputs:
  data/UPCOMING_7D_enriched.csv
Outputs:
  data/UPCOMING_7D_features.csv

What it does (best-effort, schema-agnostic):
- Diffs (home - away) for selected signals:
    spi_rank_diff, injury_index_diff, lineup_availability_diff,
    keeper_psxg_prevented_diff, passing_accuracy_diff
- Market features:
    market_dispersion (bookmaker_count), has_ou/has_btts/has_spread (from enrichment)
    btts_price_gap (abs difference if both present)
- Coherence features:
    ou_vs_1x2_expected_goals (proxy: OU total vs implied 1X2 goal proxy)
Notes:
- If a source column is missing, the feature becomes NaN.
"""

import os
import numpy as np
import pandas as pd

DATA = "data"
SRC = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
OUT = os.path.join(DATA, "UPCOMING_7D_features.csv")

def safe_read(path):
    if not os.path.exists(path): return pd.DataFrame()
    try: return pd.read_csv(path)
    except Exception: return pd.DataFrame()

def main():
    up = safe_read(SRC)
    if up.empty:
        pd.DataFrame(columns=["fixture_id"]).to_csv(OUT, index=False)
        print("feature_patcher: no enriched input; wrote header-only")
        return

    # Ensure fixture_id exists (stable key)
    if "fixture_id" not in up.columns:
        def mk_id(r):
            d = str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
            h = str(r.get("home_team","NA")).strip().lower().replace(" ","_")
            a = str(r.get("away_team","NA")).strip().lower().replace(" ","_")
            return f"{d}__{h}__vs__{a}"
        up["fixture_id"] = up.apply(mk_id, axis=1)

    feats = pd.DataFrame({"fixture_id": up["fixture_id"]})

    # --- SPI rank diff ---
    if {"home_spi_rank","away_spi_rank"}.issubset(up.columns):
        feats["spi_rank_diff"] = up["home_spi_rank"] - up["away_spi_rank"]

    # --- Injury/availability diffs ---
    if {"home_injury_index","away_injury_index"}.issubset(up.columns):
        feats["injury_index_diff"] = up["home_injury_index"] - up["away_injury_index"]
    if {"home_avail","away_avail"}.issubset(up.columns):
        feats["lineup_availability_diff"] = up["home_avail"] - up["away_avail"]

    # --- Keeper PSxG prevented diff (if present from FBref keeper slice) ---
    # Expect columns like home_<something_keeper_psxg_prevented>, away_<...>
    home_keeper_cols = [c for c in up.columns if c.startswith("home_") and "psxg" in c.lower() and "prevent" in c.lower()]
    away_keeper_cols = [c for c in up.columns if c.startswith("away_") and "psxg" in c.lower() and "prevent" in c.lower()]
    if home_keeper_cols and away_keeper_cols:
        hk = home_keeper_cols[0]; ak = away_keeper_cols[0]
        feats["keeper_psxg_prevented_diff"] = up[hk] - up[ak]

    # --- Passing accuracy diff (if present from FBref passing slice) ---
    # Expect columns like home_<...passing...> or home_pass_pct_*
    hpass = [c for c in up.columns if c.startswith("home_") and ("pass_pct" in c.lower() or "passing" in c.lower())]
    apass = [c for c in up.columns if c.startswith("away_") and ("pass_pct" in c.lower() or "passing" in c.lower())]
    if hpass and apass:
        feats["passing_accuracy_diff"] = up[hpass[0]] - up[apass[0]]

    # --- Market dispersion & flags (already in enrichment if odds added) ---
    if "bookmaker_count" in up.columns:
        feats["market_dispersion"] = up["bookmaker_count"]
    for flag in ["has_ou","has_btts","has_spread"]:
        if flag in up.columns:
            feats[flag] = up[flag]

    # --- BTTS gap (abs difference in prices) ---
    if {"btts_yes_price","btts_no_price"}.issubset(up.columns):
        try:
            feats["btts_price_gap"] = (up["btts_yes_price"].astype(float) - up["btts_no_price"].astype(float)).abs()
        except Exception:
            feats["btts_price_gap"] = np.nan

    # --- OU vs 1X2 expected goals (coherence proxy)
    # If you later compute a 1X2-based expected goals proxy, replace this placeholder.
    # For now, if we have ou_main_total, we simply pass it through as a feature.
    if "ou_main_total" in up.columns:
        feats["ou_main_total"] = up["ou_main_total"]

    # Basic context passthrough (optional but useful in model stage)
    for col in ["date","league","home_team","away_team"]:
        if col in up.columns:
            feats[col] = up[col]

    feats.to_csv(OUT, index=False)
    print(f"feature_patcher: wrote {OUT} rows={len(feats)}")

if __name__ == "__main__":
    main()