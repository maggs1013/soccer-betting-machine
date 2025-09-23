#!/usr/bin/env python3
"""
add_odds_move_deltas.py
Derive odds move deltas and implied-probability shifts (AM vs T-60) for fixtures.

Prefers raw snapshots if available:
  data/odds_snapshot_morning.csv
  data/odds_snapshot_tminus60.csv
Fallbacks to RUN_DIR/LINE_MOVE_LOG.csv if snapshots are missing.

Outputs:
  - runs/YYYY-MM-DD/ODDS_MOVE_FEATURES.csv
  - data/UPCOMING_7D_enriched.csv merged with:
      engine_home_odds_move, engine_away_odds_move, engine_over_odds_move
      (approximate deltas; for Council risk overlay, not used in model training)
"""

import os
import numpy as np
import pandas as pd
from datetime import datetime

DATA = "data"
RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

AM   = os.path.join(DATA, "odds_snapshot_morning.csv")
TM60 = os.path.join(DATA, "odds_snapshot_tminus60.csv")
LINE = os.path.join(RUN_DIR, "LINE_MOVE_LOG.csv")

UP   = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
OUTF = os.path.join(RUN_DIR, "ODDS_MOVE_FEATURES.csv")

def safe_read(path, cols=None):
    if not os.path.exists(path):
        return pd.DataFrame(columns=cols or [])
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=cols or [])
    if cols:
        for c in cols:
            if c not in df.columns: df[c] = np.nan
    return df

def implied(dec):
    try:
        d = float(dec)
        return 1.0/d if d > 0 else np.nan
    except Exception:
        return np.nan

def main():
    up = safe_read(UP)
    if up.empty:
        # still produce an empty features file so downstream won't crash
        pd.DataFrame(columns=[
            "fixture_id","league",
            "delta_oddsH","delta_oddsA","delta_total",
            "delta_implied_home","delta_implied_away","delta_implied_over"
        ]).to_csv(OUTF, index=False)
        print(f"[WARN] {UP} missing/empty; wrote empty ODDS_MOVE_FEATURES.csv")
        return

    # Try snapshots first
    am = safe_read(AM)
    tm = safe_read(TM60)
    features = pd.DataFrame()

    if not am.empty and not tm.empty:
        # Expect columns: fixture_id, league, oddsH, oddsA, odds_over, total_line (flexible)
        cols = ["fixture_id","league","oddsH","oddsA","odds_over","total_line"]
        for df in (am, tm):
            for c in cols:
                if c not in df.columns:
                    df[c] = np.nan
        m = am.merge(tm, on=["fixture_id","league"], suffixes=("_am","_tm"), how="inner")
        if m.empty:
            features = pd.DataFrame()
        else:
            # deltas in decimal odds
            m["delta_oddsH"] = m["oddsH_tm"] - m["oddsH_am"]
            m["delta_oddsA"] = m["oddsA_tm"] - m["oddsA_am"]
            m["delta_total"] = m["odds_over_tm"] - m["odds_over_am"]
            # implied deltas
            m["delta_implied_home"] = m.apply(lambda r: implied(r["oddsH_tm"]) - implied(r["oddsH_am"]), axis=1)
            m["delta_implied_away"] = m.apply(lambda r: implied(r["oddsA_tm"]) - implied(r["oddsA_am"]), axis=1)
            m["delta_implied_over"] = m.apply(lambda r: implied(r["odds_over_tm"]) - implied(r["odds_over_am"]), axis=1)

            features = m[[
                "fixture_id","league","delta_oddsH","delta_oddsA","delta_total",
                "delta_implied_home","delta_implied_away","delta_implied_over"
            ]]
    else:
        # Fallback to LINE_MOVE_LOG deltas (no implied because we lack both snapshots)
        line = safe_read(LINE, ["fixture_id","league","delta_oddsH","delta_oddsA","delta_total"])
        features = line.copy()

        # add approx implied change using current odds if available in UPCOMING
        # Δ(1/odds) ≈ -(Δodds) / odds^2
        if not features.empty:
            # try to fetch current odds from UPCOMING (if present)
            for col_src, col_delta, col_out in [
                ("home_odds_dec","delta_oddsH","delta_implied_home"),
                ("away_odds_dec","delta_oddsA","delta_implied_away"),
                ("over_odds","delta_total","delta_implied_over")
            ]:
                if col_src in up.columns and col_delta in features.columns:
                    m = features.merge(up[["fixture_id", col_src]], on="fixture_id", how="left")
                    features[col_out] = - m[col_delta] / (m[col_src]**2)
                else:
                    features[col_out] = np.nan

    # Save features
    if features is None or features.empty:
        features = pd.DataFrame(columns=[
            "fixture_id","league",
            "delta_oddsH","delta_oddsA","delta_total",
            "delta_implied_home","delta_implied_away","delta_implied_over"
        ])
    features.to_csv(OUTF, index=False)
    print(f"[OK] ODDS_MOVE_FEATURES.csv written → {OUTF} rows={len(features)}")

    # Merge a light subset into UPCOMING for Council risk overlay
    if "fixture_id" in up.columns:
        merged = up.merge(features[["fixture_id","delta_implied_home","delta_implied_away","delta_implied_over"]],
                          on="fixture_id", how="left")
        merged.rename(columns={
            "delta_implied_home":"engine_home_odds_move",
            "delta_implied_away":"engine_away_odds_move",
            "delta_implied_over":"engine_over_odds_move"
        }, inplace=True)
        merged.to_csv(UP, index=False)
        print(f"[OK] odds move overlay merged into {UP}")

if __name__ == "__main__":
    main()