#!/usr/bin/env python3
"""
enrich_features.py  —  robust enrichment pass

Purpose
-------
Add contextual enrichments (injuries/lineups, referee, stadium/crowd, travel)
to the upcoming fixtures table, without ever failing when inputs are missing.

Inputs (data/):
  UPCOMING_7D_enriched.csv      ← produced by 01_enrich_fixtures/build_upcoming_window
  lineups.csv                   ← optional, manual overrides (injuries/absences)
  referee_tendencies.csv        ← optional, per-ref penalties/cards
  stadium_crowd.csv             ← optional, crowd indices per home venue
  travel_matrix.csv             ← optional, precomputed distances (home vs away)

Outputs (data/):
  UPCOMING_7D_enriched.csv      ← same file, updated in-place with safe defaults
"""

import os
import numpy as np
import pandas as pd

DATA = "data"
UP_PATH   = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
LINEUPS   = os.path.join(DATA, "lineups.csv")
REFS      = os.path.join(DATA, "referee_tendencies.csv")
STADIUM   = os.path.join(DATA, "stadium_crowd.csv")
TRAVEL    = os.path.join(DATA, "travel_matrix.csv")

SAFE_DEFAULTS = {
    "home_injury_index": 0.30, "away_injury_index": 0.30,
    "ref_pen_rate": 0.30,
    "crowd_index": 0.70,
    "home_travel_km": 0.0, "away_travel_km": 200.0,
}

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

def ensure_cols(df, col_defaults: dict):
    for c, v in col_defaults.items():
        if c not in df.columns:
            df[c] = v
    return df

def main():
    if not os.path.exists(UP_PATH):
        print(f"[WARN] {UP_PATH} missing; writing header-only shell.")
        pd.DataFrame(columns=["date","league","fixture_id","home_team","away_team"]).to_csv(UP_PATH, index=False)
        return

    up = pd.read_csv(UP_PATH)
    if "league" not in up.columns:
        up["league"] = "GLOBAL"
    if "fixture_id" not in up.columns:
        # mirror 01_enrich_fixtures generation if needed
        up["fixture_id"] = up.apply(
            lambda r: f"{str(r.get('date','NA')).replace('-','')}__{str(r.get('home_team','NA')).strip().lower().replace(' ','_')}__vs__{str(r.get('away_team','NA')).strip().lower().replace(' ','_')}",
            axis=1
        )

    # Defaults so downstream never breaks
    up = ensure_cols(up, SAFE_DEFAULTS)

    # --- Lineups / injuries (optional) ---
    # Expect columns: fixture_id, home_injury_index, away_injury_index (0..1)
    ln = safe_read(LINEUPS, ["fixture_id","home_injury_index","away_injury_index"])
    if not ln.empty and "fixture_id" in ln.columns:
        ln = ln.drop_duplicates("fixture_id")
        up = up.merge(ln, on="fixture_id", how="left", suffixes=("", "_from_lineup"))
        for c in ["home_injury_index","away_injury_index"]:
            if f"{c}_from_lineup" in up.columns:
                up[c] = np.where(up[f"{c}_from_lineup"].notna(), up[f"{c}_from_lineup"], up[c])
                up.drop(columns=[f"{c}_from_lineup"], inplace=True, errors="ignore")

    # --- Referee tendencies (optional) ---
    # Expect columns: fixture_id or referee_name; we prioritize fixture_id
    rf = safe_read(REFS)
    if not rf.empty:
        if "fixture_id" in rf.columns:
            # direct join
            keep = ["fixture_id"]
            if "ref_pen_rate" in rf.columns: keep.append("ref_pen_rate")
            up = up.merge(rf[keep].drop_duplicates("fixture_id"), on="fixture_id", how="left", suffixes=("", "_rf"))
            if "ref_pen_rate_rf" in up.columns:
                up["ref_pen_rate"] = np.where(up["ref_pen_rate_rf"].notna(), up["ref_pen_rate_rf"], up["ref_pen_rate"])
                up.drop(columns=["ref_pen_rate_rf"], inplace=True, errors="ignore")

    # --- Stadium crowd (optional) ---
    # Expect columns: home_team, crowd_index (0..1) or venue + mapping
    sc = safe_read(STADIUM)
    if not sc.empty:
        # try to join on home_team
        if "home_team" in up.columns and "home_team" in sc.columns and "crowd_index" in sc.columns:
            sc_ht = sc[["home_team","crowd_index"]].drop_duplicates("home_team")
            up = up.merge(sc_ht, on="home_team", how="left", suffixes=("", "_st"))
            if "crowd_index_st" in up.columns:
                up["crowd_index"] = np.where(up["crowd_index_st"].notna(), up["crowd_index_st"], up["crowd_index"])
                up.drop(columns=["crowd_index_st"], inplace=True, errors="ignore")

    # --- Travel distances (optional) ---
    # Expect columns: home_team, away_team, travel_km_home, travel_km_away
    tv = safe_read(TRAVEL)
    if not tv.empty:
        cols = {c.lower(): c for c in tv.columns}
        hh = cols.get("home_team", "home_team") if "home_team" in cols or "home_team" in tv.columns else None
        aa = cols.get("away_team", "away_team") if "away_team" in cols or "away_team" in tv.columns else None
        th = cols.get("travel_km_home") or cols.get("home_travel_km") or None
        ta = cols.get("travel_km_away") or cols.get("away_travel_km") or None
        if hh and aa and th and ta:
            keep = [hh, aa, th, ta]
            t2 = tv[keep].drop_duplicates([hh, aa]).rename(columns={
                hh:"home_team", aa:"away_team", th:"home_travel_km", ta:"away_travel_km"
            })
            up = up.merge(t2, on=["home_team","away_team"], how="left", suffixes=("", "_tv"))
            for c in ["home_travel_km","away_travel_km"]:
                if f"{c}_tv" in up.columns:
                    up[c] = np.where(up[f"{c}_tv"].notna(), up[f"{c}_tv"], up[c])
                    up.drop(columns=[f"{c}_tv"], inplace=True, errors="ignore")

    # Final safe types / ordering
    if "date" in up.columns:
        try:
            up["date"] = pd.to_datetime(up["date"], errors="coerce")
            up = up.sort_values(["date","home_team","away_team"], na_position="last")
        except Exception:
            pass

    up.to_csv(UP_PATH, index=False)
    print(f"[OK] enrich_features: wrote {UP_PATH} rows={len(up)}")

if __name__ == "__main__":
    main()