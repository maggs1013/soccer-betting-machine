#!/usr/bin/env python3
"""
enrich_features.py — robust enrichment pass (REPLACEMENT)
- Safe with missing inputs; always writes a valid UPCOMING_7D_enriched.csv
- Adds odds (OU/BTTS/spreads) when present in fixtures
- Adds FBref multi-slice columns if present
- Adds SPI rank/extra fields if present
- Adds lineups/injury/availability from provider connector (not manual)
- Preserves referee, crowd, travel enrichments
"""

import os
import numpy as np
import pandas as pd

DATA = "data"
UP_PATH   = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
FIX_PATH  = os.path.join(DATA, "UPCOMING_fixtures.csv")
LINEUPS   = os.path.join(DATA, "lineups.csv")                 # produced by connectors/lineups_fetch.py
REFS      = os.path.join(DATA, "referee_tendencies.csv")      # optional
STADIUM   = os.path.join(DATA, "stadium_crowd.csv")           # optional
TRAVEL    = os.path.join(DATA, "travel_matrix.csv")           # optional
FBREF     = os.path.join(DATA, "sd_fbref_team_stats.csv")     # multi-slice merged file
SPI       = os.path.join(DATA, "sd_538_spi.csv")              # robust parser output

SAFE_DEFAULTS = {
    "home_injury_index": 0.30, "away_injury_index": 0.30,
    "ref_pen_rate": 0.30,
    "crowd_index": 0.70,
    "home_travel_km": 0.0, "away_travel_km": 200.0,
    # Odds-related defaults (H2H already in fixtures)
    "has_ou": 0, "has_btts": 0, "has_spread": 0,
}

# Flexible column guards for optional enrichments
ODDS_EXTRA_FIELDS = [
    # Add more as you begin storing them in UPCOMING_fixtures.csv
    "ou_main_total", "ou_over_price", "ou_under_price",
    "btts_yes_price", "btts_no_price",
    "spread_home_line", "spread_home_price", "spread_away_line", "spread_away_price",
    "bookmaker_count", "has_opening_odds", "has_closing_odds"
]

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

def normalize_fixture_id(row):
    d = str(row.get("date","NA")).replace("-","").replace("T","_").replace(":","")
    h = str(row.get("home_team","NA")).strip().lower().replace(" ","_")
    a = str(row.get("away_team","NA")).strip().lower().replace(" ","_")
    return f"{d}__{h}__vs__{a}"

def main():
    # Start from the last enriched file if present; otherwise begin from fixtures
    up = safe_read(UP_PATH)
    fx = safe_read(FIX_PATH)
    if fx.empty and up.empty:
        # write minimal shell so pipeline never breaks
        pd.DataFrame(columns=["date","league","fixture_id","home_team","away_team"]).to_csv(UP_PATH, index=False)
        print("[WARN] No fixtures/enriched input; wrote header-only")
        return

    # If no enriched table yet, clone fixtures skeleton
    if up.empty and not fx.empty:
        up = fx.copy()
    # Ensure keys exist
    if "league" not in up.columns:
        up["league"] = "GLOBAL"
    if "fixture_id" not in up.columns:
        up["fixture_id"] = up.apply(normalize_fixture_id, axis=1)

    # Safe defaults
    up = ensure_cols(up, SAFE_DEFAULTS)

    # --- Odds: bring over extra markets if already in fixtures ---
    if not fx.empty and "fixture_id" in fx.columns:
        # mark has_* flags when those columns are present
        cols = ["fixture_id"] + [c for c in ODDS_EXTRA_FIELDS if c in fx.columns]
        tmp = fx[cols].copy() if len(cols) > 1 else pd.DataFrame()
        if not tmp.empty:
            up = up.merge(tmp, on="fixture_id", how="left", suffixes=("", "_od"))
            # set has_* based on presence of values
            if "ou_over_price" in up.columns or "ou_under_price" in up.columns:
                up["has_ou"] = 1
            if "btts_yes_price" in up.columns or "btts_no_price" in up.columns:
                up["has_btts"] = 1
            if "spread_home_line" in up.columns or "spread_away_line" in up.columns:
                up["has_spread"] = 1

    # --- Lineups / injuries / availability (provider) ---
    ln = safe_read(LINEUPS, ["fixture_id","home_injury_index","away_injury_index","home_avail","away_avail"])
    if not ln.empty and "fixture_id" in ln.columns:
        ln = ln.drop_duplicates("fixture_id")
        up = up.merge(ln, on="fixture_id", how="left", suffixes=("", "_ln"))
        for c in ["home_injury_index","away_injury_index","home_avail","away_avail"]:
            if f"{c}_ln" in up.columns:
                up[c] = np.where(up[f"{c}_ln"].notna(), up[f"{c}_ln"], up.get(c, np.nan))
                up.drop(columns=[f"{c}_ln"], inplace=True, errors="ignore")

    # --- SPI (rank/conf intervals if present) ---
    spi = safe_read(SPI)
    if not spi.empty:
        if {"team","rank"}.issubset(set(spi.columns)):
            rank_map = dict(zip(spi["team"].astype(str), spi["rank"]))
            up["home_spi_rank"] = up["home_team"].astype(str).map(rank_map)
            up["away_spi_rank"] = up["away_team"].astype(str).map(rank_map)

    # --- FBref multi-slice (schema-agnostic) ---
    fb = safe_read(FBREF)
    if not fb.empty and {"team","league","season"}.issubset(fb.columns):
        # Prepare reduced set to avoid explosion; include all non-key cols
        key_cols = {"team","league","season"}
        other_cols = [c for c in fb.columns if c not in key_cols]
        fb_red = fb[list(key_cols) + other_cols].copy()

        # Home join
        fb_home = fb_red.copy()
        fb_home.columns = [f"home_{c}" if c not in ("team","league","season") else c for c in fb_home.columns]
        up = up.merge(fb_home, left_on=["home_team","league"], right_on=["team","league"], how="left")
        up.drop(columns=["team","season"], errors="ignore", inplace=True)

        # Away join
        fb_away = fb_red.copy()
        fb_away.columns = [f"away_{c}" if c not in ("team","league","season") else c for c in fb_away.columns]
        up = up.merge(fb_away, left_on=["away_team","league"], right_on=["team","league"], how="left")
        up.drop(columns=["team","season"], errors="ignore", inplace=True)

    # --- Referee tendencies (optional) ---
    rf = safe_read(REFS)
    if not rf.empty:
        if "fixture_id" in rf.columns:
            keep = ["fixture_id"]
            if "ref_pen_rate" in rf.columns: keep.append("ref_pen_rate")
            up = up.merge(rf[keep].drop_duplicates("fixture_id"), on="fixture_id", how="left", suffixes=("", "_rf"))
            if "ref_pen_rate_rf" in up.columns:
                up["ref_pen_rate"] = np.where(up["ref_pen_rate_rf"].notna(), up["ref_pen_rate_rf"], up["ref_pen_rate"])
                up.drop(columns=["ref_pen_rate_rf"], inplace=True, errors="ignore")

    # --- Stadium crowd (optional) ---
    sc = safe_read(STADIUM)
    if not sc.empty and {"home_team","crowd_index"}.issubset(sc.columns) and "home_team" in up.columns:
        sc_ht = sc[["home_team","crowd_index"]].drop_duplicates("home_team")
        up = up.merge(sc_ht, on="home_team", how="left", suffixes=("", "_st"))
        if "crowd_index_st" in up.columns:
            up["crowd_index"] = np.where(up["crowd_index_st"].notna(), up["crowd_index_st"], up["crowd_index"])
            up.drop(columns=["crowd_index_st"], inplace=True, errors="ignore")

    # --- Travel distances (optional) ---
    tv = safe_read(TRAVEL)
    if not tv.empty:
        cols = {c.lower(): c for c in tv.columns}
        hh = cols.get("home_team") or "home_team"
        aa = cols.get("away_team") or "away_team"
        th = cols.get("travel_km_home") or cols.get("home_travel_km")
        ta = cols.get("travel_km_away") or cols.get("away_travel_km")
        if all([hh in tv.columns, aa in tv.columns, th in tv.columns, ta in tv.columns]):
            t2 = tv[[hh, aa, th, ta]].drop_duplicates([hh, aa]).rename(columns={
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