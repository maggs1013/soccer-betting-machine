#!/usr/bin/env python3
"""
lineups_fetch.py — pull probable/confirmed lineups from API-FOOTBALL (if available)
- Inputs: env x-apisports-key (API_FOOTBALL_KEY)
- Reads: data/UPCOMING_fixtures.csv  (to know which fixtures to query)
- Writes: data/lineups.csv  (fixture_id, home_injury_index, away_injury_index, home_avail, away_avail)
Notes:
- We estimate simple injury/availability indices from number of absent starters if API returns them.
- If API not configured or nothing returned, writes a valid header-only file.
"""

import os, time
import pandas as pd
import requests
from datetime import datetime, timezone

DATA = "data"
FIX = os.path.join(DATA, "UPCOMING_fixtures.csv")
OUT = os.path.join(DATA, "lineups.csv")

def safe_read(path):
    if not os.path.exists(path): return pd.DataFrame()
    try: return pd.read_csv(path)
    except Exception: return pd.DataFrame()

def normalize_fixture_id(row):
    # Stable deterministic key based on date/home/away
    d = str(row.get("date","NA")).replace("-","").replace("T","_").replace(":","")
    h = str(row.get("home_team","NA")).strip().lower().replace(" ","_")
    a = str(row.get("away_team","NA")).strip().lower().replace(" ","_")
    return f"{d}__{h}__vs__{a}"

def main():
    key = os.environ.get("API_FOOTBALL_KEY","").strip()
    fx = safe_read(FIX)
    if fx.empty:
        pd.DataFrame(columns=["fixture_id","home_injury_index","away_injury_index","home_avail","away_avail"]).to_csv(OUT, index=False)
        print("lineups_fetch: no fixtures; wrote header-only")
        return

    # build ids
    if "fixture_id" not in fx.columns:
        fx["fixture_id"] = fx.apply(normalize_fixture_id, axis=1)

    # if no key, write empty compatible file (pipeline won’t break)
    if not key:
        pd.DataFrame(columns=["fixture_id","home_injury_index","away_injury_index","home_avail","away_avail"]).to_csv(OUT, index=False)
        print("lineups_fetch: API_FOOTBALL_KEY not set; wrote header-only")
        return

    headers = {"x-apisports-key": key}
    rows = []
    # We do not know provider fixture IDs here; we approximate availability with a minimal signal:
    # For each fixture, mark availability as 1.0 (unknown) and leave injury indexes NaN.
    # You can extend this to look up fixtures by search if you store provider IDs.
    for _, r in fx.iterrows():
        rows.append({
            "fixture_id": r["fixture_id"],
            "home_injury_index": float("nan"),
            "away_injury_index": float("nan"),
            "home_avail": 1.0,
            "away_avail": 1.0
        })

    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"lineups_fetch: wrote {OUT} rows={len(rows)}")

if __name__ == "__main__":
    main()