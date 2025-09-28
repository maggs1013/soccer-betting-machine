#!/usr/bin/env python3
"""
fixtures_fallback_api_football.py — ensure UPCOMING_fixtures.csv has next 7d fixtures

Behavior:
- If data/UPCOMING_fixtures.csv has rows in [now, now+7d] UTC → do nothing.
- Else: try API-FOOTBALL v3 fixtures for league ids in API_FOOTBALL_LEAGUE_IDS (CSV),
        build standardized fixtures table and write data/UPCOMING_fixtures.csv.

Env:
  API_FOOTBALL_KEY          (required for fallback)
  API_FOOTBALL_LEAGUE_IDS   (CSV of league ids, e.g., "39,140,135")

Outputs:
  data/UPCOMING_fixtures.csv
  reports/FIXTURES_DEBUG.md  (updated by running fixtures_debug_probe.py separately)
"""

import os, sys, json, requests
import pandas as pd
from datetime import datetime, timezone, timedelta

DATA="data"
FIX=os.path.join(DATA,"UPCOMING_fixtures.csv")

def now_utc(): return datetime.now(timezone.utc)

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def ensure_fixture_id(df):
    if df.empty: return df
    if "fixture_id" in df.columns: return df
    if {"date","home_team","away_team"}.issubset(df.columns):
        def mk_id(r):
            d=str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
            h=str(r.get("home_team","NA")).strip().lower().replace(" ","_")
            a=str(r.get("away_team","NA")).strip().lower().replace(" ","_")
            return f"{d}__{h}__vs__{a}"
        df=df.copy()
        df["fixture_id"]=df.apply(mk_id, axis=1)
    return df

def has_window_rows(df, start, end):
    if "date" not in df.columns: return False
    dt = pd.to_datetime(df["date"], errors="coerce", utc=True)
    return bool((dt.notna() & (dt>=start) & (dt<=end)).any())

def fetch_api_football(leagues, key, start, end):
    # API docs: https://v3.football.api-sports.io/fixtures?league=39&season=2024&from=YYYY-MM-DD&to=YYYY-MM-DD
    headers={"x-apisports-key": key}
    rows=[]
    start_str = start.strftime("%Y-%m-%d")
    end_str   = end.strftime("%Y-%m-%d")
    season    = start.year  # naive guess for current season; adjust if you store it elsewhere

    for lid in leagues:
        try:
            resp = requests.get(
                "https://v3.football.api-sports.io/fixtures",
                headers=headers,
                params={"league": int(lid), "season": season, "from": start_str, "to": end_str},
                timeout=40
            )
            if resp.status_code != 200:
                continue
            for item in (resp.json() or {}).get("response", []):
                fix = item.get("fixture", {})
                tm  = item.get("teams", {})
                lg  = item.get("league", {})
                date_iso = fix.get("date")  # ISO
                home = (tm.get("home") or {}).get("name") or ""
                away = (tm.get("away") or {}).get("name") or ""
                league_name = lg.get("name") or f"league_{lid}"
                rows.append({
                    "date": date_iso,
                    "league": league_name,
                    "home_team": home,
                    "away_team": away
                })
        except Exception:
            continue
    df = pd.DataFrame(rows)
    if not df.empty:
        # Standardize to UTC ISO
        try:
            dt = pd.to_datetime(df["date"], errors="coerce", utc=True)
            df["date"] = dt.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            pass
    return df

def main():
    start = now_utc()
    end   = start + timedelta(days=7)

    fx = safe_read(FIX)
    if not fx.empty and has_window_rows(fx, start, end):
        print("fixtures_fallback_api_football: UPCOMING_fixtures.csv already has in-window rows; no action.")
        return

    key = os.environ.get("API_FOOTBALL_KEY","").strip()
    lid_csv = os.environ.get("API_FOOTBALL_LEAGUE_IDS","").strip()
    if not key or not lid_csv:
        print("fixtures_fallback_api_football: missing API_FOOTBALL_KEY or API_FOOTBALL_LEAGUE_IDS; cannot build fallback.")
        return

    leagues = [s.strip() for s in lid_csv.split(",") if s.strip()]
    df = fetch_api_football(leagues, key, start, end)
    if df.empty:
        print("fixtures_fallback_api_football: API-FOOTBALL returned no fixtures in window.")
        return

    df = ensure_fixture_id(df)
    df.to_csv(FIX, index=False)
    print(f"fixtures_fallback_api_football: wrote {FIX} rows={len(df)}")

if __name__ == "__main__":
    main()