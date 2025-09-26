#!/usr/bin/env python3
"""
augment_fixtures_with_apifootball_ids.py
- Reads data/UPCOMING_fixtures.csv (from odds)
- Looks up likely API-FOOTBALL fixture IDs by date & team names
- Appends: api_football_fixture_id, api_football_league_id
Notes:
- Requires env: API_FOOTBALL_KEY
- Uses a fuzzy name match (SequenceMatcher) with optional team_name_map.csv aliases
- Safe no-op if key missing or network fails
"""

import os, requests, time, difflib, re
import pandas as pd
from datetime import datetime

DATA = "data"
FIX = os.path.join(DATA, "UPCOMING_fixtures.csv")
TEAM_MAP = os.path.join(DATA, "team_name_map.csv")  # optional with columns: source_name,target_name
OUT = FIX  # in-place augment

def safe_read(path):
    if not os.path.exists(path): return pd.DataFrame()
    try: return pd.read_csv(path)
    except Exception: return pd.DataFrame()

def norm(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s

def best_match(name, candidates):
    if not name or not candidates: return None, 0.0
    ratios = [(c, difflib.SequenceMatcher(None, name, c).ratio()) for c in candidates]
    ratios.sort(key=lambda x: x[1], reverse=True)
    return ratios[0]

def pull_day_fixtures(key: str, iso_date: str):
    """GET /fixtures?date=YYYY-MM-DD"""
    url = "https://v3.football.api-sports.io/fixtures"
    h = {"x-apisports-key": key}
    r = requests.get(url, headers=h, params={"date": iso_date}, timeout=30)
    if r.status_code != 200: return []
    return (r.json() or {}).get("response") or []

def main():
    key = os.environ.get("API_FOOTBALL_KEY","").strip()
    fx = safe_read(FIX)
    if fx.empty:
        print("augment: no fixtures; nothing to do")
        return

    # alias map (optional)
    alias = {}
    amap = safe_read(TEAM_MAP)
    if not amap.empty and {"source_name","target_name"}.issubset(amap.columns):
        for _, r in amap.iterrows():
            alias[norm(r["source_name"])] = norm(r["target_name"])

    # ensure fixture_id exists
    if "fixture_id" not in fx.columns:
        def mk_id(r):
            d = str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
            h = norm(r.get("home_team","NA")).replace(" ", "_")
            a = norm(r.get("away_team","NA")).replace(" ", "_")
            return f"{d}__{h}__vs__{a}"
        fx["fixture_id"] = fx.apply(mk_id, axis=1)

    if not key:
        print("augment: API_FOOTBALL_KEY not set; writing passthrough")
        fx.to_csv(OUT, index=False); return

    # group queries by calendar day to minimize calls
    fx["date_only"] = fx["date"].astype(str).str.slice(0,10)
    days = sorted(fx["date_only"].dropna().unique())
    day_cache = {}  # date -> list(fixtures)

    for d in days:
        try:
            day_cache[d] = pull_day_fixtures(key, d)
            time.sleep(0.4)  # be polite
        except Exception as e:
            day_cache[d] = []
            print(f"augment: API error for {d}: {e}")

    # build augmented columns
    fx["api_football_fixture_id"] = pd.NA
    fx["api_football_league_id"]  = pd.NA

    for i, r in fx.iterrows():
        d = r["date_only"]
        cands = day_cache.get(d, [])
        if not cands: continue

        h = norm(r.get("home_team",""))
        a = norm(r.get("away_team",""))
        h = alias.get(h, h)
        a = alias.get(a, a)

        # build candidate name pairs for today
        pairs = []
        for m in cands:
            home = norm(m.get("teams",{}).get("home",{}).get("name",""))
            away = norm(m.get("teams",{}).get("away",{}).get("name",""))
            fid  = m.get("fixture",{}).get("id")
            lid  = m.get("league",{}).get("id")
            pairs.append((home, away, fid, lid))

        # score by sum of similarity
        best = None
        best_score = 0.0
        for home, away, fid, lid in pairs:
            s1 = difflib.SequenceMatcher(None, h, home).ratio()
            s2 = difflib.SequenceMatcher(None, a, away).ratio()
            score = (s1 + s2) / 2.0
            if score > best_score:
                best_score = score
                best = (fid, lid)

        if best and best_score >= 0.72:  # reasonable threshold
            fx.at[i, "api_football_fixture_id"] = best[0]
            fx.at[i, "api_football_league_id"]  = best[1]

    fx.drop(columns=["date_only"], inplace=True, errors="ignore")
    fx.to_csv(OUT, index=False)
    print(f"augment: wrote {OUT} with API-FOOTBALL IDs")
if __name__ == "__main__":
    main()