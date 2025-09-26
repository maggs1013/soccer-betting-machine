#!/usr/bin/env python3
"""
api_football_attach_ids.py
- Read data/UPCOMING_fixtures.csv (from your odds connector)
- Query API-FOOTBALL fixtures for the next 7 days for configured leagues
- Match by kickoff datetime + normalized home/away team names
- Write back official API-FOOTBALL IDs into UPCOMING_fixtures.csv:
    api_football_fixture_id, api_football_home_id, api_football_away_id,
    api_football_league_id, api_football_season
Safe behavior:
- Writes the same file with new columns (or keeps existing)
- If API key missing or nothing matches, leaves file intact with empty columns
"""

import os, re
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

DATA = "data"
FIX = os.path.join(DATA, "UPCOMING_fixtures.csv")

# Default API-FOOTBALL league ids (edit with repo variable or secrets as needed)
API_FOOTBALL_DEFAULTS = [
    39,   # EPL
    140,  # La Liga
    135,  # Serie A
    78,   # Bundesliga
    61,   # Ligue 1
    2,    # UEFA CL
    3,    # UEFA EL
    848,  # UEFA ECL (check)
    253,  # MLS (check)
    88,   # Eredivisie
    94,   # Primeira Liga
    144   # Belgium 1A
]

def norm_name(s: str) -> str:
    if not isinstance(s, str): return ""
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = s.replace("fc", " ").replace("cf", " ").replace("club", " ")
    return re.sub(r"\s+", " ", s).strip()

def to_utc_dt(iso_str: str):
    try:
        # handle both ISO with 'Z' and with timezone
        return datetime.fromisoformat(iso_str.replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def main():
    key = os.environ.get("API_FOOTBALL_KEY", "").strip()
    # allow override of league ids via repository variable (comma-separated)
    env_ids = os.environ.get("API_FOOTBALL_LEAGUE_IDS", "").strip()
    league_ids = [int(x) for x in env_ids.split(",") if x.strip().isdigit()] or API_FOOTBALL_DEFAULTS

    if not os.path.exists(FIX):
        print("No UPCOMING_fixtures.csv found; nothing to attach.")
        return

    fx = pd.read_csv(FIX)
    if fx.empty:
        print("UPCOMING_fixtures.csv is empty; nothing to attach.")
        return

    # Ensure new columns exist
    for c in ["api_football_fixture_id","api_football_home_id","api_football_away_id",
              "api_football_league_id","api_football_season"]:
        if c not in fx.columns:
            fx[c] = pd.NA

    if not key:
        fx.to_csv(FIX, index=False)
        print("API_FOOTBALL_KEY not set; wrote file unchanged with empty ID columns.")
        return

    headers = {"x-apisports-key": key}
    today = datetime.utcnow().date()
    date_from = today
    date_to = today + timedelta(days=7)

    # Build a quick index for local fixtures
    # normalized (date hour/minute to nearest 5min) + norm(home)+norm(away)
    def stamp_local(row):
        d = to_utc_dt(str(row.get("date","")))
        if not d: return None
        d = d.replace(second=0, microsecond=0)
        # round to nearest 5 minutes to be tolerant
        minute = (d.minute // 5) * 5
        d = d.replace(minute=minute)
        return (d.isoformat(), norm_name(str(row.get("home_team",""))), norm_name(str(row.get("away_team",""))))

    fx["_match_key"] = fx.apply(stamp_local, axis=1)

    matches = {}
    # Pull fixtures for each configured league
    for lid in league_ids:
        try:
            resp = requests.get(
                "https://v3.football.api-sports.io/fixtures",
                headers=headers,
                params={"league": lid, "season": _infer_season(today),
                        "from": str(date_from), "to": str(date_to)},
                timeout=40
            )
            if resp.status_code != 200:
                continue
            for m in (resp.json() or {}).get("response", []):
                fix = m.get("fixture", {})
                teams = m.get("teams", {})
                dt = to_utc_dt(fix.get("date",""))
                if not dt: continue
                dt = dt.replace(second=0, microsecond=0)
                minute = (dt.minute // 5) * 5
                dt = dt.replace(minute=minute)
                key_tup = (dt.isoformat(),
                           norm_name(teams.get("home", {}).get("name","")),
                           norm_name(teams.get("away", {}).get("name","")))
                matches[key_tup] = {
                    "fixture_id": fix.get("id"),
                    "home_id": teams.get("home", {}).get("id"),
                    "away_id": teams.get("away", {}).get("id"),
                    "league_id": lid,
                    "season": _infer_season(today),
                }
        except Exception:
            continue

    # Attach IDs where keys match
    def attach(row):
        k = row["_match_key"]
        if k and k in matches:
            m = matches[k]
            row["api_football_fixture_id"] = m["fixture_id"]
            row["api_football_home_id"] = m["home_id"]
            row["api_football_away_id"] = m["away_id"]
            row["api_football_league_id"] = m["league_id"]
            row["api_football_season"] = m["season"]
        return row

    fx = fx.apply(attach, axis=1)
    fx.drop(columns=["_match_key"], inplace=True, errors="ignore")
    fx.to_csv(FIX, index=False)
    print(f"api_football_attach_ids: wrote IDs into {FIX} rows={len(fx)}")

def _infer_season(today):
    # Simple inference for Euro seasons: Aug–Dec => current year; Jan–Jul => current-1
    y = today.year
    return y if today.month >= 8 else y - 1

if __name__ == "__main__":
    main()