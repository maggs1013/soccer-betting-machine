#!/usr/bin/env python3
"""
api_football_connect_smoke.py — wide-horizon smoke test for API-Football

Fixes:
- Use per-league *current season* from data/discovered_leagues.csv (not calendar year),
  which avoids "empty response" when e.g. EPL current season is 2024 but today.year==2025.

Behavior:
- Reads API_FOOTBALL_KEY from env
- Reads league IDs from API_FOOTBALL_LEAGUE_IDS (exported by discovery or locked in workflow)
- Loads per-league season from data/discovered_leagues.csv when present
- Counts fixtures in [today, today+AF_FIXTURES_LOOKAHEAD_DAYS] (default 120)
- If window returns 0, **fallback** to /fixtures?league=<id>&season=<s>&next=AF_FALLBACK_NEXT
- Caps by AF_SMOKE_MAX_LEAGUES and AF_SMOKE_MAX_FIXTURES, sleeps between calls to respect rate limits
- Prints a JSON summary consumed by connectors_health_probe.py

Env knobs:
  API_FOOTBALL_KEY               (required)
  API_FOOTBALL_LEAGUE_IDS        (CSV, e.g., 2,3,39,140,...)
  AF_FIXTURES_LOOKAHEAD_DAYS     (default 120)
  AF_SMOKE_MAX_LEAGUES           (default 100)
  AF_SMOKE_MAX_FIXTURES          (default 3000)
  AF_FALLBACK_NEXT               (default 300)
  AF_SMOKE_SLEEP_SEC             (default 6)
  HTTP_TIMEOUT_SEC               (default 40)
  HTTP_RETRIES                   (default 3)
"""

import os
import json
import csv
import time
import datetime
from connectors.http_client import HttpClient

BASE = "https://v3.football.api-sports.io"

def _iso(d): return d.strftime("%Y-%m-%d")
def _today(): return datetime.datetime.utcnow().date()

def _ids_from_env(name):
    raw = os.environ.get(name, "").strip()
    return [s.strip() for s in raw.split(",") if s.strip()]

def _season_map_from_csv(path="data/discovered_leagues.csv"):
    """
    Returns dict[str_id] -> int season.
    discovery writes: league_id,league_name,country,season,type
    """
    m = {}
    if not os.path.exists(path): return m
    try:
        with open(path, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                lid = str(r.get("league_id","")).strip()
                s   = str(r.get("season","")).strip()
                if lid and s.isdigit():
                    m[lid] = int(s)
    except Exception:
        pass
    return m

def smoke():
    key = os.environ.get("API_FOOTBALL_KEY","").strip()
    ids = _ids_from_env("API_FOOTBALL_LEAGUE_IDS")
    lookahead     = int(os.environ.get("AF_FIXTURES_LOOKAHEAD_DAYS", "120"))
    max_leagues   = int(os.environ.get("AF_SMOKE_MAX_LEAGUES", "100"))   # updated default
    max_fixtures  = int(os.environ.get("AF_SMOKE_MAX_FIXTURES", "3000"))
    fallback_next = int(os.environ.get("AF_FALLBACK_NEXT", "300"))
    sleep_sec     = int(os.environ.get("AF_SMOKE_SLEEP_SEC", "6"))
    timeout       = int(os.environ.get("HTTP_TIMEOUT_SEC", "40"))
    retries       = int(os.environ.get("HTTP_RETRIES", "3"))

    res = {
        "source": "api_football",
        "ok": False,
        "leagues_used": [],
        "fixtures_window_days": lookahead,
        "fixtures_total": 0,
        "errors": []
    }

    if not key:
        res["errors"].append("API_FOOTBALL_KEY missing")
        print(json.dumps(res, indent=2)); return res
    if not ids:
        res["errors"].append("API_FOOTBALL_LEAGUE_IDS missing/empty (discovery didn’t export?)")
        print(json.dumps(res, indent=2)); return res

    season_map = _season_map_from_csv()
    http = HttpClient(provider="apifootball", timeout=timeout, retries=retries)
    headers = {"x-apisports-key": key}

    today = _today()
    end   = today + datetime.timedelta(days=lookahead)

    total = 0
    used  = []

    for i, lid in enumerate(ids[:max_leagues]):
        season = season_map.get(str(lid), today.year)

        # Primary: date window
        sc, body, _ = http.get(
            f"{BASE}/fixtures",
            headers=headers,
            params={"league": int(lid), "season": int(season), "from": _iso(today), "to": _iso(end)}
        )
        count = 0
        if sc == 200 and isinstance(body, dict):
            count = len((body or {}).get("response", []))
        else:
            preview = body if isinstance(body, str) else json.dumps(body)[:160]
            res["errors"].append(f"fixtures(lid={lid}, season={season}) sc={sc} {str(preview)[:160]}")

        # Fallback: next=N if primary returns zero
        if count == 0:
            sc2, body2, _ = http.get(
                f"{BASE}/fixtures",
                headers=headers,
                params={"league": int(lid), "season": int(season), "next": fallback_next}
            )
            if sc2 == 200 and isinstance(body2, dict):
                count = len((body2 or {}).get("response", []))
            else:
                prev2 = body2 if isinstance(body2, str) else json.dumps(body2)[:160]
                res["errors"].append(f"fixtures(lid={lid}, season={season}, next={fallback_next}) sc={sc2} {str(prev2)[:160]}")

        total += count
        used.append(str(lid))

        if total >= max_fixtures:
            res["errors"].append(f"hit AF_SMOKE_MAX_FIXTURES cap ({max_fixtures}), stopping early")
            break

        # respect provider rate limits
        if i < len(ids[:max_leagues]) - 1:
            time.sleep(sleep_sec)

    res["leagues_used"] = used
    res["fixtures_total"] = total
    res["ok"] = total > 0
    print(json.dumps(res, indent=2))
    return res

if __name__ == "__main__":
    smoke()