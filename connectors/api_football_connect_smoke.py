#!/usr/bin/env python3
"""
api_football_connect_smoke.py — wide-horizon smoke test for API-Football

Fixes / Adds:
- Uses per-league *current season* from data/discovered_leagues.csv (not calendar year)
- Counts fixtures in [today, today+AF_FIXTURES_LOOKAHEAD_DAYS]
- If that window returns 0, **fallback** to /fixtures?next=AF_FALLBACK_NEXT
- Respects caps + sleep between calls to avoid 429
- Prints JSON summary for connectors_health_probe.py

Env knobs:
  API_FOOTBALL_KEY               (required)
  API_FOOTBALL_LEAGUE_IDS        (CSV set by workflow; e.g. CORE leagues)
  AF_FIXTURES_LOOKAHEAD_DAYS     (default 120)
  AF_SMOKE_MAX_LEAGUES           (default 100)
  AF_SMOKE_MAX_FIXTURES          (default 3000)
  AF_FALLBACK_NEXT               (default 300)
  AF_SMOKE_SLEEP_SEC             (default 6)
  HTTP_TIMEOUT_SEC               (default 40)
  HTTP_RETRIES                   (default 3)
"""

import os, json, csv, time, datetime
from connectors.http_client import HttpClient

BASE = "https://v3.football.api-sports.io"

def _iso(d): return d.strftime("%Y-%m-%d")
def _today(): return datetime.datetime.utcnow().date()

def _ids_from_env(name):
    raw = os.environ.get(name, "").strip()
    return [s.strip() for s in raw.split(",") if s.strip()]

def _season_map_from_csv(path="data/discovered_leagues.csv"):
    """
    discovery writes: league_id,league_name,country,season,type
    return dict[str(lid)] -> int(season)
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
    ids = _ids_from_env("API_FOOTBALL_LEAGUE_IDS")         # locked in workflow to CORE ids
    lookahead     = int(os.environ.get("AF_FIXTURES_LOOKAHEAD_DAYS", "120"))
    max_leagues   = int(os.environ.get("AF_SMOKE_MAX_LEAGUES", "100"))
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
        res["errors"].append("API_FOOTBALL_KEY missing"); print(json.dumps(res, indent=2)); return res
    if not ids:
        res["errors"].append("API_FOOTBALL_LEAGUE_IDS missing/empty"); print(json.dumps(res, indent=2)); return res

    season_map = _season_map_from_csv()
    http = HttpClient(provider="apifootball", timeout=timeout, retries=retries)
    headers = {"x-apisports-key": key}

    today = _today()
    end   = today + datetime.timedelta(days=lookahead)
    total, used = 0, []

    for i, lid in enumerate(ids[:max_leagues]):
        season = season_map.get(str(lid), today.year)

        # Primary: season + from/to window
        sc, body, _ = http.get(f"{BASE}/fixtures", headers=headers,
                               params={"league": int(lid), "season": int(season),
                                       "from": _iso(today), "to": _iso(end)})
        count = 0
        if sc == 200 and isinstance(body, dict):
            count = len((body or {}).get("response", []))
        else:
            prev = body if isinstance(body, str) else json.dumps(body)[: