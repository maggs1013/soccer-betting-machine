#!/usr/bin/env python3
"""
api_football_connect_smoke.py — wide-horizon smoke test for API-Football

What it does now
----------------
- Reads API_FOOTBALL_KEY from env (canonical; mapped in workflow)
- Loops *discovered* league IDs (from env: API_FOOTBALL_LEAGUE_IDS)
- Counts fixtures in [today, today+H] where H = AF_FIXTURES_LOOKAHEAD_DAYS (default 7; you can set 120)
- Caps work with AF_SMOKE_MAX_LEAGUES (default 60) and AF_SMOKE_MAX_FIXTURES (default 3000)
- Returns a JSON summary (printed to stdout) for CONNECTOR_HEALTH.md

Env knobs
---------
API_FOOTBALL_KEY          : required
API_FOOTBALL_LEAGUE_IDS   : comma-separated IDs (set by discovery)
AF_FIXTURES_LOOKAHEAD_DAYS: default 7  (set 120 to look far ahead)
AF_SMOKE_MAX_LEAGUES      : default 60 (increase if you want)
AF_SMOKE_MAX_FIXTURES     : default 3000 (stop early to save quota)
HTTP_TIMEOUT_SEC          : default 40
HTTP_RETRIES              : default 3
APIFOOTBALL_MIN_INTERVAL_SEC / APIFOOTBALL_MAX_CALLS_PER_MIN (rarely needed)

Outputs (printed JSON)
----------------------
{
  "source": "api_football",
  "ok": true/false,
  "leagues_used": [ ... up to AF_SMOKE_MAX_LEAGUES ... ],
  "fixtures_window_days": 120,
  "fixtures_total": 2345,
  "errors": []
}
"""

import os, json, datetime
from connectors.http_client import HttpClient

BASE = "https://v3.football.api-sports.io"

def _iso(d): return d.strftime("%Y-%m-%d")
def _today(): return datetime.datetime.utcnow().date()

def _ids_from_env(name):
    raw = os.environ.get(name, "").strip()
    return [s.strip() for s in raw.split(",") if s.strip()]

def smoke():
    key = os.environ.get("API_FOOTBALL_KEY","").strip()
    ids = _ids_from_env("API_FOOTBALL_LEAGUE_IDS")
    lookahead = int(os.environ.get("AF_FIXTURES_LOOKAHEAD_DAYS", "7"))
    max_leagues = int(os.environ.get("AF_SMOKE_MAX_LEAGUES", "60"))
    max_fixtures = int(os.environ.get("AF_SMOKE_MAX_FIXTURES", "3000"))
    timeout = int(os.environ.get("HTTP_TIMEOUT_SEC", "40"))
    retries = int(os.environ.get("HTTP_RETRIES", "3"))

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

    http = HttpClient(provider="apifootball", timeout=timeout, retries=retries)
    headers = {"x-apisports-key": key}
    today = _today()
    end   = today + datetime.timedelta(days=lookahead)

    total = 0
    used  = []
    for lid in ids[:max_leagues]:
        sc, body, _ = http.get(
            f"{BASE}/fixtures",
            headers=headers,
            params={"league": int(lid), "season": today.year, "from": _iso(today), "to": _iso(end)}
        )
        if sc == 200 and isinstance(body, dict):
            n = len((body or {}).get("response", []))
            total += n
            used.append(lid)
        else:
            preview = body if isinstance(body, str) else json.dumps(body)[:160]
            res["errors"].append(f"fixtures({lid}) sc={sc} {str(preview)[:160]}")
        if total >= max_fixtures:
            res["errors"].append(f"hit AF_SMOKE_MAX_FIXTURES cap ({max_fixtures}), stopping early")
            break

    res["leagues_used"] = used
    res["fixtures_total"] = total
    res["ok"] = total > 0

    print(json.dumps(res, indent=2))
    return res

if __name__ == "__main__":
    smoke()