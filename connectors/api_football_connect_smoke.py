#!/usr/bin/env python3
"""
api_football_connect_smoke.py — wide-horizon smoke test for API-Football

Fixes:
- Use per-league *current season* from data/discovered_leagues.csv (not calendar year),
  which avoids "empty response" when e.g. EPL current season is 2024 but today.year==2025.

Behavior:
- Reads API_FOOTBALL_KEY from env
- Reads league IDs from API_FOOTBALL_LEAGUE_IDS (exported by discovery) and
  the season mapping from data/discovered_leagues.csv when present.
- Counts fixtures in [today, today+AF_FIXTURES_LOOKAHEAD_DAYS] (default 120)
- Caps work by AF_SMOKE_MAX_LEAGUES and AF_SMOKE_MAX_FIXTURES to respect quotas
- Prints a JSON summary consumed by connectors_health_probe.py

Env knobs:
  API_FOOTBALL_KEY               (required)
  API_FOOTBALL_LEAGUE_IDS        (CSV, from discovery or fallback)
  AF_FIXTURES_LOOKAHEAD_DAYS     (default 120)
  AF_SMOKE_MAX_LEAGUES           (default 120)
  AF_SMOKE_MAX_FIXTURES          (default 3000)
  HTTP_TIMEOUT_SEC               (default 40)
  HTTP_RETRIES                   (default 3)
"""

import os, json, csv, datetime
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
    discovery writes league_id,league_name,country,season,type
    """
    m = {}
    if not os.path.exists(path): return m
    try:
        with open(path, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                lid = str(r.get("league_id","")).strip()
                try:
                    yr = int(str(r.get("season","")).strip())
                except Exception:
                    yr = None
                if lid and yr:
                    m[lid] = yr
    except Exception:
        pass
    return m

def smoke():
    key = os.environ.get("API_FOOTBALL_KEY","").strip()
    ids = _ids_from_env("API_FOOTBALL_LEAGUE_IDS")
    lookahead = int(os.environ.get("AF_FIXTURES_LOOKAHEAD_DAYS", "120"))
    max_leagues = int(os.environ.get("AF_SMOKE_MAX_LEAGUES", "120"))
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
        res["errors"].append("API_FOOTBALL_KEY missing"); print(json.dumps(res, indent=2)); return res
    if not ids:
        res["errors"].append("API_FOOTBALL_LEAGUE_IDS missing/empty (discovery didn’t export?)")
        print(json.dumps(res, indent=2)); return res

    # Load season map discovered from API (current=True seasons)
    season_map = _season_map_from_csv()

    http = HttpClient(provider="apifootball", timeout=timeout, retries=retries)
    headers = {"x-apisports-key": key}
    today = _today()
    end   = today + datetime.timedelta(days=lookahead)

    total = 0
    used  = []
    for lid in ids[:max_leagues]:
        # per-league season from discovery, fallback to calendar year
        season = season_map.get(str(lid), today.year)

        sc, body, _ = http.get(
            f"{BASE}/fixtures",
            headers=headers,
            params={"league": int(lid), "season": season, "from": _iso(today), "to": _iso(end)}
        )
        if sc == 200 and isinstance(body, dict):
            n = len((body or {}).get("response", []))
            total += n
            used.append(str(lid))
        else:
            preview = body if isinstance(body, str) else json.dumps(body)[:160]
            res["errors"].append(f"fixtures(lid={lid}, season={season}) sc={sc} {str(preview)[:160]}")
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