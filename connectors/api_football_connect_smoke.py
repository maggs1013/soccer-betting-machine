#!/usr/bin/env python3
"""
connectors/api_football_connect_smoke.py — wide-horizon smoke test for API-Football

Purpose
-------
Give a fast, reliable signal that API-Football is returning upcoming fixtures for the
leagues we care about (usually the Must-Have set that the workflow locks into
API_FOOTBALL_LEAGUE_IDS). It avoids “false zero” results by:

1) Using the *per-league current season* discovered earlier
   (data/discovered_leagues.csv), NOT the calendar year.
2) Querying a wide date window [today, today+H] (default H=120 days).
3) Falling back to /fixtures?next=N (default N=300) if the window yields 0,
   which returns “the next N fixtures” regardless of date filters.
4) Respecting plan rate limits by sleeping between requests.

Output
------
Prints a single JSON object to stdout, for connectors_health_probe.py to consume:

{
  "source": "api_football",
  "ok": true/false,
  "leagues_used": ["39","140",...],
  "fixtures_window_days": 120,
  "fixtures_total": 1234,
  "errors": ["...optional debug lines..."]
}

Environment (all optional except API_FOOTBALL_KEY and API_FOOTBALL_LEAGUE_IDS)
------------------------------------------------------------------------------
API_FOOTBALL_KEY               : required (string)
API_FOOTBALL_LEAGUE_IDS        : required (CSV of league ids; e.g., "2,3,39,140,...")
AF_FIXTURES_LOOKAHEAD_DAYS     : int, default "120"
AF_SMOKE_MAX_LEAGUES           : int, default "100"
AF_SMOKE_MAX_FIXTURES          : int, default "3000"
AF_FALLBACK_NEXT               : int, default "300"   (per league)
AF_SMOKE_SLEEP_SEC             : int, default "6"     (respect 10 calls/min)
HTTP_TIMEOUT_SEC               : int, default "40"
HTTP_RETRIES                   : int, default "3"

Notes
-----
- Uses the shared connectors/http_client.py (handles retries/429 Retry-After).
- We still add an explicit sleep to respect plan rate limits consistently.
"""

from __future__ import annotations

import os
import csv
import json
import time
import datetime
from typing import Dict, List

from connectors.http_client import HttpClient  # shared resilient client

BASE = "https://v3.football.api-sports.io"


def _iso(d: datetime.date) -> str:
    return d.strftime("%Y-%m-%d")


def _today() -> datetime.date:
    return datetime.datetime.utcnow().date()


def _env_csv(name: str) -> List[str]:
    raw = os.environ.get(name, "").strip()
    return [s.strip() for s in raw.split(",") if s.strip()]


def _season_map_from_csv(path: str = "data/discovered_leagues.csv") -> Dict[str, int]:
    """
    Return mapping: league_id (str) -> season (int)
    Discovery writes: league_id,league_name,country,season,type
    """
    mapping: Dict[str, int] = {}
    if not os.path.exists(path):
        return mapping
    try:
        with open(path, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                lid = str(r.get("league_id", "")).strip()
                s   = str(r.get("season", "")).strip()
                if lid and s.isdigit():
                    mapping[lid] = int(s)
    except Exception:
        # Best effort — if discovery can't be read we’ll fall back to calendar year
        pass
    return mapping


def smoke() -> Dict[str, object]:
    # ---- read env knobs ----
    key           = os.environ.get("API_FOOTBALL_KEY", "").strip()
    league_ids    = _env_csv("API_FOOTBALL_LEAGUE_IDS")  # workflow sets this (e.g., CORE ids)
    lookahead     = int(os.environ.get("AF_FIXTURES_LOOKAHEAD_DAYS", "120"))
    max_leagues   = int(os.environ.get("AF_SMOKE_MAX_LEAGUES", "100"))
    max_fixtures  = int(os.environ.get("AF_SMOKE_MAX_FIXTURES", "3000"))
    fallback_next = int(os.environ.get("AF_FALLBACK_NEXT", "300"))
    sleep_sec     = int(os.environ.get("AF_SMOKE_SLEEP_SEC", "6"))
    timeout       = int(os.environ.get("HTTP_TIMEOUT_SEC", "40"))
    retries       = int(os.environ.get("HTTP_RETRIES", "3"))

    # ---- response shell ----
    out: Dict[str, object] = {
        "source": "api_football",
        "ok": False,
        "leagues_used": [],
        "fixtures_window_days": lookahead,
        "fixtures_total": 0,
        "errors": []
    }

    # ---- sanity on required envs ----
    if not key:
        out["errors"].append("API_FOOTBALL_KEY missing")
        print(json.dumps(out, indent=2))
        return out
    if not league_ids:
        out["errors"].append("API_FOOTBALL_LEAGUE_IDS missing/empty")
        print(json.dumps(out, indent=2))
        return out

    # ---- season mapping from discovery (fallback to calendar year if missing) ----
    seasons = _season_map_from_csv()

    # ---- HTTP setup ----
    http = HttpClient(provider="apifootball", timeout=timeout, retries=retries)
    headers = {"x-apisports-key": key}

    today = _today()
    end   = today + datetime.timedelta(days=lookahead)

    total = 0
    used: List[str] = []

    # ---- main loop ----
    for i, lid in enumerate(league_ids[:max_leagues]):
        season = seasons.get(str(lid), today.year)

        # Primary: season + date window
        sc, body, _ = http.get(
            f"{BASE}/fixtures",
            headers=headers,
            params={"league": int(lid), "season": int(season), "from": _iso(today), "to": _iso(end)}
        )

        count = 0
        if sc == 200 and isinstance(body, dict):
            count = len((body or {}).get("response", []))
        else:
            preview = (body if isinstance(body, str) else json.dumps(body))[:160]
            out["errors"].append(f"fixtures(lid={lid}, season={season}) sc={sc} {preview}")

        # Fallback: ask for “next N” upcoming fixtures (ignores window)
        if count == 0:
            sc2, body2, _ = http.get(
                f"{BASE}/fixtures",
                headers=headers,
                params={"league": int(lid), "season": int(season), "next": fallback_next}
            )
            if sc2 == 200 and isinstance(body2, dict):
                count = len((body2 or {}).get("response", []))
            else:
                prev2 = (body2 if isinstance(body2, str) else json.dumps(body2))[:160]
                out["errors"].append(f"next(lid={lid}, season={season}, n={fallback_next}) sc={sc2} {prev2}")

        total += count
        used.append(str(lid))

        # global cap to avoid blowing quotas
        if total >= max_fixtures:
            out["errors"].append(f"hit AF_SMOKE_MAX_FIXTURES cap ({max_fixtures}), stopping early")
            break

        # respect provider rate limits (many plans are ~10 calls/min)
        if i < len(league_ids[:max_leagues]) - 1:
            time.sleep(max(0, sleep_sec))

    out["leagues_used"]   = used
    out["fixtures_total"] = total
    out["ok"]             = total > 0

    print(json.dumps(out, indent=2))
    return out


if __name__ == "__main__":
    smoke()