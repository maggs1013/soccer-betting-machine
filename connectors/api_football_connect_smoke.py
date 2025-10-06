#!/usr/bin/env python3
"""
connectors/api_football_connect_smoke.py — Must-Have smoke with triple fallback

Order of attempts per league id:
  1) season (from discovery) + [today, today+H]
  2) next=N with that season
  3) (if no season or still 0) fetch current season via /leagues?id=<lid>, then:
       a) season_from_api + [today, today+H]
       b) next=N with season_from_api
  4) final fallback: next=N WITHOUT season
Counts any fixtures found; logs exact status/preview when not.
"""

import os, json, csv, time, datetime
from typing import Optional
from connectors.http_client import HttpClient

BASE = "https://v3.football.api-sports.io"

def _iso(d): return d.strftime("%Y-%m-%d")
def _today(): return datetime.datetime.utcnow().date()

def _env_csv(name):
    raw = os.environ.get(name, "").strip()
    return [s.strip() for s in raw.split(",") if s.strip()]

def _season_map_from_csv(path="data/discovered_leagues.csv"):
    m = {}
    if os.path.exists(path):
        try:
            with open(path, newline="", encoding="utf-8") as fh:
                for r in csv.DictReader(fh):
                    lid = (r.get("league_id") or "").strip()
                    s   = (r.get("season") or "").strip()
                    if lid and s.isdigit():
                        m[lid] = int(s)
        except Exception:
            pass
    return m

def _fetch_current_season(http: HttpClient, headers: dict, lid: str) -> Optional[int]:
    # /leagues?id=<lid> → response[0].seasons[] where current==True → year
    sc, body, _ = http.get(f"{BASE}/leagues", headers=headers, params={"id": int(lid)})
    if sc == 200 and isinstance(body, dict):
        resp = body.get("response") or []
        if resp:
            seasons = resp[0].get("seasons") or []
            for s in seasons:
                if s.get("current") is True and str(s.get("year","")).isdigit():
                    return int(s["year"])
    return None

def _count_fixtures(http: HttpClient, headers: dict, lid: str, season: Optional[int],
                    today: datetime.date, end: datetime.date, next_n: int):
    """
    Try window → next (with season) → next (no season)
    Returns (count, last_status, last_preview)
    """
    last_status = None
    last_preview = ""
    count = 0

    if season is not None:
        # window
        sc, body, _ = http.get(f"{BASE}/fixtures", headers=headers,
                               params={"league": int(lid), "season": int(season),
                                       "from": _iso(today), "to": _iso(end)})
        last_status = sc
        last_preview = (body if isinstance(body, str) else json.dumps(body))[:160]
        if sc == 200 and isinstance(body, dict):
            count = len((body or {}).get("response", []))
            if count > 0:
                return count, sc, last_preview

        # next with season
        sc, body, _ = http.get(f"{BASE}/fixtures", headers=headers,
                               params={"league": int(lid), "season": int(season),
                                       "next": next_n})
        last_status = sc
        last_preview = (body if isinstance(body, str) else json.dumps(body))[:160]
        if sc == 200 and isinstance(body, dict):
            count = len((body or {}).get("response", []))
            if count > 0:
                return count, sc, last_preview

    # final fallback: next without season
    sc, body, _ = http.get(f"{BASE}/fixtures", headers=headers,
                           params={"league": int(lid), "next": next_n})
    last_status = sc
    last_preview = (body if isinstance(body, str) else json.dumps(body))[:160]
    if sc == 200 and isinstance(body, dict):
        count = len((body or {}).get("response", []))

    return count, last_status, last_preview

def smoke():
    key = os.environ.get("API_FOOTBALL_KEY","").strip()
    ids = _env_csv("API_FOOTBALL_LEAGUE_IDS")
    lookahead     = int(os.environ.get("AF_FIXTURES_LOOKAHEAD_DAYS", "120"))
    max_leagues   = int(os.environ.get("AF_SMOKE_MAX_LEAGUES", "100"))
    max_fixtures  = int(os.environ.get("AF_SMOKE_MAX_FIXTURES", "3000"))
    fallback_next = int(os.environ.get("AF_FALLBACK_NEXT", "300"))
    sleep_sec     = int(os.environ.get("AF_SMOKE_SLEEP_SEC", "6"))
    timeout       = int(os.environ.get("HTTP_TIMEOUT_SEC", "40"))
    retries       = int(os.environ.get("HTTP_RETRIES", "3"))

    out = {"source":"api_football","ok":False,"leagues_used":[],
           "fixtures_window_days":lookahead,"fixtures_total":0,"errors":[]}

    if not key:
        out["errors"].append("API_FOOTBALL_KEY missing"); print(json.dumps(out, indent=2)); return out
    if not ids:
        out["errors"].append("API_FOOTBALL_LEAGUE_IDS missing/empty"); print(json.dumps(out, indent=2)); return out

    seasons_csv = _season_map_from_csv()
    http = HttpClient(provider="apifootball", timeout=timeout, retries=retries)
    headers = {"x-apisports-key": key}

    today = _today()
    end   = today + datetime.timedelta(days=lookahead)
    total = 0; used = []

    for i, lid in enumerate(ids[:max_leagues]):
        season = seasons_csv.get(str(lid))  # may be None

        # 1) Try with known season → fallback next
        cnt, st, prev = _count_fixtures(http, headers, lid, season, today, end, fallback_next)

        # 2) If still 0 and we don't know season, ask the API for current season and retry
        if cnt == 0 and season is None:
            season_api = _fetch_current_season(http, headers, lid)
            if season_api:
                cnt, st, prev = _count_fixtures(http, headers, lid, season_api, today, end, fallback_next)

        total += cnt
        used.append(str(lid))
        if cnt == 0:
            out["errors"].append(f"lid={lid} season={season if season is not None else 'NA'} last_status={st} preview={prev}")

        if total >= max_fixtures:
            out["errors"].append(f"hit AF_SMOKE_MAX_FIXTURES cap ({max_fixtures}), stopping early")
            break

        if i < len(ids[:max_leagues]) - 1:
            time.sleep(max(0, sleep_sec))  # respect plan rate limits

    out["leagues_used"] = used
    out["fixtures_total"] = total
    out["ok"] = total > 0
    print(json.dumps(out, indent=2))
    return out

if __name__ == "__main__":
    smoke()