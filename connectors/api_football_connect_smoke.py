#!/usr/bin/env python3
"""
api_football_connect_smoke.py — quick counts for API-Football

Env required:
  API_FOOTBALL_KEY          (GitHub Secret)
  API_FOOTBALL_LEAGUE_IDS   (GitHub Variable, CSV like "39,140,135")

What it does:
- For each league id, fetch fixtures in the next 7 days.
- Tries injuries endpoint for the last 14 days (optional).
- Prints counts to stdout and returns a dict (when imported).
"""
import os, sys, json, datetime, requests

def _today_utc():
    return datetime.datetime.utcnow().date()

def _iso(d):
    return d.strftime("%Y-%m-%d")

def smoke():
    key = os.environ.get("API_FOOTBALL_KEY", "").strip()
    lid_csv = os.environ.get("API_FOOTBALL_LEAGUE_IDS", "").strip()

    result = {
        "source": "api_football",
        "ok": False,
        "leagues": [],
        "fixtures_7d_total": 0,
        "injuries_14d_total": 0,
        "errors": []
    }

    if not key:
        result["errors"].append("API_FOOTBALL_KEY missing")
        return result
    if not lid_csv:
        result["errors"].append("API_FOOTBALL_LEAGUE_IDS missing")
        return result

    headers = {"x-apisports-key": key}
    leagues = [s.strip() for s in lid_csv.split(",") if s.strip()]
    result["leagues"] = leagues

    today = _today_utc()
    end7  = today + datetime.timedelta(days=7)
    start14= today - datetime.timedelta(days=14)

    fixtures_total = 0
    injuries_total = 0

    for lid in leagues:
        try:
            # Fixtures next 7 days
            fx = requests.get(
                "https://v3.football.api-sports.io/fixtures",
                headers=headers,
                params={"league": int(lid), "season": today.year, "from": _iso(today), "to": _iso(end7)},
                timeout=40
            )
            if fx.status_code != 200:
                result["errors"].append(f"fixtures(leag={lid}) status={fx.status_code} body={fx.text[:200]}")
            else:
                fixtures_total += len((fx.json() or {}).get("response", []))
        except Exception as e:
            result["errors"].append(f"fixtures(leag={lid}) exc={e}")

        try:
            # Injuries last 14 days (optional; not all plans include injuries)
            inj = requests.get(
                "https://v3.football.api-sports.io/injuries",
                headers=headers,
                params={"league": int(lid), "season": today.year, "from": _iso(start14), "to": _iso(today)},
                timeout=40
            )
            if inj.status_code == 200:
                injuries_total += len((inj.json() or {}).get("response", []))
            else:
                # Don't treat injuries failure as fatal; note and continue
                result["errors"].append(f"injuries(leag={lid}) status={inj.status_code}")
        except Exception as e:
            result["errors"].append(f"injuries(leag={lid}) exc={e}")

    result["fixtures_7d_total"]  = fixtures_total
    result["injuries_14d_total"] = injuries_total
    result["ok"] = fixtures_total > 0  # basic “green light” if we see any fixtures

    print(json.dumps(result, indent=2))
    return result

if __name__ == "__main__":
    smoke()