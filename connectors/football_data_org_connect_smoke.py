#!/usr/bin/env python3
"""
football_data_org_connect_smoke.py — quick counts for Football-Data.org (FD.org)

FD.org API tokens are sometimes required for higher quotas. This smoke test
hits public endpoints commonly used for:
  - competitions
  - matches (next 7 days; last 30 days)
  - standings
  - scorers

Env (optional):
  FDORG_TOKEN   (GitHub Secret) — if you have one; if not, it still tries.
"""
import os, sys, json, datetime, requests

BASE = "https://api.football-data.org/v4"

def _today_utc():
    return datetime.datetime.utcnow().date()

def _iso(d):
    return d.strftime("%Y-%m-%d")

def _headers():
    token = os.environ.get("FDORG_TOKEN", "").strip()
    return {"X-Auth-Token": token} if token else {}

def _get(path, params=None):
    try:
        r = requests.get(f"{BASE}{path}", headers=_headers(), params=params or {}, timeout=40)
        return r.status_code, (r.json() if r.headers.get("content-type","").startswith("application/json") else r.text)
    except Exception as e:
        return -1, str(e)

def smoke():
    today = _today_utc()
    end7  = today + datetime.timedelta(days=7)
    start30 = today - datetime.timedelta(days=30)

    result = {
        "source":"football_data_org",
        "ok": False,
        "competitions": 0,
        "matches_next7d": 0,
        "matches_past30d": 0,
        "standings": 0,
        "scorers": 0,
        "errors": []
    }

    # competitions
    sc, body = _get("/competitions")
    if sc == 200:
        result["competitions"] = len(body.get("competitions", []))
    else:
        result["errors"].append(f"competitions sc={sc} body={str(body)[:160]}")

    # matches next 7d
    sc, body = _get("/matches", {"dateFrom": _iso(today), "dateTo": _iso(end7)})
    if sc == 200:
        result["matches_next7d"] = len(body.get("matches", []))
    else:
        result["errors"].append(f"matches_next7d sc={sc} body={str(body)[:160]}")

    # matches past 30d
    sc, body = _get("/matches", {"dateFrom": _iso(start30), "dateTo": _iso(today)})
    if sc == 200:
        result["matches_past30d"] = len(body.get("matches", []))
    else:
        result["errors"].append(f"matches_past30d sc={sc} body={str(body)[:160]}")

    # standings (for a couple common competitions, try EPL=PL, UCL=CL)
    for comp in ["PL", "CL"]:
        sc, body = _get(f"/competitions/{comp}/standings")
        if sc == 200:
            result["standings"] += 1  # we got 1 table for that comp
        else:
            result["errors"].append(f"standings({comp}) sc={sc} body={str(body)[:160]}")

    # scorers (EPL as sample)
    sc, body = _get("/competitions/PL/scorers")
    if sc == 200:
        result["scorers"] = len(body.get("scorers", []))
    else:
        result["errors"].append(f"scorers(PL) sc={sc} body={str(body)[:160]}")

    # green light = at least one of the match pulls gave results
    result["ok"] = (result["matches_next7d"] + result["matches_past30d"]) > 0
    print(json.dumps(result, indent=2))
    return result

if __name__ == "__main__":
    smoke()