#!/usr/bin/env python3
"""
football_data_org_connect_smoke.py — quick health counts for Football-Data.org (FD.org)

What it checks
--------------
- competitions
- matches (next 7 days; past 30 days)
- standings (PL, CL samples)
- scorers (PL sample)

Why this version?
-----------------
- Uses the shared HttpClient (rate-limits, retries, 429 Retry-After, timeouts)
- Restores body-preview on errors (first 160 chars)
- Surfaces useful rate-limit headers (X-Requests-Available-Minute, X-Requests-Used)
- Keeps a compact JSON summary to stdout (consumed by connectors_health_probe.py)

Env (optional)
--------------
FDORG_TOKEN  : bearer token for higher quotas (secret). Will try unauth if not set.
HTTP_TIMEOUT_SEC / HTTP_RETRIES: override client defaults if desired.
FDORG_MIN_INTERVAL_SEC / FDORG_MAX_CALLS_PER_MIN: per-provider rate knobs (rarely needed).

Outputs
-------
Prints a JSON dict with counts and errors, e.g.:
{
  "source": "football_data_org",
  "ok": true,
  "competitions": 63,
  "matches_next7d": 25,
  "matches_past30d": 240,
  "standings": 2,
  "scorers": 19,
  "errors": [],
  "rate_limit": {"avail_min": 10, "used": 50}
}
"""

import os
import json
import datetime
from connectors.http_client import HttpClient

BASE = "https://api.football-data.org/v4"

def _today_utc() -> datetime.date:
    return datetime.datetime.utcnow().date()

def _iso(d: datetime.date) -> str:
    return d.strftime("%Y-%m-%d")

def _headers():
    token = os.environ.get("FDORG_TOKEN", "").strip()
    return {"X-Auth-Token": token} if token else {}

def _hdr_int(h, key, default=0):
    try:
        return int(h.get(key, default))
    except Exception:
        return default

def smoke():
    http = HttpClient(provider="fdorg", timeout=int(os.environ.get("HTTP_TIMEOUT_SEC", 40)),
                      retries=int(os.environ.get("HTTP_RETRIES", 3)))
    headers = _headers()

    today   = _today_utc()
    end7    = today + datetime.timedelta(days=7)
    start30 = today - datetime.timedelta(days=30)

    result = {
        "source": "football_data_org",
        "ok": False,
        "competitions": 0,
        "matches_next7d": 0,
        "matches_past30d": 0,
        "standings": 0,
        "scorers": 0,
        "errors": [],
        "rate_limit": {}
    }

    # --- competitions
    sc, body, hdr = http.get(f"{BASE}/competitions", headers=headers)
    if sc == 200 and isinstance(body, dict):
        result["competitions"] = len(body.get("competitions", []))
    else:
        preview = body if isinstance(body, str) else json.dumps(body)[:160]
        result["errors"].append(f"competitions sc={sc} body={str(preview)[:160]}")

    # capture rate-limit info if present
    if isinstance(hdr, dict):
        result["rate_limit"] = {
            "avail_min": _hdr_int(hdr, "X-Requests-Available-Minute", 0),
            "used": _hdr_int(hdr, "X-Requests-Used", 0),
        }

    # --- matches next 7 days
    sc, body, _ = http.get(f"{BASE}/matches", headers=headers,
                           params={"dateFrom": _iso(today), "dateTo": _iso(end7)})
    if sc == 200 and isinstance(body, dict):
        result["matches_next7d"] = len(body.get("matches", []))
    else:
        preview = body if isinstance(body, str) else json.dumps(body)[:160]
        result["errors"].append(f"matches_next7d sc={sc} body={str(preview)[:160]}")

    # --- matches past 30 days
    sc, body, _ = http.get(f"{BASE}/matches", headers=headers,
                           params={"dateFrom": _iso(start30), "dateTo": _iso(today)})
    if sc == 200 and isinstance(body, dict):
        result["matches_past30d"] = len(body.get("matches", []))
    else:
        preview = body if isinstance(body, str) else json.dumps(body)[:160]
        result["errors"].append(f"matches_past30d sc={sc} body={str(preview)[:160]}")

    # --- standings (sample comps: EPL=PL, UCL=CL)
    for comp in ("PL", "CL"):
        sc, body, _ = http.get(f"{BASE}/competitions/{comp}/standings", headers=headers)
        if sc == 200 and isinstance(body, dict):
            result["standings"] += 1
        else:
            preview = body if isinstance(body, str) else json.dumps(body)[:160]
            result["errors"].append(f"standings({comp}) sc={sc} body={str(preview)[:160]}")

    # --- scorers (sample: EPL)
    sc, body, _ = http.get(f"{BASE}/competitions/PL/scorers", headers=headers)
    if sc == 200 and isinstance(body, dict):
        result["scorers"] = len(body.get("scorers", []))
    else:
        preview = body if isinstance(body, str) else json.dumps(body)[:160]
        result["errors"].append(f"scorers(PL) sc={sc} body={str(preview)[:160]}")

    # green light if we see matches either in next7d or past30d
    result["ok"] = (result["matches_next7d"] + result["matches_past30d"]) > 0

    print(json.dumps(result, indent=2))
    return result

if __name__ == "__main__":
    smoke()